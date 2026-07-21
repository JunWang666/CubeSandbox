// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Interactive terminal over WebSocket.
//
// Bridges a browser WebSocket to the envd PTY stream inside a running
// sandbox (envd listens on ENVD_PORT and speaks Connect-RPC, reached through
// CubeProxy via Host-header routing — the same wire protocol as the Go
// SDK's `pty.go`).
//
// Sandboxes created with `allowPublicTraffic = false` require CubeProxy's
// `e2b-traffic-access-token` header. This endpoint does not send one: the
// token is only handed out at sandbox create time (CubeProxy enforces it
// against Redis directly), so terminal access to such sandboxes is rejected
// by the proxy and surfaces as an error frame.
//
// Hardening notes:
//
// - Auth token transport: browsers pass the auth token as a WebSocket
//   subprotocol (`Sec-WebSocket-Protocol: cube-terminal.<token>`) alongside
//   the token-free base protocol `cube-terminal`. The `token` query param
//   remains as a documented fallback for non-browser clients (CLI scripts,
//   curl) that can set arbitrary handshake headers anyway; the subprotocol
//   wins when both are present. When the client offered `cube-terminal`,
//   the server selects exactly that base protocol in the upgrade response —
//   Chrome aborts the handshake when it offered subprotocols but the server
//   selects none — while the token-bearing entry is never selected, so the
//   token is not echoed back.
// - Origin check: when an `Origin` header is present (browsers always send
//   one on WebSocket handshakes) its hostname must match the request `Host`
//   header; explicit ports must match as well, with a port-less `Host` read
//   as the Origin scheme's default port (80/443) so a proxy cannot widen
//   the check to arbitrary same-host services by stripping the port. A
//   mismatch is rejected with 403 before the upgrade. Clients that send no
//   Origin (curl, python, CLI) are unaffected.
// - Session cap: at most `terminal_max_sessions_per_sandbox` concurrent
//   sessions per sandbox (default 8); beyond the cap → 429.
// - Frame cap: browser WebSocket messages and frames are capped at 64 KiB;
//   oversized client traffic terminates the session with a protocol error.
// - Write deadline: every client-bound send has a 10 s deadline so a client
//   that stops reading cannot pin the writer task.
//
// Authorization scope (known gap): authentication proves *a* valid user,
// but there is no per-sandbox ownership or tenancy check — any
// authenticated user can open a terminal on any sandbox. CubeAPI's sandbox
// APIs have no per-sandbox ownership/tenancy model today, so this endpoint
// inherits the same platform-wide posture as the other sandbox actions
// (pause/resume/kill). Proper per-sandbox authorization is future
// cross-API multi-tenancy work.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        OriginalUri, Path, Query, State,
    },
    http::HeaderMap,
    response::Response,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

use crate::{
    cubemaster::SandboxStatus,
    error::{AppError, AppResult},
    state::AppState,
};

/// envd's Connect-RPC port inside every sandbox.
const ENVD_PORT: u16 = 49983;
/// Connect-RPC streaming content type.
const CONNECT_JSON: &str = "application/connect+json";
/// envd's built-in root credential (`Basic base64("root:")`).
const ENVD_BASIC_AUTH: &str = "Basic cm9vdDo=";
/// Server-side deadline handed to envd for the Start stream
/// (`Connect-Timeout-Ms`). Generous on purpose: the *idle* timeout below is
/// what actually reaps inactive sessions, so an actively used terminal is
/// not cut off mid-session.
const ENVD_STREAM_TIMEOUT_MS: &str = "86400000"; // 24 h
/// How long to wait for envd's start event before giving up on the session.
const START_EVENT_TIMEOUT: Duration = Duration::from_secs(30);
/// Deadline for one envd HTTP call (Start / SendInput / Update /
/// SendSignal) — *not* the long-lived Start stream, only the request up to
/// the response headers. Without it a hung envd (or a hung CubeProxy in
/// front of it) parks the awaiting `select!` branch in `pump_loop` forever:
/// the idle timer, disconnect detection and output reads all stop, leaking
/// the session slot and the sandbox shell. Once the deadline fires the
/// caller takes the normal error path (log / `teardown_session`) and the
/// pump keeps going or exits.
const ENVD_CALL_TIMEOUT: Duration = Duration::from_secs(10);
/// Subprotocol prefix browsers use to carry the auth token
/// (`Sec-WebSocket-Protocol: cube-terminal.<token>`).
const TOKEN_SUBPROTOCOL_PREFIX: &str = "cube-terminal.";
/// Base subprotocol the server selects in the 101 response when offered (see
/// `offered_base_subprotocol` — Chrome requires a selection).
const TERMINAL_SUBPROTOCOL: &str = "cube-terminal";
/// Browser WebSocket message/frame size cap (64 KiB).
const MAX_WS_MESSAGE_SIZE: usize = 64 * 1024;
/// Deadline for every client-bound send — a client that stops reading must
/// not pin the writer.
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// PTY size bounds accepted from clients.
const MIN_PTY_SIZE: u32 = 1;
const MAX_PTY_SIZE: u32 = 500;
const DEFAULT_COLS: u32 = 80;
const DEFAULT_ROWS: u32 = 24;

#[derive(Debug, Deserialize)]
pub struct TerminalQuery {
    cols: Option<u32>,
    rows: Option<u32>,
    /// Auth credential fallback for non-browser clients. Browsers pass the
    /// token via the `cube-terminal.<token>` WebSocket subprotocol instead
    /// (see `handshake_token`); the query param is only used when no token
    /// subprotocol was offered.
    token: Option<String>,
}

fn clamp_size(value: Option<u32>, default: u32) -> u32 {
    value.unwrap_or(default).clamp(MIN_PTY_SIZE, MAX_PTY_SIZE)
}

/// Client → server WebSocket messages.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ClientMessage {
    /// Base64-encoded bytes written to the PTY.
    Input {
        data: String,
    },
    Resize {
        cols: u32,
        rows: u32,
    },
}

/// Server → client WebSocket messages.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ServerMessage {
    Ready {
        pid: i64,
    },
    /// Base64-encoded bytes read from the PTY.
    Output {
        data: String,
    },
    Exit {
        code: Option<i64>,
    },
    Error {
        message: String,
    },
}

impl ServerMessage {
    fn to_message(&self) -> Message {
        Message::Text(serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string()))
    }
}

/// Shared per-sandbox live-session counters backing the concurrent-session
/// cap. Lives on `AppState` so every handler clone sees the same counts.
/// Uses a std Mutex, held only for a map update and never across `.await`.
#[derive(Clone, Default)]
pub struct TerminalSessionTracker {
    counts: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
}

impl TerminalSessionTracker {
    /// Take a session slot for `sandbox_id`, or return None when the sandbox
    /// already runs `max` live sessions. The returned guard releases the
    /// slot exactly once on drop, covering every session exit path.
    fn acquire(&self, sandbox_id: &str, max: usize) -> Option<TerminalSessionGuard> {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = counts.entry(sandbox_id.to_string()).or_insert(0);
        if *count >= max {
            return None;
        }
        *count += 1;
        Some(TerminalSessionGuard {
            tracker: self.clone(),
            sandbox_id: sandbox_id.to_string(),
        })
    }
}

/// RAII holder for one session slot — see `TerminalSessionTracker::acquire`.
struct TerminalSessionGuard {
    tracker: TerminalSessionTracker,
    sandbox_id: String,
}

impl Drop for TerminalSessionGuard {
    fn drop(&mut self) {
        let mut counts = self
            .tracker
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = counts.get_mut(&self.sandbox_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&self.sandbox_id);
            }
        }
    }
}

/// Auth credential offered on the WebSocket handshake, mirroring the
/// Bearer / X-API-Key split of `middleware::auth::unified_auth`: the
/// `cube-terminal.<token>` subprotocol (then the `token` query fallback)
/// maps to Bearer; the `X-API-Key` header maps to an API key.
#[derive(Debug)]
enum TerminalCredential {
    /// `cube-terminal.<token>` subprotocol or `token` query param.
    Bearer(String),
    /// `X-API-Key: <key>` header.
    ApiKey(String),
}

impl TerminalCredential {
    /// The raw credential string, whichever transport it arrived on.
    fn secret(&self) -> &str {
        match self {
            TerminalCredential::Bearer(token) => token,
            TerminalCredential::ApiKey(key) => key,
        }
    }
}

/// Pull the auth credential out of the WebSocket handshake. Bearer
/// (subprotocol, then query param — see `handshake_token`) takes priority
/// over `X-API-Key`, exactly as in `unified_auth`; a Bearer value that
/// trims to empty falls through to the header.
fn handshake_credential(
    headers: &HeaderMap,
    query_token: Option<String>,
) -> Option<TerminalCredential> {
    if let Some(token) = handshake_token(headers, query_token) {
        let token = token.trim();
        if !token.is_empty() {
            return Some(TerminalCredential::Bearer(token.to_string()));
        }
    }
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(|k| TerminalCredential::ApiKey(k.to_string()))
}

/// Pull the auth token out of the WebSocket handshake: the
/// `cube-terminal.<token>` subprotocol (browser path) wins; the `token`
/// query param is the documented fallback for non-browser clients.
fn handshake_token(headers: &HeaderMap, query_token: Option<String>) -> Option<String> {
    let from_subprotocol = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.split(',')
                .map(str::trim)
                .find_map(|p| p.strip_prefix(TOKEN_SUBPROTOCOL_PREFIX))
        })
        .map(str::to_string);
    from_subprotocol.or(query_token)
}

/// Whether the client offered the base `cube-terminal` subprotocol. When it
/// did, the server selects it in the 101 response — Chrome aborts the
/// handshake ("error" + close 1006, never firing `open`) when it offered
/// subprotocols but the server selects none, even though declining is legal
/// per RFC 6455. The token-bearing entry is never selected, so the token is
/// not echoed back.
fn offered_base_subprotocol(headers: &HeaderMap) -> bool {
    headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .map(str::trim)
                .any(|p| p == TERMINAL_SUBPROTOCOL)
        })
}

/// Whether the client offered a token-bearing `cube-terminal.<token>`
/// subprotocol.
fn offered_token_subprotocol(headers: &HeaderMap) -> bool {
    headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .map(str::trim)
                .any(|p| p.starts_with(TOKEN_SUBPROTOCOL_PREFIX))
        })
}

/// CSRF guard for browser clients: when an `Origin` header is present, its
/// hostname must match the request `Host` header (compared
/// case-insensitively). Port rules:
///
/// - both sides carry an explicit port → the ports must match;
/// - a port-less Origin (the scheme default) matches on hostname alone;
/// - an explicit Origin port against a port-less Host (a proxy that strips
///   the port from `Host`, e.g. nginx `proxy_set_header Host $host`) only
///   matches when the port equals the Origin scheme's default (80/443) —
///   anything else could be a different service merely sharing the
///   hostname. Proxies listening on a non-default port must forward the
///   full authority instead (`proxy_set_header Host $http_host`).
///
/// Clients that send no Origin header (curl, python, CLI) are not checked.
fn origin_matches_host(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return true;
    };
    let Some(host) = headers.get("host").and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some((scheme, authority)) = origin_parts(origin) else {
        return false;
    };
    if authority.eq_ignore_ascii_case(host) {
        return true;
    }
    fn split_port(s: &str) -> (&str, Option<&str>) {
        // Bracketed IPv6 literal without a port (e.g. "[::1]") has no port.
        if s.ends_with(']') {
            return (s, None);
        }
        match s.rsplit_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (s, None),
        }
    }
    let (o_host, o_port) = split_port(authority);
    let (h_host, h_port) = split_port(host);
    if !o_host.eq_ignore_ascii_case(h_host) {
        return false;
    }
    match (o_port, h_port) {
        (Some(a), Some(b)) => a == b,
        // Port-less Origin: scheme default, hostname match is enough.
        (None, _) => true,
        // Explicit Origin port vs port-less Host: the Host then *means* the
        // scheme default port, so only that port may match (see doc
        // comment).
        (Some(port), None) => port.parse::<u16>().ok() == scheme_default_port(scheme),
    }
}

/// Default port for Origin schemes that imply one.
fn scheme_default_port(scheme: &str) -> Option<u16> {
    if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("ws") {
        Some(80)
    } else if scheme.eq_ignore_ascii_case("https") || scheme.eq_ignore_ascii_case("wss") {
        Some(443)
    } else {
        None
    }
}

/// `scheme://authority/path` → `(scheme, authority)` (None for malformed
/// values).
fn origin_parts(origin: &str) -> Option<(&str, &str)> {
    let (scheme, rest) = origin.split_once("://")?;
    let authority = rest.split('/').next()?;
    if authority.is_empty() {
        None
    } else {
        Some((scheme, authority))
    }
}

/// GET /cubeapi/v1/sandboxes/:sandboxID/terminal/ws
///
/// Origin check, auth, sandbox validation and the per-sandbox session cap
/// all happen *before* the WebSocket upgrade so failures get proper HTTP
/// status codes (401/403/404/409/429); the browser only sees a successful
/// upgrade once the session is allowed to start.
pub async fn terminal_ws(
    State(state): State<AppState>,
    Path(sandbox_id): Path<String>,
    Query(query): Query<TerminalQuery>,
    headers: HeaderMap,
    uri: OriginalUri,
    ws: WebSocketUpgrade,
) -> AppResult<Response> {
    let cols = clamp_size(query.cols, DEFAULT_COLS);
    let rows = clamp_size(query.rows, DEFAULT_ROWS);
    let client_ip = client_ip(&headers);

    // Browser CSRF guard: an Origin that does not match the request host is
    // rejected before any auth work. Header-less clients skip the check.
    if !origin_matches_host(&headers) {
        audit_log(
            "auth-failure",
            &sandbox_id,
            None,
            None,
            client_ip.as_deref(),
            Some("origin-mismatch"),
        );
        return Err(AppError::Forbidden(
            "origin host does not match request host".to_string(),
        ));
    }

    let credential = handshake_credential(&headers, query.token);
    let user = match authenticate(&state, credential.as_ref(), uri.path()).await {
        Ok(user) => user,
        Err(err) => {
            audit_log(
                "auth-failure",
                &sandbox_id,
                None,
                None,
                client_ip.as_deref(),
                None,
            );
            return Err(err);
        }
    };

    let status = match state
        .services
        .sandboxes
        .get_sandbox_status(&sandbox_id)
        .await
    {
        Ok(status) => status,
        Err(err) => {
            if matches!(err, AppError::NotFound(_)) {
                audit_log(
                    "rejected",
                    &sandbox_id,
                    None,
                    user.as_deref(),
                    client_ip.as_deref(),
                    Some("sandbox-not-found"),
                );
            }
            return Err(err);
        }
    };
    if status != SandboxStatus::Running {
        audit_log(
            "rejected",
            &sandbox_id,
            None,
            user.as_deref(),
            client_ip.as_deref(),
            Some("sandbox-not-running"),
        );
        return Err(AppError::Conflict(format!(
            "sandbox {} is not running (status: {:?})",
            sandbox_id, status
        )));
    }

    let max_sessions = state.config.terminal_max_sessions_per_sandbox.max(1);
    let Some(session_guard) = state.terminal_sessions.acquire(&sandbox_id, max_sessions) else {
        audit_log(
            "rejected",
            &sandbox_id,
            None,
            user.as_deref(),
            client_ip.as_deref(),
            Some("session-limit"),
        );
        return Err(AppError::TooManyRequests(format!(
            "sandbox {} already has {} terminal sessions",
            sandbox_id, max_sessions
        )));
    };

    let idle_timeout = Duration::from_secs(state.config.terminal_idle_timeout_secs.max(1));
    let domain = state.config.sandbox_domain.clone();
    // A client offering the token-bearing `cube-terminal.<token>` subprotocol
    // must also offer the token-free base protocol: the server may only
    // select an offered protocol, never selects the token one (so the token
    // is not echoed), and browsers abort the handshake when nothing they
    // offered is selected. Without the base entry no valid selection exists.
    if offered_token_subprotocol(&headers) && !offered_base_subprotocol(&headers) {
        return Err(AppError::BadRequest(
            "offering a cube-terminal.<token> subprotocol requires also offering the base \
             cube-terminal subprotocol"
                .to_string(),
        ));
    }
    // Chrome aborts the handshake when it offered subprotocols but the server
    // selects none, so select the base `cube-terminal` protocol when offered.
    // The token-bearing `cube-terminal.<token>` entry is never selected —
    // the token is read from the request, never echoed (see module header).
    let ws = ws
        .max_message_size(MAX_WS_MESSAGE_SIZE)
        .max_frame_size(MAX_WS_MESSAGE_SIZE);
    let ws = if offered_base_subprotocol(&headers) {
        ws.protocols([TERMINAL_SUBPROTOCOL])
    } else {
        ws
    };
    Ok(ws.on_upgrade(move |socket| {
        let audit_user = user.clone();
        let audit_ip = client_ip.clone();
        async move {
            // Held until run_session returns, releasing the session slot
            // on every exit path (disconnect, error, idle timeout).
            let _session_guard = session_guard;
            run_session(
                socket,
                state,
                sandbox_id,
                domain,
                cols,
                rows,
                idle_timeout,
                audit_user,
                audit_ip,
            )
            .await;
        }
    }))
}

/// Validate the handshake credential (subprotocol or query token mapped to
/// Bearer, or the `X-API-Key` header — see `handshake_credential`) against
/// whichever auth backend is configured, mirroring `unified_auth`: auth
/// callback first (forwarding the credential on the same header the client
/// used), then the simple API key (`cube_api_key`), then open mode.
/// Returns the authenticated identity when one is known.
async fn authenticate(
    state: &AppState,
    credential: Option<&TerminalCredential>,
    request_path: &str,
) -> AppResult<Option<String>> {
    const MISSING_CREDENTIAL: &str = "Missing authentication token (cube-terminal subprotocol, \
         token query parameter, or X-API-Key header)";

    if let Some(callback_url) = state
        .config
        .auth_callback_url
        .as_deref()
        .filter(|u| !u.is_empty())
    {
        let credential =
            credential.ok_or_else(|| AppError::Unauthorized(MISSING_CREDENTIAL.to_string()))?;
        let req = state
            .http_client
            .post(callback_url)
            .header("X-Request-Path", request_path)
            .header("X-Request-Method", "GET");
        // Forward the credential on the same header the client used, like
        // `unified_auth` does, so the callback sees no behavioral difference
        // between the terminal and the plain HTTP routes.
        let req = match credential {
            TerminalCredential::Bearer(token) => {
                req.header("Authorization", format!("Bearer {}", token))
            }
            TerminalCredential::ApiKey(key) => req.header("X-API-Key", key),
        };
        let resp = req.send().await.map_err(|e| {
            tracing::error!(error = %e, callback_url = %callback_url, "auth callback request failed");
            AppError::Internal(anyhow::anyhow!("Auth callback unreachable: {}", e))
        })?;
        if resp.status().as_u16() == 200 {
            // The callback only answers allow/deny; the caller's identity is
            // not known to CubeAPI in this mode.
            return Ok(None);
        }
        return Err(AppError::Unauthorized(
            "Authentication rejected by callback".to_string(),
        ));
    }

    if let Some(expected_key) = state
        .config
        .cube_api_key
        .as_deref()
        .filter(|k| !k.is_empty())
    {
        let credential =
            credential.ok_or_else(|| AppError::Unauthorized(MISSING_CREDENTIAL.to_string()))?;
        if credential.secret() != expected_key {
            return Err(AppError::Unauthorized(
                "Invalid API key or token".to_string(),
            ));
        }
        // Simple-key mode only proves knowledge of the shared key; the
        // caller's identity is not known to CubeAPI in this mode.
        return Ok(None);
    }

    Ok(None)
}

fn client_ip(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string)
        })
}

fn audit_log(
    event: &str,
    sandbox_id: &str,
    pid: Option<i64>,
    user: Option<&str>,
    client_ip: Option<&str>,
    reason: Option<&str>,
) {
    tracing::info!(
        event = event,
        sandbox_id = sandbox_id,
        pid = pid,
        user = user.unwrap_or(""),
        client_ip = client_ip.unwrap_or(""),
        reason = reason.unwrap_or(""),
        "terminal session"
    );
}

/// One Connect-RPC envelope read from the envd response stream.
struct ConnectFrame {
    flags: u8,
    payload: Vec<u8>,
}

impl ConnectFrame {
    fn is_end_stream(&self) -> bool {
        self.flags & 0b10 != 0
    }
}

/// Incremental reader for Connect-RPC streaming responses (1 byte flags +
/// 4 bytes big-endian length + JSON payload per envelope).
struct ConnectFrameReader {
    resp: reqwest::Response,
    buf: Vec<u8>,
    eof: bool,
}

impl ConnectFrameReader {
    fn new(resp: reqwest::Response) -> Self {
        Self {
            resp,
            buf: Vec::new(),
            eof: false,
        }
    }

    async fn next_frame(&mut self) -> AppResult<Option<ConnectFrame>> {
        loop {
            if self.buf.len() >= 5 {
                let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]])
                    as usize;
                if self.buf.len() >= 5 + len {
                    let frame = ConnectFrame {
                        flags: self.buf[0],
                        payload: self.buf[5..5 + len].to_vec(),
                    };
                    self.buf.drain(..5 + len);
                    return Ok(Some(frame));
                }
            }
            if self.eof {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Err(AppError::Internal(anyhow::anyhow!(
                    "truncated envd terminal stream"
                )));
            }
            match self.resp.chunk().await {
                Ok(Some(chunk)) => self.buf.extend_from_slice(&chunk),
                Ok(None) => self.eof = true,
                Err(e) => {
                    return Err(AppError::Internal(anyhow::anyhow!(
                        "failed reading envd terminal stream: {}",
                        e
                    )))
                }
            }
        }
    }
}

fn envd_url(state: &AppState, method: &str) -> String {
    format!(
        "{}/process.Process/{}",
        state.config.sandbox_proxy_url.trim_end_matches('/'),
        method
    )
}

/// Unary envd call (SendInput / Update / SendSignal): plain JSON, no Connect
/// envelope. Failures are best-effort by design — a 404 simply means the
/// process already exited — so callers log and carry on.
async fn envd_unary(state: &AppState, host: &str, method: &str, body: Value) -> AppResult<()> {
    let resp = tokio::time::timeout(
        ENVD_CALL_TIMEOUT,
        state
            .http_client
            .post(envd_url(state, method))
            .header("Host", host)
            .header("Content-Type", "application/json")
            .header("Connect-Protocol-Version", "1")
            .header("Authorization", ENVD_BASIC_AUTH)
            .json(&body)
            .send(),
    )
    .await
    .map_err(|_| {
        AppError::Internal(anyhow::anyhow!(
            "envd {} request timed out after {:?}",
            method,
            ENVD_CALL_TIMEOUT
        ))
    })?
    .map_err(|e| AppError::Internal(anyhow::anyhow!("envd {} request failed: {}", method, e)))?;

    if !resp.status().is_success() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "envd {} returned HTTP {}",
            method,
            resp.status()
        )));
    }
    Ok(())
}

/// POST process.Process/Start and return the streaming frame reader.
async fn envd_start(
    state: &AppState,
    host: &str,
    cols: u32,
    rows: u32,
) -> AppResult<ConnectFrameReader> {
    let payload = json!({
        "process": {
            "cmd": "/bin/bash",
            "args": ["-i", "-l"],
            "envs": {
                "TERM": "xterm-256color",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
            },
        },
        "pty": { "size": { "rows": rows, "cols": cols } },
    });
    let body = connect_envelope(&serde_json::to_vec(&payload).map_err(anyhow::Error::from)?);

    let resp = tokio::time::timeout(
        ENVD_CALL_TIMEOUT,
        state
            .http_client
            .post(envd_url(state, "Start"))
            .header("Host", host)
            .header("Content-Type", CONNECT_JSON)
            .header("Connect-Protocol-Version", "1")
            .header("Connect-Content-Encoding", "identity")
            .header("Connect-Timeout-Ms", ENVD_STREAM_TIMEOUT_MS)
            .header("Authorization", ENVD_BASIC_AUTH)
            .body(body)
            .send(),
    )
    .await
    .map_err(|_| {
        AppError::Internal(anyhow::anyhow!(
            "envd terminal start request timed out after {:?}",
            ENVD_CALL_TIMEOUT
        ))
    })?
    .map_err(|e| {
        AppError::Internal(anyhow::anyhow!("envd terminal start request failed: {}", e))
    })?;

    if !resp.status().is_success() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "envd terminal start returned HTTP {}",
            resp.status()
        )));
    }
    Ok(ConnectFrameReader::new(resp))
}

fn connect_envelope(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(0);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// What a single envd data frame means for the terminal client.
enum EnvdEvent {
    Output(String),
    Exit(Option<i64>),
}

/// Parse a non-end-stream envelope payload. Returns Ok(None) for frames that
/// carry no terminal-relevant event (keepalives, stdout/stderr data in
/// non-PTY mode, the start event once consumed).
fn parse_data_frame(payload: &[u8]) -> AppResult<Option<EnvdEvent>> {
    let v: Value = serde_json::from_slice(payload)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("invalid envd JSON event: {}", e)))?;
    let Some(event) = v.get("event") else {
        return Ok(None);
    };
    if let Some(pty) = event
        .get("data")
        .and_then(|d| d.get("pty"))
        .and_then(Value::as_str)
    {
        return Ok(Some(EnvdEvent::Output(pty.to_string())));
    }
    if let Some(end) = event.get("end") {
        let code = end
            .get("exitCode")
            .and_then(Value::as_i64)
            .or_else(|| parse_exit_status(end.get("status").and_then(Value::as_str)));
        return Ok(Some(EnvdEvent::Exit(code)));
    }
    Ok(None)
}

fn parse_exit_status(status: Option<&str>) -> Option<i64> {
    status?
        .strip_prefix("exit status ")
        .and_then(|v| v.trim().parse::<i64>().ok())
}

/// Extract the end-stream error message, if the final envelope carries one.
fn end_stream_error(payload: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(payload).ok()?;
    v.get("error").map(|e| e.to_string())
}

/// Wait for envd's `{"event":{"start":{"pid":N}}}` frame.
async fn wait_for_start(reader: &mut ConnectFrameReader) -> AppResult<i64> {
    let wait = async {
        loop {
            let Some(frame) = reader.next_frame().await? else {
                return Err(AppError::Internal(anyhow::anyhow!(
                    "envd terminal stream ended before start event"
                )));
            };
            if frame.is_end_stream() {
                let detail = end_stream_error(&frame.payload)
                    .unwrap_or_else(|| "envd ended the terminal stream".to_string());
                return Err(AppError::Internal(anyhow::anyhow!(
                    "envd terminal start failed: {}",
                    detail
                )));
            }
            let v: Value = serde_json::from_slice(&frame.payload).map_err(|e| {
                AppError::Internal(anyhow::anyhow!("invalid envd JSON event: {}", e))
            })?;
            if let Some(pid) = v
                .get("event")
                .and_then(|e| e.get("start"))
                .and_then(|s| s.get("pid"))
                .and_then(Value::as_i64)
            {
                return Ok(pid);
            }
        }
    };
    tokio::time::timeout(START_EVENT_TIMEOUT, wait)
        .await
        .map_err(|_| {
            AppError::Internal(anyhow::anyhow!("timed out waiting for envd start event"))
        })?
}

/// Why a session is being torn down; recorded in the audit log.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CloseReason {
    ClientDisconnect,
    EnvdExit,
    IdleTimeout,
    Error,
}

impl CloseReason {
    fn as_str(&self) -> &'static str {
        match self {
            CloseReason::ClientDisconnect => "client-disconnect",
            CloseReason::EnvdExit => "envd-exit",
            CloseReason::IdleTimeout => "idle-timeout",
            CloseReason::Error => "error",
        }
    }
}

/// Send one client-bound message with a hard deadline. Returns false when
/// the send fails or times out — the caller must end the session so a
/// client that stopped reading cannot pin the writer.
async fn ws_send(tx: &mut futures::stream::SplitSink<WebSocket, Message>, msg: Message) -> bool {
    match tokio::time::timeout(WS_SEND_TIMEOUT, tx.send(msg)).await {
        Ok(Ok(())) => true,
        Ok(Err(err)) => {
            tracing::debug!(error = %err, "terminal: client send failed");
            false
        }
        Err(_) => {
            tracing::warn!("terminal: client send timed out");
            false
        }
    }
}

/// Best-effort close handshake, deadline-bound for the same reason as
/// `ws_send`.
async fn ws_close(tx: &mut futures::stream::SplitSink<WebSocket, Message>) {
    let _ = tokio::time::timeout(WS_SEND_TIMEOUT, tx.close()).await;
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    socket: WebSocket,
    state: AppState,
    sandbox_id: String,
    domain: String,
    cols: u32,
    rows: u32,
    idle_timeout: Duration,
    user: Option<String>,
    client_ip: Option<String>,
) {
    let host = format!("{}-{}.{}", ENVD_PORT, sandbox_id, domain);
    let (mut ws_tx, mut ws_rx) = socket.split();

    let mut reader = match envd_start(&state, &host, cols, rows).await {
        Ok(reader) => reader,
        Err(err) => {
            tracing::warn!(sandbox_id = %sandbox_id, error = %err, "terminal: envd start failed");
            let _ = ws_send(
                &mut ws_tx,
                ServerMessage::Error {
                    message: err.to_string(),
                }
                .to_message(),
            )
            .await;
            ws_close(&mut ws_tx).await;
            audit_log(
                "close",
                &sandbox_id,
                None,
                user.as_deref(),
                client_ip.as_deref(),
                Some(CloseReason::Error.as_str()),
            );
            return;
        }
    };

    let pid = match wait_for_start(&mut reader).await {
        Ok(pid) => pid,
        Err(err) => {
            tracing::warn!(sandbox_id = %sandbox_id, error = %err, "terminal: no start event");
            let _ = ws_send(
                &mut ws_tx,
                ServerMessage::Error {
                    message: err.to_string(),
                }
                .to_message(),
            )
            .await;
            ws_close(&mut ws_tx).await;
            audit_log(
                "close",
                &sandbox_id,
                None,
                user.as_deref(),
                client_ip.as_deref(),
                Some(CloseReason::Error.as_str()),
            );
            return;
        }
    };

    // The client vanished between the upgrade and the Ready frame: run the
    // same teardown as any other exit path so the shell is not left running.
    if !ws_send(&mut ws_tx, ServerMessage::Ready { pid }.to_message()).await {
        drop(reader);
        teardown_session(
            &state,
            &host,
            &sandbox_id,
            pid,
            &mut ws_tx,
            CloseReason::ClientDisconnect,
            user.as_deref(),
            client_ip.as_deref(),
        )
        .await;
        return;
    }
    audit_log(
        "open",
        &sandbox_id,
        Some(pid),
        user.as_deref(),
        client_ip.as_deref(),
        None,
    );

    let reason = pump_loop(
        &state,
        &host,
        pid,
        &mut reader,
        &mut ws_tx,
        &mut ws_rx,
        idle_timeout,
    )
    .await;

    if reason == CloseReason::IdleTimeout {
        audit_log(
            "timeout",
            &sandbox_id,
            Some(pid),
            user.as_deref(),
            client_ip.as_deref(),
            None,
        );
    }

    drop(reader);
    teardown_session(
        &state,
        &host,
        &sandbox_id,
        pid,
        &mut ws_tx,
        reason,
        user.as_deref(),
        client_ip.as_deref(),
    )
    .await;
}

/// Shared session teardown: best-effort SIGKILL of the shell (a 404 just
/// means the process already exited), close handshake, and the audit close
/// record. Runs on every exit path after the shell has started so the
/// sandbox never keeps an orphaned shell.
#[allow(clippy::too_many_arguments)]
async fn teardown_session(
    state: &AppState,
    host: &str,
    sandbox_id: &str,
    pid: i64,
    ws_tx: &mut futures::stream::SplitSink<WebSocket, Message>,
    reason: CloseReason,
    user: Option<&str>,
    client_ip: Option<&str>,
) {
    if let Err(err) = envd_unary(
        state,
        host,
        "SendSignal",
        json!({"process": {"pid": pid}, "signal": "SIGNAL_SIGKILL"}),
    )
    .await
    {
        tracing::debug!(sandbox_id = %sandbox_id, pid = pid, error = %err, "terminal: SIGKILL failed");
    }
    let _ = ws_send(ws_tx, Message::Close(None)).await;
    ws_close(ws_tx).await;
    audit_log(
        "close",
        sandbox_id,
        Some(pid),
        user,
        client_ip,
        Some(reason.as_str()),
    );
}

#[allow(clippy::too_many_arguments)]
async fn pump_loop(
    state: &AppState,
    host: &str,
    pid: i64,
    reader: &mut ConnectFrameReader,
    ws_tx: &mut futures::stream::SplitSink<WebSocket, Message>,
    ws_rx: &mut futures::stream::SplitStream<WebSocket>,
    idle_timeout: Duration,
) -> CloseReason {
    // The idle timer only reaps truly dormant sessions: any client message
    // or shell output resets it, so a session actively streaming output
    // (tail -f, a running build) is not cut off just because the user is
    // watching rather than typing.
    let idle = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle);
    let reset_idle = |idle: std::pin::Pin<&mut tokio::time::Sleep>| {
        idle.reset(tokio::time::Instant::now() + idle_timeout);
    };
    loop {
        tokio::select! {
            frame = reader.next_frame() => {
                match frame {
                    Ok(Some(frame)) if frame.is_end_stream() => {
                        if let Some(detail) = end_stream_error(&frame.payload) {
                            let _ = ws_send(ws_tx, ServerMessage::Error { message: detail }.to_message()).await;
                        }
                        return CloseReason::EnvdExit;
                    }
                    Ok(Some(frame)) => match parse_data_frame(&frame.payload) {
                        Ok(Some(EnvdEvent::Output(data))) => {
                            reset_idle(idle.as_mut());
                            if !ws_send(ws_tx, ServerMessage::Output { data }.to_message()).await {
                                return CloseReason::ClientDisconnect;
                            }
                        }
                        Ok(Some(EnvdEvent::Exit(code))) => {
                            let _ = ws_send(ws_tx, ServerMessage::Exit { code }.to_message()).await;
                            return CloseReason::EnvdExit;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "terminal: bad envd frame");
                        }
                    },
                    // envd closed the stream without an end event.
                    Ok(None) => {
                        let _ = ws_send(ws_tx, ServerMessage::Exit { code: None }.to_message()).await;
                        return CloseReason::EnvdExit;
                    }
                    Err(err) => {
                        let _ = ws_send(ws_tx, ServerMessage::Error { message: err.to_string() }.to_message()).await;
                        return CloseReason::Error;
                    }
                }
            }
            msg = ws_rx.next() => {
                match msg {
                    // Client went away (close frame or dropped connection).
                    None => return CloseReason::ClientDisconnect,
                    Some(Ok(Message::Close(_))) => return CloseReason::ClientDisconnect,
                    // Read error (e.g. a message over the 64 KiB cap):
                    // best-effort error frame, then end the session.
                    Some(Err(err)) => {
                        let _ = ws_send(
                            ws_tx,
                            ServerMessage::Error {
                                message: format!("connection error: {}", err),
                            }
                            .to_message(),
                        )
                        .await;
                        return CloseReason::ClientDisconnect;
                    }
                    Some(Ok(Message::Text(text))) => {
                        reset_idle(idle.as_mut());
                        handle_client_message(state, host, pid, &text).await;
                    }
                    // Binary / ping / pong frames carry no terminal meaning,
                    // but still count as client liveness.
                    Some(Ok(_)) => reset_idle(idle.as_mut()),
                }
            }
            () = &mut idle => {
                let _ = ws_send(ws_tx, ServerMessage::Error {
                    message: format!(
                        "terminal session timed out after {}s of inactivity",
                        idle_timeout.as_secs()
                    ),
                }
                .to_message())
                .await;
                return CloseReason::IdleTimeout;
            }
        }
    }
}

/// Handle one client text message. envd call errors are tolerated (logged)
/// because the process may have already exited.
async fn handle_client_message(state: &AppState, host: &str, pid: i64, text: &str) {
    let msg: ClientMessage = match serde_json::from_str(text) {
        Ok(msg) => msg,
        Err(_) => return,
    };
    let result = match msg {
        ClientMessage::Input { data } => {
            envd_unary(
                state,
                host,
                "SendInput",
                json!({"process": {"pid": pid}, "input": {"pty": data}}),
            )
            .await
        }
        ClientMessage::Resize { cols, rows } => {
            envd_unary(
                state,
                host,
                "Update",
                json!({
                    "process": {"pid": pid},
                    "pty": {"size": {
                        "rows": clamp_size(Some(rows), DEFAULT_ROWS),
                        "cols": clamp_size(Some(cols), DEFAULT_COLS),
                    }},
                }),
            )
            .await
        }
    };
    if let Err(err) = result {
        tracing::debug!(pid = pid, error = %err, "terminal: envd call failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ServerConfig,
        logging::{arc, noop::NoopLogger},
        routes::build_router,
    };
    use axum::{
        body::{Body, Bytes},
        extract::State as AxumState,
        http::StatusCode,
        response::IntoResponse,
        routing::{get, post},
        Json, Router,
    };
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
    use futures::channel::mpsc::{unbounded, UnboundedSender};
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio_tungstenite::{connect_async, tungstenite};

    const SANDBOX_ID: &str = "sb-terminal-test";
    const MOCK_PID: i64 = 4321;
    const STATUS_RUNNING: i32 = 1;
    const STATUS_PAUSED: i32 = 5;
    const RET_CODE_NOT_FOUND: i32 = 130404;

    // ── Mock envd (Connect-RPC over HTTP) ─────────────────────────────────

    #[derive(Default)]
    struct EnvdSpy {
        start_payload: Option<Value>,
        send_input: Vec<Value>,
        update: Vec<Value>,
        send_signal: Vec<Value>,
    }

    type FrameTx = UnboundedSender<Result<Vec<u8>, std::io::Error>>;

    #[derive(Clone, Default)]
    struct MockEnvd {
        spy: Arc<Mutex<EnvdSpy>>,
        stream_tx: Arc<Mutex<Option<FrameTx>>>,
        /// When set, SendInput never responds, simulating a hung envd.
        hang_send_input: Arc<std::sync::atomic::AtomicBool>,
    }

    impl MockEnvd {
        async fn push_frame(&self, payload: Value) {
            if let Some(tx) = &*self.stream_tx.lock().await {
                let bytes = serde_json::to_vec(&payload).expect("frame JSON");
                let _ = tx.unbounded_send(Ok(connect_envelope(&bytes)));
            }
        }
    }

    async fn envd_start_handler(
        AxumState(mock): AxumState<MockEnvd>,
        body: Bytes,
    ) -> impl IntoResponse {
        // The request must be exactly one Connect envelope (flags=0).
        assert!(
            body.len() >= 5,
            "start request must carry a Connect envelope"
        );
        let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        assert_eq!(body[0], 0, "start envelope flags must be 0");
        assert_eq!(len, body.len() - 5, "start envelope length must match body");
        let payload: Value = serde_json::from_slice(&body[5..]).expect("start payload JSON");
        mock.spy.lock().await.start_payload = Some(payload);

        let (tx, rx) = unbounded::<Result<Vec<u8>, std::io::Error>>();
        *mock.stream_tx.lock().await = Some(tx.clone());
        let start = serde_json::to_vec(&json!({"event": {"start": {"pid": MOCK_PID}}}))
            .expect("start event JSON");
        let _ = tx.unbounded_send(Ok(connect_envelope(&start)));

        (
            [(axum::http::header::CONTENT_TYPE, CONNECT_JSON)],
            Body::from_stream(rx),
        )
    }

    async fn envd_send_input_handler(
        AxumState(mock): AxumState<MockEnvd>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        if mock
            .hang_send_input
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            // A hung envd never answers; the client's ENVD_CALL_TIMEOUT is
            // what unblocks the session.
            std::future::pending::<()>().await;
        }
        mock.spy.lock().await.send_input.push(body.clone());
        // Echo the input back as PTY output, like a real shell would.
        if let Some(pty) = body.get("input").and_then(|i| i.get("pty")).cloned() {
            mock.push_frame(json!({"event": {"data": {"pty": pty}}}))
                .await;
        }
        Json(json!({}))
    }

    async fn envd_update_handler(
        AxumState(mock): AxumState<MockEnvd>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        mock.spy.lock().await.update.push(body);
        Json(json!({}))
    }

    async fn envd_send_signal_handler(
        AxumState(mock): AxumState<MockEnvd>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        mock.spy.lock().await.send_signal.push(body);
        // Killing the shell ends the process and then the stream.
        mock.push_frame(json!({"event": {"end": {"exitCode": 137}}}))
            .await;
        let _ = mock.stream_tx.lock().await.take();
        Json(json!({}))
    }

    async fn spawn_mock_envd() -> (String, MockEnvd) {
        spawn_mock_envd_with(MockEnvd::default()).await
    }

    async fn spawn_mock_envd_with(mock: MockEnvd) -> (String, MockEnvd) {
        let app = Router::new()
            .route("/process.Process/Start", post(envd_start_handler))
            .route("/process.Process/SendInput", post(envd_send_input_handler))
            .route("/process.Process/Update", post(envd_update_handler))
            .route(
                "/process.Process/SendSignal",
                post(envd_send_signal_handler),
            )
            .with_state(mock.clone());
        (spawn_server(app).await, mock)
    }

    // ── Mock CubeMaster ───────────────────────────────────────────────────

    async fn spawn_mock_master(status: i32) -> String {
        let handler = move || async move {
            Json(json!({
                "requestID": "req-info",
                "ret": { "ret_code": 0, "ret_msg": "ok" },
                "data": [{
                    "sandbox_id": SANDBOX_ID,
                    "status": status,
                    "host_id": "",
                    "containers": [],
                }],
            }))
        };
        spawn_server(Router::new().route("/cube/sandbox/info", get(handler))).await
    }

    async fn spawn_mock_master_not_found() -> String {
        async fn handler() -> Json<Value> {
            Json(json!({
                "requestID": "req-info",
                "ret": { "ret_code": RET_CODE_NOT_FOUND, "ret_msg": "no such sandbox" },
                "data": [],
            }))
        }
        spawn_server(Router::new().route("/cube/sandbox/info", get(handler))).await
    }

    // ── Mock auth callback ────────────────────────────────────────────────

    type CapturedHeaders = Arc<Mutex<Vec<(String, String)>>>;

    async fn spawn_auth_callback(status: StatusCode) -> (String, CapturedHeaders) {
        let captured: CapturedHeaders = Arc::new(Mutex::new(Vec::new()));
        let captured_clone = captured.clone();
        let handler = move |req: axum::http::Request<Body>| {
            let captured = captured_clone.clone();
            async move {
                let mut guard = captured.lock().await;
                for (k, v) in req.headers() {
                    guard.push((k.to_string(), v.to_str().unwrap_or("").to_string()));
                }
                axum::http::Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .expect("callback response")
            }
        };
        let url = spawn_server(Router::new().route("/auth", post(handler))).await;
        (format!("{}/auth", url), captured)
    }

    // ── Shared helpers ────────────────────────────────────────────────────

    async fn spawn_server(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });
        format!("http://{}", addr)
    }

    fn test_config(proxy_url: &str, master_url: &str) -> ServerConfig {
        ServerConfig {
            cubemaster_url: master_url.to_string(),
            sandbox_proxy_url: proxy_url.to_string(),
            ..Default::default()
        }
    }

    async fn spawn_app(config: ServerConfig) -> String {
        let state = AppState::new(config, arc(NoopLogger)).await;
        spawn_server(build_router(state)).await
    }

    fn ws_url(app: &str, query: &str) -> String {
        format!(
            "{}/cubeapi/v1/sandboxes/{}/terminal/ws{}",
            app.replacen("http", "ws", 1),
            SANDBOX_ID,
            query
        )
    }

    async fn recv_json<S, E>(ws: &mut S) -> Value
    where
        S: futures::Stream<Item = Result<tungstenite::Message, E>> + Unpin,
        E: std::fmt::Debug,
    {
        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("timed out waiting for ws message")
            .expect("ws stream ended unexpectedly")
            .expect("ws read error");
        match msg {
            tungstenite::Message::Text(text) => serde_json::from_str(&text).expect("message JSON"),
            other => panic!("expected text message, got {:?}", other),
        }
    }

    async fn wait_for_recorded<F>(fetch: F) -> Value
    where
        F: Fn() -> futures::future::BoxFuture<'static, Option<Value>>,
    {
        for _ in 0..100 {
            if let Some(v) = fetch().await {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("expected envd call was not recorded");
    }

    fn first_of(
        spy: &Arc<Mutex<EnvdSpy>>,
        pick: fn(&EnvdSpy) -> Option<Value>,
    ) -> impl Fn() -> futures::future::BoxFuture<'static, Option<Value>> {
        let spy = spy.clone();
        move || {
            let spy = spy.clone();
            Box::pin(async move {
                let guard = spy.lock().await;
                pick(&guard)
            })
        }
    }

    type ConnectResult = Result<
        (
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            axum::http::Response<Option<Vec<u8>>>,
        ),
        tungstenite::Error,
    >;

    fn expect_upgrade_error(result: ConnectResult, status: StatusCode) {
        match result {
            Err(tungstenite::Error::Http(resp)) => assert_eq!(resp.status(), status),
            other => panic!(
                "expected HTTP {} before upgrade, got {:?}",
                status,
                other.is_ok()
            ),
        }
    }

    /// Raw WebSocket handshake carrying the given `Sec-WebSocket-Protocol`
    /// header value. Returns the response head and the still-open TCP stream
    /// so callers can assert on the status line before (maybe) continuing as
    /// a WebSocket.
    async fn raw_handshake(app: &str, protocols: &str) -> (String, tokio::net::TcpStream) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let url = ws_url(app, "");
        let without_scheme = url.strip_prefix("ws://").expect("ws url");
        let (authority, path) = without_scheme
            .split_once('/')
            .map(|(a, p)| (a.to_string(), format!("/{}", p)))
            .expect("ws path");

        let mut stream = tokio::net::TcpStream::connect(&authority)
            .await
            .expect("tcp connect");
        let request = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: {authority}\r\n\
             Connection: Upgrade\r\n\
             Upgrade: websocket\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: {key}\r\n\
             Sec-WebSocket-Protocol: {protocols}\r\n\
             \r\n",
            key = BASE64.encode(b"0123456789abcdef"),
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write handshake");

        // Byte-by-byte so no WebSocket frame bytes past the header
        // terminator are swallowed.
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        while !buf.ends_with(b"\r\n\r\n") {
            let n = stream.read(&mut byte).await.expect("read handshake");
            assert_eq!(n, 1, "handshake response truncated");
            buf.push(byte[0]);
        }
        (String::from_utf8_lossy(&buf).to_string(), stream)
    }

    /// Manual WebSocket handshake carrying `Sec-WebSocket-Protocol` headers.
    /// The browser offers both the base `cube-terminal` protocol and the
    /// token-bearing `cube-terminal.<token>` entry; the server must select
    /// exactly the base protocol (Chrome aborts the handshake when offered
    /// subprotocols go unanswered) and must never echo the token one.
    async fn connect_with_subprotocol(
        app: &str,
        token_subprotocol: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
        let (head, stream) = raw_handshake(
            app,
            &format!("{}, {}", TERMINAL_SUBPROTOCOL, token_subprotocol),
        )
        .await;
        assert!(
            head.starts_with("HTTP/1.1 101"),
            "expected 101 Switching Protocols, got: {}",
            head
        );
        let selected = head
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("sec-websocket-protocol"))
            .map(|l| {
                l.split_once(':')
                    .expect("header colon")
                    .1
                    .trim()
                    .to_string()
            });
        assert_eq!(
            selected.as_deref(),
            Some("cube-terminal"),
            "server must select exactly the base subprotocol: {}",
            head
        );

        tokio_tungstenite::WebSocketStream::from_raw_socket(
            stream,
            tungstenite::protocol::Role::Client,
            None,
        )
        .await
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn happy_path_bridges_ws_and_envd_pty() {
        let (proxy_url, mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let app = spawn_app(test_config(&proxy_url, &master_url)).await;

        let (mut ws, _resp) = connect_async(ws_url(&app, "?cols=100&rows=30"))
            .await
            .expect("websocket upgrade should succeed");

        let ready = recv_json(&mut ws).await;
        assert_eq!(ready, json!({"type": "ready", "pid": MOCK_PID}));

        // The Start request went out with the requested PTY size.
        let start = mock
            .spy
            .lock()
            .await
            .start_payload
            .clone()
            .expect("start payload recorded");
        assert_eq!(start["pty"]["size"], json!({"rows": 30, "cols": 100}));
        assert_eq!(start["process"]["cmd"], json!("/bin/bash"));

        // Client input is forwarded to envd SendInput, and envd PTY data
        // frames come back as output messages.
        let input_b64 = BASE64.encode("echo hi\n");
        ws.send(tungstenite::Message::Text(
            json!({"type": "input", "data": input_b64}).to_string(),
        ))
        .await
        .expect("send input");

        let output = recv_json(&mut ws).await;
        assert_eq!(output, json!({"type": "output", "data": input_b64}));

        let send_input =
            wait_for_recorded(first_of(&mock.spy, |s| s.send_input.first().cloned())).await;
        assert_eq!(
            send_input,
            json!({"process": {"pid": MOCK_PID}, "input": {"pty": input_b64}})
        );

        // Resize is forwarded to envd Update.
        ws.send(tungstenite::Message::Text(
            json!({"type": "resize", "cols": 120, "rows": 40}).to_string(),
        ))
        .await
        .expect("send resize");

        let update = wait_for_recorded(first_of(&mock.spy, |s| s.update.first().cloned())).await;
        assert_eq!(
            update,
            json!({"process": {"pid": MOCK_PID}, "pty": {"size": {"rows": 40, "cols": 120}}})
        );

        // An envd end event becomes an exit message, and teardown kills the
        // shell via SendSignal SIGKILL.
        mock.push_frame(json!({"event": {"end": {"exitCode": 0}}}))
            .await;
        let exit = recv_json(&mut ws).await;
        assert_eq!(exit, json!({"type": "exit", "code": 0}));

        let signal =
            wait_for_recorded(first_of(&mock.spy, |s| s.send_signal.first().cloned())).await;
        assert_eq!(
            signal,
            json!({"process": {"pid": MOCK_PID}, "signal": "SIGNAL_SIGKILL"})
        );
    }

    #[tokio::test]
    async fn client_disconnect_kills_the_shell() {
        let (proxy_url, mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let app = spawn_app(test_config(&proxy_url, &master_url)).await;

        let (mut ws, _resp) = connect_async(ws_url(&app, ""))
            .await
            .expect("websocket upgrade should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        ws.close(None).await.expect("close websocket");
        let signal =
            wait_for_recorded(first_of(&mock.spy, |s| s.send_signal.first().cloned())).await;
        assert_eq!(signal["signal"], json!("SIGNAL_SIGKILL"));
        assert_eq!(signal["process"]["pid"], json!(MOCK_PID));
    }

    #[tokio::test]
    async fn idle_timeout_terminates_session() {
        let (proxy_url, mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.terminal_idle_timeout_secs = 1;
        let app = spawn_app(config).await;

        let (mut ws, _resp) = connect_async(ws_url(&app, ""))
            .await
            .expect("websocket upgrade should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        // No client traffic → the server must close the session and reap the
        // shell well before the (24 h) envd stream deadline.
        let error = recv_json(&mut ws).await;
        assert_eq!(error["type"], json!("error"));
        assert!(
            error["message"]
                .as_str()
                .expect("error message")
                .contains("inactivity"),
            "unexpected idle error message: {}",
            error["message"]
        );

        let signal =
            wait_for_recorded(first_of(&mock.spy, |s| s.send_signal.first().cloned())).await;
        assert_eq!(signal["signal"], json!("SIGNAL_SIGKILL"));
    }

    #[tokio::test]
    async fn output_activity_extends_idle_timeout() {
        let (proxy_url, mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.terminal_idle_timeout_secs = 1;
        let app = spawn_app(config).await;

        let (mut ws, _resp) = connect_async(ws_url(&app, ""))
            .await
            .expect("websocket upgrade should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        // Shell output every 300 ms keeps the session alive well past the
        // 1 s idle timeout: five output frames arrive over ~1.5 s without
        // any client input.
        for _ in 0..5 {
            mock.push_frame(json!({"event": {"data": {"pty": BASE64.encode("tick")}}}))
                .await;
            let output = recv_json(&mut ws).await;
            assert_eq!(output["type"], json!("output"));
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        // Once the output stops and the client stays silent, the idle
        // timeout fires and the shell is reaped.
        let error = recv_json(&mut ws).await;
        assert_eq!(error["type"], json!("error"));
        assert!(
            error["message"]
                .as_str()
                .expect("error message")
                .contains("inactivity"),
            "unexpected idle error message: {}",
            error["message"]
        );
        let signal =
            wait_for_recorded(first_of(&mock.spy, |s| s.send_signal.first().cloned())).await;
        assert_eq!(signal["signal"], json!("SIGNAL_SIGKILL"));
    }

    #[tokio::test]
    async fn token_subprotocol_without_base_is_rejected() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let app = spawn_app(test_config(&proxy_url, &master_url)).await;

        // Only the token-bearing subprotocol, no base `cube-terminal`: no
        // valid selection exists (the token entry is never echoed), so the
        // request is rejected with 400 before the upgrade.
        let (head, _stream) = raw_handshake(&app, "cube-terminal.some-token").await;
        assert!(
            head.starts_with("HTTP/1.1 400"),
            "expected 400 Bad Request, got: {}",
            head
        );
    }

    #[tokio::test]
    async fn rejects_when_callback_auth_fails_or_token_missing() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let (callback_url, _captured) = spawn_auth_callback(StatusCode::FORBIDDEN).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.auth_callback_url = Some(callback_url);
        let app = spawn_app(config).await;

        // Missing token → 401 before upgrade.
        expect_upgrade_error(
            connect_async(ws_url(&app, "")).await,
            StatusCode::UNAUTHORIZED,
        );
        // Token present but callback denies → 401 before upgrade.
        expect_upgrade_error(
            connect_async(ws_url(&app, "?token=bad-token")).await,
            StatusCode::UNAUTHORIZED,
        );
    }

    #[tokio::test]
    async fn callback_auth_forwards_path_method_and_bearer_token() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let (callback_url, captured) = spawn_auth_callback(StatusCode::OK).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.auth_callback_url = Some(callback_url);
        let app = spawn_app(config).await;

        let (mut ws, _resp) = connect_async(ws_url(&app, "?token=good-token"))
            .await
            .expect("websocket upgrade should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        let guard = captured.lock().await;
        let header = |name: &str| {
            guard
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(
            header("x-request-path").as_deref(),
            Some(format!("/cubeapi/v1/sandboxes/{}/terminal/ws", SANDBOX_ID).as_str())
        );
        assert_eq!(header("x-request-method").as_deref(), Some("GET"));
        assert_eq!(
            header("authorization").as_deref(),
            Some("Bearer good-token")
        );
    }

    #[tokio::test]
    async fn rejects_sandbox_that_is_not_running() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_PAUSED).await;
        let app = spawn_app(test_config(&proxy_url, &master_url)).await;

        expect_upgrade_error(connect_async(ws_url(&app, "")).await, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn rejects_unknown_sandbox() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master_not_found().await;
        let app = spawn_app(test_config(&proxy_url, &master_url)).await;

        expect_upgrade_error(connect_async(ws_url(&app, "")).await, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn subprotocol_token_authenticates_without_query_param() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let (callback_url, captured) = spawn_auth_callback(StatusCode::OK).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.auth_callback_url = Some(callback_url);
        let app = spawn_app(config).await;

        // Browser-style handshake: the token rides Sec-WebSocket-Protocol as
        // `cube-terminal.<token>` and there is no query param at all. The
        // helper also asserts the server does not echo a subprotocol.
        let mut ws = connect_with_subprotocol(&app, "cube-terminal.good-token").await;

        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        // The callback received the subprotocol token as Bearer.
        let guard = captured.lock().await;
        let authz = guard
            .iter()
            .find(|(k, _)| k == "authorization")
            .map(|(_, v)| v.clone());
        assert_eq!(authz.as_deref(), Some("Bearer good-token"));
    }

    #[test]
    fn origin_host_matching_rules() {
        use axum::http::HeaderValue;

        let check = |origin: Option<&str>, host: &str| {
            let mut headers = HeaderMap::new();
            headers.insert("host", HeaderValue::from_str(host).unwrap());
            if let Some(o) = origin {
                headers.insert("origin", HeaderValue::from_str(o).unwrap());
            }
            origin_matches_host(&headers)
        };

        // Exact match, case-insensitive host.
        assert!(check(Some("http://example.com:8443"), "example.com:8443"));
        assert!(check(Some("https://EXAMPLE.com"), "example.com"));
        // Port-less Origin (scheme default) matches on hostname alone.
        assert!(check(Some("http://example.com"), "example.com:3000"));
        // An explicit port equal to the scheme default is the same as
        // port-less, even against a port-less Host.
        assert!(check(Some("http://example.com:80"), "example.com"));
        assert!(check(Some("https://example.com:443"), "example.com"));
        // An explicit non-default Origin port vs a port-less Host must NOT
        // match: a proxy that strips the port (`proxy_set_header Host
        // $host`) must not widen the check to other same-host services.
        // Such proxies must forward the full authority (`Host $http_host`).
        assert!(!check(Some("http://example.com:12088"), "example.com"));
        assert!(!check(Some("https://example.com:80"), "example.com"));
        // Both ported: ports must agree.
        assert!(!check(Some("http://example.com:12088"), "example.com:3000"));
        assert!(check(Some("http://example.com:12088"), "example.com:12088"));
        // Different hostnames never match, even port-less.
        assert!(!check(Some("http://evil.com"), "example.com"));
        assert!(!check(Some("http://example.com.evil.com"), "example.com"));
        // No Origin header: not a browser, skip the check.
        assert!(check(None, "example.com"));
        // Malformed Origin: reject.
        assert!(!check(Some("not-a-url"), "example.com"));
        // Bracketed IPv6 without a port is not misparsed as host+port.
        assert!(check(Some("http://[::1]"), "[::1]"));
    }

    #[tokio::test]
    async fn origin_mismatch_is_forbidden_before_upgrade() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let app = spawn_app(test_config(&proxy_url, &master_url)).await;

        // The handshake Host is 127.0.0.1:<port>; a foreign Origin → 403.
        let request =
            tungstenite::ClientRequestBuilder::new(ws_url(&app, "").parse().expect("uri"))
                .with_header("Origin", "http://evil.example.com");
        expect_upgrade_error(connect_async(request).await, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn matching_origin_upgrades() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let app = spawn_app(test_config(&proxy_url, &master_url)).await;

        // Exact authority match (scheme + host + port).
        let request =
            tungstenite::ClientRequestBuilder::new(ws_url(&app, "").parse().expect("uri"))
                .with_header("Origin", app.clone());
        let (mut ws, _resp) = connect_async(request)
            .await
            .expect("same-origin upgrade should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));
        ws.close(None).await.expect("close websocket");

        // An Origin without an explicit port matches on hostname alone.
        let request =
            tungstenite::ClientRequestBuilder::new(ws_url(&app, "").parse().expect("uri"))
                .with_header("Origin", "http://127.0.0.1");
        let (mut ws, _resp) = connect_async(request)
            .await
            .expect("default-port origin upgrade should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));
    }

    #[tokio::test]
    async fn per_sandbox_session_cap_returns_429() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.terminal_max_sessions_per_sandbox = 1;
        let app = spawn_app(config).await;

        let (mut ws, _resp) = connect_async(ws_url(&app, ""))
            .await
            .expect("first session should upgrade");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        // A second concurrent session on the same sandbox exceeds the cap.
        expect_upgrade_error(
            connect_async(ws_url(&app, "")).await,
            StatusCode::TOO_MANY_REQUESTS,
        );

        // Closing the first session releases the slot. Teardown is async,
        // so poll until the tracker frees it.
        ws.close(None).await.expect("close websocket");
        let mut upgraded = None;
        for _ in 0..100 {
            match connect_async(ws_url(&app, "")).await {
                Ok((ws, _resp)) => {
                    upgraded = Some(ws);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
        let mut ws = upgraded.expect("slot should be released after close");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));
    }

    #[tokio::test]
    async fn oversized_client_message_terminates_session() {
        let (proxy_url, mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let app = spawn_app(test_config(&proxy_url, &master_url)).await;

        let (mut ws, _resp) = connect_async(ws_url(&app, ""))
            .await
            .expect("websocket upgrade should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        // Well over the 64 KiB server-side message cap.
        let big = json!({"type": "input", "data": "x".repeat(128 * 1024)}).to_string();
        ws.send(tungstenite::Message::Text(big))
            .await
            .expect("send oversized message");

        // The server answers with an error frame and ends the session.
        let error = recv_json(&mut ws).await;
        assert_eq!(error["type"], json!("error"));

        // The oversized payload must never reach envd.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(mock.spy.lock().await.send_input.is_empty());
    }

    #[tokio::test]
    async fn hung_envd_call_times_out_and_the_pump_survives() {
        let hanging = MockEnvd::default();
        hanging
            .hang_send_input
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let (proxy_url, mock) = spawn_mock_envd_with(hanging).await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let app = spawn_app(test_config(&proxy_url, &master_url)).await;

        let (mut ws, _resp) = connect_async(ws_url(&app, ""))
            .await
            .expect("websocket upgrade should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        // This input hangs inside envd forever. The envd call deadline
        // (ENVD_CALL_TIMEOUT) must bound the stall: afterwards the pump
        // must keep pumping instead of wedging the select loop (idle timer,
        // disconnect detection, output reads).
        let input_b64 = BASE64.encode("echo hi\n");
        ws.send(tungstenite::Message::Text(
            json!({"type": "input", "data": input_b64}).to_string(),
        ))
        .await
        .expect("send input");

        // Shell output produced while SendInput is stuck must still be
        // delivered once the deadline fires.
        mock.push_frame(json!({"event": {"data": {"pty": BASE64.encode("tick")}}}))
            .await;
        let msg = tokio::time::timeout(ENVD_CALL_TIMEOUT + Duration::from_secs(15), ws.next())
            .await
            .expect("pump must recover after the envd call deadline")
            .expect("ws stream ended unexpectedly")
            .expect("ws read error");
        let tungstenite::Message::Text(text) = msg else {
            panic!("expected text message, got {:?}", msg);
        };
        let output: Value = serde_json::from_str(&text).expect("message JSON");
        assert_eq!(output["type"], json!("output"));

        // And teardown still reaps the shell afterwards.
        ws.close(None).await.expect("close websocket");
        let signal =
            wait_for_recorded(first_of(&mock.spy, |s| s.send_signal.first().cloned())).await;
        assert_eq!(signal["signal"], json!("SIGNAL_SIGKILL"));
    }

    #[tokio::test]
    async fn handshake_is_rate_limited_when_auth_is_configured() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.cube_api_key = Some("secret-key".to_string());
        // One request per second per key: the first handshake drains the
        // bucket, the second must be rejected with 429 before reaching the
        // handler — the terminal route carries the same rate limit as the
        // other sandbox routes.
        config.rate_limit_per_sec = 1;
        let app = spawn_app(config).await;

        expect_upgrade_error(
            connect_async(ws_url(&app, "?token=wrong-key")).await,
            StatusCode::UNAUTHORIZED,
        );
        expect_upgrade_error(
            connect_async(ws_url(&app, "?token=wrong-key")).await,
            StatusCode::TOO_MANY_REQUESTS,
        );
    }

    #[tokio::test]
    async fn callback_auth_accepts_x_api_key_header() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let (callback_url, captured) = spawn_auth_callback(StatusCode::OK).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.auth_callback_url = Some(callback_url);
        let app = spawn_app(config).await;

        // Non-browser client style: an X-API-Key header and no token
        // anywhere else.
        let request =
            tungstenite::ClientRequestBuilder::new(ws_url(&app, "").parse().expect("uri"))
                .with_header("X-API-Key", "good-key");
        let (mut ws, _resp) = connect_async(request)
            .await
            .expect("x-api-key handshake should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        // The callback received the credential on X-API-Key (not Bearer),
        // exactly like unified_auth forwards it for the HTTP routes.
        let guard = captured.lock().await;
        let header = |name: &str| {
            guard
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(header("x-api-key").as_deref(), Some("good-key"));
        assert_eq!(header("authorization"), None);
    }

    #[tokio::test]
    async fn bearer_token_takes_priority_over_x_api_key() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let (callback_url, captured) = spawn_auth_callback(StatusCode::OK).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.auth_callback_url = Some(callback_url);
        let app = spawn_app(config).await;

        // Both credential transports present: Bearer wins, mirroring
        // unified_auth's extraction order.
        let request = tungstenite::ClientRequestBuilder::new(
            ws_url(&app, "?token=good-token").parse().expect("uri"),
        )
        .with_header("X-API-Key", "other-key");
        let (mut ws, _resp) = connect_async(request)
            .await
            .expect("handshake should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));

        let guard = captured.lock().await;
        let header = |name: &str| {
            guard
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(
            header("authorization").as_deref(),
            Some("Bearer good-token")
        );
        assert_eq!(header("x-api-key"), None);
    }

    #[tokio::test]
    async fn simple_key_auth_accepts_x_api_key_header() {
        let (proxy_url, _mock) = spawn_mock_envd().await;
        let master_url = spawn_mock_master(STATUS_RUNNING).await;
        let mut config = test_config(&proxy_url, &master_url);
        config.cube_api_key = Some("secret-key".to_string());
        let app = spawn_app(config).await;

        // Wrong key → 401 before upgrade.
        let request =
            tungstenite::ClientRequestBuilder::new(ws_url(&app, "").parse().expect("uri"))
                .with_header("X-API-Key", "wrong-key");
        expect_upgrade_error(connect_async(request).await, StatusCode::UNAUTHORIZED);

        // Matching key → upgrade succeeds with no token param at all.
        let request =
            tungstenite::ClientRequestBuilder::new(ws_url(&app, "").parse().expect("uri"))
                .with_header("X-API-Key", "secret-key");
        let (mut ws, _resp) = connect_async(request)
            .await
            .expect("x-api-key handshake should succeed");
        let ready = recv_json(&mut ws).await;
        assert_eq!(ready["type"], json!("ready"));
    }
}
