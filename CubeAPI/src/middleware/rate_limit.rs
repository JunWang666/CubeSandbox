// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

use crate::error::AppError;
use crate::state::AppState;
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};

/// Subprotocol prefix browsers use to carry the terminal auth token
/// (`Sec-WebSocket-Protocol: cube-terminal.<token>`). Kept in sync with
/// `TOKEN_SUBPROTOCOL_PREFIX` in `handlers::terminal`.
const TOKEN_SUBPROTOCOL_PREFIX: &str = "cube-terminal.";

/// Per-API-key token bucket rate limiter middleware.
/// Derives the bucket key from the request credential and checks the shared
/// governor limiter. Returns 429 if the key has exceeded its quota.
pub async fn rate_limit(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    let key = rate_limit_key(&request);

    match state.rate_limiter.check_key(&key) {
        Ok(_) => Ok(next.run(request).await),
        Err(_) => Err(AppError::TooManyRequests(
            "Rate limit exceeded. Slow down.".to_string(),
        )),
    }
}

/// Derive the rate-limit bucket key for this request, mirroring the
/// credential precedence of `unified_auth` / `handshake_credential`: the
/// terminal WebSocket token (subprotocol first, then the `token` query param)
/// maps to Bearer and wins over `X-API-Key`; with no credential at all the
/// key is "anonymous".
///
/// Browser terminal handshakes cannot set headers, so their token arrives as
/// a `cube-terminal.<token>` WebSocket subprotocol — keying only on
/// `X-API-Key` would bucket every browser user together as "anonymous",
/// letting anyone exhaust the shared quota with bare handshakes. The token is
/// used only as the in-memory limiter key; it is never logged.
fn rate_limit_key(request: &Request) -> String {
    let headers = request.headers();

    // Browser terminal handshake: `cube-terminal.<token>` subprotocol.
    if let Some(token) = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.split(',')
                .map(str::trim)
                .find_map(|p| p.strip_prefix(TOKEN_SUBPROTOCOL_PREFIX))
                .filter(|t| !t.is_empty())
        })
    {
        return token.to_string();
    }

    // Non-browser terminal clients: `?token=` query param. The raw
    // (still percent-encoded) value is good enough as a bucket key — the
    // same client encodes it the same way on every request.
    if let Some(token) = query_param(request.uri().query().unwrap_or(""), "token") {
        return token.to_string();
    }

    if let Some(key) = headers
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .filter(|k| !k.is_empty())
    {
        return key.to_string();
    }

    "anonymous".to_string()
}

/// Extract one raw query parameter value (`name=value`), without
/// percent-decoding. Empty values count as absent.
fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        if k == name && !v.is_empty() {
            Some(v)
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    fn request(uri: &str, headers: &[(&str, &str)]) -> Request {
        let mut builder = Request::builder().method("GET").uri(uri);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(Body::empty()).expect("request should build")
    }

    #[test]
    fn key_prefers_subprotocol_token() {
        let req = request(
            "/cubeapi/v1/sandboxes/sb-1/terminal/ws",
            &[
                ("sec-websocket-protocol", "cube-terminal, cube-terminal.tok-1"),
                ("x-api-key", "other-key"),
            ],
        );
        assert_eq!(rate_limit_key(&req), "tok-1");
    }

    #[test]
    fn key_uses_query_token_without_subprotocol() {
        let req = request(
            "/cubeapi/v1/sandboxes/sb-1/terminal/ws?token=tok-2&cols=80",
            &[],
        );
        assert_eq!(rate_limit_key(&req), "tok-2");
    }

    #[test]
    fn empty_subprotocol_token_falls_back_to_query_token() {
        let req = request(
            "/cubeapi/v1/sandboxes/sb-1/terminal/ws?token=tok-3",
            &[("sec-websocket-protocol", "cube-terminal, cube-terminal.")],
        );
        assert_eq!(rate_limit_key(&req), "tok-3");
    }

    #[test]
    fn key_falls_back_to_x_api_key() {
        let req = request("/sandboxes/sb-1", &[("x-api-key", "api-key-1")]);
        assert_eq!(rate_limit_key(&req), "api-key-1");
    }

    #[test]
    fn key_defaults_to_anonymous() {
        let req = request("/sandboxes/sb-1", &[]);
        assert_eq!(rate_limit_key(&req), "anonymous");
    }
}
