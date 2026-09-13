// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

// Package grpcplugin implements external scheduler plugins over gRPC. It uses
// Unix Domain Sockets by default, performs a version/capability handshake,
// validates all plugin output, and bounds failures with timeouts and a small
// circuit breaker. Snapshot delivery has two modes (snapshot_mode): "request"
// embeds the frozen candidate snapshot in every Filter/Score call so plugins
// stay stateless; "sync" pushes snapshots under a content-addressed version
// via SyncSnapshot and queries by version, deduplicating unchanged snapshots.
// Neither mode holds a lock across an RPC, so concurrent scheduling attempts
// never serialize.
package grpcplugin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/config"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/scheduler/selctx"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/selector/filter"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/selector/score"
	schedulerplugin "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/schedulerplugin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const ProtocolVersion = "v1"

// Snapshot delivery modes for SchedulerProfilePluginConf.SnapshotMode.
const (
	// SnapshotModeRequest embeds the frozen candidate snapshot in every
	// Filter/Score request (default). Plugins stay stateless.
	SnapshotModeRequest = "request"
	// SnapshotModeSync pushes the snapshot once per distinct content via
	// SyncSnapshot, keyed by a content hash; queries carry only the version.
	SnapshotModeSync = "sync"
)

// CapabilitySnapshotSync is the handshake capability a plugin must advertise
// for clients configured with snapshot_mode: sync.
const CapabilitySnapshotSync = "snapshot_sync"

var (
	ErrCircuitOpen     = errors.New("external scheduler plugin circuit is open")
	ErrVersionMismatch = errors.New("external scheduler plugin snapshot version mismatch")
)

type breaker struct {
	mu        sync.Mutex
	failures  int
	threshold int
	cooldown  time.Duration
	openUntil time.Time
}

func (b *breaker) before() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.openUntil.After(time.Now()) {
		return ErrCircuitOpen
	}
	if !b.openUntil.IsZero() {
		b.openUntil = time.Time{}
		b.failures = 0
	}
	return nil
}

func (b *breaker) succeeded() {
	b.mu.Lock()
	b.failures = 0
	b.openUntil = time.Time{}
	b.mu.Unlock()
}

func (b *breaker) failed() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.failures >= b.threshold {
		b.openUntil = time.Now().Add(b.cooldown)
	}
}

type client struct {
	name       string
	timeout    time.Duration
	capability string
	syncMode   bool
	connection *grpc.ClientConn
	rpc        schedulerplugin.SchedulerPluginClient
	breaker    breaker

	// pushed tracks snapshot versions already synced in sync mode. It is only
	// an optimization: the server may evict versions independently, and a
	// query miss (FAILED_PRECONDITION) triggers a re-push.
	pushedMu    sync.Mutex
	pushed      map[string]struct{}
	pushedOrder []string

	closeOnce sync.Once
}

func newClient(ctx context.Context, conf config.SchedulerProfilePluginConf, capability string) (*client, error) {
	name := strings.TrimSpace(conf.Name)
	if name == "" {
		return nil, errors.New("external scheduler plugin name is empty")
	}
	target := strings.TrimSpace(conf.SocketPath)
	if target == "" {
		return nil, fmt.Errorf("external scheduler plugin %q socket_path is empty", name)
	}
	dialTarget := target
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if strings.HasPrefix(target, "/") || strings.HasPrefix(target, "unix://") {
		path := strings.TrimPrefix(target, "unix://")
		dialTarget = "passthrough:///" + path
		options = append(options, grpc.WithContextDialer(func(dialCtx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(dialCtx, "unix", path)
		}))
	}
	connection, err := grpc.NewClient(dialTarget, options...)
	if err != nil {
		return nil, fmt.Errorf("dial external scheduler plugin %q: %w", name, err)
	}
	return newClientFromConn(ctx, conf, capability, connection)
}

func newClientFromConn(ctx context.Context, conf config.SchedulerProfilePluginConf, capability string, connection *grpc.ClientConn) (*client, error) {
	name := strings.TrimSpace(conf.Name)
	if name == "" {
		_ = connection.Close()
		return nil, errors.New("external scheduler plugin name is empty")
	}
	timeout := conf.Timeout
	if timeout <= 0 {
		timeout = 100 * time.Millisecond
	}
	threshold := conf.CircuitBreakerFailures
	if threshold <= 0 {
		threshold = 3
	}
	cooldown := conf.CircuitBreakerCooldown
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	syncMode, err := parseSnapshotMode(conf.SnapshotMode)
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("external scheduler plugin %q: %w", name, err)
	}
	c := &client{
		name: name, timeout: timeout, capability: capability, syncMode: syncMode,
		connection: connection, rpc: schedulerplugin.NewSchedulerPluginClient(connection),
		breaker: breaker{threshold: threshold, cooldown: cooldown},
		pushed:  make(map[string]struct{}),
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	handshakeRequest := &schedulerplugin.HandshakeRequest{
		ProtocolVersion: ProtocolVersion,
		PluginName:      name,
	}
	handshakeStart := time.Now()
	response, err := c.rpc.Handshake(handshakeCtx, handshakeRequest)
	c.observeRPC(rpcMethodHandshake, handshakeStart, handshakeRequest, response, err)
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("external scheduler plugin %q handshake: %w", name, err)
	}
	if response.GetProtocolVersion() != ProtocolVersion {
		_ = connection.Close()
		return nil, fmt.Errorf("external scheduler plugin %q protocol version %q, want %q", name, response.GetProtocolVersion(), ProtocolVersion)
	}
	if response.GetPluginName() != "" && response.GetPluginName() != name {
		_ = connection.Close()
		return nil, fmt.Errorf("external scheduler plugin name %q, want %q", response.GetPluginName(), name)
	}
	if !slices.Contains(response.GetCapabilities(), capability) {
		_ = connection.Close()
		return nil, fmt.Errorf("external scheduler plugin %q does not advertise %q capability", name, capability)
	}
	if c.syncMode && !slices.Contains(response.GetCapabilities(), CapabilitySnapshotSync) {
		_ = connection.Close()
		return nil, fmt.Errorf("external scheduler plugin %q does not advertise %q capability required by snapshot_mode %q",
			name, CapabilitySnapshotSync, SnapshotModeSync)
	}
	return c, nil
}

func parseSnapshotMode(mode string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", SnapshotModeRequest:
		return false, nil
	case SnapshotModeSync:
		return true, nil
	default:
		return false, fmt.Errorf("unknown snapshot_mode %q (want %q or %q)", mode, SnapshotModeRequest, SnapshotModeSync)
	}
}

func (c *client) Close() error {
	var err error
	c.closeOnce.Do(func() { err = c.connection.Close() })
	return err
}

func (c *client) call(ctx context.Context, invoke func(context.Context) error) error {
	if err := c.breaker.before(); err != nil {
		return fmt.Errorf("plugin %q: %w", c.name, err)
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if err := invoke(callCtx); err != nil {
		c.breaker.failed()
		return err
	}
	return nil
}

// callQuery is call for Filter/Score RPCs. In sync mode a FAILED_PRECONDITION
// (unknown snapshot version) is an expected miss after a server restart or
// eviction, so it does not count against the circuit breaker; the caller
// re-syncs and retries once.
func (c *client) callQuery(ctx context.Context, invoke func(context.Context) error) error {
	if err := c.breaker.before(); err != nil {
		return fmt.Errorf("plugin %q: %w", c.name, err)
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if err := invoke(callCtx); err != nil {
		if c.syncMode && isUnknownSnapshot(err) {
			return err
		}
		c.breaker.failed()
		return err
	}
	return nil
}

// isUnknownSnapshot reports the protocol-level "the server does not hold this
// snapshot version" signal: FAILED_PRECONDITION in sync mode.
func isUnknownSnapshot(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}

// reject marks a syntactically successful RPC with an invalid response as a
// plugin failure. Only a fully validated Filter or Score response resets the
// breaker.
func (c *client) reject(err error) error {
	c.breaker.failed()
	return err
}

// snapshotNodes serializes the frozen candidate set. In request mode it
// travels inside every Filter/Score request; in sync mode it is pushed once
// per distinct content via SyncSnapshot.
func snapshotNodes(selection *selctx.SelectorCtx) []*schedulerplugin.SnapshotNode {
	snapshot := selection.SnapshotNodes()
	nodes := make([]*schedulerplugin.SnapshotNode, 0, len(snapshot))
	for _, candidate := range snapshot {
		nodes = append(nodes, snapshotNode(selection, candidate))
	}
	return nodes
}

// snapshotContentVersion derives a content-addressed snapshot version: the
// same frozen candidate set always hashes to the same version regardless of
// node order, so unchanged snapshots deduplicate to a single SyncSnapshot
// push and concurrent attempts never disturb each other's server-side state.
func snapshotContentVersion(nodes []*schedulerplugin.SnapshotNode) (string, error) {
	digests := make([][]byte, 0, len(nodes))
	for _, n := range nodes {
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(n)
		if err != nil {
			return "", err
		}
		digest := sha256.Sum256(raw)
		digests = append(digests, digest[:])
	}
	slices.SortFunc(digests, bytes.Compare)
	sum := sha256.New()
	for _, digest := range digests {
		sum.Write(digest)
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
}

// maxPushedVersions bounds the client-side set of pushed snapshot versions.
// Eviction only costs an occasional redundant SyncSnapshot push.
const maxPushedVersions = 64

func (c *client) isPushed(version string) bool {
	c.pushedMu.Lock()
	defer c.pushedMu.Unlock()
	_, ok := c.pushed[version]
	return ok
}

func (c *client) markPushed(version string) {
	c.pushedMu.Lock()
	defer c.pushedMu.Unlock()
	if _, ok := c.pushed[version]; ok {
		return
	}
	c.pushed[version] = struct{}{}
	c.pushedOrder = append(c.pushedOrder, version)
	if len(c.pushedOrder) > maxPushedVersions {
		delete(c.pushed, c.pushedOrder[0])
		c.pushedOrder = c.pushedOrder[1:]
	}
}

// pushSnapshot syncs the snapshot under a content-addressed version. Pushes
// are idempotent (same version means same content), so concurrent attempts
// may race them safely and no mutex is ever held across an RPC.
func (c *client) pushSnapshot(selection *selctx.SelectorCtx, version string, nodes []*schedulerplugin.SnapshotNode) (err error) {
	request := &schedulerplugin.SnapshotRequest{SnapshotVersion: version, Nodes: nodes}
	var response *schedulerplugin.SnapshotResponse
	rpcStart := time.Now()
	defer func() { c.observeRPC(rpcMethodSyncSnapshot, rpcStart, request, response, err) }()
	if err = c.call(selection.Ctx, func(ctx context.Context) error {
		var rpcErr error
		response, rpcErr = c.rpc.SyncSnapshot(ctx, request)
		return rpcErr
	}); err != nil {
		return fmt.Errorf("external scheduler plugin %q sync snapshot: %w", c.name, err)
	}
	if response.GetSnapshotVersion() != version {
		return c.reject(fmt.Errorf("%w: plugin %q returned %q, want %q", ErrVersionMismatch, c.name, response.GetSnapshotVersion(), version))
	}
	c.markPushed(version)
	return nil
}

func (c *client) ensureSnapshotSynced(selection *selctx.SelectorCtx, version string, nodes []*schedulerplugin.SnapshotNode) error {
	if c.isPushed(version) {
		return nil
	}
	return c.pushSnapshot(selection, version, nodes)
}

// resolveSnapshotVersion returns the snapshot version the query will carry.
// Request mode uses the per-attempt version and the caller embeds the
// snapshot in the request; sync mode uses a content hash and makes sure the
// server holds it.
func (c *client) resolveSnapshotVersion(selection *selctx.SelectorCtx, nodes []*schedulerplugin.SnapshotNode) (string, error) {
	if !c.syncMode {
		if selection.SnapshotVersion == "" {
			return "", errors.New("scheduler snapshot version is empty")
		}
		return selection.SnapshotVersion, nil
	}
	version, err := snapshotContentVersion(nodes)
	if err != nil {
		return "", fmt.Errorf("hash scheduler snapshot: %w", err)
	}
	if err := c.ensureSnapshotSynced(selection, version, nodes); err != nil {
		return "", err
	}
	return version, nil
}

func snapshotNode(selection *selctx.SelectorCtx, candidate *node.Node) *schedulerplugin.SnapshotNode {
	result := &schedulerplugin.SnapshotNode{
		Id: candidate.ID(), Ip: candidate.HostIP(), Healthy: candidate.Healthy,
		CpuTotal: int64(candidate.CpuTotal), CpuUtil: candidate.CpuUtil, CpuLoad: candidate.CpuLoadUsage,
		MemTotalMb: candidate.MemMBTotal, MemUsageMb: candidate.MemUsage,
		QuotaCpu: candidate.QuotaCpu, AllocatedCpu: candidate.QuotaCpuUsage,
		QuotaMemMb: candidate.QuotaMem, AllocatedMemMb: candidate.QuotaMemUsage,
		Creating: candidate.RealTimeCreateNum, LocalCreating: candidate.LocalCreateNum,
		Reserved: candidate.ReservedNum,
		MvmNum:   candidate.MvmNum, SystemDiskSize: candidate.SystemDiskSize,
		DataDiskUsage: candidate.DataDiskUsagePer, StorageDiskUsage: candidate.StorageDiskUsagePer,
		SystemDiskUsage: candidate.SysDiskUsagePer, Labels: candidate.Labels(),
		LocalTemplates: append([]string(nil), candidate.LocalTemplates...),
	}
	if facts, ok := selection.SnapshotFacts(candidate.ID()); ok {
		result.TemplateLocal = facts.TemplateLocal
		result.SnapshotStorageWritable = facts.SnapshotStorageAllowed
	}
	return result
}

func requestContext(selection *selctx.SelectorCtx) *schedulerplugin.RequestContext {
	request := &schedulerplugin.RequestContext{
		InstanceType: selection.InstanceType,
		Labels:       cloneMap(selection.RequestLabels),
	}
	if resources := selection.GetReqRes(); resources != nil {
		request.CpuMillis = resources.Cpu.MilliValue()
		request.MemoryBytes = resources.Mem.Value()
		request.SystemDiskSize = resources.SystemDiskSize
		request.TemplateId = resources.TemplateID
	}
	return request
}

func cloneMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func candidateIndex(candidates node.NodeList) (map[string]*node.Node, []string, error) {
	byID := make(map[string]*node.Node, len(candidates))
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil || candidate.ID() == "" {
			return nil, nil, errors.New("scheduler candidate has an empty id")
		}
		if _, exists := byID[candidate.ID()]; exists {
			return nil, nil, fmt.Errorf("duplicate scheduler candidate id %q", candidate.ID())
		}
		byID[candidate.ID()] = candidate
		ids = append(ids, candidate.ID())
	}
	return byID, ids, nil
}

type filterPlugin struct{ client *client }

func NewFilter(ctx context.Context, conf config.SchedulerProfilePluginConf) (filter.Selector, error) {
	client, err := newClient(ctx, conf, "filter")
	if err != nil {
		return nil, err
	}
	return &filterPlugin{client: client}, nil
}

func (p *filterPlugin) ID() string   { return "filter/grpc/" + p.client.name }
func (p *filterPlugin) Close() error { return p.client.Close() }

func (p *filterPlugin) Select(selection *selctx.SelectorCtx) (result node.NodeList, err error) {
	candidates := selection.Nodes()
	byID, ids, err := candidateIndex(candidates)
	if err != nil {
		return nil, err
	}
	nodes := snapshotNodes(selection)
	version, err := p.client.resolveSnapshotVersion(selection, nodes)
	if err != nil {
		return nil, err
	}
	var response *schedulerplugin.FilterResponse
	for attempt := 0; ; attempt++ {
		request := &schedulerplugin.FilterRequest{
			SnapshotVersion: version,
			Request:         requestContext(selection),
			CandidateIds:    ids,
		}
		if !p.client.syncMode {
			request.Snapshot = nodes
		}
		rpcStart := time.Now()
		rpcErr := p.client.callQuery(selection.Ctx, func(ctx context.Context) error {
			var err error
			response, err = p.client.rpc.Filter(ctx, request)
			return err
		})
		p.client.observeRPC(rpcMethodFilter, rpcStart, request, response, rpcErr)
		if rpcErr == nil {
			break
		}
		if !p.client.syncMode || !isUnknownSnapshot(rpcErr) || attempt >= 1 {
			return nil, fmt.Errorf("external scheduler filter %q: %w", p.client.name, rpcErr)
		}
		// The server lost this snapshot version (restart or eviction). Re-push
		// and retry once; the miss is not a plugin failure.
		if syncErr := p.client.pushSnapshot(selection, version, nodes); syncErr != nil {
			return nil, syncErr
		}
	}
	if response.GetSnapshotVersion() != version {
		return nil, p.client.reject(fmt.Errorf("%w: plugin %q returned %q, want %q", ErrVersionMismatch, p.client.name, response.GetSnapshotVersion(), version))
	}
	kept := make(map[string]struct{}, len(response.GetKeptIds()))
	for _, id := range response.GetKeptIds() {
		if _, exists := byID[id]; !exists {
			return nil, p.client.reject(fmt.Errorf("external scheduler filter %q returned non-candidate node %q", p.client.name, id))
		}
		if _, duplicate := kept[id]; duplicate {
			return nil, p.client.reject(fmt.Errorf("external scheduler filter %q returned duplicate node %q", p.client.name, id))
		}
		kept[id] = struct{}{}
	}
	result = make(node.NodeList, 0, len(kept))
	for _, candidate := range candidates {
		if _, ok := kept[candidate.ID()]; ok {
			result = append(result, candidate)
		}
	}
	p.client.breaker.succeeded()
	return result, nil
}

type scorePlugin struct {
	client *client
	weight float64
}

func NewScore(ctx context.Context, conf config.SchedulerProfilePluginConf) (score.Selector, error) {
	client, err := newClient(ctx, conf, "score")
	if err != nil {
		return nil, err
	}
	weight := conf.Weight
	if weight == 0 {
		weight = 1
	}
	return &scorePlugin{client: client, weight: weight}, nil
}

func (p *scorePlugin) ID() string      { return "score/grpc/" + p.client.name }
func (p *scorePlugin) Weight() float64 { return p.weight }
func (p *scorePlugin) Disable() bool   { return false }
func (p *scorePlugin) Close() error    { return p.client.Close() }

func (p *scorePlugin) Select(selection *selctx.SelectorCtx) (result node.NodeScoreList, err error) {
	candidates := selection.Nodes()
	byID, ids, err := candidateIndex(candidates)
	if err != nil {
		return nil, err
	}
	nodes := snapshotNodes(selection)
	version, err := p.client.resolveSnapshotVersion(selection, nodes)
	if err != nil {
		return nil, err
	}
	var response *schedulerplugin.ScoreResponse
	for attempt := 0; ; attempt++ {
		request := &schedulerplugin.ScoreRequest{
			SnapshotVersion: version,
			Request:         requestContext(selection),
			CandidateIds:    ids,
		}
		if !p.client.syncMode {
			request.Snapshot = nodes
		}
		rpcStart := time.Now()
		rpcErr := p.client.callQuery(selection.Ctx, func(ctx context.Context) error {
			var err error
			response, err = p.client.rpc.Score(ctx, request)
			return err
		})
		p.client.observeRPC(rpcMethodScore, rpcStart, request, response, rpcErr)
		if rpcErr == nil {
			break
		}
		if !p.client.syncMode || !isUnknownSnapshot(rpcErr) || attempt >= 1 {
			return nil, fmt.Errorf("external scheduler score %q: %w", p.client.name, rpcErr)
		}
		// The server lost this snapshot version (restart or eviction). Re-push
		// and retry once; the miss is not a plugin failure.
		if syncErr := p.client.pushSnapshot(selection, version, nodes); syncErr != nil {
			return nil, syncErr
		}
	}
	if response.GetSnapshotVersion() != version {
		return nil, p.client.reject(fmt.Errorf("%w: plugin %q returned %q, want %q", ErrVersionMismatch, p.client.name, response.GetSnapshotVersion(), version))
	}
	values := make(map[string]float64, len(response.GetScores()))
	for _, item := range response.GetScores() {
		id, value := item.GetNodeId(), item.GetScore()
		if _, exists := byID[id]; !exists {
			return nil, p.client.reject(fmt.Errorf("external scheduler score %q returned non-candidate node %q", p.client.name, id))
		}
		if _, duplicate := values[id]; duplicate {
			return nil, p.client.reject(fmt.Errorf("external scheduler score %q returned duplicate node %q", p.client.name, id))
		}
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
			return nil, p.client.reject(fmt.Errorf("external scheduler score %q returned %v for node %q outside [0,100]", p.client.name, value, id))
		}
		values[id] = value
	}
	if len(values) != len(candidates) {
		return nil, p.client.reject(fmt.Errorf("external scheduler score %q returned %d scores for %d candidates", p.client.name, len(values), len(candidates)))
	}
	result = make(node.NodeScoreList, 0, len(candidates))
	for _, candidate := range candidates {
		result = append(result, &node.NodeScore{
			InsID: candidate.ID(), Score: values[candidate.ID()], MvmNum: candidate.MvmNum, OrigNode: candidate,
		})
	}
	p.client.breaker.succeeded()
	return result, nil
}
