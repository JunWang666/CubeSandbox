// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package grpcplugin

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/config"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/scheduler/selctx"
	schedulerplugin "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/schedulerplugin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeSchedulerPlugin struct {
	schedulerplugin.UnimplementedSchedulerPluginServer
	mu                sync.Mutex
	snapshots         map[string]map[string]struct{} // version -> candidate ids
	syncCount         int
	lastFilterVersion string
	lastSnapshotSize  int
	noSyncCapability  bool
	invalidFilter     bool
	invalidScore      bool
	failFilter        bool
}

func (f *fakeSchedulerPlugin) Handshake(context.Context, *schedulerplugin.HandshakeRequest) (*schedulerplugin.HandshakeResponse, error) {
	capabilities := []string{"filter", "score"}
	if !f.noSyncCapability {
		capabilities = append(capabilities, CapabilitySnapshotSync)
	}
	return &schedulerplugin.HandshakeResponse{ProtocolVersion: ProtocolVersion, PluginName: "fake", Capabilities: capabilities}, nil
}

func (f *fakeSchedulerPlugin) SyncSnapshot(_ context.Context, request *schedulerplugin.SnapshotRequest) (*schedulerplugin.SnapshotResponse, error) {
	ids := make(map[string]struct{}, len(request.GetNodes()))
	for _, candidate := range request.GetNodes() {
		ids[candidate.GetId()] = struct{}{}
	}
	f.mu.Lock()
	f.snapshots[request.GetSnapshotVersion()] = ids
	f.syncCount++
	f.mu.Unlock()
	return &schedulerplugin.SnapshotResponse{SnapshotVersion: request.GetSnapshotVersion()}, nil
}

// dropSnapshots simulates a server restart: all synced versions are gone.
func (f *fakeSchedulerPlugin) dropSnapshots() {
	f.mu.Lock()
	f.snapshots = make(map[string]map[string]struct{})
	f.mu.Unlock()
}

func (f *fakeSchedulerPlugin) counts() (syncs int, lastVersion string, lastSize int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncCount, f.lastFilterVersion, f.lastSnapshotSize
}

// Filter behaves like a strict plugin: every candidate must be backed by the
// snapshot embedded in the request (request mode) or by the version synced
// earlier (sync mode, FAILED_PRECONDITION on unknown versions).
func (f *fakeSchedulerPlugin) Filter(_ context.Context, request *schedulerplugin.FilterRequest) (*schedulerplugin.FilterResponse, error) {
	f.mu.Lock()
	invalid := f.invalidFilter
	fail := f.failFilter
	f.lastFilterVersion = request.GetSnapshotVersion()
	f.lastSnapshotSize = len(request.GetSnapshot())
	f.mu.Unlock()
	if fail {
		return nil, status.Error(codes.Unavailable, "filter unavailable")
	}
	var snapshot map[string]struct{}
	if len(request.GetSnapshot()) > 0 {
		snapshot = make(map[string]struct{}, len(request.GetSnapshot()))
		for _, candidate := range request.GetSnapshot() {
			snapshot[candidate.GetId()] = struct{}{}
		}
	} else {
		f.mu.Lock()
		synced, ok := f.snapshots[request.GetSnapshotVersion()]
		f.mu.Unlock()
		if !ok {
			return nil, status.Errorf(codes.FailedPrecondition, "snapshot %q is not synchronized", request.GetSnapshotVersion())
		}
		snapshot = synced
	}
	for _, id := range request.GetCandidateIds() {
		if _, ok := snapshot[id]; !ok {
			return nil, status.Errorf(codes.FailedPrecondition, "candidate %q missing from snapshot", id)
		}
	}
	if invalid {
		return &schedulerplugin.FilterResponse{SnapshotVersion: request.GetSnapshotVersion(), KeptIds: []string{"not-a-candidate"}}, nil
	}
	return &schedulerplugin.FilterResponse{SnapshotVersion: request.GetSnapshotVersion(), KeptIds: []string{request.GetCandidateIds()[0]}}, nil
}

func (f *fakeSchedulerPlugin) Score(_ context.Context, request *schedulerplugin.ScoreRequest) (*schedulerplugin.ScoreResponse, error) {
	f.mu.Lock()
	invalid := f.invalidScore
	f.mu.Unlock()
	response := &schedulerplugin.ScoreResponse{SnapshotVersion: request.GetSnapshotVersion()}
	for index, id := range request.GetCandidateIds() {
		value := float64(90 - index*10)
		if invalid && index == 0 {
			value = 101
		}
		response.Scores = append(response.Scores, &schedulerplugin.NodeScore{NodeId: id, Score: value})
	}
	return response, nil
}

func startPluginServer(t *testing.T) (*grpc.ClientConn, *fakeSchedulerPlugin) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	fake := &fakeSchedulerPlugin{snapshots: make(map[string]map[string]struct{})}
	schedulerplugin.RegisterSchedulerPluginServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return connection, fake
}

func newFilterSelector(t *testing.T, connection *grpc.ClientConn, mode string) *filterPlugin {
	t.Helper()
	client, err := newClientFromConn(context.Background(), config.SchedulerProfilePluginConf{
		Name: "fake", Timeout: time.Second, SnapshotMode: mode,
	}, "filter", connection)
	if err != nil {
		t.Fatal(err)
	}
	selector := &filterPlugin{client: client}
	t.Cleanup(func() { _ = selector.Close() })
	return selector
}

func grpcSelection() *selctx.SelectorCtx {
	selection := selctx.New("random")
	selection.Ctx = context.Background()
	selection.InstanceType = "small"
	selection.SetNodes(node.NodeList{
		{InsID: "n1", Healthy: true, CpuUtil: 10},
		{InsID: "n2", Healthy: true, CpuUtil: 20},
	})
	selection.FreezeSnapshot()
	return selection
}

func TestExternalFilterCarriesSnapshotAndValidatesResult(t *testing.T) {
	connection, server := startPluginServer(t)
	selector := newFilterSelector(t, connection, "")
	selection := grpcSelection()
	kept, err := selector.Select(selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 1 || kept[0].ID() != "n1" {
		t.Fatalf("kept = %v", kept)
	}
	syncs, lastVersion, snapshotSize := server.counts()
	if lastVersion != selection.SnapshotVersion {
		t.Fatalf("request version = %q, want %q", lastVersion, selection.SnapshotVersion)
	}
	if snapshotSize != 2 {
		t.Fatalf("request snapshot size = %d, want 2", snapshotSize)
	}
	if syncs != 0 {
		t.Fatalf("request mode must not sync snapshots, got %d pushes", syncs)
	}
	server.mu.Lock()
	server.invalidFilter = true
	server.mu.Unlock()
	selection.FreezeSnapshot()
	if _, err := selector.Select(selection); err == nil {
		t.Fatal("non-candidate filter result must be rejected")
	}
}

func TestExternalFilterSyncModeDeduplicatesPushes(t *testing.T) {
	connection, server := startPluginServer(t)
	selector := newFilterSelector(t, connection, SnapshotModeSync)

	// Two attempts with identical snapshot content must share one
	// content-addressed version and a single SyncSnapshot push.
	for i := 0; i < 2; i++ {
		kept, err := selector.Select(grpcSelection())
		if err != nil {
			t.Fatal(err)
		}
		if len(kept) != 1 || kept[0].ID() != "n1" {
			t.Fatalf("kept = %v", kept)
		}
	}
	syncs, version, snapshotSize := server.counts()
	if syncs != 1 {
		t.Fatalf("sync pushes = %d, want 1 (identical snapshots must deduplicate)", syncs)
	}
	if snapshotSize != 0 {
		t.Fatalf("sync mode must not embed snapshots in queries, got %d nodes", snapshotSize)
	}
	wantVersion, err := snapshotContentVersion(snapshotNodes(grpcSelection()))
	if err != nil {
		t.Fatal(err)
	}
	if version != wantVersion {
		t.Fatalf("query version = %q, want content version %q", version, wantVersion)
	}
}

func TestExternalFilterSyncModeResyncsAfterServerLoss(t *testing.T) {
	connection, server := startPluginServer(t)
	selector := newFilterSelector(t, connection, SnapshotModeSync)

	if _, err := selector.Select(grpcSelection()); err != nil {
		t.Fatal(err)
	}
	server.dropSnapshots()
	if _, err := selector.Select(grpcSelection()); err != nil {
		t.Fatalf("select after server snapshot loss must re-sync and succeed: %v", err)
	}
	syncs, _, _ := server.counts()
	if syncs != 2 {
		t.Fatalf("sync pushes = %d, want 2 (initial push + re-push after loss)", syncs)
	}
}

func TestSyncModeRequiresAdvertisedCapability(t *testing.T) {
	connection, server := startPluginServer(t)
	server.noSyncCapability = true
	if _, err := newClientFromConn(context.Background(), config.SchedulerProfilePluginConf{
		Name: "fake", Timeout: time.Second, SnapshotMode: SnapshotModeSync,
	}, "filter", connection); err == nil {
		t.Fatal("sync mode without the snapshot_sync capability must fail the handshake")
	}
}

func TestUnknownSnapshotModeRejected(t *testing.T) {
	connection, _ := startPluginServer(t)
	if _, err := newClientFromConn(context.Background(), config.SchedulerProfilePluginConf{
		Name: "fake", Timeout: time.Second, SnapshotMode: "delta",
	}, "filter", connection); err == nil {
		t.Fatal("unknown snapshot_mode must be rejected")
	}
}

func TestExternalScoreRejectsOutOfRangeValues(t *testing.T) {
	connection, server := startPluginServer(t)
	client, err := newClientFromConn(context.Background(), config.SchedulerProfilePluginConf{
		Name: "fake", Timeout: time.Second, Weight: 2,
	}, "score", connection)
	if err != nil {
		t.Fatal(err)
	}
	selector := &scorePlugin{client: client, weight: 2}
	t.Cleanup(func() { _ = selector.Close() })
	scores, err := selector.Select(grpcSelection())
	if err != nil {
		t.Fatal(err)
	}
	if len(scores) != 2 || scores[0].Score != 90 || scores[1].Score != 80 {
		t.Fatalf("scores = %+v", scores)
	}
	server.mu.Lock()
	server.invalidScore = true
	server.mu.Unlock()
	if _, err := selector.Select(grpcSelection()); err == nil {
		t.Fatal("out-of-range external score must be rejected")
	}
}

func TestExternalFilterConcurrentSelectsStayIsolated(t *testing.T) {
	for _, mode := range []string{SnapshotModeRequest, SnapshotModeSync} {
		t.Run(mode, func(t *testing.T) {
			connection, _ := startPluginServer(t)
			selector := newFilterSelector(t, connection, mode)

			// Each request-mode select uses a fresh snapshot version; sync-mode
			// selects share one content version. Either way the strict server
			// rejects candidates not backed by the expected snapshot, so
			// concurrent attempts must not interfere with each other.
			var wg sync.WaitGroup
			errs := make(chan error, 32)
			for i := 0; i < 32; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, err := selector.Select(grpcSelection()); err != nil {
						errs <- err
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatalf("concurrent select failed: %v", err)
			}
		})
	}
}

func TestExternalFilterCircuitBreakerOpensAfterFailures(t *testing.T) {
	connection, server := startPluginServer(t)
	client, err := newClientFromConn(context.Background(), config.SchedulerProfilePluginConf{
		Name: "fake", Timeout: time.Second, CircuitBreakerFailures: 2, CircuitBreakerCooldown: time.Minute,
	}, "filter", connection)
	if err != nil {
		t.Fatal(err)
	}
	selector := &filterPlugin{client: client}
	t.Cleanup(func() { _ = selector.Close() })
	server.mu.Lock()
	server.failFilter = true
	server.mu.Unlock()

	for attempt := 0; attempt < 2; attempt++ {
		if _, err := selector.Select(grpcSelection()); err == nil || errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("attempt %d error = %v, want RPC failure", attempt+1, err)
		}
	}
	if _, err := selector.Select(grpcSelection()); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("third attempt error = %v, want circuit open", err)
	}
}
