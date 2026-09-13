// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

// scheduler-plugin is a minimal external Filter+Score plugin example. It keeps
// nodes with fewer than eight in-flight creates and scores lower CPU usage
// higher. Run with SOCKET=/run/cube-scheduler-example.sock.
//
// Both snapshot delivery modes are supported: with snapshot_mode=request the
// snapshot arrives embedded in every Filter/Score call; with
// snapshot_mode=sync the client pushes content-addressed snapshots via
// SyncSnapshot and queries by version. Snapshots are keyed by version (a
// small bounded map, never a single slot), so concurrent attempts never
// interfere; unknown versions answer FAILED_PRECONDITION and the client
// re-syncs.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	schedulerplugin "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/schedulerplugin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const protocolVersion = "v1"

// maxSnapshots bounds how many snapshot versions are kept for sync-mode
// clients. Evicted versions are re-pushed by the client on the next miss.
const maxSnapshots = 8

type server struct {
	schedulerplugin.UnimplementedSchedulerPluginServer
	mu        sync.RWMutex
	snapshots map[string]map[string]*schedulerplugin.SnapshotNode
	order     []string // FIFO of snapshot versions for eviction
}

func (s *server) Handshake(_ context.Context, request *schedulerplugin.HandshakeRequest) (*schedulerplugin.HandshakeResponse, error) {
	if request.GetProtocolVersion() != protocolVersion {
		return nil, fmt.Errorf("unsupported protocol version %q", request.GetProtocolVersion())
	}
	return &schedulerplugin.HandshakeResponse{
		ProtocolVersion: protocolVersion,
		PluginName:      request.GetPluginName(),
		Capabilities:    []string{"filter", "score", "snapshot_sync"},
	}, nil
}

func (s *server) SyncSnapshot(_ context.Context, request *schedulerplugin.SnapshotRequest) (*schedulerplugin.SnapshotResponse, error) {
	nodes := snapshotIndex(request.GetNodes())
	version := request.GetSnapshotVersion()
	s.mu.Lock()
	if _, exists := s.snapshots[version]; !exists {
		s.order = append(s.order, version)
		for len(s.order) > maxSnapshots {
			delete(s.snapshots, s.order[0])
			s.order = s.order[1:]
		}
	}
	s.snapshots[version] = nodes
	s.mu.Unlock()
	return &schedulerplugin.SnapshotResponse{SnapshotVersion: version}, nil
}

// resolveSnapshot prefers the snapshot embedded in the request (request mode)
// and otherwise looks up the version received via SyncSnapshot (sync mode).
func (s *server) resolveSnapshot(version string, embedded []*schedulerplugin.SnapshotNode) (map[string]*schedulerplugin.SnapshotNode, error) {
	if len(embedded) > 0 {
		return snapshotIndex(embedded), nil
	}
	s.mu.RLock()
	nodes, ok := s.snapshots[version]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "snapshot %q is not synchronized or has been evicted", version)
	}
	return nodes, nil
}

func snapshotIndex(snapshot []*schedulerplugin.SnapshotNode) map[string]*schedulerplugin.SnapshotNode {
	nodes := make(map[string]*schedulerplugin.SnapshotNode, len(snapshot))
	for _, candidate := range snapshot {
		nodes[candidate.GetId()] = candidate
	}
	return nodes
}

func (s *server) Filter(_ context.Context, request *schedulerplugin.FilterRequest) (*schedulerplugin.FilterResponse, error) {
	nodes, err := s.resolveSnapshot(request.GetSnapshotVersion(), request.GetSnapshot())
	if err != nil {
		return nil, err
	}
	response := &schedulerplugin.FilterResponse{SnapshotVersion: request.GetSnapshotVersion()}
	for _, id := range request.GetCandidateIds() {
		candidate := nodes[id]
		if candidate != nil && candidate.GetCreating() < 8 {
			response.KeptIds = append(response.KeptIds, id)
		}
	}
	return response, nil
}

func (s *server) Score(_ context.Context, request *schedulerplugin.ScoreRequest) (*schedulerplugin.ScoreResponse, error) {
	nodes, err := s.resolveSnapshot(request.GetSnapshotVersion(), request.GetSnapshot())
	if err != nil {
		return nil, err
	}
	response := &schedulerplugin.ScoreResponse{SnapshotVersion: request.GetSnapshotVersion()}
	for _, id := range request.GetCandidateIds() {
		candidate := nodes[id]
		if candidate == nil {
			return nil, fmt.Errorf("unknown candidate %q", id)
		}
		value := 100 - candidate.GetCpuUtil()
		if value < 0 {
			value = 0
		}
		if value > 100 {
			value = 100
		}
		response.Scores = append(response.Scores, &schedulerplugin.NodeScore{NodeId: id, Score: value})
	}
	return response, nil
}

func main() {
	socket := os.Getenv("SOCKET")
	if socket == "" {
		socket = "/run/cube-scheduler-example.sock"
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatalf("listen on %s: %v", socket, err)
	}
	grpcServer := grpc.NewServer()
	schedulerplugin.RegisterSchedulerPluginServer(grpcServer, &server{
		snapshots: make(map[string]map[string]*schedulerplugin.SnapshotNode),
	})
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		grpcServer.GracefulStop()
	}()
	log.Printf("scheduler plugin listening on %s", socket)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
