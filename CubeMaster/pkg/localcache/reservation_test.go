// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package localcache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/patrickmn/go-cache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/config"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
)

// reservationTestEnv installs a fresh node cache and scheduler config and
// restores both afterwards. Tests in this package run sequentially.
func reservationTestEnv(t *testing.T) {
	t.Helper()
	origCache := l.cache
	cfg := config.GetConfig()
	origScheduler := cfg.Scheduler
	origCubelet := cfg.CubeletConf
	origRedis := cfg.RedisConf
	t.Cleanup(func() {
		l.cache = origCache
		cfg.Scheduler = origScheduler
		cfg.CubeletConf = origCubelet
		cfg.RedisConf = origRedis
		reservationRegistry.Lock()
		reservationRegistry.nodes = make(map[string]*nodeReservationAmount)
		reservationRegistry.Unlock()
	})

	l.cache = cache.New(0, 0)
	cfg.Scheduler = &config.WrapperSchedulerConf{
		SchedulerConf: config.SchedulerConf{
			NodeMaxMvmNum:                  100,
			NodeMaxMvmNumReserveNumPercent: 1.0,
		},
	}
	cfg.CubeletConf = &config.CubeletConf{CreateTimeoutInsec: 600}
	cfg.RedisConf = nil
	reservationRegistry.Lock()
	reservationRegistry.nodes = make(map[string]*nodeReservationAmount)
	reservationRegistry.Unlock()
}

func reservationTestNode(id string) *node.Node {
	return &node.Node{
		InsID:               id,
		IP:                  "10.0.0.1",
		ReportedReady:       true,
		Healthy:             true,
		MetaDataUpdateAt:    time.Now(),
		QuotaCpu:            64000, // milli-cores
		QuotaMem:            65536, // MB
		MaxMvmLimit:         100,
		CreateConcurrentNum: 10,
	}
}

func registryAmount(t *testing.T, nodeID string) *nodeReservationAmount {
	t.Helper()
	reservationRegistry.Lock()
	defer reservationRegistry.Unlock()
	return reservationRegistry.nodes[nodeID]
}

func cachedReservedNum(t *testing.T, nodeID string) int64 {
	t.Helper()
	raw, ok := l.cache.Get(nodeID)
	if !ok {
		t.Fatalf("node %s missing from cache", nodeID)
	}
	return raw.(*node.Node).ReservedNum
}

func TestTryReserveNodeLocalOnly(t *testing.T) {
	reservationTestEnv(t)
	l.cache.SetDefault("node-r1", reservationTestNode("node-r1"))
	ctx := context.Background()

	rsv, err := TryReserveNode(ctx, "node-r1", 1000, 1024)
	if err != nil {
		t.Fatalf("TryReserveNode error: %v", err)
	}
	if got := cachedReservedNum(t, "node-r1"); got != 1 {
		t.Fatalf("ReservedNum=%d want 1", got)
	}
	amount := registryAmount(t, "node-r1")
	if amount == nil || amount.cpuMilli != 1000 || amount.memMB != 1024 || amount.mvm != 1 || amount.creating != 1 {
		t.Fatalf("registry amount=%+v", amount)
	}

	// CPU: raw quota is 64000, so asking for 200000 must conflict.
	if _, err := TryReserveNode(ctx, "node-r1", 200000, 1024); !errors.Is(err, ErrNodeReservationConflict) {
		t.Fatalf("cpu over-commit err=%v, want ErrNodeReservationConflict", err)
	}
	// Memory: raw quota is 65536.
	if _, err := TryReserveNode(ctx, "node-r1", 1000, 200000); !errors.Is(err, ErrNodeReservationConflict) {
		t.Fatalf("mem over-commit err=%v, want ErrNodeReservationConflict", err)
	}
	// Failed attempts must not charge the registry.
	if amount := registryAmount(t, "node-r1"); amount.cpuMilli != 1000 || amount.mvm != 1 {
		t.Fatalf("registry charged by rejected reservation: %+v", amount)
	}

	rsv.Release(ctx)
	rsv.Release(ctx) // idempotent
	if amount := registryAmount(t, "node-r1"); amount != nil {
		t.Fatalf("registry entry not cleaned: %+v", amount)
	}
	if got := cachedReservedNum(t, "node-r1"); got != 0 {
		t.Fatalf("ReservedNum=%d want 0 after release", got)
	}
	if _, err := TryReserveNode(ctx, "node-r1", 63500, 1024); err != nil {
		// 63500 only fits if the released 1000 milli-cores were actually
		// returned (free is 64000, would be 63000 on a leak).
		t.Fatalf("reservation after release should succeed: %v", err)
	}
}

func TestTryReserveNodeMvmAndCreatingLimits(t *testing.T) {
	reservationTestEnv(t)
	mvmNode := reservationTestNode("node-r2")
	mvmNode.MvmNum = 99 // limit is 100
	l.cache.SetDefault("node-r2", mvmNode)

	creatingNode := reservationTestNode("node-r3")
	creatingNode.RealTimeCreateNum = 9 // limit is 10
	l.cache.SetDefault("node-r3", creatingNode)

	ctx := context.Background()
	if _, err := TryReserveNode(ctx, "node-r2", 1000, 1024); err != nil {
		t.Fatalf("first mvm reservation should succeed: %v", err)
	}
	if _, err := TryReserveNode(ctx, "node-r2", 1000, 1024); !errors.Is(err, ErrNodeReservationConflict) {
		t.Fatalf("mvm limit err=%v, want ErrNodeReservationConflict", err)
	}
	if _, err := TryReserveNode(ctx, "node-r3", 1000, 1024); err != nil {
		t.Fatalf("first creating reservation should succeed: %v", err)
	}
	if _, err := TryReserveNode(ctx, "node-r3", 1000, 1024); !errors.Is(err, ErrNodeReservationConflict) {
		t.Fatalf("creating limit err=%v, want ErrNodeReservationConflict", err)
	}
	if _, err := TryReserveNode(ctx, "node-missing", 1000, 1024); !errors.Is(err, ErrNodeReservationConflict) {
		t.Fatalf("missing node err=%v, want ErrNodeReservationConflict", err)
	}
}

// TestReservedNumClampOnReplacedNode verifies the mirror never goes negative
// when the cached node object is replaced (re-registration, TTL expiry) while
// a reservation is held: the +1 landed on the old object, the -1 lands on the
// fresh one and must clamp at zero.
func TestReservedNumClampOnReplacedNode(t *testing.T) {
	reservationTestEnv(t)
	l.cache.SetDefault("node-r6", reservationTestNode("node-r6"))
	ctx := context.Background()

	rsv, err := TryReserveNode(ctx, "node-r6", 1000, 1024)
	if err != nil {
		t.Fatalf("TryReserveNode error: %v", err)
	}
	// The node re-registers while the reservation is held: the cache entry is
	// replaced by a fresh object whose ReservedNum starts at 0.
	l.cache.SetDefault("node-r6", reservationTestNode("node-r6"))

	rsv.Release(ctx)
	if got := cachedReservedNum(t, "node-r6"); got != 0 {
		t.Fatalf("ReservedNum=%d want clamped 0 after release on a replaced node", got)
	}
}

// Configuring Redis must not reintroduce any reservation command, even when
// the shared Redis instance is unavailable.
func TestReservationsDoNotAccessRedis(t *testing.T) {
	for _, offline := range []bool{false, true} {
		name := "online"
		if offline {
			name = "offline"
		}
		t.Run(name, func(t *testing.T) {
			reservationTestEnv(t)
			server := miniredis.RunT(t)
			addr := server.Addr()
			if offline {
				server.Close()
			}
			config.GetConfig().RedisConf = &config.RedisConf{
				Nodes: addr, MaxActive: 1, MaxIdle: 1, MaxRetry: 1,
			}
			l.cache.SetDefault("local-redis", reservationTestNode("local-redis"))
			ctx := context.Background()
			r, err := TryReserveNode(ctx, "local-redis", 1000, 1024)
			if err != nil {
				t.Fatal(err)
			}
			if got := cachedReservedNum(t, "local-redis"); got != 1 {
				t.Fatalf("local mirror = %d, want 1", got)
			}
			r.Release(ctx)
			r.Release(ctx)
			if registryAmount(t, "local-redis") != nil {
				t.Fatal("reservation was not released")
			}
			if !offline && server.CommandCount() != 0 {
				t.Fatalf("reservation issued %d Redis commands", server.CommandCount())
			}
		})
	}
}

func TestConcurrentLocalReservationsAndRelease(t *testing.T) {
	reservationTestEnv(t)
	n := reservationTestNode("local-concurrent")
	n.QuotaCpu = 10500 // exactly ten 1000m reservations fit the strict CPU guard
	n.CreateConcurrentNum = 100
	l.cache.SetDefault(n.ID(), n)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reservations := make(chan *NodeReservation, 100)
	errs := make(chan error, 100)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, err := TryReserveNode(ctx, n.ID(), 1000, 1024)
			if err != nil {
				errs <- err
				return
			}
			reservations <- r
		}()
	}
	close(start)
	wg.Wait()
	close(reservations)
	close(errs)
	if len(reservations) != 10 {
		t.Fatalf("admitted %d requests, want 10", len(reservations))
	}
	for err := range errs {
		if !errors.Is(err, ErrNodeReservationConflict) {
			t.Fatalf("unexpected admission error: %v", err)
		}
	}
	cancel()
	for r := range reservations {
		// Concurrent duplicate releases must only subtract once, and cleanup
		// must still work when the request has been canceled.
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(r *NodeReservation) {
				defer wg.Done()
				r.Release(ctx)
			}(r)
		}
	}
	wg.Wait()
	if registryAmount(t, n.ID()) != nil || cachedReservedNum(t, n.ID()) != 0 {
		t.Fatal("concurrent release did not clear local accounting")
	}
}
