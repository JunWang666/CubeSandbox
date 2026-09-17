// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

// Reservation management for the scheduler.
//
// After selection, CubeMaster re-checks capacity under a process-local lock
// and reserves CPU, memory, one MVM slot and one create-concurrency slot.
// A conflict triggers bounded re-selection. Release runs when Cubelet returns;
// the next metric report may arrive later, leaving a visibility window.
//
// Reservations are local to one CubeMaster. They do not access Redis or
// atomically coordinate capacity across replicas. Existing reported metrics
// and create-concurrency estimates remain the cross-replica pressure signals.
package localcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/config"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
)

// ErrNodeReservationConflict means the selected node cannot take the request
// right now once in-flight reservations are accounted for. It is a normal,
// retryable scheduling outcome: the caller should mark the node bad for this
// attempt and re-schedule.
var ErrNodeReservationConflict = errors.New("node reservation conflict")

// NodeReservation is one held reservation. Release is idempotent and safe to
// call from any goroutine.
type NodeReservation struct {
	NodeID   string
	CpuMilli int64
	MemMB    int64

	released atomic.Bool
}

// nodeReservationAmount is the per-node aggregate of what one CubeMaster
// replica has reserved but not yet released.
type nodeReservationAmount struct {
	cpuMilli int64
	memMB    int64
	mvm      int64
	creating int64
}

func (a *nodeReservationAmount) empty() bool {
	return a.cpuMilli == 0 && a.memMB == 0 && a.mvm == 0 && a.creating == 0
}

// reservationRegistry is the authoritative local accounting. node.ReservedNum
// is only a best-effort visibility mirror for snapshots (CEL / gRPC plugins).
var reservationRegistry = struct {
	sync.Mutex
	nodes map[string]*nodeReservationAmount
}{nodes: make(map[string]*nodeReservationAmount)}

// TryReserveNode re-reads the node from the cache and, under the registry
// lock, re-checks the CPU / memory quota, MVM limit, and create-concurrency
// predicates with this replica's outstanding reservations already charged.
// On success the local record remains charged until Release is called.
// Redis configuration and availability do not affect this operation.
func TryReserveNode(ctx context.Context, nodeID string, cpuMilli, memMB int64) (*NodeReservation, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("%w: empty node id", ErrNodeReservationConflict)
	}
	sconf := config.GetConfig().Scheduler
	if sconf == nil {
		return nil, errors.New("TryReserveNode: scheduler config is nil")
	}

	reservationRegistry.Lock()
	current, ok := GetNode(nodeID)
	if !ok {
		reservationRegistry.Unlock()
		return nil, fmt.Errorf("%w: node %s missing from cache", ErrNodeReservationConflict, nodeID)
	}
	local := reservationRegistry.nodes[nodeID]
	if local == nil {
		local = &nodeReservationAmount{}
	}
	if err := checkReservationCapacity(&sconf.SchedulerConf, current, local, cpuMilli, memMB); err != nil {
		reservationRegistry.Unlock()
		return nil, err
	}
	local.cpuMilli += cpuMilli
	local.memMB += memMB
	local.mvm++
	local.creating++
	reservationRegistry.nodes[nodeID] = local
	bumpReservedNum(nodeID, 1)
	reservationRegistry.Unlock()

	return &NodeReservation{NodeID: nodeID, CpuMilli: cpuMilli, MemMB: memMB}, nil
}

// checkReservationCapacity mirrors the mandatory guard predicates, charging
// this replica's outstanding reservations on top of the last reported usage.
// The comparisons stay one request stricter than the filters (free must
// exceed the request, matching cpufilter/memfilter).
func checkReservationCapacity(sconf *config.SchedulerConf, n *node.Node, local *nodeReservationAmount, cpuMilli, memMB int64) error {
	cpuFree := n.QuotaCpu -
		sconf.EffectiveAllocated(n.QuotaCpuUsage) - local.cpuMilli
	if cpuFree <= cpuMilli {
		return fmt.Errorf("%w: node %s cpu free %d, want > %d", ErrNodeReservationConflict, n.ID(), cpuFree, cpuMilli)
	}
	memFree := n.QuotaMem -
		sconf.EffectiveAllocated(n.QuotaMemUsage) - local.memMB
	if memFree <= memMB {
		return fmt.Errorf("%w: node %s mem free %d, want > %d", ErrNodeReservationConflict, n.ID(), memFree, memMB)
	}
	if mvmLimit := RealMaxMvmLimit(n); n.MvmNum+local.mvm >= mvmLimit {
		return fmt.Errorf("%w: node %s mvm %d reserved %d, limit %d",
			ErrNodeReservationConflict, n.ID(), n.MvmNum, local.mvm, mvmLimit)
	}
	if createLimit := CreateConcurrentLimit(n); n.RealTimeCreateNum+local.creating >= createLimit {
		return fmt.Errorf("%w: node %s creating %d reserved %d, limit %d",
			ErrNodeReservationConflict, n.ID(), n.RealTimeCreateNum, local.creating, createLimit)
	}
	return nil
}

// Release returns the local reservation, including after request cancellation.
// Metric updates arrive independently; release does not wait for an updated report.
func (r *NodeReservation) Release(ctx context.Context) {
	if r == nil || !r.released.CompareAndSwap(false, true) {
		return
	}
	r.rollbackLocal()
}

// rollbackLocal undoes the local record. Caller must not hold the registry
// lock; the function takes it itself.
func (r *NodeReservation) rollbackLocal() {
	reservationRegistry.Lock()
	defer reservationRegistry.Unlock()
	local := reservationRegistry.nodes[r.NodeID]
	if local != nil {
		local.cpuMilli -= r.CpuMilli
		local.memMB -= r.MemMB
		local.mvm--
		local.creating--
		if local.empty() {
			delete(reservationRegistry.nodes, r.NodeID)
		}
	}
	bumpReservedNum(r.NodeID, -1)
}

// bumpReservedNum mirrors the reservation count onto the cached node so
// frozen snapshots expose it as SnapshotNode.reserved. Best-effort: if the
// node was re-registered and the cache entry replaced, the mirror restarts
// from zero while the registry keeps the authoritative counts.
func bumpReservedNum(nodeID string, delta int64) {
	if elem, ok := l.cache.Get(nodeID); ok {
		if cached, ok := elem.(*node.Node); ok && cached != nil {
			cached.ReservedNumIncrBy(delta)
		}
	}
}
