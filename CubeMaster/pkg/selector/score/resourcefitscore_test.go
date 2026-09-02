// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package score

import "testing"

func TestCalculateResourceFitScorePrefersTighterBalancedFit(t *testing.T) {
	// 节点 A：8 核/16 GB，已使用 4 核/8 GB。
	// 放入 2 核/4 GB 请求后，CPU 和内存都剩余 25%。
	tightFit := calculateResourceFitScore(
		8000, 4000, 2000,
		16384, 8192, 4096,
		0.5,
	)

	// 节点 B：8 核/16 GB，当前完全空闲。
	// 放入相同请求后，CPU 和内存都剩余 75%。
	looseFit := calculateResourceFitScore(
		8000, 0, 2000,
		16384, 0, 4096,
		0.5,
	)

	if tightFit <= looseFit {
		t.Fatalf(
			"tighter fit should receive a higher score: tight=%f loose=%f",
			tightFit,
			looseFit,
		)
	}
}

func TestCalculateResourceFitScorePenalizesImbalance(t *testing.T) {
	// 放入请求后，CPU 和内存都剩余 25%。
	balanced := calculateResourceFitScore(
		8000, 4000, 2000,
		16384, 8192, 4096,
		0.5,
	)

	// 放入请求后，CPU 剩余 0%，内存剩余 50%。
	// 总体剩余资源与 balanced 相近，但资源比例严重失衡。
	imbalanced := calculateResourceFitScore(
		8000, 6000, 2000,
		16384, 4096, 4096,
		0.5,
	)

	if balanced <= imbalanced {
		t.Fatalf(
			"balanced fit should receive a higher score: balanced=%f imbalanced=%f",
			balanced,
			imbalanced,
		)
	}
}

func TestCalculateResourceFitScoreRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string

		cpuCapacity int64
		cpuUsed     int64
		cpuRequest  int64

		memCapacity int64
		memUsed     int64
		memRequest  int64

		imbalancePenalty float64
	}{
		{
			name:             "zero cpu capacity",
			cpuCapacity:      0,
			memCapacity:      16384,
			imbalancePenalty: 0.5,
		},
		{
			name:             "negative usage",
			cpuCapacity:      8000,
			cpuUsed:          -1,
			memCapacity:      16384,
			imbalancePenalty: 0.5,
		},
		{
			name:             "cpu request exceeds remaining capacity",
			cpuCapacity:      8000,
			cpuUsed:          7000,
			cpuRequest:       2000,
			memCapacity:      16384,
			memUsed:          4096,
			memRequest:       4096,
			imbalancePenalty: 0.5,
		},
		{
			name:             "memory request exceeds remaining capacity",
			cpuCapacity:      8000,
			cpuUsed:          2000,
			cpuRequest:       2000,
			memCapacity:      16384,
			memUsed:          15000,
			memRequest:       4096,
			imbalancePenalty: 0.5,
		},
		{
			name:             "negative imbalance penalty",
			cpuCapacity:      8000,
			cpuRequest:       2000,
			memCapacity:      16384,
			memRequest:       4096,
			imbalancePenalty: -0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateResourceFitScore(
				tt.cpuCapacity,
				tt.cpuUsed,
				tt.cpuRequest,
				tt.memCapacity,
				tt.memUsed,
				tt.memRequest,
				tt.imbalancePenalty,
			)

			if got != 0 {
				t.Fatalf("invalid input should return zero score, got=%f", got)
			}
		})
	}
}
