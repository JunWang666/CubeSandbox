// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package score

import "math"

// calculateResourceFitScore 计算请求放入节点后的资源适配分数。
//
// cpuCapacity、cpuUsed 和 cpuRequest 必须使用相同单位，例如 millicore；
// memCapacity、memUsed 和 memRequest 必须使用相同单位，例如 MB。
//
// 分数由两部分组成：
//   - packingScore：放入请求后剩余资源越少，分数越高；
//   - imbalancePenalty：CPU 和内存剩余比例差异越大，扣分越多。
//
// 返回值范围为 0～100，分数越高表示节点越适合当前请求。
func calculateResourceFitScore(
	cpuCapacity int64,
	cpuUsed int64,
	cpuRequest int64,
	memCapacity int64,
	memUsed int64,
	memRequest int64,
	imbalancePenalty float64,
) float64 {
	if cpuCapacity <= 0 || memCapacity <= 0 {
		return 0
	}

	if cpuUsed < 0 || cpuRequest < 0 ||
		memUsed < 0 || memRequest < 0 {
		return 0
	}

	if math.IsNaN(imbalancePenalty) ||
		math.IsInf(imbalancePenalty, 0) ||
		imbalancePenalty < 0 {
		return 0
	}

	if cpuUsed > cpuCapacity || memUsed > memCapacity {
		return 0
	}

	// 使用减法判断，避免 cpuUsed + cpuRequest 发生整数溢出。
	if cpuRequest > cpuCapacity-cpuUsed {
		return 0
	}

	if memRequest > memCapacity-memUsed {
		return 0
	}

	cpuRemaining := cpuCapacity - cpuUsed - cpuRequest
	memRemaining := memCapacity - memUsed - memRequest

	cpuRemainingRatio :=
		float64(cpuRemaining) / float64(cpuCapacity)

	memRemainingRatio :=
		float64(memRemaining) / float64(memCapacity)

	averageRemainingRatio :=
		(cpuRemainingRatio + memRemainingRatio) / 2

	packingScore := (1 - averageRemainingRatio) * 100

	imbalance :=
		math.Abs(cpuRemainingRatio-memRemainingRatio) * 100

	score := packingScore - imbalancePenalty*imbalance

	if score < 0 {
		return 0
	}

	if score > 100 {
		return 100
	}

	return score
}
