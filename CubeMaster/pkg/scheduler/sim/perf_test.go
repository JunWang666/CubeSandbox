// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package sim

import (
	"context"
	"testing"
)

// TestPerfModeMatchesQualityMode runs the same deterministic scenario through
// the opaque scheduler.Select (quality) and the sim-side staged driver
// (performance) and requires identical placement-quality summaries. The
// scenario is tie-invariant (8 identical requests over 4 nodes distribute
// 2-2-2-2 under least-loaded scoring no matter how ties break), so every
// placement-derived metric must match exactly; only the wall-clock latency
// keys are allowed to differ.
func TestPerfModeMatchesQualityMode(t *testing.T) {
	bootstrapOnce(t)

	base := Params{
		Trace:           mkTrace(8, 1000, 60000, 1000, 2048, "tpl-perf-eq"),
		Nodes:           4,
		NodeCPUMillis:   64000,
		NodeMemMiB:      65536,
		InstanceType:    "sim",
		TemplatePreload: 1.0,
		Seed:            7,
		RoundID:         40,
	}
	quality, err := RunRound(context.Background(), base)
	if err != nil {
		t.Fatalf("quality RunRound: %v", err)
	}
	perfParams := base
	perfParams.Perf = true
	perfParams.RoundID = 41
	perf, err := RunRound(context.Background(), perfParams)
	if err != nil {
		t.Fatalf("perf RunRound: %v", err)
	}
	if perf.Perf == nil {
		t.Fatalf("perf round must carry a Perf summary")
	}

	for _, k := range SummaryKeys {
		switch k {
		case "sched_latency_p50_ms", "sched_latency_p95_ms", "sched_latency_p99_ms":
			continue // wall-clock values legitimately differ between runs
		}
		approx(t, "quality-vs-perf "+k, perf.Summary[k], quality.Summary[k], 1e-9)
	}

	// Perf plumbing sanity: every request produced one sample per stage, and
	// the staged total is a real wall-clock duration.
	for _, stage := range PerfStageKeys {
		st := perf.Perf.Stages[stage]
		if st == nil {
			t.Fatalf("perf stage %q missing", stage)
		}
		if st.Count != 8 {
			t.Fatalf("stage %q count = %d, want 8", stage, st.Count)
		}
		if st.P99Ms < st.P50Ms {
			t.Fatalf("stage %q p99 < p50 (%v < %v)", stage, st.P99Ms, st.P50Ms)
		}
	}
	if perf.Perf.WallSeconds <= 0 || perf.Perf.ThroughputRPS <= 0 {
		t.Fatalf("perf throughput not recorded: %+v", perf.Perf)
	}
}

// TestPerfSummaryAggregation hand-checks the percentile and cross-round
// aggregation math.
func TestPerfSummaryAggregation(t *testing.T) {
	s := newStageSamples()
	for i := 1; i <= 100; i++ {
		s.add(stageTimes{
			prefilter: float64(i),
			guards:    1,
			filter:    2,
			score:     3,
			pick:      0.5,
			total:     float64(i) + 6.5,
		})
	}
	perf := s.summarize(0, 0)
	approx(t, "prefilter p50", perf.Stages["prefilter"].P50Ms, 50, 1e-9)
	approx(t, "prefilter mean", perf.Stages["prefilter"].MeanMs, 50.5, 1e-9)
	approx(t, "guards p99", perf.Stages["guards"].P99Ms, 1, 1e-9)
	if perf.ThroughputRPS != 0 {
		t.Fatalf("zero wall must yield zero throughput, got %v", perf.ThroughputRPS)
	}

	rounds := []*RoundResult{
		{Seed: 1, Perf: perf},
		{Seed: 2, Perf: perf},
		{Seed: 3}, // quality round: must be skipped, not zero-averaged
	}
	agg := AggregatePerf(rounds, 100)
	if agg == nil {
		t.Fatalf("AggregatePerf returned nil")
	}
	approx(t, "agg prefilter p50", agg.Stages["prefilter"].P50Ms, 50, 1e-9)
	if agg.Stages["prefilter"].Count != 200 {
		t.Fatalf("agg count = %d, want 200", agg.Stages["prefilter"].Count)
	}

	if AggregatePerf([]*RoundResult{{Seed: 1}}, 10) != nil {
		t.Fatalf("AggregatePerf over perf-less rounds must be nil")
	}
}
