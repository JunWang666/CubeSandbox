# 调度仿真评测报告（schedsim）

> 日期：2026-09-09 ｜ 代码：`9441033c` + 未提交的模块修复（schedsim
> `--mode=performance`、cube-bench `mixed_spec` 内置模板池、
> schedsim `--template-preload` 默认值 1.0 → 0.3）
> 环境：schedsim（纯 Go、进程内调度核心）运行在 4 vCPU AMD Ryzen 7 7840HS /
> 约 4 GB 可用内存的开发机，go1.25.7 linux/amd64。

本报告是评测方案的仿真腿（仿真器大规模模拟）：用固定请求 trace 在模拟同构
集群上回放真实调度核心（quality 模式走 `scheduler.Select`，performance 模式走
sim 侧分阶段复刻实现），对比 legacy 调度配置与三个场景策略
（`burst_balance` / `template_reuse` / `mixed_binpack`）。真实控制面 A/B 实测见
[调度策略基准报告](./scheduler-eval-benchmark)——该报告开头有时效说明，记录了
此后代码发生的变化。

## 运行元数据（所有实验共用）

| 项 | 值 |
| --- | --- |
| trace | cube-bench `--dump-trace`，seed 42：`burst`（500 请求，泊松 50/s，寿命 U(10s,120s)，tpl-burst 1C2G）、`template_storm`（300 请求，30/s，U(30s,90s)，单一 tpl-storm 2C4G）、`mixed_spec`（400 请求，10/s，U(30s,300s)，内置模板池 1C2G:2C4G:8C16G = 6:3:1） |
| 集群 | 300 个同构节点 × 64C / 128GiB（"500 节点规模验证"一节用 500） |
| 超卖 | cpu_ratio 3.0 / mem_ratio 2.0（overcommit 调优组除外） |
| template_preload | 0.3（30% 节点预置每个模板的本地副本） |
| allow_non_local_template | true（冷节点远程 restore 并升温本地缓存，对齐真机 A/B 的 fake-cubelet 行为模型） |
| seed / 轮数 | 基准 seed 42，每变体 5 轮（seed 42–46）；工具链 95% 置信区间 = 均值 ± 1.96·s/√n（与 cube-bench compare 对齐）——下文各表早于该改动，使用 t(0.975, n−1) 乘子，因此比当前工具产出的区间宽约 1.4 倍 |
| 变体 | `legacy` = `cmd/schedsim/example.sim.yaml`（least-loaded top-1 打分）；`legacy_firstfit` = 同配置去掉 `score:` 段（退化为近似 first-fit，即真机 A/B 记录的修复前行为）；外加与负载匹配的场景策略 |
| 模式 | `quality`（虚拟时钟、质量指标）与 `performance`（分阶段墙钟计时） |
| 报告元数据 | 每次运行的 JSON 报告在 `config.version`（二进制的 git revision，有未提交改动时带 `-dirty` 后缀）与 `config.metric_sync_interval`（实际生效的 `scheduler.metric_update_timeout`，秒）记录代码版本与指标同步周期 |

设置 `legacy_firstfit` 的原因：真机 A/B 报告里的 "legacy" 是在零 scorer 下
运行的（该报告问题清单第 3 条）；而仓库自带的 sim 示例配置已带 least-loaded
打分，不加这个变体就看不到摊平效果。

## 多 seed A/B 对照（quality 模式，300 节点，5 个 seed）

数值为逐轮均值 ± 95% 置信区间。Δ 相对 `legacy`。

### burst ↔ burst_balance

| 指标 | legacy | burst_balance | Δ | legacy_firstfit |
| --- | --- | --- | --- | --- |
| success_rate | 1.0000 ± 0 | 1.0000 ± 0 | +0.0% | 1.0000 ± 0 |
| sched_latency_p50_ms | 0.648 ± 0.135 | 0.704 ± 0.174 | +8.7% | 0.602 ± 0.122 |
| sched_latency_p99_ms | 2.512 ± 1.941 | 1.814 ± 0.633 | −27.8% | 2.975 ± 2.201 |
| load_cv_cpu | 1.286 ± 0.006 | 1.285 ± 0.023 | −0.0% | 1.561 ± 0.055 |
| jain_cpu | 0.5672 ± 0.0015 | 0.5659 ± 0.0096 | −0.2% | 0.4175 ± 0.0205 |
| herding_top1_share | 0.0040 ± 0 | 0.0040 ± 0 | +0.0% | 0.0132 ± 0.0033 |
| template_hit_rate | 0.594 ± 0.012 | 0.594 ± 0.012 | +0.0% | 0.668 ± 0.013 |
| active_nodes_avg | 187.2 ± 0.5 | 186.8 ± 2.9 | −0.2% | 156.3 ± 3.1 |

**解读。** `legacy` 基线本身已通过 least-loaded top-1 摊平，因此
`burst_balance`（同一打分族 + spread top-3）相对它放置中性——均衡、命中率、
成功率完全一致。真正的差异在相对修复前基线（`legacy_firstfit`）：Jain
0.418 → 0.567（+36%）、CV −21%、羊群度 1.3% → 0.4%，成功率无代价。同时注意
first-fit 堆叠的模板命中率反而更高（0.668 vs 0.594）：集中放置天然有利于缓存
保温——这正是真机 A/B 中 legacy 表现"更好"的同一机制。

### template_storm ↔ template_reuse

| 指标 | legacy | template_reuse | Δ | legacy_firstfit |
| --- | --- | --- | --- | --- |
| success_rate | 1.0000 ± 0 | 1.0000 ± 0 | +0.0% | 1.0000 ± 0 |
| sched_latency_p50_ms | 0.637 ± 0.132 | 0.743 ± 0.164 | +16.6% | 0.510 ± 0.140 |
| sched_latency_p99_ms | 1.889 ± 1.065 | 2.188 ± 0.939 | +15.8% | 1.483 ± 0.384 |
| load_cv_cpu | 1.431 ± 0 | 2.305 ± 0.034 | +61.1% | 1.997 ± 0.051 |
| jain_cpu | 0.609 ± 0 | 0.243 ± 0.009 | −60.1% | 0.342 ± 0.016 |
| herding_top1_share | 0.0033 ± 0 | 0.0127 ± 0.0019 | +280% | 0.0167 ± 0 |
| **template_hit_rate** | 0.324 ± 0.020 | **1.000 ± 0** | **+208.6%** | 0.573 ± 0.008 |
| active_nodes_avg | 182.8 ± 0 | 78.4 ± 2.7 | −57.1% | 126.7 ± 4.6 |

**解读。** `template_reuse` 达到完美的 1.000 模板命中率（legacy 随机散布，命中率
≈ 预置比例 0.324；first-fit 只有 0.573，因为它的集中是模板无关的）。代价是均衡：
负载集中在大约 90 个持有副本的节点上（Jain 0.243、活跃节点 −57%）——这正是同模板
风暴场景下"本地化优先于均衡"的预期取舍。

### mixed_spec ↔ mixed_binpack

| 指标 | legacy | mixed_binpack | Δ | legacy_firstfit |
| --- | --- | --- | --- | --- |
| success_rate | 1.0000 ± 0 | 1.0000 ± 0 | +0.0% | 1.0000 ± 0 |
| sched_latency_p50_ms | 0.646 ± 0.146 | 0.740 ± 0.205 | +14.5% | 0.529 ± 0.135 |
| sched_latency_p99_ms | 1.558 ± 0.318 | 2.541 ± 0.971 | +63.0% | 2.107 ± 0.777 |
| load_cv_cpu | 2.468 ± 0.008 | 7.021 ± 0.008 | +184.4% | 2.719 ± 0.091 |
| jain_cpu | 0.311 ± 0.001 | 0.0225 ± 0.0001 | −92.8% | 0.230 ± 0.015 |
| herding_top1_share | 0.0055 ± 0.0014 | 0.160 ± 0 | +2809% | 0.014 ± 0.0017 |
| **template_hit_rate** | 0.382 ± 0.022 | **0.956 ± 0.012** | **+150.5%** | 0.468 ± 0.018 |
| **active_nodes_avg** | 165.5 ± 1.6 | **7.26 ± 0** | **−95.6%** | 131.9 ± 5.1 |

**解读。** `mixed_binpack` 把 400 个混合规格请求整体收拢到 300 节点中的约 7.3 个
（峰值需求 ≈ 800 vCPU，7 × 64C × 3.0 超卖容量可以容纳），约 293 个节点保持完全
空闲以承接后续大规格请求；装箱的副作用是热节点持续热，模板命中率达到 0.956。
本实验中 `fragmentation_ratio` 恒为 0——集群分配率仅 ~2.3%，没有可被碎片化的空闲
资源；该指标需要接近饱和的负载才能区分策略。代价：均衡指标被主动牺牲（这正是
装箱的定义），且羊群度升至 16%——`spread top_n=2` 只在分数最高的 2 个候选中选，
而它们通常就是同一批已装箱节点。P99 决策延迟 +63%（1.56 → 2.54 ms），仍在个位
数毫秒量级。

## 调度开销（performance 模式，300 节点，5 个 seed）

`--mode=performance` 用 sim 侧逐阶段复刻的流水线（`pkg/scheduler/sim/perf.go`，
与真实 `Select` 的等价性由 `TestPerfModeMatchesQualityMode` 保证）驱动每次决策，
并按墙钟记录各阶段耗时。下表为跨轮均值；`total` 为完整决策耗时。

| 阶段 P50（ms） | legacy | burst_balance | legacy | template_reuse | legacy | mixed_binpack |
| --- | --- | --- | --- | --- | --- | --- |
| 负载 | burst | burst | storm | storm | mixed | mixed |
| prefilter | 0.409 | 0.425 | 0.385 | 0.384 | 0.408 | 0.393 |
| guards | 0.0001 | 0.0932 | 0.0001 | 0.0823 | 0.0001 | 0.0836 |
| filter | 0.0612 | 0.0001 | 0.0601 | 0.0001 | 0.0610 | 0.0001 |
| score | 0.0828 | 0.0826 | 0.0820 | 0.1089 | 0.0893 | 0.0914 |
| pick | 0.0005 | 0.0001 | 0.0005 | 0.0001 | 0.0005 | 0.0001 |
| **total P50** | 0.597 | 0.666 | 0.575 | 0.650 | 0.608 | 0.624 |
| **total P95** | 1.515 | 1.675 | 1.079 | 1.215 | 1.111 | 1.262 |
| **total P99** | 3.192 | 3.459 | 1.955 | 1.695 | 1.621 | 1.923 |
| **吞吐（次决策/秒）** | 1113 | 1036 | 1181 | 1389 | 1175 | 1377 |

**解读。**

- 插件框架自身的开销很小，且主要是阶段搬迁：Profile 流水线新增强制 guards
  阶段（P50 约 0.09 ms），但不再运行可选 filter（legacy 在此花约 0.06 ms）——
  total P50 净增 +0.02…0.07 ms（+3%…+13%）。
- prefilter（300 节点的候选枚举 + 快照冻结）占决策成本约 65%，score 约 15%；
  两者都不随策略增长。
- 吞吐差异是二阶的，且在 storm/mixed 上反而**有利于**新策略（+17%）：
  本地化/装箱过滤在打分前缩小了候选集，逐节点打分工作量更小。
- 这些是 4 vCPU 机器上单进程、零排队条件下的数字，用于量化框架开销，
  不代表生产延迟预算。

## 500 节点规模验证（quality 模式，3 轮，seed 42–44）

同一批 trace，集群扩到 500 节点。各项指标的方向和量级与 300 节点一致
（n=3 时延迟 CI 变宽；均衡/分配类指标接近确定性）：

| 负载 | 指标 | legacy | 策略 | 解读 |
| --- | --- | --- | --- | --- |
| burst | jain_cpu / active_nodes | 0.514 / 256.8 | 0.514 / 256.8 | burst_balance 在 500 节点下相对带打分的 legacy 仍为放置中性 |
| storm | template_hit_rate | 0.308 ± 0.104 | **1.000 ± 0** | 完美本地化在 500 节点下保持 |
| storm | active_nodes_avg | 182.8 | 112.4 ± 12.9 | 负载 confined 在副本节点（约 150 个预置节点） |
| mixed | active_nodes_avg | 197.4 | **7.25 ± 0.05** | 装箱效果与规模无关（由需求量决定，而非集群规模） |
| mixed | template_hit_rate | 0.296 ± 0.031 | 0.961 ± 0.019 | 命中率收益保持 |
| 全部 | sched_latency_p50_ms | 0.89–0.93 | 0.98–1.13 | 决策成本亚线性增长（节点 1.67× → P50 约 1.5×），500 节点无拐点 |
| 全部 | success_rate | 1.0 | 1.0 | 规模下无失败 |

## 单变量调优（mixed_spec trace，300 节点，5 个 seed）

两组实验各只动 `mixed_binpack` 的一个旋钮（出厂配置
`resource_fit_score:real_time_weighted_average` = 0.7:0.3、cpu_ratio 3.0）。

### Score 权重（fit : realtime）

| 权重 | active_nodes_avg | jain_cpu | herding_top1 | template_hit | 结论 |
| --- | --- | --- | --- | --- | --- |
| 0.5 : 0.5 | 166.0 ± 1.6 | 0.311 | 0.006 | 0.378 | 退化为均衡策略——装箱能力丢失 |
| **0.7 : 0.3（出厂）** | 7.24 ± 0.02 | 0.0225 | 0.160 | 0.956 | 正常装箱；基准点 |
| 0.9 : 0.1 | 7.25 ± 0.02 | 0.0225 | 0.160 | 0.954 | 与出厂完全一致 |

**0.5:0.5 与 0.7:0.3 之间存在一个相变边界**：一旦均衡打分器权重追平，放置行为
就从装箱翻转成均衡（活跃节点 7 → 166）。边界之上对精确比例不敏感。建议：维持
0.7:0.3，不要把 fit 权重降到 0.5 附近。

### Overcommit（cpu_ratio；mem_ratio 固定 2.0）

| cpu_ratio | active_nodes_avg | jain_cpu | herding_top1 | template_hit | fragmentation | success |
| --- | --- | --- | --- | --- | --- | --- |
| **3.0（出厂）** | 7.24 ± 0.02 | 0.0224 | 0.160 | 0.959 ± 0.008 | 0 | 1.0 |
| 1.5 | 9.01 ± 0.04 | 0.0271 | 0.128 | 0.956 ± 0.010 | 0.0001 | 1.0 |
| 1.0 | 13.18 ± 0 | 0.0394 | 0.090 | 0.919 ± 0.006 | 0.0003 | 1.0 |

降低超卖比会**减弱**装箱集中度（活跃节点 7.2 → 9.0 → 13.2）：单节点有效容量变小，
已装箱节点更快被填满，放置提前外溢到新节点；羊群度随之缓解，碎片率仅在 1.0 时
微弱出现（≤0.03%）。本负载下各档成功率均为 1。建议：追求最大装箱保持 3.0；若更
在意单节点突发风险而非空节点数，1.5 可以用约 2 个额外活跃节点换取羊群度下降 20%。

## 权衡与注意事项

- **本地化与均衡是可量化的真实权衡**：template_reuse 用 Jain 0.243 换 1.000
  命中率；mixed_binpack 用 Jain 0.0225 和 16% 羊群度换 96% 的空闲节点释放。
  两者都不是"免费的"，应按负载类型选用。
- **所有运行中 fragmentation_ratio 均为 0**：集群分配率仅 2–4%，没有可碎片化
  的空闲资源。要让该指标区分策略，需要接近饱和的 trace。
- **n=5 的共享 4 vCPU 机器上延迟 CI 很宽**（P99 ±40–100%）；quality 模式 P50
  ±10% 以内的差异不应过度解读。均衡、命中率、装箱类指标接近确定（CI ≪ 1%），
  是可靠的结论依据。
- **CI 口径变更**：schedsim compare 原先渲染的是样本标准差，且本报告各表用
  t(0.975, n−1) 乘子计算；工具现已改为均值 ± 1.96·s/√n（cube-bench compare
  口径），n=5 时新报告区间比下表窄约 1.4 倍。
- performance 模式测量的是 sim 侧分阶段复刻的 `Select`：其逐请求 total 与
  quality 模式的 `sched_latency` 相差约 10%（同一流水线，少一个指标 hook），
  两种模式的放置质量摘要在 tie-break 噪声范围内一致。
- sim 的 legacy 基线（`example.sim.yaml`）自带 least-loaded 打分；原始真机 A/B
  中零 scorer 的 first-fit 堆叠行为在本报告中以 `legacy_firstfit` 复现。

## 复现方法

```bash
# 生成 trace（cube-bench 在运行开始前先写 trace；文件落盘后即可终止进程）
cd examples/cube-bench && go run . --workload mixed_spec --dry-run --no-tui --seed 42 --dump-trace /tmp/mixed.trace.json
#（burst：--templates "tpl-burst:1:1000:2048"；storm：--workload template_storm --templates "tpl-storm:1:2000:4096"）

# A/B/C 对照，5 个 seed
cd CubeMaster && go build -o /tmp/schedsim ./cmd/schedsim
/tmp/schedsim --compare legacy=cmd/schedsim/example.sim.yaml,\
mixed_binpack=cmd/schedsim/mixed_binpack.profiles.sim.yaml,\
legacy_firstfit=<去掉 score: 段的 example.sim.yaml> \
  --trace /tmp/mixed.trace.json --nodes 300 --rounds 5 --seed 42 \
  --allow-non-local-template=true -o compare.md --out-dir ./sim-out

# performance 模式（分阶段耗时 + 吞吐）
/tmp/schedsim --mode=performance --compare legacy=...,burst_balance=... \
  --trace /tmp/burst.trace.json --nodes 300 --rounds 5 --seed 42 \
  --allow-non-local-template=true -o perf.md
```
