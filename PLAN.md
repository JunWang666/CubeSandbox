# CubeSandbox 调度评估体系与可扩展调度策略系统 —— 实施方案

> 状态：设计草案，待评审
> 范围：`CubeMaster/pkg/scheduler/`、`CubeMaster/pkg/selector/`、`CubeMaster/pkg/localcache/`
> 核心取向：**策略用表达式写（改配置即生效），扩展用常驻子进程做（不重编译 CubeMaster），Go 只保留框架、状态与安全底线**

---

## 1. 背景与问题

CubeMaster 在多节点集群中把沙箱创建请求调度到各 Cubelet。现有流水线是
PreFilter → Filter → Score → PostScore → 加权随机选点，内置 6 个过滤器和 4 个打分器。

在 AI Agent 场景下负载形态差异极大：突发短生命周期沙箱、同模板高频热启动、
大规格长驻沙箱、快照/克隆密集型任务，对调度的要求彼此冲突（分散 ↔ 亲和 ↔ 装箱）。
当前实现存在以下问题。

### 1.1 默认配置下根本没有打分

三份随仓库发布的配置（`CubeMaster/conf.yaml`、
`deploy/kubernetes/chart/files/cube-master/conf.yaml`、
`configs/single-node/cubemaster.yaml`）的 `scheduler` 段落完全一致，
只启用了 4 个过滤器，**没有 `score:` 段**。

而 `score.NewSelector()` 在 `conf.Score == nil` 时返回空切片，
`runScoreFilter` 第一行即 return，`resultWithScore` 始终为空。
`LeastRandomSelect(1)` 走无分数分支，取过滤后列表的第一个节点——
而该列表自 `localcache.sortedNodesByClusters`（按节点 `Index` 排序）一路保序传下来。

**结论：开箱即用的 CubeSandbox，调度等价于"选注册序最靠前的、且通过四道硬门禁的那台节点"。**
打分插件、加权随机、异步打分循环、白名单 PostScore 全部存在但默认不执行。
这是负载不均、突发打热点、模板命中率低的直接原因。

### 1.2 插件机制无法扩展

`filter.filters` / `score.scores` 是私有 `map[string]interface{}`，
通过 `reflect.ValueOf(fn).Call(nil)` 构造。out-of-tree 代码无法注册；
配置里插件名写错时 `!fn.IsValid()` 静默 `continue`，没有任何提示；
工厂函数无 error 返回，`NewImageScore` 等在配置缺失时直接 `panic`。

### 1.3 打分归一化在数学上不成立

`runScoreFilter` 中 `totalPluginWeight` 是全局分母，但若某插件只覆盖部分节点，
未覆盖的节点缺失该分量却除以同一分母，被系统性低估。
且各插件量纲不统一：`imageScore` 输出 0..100（`fwk.MaxNodeScore`），
`realTimeWeightedAverageScore` 是因子加权和除以因子权重和，
`multiFactorWeightedAverageScore` 直接透传异步算出的 `n.Score` 原始值。
配权重时无法预期效果。

### 1.4 框架缺少插件级防护

- `parallelRunFilters` 中 `eg, _ := errgroup.WithContext(selCtx.Ctx)` 把派生 context 丢弃了，
  插件拿不到取消信号；`runScoreFilter` 是纯串行调用。**没有任何 per-plugin 超时。**
- Filter 阶段的 panic 会转成 error 导致**整个 Filter 阶段失败**并降级到 backoff；
  Score 阶段框架层没有 per-plugin recover，第三方插件 panic 会让整次调度失败。
- 插件通过 `selCtx.Nodes()` 拿到 `[]*node.Node` 共享指针，可写；
  接口传的是 `*selctx.SelectorCtx`，插件可调 `SetNodes()`，在并发 Filter 阶段即数据竞争。

### 1.5 零可观测性、无 benchmark

全仓 Prometheus 埋点只有 `templatecenter`、`sandboxspec`、`server` 三处，调度器一个指标都没有。
`pkg/scheduler/local.go` 里有一套走 `CubeLog.Trace` 的上报，
`ReportStdevMetric` 打开时会报 CPU/内存配额使用率和沙箱数的样本标准差——
这是目前唯一衡量集群均衡度的东西。没有任何 benchmark 能回答"改了权重之后好了多少"。

### 1.6 其他

- `PrioritySelectNum` 代码默认 `-1`（全集群加权随机，稀释打分结果），
  配置文件里写的是 `1`（贪心），两者不一致。
- `LeastRandomSelect` 用 `int(score*1e6)` 作权重，对负分无防御。
- `thirtparty` 过滤器是空壳，所有分支原样返回全部节点，等于没有。
- Backoff 兜底路径在未配置 `BackoffNodeSelector` 时直接返回空列表，默认等于关闭。

---

## 2. 设计取向

三条原则决定了整个方案的形状。

**策略层 100% 表达式化。** 打分和业务过滤本质是纯函数 `f(node, req, cluster) → score/bool`，
Go 插件相对表达式没有表达力优势，却带来重编译、重发版、滚动重启的成本。
内置策略也用表达式写，随发行版分发，同时充当示例文档。

**扩展走常驻子进程，不重编译 CubeMaster。** 需要新数据维度、外部系统数据、
任意语言实现时，用常驻子进程（Unix socket gRPC）而非 Go 插件。
关键是**常驻**——Volume 插件的 `binary` 驱动是 fork-per-op（见
`CubeMaster/pkg/volume/plugin/binary/driver.go` 的调用约定注释），
对秒级低频的 volume create 没问题，但调度是每次创建沙箱都走的热路径，
突发 500 QPS 下每秒 fork 500 个进程不可接受。

**子进程供货，表达式决策。** 子进程不返回最终分数，而是注入自定义变量到 `node.ext.*`，
策略仍然用表达式写。于是加新维度改子进程、调策略改 YAML，两者都不需要重编译 CubeMaster。

Go 只保留四样东西：流水线编排、安全底线（guard）、跨请求状态、数据供给。

---

## 3. 架构总览

```
┌── 编排层（Go，不可配）
│   PreFilter/Filter/Score/PostScore 流水线、backoff、重试熔断、cordon 复核
│   per-plugin 超时 / panic 隔离 / 熔断 / 降级基线
│
├── guard 层（Go，不可关闭）
│   容量上限（含 overcommit）、节点健康、cordon、指标新鲜度、沙箱数上限
│   语义：最终候选集 = guard(全集) ∩ policy(全集)，策略只能收紧不能放宽
│
├── 数据供给层（Go）
│   调度上下文视图（node/req/cluster/node.ext）、按需计算调度、
│   assume 乐观预留状态、模板副本索引、集群聚合量
│
├── ★ 策略层（表达式，100% 配置化，含全部内置策略）
│   policy filter 表达式 + score 表达式 + 权重 + 选点策略 + Profile 路由
│
└── 扩展层（不重编译 CubeMaster）
    常驻子进程 Extender：注入 node.ext.* 变量 / 参与 Filter / 参与 Score / 接管 Bind
    Go SPI：仅保留给上游维护者，不作为用户侧扩展方式对外宣传
```

---

## 4. 分层设计

### L0 框架加固（前置条件，所有后续工作依赖它）

一律在框架层实现，Go 插件、表达式、子进程三种扩展共用。

| 项 | 设计 |
|---|---|
| per-plugin 超时 | 每次插件调用包一层带 deadline 的 context，默认 filter 10ms / score 10ms / extender 30ms |
| panic 隔离 | 每个插件调用点 recover，转成该插件失败，**不再让单个插件拖垮整个阶段** |
| 熔断 | 连续失败 N 次熔断 T 秒，期间不调用，出指标 |
| 失败语义 | `on_error: fail_open`（默认，该插件当作未参与）/ `fail_closed`（整体失败），逐插件可配 |
| 降级基线 | 任何扩展不可用时回落到内置基线 profile，保证调度不中断 |
| 只读视图 | 插件收到的是值语义的 `NodeView` 快照，不是 `*node.Node`，杜绝污染共享缓存 |
| 打分归一化 | 每插件输出线性归一到 `[0, 100]`；分母改为 profile 内启用插件的权重和；缺失分量按声明的 `default_score`（默认 0）补齐 |
| 权重钳制 | `LeastRandomSelect` 的 `int(score*1e6)` 增加 `max(0, ...)` |

**兼容性**：归一化与新分母语义受 profile 级 `normalize` 开关控制，`default` profile 设 `false`
保持旧语义，升级后打分结果逐位不变；用 benchmark 数据证明新语义更优后由运维显式切换。

### L1 调度上下文视图 Schema（地基，唯一不可回退的部分）

只定义一次，四处绑定：Go SPI 的入参、表达式的求值环境、Extender 的 proto message、
仿真器伪造的对象。字段一旦发布即为对外 API，只增不改不删。

```
node.id / ip / zone / region / cluster / instance_type / cpu_type
node.quota_cpu / quota_mem                          原始配额
node.cpu_alloc_ratio        / mem_alloc_ratio        已分配 / 原始配额
node.cpu_alloc_ratio_eff    / mem_alloc_ratio_eff    已分配 / 有效容量（含 overcommit）
node.cpu_alloc_ratio_after  / mem_alloc_ratio_after  (已分配 + 预留 + 本次请求) / 有效容量
node.cpu_util / mem_util / cpu_load_ratio
node.mvm_num / mvm_ratio                             沙箱数 / 上限
node.inflight / inflight_ratio                       本副本在途创建数
node.data_disk_usage / storage_disk_usage / sys_disk_usage
node.labels{}
node.has_template / zone_has_template / template_replicas / template_inflight
node.ext{}                                           ← 子进程注入的自定义维度

req.cpu / mem / disk / template_id / image_ids[] / instance_type / zone / labels{}

cluster.node_count / avg_cpu_alloc_ratio / cpu_alloc_cv / mem_alloc_cv
```

命名规则：凡是口径有歧义的量，把口径写进名字。
`cpu_alloc_ratio` / `_eff` / `_after` 三个变体宁可名字长，也不能让写表达式的人猜。

**按需计算。** 变量按代价分级注册（零成本字段直读 / 一次算术 / 一次 map 查找 / 遍历全集群）。
配置加载时从表达式 AST 提取被引用的标识符集合，运行时只计算该集合。
`cluster.*` 聚合量每次调度只算一次并缓存在上下文里，绝不进 per-node 循环。

### L2 表达式策略层

引擎选 **`expr-lang/expr`**：纯 Go、无 cgo、依赖极小、编译成字节码、
结构体绑定零分配、每次求值约 100–300ns，且**不图灵完备**（保证终止）。

> 备选 `cel-go`（与 k8s 生态一致，但拖 antlr + protobuf）。
> `gopher-lua` 虽已作为 `miniredis` 的间接依赖存在于 `go.sum`，但它是测试依赖，
> 用于生产需提升为直接依赖并自行实现指令计数、库裁剪、VM 池，性价比不足。

护栏：

- **编译期校验**：配置加载时编译并做类型检查，失败则拒绝启动 / 拒绝热更新并保留旧配置
- **输出净化**：NaN / Inf / 越界一律 clamp 到 `[0, 100]` 并计数告警
- **求值隔离**：panic recover，按 `on_error` 降级
- **热更新**：复用现有 `hotswap` 监听（`config.listener.OnEvent`），编译成功后原子替换

### L3 Profile 机制

`SchedulerConf` 新增 `profiles` 与 `profile_selector`，现有 `Filter` / `Score` / `PostScore`
字段全部保留。路由优先级：`selCtx.ProfileName` 显式指定 > 请求 label 匹配 >
instance type 匹配 > `default`。运行期命中不存在的 profile 记 warn 回落 default，启动期严格校验。

**回落兼容**：`preHandleScheduler` 中若 `Profiles` 为空，
用现有 `Filter.EnableFilters` + `Score.EnableScorers` + `ScorePluginConf`
合成一个 `normalize: false` 的 `default` profile。存量 `conf.yaml`、Helm chart、
单机 configs **一行都不用改**。

### L4 常驻子进程 Extender

配置形状对齐仓库既有的 Volume 插件语汇（`type: binary` 托管 / `type: rpc` 外部）：

```yaml
scheduler:
  extenders:
    - name: tenant-affinity
      type: binary                                    # CubeMaster 托管：启动拉起、崩溃重启
      binary_path: /opt/cube/plugins/tenant-affinity
      stages: [enrich]
      provides: [tenant_sandbox_count]                # 注入到 node.ext.*
      fields: [id, zone, labels]                      # 声明需要同步哪些字段
      timeout: 30ms
      on_error: fail_open
      circuit_breaker: { threshold: 5, cooldown: 30s }

    - name: ml-scorer
      type: rpc                                       # 外部部署：k8s sidecar / systemd
      socket_path: unix:///run/cube/sched-ml.sock
      stages: [enrich]
      provides: [ml_health_score]
```

生产推荐 `rpc`（重启策略交给 kubelet，资源限制用 cgroup，日志走标准采集）；
`binary` 用于单机版与 dev-env，代价是 CubeMaster 要处理僵尸进程、
重启指数退避、日志转发、优雅退出。

#### 协议：状态同步与调度决策分离

**这是设计中最容易做错的地方。** 直觉做法是每次调度把候选节点整个序列化传过去：
1000 节点 × 40 字段 ≈ 100–200KB，500 QPS 即每秒 50–100MB 穿过 socket 外加两侧
marshal/unmarshal，开销会彻底盖过调度本身。

拆成两条通道：

```protobuf
service SchedulerExtender {
  rpc Handshake(HandshakeRequest) returns (HandshakeResponse);   // 协议版本 / 阶段 / 字段协商
  rpc WatchNodes(WatchRequest) returns (stream NodeEvent);       // 通道 1：节点增量，子进程维护本地镜像
  rpc Schedule(ScheduleRequest) returns (ScheduleResponse);      // 通道 2：只传请求 + 候选 ID
}

message ScheduleRequest {
  Request req = 1;
  repeated string candidate_ids = 2;   // 只是 ID，不带节点详情
  uint64 snapshot_seq = 3;             // 子进程据此判断镜像是否够新
}

message ScheduleResponse {
  repeated NodeAnnotation annotations = 1;  // node_id → {key: value}，挂到 node.ext.*
  repeated string filtered_out = 2;         // 可选，参与 Filter
  map<string, double> scores = 3;           // 可选，直接参与 Score
  string bind_node_id = 4;                  // 可选，接管最终选点
}
```

每次调度载荷从 100KB 降到几百字节，一次 RTT 搞定。叠加三个优化：

- **字段裁剪**：握手时声明需要的字段，状态流只推这些（与表达式静态分析共用机制）
- **候选集裁剪**：CubeMaster 先跑完 guard，只把幸存者 ID 交出去
- **合并调用**：Filter 与 Score 合并为一次 `Schedule`，避免两次 RTT

`snapshot_seq` 用于发现镜像落后过多，超过阈值时子进程主动请求全量重同步。
镜像滞后是可接受的——CubeMaster 自己的节点数据也来自心跳，本就不是强一致。

#### 可靠性

硬超时、熔断、背压（并发调用数上限，超过直接降级）、
崩溃自动重启带指数退避、版本协商失败拒绝加载并告警。
**降级基线是核心**：子进程超时/崩溃/熔断时必须回落到不含 `node.ext.*` 的基线表达式，
绝不能出现"扩展进程挂了 → 集群创建不了沙箱"。

#### 代价（必须写进文档）

延迟增加 0.2–1ms。相对当前微秒级的调度是数量级增长，
但沙箱创建端到端是几百毫秒到秒级（冷启动要拉模板），占比千分之几，trade-off 划算。
另需承担多一个进程的监控、日志、版本兼容矩阵成本。

### L5 评估指标

#### 指标集（7 项，超出验收要求的 5 项）

| 指标 | 定义 |
|---|---|
| 装箱率 | CPU/Mem 分别：Σ已分配 / Σ有效可调度容量（含 overcommit）。另报**饱和点装箱率**：集群首次拒绝请求时刻的分配率 |
| 负载均衡度 | 节点利用率的变异系数 CV = σ/μ、Jain 公平指数、max−min 极差 |
| 模板本地命中率 | 命中本地副本的调度数 / 带 TemplateID 的调度数；派生冷启动次数 |
| 调度成功率 | 成功 / 总请求，按失败原因下钻（NoRes / filter 全灭 / 走 backoff） |
| 调度延迟 | `Select()` 耗时 P50/P95/P99，可按 phase 与 plugin 下钻 |
| 端到端创建延迟 | 生产侧真实测量；仿真侧按命中/未命中模板分别采样启动延迟分布 |
| 碎片率与羊群度 | 碎片率 = 剩余资源装不下最大规格请求的节点占比；羊群度 = 同 100ms 窗口内落到同一节点的请求数 P95 |

#### 生产侧埋点

新增 `CubeMaster/pkg/scheduler/metrics/`，沿用 `pkg/sandboxspec/metrics.go` 的 promauto 风格：

```
cube_scheduler_attempt_total{profile, result}                counter
cube_scheduler_latency_seconds{profile, phase}               histogram
cube_scheduler_plugin_latency_seconds{plugin, type}          histogram
cube_scheduler_filter_rejected_nodes_total{plugin}           counter    排障用：哪个过滤器把节点滤光了
cube_scheduler_template_locality_total{hit}                  counter
cube_scheduler_expr_error_total{profile, plugin, reason}     counter
cube_scheduler_extender_latency_seconds{name}                histogram
cube_scheduler_extender_failure_total{name, reason}          counter
cube_scheduler_extender_circuit_open{name}                   gauge
cube_scheduler_node_alloc_ratio{node, resource}              gauge
cube_scheduler_cluster_alloc_cv{resource}                    gauge
cube_scheduler_assume_pending{profile}                       gauge
```

埋点位置：`Select()` 入口出口、`parallelRunFilters` 每个插件前后、`runScoreFilter` 每插件、
`sandbox_run.go` 的 `schedule()`。集群级 gauge 由现有 `CollectMetricInterval` 周期任务顺带算出。

### L6 Benchmark 仿真器

位置：`CubeMaster/pkg/scheduler/simulator/`（库）+ `CubeMaster/cmd/cubeschedbench/`（CLI，加入 Makefile `APPS`）。

**核心取舍：仿真器调用真实的 `scheduler.Select()`**，只把集群状态和时间换成可控的内存实现。
这样 benchmark 结论对生产有效，调度器改动会立刻反映，不存在两套逻辑漂移的风险。

**可行性前提**：`localcache.Init()` 依赖 DB + Redis 不能用于仿真，
但内部 `l.cache` 是纯内存 go-cache，`UpsertNode` / `RegisterTemplateReplica` /
`SetSnapshotStorageState` / `SyncNodeTemplates` 全是导出的内存写入。
新增 `localcache.InitInMemory()`（只做 `Init()` 里 `cache.New` 那几行），
现有那些手工 `l.cache = cache.New(0,0)` 的测试也受益。

**事件驱动模型**（单 goroutine、固定 `--seed` 完全可复现）：

```
CreateArrival(t) → 组装 SelectorCtx → scheduler.Select() → 记录延迟/结果
                 → 成功则 assume 预留，按模板命中与否采样启动延迟 d
                 → 排入 Started(t+d)：真正扣减 QuotaCpuUsage/QuotaMemUsage、MvmNum++
                 → 排入 Destroy(t+d+lifetime)：释放资源
MetricSync(t)    → 模拟心跳周期刷新节点快照，制造"陈旧视图"——羊群效应正是这么复现的
```

**四种 workload**：

| Workload | 参数要点 | 主要考核 |
|---|---|---|
| `burst-short-lived` | 脉冲到达（500/s 持续 10s，间隔 30s，共 10 轮），lifetime ~ Exp(30s)，1c2g，模板集中在 2 个 | 羊群度、调度延迟 P95、成功率 |
| `template-repeat` | 5 个模板 Zipf 分布高频创建，副本只在 30% 节点上 | 模板命中率、端到端创建延迟 |
| `mixed-size` | 1c2g : 4c8g : 16c32g = 70 : 25 : 5，lifetime ~ LogNormal(2h) | 装箱率、碎片率、均衡度 |
| `snapshot-clone-heavy` | `EnforceSnapshotStorage=true` 的克隆密集流量 | 约束下的成功率与候选集收敛 |

**一键运行与报告**：

```bash
make -C CubeMaster sched-bench      # 全 workload × 全 profile → reports/

_output/bin/cubeschedbench run \
  --cluster   configs/bench/cluster-100node.yaml \
  --workload  configs/bench/workloads/burst-short-lived.yaml \
  --profiles  default,burst-spread \
  --seed 42 --repeat 5 \
  --out reports/burst/
```

输出 `report.json`（机器可读，供 CI 回归）+ `report.md`（对比表：baseline / candidate / Δ% / 显著性）
+ `raw.csv`（每次调度明细）。`--repeat` 跑多个种子给均值与标准差，避免拿单次随机波动当结论。

报告尾部自动生成规则驱动的调优建议，例如：

- `CV > 0.30` → 提高 spread 项权重，或把 `priority_select_num` 从 1 提到 3
- `装箱率 < 60% 且成功率 = 100%` → 提高 `overcommit_ratio`，或切到 `bin-packing` profile
- `模板命中率 < 50% 且冷启动占延迟 60%+` → 提高 template 项权重，或增加模板副本数
- `filter_rejected` 集中在单个插件 → 该过滤器可能过严，给出对应配置项名

### L7 内置策略（全部用表达式实现，Go 里不留策略插件）

#### 配置示例

```yaml
scheduler:
  priority_select_num: 1
  overcommit_ratio: { cpu_ratio: 3.0, mem_ratio: 2.0 }

  profiles:
    # ── 兼容用：保持升级前行为，打分结果逐位不变 ──────────────────
    - name: default
      score:
        normalize: false
        plugins: []

    # ── 策略 A：高并发短生命周期 ─────────────────────────────────
    - name: burst-spread
      priority_select_num: 3            # top-3 内加权随机，进一步打散
      assume: { enabled: true, ttl: 20s }
      filters:
        - name: expr_filter
          config: { expr: 'node.mvm_ratio < 0.9' }
      score:
        normalize: true
        plugins:
          - name: expr_score
            weight: 3.0
            config:
              expr: '100 - (node.mvm_ratio * 50 + node.cpu_util / 100 * 30 + node.inflight_ratio * 20)'
          - name: expr_score
            weight: 1.0
            config:
              expr: '100 * (1 - abs(node.cpu_alloc_ratio_after - node.mem_alloc_ratio_after))'
          - name: expr_score
            weight: 0.5
            config:
              expr: 'node.has_template ? 100 : 0'

    # ── 策略 B：同模板高频热启动 ─────────────────────────────────
    - name: template-affinity
      score:
        normalize: true
        plugins:
          - name: expr_score
            weight: 3.0
            config:
              expr: |
                node.has_template
                  ? 100 * clamp(1 - node.template_inflight / 8, 0.2, 1)
                  : (node.zone_has_template ? 40 : 0)
          - name: expr_score
            weight: 1.0
            config: { expr: '100 - node.mvm_ratio * 100' }

    # ── 策略 C：大规格长驻，提装箱率 ─────────────────────────────
    - name: bin-packing
      priority_select_num: 1            # 严格贪心
      score:
        normalize: true
        plugins:
          - name: expr_score
            weight: 1.0
            config:
              expr: '(node.cpu_alloc_ratio_after * 0.6 + node.mem_alloc_ratio_after * 0.4) * 100'

    # ── 策略 D：混合规格抗碎片 ───────────────────────────────────
    - name: balanced-mixed
      score:
        normalize: true
        plugins:
          - name: expr_score
            weight: 2.0
            config:
              expr: '(node.cpu_alloc_ratio_after * 0.5 + node.mem_alloc_ratio_after * 0.5) * 100'
          - name: expr_score
            weight: 1.0
            config:
              expr: '100 * (1 - abs(node.cpu_alloc_ratio_after - node.mem_alloc_ratio_after))'

  profile_selector:
    default: default
    by_instance_type:
      cubebox_gpu: bin-packing
    by_request_label:
      "cube.io/workload=burst":    burst-spread
      "cube.io/workload=template": template-affinity
      "cube.io/workload=longrun":  bin-packing
```

#### 适用场景与 trade-off

| 策略 | 适用 | 预期改善 | Trade-off |
|---|---|---|---|
| `burst-spread` | Agent 会话型沙箱，秒级到分钟级生命周期，创建 QPS 脉冲 | 羊群度 P95、调度失败率下降 | 模板命中率下降，故保留 0.5 权重的弱亲和 |
| `template-affinity` | 少量模板反复创建，冷启动是主要成本 | 模板命中率、端到端创建延迟 P95 改善 | 装箱率与均衡度可能下降 |
| `bin-packing` | 数小时到数天的长驻沙箱、大规格实例 | 装箱率提升 | 单节点故障爆炸半径变大，突发下更易触碰 `NodeMaxCpuUtil` |
| `balanced-mixed` | 混合规格，CPU/内存消耗比例不一 | 碎片率下降 | 极端装箱率略低于纯 bin-packing |

三者互为 trade-off（装箱 ↔ 均衡 ↔ 亲和），报告必须同时列出，不得只报好看的那项。

#### 支撑这些表达式所需的 Go 侧工作

- `node.inflight` / `inflight_ratio` / `cpu_alloc_ratio_after`：需要 **assume 乐观预留**。
  `Select()` 返回前把本次请求的 CPU/内存/沙箱数记入进程内 TTL 计数器，
  创建成功（心跳带回真实 usage）或失败/超时（TTL，默认 20s）后释放。
  这是消除羊群效应最有效的一招——当前从 `Select()` 返回到 `QuotaCpuUsage` 更新之间
  有可观延迟（经 Redis 与心跳），同一毫秒级窗口内的并发请求看到同一份陈旧快照。
  **边界必须写明**：多 master 副本下预留只在本副本生效，跨副本仍依赖 Redis 的
  `RealTimeCreateNum` 兜底；TTL 到期强制释放，避免创建失败时资源泄漏。
- `node.template_inflight`：按模板分组的在途创建计数器。
- `node.zone_has_template`：模板副本的 zone 级索引。
- `cluster.*`：每次调度算一次的集群聚合量。

### L8 文档与示例

| 文档 | 内容 |
|---|---|
| `docs/dev/scheduler-context-schema.md` | **调度上下文视图字段清单与精确语义**（对外 API，优先级最高） |
| `docs/guide/scheduler-profiles.md` | Profile 配置、表达式语法、4 种内置策略的场景与完整示例 |
| `docs/dev/scheduler-extender.md` | 子进程协议、状态流、握手、超时熔断语义、参考实现 |
| `docs/dev/scheduler-metrics.md` | 指标定义：公式、采集点、正常区间、异常处置 |
| `docs/guide/scheduler-benchmark.md` | 如何跑 benchmark、如何读报告、如何按报告调优 |

均补 `docs/zh/` 对应中文版（`docs-bilingual-check` 只强制
troubleshooting/usecases/integrations 三个目录，但 `docs/zh` 是全量镜像，保持一致）。

示例：`examples/scheduler-extender/`——一个注入 `tenant_sandbox_count` 的参考子进程
（Go 实现 + Dockerfile + k8s sidecar 片段 + 对应的表达式 profile），
用户照着改就能接自己的数据源。

新增 `cubemastercli sched explain --template tpl-x --cpu 2 --mem 4Gi`：
打印每个候选节点在每个变量上的取值、每个插件/子表达式的中间结果、以及被谁淘汰。
自定义逻辑放开后"为什么调度到这台"会变成高频问题，这个命令是**必须项**不是加分项。

---

## 5. 验收标准对照

| 验收项 | 落点 |
|---|---|
| 调度评估指标定义文档，benchmark 输出 ≥5 项指标 | L5（7 项）+ `docs/dev/scheduler-metrics.md` |
| 新插件可通过统一机制接入，配置文件切换 Profile | L2 表达式 + L3 Profile + L4 Extender |
| ≥3 种内置策略，每种有场景说明与配置示例 | L7（4 种，全表达式） |
| 用户自定义扩展的开发示例与文档 | `examples/scheduler-extender/` + `docs/dev/scheduler-extender.md`（**不需要重编译 CubeMaster**） |
| benchmark ≥3 种 workload，一键运行，生成对比报告 | L6（4 种）+ `make sched-bench` |
| 至少一项指标明显改善，trade-off 需说明 | L6 报告 + L7 trade-off 表 |
| 单元测试覆盖新增核心逻辑，通过代码规范检查 | 第 7 节 |

---

## 6. PR 拆分

按依赖顺序，每个可独立评审、独立回滚。

| PR | 内容 | 行为变化 |
|---|---|---|
| 1 | L1 调度上下文视图 Schema + 只读 `NodeView` + 按需计算调度 + 单测 | 无 |
| 2 | L0 框架加固：per-plugin 超时 / panic 隔离 / 熔断 / 归一化开关 / 权重钳制 | 仅"单插件故障不再拖垮整阶段" |
| 3 | L3 Profile 配置与路由 + 老配置回落 + 未知插件 fail-fast + 单测 + 文档 | 无（不写 `profiles` 时等价） |
| 4 | L5 调度 Prometheus 指标 + 指标定义文档 | 无（只加观测） |
| 5 | L6 仿真器 + `cubeschedbench` + 4 workload + 报告 + `localcache.InitInMemory` | 无（新增工具） |
| 6 | L2 表达式引擎（`expr_score` / `expr_filter`）+ 编译期校验 + `sched explain` + 文档 | 需显式配置才生效 |
| 7 | assume 乐观预留 + `template_inflight` / `zone_has_template` 等变量供给 | 需 profile 显式启用 |
| 8 | L7 四个内置策略 profile（纯 YAML）+ 单测 + 配置示例 | 需显式切换 profile |
| 9 | L4 Extender：proto + 状态流 + 托管子进程生命周期 + 超时熔断背压 + `node.ext.*` 接入 | 无（未配置即不加载） |
| 10 | 量化对比报告 + 调优建议 + `examples/scheduler-extender/` + 全部文档 | 无 |

若上游偏好大颗粒，可合并为四组：`1–2` 框架、`3–5` 配置与评估、`6–8` 表达式与策略、`9–10` 扩展与文档。

---

## 7. 测试计划

**单元测试**（表驱动，风格对齐 `template_locality_test.go` / `imagescore_test.go`）：

- 上下文视图：各 ratio 口径正确性（含/不含 overcommit、含/不含预留）、除零、容量为 0
- 按需计算：AST 标识符提取、只计算被引用变量、`cluster.*` 每次调度只算一次
- 表达式：编译失败拒绝生效并保留旧配置、类型错误、NaN/Inf/越界 clamp、panic 降级
- 框架加固：per-plugin 超时触发、panic 不影响其他插件、熔断开合、`fail_open` / `fail_closed` 语义
- 归一化：线性拉伸、全等分输入、缺失分量补齐、负分钳制
- Profile：解析、profile 级覆盖全局、路由优先级、**无 `profiles` 段的回落等价性**（关键回归保护）
- assume：预留生效、TTL 过期释放、创建失败回滚、并发安全（`-race`）
- Extender：握手版本协商、超时、熔断、背压、镜像 seq 落后触发重同步、`fail_open` 降级到基线
- 统计函数：CV / Jain / 分位数用固定输入断言精确值
- 仿真器 smoke：`-short` 下 10 节点 × 200 请求，断言成功率 100%，且**同种子两次运行结果完全一致**

**回归**：现有 `schedule_test.go`、`prefilter_test.go`、`selectcontext_test.go`、
`asyncscore_test.go`、`realtimescore_test.go`、`imagescore_test.go` 全部保持不变且通过。

**执行**（依据 `.claude/skills/run-dev/SKILL.md`）：

```bash
make builder-run BUILDER_CMD='cd /workspace/CubeMaster && make proto && \
  CI=true CUBE_MASTER_CONFIG_PATH=/workspace/CubeMaster/conf.yaml \
  go test -short -race ./pkg/scheduler/... ./pkg/selector/...'
make fmt
```

**CI 门禁**：`fmt-check`（gofmt）、`CubeMaster/.golangci.yaml`、`dco-check`。
按 `AGENTS.md`，AI Agent 不加 `Signed-off-by`，commit / PR 描述中加
`Assisted-by:` 或 `Autonomously-by:` 标记，DCO 由人类提交者补签。

---

## 8. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 归一化改变默认打分结果 | profile 级 `normalize` 开关，`default` 保持旧语义；用 benchmark 数据说服后再切 |
| 表达式改配置即全集群生效，无 code review 保护 | 加载时编译校验、`--dry-run` 对着仿真器出对比报告、按 instance type 灰度、配置版本号与一键回滚 |
| 用户误写表达式放宽安全约束 | guard 层 Go 写死不可关闭，表达式只能在其之上收紧；语义上 `候选集 = guard ∩ policy` |
| 变量口径歧义导致表达式语义错误 | 口径写进变量名（`_eff` / `_after`）；`sched explain` 打印中间值 |
| assume 在创建失败时泄漏预留 | TTL 强制释放 + 失败路径显式回滚 + `assume_pending` gauge 告警 |
| 多 master 副本下 assume 只在本副本生效 | 文档明确；Redis `RealTimeCreateNum` 硬过滤仍作全局兜底 |
| 子进程挂掉影响调度可用性 | 降级基线（不含 `node.ext.*` 的 profile）+ 熔断 + 背压；子进程不可用时调度必须继续 |
| 子进程状态镜像滞后 | `snapshot_seq` 感知 + 落后阈值触发全量重同步；接受最终一致 |
| 每次调度传全量节点导致 IPC 打爆 | 状态流 + 候选 ID 协议 + 字段裁剪 + 候选集裁剪 |
| `localcache` 包级单例被仿真器污染 | `InitInMemory` 仅在 `cubeschedbench` 与测试中调用，生产路径不引用 |
| 仿真结论与真实集群偏差 | 启动延迟分布参数从生产 Prometheus 直方图标定；报告标注"相对趋势可信、绝对值需实测校准" |
| `PrioritySelectNum` 代码默认 `-1` 与配置 `1` 不一致 | Profile 内显式声明，消除隐式默认；PR 3 文档中专门说明 |

---

## 9. 非目标

以下明确不在本次范围内，列入路线图：

- **动态热加载 Go 插件**（`.so` / plugin 包）——Go 的 plugin 机制版本耦合脆弱，
  用子进程和表达式已覆盖同类需求
- **WASM 插件**——虽然同样不需要重编译 CubeMaster 且延迟低于子进程（~100–300µs vs ~0.3–1ms），
  但它不能做 I/O（给 host function 就破坏隔离了），而用户选择子进程的场景多半正是要连外部系统；
  且需要用户搞 WASM 工具链和 ABI，上手成本高。优先级低于子进程
- **共享内存 / mmap 零拷贝状态同步**——需要双缓冲或 seqlock，复杂度陡增，
  先用状态流 + 候选 ID 协议，超大集群再评估
- **抢占与重调度**——现有架构无此概念，属于独立课题
- **对 Go SPI 做用户侧包装**（稳定 API 承诺、工程模板、版本兼容矩阵）——
  Go SPI 仅保留给上游维护者，不作为用户扩展方式对外宣传
- **`thirtparty` 过滤器**（当前为空壳，所有分支原样返回全部节点）——
  由 `expr_filter` 与 Extender filter 接管其位置后标记 deprecated
