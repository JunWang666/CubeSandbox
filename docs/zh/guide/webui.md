---
title: WebUI 控制台
---

# WebUI 控制台

Cube Sandbox **Dashboard（控制台）** 是一个内置的网页界面，让你在浏览器里就能看清集群里跑着什么、管理沙箱、构建模板、检查节点健康——不用敲一行 CLI。

> ⏱ 大约 3 分钟读完。读完之后，你就能在笔记本上把控一个集群。

## 1. 在哪里打开？

Dashboard 是一个静态前端，由 **控制节点** 上的 nginx 容器托管。

| 部署方式 | 访问地址 | 说明 |
| --- | --- | --- |
| 一键部署 / 多机集群 | `http://<控制节点IP>:12088` | 默认端口，可通过 `WEB_UI_HOST_PORT` 修改 |
| 裸金属 / 物理机部署 | `http://<服务器IP>:12088` | 同样使用 12088 |
| 本地开发 | `http://localhost:5173` | Vite 开发服务器，自动代理 `/cubeapi` 到 `127.0.0.1:3000` |

::: tip 记住 12088，不是 3000
`3000` 端口是 E2B 兼容的 REST API（CubeAPI），`12088` 是给人用的 Dashboard。Dashboard 在内部会通过同源前缀 `/cubeapi/v1` 调用 CubeAPI，所以你只需要打开 `12088` 这一个端口。
:::

如果你不知道控制节点的 IP，可以在服务器上跑 `ip -4 addr`，或者在同网段下直接访问 `http://<主机名>:12088`。

## 2. 一眼看完侧边栏

所有功能都在左侧栏的 11 个图标后面。鼠标悬停会显示名字。

| # | 图标 | 页面 | 用途 |
| --- | --- | --- | --- |
| 1 | 📊 | **Overview（概览）** | 集群关键指标：运行中沙箱数、CPU/内存使用率、健康节点数 |
| 2 | 📦 | **Sandboxes（沙箱）** | 所有 micro-VM 的实时列表，支持暂停 / 恢复 / 销毁 |
| 3 | 🧩 | **Templates（模板）** | 可复用的沙箱快照目录，支持从 OCI 镜像创建新模板 |
| 4 | 🖥️ | **Nodes（节点）** | 集群健康：每台宿主机的 CPU、内存、可用槽位 |
| 5 | 🧬 | **Versions（版本矩阵）** | 跨节点的组件版本分布（内核、agent、guest 镜像） |
| 6 | 🌐 | **Network（网络）** | API 网关配置与每节点速率限制 |
| 7 | 📈 | **Observability（可观测性）** | 运行时状态、沙箱健康、模板构建总览 |
| 8 | 🔑 | **API Keys（API 密钥）** | 存储 Dashboard 请求使用的 `X-API-Key` |
| 9 | 🏪 | **Template Store（模板商店）** | 安装官方预置镜像，一键生成模板 |
| 10 | 🤖 | **AgentHub（智能体中心）** | 在 Cube Sandbox 上招募并管理 AI 智能体实例 |
| 11 | ⚙️ | **Settings（设置）** | 主题、语言、集群信息、键盘快捷键 |

::: tip 新用户？从 **Overview** 开始。
它把最重要的信息聚在同一屏上，并且会自动刷新。
:::

## 3. 头三件你一定会做的事

### 3.1 检查集群是否健康

打开 **Overview**（`/`）。你应该看到四张偏绿色的 KPI 卡片：

- **Running Sandboxes** — 当前活跃的 micro-VM 数量
- **CPU / Memory Utilization** — 整集群的压力
- **Healthy Nodes** — `N/M` 个节点处于 `Ready` 状态

如果哪项数字飘红，点进 **Nodes** 看是哪个宿主出了问题。

### 3.2 创建一个沙箱

1. 点左侧栏的 **Sandboxes**，再点右上角 **+ New sandbox**。
2. 在网格里挑一个模板。标记为 `STALE` 的不可用——选 `READY` 的。
3. （可选）填几对 `meta` 键值对作为标签。
4. 点 **Create**。几秒内你就会被跳转到该沙箱的详情页，能看到日志在实时滚动。

要停掉一个沙箱，去 **Sandboxes** 列表，找到对应行，点最右边的暂停 / 销毁按钮。

### 3.3 配置 API Key（仅在开启鉴权时需要）

如果你的部署开启了鉴权，Dashboard 必须在请求里带上 API Key，否则所有请求都会失败。

1. 点左侧栏的 **API Keys**。
2. 把 Key（形如 `sk-cube-…`）粘进输入框。
3. 点 **Save**。Key 会保存在浏览器的 `localStorage.cube.apiKey`，Dashboard 之后每次请求都会自动带上 `X-API-Key` 请求头。

::: details 这个 Key 从哪来？
开启鉴权的人会生成它。完整流程见 [鉴权](./authentication.md)。
:::

## 4. Web 终端

Dashboard 内置了 **交互式 Web 终端**，你可以直接从浏览器进入运行中沙箱的 Shell——不需要 SDK，也不需要 SSH。

### 4.1 打开终端

有两个入口，只有在沙箱处于 **运行中（running）** 状态时可用（鼠标悬停按钮会提示原因）：

- **沙箱详情页** — 页头的 **Open Terminal** 按钮。
- **Sandboxes 列表** — 行操作里的终端图标。

<!-- TODO: screenshot: terminal dialog -->
![Web 终端弹窗](../assets/webui-terminal.png)

### 4.2 终端能力

弹窗是一个完整的 [xterm.js](https://xtermjs.org/) 终端，在沙箱内以 **root** 身份运行 `/bin/bash` 登录 Shell：

- ANSI 颜色与光标控制（vim、htop 等都能正常用）
- 复制 / 粘贴——`Ctrl+Shift+V` 或右键粘贴；选中文字即原生复制
- 滚动回退、窗口大小同步、全屏切换、字体大小 `+`/`-` 调节

### 4.3 会话管理、重连与空闲超时

- **多会话** — 每次打开终端都会启动独立 Shell；不同沙箱的会话可以并存。
- **单沙箱会话上限** — 每个沙箱最多同时容纳 8 个终端会话，超出的连接会被拒绝。可通过 CubeAPI 环境变量 `TERMINAL_MAX_SESSIONS_PER_SANDBOX` 调整（默认 `8`）。
- **重连** — 异常断开、Shell 退出或出错时，弹窗会显示明确状态并提供 **Reconnect（重连）** 按钮。
- **空闲超时** — 30 分钟既没有任何输入也没有任何 Shell 输出的会话会被服务端终止；任何活动都会重置计时器，所以 `tail -f`、长时间构建等持续输出会话不会因没敲键盘而被断开。可通过 CubeAPI 环境变量 `TERMINAL_IDLE_TIMEOUT_SECS` 调整（单位秒，默认 `1800`）。
- **传输限制** — 客户端 WebSocket 消息上限为 64 KiB；服务端写入带有 10 秒超时。

### 4.4 工作原理

浏览器 xterm.js ⇄ WSS ⇄ CubeAPI（`GET /cubeapi/v1/sandboxes/{sandboxID}/terminal/ws`）⇄ CubeProxy ⇄ `envd`（端口 `49983`），由 envd 在沙箱内托管 PTY。部署启用 TLS 时，传输链路复用同一套 HTTPS/WSS 加密。Shell 只是 **沙箱内的 root**——与 SDK `exec` 的权限边界一致——并不能借此访问宿主机。

### 4.5 鉴权与审计

- **开启鉴权时**（auth callback / WebUI 会话登录）：终端要求相同的登录会话，未授权用户会收到 `401`，无法建立会话。详见 [鉴权](./authentication.md)。注意在 WebUI 会话模式下，终端端点是**自行强制**校验会话的——目前沙箱的其他 REST API（pause/resume/kill 等）在该模式下并未做会话保护，终端比它们更严格。会话凭证通过 `Sec-WebSocket-Protocol` 子协议头（`cube-terminal.<token>`）传输，而不是 URL——令牌不会出现在 URL、服务端访问日志或浏览器历史记录中。非浏览器 API 客户端也可以改用 `token` 查询参数，但请注意查询参数中的令牌可能落入日志。
- **Origin 校验** — CubeAPI 会拒绝 `Origin` 主机与请求主机不一致的 WebSocket 升级请求，跨源的浏览器连接将收到 `403`。
- **未开启鉴权（开放模式）**：任何能访问 Dashboard 的人都能对任意运行中的沙箱打开终端——请据此控制 Dashboard 的访问范围。
- **审计** — CubeAPI 会记录会话打开 / 关闭 / 超时事件，包含时间戳、用户身份（可用时）、客户端 IP、沙箱 ID 和 Shell PID。被拒绝的尝试（令牌错误、Origin 不匹配、沙箱不存在或未运行、超出会话上限）同样会被审计，并记录原因和客户端 IP。

### 4.6 已知限制

- 创建时限制了公网访问的沙箱（`allowPublicTraffic=false` / 流量访问令牌）无法使用 Web 终端——流量令牌在创建后不可恢复，弹窗会显示连接错误。
- 终端访问只做用户身份认证，**不做**按沙箱的授权——任何已认证用户都能对任意沙箱打开终端。这与当前沙箱 API 的权限模型一致（尚无按用户的沙箱归属），已纳入未来的多租户工作。
- 沙箱镜像必须包含 `envd`（所有标准模板都已包含）。
- 多容器沙箱：Shell 落在沙箱的默认环境中；要进入特定容器，请使用沙箱内的常规工具（如 `docker exec`）。

## 5. 键盘快捷键

Dashboard 对键盘很友好。最常用的三个：

| 按键 | 作用 |
| --- | --- |
| `⌘ K` / `Ctrl K` | 打开 **Command Palette**——输入页面名直接跳转 |
| `?` | 打开 **Settings → Shortcuts**（应用内查看完整快捷键列表） |
| `R` | 刷新所有可见数据面板 |
| `Esc` | 关闭弹窗或 Command Palette |

## 6. 个性化

打开左侧栏的 **Settings**：

- **Appearance → Theme** — 浅色 / 深色 / 跟随系统
- **Appearance → Language** — English 或 简体中文
- **Cluster** — 只读展示 CubeAPI 端点、沙箱域名、默认实例类型、速率限制、鉴权开关

顶栏右上角和 ⌘K 输入框里也有同样的快捷开关。

## 7. 常见问题

**为什么还要单独做个 Dashboard，不能直接用 curl 吗？**
绝大多数操作（从镜像创建模板、看版本矩阵、排查节点）在 UI 里更容易发现和理解。Dashboard 本质上只是 CubeAPI 的一个轻量客户端——每个页面背后都是一次 `/cubeapi/v1/*` 请求，这跟 E2B SDK、`curl` 调的是同一个 E2B 兼容 REST API。

**Dashboard 会保存我的数据吗？**
只会在浏览器里保存一样东西：`localStorage.cube.apiKey` 里的 API Key。其他所有状态（模板、沙箱、日志）都在集群上。

**能改端口吗？**
可以——在 `.env` 里设 `WEB_UI_HOST_PORT`，然后重跑 `install.sh`。改动会在下次启动 `cube-sandbox-webui.service` 时生效。

**能关掉 Dashboard 吗？**
可以——把 `.env` 里的 `WEB_UI_ENABLE` 设成 `0`（或不设）。集群照常运行，只是不再提供 WebUI；`3000` 端口的 E2B 兼容 API 不受影响。

**Dashboard 是开源的吗？我能自己构建吗？**
可以——它在仓库的 `web/` 目录里，用 Vite + React + TypeScript + Tailwind 构建。详见 [本地构建部署](./self-build-deploy.md) 和 [`web/README.md`](https://github.com/TencentCloud/CubeSandbox/blob/master/web/README.md)。

## 8. 下一步

- [快速开始](./quickstart.md) — 如果你还没安装，几分钟到能跑的 Dashboard
- [服务管理与日志](./service-management.md) — 如何启停 / 重启 `cube-sandbox-webui.service` 容器
- [鉴权](./authentication.md) — 还没开启 API Key？这里有完整步骤
- [HTTPS 证书与域名解析](./https-and-domain.md) — 给 Dashboard 加 TLS
- [架构概览](../architecture/overview.md) — 了解 Dashboard 背后的 CubeAPI / CubeMaster / Cubelet 怎么协作
