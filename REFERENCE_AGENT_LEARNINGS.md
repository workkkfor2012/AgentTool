# AgentTool 参考实现学习与设计决策

这份文档回答两个问题：

1. 我们能从 Codex 官方和 Claude Code 学到什么
2. AgentTool 应该吸收什么，不应该照搬什么

结论先写在前面：

- Codex 更像“共享控制面 + 树状会话 + mailbox”
- Claude Code 更像“任务系统 + worker backend + 通知回灌”
- AgentTool 当前最适合走“常驻 agent + 异步 task + 可重启 runtime + 结构化事件回报”

## 1. 两套参考实现的核心气质

### 1.1 Codex 官方

Codex 的 subagent 更像一个根会话树里的共享控制面：

- 用 `AgentControl` 统一管理 spawn、发消息、等待、恢复
- 父子关系落在线程元数据里
- agent 之间通信走 mailbox
- `wait_agent` 只等待邮箱变化，不直接承担“取结果”
- 子 agent 完成后，通过结构化通知回到父 agent

它的重心是：

- 干净的会话树
- 明确的 agent 寻址
- 轻量的事件等待
- 结构化完成通知

### 1.2 Claude Code

Claude Code 的 subagent 不止一种：

- 普通异步子 agent
- team / teammate worker
- 远程 CCR worker

它的重心不是纯粹的多 agent 内核，而是：

- 一切先变成任务对象
- 本地 worker、可见 pane worker、远程 worker 都可以被统一调度
- 结果统一通过 `<task-notification>` 回灌
- 同一个 worker 可以继续、停止、恢复

它更像一个产品级 worker 编排层。

## 2. 关键分歧：子 agent 是常驻，还是只是异步任务

这是最重要的设计岔路。

正确的拆法不是二选一，而是拆成四层：

- `agent`
  - 长期身份
  - 负责某个固定职责
- `task`
  - 某一轮具体工作单
  - 可以创建、推进、关闭
- `runtime`
  - 会话、窗口、bridge、keeper、thread
  - 可以挂掉、恢复、替换
- `context`
  - 项目理解、最近边界、角色约束
  - 应尽量保留

因此 AgentTool 的设计决策不是：

- “要常驻 agent”
- 或者“要异步任务”

而是：

- `agent` 常驻
- `task` 异步
- `runtime` 可重启
- `context` 连续

## 3. 设计决策表

| 设计问题 | Codex 官方倾向 | Claude Code 倾向 | AgentTool 决策 | 原因 |
|---|---|---|---|---|
| 子 agent 身份 | 树状、长期存在 | 普通 subagent 偏任务，teammate 偏常驻 worker | 采用常驻身份 | 你要的是固定 7 个节点，不是每轮临时 spawn |
| 调度对象 | 会话树中的 agent | AppState/task 中的 worker 任务 | agent + task 双层 | 只做 agent 不够细，只做 task 会丢角色 |
| 通信模型 | mailbox + 结构化通知 | mailbox + task-notification | 采用 mailbox + task event | 兼顾实时性和状态机收口 |
| 等待模型 | wait 只等事件变化 | 等通知入主线程 | 采用轻量 wait | 不把“等待”做成结果读取接口 |
| 完成回报 | 子 agent 主动通知父 agent | task-notification 回灌 | 采用结构化完成/阻塞回报 | UI 和主 agent 都不应靠猜 |
| 可见 worker 派工 | 官方源码不是窗口产品主路径 | pane worker 明确走 mailbox，不靠终端注入 | 采用 mailbox 派工 | 更稳，也更接近你要的“窗口只作视图层” |
| 远程 worker 保活 | 不强调独立 heartbeat worker | CCR worker 有 heartbeat + epoch | 只给长期 bridge/远程 worker 上 heartbeat | 本地普通 agent 没必要加重协议 |
| 上下文策略 | 支持 fork none/all/last N turns | 继续旧 worker 或重新 spawn fresh worker | 角色级上下文常驻，任务级输入压缩 | 你担忧的是主上下文失控，不是执行端没上下文 |
| 状态展示 | 更偏结构化会话状态 | 更偏任务状态和通知状态 | 展示 agent runtime + task semantic | 这正好匹配你的 dashboard 目标 |
| stop / resume | 存在恢复能力 | continue/stop/resume 很明确 | 需要做成一等能力 | 长期可用系统必须能收敛异常任务 |

## 4. 从 Codex 学到的东西

### 4.1 共享控制面优先于“agent 两两直连”

不要把多 agent 第一反应做成“每个 agent 一个 daemon，然后互连”。

更稳的做法是：

- 有一个中心控制面
- 统一保存 agent 注册表
- 统一保存父子边与状态
- 统一处理消息投递、等待和恢复

这正适合 AgentTool 当前的 `agentd`。

### 4.2 agent 要有规范化寻址

只用中文显示名不够。

需要稳定的内部身份，例如：

- `main`
- `advisor_high`
- `advisor_xhigh`
- `guardpro_control`

如果未来支持更深层级，可以继续扩展为树状 path。

### 4.3 wait 要轻，结果靠通知回来

主协调端不应该不停轮询“子 agent 的完整输出”。

更稳的设计是：

- wait 只等 mailbox / event 更新
- 真正的结果通过结构化事件回到主协调端

这样状态机更清楚，UI 也更容易做。

### 4.4 完成通知必须结构化

“子 agent 看起来像做完了”不算完成。

必须有结构化回报，例如：

- `done`
- `blocked`
- `need_decision`
- `failed`

然后由 `agentd` 落状态机。

## 5. 从 Claude Code 学到的东西

### 5.1 一切都应该先变成任务对象

Claude Code 很强的一点是：普通 agent、远程 agent、worker，都被包进任务生命周期。

这给 AgentTool 的启发是：

- dashboard 主视图看的是 agent
- 但内部状态机一定要有 task object

否则你没法清楚表达：

- 这是谁
- 这一轮做什么
- 现在做到哪
- 是否等待主协调拍板

### 5.2 continue / stop / resume 必须是一等能力

真正长期运行的系统里，不可能每次出错都靠人工重开。

必须有：

- 继续同一 agent
- 停止当前任务
- 恢复到可用状态

Claude Code 在这方面做得比 Codex 产品化得更彻底。

### 5.3 可见 worker 不应该靠终端注入文本来派工

Claude Code 的 pane worker 是：

- 先启动 worker CLI
- 再通过 mailbox 发第一条任务
- 后续继续走 mailbox

这点对 AgentTool 很重要。

因为你的目标不是“用户在子窗口里手动盯着输入输出”，而是：

- 用户只主要和主 agent 对话
- 子 agent 对用户透明
- 子窗口可见，但只是观察面板

### 5.4 heartbeat 只该用于真正的 worker 生命周期

Claude Code 的 CCR worker 会：

- 初始化注册 worker 状态
- 定时 heartbeat
- 用 epoch 防止旧 worker 抢状态

这告诉我们：

- 不是所有 agent 都需要复杂保活协议
- 只有真正长期在线、可能断线重连、可能被替换的 runtime，才需要 heartbeat / epoch 这一层

## 6. AgentTool 应该吸收什么

当前建议正式吸收以下内容：

### 6.1 长期保留的 agent endpoint

固定节点长期存在：

- `main`
- `advisor_high`
- `advisor_xhigh`
- 四个执行 agent

每个节点都应有稳定身份、角色、仓库、提示词、最近上下文摘要。

### 6.2 每轮工作作为 task/round

每一轮协作都应落在 task 上，而不是直接覆盖 agent 状态。

一个 task 至少应包含：

- `task_id`
- `from_agent`
- `to_agent`
- `summary`
- `status`
- `current_round`
- `latest_report`
- `latest_decision`

### 6.3 mailbox / event channel

主协调与子 agent 之间应通过统一事件通道通信，至少支持：

- `task_assigned`
- `task_round_reported`
- `decision_sent`
- `task_closed`
- `task_failed`

### 6.4 运行态与语义态分层

`agentd` 硬保证的应是：

- 窗口/bridge/session 是否在线
- 当前 thread 是否附着
- 是否 ready

模型协议上报的应是：

- 我完成了
- 我被阻塞了
- 我需要主协调拍板
- 我失败了

### 6.5 dashboard 继续保持 read-only

当前 dashboard 的正确定位不是“控制台替代品”，而是：

- 实时 agent 状态面板
- 当前任务流转面板
- 事件流可视化面板

## 7. AgentTool 不该照搬什么

### 7.1 不照搬 Claude Code 的文件 mailbox

文件 inbox 很实用，但更像工程折中。

AgentTool 当前更适合：

- `agentd` 内存态为真相
- SQLite 做兜底
- bridge / websocket / control socket 做投递

### 7.2 不照搬 Codex 的完整线程树复杂度

Codex 的会话树非常干净，但对当前 AgentTool 来说，没必要一开始就上完整深层子树恢复。

当前更重要的是先把这 7 个固定节点做稳。

### 7.3 不把“窗口状态”误当成“agent 语义状态”

窗口活着，只说明 runtime 活着。

不说明：

- agent 是否看到了任务
- agent 是否真正开始执行
- agent 是否已经形成结论

因此 UI 不能靠窗口推语义。

## 8. 对 AgentTool 的正式收敛结论

AgentTool 下一阶段的正式模型应是：

- `Agent`
  - 长期存在的职责端点
- `Task`
  - 派给某个 agent 的一轮工作单
- `Round`
  - 该 task 下的一次来回反馈
- `Runtime`
  - 承载该 agent 的窗口、session、bridge、thread

一句话定义：

`Agent = long-lived endpoint, Task = per-round work item, Runtime = replaceable carrier`

## 9. 对当前实现的直接指导

如果继续推进 AgentTool，优先级应是：

1. 固化这 7 个长期 agent 的身份模型
2. 继续把通信收敛到 task / round / decision 事件
3. 把 bridge 视为 runtime 层，而不是业务语义层
4. 让 dashboard 只显示可验证状态
5. 把 continue / stop / recover 做稳

这样系统会更接近“工业级单机多 agent 控制平面”，而不是“多个可见终端的聊天拼装器”。
