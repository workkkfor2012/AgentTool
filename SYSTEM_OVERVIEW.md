# AgentTool 系统总览

这份文档是给下一次接手的主模型看的单文档入口。目标不是覆盖全部实现细节，而是让模型在一次阅读后，快速把握当前多 agent 架构、角色分层、通信协议、`agentd/agentctl` 用法、dashboard 定位，以及继续推进时应该先看什么。

## 1. 系统目标

AgentTool 是一层本地多 agent 编排与可视化控制平面，当前运行在 Windows 上，面向“主协调 agent + 执行 agent”的可见工作流。

当前阶段的核心目标只有三个：

- 让主 agent 与多个子 agent 的通信和任务流转有统一入口
- 让主从协作通过 `agentd` 的任务路由、状态记录和通信汇聚收口，而不是靠聊天文本和手工复制
- 让每个执行子 agent 只写自己的仓库，由主协调层负责分析、派单和跨仓协调

当前明确不是目标的内容：

- 不做历史回放产品
- 不展示 `agentd` 无法可靠判定的“内心状态”，例如“正在深度思考”
- 不把 `agentd` 做成重型自治调度器

## 1.1 当前使用形态

当前系统按两种使用形态理解最准确：

- 本地人工驾驶
  - 命令行是主操作面
  - Web 是辅助面板，可看状态、可补充控制，但不是必看
- 远程控制
  - Web 是主操作面
  - 命令行只是远端运行载体，本地操作者不直接看 pane

因此，当前 AgentTool 的真实定位是：

- `agentd` 是轻量路由、记账、通信汇聚中枢
- 人类操作者自己决定是否接管某个 agent、是否在命令行直接输入
- 系统默认服务于“人工驾驶 + agent 辅助执行”，而不是“全自动无人值守”

## 2. 当前拓扑

从协作语义上看，当前系统默认分两层：

- `orchestrator`：主协调层，对外唯一默认对话入口
- `executor`：执行层，负责各自仓库内的实现与反馈

当前默认可见布局是 5 个 pane：

- 1 个主 agent：`main`
- 4 个执行 agent：
  - `guardpro_backend_cloud`
  - `guardpro_backend_control`
  - `guardpro_factory`
  - `guardpro_control`

`advisor` 不再属于默认常驻拓扑，只保留为可选的人工升级工具。

当前 `agentd` 运行时原生角色仍然主要是两类：

- `main`
- `child`

也就是说，`orchestrator / executor` 目前是上层协作语义，不是 `agentd` 内核里的原生枚举。

## 3. 顶层设计原则

AgentTool 当前应按四层模型理解，而不是把所有“agent”概念混成一类：

- `agent identity`
  - 这是“谁在负责什么”，例如 `main` 或 `guardpro_control`
  - 应该是长期稳定的
- `task`
  - 这是“这一轮派给它什么工作”
  - 应该是按轮次创建、推进、关闭的异步工作单
- `runtime`
  - 这是承载 agent 的具体会话、窗口、bridge、thread、keeper
  - 可以被替换、重启、恢复
- `context`
  - 这是该 agent 持续保留的项目理解、职责边界、最近任务上下文
  - 应尽量延续，而不是每轮从零重灌

正式收敛后的设计结论是：

- `agent` 常驻
- `task` 异步
- `runtime` 可重启
- `context` 连续

这也是当前 AgentTool 与“纯异步任务系统”的关键区别。

## 4. 外部标杆与学习入口

如果要理解 AgentTool 为什么这样收敛，先看这份对照学习文档：

- [`REFERENCE_AGENT_LEARNINGS.md`](F:\work\github\AgentTool\REFERENCE_AGENT_LEARNINGS.md)

这份文档总结了从两套顶级 coding agent 实现里学到的东西：

- OpenAI Codex 官方源码
- Claude Code 源码

核心结论先写在这里：

- 从 Codex 学到的是：共享控制面、树状寻址、mailbox、轻量 wait、结构化完成通知
- 从 Claude Code 学到的是：任务对象化、continue/stop/resume、可见 worker 的 mailbox 派工、远程 worker 的 heartbeat
- AgentTool 不应照搬它们的全部实现，而应吸收对当前单机多 agent 场景真正有用的子集

## 5. 仓库与提示词分布

工作区根目录：

- `F:\work\github\hackman`

主协调提示词：

- [`MAIN_AGENT_PROMPT.md`](F:\work\github\hackman\MAIN_AGENT_PROMPT.md)

可选顾问提示词：

- [`THINKING_ADVISOR_HIGH_PROMPT.md`](F:\work\github\hackman\THINKING_ADVISOR_HIGH_PROMPT.md)
- [`THINKING_ADVISOR_XHIGH_PROMPT.md`](F:\work\github\hackman\THINKING_ADVISOR_XHIGH_PROMPT.md)

执行子 agent 提示词：

- [`SUBAGENT_PROMPT.md`](F:\work\github\hackman\guardpro_backend_cloud\SUBAGENT_PROMPT.md)
- [`SUBAGENT_PROMPT.md`](F:\work\github\hackman\guardpro_backend_control\SUBAGENT_PROMPT.md)
- [`SUBAGENT_PROMPT.md`](F:\work\github\hackman\guardpro_factory\SUBAGENT_PROMPT.md)
- [`SUBAGENT_PROMPT.md`](F:\work\github\hackman\guardpro_control\SUBAGENT_PROMPT.md)

Repo-local 工作记录：

- [`work.md`](F:\work\github\hackman\guardpro_backend_cloud\work.md)
- [`work.md`](F:\work\github\hackman\guardpro_backend_control\work.md)
- [`work.md`](F:\work\github\hackman\guardpro_factory\work.md)
- [`work.md`](F:\work\github\hackman\guardpro_control\work.md)

重要说明：

- `latest_reply.md` 已经不是当前系统的一部分，不再作为必写协议
- 当前实时通信真相在 `agentd` 内存和 SQLite，不在 repo 文件里
- `work.md` 只是 repo-local 辅助上下文，不是主从通信主通道

## 6. 建议阅读顺序

下一次模型接手时，建议按这个顺序读：

1. [`SYSTEM_OVERVIEW.md`](F:\work\github\AgentTool\SYSTEM_OVERVIEW.md)
2. [`REFERENCE_AGENT_LEARNINGS.md`](F:\work\github\AgentTool\REFERENCE_AGENT_LEARNINGS.md)
3. [`OPERATIONS_MANUAL.md`](F:\work\github\AgentTool\OPERATIONS_MANUAL.md)
4. [`ROLE_CONTRACT.md`](F:\work\github\AgentTool\ROLE_CONTRACT.md)
5. 自己对应角色的提示词文件
6. 自己对应仓库下的 `work.md`
7. 当前运行态：先看 `agentctl overview`，再按需看 `agentctl agent-context --agent <name>` / `agentctl task-context --task <id>` / `agentctl trace --agent <name>`

顺序原则是：

- 先拿系统模型
- 再拿外部标杆学习结论
- 再拿操作方式
- 再拿角色契约
- 最后才钻进具体 repo 和运行态

补充说明：

- `agentctl overview` 当前优先展示 `runtime / bootstrap / transport / task / context`，不再把 pane、窗口心跳这类底层实现细节放在最前面
- `agentctl agent-context` 与 `task-context` 现在会直接返回 `context_sources`，用于明确该 agent 当前长期上下文到底来自哪些 repo-local 文件

## 7. 状态模型

当前系统应按三层状态理解：

### 7.1 Runtime State

这一层是 `agentd` 直接可观察、可硬保证的事实状态，例如：

- agent 是否注册
- bridge 是否连接
- session 是否附着
- 当前是否有 in-flight task
- agent 当前是否 `idle / busy / blocked / offline`

这一层由 `agentd` 负责，不依赖模型自由文本。

### 7.2 Bootstrap State

这一层是初始化状态，当前主要有：

- `awaiting_init`
- `ready`

这一层已经做了硬化：

- 模型仍然负责读取角色提示词并完成初始化总结
- 但 `ready` 落库不再依赖模型自己手敲 `agentready`
- 外层 shell/managed bootstrap 在收到合格 payload 后自动 `mark-agent-ready`

原则是：

- 初始化语义由模型完成
- 初始化状态由 `agentd/shell` 收口确认

### 7.3 Task Semantic State

这一层是任务语义状态，主要由 agent 按协议上报，`agentd` 负责校验、落库和状态流转。

关键 round 语义值：

- `result`
- `report`
- `wait_decision`

对应 task 主状态机可进入：

- `pending`
- `accepted`
- `running`
- `reported`
- `blocked_waiting_decision`
- `decision_sent`
- `closed`
- `failed`
- `cancelled`

原则是：

- 语义判断由模型给出
- 状态机迁移由 `agentd` 决定

## 8. 通信模型

当前主从通信主通道不是 repo 文件，而是 `agentd` 里的任务、决策、snapshot、bridge 事件。

也就是说：

- 主 agent 派任务，用 `task`
- 子 agent 回报一轮结果，用 task round payload
- 主 agent 拍板，用 `decision`
- dashboard 看的是 `agentd` runtime snapshot 和流事件

对 AgentTool 的当前目标形态，应这样理解：

- 主 agent 与子 agent 之间需要的是结构化 mailbox / task channel
- 不应该把“窗口里能看到什么”当成主通信真相
- 可见窗口是视图层，不是协议层
- 当操作者直接在本地命令行里与某个 agent 对话时，这属于人工控制通道，不要求 `agentd` 对该轮语义做强约束解释

## 9. 角色边界

角色契约详见 [`ROLE_CONTRACT.md`](F:\work\github\AgentTool\ROLE_CONTRACT.md)。

这里给最短版：

- 人默认只与 `orchestrator` 对话
- 默认常驻的只有 `orchestrator + 4 executors`
- `executor` 只写自己的仓库，可以读其他仓库
- 跨仓改动由 `orchestrator` 再派给目标 `executor`
- 同一 `executor` 同时只允许 1 个 active task

当前约定下：

- `main` 读全局，默认不直接改 `guardpro_*` 业务代码
- 四个执行子 agent 各自只写自己的 repo
- 默认通过 richer task packet 提高单轮吞吐，而不是通过常驻思考层增加链路长度
- 本地模式下，操作者可以直接与主 agent 或某个执行 agent 对话；是否插手、何时插手，由操作者自己负责把控

## 10. 启动入口

执行层 2x2 窗口：

- [`Launch-AgentToolVisibleLayout.cmd`](F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.cmd)
- [`Launch-AgentToolVisibleLayout.ps1`](F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.ps1)

可选决策层 1+2 窗口：

- [`Launch-AgentToolDecisionLayout.cmd`](F:\work\github\AgentTool\scripts\Launch-AgentToolDecisionLayout.cmd)
- [`Launch-AgentToolDecisionLayout.ps1`](F:\work\github\AgentTool\scripts\Launch-AgentToolDecisionLayout.ps1)

总启动入口：

- [`Launch-AgentToolAll.cmd`](F:\work\github\AgentTool\scripts\Launch-AgentToolAll.cmd)
- [`Launch-AgentToolAll.ps1`](F:\work\github\AgentTool\scripts\Launch-AgentToolAll.ps1)

当前默认是两个 Windows Terminal 窗口承载 5 个 pane：一个主窗口 + 一个 2x2 执行端窗口。
`Launch-AgentToolDecisionLayout` 只保留为可选人工升级路径，不属于默认启动流。

## 11. agentd / agentctl

守护进程：

- [`agentd.exe`](F:\work\github\AgentTool\target\debug\agentd.exe)

CLI：

- [`agentctl.exe`](F:\work\github\AgentTool\target\debug\agentctl.exe)

高频命令可以分四类理解：

### 11.1 观察类

- `ping`
- `overview`
- `status`
- `trace --agent <name>`
- `trace --task <id>`
- `trace --session <id>`
- `main-inbox`
- `agent-context --agent <name>`
- `task-context --task <id>`

### 11.2 初始化与会话类

- `register-agent`
- `begin-agent-bootstrap`
- `mark-agent-ready`
- `touch-agent`
- `reset-agent-thread`
- `recover-agent`
- `stop-agent-session`
- `ensure-managed-session`

### 11.3 任务流转类

- `create-task`
- `run-task-round`
- `begin-visible-task`
- `submit-visible-task-round`
- `send-decision`
- `ack-decision`
- `close-task`
- `cancel-task`
- `retry-task`

### 11.4 运行修复类

- `repair-runtime-state`
- `cleanup-demo-data`
- `stop-managed-sessions`
- `stop-visible-panes`

## 12. Dashboard 定位

dashboard 当前已经是可操作控制台，但它的定位要分场景理解：

- 本地模式：辅助面板
- 远程模式：主控制台

它应该主要展示：

- agent runtime 状态
- 当前 task / decision 流转
- 最近通信事件
- 哪些 agent 已 `ready`
- 哪些 agent 当前 `idle / busy / blocked / offline`

它当前也可以承担两类操作：

- 向某个 agent 直接发一轮受控消息
- 由当前 agent 生成并派发一条结构化任务给目标执行端

它不应该假装知道：

- 模型是不是“正在思考”
- 模型是不是“即将输出”
- 模型内部理由链路

原则仍然是：只展示可验证状态；不要把它设计成“读取模型心智”的花哨面板。
