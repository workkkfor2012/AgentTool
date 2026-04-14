# AgentTool 角色契约与任务流规则

本文档定义当前阶段在 AgentTool 中推荐采用的默认协作结构：`1 个 orchestrator + 4 个 executor`。目标不是把思考层做厚，而是把路由做薄、把执行吞吐做高。

## 1. 默认拓扑

- 人类操作者默认只与 `orchestrator` 对话。
- 默认常驻节点只有：
  - `main`
  - `guardpro_backend_cloud`
  - `guardpro_backend_control`
  - `guardpro_factory`
  - `guardpro_control`
- `advisor` 不再是默认常驻层，不属于默认启动拓扑。
- `advisor` 只保留为可选的临时人工升级路径，不参与日常派单主链。

补充说明：

- 这是默认协作拓扑，不是强封锁的人机边界。
- 在本地人工驾驶模式下，操作者可以直接与某个 `executor` 对话。
- 一旦操作者直接插手某个 `executor`，就不要再把该轮上下文当成完全由 `agentd` 自动编排的纯系统轮次。

## 2. 角色定义

### orchestrator

- 唯一职责是控制面：查询、路由、派单、汇总、收口。
- 默认推理强度应为 `medium`。
- 可以读取所有相关仓库与文档。
- 默认只写协调层文档、协议文档、提示词和任务说明。
- 默认不直接修改 `guardpro_*` 业务代码。
- 不做长链深度分析，不充当第二个执行 agent。
- 在本地模式下，它仍然是默认主入口；在远程模式下，它通常是 Web 控制台的默认对话目标。

### executor

- 负责自己仓库范围内的实现、验证和回报。
- 可以读取其他仓库，用于确认接口、调用链、协议和状态机边界。
- 只能写自己的仓库。
- 同一时间只处理 1 个 active task。
- 若判断其他仓库也必须改动，只能上报 `orchestrator`，不得跨仓直接落实现。
- 本地模式下，操作者可以直接与 `executor` 对话，但这属于人工接管，不属于纯自动派单闭环。

## 3. 默认工作模式

- 进度查询：`orchestrator` 直接读取运行态与上下文，不派单。
- 目标明确：`orchestrator` 直接创建任务并派给目标 `executor`。
- 同仓新增需求：优先判断能否合并进同一个 richer task packet；不能安全合并时，排队，不做同仓并发。
- 跨仓复杂需求：由 `orchestrator` 选择一个主责任 `executor`，扩大其读取范围；必要时提升该任务的 `effort`，但不默认引入常驻思考层。

## 4. Single Active Task 规则

- 每个 `executor` 同时只能有 1 个 active task。
- `agentd` 继续保持单 in-flight task 约束。
- 新来的同仓任务只能有三种处理方式：
  - 合并：目标一致、写入范围一致、验收标准一致时，合并成一个 richer task packet
  - 排队：当前 task 未结束且新需求不适合合并时，等待当前 task `closed`
- 中断：仅限 P0，且必须由 `orchestrator` 明确决策

补充说明：

- 这个规则约束的是 `agentd` 创建和路由的正式任务。
- 它不阻止操作者在本地命令行里临时与某个空闲 `executor` 直接对话。
- 是否插手、是否会干扰当前上下文，由操作者自己负责判断。

## 5. Rich Task Packet

默认任务包字段如下：

- `title`
- `summary`
- `effort`
- `read_scope`
- `write_scope`
- `acceptance`

字段含义：

- `title`：一句话任务标题
- `summary`：本轮要完成的目标摘要，可包含多个相关子项
- `effort`：本轮建议投入的推理力度，常用值如 `medium` / `high` / `xhigh`
- `read_scope`：允许或建议读取的仓库、目录、文件
- `write_scope`：允许修改的范围，默认应只包含本仓路径
- `acceptance`：验收标准、测试要求、输出要求

原则：

- rich task packet 用来减少来回追问，提升单轮吞吐。
- rich task packet 不等于跨仓写权限。
- 同一个 packet 可以包含多个相关子项，但必须共享同一目标和同一写入边界。

## 6. 任务流规则

标准流转如下：

1. 人类向 `orchestrator` 提需求。
2. `orchestrator` 判断这是进度查询、直接派单、合并已有任务，还是排队等待。
3. 若只是进度查询：直接读取 `agent-context` / `main-inbox` / `task-context` / `work.md`，不派单。
4. 若目标明确：`orchestrator` 创建 richer task packet 并派给目标 `executor`。
5. `executor` 在本仓内实现、验证并回报。
6. `orchestrator` 基于反馈做继续、收口、排队下一个任务或派发到另一仓。

在当前阶段，还要补一条现实规则：

7. 如果操作者直接接管某个 `executor`，则该轮以人工驾驶为准；`agentd` 仍可记录运行态，但不负责为这轮人工对话提供强语义仲裁。

## 7. 可选升级路径

- 默认不保留常驻 `advisor`。
- 只有当人类明确要求，或出现重大架构分歧且当前 `executor` 无法收敛时，才允许临时拉起分析工位。
- 默认优先级不是“先开 advisor”，而是“先选责任 executor，再提升该任务的 `effort` 或扩大 `read_scope`”。

## 8. 权限边界

- `orchestrator`：读全局，写协调层，默认不改业务代码。
- `executor`：写本仓库，读其他仓库。

## 9. 当前实现约束

- 当前 `agentd` 运行时角色仍是 `main / child`，尚未原生扩展为 `orchestrator / executor` 枚举。
- 默认总启动器现在应收敛为 `main + 4 executors` 的 5 pane 拓扑。
- `Launch-AgentToolDecisionLayout` 可以保留为可选人工工具，但不再属于默认路径。
- `agentd` 当前定位应理解为：任务路由中心、状态记录中心、通信汇聚中心，而不是重型自治调度器。
