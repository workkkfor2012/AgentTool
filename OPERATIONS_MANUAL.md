# AgentTool 操作手册

这份手册面向实际操作，不讲架构原理，重点回答“怎么启动、怎么观察、怎么派单、怎么回报、网页上怎么看”。

## 0. 当前推荐使用方式

- 本地使用：命令行为主，Web 为辅。
- 远程使用：Web 为主，命令行只是远端运行载体。
- `agentd` 当前主要负责任务路由、状态记录、通信汇聚，不负责替操作者做复杂仲裁。

## 1. 启动

完整默认启动：

```powershell
F:\work\github\AgentTool\scripts\Launch-AgentToolAll.cmd
```

这会启动：

- 1 个主 agent pane
- 4 个 executor pane

默认行为：

- 如 `agentd` 未运行，则自动拉起
- 自动注册当前 5 个默认 agent
- 每个 pane 默认直接启动 `mycodex` / `codex`
- 空参数启动时先通过隐藏 websocket bootstrap 完成初始化，再自动进入 `ready`
- 默认总启动器现在直接拉起可交互的主 agent 和 4 个可交互 executor pane，而不是只读 viewer

只启动执行层 4 个 pane：

```powershell
F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.cmd -SkipMainWindow
```

只启动决策层 1+2 pane：

```powershell
F:\work\github\AgentTool\scripts\Launch-AgentToolDecisionLayout.cmd
```

## 2. 启动后预期

正常情况下，每个 pane 启动后会经历：

1. `begin-agent-bootstrap`
2. 隐藏 `app-server` websocket bootstrap
3. 自动 `mark-agent-ready`
4. 如果 bootstrap 线程不保活，可以直接清洗 bootstrap 用户消息对应的 rollout
5. 如果 keeper 已接管当前 thread，则跳过在线 rollout 改写
6. 恢复到同一个可见交互 thread

所以预期结果是：

- pane 可见
- `agentd` 中 `bootstrap_state=ready`
- `bootstrap_summary` 有值
- `thread_id` 已绑定
- keepalive 路径下，活线程 rollout 由 `app-server` 持有，不再由外层脚本直接改文件

如果 pane 在线但 `bootstrap_state=awaiting_init`，说明 bootstrap contract 没有收口成功。

## 3. 运行态检查

先看适合人读的总览：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe overview
```

机器可读全量快照：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe status
```

看单个 agent：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe agent-context --agent main
F:\work\github\AgentTool\target\debug\agentctl.exe agent-context --agent guardpro_factory
```

看主端收件箱：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe main-inbox
```

看某个 agent / task / session 的运行事件链：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe trace --agent main --limit 20
F:\work\github\AgentTool\target\debug\agentctl.exe trace --task T-REPLACE-ME --limit 20
F:\work\github\AgentTool\target\debug\agentctl.exe trace --session S-OR-B-REPLACE-ME --limit 20
```

## 4. pane 内可用命令

每个 AgentTool shell 里默认有这些辅助命令：

- `agt`：直接转发到 `agentctl`
- `agentprompt`：打印当前 bootstrap prompt + contract
- `agentbootstrap`：手动重跑 bootstrap
- `agentready "<summary>"`：bootstrap 失败时手动兜底上报
- `codex` / `mycodex`：带 AgentTool 包装的启动入口

常用检查：

```powershell
agt agent-context --agent main
agt agent-context --agent guardpro_factory
```

## 5. 主 agent 派单

创建任务：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe create-task --from main --to guardpro_factory --title "demo" --summary "demo task"
```

当前 Web 页上的“派任务”模式已经改成：

- 你只填目标执行端
- 你只写任务意图
- 结构化标题、摘要、范围、验收标准由当前 agent 生成，再由 `agentd` 创建正式任务

也就是说，网页不再要求操作者手工填写 rich task packet 的所有字段。

如果是自动 round backend 测试，可以直接：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe run-task-round --task T-REPLACE-ME
```

但当前可见 pane 主流程更推荐 visible 协议，而不是只跑非可见 round。

## 6. 子 agent visible 回报

子 agent 开始处理当前 task：

```powershell
agt begin-visible-task --agent guardpro_factory
```

子 agent 一轮回报：

```powershell
agt submit-visible-task-round --task T-REPLACE-ME --agent guardpro_factory --status report --summary "need main decision" --blocking P1 --topic "api-contract" --details "child round summary"
```

状态值当前主要有：

- `result`
- `report`
- `wait_decision`

## 7. 主 agent 拍板

主端发送 decision，但保持 task 打开：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe send-decision --task T-REPLACE-ME --issued-by main --target-agent guardpro_factory --summary "continue with the next change"
```

主端发送 decision 并直接关闭：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe send-decision --task T-REPLACE-ME --issued-by main --target-agent guardpro_factory --summary "approved" --close
```

子端确认 decision：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe ack-decision --task T-REPLACE-ME --agent guardpro_factory
```

最终关闭任务：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe close-task --task T-REPLACE-ME --agent guardpro_factory
```

## 8. Dashboard 怎么看

页面文件：

- [`dashboard/index.html`](F:\work\github\AgentTool\dashboard\index.html)

本地模式下：

- 命令行是主操作面
- dashboard 主要用来看汇总状态、最近输出、派单记录和调试信息

远程模式下：

- dashboard 会成为主操作面
- 这时你不再依赖本地 pane，可直接通过网页发消息、派任务、看最近输出

它当前主要看三类东西：

- agent runtime 状态：在线、离线、bootstrap 是否 ready
- task / decision 流转状态
- 最近通信流事件

现在还多了两块更适合排障的视图：

- `系统问题`：只列异常和不一致，例如 bridge 断开、心跳失效、状态与任务槽不一致
- `运行事件`：按时间展示 agent / task / session / bridge 的运行期事件账本，和 `agentctl trace` 同一语义层

不要把 dashboard 当成“模型心智可视化器”。当前应该只相信这些可验证状态：

- `idle / busy / blocked / offline`
- `awaiting_init / ready`
- task 是否 `running / reported / blocked_waiting_decision / closed`

## 9. 出错时怎么处理

bootstrap 没 ready：

1. 先在 pane 内运行 `agentprompt`
2. 再运行 `agentbootstrap`
3. 还不行，再手动 `agentready "<summary>"`
4. 最后用 `agt agent-context --agent <name>` 看状态

task 卡住：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe task-context --task T-REPLACE-ME
F:\work\github\AgentTool\target\debug\agentctl.exe stop-agent-session --agent guardpro_factory
F:\work\github\AgentTool\target\debug\agentctl.exe recover-agent --agent guardpro_factory
```

明显状态不一致时：

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe repair-runtime-state --requested-by main
```

## 10. 推荐测试顺序

重测默认 5 个 pane 时，建议按这个顺序：

1. 关闭现有 pane
2. 运行总启动命令
3. 等所有 pane 都进入待命
4. 执行 `agentctl status`
5. 确认 5 个默认 agent 的 `bootstrap_state` 都是 `ready`
6. 打开 dashboard 对照状态

如果只是验证 bootstrap 是否稳定，先不要发真实任务，先看 5 个 `ready` 是否全部收口。
