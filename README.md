# AgentTool

AgentTool is the local orchestration layer for a visible multi-agent Codex workflow on Windows. It keeps runtime state in memory, persists every state transition to SQLite, and exposes a WebSocket-only dashboard feed for real-time inspection and lightweight control.

Recommended operating modes:

- local operation: command lines are primary, the dashboard is secondary
- remote operation: the dashboard becomes the primary control surface

If you want one handoff document for the current multi-agent design, read [`SYSTEM_OVERVIEW.md`](./SYSTEM_OVERVIEW.md) first.
If you want the design rationale distilled from Codex and Claude Code, read [`REFERENCE_AGENT_LEARNINGS.md`](./REFERENCE_AGENT_LEARNINGS.md) next.

Current implementation:

- `agentd` daemon with in-memory agent/task/decision state
- SQLite event log and restart recovery
- `agentctl` local control client
- WebSocket event stream discovered from `data/runtime_endpoint.json`
- bridge WebSocket discovered from the same runtime endpoint record for persistent `agentd <-> pane` connectivity
- local dashboard page at [`dashboard/index.html`](./dashboard/index.html)
- Codex task-round backend based on `codex exec --json`
- visible-shell bootstrap backend based on `mycodex app-server` over localhost websocket, with same-thread `resume`
- daemon-owned managed app-server sessions that `agentd` can spawn and bootstrap directly through `agentctl ensure-managed-session`
- persistent bridge session tracking on each agent record, including connection mode, session id, last seen, and delivery ack counters
- delivery queue with reconnect replay for task dispatch and task-feedback push messages
- event-driven `agenthost` bridge client with passive mode and autorun mode
- unified backend start/stream/finish abstraction, currently implemented by the round backend
- extracted round backend module for command construction, event streaming, and stdout JSON parsing
- session lifecycle tracking for each Codex round, including pid, mode, thread, status, and timing
- strict single in-flight task rule per child agent
- task round schema enforcement for structured child-agent replies
- readable upstream error extraction from Codex JSON events
- agent-level `prompt_path` support with automatic discovery of `MAIN_AGENT_PROMPT.md` or `SUBAGENT_PROMPT.md` from each agent cwd
- prompt-aware round composition that pulls in repo-local context such as `work.md` when present, while keeping live communication state in memory and SQLite

Not implemented yet:

- persistent PTY-backed visible Codex session windows controlled by `agentd`
- direct stdin/stdout streaming into long-lived interactive Codex sessions

Current bridge for visible windows:

- [`scripts/Launch-AgentToolVisibleLayout.ps1`](./scripts/Launch-AgentToolVisibleLayout.ps1) launches one separate main-agent window plus one 2x2 Windows Terminal child-agent layout
- it also starts `agentd` when needed and re-registers the four current GuardPro child agents with their repo-local `SUBAGENT_PROMPT.md`
- when `ChildStartMode=view`, the launcher now opens the read-only `agentwatch` panes first and then schedules each child managed bootstrap in the background, so the visible layout appears immediately instead of blocking on the first `ensure-managed-session`
- [`scripts/Enter-AgentShell.ps1`](./scripts/Enter-AgentShell.ps1) now also starts a background passive bridge in `shell` and `codex` modes, so `agentd` can observe the pane connection without relying on polling heartbeats
- `host` mode now runs the same bridge client in `autorun` mode, so task dispatch is driven by server push instead of `Snapshot` polling
- [`scripts/Launch-AgentToolAll.ps1`](./scripts/Launch-AgentToolAll.ps1) now defaults to one interactive main-agent pane plus four interactive child panes for local operator-driven work
- `view` mode is still available when you explicitly want read-only child viewers, but it is no longer the default local path
- background launcher-side managed bootstrap logs are written under `data/launch_logs/managed-bootstrap-*.out.log` and `data/launch_logs/managed-bootstrap-*.err.log`

## Runtime model

- Runtime truth lives in `agentd` memory.
- SQLite is the durable event ledger, not the realtime transport.
- Runtime state and semantic task state are separate: bridge connection state is runtime truth; task/decision snapshots carry the semantic workflow.
- Task and decision snapshots are the primary main/child communication channel.
- Dashboard runtime data comes only from WebSocket events and snapshot messages.
- A child agent cannot receive a new task until the previous task is fully `closed`.
- `agentd` is best understood as a routing, recording, and communication hub, not as a heavy autonomous scheduler.

## Binaries

Build the project:

```powershell
cargo build
```

On Windows, prefer running the compiled binaries directly for long-lived daemon work. Repeated `cargo run` calls can conflict with the locked executable for `agentd.exe`.

## Start the daemon

```powershell
F:\work\github\AgentTool\target\debug\agentd.exe
```

Default bindings:

- WebSocket bind request: `127.0.0.1:0` (OS-assigned free port)
- Control bind request: `127.0.0.1:0` (OS-assigned free port)
- Data directory: `F:\work\github\AgentTool\data`
- Database: `F:\work\github\AgentTool\data\agenttool.db`
- Runtime endpoint record: `F:\work\github\AgentTool\data\runtime_endpoint.json`
- Dashboard runtime script: `F:\work\github\AgentTool\dashboard\runtime-endpoint.js`

Runtime discovery:

- `agentd` writes the actual WebSocket and control port to `data/runtime_endpoint.json` on startup
- `agentctl`, `agenthost`, and `agentwatch` read that file by default instead of assuming a fixed port
- the dashboard reads `dashboard/runtime-endpoint.js`, which is generated from the same runtime endpoint record

Environment overrides:

- `AGENTTOOL_ROOT`
- `AGENTTOOL_DATA_DIR`
- `AGENTTOOL_DB_PATH`
- `AGENTTOOL_WS_BIND`
- `AGENTTOOL_CONTROL_BIND`
- `AGENTTOOL_CODEX_LAUNCHER`
- `AGENTTOOL_APP_SERVER_START_TIMEOUT_SECONDS`
- `AGENTTOOL_MANAGED_BOOTSTRAP_TIMEOUT_SECONDS`

## Common commands

Ping the daemon:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe ping
```

Read a full snapshot:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe status
```

Read the main-agent inbox of tasks that are waiting for a decision:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe main-inbox
```

Register a child agent:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe register-agent --name guardpro_factory --role child --cwd F:\work\github\hackman\guardpro_factory --repo-name guardpro_factory
```

Optional prompt binding:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe register-agent --name guardpro_factory --role child --cwd F:\work\github\hackman\guardpro_factory --repo-name guardpro_factory --prompt-path F:\work\github\hackman\guardpro_factory\SUBAGENT_PROMPT.md
```

If `--prompt-path` is omitted, `agentd` now auto-detects `SUBAGENT_PROMPT.md` for child agents and `MAIN_AGENT_PROMPT.md` for the built-in `main` agent when those files exist under the agent cwd.

Run an ad hoc Codex round for a child agent:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe run-agent-round --agent guardpro_factory --prompt "Reply exactly OK and nothing else."
```

Create a task:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe create-task --from main --to guardpro_factory --title "demo" --summary "demo task"
```

Create a richer task packet:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe create-task --from main --to guardpro_factory --title "位置上报链路补齐" --summary "补齐孩子端定位采集、上报接口对接、失败重试和最小验证" --effort high --read-scope F:\work\github\hackman\guardpro_factory --read-scope F:\work\github\hackman\guardpro_backend_cloud --write-scope F:\work\github\hackman\guardpro_factory --acceptance "定位采集与上报主链可跑通" --acceptance "相关变更写入 work.md"
```

Create a task with daemon-side auto resolution policy:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe create-task --from main --to guardpro_factory --title "demo" --summary "demo task" --auto-resolve-by main --auto-resolve-summary "approved"
```

Run one structured task round:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe run-task-round --task T-REPLACE-ME
```

Clean up demo and probe records from the local runtime database:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe cleanup-demo-data --requested-by main
```

Repair obvious runtime and SQLite inconsistencies conservatively:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe repair-runtime-state --requested-by main
```

Cancel a task and release the child agent when no live session is attached:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe cancel-task --task T-REPLACE-ME --requested-by main
```

Retry a failed or cancelled task on its original child agent:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe retry-task --task T-REPLACE-ME --requested-by main
```

Stop the current live session for an agent:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe stop-agent-session --agent guardpro_factory
```

Stop all live daemon-managed app-server sessions owned by the current `agentd`:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe stop-managed-sessions
```

Stop all visible shell/view panes that were registered to the current `agentd`:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe stop-visible-panes
```

Start or re-bootstrap one daemon-owned managed app-server session for an agent:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe ensure-managed-session --agent guardpro_factory --bootstrap-prompt "先按 UTF-8 读取当前工作区中的 SUBAGENT_PROMPT.md，并将其视为你的角色契约。暂时不要开始工作，先总结自己的当前职责边界和待命状态，不要开始新的工作，等待下一条消息。"
```

Recover a blocked agent after the failure has been handled:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe recover-agent --agent guardpro_factory
```

Read the current visible-session context for one agent:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe agent-context --agent guardpro_factory
```

This now includes `context_sources`, so you can see exactly which repo-local prompt/work files currently define that agent's long-lived context.

Read the full context for one task:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe task-context --task T-REPLACE-ME
```

This also includes `context_sources` for the assigned agent when one is attached.

Mark a visible child task as actively running in the child shell:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe begin-visible-task --agent guardpro_factory
```

Submit one visible child round back to the main agent:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe submit-visible-task-round --task T-REPLACE-ME --agent guardpro_factory --status report --summary "need main decision" --blocking P1 --topic "api-contract" --details "child round summary"
```

Reset a stored Codex thread binding for an idle or blocked agent with no live session:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe reset-agent-thread --agent guardpro_factory
```

Acknowledge the latest pending decision for a task:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe ack-decision --task T-REPLACE-ME --agent guardpro_factory
```

Resolve a reported task in one step:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe resolve-task --task T-REPLACE-ME --analyzer main --summary "approved"
```

Send a decision and keep the same task open for the next child round:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe send-decision --task T-REPLACE-ME --issued-by main --target-agent guardpro_factory --summary "continue with the next change"
```

Send a decision and close the task in one step:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe send-decision --task T-REPLACE-ME --issued-by main --target-agent guardpro_factory --summary "approved" --close
```

Close a task after the decision is handled:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe close-task --task T-REPLACE-ME --agent guardpro_factory
```

`resolve-task` now analyzes the task, sends the decision, and reopens the same task as `pending` for the next child round in one daemon round trip.
`send-decision` now keeps the same task open and returns it to `pending` for the next child round by default.
`send-decision --close` now applies the decision, acknowledges it, closes the task, and releases the child agent in one round trip.
`close-task` now auto-acknowledges the latest pending decision for that task before releasing the child agent.
`run-agent-round` now prepends the configured agent prompt file to the round by telling Codex to read the prompt path first, instead of requiring the caller to inline the full role prompt manually.
`run-task-round` now auto-resolves `report` and `wait_decision` tasks when the task was created with both `--auto-resolve-by` and `--auto-resolve-summary`.
`run-task-round` now acts as a transport adapter for repo-local prompt systems: it asks Codex to read the configured prompt file plus `work.md` when present, while still forcing the final stdout into the strict task-round JSON schema and carrying the latest child/main communication state in task snapshots.
`ensure-managed-session` now lets `agentd` own a long-lived `mycodex app-server` process for an agent, bootstrap it on the daemon side, and route later structured task rounds through that managed transport without requiring a visible pane bridge.
Managed bootstrap now has a daemon-side hard timeout (`AGENTTOOL_MANAGED_BOOTSTRAP_TIMEOUT_SECONDS`, default `90`) so a stalled app-server/bootstrap websocket cannot block the control request forever.
`stop-managed-sessions` now gives `agentd` a precise shutdown path for all live daemon-owned managed app-server sessions, instead of relying on external process matching.
Visible shell/view panes can now register their pane PID and pane kind back to `agentd`, and `stop-visible-panes` lets the daemon stop those registered panes precisely by PID instead of matching broad `powershell.exe` or `codex.exe` processes.
Visible pane registration is best-effort on shell exit, and `agentd` also clears stale pane registrations during startup recovery so a crashed pane does not leave a permanent fake live-pane record in SQLite.
Each task now keeps the latest child-feedback summary, blocking level, topic, details, and completed round count, and it also snapshots the latest main-agent decision id, summary, status, issuer, and issue time.
That lets the next child round prompt and the dashboard read current communication context directly from the task record instead of recomputing it from decision history.
Blocked agents are now rejected for new task assignment and ad hoc rounds until they are explicitly recovered.
`cancel-task` now gives the main agent an explicit abort path for non-live tasks and releases the child agent back to `idle`.
`retry-task` now reopens `failed` or `cancelled` tasks as `pending` and reassigns them to the original child agent when that agent is idle.
`reset-agent-thread` now clears a persisted `thread_id` without touching SQLite manually, as long as the agent has no live session and no in-flight task.
`cleanup-demo-data` now removes demo child agents, their related tasks, decisions, sessions, and stream records from SQLite and the in-memory runtime.
`repair-runtime-state` now repairs obvious inconsistencies such as stale `current_session_id`, stale `current_task_id`, missing `closed_at`, and orphaned `running` sessions without a live handle.

## Role contract

The current recommended collaboration contract is documented in:

- [`ROLE_CONTRACT.md`](./ROLE_CONTRACT.md)

Summary:

- humans talk to the `orchestrator`, not directly to child executors
- the default always-on topology is `main + 4 executors`
- executor agents own implementation inside their own repositories
- current `agentd` runtime roles remain `main / child`, so the router/executor split is still a contract layered above the runtime

## Visible window launcher

Use the repo-owned launcher when you want all five Codex shells visible at once:

```powershell
F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.cmd
```

If you want the full default visible setup with 4 executor panes plus 1 orchestrator pane, use the no-arg top-level launcher:

```powershell
F:\work\github\AgentTool\scripts\Launch-AgentToolAll.cmd
```

Default behavior:

- starts `agentd` in the background if it is not already reachable
- registers `guardpro_backend_cloud`, `guardpro_backend_control`, `guardpro_factory`, and `guardpro_control`
- opens one Windows Terminal window with a 2x2 child-agent layout
- opens one separate main-agent window rooted at `F:\work\github\hackman`
- starts Codex directly in every child pane by default
- starts Codex directly in the main-agent window by default
- does not start advisor panes in the default path

Useful options:

```powershell
F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.cmd -DryRun
F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.cmd -ChildStartMode shell
F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.cmd -ChildStartMode host
F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.cmd -MainStartMode shell
F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.cmd -SkipRegister
F:\work\github\AgentTool\scripts\Launch-AgentToolVisibleLayout.cmd -SkipMainWindow
```

Notes:

- `-DryRun` prints the generated `agentctl` and `wt.exe` commands without opening windows
- `-ChildStartMode shell` or `-MainStartMode shell` falls back to a visible shell instead of launching Codex
- `-SkipMainWindow` keeps the 2x2 executor window and suppresses the separate main window so it can be paired with the dedicated decision layout
- each shell defines `codex` as an AgentTool-wrapped function and defines `agt` as a shortcut to the local `agentctl.exe`
- if `F:\Users\schu\bin\mycodex.bat` exists, each shell also defines `mycodex` as an AgentTool-wrapped function and the launcher passes that batch file through as the preferred Codex entrypoint
- shell 内直接运行 `codex` 或 `mycodex` 时，AgentTool 会自动补齐角色参数：主 agent 使用 `gpt-5.4 / medium`，子 agent 使用 `gpt-5.4 / high`，并统一附加 `-s danger-full-access -a never`
- 当 `codex` 或 `mycodex` 以空参数启动时，AgentTool 会先通过隐藏的 `app-server` websocket 在最终可见 thread 上完成 bootstrap，自动上报 `ready` 并绑定 `thread_id`，然后再 `resume` 到同一个可见交互 thread；如需查看手动 bootstrap 提示，可在 shell 中输入 `agentprompt`
- 当 bootstrap 需要保持当前 thread 在线时，活线程 rollout 由 `app-server` 持有；这条路径会跳过在线 rollout 改写，不再由外层脚本直接清洗活跃 jsonl
- shell 中可用 `agentbootstrap` 手动重跑 bootstrap；如果自动 bootstrap 失败，AgentTool 会打开普通交互，但不会再把 bootstrap 提示词直接显示到窗口里；只有这种情况下，才需要手工执行 `agentready`
- shell mode now starts a persistent bridge/heartbeat path immediately, so the dashboard can show runtime connectivity even before you manually launch Codex
- `-ChildStartMode host` is still available for host-based experiments, but it is not the default path
- Codex launch mode now also starts a lightweight background bridge/heartbeat job so the dashboard can at least see that the visible agent runtime is still alive
- this launcher still keeps task execution on the round-based backend, while only the visible-shell bootstrap path now uses the hidden websocket `app-server` flow

Visible-session prompt contract:

- when `AGENTTOOL_CTL` is present, prompt files should treat `agentd + agentctl` as the realtime communication source of truth
- child prompts should restore context with `agent-context` and `task-context`, then use `begin-visible-task` and `submit-visible-task-round` for each round
- main prompts should inspect child progress through `agent-context`, `task-context`, and `main-inbox`

## Decision layout launcher

Use the dedicated decision-layer launcher only when you explicitly want one visible orchestrator pane plus two temporary advisor panes:

```powershell
F:\work\github\AgentTool\scripts\Launch-AgentToolDecisionLayout.cmd
```

Default behavior:

- starts `agentd` in the background if it is not already reachable
- opens one wide orchestrator pane rooted at `F:\work\github\hackman`
- opens one top-right `advisor_high` pane rooted at `F:\work\github\hackman`
- opens one bottom-right `advisor_xhigh` pane rooted at `F:\work\github\hackman`
- uses `MAIN_AGENT_PROMPT.md` for the orchestrator pane
- uses `THINKING_ADVISOR_HIGH_PROMPT.md` and `THINKING_ADVISOR_XHIGH_PROMPT.md` for the two advisor panes

Useful options:

```powershell
F:\work\github\AgentTool\scripts\Launch-AgentToolDecisionLayout.cmd -DryRun
F:\work\github\AgentTool\scripts\Launch-AgentToolDecisionLayout.cmd -OrchestratorStartMode shell
F:\work\github\AgentTool\scripts\Launch-AgentToolDecisionLayout.cmd -AdvisorStartMode shell
```

Current implementation notes:

- the orchestrator pane still maps to the existing runtime `main` agent
- the two advisor panes are optional consultation workers, not part of the default topology
- the default recommended workflow remains `main + 4 executors`

## Task lifecycle

Main states:

- `pending`
- `accepted`
- `running`
- `completed`
- `reported`
- `analyzed`
- `decision_sent`
- `closed`
- `blocked_waiting_decision`
- `cancelled`
- `failed`

Operational rule:

- `completed` does not unlock the next task.
- only `closed` unlocks the next task for that child agent.
- a decision can reopen the same task back to `pending`, so one task can span many main/child rounds before it is eventually closed.

## Dashboard

Open the local file in a browser:

- [`F:\work\github\AgentTool\dashboard\index.html`](F:\work\github\AgentTool\dashboard\index.html)

The dashboard connects to:

- the current websocket URL from `data/runtime_endpoint.json`

It renders:

- live communication state for open tasks between the main agent and child agents
- agents
- tasks
- decisions
- sessions
- recent stream events
- filters for active-only view, stderr hiding, and text search
- inspector details for a selected agent, task, decision, session, or stream

In the current UI, the dashboard can also:

- send one controlled round to a target agent
- let the current agent generate and dispatch a structured task to a target executor

The initial snapshot now includes `sessions` and `recent_streams`, so the page does not need to wait for new log lines before showing context.
Agents now expose both `current_session_id` and `prompt_path`, so the dashboard can show which live session and prompt binding are attached to each agent.
The dashboard now emphasizes `runtime state + task semantic state` for open tasks instead of building a history replay workflow.
For local work, it should still be treated as a secondary surface beside the visible panes.
Open-task rows now also surface the latest child-feedback summary, round count, and latest main decision snapshot.

## Structured task rounds

`run-task-round` asks Codex to return exactly one JSON object validated by:

- [`schemas/task_round.schema.json`](./schemas/task_round.schema.json)

Supported status values:

- `result`
- `report`
- `wait_decision`

The daemon-side prompt wrapper lets a child repository keep its own human-oriented `[REPORT]` and optional repo notes such as `work.md`, but the transport source of truth is the strict JSON round output plus daemon-side task and decision state in memory and SQLite.

## Structured bootstrap ready

Visible interactive shells now use a fixed bootstrap-ready contract instead of relying on the model to manually call `agentready`.

The bootstrap flow is:

- `begin-agent-bootstrap`
- hidden `mycodex app-server --listen ws://127.0.0.1:<port>`
- `thread/start` with prompt-file content injected as persistent `developerInstructions`
- `turn/start` on that same thread with the bootstrap-ready output schema
- shell-side `mark-agent-ready --thread-id ... --summary ...`
- offline/non-keepalive bootstrap may sanitize the rollout to remove the bootstrap user message while keeping the assistant ready reply
- keepalive bootstrap skips online rollout rewrite because the active thread resource is owned by `app-server`
- interactive `resume` into the same thread

This keeps the `ready` state machine deterministic while preserving direct CLI access for testing and manual recovery.

## Known limitations

- The current Codex backend is round-based, not PTY-based.
- `src/backend.rs` already has the PTY dispatch point, but it currently returns `pty backend not implemented yet`.
- `cancel-task` refuses to touch a task that still has a live session attached; use `stop-agent-session` first.
- `stop-agent-session` only works for a live session owned by the current `agentd` process. Recovered historical `running` records do not have a kill handle.
- `recover-agent` only works when the agent has no in-flight task and no live session attached.
- `reset-agent-thread` only works when the agent has no in-flight task and no live session attached.
- `cleanup-demo-data` only targets built-in demo/probe agent names such as `demo_*` and `usage_limit_probe`.
- `repair-runtime-state` is intentionally conservative and does not guess across multiple open tasks for the same agent.
- If Codex account limits are hit, `run-task-round` now surfaces the upstream readable error message instead of a generic exit-code failure.
- Historical demo data in SQLite may show older agent states created before the latest state-release fixes.

## Files

- [`SYSTEM_OVERVIEW.md`](./SYSTEM_OVERVIEW.md): single-document handoff for the current multi-agent architecture
- [`ROLE_CONTRACT.md`](./ROLE_CONTRACT.md): collaboration contract and role boundaries
- [`dashboard/index.html`](./dashboard/index.html): local realtime dashboard
- [`schemas/bootstrap_ready.schema.json`](./schemas/bootstrap_ready.schema.json): bootstrap ready contract
- [`schemas/task_round.schema.json`](./schemas/task_round.schema.json): structured child-agent result schema
