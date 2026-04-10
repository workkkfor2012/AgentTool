# AgentTool

AgentTool is the local orchestration layer for a visible multi-agent Codex workflow on Windows. It keeps runtime state in memory, persists every state transition to SQLite, and exposes a WebSocket-only dashboard feed for real-time inspection.
The dashboard is intentionally read-only for now.

Current implementation:

- `agentd` daemon with in-memory agent/task/decision state
- SQLite event log and restart recovery
- `agentctl` local control client
- WebSocket event stream on `ws://127.0.0.1:7080/ws`
- local dashboard page at [`dashboard/index.html`](./dashboard/index.html)
- Codex round backend based on `codex exec --json` and `codex exec resume --json`
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

## Runtime model

- Runtime truth lives in `agentd` memory.
- SQLite is the durable event ledger, not the realtime transport.
- Task and decision snapshots are the primary main/child communication channel.
- Dashboard runtime data comes only from WebSocket events and snapshot messages.
- A child agent cannot receive a new task until the previous task is fully `closed`.

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

- WebSocket: `127.0.0.1:7080`
- Control socket: `127.0.0.1:7081`
- Data directory: `F:\work\github\AgentTool\data`
- Database: `F:\work\github\AgentTool\data\agenttool.db`

Environment overrides:

- `AGENTTOOL_ROOT`
- `AGENTTOOL_DATA_DIR`
- `AGENTTOOL_DB_PATH`
- `AGENTTOOL_WS_BIND`
- `AGENTTOOL_CONTROL_BIND`

## Common commands

Ping the daemon:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe ping
```

Read a full snapshot:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe status
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

Recover a blocked agent after the failure has been handled:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe recover-agent --agent guardpro_factory
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
Each task now keeps the latest child-feedback summary, blocking level, topic, details, and completed round count, and it also snapshots the latest main-agent decision id, summary, status, issuer, and issue time.
That lets the next child round prompt and the dashboard read current communication context directly from the task record instead of recomputing it from decision history.
Blocked agents are now rejected for new task assignment and ad hoc rounds until they are explicitly recovered.
`cancel-task` now gives the main agent an explicit abort path for non-live tasks and releases the child agent back to `idle`.
`retry-task` now reopens `failed` or `cancelled` tasks as `pending` and reassigns them to the original child agent when that agent is idle.
`reset-agent-thread` now clears a persisted `thread_id` without touching SQLite manually, as long as the agent has no live session and no in-flight task.
`cleanup-demo-data` now removes demo child agents, their related tasks, decisions, sessions, and stream records from SQLite and the in-memory runtime.
`repair-runtime-state` now repairs obvious inconsistencies such as stale `current_session_id`, stale `current_task_id`, missing `closed_at`, and orphaned `running` sessions without a live handle.

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

- `ws://127.0.0.1:7080/ws`

It renders:

- live communication state for open tasks between the main agent and child agents
- agents
- tasks
- decisions
- sessions
- recent stream events
- read-only filters for active-only view, stderr hiding, and text search
- read-only inspector details for a selected agent, task, decision, session, or stream

The initial snapshot now includes `sessions` and `recent_streams`, so the page does not need to wait for new log lines before showing context.
Agents now expose both `current_session_id` and `prompt_path`, so the dashboard can show which live session and prompt binding are attached to each agent.
The dashboard now emphasizes live communication flow for open tasks instead of building a history replay workflow.
Open-task rows now also surface the latest child-feedback summary, round count, and latest main decision snapshot.

## Structured task rounds

`run-task-round` asks Codex to return exactly one JSON object validated by:

- [`schemas/task_round.schema.json`](./schemas/task_round.schema.json)

Supported status values:

- `result`
- `report`
- `wait_decision`

The daemon-side prompt wrapper lets a child repository keep its own human-oriented `[REPORT]` and optional repo notes such as `work.md`, but the transport source of truth is the strict JSON round output plus daemon-side task and decision state in memory and SQLite.

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

- [`DESIGN.md`](./DESIGN.md): current architecture and protocol notes
- [`dashboard/index.html`](./dashboard/index.html): local realtime dashboard
- [`schemas/task_round.schema.json`](./schemas/task_round.schema.json): structured child-agent result schema
