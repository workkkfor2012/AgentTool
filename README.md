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

Not implemented yet:

- persistent PTY-backed visible Codex session windows controlled by `agentd`
- direct stdin/stdout streaming into long-lived interactive Codex sessions
- cleanup tooling for stale historical demo records

## Runtime model

- Runtime truth lives in `agentd` memory.
- SQLite is the durable event ledger, not the realtime transport.
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

Stop the current live session for an agent:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe stop-agent-session --agent guardpro_factory
```

Acknowledge the latest pending decision for a task:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe ack-decision --task T-REPLACE-ME --agent guardpro_factory
```

Resolve a reported task in one step:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe resolve-task --task T-REPLACE-ME --analyzer main --summary "approved"
```

Send a decision and close the task in one step:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe send-decision --task T-REPLACE-ME --issued-by main --target-agent guardpro_factory --summary "approved" --close
```

Close a task after the decision is handled:

```powershell
F:\work\github\AgentTool\target\debug\agentctl.exe close-task --task T-REPLACE-ME --agent guardpro_factory
```

`resolve-task` now analyzes the task, sends the decision, acknowledges it, closes the task, and releases the child agent in one daemon round trip.
`send-decision --close` now applies the decision, acknowledges it, closes the task, and releases the child agent in one round trip.
`close-task` now auto-acknowledges the latest pending decision for that task before releasing the child agent.
`run-task-round` now auto-resolves `report` and `wait_decision` tasks when the task was created with both `--auto-resolve-by` and `--auto-resolve-summary`.

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

## Dashboard

Open the local file in a browser:

- [`F:\work\github\AgentTool\dashboard\index.html`](F:\work\github\AgentTool\dashboard\index.html)

The dashboard connects to:

- `ws://127.0.0.1:7080/ws`

It renders:

- agents
- tasks
- decisions
- sessions
- recent stream events

The initial snapshot now includes `sessions` and `recent_streams`, so the page does not need to wait for new log lines before showing context.
Agents also expose their `current_session_id`, so the dashboard can show which live session is attached to which agent.

## Structured task rounds

`run-task-round` asks Codex to return exactly one JSON object validated by:

- [`schemas/task_round.schema.json`](./schemas/task_round.schema.json)

Supported status values:

- `result`
- `report`
- `wait_decision`

## Known limitations

- The current Codex backend is round-based, not PTY-based.
- `src/backend.rs` already has the PTY dispatch point, but it currently returns `pty backend not implemented yet`.
- `stop-agent-session` only works for a live session owned by the current `agentd` process. Recovered historical `running` records do not have a kill handle.
- If Codex account limits are hit, `run-task-round` now surfaces the upstream readable error message instead of a generic exit-code failure.
- Historical demo data in SQLite may show older agent states created before the latest state-release fixes.

## Files

- [`DESIGN.md`](./DESIGN.md): current architecture and protocol notes
- [`dashboard/index.html`](./dashboard/index.html): local realtime dashboard
- [`schemas/task_round.schema.json`](./schemas/task_round.schema.json): structured child-agent result schema
