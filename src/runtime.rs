use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use chrono::Utc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, broadcast};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::backend::{
    BackendEvent, BackendStartRequest, BackendStopSignal, BackendStream, start_backend,
};
use crate::config::AppConfig;
use crate::control::{ControlRequest, ControlResponse};
use crate::db::Database;
use crate::models::{
    AgentRole, AgentRoundResult, AgentSessionState, AgentSummary, CleanupSummary,
    CodexSessionStatus, DashboardEvent, DashboardSnapshot, DecisionSummary, RepairSummary,
    SessionMode, SessionSummary, StreamEventRecord, TaskRoundPayload, TaskRoundStatus, TaskState,
    TaskSummary, WsClientMessage,
};

#[derive(Clone)]
pub struct AppShared {
    config: Arc<AppConfig>,
    state: Arc<RwLock<RuntimeState>>,
    db: Arc<Mutex<Database>>,
    events_tx: broadcast::Sender<DashboardEvent>,
    active_sessions: Arc<Mutex<HashMap<String, ActiveSessionControl>>>,
}

pub struct RuntimeState {
    pub agents: HashMap<String, AgentSummary>,
    pub tasks: HashMap<String, TaskSummary>,
    pub decisions: HashMap<String, DecisionSummary>,
    pub sessions: HashMap<String, SessionSummary>,
    pub recent_streams: Vec<StreamEventRecord>,
}

struct ActiveSessionControl {
    agent_name: String,
    stop: BackendStopSignal,
}

const MAX_RECENT_STREAMS: usize = 500;

impl RuntimeState {
    pub fn snapshot(&self) -> DashboardSnapshot {
        let mut agents = self.agents.values().cloned().collect::<Vec<_>>();
        agents.sort_by(|a, b| a.name.cmp(&b.name));

        let mut tasks = self.tasks.values().cloned().collect::<Vec<_>>();
        tasks.sort_by(|a, b| a.created_at.cmp(&b.created_at));

        let mut decisions = self.decisions.values().cloned().collect::<Vec<_>>();
        decisions.sort_by(|a, b| a.created_at.cmp(&b.created_at));

        let mut sessions = self.sessions.values().cloned().collect::<Vec<_>>();
        sessions.sort_by(|a, b| a.started_at.cmp(&b.started_at));

        DashboardSnapshot {
            agents,
            tasks,
            decisions,
            sessions,
            recent_streams: self.recent_streams.clone(),
            generated_at: Utc::now(),
        }
    }
}

pub async fn run_agentd(config: AppConfig) -> Result<()> {
    config.ensure_dirs()?;

    let db = Database::open(&config.db_path)?;
    let existing_agents = db.load_agents()?;
    let existing_tasks = db.load_tasks()?;
    let existing_decisions = db.load_decisions()?;
    let existing_sessions = {
        let mut merged = HashMap::new();
        for session in db.load_recent_sessions(200)? {
            merged.insert(session.session_id.clone(), session);
        }
        for session in db.load_running_sessions()? {
            merged.insert(session.session_id.clone(), session);
        }
        merged.into_values().collect::<Vec<_>>()
    };
    let existing_streams = db.load_recent_stream_events(200)?;

    let (events_tx, _) = broadcast::channel(512);
    let shared = AppShared {
        config: Arc::new(config.clone()),
        state: Arc::new(RwLock::new(RuntimeState {
            agents: existing_agents
                .into_iter()
                .map(|agent| (agent.name.clone(), agent))
                .collect(),
            tasks: existing_tasks
                .into_iter()
                .map(|task| (task.task_id.clone(), task))
                .collect(),
            decisions: existing_decisions
                .into_iter()
                .map(|decision| (decision.decision_id.clone(), decision))
                .collect(),
            sessions: existing_sessions
                .into_iter()
                .map(|session| (session.session_id.clone(), session))
                .collect(),
            recent_streams: existing_streams,
        })),
        db: Arc::new(Mutex::new(db)),
        events_tx,
        active_sessions: Arc::new(Mutex::new(HashMap::new())),
    };

    ensure_main_agent(&shared).await?;
    recover_stale_sessions_on_startup(&shared).await?;
    let ws_shared = shared.clone();
    let control_shared = shared.clone();

    let ws_listener = TcpListener::bind(config.ws_bind)
        .await
        .with_context(|| format!("failed to bind websocket listener at {}", config.ws_bind))?;
    let control_listener = TcpListener::bind(config.control_bind)
        .await
        .with_context(|| format!("failed to bind control listener at {}", config.control_bind))?;

    info!("agentd websocket listening on {}", config.ws_bind);
    info!("agentd control listening on {}", config.control_bind);

    let ws_task = tokio::spawn(async move { run_ws_server(ws_listener, ws_shared).await });
    let control_task =
        tokio::spawn(async move { run_control_server(control_listener, control_shared).await });

    tokio::select! {
        result = ws_task => result??,
        result = control_task => result??,
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for ctrl_c")?;
            info!("received ctrl_c, shutting down agentd");
        }
    }

    Ok(())
}

async fn ensure_main_agent(shared: &AppShared) -> Result<()> {
    if shared.state.read().await.agents.contains_key("main") {
        return Ok(());
    }

    let main_agent = AgentSummary {
        name: "main".to_string(),
        role: AgentRole::Main,
        repo_name: Some("hackman".to_string()),
        cwd: ".".to_string(),
        thread_id: None,
        current_session_id: None,
        state: AgentSessionState::Idle,
        current_task_id: None,
        last_output_at: None,
        last_heartbeat_at: Some(Utc::now()),
    };

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&main_agent)?;
    }

    {
        let mut state = shared.state.write().await;
        state
            .agents
            .insert(main_agent.name.clone(), main_agent.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::AgentStateChanged { agent: main_agent })
        .ok();
    Ok(())
}

async fn run_ws_server(listener: TcpListener, shared: AppShared) -> Result<()> {
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(shared);

    axum::serve(listener, app)
        .await
        .context("websocket server crashed")
}

async fn ws_handler(ws: WebSocketUpgrade, State(shared): State<AppShared>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_session(socket, shared))
}

async fn ws_session(mut socket: WebSocket, shared: AppShared) {
    let snapshot = shared.state.read().await.snapshot();
    if socket
        .send(Message::Text(
            serde_json::to_string(&DashboardEvent::Snapshot { snapshot })
                .unwrap_or_else(|_| "{\"type\":\"snapshot_error\"}".to_string())
                .into(),
        ))
        .await
        .is_err()
    {
        return;
    }

    let mut rx = shared.events_tx.subscribe();

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(msg) = serde_json::from_str::<WsClientMessage>(&text) {
                            match msg {
                                WsClientMessage::RequestSnapshot => {
                                    let snapshot = shared.state.read().await.snapshot();
                                    let payload = serde_json::to_string(&DashboardEvent::Snapshot { snapshot })
                                        .unwrap_or_else(|_| "{\"type\":\"snapshot_error\"}".to_string());
                                    if socket.send(Message::Text(payload.into())).await.is_err() {
                                        break;
                                    }
                                }
                                WsClientMessage::Ping => {
                                    if socket.send(Message::Text("{\"type\":\"pong\"}".into())).await.is_err() {
                                        break;
                                    }
                                }
                                WsClientMessage::Subscribe { .. } => {}
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        warn!("websocket receive error: {err}");
                        break;
                    }
                }
            }
            outgoing = rx.recv() => {
                match outgoing {
                    Ok(event) => {
                        let payload = match serde_json::to_string(&event) {
                            Ok(payload) => payload,
                            Err(err) => {
                                warn!("failed to serialize dashboard event: {err}");
                                continue;
                            }
                        };
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        warn!("dashboard event stream error: {err}");
                        break;
                    }
                }
            }
        }
    }
}

async fn run_control_server(listener: TcpListener, shared: AppShared) -> Result<()> {
    loop {
        let (stream, addr) = listener.accept().await.context("control accept failed")?;
        let shared = shared.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_control_connection(stream, shared).await {
                error!("control connection error from {addr}: {err:#}");
            }
        });
    }
}

async fn handle_control_connection(stream: TcpStream, shared: AppShared) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let read = reader.read_line(&mut line).await?;
    if read == 0 {
        return Ok(());
    }

    let request: ControlRequest =
        serde_json::from_str(line.trim()).context("failed to parse control request json")?;

    let response = handle_control_request(shared, request).await;
    let payload =
        serde_json::to_string(&response).context("failed to serialize control response")?;
    writer.write_all(payload.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn handle_control_request(shared: AppShared, request: ControlRequest) -> ControlResponse {
    match dispatch_control_request(shared, request).await {
        Ok(response) => response,
        Err(err) => ControlResponse::Error {
            message: err.to_string(),
        },
    }
}

async fn dispatch_control_request(
    shared: AppShared,
    request: ControlRequest,
) -> Result<ControlResponse> {
    match request {
        ControlRequest::Ping => Ok(ControlResponse::Pong),
        ControlRequest::Snapshot => {
            let snapshot = shared.state.read().await.snapshot();
            Ok(ControlResponse::Snapshot { snapshot })
        }
        ControlRequest::RegisterAgent {
            name,
            role,
            repo_name,
            cwd,
        } => {
            let role = match role.as_str() {
                "main" => AgentRole::Main,
                "child" => AgentRole::Child,
                other => bail!("unsupported agent role: {other}"),
            };

            let agent = AgentSummary {
                name: name.clone(),
                role,
                repo_name,
                cwd,
                thread_id: None,
                current_session_id: None,
                state: AgentSessionState::Idle,
                current_task_id: None,
                last_output_at: None,
                last_heartbeat_at: Some(Utc::now()),
            };

            {
                let db = shared.db.lock().expect("db mutex poisoned");
                db.upsert_agent(&agent)?;
            }

            {
                let mut state = shared.state.write().await;
                state.agents.insert(name, agent.clone());
            }

            shared
                .events_tx
                .send(DashboardEvent::AgentStateChanged {
                    agent: agent.clone(),
                })
                .ok();

            Ok(ControlResponse::Agent { agent })
        }
        ControlRequest::RunAgentRound { agent, prompt } => {
            let result = run_agent_round(&shared, &agent, &prompt).await?;
            Ok(ControlResponse::RoundResult { result })
        }
        ControlRequest::RunTaskRound { task_id } => run_task_round(&shared, &task_id).await,
        ControlRequest::CleanupDemoData { requested_by } => {
            let summary = cleanup_demo_data(&shared, &requested_by).await?;
            Ok(ControlResponse::Cleanup { summary })
        }
        ControlRequest::RepairRuntimeState { requested_by } => {
            let summary = repair_runtime_state(&shared, &requested_by).await?;
            Ok(ControlResponse::Repair { summary })
        }
        ControlRequest::CancelTask {
            task_id,
            requested_by,
        } => {
            let task = cancel_task(&shared, &task_id, &requested_by).await?;
            Ok(ControlResponse::Task { task })
        }
        ControlRequest::RetryTask {
            task_id,
            requested_by,
        } => {
            let task = retry_task(&shared, &task_id, &requested_by).await?;
            Ok(ControlResponse::Task { task })
        }
        ControlRequest::ResetAgentThread { agent } => {
            let agent = reset_agent_thread(&shared, &agent).await?;
            Ok(ControlResponse::Agent { agent })
        }
        ControlRequest::RecoverAgent { agent } => {
            let agent = recover_agent(&shared, &agent).await?;
            Ok(ControlResponse::Agent { agent })
        }
        ControlRequest::StopAgentSession { agent } => {
            let session_id = stop_agent_session(&shared, &agent).await?;
            Ok(ControlResponse::Ack {
                message: format!(
                    "stop requested for current session {session_id} on agent {agent}"
                ),
            })
        }
        ControlRequest::CreateTask {
            from_agent,
            to_agent,
            title,
            summary,
            auto_resolve_by,
            auto_resolve_summary,
        } => {
            {
                let state = shared.state.read().await;
                let target = state
                    .agents
                    .get(&to_agent)
                    .ok_or_else(|| anyhow!("target agent not registered: {to_agent}"))?;

                ensure_agent_ready_for_new_task(target)?;

                match (&auto_resolve_by, &auto_resolve_summary) {
                    (Some(analyzer), Some(_)) => {
                        if !state.agents.contains_key(analyzer) {
                            bail!("auto resolve analyzer not registered: {analyzer}");
                        }
                    }
                    (None, None) => {}
                    _ => {
                        bail!("auto_resolve_by and auto_resolve_summary must be set together");
                    }
                }
            }

            let now = Utc::now();
            let task = TaskSummary {
                task_id: format!("T-{}", Uuid::now_v7().simple()),
                from_agent,
                to_agent: to_agent.clone(),
                title,
                summary,
                auto_resolve_by,
                auto_resolve_summary,
                state: TaskState::Pending,
                created_at: now,
                updated_at: now,
                closed_at: None,
            };

            {
                let db = shared.db.lock().expect("db mutex poisoned");
                db.insert_task(&task)?;
                db.insert_task_event(
                    &task.task_id,
                    "task_created",
                    None,
                    Some(TaskState::Pending),
                    "{}",
                )?;
            }

            let mut changed_agent = None;
            {
                let mut state = shared.state.write().await;
                state.tasks.insert(task.task_id.clone(), task.clone());
                if let Some(agent) = state.agents.get_mut(&to_agent) {
                    claim_task_for_agent_summary(agent, &task.task_id);
                    changed_agent = Some(agent.clone());
                }
            }

            if let Some(agent) = changed_agent {
                let db = shared.db.lock().expect("db mutex poisoned");
                db.upsert_agent(&agent)?;
                shared
                    .events_tx
                    .send(DashboardEvent::AgentStateChanged { agent })
                    .ok();
            }

            shared
                .events_tx
                .send(DashboardEvent::TaskEvent {
                    task: task.clone(),
                    event_type: "task_created".to_string(),
                })
                .ok();

            Ok(ControlResponse::Task { task })
        }
        ControlRequest::AcceptTask { task_id, agent } => {
            let task = transition_task_state(
                &shared,
                &task_id,
                &agent,
                TaskState::Pending,
                TaskState::Accepted,
                "task_accepted",
            )
            .await?;
            Ok(ControlResponse::Task { task })
        }
        ControlRequest::StartTask { task_id, agent } => {
            let task = transition_task_state(
                &shared,
                &task_id,
                &agent,
                TaskState::Accepted,
                TaskState::Running,
                "task_running",
            )
            .await?;
            Ok(ControlResponse::Task { task })
        }
        ControlRequest::CompleteTask { task_id, agent } => {
            let task = transition_task_state(
                &shared,
                &task_id,
                &agent,
                TaskState::Running,
                TaskState::Completed,
                "task_completed",
            )
            .await?;
            Ok(ControlResponse::Task { task })
        }
        ControlRequest::ReportTask {
            task_id,
            agent,
            blocking,
            topic,
            details,
        } => {
            let task = transition_task_state(
                &shared,
                &task_id,
                &agent,
                TaskState::Completed,
                TaskState::Reported,
                "report_submitted",
            )
            .await?;

            let payload = serde_json::json!({
                "blocking": blocking,
                "topic": topic,
                "details": details,
            });

            {
                let db = shared.db.lock().expect("db mutex poisoned");
                db.insert_task_event(
                    &task.task_id,
                    "report_payload",
                    Some(TaskState::Reported),
                    Some(TaskState::Reported),
                    &payload.to_string(),
                )?;
            }
            record_stream_event(&shared, &agent, "stdout", &format!("[REPORT] {payload}")).await?;

            Ok(ControlResponse::Task { task })
        }
        ControlRequest::AnalyzeTask { task_id, analyzer } => {
            let task = analyze_task_for_main(&shared, &task_id, &analyzer).await?;
            Ok(ControlResponse::Task { task })
        }
        ControlRequest::ResolveTask {
            task_id,
            analyzer,
            summary,
        } => {
            let (decision, task) =
                resolve_task_for_main(&shared, &task_id, &analyzer, &summary).await?;
            Ok(ControlResponse::DecisionTask { decision, task })
        }
        ControlRequest::SendDecision {
            task_id,
            issued_by,
            target_agent,
            summary,
            auto_close,
        } => {
            let decision =
                send_decision_to_task(&shared, &task_id, &issued_by, &target_agent, &summary)
                    .await?;

            if auto_close {
                let closed_task =
                    close_task_after_decision(&shared, &task_id, &target_agent).await?;
                return Ok(ControlResponse::DecisionTask {
                    decision,
                    task: closed_task,
                });
            }

            Ok(ControlResponse::Decision { decision })
        }
        ControlRequest::AcknowledgeDecision { task_id, agent } => {
            let decision = acknowledge_latest_decision_for_task(&shared, &task_id, &agent).await?;
            Ok(ControlResponse::Decision { decision })
        }
        ControlRequest::CloseTask { task_id, agent } => {
            let task = close_task_after_decision(&shared, &task_id, &agent).await?;
            Ok(ControlResponse::Task { task })
        }
    }
}

async fn run_agent_round(
    shared: &AppShared,
    agent_name: &str,
    prompt: &str,
) -> Result<AgentRoundResult> {
    let agent = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    ensure_agent_ready_for_ad_hoc_round(&agent)?;

    set_agent_runtime_state(shared, agent_name, AgentSessionState::Busy).await?;

    let execution = execute_codex_round(shared, &agent, prompt, None).await;

    match execution {
        Ok(result) => {
            let mut updated_agent = {
                let state = shared.state.read().await;
                state
                    .agents
                    .get(agent_name)
                    .cloned()
                    .ok_or_else(|| anyhow!("agent disappeared during round: {agent_name}"))?
            };
            updated_agent.thread_id = Some(result.thread_id.clone());
            updated_agent.state = AgentSessionState::Idle;
            updated_agent.last_output_at = Some(result.completed_at);
            updated_agent.last_heartbeat_at = Some(Utc::now());

            {
                let db = shared.db.lock().expect("db mutex poisoned");
                db.upsert_agent(&updated_agent)?;
            }

            {
                let mut state = shared.state.write().await;
                state
                    .agents
                    .insert(agent_name.to_string(), updated_agent.clone());
            }

            shared
                .events_tx
                .send(DashboardEvent::AgentStateChanged {
                    agent: updated_agent,
                })
                .ok();
            record_stream_event(
                shared,
                agent_name,
                "stdout",
                &format!("[RESULT] {}", result.final_message),
            )
            .await?;

            Ok(result)
        }
        Err(err) => {
            let next_state = if is_session_stop_error(&err) {
                AgentSessionState::Idle
            } else {
                AgentSessionState::Blocked
            };
            set_agent_runtime_state(shared, agent_name, next_state).await?;
            Err(err)
        }
    }
}

fn ensure_agent_ready_for_new_task(agent: &AgentSummary) -> Result<()> {
    if agent.current_task_id.is_some() {
        bail!("target agent {} already has an in-flight task", agent.name);
    }
    if agent.current_session_id.is_some() {
        bail!(
            "target agent {} already has a live session attached",
            agent.name
        );
    }
    if agent.state != AgentSessionState::Idle {
        bail!(
            "target agent {} must be idle before receiving a new task, actual {:?}",
            agent.name,
            agent.state
        );
    }
    Ok(())
}

fn ensure_agent_ready_for_ad_hoc_round(agent: &AgentSummary) -> Result<()> {
    if agent.current_task_id.is_some() {
        bail!(
            "agent {} has an in-flight task and cannot run an ad hoc round",
            agent.name
        );
    }
    if agent.current_session_id.is_some() {
        bail!("agent {} already has a live session attached", agent.name);
    }
    if agent.state != AgentSessionState::Idle {
        bail!(
            "agent {} must be idle before running an ad hoc round, actual {:?}",
            agent.name,
            agent.state
        );
    }
    Ok(())
}

fn claim_task_for_agent_summary(agent: &mut AgentSummary, task_id: &str) {
    agent.current_task_id = Some(task_id.to_string());
    agent.state = AgentSessionState::Busy;
    agent.last_output_at = Some(Utc::now());
    agent.last_heartbeat_at = Some(Utc::now());
}

fn ensure_agent_ready_for_task_round(agent: &AgentSummary, task_id: &str) -> Result<()> {
    if agent.current_task_id.as_deref() != Some(task_id) {
        bail!(
            "agent {} is not currently assigned to task {}",
            agent.name,
            task_id
        );
    }
    if agent.current_session_id.is_some() {
        bail!("agent {} already has a live session attached", agent.name);
    }
    if agent.state != AgentSessionState::Busy {
        bail!(
            "agent {} must be busy before running assigned task {}, actual {:?}",
            agent.name,
            task_id,
            agent.state
        );
    }
    Ok(())
}

async fn ensure_registered_agent(shared: &AppShared, agent_name: &str) -> Result<()> {
    let state = shared.state.read().await;
    if !state.agents.contains_key(agent_name) {
        bail!("agent not registered: {agent_name}");
    }
    Ok(())
}

fn is_task_terminal(state: &TaskState) -> bool {
    matches!(
        state,
        TaskState::Closed | TaskState::Cancelled | TaskState::Failed
    )
}

fn desired_agent_state_for_task(task: &TaskSummary) -> AgentSessionState {
    match task.state {
        TaskState::Pending | TaskState::Accepted | TaskState::Running | TaskState::Completed => {
            AgentSessionState::Busy
        }
        TaskState::Reported
        | TaskState::Analyzed
        | TaskState::DecisionSent
        | TaskState::BlockedWaitingDecision => AgentSessionState::Blocked,
        TaskState::Closed | TaskState::Cancelled | TaskState::Failed => AgentSessionState::Idle,
    }
}

fn is_demo_agent_name(agent_name: &str) -> bool {
    agent_name.starts_with("demo_") || agent_name == "usage_limit_probe"
}

async fn broadcast_runtime_snapshot(shared: &AppShared) {
    let snapshot = shared.state.read().await.snapshot();
    shared
        .events_tx
        .send(DashboardEvent::Snapshot { snapshot })
        .ok();
}

async fn run_task_round(shared: &AppShared, task_id: &str) -> Result<ControlResponse> {
    let task = {
        let state = shared.state.read().await;
        state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?
    };

    if task.state != TaskState::Pending {
        bail!(
            "task {task_id} must be in pending state to run, actual {:?}",
            task.state
        );
    }

    let agent = {
        let state = shared.state.read().await;
        state
            .agents
            .get(&task.to_agent)
            .cloned()
            .ok_or_else(|| anyhow!("target agent not registered: {}", task.to_agent))?
    };
    ensure_agent_ready_for_task_round(&agent, &task.task_id)?;

    let accepted = transition_task_state(
        shared,
        &task.task_id,
        &task.to_agent,
        TaskState::Pending,
        TaskState::Accepted,
        "task_accepted",
    )
    .await?;

    let _running = transition_task_state(
        shared,
        &accepted.task_id,
        &accepted.to_agent,
        TaskState::Accepted,
        TaskState::Running,
        "task_running",
    )
    .await?;

    let prompt = compose_task_prompt(&task);
    let schema_path = shared
        .config
        .root_dir
        .join("schemas")
        .join("task_round.schema.json");
    let round = match execute_codex_round(
        shared,
        &agent,
        &prompt,
        Some(schema_path.to_string_lossy().to_string()),
    )
    .await
    {
        Ok(round) => round,
        Err(err) => {
            let failed_task = transition_task_state(
                shared,
                &task.task_id,
                &task.to_agent,
                TaskState::Running,
                TaskState::Failed,
                "task_failed",
            )
            .await?;
            release_agent_after_terminal_task(shared, &task.to_agent, AgentSessionState::Blocked)
                .await?;
            return Ok(ControlResponse::TaskRound {
                task: failed_task,
                result: AgentRoundResult {
                    agent: agent.name.clone(),
                    thread_id: agent.thread_id.unwrap_or_default(),
                    final_message: err.to_string(),
                    completed_at: Utc::now(),
                },
                payload: TaskRoundPayload {
                    status: TaskRoundStatus::Report,
                    summary: "Task execution failed".to_string(),
                    blocking: Some("P0".to_string()),
                    topic: Some("agenttool execution failure".to_string()),
                    details: Some(err.to_string()),
                    reason: None,
                    next_suggestion: Some(
                        "Inspect stream_events and stderr, then retry or repair the session"
                            .to_string(),
                    ),
                    changed_files: Vec::new(),
                },
                decision: None,
            });
        }
    };

    let payload: TaskRoundPayload =
        serde_json::from_str(&round.final_message).with_context(|| {
            format!(
                "failed to parse task round payload as json for task {}",
                task.task_id
            )
        })?;

    let final_task = match payload.status {
        TaskRoundStatus::Result => {
            let completed = transition_task_state(
                shared,
                &task.task_id,
                &task.to_agent,
                TaskState::Running,
                TaskState::Completed,
                "task_completed",
            )
            .await?;

            transition_task_state(
                shared,
                &completed.task_id,
                &completed.to_agent,
                TaskState::Completed,
                TaskState::Reported,
                "result_reported",
            )
            .await?
        }
        TaskRoundStatus::Report => {
            transition_task_state(
                shared,
                &task.task_id,
                &task.to_agent,
                TaskState::Running,
                TaskState::Reported,
                "report_submitted",
            )
            .await?
        }
        TaskRoundStatus::WaitDecision => {
            transition_task_state(
                shared,
                &task.task_id,
                &task.to_agent,
                TaskState::Running,
                TaskState::BlockedWaitingDecision,
                "task_waiting_decision",
            )
            .await?
        }
    };

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.insert_task_event(
            &task.task_id,
            "task_round_payload",
            Some(final_task.state.clone()),
            Some(final_task.state.clone()),
            &round.final_message,
        )?;
    }
    record_stream_event(
        shared,
        &task.to_agent,
        "stdout",
        &format!("[TASK_ROUND_RESULT] {}", round.final_message),
    )
    .await?;

    let next_agent_state = match payload.status {
        TaskRoundStatus::Result => AgentSessionState::Busy,
        TaskRoundStatus::Report | TaskRoundStatus::WaitDecision => AgentSessionState::Blocked,
    };
    update_agent_after_round(
        shared,
        &task.to_agent,
        Some(round.thread_id.clone()),
        next_agent_state,
        round.completed_at,
    )
    .await?;

    let (task_for_response, decision) =
        maybe_auto_resolve_task(shared, &final_task, &payload).await?;

    Ok(ControlResponse::TaskRound {
        task: task_for_response,
        result: round,
        payload,
        decision,
    })
}

async fn cleanup_demo_data(shared: &AppShared, requested_by: &str) -> Result<CleanupSummary> {
    ensure_registered_agent(shared, requested_by).await?;

    let (demo_agent_names, task_ids, live_agent_names) = {
        let state = shared.state.read().await;
        let demo_agent_names = state
            .agents
            .values()
            .filter(|agent| is_demo_agent_name(&agent.name))
            .map(|agent| agent.name.clone())
            .collect::<Vec<_>>();
        let demo_agent_set = demo_agent_names.iter().cloned().collect::<HashSet<_>>();
        let task_ids = state
            .tasks
            .values()
            .filter(|task| {
                demo_agent_set.contains(&task.from_agent) || demo_agent_set.contains(&task.to_agent)
            })
            .map(|task| task.task_id.clone())
            .collect::<Vec<_>>();
        let live_agent_names = state
            .agents
            .values()
            .filter(|agent| {
                demo_agent_set.contains(&agent.name) && agent.current_session_id.is_some()
            })
            .map(|agent| agent.name.clone())
            .collect::<Vec<_>>();
        (demo_agent_names, task_ids, live_agent_names)
    };

    if !live_agent_names.is_empty() {
        bail!(
            "cleanup refused because demo agents still have live sessions: {}",
            live_agent_names.join(", ")
        );
    }

    if demo_agent_names.is_empty() && task_ids.is_empty() {
        return Ok(CleanupSummary {
            requested_by: requested_by.to_string(),
            removed_agents: 0,
            removed_tasks: 0,
            removed_task_events: 0,
            removed_decisions: 0,
            removed_sessions: 0,
            removed_stream_events: 0,
            removed_agent_names: Vec::new(),
        });
    }

    let summary = {
        let db = shared.db.lock().expect("db mutex poisoned");
        let removed_stream_events = db.delete_stream_events_by_agent_names(&demo_agent_names)?;
        let removed_decisions = db.delete_decisions_by_task_ids(&task_ids)?;
        let removed_task_events = db.delete_task_events_by_task_ids(&task_ids)?;
        let removed_tasks = db.delete_tasks_by_ids(&task_ids)?;
        let removed_sessions = db.delete_sessions_by_agent_names(&demo_agent_names)?;
        let removed_agents = db.delete_agents_by_names(&demo_agent_names)?;

        CleanupSummary {
            requested_by: requested_by.to_string(),
            removed_agents,
            removed_tasks,
            removed_task_events,
            removed_decisions,
            removed_sessions,
            removed_stream_events,
            removed_agent_names: demo_agent_names.clone(),
        }
    };

    let demo_agent_set = demo_agent_names.iter().cloned().collect::<HashSet<_>>();
    let task_id_set = task_ids.iter().cloned().collect::<HashSet<_>>();

    {
        let mut active = shared
            .active_sessions
            .lock()
            .expect("active session mutex poisoned");
        active.retain(|_, control| !demo_agent_set.contains(&control.agent_name));
    }

    {
        let mut state = shared.state.write().await;
        state
            .agents
            .retain(|agent_name, _| !demo_agent_set.contains(agent_name));
        state.tasks.retain(|task_id, task| {
            !task_id_set.contains(task_id)
                && !demo_agent_set.contains(&task.from_agent)
                && !demo_agent_set.contains(&task.to_agent)
        });
        state
            .decisions
            .retain(|_, decision| !task_id_set.contains(&decision.task_id));
        state
            .sessions
            .retain(|_, session| !demo_agent_set.contains(&session.agent_name));
        state
            .recent_streams
            .retain(|event| !demo_agent_set.contains(&event.agent));
    }

    record_stream_event(
        shared,
        requested_by,
        "stdout",
        &format!(
            "[CLEANUP_DEMO_DATA] agents={} tasks={}",
            summary.removed_agents, summary.removed_tasks
        ),
    )
    .await?;
    broadcast_runtime_snapshot(shared).await;

    Ok(summary)
}

async fn repair_runtime_state(shared: &AppShared, requested_by: &str) -> Result<RepairSummary> {
    ensure_registered_agent(shared, requested_by).await?;

    let now = Utc::now();
    let active_session_ids = {
        let active = shared
            .active_sessions
            .lock()
            .expect("active session mutex poisoned");
        active.keys().cloned().collect::<HashSet<_>>()
    };

    let (mut agents, mut tasks, decisions, mut sessions, recent_streams) = {
        let state = shared.state.read().await;
        (
            state.agents.clone(),
            state.tasks.clone(),
            state.decisions.clone(),
            state.sessions.clone(),
            state.recent_streams.clone(),
        )
    };

    let mut repaired_agents = HashSet::new();
    let mut repaired_tasks = HashSet::new();
    let mut repaired_sessions = HashSet::new();
    let mut notes = Vec::new();

    for task in tasks.values_mut() {
        let mut changed = false;
        let original_state = task.state.clone();

        if is_task_terminal(&task.state) && task.closed_at.is_none() {
            task.closed_at = Some(task.updated_at);
            changed = true;
            notes.push(format!(
                "task {} had terminal state {:?} without closed_at; backfilled closed_at",
                task.task_id, original_state
            ));
        } else if !is_task_terminal(&task.state) && task.closed_at.is_some() {
            task.closed_at = None;
            changed = true;
            notes.push(format!(
                "task {} was non-terminal {:?} but had closed_at; cleared it",
                task.task_id, original_state
            ));
        }

        if changed {
            task.updated_at = now;
            let db = shared.db.lock().expect("db mutex poisoned");
            db.update_task(task)?;
            db.insert_task_event(
                &task.task_id,
                "task_repaired",
                Some(original_state.clone()),
                Some(original_state),
                "{}",
            )?;
            repaired_tasks.insert(task.task_id.clone());
        }
    }

    for session in sessions.values_mut() {
        let mut changed = false;

        if session.status == CodexSessionStatus::Running
            && !active_session_ids.contains(&session.session_id)
        {
            session.status = CodexSessionStatus::Failed;
            if session.ended_at.is_none() {
                session.ended_at = Some(now);
            }
            if session.last_output_at.is_none() {
                session.last_output_at = Some(now);
            }
            changed = true;
            notes.push(format!(
                "session {} was marked running without a live handle; normalized to failed",
                session.session_id
            ));
        } else if session.status != CodexSessionStatus::Running && session.ended_at.is_none() {
            session.ended_at = Some(session.last_output_at.unwrap_or(now));
            changed = true;
            notes.push(format!(
                "session {} was finished but missing ended_at; backfilled it",
                session.session_id
            ));
        } else if session.status == CodexSessionStatus::Running
            && session.ended_at.is_some()
            && active_session_ids.contains(&session.session_id)
        {
            session.ended_at = None;
            changed = true;
            notes.push(format!(
                "session {} was running but had ended_at set; cleared it",
                session.session_id
            ));
        }

        if changed {
            let db = shared.db.lock().expect("db mutex poisoned");
            db.upsert_session(session)?;
            repaired_sessions.insert(session.session_id.clone());
        }
    }

    let mut open_tasks_by_agent: HashMap<String, Vec<String>> = HashMap::new();
    for task in tasks.values() {
        if !is_task_terminal(&task.state) {
            open_tasks_by_agent
                .entry(task.to_agent.clone())
                .or_default()
                .push(task.task_id.clone());
        }
    }

    for agent in agents.values_mut() {
        let original_task_id = agent.current_task_id.clone();
        let original_session_id = agent.current_session_id.clone();
        let original_state = agent.state.clone();
        let mut changed = false;

        if let Some(session_id) = agent.current_session_id.clone() {
            let valid_running_session = sessions
                .get(&session_id)
                .map(|session| session.status == CodexSessionStatus::Running)
                .unwrap_or(false);
            if !valid_running_session {
                agent.current_session_id = None;
                changed = true;
                notes.push(format!(
                    "agent {} referenced stale current_session_id {}; cleared it",
                    agent.name, session_id
                ));
            }
        }

        if let Some(task_id) = agent.current_task_id.clone() {
            let valid_open_task = tasks
                .get(&task_id)
                .map(|task| !is_task_terminal(&task.state))
                .unwrap_or(false);
            if !valid_open_task {
                agent.current_task_id = None;
                changed = true;
                notes.push(format!(
                    "agent {} referenced stale current_task_id {}; cleared it",
                    agent.name, task_id
                ));
            }
        }

        if agent.current_task_id.is_none() && agent.current_session_id.is_none() {
            let open_task_ids = open_tasks_by_agent
                .get(&agent.name)
                .cloned()
                .unwrap_or_default();
            if open_task_ids.len() == 1 {
                let rebound_task_id = open_task_ids[0].clone();
                agent.current_task_id = Some(rebound_task_id.clone());
                if let Some(task) = tasks.get(&rebound_task_id) {
                    agent.state = desired_agent_state_for_task(task);
                }
                changed = true;
                notes.push(format!(
                    "agent {} had exactly one open task {}; rebound current_task_id",
                    agent.name, rebound_task_id
                ));
            } else if open_task_ids.is_empty() && agent.state == AgentSessionState::Busy {
                agent.state = AgentSessionState::Idle;
                changed = true;
                notes.push(format!(
                    "agent {} was busy without task or session; normalized to idle",
                    agent.name
                ));
            } else if open_task_ids.len() > 1 {
                notes.push(format!(
                    "agent {} has {} open tasks and was not auto-repaired",
                    agent.name,
                    open_task_ids.len()
                ));
            }
        } else if agent.current_session_id.is_some() && agent.state != AgentSessionState::Busy {
            agent.state = AgentSessionState::Busy;
            changed = true;
            notes.push(format!(
                "agent {} had a live session but state {:?}; normalized to busy",
                agent.name, original_state
            ));
        } else if let Some(task_id) = &agent.current_task_id
            && let Some(task) = tasks.get(task_id)
        {
            let desired_state = desired_agent_state_for_task(task);
            if agent.current_session_id.is_none() && agent.state != desired_state {
                agent.state = desired_state.clone();
                changed = true;
                notes.push(format!(
                    "agent {} state {:?} did not match task {}; normalized to {:?}",
                    agent.name, original_state, task_id, desired_state
                ));
            }
        }

        if changed {
            if original_task_id != agent.current_task_id
                || original_session_id != agent.current_session_id
                || original_state != agent.state
            {
                agent.last_heartbeat_at = Some(now);
            }
            let db = shared.db.lock().expect("db mutex poisoned");
            db.upsert_agent(agent)?;
            repaired_agents.insert(agent.name.clone());
        }
    }

    {
        let mut state = shared.state.write().await;
        state.agents = agents;
        state.tasks = tasks;
        state.decisions = decisions;
        state.sessions = sessions;
        state.recent_streams = recent_streams;
    }

    record_stream_event(
        shared,
        requested_by,
        "stdout",
        &format!(
            "[REPAIR_RUNTIME_STATE] agents={} tasks={} sessions={}",
            repaired_agents.len(),
            repaired_tasks.len(),
            repaired_sessions.len()
        ),
    )
    .await?;
    broadcast_runtime_snapshot(shared).await;

    Ok(RepairSummary {
        requested_by: requested_by.to_string(),
        repaired_agents: repaired_agents.len(),
        repaired_tasks: repaired_tasks.len(),
        repaired_sessions: repaired_sessions.len(),
        notes,
    })
}

async fn cancel_task(shared: &AppShared, task_id: &str, requested_by: &str) -> Result<TaskSummary> {
    ensure_registered_agent(shared, requested_by).await?;

    let task = {
        let state = shared.state.read().await;
        state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?
    };

    let assigned_agent = {
        let state = shared.state.read().await;
        state.agents.get(&task.to_agent).cloned()
    };

    if let Some(agent) = &assigned_agent
        && agent.current_task_id.as_deref() == Some(task_id)
        && agent.current_session_id.is_some()
    {
        bail!(
            "task {task_id} has a live session attached on agent {}; stop-agent-session first",
            agent.name
        );
    }

    let cancelled = transition_any_of(
        shared,
        task_id,
        &[
            TaskState::Pending,
            TaskState::Accepted,
            TaskState::Running,
            TaskState::Completed,
            TaskState::Reported,
            TaskState::Analyzed,
            TaskState::BlockedWaitingDecision,
        ],
        TaskState::Cancelled,
        "task_cancelled",
    )
    .await?;

    if let Some(agent) = assigned_agent
        && agent.current_task_id.as_deref() == Some(task_id)
    {
        release_agent_after_terminal_task(shared, &agent.name, AgentSessionState::Idle).await?;
    }

    let payload = serde_json::json!({ "requested_by": requested_by });
    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.insert_task_event(
            &cancelled.task_id,
            "task_cancel_payload",
            Some(TaskState::Cancelled),
            Some(TaskState::Cancelled),
            &payload.to_string(),
        )?;
    }

    record_stream_event(
        shared,
        requested_by,
        "stdout",
        &format!("[TASK_CANCEL] task={task_id}"),
    )
    .await?;

    Ok(cancelled)
}

async fn retry_task(shared: &AppShared, task_id: &str, requested_by: &str) -> Result<TaskSummary> {
    ensure_registered_agent(shared, requested_by).await?;

    let task = {
        let state = shared.state.read().await;
        state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?
    };

    if !matches!(task.state, TaskState::Failed | TaskState::Cancelled) {
        bail!(
            "task {task_id} must be failed or cancelled before retry, actual {:?}",
            task.state
        );
    }

    let agent = {
        let state = shared.state.read().await;
        state
            .agents
            .get(&task.to_agent)
            .cloned()
            .ok_or_else(|| anyhow!("target agent not registered: {}", task.to_agent))?
    };
    ensure_agent_ready_for_new_task(&agent)?;

    let retried = reopen_task_as_pending(shared, &task, "task_retried").await?;

    let mut changed_agent = None;
    {
        let mut state = shared.state.write().await;
        if let Some(agent) = state.agents.get_mut(&task.to_agent) {
            claim_task_for_agent_summary(agent, &task.task_id);
            changed_agent = Some(agent.clone());
        }
    }

    if let Some(agent) = changed_agent {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&agent)?;
        shared
            .events_tx
            .send(DashboardEvent::AgentStateChanged { agent })
            .ok();
    }

    let payload = serde_json::json!({ "requested_by": requested_by });
    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.insert_task_event(
            &retried.task_id,
            "task_retry_payload",
            Some(TaskState::Pending),
            Some(TaskState::Pending),
            &payload.to_string(),
        )?;
    }

    record_stream_event(
        shared,
        requested_by,
        "stdout",
        &format!(
            "[TASK_RETRY] task={} target={}",
            retried.task_id, retried.to_agent
        ),
    )
    .await?;

    Ok(retried)
}

async fn reopen_task_as_pending(
    shared: &AppShared,
    task: &TaskSummary,
    event_type: &str,
) -> Result<TaskSummary> {
    let mut updated = task.clone();
    let from_state = updated.state.clone();
    updated.state = TaskState::Pending;
    updated.updated_at = Utc::now();
    updated.closed_at = None;

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.update_task(&updated)?;
        db.insert_task_event(
            &updated.task_id,
            event_type,
            Some(from_state),
            Some(TaskState::Pending),
            "{}",
        )?;
    }

    {
        let mut state = shared.state.write().await;
        state.tasks.insert(updated.task_id.clone(), updated.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::TaskEvent {
            task: updated.clone(),
            event_type: event_type.to_string(),
        })
        .ok();

    Ok(updated)
}

fn compose_task_prompt(task: &TaskSummary) -> String {
    format!(
        "You are executing one repository-scoped task. Work only inside the current repository and do not assume cross-repo facts.\n\
\n\
Return exactly one JSON object that matches the provided schema and nothing else.\n\
\n\
Task context:\n\
- task_id: {task_id}\n\
- from: {from_agent}\n\
- to: {to_agent}\n\
- title: {title}\n\
- summary: {summary}\n\
\n\
Interpretation rules:\n\
- Use status=result when you completed the requested work for this round.\n\
- Use status=report when you have a concrete issue, gap, or uncertainty for the main agent to analyze.\n\
- Use status=wait_decision when you must stop and wait before continuing.\n\
- Keep changed_files limited to files you actually changed in this repository.\n\
- Do not wrap the JSON in markdown fences.\n",
        task_id = task.task_id,
        from_agent = task.from_agent,
        to_agent = task.to_agent,
        title = task.title,
        summary = task.summary
    )
}

async fn maybe_auto_resolve_task(
    shared: &AppShared,
    task: &TaskSummary,
    payload: &TaskRoundPayload,
) -> Result<(TaskSummary, Option<DecisionSummary>)> {
    if !matches!(
        payload.status,
        TaskRoundStatus::Report | TaskRoundStatus::WaitDecision
    ) {
        return Ok((task.clone(), None));
    }

    let (analyzer, summary) = match (&task.auto_resolve_by, &task.auto_resolve_summary) {
        (Some(analyzer), Some(summary)) => (analyzer.clone(), summary.clone()),
        _ => return Ok((task.clone(), None)),
    };

    record_stream_event(
        shared,
        &analyzer,
        "stdout",
        &format!(
            "[AUTO_RESOLVE] task={} status={}",
            task.task_id,
            serde_json::to_string(&payload.status).unwrap_or_else(|_| "\"unknown\"".to_string())
        ),
    )
    .await?;

    let (decision, closed_task) =
        resolve_task_for_main(shared, &task.task_id, &analyzer, &summary).await?;
    Ok((closed_task, Some(decision)))
}

async fn analyze_task_for_main(
    shared: &AppShared,
    task_id: &str,
    analyzer: &str,
) -> Result<TaskSummary> {
    let task = transition_any_of(
        shared,
        task_id,
        &[TaskState::Reported, TaskState::BlockedWaitingDecision],
        TaskState::Analyzed,
        "main_analyzed",
    )
    .await?;

    record_stream_event(
        shared,
        analyzer,
        "stdout",
        &format!("[ANALYZE] task={task_id}"),
    )
    .await?;

    Ok(task)
}

async fn send_decision_to_task(
    shared: &AppShared,
    task_id: &str,
    issued_by: &str,
    target_agent: &str,
    summary: &str,
) -> Result<DecisionSummary> {
    let assigned_task = {
        let state = shared.state.read().await;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?;

        if task.to_agent != target_agent {
            bail!(
                "task {task_id} is assigned to {}, not {target_agent}",
                task.to_agent
            );
        }

        if task.state != TaskState::Analyzed {
            bail!(
                "task {task_id} must be in analyzed state, actual {:?}",
                task.state
            );
        }

        task
    };

    let task = transition_task_state(
        shared,
        &assigned_task.task_id,
        &assigned_task.to_agent,
        TaskState::Analyzed,
        TaskState::DecisionSent,
        "decision_sent",
    )
    .await?;

    let decision = DecisionSummary {
        decision_id: format!("D-{}", Uuid::now_v7().simple()),
        task_id: task.task_id.clone(),
        issued_by: issued_by.to_string(),
        target_agent: target_agent.to_string(),
        summary: summary.to_string(),
        status: "sent".to_string(),
        created_at: Utc::now(),
        acknowledged_at: None,
    };

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.insert_decision(&decision, "{}")?;
    }

    {
        let mut state = shared.state.write().await;
        state
            .decisions
            .insert(decision.decision_id.clone(), decision.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::DecisionEvent {
            decision: decision.clone(),
            event_type: "decision_sent".to_string(),
        })
        .ok();

    Ok(decision)
}

async fn resolve_task_for_main(
    shared: &AppShared,
    task_id: &str,
    analyzer: &str,
    summary: &str,
) -> Result<(DecisionSummary, TaskSummary)> {
    let target_agent = {
        let state = shared.state.read().await;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?;

        match task.state {
            TaskState::Reported | TaskState::BlockedWaitingDecision | TaskState::Analyzed => {
                task.to_agent
            }
            _ => {
                bail!(
                    "task {task_id} must be in reported, blocked_waiting_decision, or analyzed state to resolve, actual {:?}",
                    task.state
                )
            }
        }
    };

    let already_analyzed = {
        let state = shared.state.read().await;
        let task = state
            .tasks
            .get(task_id)
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?;
        task.state == TaskState::Analyzed
    };

    if !already_analyzed {
        analyze_task_for_main(shared, task_id, analyzer).await?;
    }

    let decision = send_decision_to_task(shared, task_id, analyzer, &target_agent, summary).await?;
    let task = close_task_after_decision(shared, task_id, &target_agent).await?;

    Ok((decision, task))
}

async fn acknowledge_latest_decision_for_task(
    shared: &AppShared,
    task_id: &str,
    agent_name: &str,
) -> Result<DecisionSummary> {
    let (task, candidate) = {
        let state = shared.state.read().await;
        let task = state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?;

        if task.to_agent != agent_name {
            bail!(
                "task {task_id} is assigned to {}, not {agent_name}",
                task.to_agent
            );
        }

        if !matches!(task.state, TaskState::DecisionSent | TaskState::Closed) {
            bail!(
                "task {task_id} must be in decision_sent or closed state to acknowledge a decision, actual {:?}",
                task.state
            );
        }

        let decisions = state
            .decisions
            .values()
            .filter(|decision| decision.task_id == task_id && decision.target_agent == agent_name)
            .cloned()
            .collect::<Vec<_>>();

        let candidate = decisions
            .iter()
            .filter(|decision| decision.acknowledged_at.is_none())
            .max_by_key(|decision| decision.created_at)
            .cloned()
            .or_else(|| {
                decisions
                    .into_iter()
                    .max_by_key(|decision| decision.created_at)
            })
            .ok_or_else(|| {
                anyhow!("no decision found for task {task_id} and agent {agent_name}")
            })?;

        (task, candidate)
    };

    if candidate.acknowledged_at.is_some() {
        return Ok(candidate);
    }

    let mut updated = candidate.clone();
    updated.status = "acknowledged".to_string();
    updated.acknowledged_at = Some(Utc::now());

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.update_decision_acknowledged(&updated)?;
    }

    {
        let mut state = shared.state.write().await;
        state
            .decisions
            .insert(updated.decision_id.clone(), updated.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::DecisionEvent {
            decision: updated.clone(),
            event_type: "decision_acknowledged".to_string(),
        })
        .ok();

    record_stream_event(
        shared,
        agent_name,
        "stdout",
        &format!(
            "[DECISION_ACK] task={} decision={}",
            task.task_id, updated.decision_id
        ),
    )
    .await?;

    Ok(updated)
}

async fn close_task_after_decision(
    shared: &AppShared,
    task_id: &str,
    agent_name: &str,
) -> Result<TaskSummary> {
    acknowledge_latest_decision_for_task(shared, task_id, agent_name).await?;

    let task = transition_task_state(
        shared,
        task_id,
        agent_name,
        TaskState::DecisionSent,
        TaskState::Closed,
        "task_closed",
    )
    .await?;

    let mut changed_agent = None;
    {
        let mut state = shared.state.write().await;
        if let Some(agent_summary) = state.agents.get_mut(agent_name) {
            agent_summary.current_task_id = None;
            agent_summary.state = AgentSessionState::Idle;
            agent_summary.last_output_at = Some(Utc::now());
            changed_agent = Some(agent_summary.clone());
        }
    }

    if let Some(agent_summary) = changed_agent {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&agent_summary)?;
        shared
            .events_tx
            .send(DashboardEvent::AgentStateChanged {
                agent: agent_summary,
            })
            .ok();
    }

    Ok(task)
}

async fn upsert_session_state(
    shared: &AppShared,
    session: SessionSummary,
    event_type: &str,
) -> Result<()> {
    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_session(&session)?;
    }

    {
        let mut state = shared.state.write().await;
        state
            .sessions
            .insert(session.session_id.clone(), session.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::SessionEvent {
            session,
            event_type: event_type.to_string(),
        })
        .ok();

    Ok(())
}

async fn set_agent_current_session(
    shared: &AppShared,
    agent_name: &str,
    session_id: Option<String>,
) -> Result<()> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    updated.current_session_id = session_id;
    updated.last_heartbeat_at = Some(Utc::now());

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&updated)?;
    }

    {
        let mut state = shared.state.write().await;
        state.agents.insert(agent_name.to_string(), updated.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::AgentStateChanged { agent: updated })
        .ok();

    Ok(())
}

fn register_active_session(
    shared: &AppShared,
    session_id: &str,
    agent_name: &str,
    stop: BackendStopSignal,
) {
    let mut active = shared
        .active_sessions
        .lock()
        .expect("active session mutex poisoned");
    active.insert(
        session_id.to_string(),
        ActiveSessionControl {
            agent_name: agent_name.to_string(),
            stop,
        },
    );
}

fn unregister_active_session(shared: &AppShared, session_id: &str) {
    let mut active = shared
        .active_sessions
        .lock()
        .expect("active session mutex poisoned");
    active.remove(session_id);
}

async fn update_session_thread_id(
    shared: &AppShared,
    session_id: &str,
    thread_id: &str,
) -> Result<()> {
    let mut updated = {
        let state = shared.state.read().await;
        match state.sessions.get(session_id).cloned() {
            Some(session) => session,
            None => return Ok(()),
        }
    };

    updated.thread_id = Some(thread_id.to_string());
    upsert_session_state(shared, updated, "session_thread_attached").await
}

async fn touch_session_output(
    shared: &AppShared,
    session_id: &str,
    at: chrono::DateTime<Utc>,
) -> Result<()> {
    let mut updated = {
        let state = shared.state.read().await;
        match state.sessions.get(session_id).cloned() {
            Some(session) => session,
            None => return Ok(()),
        }
    };

    updated.last_output_at = Some(at);

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_session(&updated)?;
    }

    {
        let mut state = shared.state.write().await;
        state.sessions.insert(session_id.to_string(), updated);
    }

    Ok(())
}

async fn finalize_session_state(
    shared: &AppShared,
    session_id: &str,
    status: CodexSessionStatus,
    ended_at: chrono::DateTime<Utc>,
) -> Result<()> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| anyhow!("session not found: {session_id}"))?
    };

    updated.status = status.clone();
    updated.ended_at = Some(ended_at);
    if updated.last_output_at.is_none() {
        updated.last_output_at = Some(ended_at);
    }

    let event_type = match status {
        CodexSessionStatus::Succeeded => "session_succeeded",
        CodexSessionStatus::Failed => "session_failed",
        CodexSessionStatus::Running => "session_running",
    };

    let agent_name = updated.agent_name.clone();
    upsert_session_state(shared, updated, event_type).await?;
    set_agent_current_session(shared, &agent_name, None).await?;
    Ok(())
}

async fn stop_agent_session(shared: &AppShared, agent_name: &str) -> Result<String> {
    let session_id = {
        let state = shared.state.read().await;
        let agent = state
            .agents
            .get(agent_name)
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?;

        agent
            .current_session_id
            .clone()
            .ok_or_else(|| anyhow!("agent {agent_name} has no running session"))?
    };

    let control = {
        let mut active = shared
            .active_sessions
            .lock()
            .expect("active session mutex poisoned");
        active.remove(&session_id)
    };

    let control = match control {
        Some(control) => control,
        None => {
            let state = shared.state.read().await;
            let session = state
                .sessions
                .get(&session_id)
                .ok_or_else(|| anyhow!("session not found: {session_id}"))?;
            if session.status == CodexSessionStatus::Running {
                bail!(
                    "session {session_id} is marked running but has no live stop handle; restart cleanup may have detached it"
                );
            }
            bail!("session {session_id} is no longer running");
        }
    };

    if control.agent_name != agent_name {
        bail!(
            "session {session_id} belongs to {}, not {agent_name}",
            control.agent_name
        );
    }

    control.stop.stop()?;
    record_stream_event_with_session(
        shared,
        Some(&session_id),
        agent_name,
        "stdout",
        "[SESSION_STOP_REQUESTED]",
    )
    .await?;

    Ok(session_id)
}

async fn recover_agent(shared: &AppShared, agent_name: &str) -> Result<AgentSummary> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    if updated.current_task_id.is_some() {
        bail!("agent {agent_name} still has an in-flight task and cannot be recovered");
    }
    if updated.current_session_id.is_some() {
        bail!("agent {agent_name} still has a live session and cannot be recovered");
    }
    if updated.state != AgentSessionState::Blocked {
        bail!(
            "agent {agent_name} must be blocked before recovery, actual {:?}",
            updated.state
        );
    }

    updated.state = AgentSessionState::Idle;
    updated.last_heartbeat_at = Some(Utc::now());

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&updated)?;
    }

    {
        let mut state = shared.state.write().await;
        state.agents.insert(agent_name.to_string(), updated.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::AgentStateChanged {
            agent: updated.clone(),
        })
        .ok();
    record_stream_event(shared, agent_name, "stdout", "[AGENT_RECOVERED]").await?;

    Ok(updated)
}

async fn reset_agent_thread(shared: &AppShared, agent_name: &str) -> Result<AgentSummary> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    if updated.current_session_id.is_some() {
        bail!("agent {agent_name} still has a live session and cannot reset thread");
    }
    if updated.current_task_id.is_some() {
        bail!("agent {agent_name} still has an in-flight task and cannot reset thread");
    }

    updated.thread_id = None;
    updated.last_heartbeat_at = Some(Utc::now());

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&updated)?;
    }

    {
        let mut state = shared.state.write().await;
        state.agents.insert(agent_name.to_string(), updated.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::AgentStateChanged {
            agent: updated.clone(),
        })
        .ok();
    record_stream_event(shared, agent_name, "stdout", "[THREAD_RESET]").await?;

    Ok(updated)
}

fn is_session_stop_error(err: &anyhow::Error) -> bool {
    err.to_string()
        .contains("codex session stop requested for agent")
}

async fn recover_stale_sessions_on_startup(shared: &AppShared) -> Result<()> {
    let stale_sessions = {
        let state = shared.state.read().await;
        state
            .sessions
            .values()
            .filter(|session| session.status == CodexSessionStatus::Running)
            .cloned()
            .collect::<Vec<_>>()
    };

    if stale_sessions.is_empty() {
        return Ok(());
    }

    let now = Utc::now();
    for session in stale_sessions {
        finalize_session_state(shared, &session.session_id, CodexSessionStatus::Failed, now)
            .await?;

        let agent = {
            let state = shared.state.read().await;
            state.agents.get(&session.agent_name).cloned()
        };

        if let Some(mut agent) = agent {
            if agent.current_session_id.is_none() && agent.current_task_id.is_some() {
                agent.state = AgentSessionState::Blocked;
                agent.last_heartbeat_at = Some(now);

                {
                    let db = shared.db.lock().expect("db mutex poisoned");
                    db.upsert_agent(&agent)?;
                }

                {
                    let mut state = shared.state.write().await;
                    state.agents.insert(agent.name.clone(), agent.clone());
                }

                shared
                    .events_tx
                    .send(DashboardEvent::AgentStateChanged { agent })
                    .ok();
            }
        }
    }

    Ok(())
}

async fn record_stream_event(
    shared: &AppShared,
    agent_name: &str,
    stream: &str,
    content: &str,
) -> Result<()> {
    record_stream_event_with_session(shared, None, agent_name, stream, content).await
}

async fn record_stream_event_with_session(
    shared: &AppShared,
    session_id: Option<&str>,
    agent_name: &str,
    stream: &str,
    content: &str,
) -> Result<()> {
    let event = StreamEventRecord {
        session_id: session_id.map(ToString::to_string),
        agent: agent_name.to_string(),
        stream: stream.to_string(),
        content: content.to_string(),
        at: Utc::now(),
    };

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.append_stream_event(session_id, agent_name, stream, content)?;
    }

    if let Some(session_id) = session_id {
        touch_session_output(shared, session_id, event.at).await?;
    }

    {
        let mut state = shared.state.write().await;
        state.recent_streams.push(event.clone());
        if state.recent_streams.len() > MAX_RECENT_STREAMS {
            let drop_count = state.recent_streams.len() - MAX_RECENT_STREAMS;
            state.recent_streams.drain(0..drop_count);
        }
    }

    shared
        .events_tx
        .send(DashboardEvent::StreamChunk { event })
        .ok();

    Ok(())
}

async fn set_agent_runtime_state(
    shared: &AppShared,
    agent_name: &str,
    new_state: AgentSessionState,
) -> Result<()> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    updated.state = new_state;
    updated.last_heartbeat_at = Some(Utc::now());

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&updated)?;
    }

    {
        let mut state = shared.state.write().await;
        state.agents.insert(agent_name.to_string(), updated.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::AgentStateChanged { agent: updated })
        .ok();

    Ok(())
}

async fn update_agent_after_round(
    shared: &AppShared,
    agent_name: &str,
    thread_id: Option<String>,
    state_value: AgentSessionState,
    completed_at: chrono::DateTime<Utc>,
) -> Result<()> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    updated.thread_id = thread_id;
    updated.state = state_value;
    updated.last_output_at = Some(completed_at);
    updated.last_heartbeat_at = Some(Utc::now());

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&updated)?;
    }

    {
        let mut state = shared.state.write().await;
        state.agents.insert(agent_name.to_string(), updated.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::AgentStateChanged { agent: updated })
        .ok();

    Ok(())
}

async fn release_agent_after_terminal_task(
    shared: &AppShared,
    agent_name: &str,
    state_value: AgentSessionState,
) -> Result<()> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    updated.current_task_id = None;
    updated.state = state_value;
    updated.last_heartbeat_at = Some(Utc::now());

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&updated)?;
    }

    {
        let mut state = shared.state.write().await;
        state.agents.insert(agent_name.to_string(), updated.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::AgentStateChanged { agent: updated })
        .ok();

    Ok(())
}

async fn execute_codex_round(
    shared: &AppShared,
    agent: &AgentSummary,
    prompt: &str,
    output_schema: Option<String>,
) -> Result<AgentRoundResult> {
    let backend = start_backend(BackendStartRequest {
        agent: agent.clone(),
        prompt: prompt.to_string(),
        output_schema,
        session_mode: SessionMode::Round,
    })?;
    let crate::backend::BackendHandle {
        session_mode,
        pid,
        stop,
        mut events,
        completion,
    } = backend;

    let session = SessionSummary {
        session_id: format!("S-{}", Uuid::now_v7().simple()),
        agent_name: agent.name.clone(),
        session_mode,
        pid,
        thread_id: agent.thread_id.clone(),
        status: CodexSessionStatus::Running,
        started_at: Utc::now(),
        ended_at: None,
        last_output_at: None,
    };
    upsert_session_state(shared, session.clone(), "session_started").await?;
    set_agent_current_session(shared, &agent.name, Some(session.session_id.clone())).await?;
    register_active_session(shared, &session.session_id, &agent.name, stop);

    while let Some(event) = events.recv().await {
        match event {
            BackendEvent::Line { stream, line } => match stream {
                BackendStream::Stdout => {
                    record_stream_event_with_session(
                        shared,
                        Some(&session.session_id),
                        &agent.name,
                        "stdout",
                        &line,
                    )
                    .await?;
                }
                BackendStream::Stderr => {
                    record_stream_event_with_session(
                        shared,
                        Some(&session.session_id),
                        &agent.name,
                        "stderr",
                        &line,
                    )
                    .await?;
                }
            },
            BackendEvent::ThreadStarted { thread_id } => {
                update_session_thread_id(shared, &session.session_id, &thread_id).await?;
            }
        }
    }

    let finished = completion
        .await
        .context("backend round task join failed")??;
    unregister_active_session(shared, &session.session_id);
    finalize_session_state(
        shared,
        &session.session_id,
        if finished.status.success() {
            CodexSessionStatus::Succeeded
        } else {
            CodexSessionStatus::Failed
        },
        Utc::now(),
    )
    .await?;

    if finished.stopped {
        bail!("codex session stop requested for agent {}", agent.name);
    }

    if !finished.status.success() {
        let stderr_summary = finished
            .stderr_lines
            .iter()
            .rev()
            .find(|line| !line.trim().is_empty())
            .cloned();
        let detail = finished
            .parsed
            .error_message
            .clone()
            .or(stderr_summary)
            .unwrap_or_else(|| format!("process exited with status {}", finished.status));
        bail!("codex round failed for agent {}: {}", agent.name, detail);
    }

    finished.parsed.into_round_result(&agent.name, Utc::now())
}

async fn transition_task_state(
    shared: &AppShared,
    task_id: &str,
    agent: &str,
    expected: TaskState,
    next: TaskState,
    event_type: &str,
) -> Result<TaskSummary> {
    let task = {
        let state = shared.state.read().await;
        let task = state
            .tasks
            .get(task_id)
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?;

        if task.to_agent != agent {
            bail!(
                "task {task_id} is assigned to {}, not {agent}",
                task.to_agent
            );
        }
        if task.state != expected {
            bail!(
                "task {task_id} must be in state {:?}, actual {:?}",
                expected,
                task.state
            );
        }
        task.clone()
    };

    transition_impl(shared, task, next, event_type).await
}

async fn transition_any_of(
    shared: &AppShared,
    task_id: &str,
    expected: &[TaskState],
    next: TaskState,
    event_type: &str,
) -> Result<TaskSummary> {
    let task = {
        let state = shared.state.read().await;
        let task = state
            .tasks
            .get(task_id)
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?;

        if !expected.contains(&task.state) {
            bail!(
                "task {task_id} must be in one of {:?}, actual {:?}",
                expected,
                task.state
            );
        }
        task.clone()
    };

    transition_impl(shared, task, next, event_type).await
}

async fn transition_impl(
    shared: &AppShared,
    mut task: TaskSummary,
    next: TaskState,
    event_type: &str,
) -> Result<TaskSummary> {
    let from_state = task.state.clone();
    task.state = next.clone();
    task.updated_at = Utc::now();
    if matches!(
        next,
        TaskState::Closed | TaskState::Cancelled | TaskState::Failed
    ) {
        task.closed_at = Some(Utc::now());
    }

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.update_task(&task)?;
        db.insert_task_event(
            &task.task_id,
            event_type,
            Some(from_state),
            Some(next),
            "{}",
        )?;
    }

    {
        let mut state = shared.state.write().await;
        state.tasks.insert(task.task_id.clone(), task.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::TaskEvent {
            task: task.clone(),
            event_type: event_type.to_string(),
        })
        .ok();

    Ok(task)
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_agent_ready_for_ad_hoc_round, ensure_agent_ready_for_new_task,
        ensure_agent_ready_for_task_round,
    };
    use crate::models::{AgentRole, AgentSessionState, AgentSummary};

    fn sample_agent() -> AgentSummary {
        AgentSummary {
            name: "child".to_string(),
            role: AgentRole::Child,
            repo_name: Some("repo".to_string()),
            cwd: ".".to_string(),
            thread_id: None,
            current_session_id: None,
            state: AgentSessionState::Idle,
            current_task_id: None,
            last_output_at: None,
            last_heartbeat_at: None,
        }
    }

    #[test]
    fn idle_agent_is_ready_for_new_task_and_ad_hoc_round() {
        let agent = sample_agent();
        assert!(ensure_agent_ready_for_new_task(&agent).is_ok());
        assert!(ensure_agent_ready_for_ad_hoc_round(&agent).is_ok());
    }

    #[test]
    fn blocked_agent_is_rejected_for_new_task() {
        let mut agent = sample_agent();
        agent.state = AgentSessionState::Blocked;
        let err = ensure_agent_ready_for_new_task(&agent).unwrap_err();
        assert!(err.to_string().contains("must be idle"));
    }

    #[test]
    fn ad_hoc_round_rejects_agent_with_live_session() {
        let mut agent = sample_agent();
        agent.current_session_id = Some("S-1".to_string());
        let err = ensure_agent_ready_for_ad_hoc_round(&agent).unwrap_err();
        assert!(err.to_string().contains("live session"));
    }

    #[test]
    fn task_round_requires_matching_busy_assignment() {
        let mut agent = sample_agent();
        agent.state = AgentSessionState::Busy;
        agent.current_task_id = Some("T-1".to_string());
        assert!(ensure_agent_ready_for_task_round(&agent, "T-1").is_ok());

        let err = ensure_agent_ready_for_task_round(&agent, "T-2").unwrap_err();
        assert!(err.to_string().contains("not currently assigned"));
    }
}
