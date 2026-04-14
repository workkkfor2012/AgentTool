use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::{RwLock, broadcast, mpsc};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::backend::{
    AppServerStartRequest, BackendEvent, BackendStartRequest, BackendStopSignal, BackendStream,
    run_remote_bootstrap_round, start_app_server_backend, start_backend,
    wait_for_app_server_ready,
};
use crate::config::{AppConfig, RuntimeEndpointRecord};
use crate::control::{ControlRequest, ControlResponse};
use crate::db::Database;
use crate::models::{
    AgentBootstrapState, AgentContextSource, AgentContextSourceKind, AgentContextView, AgentRole,
    AgentRoundResult, AgentSessionState, AgentSummary, AppServerOwner, BridgeClientMessage,
    BridgeConnectionState, BridgeDelivery, BridgeDeliveryKind, BridgeMode, BridgeServerMessage,
    BridgeSyncSnapshot, CleanupSummary, CodexSessionStatus, DashboardEvent, DashboardSnapshot,
    DecisionSummary, RemoveAgentSummary, RepairSummary, RuntimeEventRecord, RuntimeTraceView,
    SessionMode, SessionSummary, StreamEventRecord, TaskContextView, TaskDraftPayload,
    TaskRoundPayload, TaskRoundStatus, TaskState, TaskSummary, VisiblePaneKind, WsClientMessage,
};

#[derive(Clone)]
pub struct AppShared {
    config: Arc<AppConfig>,
    state: Arc<RwLock<RuntimeState>>,
    db: Arc<Mutex<Database>>,
    events_tx: broadcast::Sender<DashboardEvent>,
    active_sessions: Arc<Mutex<HashMap<String, ActiveSessionControl>>>,
    active_managed_sessions: Arc<Mutex<HashMap<String, ActiveManagedSessionControl>>>,
    active_bridges: Arc<Mutex<HashMap<String, ActiveBridgeSession>>>,
    bridge_queues: Arc<Mutex<HashMap<String, BridgeDeliveryQueue>>>,
}

pub struct RuntimeState {
    pub agents: HashMap<String, AgentSummary>,
    pub tasks: HashMap<String, TaskSummary>,
    pub decisions: HashMap<String, DecisionSummary>,
    pub sessions: HashMap<String, SessionSummary>,
    pub recent_streams: Vec<StreamEventRecord>,
    pub recent_runtime_events: Vec<RuntimeEventRecord>,
}

struct ActiveSessionControl {
    agent_name: String,
    stop: BackendStopSignal,
}

struct ActiveManagedSessionControl {
    session_id: String,
    stop: BackendStopSignal,
}

struct ActiveBridgeSession {
    session_id: String,
    sender: mpsc::UnboundedSender<BridgeServerMessage>,
}

struct BridgeDeliveryQueue {
    next_delivery_id: u64,
    pending: BTreeMap<u64, BridgeDelivery>,
}

#[derive(Debug, Clone, Deserialize)]
struct WindowsProcessInfo {
    process_id: u32,
    name: Option<String>,
    executable_path: Option<String>,
    command_line: Option<String>,
}

const MAX_RECENT_STREAMS: usize = 500;
const MAX_RECENT_RUNTIME_EVENTS: usize = 500;

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
            recent_runtime_events: self.recent_runtime_events.clone(),
            generated_at: Utc::now(),
        }
    }
}

async fn ensure_bridge_queue_exists(shared: &AppShared, agent: &AgentSummary) {
    let mut queues = shared.bridge_queues.lock().expect("bridge queue mutex poisoned");
    queues
        .entry(agent.name.clone())
        .or_insert_with(|| BridgeDeliveryQueue {
            next_delivery_id: agent.bridge_last_delivery_id,
            pending: BTreeMap::new(),
        });
}

pub async fn run_agentd(config: AppConfig) -> Result<()> {
    let mut config = config;
    config.ensure_dirs()?;

    let ws_listener = TcpListener::bind(config.ws_bind)
        .await
        .with_context(|| format!("failed to bind websocket listener at {}", config.ws_bind))?;
    let control_listener = TcpListener::bind(config.control_bind)
        .await
        .with_context(|| format!("failed to bind control listener at {}", config.control_bind))?;
    config.ws_bind = ws_listener
        .local_addr()
        .context("failed to resolve websocket listener local addr")?;
    config.control_bind = control_listener
        .local_addr()
        .context("failed to resolve control listener local addr")?;

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
    let existing_runtime_events = db.load_recent_runtime_events(200)?;

    let (events_tx, _) = broadcast::channel(512);
    let bridge_queues = existing_agents
        .iter()
        .map(|agent| {
            (
                agent.name.clone(),
                BridgeDeliveryQueue {
                    next_delivery_id: agent.bridge_last_delivery_id,
                    pending: BTreeMap::new(),
                },
            )
        })
        .collect::<HashMap<_, _>>();
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
            recent_runtime_events: existing_runtime_events,
        })),
        db: Arc::new(Mutex::new(db)),
        events_tx,
        active_sessions: Arc::new(Mutex::new(HashMap::new())),
        active_managed_sessions: Arc::new(Mutex::new(HashMap::new())),
        active_bridges: Arc::new(Mutex::new(HashMap::new())),
        bridge_queues: Arc::new(Mutex::new(bridge_queues)),
    };

    sync_agent_prompt_defaults_on_startup(&shared).await?;
    sync_task_latest_decision_fields_on_startup(&shared).await?;
    ensure_main_agent(&shared).await?;
    recover_stale_sessions_on_startup(&shared).await?;
    normalize_runtime_state_on_startup(&shared).await?;
    recover_stale_visible_panes_on_startup(&shared).await?;

    config.write_runtime_endpoint(&RuntimeEndpointRecord {
        ws_addr: config.ws_bind.to_string(),
        control_addr: config.control_bind.to_string(),
        pid: std::process::id(),
        started_at: Utc::now(),
    })?;

    let ws_shared = shared.clone();
    let control_shared = shared.clone();

    info!("agentd websocket listening on {}", config.ws_bind);
    info!("agentd control listening on {}", config.control_bind);

    let ws_task = tokio::spawn(async move { run_ws_server(ws_listener, ws_shared).await });
    let control_task =
        tokio::spawn(async move { run_control_server(control_listener, control_shared).await });

    let run_result: Result<()> = tokio::select! {
        result = ws_task => {
            result??;
            Ok(())
        },
        result = control_task => {
            result??;
            Ok(())
        },
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for ctrl_c")?;
            info!("received ctrl_c, shutting down agentd");
            Ok(())
        }
    };

    let _ = config.clear_runtime_endpoint_if_matches(std::process::id());
    run_result
}

async fn ensure_main_agent(shared: &AppShared) -> Result<()> {
    let existing = shared.state.read().await.agents.get("main").cloned();
    let main_cwd = detect_main_agent_cwd(&shared.config);
    let main_prompt_path = default_prompt_path_for_role(&AgentRole::Main, &main_cwd)
        .map(|path| path.to_string_lossy().to_string());
    let mut main_agent = existing.clone().unwrap_or(AgentSummary {
        name: "main".to_string(),
        role: AgentRole::Main,
        repo_name: Some("hackman".to_string()),
        cwd: main_cwd.to_string_lossy().to_string(),
        prompt_path: main_prompt_path.clone(),
        thread_id: None,
        app_server_url: None,
        app_server_owner: None,
        app_server_registered_at: None,
        visible_pane_pid: None,
        visible_pane_kind: None,
        visible_pane_registered_at: None,
        current_session_id: None,
        state: AgentSessionState::Idle,
        bootstrap_state: AgentBootstrapState::AwaitingInit,
        bootstrap_summary: None,
        bootstrap_completed_at: None,
        current_task_id: None,
        last_output_at: None,
        last_heartbeat_at: Some(Utc::now()),
        bridge_state: BridgeConnectionState::Disconnected,
        bridge_mode: None,
        bridge_session_id: None,
        bridge_connected_at: None,
        bridge_last_seen_at: None,
        bridge_last_delivery_id: 0,
        bridge_last_ack_delivery_id: 0,
        bridge_pending_delivery_count: 0,
    });

    let mut changed = existing.is_none();
    if main_agent.cwd == "." {
        main_agent.cwd = main_cwd.to_string_lossy().to_string();
        changed = true;
    }
    if main_agent.prompt_path.is_none() && main_prompt_path.is_some() {
        main_agent.prompt_path = main_prompt_path;
        changed = true;
    }

    if !changed {
        return Ok(());
    }

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
    ensure_bridge_queue_exists(shared, &main_agent).await;

    shared
        .events_tx
        .send(DashboardEvent::AgentStateChanged { agent: main_agent })
        .ok();

    Ok(())
}

fn detect_main_agent_cwd(config: &AppConfig) -> PathBuf {
    let sibling_hackman = config
        .root_dir
        .parent()
        .map(|parent| parent.join("hackman"));
    match sibling_hackman {
        Some(path) if path.is_dir() => path,
        _ => config.root_dir.clone(),
    }
}

fn default_prompt_path_for_role(role: &AgentRole, cwd: &Path) -> Option<PathBuf> {
    let file_name = match role {
        AgentRole::Main => "MAIN_AGENT_PROMPT.md",
        AgentRole::Child => "SUBAGENT_PROMPT.md",
    };
    let path = cwd.join(file_name);
    path.is_file().then_some(path)
}

fn normalize_prompt_path(
    cwd: &str,
    prompt_path: Option<String>,
    role: &AgentRole,
) -> Option<String> {
    if let Some(prompt_path) = prompt_path {
        let prompt = PathBuf::from(&prompt_path);
        if prompt.is_absolute() {
            return Some(prompt.to_string_lossy().to_string());
        }
        return Some(
            PathBuf::from(cwd)
                .join(prompt)
                .to_string_lossy()
                .to_string(),
        );
    }

    default_prompt_path_for_role(role, Path::new(cwd))
        .map(|path| path.to_string_lossy().to_string())
}

async fn sync_agent_prompt_defaults_on_startup(shared: &AppShared) -> Result<()> {
    let updates = {
        let state = shared.state.read().await;
        state
            .agents
            .values()
            .filter_map(|agent| {
                let mut updated = agent.clone();
                let mut changed = false;

                if agent.name == "main" && agent.cwd == "." {
                    updated.cwd = detect_main_agent_cwd(&shared.config)
                        .to_string_lossy()
                        .to_string();
                    changed = true;
                }

                let normalized_prompt_path =
                    normalize_prompt_path(&updated.cwd, updated.prompt_path.clone(), &updated.role);
                if updated.prompt_path != normalized_prompt_path {
                    updated.prompt_path = normalized_prompt_path;
                    changed = true;
                }

                changed.then_some(updated)
            })
            .collect::<Vec<_>>()
    };

    if updates.is_empty() {
        return Ok(());
    }

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        for agent in &updates {
            db.upsert_agent(agent)?;
        }
    }

    {
        let mut state = shared.state.write().await;
        for agent in updates {
            state.agents.insert(agent.name.clone(), agent);
        }
    }

    Ok(())
}

async fn run_ws_server(listener: TcpListener, shared: AppShared) -> Result<()> {
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/bridge", get(bridge_ws_handler))
        .with_state(shared);

    axum::serve(listener, app)
        .await
        .context("websocket server crashed")
}

async fn ws_handler(ws: WebSocketUpgrade, State(shared): State<AppShared>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_session(socket, shared))
}

async fn bridge_ws_handler(
    ws: WebSocketUpgrade,
    State(shared): State<AppShared>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| bridge_ws_session(socket, shared))
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
                                WsClientMessage::ControlRequest { request_id, request } => {
                                    let response = handle_control_request(shared.clone(), request).await;
                                    let payload = json!({
                                        "type": "control_response",
                                        "request_id": request_id,
                                        "response": response,
                                    })
                                    .to_string();
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

async fn bridge_ws_session(socket: WebSocket, shared: AppShared) {
    let (mut writer, mut reader) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<BridgeServerMessage>();
    let writer_task = tokio::spawn(async move {
        while let Some(message) = out_rx.recv().await {
            let payload = match serde_json::to_string(&message) {
                Ok(payload) => payload,
                Err(err) => {
                    warn!("failed to serialize bridge message: {err}");
                    continue;
                }
            };

            if writer.send(Message::Text(payload.into())).await.is_err() {
                break;
            }
        }
    });

    let mut registered_agent: Option<String> = None;
    let mut registered_session_id: Option<String> = None;

    while let Some(incoming) = reader.next().await {
        match incoming {
            Ok(Message::Text(text)) => {
                let parsed = match serde_json::from_str::<BridgeClientMessage>(&text) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        out_tx
                            .send(BridgeServerMessage::Error {
                                message: format!("invalid bridge message: {err}"),
                            })
                            .ok();
                        continue;
                    }
                };

                match parsed {
                    BridgeClientMessage::Hello {
                        agent,
                        mode,
                        last_ack_delivery_id,
                    } => {
                        if let (Some(previous_agent), Some(previous_session_id)) =
                            (&registered_agent, &registered_session_id)
                        {
                            disconnect_bridge_session(
                                &shared,
                                previous_agent,
                                previous_session_id,
                            )
                            .await;
                        }

                        match register_bridge_session(
                            &shared,
                            &agent,
                            mode,
                            out_tx.clone(),
                            last_ack_delivery_id,
                        )
                        .await
                        {
                            Ok((session_id, snapshot, pending_deliveries)) => {
                                registered_agent = Some(agent);
                                registered_session_id = Some(session_id.clone());
                                out_tx
                                    .send(BridgeServerMessage::Welcome {
                                        session_id,
                                        snapshot,
                                        pending_deliveries,
                                    })
                                    .ok();
                            }
                            Err(err) => {
                                out_tx
                                    .send(BridgeServerMessage::Error {
                                        message: err.to_string(),
                                    })
                                    .ok();
                            }
                        }
                    }
                    BridgeClientMessage::Heartbeat { session_id } => {
                        let Some(agent_name) = registered_agent.as_deref() else {
                            out_tx
                                .send(BridgeServerMessage::Error {
                                    message: "bridge heartbeat received before hello".to_string(),
                                })
                                .ok();
                            continue;
                        };

                        if let Err(err) =
                            touch_bridge_session(&shared, agent_name, &session_id).await
                        {
                            out_tx
                                .send(BridgeServerMessage::Error {
                                    message: err.to_string(),
                                })
                                .ok();
                        }
                    }
                    BridgeClientMessage::DeliveryAck {
                        session_id,
                        delivery_id,
                    } => {
                        let Some(agent_name) = registered_agent.as_deref() else {
                            out_tx
                                .send(BridgeServerMessage::Error {
                                    message: "bridge ack received before hello".to_string(),
                                })
                                .ok();
                            continue;
                        };

                        if let Err(err) = acknowledge_bridge_deliveries(
                            &shared,
                            agent_name,
                            Some(&session_id),
                            delivery_id,
                        )
                        .await
                        {
                            out_tx
                                .send(BridgeServerMessage::Error {
                                    message: err.to_string(),
                                })
                                .ok();
                        }
                    }
                    BridgeClientMessage::RequestSync { session_id } => {
                        let Some(agent_name) = registered_agent.as_deref() else {
                            out_tx
                                .send(BridgeServerMessage::Error {
                                    message: "bridge sync requested before hello".to_string(),
                                })
                                .ok();
                            continue;
                        };

                        match build_bridge_sync_message(&shared, agent_name, &session_id, "manual_sync")
                            .await
                        {
                            Ok(message) => {
                                out_tx.send(message).ok();
                            }
                            Err(err) => {
                                out_tx
                                    .send(BridgeServerMessage::Error {
                                        message: err.to_string(),
                                    })
                                    .ok();
                            }
                        }
                    }
                    BridgeClientMessage::Ping => {
                        out_tx.send(BridgeServerMessage::Pong).ok();
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(err) => {
                warn!("bridge websocket receive error: {err}");
                break;
            }
        }
    }

    if let (Some(agent_name), Some(session_id)) =
        (registered_agent.as_deref(), registered_session_id.as_deref())
    {
        disconnect_bridge_session(&shared, agent_name, session_id).await;
    }

    writer_task.abort();
}

async fn register_bridge_session(
    shared: &AppShared,
    agent_name: &str,
    mode: BridgeMode,
    sender: mpsc::UnboundedSender<BridgeServerMessage>,
    last_ack_delivery_id: Option<u64>,
) -> Result<(String, BridgeSyncSnapshot, Vec<BridgeDelivery>)> {
    ensure_registered_agent(shared, agent_name).await?;

    let agent_for_queue = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };
    ensure_bridge_queue_exists(shared, &agent_for_queue).await;

    if let Some(delivery_id) = last_ack_delivery_id {
        acknowledge_bridge_deliveries(shared, agent_name, None, delivery_id).await?;
    }

    let session_id = format!("B-{}", Uuid::now_v7().simple());
    {
        let mut bridges = shared
            .active_bridges
            .lock()
            .expect("bridge session mutex poisoned");
        bridges.insert(
            agent_name.to_string(),
            ActiveBridgeSession {
                session_id: session_id.clone(),
                sender,
            },
        );
    }

    update_agent_bridge_presence(
        shared,
        agent_name,
        BridgeConnectionState::Connected,
        Some(mode),
        Some(session_id.clone()),
    )
    .await?;

    let snapshot = build_bridge_sync_snapshot(shared, agent_name).await?;
    let pending_deliveries = pending_bridge_deliveries(shared, agent_name).await?;
    Ok((session_id, snapshot, pending_deliveries))
}

async fn disconnect_bridge_session(shared: &AppShared, agent_name: &str, session_id: &str) {
    let removed = {
        let mut bridges = shared
            .active_bridges
            .lock()
            .expect("bridge session mutex poisoned");
        if bridges
            .get(agent_name)
            .map(|session| session.session_id.as_str())
            == Some(session_id)
        {
            bridges.remove(agent_name);
            true
        } else {
            false
        }
    };

    if removed {
        if let Err(err) = update_agent_bridge_presence(
            shared,
            agent_name,
            BridgeConnectionState::Disconnected,
            None,
            None,
        )
        .await
        {
            warn!("failed to persist bridge disconnect for {agent_name}: {err:#}");
        }
    }
}

async fn touch_bridge_session(
    shared: &AppShared,
    agent_name: &str,
    session_id: &str,
) -> Result<AgentSummary> {
    validate_bridge_session(shared, agent_name, session_id)?;

    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    let now = Utc::now();
    updated.bridge_state = BridgeConnectionState::Connected;
    updated.bridge_last_seen_at = Some(now);
    updated.last_heartbeat_at = Some(now);

    persist_agent_update(shared, updated).await
}

async fn acknowledge_bridge_deliveries(
    shared: &AppShared,
    agent_name: &str,
    session_id: Option<&str>,
    delivery_id: u64,
) -> Result<AgentSummary> {
    if let Some(session_id) = session_id {
        validate_bridge_session(shared, agent_name, session_id)?;
    }

    let normalized_delivery_id = {
        let mut queues = shared.bridge_queues.lock().expect("bridge queue mutex poisoned");
        let queue = queues
            .entry(agent_name.to_string())
            .or_insert_with(|| BridgeDeliveryQueue {
                next_delivery_id: 0,
                pending: BTreeMap::new(),
            });
        let normalized = delivery_id.min(queue.next_delivery_id);
        queue.pending.retain(|id, _| *id > normalized);
        normalized
    };

    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    let pending_count = bridge_pending_count(shared, agent_name);
    let now = Utc::now();
    updated.bridge_last_ack_delivery_id =
        updated.bridge_last_ack_delivery_id.max(normalized_delivery_id);
    updated.bridge_pending_delivery_count = pending_count;
    updated.bridge_last_seen_at = Some(now);
    updated.last_heartbeat_at = Some(now);

    let updated = persist_agent_update(shared, updated).await?;
    append_runtime_event(
        shared,
        "bridge",
        agent_name,
        Some(agent_name),
        updated.current_task_id.as_deref(),
        session_id,
        Some(agent_name),
        "bridge_delivery_acked",
        format!("acked deliveries through {}", normalized_delivery_id),
        None,
        Some(
            json!({
                "delivery_id": delivery_id,
                "normalized_delivery_id": normalized_delivery_id,
                "pending_delivery_count": updated.bridge_pending_delivery_count,
            })
            .to_string(),
        ),
    )
    .await?;

    Ok(updated)
}

async fn build_bridge_sync_message(
    shared: &AppShared,
    agent_name: &str,
    session_id: &str,
    reason: &str,
) -> Result<BridgeServerMessage> {
    validate_bridge_session(shared, agent_name, session_id)?;
    Ok(BridgeServerMessage::SyncSnapshot {
        session_id: session_id.to_string(),
        reason: reason.to_string(),
        snapshot: build_bridge_sync_snapshot(shared, agent_name).await?,
        pending_deliveries: pending_bridge_deliveries(shared, agent_name).await?,
    })
}

async fn build_bridge_sync_snapshot(
    shared: &AppShared,
    agent_name: &str,
) -> Result<BridgeSyncSnapshot> {
    let state = shared.state.read().await;
    let agent = state
        .agents
        .get(agent_name)
        .cloned()
        .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?;
    let current_task = agent
        .current_task_id
        .as_ref()
        .and_then(|task_id| state.tasks.get(task_id).cloned());
    let latest_decision = current_task
        .as_ref()
        .and_then(|task| latest_decision_for_task(&state, &task.task_id));
    Ok(BridgeSyncSnapshot {
        agent,
        current_task,
        latest_decision,
    })
}

async fn pending_bridge_deliveries(
    shared: &AppShared,
    agent_name: &str,
) -> Result<Vec<BridgeDelivery>> {
    let queues = shared.bridge_queues.lock().expect("bridge queue mutex poisoned");
    Ok(queues
        .get(agent_name)
        .map(|queue| queue.pending.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default())
}

fn bridge_pending_count(shared: &AppShared, agent_name: &str) -> u32 {
    let queues = shared.bridge_queues.lock().expect("bridge queue mutex poisoned");
    queues
        .get(agent_name)
        .map(|queue| queue.pending.len() as u32)
        .unwrap_or(0)
}

fn validate_bridge_session(shared: &AppShared, agent_name: &str, session_id: &str) -> Result<()> {
    let bridges = shared
        .active_bridges
        .lock()
        .expect("bridge session mutex poisoned");
    let session = bridges
        .get(agent_name)
        .ok_or_else(|| anyhow!("no active bridge session for agent {agent_name}"))?;
    if session.session_id != session_id {
        bail!(
            "stale bridge session for agent {agent_name}: expected {}, got {}",
            session.session_id,
            session_id
        );
    }
    Ok(())
}

async fn update_agent_bridge_presence(
    shared: &AppShared,
    agent_name: &str,
    bridge_state: BridgeConnectionState,
    mode: Option<BridgeMode>,
    session_id: Option<String>,
) -> Result<AgentSummary> {
    let (previous_bridge_state, previous_bridge_session_id, previous_bridge_mode, mut updated) = {
        let state = shared.state.read().await;
        let agent = state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?;
        (
            agent.bridge_state.clone(),
            agent.bridge_session_id.clone(),
            agent.bridge_mode.clone(),
            agent,
        )
    };

    let now = Utc::now();
    updated.bridge_state = bridge_state;
    updated.bridge_mode = mode;
    updated.bridge_session_id = session_id;
    updated.bridge_last_seen_at = Some(now);
    updated.last_heartbeat_at = Some(now);
    if updated.bridge_state == BridgeConnectionState::Connected {
        updated.bridge_connected_at = Some(now);
    }
    updated.bridge_pending_delivery_count = bridge_pending_count(shared, agent_name);

    let updated = persist_agent_update(shared, updated).await?;
    let event_type = match updated.bridge_state {
        BridgeConnectionState::Connected => "bridge_connected",
        BridgeConnectionState::Disconnected => "bridge_disconnected",
    };
    append_runtime_event(
        shared,
        "bridge",
        agent_name,
        Some(agent_name),
        updated.current_task_id.as_deref(),
        updated.bridge_session_id.as_deref(),
        Some(agent_name),
        event_type,
        format!(
            "bridge {:?} -> {:?}",
            previous_bridge_state, updated.bridge_state
        ),
        Some("bridge_presence_changed".to_string()),
        Some(
            json!({
                "previous_state": previous_bridge_state,
                "state": updated.bridge_state,
                "previous_session_id": previous_bridge_session_id,
                "session_id": updated.bridge_session_id,
                "previous_mode": previous_bridge_mode,
                "mode": updated.bridge_mode,
                "pending_delivery_count": updated.bridge_pending_delivery_count,
            })
            .to_string(),
        ),
    )
    .await?;

    Ok(updated)
}

async fn persist_agent_update(shared: &AppShared, updated: AgentSummary) -> Result<AgentSummary> {
    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.upsert_agent(&updated)?;
    }

    {
        let mut state = shared.state.write().await;
        state.agents.insert(updated.name.clone(), updated.clone());
    }

    shared
        .events_tx
        .send(DashboardEvent::AgentStateChanged {
            agent: updated.clone(),
        })
        .ok();

    Ok(updated)
}

async fn queue_bridge_delivery(
    shared: &AppShared,
    recipient: &str,
    kind: BridgeDeliveryKind,
    task: Option<TaskSummary>,
    decision: Option<DecisionSummary>,
    reason: Option<String>,
) -> Result<()> {
    let exists = {
        let state = shared.state.read().await;
        state.agents.contains_key(recipient)
    };
    if !exists {
        warn!("skipping bridge delivery for unregistered agent {recipient}");
        return Ok(());
    }

    let delivery = {
        let mut queues = shared.bridge_queues.lock().expect("bridge queue mutex poisoned");
        let next_id = {
            let queue = queues
                .entry(recipient.to_string())
                .or_insert_with(|| BridgeDeliveryQueue {
                    next_delivery_id: 0,
                    pending: BTreeMap::new(),
                });
            queue.next_delivery_id = queue.next_delivery_id.saturating_add(1);
            queue.next_delivery_id
        };

        let delivery = BridgeDelivery {
            delivery_id: next_id,
            agent: recipient.to_string(),
            kind,
            task,
            decision,
            reason,
            created_at: Utc::now(),
        };

        queues
            .get_mut(recipient)
            .expect("queue inserted above")
            .pending
            .insert(next_id, delivery.clone());
        delivery
    };

    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(recipient)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {recipient}"))?
    };
    updated.bridge_last_delivery_id = delivery.delivery_id;
    updated.bridge_pending_delivery_count = bridge_pending_count(shared, recipient);
    persist_agent_update(shared, updated).await?;
    append_runtime_event(
        shared,
        "bridge",
        recipient,
        Some(recipient),
        delivery.task.as_ref().map(|task| task.task_id.as_str()),
        None,
        Some(recipient),
        "bridge_delivery_queued",
        format!("queued {:?} delivery {}", delivery.kind, delivery.delivery_id),
        delivery.reason.clone(),
        Some(
            json!({
                "delivery_id": delivery.delivery_id,
                "kind": delivery.kind,
                "task_id": delivery.task.as_ref().map(|task| task.task_id.clone()),
                "decision_id": delivery.decision.as_ref().map(|decision| decision.decision_id.clone()),
            })
            .to_string(),
        ),
    )
    .await?;

    let active_target = {
        let bridges = shared
            .active_bridges
            .lock()
            .expect("bridge session mutex poisoned");
        bridges
            .get(recipient)
            .map(|session| (session.session_id.clone(), session.sender.clone()))
    };
    if let Some((session_id, sender)) = active_target {
        sender
            .send(BridgeServerMessage::Delivery {
                session_id,
                delivery,
            })
            .ok();
    }

    Ok(())
}

async fn sync_bridge_for_task(shared: &AppShared, task: &TaskSummary, reason: &str) -> Result<()> {
    let decision = {
        let state = shared.state.read().await;
        latest_decision_for_task(&state, &task.task_id)
    };

    match task.state {
        TaskState::Pending => {
            queue_bridge_delivery(
                shared,
                &task.to_agent,
                BridgeDeliveryKind::TaskDispatch,
                Some(task.clone()),
                decision,
                Some(reason.to_string()),
            )
            .await?;
        }
        TaskState::Reported | TaskState::BlockedWaitingDecision => {
            queue_bridge_delivery(
                shared,
                &task.from_agent,
                BridgeDeliveryKind::TaskFeedback,
                Some(task.clone()),
                decision,
                Some(reason.to_string()),
            )
            .await?;
        }
        TaskState::Cancelled => {
            queue_bridge_delivery(
                shared,
                &task.to_agent,
                BridgeDeliveryKind::TaskCancelled,
                Some(task.clone()),
                decision,
                Some(reason.to_string()),
            )
            .await?;
        }
        TaskState::Closed => {
            queue_bridge_delivery(
                shared,
                &task.to_agent,
                BridgeDeliveryKind::TaskClosed,
                Some(task.clone()),
                decision,
                Some(reason.to_string()),
            )
            .await?;
        }
        _ => {}
    }

    Ok(())
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
        ControlRequest::Trace {
            agent,
            task_id,
            session_id,
            limit,
        } => {
            let trace = trace_runtime_events(&shared, agent, task_id, session_id, limit).await?;
            Ok(ControlResponse::Trace { trace })
        }
        ControlRequest::RegisterAgent {
            name,
            role,
            repo_name,
            cwd,
            prompt_path,
        } => {
            let role = match role.as_str() {
                "main" => AgentRole::Main,
                "child" => AgentRole::Child,
                other => bail!("unsupported agent role: {other}"),
            };
            let prompt_path = normalize_prompt_path(&cwd, prompt_path, &role);

            let agent = AgentSummary {
                name: name.clone(),
                role,
                repo_name,
                cwd,
                prompt_path,
                thread_id: None,
                app_server_url: None,
                app_server_owner: None,
                app_server_registered_at: None,
                visible_pane_pid: None,
                visible_pane_kind: None,
                visible_pane_registered_at: None,
                current_session_id: None,
                state: AgentSessionState::Idle,
                bootstrap_state: AgentBootstrapState::AwaitingInit,
                bootstrap_summary: None,
                bootstrap_completed_at: None,
                current_task_id: None,
                last_output_at: None,
                last_heartbeat_at: Some(Utc::now()),
                bridge_state: BridgeConnectionState::Disconnected,
                bridge_mode: None,
                bridge_session_id: None,
                bridge_connected_at: None,
                bridge_last_seen_at: None,
                bridge_last_delivery_id: 0,
                bridge_last_ack_delivery_id: 0,
                bridge_pending_delivery_count: 0,
            };

            {
                let db = shared.db.lock().expect("db mutex poisoned");
                db.upsert_agent(&agent)?;
            }

            {
                let mut state = shared.state.write().await;
                state.agents.insert(name, agent.clone());
            }
            ensure_bridge_queue_exists(&shared, &agent).await;

            shared
                .events_tx
                .send(DashboardEvent::AgentStateChanged {
                    agent: agent.clone(),
                })
                .ok();
            append_runtime_event(
                &shared,
                "agent",
                &agent.name,
                Some(&agent.name),
                None,
                None,
                Some(&agent.name),
                "agent_registered",
                format!("registered {:?} agent", agent.role),
                Some("control_request".to_string()),
                Some(
                    json!({
                        "role": agent.role,
                        "cwd": agent.cwd,
                        "prompt_path": agent.prompt_path,
                    })
                    .to_string(),
                ),
            )
            .await?;

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
        ControlRequest::RemoveAgent { agent } => {
            let summary = remove_agent(&shared, &agent).await?;
            Ok(ControlResponse::RemoveAgent { summary })
        }
        ControlRequest::RepairRuntimeState { requested_by } => {
            let summary = repair_runtime_state(&shared, &requested_by).await?;
            Ok(ControlResponse::Repair { summary })
        }
        ControlRequest::TouchAgent { agent } => {
            let agent = touch_agent_heartbeat(&shared, &agent).await?;
            Ok(ControlResponse::Agent { agent })
        }
        ControlRequest::BeginAgentBootstrap { agent } => {
            let agent = begin_agent_bootstrap(&shared, &agent).await?;
            Ok(ControlResponse::Agent { agent })
        }
        ControlRequest::MarkAgentReady {
            agent,
            summary,
            thread_id,
        } => {
            let agent = mark_agent_ready(&shared, &agent, summary, thread_id).await?;
            Ok(ControlResponse::Agent { agent })
        }
        ControlRequest::SetAgentVisiblePane { agent, pid, kind } => {
            let kind = kind
                .as_deref()
                .map(parse_visible_pane_kind_label)
                .transpose()?;
            let agent = set_agent_visible_pane(&shared, &agent, pid, kind).await?;
            Ok(ControlResponse::Agent { agent })
        }
        ControlRequest::SetAgentAppServer {
            agent,
            app_server_url,
            owner,
        } => {
            let owner = owner
                .as_deref()
                .map(parse_app_server_owner_label)
                .transpose()?;
            let agent = set_agent_app_server(&shared, &agent, app_server_url, owner).await?;
            Ok(ControlResponse::Agent { agent })
        }
        ControlRequest::AppendRuntimeEvent {
            scope,
            scope_id,
            agent,
            task_id,
            session_id,
            actor,
            event_type,
            summary,
            reason,
            payload_json,
        } => {
            if let Some(payload) = payload_json.as_deref() {
                serde_json::from_str::<serde_json::Value>(payload)
                    .context("append-runtime-event payload_json must be valid JSON")?;
            }
            append_runtime_event(
                &shared,
                &scope,
                &scope_id,
                agent.as_deref(),
                task_id.as_deref(),
                session_id.as_deref(),
                actor.as_deref(),
                &event_type,
                summary,
                reason,
                payload_json,
            )
            .await?;
            Ok(ControlResponse::Ack {
                message: format!("runtime event appended: {event_type}"),
            })
        }
        ControlRequest::EnsureManagedSession {
            agent,
            bootstrap_prompt,
        } => {
            let agent = ensure_managed_session(&shared, &agent, bootstrap_prompt).await?;
            Ok(ControlResponse::Agent { agent })
        }
        ControlRequest::AgentContext { agent } => {
            let context = agent_context(&shared, &agent).await?;
            Ok(ControlResponse::AgentContext { context })
        }
        ControlRequest::TaskContext { task_id } => {
            let context = task_context(&shared, &task_id).await?;
            Ok(ControlResponse::TaskContext { context })
        }
        ControlRequest::BeginVisibleTask { agent } => {
            let task = begin_visible_task(&shared, &agent).await?;
            Ok(ControlResponse::Task { task })
        }
        ControlRequest::SubmitVisibleTaskRound {
            task_id,
            agent,
            payload,
        } => {
            let (task, decision) =
                submit_visible_task_round(&shared, &task_id, &agent, payload.clone()).await?;
            Ok(ControlResponse::VisibleTaskRound {
                task,
                payload,
                decision,
            })
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
        ControlRequest::StopManagedSessions => {
            let (stopped_agents, failures) = stop_managed_sessions(&shared).await?;
            let message = match (stopped_agents.is_empty(), failures.is_empty()) {
                (true, true) => "no live daemon-managed sessions".to_string(),
                (false, true) => format!(
                    "stop requested for daemon-managed sessions on agents: {}",
                    stopped_agents.join(", ")
                ),
                (true, false) => format!(
                    "no live daemon-managed sessions stopped; failures: {}",
                    failures.join("; ")
                ),
                (false, false) => format!(
                    "stop requested for daemon-managed sessions on agents: {}; failures: {}",
                    stopped_agents.join(", "),
                    failures.join("; ")
                ),
            };
            Ok(ControlResponse::Ack { message })
        }
        ControlRequest::StopVisiblePanes => {
            let (stopped_agents, failures) = stop_visible_panes(&shared).await?;
            let message = match (stopped_agents.is_empty(), failures.is_empty()) {
                (true, true) => "no registered visible panes".to_string(),
                (false, true) => format!(
                    "stop requested for visible panes on agents: {}",
                    stopped_agents.join(", ")
                ),
                (true, false) => format!(
                    "no visible panes stopped; failures: {}",
                    failures.join("; ")
                ),
                (false, false) => format!(
                    "stop requested for visible panes on agents: {}; failures: {}",
                    stopped_agents.join(", "),
                    failures.join("; ")
                ),
            };
            Ok(ControlResponse::Ack { message })
        }
        ControlRequest::CreateTask {
            from_agent,
            to_agent,
            title,
            summary,
            effort,
            read_scope,
            write_scope,
            acceptance,
            auto_resolve_by,
            auto_resolve_summary,
        } => {
            let task = create_task_record(
                &shared,
                &from_agent,
                &to_agent,
                TaskDraftPayload {
                    title,
                    summary,
                    effort,
                    read_scope,
                    write_scope,
                    acceptance,
                },
                auto_resolve_by,
                auto_resolve_summary,
                "control_request",
            )
            .await?;
            Ok(ControlResponse::Task { task })
        }
        ControlRequest::CreateTaskFromPrompt {
            from_agent,
            to_agent,
            request,
        } => {
            let task = create_task_from_prompt(&shared, &from_agent, &to_agent, &request).await?;
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
            let reported_task = transition_task_state(
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
                    &reported_task.task_id,
                    "report_payload",
                    Some(TaskState::Reported),
                    Some(TaskState::Reported),
                    &payload.to_string(),
                )?;
            }
            record_stream_event(&shared, &agent, "stdout", &format!("[REPORT] {payload}")).await?;

            let task = update_task_latest_child_feedback(
                &shared,
                &reported_task,
                "report",
                Some(topic.clone()),
                Some(blocking.clone()),
                Some(topic),
                Some(details),
                true,
                "report_payload",
            )
            .await?;

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

            let continued_task =
                reopen_task_for_next_round(&shared, &task_id, &target_agent).await?;
            Ok(ControlResponse::DecisionTask {
                decision,
                task: continued_task,
            })
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

    let prompt = compose_agent_round_prompt(&agent, prompt)?;
    let execution = execute_codex_round(shared, &agent, &prompt, None).await;

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

async fn run_agent_structured_round(
    shared: &AppShared,
    agent: &AgentSummary,
    prompt: &str,
    output_schema: String,
) -> Result<AgentRoundResult> {
    ensure_agent_ready_for_ad_hoc_round(agent)?;
    set_agent_runtime_state(shared, &agent.name, AgentSessionState::Busy).await?;

    let existing_thread_id = agent.thread_id.clone();
    let execution = execute_codex_round(shared, agent, prompt, Some(output_schema)).await;

    match execution {
        Ok(result) => {
            let mut updated_agent = {
                let state = shared.state.read().await;
                state
                    .agents
                    .get(&agent.name)
                    .cloned()
                    .ok_or_else(|| anyhow!("agent disappeared during structured round: {}", agent.name))?
            };
            updated_agent.thread_id =
                existing_thread_id.or_else(|| Some(result.thread_id.clone()));
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
                    .insert(agent.name.clone(), updated_agent.clone());
            }

            shared
                .events_tx
                .send(DashboardEvent::AgentStateChanged {
                    agent: updated_agent,
                })
                .ok();
            record_stream_event(
                shared,
                &agent.name,
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
            set_agent_runtime_state(shared, &agent.name, next_state).await?;
            Err(err)
        }
    }
}

async fn create_task_from_prompt(
    shared: &AppShared,
    from_agent: &str,
    to_agent: &str,
    request: &str,
) -> Result<TaskSummary> {
    let request = request.trim();
    if request.is_empty() {
        bail!("task request cannot be empty");
    }

    let (source_agent, target_agent) = {
        let state = shared.state.read().await;
        let source_agent = state
            .agents
            .get(from_agent)
            .cloned()
            .ok_or_else(|| anyhow!("source agent not registered: {from_agent}"))?;
        let target_agent = state
            .agents
            .get(to_agent)
            .cloned()
            .ok_or_else(|| anyhow!("target agent not registered: {to_agent}"))?;
        ensure_agent_ready_for_ad_hoc_round(&source_agent)?;
        ensure_agent_ready_for_new_task(&target_agent)?;
        (source_agent, target_agent)
    };

    let prompt = compose_task_creation_prompt(&source_agent, &target_agent, request)?;
    let schema_path = shared
        .config
        .root_dir
        .join("schemas")
        .join("task_draft.schema.json");
    let round = run_agent_structured_round(
        shared,
        &source_agent,
        &prompt,
        schema_path.to_string_lossy().to_string(),
    )
    .await?;

    let payload: TaskDraftPayload = serde_json::from_str(&round.final_message).with_context(|| {
        format!(
            "failed to parse generated task payload as json from agent {}",
            source_agent.name
        )
    })?;

    create_task_record(
        shared,
        from_agent,
        to_agent,
        payload,
        None,
        None,
        "agent_generated_task",
    )
    .await
}

fn ensure_agent_ready_for_new_task(agent: &AgentSummary) -> Result<()> {
    if agent.bootstrap_state != AgentBootstrapState::Ready {
        bail!(
            "target agent {} has not completed bootstrap, actual {:?}",
            agent.name,
            agent.bootstrap_state
        );
    }
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
    if agent.bootstrap_state != AgentBootstrapState::Ready {
        bail!(
            "agent {} has not completed bootstrap, actual {:?}",
            agent.name,
            agent.bootstrap_state
        );
    }
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

fn normalize_task_draft_payload(
    target_agent: &AgentSummary,
    payload: TaskDraftPayload,
) -> Result<TaskDraftPayload> {
    let title = payload.title.trim().to_string();
    if title.is_empty() {
        bail!("generated task title cannot be empty");
    }

    let summary = payload.summary.trim().to_string();
    if summary.is_empty() {
        bail!("generated task summary cannot be empty");
    }

    let effort = payload.effort.and_then(|value| {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "" => None,
            "medium" | "high" | "xhigh" => Some(normalized),
            _ => None,
        }
    });

    let read_scope = sanitize_task_scope_list(payload.read_scope);
    let mut write_scope = sanitize_task_scope_list(payload.write_scope);
    if write_scope.is_empty() {
        write_scope.push(target_agent.cwd.clone());
    }

    for item in &write_scope {
        if !is_path_within_root(item, &target_agent.cwd) {
            bail!(
                "generated write_scope entry {} is outside target repository {}",
                item,
                target_agent.cwd
            );
        }
    }

    let acceptance = payload
        .acceptance
        .into_iter()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();

    Ok(TaskDraftPayload {
        title,
        summary,
        effort,
        read_scope,
        write_scope,
        acceptance,
    })
}

fn sanitize_task_scope_list(items: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();

    for item in items {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let key = trimmed.replace('/', "\\").to_ascii_lowercase();
        if seen.insert(key) {
            normalized.push(trimmed.to_string());
        }
    }

    normalized
}

fn is_path_within_root(candidate: &str, root: &str) -> bool {
    let normalize = |value: &str| {
        value
            .trim()
            .replace('/', "\\")
            .trim_end_matches('\\')
            .to_ascii_lowercase()
    };

    let candidate = normalize(candidate);
    let root = normalize(root);
    candidate == root || candidate.starts_with(&format!("{root}\\"))
}

async fn create_task_record(
    shared: &AppShared,
    from_agent: &str,
    to_agent: &str,
    payload: TaskDraftPayload,
    auto_resolve_by: Option<String>,
    auto_resolve_summary: Option<String>,
    runtime_reason: &str,
) -> Result<TaskSummary> {
    let target = {
        let state = shared.state.read().await;
        let target = state
            .agents
            .get(to_agent)
            .cloned()
            .ok_or_else(|| anyhow!("target agent not registered: {to_agent}"))?;

        ensure_agent_ready_for_new_task(&target)?;

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

        target
    };

    let payload = normalize_task_draft_payload(&target, payload)?;
    let now = Utc::now();
    let task = TaskSummary {
        task_id: format!("T-{}", Uuid::now_v7().simple()),
        from_agent: from_agent.to_string(),
        to_agent: to_agent.to_string(),
        title: payload.title,
        summary: payload.summary,
        effort: payload.effort,
        read_scope: payload.read_scope,
        write_scope: payload.write_scope,
        acceptance: payload.acceptance,
        auto_resolve_by,
        auto_resolve_summary,
        round_count: 0,
        latest_child_status: None,
        latest_child_summary: None,
        latest_child_blocking: None,
        latest_child_topic: None,
        latest_child_details: None,
        latest_decision_id: None,
        latest_decision_summary: None,
        latest_decision_status: None,
        latest_decision_issued_by: None,
        latest_decision_at: None,
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
        if let Some(agent) = state.agents.get_mut(to_agent) {
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
    append_runtime_event(
        shared,
        "task",
        &task.task_id,
        Some(&task.to_agent),
        Some(&task.task_id),
        None,
        Some(&task.from_agent),
        "task_created",
        format!("created task for {}", task.to_agent),
        Some(runtime_reason.to_string()),
        Some(
            json!({
                "title": task.title.clone(),
                "summary": task.summary.clone(),
                "state": task.state.clone(),
            })
            .to_string(),
        ),
    )
    .await?;
    sync_bridge_for_task(shared, &task, "task_created").await?;

    Ok(task)
}

fn desired_agent_runtime_state(state: &RuntimeState, agent: &AgentSummary) -> AgentSessionState {
    if agent.current_session_id.is_some() {
        return AgentSessionState::Busy;
    }

    if let Some(task_id) = &agent.current_task_id
        && let Some(task) = state.tasks.get(task_id)
    {
        return desired_agent_state_for_task(task);
    }

    AgentSessionState::Idle
}

fn claim_task_for_agent_summary(agent: &mut AgentSummary, task_id: &str) {
    agent.current_task_id = Some(task_id.to_string());
    agent.state = AgentSessionState::Busy;
    agent.last_output_at = Some(Utc::now());
    agent.last_heartbeat_at = Some(Utc::now());
}

fn ensure_agent_ready_for_task_round(agent: &AgentSummary, task_id: &str) -> Result<()> {
    if agent.bootstrap_state != AgentBootstrapState::Ready {
        bail!(
            "agent {} has not completed bootstrap, actual {:?}",
            agent.name,
            agent.bootstrap_state
        );
    }
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

fn latest_decision_for_task(state: &RuntimeState, task_id: &str) -> Option<DecisionSummary> {
    state
        .decisions
        .values()
        .filter(|decision| decision.task_id == task_id)
        .max_by_key(|decision| decision.created_at)
        .cloned()
}

fn apply_latest_decision_fields(task: &mut TaskSummary, decision: Option<&DecisionSummary>) {
    if let Some(decision) = decision {
        task.latest_decision_id = Some(decision.decision_id.clone());
        task.latest_decision_summary = Some(decision.summary.clone());
        task.latest_decision_status = Some(decision.status.clone());
        task.latest_decision_issued_by = Some(decision.issued_by.clone());
        task.latest_decision_at = Some(decision.created_at.clone());
    } else {
        task.latest_decision_id = None;
        task.latest_decision_summary = None;
        task.latest_decision_status = None;
        task.latest_decision_issued_by = None;
        task.latest_decision_at = None;
    }
}

fn task_latest_decision_is_current(task: &TaskSummary, decision: Option<&DecisionSummary>) -> bool {
    match decision {
        Some(decision) => {
            task.latest_decision_id.as_deref() == Some(decision.decision_id.as_str())
                && task.latest_decision_summary.as_deref() == Some(decision.summary.as_str())
                && task.latest_decision_status.as_deref() == Some(decision.status.as_str())
                && task.latest_decision_issued_by.as_deref() == Some(decision.issued_by.as_str())
                && task.latest_decision_at == Some(decision.created_at.clone())
        }
        None => {
            task.latest_decision_id.is_none()
                && task.latest_decision_summary.is_none()
                && task.latest_decision_status.is_none()
                && task.latest_decision_issued_by.is_none()
                && task.latest_decision_at.is_none()
        }
    }
}

async fn sync_task_latest_decision_fields_on_startup(shared: &AppShared) -> Result<()> {
    let updates = {
        let state = shared.state.read().await;
        state
            .tasks
            .values()
            .filter_map(|task| {
                let latest_decision = latest_decision_for_task(&state, &task.task_id);
                if task_latest_decision_is_current(task, latest_decision.as_ref()) {
                    return None;
                }

                let mut updated = task.clone();
                apply_latest_decision_fields(&mut updated, latest_decision.as_ref());
                Some(updated)
            })
            .collect::<Vec<_>>()
    };

    if updates.is_empty() {
        return Ok(());
    }

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        for task in &updates {
            db.update_task(task)?;
        }
    }

    {
        let mut state = shared.state.write().await;
        for task in updates {
            state.tasks.insert(task.task_id.clone(), task);
        }
    }

    Ok(())
}

async fn broadcast_runtime_snapshot(shared: &AppShared) {
    let snapshot = shared.state.read().await.snapshot();
    shared
        .events_tx
        .send(DashboardEvent::Snapshot { snapshot })
        .ok();
}

async fn append_runtime_event(
    shared: &AppShared,
    scope: &str,
    scope_id: &str,
    agent_name: Option<&str>,
    task_id: Option<&str>,
    session_id: Option<&str>,
    actor_name: Option<&str>,
    event_type: &str,
    summary: impl Into<String>,
    reason: Option<String>,
    payload_json: Option<String>,
) -> Result<()> {
    let summary = summary.into();
    let event = {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.insert_runtime_event(
            scope,
            scope_id,
            agent_name,
            task_id,
            session_id,
            actor_name,
            event_type,
            &summary,
            reason.as_deref(),
            payload_json.as_deref(),
        )?
    };

    {
        let mut state = shared.state.write().await;
        state.recent_runtime_events.push(event.clone());
        if state.recent_runtime_events.len() > MAX_RECENT_RUNTIME_EVENTS {
            let drop_count = state.recent_runtime_events.len() - MAX_RECENT_RUNTIME_EVENTS;
            state.recent_runtime_events.drain(0..drop_count);
        }
    }

    shared
        .events_tx
        .send(DashboardEvent::RuntimeEvent { event })
        .ok();
    Ok(())
}

async fn trace_runtime_events(
    shared: &AppShared,
    agent: Option<String>,
    task_id: Option<String>,
    session_id: Option<String>,
    limit: usize,
) -> Result<RuntimeTraceView> {
    let limit = limit.clamp(1, 500);
    let db = shared.db.lock().expect("db mutex poisoned");

    let (query_kind, query_value, events) = if let Some(agent_name) = agent {
        (
            "agent".to_string(),
            agent_name.clone(),
            db.load_runtime_events_for_agent(&agent_name, limit)?,
        )
    } else if let Some(task_id) = task_id {
        (
            "task".to_string(),
            task_id.clone(),
            db.load_runtime_events_for_task(&task_id, limit)?,
        )
    } else if let Some(session_id) = session_id {
        (
            "session".to_string(),
            session_id.clone(),
            db.load_runtime_events_for_session(&session_id, limit)?,
        )
    } else {
        bail!("trace requires one of agent, task_id, or session_id");
    };

    Ok(RuntimeTraceView {
        query_kind,
        query_value,
        event_count: events.len(),
        events,
    })
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

    let prompt = compose_task_prompt(&agent, &task)?;
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

    let (task_for_response, decision) = finalize_task_round_payload(
        shared,
        &task,
        &payload,
        Some(round.thread_id.clone()),
        round.completed_at,
        &round.final_message,
        "task_round_payload",
        "TASK_ROUND_RESULT",
    )
    .await?;

    Ok(ControlResponse::TaskRound {
        task: task_for_response,
        result: round,
        payload,
        decision,
    })
}

async fn agent_context(shared: &AppShared, agent_name: &str) -> Result<AgentContextView> {
    let state = shared.state.read().await;
    let agent = state
        .agents
        .get(agent_name)
        .cloned()
        .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?;

    let current_task = agent
        .current_task_id
        .as_ref()
        .and_then(|task_id| state.tasks.get(task_id).cloned());

    let latest_decision = current_task
        .as_ref()
        .and_then(|task| latest_decision_for_task(&state, &task.task_id));

    let current_session = agent
        .current_session_id
        .as_ref()
        .and_then(|session_id| state.sessions.get(session_id).cloned());
    let context_sources = agent_context_sources(&agent);

    Ok(AgentContextView {
        agent,
        current_task,
        latest_decision,
        current_session,
        context_sources,
    })
}

async fn task_context(shared: &AppShared, task_id: &str) -> Result<TaskContextView> {
    let state = shared.state.read().await;
    let task = state
        .tasks
        .get(task_id)
        .cloned()
        .ok_or_else(|| anyhow!("task not found: {task_id}"))?;

    let agent = state.agents.get(&task.to_agent).cloned();
    let latest_decision = latest_decision_for_task(&state, task_id);
    let current_session = agent
        .as_ref()
        .and_then(|agent| agent.current_session_id.as_ref())
        .and_then(|session_id| state.sessions.get(session_id).cloned());
    let context_sources = agent
        .as_ref()
        .map(agent_context_sources)
        .unwrap_or_default();

    Ok(TaskContextView {
        task,
        agent,
        latest_decision,
        current_session,
        context_sources,
    })
}

async fn begin_visible_task(shared: &AppShared, agent_name: &str) -> Result<TaskSummary> {
    let agent = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    let task_id = agent
        .current_task_id
        .clone()
        .ok_or_else(|| anyhow!("agent {agent_name} has no assigned task"))?;

    let task = {
        let state = shared.state.read().await;
        state
            .tasks
            .get(&task_id)
            .cloned()
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?
    };

    match task.state {
        TaskState::Pending => {
            let accepted = transition_task_state(
                shared,
                &task.task_id,
                agent_name,
                TaskState::Pending,
                TaskState::Accepted,
                "task_accepted",
            )
            .await?;

            transition_task_state(
                shared,
                &accepted.task_id,
                agent_name,
                TaskState::Accepted,
                TaskState::Running,
                "task_running",
            )
            .await
        }
        TaskState::Accepted => {
            transition_task_state(
                shared,
                &task.task_id,
                agent_name,
                TaskState::Accepted,
                TaskState::Running,
                "task_running",
            )
            .await
        }
        TaskState::Running => Ok(task),
        other => bail!(
            "task {} on agent {} must be pending, accepted, or running before begin-visible-task, actual {:?}",
            task.task_id,
            agent_name,
            other
        ),
    }
}

async fn submit_visible_task_round(
    shared: &AppShared,
    task_id: &str,
    agent_name: &str,
    payload: TaskRoundPayload,
) -> Result<(TaskSummary, Option<DecisionSummary>)> {
    let task = {
        let state = shared.state.read().await;
        state
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| anyhow!("task not found: {task_id}"))?
    };

    if task.to_agent != agent_name {
        bail!(
            "task {task_id} belongs to {}, not {agent_name}",
            task.to_agent
        );
    }
    if task.state != TaskState::Running {
        bail!(
            "task {task_id} must be running before submit-visible-task-round, actual {:?}",
            task.state
        );
    }

    let rendered_payload = serde_json::to_string(&payload)
        .context("failed to serialize visible task round payload")?;

    finalize_task_round_payload(
        shared,
        &task,
        &payload,
        None,
        Utc::now(),
        &rendered_payload,
        "visible_task_round_payload",
        "VISIBLE_TASK_ROUND",
    )
    .await
}

async fn finalize_task_round_payload(
    shared: &AppShared,
    task: &TaskSummary,
    payload: &TaskRoundPayload,
    thread_id: Option<String>,
    completed_at: chrono::DateTime<Utc>,
    raw_payload: &str,
    db_event_type: &str,
    stream_label: &str,
) -> Result<(TaskSummary, Option<DecisionSummary>)> {
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
            db_event_type,
            Some(final_task.state.clone()),
            Some(final_task.state.clone()),
            raw_payload,
        )?;
    }

    record_stream_event(
        shared,
        &task.to_agent,
        "stdout",
        &format!("[{stream_label}] {raw_payload}"),
    )
    .await?;

    let next_agent_state = desired_agent_state_for_task(&final_task);
    if let Some(thread_id) = thread_id {
        update_agent_after_round(
            shared,
            &task.to_agent,
            Some(thread_id),
            next_agent_state,
            completed_at,
        )
        .await?;
    } else {
        set_agent_runtime_state(shared, &task.to_agent, next_agent_state).await?;
    }

    let final_task = update_task_latest_child_feedback(
        shared,
        &final_task,
        match payload.status {
            TaskRoundStatus::Result => "result",
            TaskRoundStatus::Report => "report",
            TaskRoundStatus::WaitDecision => "wait_decision",
        },
        Some(payload.summary.clone()),
        payload.blocking.clone(),
        payload.topic.clone(),
        payload.details.clone(),
        true,
        "task_feedback_updated",
    )
    .await?;

    maybe_auto_resolve_task(shared, &final_task, payload).await
}

async fn remove_agent(shared: &AppShared, agent_name: &str) -> Result<RemoveAgentSummary> {
    if agent_name == "main" {
        bail!("refusing to remove built-in main agent");
    }

    let (task_ids, live_task_ids, live_session_ids, bridge_connected, pending_delivery_count) = {
        let state = shared.state.read().await;
        let agent = state
            .agents
            .get(agent_name)
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?;

        let task_ids = state
            .tasks
            .values()
            .filter(|task| task.from_agent == agent_name || task.to_agent == agent_name)
            .map(|task| task.task_id.clone())
            .collect::<Vec<_>>();
        let live_task_ids = state
            .tasks
            .values()
            .filter(|task| {
                (task.from_agent == agent_name || task.to_agent == agent_name)
                    && !is_task_terminal(&task.state)
            })
            .map(|task| task.task_id.clone())
            .collect::<Vec<_>>();
        let live_session_ids = state
            .sessions
            .values()
            .filter(|session| {
                session.agent_name == agent_name && session.status == CodexSessionStatus::Running
            })
            .map(|session| session.session_id.clone())
            .collect::<Vec<_>>();

        (
            task_ids,
            live_task_ids,
            live_session_ids,
            agent.bridge_state == BridgeConnectionState::Connected,
            agent.bridge_pending_delivery_count,
        )
    };

    if !live_task_ids.is_empty() {
        bail!(
            "refusing to remove agent {agent_name} because it still has non-terminal tasks: {}",
            live_task_ids.join(", ")
        );
    }

    let has_active_session = {
        let active = shared
            .active_sessions
            .lock()
            .expect("active session mutex poisoned");
        active.values().any(|control| control.agent_name == agent_name)
    };
    if has_active_session || !live_session_ids.is_empty() {
        let mut session_ids = live_session_ids;
        if session_ids.is_empty() {
            session_ids.push("<active_handle>".to_string());
        }
        bail!(
            "refusing to remove agent {agent_name} because it still has a live session: {}",
            session_ids.join(", ")
        );
    }

    let has_active_bridge = {
        let bridges = shared
            .active_bridges
            .lock()
            .expect("bridge session mutex poisoned");
        bridges.contains_key(agent_name)
    };
    if bridge_connected || has_active_bridge {
        bail!("refusing to remove agent {agent_name} because its bridge is still connected");
    }

    if pending_delivery_count > 0 {
        bail!(
            "refusing to remove agent {agent_name} because it still has {} pending bridge deliveries",
            pending_delivery_count
        );
    }

    let agent_names = vec![agent_name.to_string()];
    let summary = {
        let db = shared.db.lock().expect("db mutex poisoned");
        let removed_stream_events = db.delete_stream_events_by_agent_names(&agent_names)?;
        let removed_runtime_events = db.delete_runtime_events_by_task_ids(&task_ids)?
            + db.delete_runtime_events_by_agent_names(&agent_names)?;
        let removed_decisions = db.delete_decisions_by_task_ids(&task_ids)?;
        let removed_task_events = db.delete_task_events_by_task_ids(&task_ids)?;
        let removed_tasks = db.delete_tasks_by_ids(&task_ids)?;
        let removed_sessions = db.delete_sessions_by_agent_names(&agent_names)?;
        let removed_agents = db.delete_agents_by_names(&agent_names)?;

        if removed_agents == 0 {
            bail!("agent {agent_name} was not removed from sqlite");
        }

        RemoveAgentSummary {
            agent: agent_name.to_string(),
            removed_tasks,
            removed_task_events,
            removed_runtime_events,
            removed_decisions,
            removed_sessions,
            removed_stream_events,
        }
    };

    let task_id_set = task_ids.iter().cloned().collect::<HashSet<_>>();

    {
        let mut active = shared
            .active_sessions
            .lock()
            .expect("active session mutex poisoned");
        active.retain(|_, control| control.agent_name != agent_name);
    }

    {
        let mut bridges = shared
            .active_bridges
            .lock()
            .expect("bridge session mutex poisoned");
        bridges.remove(agent_name);
    }

    {
        let mut queues = shared.bridge_queues.lock().expect("bridge queue mutex poisoned");
        queues.remove(agent_name);
    }

    {
        let mut state = shared.state.write().await;
        state.agents.remove(agent_name);
        state.tasks.retain(|task_id, task| {
            !task_id_set.contains(task_id)
                && task.from_agent != agent_name
                && task.to_agent != agent_name
        });
        state
            .decisions
            .retain(|_, decision| !task_id_set.contains(&decision.task_id));
        state
            .sessions
            .retain(|_, session| session.agent_name != agent_name);
        state.recent_streams.retain(|event| event.agent != agent_name);
        state.recent_runtime_events.retain(|event| {
            event.agent_name.as_deref() != Some(agent_name)
                && event.actor_name.as_deref() != Some(agent_name)
                && !event
                    .task_id
                    .as_ref()
                    .map(|task_id| task_id_set.contains(task_id))
                    .unwrap_or(false)
        });
    }

    broadcast_runtime_snapshot(shared).await;

    Ok(summary)
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
            removed_runtime_events: 0,
            removed_decisions: 0,
            removed_sessions: 0,
            removed_stream_events: 0,
            removed_agent_names: Vec::new(),
        });
    }

    let summary = {
        let db = shared.db.lock().expect("db mutex poisoned");
        let removed_stream_events = db.delete_stream_events_by_agent_names(&demo_agent_names)?;
        let removed_runtime_events = db.delete_runtime_events_by_task_ids(&task_ids)?
            + db.delete_runtime_events_by_agent_names(&demo_agent_names)?;
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
            removed_runtime_events,
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
        state.recent_runtime_events.retain(|event| {
            !event
                .agent_name
                .as_ref()
                .map(|agent| demo_agent_set.contains(agent))
                .unwrap_or(false)
                && !event
                    .actor_name
                    .as_ref()
                    .map(|agent| demo_agent_set.contains(agent))
                    .unwrap_or(false)
                && !event
                    .task_id
                    .as_ref()
                    .map(|task_id| task_id_set.contains(task_id))
                    .unwrap_or(false)
        });
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

    let (mut agents, mut tasks, decisions, mut sessions, recent_streams, recent_runtime_events) = {
        let state = shared.state.read().await;
        (
            state.agents.clone(),
            state.tasks.clone(),
            state.decisions.clone(),
            state.sessions.clone(),
            state.recent_streams.clone(),
            state.recent_runtime_events.clone(),
        )
    };

    let mut repaired_agents = HashSet::new();
    let mut repaired_tasks = HashSet::new();
    let mut repaired_sessions = HashSet::new();
    let mut notes = Vec::new();
    let latest_decisions_by_task = decisions.values().fold(
        HashMap::<String, DecisionSummary>::new(),
        |mut latest, decision| {
            let should_replace = latest
                .get(&decision.task_id)
                .map(|current| current.created_at < decision.created_at)
                .unwrap_or(true);
            if should_replace {
                latest.insert(decision.task_id.clone(), decision.clone());
            }
            latest
        },
    );

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

        let latest_decision = latest_decisions_by_task.get(&task.task_id);
        if !task_latest_decision_is_current(task, latest_decision) {
            apply_latest_decision_fields(task, latest_decision);
            changed = true;
            notes.push(format!(
                "task {} latest decision snapshot did not match decision ledger; synchronized task fields",
                task.task_id
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
            } else if open_task_ids.is_empty() && agent.state != AgentSessionState::Idle {
                agent.state = AgentSessionState::Idle;
                changed = true;
                notes.push(format!(
                    "agent {} had no task or session but state {:?}; normalized to idle",
                    agent.name, original_state
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
        state.recent_runtime_events = recent_runtime_events;
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
    if matches!(
        updated.state,
        TaskState::Reported | TaskState::BlockedWaitingDecision
    ) {
        sync_bridge_for_task(shared, &updated, event_type).await?;
    }

    Ok(updated)
}

async fn update_task_latest_child_feedback(
    shared: &AppShared,
    task: &TaskSummary,
    status: &str,
    summary: Option<String>,
    blocking: Option<String>,
    topic: Option<String>,
    details: Option<String>,
    increment_round_count: bool,
    event_type: &str,
) -> Result<TaskSummary> {
    let mut updated = task.clone();
    if increment_round_count {
        updated.round_count = updated.round_count.saturating_add(1);
    }
    updated.latest_child_status = Some(status.to_string());
    updated.latest_child_summary = summary;
    updated.latest_child_blocking = blocking;
    updated.latest_child_topic = topic;
    updated.latest_child_details = details;
    updated.updated_at = Utc::now();

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.update_task(&updated)?;
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
    append_runtime_event(
        shared,
        "task",
        &updated.task_id,
        Some(&updated.to_agent),
        Some(&updated.task_id),
        None,
        Some(&updated.to_agent),
        event_type,
        format!(
            "child feedback status={} summary={}",
            updated.latest_child_status.clone().unwrap_or_default(),
            updated.latest_child_summary.clone().unwrap_or_default()
        ),
        updated.latest_child_blocking.clone(),
        Some(
            json!({
                "round_count": updated.round_count,
                "latest_child_status": updated.latest_child_status,
                "latest_child_summary": updated.latest_child_summary,
                "latest_child_topic": updated.latest_child_topic,
                "latest_child_details": updated.latest_child_details,
            })
            .to_string(),
        ),
    )
    .await?;
    sync_bridge_for_task(shared, &updated, event_type).await?;

    Ok(updated)
}

async fn update_task_latest_main_decision(
    shared: &AppShared,
    task: &TaskSummary,
    decision: Option<&DecisionSummary>,
    event_type: &str,
) -> Result<TaskSummary> {
    if task_latest_decision_is_current(task, decision) {
        return Ok(task.clone());
    }

    let mut updated = task.clone();
    apply_latest_decision_fields(&mut updated, decision);
    updated.updated_at = Utc::now();

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        db.update_task(&updated)?;
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
    append_runtime_event(
        shared,
        "task",
        &updated.task_id,
        Some(&updated.to_agent),
        Some(&updated.task_id),
        None,
        decision.map(|item| item.issued_by.as_str()),
        event_type,
        updated
            .latest_decision_summary
            .clone()
            .unwrap_or_else(|| "latest main decision snapshot updated".to_string()),
        Some("task_latest_decision_synced".to_string()),
        Some(
            json!({
                "latest_decision_id": updated.latest_decision_id,
                "latest_decision_status": updated.latest_decision_status,
                "latest_decision_issued_by": updated.latest_decision_issued_by,
                "latest_decision_at": updated.latest_decision_at,
            })
            .to_string(),
        ),
    )
    .await?;

    Ok(updated)
}

fn display_agent_context_path(agent: &AgentSummary, path: &Path) -> String {
    path.strip_prefix(&agent.cwd)
        .map(|relative| relative.to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string_lossy().to_string())
}

fn collect_agent_context_source_entries(agent: &AgentSummary) -> Vec<(AgentContextSource, PathBuf)> {
    let mut entries = Vec::new();
    let mut seen = HashSet::new();

    if let Some(prompt_path) = &agent.prompt_path {
        let prompt_path = PathBuf::from(prompt_path);
        let display = display_agent_context_path(agent, &prompt_path);
        if seen.insert(display.clone()) {
            entries.push((
                AgentContextSource {
                    kind: AgentContextSourceKind::PromptContract,
                    path: display,
                    exists: prompt_path.is_file(),
                },
                prompt_path,
            ));
        }
    }

    let work_path = Path::new(&agent.cwd).join("work.md");
    let work_display = display_agent_context_path(agent, &work_path);
    if seen.insert(work_display.clone()) && work_path.is_file() {
        entries.push((
            AgentContextSource {
                kind: AgentContextSourceKind::WorkingNotes,
                path: work_display,
                exists: true,
            },
            work_path,
        ));
    }

    entries
}

fn agent_context_sources(agent: &AgentSummary) -> Vec<AgentContextSource> {
    collect_agent_context_source_entries(agent)
        .into_iter()
        .map(|(source, _)| source)
        .collect()
}

fn existing_agent_context_files(agent: &AgentSummary) -> Vec<PathBuf> {
    collect_agent_context_source_entries(agent)
        .into_iter()
        .filter_map(|(source, path)| source.exists.then_some(path))
        .collect()
}

fn compose_agent_context_prompt(agent: &AgentSummary) -> Result<String> {
    let context_files = existing_agent_context_files(agent);
    let missing_prompt_note = agent
        .prompt_path
        .as_ref()
        .filter(|path| !Path::new(path).is_file())
        .map(|path| {
            format!(
                "Configured agent prompt path was not found, so continue with repo discovery only: {path}\n\n"
            )
        })
        .unwrap_or_default();

    if context_files.is_empty() {
        return Ok(format!(
            "{missing_prompt_note}No agent-specific prompt file or context file was found automatically. Inspect the repository directly before acting.\n\n"
        ));
    }

    let mut blocks = String::new();
    for path in context_files {
        let display = display_agent_context_path(agent, &path);
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read agent context file {}", path.display()))?;
        blocks.push_str(&format!("File: {display}\n```md\n{content}\n```\n\n"));
    }

    Ok(format!(
        "{missing_prompt_note}The following repo-local context files are already loaded for this round. Treat them as the active role contract and long-lived working context. Real-time child/main communication is supplied separately through daemon task and decision state, not repo-local status files.\n\n\
{blocks}"
    ))
}

fn compose_agent_round_prompt(agent: &AgentSummary, request: &str) -> Result<String> {
    let context_prompt = compose_agent_context_prompt(agent)?;
    Ok(format!(
        "You are operating as agent `{agent_name}` inside repository `{cwd}`.\n\
Work only inside this repository unless the repo-local instructions explicitly tell you to inspect another local path.\n\
\n\
{context_prompt}\
Round request:\n\
{request}\n",
        agent_name = agent.name,
        cwd = agent.cwd,
        context_prompt = context_prompt,
        request = request,
    ))
}

fn compose_task_creation_prompt(
    source_agent: &AgentSummary,
    target_agent: &AgentSummary,
    request: &str,
) -> Result<String> {
    let context_prompt = compose_agent_context_prompt(source_agent)?;
    let target_prompt_path = target_agent.prompt_path.as_deref().unwrap_or("-");
    Ok(format!(
        "You are acting as source agent `{source_agent_name}` and must prepare one concrete delegated task for target agent `{target_agent_name}`.\n\
Do not execute the work yourself in this round. Do not modify files in this round. Your only job is to translate the operator intent into a structured task contract that agentd will create after your JSON reply.\n\
\n\
{context_prompt}\
Dispatch contract:\n\
- Return exactly one JSON object matching the provided schema and nothing else.\n\
- `title` should be a short actionable task title.\n\
- `summary` should explain the goal, boundary, and expected outcome for the target agent.\n\
- `effort` should be `null`, `medium`, `high`, or `xhigh`. Prefer `null` unless a stronger effort is clearly justified.\n\
- `read_scope` should contain the minimal absolute paths the target agent may need to read.\n\
- `write_scope` must stay inside the target repository.\n\
- `acceptance` should contain concrete verification points or deliverables.\n\
- If the operator request is broad, break out only the single next task for this target agent.\n\
- Do not wrap the JSON in markdown fences.\n\
\n\
Dispatch context:\n\
- source_agent: {source_agent_name}\n\
- source_cwd: {source_cwd}\n\
- target_agent: {target_agent_name}\n\
- target_cwd: {target_cwd}\n\
- target_prompt_path: {target_prompt_path}\n\
\n\
Operator intent:\n\
{request}\n",
        source_agent_name = source_agent.name,
        target_agent_name = target_agent.name,
        source_cwd = source_agent.cwd,
        target_cwd = target_agent.cwd,
        target_prompt_path = target_prompt_path,
        context_prompt = context_prompt,
        request = request.trim(),
    ))
}

fn compose_task_prompt(agent: &AgentSummary, task: &TaskSummary) -> Result<String> {
    let context_prompt = compose_agent_context_prompt(agent)?;
    let effort = task.effort.as_deref().unwrap_or("high");
    let read_scope = if task.read_scope.is_empty() {
        "- current repository only".to_string()
    } else {
        task.read_scope
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let write_scope = if task.write_scope.is_empty() {
        format!("- {}", agent.cwd)
    } else {
        task.write_scope
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let acceptance = if task.acceptance.is_empty() {
        "- not explicitly provided".to_string()
    } else {
        task.acceptance
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let latest_child_context = if task.round_count == 0 {
        "- latest_child_round: none\n".to_string()
    } else {
        format!(
            "- latest_child_round: {round_count}\n\
- latest_child_status: {status}\n\
- latest_child_summary: {summary}\n\
- latest_child_blocking: {blocking}\n\
- latest_child_topic: {topic}\n\
- latest_child_details: {details}\n",
            round_count = task.round_count,
            status = task.latest_child_status.as_deref().unwrap_or("-"),
            summary = task.latest_child_summary.as_deref().unwrap_or("-"),
            blocking = task.latest_child_blocking.as_deref().unwrap_or("-"),
            topic = task.latest_child_topic.as_deref().unwrap_or("-"),
            details = task.latest_child_details.as_deref().unwrap_or("-"),
        )
    };

    let latest_decision_context = if let Some(decision_id) = task.latest_decision_id.as_deref() {
        format!(
            "- latest_main_decision_id: {id}\n\
- latest_main_decision_status: {status}\n\
- latest_main_decision_summary: {summary}\n\
- latest_main_decision_issued_by: {issued_by}\n\
- latest_main_decision_at: {decision_at}\n",
            id = decision_id,
            status = task.latest_decision_status.as_deref().unwrap_or("-"),
            summary = task.latest_decision_summary.as_deref().unwrap_or("-"),
            issued_by = task.latest_decision_issued_by.as_deref().unwrap_or("-"),
            decision_at = task
                .latest_decision_at
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
        )
    } else {
        "- latest_main_decision: none\n".to_string()
    };

    Ok(format!(
        "You are executing one repository-scoped task as child agent `{agent_name}`.\n\
Work only inside the allowed write scope. You may read outside the current repository only when the repo-local instructions or the task read scope explicitly allow it.\n\
\n\
{context_prompt}\
Transport contract:\n\
- Your repo-local prompt may still ask for a human-readable `[REPORT]` or a repository progress-note update such as `work.md`. Treat those files as optional local workflow aids, not as the transport source of truth.\n\
- The authoritative communication channel for this round is the structured JSON payload plus daemon-side task and decision state kept in memory and SQLite.\n\
- Your final stdout for this round must still be exactly one JSON object that matches the provided schema and nothing else.\n\
- If you would normally answer with `[REPORT]`, translate that report into the JSON fields `summary`, `blocking`, `topic`, `details`, and `next_suggestion`.\n\
- Use `status=wait_decision` only when you must stop before continuing. Use `status=report` when you can summarize a concrete gap or issue for the main agent to analyze.\n\
\n\
Return exactly one JSON object that matches the provided schema and nothing else.\n\
\n\
Task context:\n\
- task_id: {task_id}\n\
- from: {from_agent}\n\
- to: {to_agent}\n\
- title: {title}\n\
- summary: {summary}\n\
- effort: {effort}\n\
- round_count_completed: {round_count}\n\
\n\
Read scope:\n\
{read_scope}\n\
\n\
Write scope:\n\
{write_scope}\n\
\n\
Acceptance:\n\
{acceptance}\n\
\n\
Most recent round context:\n\
{latest_child_context}\
{latest_decision_context}\
\n\
Interpretation rules:\n\
- Use status=result when you completed the requested work for this round.\n\
- Use status=report when you have a concrete issue, gap, or uncertainty for the main agent to analyze.\n\
- Use status=wait_decision when you must stop and wait before continuing.\n\
- If a latest main decision exists, treat it as the current instruction for this new round.\n\
- Keep changed_files limited to files you actually changed in this repository.\n\
- Do not wrap the JSON in markdown fences.\n",
        agent_name = agent.name,
        context_prompt = context_prompt,
        task_id = task.task_id,
        from_agent = task.from_agent,
        to_agent = task.to_agent,
        title = task.title,
        summary = task.summary,
        effort = effort,
        round_count = task.round_count,
        read_scope = read_scope,
        write_scope = write_scope,
        acceptance = acceptance,
        latest_child_context = latest_child_context,
        latest_decision_context = latest_decision_context,
    ))
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

    let (decision, continued_task) =
        resolve_task_for_main(shared, &task.task_id, &analyzer, &summary).await?;
    Ok((continued_task, Some(decision)))
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

    update_task_latest_main_decision(
        shared,
        &task,
        Some(&decision),
        "task_latest_decision_updated",
    )
    .await?;

    shared
        .events_tx
        .send(DashboardEvent::DecisionEvent {
            decision: decision.clone(),
            event_type: "decision_sent".to_string(),
        })
        .ok();
    append_runtime_event(
        shared,
        "task",
        &task.task_id,
        Some(&task.to_agent),
        Some(&task.task_id),
        None,
        Some(issued_by),
        "decision_sent",
        format!("sent decision {}", decision.decision_id),
        Some("main_decision".to_string()),
        Some(
            json!({
                "decision_id": decision.decision_id,
                "summary": decision.summary,
                "target_agent": decision.target_agent,
            })
            .to_string(),
        ),
    )
    .await?;

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
    let task = reopen_task_for_next_round(shared, task_id, &target_agent).await?;

    Ok((decision, task))
}

async fn acknowledge_latest_decision_for_task(
    shared: &AppShared,
    task_id: &str,
    agent_name: &str,
) -> Result<DecisionSummary> {
    acknowledge_latest_decision_for_task_with_stream(shared, task_id, agent_name, true).await
}

async fn acknowledge_latest_decision_for_task_with_stream(
    shared: &AppShared,
    task_id: &str,
    agent_name: &str,
    emit_stream_event: bool,
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

    update_task_latest_main_decision(
        shared,
        &task,
        Some(&updated),
        "task_latest_decision_updated",
    )
    .await?;

    shared
        .events_tx
        .send(DashboardEvent::DecisionEvent {
            decision: updated.clone(),
            event_type: "decision_acknowledged".to_string(),
        })
        .ok();
    append_runtime_event(
        shared,
        "task",
        &task.task_id,
        Some(agent_name),
        Some(&task.task_id),
        None,
        Some(agent_name),
        "decision_acknowledged",
        format!("acknowledged decision {}", updated.decision_id),
        Some("child_acknowledged".to_string()),
        Some(
            json!({
                "decision_id": updated.decision_id,
                "status": updated.status,
                "acknowledged_at": updated.acknowledged_at,
            })
            .to_string(),
        ),
    )
    .await?;

    if emit_stream_event {
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
    }

    Ok(updated)
}

async fn reopen_task_for_next_round(
    shared: &AppShared,
    task_id: &str,
    agent_name: &str,
) -> Result<TaskSummary> {
    acknowledge_latest_decision_for_task_with_stream(shared, task_id, agent_name, false).await?;

    let task = transition_task_state(
        shared,
        task_id,
        agent_name,
        TaskState::DecisionSent,
        TaskState::Pending,
        "task_reopened",
    )
    .await?;

    let mut changed_agent = None;
    {
        let mut state = shared.state.write().await;
        if let Some(agent_summary) = state.agents.get_mut(agent_name) {
            agent_summary.current_task_id = Some(task.task_id.clone());
            agent_summary.state = AgentSessionState::Busy;
            agent_summary.last_output_at = Some(Utc::now());
            agent_summary.last_heartbeat_at = Some(Utc::now());
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
            session: session.clone(),
            event_type: event_type.to_string(),
        })
        .ok();

    append_runtime_event(
        shared,
        "session",
        &session.session_id,
        Some(&session.agent_name),
        None,
        Some(&session.session_id),
        Some(&session.agent_name),
        event_type,
        format!(
            "session {:?} status {:?}",
            session.session_mode, session.status
        ),
        Some("session_state_changed".to_string()),
        Some(
            json!({
                "thread_id": session.thread_id,
                "pid": session.pid,
                "status": session.status,
                "started_at": session.started_at,
                "ended_at": session.ended_at,
                "last_output_at": session.last_output_at,
            })
            .to_string(),
        ),
    )
    .await?;

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

fn register_active_managed_session(
    shared: &AppShared,
    agent_name: &str,
    session_id: &str,
    stop: BackendStopSignal,
) {
    let mut active = shared
        .active_managed_sessions
        .lock()
        .expect("active managed session mutex poisoned");
    active.insert(
        agent_name.to_string(),
        ActiveManagedSessionControl {
            session_id: session_id.to_string(),
            stop,
        },
    );
}

fn take_active_managed_session(
    shared: &AppShared,
    agent_name: &str,
) -> Option<ActiveManagedSessionControl> {
    let mut active = shared
        .active_managed_sessions
        .lock()
        .expect("active managed session mutex poisoned");
    active.remove(agent_name)
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
    finalize_session_state_inner(shared, session_id, status, ended_at, true).await
}

async fn finalize_session_state_inner(
    shared: &AppShared,
    session_id: &str,
    status: CodexSessionStatus,
    ended_at: chrono::DateTime<Utc>,
    clear_current_session: bool,
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
    if clear_current_session {
        set_agent_current_session(shared, &agent_name, None).await?;
    }
    Ok(())
}

async fn stop_agent_session(shared: &AppShared, agent_name: &str) -> Result<String> {
    let (round_session_id, daemon_owned_app_server) = {
        let state = shared.state.read().await;
        let agent = state
            .agents
            .get(agent_name)
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?;

        (
            agent.current_session_id.clone(),
            agent.app_server_owner == Some(AppServerOwner::Daemon),
        )
    };

    if round_session_id.is_none() && daemon_owned_app_server {
        let control = take_active_managed_session(shared, agent_name)
            .ok_or_else(|| anyhow!("agent {agent_name} has no live managed session"))?;
        control.stop.stop()?;
        record_stream_event_with_session(
            shared,
            Some(&control.session_id),
            agent_name,
            "stdout",
            "[MANAGED_SESSION_STOP_REQUESTED]",
        )
        .await?;
        return Ok(control.session_id);
    }

    let session_id = round_session_id
        .ok_or_else(|| anyhow!("agent {agent_name} has no running session"))?;

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

async fn stop_managed_sessions(shared: &AppShared) -> Result<(Vec<String>, Vec<String>)> {
    let controls = {
        let mut active = shared
            .active_managed_sessions
            .lock()
            .expect("active managed session mutex poisoned");
        let mut controls = active.drain().collect::<Vec<_>>();
        controls.sort_by(|left, right| left.0.cmp(&right.0));
        controls
    };

    let mut stopped_agents = Vec::new();
    let mut failures = Vec::new();

    for (agent_name, control) in controls {
        match control.stop.stop() {
            Ok(()) => {
                record_stream_event_with_session(
                    shared,
                    Some(&control.session_id),
                    &agent_name,
                    "stdout",
                    "[MANAGED_SESSION_STOP_REQUESTED]",
                )
                .await?;
                stopped_agents.push(agent_name);
            }
            Err(err) => {
                failures.push(format!("{agent_name}: {err}"));
            }
        }
    }

    Ok((stopped_agents, failures))
}

enum VisiblePaneStopOutcome {
    StopRequested,
    AlreadyGone,
}

async fn stop_visible_panes(shared: &AppShared) -> Result<(Vec<String>, Vec<String>)> {
    let mut registered = {
        let state = shared.state.read().await;
        state
            .agents
            .values()
            .filter(|agent| agent.visible_pane_pid.is_some())
            .cloned()
            .collect::<Vec<_>>()
    };
    registered.sort_by(|left, right| left.name.cmp(&right.name));

    let mut stopped_agents = Vec::new();
    let mut failures = Vec::new();

    for agent in registered {
        let Some(pid) = agent.visible_pane_pid else {
            continue;
        };

        match terminate_registered_visible_pane(pid, &agent.name).await {
            Ok(outcome) => {
                let updated = set_agent_visible_pane(shared, &agent.name, None, None).await?;
                let (event_type, summary) = match outcome {
                    VisiblePaneStopOutcome::StopRequested => (
                        "visible_pane_stop_requested",
                        format!("stop requested for visible pane {}", pid),
                    ),
                    VisiblePaneStopOutcome::AlreadyGone => (
                        "visible_pane_missing",
                        format!("visible pane {} was already gone; cleared registration", pid),
                    ),
                };
                append_runtime_event(
                    shared,
                    "agent",
                    &agent.name,
                    Some(&agent.name),
                    updated.current_task_id.as_deref(),
                    None,
                    Some(&agent.name),
                    event_type,
                    summary,
                    Some("visible_pane".to_string()),
                    Some(
                        json!({
                            "visible_pane_pid": pid,
                            "visible_pane_kind": agent.visible_pane_kind,
                        })
                        .to_string(),
                    ),
                )
                .await?;
                stopped_agents.push(agent.name);
            }
            Err(err) => failures.push(format!("{}: {}", agent.name, err)),
        }
    }

    Ok((stopped_agents, failures))
}

async fn terminate_registered_visible_pane(
    pid: u32,
    agent_name: &str,
) -> Result<VisiblePaneStopOutcome> {
    #[cfg(windows)]
    {
        let process = match lookup_registered_visible_pane(pid, agent_name).await? {
            Some(process) => process,
            None => return Ok(VisiblePaneStopOutcome::AlreadyGone),
        };
        if !matches_registered_visible_pane(&process, agent_name, pid) {
            bail!(
                "pid {} no longer looks like the registered visible pane for agent {}",
                pid,
                agent_name
            );
        }

        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .context("failed to invoke taskkill for visible pane")?;
        if status.success() {
            return Ok(VisiblePaneStopOutcome::StopRequested);
        }

        if lookup_registered_visible_pane(pid, agent_name).await?.is_none() {
            return Ok(VisiblePaneStopOutcome::AlreadyGone);
        }

        bail!("taskkill exited with status {}", status);
    }

    #[cfg(not(windows))]
    {
        let _ = (pid, agent_name);
        bail!("visible pane stop is only supported on Windows");
    }
}

async fn lookup_registered_visible_pane(
    pid: u32,
    agent_name: &str,
) -> Result<Option<WindowsProcessInfo>> {
    #[cfg(windows)]
    {
        let script = format!(
            "$p = Get-CimInstance Win32_Process -Filter \"ProcessId = {pid}\" -ErrorAction SilentlyContinue; \
if ($null -eq $p) {{ exit 3 }}; \
[pscustomobject]@{{ process_id = [uint32]$p.ProcessId; name = [string]$p.Name; executable_path = [string]$p.ExecutablePath; command_line = [string]$p.CommandLine }} | ConvertTo-Json -Compress"
        );
        let output = Command::new("powershell.exe")
            .args(["-NoProfile", "-Command", &script])
            .stdin(std::process::Stdio::null())
            .output()
            .await
            .context("failed to inspect visible pane process")?;

        match output.status.code() {
            Some(0) => {
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if stdout.is_empty() {
                    bail!(
                        "process inspection returned empty payload for visible pane {} ({})",
                        agent_name,
                        pid
                    );
                }
                let process = serde_json::from_str::<WindowsProcessInfo>(&stdout)
                    .with_context(|| format!("failed to parse process inspection payload: {stdout}"))?;
                Ok(Some(process))
            }
            Some(3) => Ok(None),
            _ => {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                bail!(
                    "process inspection failed for visible pane {} ({}): {}",
                    agent_name,
                    pid,
                    if stderr.is_empty() {
                        output.status.to_string()
                    } else {
                        stderr
                    }
                )
            }
        }
    }

    #[cfg(not(windows))]
    {
        let _ = (pid, agent_name);
        Ok(None)
    }
}

fn matches_registered_visible_pane(process: &WindowsProcessInfo, agent_name: &str, pid: u32) -> bool {
    if process.process_id != pid {
        return false;
    }

    let process_name = process.name.as_deref().unwrap_or_default().to_ascii_lowercase();
    if process_name != "powershell.exe" && process_name != "pwsh.exe" {
        return false;
    }

    let executable_path = process
        .executable_path
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if executable_path.contains("\\windows\\system32\\windowspowershell\\")
        || executable_path.ends_with("\\powershell.exe")
        || executable_path.ends_with("\\pwsh.exe")
    {
        // expected shell executable
    } else if !executable_path.is_empty() {
        return false;
    }

    let command_line = process
        .command_line
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if command_line.is_empty() {
        return false;
    }

    let has_script_marker = command_line.contains("enter-agentshell.ps1")
        || command_line.contains("enter-agentview.ps1");
    let has_agent_marker = command_line.contains("-agentname")
        && command_line.contains(&agent_name.to_ascii_lowercase());

    has_script_marker && has_agent_marker
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
    append_runtime_event(
        shared,
        "agent",
        agent_name,
        Some(agent_name),
        None,
        None,
        Some(agent_name),
        "agent_recovered",
        "manually recovered blocked agent to idle",
        Some("control_request".to_string()),
        None,
    )
    .await?;

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
    append_runtime_event(
        shared,
        "agent",
        agent_name,
        Some(agent_name),
        None,
        None,
        Some(agent_name),
        "thread_reset",
        "cleared cached thread id",
        Some("control_request".to_string()),
        None,
    )
    .await?;

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
        finalize_session_state_inner(
            shared,
            &session.session_id,
            CodexSessionStatus::Failed,
            now,
            session.session_mode != SessionMode::AppServer,
        )
        .await?;

        if session.session_mode == SessionMode::AppServer {
            let _ = reset_daemon_app_server_binding(
                shared,
                &session.agent_name,
                "daemon-managed session was still marked running on startup",
            )
            .await;
        }

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

async fn normalize_runtime_state_on_startup(shared: &AppShared) -> Result<()> {
    let now = Utc::now();
    let updates = {
        let state = shared.state.read().await;
        let mut open_tasks_by_agent: HashMap<String, Vec<String>> = HashMap::new();
        for task in state.tasks.values() {
            if !is_task_terminal(&task.state) {
                open_tasks_by_agent
                    .entry(task.to_agent.clone())
                    .or_default()
                    .push(task.task_id.clone());
            }
        }

        state
            .agents
            .values()
            .filter_map(|agent| {
                let mut updated = agent.clone();
                let mut changed = false;

                if let Some(session_id) = updated.current_session_id.clone() {
                    let valid_running_session = state
                        .sessions
                        .get(&session_id)
                        .map(|session| session.status == CodexSessionStatus::Running)
                        .unwrap_or(false);
                    if !valid_running_session {
                        updated.current_session_id = None;
                        changed = true;
                    }
                }

                if updated.app_server_owner == Some(AppServerOwner::Daemon) {
                    let has_live_daemon_session = state.sessions.values().any(|session| {
                        session.agent_name == updated.name
                            && session.session_mode == SessionMode::AppServer
                            && session.status == CodexSessionStatus::Running
                    });
                    if !has_live_daemon_session
                        && (updated.app_server_url.is_some()
                            || updated.thread_id.is_some()
                            || updated.bootstrap_state == AgentBootstrapState::Ready)
                    {
                        updated.app_server_url = None;
                        updated.app_server_owner = None;
                        updated.app_server_registered_at = None;
                        updated.thread_id = None;
                        updated.bootstrap_state = AgentBootstrapState::AwaitingInit;
                        updated.bootstrap_summary = None;
                        updated.bootstrap_completed_at = None;
                        changed = true;
                    }
                }

                if let Some(task_id) = updated.current_task_id.clone() {
                    let valid_open_task = state
                        .tasks
                        .get(&task_id)
                        .map(|task| !is_task_terminal(&task.state))
                        .unwrap_or(false);
                    if !valid_open_task {
                        updated.current_task_id = None;
                        changed = true;
                    }
                }

                if updated.current_task_id.is_none() && updated.current_session_id.is_none() {
                    let open_task_ids = open_tasks_by_agent
                        .get(&updated.name)
                        .cloned()
                        .unwrap_or_default();
                    if open_task_ids.len() == 1 {
                        updated.current_task_id = Some(open_task_ids[0].clone());
                        changed = true;
                    }
                }

                let desired_state = desired_agent_runtime_state(&state, &updated);
                if updated.state != desired_state {
                    updated.state = desired_state;
                    changed = true;
                }

                if changed {
                    updated.last_heartbeat_at = Some(now);
                    Some(updated)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    };

    if updates.is_empty() {
        return Ok(());
    }

    {
        let db = shared.db.lock().expect("db mutex poisoned");
        for agent in &updates {
            db.upsert_agent(agent)?;
        }
    }

    {
        let mut state = shared.state.write().await;
        for agent in updates {
            state.agents.insert(agent.name.clone(), agent);
        }
    }

    Ok(())
}

async fn recover_stale_visible_panes_on_startup(shared: &AppShared) -> Result<()> {
    let registered = {
        let state = shared.state.read().await;
        state
            .agents
            .values()
            .filter(|agent| agent.visible_pane_pid.is_some())
            .cloned()
            .collect::<Vec<_>>()
    };

    for agent in registered {
        let Some(pid) = agent.visible_pane_pid else {
            continue;
        };
        if lookup_registered_visible_pane(pid, &agent.name).await?.is_some() {
            continue;
        }

        let updated = set_agent_visible_pane(shared, &agent.name, None, None).await?;
        append_runtime_event(
            shared,
            "agent",
            &agent.name,
            Some(&agent.name),
            updated.current_task_id.as_deref(),
            None,
            Some(&agent.name),
            "visible_pane_recovered",
            format!("cleared stale visible pane registration {}", pid),
            Some("startup_recovery".to_string()),
            None,
        )
        .await?;
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

async fn touch_agent_heartbeat(shared: &AppShared, agent_name: &str) -> Result<AgentSummary> {
    let mut updated = {
        let state = shared.state.read().await;
        let agent = state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?;

        let mut updated = agent.clone();
        updated.last_heartbeat_at = Some(Utc::now());
        if updated.state == AgentSessionState::Offline {
            updated.state = desired_agent_runtime_state(&state, &agent);
        }
        updated
    };

    if updated.role == AgentRole::Main && updated.state == AgentSessionState::Offline {
        updated.state = AgentSessionState::Idle;
    }

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

    Ok(updated)
}

async fn begin_agent_bootstrap(shared: &AppShared, agent_name: &str) -> Result<AgentSummary> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    updated.bootstrap_state = AgentBootstrapState::AwaitingInit;
    updated.bootstrap_summary = None;
    updated.bootstrap_completed_at = None;
    updated.app_server_url = None;
    updated.app_server_owner = None;
    updated.app_server_registered_at = None;
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

    record_stream_event(shared, agent_name, "stdout", "[AGENT_BOOTSTRAP_PENDING]").await?;
    append_runtime_event(
        shared,
        "agent",
        agent_name,
        Some(agent_name),
        None,
        None,
        Some(agent_name),
        "agent_bootstrap_started",
        "bootstrap reset and awaiting init",
        Some("bootstrap_contract".to_string()),
        None,
    )
    .await?;

    Ok(updated)
}

async fn mark_agent_ready(
    shared: &AppShared,
    agent_name: &str,
    summary: Option<String>,
    thread_id: Option<String>,
) -> Result<AgentSummary> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    let normalized_summary = summary
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let normalized_thread_id = thread_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    updated.bootstrap_state = AgentBootstrapState::Ready;
    updated.bootstrap_summary = normalized_summary.clone();
    updated.bootstrap_completed_at = Some(Utc::now());
    updated.last_heartbeat_at = Some(Utc::now());
    if let Some(thread_id) = normalized_thread_id {
        updated.thread_id = Some(thread_id);
    }

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

    let content = if let Some(summary) = normalized_summary {
        format!("[AGENT_READY] {summary}")
    } else {
        "[AGENT_READY]".to_string()
    };
    record_stream_event(shared, agent_name, "stdout", &content).await?;
    append_runtime_event(
        shared,
        "agent",
        agent_name,
        Some(agent_name),
        None,
        None,
        Some(agent_name),
        "agent_ready",
        updated
            .bootstrap_summary
            .clone()
            .unwrap_or_else(|| "agent ready".to_string()),
        Some("bootstrap_contract".to_string()),
        Some(
            json!({
                "thread_id": updated.thread_id.clone(),
                "bootstrap_completed_at": updated.bootstrap_completed_at.clone(),
            })
            .to_string(),
        ),
    )
    .await?;

    Ok(updated)
}

async fn set_agent_visible_pane(
    shared: &AppShared,
    agent_name: &str,
    pid: Option<u32>,
    kind: Option<VisiblePaneKind>,
) -> Result<AgentSummary> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    let now = Utc::now();
    match pid {
        Some(pid) => {
            let kind =
                kind.ok_or_else(|| anyhow!("visible pane kind is required when pid is set"))?;
            updated.visible_pane_pid = Some(pid);
            updated.visible_pane_kind = Some(kind.clone());
            updated.visible_pane_registered_at = Some(now);
            updated.last_heartbeat_at = Some(now);

            let updated = persist_agent_update(shared, updated).await?;
            append_runtime_event(
                shared,
                "agent",
                agent_name,
                Some(agent_name),
                updated.current_task_id.as_deref(),
                None,
                Some(agent_name),
                "visible_pane_registered",
                format!("registered {:?} pane {}", kind, pid),
                Some("visible_pane".to_string()),
                Some(
                    json!({
                        "visible_pane_pid": updated.visible_pane_pid,
                        "visible_pane_kind": updated.visible_pane_kind,
                        "visible_pane_registered_at": updated.visible_pane_registered_at,
                    })
                    .to_string(),
                ),
            )
            .await?;
            Ok(updated)
        }
        None => {
            let had_registration = updated.visible_pane_pid.is_some()
                || updated.visible_pane_kind.is_some()
                || updated.visible_pane_registered_at.is_some();
            updated.visible_pane_pid = None;
            updated.visible_pane_kind = None;
            updated.visible_pane_registered_at = None;
            updated.last_heartbeat_at = Some(now);

            let updated = persist_agent_update(shared, updated).await?;
            if had_registration {
                append_runtime_event(
                    shared,
                    "agent",
                    agent_name,
                    Some(agent_name),
                    updated.current_task_id.as_deref(),
                    None,
                    Some(agent_name),
                    "visible_pane_cleared",
                    "visible pane registration cleared",
                    Some("visible_pane".to_string()),
                    None,
                )
                .await?;
            }
            Ok(updated)
        }
    }
}

async fn set_agent_app_server(
    shared: &AppShared,
    agent_name: &str,
    app_server_url: Option<String>,
    owner: Option<AppServerOwner>,
) -> Result<AgentSummary> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    let normalized_url = app_server_url
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let now = Utc::now();

    updated.app_server_url = normalized_url.clone();
    updated.app_server_owner = normalized_url
        .as_ref()
        .map(|_| owner.unwrap_or_else(|| default_app_server_owner(&updated)));
    updated.app_server_registered_at = normalized_url.as_ref().map(|_| now);
    if normalized_url.is_none() {
        updated.app_server_owner = None;
    }
    updated.last_heartbeat_at = Some(now);

    let updated = persist_agent_update(shared, updated).await?;
    append_runtime_event(
        shared,
        "agent",
        agent_name,
        Some(agent_name),
        None,
        None,
        Some(agent_name),
        if updated.app_server_url.is_some() {
            "app_server_registered"
        } else {
            "app_server_cleared"
        },
        updated
            .app_server_url
            .clone()
            .unwrap_or_else(|| "app server cleared".to_string()),
        Some("app_server_registration".to_string()),
        Some(
            json!({
                "app_server_url": updated.app_server_url.clone(),
                "app_server_owner": updated.app_server_owner.clone(),
                "registered_at": updated.app_server_registered_at.clone(),
            })
            .to_string(),
        ),
    )
    .await?;

    Ok(updated)
}

fn default_app_server_owner(agent: &AgentSummary) -> AppServerOwner {
    if agent.bridge_state == BridgeConnectionState::Connected {
        AppServerOwner::Bridge
    } else {
        AppServerOwner::Daemon
    }
}

fn parse_app_server_owner_label(value: &str) -> Result<AppServerOwner> {
    match value.trim().to_ascii_lowercase().as_str() {
        "bridge" => Ok(AppServerOwner::Bridge),
        "daemon" => Ok(AppServerOwner::Daemon),
        other => bail!("unsupported app-server owner: {other}"),
    }
}

fn parse_visible_pane_kind_label(value: &str) -> Result<VisiblePaneKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "shell" => Ok(VisiblePaneKind::Shell),
        "view" => Ok(VisiblePaneKind::View),
        other => bail!("unsupported visible pane kind: {other}"),
    }
}

fn default_managed_bootstrap_prompt(agent: &AgentSummary) -> String {
    let prompt_label = agent
        .prompt_path
        .as_ref()
        .and_then(|path| Path::new(path).file_name())
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| {
            if agent.role == AgentRole::Main {
                "MAIN_AGENT_PROMPT.md".to_string()
            } else {
                "SUBAGENT_PROMPT.md".to_string()
            }
        });

    format!(
        "先按 UTF-8 读取当前工作区中的 {prompt_label}，并将其视为你的角色契约。暂时不要开始工作，先总结自己的当前职责边界和待命状态，不要开始新的工作，等待下一条消息。"
    )
}

fn load_managed_session_developer_instructions(
    shared: &AppShared,
    agent: &AgentSummary,
) -> Result<Option<String>> {
    let Some(prompt_path) = agent.prompt_path.as_ref() else {
        return Ok(None);
    };

    let prompt_text = fs::read_to_string(prompt_path)
        .with_context(|| format!("failed to read agent prompt as UTF-8: {prompt_path}"))?;
    let mut sections = vec![
        format!(
            "以下内容由 AgentTool 以 UTF-8 从工作区提示词文件读取，并作为你的长期角色契约。\n契约文件: {prompt_path}"
        ),
        prompt_text.trim().to_string(),
    ];

    if agent.name == "main" {
        let role_contract_path = shared.config.root_dir.join("ROLE_CONTRACT.md");
        if role_contract_path.is_file() {
            let role_contract_text = fs::read_to_string(&role_contract_path).with_context(|| {
                format!(
                    "failed to read role contract as UTF-8: {}",
                    role_contract_path.display()
                )
            })?;
            sections.push(format!(
                "以下内容同样由 AgentTool 以 UTF-8 读取，并作为主 agent 的额外任务流约束。\n契约文件: {}",
                role_contract_path.display()
            ));
            sections.push(role_contract_text.trim().to_string());
        }
    }

    Ok(Some(sections.join("\n\n")))
}

async fn reset_daemon_app_server_binding(
    shared: &AppShared,
    agent_name: &str,
    reason: &str,
) -> Result<AgentSummary> {
    let mut updated = {
        let state = shared.state.read().await;
        state
            .agents
            .get(agent_name)
            .cloned()
            .ok_or_else(|| anyhow!("agent not registered: {agent_name}"))?
    };

    updated.app_server_url = None;
    updated.app_server_owner = None;
    updated.app_server_registered_at = None;
    updated.thread_id = None;
    updated.bootstrap_state = AgentBootstrapState::AwaitingInit;
    updated.bootstrap_summary = None;
    updated.bootstrap_completed_at = None;
    updated.last_heartbeat_at = Some(Utc::now());
    if updated.current_task_id.is_some() {
        updated.state = AgentSessionState::Blocked;
    } else if updated.current_session_id.is_none() {
        updated.state = AgentSessionState::Idle;
    }

    let updated = persist_agent_update(shared, updated).await?;
    append_runtime_event(
        shared,
        "agent",
        agent_name,
        Some(agent_name),
        updated.current_task_id.as_deref(),
        None,
        Some(agent_name),
        "daemon_app_server_reset",
        reason.to_string(),
        Some("managed_session".to_string()),
        Some(
            json!({
                "bootstrap_state": updated.bootstrap_state,
                "state": updated.state,
            })
            .to_string(),
        ),
    )
    .await?;

    Ok(updated)
}

async fn ensure_managed_session(
    shared: &AppShared,
    agent_name: &str,
    bootstrap_prompt: Option<String>,
) -> Result<AgentSummary> {
    let already_active = {
        let active = shared
            .active_managed_sessions
            .lock()
            .expect("active managed session mutex poisoned");
        active.contains_key(agent_name)
    };
    if already_active {
        let state = shared.state.read().await;
        if let Some(agent) = state.agents.get(agent_name).cloned() {
            return Ok(agent);
        }
    }

    let agent = begin_agent_bootstrap(shared, agent_name).await?;
    let backend = start_app_server_backend(AppServerStartRequest {
        agent: agent.clone(),
        launcher_path: shared.config.codex_launcher.clone(),
    })?;
    let session_id = format!("S-{}", Uuid::now_v7().simple());
    let app_server_url = backend
        .endpoint
        .clone()
        .ok_or_else(|| anyhow!("managed app-server did not expose an endpoint"))?;
    let crate::backend::BackendHandle {
        session_mode,
        pid,
        endpoint: _,
        stop,
        mut events,
        completion,
    } = backend;

    let session = SessionSummary {
        session_id: session_id.clone(),
        agent_name: agent.name.clone(),
        session_mode,
        pid,
        thread_id: None,
        status: CodexSessionStatus::Running,
        started_at: Utc::now(),
        ended_at: None,
        last_output_at: None,
    };
    upsert_session_state(shared, session.clone(), "session_started").await?;
    register_active_managed_session(shared, agent_name, &session_id, stop);

    let shared_for_monitor = shared.clone();
    let monitor_agent = agent.name.clone();
    let monitor_session_id = session_id.clone();
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let BackendEvent::Line { stream, line } = event {
                let stream_name = match stream {
                    BackendStream::Stdout => "stdout",
                    BackendStream::Stderr => "stderr",
                };
                if let Err(err) = record_stream_event_with_session(
                    &shared_for_monitor,
                    Some(&monitor_session_id),
                    &monitor_agent,
                    stream_name,
                    &line,
                )
                .await
                {
                    warn!(agent = %monitor_agent, session = %monitor_session_id, %err, "failed to persist managed session stream event");
                }
            }
        }

        let finished = match completion.await {
            Ok(Ok(finished)) => finished,
            Ok(Err(err)) => {
                warn!(agent = %monitor_agent, session = %monitor_session_id, %err, "managed session completion failed");
                if let Err(reset_err) = reset_daemon_app_server_binding(
                    &shared_for_monitor,
                    &monitor_agent,
                    &format!("managed session completion failed: {err}"),
                )
                .await
                {
                    warn!(agent = %monitor_agent, %reset_err, "failed to reset agent after managed session completion error");
                }
                return;
            }
            Err(err) => {
                warn!(agent = %monitor_agent, session = %monitor_session_id, %err, "managed session join failed");
                if let Err(reset_err) = reset_daemon_app_server_binding(
                    &shared_for_monitor,
                    &monitor_agent,
                    &format!("managed session join failed: {err}"),
                )
                .await
                {
                    warn!(agent = %monitor_agent, %reset_err, "failed to reset agent after managed session join failure");
                }
                return;
            }
        };

        let _ = take_active_managed_session(&shared_for_monitor, &monitor_agent);
        if let Err(err) = finalize_session_state_inner(
            &shared_for_monitor,
            &monitor_session_id,
            if finished.success || finished.stopped {
                CodexSessionStatus::Succeeded
            } else {
                CodexSessionStatus::Failed
            },
            Utc::now(),
            false,
        )
        .await
        {
            warn!(agent = %monitor_agent, session = %monitor_session_id, %err, "failed to finalize managed session state");
        }

        let stderr_tail = finished
            .stderr_lines
            .iter()
            .rev()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        let process_state = if finished.stopped {
            "stopped"
        } else if finished.success {
            "exited"
        } else {
            "failed"
        };
        let observation_summary = if finished.stopped {
            "daemon 管理的 app-server 已按请求停止。".to_string()
        } else if finished.success {
            "daemon 管理的 app-server 已退出。".to_string()
        } else {
            "daemon 管理的 app-server 异常退出。".to_string()
        };
        if let Err(err) = append_runtime_event(
            &shared_for_monitor,
            "agent",
            &monitor_agent,
            Some(&monitor_agent),
            None,
            Some(&monitor_session_id),
            Some(&monitor_agent),
            "managed_app_server_finished",
            observation_summary,
            Some("managed_session_monitor".to_string()),
            Some(
                json!({
                    "session_id": monitor_session_id,
                    "process_state": process_state,
                    "status_label": finished.status_label,
                    "success": finished.success,
                    "stopped": finished.stopped,
                    "stderr_tail": stderr_tail,
                })
                .to_string(),
            ),
        )
        .await
        {
            warn!(agent = %monitor_agent, session = %monitor_session_id, %err, "failed to append managed app-server observation");
        }

        let reason = if finished.stopped {
            "managed session stopped by request".to_string()
        } else if finished.success {
            "managed session exited".to_string()
        } else {
            let detail = finished
                .stderr_lines
                .last()
                .cloned()
                .unwrap_or_else(|| finished.status_label.clone());
            format!("managed session exited unexpectedly: {detail}")
        };
        if let Err(err) =
            reset_daemon_app_server_binding(&shared_for_monitor, &monitor_agent, &reason).await
        {
            warn!(agent = %monitor_agent, %err, "failed to reset daemon app-server binding");
        }
    });

    wait_for_app_server_ready(
        &app_server_url,
        std::time::Duration::from_secs(shared.config.app_server_start_timeout_seconds),
    )
    .await
    .with_context(|| format!("managed app-server did not become ready for agent {agent_name}"))?;

    set_agent_app_server(
        shared,
        agent_name,
        Some(app_server_url.clone()),
        Some(AppServerOwner::Daemon),
    )
    .await?;

    let developer_instructions = load_managed_session_developer_instructions(shared, &agent)?;
    let bootstrap_prompt = bootstrap_prompt.unwrap_or_else(|| default_managed_bootstrap_prompt(&agent));
    let schema_path = shared
        .config
        .root_dir
        .join("schemas")
        .join("bootstrap_ready.schema.json");

    let bootstrap_timeout = std::time::Duration::from_secs(
        shared.config.managed_bootstrap_timeout_seconds,
    );
    let round = match tokio::time::timeout(
        bootstrap_timeout,
        run_remote_bootstrap_round(
            &agent.name,
            &app_server_url,
            &agent.cwd,
            developer_instructions.as_deref(),
            &bootstrap_prompt,
            Some(schema_path.to_string_lossy().as_ref()),
        ),
    )
    .await
    {
        Ok(Ok(round)) => round,
        Ok(Err(err)) => {
            if let Some(control) = take_active_managed_session(shared, agent_name) {
                let _ = control.stop.stop();
            }
            let _ = reset_daemon_app_server_binding(
                shared,
                agent_name,
                &format!("managed bootstrap failed: {err}"),
            )
            .await;
            return Err(err);
        }
        Err(_) => {
            let timeout_message = format!(
                "managed bootstrap timed out after {}s",
                shared.config.managed_bootstrap_timeout_seconds
            );
            if let Some(control) = take_active_managed_session(shared, agent_name) {
                let _ = control.stop.stop();
            }
            let _ = reset_daemon_app_server_binding(shared, agent_name, &timeout_message).await;
            return Err(anyhow!(timeout_message));
        }
    };

    let payload: serde_json::Value = serde_json::from_str(&round.final_message).with_context(|| {
        format!("failed to parse managed bootstrap payload as json for agent {agent_name}")
    })?;
    let status = payload
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    if status != "ready" {
        bail!("managed bootstrap payload status must be ready, actual {status}");
    }
    let summary = payload
        .get("summary")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("managed bootstrap payload summary must not be empty"))?
        .to_string();

    let updated = mark_agent_ready(shared, agent_name, Some(summary), Some(round.thread_id)).await?;
    append_runtime_event(
        shared,
        "agent",
        agent_name,
        Some(agent_name),
        None,
        Some(&session_id),
        Some(agent_name),
        "managed_session_ready",
        format!("daemon-managed app-server ready at {app_server_url}"),
        Some("managed_session".to_string()),
        Some(
            json!({
                "app_server_url": app_server_url,
                "session_id": session_id,
            })
            .to_string(),
        ),
    )
    .await?;

    Ok(updated)
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
        endpoint: _,
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
        if finished.success {
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

    if !finished.success {
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
            .unwrap_or_else(|| format!("backend ended with status {}", finished.status_label));
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

    transition_impl(shared, task, next, event_type, Some(agent)).await
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

    transition_impl(shared, task, next, event_type, None).await
}

async fn transition_impl(
    shared: &AppShared,
    mut task: TaskSummary,
    next: TaskState,
    event_type: &str,
    actor_name: Option<&str>,
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
            Some(from_state.clone()),
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
    append_runtime_event(
        shared,
        "task",
        &task.task_id,
        Some(&task.to_agent),
        Some(&task.task_id),
        None,
        actor_name,
        event_type,
        format!("task state {:?} -> {:?}", from_state, task.state),
        Some("task_state_transition".to_string()),
        Some(
            json!({
                "from_state": from_state.clone(),
                "to_state": task.state.clone(),
                "from_agent": task.from_agent.clone(),
                "to_agent": task.to_agent.clone(),
            })
            .to_string(),
        ),
    )
    .await?;
    if matches!(
        task.state,
        TaskState::Pending | TaskState::Cancelled | TaskState::Closed
    ) {
        sync_bridge_for_task(shared, &task, event_type).await?;
    }

    Ok(task)
}

#[cfg(test)]
mod tests {
    use super::{
        compose_agent_round_prompt, compose_task_creation_prompt, compose_task_prompt,
        desired_agent_state_for_task, ensure_agent_ready_for_ad_hoc_round,
        ensure_agent_ready_for_new_task, ensure_agent_ready_for_task_round,
        normalize_task_draft_payload,
    };
    use crate::models::{
        AgentBootstrapState, AgentRole, AgentSessionState, AgentSummary, BridgeConnectionState,
        TaskDraftPayload, TaskState, TaskSummary,
    };
    use chrono::Utc;
    use std::fs;

    fn sample_agent() -> AgentSummary {
        AgentSummary {
            name: "child".to_string(),
            role: AgentRole::Child,
            repo_name: Some("repo".to_string()),
            cwd: ".".to_string(),
            prompt_path: None,
            thread_id: None,
            app_server_url: None,
            app_server_owner: None,
            app_server_registered_at: None,
            visible_pane_pid: None,
            visible_pane_kind: None,
            visible_pane_registered_at: None,
            current_session_id: None,
            state: AgentSessionState::Idle,
            bootstrap_state: AgentBootstrapState::Ready,
            bootstrap_summary: None,
            bootstrap_completed_at: None,
            current_task_id: None,
            last_output_at: None,
            last_heartbeat_at: None,
            bridge_state: BridgeConnectionState::Disconnected,
            bridge_mode: None,
            bridge_session_id: None,
            bridge_connected_at: None,
            bridge_last_seen_at: None,
            bridge_last_delivery_id: 0,
            bridge_last_ack_delivery_id: 0,
            bridge_pending_delivery_count: 0,
        }
    }

    fn sample_task(state: TaskState) -> TaskSummary {
        TaskSummary {
            task_id: "T-1".to_string(),
            from_agent: "main".to_string(),
            to_agent: "child".to_string(),
            title: "demo".to_string(),
            summary: "demo".to_string(),
            effort: None,
            read_scope: Vec::new(),
            write_scope: Vec::new(),
            acceptance: Vec::new(),
            auto_resolve_by: None,
            auto_resolve_summary: None,
            round_count: 0,
            latest_child_status: None,
            latest_child_summary: None,
            latest_child_blocking: None,
            latest_child_topic: None,
            latest_child_details: None,
            latest_decision_id: None,
            latest_decision_summary: None,
            latest_decision_status: None,
            latest_decision_issued_by: None,
            latest_decision_at: None,
            state,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            closed_at: None,
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

    #[test]
    fn reported_like_task_states_block_the_agent() {
        for task_state in [
            TaskState::Reported,
            TaskState::Analyzed,
            TaskState::DecisionSent,
            TaskState::BlockedWaitingDecision,
        ] {
            let task = sample_task(task_state);
            assert_eq!(
                desired_agent_state_for_task(&task),
                AgentSessionState::Blocked
            );
        }
    }

    #[test]
    fn task_prompt_includes_latest_round_and_decision_context() {
        let mut task = sample_task(TaskState::Pending);
        task.round_count = 2;
        task.latest_child_status = Some("wait_decision".to_string());
        task.latest_child_summary = Some("Need input".to_string());
        task.latest_child_blocking = Some("P1".to_string());
        task.latest_child_topic = Some("schema".to_string());
        task.latest_child_details = Some("Need the field contract".to_string());
        task.latest_decision_id = Some("D-1".to_string());
        task.latest_decision_summary = Some("Proceed with the new schema".to_string());
        task.latest_decision_status = Some("acknowledged".to_string());
        task.latest_decision_issued_by = Some("main".to_string());
        task.latest_decision_at = Some(Utc::now());

        let prompt = compose_task_prompt(&sample_agent(), &task).expect("compose task prompt");
        assert!(prompt.contains("latest_child_round: 2"));
        assert!(prompt.contains("latest_child_summary: Need input"));
        assert!(prompt.contains("latest_main_decision_summary: Proceed with the new schema"));
        assert!(prompt.contains("latest_main_decision_issued_by: main"));
    }

    #[test]
    fn agent_round_prompt_reads_repo_prompt_and_context_files() {
        let temp_dir = std::env::temp_dir().join(format!(
            "agenttool_prompt_test_{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        fs::write(temp_dir.join("SUBAGENT_PROMPT.md"), "# role").expect("write prompt");
        fs::write(temp_dir.join("work.md"), "current work").expect("write work");

        let mut agent = sample_agent();
        agent.cwd = temp_dir.to_string_lossy().to_string();
        agent.prompt_path = Some(
            temp_dir
                .join("SUBAGENT_PROMPT.md")
                .to_string_lossy()
                .to_string(),
        );

        let prompt = compose_agent_round_prompt(&agent, "Continue the assigned role.")
            .expect("compose agent round prompt");
        assert!(prompt.contains("SUBAGENT_PROMPT.md"));
        assert!(prompt.contains("work.md"));
        assert!(prompt.contains("# role"));
        assert!(prompt.contains("current work"));
        assert!(prompt.contains("Continue the assigned role."));

        fs::remove_dir_all(&temp_dir).expect("cleanup temp dir");
    }

    #[test]
    fn task_creation_prompt_includes_target_context_and_request() {
        let mut source = sample_agent();
        source.name = "main".to_string();
        source.cwd = "F:\\work\\github\\hackman".to_string();

        let mut target = sample_agent();
        target.name = "guardpro_control".to_string();
        target.cwd = "F:\\work\\github\\hackman\\guardpro_control".to_string();
        target.prompt_path = Some(
            "F:\\work\\github\\hackman\\guardpro_control\\SUBAGENT_PROMPT.md".to_string(),
        );

        let prompt = compose_task_creation_prompt(
            &source,
            &target,
            "分析当前位置页并整理成一条可执行任务。",
        )
        .expect("compose task creation prompt");

        assert!(prompt.contains("source agent `main`"));
        assert!(prompt.contains("target agent `guardpro_control`"));
        assert!(prompt.contains("F:\\work\\github\\hackman\\guardpro_control"));
        assert!(prompt.contains("分析当前位置页并整理成一条可执行任务。"));
    }

    #[test]
    fn task_draft_normalization_defaults_and_constrains_write_scope() {
        let mut target = sample_agent();
        target.cwd = "F:\\work\\github\\hackman\\guardpro_control".to_string();

        let normalized = normalize_task_draft_payload(
            &target,
            TaskDraftPayload {
                title: "  修复位置页  ".to_string(),
                summary: "  补齐状态与交互  ".to_string(),
                effort: Some("HIGH".to_string()),
                read_scope: vec!["  ".to_string(), target.cwd.clone()],
                write_scope: Vec::new(),
                acceptance: vec!["  页面可打开  ".to_string(), "".to_string()],
            },
        )
        .expect("normalize task draft");

        assert_eq!(normalized.title, "修复位置页");
        assert_eq!(normalized.summary, "补齐状态与交互");
        assert_eq!(normalized.effort.as_deref(), Some("high"));
        assert_eq!(normalized.write_scope, vec![target.cwd.clone()]);
        assert_eq!(normalized.acceptance, vec!["页面可打开".to_string()]);

        let err = normalize_task_draft_payload(
            &target,
            TaskDraftPayload {
                title: "x".to_string(),
                summary: "y".to_string(),
                effort: None,
                read_scope: Vec::new(),
                write_scope: vec!["F:\\work\\github\\hackman\\other_repo".to_string()],
                acceptance: Vec::new(),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("outside target repository"));
    }
}
