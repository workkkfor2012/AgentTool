use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Main,
    Child,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentSessionState {
    Idle,
    Busy,
    Blocked,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentBootstrapState {
    AwaitingInit,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentContextSourceKind {
    PromptContract,
    WorkingNotes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Accepted,
    Running,
    Completed,
    Reported,
    Analyzed,
    DecisionSent,
    Closed,
    BlockedWaitingDecision,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CodexSessionStatus {
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionMode {
    Round,
    Pty,
    AppServer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BridgeConnectionState {
    Disconnected,
    Connected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BridgeMode {
    Passive,
    Autorun,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppServerOwner {
    Bridge,
    Daemon,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VisiblePaneKind {
    Shell,
    View,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSummary {
    pub name: String,
    pub role: AgentRole,
    pub repo_name: Option<String>,
    pub cwd: String,
    pub prompt_path: Option<String>,
    pub thread_id: Option<String>,
    pub app_server_url: Option<String>,
    pub app_server_owner: Option<AppServerOwner>,
    pub app_server_registered_at: Option<DateTime<Utc>>,
    pub visible_pane_pid: Option<u32>,
    pub visible_pane_kind: Option<VisiblePaneKind>,
    pub visible_pane_registered_at: Option<DateTime<Utc>>,
    pub current_session_id: Option<String>,
    pub state: AgentSessionState,
    pub bootstrap_state: AgentBootstrapState,
    pub bootstrap_summary: Option<String>,
    pub bootstrap_completed_at: Option<DateTime<Utc>>,
    pub current_task_id: Option<String>,
    pub last_output_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub bridge_state: BridgeConnectionState,
    pub bridge_mode: Option<BridgeMode>,
    pub bridge_session_id: Option<String>,
    pub bridge_connected_at: Option<DateTime<Utc>>,
    pub bridge_last_seen_at: Option<DateTime<Utc>>,
    pub bridge_last_delivery_id: u64,
    pub bridge_last_ack_delivery_id: u64,
    pub bridge_pending_delivery_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSummary {
    pub task_id: String,
    pub from_agent: String,
    pub to_agent: String,
    pub title: String,
    pub summary: String,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub read_scope: Vec<String>,
    #[serde(default)]
    pub write_scope: Vec<String>,
    #[serde(default)]
    pub acceptance: Vec<String>,
    pub auto_resolve_by: Option<String>,
    pub auto_resolve_summary: Option<String>,
    pub round_count: u32,
    pub latest_child_status: Option<String>,
    pub latest_child_summary: Option<String>,
    pub latest_child_blocking: Option<String>,
    pub latest_child_topic: Option<String>,
    pub latest_child_details: Option<String>,
    pub latest_decision_id: Option<String>,
    pub latest_decision_summary: Option<String>,
    pub latest_decision_status: Option<String>,
    pub latest_decision_issued_by: Option<String>,
    pub latest_decision_at: Option<DateTime<Utc>>,
    pub state: TaskState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionSummary {
    pub decision_id: String,
    pub task_id: String,
    pub issued_by: String,
    pub target_agent: String,
    pub summary: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub acknowledged_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub agent_name: String,
    pub session_mode: SessionMode,
    pub pid: Option<u32>,
    pub thread_id: Option<String>,
    pub status: CodexSessionStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub last_output_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRoundResult {
    pub agent: String,
    pub thread_id: String,
    pub final_message: String,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEventRecord {
    pub session_id: Option<String>,
    pub agent: String,
    pub stream: String,
    pub content: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeEventRecord {
    pub id: i64,
    pub scope: String,
    pub scope_id: String,
    pub agent_name: Option<String>,
    pub task_id: Option<String>,
    pub session_id: Option<String>,
    pub actor_name: Option<String>,
    pub event_type: String,
    pub summary: String,
    pub reason: Option<String>,
    pub payload_json: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeTraceView {
    pub query_kind: String,
    pub query_value: String,
    pub event_count: usize,
    pub events: Vec<RuntimeEventRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupSummary {
    pub requested_by: String,
    pub removed_agents: usize,
    pub removed_tasks: usize,
    pub removed_task_events: usize,
    pub removed_runtime_events: usize,
    pub removed_decisions: usize,
    pub removed_sessions: usize,
    pub removed_stream_events: usize,
    pub removed_agent_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveAgentSummary {
    pub agent: String,
    pub removed_tasks: usize,
    pub removed_task_events: usize,
    pub removed_runtime_events: usize,
    pub removed_decisions: usize,
    pub removed_sessions: usize,
    pub removed_stream_events: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairSummary {
    pub requested_by: String,
    pub repaired_agents: usize,
    pub repaired_tasks: usize,
    pub repaired_sessions: usize,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentContextSource {
    pub kind: AgentContextSourceKind,
    pub path: String,
    pub exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentContextView {
    pub agent: AgentSummary,
    pub current_task: Option<TaskSummary>,
    pub latest_decision: Option<DecisionSummary>,
    pub current_session: Option<SessionSummary>,
    #[serde(default)]
    pub context_sources: Vec<AgentContextSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskContextView {
    pub task: TaskSummary,
    pub agent: Option<AgentSummary>,
    pub latest_decision: Option<DecisionSummary>,
    pub current_session: Option<SessionSummary>,
    #[serde(default)]
    pub context_sources: Vec<AgentContextSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskRoundStatus {
    Result,
    Report,
    WaitDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRoundPayload {
    pub status: TaskRoundStatus,
    pub summary: String,
    pub blocking: Option<String>,
    pub topic: Option<String>,
    pub details: Option<String>,
    pub reason: Option<String>,
    pub next_suggestion: Option<String>,
    pub changed_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDraftPayload {
    pub title: String,
    pub summary: String,
    pub effort: Option<String>,
    #[serde(default)]
    pub read_scope: Vec<String>,
    #[serde(default)]
    pub write_scope: Vec<String>,
    #[serde(default)]
    pub acceptance: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BridgeDeliveryKind {
    TaskDispatch,
    TaskFeedback,
    TaskCancelled,
    TaskClosed,
    SyncRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeDelivery {
    pub delivery_id: u64,
    pub agent: String,
    pub kind: BridgeDeliveryKind,
    pub task: Option<TaskSummary>,
    pub decision: Option<DecisionSummary>,
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeSyncSnapshot {
    pub agent: AgentSummary,
    pub current_task: Option<TaskSummary>,
    pub latest_decision: Option<DecisionSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardSnapshot {
    pub agents: Vec<AgentSummary>,
    pub tasks: Vec<TaskSummary>,
    pub decisions: Vec<DecisionSummary>,
    pub sessions: Vec<SessionSummary>,
    pub recent_streams: Vec<StreamEventRecord>,
    pub recent_runtime_events: Vec<RuntimeEventRecord>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DashboardEvent {
    Snapshot {
        snapshot: DashboardSnapshot,
    },
    AgentStateChanged {
        agent: AgentSummary,
    },
    TaskEvent {
        task: TaskSummary,
        event_type: String,
    },
    DecisionEvent {
        decision: DecisionSummary,
        event_type: String,
    },
    SessionEvent {
        session: SessionSummary,
        event_type: String,
    },
    StreamChunk {
        event: StreamEventRecord,
    },
    RuntimeEvent {
        event: RuntimeEventRecord,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsClientMessage {
    Subscribe { channels: Vec<String> },
    RequestSnapshot,
    ControlRequest {
        request_id: String,
        request: crate::control::ControlRequest,
    },
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BridgeClientMessage {
    Hello {
        agent: String,
        mode: BridgeMode,
        last_ack_delivery_id: Option<u64>,
    },
    Heartbeat {
        session_id: String,
    },
    DeliveryAck {
        session_id: String,
        delivery_id: u64,
    },
    RequestSync {
        session_id: String,
    },
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BridgeServerMessage {
    Welcome {
        session_id: String,
        snapshot: BridgeSyncSnapshot,
        pending_deliveries: Vec<BridgeDelivery>,
    },
    Delivery {
        session_id: String,
        delivery: BridgeDelivery,
    },
    SyncSnapshot {
        session_id: String,
        reason: String,
        snapshot: BridgeSyncSnapshot,
        pending_deliveries: Vec<BridgeDelivery>,
    },
    Error {
        message: String,
    },
    Pong,
}
