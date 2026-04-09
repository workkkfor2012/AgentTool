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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSummary {
    pub name: String,
    pub role: AgentRole,
    pub repo_name: Option<String>,
    pub cwd: String,
    pub thread_id: Option<String>,
    pub current_session_id: Option<String>,
    pub state: AgentSessionState,
    pub current_task_id: Option<String>,
    pub last_output_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSummary {
    pub task_id: String,
    pub from_agent: String,
    pub to_agent: String,
    pub title: String,
    pub summary: String,
    pub auto_resolve_by: Option<String>,
    pub auto_resolve_summary: Option<String>,
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
pub struct CleanupSummary {
    pub requested_by: String,
    pub removed_agents: usize,
    pub removed_tasks: usize,
    pub removed_task_events: usize,
    pub removed_decisions: usize,
    pub removed_sessions: usize,
    pub removed_stream_events: usize,
    pub removed_agent_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairSummary {
    pub requested_by: String,
    pub repaired_agents: usize,
    pub repaired_tasks: usize,
    pub repaired_sessions: usize,
    pub notes: Vec<String>,
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
pub struct DashboardSnapshot {
    pub agents: Vec<AgentSummary>,
    pub tasks: Vec<TaskSummary>,
    pub decisions: Vec<DecisionSummary>,
    pub sessions: Vec<SessionSummary>,
    pub recent_streams: Vec<StreamEventRecord>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsClientMessage {
    Subscribe { channels: Vec<String> },
    RequestSnapshot,
    Ping,
}
