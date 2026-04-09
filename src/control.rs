use serde::{Deserialize, Serialize};

use crate::models::{
    AgentRoundResult, AgentSummary, DashboardSnapshot, DecisionSummary, TaskRoundPayload,
    TaskSummary,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    Ping,
    Snapshot,
    RegisterAgent {
        name: String,
        role: String,
        repo_name: Option<String>,
        cwd: String,
    },
    RunAgentRound {
        agent: String,
        prompt: String,
    },
    RunTaskRound {
        task_id: String,
    },
    RecoverAgent {
        agent: String,
    },
    StopAgentSession {
        agent: String,
    },
    CreateTask {
        from_agent: String,
        to_agent: String,
        title: String,
        summary: String,
        auto_resolve_by: Option<String>,
        auto_resolve_summary: Option<String>,
    },
    AcceptTask {
        task_id: String,
        agent: String,
    },
    StartTask {
        task_id: String,
        agent: String,
    },
    CompleteTask {
        task_id: String,
        agent: String,
    },
    ReportTask {
        task_id: String,
        agent: String,
        blocking: String,
        topic: String,
        details: String,
    },
    AnalyzeTask {
        task_id: String,
        analyzer: String,
    },
    ResolveTask {
        task_id: String,
        analyzer: String,
        summary: String,
    },
    SendDecision {
        task_id: String,
        issued_by: String,
        target_agent: String,
        summary: String,
        auto_close: bool,
    },
    AcknowledgeDecision {
        task_id: String,
        agent: String,
    },
    CloseTask {
        task_id: String,
        agent: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlResponse {
    Pong,
    Snapshot {
        snapshot: DashboardSnapshot,
    },
    Ack {
        message: String,
    },
    Task {
        task: TaskSummary,
    },
    Agent {
        agent: AgentSummary,
    },
    RoundResult {
        result: AgentRoundResult,
    },
    TaskRound {
        task: TaskSummary,
        result: AgentRoundResult,
        payload: TaskRoundPayload,
        decision: Option<DecisionSummary>,
    },
    DecisionTask {
        decision: DecisionSummary,
        task: TaskSummary,
    },
    Decision {
        decision: DecisionSummary,
    },
    Error {
        message: String,
    },
}
