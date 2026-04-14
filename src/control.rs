use serde::{Deserialize, Serialize};

use crate::models::{
    AgentContextView, AgentRoundResult, AgentSummary, CleanupSummary, DashboardSnapshot,
    DecisionSummary, RemoveAgentSummary, RepairSummary, RuntimeTraceView, TaskContextView,
    TaskRoundPayload, TaskSummary,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    Ping,
    Snapshot,
    Trace {
        agent: Option<String>,
        task_id: Option<String>,
        session_id: Option<String>,
        limit: usize,
    },
    RegisterAgent {
        name: String,
        role: String,
        repo_name: Option<String>,
        cwd: String,
        prompt_path: Option<String>,
    },
    RunAgentRound {
        agent: String,
        prompt: String,
    },
    RunTaskRound {
        task_id: String,
    },
    CleanupDemoData {
        requested_by: String,
    },
    RemoveAgent {
        agent: String,
    },
    RepairRuntimeState {
        requested_by: String,
    },
    TouchAgent {
        agent: String,
    },
    BeginAgentBootstrap {
        agent: String,
    },
    MarkAgentReady {
        agent: String,
        summary: Option<String>,
        thread_id: Option<String>,
    },
    SetAgentVisiblePane {
        agent: String,
        pid: Option<u32>,
        kind: Option<String>,
    },
    SetAgentAppServer {
        agent: String,
        app_server_url: Option<String>,
        owner: Option<String>,
    },
    AppendRuntimeEvent {
        scope: String,
        scope_id: String,
        agent: Option<String>,
        task_id: Option<String>,
        session_id: Option<String>,
        actor: Option<String>,
        event_type: String,
        summary: String,
        reason: Option<String>,
        payload_json: Option<String>,
    },
    EnsureManagedSession {
        agent: String,
        bootstrap_prompt: Option<String>,
    },
    AgentContext {
        agent: String,
    },
    TaskContext {
        task_id: String,
    },
    BeginVisibleTask {
        agent: String,
    },
    SubmitVisibleTaskRound {
        task_id: String,
        agent: String,
        payload: TaskRoundPayload,
    },
    CancelTask {
        task_id: String,
        requested_by: String,
    },
    RetryTask {
        task_id: String,
        requested_by: String,
    },
    ResetAgentThread {
        agent: String,
    },
    RecoverAgent {
        agent: String,
    },
    StopAgentSession {
        agent: String,
    },
    StopManagedSessions,
    StopVisiblePanes,
    CreateTask {
        from_agent: String,
        to_agent: String,
        title: String,
        summary: String,
        effort: Option<String>,
        read_scope: Vec<String>,
        write_scope: Vec<String>,
        acceptance: Vec<String>,
        auto_resolve_by: Option<String>,
        auto_resolve_summary: Option<String>,
    },
    CreateTaskFromPrompt {
        from_agent: String,
        to_agent: String,
        request: String,
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
    Trace {
        trace: RuntimeTraceView,
    },
    Cleanup {
        summary: CleanupSummary,
    },
    RemoveAgent {
        summary: RemoveAgentSummary,
    },
    Repair {
        summary: RepairSummary,
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
    AgentContext {
        context: AgentContextView,
    },
    TaskContext {
        context: TaskContextView,
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
    VisibleTaskRound {
        task: TaskSummary,
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
