use std::path::Path;

use std::fs;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

use agenttool::config::AppConfig;
use agenttool::control::{ControlRequest, ControlResponse};
use agenttool::models::{
    AgentBootstrapState, AgentRole, AgentSessionState, BridgeConnectionState, DashboardSnapshot,
    RuntimeTraceView, TaskRoundPayload, TaskRoundStatus, TaskState,
};

#[derive(Parser, Debug)]
#[command(name = "agentctl")]
#[command(about = "Local control client for AgentTool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping,
    Status,
    Overview,
    Trace {
        #[arg(long)]
        agent: Option<String>,
        #[arg(long = "task")]
        task_id: Option<String>,
        #[arg(long = "session")]
        session_id: Option<String>,
        #[arg(long, default_value_t = 80)]
        limit: usize,
    },
    MainInbox {
        #[arg(long, default_value = "main")]
        agent: String,
    },
    RegisterAgent {
        #[arg(long)]
        name: String,
        #[arg(long)]
        role: String,
        #[arg(long)]
        cwd: String,
        #[arg(long)]
        repo_name: Option<String>,
        #[arg(long)]
        prompt_path: Option<String>,
    },
    RunAgentRound {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        prompt: String,
    },
    RunTaskRound {
        #[arg(long)]
        task: String,
    },
    CleanupDemoData {
        #[arg(long)]
        requested_by: String,
    },
    RemoveAgent {
        #[arg(long)]
        agent: String,
    },
    RepairRuntimeState {
        #[arg(long)]
        requested_by: String,
    },
    TouchAgent {
        #[arg(long)]
        agent: String,
    },
    BeginAgentBootstrap {
        #[arg(long)]
        agent: String,
    },
    MarkAgentReady {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        summary: Option<String>,
        #[arg(long)]
        thread_id: Option<String>,
    },
    SetAgentVisiblePane {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        pid: Option<u32>,
        #[arg(long)]
        kind: Option<String>,
    },
    SetAgentAppServer {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        owner: Option<String>,
    },
    AppendRuntimeEvent {
        #[arg(long)]
        scope: String,
        #[arg(long = "scope-id")]
        scope_id: String,
        #[arg(long)]
        agent: Option<String>,
        #[arg(long = "task")]
        task_id: Option<String>,
        #[arg(long = "session")]
        session_id: Option<String>,
        #[arg(long)]
        actor: Option<String>,
        #[arg(long = "event-type")]
        event_type: String,
        #[arg(long)]
        summary: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long = "payload-json")]
        payload_json: Option<String>,
        #[arg(long = "payload-file")]
        payload_file: Option<String>,
    },
    EnsureManagedSession {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        bootstrap_prompt: Option<String>,
    },
    AgentContext {
        #[arg(long)]
        agent: String,
    },
    TaskContext {
        #[arg(long)]
        task: String,
    },
    BeginVisibleTask {
        #[arg(long)]
        agent: String,
    },
    SubmitVisibleTaskRound {
        #[arg(long)]
        task: String,
        #[arg(long)]
        agent: String,
        #[arg(long)]
        status: String,
        #[arg(long)]
        summary: String,
        #[arg(long)]
        blocking: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        details: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        next_suggestion: Option<String>,
        #[arg(long = "changed-file")]
        changed_files: Vec<String>,
    },
    CancelTask {
        #[arg(long)]
        task: String,
        #[arg(long)]
        requested_by: String,
    },
    RetryTask {
        #[arg(long)]
        task: String,
        #[arg(long)]
        requested_by: String,
    },
    ResetAgentThread {
        #[arg(long)]
        agent: String,
    },
    RecoverAgent {
        #[arg(long)]
        agent: String,
    },
    StopAgentSession {
        #[arg(long)]
        agent: String,
    },
    StopManagedSessions,
    StopVisiblePanes,
    CreateTask {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        summary: String,
        #[arg(long)]
        effort: Option<String>,
        #[arg(long = "read-scope")]
        read_scope: Vec<String>,
        #[arg(long = "write-scope")]
        write_scope: Vec<String>,
        #[arg(long)]
        acceptance: Vec<String>,
        #[arg(long)]
        auto_resolve_by: Option<String>,
        #[arg(long)]
        auto_resolve_summary: Option<String>,
    },
    AcceptTask {
        #[arg(long)]
        task: String,
        #[arg(long)]
        agent: String,
    },
    StartTask {
        #[arg(long)]
        task: String,
        #[arg(long)]
        agent: String,
    },
    CompleteTask {
        #[arg(long)]
        task: String,
        #[arg(long)]
        agent: String,
    },
    ReportTask {
        #[arg(long)]
        task: String,
        #[arg(long)]
        agent: String,
        #[arg(long)]
        blocking: String,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        details: String,
    },
    AnalyzeTask {
        #[arg(long)]
        task: String,
        #[arg(long)]
        analyzer: String,
    },
    ResolveTask {
        #[arg(long)]
        task: String,
        #[arg(long)]
        analyzer: String,
        #[arg(long)]
        summary: String,
    },
    SendDecision {
        #[arg(long)]
        task: String,
        #[arg(long)]
        issued_by: String,
        #[arg(long)]
        target_agent: String,
        #[arg(long)]
        summary: String,
        #[arg(long, default_value_t = false)]
        close: bool,
    },
    AckDecision {
        #[arg(long)]
        task: String,
        #[arg(long)]
        agent: String,
    },
    CloseTask {
        #[arg(long)]
        task: String,
        #[arg(long)]
        agent: String,
    },
}

#[derive(Debug, Clone, Copy)]
enum ResponseFormat {
    Default,
    Overview,
    MainInbox,
    Trace,
}

#[derive(Debug, Serialize)]
struct MainInboxResponse {
    agent: String,
    task_count: usize,
    tasks: Vec<MainInboxTask>,
}

#[derive(Debug, Serialize)]
struct MainInboxTask {
    task_id: String,
    to_agent: String,
    child_state: Option<String>,
    title: String,
    state: TaskState,
    updated_at: DateTime<Utc>,
    latest_child_status: Option<String>,
    latest_child_summary: Option<String>,
    latest_child_blocking: Option<String>,
    latest_child_topic: Option<String>,
    latest_decision_summary: Option<String>,
    latest_decision_status: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = AppConfig::load_client()?;

    let (request, response_format, inbox_agent) = match cli.command {
        Command::Ping => (ControlRequest::Ping, ResponseFormat::Default, None),
        Command::Status => (ControlRequest::Snapshot, ResponseFormat::Default, None),
        Command::Overview => (ControlRequest::Snapshot, ResponseFormat::Overview, None),
        Command::Trace {
            agent,
            task_id,
            session_id,
            limit,
        } => {
            let selected = usize::from(agent.is_some())
                + usize::from(task_id.is_some())
                + usize::from(session_id.is_some());
            if selected != 1 {
                bail!("trace requires exactly one of --agent, --task, or --session");
            }

            (
                ControlRequest::Trace {
                    agent,
                    task_id,
                    session_id,
                    limit,
                },
                ResponseFormat::Trace,
                None,
            )
        }
        Command::MainInbox { agent } => (
            ControlRequest::Snapshot,
            ResponseFormat::MainInbox,
            Some(agent),
        ),
        Command::RegisterAgent {
            name,
            role,
            cwd,
            repo_name,
            prompt_path,
        } => (
            ControlRequest::RegisterAgent {
                name,
                role,
                repo_name,
                cwd,
                prompt_path,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::RunAgentRound { agent, prompt } => (
            ControlRequest::RunAgentRound { agent, prompt },
            ResponseFormat::Default,
            None,
        ),
        Command::RunTaskRound { task } => (
            ControlRequest::RunTaskRound { task_id: task },
            ResponseFormat::Default,
            None,
        ),
        Command::CleanupDemoData { requested_by } => (
            ControlRequest::CleanupDemoData { requested_by },
            ResponseFormat::Default,
            None,
        ),
        Command::RemoveAgent { agent } => (
            ControlRequest::RemoveAgent { agent },
            ResponseFormat::Default,
            None,
        ),
        Command::RepairRuntimeState { requested_by } => (
            ControlRequest::RepairRuntimeState { requested_by },
            ResponseFormat::Default,
            None,
        ),
        Command::TouchAgent { agent } => (
            ControlRequest::TouchAgent { agent },
            ResponseFormat::Default,
            None,
        ),
        Command::BeginAgentBootstrap { agent } => (
            ControlRequest::BeginAgentBootstrap { agent },
            ResponseFormat::Default,
            None,
        ),
        Command::MarkAgentReady {
            agent,
            summary,
            thread_id,
        } => (
            ControlRequest::MarkAgentReady {
                agent,
                summary,
                thread_id,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::SetAgentAppServer { agent, url, owner } => (
            ControlRequest::SetAgentAppServer {
                agent,
                app_server_url: url,
                owner,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::AppendRuntimeEvent {
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
            payload_file,
        } => (
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
                payload_json: resolve_append_runtime_payload(payload_json, payload_file)?,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::SetAgentVisiblePane { agent, pid, kind } => (
            ControlRequest::SetAgentVisiblePane { agent, pid, kind },
            ResponseFormat::Default,
            None,
        ),
        Command::EnsureManagedSession {
            agent,
            bootstrap_prompt,
        } => (
            ControlRequest::EnsureManagedSession {
                agent,
                bootstrap_prompt,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::AgentContext { agent } => (
            ControlRequest::AgentContext { agent },
            ResponseFormat::Default,
            None,
        ),
        Command::TaskContext { task } => (
            ControlRequest::TaskContext { task_id: task },
            ResponseFormat::Default,
            None,
        ),
        Command::BeginVisibleTask { agent } => (
            ControlRequest::BeginVisibleTask { agent },
            ResponseFormat::Default,
            None,
        ),
        Command::SubmitVisibleTaskRound {
            task,
            agent,
            status,
            summary,
            blocking,
            topic,
            details,
            reason,
            next_suggestion,
            changed_files,
        } => (
            ControlRequest::SubmitVisibleTaskRound {
                task_id: task,
                agent,
                payload: TaskRoundPayload {
                    status: parse_task_round_status(&status)?,
                    summary,
                    blocking,
                    topic,
                    details,
                    reason,
                    next_suggestion,
                    changed_files,
                },
            },
            ResponseFormat::Default,
            None,
        ),
        Command::CancelTask { task, requested_by } => (
            ControlRequest::CancelTask {
                task_id: task,
                requested_by,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::RetryTask { task, requested_by } => (
            ControlRequest::RetryTask {
                task_id: task,
                requested_by,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::ResetAgentThread { agent } => (
            ControlRequest::ResetAgentThread { agent },
            ResponseFormat::Default,
            None,
        ),
        Command::RecoverAgent { agent } => (
            ControlRequest::RecoverAgent { agent },
            ResponseFormat::Default,
            None,
        ),
        Command::StopAgentSession { agent } => (
            ControlRequest::StopAgentSession { agent },
            ResponseFormat::Default,
            None,
        ),
        Command::StopManagedSessions => (
            ControlRequest::StopManagedSessions,
            ResponseFormat::Default,
            None,
        ),
        Command::StopVisiblePanes => (
            ControlRequest::StopVisiblePanes,
            ResponseFormat::Default,
            None,
        ),
        Command::CreateTask {
            from,
            to,
            title,
            summary,
            effort,
            read_scope,
            write_scope,
            acceptance,
            auto_resolve_by,
            auto_resolve_summary,
        } => (
            ControlRequest::CreateTask {
                from_agent: from,
                to_agent: to,
                title,
                summary,
                effort,
                read_scope,
                write_scope,
                acceptance,
                auto_resolve_by,
                auto_resolve_summary,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::AcceptTask { task, agent } => (
            ControlRequest::AcceptTask {
                task_id: task,
                agent,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::StartTask { task, agent } => (
            ControlRequest::StartTask {
                task_id: task,
                agent,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::CompleteTask { task, agent } => (
            ControlRequest::CompleteTask {
                task_id: task,
                agent,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::ReportTask {
            task,
            agent,
            blocking,
            topic,
            details,
        } => (
            ControlRequest::ReportTask {
                task_id: task,
                agent,
                blocking,
                topic,
                details,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::AnalyzeTask { task, analyzer } => (
            ControlRequest::AnalyzeTask {
                task_id: task,
                analyzer,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::ResolveTask {
            task,
            analyzer,
            summary,
        } => (
            ControlRequest::ResolveTask {
                task_id: task,
                analyzer,
                summary,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::SendDecision {
            task,
            issued_by,
            target_agent,
            summary,
            close,
        } => (
            ControlRequest::SendDecision {
                task_id: task,
                issued_by,
                target_agent,
                summary,
                auto_close: close,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::AckDecision { task, agent } => (
            ControlRequest::AcknowledgeDecision {
                task_id: task,
                agent,
            },
            ResponseFormat::Default,
            None,
        ),
        Command::CloseTask { task, agent } => (
            ControlRequest::CloseTask {
                task_id: task,
                agent,
            },
            ResponseFormat::Default,
            None,
        ),
    };

    let response_timeout = control_request_timeout(&request, &config);
    let response = send_request(config.control_bind.to_string(), &request, response_timeout).await?;
    match response_format {
        ResponseFormat::Default => print_response(&response)?,
        ResponseFormat::Overview => print_overview(&response)?,
        ResponseFormat::MainInbox => {
            let agent = inbox_agent.context("main inbox agent was not set")?;
            print_main_inbox(&agent, &response)?;
        }
        ResponseFormat::Trace => print_trace(&response)?,
    }
    Ok(())
}

fn control_request_timeout(request: &ControlRequest, config: &AppConfig) -> Duration {
    match request {
        ControlRequest::EnsureManagedSession { .. } => {
            Duration::from_secs(config.managed_bootstrap_timeout_seconds.saturating_add(30))
        }
        ControlRequest::RunAgentRound { .. } | ControlRequest::RunTaskRound { .. } => {
            Duration::from_secs(90)
        }
        ControlRequest::StopManagedSessions
        | ControlRequest::StopVisiblePanes
        | ControlRequest::CleanupDemoData { .. }
        | ControlRequest::RepairRuntimeState { .. } => Duration::from_secs(20),
        _ => Duration::from_secs(4),
    }
}

async fn send_request(
    addr: String,
    request: &ControlRequest,
    timeout_window: Duration,
) -> Result<ControlResponse> {
    let connect_result = timeout(timeout_window, TcpStream::connect(&addr))
        .await
        .with_context(|| format!("timed out connecting to agentd at {addr}"))?;
    let mut stream = connect_result
        .with_context(|| format!("failed to connect to agentd at {addr}"))?;

    let payload = serde_json::to_string(request).context("failed to serialize request")?;
    stream.write_all(payload.as_bytes()).await?;
    stream.write_all(b"\n").await?;
    stream.flush().await?;

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    let read_result = timeout(timeout_window, reader.read_line(&mut response))
        .await
        .with_context(|| format!("timed out waiting for control response from {addr}"))?;
    let bytes_read = read_result?;
    if bytes_read == 0 {
        bail!(
            "control connection closed without a response; the online agentd may be outdated and not support this request"
        );
    }

    let trimmed = response.trim();
    let response: ControlResponse = serde_json::from_str(trimmed).with_context(|| {
        format!("failed to parse control response: {trimmed}")
    })?;
    Ok(response)
}

fn print_response(response: &ControlResponse) -> Result<()> {
    match response {
        ControlResponse::Error { message } => bail!(message.clone()),
        _ => print_json(response),
    }
}

fn print_main_inbox(agent: &str, response: &ControlResponse) -> Result<()> {
    let snapshot = match response {
        ControlResponse::Snapshot { snapshot } => snapshot,
        ControlResponse::Error { message } => bail!(message.clone()),
        other => bail!(
            "main-inbox expected a snapshot response, received {}",
            serde_json::to_string(other)?
        ),
    };
    let inbox = build_main_inbox(agent, snapshot);
    print_json(&inbox)
}

fn print_overview(response: &ControlResponse) -> Result<()> {
    let snapshot = match response {
        ControlResponse::Snapshot { snapshot } => snapshot,
        ControlResponse::Error { message } => bail!(message.clone()),
        other => bail!(
            "overview expected a snapshot response, received {}",
            serde_json::to_string(other)?
        ),
    };

    render_overview(snapshot);
    Ok(())
}

fn print_trace(response: &ControlResponse) -> Result<()> {
    let trace = match response {
        ControlResponse::Trace { trace } => trace,
        ControlResponse::Error { message } => bail!(message.clone()),
        other => bail!(
            "trace expected a trace response, received {}",
            serde_json::to_string(other)?
        ),
    };

    render_trace(trace);
    Ok(())
}

fn build_main_inbox(agent: &str, snapshot: &DashboardSnapshot) -> MainInboxResponse {
    let mut tasks = snapshot
        .tasks
        .iter()
        .filter(|task| task.from_agent == agent)
        .filter(|task| {
            matches!(
                task.state,
                TaskState::Reported | TaskState::BlockedWaitingDecision | TaskState::Analyzed
            )
        })
        .map(|task| {
            let child_state = snapshot
                .agents
                .iter()
                .find(|candidate| candidate.name == task.to_agent)
                .map(|candidate| format!("{:?}", candidate.state).to_lowercase());

            MainInboxTask {
                task_id: task.task_id.clone(),
                to_agent: task.to_agent.clone(),
                child_state,
                title: task.title.clone(),
                state: task.state.clone(),
                updated_at: task.updated_at,
                latest_child_status: task.latest_child_status.clone(),
                latest_child_summary: task.latest_child_summary.clone(),
                latest_child_blocking: task.latest_child_blocking.clone(),
                latest_child_topic: task.latest_child_topic.clone(),
                latest_decision_summary: task.latest_decision_summary.clone(),
                latest_decision_status: task.latest_decision_status.clone(),
            }
        })
        .collect::<Vec<_>>();

    tasks.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));

    MainInboxResponse {
        agent: agent.to_string(),
        task_count: tasks.len(),
        tasks,
    }
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    let rendered = serde_json::to_string_pretty(value)?;
    println!("{rendered}");
    Ok(())
}

fn resolve_append_runtime_payload(
    payload_json: Option<String>,
    payload_file: Option<String>,
) -> Result<Option<String>> {
    if payload_json.is_some() && payload_file.is_some() {
        bail!("append-runtime-event accepts only one of --payload-json or --payload-file");
    }

    if let Some(payload_json) = payload_json {
        return Ok(Some(payload_json));
    }

    let Some(payload_file) = payload_file else {
        return Ok(None);
    };

    let content = fs::read_to_string(&payload_file)
        .with_context(|| format!("failed to read payload file: {payload_file}"))?;
    Ok(Some(content))
}

fn render_overview(snapshot: &DashboardSnapshot) {
    let mut agents = snapshot.agents.clone();
    agents.sort_by_key(|agent| (role_rank(&agent.role), agent.name.clone()));

    let ready_count = agents
        .iter()
        .filter(|agent| agent.bootstrap_state == AgentBootstrapState::Ready)
        .count();
    let connected_count = agents
        .iter()
        .filter(|agent| agent.bridge_state == BridgeConnectionState::Connected)
        .count();
    let open_tasks = snapshot
        .tasks
        .iter()
        .filter(|task| {
            !matches!(
                task.state,
                TaskState::Closed | TaskState::Cancelled | TaskState::Failed
            )
        })
        .collect::<Vec<_>>();

    println!(
        "Overview at {}",
        snapshot.generated_at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S")
    );
    println!(
        "Agents total={} ready={} bridge_connected={} open_tasks={}",
        agents.len(),
        ready_count,
        connected_count,
        open_tasks.len()
    );
    println!();
    println!(
        "{:<24} {:<6} {:<10} {:<12} {:<18} {:<26} {}",
        "agent", "role", "runtime", "bootstrap", "transport", "task", "context"
    );
    println!("{}", "-".repeat(116));
    for agent in &agents {
        let task = format!(
            "{} / {}",
            agent.current_task_id.as_deref().unwrap_or("-"),
            agent.current_session_id.as_deref().unwrap_or("-")
        );
        println!(
            "{:<24} {:<6} {:<10} {:<12} {:<18} {:<26} {}",
            agent.name,
            enum_label(&agent.role),
            format!("{:?}", agent.state).to_lowercase(),
            enum_label(&agent.bootstrap_state),
            overview_transport(agent, snapshot),
            task,
            overview_context_badge(agent)
        );
    }

    let mut issues = Vec::new();
    for agent in &agents {
        if agent.bootstrap_state != AgentBootstrapState::Ready {
            issues.push(format!(
                "{} bootstrap={}",
                agent.name,
                enum_label(&agent.bootstrap_state)
            ));
        }
        let transport = overview_transport(agent, snapshot);
        if transport == "none" {
            issues.push(format!(
                "{} missing_runtime_link",
                agent.name
            ));
        }
        if agent.bridge_pending_delivery_count > 0 {
            issues.push(format!(
                "{} bridge_pending={}",
                agent.name, agent.bridge_pending_delivery_count
            ));
        }
        if agent.current_task_id.is_none() && agent.state != AgentSessionState::Idle {
            issues.push(format!(
                "{} state={} without current_task",
                agent.name,
                enum_label(&agent.state)
            ));
        }
        if agent.current_task_id.is_some() && agent.state == AgentSessionState::Idle {
            issues.push(format!("{} idle_with_current_task", agent.name));
        }
        if prompt_contract_missing(agent) {
            issues.push(format!(
                "{} prompt_contract_missing={}",
                agent.name,
                agent.prompt_path.as_deref().unwrap_or("-")
            ));
        }
        if let Some(session_id) = agent.current_session_id.as_deref() {
            if current_session_for_agent(snapshot, agent).is_none() {
                issues.push(format!(
                    "{} current_session_missing={}",
                    agent.name, session_id
                ));
            }
        }
    }

    println!();
    println!("Issues {}", issues.len());
    if issues.is_empty() {
        println!("  none");
    } else {
        for issue in issues {
            println!("  - {issue}");
        }
    }

    if !open_tasks.is_empty() {
        println!();
        println!(
            "{:<26} {:<20} {:<20} {:<24} {}",
            "task", "from", "to", "state", "title"
        );
        println!("{}", "-".repeat(116));
        for task in open_tasks {
            println!(
                "{:<26} {:<20} {:<20} {:<24} {}",
                task.task_id,
                task.from_agent,
                task.to_agent,
                format!("{:?}", task.state).to_lowercase(),
                task.title
            );
        }
    }
}

fn render_trace(trace: &RuntimeTraceView) {
    println!(
        "Trace {}={} events={}",
        trace.query_kind, trace.query_value, trace.event_count
    );
    println!();

    for event in &trace.events {
        let scope = format!("{}/{}", event.scope, event.scope_id);
        let mut meta = Vec::new();
        if let Some(actor) = &event.actor_name {
            meta.push(format!("actor={actor}"));
        }
        if let Some(agent) = &event.agent_name {
            meta.push(format!("agent={agent}"));
        }
        if let Some(task_id) = &event.task_id {
            meta.push(format!("task={task_id}"));
        }
        if let Some(session_id) = &event.session_id {
            meta.push(format!("session={session_id}"));
        }
        if let Some(reason) = &event.reason {
            meta.push(format!("reason={reason}"));
        }

        println!(
            "[{}] {} {} {}",
            event.created_at.to_rfc3339(),
            event.event_type,
            scope,
            event.summary
        );
        if !meta.is_empty() {
            println!("  {}", meta.join(" | "));
        }
        if let Some(payload) = &event.payload_json {
            let payload = payload.trim();
            if !payload.is_empty() && payload != "{}" {
                println!("  payload={payload}");
            }
        }
        println!();
    }
}

fn parse_task_round_status(value: &str) -> Result<agenttool::models::TaskRoundStatus> {
    match value {
        "result" => Ok(TaskRoundStatus::Result),
        "report" => Ok(TaskRoundStatus::Report),
        "wait_decision" => Ok(TaskRoundStatus::WaitDecision),
        other => bail!("unsupported task round status: {other}"),
    }
}

fn role_rank(role: &AgentRole) -> u8 {
    match role {
        AgentRole::Main => 0,
        AgentRole::Child => 1,
    }
}

fn current_session_for_agent<'a>(
    snapshot: &'a DashboardSnapshot,
    agent: &agenttool::models::AgentSummary,
) -> Option<&'a agenttool::models::SessionSummary> {
    agent.current_session_id
        .as_deref()
        .and_then(|session_id| snapshot.sessions.iter().find(|session| session.session_id == session_id))
}

fn overview_transport(
    agent: &agenttool::models::AgentSummary,
    snapshot: &DashboardSnapshot,
) -> String {
    if let Some(session) = current_session_for_agent(snapshot, agent) {
        if matches!(session.status, agenttool::models::CodexSessionStatus::Running) {
            return format!("managed/{}", enum_label(&session.session_mode));
        }
    }

    if agent.bridge_state == BridgeConnectionState::Connected {
        return match &agent.bridge_mode {
            Some(mode) => format!("bridge/{}", enum_label(mode)),
            None => "bridge".to_string(),
        };
    }

    if agent.visible_pane_pid.is_some() {
        return "visible_runtime".to_string();
    }

    "none".to_string()
}

fn overview_context_badge(agent: &agenttool::models::AgentSummary) -> String {
    let mut badges = Vec::new();

    match &agent.prompt_path {
        Some(path) if Path::new(path).is_file() => badges.push("prompt"),
        Some(_) => badges.push("prompt!"),
        None => {}
    }

    if Path::new(&agent.cwd).join("work.md").is_file() {
        badges.push("work");
    }

    if badges.is_empty() {
        "none".to_string()
    } else {
        badges.join("+")
    }
}

fn prompt_contract_missing(agent: &agenttool::models::AgentSummary) -> bool {
    agent.prompt_path
        .as_deref()
        .map(|path| !Path::new(path).is_file())
        .unwrap_or(false)
}

fn enum_label<T: std::fmt::Debug>(value: &T) -> String {
    format!("{:?}", value).to_lowercase()
}
