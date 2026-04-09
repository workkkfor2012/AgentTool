use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use agenttool::config::AppConfig;
use agenttool::control::{ControlRequest, ControlResponse};

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
    RegisterAgent {
        #[arg(long)]
        name: String,
        #[arg(long)]
        role: String,
        #[arg(long)]
        cwd: String,
        #[arg(long)]
        repo_name: Option<String>,
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
    RecoverAgent {
        #[arg(long)]
        agent: String,
    },
    StopAgentSession {
        #[arg(long)]
        agent: String,
    },
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = AppConfig::load()?;

    let request = match cli.command {
        Command::Ping => ControlRequest::Ping,
        Command::Status => ControlRequest::Snapshot,
        Command::RegisterAgent {
            name,
            role,
            cwd,
            repo_name,
        } => ControlRequest::RegisterAgent {
            name,
            role,
            repo_name,
            cwd,
        },
        Command::RunAgentRound { agent, prompt } => ControlRequest::RunAgentRound { agent, prompt },
        Command::RunTaskRound { task } => ControlRequest::RunTaskRound { task_id: task },
        Command::CancelTask { task, requested_by } => ControlRequest::CancelTask {
            task_id: task,
            requested_by,
        },
        Command::RetryTask { task, requested_by } => ControlRequest::RetryTask {
            task_id: task,
            requested_by,
        },
        Command::RecoverAgent { agent } => ControlRequest::RecoverAgent { agent },
        Command::StopAgentSession { agent } => ControlRequest::StopAgentSession { agent },
        Command::CreateTask {
            from,
            to,
            title,
            summary,
            auto_resolve_by,
            auto_resolve_summary,
        } => ControlRequest::CreateTask {
            from_agent: from,
            to_agent: to,
            title,
            summary,
            auto_resolve_by,
            auto_resolve_summary,
        },
        Command::AcceptTask { task, agent } => ControlRequest::AcceptTask {
            task_id: task,
            agent,
        },
        Command::StartTask { task, agent } => ControlRequest::StartTask {
            task_id: task,
            agent,
        },
        Command::CompleteTask { task, agent } => ControlRequest::CompleteTask {
            task_id: task,
            agent,
        },
        Command::ReportTask {
            task,
            agent,
            blocking,
            topic,
            details,
        } => ControlRequest::ReportTask {
            task_id: task,
            agent,
            blocking,
            topic,
            details,
        },
        Command::AnalyzeTask { task, analyzer } => ControlRequest::AnalyzeTask {
            task_id: task,
            analyzer,
        },
        Command::ResolveTask {
            task,
            analyzer,
            summary,
        } => ControlRequest::ResolveTask {
            task_id: task,
            analyzer,
            summary,
        },
        Command::SendDecision {
            task,
            issued_by,
            target_agent,
            summary,
            close,
        } => ControlRequest::SendDecision {
            task_id: task,
            issued_by,
            target_agent,
            summary,
            auto_close: close,
        },
        Command::AckDecision { task, agent } => ControlRequest::AcknowledgeDecision {
            task_id: task,
            agent,
        },
        Command::CloseTask { task, agent } => ControlRequest::CloseTask {
            task_id: task,
            agent,
        },
    };

    let response = send_request(config.control_bind.to_string(), &request).await?;
    print_response(&response)?;
    Ok(())
}

async fn send_request(addr: String, request: &ControlRequest) -> Result<ControlResponse> {
    let mut stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("failed to connect to agentd at {addr}"))?;

    let payload = serde_json::to_string(request).context("failed to serialize request")?;
    stream.write_all(payload.as_bytes()).await?;
    stream.write_all(b"\n").await?;
    stream.flush().await?;

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response).await?;

    let response: ControlResponse =
        serde_json::from_str(response.trim()).context("failed to parse control response")?;
    Ok(response)
}

fn print_response(response: &ControlResponse) -> Result<()> {
    match response {
        ControlResponse::Error { message } => bail!(message.clone()),
        _ => print_json(response),
    }
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    let rendered = serde_json::to_string_pretty(value)?;
    println!("{rendered}");
    Ok(())
}
