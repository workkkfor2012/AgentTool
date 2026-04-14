use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use agenttool::config::AppConfig;
use agenttool::models::{
    AgentBootstrapState, AgentSessionState, AppServerOwner, CodexSessionStatus, DashboardEvent,
    DashboardSnapshot, SessionMode, TaskState,
};

#[derive(Parser, Debug)]
#[command(name = "agentwatch")]
#[command(about = "Read-only live viewer for one AgentTool agent")]
struct Cli {
    #[arg(long)]
    agent: String,

    #[arg(long)]
    title: Option<String>,

    #[arg(long, default_value_t = 12)]
    recent_event_limit: usize,

    #[arg(long, default_value_t = 3)]
    reconnect_seconds: u64,

    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    show_streams: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    loop {
        let config = match AppConfig::load_client() {
            Ok(config) => config,
            Err(err) => {
                log_line("endpoint", &format!("runtime endpoint unavailable: {err}"));
                tokio::time::sleep(Duration::from_secs(cli.reconnect_seconds.max(1))).await;
                continue;
            }
        };
        let ws_url = format!("ws://{}/ws", config.ws_bind);

        print_banner(&cli, &ws_url);

        match run_watch_session(&cli, &ws_url).await {
            Ok(()) => {
                log_line("连接", "会话已结束，准备重连。");
            }
            Err(err) => {
                log_line("错误", &format!("连接失败: {err}"));
            }
        }

        tokio::time::sleep(Duration::from_secs(cli.reconnect_seconds.max(1))).await;
    }
}

async fn run_watch_session(cli: &Cli, ws_url: &str) -> Result<()> {
    let (mut socket, _) = connect_async(ws_url)
        .await
        .with_context(|| format!("failed to connect websocket {ws_url}"))?;
    log_line("连接", &format!("已连接到 {ws_url}"));

    while let Some(message) = socket.next().await {
        match message.context("websocket read failed")? {
            Message::Text(text) => {
                if text == "{\"type\":\"pong\"}" {
                    continue;
                }

                let event: DashboardEvent =
                    serde_json::from_str(&text).context("failed to parse dashboard event json")?;
                handle_event(cli, event);
            }
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .context("failed to send websocket pong")?;
            }
            Message::Close(_) => {
                log_line("连接", "服务端关闭了 websocket。");
                return Ok(());
            }
            _ => {}
        }
    }

    Ok(())
}

fn handle_event(cli: &Cli, event: DashboardEvent) {
    match event {
        DashboardEvent::Snapshot { snapshot } => print_snapshot(cli, &snapshot),
        DashboardEvent::AgentStateChanged { agent } => {
            if agent.name == cli.agent {
                log_line(
                    "状态",
                    &format!(
                        "{} | 初始化={} | 会话状态={} | app-server={} | 任务={}",
                        label_agent_state(&agent.state),
                        label_bootstrap_state(&agent.bootstrap_state),
                        match &agent.current_session_id {
                            Some(session) => format!("在线({session})"),
                            None => "无在线轮次会话".to_string(),
                        },
                        match (&agent.app_server_owner, &agent.app_server_url) {
                            (Some(owner), Some(url)) => {
                                format!("{} {}", label_app_server_owner(owner), shorten(url, 52))
                            }
                            _ => "未绑定".to_string(),
                        },
                        agent.current_task_id.as_deref().unwrap_or("无"),
                    ),
                );
            }
        }
        DashboardEvent::TaskEvent { task, event_type } => {
            if task.to_agent == cli.agent || task.from_agent == cli.agent {
                log_line(
                    "任务",
                    &format!(
                        "{} | {} | {} | {}",
                        event_type,
                        task.task_id,
                        label_task_state(&task.state),
                        shorten(&task.title, 80)
                    ),
                );
                if let Some(summary) = task.latest_child_summary.as_deref() {
                    log_line("任务", &format!("子反馈: {}", shorten(summary, 120)));
                }
                if let Some(summary) = task.latest_decision_summary.as_deref() {
                    log_line("任务", &format!("主决策: {}", shorten(summary, 120)));
                }
            }
        }
        DashboardEvent::DecisionEvent { decision, event_type } => {
            if decision.target_agent == cli.agent || decision.issued_by == cli.agent {
                log_line(
                    "决策",
                    &format!(
                        "{} | {} | {}",
                        event_type,
                        decision.task_id,
                        shorten(&decision.summary, 120)
                    ),
                );
            }
        }
        DashboardEvent::SessionEvent {
            session,
            event_type,
        } => {
            if session.agent_name == cli.agent {
                log_line(
                    "会话",
                    &format!(
                        "{} | {} | {} | {}",
                        event_type,
                        session.session_id,
                        label_session_mode(&session.session_mode),
                        label_session_status(&session.status)
                    ),
                );
            }
        }
        DashboardEvent::RuntimeEvent { event } => {
            let relevant_agent = event.agent_name.as_deref() == Some(cli.agent.as_str())
                || event.actor_name.as_deref() == Some(cli.agent.as_str());
            if relevant_agent {
                log_line(
                    "运行",
                    &format!(
                        "{} | {}",
                        event.event_type,
                        shorten(&event.summary, 140)
                    ),
                );
            }
        }
        DashboardEvent::StreamChunk { event } => {
            if cli.show_streams && event.agent == cli.agent && !looks_like_json(&event.content) {
                log_line(
                    "输出",
                    &format!("[{}] {}", event.stream, shorten(&event.content, 160)),
                );
            }
        }
    }
}

fn print_snapshot(cli: &Cli, snapshot: &DashboardSnapshot) {
    let Some(agent) = snapshot.agents.iter().find(|agent| agent.name == cli.agent) else {
        log_line("快照", "agent 尚未注册。");
        return;
    };

    log_line(
        "快照",
        &format!(
            "初始化={} | 运行状态={} | app-server={} | 当前任务={}",
            label_bootstrap_state(&agent.bootstrap_state),
            label_agent_state(&agent.state),
            match (&agent.app_server_owner, &agent.app_server_url) {
                (Some(owner), Some(url)) => {
                    format!("{} {}", label_app_server_owner(owner), shorten(url, 52))
                }
                _ => "未绑定".to_string(),
            },
            agent.current_task_id.as_deref().unwrap_or("无"),
        ),
    );

    let recent_events = snapshot
        .recent_runtime_events
        .iter()
        .filter(|event| {
            event.agent_name.as_deref() == Some(cli.agent.as_str())
                || event.actor_name.as_deref() == Some(cli.agent.as_str())
        })
        .rev()
        .take(cli.recent_event_limit)
        .collect::<Vec<_>>();

    if !recent_events.is_empty() {
        log_line("快照", "最近事件:");
        for event in recent_events.into_iter().rev() {
            log_line(
                "事件",
                &format!("{} | {}", event.event_type, shorten(&event.summary, 140)),
            );
        }
    }
}

fn print_banner(cli: &Cli, ws_url: &str) {
    println!("========================================================================");
    println!("AgentWatch : {}", cli.agent);
    println!(
        "Title      : {}",
        cli.title.clone().unwrap_or_else(|| cli.agent.clone())
    );
    println!("WebSocket  : {}", ws_url);
    println!("Mode       : read-only viewer");
    println!("========================================================================");
}

fn log_line(tag: &str, message: &str) {
    println!(
        "[{}][{}] {}",
        Local::now().format("%H:%M:%S"),
        tag,
        message
    );
}

fn shorten(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let shortened = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{shortened}...")
    } else {
        shortened
    }
}

fn looks_like_json(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.starts_with('{') || trimmed.starts_with('[')
}

fn label_agent_state(state: &AgentSessionState) -> &'static str {
    match state {
        AgentSessionState::Idle => "空闲",
        AgentSessionState::Busy => "运行中",
        AgentSessionState::Blocked => "阻塞",
        AgentSessionState::Offline => "离线",
    }
}

fn label_bootstrap_state(state: &AgentBootstrapState) -> &'static str {
    match state {
        AgentBootstrapState::AwaitingInit => "待初始化",
        AgentBootstrapState::Ready => "已就绪",
    }
}

fn label_task_state(state: &TaskState) -> &'static str {
    match state {
        TaskState::Pending => "待处理",
        TaskState::Accepted => "已接单",
        TaskState::Running => "运行中",
        TaskState::Completed => "已完成",
        TaskState::Reported => "已反馈",
        TaskState::Analyzed => "已分析",
        TaskState::DecisionSent => "已发决策",
        TaskState::Closed => "已关闭",
        TaskState::BlockedWaitingDecision => "等主决策",
        TaskState::Cancelled => "已取消",
        TaskState::Failed => "失败",
    }
}

fn label_session_mode(mode: &SessionMode) -> &'static str {
    match mode {
        SessionMode::Round => "轮次",
        SessionMode::Pty => "终端",
        SessionMode::AppServer => "App Server",
    }
}

fn label_session_status(status: &CodexSessionStatus) -> &'static str {
    match status {
        CodexSessionStatus::Running => "运行中",
        CodexSessionStatus::Succeeded => "成功",
        CodexSessionStatus::Failed => "失败",
    }
}

fn label_app_server_owner(owner: &AppServerOwner) -> &'static str {
    match owner {
        AppServerOwner::Bridge => "桥接",
        AppServerOwner::Daemon => "中心托管",
    }
}
