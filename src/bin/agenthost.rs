use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use agenttool::config::AppConfig;
use agenttool::control::{ControlRequest, ControlResponse};
use agenttool::models::{
    AgentSummary, BridgeClientMessage, BridgeDelivery, BridgeDeliveryKind, BridgeMode,
    BridgeServerMessage, BridgeSyncSnapshot, TaskState, TaskSummary,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum BridgeModeArg {
    Passive,
    Autorun,
}

impl From<BridgeModeArg> for BridgeMode {
    fn from(value: BridgeModeArg) -> Self {
        match value {
            BridgeModeArg::Passive => BridgeMode::Passive,
            BridgeModeArg::Autorun => BridgeMode::Autorun,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "agenthost")]
#[command(about = "Persistent bridge client for one registered agent")]
struct Cli {
    #[arg(long)]
    agent: String,

    #[arg(long, value_enum, default_value_t = BridgeModeArg::Passive)]
    bridge_mode: BridgeModeArg,

    #[arg(long, default_value_t = 3)]
    heartbeat_seconds: u64,

    #[arg(long, default_value_t = 3)]
    reconnect_seconds: u64,

    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    quiet: bool,
}

#[derive(Debug, Clone, Default)]
struct LocalState {
    agent: Option<AgentSummary>,
    current_task: Option<TaskSummary>,
    session_id: Option<String>,
    last_ack_delivery_id: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let heartbeat_interval = Duration::from_secs(cli.heartbeat_seconds.max(1));
    let reconnect_delay = Duration::from_secs(cli.reconnect_seconds.max(1));
    let bridge_mode: BridgeMode = cli.bridge_mode.into();
    let mut local_state = LocalState::default();

    loop {
        let config = match AppConfig::load_client() {
            Ok(config) => config,
            Err(err) => {
                if !cli.quiet {
                    eprintln!("[agenthost] runtime endpoint unavailable: {err:#}");
                }
                tokio::time::sleep(reconnect_delay).await;
                continue;
            }
        };
        let bridge_url = format!("ws://{}/bridge", config.ws_bind);

        if !cli.quiet {
            println!("========================================================================");
            println!("AgentHost : {}", cli.agent);
            println!("Bridge    : {}", bridge_url);
            println!("Mode      : {:?}", cli.bridge_mode);
            println!("Heartbeat : {}s", heartbeat_interval.as_secs());
            println!("Reconnect : {}s", reconnect_delay.as_secs());
            println!("========================================================================");
        }

        match run_bridge_loop(
            &cli,
            &config,
            &bridge_url,
            bridge_mode.clone(),
            heartbeat_interval,
            &mut local_state,
        )
        .await
        {
            Ok(()) => break,
            Err(err) => {
                eprintln!("[agenthost] bridge loop failed: {err:#}");
                tokio::time::sleep(reconnect_delay).await;
            }
        }
    }

    Ok(())
}

async fn run_bridge_loop(
    cli: &Cli,
    config: &AppConfig,
    bridge_url: &str,
    bridge_mode: BridgeMode,
    heartbeat_interval: Duration,
    local_state: &mut LocalState,
) -> Result<()> {
    let (socket, _) = connect_async(bridge_url)
        .await
        .with_context(|| format!("failed to connect to bridge websocket {bridge_url}"))?;
    let (mut writer, mut reader) = socket.split();

    send_bridge_message(
        &mut writer,
        &BridgeClientMessage::Hello {
            agent: cli.agent.clone(),
            mode: bridge_mode,
            last_ack_delivery_id: Some(local_state.last_ack_delivery_id),
        },
    )
    .await?;

    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if let Some(session_id) = &local_state.session_id {
                    send_bridge_message(
                        &mut writer,
                        &BridgeClientMessage::Heartbeat {
                            session_id: session_id.clone(),
                        },
                    ).await?;
                }
            }
            incoming = reader.next() => {
                let Some(incoming) = incoming else {
                    bail!("bridge websocket closed");
                };
                let incoming = incoming.context("bridge websocket read failed")?;
                match incoming {
                    Message::Text(text) => {
                        let message: BridgeServerMessage = serde_json::from_str(&text)
                            .context("failed to parse bridge server message")?;
                        handle_server_message(cli, config, &mut writer, message, local_state).await?;
                    }
                    Message::Ping(payload) => {
                        writer.send(Message::Pong(payload)).await.context("failed to send websocket pong")?;
                    }
                    Message::Close(_) => bail!("bridge websocket closed by server"),
                    _ => {}
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for ctrl_c")?;
                if !cli.quiet {
                    println!("[agenthost] stop requested for {}", cli.agent);
                }
                return Ok(());
            }
        }
    }
}

async fn handle_server_message(
    cli: &Cli,
    config: &AppConfig,
    writer: &mut (impl Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    message: BridgeServerMessage,
    local_state: &mut LocalState,
) -> Result<()> {
    match message {
        BridgeServerMessage::Welcome {
            session_id,
            snapshot,
            pending_deliveries,
        } => {
            local_state.session_id = Some(session_id.clone());
            apply_snapshot(cli, local_state, snapshot);
            if !cli.quiet {
                println!(
                    "[agenthost] bridge attached | agent={} | session={} | pending={}",
                    cli.agent,
                    session_id,
                    pending_deliveries.len()
                );
            }
            process_pending_deliveries(cli, config, writer, &session_id, pending_deliveries, local_state)
                .await?;
        }
        BridgeServerMessage::SyncSnapshot {
            session_id,
            reason,
            snapshot,
            pending_deliveries,
        } => {
            local_state.session_id = Some(session_id.clone());
            apply_snapshot(cli, local_state, snapshot);
            if !cli.quiet {
                println!(
                    "[agenthost] sync snapshot | agent={} | reason={} | pending={}",
                    cli.agent,
                    reason,
                    pending_deliveries.len()
                );
            }
            process_pending_deliveries(cli, config, writer, &session_id, pending_deliveries, local_state)
                .await?;
        }
        BridgeServerMessage::Delivery {
            session_id,
            delivery,
        } => {
            ensure_session_matches(cli, &session_id, local_state)?;
            process_delivery(cli, config, writer, &session_id, delivery, local_state).await?;
        }
        BridgeServerMessage::Error { message } => {
            bail!("bridge server error: {message}");
        }
        BridgeServerMessage::Pong => {}
    }

    Ok(())
}

async fn process_pending_deliveries(
    cli: &Cli,
    config: &AppConfig,
    writer: &mut (impl Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    session_id: &str,
    pending_deliveries: Vec<BridgeDelivery>,
    local_state: &mut LocalState,
) -> Result<()> {
    let mut sorted = pending_deliveries;
    sorted.sort_by_key(|delivery| delivery.delivery_id);
    for delivery in sorted {
        process_delivery(cli, config, writer, session_id, delivery, local_state).await?;
    }
    Ok(())
}

async fn process_delivery(
    cli: &Cli,
    config: &AppConfig,
    writer: &mut (impl Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    session_id: &str,
    delivery: BridgeDelivery,
    local_state: &mut LocalState,
) -> Result<()> {
    let summary = describe_delivery(&delivery);
    if !cli.quiet {
        println!(
            "[agenthost] delivery {} | kind={:?} | {}",
            delivery.delivery_id,
            delivery.kind,
            summary
        );
    }

    match delivery.kind {
        BridgeDeliveryKind::TaskDispatch => {
            if let Some(task) = &delivery.task {
                local_state.current_task = Some(task.clone());
            }

            if cli.bridge_mode == BridgeModeArg::Autorun
                && delivery
                    .task
                    .as_ref()
                    .map(|task| task.state == TaskState::Pending)
                    .unwrap_or(false)
            {
                let task = delivery
                    .task
                    .as_ref()
                    .ok_or_else(|| anyhow!("task_dispatch delivery missing task payload"))?;
                let response = run_task_round(config, &task.task_id).await?;
                if !cli.quiet {
                    match &response {
                        ControlResponse::TaskRound { task, payload, .. } => {
                            println!(
                                "[agenthost] autorun complete | task={} | state={:?} | summary={}",
                                task.task_id,
                                payload.status,
                                payload.summary
                            );
                        }
                        other => {
                            println!(
                                "[agenthost] autorun returned unexpected response: {}",
                                serde_json::to_string(other)
                                    .unwrap_or_else(|_| "\"unserializable\"".to_string())
                            );
                        }
                    }
                }
            }
        }
        BridgeDeliveryKind::TaskFeedback
        | BridgeDeliveryKind::TaskCancelled
        | BridgeDeliveryKind::TaskClosed
        | BridgeDeliveryKind::SyncRequired => {
            if let Some(task) = &delivery.task {
                local_state.current_task = Some(task.clone());
            }
        }
    }

    local_state.last_ack_delivery_id = local_state
        .last_ack_delivery_id
        .max(delivery.delivery_id);
    send_bridge_message(
        writer,
        &BridgeClientMessage::DeliveryAck {
            session_id: session_id.to_string(),
            delivery_id: delivery.delivery_id,
        },
    )
    .await?;

    if matches!(delivery.kind, BridgeDeliveryKind::TaskDispatch) && cli.bridge_mode == BridgeModeArg::Autorun {
        send_bridge_message(
            writer,
            &BridgeClientMessage::RequestSync {
                session_id: session_id.to_string(),
            },
        )
        .await?;
    }

    Ok(())
}

fn apply_snapshot(cli: &Cli, local_state: &mut LocalState, snapshot: BridgeSyncSnapshot) {
    local_state.agent = Some(snapshot.agent.clone());
    local_state.current_task = snapshot.current_task.clone();

    if !cli.quiet {
        let task_label = snapshot
            .current_task
            .as_ref()
            .map(|task| format!("{}:{:?}", task.task_id, task.state))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "[agenthost] snapshot | agent={} | bridge={:?} | task={}",
            snapshot.agent.name,
            snapshot.agent.bridge_state,
            task_label
        );
    }
}

fn describe_delivery(delivery: &BridgeDelivery) -> String {
    let task_part = delivery
        .task
        .as_ref()
        .map(|task| format!("task={} state={:?}", task.task_id, task.state))
        .unwrap_or_else(|| "task=-".to_string());
    let decision_part = delivery
        .decision
        .as_ref()
        .map(|decision| format!("decision={} status={}", decision.decision_id, decision.status))
        .unwrap_or_else(|| "decision=-".to_string());
    let reason_part = delivery
        .reason
        .as_deref()
        .map(|reason| format!("reason={reason}"))
        .unwrap_or_else(|| "reason=-".to_string());
    format!("{task_part} | {decision_part} | {reason_part}")
}

fn ensure_session_matches(cli: &Cli, session_id: &str, local_state: &LocalState) -> Result<()> {
    match local_state.session_id.as_deref() {
        Some(active) if active == session_id => Ok(()),
        Some(active) => bail!(
            "stale bridge session for agent {}: expected {}, got {}",
            cli.agent,
            active,
            session_id
        ),
        None => bail!("received bridge delivery before welcome for agent {}", cli.agent),
    }
}

async fn send_bridge_message(
    writer: &mut (impl Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    message: &BridgeClientMessage,
) -> Result<()> {
    let payload =
        serde_json::to_string(message).context("failed to serialize bridge client message")?;
    writer
        .send(Message::Text(payload.into()))
        .await
        .context("failed to send bridge websocket message")
}

async fn run_task_round(config: &AppConfig, task_id: &str) -> Result<ControlResponse> {
    send_request(
        config.control_bind.to_string(),
        &ControlRequest::RunTaskRound {
            task_id: task_id.to_string(),
        },
    )
    .await
}

async fn send_request(addr: String, request: &ControlRequest) -> Result<ControlResponse> {
    let mut stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("failed to connect control socket {addr}"))?;
    let payload =
        serde_json::to_string(request).context("failed to serialize control request json")?;
    stream
        .write_all(payload.as_bytes())
        .await
        .context("failed to write control request")?;
    stream
        .write_all(b"\n")
        .await
        .context("failed to write control request newline")?;
    stream.flush().await.context("failed to flush control request")?;

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .await
        .context("failed to read control response")?;

    serde_json::from_str(response.trim()).context("failed to parse control response json")
}
