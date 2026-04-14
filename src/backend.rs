use std::collections::VecDeque;
use std::env;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::models::{
    AgentRoundResult, AgentSummary, AppServerOwner, BridgeConnectionState, SessionMode,
};

const SERVBAY_CODEX_SHIM: &str = r"F:\work\useful\ServBay\packages\node\current\codex";
const SERVBAY_NODE_EXE: &str = r"F:\work\useful\ServBay\packages\node\current\node.exe";
const SERVBAY_CODEX_JS: &str =
    r"F:\work\useful\ServBay\packages\node\current\node_modules\@openai\codex\bin\codex.js";
const REMOTE_NOTIFICATION_OPTOUTS: &[&str] = &[
    "thread/started",
    "thread/status/changed",
    "thread/tokenUsage/updated",
    "turn/started",
    "turn/diff/updated",
    "turn/plan/updated",
    "item/started",
    "item/agentMessage/delta",
    "item/plan/delta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/reasoning/textDelta",
    "command/exec/outputDelta",
    "item/commandExecution/outputDelta",
    "item/commandExecution/terminalInteraction",
    "item/fileChange/outputDelta",
    "item/mcpToolCall/progress",
    "account/rateLimits/updated",
];

fn sanitize_agent_name_segment(name: &str) -> String {
    let mut safe = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
            safe.push(ch);
        } else {
            safe.push('_');
        }
    }

    if safe.trim_matches('_').is_empty() {
        "agent".to_string()
    } else {
        safe
    }
}

fn resolve_agent_codex_home(agent_name: &str) -> PathBuf {
    let safe_agent_name = sanitize_agent_name_segment(agent_name);

    if let Ok(root) = env::var("AGENTTOOL_CODEX_HOME_ROOT") {
        return PathBuf::from(root).join(&safe_agent_name);
    }

    if let Ok(user_profile) = env::var("USERPROFILE") {
        return PathBuf::from(user_profile)
            .join("codextemp")
            .join(".codex")
            .join("agents")
            .join(&safe_agent_name);
    }

    env::temp_dir()
        .join("agenttool-codex-home")
        .join(&safe_agent_name)
}

pub struct BackendStartRequest {
    pub agent: AgentSummary,
    pub prompt: String,
    pub output_schema: Option<String>,
    pub session_mode: SessionMode,
}

pub struct AppServerStartRequest {
    pub agent: AgentSummary,
    pub launcher_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendStream {
    Stdout,
    Stderr,
}

pub enum BackendEvent {
    Line { stream: BackendStream, line: String },
    ThreadStarted { thread_id: String },
}

pub struct BackendHandle {
    pub session_mode: SessionMode,
    pub pid: Option<u32>,
    pub endpoint: Option<String>,
    pub stop: BackendStopSignal,
    pub events: mpsc::Receiver<BackendEvent>,
    pub completion: JoinHandle<Result<BackendFinished>>,
}

pub struct BackendStopSignal {
    stop_tx: Option<oneshot::Sender<()>>,
}

impl BackendStopSignal {
    pub fn stop(mut self) -> Result<()> {
        let stop_tx = self
            .stop_tx
            .take()
            .ok_or_else(|| anyhow::anyhow!("backend stop signal already consumed"))?;
        stop_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("backend already finished before stop request"))?;
        Ok(())
    }
}

async fn terminate_child_process(child: &mut Child) {
    #[cfg(windows)]
    if let Some(pid) = child.id() {
        let _ = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;
    }

    let _ = child.kill().await;
}

pub struct BackendFinished {
    pub parsed: ParsedCodexRound,
    pub stderr_lines: Vec<String>,
    pub success: bool,
    pub status_label: String,
    pub stopped: bool,
}

pub struct ParsedCodexRound {
    pub thread_id: Option<String>,
    pub final_message: Option<String>,
    pub error_message: Option<String>,
}

impl ParsedCodexRound {
    pub fn new(existing_thread_id: Option<String>) -> Self {
        Self {
            thread_id: existing_thread_id,
            final_message: None,
            error_message: None,
        }
    }

    pub fn ingest_stdout_line(&mut self, line: &str) -> Option<String> {
        let value = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(value) => value,
            Err(_) => return None,
        };

        self.ingest_json_value(&value)
    }

    pub fn ingest_json_value(&mut self, value: &serde_json::Value) -> Option<String> {
        if let Some(method) = value.get("method").and_then(|method| method.as_str()) {
            match method {
                "thread/started" => {
                    if let Some(thread_id) = value
                        .get("params")
                        .and_then(|params| params.get("thread"))
                        .and_then(|thread| thread.get("id"))
                        .and_then(|id| id.as_str())
                    {
                        let thread_id = thread_id.to_string();
                        self.thread_id = Some(thread_id.clone());
                        return Some(thread_id);
                    }
                }
                "item/completed" => {
                    if let Some(text) = value
                        .get("params")
                        .and_then(|params| params.get("item"))
                        .and_then(extract_item_text)
                    {
                        self.final_message = Some(text.to_string());
                    }
                }
                "turn/completed" => {
                    if let Some(message) = value
                        .get("params")
                        .and_then(|params| params.get("turn"))
                        .and_then(extract_turn_error_message)
                    {
                        self.error_message = Some(message);
                    }
                }
                _ => {}
            }

            return None;
        }

        let kind = match value.get("type").and_then(|kind| kind.as_str()) {
            Some(kind) => kind,
            None => return None,
        };

        match kind {
            "thread.started" => {
                if let Some(thread_id) = value.get("thread_id").and_then(|v| v.as_str()) {
                    let thread_id = thread_id.to_string();
                    self.thread_id = Some(thread_id.clone());
                    return Some(thread_id);
                }
            }
            "error" | "turn.failed" => {
                if let Some(message) = extract_error_message(&value) {
                    self.error_message = Some(message);
                }
            }
            "item.completed" => {
                if let Some(text) = value.get("item").and_then(extract_item_text) {
                    self.final_message = Some(text.to_string());
                }
            }
            _ => {}
        }

        None
    }

    pub fn into_round_result(
        self,
        agent_name: &str,
        completed_at: DateTime<Utc>,
    ) -> Result<AgentRoundResult> {
        Ok(AgentRoundResult {
            agent: agent_name.to_string(),
            thread_id: self
                .thread_id
                .ok_or_else(|| anyhow::anyhow!("missing thread_id from codex output"))?,
            final_message: self.final_message.unwrap_or_default(),
            completed_at,
        })
    }
}

pub fn start_backend(request: BackendStartRequest) -> Result<BackendHandle> {
    match request.session_mode {
        SessionMode::Round => {
            if should_use_remote_round_backend(&request.agent) {
                return start_remote_round_backend(request);
            }
            start_round_backend(request)
        }
        SessionMode::Pty => bail!("pty backend not implemented yet"),
        SessionMode::AppServer => bail!("app-server backend must be started explicitly"),
    }
}

fn start_round_backend(request: BackendStartRequest) -> Result<BackendHandle> {
    let mut command = build_codex_round_command(&request.agent, request.output_schema.as_deref());

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn codex for agent {}", request.agent.name))?;
    let pid = child.id();
    let stdin = child.stdin.take().ok_or_else(|| {
        anyhow::anyhow!("failed to capture stdin for agent {}", request.agent.name)
    })?;

    let stdout = child.stdout.take().ok_or_else(|| {
        anyhow::anyhow!("failed to capture stdout for agent {}", request.agent.name)
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        anyhow::anyhow!("failed to capture stderr for agent {}", request.agent.name)
    })?;

    let (tx, rx) = mpsc::channel(256);
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let existing_thread_id = request.agent.thread_id.clone();
    let prompt = request.prompt;

    let completion = tokio::spawn(async move {
        let stdout_tx = tx.clone();
        let stderr_tx = tx.clone();
        let stdin_task = tokio::spawn(async move { write_prompt_to_stdin(stdin, &prompt).await });

        let stdout_task =
            tokio::spawn(
                async move { parse_codex_stdout(stdout, stdout_tx, existing_thread_id).await },
            );
        let stderr_task = tokio::spawn(async move { stream_stderr(stderr, stderr_tx).await });

        drop(tx);

        let (status, stopped) = tokio::select! {
            status = child.wait() => (status?, false),
            _ = &mut stop_rx => {
                terminate_child_process(&mut child).await;
                let status = child.wait().await?;
                (status, true)
            }
        };
        stdin_task.await.context("stdin task join failed")??;
        let parsed = stdout_task.await.context("stdout task join failed")??;
        let stderr_lines = stderr_task.await.context("stderr task join failed")??;

        Ok(BackendFinished {
            parsed,
            stderr_lines,
            success: status.success(),
            status_label: status.to_string(),
            stopped,
        })
    });

    Ok(BackendHandle {
        session_mode: SessionMode::Round,
        pid,
        endpoint: None,
        stop: BackendStopSignal {
            stop_tx: Some(stop_tx),
        },
        events: rx,
        completion,
    })
}

type RemoteSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn should_use_remote_round_backend(agent: &AgentSummary) -> bool {
    agent.app_server_url.is_some()
        && agent.thread_id.is_some()
        && (agent.bridge_state == BridgeConnectionState::Connected
            || agent.app_server_owner == Some(AppServerOwner::Daemon))
}

fn start_remote_round_backend(request: BackendStartRequest) -> Result<BackendHandle> {
    let (tx, rx) = mpsc::channel(256);
    let (stop_tx, stop_rx) = oneshot::channel();
    let endpoint = request.agent.app_server_url.clone();

    let completion = tokio::spawn(async move {
        run_remote_round_backend(request, tx, stop_rx).await
    });

    Ok(BackendHandle {
        session_mode: SessionMode::Round,
        pid: None,
        endpoint,
        stop: BackendStopSignal {
            stop_tx: Some(stop_tx),
        },
        events: rx,
        completion,
    })
}

pub fn start_app_server_backend(request: AppServerStartRequest) -> Result<BackendHandle> {
    let listen_url = format!("ws://127.0.0.1:{}", reserve_loopback_port()?);
    let codex_home = resolve_agent_codex_home(&request.agent.name);
    std::fs::create_dir_all(&codex_home).with_context(|| {
        format!(
            "failed to create CODEX_HOME {} for agent {}",
            codex_home.display(),
            request.agent.name
        )
    })?;
    let mut command = Command::new(&request.launcher_path);
    command
        .arg("app-server")
        .arg("--listen")
        .arg(&listen_url)
        .env("CODEX_HOME", &codex_home)
        .env("AGENTTOOL_CODEX_HOME", &codex_home)
        .current_dir(&request.agent.cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = command.spawn().with_context(|| {
        format!(
            "failed to spawn managed app-server for agent {} with launcher {}",
            request.agent.name,
            request.launcher_path.display()
        )
    })?;
    let pid = child.id();
    let stdout = child.stdout.take().ok_or_else(|| {
        anyhow!(
            "failed to capture app-server stdout for agent {}",
            request.agent.name
        )
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        anyhow!(
            "failed to capture app-server stderr for agent {}",
            request.agent.name
        )
    })?;

    let (tx, rx) = mpsc::channel(256);
    let (stop_tx, mut stop_rx) = oneshot::channel();

    let completion = tokio::spawn(async move {
        let stdout_tx = tx.clone();
        let stderr_tx = tx.clone();
        let stdout_task = tokio::spawn(async move {
            stream_output_lines(stdout, BackendStream::Stdout, stdout_tx).await
        });
        let stderr_task = tokio::spawn(async move {
            stream_output_lines(stderr, BackendStream::Stderr, stderr_tx).await
        });

        drop(tx);

        let (status, stopped) = tokio::select! {
            status = child.wait() => (status?, false),
            _ = &mut stop_rx => {
                terminate_child_process(&mut child).await;
                let status = child.wait().await?;
                (status, true)
            }
        };

        let _stdout_lines = stdout_task
            .await
            .context("app-server stdout task join failed")??;
        let stderr_lines = stderr_task
            .await
            .context("app-server stderr task join failed")??;

        Ok(BackendFinished {
            parsed: ParsedCodexRound::new(None),
            stderr_lines,
            success: status.success(),
            status_label: status.to_string(),
            stopped,
        })
    });

    Ok(BackendHandle {
        session_mode: SessionMode::AppServer,
        pid,
        endpoint: Some(listen_url),
        stop: BackendStopSignal {
            stop_tx: Some(stop_tx),
        },
        events: rx,
        completion,
    })
}

async fn run_remote_round_backend(
    request: BackendStartRequest,
    tx: mpsc::Sender<BackendEvent>,
    mut stop_rx: oneshot::Receiver<()>,
) -> Result<BackendFinished> {
    let app_server_url = request
        .agent
        .app_server_url
        .clone()
        .ok_or_else(|| anyhow!("agent {} has no registered app-server url", request.agent.name))?;
    let thread_id = request
        .agent
        .thread_id
        .clone()
        .ok_or_else(|| anyhow!("agent {} has no thread id for remote resume", request.agent.name))?;
    let output_schema = load_output_schema_value(request.output_schema.as_deref())?;

    let (mut socket, _) = connect_async(&app_server_url)
        .await
        .with_context(|| format!("failed to connect remote app-server {app_server_url}"))?;

    let mut pending_messages = VecDeque::new();
    let mut next_request_id = 0_u64;
    let mut parsed = ParsedCodexRound::new(Some(thread_id.clone()));
    let mut stderr_lines = Vec::new();

    remote_send_request(
        &mut socket,
        &mut pending_messages,
        &mut next_request_id,
        "initialize",
        json!({
            "clientInfo": {
                "name": "agenttool_runtime",
                "title": "AgentTool Runtime",
                "version": env!("CARGO_PKG_VERSION")
            },
            "capabilities": {
                "experimentalApi": true,
                "optOutNotificationMethods": REMOTE_NOTIFICATION_OPTOUTS
            }
        }),
    )
    .await?;
    remote_send_notification(&mut socket, "initialized", json!({})).await?;
    remote_send_request(
        &mut socket,
        &mut pending_messages,
        &mut next_request_id,
        "thread/resume",
        json!({
            "threadId": thread_id
        }),
    )
    .await?;

    tx.send(BackendEvent::ThreadStarted {
        thread_id: thread_id.clone(),
    })
    .await
    .ok();

    let turn_response = remote_send_request(
        &mut socket,
        &mut pending_messages,
        &mut next_request_id,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [
                {
                    "type": "text",
                    "text": request.prompt,
                    "textElements": []
                }
            ],
            "outputSchema": output_schema
        }),
    )
    .await?;
    let turn_id = turn_response
        .get("result")
        .and_then(|result| result.get("turn"))
        .and_then(|turn| turn.get("id"))
        .and_then(|id| id.as_str())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("remote turn/start did not return turn id"))?;

    let mut turn_status: Option<String> = None;
    let mut stopped = false;

    loop {
        let next_message = if let Some(message) = pending_messages.pop_front() {
            Some(Ok(message))
        } else {
            tokio::select! {
                _ = &mut stop_rx => {
                    stopped = true;
                    if let Err(err) = remote_interrupt_turn(
                        &mut socket,
                        &mut pending_messages,
                        &mut next_request_id,
                        &thread_id,
                        &turn_id,
                    ).await {
                        stderr_lines.push(err.to_string());
                    }
                    None
                }
                message = remote_read_message(&mut socket) => Some(message),
            }
        };

        let Some(message) = next_message else {
            break;
        };
        let message = message?;
        let serialized =
            serde_json::to_string(&message).context("failed to serialize remote app-server event")?;
        tx.send(BackendEvent::Line {
            stream: BackendStream::Stdout,
            line: serialized.clone(),
        })
        .await
        .ok();

        if let Some(new_thread_id) = parsed.ingest_json_value(&message) {
            tx.send(BackendEvent::ThreadStarted {
                thread_id: new_thread_id,
            })
            .await
            .ok();
        }

        if let Some((completed_turn_id, status, error_message)) =
            extract_remote_turn_completed(&message)
            && completed_turn_id == turn_id
        {
            turn_status = Some(status);
            if let Some(error_message) = error_message {
                parsed.error_message = Some(error_message.clone());
                stderr_lines.push(error_message);
            }
            break;
        }
    }

    let success = !stopped && matches!(turn_status.as_deref(), Some("completed"));
    let status_label = if stopped {
        "stopped by request".to_string()
    } else if let Some(status) = turn_status {
        status
    } else {
        "remote turn ended without turn/completed".to_string()
    };

    Ok(BackendFinished {
        parsed,
        stderr_lines,
        success,
        status_label,
        stopped,
    })
}

pub async fn wait_for_app_server_ready(url: &str, timeout: Duration) -> Result<()> {
    let started_at = tokio::time::Instant::now();

    loop {
        match connect_async(url).await {
            Ok((mut socket, _)) => {
                let _ = socket.close(None).await;
                return Ok(());
            }
            Err(err) => {
                let err_message = err.to_string();
                if started_at.elapsed() >= timeout {
                    bail!("managed app-server at {url} did not become ready: {err_message}");
                }
            }
        }

        if started_at.elapsed() >= timeout {
            bail!("managed app-server at {url} did not become ready before timeout");
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub async fn run_remote_bootstrap_round(
    agent_name: &str,
    app_server_url: &str,
    cwd: &str,
    developer_instructions: Option<&str>,
    prompt: &str,
    output_schema: Option<&str>,
) -> Result<AgentRoundResult> {
    let output_schema = load_output_schema_value(output_schema)?;
    let (mut socket, _) = connect_async(app_server_url)
        .await
        .with_context(|| format!("failed to connect managed app-server {app_server_url}"))?;

    let mut pending_messages = VecDeque::new();
    let mut next_request_id = 0_u64;
    let mut parsed = ParsedCodexRound::new(None);

    remote_send_request(
        &mut socket,
        &mut pending_messages,
        &mut next_request_id,
        "initialize",
        json!({
            "clientInfo": {
                "name": "agenttool_managed_bootstrap",
                "title": "AgentTool Managed Bootstrap",
                "version": env!("CARGO_PKG_VERSION")
            },
            "capabilities": {
                "experimentalApi": true,
                "optOutNotificationMethods": REMOTE_NOTIFICATION_OPTOUTS
            }
        }),
    )
    .await?;
    remote_send_notification(&mut socket, "initialized", json!({})).await?;

    let thread_response = remote_send_request(
        &mut socket,
        &mut pending_messages,
        &mut next_request_id,
        "thread/start",
        json!({
            "cwd": cwd,
            "serviceName": "agenttool_managed_session",
            "developerInstructions": developer_instructions,
            "personality": "pragmatic",
            "experimentalRawEvents": false,
            "persistExtendedHistory": false
        }),
    )
    .await?;

    let thread_id = thread_response
        .get("result")
        .and_then(|result| result.get("thread"))
        .and_then(|thread| thread.get("id"))
        .and_then(|id| id.as_str())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("managed bootstrap thread/start did not return thread id"))?;
    parsed.thread_id = Some(thread_id.clone());

    let turn_response = remote_send_request(
        &mut socket,
        &mut pending_messages,
        &mut next_request_id,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [
                {
                    "type": "text",
                    "text": prompt,
                    "textElements": []
                }
            ],
            "cwd": cwd,
            "outputSchema": output_schema
        }),
    )
    .await?;
    let turn_id = turn_response
        .get("result")
        .and_then(|result| result.get("turn"))
        .and_then(|turn| turn.get("id"))
        .and_then(|id| id.as_str())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("managed bootstrap turn/start did not return turn id"))?;

    let turn_status = loop {
        let message = if let Some(message) = pending_messages.pop_front() {
            message
        } else {
            remote_read_message(&mut socket).await?
        };

        let _ = parsed.ingest_json_value(&message);

        if let Some((completed_turn_id, status, error_message)) =
            extract_remote_turn_completed(&message)
            && completed_turn_id == turn_id
        {
            if let Some(error_message) = error_message {
                parsed.error_message = Some(error_message);
            }
            break status;
        }
    };

    if turn_status != "completed" {
        let detail = parsed
            .error_message
            .clone()
            .unwrap_or_else(|| "managed bootstrap turn did not complete successfully".to_string());
        bail!("{detail}");
    }

    parsed.into_round_result(agent_name, Utc::now())
}

pub fn build_codex_round_command(agent: &AgentSummary, output_schema: Option<&str>) -> Command {
    let mut command = preferred_codex_command();
    command.args(["-a", "never", "-s", "workspace-write"]);
    let allow_resume = agent.thread_id.is_some() && output_schema.is_none();

    if let Some(thread_id) = agent.thread_id.as_ref().filter(|_| allow_resume) {
        command.args([
            "exec",
            "resume",
            "--json",
            "--skip-git-repo-check",
            thread_id,
            "-",
        ]);
    } else {
        command.args([
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--cd",
            &agent.cwd,
            "-",
        ]);
    }

    if let Some(schema_path) = output_schema {
        command.args(["--output-schema", schema_path]);
    }

    command.stdin(std::process::Stdio::piped());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());
    command
}

fn preferred_codex_command() -> Command {
    if Path::new(SERVBAY_CODEX_SHIM).is_file()
        && Path::new(SERVBAY_NODE_EXE).is_file()
        && Path::new(SERVBAY_CODEX_JS).is_file()
    {
        // The PATH-leading ServBay `codex` is a POSIX shell shim.
        // Spawn the same package through node directly so Windows can launch it reliably.
        let mut command = Command::new(SERVBAY_NODE_EXE);
        command.arg(SERVBAY_CODEX_JS);
        return command;
    }

    Command::new("codex")
}

async fn write_prompt_to_stdin(mut stdin: tokio::process::ChildStdin, prompt: &str) -> Result<()> {
    match stdin.write_all(prompt.as_bytes()).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
        Err(err) => return Err(err).context("failed to write prompt to codex stdin"),
    }

    match stdin.shutdown().await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(err) => Err(err).context("failed to close codex stdin"),
    }
}

fn load_output_schema_value(output_schema_path: Option<&str>) -> Result<Option<Value>> {
    let Some(output_schema_path) = output_schema_path else {
        return Ok(None);
    };

    let schema_text = std::fs::read_to_string(output_schema_path)
        .with_context(|| format!("failed to read output schema {output_schema_path}"))?;
    let schema = serde_json::from_str::<Value>(&schema_text)
        .with_context(|| format!("failed to parse output schema json {output_schema_path}"))?;
    Ok(Some(schema))
}

async fn remote_send_notification(
    socket: &mut RemoteSocket,
    method: &str,
    params: Value,
) -> Result<()> {
    remote_send_value(
        socket,
        json!({
            "method": method,
            "params": params
        }),
    )
    .await
}

async fn remote_send_request(
    socket: &mut RemoteSocket,
    pending_messages: &mut VecDeque<Value>,
    next_request_id: &mut u64,
    method: &str,
    params: Value,
) -> Result<Value> {
    *next_request_id = next_request_id.saturating_add(1);
    let request_id = *next_request_id;
    remote_send_value(
        socket,
        json!({
            "id": request_id,
            "method": method,
            "params": params
        }),
    )
    .await?;
    remote_wait_response(socket, pending_messages, request_id).await
}

async fn remote_interrupt_turn(
    socket: &mut RemoteSocket,
    pending_messages: &mut VecDeque<Value>,
    next_request_id: &mut u64,
    thread_id: &str,
    turn_id: &str,
) -> Result<()> {
    remote_send_request(
        socket,
        pending_messages,
        next_request_id,
        "turn/interrupt",
        json!({
            "threadId": thread_id,
            "turnId": turn_id
        }),
    )
    .await?;
    Ok(())
}

async fn remote_wait_response(
    socket: &mut RemoteSocket,
    pending_messages: &mut VecDeque<Value>,
    request_id: u64,
) -> Result<Value> {
    let mut deferred = VecDeque::new();
    loop {
        let message = if let Some(message) = pending_messages.pop_front() {
            message
        } else {
            remote_read_message(socket).await?
        };

        if message
            .get("id")
            .and_then(|id| id.as_u64())
            .filter(|id| *id == request_id)
            .is_some()
        {
            while let Some(message) = deferred.pop_front() {
                pending_messages.push_back(message);
            }
            if let Some(error_message) = extract_rpc_error_message(&message) {
                bail!("remote rpc request {request_id} failed: {error_message}");
            }
            return Ok(message);
        }

        deferred.push_back(message);
    }
}

async fn remote_send_value(socket: &mut RemoteSocket, value: Value) -> Result<()> {
    let payload =
        serde_json::to_string(&value).context("failed to serialize remote app-server json")?;
    socket
        .send(Message::Text(payload.into()))
        .await
        .context("failed to send remote app-server websocket frame")
}

async fn remote_read_message(socket: &mut RemoteSocket) -> Result<Value> {
    loop {
        let message = socket
            .next()
            .await
            .ok_or_else(|| anyhow!("remote app-server websocket closed"))?
            .context("remote app-server websocket read failed")?;

        match message {
            Message::Text(text) => {
                return serde_json::from_str::<Value>(&text)
                    .with_context(|| format!("remote app-server returned invalid json: {text}"));
            }
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .context("failed to respond to remote websocket ping")?;
            }
            Message::Close(frame) => {
                let reason = frame
                    .as_ref()
                    .map(|frame| frame.reason.to_string())
                    .filter(|reason| !reason.is_empty())
                    .unwrap_or_else(|| "remote app-server closed the websocket".to_string());
                bail!(reason);
            }
            _ => {}
        }
    }
}

async fn parse_codex_stdout(
    stdout: tokio::process::ChildStdout,
    tx: mpsc::Sender<BackendEvent>,
    existing_thread_id: Option<String>,
) -> Result<ParsedCodexRound> {
    let mut reader = BufReader::new(stdout).lines();
    let mut parsed = ParsedCodexRound::new(existing_thread_id);

    while let Some(line) = reader.next_line().await? {
        tx.send(BackendEvent::Line {
            stream: BackendStream::Stdout,
            line: line.clone(),
        })
        .await
        .ok();

        if let Some(thread_id) = parsed.ingest_stdout_line(&line) {
            tx.send(BackendEvent::ThreadStarted { thread_id })
                .await
                .ok();
        }
    }

    Ok(parsed)
}

async fn stream_stderr(
    stderr: tokio::process::ChildStderr,
    tx: mpsc::Sender<BackendEvent>,
) -> Result<Vec<String>> {
    stream_output_lines(stderr, BackendStream::Stderr, tx).await
}

async fn stream_output_lines<R>(
    reader: R,
    stream: BackendStream,
    tx: mpsc::Sender<BackendEvent>,
) -> Result<Vec<String>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut reader = BufReader::new(reader).lines();
    let mut lines = Vec::new();
    while let Some(line) = reader.next_line().await? {
        tx.send(BackendEvent::Line {
            stream,
            line: line.clone(),
        })
        .await
        .ok();
        lines.push(line);
    }
    Ok(lines)
}

fn reserve_loopback_port() -> Result<u16> {
    let listener = StdTcpListener::bind(("127.0.0.1", 0))
        .context("failed to reserve a loopback port for managed app-server")?;
    let port = listener
        .local_addr()
        .context("failed to inspect reserved managed app-server port")?
        .port();
    Ok(port)
}

fn extract_error_message(value: &serde_json::Value) -> Option<String> {
    value
        .get("message")
        .and_then(|message| message.as_str())
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(|message| message.as_str())
                .map(ToString::to_string)
        })
        .or_else(|| {
            value
                .get("details")
                .and_then(|details| details.get("message"))
                .and_then(|message| message.as_str())
                .map(ToString::to_string)
        })
}

fn extract_rpc_error_message(value: &Value) -> Option<String> {
    value
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(|message| message.as_str())
        .map(ToString::to_string)
}

fn extract_turn_error_message(turn: &Value) -> Option<String> {
    turn.get("error")
        .and_then(|error| error.get("message"))
        .and_then(|message| message.as_str())
        .map(ToString::to_string)
}

fn extract_remote_turn_completed(value: &Value) -> Option<(String, String, Option<String>)> {
    let method = value.get("method").and_then(|method| method.as_str())?;
    if method != "turn/completed" {
        return None;
    }

    let turn = value.get("params").and_then(|params| params.get("turn"))?;
    let turn_id = turn.get("id").and_then(|id| id.as_str())?.to_string();
    let status = turn
        .get("status")
        .and_then(|status| status.as_str())
        .unwrap_or("unknown")
        .to_string();
    let error_message = extract_turn_error_message(turn);
    Some((turn_id, status, error_message))
}

fn extract_item_text<'a>(item: &'a serde_json::Value) -> Option<&'a str> {
    item.get("text")
        .and_then(|text| text.as_str())
        .or_else(|| {
            item.get("content").and_then(|content| {
                content.as_array().and_then(|parts| {
                    parts.iter().find_map(|part| {
                        part.get("text")
                            .and_then(|text| text.as_str())
                            .or_else(|| part.get("content").and_then(|text| text.as_str()))
                    })
                })
            })
        })
        .or_else(|| item.get("output_text").and_then(|text| text.as_str()))
}

#[cfg(test)]
mod tests {
    use crate::models::{
        AgentBootstrapState, AgentRole, AgentSessionState, AgentSummary,
        BridgeConnectionState, SessionMode,
    };

    use super::{
        BackendStartRequest, BackendStream, ParsedCodexRound, build_codex_round_command,
        start_backend,
    };

    #[test]
    fn ingests_thread_started_and_completed_item() {
        let mut parsed = ParsedCodexRound::new(None);

        let thread =
            parsed.ingest_stdout_line(r#"{"type":"thread.started","thread_id":"thr_123"}"#);
        assert_eq!(thread.as_deref(), Some("thr_123"));

        parsed.ingest_stdout_line(
            r#"{"type":"item.completed","item":{"content":[{"text":"final output"}]}}"#,
        );

        assert_eq!(parsed.thread_id.as_deref(), Some("thr_123"));
        assert_eq!(parsed.final_message.as_deref(), Some("final output"));
    }

    #[test]
    fn ingests_error_message_from_nested_payload() {
        let mut parsed = ParsedCodexRound::new(Some("thr_old".to_string()));

        parsed.ingest_stdout_line(
            r#"{"type":"turn.failed","error":{"message":"upstream usage limit"}}"#,
        );

        assert_eq!(parsed.thread_id.as_deref(), Some("thr_old"));
        assert_eq!(
            parsed.error_message.as_deref(),
            Some("upstream usage limit")
        );
    }

    #[test]
    fn backend_stream_variants_are_stable() {
        assert!(matches!(BackendStream::Stdout, BackendStream::Stdout));
        assert!(matches!(BackendStream::Stderr, BackendStream::Stderr));
    }

    #[test]
    fn round_command_uses_workspace_write_and_stdin_prompt() {
        let agent = AgentSummary {
            name: "child".to_string(),
            role: AgentRole::Child,
            repo_name: Some("repo".to_string()),
            cwd: "F:\\work\\github\\hackman\\guardpro_factory".to_string(),
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
            bootstrap_state: AgentBootstrapState::AwaitingInit,
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
        };

        let command = build_codex_round_command(&agent, Some("schema.json"));
        let args = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>();

        assert!(args.contains(&"-a".to_string()));
        assert!(args.contains(&"never".to_string()));
        assert!(args.contains(&"-s".to_string()));
        assert!(args.contains(&"workspace-write".to_string()));
        assert!(args.contains(&"-".to_string()));
        assert!(args.contains(&"--output-schema".to_string()));
        assert!(!args.contains(&"resume".to_string()));
    }

    #[test]
    fn resume_is_only_used_for_schema_less_rounds() {
        let agent = AgentSummary {
            name: "child".to_string(),
            role: AgentRole::Child,
            repo_name: Some("repo".to_string()),
            cwd: "F:\\work\\github\\hackman\\guardpro_factory".to_string(),
            prompt_path: None,
            thread_id: Some("thread-123".to_string()),
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
            last_heartbeat_at: None,
            bridge_state: BridgeConnectionState::Disconnected,
            bridge_mode: None,
            bridge_session_id: None,
            bridge_connected_at: None,
            bridge_last_seen_at: None,
            bridge_last_delivery_id: 0,
            bridge_last_ack_delivery_id: 0,
            bridge_pending_delivery_count: 0,
        };

        let schema_command = build_codex_round_command(&agent, Some("schema.json"));
        let schema_args = schema_command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert!(!schema_args.contains(&"resume".to_string()));

        let resume_command = build_codex_round_command(&agent, None);
        let resume_args = resume_command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert!(resume_args.contains(&"resume".to_string()));
    }

    #[test]
    fn pty_backend_dispatch_is_explicitly_unavailable() {
        let agent = AgentSummary {
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
            bootstrap_state: AgentBootstrapState::AwaitingInit,
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
        };

        let result = start_backend(BackendStartRequest {
            agent,
            prompt: "noop".to_string(),
            output_schema: None,
            session_mode: SessionMode::Pty,
        });

        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("pty backend not implemented yet")
        );
    }
}
