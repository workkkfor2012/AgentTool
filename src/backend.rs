use std::path::Path;
use std::process::ExitStatus;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::models::{AgentRoundResult, AgentSummary, SessionMode};

const SERVBAY_CODEX_SHIM: &str = r"F:\work\useful\ServBay\packages\node\current\codex";
const SERVBAY_NODE_EXE: &str = r"F:\work\useful\ServBay\packages\node\current\node.exe";
const SERVBAY_CODEX_JS: &str =
    r"F:\work\useful\ServBay\packages\node\current\node_modules\@openai\codex\bin\codex.js";

pub struct BackendStartRequest {
    pub agent: AgentSummary,
    pub prompt: String,
    pub output_schema: Option<String>,
    pub session_mode: SessionMode,
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

pub struct BackendFinished {
    pub parsed: ParsedCodexRound,
    pub stderr_lines: Vec<String>,
    pub status: ExitStatus,
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
        SessionMode::Round => start_round_backend(request),
        SessionMode::Pty => bail!("pty backend not implemented yet"),
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
                let _ = child.kill().await;
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
            status,
            stopped,
        })
    });

    Ok(BackendHandle {
        session_mode: SessionMode::Round,
        pid,
        stop: BackendStopSignal {
            stop_tx: Some(stop_tx),
        },
        events: rx,
        completion,
    })
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
    let mut reader = BufReader::new(stderr).lines();
    let mut lines = Vec::new();
    while let Some(line) = reader.next_line().await? {
        tx.send(BackendEvent::Line {
            stream: BackendStream::Stderr,
            line: line.clone(),
        })
        .await
        .ok();
        lines.push(line);
    }
    Ok(lines)
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
    use crate::models::{AgentRole, AgentSessionState, AgentSummary, SessionMode};

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
            current_session_id: None,
            state: AgentSessionState::Idle,
            current_task_id: None,
            last_output_at: None,
            last_heartbeat_at: None,
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
            current_session_id: None,
            state: AgentSessionState::Idle,
            current_task_id: None,
            last_output_at: None,
            last_heartbeat_at: None,
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
            current_session_id: None,
            state: AgentSessionState::Idle,
            current_task_id: None,
            last_output_at: None,
            last_heartbeat_at: None,
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
