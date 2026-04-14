use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

const DEFAULT_MYCODEX_LAUNCHER: &str = r"F:\Users\schu\bin\mycodex.bat";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeEndpointRecord {
    pub ws_addr: String,
    pub control_addr: String,
    pub pid: u32,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub root_dir: PathBuf,
    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub runtime_endpoint_path: PathBuf,
    pub dashboard_runtime_script_path: PathBuf,
    pub ws_bind: SocketAddr,
    pub control_bind: SocketAddr,
    pub codex_launcher: PathBuf,
    pub app_server_start_timeout_seconds: u64,
    pub managed_bootstrap_timeout_seconds: u64,
}

impl AppConfig {
    pub fn load_agentd() -> Result<Self> {
        let mut config = Self::load_base()?;
        config.ws_bind = env::var("AGENTTOOL_WS_BIND")
            .unwrap_or_else(|_| "127.0.0.1:0".to_string())
            .parse()
            .context("failed to parse AGENTTOOL_WS_BIND")?;
        config.control_bind = env::var("AGENTTOOL_CONTROL_BIND")
            .unwrap_or_else(|_| "127.0.0.1:0".to_string())
            .parse()
            .context("failed to parse AGENTTOOL_CONTROL_BIND")?;
        Ok(config)
    }

    pub fn load_client() -> Result<Self> {
        let mut config = Self::load_base()?;

        let explicit_ws_bind = env::var("AGENTTOOL_WS_BIND").ok();
        let explicit_control_bind = env::var("AGENTTOOL_CONTROL_BIND").ok();
        match (explicit_ws_bind, explicit_control_bind) {
            (Some(ws_bind), Some(control_bind)) => {
                config.ws_bind = ws_bind
                    .parse()
                    .context("failed to parse AGENTTOOL_WS_BIND")?;
                config.control_bind = control_bind
                    .parse()
                    .context("failed to parse AGENTTOOL_CONTROL_BIND")?;
                return Ok(config);
            }
            (Some(_), None) | (None, Some(_)) => {
                bail!(
                    "AGENTTOOL_WS_BIND and AGENTTOOL_CONTROL_BIND must be provided together for explicit client override"
                );
            }
            (None, None) => {}
        }

        let endpoint = config
            .read_runtime_endpoint()
            .with_context(|| {
                format!(
                    "failed to load AgentTool runtime endpoint from {}",
                    config.runtime_endpoint_path.display()
                )
            })?;

        if !is_agentd_pid_live(endpoint.pid) {
            bail!(
                "AgentTool runtime endpoint is stale: pid {} is not a live agentd instance",
                endpoint.pid
            );
        }

        config.ws_bind = endpoint
            .ws_addr
            .parse()
            .with_context(|| format!("failed to parse runtime websocket endpoint {}", endpoint.ws_addr))?;
        config.control_bind = endpoint
            .control_addr
            .parse()
            .with_context(|| format!("failed to parse runtime control endpoint {}", endpoint.control_addr))?;
        Ok(config)
    }

    fn load_base() -> Result<Self> {
        let root_dir = env::var("AGENTTOOL_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| env::current_dir().expect("current_dir should exist"));

        let data_dir = env::var("AGENTTOOL_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| root_dir.join("data"));

        let db_path = env::var("AGENTTOOL_DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| data_dir.join("agenttool.db"));

        let runtime_endpoint_path = env::var("AGENTTOOL_RUNTIME_ENDPOINT_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| data_dir.join("runtime_endpoint.json"));

        let dashboard_runtime_script_path = env::var("AGENTTOOL_DASHBOARD_ENDPOINT_SCRIPT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| root_dir.join("dashboard").join("runtime-endpoint.js"));

        let codex_launcher = env::var("AGENTTOOL_CODEX_LAUNCHER")
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_codex_launcher());

        let app_server_start_timeout_seconds = env::var("AGENTTOOL_APP_SERVER_START_TIMEOUT_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(20);

        let managed_bootstrap_timeout_seconds =
            env::var("AGENTTOOL_MANAGED_BOOTSTRAP_TIMEOUT_SECONDS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(90);

        Ok(Self {
            root_dir,
            data_dir,
            db_path,
            runtime_endpoint_path,
            dashboard_runtime_script_path,
            ws_bind: "127.0.0.1:0"
                .parse()
                .expect("127.0.0.1:0 must be a valid socket address"),
            control_bind: "127.0.0.1:0"
                .parse()
                .expect("127.0.0.1:0 must be a valid socket address"),
            codex_launcher,
            app_server_start_timeout_seconds,
            managed_bootstrap_timeout_seconds,
        })
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        ensure_dir(&self.root_dir)?;
        ensure_dir(&self.data_dir)?;
        if let Some(parent) = self.dashboard_runtime_script_path.parent() {
            ensure_dir(parent)?;
        }
        Ok(())
    }

    pub fn read_runtime_endpoint(&self) -> Result<RuntimeEndpointRecord> {
        let raw = fs::read_to_string(&self.runtime_endpoint_path).with_context(|| {
            format!(
                "failed to read runtime endpoint file {}",
                self.runtime_endpoint_path.display()
            )
        })?;
        serde_json::from_str(&raw).with_context(|| {
            format!(
                "failed to parse runtime endpoint file {}",
                self.runtime_endpoint_path.display()
            )
        })
    }

    pub fn write_runtime_endpoint(&self, endpoint: &RuntimeEndpointRecord) -> Result<()> {
        ensure_dir(&self.data_dir)?;
        let payload = serde_json::to_string_pretty(endpoint)
            .context("failed to serialize runtime endpoint json")?;
        write_atomic_utf8(&self.runtime_endpoint_path, &payload)?;
        self.write_dashboard_runtime_script(Some(endpoint))
    }

    pub fn clear_runtime_endpoint_if_matches(&self, pid: u32) -> Result<()> {
        if let Ok(existing) = self.read_runtime_endpoint() {
            if existing.pid == pid {
                if self.runtime_endpoint_path.is_file() {
                    fs::remove_file(&self.runtime_endpoint_path).with_context(|| {
                        format!(
                            "failed to remove runtime endpoint file {}",
                            self.runtime_endpoint_path.display()
                        )
                    })?;
                }
            }
        }
        self.write_dashboard_runtime_script(None)
    }

    fn write_dashboard_runtime_script(
        &self,
        endpoint: Option<&RuntimeEndpointRecord>,
    ) -> Result<()> {
        let body = match endpoint {
            Some(endpoint) => {
                let payload = serde_json::to_string(endpoint)
                    .context("failed to serialize dashboard runtime endpoint payload")?;
                format!("window.__AGENTTOOL_RUNTIME_ENDPOINT__ = {payload};\n")
            }
            None => "window.__AGENTTOOL_RUNTIME_ENDPOINT__ = null;\n".to_string(),
        };

        write_atomic_utf8(&self.dashboard_runtime_script_path, &body)
    }
}

fn write_atomic_utf8(path: &Path, content: &str) -> Result<()> {
    let tmp_path = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("tmp")
    ));
    fs::write(&tmp_path, content)
        .with_context(|| format!("failed to write temporary file {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to replace {} with {}",
            path.display(),
            tmp_path.display()
        )
    })
}

fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create directory {}", path.display()))
}

fn default_codex_launcher() -> PathBuf {
    let custom = PathBuf::from(DEFAULT_MYCODEX_LAUNCHER);
    if custom.is_file() {
        return custom;
    }

    PathBuf::from("codex")
}

fn is_agentd_pid_live(pid: u32) -> bool {
    #[cfg(windows)]
    {
        return is_windows_agentd_pid_live(pid);
    }

    #[cfg(not(windows))]
    {
        std::fs::metadata(format!("/proc/{pid}")).is_ok()
    }
}

#[cfg(windows)]
fn is_windows_agentd_pid_live(pid: u32) -> bool {
    let output = match std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .stdin(std::process::Stdio::null())
        .output()
    {
        Ok(output) => output,
        Err(_) => return false,
    };

    if !output.status.success() {
        return false;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.trim();
    if line.is_empty() || line.starts_with("INFO:") {
        return false;
    }

    let first_field = line
        .trim_start_matches('"')
        .split("\",")
        .next()
        .unwrap_or_default()
        .trim_matches('"')
        .to_ascii_lowercase();
    first_field == "agentd.exe"
}

#[cfg(test)]
mod tests {
    use super::RuntimeEndpointRecord;
    use chrono::Utc;

    #[test]
    fn runtime_endpoint_json_roundtrip() {
        let endpoint = RuntimeEndpointRecord {
            ws_addr: "127.0.0.1:7280".to_string(),
            control_addr: "127.0.0.1:7281".to_string(),
            pid: 1234,
            started_at: Utc::now(),
        };

        let payload = serde_json::to_string(&endpoint).expect("serialize endpoint");
        let decoded: RuntimeEndpointRecord =
            serde_json::from_str(&payload).expect("deserialize endpoint");

        assert_eq!(decoded.ws_addr, endpoint.ws_addr);
        assert_eq!(decoded.control_addr, endpoint.control_addr);
        assert_eq!(decoded.pid, endpoint.pid);
    }
}
