use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub root_dir: PathBuf,
    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub ws_bind: SocketAddr,
    pub control_bind: SocketAddr,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let root_dir = env::var("AGENTTOOL_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| env::current_dir().expect("current_dir should exist"));

        let data_dir = env::var("AGENTTOOL_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| root_dir.join("data"));

        let db_path = env::var("AGENTTOOL_DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| data_dir.join("agenttool.db"));

        let ws_bind = env::var("AGENTTOOL_WS_BIND")
            .unwrap_or_else(|_| "127.0.0.1:7080".to_string())
            .parse()
            .context("failed to parse AGENTTOOL_WS_BIND")?;

        let control_bind = env::var("AGENTTOOL_CONTROL_BIND")
            .unwrap_or_else(|_| "127.0.0.1:7081".to_string())
            .parse()
            .context("failed to parse AGENTTOOL_CONTROL_BIND")?;

        Ok(Self {
            root_dir,
            data_dir,
            db_path,
            ws_bind,
            control_bind,
        })
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        ensure_dir(&self.root_dir)?;
        ensure_dir(&self.data_dir)?;
        Ok(())
    }
}

fn ensure_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("failed to create directory {}", path.display()))
}
