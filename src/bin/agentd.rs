use anyhow::Result;
use tracing_subscriber::EnvFilter;

use agenttool::config::AppConfig;
use agenttool::runtime::run_agentd;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("agenttool=info,agentd=info")),
        )
        .init();

    let config = AppConfig::load()?;
    run_agentd(config).await
}
