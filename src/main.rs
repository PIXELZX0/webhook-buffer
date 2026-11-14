use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    process::ExitCode,
    time::Duration,
};

use anyhow::Result;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;
use webhook_buffer::{config::Config, store::Store};

/// 요청 처리 스레드 수. 웹훅 중계는 대부분 I/O 대기라서 CPU 코어 수만큼 띄울 필요가 없다.
const WORKER_THREADS: usize = 2;
/// DB 작업과 DNS 조회에 쓰는 블로킹 스레드 최대 수 (tokio 기본값은 512).
/// SQLite 연결 하나를 Mutex로 나눠 쓰므로 DB 쪽은 많이 띄워도 락에서 기다리기만 한다.
const BLOCKING_THREADS: usize = 8;

fn main() -> Result<ExitCode> {
    // `webhook-buffer healthcheck`: 떠 있는 서버의 /healthz를 찔러 보고 종료 코드로 알려 준다.
    // 이미지가 scratch라 curl이 없어서 Docker HEALTHCHECK가 이걸 쓴다.
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        return Ok(rt.block_on(healthcheck()));
    }

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .max_blocking_threads(BLOCKING_THREADS)
        .enable_all()
        .build()?
        .block_on(serve())
}

async fn serve() -> Result<ExitCode> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    let store = Store::open(&config.db_path)?;
    let listener = TcpListener::bind(config.listen_addr).await?;

    webhook_buffer::run(config, store, listener, shutdown_signal()).await?;
    Ok(ExitCode::SUCCESS)
}

async fn healthcheck() -> ExitCode {
    match check_health().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("health check failed: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn check_health() -> Result<()> {
    let addr = loopback(Config::from_env()?.listen_addr);
    let resp = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?
        .get(format!("http://{addr}/healthz"))
        .send()
        .await?;
    anyhow::ensure!(
        resp.status().is_success(),
        "unhealthy response: {}",
        resp.status()
    );
    Ok(())
}

/// 0.0.0.0이나 [::]으로 열어 둔 서버는 루프백으로 찌른다.
fn loopback(addr: SocketAddr) -> SocketAddr {
    let ip = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, addr.port())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received, stopping server");
}
