use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone)]
pub struct Config {
    /// 웹훅을 받을 주소
    pub listen_addr: SocketAddr,
    /// 웹훅을 전달할 목적지. 받은 요청의 경로와 쿼리가 뒤에 붙는다.
    pub target_url: String,
    /// 실패한 웹훅을 저장할 SQLite 파일 경로
    pub db_path: PathBuf,
    /// 목적지 요청 한 번의 타임아웃
    pub request_timeout: Duration,
    /// 재시도 간격의 시작값 (실패할 때마다 두 배씩 늘어남)
    pub min_backoff: Duration,
    /// 재시도 간격의 최댓값
    pub max_backoff: Duration,
    /// 받을 수 있는 최대 본문 크기
    pub max_body_bytes: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let target_url = env::var("TARGET_URL").context("TARGET_URL 환경 변수가 필요합니다")?;
        if !(target_url.starts_with("http://") || target_url.starts_with("https://")) {
            bail!("TARGET_URL은 http:// 또는 https://로 시작해야 합니다: {target_url}");
        }

        Ok(Self {
            listen_addr: parse_or("LISTEN_ADDR", "0.0.0.0:8080")?,
            target_url,
            db_path: env::var("DB_PATH")
                .unwrap_or_else(|_| "webhook-buffer.db".into())
                .into(),
            request_timeout: Duration::from_secs(parse_or("REQUEST_TIMEOUT_SECS", "10")?),
            min_backoff: Duration::from_millis(parse_or("MIN_BACKOFF_MS", "1000")?),
            max_backoff: Duration::from_secs(parse_or("MAX_BACKOFF_SECS", "300")?),
            max_body_bytes: parse_or("MAX_BODY_BYTES", "10485760")?,
        })
    }
}

fn parse_or<T>(key: &str, default: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse()
        .with_context(|| format!("{key} 값을 해석할 수 없습니다: {raw}"))
}
