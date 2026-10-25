use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    /// 웹훅과 패널을 받을 주소
    pub listen_addr: SocketAddr,
    /// SQLite 파일 경로
    pub db_path: PathBuf,
    /// 웹 패널과 관리 API 토큰. 비어 있으면 실행할 때 임의로 만들어 로그에 찍는다.
    pub admin_token: Option<String>,
    /// 목적지 요청 한 번의 타임아웃
    pub request_timeout: Duration,
    /// 재시도 간격의 시작값 (실패할 때마다 두 배씩 늘어남)
    pub min_backoff: Duration,
    /// 재시도 간격의 최댓값
    pub max_backoff: Duration,
    /// 받을 수 있는 최대 본문 크기
    pub max_body_bytes: usize,
    /// 동시에 보낼 수 있는 최대 요청 수
    pub max_concurrency: usize,
    /// 다 보낸 웹훅 기록을 보관하는 기간
    pub retention: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            listen_addr: parse_or("LISTEN_ADDR", "0.0.0.0:8080")?,
            db_path: env::var("DB_PATH")
                .unwrap_or_else(|_| "webhook-buffer.db".into())
                .into(),
            admin_token: env::var("ADMIN_TOKEN").ok().filter(|t| !t.is_empty()),
            request_timeout: Duration::from_secs(parse_or("REQUEST_TIMEOUT_SECS", "10")?),
            min_backoff: Duration::from_millis(parse_or("MIN_BACKOFF_MS", "1000")?),
            max_backoff: Duration::from_secs(parse_or("MAX_BACKOFF_SECS", "300")?),
            max_body_bytes: parse_or("MAX_BODY_BYTES", "10485760")?,
            max_concurrency: parse_or::<usize>("MAX_CONCURRENCY", "16")?.max(1),
            retention: Duration::from_secs(60 * 60 * 24 * parse_or::<u64>("RETENTION_DAYS", "7")?),
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
