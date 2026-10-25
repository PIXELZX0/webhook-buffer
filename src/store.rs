//! 전달에 실패한 웹훅을 SQLite에 보관한다.
//! 프로세스가 재시작돼도 아직 전달하지 못한 웹훅이 사라지지 않도록 디스크에 둔다.

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use rusqlite::{Connection, OptionalExtension, params};

/// 목적지로 보낼 웹훅 한 건
#[derive(Debug, Clone)]
pub struct Webhook {
    pub method: String,
    /// 경로 + 쿼리 (예: `/github/events?x=1`)
    pub path_and_query: String,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Bytes,
}

#[derive(Debug, Clone)]
pub struct StoredWebhook {
    pub id: i64,
    pub attempts: u32,
    pub webhook: Webhook,
}

#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("DB를 열 수 없습니다: {}", path.display()))?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS webhooks (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 method          TEXT    NOT NULL,
                 path_and_query  TEXT    NOT NULL,
                 headers         BLOB    NOT NULL,
                 body            BLOB    NOT NULL,
                 attempts        INTEGER NOT NULL DEFAULT 0,
                 next_attempt_at INTEGER NOT NULL,
                 last_error      TEXT,
                 created_at      INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_webhooks_next ON webhooks (next_attempt_at, id);",
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    async fn with_conn<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            f(&conn).map_err(Into::into)
        })
        .await?
    }

    /// 실패한 웹훅을 저장한다. `next_attempt_at`(unix ms) 이후에 재시도된다.
    pub async fn insert(
        &self,
        webhook: Webhook,
        attempts: u32,
        next_attempt_at: i64,
        error: String,
    ) -> Result<i64> {
        self.with_conn(move |c| {
            c.execute(
                "INSERT INTO webhooks
                    (method, path_and_query, headers, body, attempts, next_attempt_at, last_error, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    webhook.method,
                    webhook.path_and_query,
                    encode_headers(&webhook.headers),
                    webhook.body.as_ref(),
                    attempts,
                    next_attempt_at,
                    error,
                    now_ms(),
                ],
            )?;
            Ok(c.last_insert_rowid())
        })
        .await
    }

    /// 재시도 시각이 된 웹훅 중 가장 오래된 것부터 가져온다.
    pub async fn due(&self, now: i64, limit: usize) -> Result<Vec<StoredWebhook>> {
        self.with_conn(move |c| {
            let mut stmt = c.prepare(
                "SELECT id, attempts, method, path_and_query, headers, body
                 FROM webhooks
                 WHERE next_attempt_at <= ?1
                 ORDER BY next_attempt_at, id
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![now, limit as i64], |row| {
                Ok(StoredWebhook {
                    id: row.get(0)?,
                    attempts: row.get(1)?,
                    webhook: Webhook {
                        method: row.get(2)?,
                        path_and_query: row.get(3)?,
                        headers: decode_headers(&row.get::<_, Vec<u8>>(4)?),
                        body: Bytes::from(row.get::<_, Vec<u8>>(5)?),
                    },
                })
            })?;
            rows.collect()
        })
        .await
    }

    /// 다음 재시도 예정 시각 (저장된 웹훅이 없으면 `None`)
    pub async fn next_attempt_at(&self) -> Result<Option<i64>> {
        self.with_conn(|c| {
            c.query_row("SELECT MIN(next_attempt_at) FROM webhooks", [], |r| {
                r.get(0)
            })
            .optional()
            .map(Option::flatten)
        })
        .await
    }

    pub async fn delete(&self, id: i64) -> Result<()> {
        self.with_conn(move |c| {
            c.execute("DELETE FROM webhooks WHERE id = ?1", [id])
                .map(drop)
        })
        .await
    }

    pub async fn reschedule(
        &self,
        id: i64,
        attempts: u32,
        next_attempt_at: i64,
        error: String,
    ) -> Result<()> {
        self.with_conn(move |c| {
            c.execute(
                "UPDATE webhooks SET attempts = ?2, next_attempt_at = ?3, last_error = ?4 WHERE id = ?1",
                params![id, attempts, next_attempt_at, error],
            )
            .map(drop)
        })
        .await
    }

    pub async fn pending_count(&self) -> Result<i64> {
        self.with_conn(|c| c.query_row("SELECT COUNT(*) FROM webhooks", [], |r| r.get(0)))
            .await
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// 헤더 값에는 줄바꿈이 들어갈 수 없으므로 "name: value\n" 형태로 그대로 이어 붙여 저장한다.
fn encode_headers(headers: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, value) in headers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value);
        out.push(b'\n');
    }
    out
}

fn decode_headers(raw: &[u8]) -> Vec<(String, Vec<u8>)> {
    raw.split(|&b| b == b'\n')
        .filter_map(|line| {
            let pos = line.windows(2).position(|w| w == b": ")?;
            let name = String::from_utf8(line[..pos].to_vec()).ok()?;
            Some((name, line[pos + 2..].to_vec()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_roundtrip() {
        let headers = vec![
            ("content-type".to_string(), b"application/json".to_vec()),
            ("x-signature".to_string(), b"sha256=abc: def".to_vec()),
            ("x-empty".to_string(), Vec::new()),
        ];
        assert_eq!(decode_headers(&encode_headers(&headers)), headers);
    }
}
