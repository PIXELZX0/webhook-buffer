//! 엔드포인트, 목적지, 받은 웹훅, 목적지별 전달 상태를 SQLite에 보관한다.
//! 받은 웹훅은 전달하기 전에 먼저 디스크에 저장하므로 프로세스가 죽어도 잃어버리지 않는다.

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;

/// 목적지로 보낼 웹훅 한 건
#[derive(Debug, Clone)]
pub struct Webhook {
    pub method: String,
    /// `/h/{slug}` 뒤에 붙어 온 경로와 쿼리 (예: `/push?x=1`, 없으면 빈 문자열)
    pub path_and_query: String,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Bytes,
}

#[derive(Debug, Clone, Serialize)]
pub struct Endpoint {
    pub id: i64,
    pub name: String,
    pub slug: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Destination {
    pub id: i64,
    pub endpoint_id: i64,
    pub url: String,
    pub enabled: bool,
    /// 에러 응답을 받았을 때 다시 보낼 최대 횟수. `None`이면 성공할 때까지 계속 보낸다.
    pub max_retries: Option<u32>,
    /// 목적지가 응답하지 않기 시작한 시각. 이 동안은 보내지 않고 쌓아 두기만 한다.
    pub down_since: Option<i64>,
    /// 목적지 서버가 "다시 살아났다"고 알릴 때 쓰는 토큰
    pub recover_token: String,
    pub created_at: i64,
}

#[derive(Debug, Serialize)]
pub struct EndpointSummary {
    #[serde(flatten)]
    pub endpoint: Endpoint,
    pub destinations: Vec<Destination>,
    pub event_count: i64,
    pub pending_count: i64,
    pub last_received_at: Option<i64>,
}

/// 워커가 처리할 전달 한 건
#[derive(Debug, Clone)]
pub struct Job {
    pub delivery_id: i64,
    pub destination_id: i64,
    pub attempts: u32,
    /// 목적지가 에러로 응답한 횟수 (응답이 아예 없었던 시도는 세지 않는다)
    pub error_responses: u32,
    pub max_retries: Option<u32>,
    pub url: String,
    pub webhook: Webhook,
}

#[derive(Debug, Serialize)]
pub struct EventSummary {
    pub id: i64,
    pub endpoint_id: i64,
    pub endpoint_name: String,
    pub method: String,
    pub path: String,
    pub received_at: i64,
    pub body_size: i64,
    pub delivered: i64,
    pub pending: i64,
    pub failed: i64,
}

#[derive(Debug, Serialize)]
pub struct DeliveryInfo {
    pub id: i64,
    pub event_id: i64,
    pub endpoint_name: String,
    pub destination_id: i64,
    pub url: String,
    /// `pending`(보내는 중/재시도 대기), `delivered`(성공), `failed`(재시도 횟수 초과)
    pub state: String,
    pub attempts: u32,
    pub last_status: Option<u16>,
    pub last_error: Option<String>,
    pub next_attempt_at: Option<i64>,
    pub delivered_at: Option<i64>,
    pub created_at: i64,
    /// 목적지가 응답 없음 상태라서 복구 알림을 기다리는 중인지
    pub destination_down: bool,
}

#[derive(Debug, Serialize)]
pub struct EventDetail {
    #[serde(flatten)]
    pub summary: EventSummary,
    pub headers: Vec<(String, String)>,
    pub body: String,
    pub deliveries: Vec<DeliveryInfo>,
}

#[derive(Debug, Serialize)]
pub struct Stats {
    pub endpoints: i64,
    pub destinations: i64,
    /// 응답 없음 상태인 목적지 수
    pub down_destinations: i64,
    pub events_24h: i64,
    pub delivered_24h: i64,
    pub pending: i64,
    /// 에러 응답을 받아 재시도를 기다리는 전달 수
    pub retrying: i64,
    /// 재시도 횟수를 다 써서 멈춘 전달 수
    pub failed: i64,
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
             PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS endpoints (
                 id         INTEGER PRIMARY KEY AUTOINCREMENT,
                 name       TEXT    NOT NULL,
                 slug       TEXT    NOT NULL UNIQUE,
                 created_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS destinations (
                 id          INTEGER PRIMARY KEY AUTOINCREMENT,
                 endpoint_id INTEGER NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
                 url         TEXT    NOT NULL,
                 enabled     INTEGER NOT NULL DEFAULT 1,
                 created_at  INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS events (
                 id             INTEGER PRIMARY KEY AUTOINCREMENT,
                 endpoint_id    INTEGER NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
                 method         TEXT    NOT NULL,
                 path_and_query TEXT    NOT NULL,
                 headers        BLOB    NOT NULL,
                 body           BLOB    NOT NULL,
                 received_at    INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS deliveries (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 event_id        INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
                 destination_id  INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
                 state           TEXT    NOT NULL DEFAULT 'pending',
                 attempts        INTEGER NOT NULL DEFAULT 0,
                 next_attempt_at INTEGER,
                 last_status     INTEGER,
                 last_error      TEXT,
                 delivered_at    INTEGER,
                 created_at      INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_destinations_endpoint ON destinations (endpoint_id);
             CREATE INDEX IF NOT EXISTS idx_events_endpoint ON events (endpoint_id, id);
             CREATE INDEX IF NOT EXISTS idx_deliveries_event ON deliveries (event_id);
             CREATE INDEX IF NOT EXISTS idx_deliveries_destination ON deliveries (destination_id);
             CREATE INDEX IF NOT EXISTS idx_deliveries_due ON deliveries (state, next_attempt_at);",
        )?;
        migrate(&conn)?;
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

    // ---------- 엔드포인트 / 목적지 ----------

    pub async fn create_endpoint(&self, name: String, slug: String) -> Result<Endpoint> {
        self.with_conn(move |c| {
            let created_at = now_ms();
            c.execute(
                "INSERT INTO endpoints (name, slug, created_at) VALUES (?1, ?2, ?3)",
                params![name, slug, created_at],
            )?;
            Ok(Endpoint {
                id: c.last_insert_rowid(),
                name,
                slug,
                created_at,
            })
        })
        .await
    }

    pub async fn slug_exists(&self, slug: String) -> Result<bool> {
        self.with_conn(move |c| {
            c.query_row(
                "SELECT 1 FROM endpoints WHERE slug = ?1",
                [slug],
                |_| Ok(()),
            )
            .optional()
            .map(|r| r.is_some())
        })
        .await
    }

    pub async fn delete_endpoint(&self, id: i64) -> Result<bool> {
        self.with_conn(move |c| {
            c.execute("DELETE FROM endpoints WHERE id = ?1", [id])
                .map(|n| n > 0)
        })
        .await
    }

    pub async fn list_endpoints(&self) -> Result<Vec<EndpointSummary>> {
        self.with_conn(|c| {
            let mut endpoints = c
                .prepare(
                    "SELECT ep.id, ep.name, ep.slug, ep.created_at,
                        (SELECT COUNT(*) FROM events WHERE endpoint_id = ep.id),
                        (SELECT COUNT(*) FROM deliveries d JOIN events e ON e.id = d.event_id
                          WHERE e.endpoint_id = ep.id AND d.state != 'delivered'),
                        (SELECT MAX(received_at) FROM events WHERE endpoint_id = ep.id)
                     FROM endpoints ep ORDER BY ep.id",
                )?
                .query_map([], |r| {
                    Ok(EndpointSummary {
                        endpoint: Endpoint {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            slug: r.get(2)?,
                            created_at: r.get(3)?,
                        },
                        destinations: Vec::new(),
                        event_count: r.get(4)?,
                        pending_count: r.get(5)?,
                        last_received_at: r.get(6)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            let destinations = c
                .prepare(&format!("{DESTINATION_SELECT} ORDER BY id"))?
                .query_map([], destination_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for d in destinations {
                if let Some(ep) = endpoints
                    .iter_mut()
                    .find(|e| e.endpoint.id == d.endpoint_id)
                {
                    ep.destinations.push(d);
                }
            }
            Ok(endpoints)
        })
        .await
    }

    /// 슬러그로 엔드포인트와 켜져 있는 목적지들을 찾는다.
    pub async fn endpoint_by_slug(&self, slug: String) -> Result<Option<(Endpoint, Vec<i64>)>> {
        self.with_conn(move |c| {
            let Some(endpoint) = c
                .query_row(
                    "SELECT id, name, slug, created_at FROM endpoints WHERE slug = ?1",
                    [slug],
                    |r| {
                        Ok(Endpoint {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            slug: r.get(2)?,
                            created_at: r.get(3)?,
                        })
                    },
                )
                .optional()?
            else {
                return Ok(None);
            };
            let destination_ids = c
                .prepare("SELECT id FROM destinations WHERE endpoint_id = ?1 AND enabled = 1")?
                .query_map([endpoint.id], |r| r.get(0))?
                .collect::<rusqlite::Result<Vec<i64>>>()?;
            Ok(Some((endpoint, destination_ids)))
        })
        .await
    }

    /// 목적지를 추가한다. 엔드포인트가 없으면 `None`.
    pub async fn add_destination(
        &self,
        endpoint_id: i64,
        url: String,
        max_retries: Option<u32>,
        recover_token: String,
    ) -> Result<Option<Destination>> {
        self.with_conn(move |c| {
            let exists = c
                .query_row(
                    "SELECT 1 FROM endpoints WHERE id = ?1",
                    [endpoint_id],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if !exists {
                return Ok(None);
            }
            let created_at = now_ms();
            c.execute(
                "INSERT INTO destinations (endpoint_id, url, max_retries, recover_token, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![endpoint_id, url, max_retries, recover_token, created_at],
            )?;
            Ok(Some(Destination {
                id: c.last_insert_rowid(),
                endpoint_id,
                url,
                enabled: true,
                max_retries,
                down_since: None,
                recover_token,
                created_at,
            }))
        })
        .await
    }

    pub async fn set_destination_enabled(&self, id: i64, enabled: bool) -> Result<bool> {
        self.with_conn(move |c| {
            c.execute(
                "UPDATE destinations SET enabled = ?2 WHERE id = ?1",
                params![id, enabled],
            )
            .map(|n| n > 0)
        })
        .await
    }

    pub async fn set_max_retries(&self, id: i64, max_retries: Option<u32>) -> Result<bool> {
        self.with_conn(move |c| {
            c.execute(
                "UPDATE destinations SET max_retries = ?2 WHERE id = ?1",
                params![id, max_retries],
            )
            .map(|n| n > 0)
        })
        .await
    }

    pub async fn delete_destination(&self, id: i64) -> Result<bool> {
        self.with_conn(move |c| {
            c.execute("DELETE FROM destinations WHERE id = ?1", [id])
                .map(|n| n > 0)
        })
        .await
    }

    /// 목적지가 다시 살아났다고 표시하고, 쌓아 둔 전달을 바로 보내도록 한다.
    /// 목적지가 없으면 `None`, 있으면 바로 보낼 전달 수를 돌려준다.
    pub async fn mark_destination_alive(&self, id: i64) -> Result<Option<usize>> {
        self.with_conn(move |c| set_alive(c, id)).await
    }

    /// 복구 토큰으로 목적지를 찾아 살아났다고 표시한다. 돌려주는 값은 `(목적지 id, 보낼 전달 수)`.
    pub async fn mark_alive_by_token(&self, token: String) -> Result<Option<(i64, usize)>> {
        self.with_conn(move |c| {
            let Some(id) = c
                .query_row(
                    "SELECT id FROM destinations WHERE recover_token = ?1",
                    [token],
                    |r| r.get::<_, i64>(0),
                )
                .optional()?
            else {
                return Ok(None);
            };
            Ok(set_alive(c, id)?.map(|n| (id, n)))
        })
        .await
    }

    // ---------- 웹훅 수신 / 전달 ----------

    /// 받은 웹훅을 저장하고, 목적지마다 바로 보낼 전달 작업을 만든다.
    pub async fn insert_event(
        &self,
        endpoint_id: i64,
        webhook: Webhook,
        destination_ids: Vec<i64>,
    ) -> Result<i64> {
        self.with_conn(move |c| {
            let tx = c.unchecked_transaction()?;
            let now = now_ms();
            tx.execute(
                "INSERT INTO events (endpoint_id, method, path_and_query, headers, body, received_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    endpoint_id,
                    webhook.method,
                    webhook.path_and_query,
                    encode_headers(&webhook.headers),
                    webhook.body.as_ref(),
                    now,
                ],
            )?;
            let event_id = tx.last_insert_rowid();
            insert_deliveries(&tx, event_id, &destination_ids, now)?;
            tx.commit()?;
            Ok(event_id)
        })
        .await
    }

    /// 이미 받은 웹훅을 현재 켜져 있는 목적지 전체로 다시 보낸다. 웹훅이 없으면 `None`.
    pub async fn replay_event(&self, event_id: i64) -> Result<Option<usize>> {
        self.with_conn(move |c| {
            let Some(endpoint_id) = c
                .query_row(
                    "SELECT endpoint_id FROM events WHERE id = ?1",
                    [event_id],
                    |r| r.get::<_, i64>(0),
                )
                .optional()?
            else {
                return Ok(None);
            };
            let destination_ids = c
                .prepare("SELECT id FROM destinations WHERE endpoint_id = ?1 AND enabled = 1")?
                .query_map([endpoint_id], |r| r.get(0))?
                .collect::<rusqlite::Result<Vec<i64>>>()?;
            let tx = c.unchecked_transaction()?;
            insert_deliveries(&tx, event_id, &destination_ids, now_ms())?;
            tx.commit()?;
            Ok(Some(destination_ids.len()))
        })
        .await
    }

    /// 보낼 시각이 된 전달을 가져오면서 `lease_until`까지 다른 쪽에서 다시 집어가지 않도록 잡아 둔다.
    /// 꺼져 있거나 응답 없음 상태인 목적지로 가는 전달은 가져오지 않는다.
    pub async fn claim_due(&self, now: i64, limit: usize, lease_until: i64) -> Result<Vec<Job>> {
        self.with_conn(move |c| {
            let tx = c.unchecked_transaction()?;
            let jobs = tx
                .prepare(
                    "SELECT d.id, d.destination_id, d.attempts, d.error_responses, dst.max_retries,
                            dst.url, e.method, e.path_and_query, e.headers, e.body
                     FROM deliveries d
                     JOIN destinations dst ON dst.id = d.destination_id
                     JOIN events e ON e.id = d.event_id
                     WHERE d.state = 'pending' AND d.next_attempt_at <= ?1
                       AND dst.enabled = 1 AND dst.down_since IS NULL
                     ORDER BY d.next_attempt_at, d.id
                     LIMIT ?2",
                )?
                .query_map(params![now, limit as i64], |r| {
                    Ok(Job {
                        delivery_id: r.get(0)?,
                        destination_id: r.get(1)?,
                        attempts: r.get(2)?,
                        error_responses: r.get(3)?,
                        max_retries: r.get(4)?,
                        url: r.get(5)?,
                        webhook: Webhook {
                            method: r.get(6)?,
                            path_and_query: r.get(7)?,
                            headers: decode_headers(&r.get::<_, Vec<u8>>(8)?),
                            body: Bytes::from(r.get::<_, Vec<u8>>(9)?),
                        },
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            {
                let mut lease =
                    tx.prepare("UPDATE deliveries SET next_attempt_at = ?2 WHERE id = ?1")?;
                for job in &jobs {
                    lease.execute(params![job.delivery_id, lease_until])?;
                }
            }
            tx.commit()?;
            Ok(jobs)
        })
        .await
    }

    /// 다음으로 보낼 전달의 예정 시각 (보낼 것이 없으면 `None`)
    pub async fn next_due(&self) -> Result<Option<i64>> {
        self.with_conn(|c| {
            c.query_row(
                "SELECT MIN(d.next_attempt_at) FROM deliveries d
                 JOIN destinations dst ON dst.id = d.destination_id
                 WHERE d.state = 'pending' AND dst.enabled = 1 AND dst.down_since IS NULL",
                [],
                |r| r.get(0),
            )
        })
        .await
    }

    pub async fn mark_delivered(&self, id: i64, attempts: u32, status: u16) -> Result<()> {
        self.with_conn(move |c| {
            c.execute(
                "UPDATE deliveries
                 SET state = 'delivered', attempts = ?2, last_status = ?3, last_error = NULL,
                     next_attempt_at = NULL, delivered_at = ?4
                 WHERE id = ?1",
                params![id, attempts, status, now_ms()],
            )
            .map(drop)
        })
        .await
    }

    /// 목적지가 에러로 응답했다. `next_attempt_at`이 `None`이면 재시도 횟수를 다 쓴 것이므로 멈춘다.
    pub async fn mark_error_response(
        &self,
        id: i64,
        attempts: u32,
        error_responses: u32,
        next_attempt_at: Option<i64>,
        status: Option<u16>,
        error: String,
    ) -> Result<()> {
        self.with_conn(move |c| {
            c.execute(
                "UPDATE deliveries
                 SET state = CASE WHEN ?4 IS NULL THEN 'failed' ELSE 'pending' END,
                     attempts = ?2, error_responses = ?3, next_attempt_at = ?4,
                     last_status = ?5, last_error = ?6
                 WHERE id = ?1 AND state = 'pending'",
                params![
                    id,
                    attempts,
                    error_responses,
                    next_attempt_at,
                    status,
                    error
                ],
            )
            .map(drop)
        })
        .await
    }

    /// 목적지가 아예 응답하지 않았다. 목적지를 응답 없음 상태로 바꾸고, 전달은 복구 알림이 올 때까지 쌓아 둔다.
    pub async fn mark_unreachable(
        &self,
        id: i64,
        destination_id: i64,
        attempts: u32,
        error: String,
    ) -> Result<()> {
        self.with_conn(move |c| {
            let tx = c.unchecked_transaction()?;
            let now = now_ms();
            tx.execute(
                "UPDATE deliveries
                 SET attempts = ?2, next_attempt_at = ?3, last_status = NULL, last_error = ?4
                 WHERE id = ?1 AND state = 'pending'",
                params![id, attempts, now, error],
            )?;
            tx.execute(
                "UPDATE destinations SET down_since = COALESCE(down_since, ?2) WHERE id = ?1",
                params![destination_id, now],
            )?;
            tx.commit()
        })
        .await
    }

    /// 대기 중이거나 멈춘 전달을 지금 바로 다시 보내도록 한다.
    pub async fn retry_now(&self, id: i64) -> Result<bool> {
        self.with_conn(move |c| {
            c.execute(
                "UPDATE deliveries SET state = 'pending', next_attempt_at = ?2
                 WHERE id = ?1 AND state IN ('pending', 'failed')",
                params![id, now_ms()],
            )
            .map(|n| n > 0)
        })
        .await
    }

    /// 아직 전달하지 못한 건을 포기하고 지운다.
    pub async fn delete_delivery(&self, id: i64) -> Result<bool> {
        self.with_conn(move |c| {
            c.execute(
                "DELETE FROM deliveries WHERE id = ?1 AND state IN ('pending', 'failed')",
                [id],
            )
            .map(|n| n > 0)
        })
        .await
    }

    // ---------- 패널용 조회 ----------

    pub async fn list_events(
        &self,
        endpoint_id: Option<i64>,
        before: Option<i64>,
        limit: usize,
    ) -> Result<Vec<EventSummary>> {
        self.with_conn(move |c| {
            c.prepare(&format!(
                "{EVENT_SUMMARY_SELECT}
                 WHERE (?1 IS NULL OR e.endpoint_id = ?1) AND (?2 IS NULL OR e.id < ?2)
                 ORDER BY e.id DESC LIMIT ?3"
            ))?
            .query_map(
                params![endpoint_id, before, limit as i64],
                event_summary_from_row,
            )?
            .collect()
        })
        .await
    }

    pub async fn get_event(&self, id: i64) -> Result<Option<EventDetail>> {
        self.with_conn(move |c| {
            let Some((summary, headers, body)) = c
                .query_row(
                    &format!("{EVENT_SUMMARY_SELECT} WHERE e.id = ?1"),
                    [id],
                    |r| {
                        Ok((
                            event_summary_from_row(r)?,
                            r.get::<_, Vec<u8>>(10)?,
                            r.get::<_, Vec<u8>>(11)?,
                        ))
                    },
                )
                .optional()?
            else {
                return Ok(None);
            };
            let deliveries = c
                .prepare(&format!(
                    "{DELIVERY_SELECT} WHERE d.event_id = ?1 ORDER BY d.id"
                ))?
                .query_map([id], delivery_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Some(EventDetail {
                summary,
                headers: decode_headers(&headers)
                    .into_iter()
                    .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
                    .collect(),
                body: String::from_utf8_lossy(&body).into_owned(),
                deliveries,
            }))
        })
        .await
    }

    /// 아직 전달하지 못한 건 (대기 중 + 재시도 횟수를 다 써서 멈춘 것)
    pub async fn list_undelivered(&self, limit: usize) -> Result<Vec<DeliveryInfo>> {
        self.with_conn(move |c| {
            c.prepare(&format!(
                "{DELIVERY_SELECT} WHERE d.state IN ('pending', 'failed')
                 ORDER BY d.state DESC, d.id LIMIT ?1"
            ))?
            .query_map([limit as i64], delivery_from_row)?
            .collect()
        })
        .await
    }

    pub async fn stats(&self) -> Result<Stats> {
        self.with_conn(|c| {
            let since = now_ms() - 24 * 60 * 60 * 1000;
            c.query_row(
                "SELECT
                    (SELECT COUNT(*) FROM endpoints),
                    (SELECT COUNT(*) FROM destinations),
                    (SELECT COUNT(*) FROM destinations WHERE down_since IS NOT NULL),
                    (SELECT COUNT(*) FROM events WHERE received_at >= ?1),
                    (SELECT COUNT(*) FROM deliveries WHERE state = 'delivered' AND delivered_at >= ?1),
                    (SELECT COUNT(*) FROM deliveries WHERE state = 'pending'),
                    (SELECT COUNT(*) FROM deliveries WHERE state = 'pending' AND error_responses > 0),
                    (SELECT COUNT(*) FROM deliveries WHERE state = 'failed')",
                [since],
                |r| {
                    Ok(Stats {
                        endpoints: r.get(0)?,
                        destinations: r.get(1)?,
                        down_destinations: r.get(2)?,
                        events_24h: r.get(3)?,
                        delivered_24h: r.get(4)?,
                        pending: r.get(5)?,
                        retrying: r.get(6)?,
                        failed: r.get(7)?,
                    })
                },
            )
        })
        .await
    }

    /// `cutoff`보다 오래됐고 모든 목적지에 전달이 끝난 웹훅 기록을 지운다.
    pub async fn prune(&self, cutoff: i64) -> Result<usize> {
        self.with_conn(move |c| {
            c.execute(
                "DELETE FROM events WHERE received_at < ?1
                 AND NOT EXISTS (SELECT 1 FROM deliveries d
                                 WHERE d.event_id = events.id AND d.state != 'delivered')",
                [cutoff],
            )
        })
        .await
    }
}

/// 처음 만든 스키마 뒤로 추가된 컬럼들을 붙인다. `user_version`으로 어디까지 했는지 기록한다.
fn migrate(c: &Connection) -> rusqlite::Result<()> {
    let version: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version < 1 {
        let tx = c.unchecked_transaction()?;
        tx.execute_batch(
            "ALTER TABLE destinations ADD COLUMN max_retries INTEGER;
             ALTER TABLE destinations ADD COLUMN down_since INTEGER;
             ALTER TABLE destinations ADD COLUMN recover_token TEXT;
             ALTER TABLE deliveries ADD COLUMN error_responses INTEGER NOT NULL DEFAULT 0;",
        )?;
        // 이미 있던 목적지에도 복구 토큰을 만들어 준다.
        let ids = tx
            .prepare("SELECT id FROM destinations")?
            .query_map([], |r| r.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for id in ids {
            tx.execute(
                "UPDATE destinations SET recover_token = ?2 WHERE id = ?1",
                params![id, crate::random_hex(16)],
            )?;
        }
        tx.execute_batch(
            "CREATE UNIQUE INDEX idx_destinations_recover_token ON destinations (recover_token);
             PRAGMA user_version = 1;",
        )?;
        tx.commit()?;
    }
    Ok(())
}

fn set_alive(c: &Connection, id: i64) -> rusqlite::Result<Option<usize>> {
    let tx = c.unchecked_transaction()?;
    if tx.execute(
        "UPDATE destinations SET down_since = NULL WHERE id = ?1",
        [id],
    )? == 0
    {
        return Ok(None);
    }
    let queued = tx.execute(
        "UPDATE deliveries SET next_attempt_at = ?2 WHERE destination_id = ?1 AND state = 'pending'",
        params![id, now_ms()],
    )?;
    tx.commit()?;
    Ok(Some(queued))
}

const DESTINATION_SELECT: &str = "SELECT id, endpoint_id, url, enabled, max_retries, down_since,
        recover_token, created_at
     FROM destinations";

const EVENT_SUMMARY_SELECT: &str =
    "SELECT e.id, e.endpoint_id, ep.name, e.method, e.path_and_query,
        e.received_at, length(e.body),
        (SELECT COUNT(*) FROM deliveries WHERE event_id = e.id AND state = 'delivered'),
        (SELECT COUNT(*) FROM deliveries WHERE event_id = e.id AND state = 'pending'),
        (SELECT COUNT(*) FROM deliveries WHERE event_id = e.id AND state = 'failed'),
        e.headers, e.body
     FROM events e JOIN endpoints ep ON ep.id = e.endpoint_id";

const DELIVERY_SELECT: &str =
    "SELECT d.id, d.event_id, ep.name, d.destination_id, dst.url, d.state,
        d.attempts, d.last_status, d.last_error, d.next_attempt_at, d.delivered_at, d.created_at,
        dst.down_since IS NOT NULL
     FROM deliveries d
     JOIN destinations dst ON dst.id = d.destination_id
     JOIN events e ON e.id = d.event_id
     JOIN endpoints ep ON ep.id = e.endpoint_id";

fn insert_deliveries(
    c: &Connection,
    event_id: i64,
    destination_ids: &[i64],
    now: i64,
) -> rusqlite::Result<()> {
    let mut stmt = c.prepare(
        "INSERT INTO deliveries (event_id, destination_id, next_attempt_at, created_at)
         VALUES (?1, ?2, ?3, ?3)",
    )?;
    for id in destination_ids {
        stmt.execute(params![event_id, id, now])?;
    }
    Ok(())
}

fn destination_from_row(r: &Row) -> rusqlite::Result<Destination> {
    Ok(Destination {
        id: r.get(0)?,
        endpoint_id: r.get(1)?,
        url: r.get(2)?,
        enabled: r.get(3)?,
        max_retries: r.get(4)?,
        down_since: r.get(5)?,
        recover_token: r.get(6)?,
        created_at: r.get(7)?,
    })
}

fn event_summary_from_row(r: &Row) -> rusqlite::Result<EventSummary> {
    Ok(EventSummary {
        id: r.get(0)?,
        endpoint_id: r.get(1)?,
        endpoint_name: r.get(2)?,
        method: r.get(3)?,
        path: r.get(4)?,
        received_at: r.get(5)?,
        body_size: r.get(6)?,
        delivered: r.get(7)?,
        pending: r.get(8)?,
        failed: r.get(9)?,
    })
}

fn delivery_from_row(r: &Row) -> rusqlite::Result<DeliveryInfo> {
    Ok(DeliveryInfo {
        id: r.get(0)?,
        event_id: r.get(1)?,
        endpoint_name: r.get(2)?,
        destination_id: r.get(3)?,
        url: r.get(4)?,
        state: r.get(5)?,
        attempts: r.get(6)?,
        last_status: r.get(7)?,
        last_error: r.get(8)?,
        next_attempt_at: r.get(9)?,
        delivered_at: r.get(10)?,
        created_at: r.get(11)?,
        destination_down: r.get(12)?,
    })
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

    fn webhook() -> Webhook {
        Webhook {
            method: "POST".into(),
            path_and_query: String::new(),
            headers: vec![],
            body: Bytes::from_static(b"x"),
        }
    }

    async fn destination(store: &Store, endpoint_id: i64, url: &str) -> Destination {
        store
            .add_destination(endpoint_id, url.into(), None, crate::random_hex(16))
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn claim_leases_jobs_and_skips_disabled_destinations() {
        let store = Store::open_in_memory().unwrap();
        let ep = store.create_endpoint("a".into(), "a".into()).await.unwrap();
        let d1 = destination(&store, ep.id, "http://one").await;
        let d2 = destination(&store, ep.id, "http://two").await;
        store
            .insert_event(ep.id, webhook(), vec![d1.id, d2.id])
            .await
            .unwrap();
        store.set_destination_enabled(d2.id, false).await.unwrap();

        let now = now_ms();
        let jobs = store.claim_due(now, 10, now + 60_000).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].url, "http://one");
        // 잡아 둔 작업은 다시 나오지 않는다
        assert!(
            store
                .claim_due(now, 10, now + 60_000)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn unreachable_destination_holds_deliveries_until_alive() {
        let store = Store::open_in_memory().unwrap();
        let ep = store.create_endpoint("a".into(), "a".into()).await.unwrap();
        let d = destination(&store, ep.id, "http://one").await;
        store
            .insert_event(ep.id, webhook(), vec![d.id])
            .await
            .unwrap();

        let now = now_ms();
        let job = store
            .claim_due(now, 10, now + 60_000)
            .await
            .unwrap()
            .remove(0);
        store
            .mark_unreachable(job.delivery_id, d.id, 1, "connect refused".into())
            .await
            .unwrap();
        // 응답 없음 상태에서는 새로 들어온 것도 보내지 않는다
        store
            .insert_event(ep.id, webhook(), vec![d.id])
            .await
            .unwrap();
        assert!(
            store
                .claim_due(now_ms(), 10, now + 60_000)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.next_due().await.unwrap(), None);
        assert_eq!(store.stats().await.unwrap().down_destinations, 1);

        assert_eq!(
            store
                .mark_alive_by_token(d.recover_token.clone())
                .await
                .unwrap(),
            Some((d.id, 2))
        );
        assert_eq!(
            store
                .claim_due(now_ms(), 10, now + 60_000)
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            store.mark_alive_by_token("nope".into()).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn error_response_without_next_attempt_marks_failed() {
        let store = Store::open_in_memory().unwrap();
        let ep = store.create_endpoint("a".into(), "a".into()).await.unwrap();
        let d = destination(&store, ep.id, "http://one").await;
        let event = store
            .insert_event(ep.id, webhook(), vec![d.id])
            .await
            .unwrap();
        let now = now_ms();
        let job = store
            .claim_due(now, 10, now + 60_000)
            .await
            .unwrap()
            .remove(0);
        store
            .mark_error_response(job.delivery_id, 1, 1, None, Some(500), "500".into())
            .await
            .unwrap();

        let stats = store.stats().await.unwrap();
        assert_eq!((stats.pending, stats.failed), (0, 1));
        // 다 보내지 못한 웹훅은 보관 기간이 지나도 지우지 않는다
        assert_eq!(store.prune(now_ms() + 1).await.unwrap(), 0);

        assert!(store.retry_now(job.delivery_id).await.unwrap());
        assert_eq!(
            store
                .get_event(event)
                .await
                .unwrap()
                .unwrap()
                .summary
                .pending,
            1
        );
    }

    #[tokio::test]
    async fn prune_keeps_events_with_pending_deliveries() {
        let store = Store::open_in_memory().unwrap();
        let ep = store.create_endpoint("a".into(), "a".into()).await.unwrap();
        let d = destination(&store, ep.id, "http://one").await;
        let pending = store
            .insert_event(ep.id, webhook(), vec![d.id])
            .await
            .unwrap();
        let done = store.insert_event(ep.id, webhook(), vec![]).await.unwrap();

        assert_eq!(store.prune(now_ms() + 1).await.unwrap(), 1);
        assert!(store.get_event(pending).await.unwrap().is_some());
        assert!(store.get_event(done).await.unwrap().is_none());
    }

    #[test]
    fn migrates_v0_database() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE endpoints (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                 slug TEXT NOT NULL UNIQUE, created_at INTEGER NOT NULL);
             CREATE TABLE destinations (id INTEGER PRIMARY KEY AUTOINCREMENT, endpoint_id INTEGER NOT NULL,
                 url TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL);
             INSERT INTO endpoints VALUES (1, 'a', 'a', 0);
             INSERT INTO destinations (endpoint_id, url, created_at) VALUES (1, 'http://x', 0);",
        )
        .unwrap();
        let store = Store::init(c).unwrap();
        let conn = store.conn.lock().unwrap();
        let (token, max): (String, Option<u32>) = conn
            .query_row(
                "SELECT recover_token, max_retries FROM destinations",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(token.len(), 32);
        assert_eq!(max, None);
    }
}
