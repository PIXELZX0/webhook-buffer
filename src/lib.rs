pub mod api;
pub mod config;
pub mod forwarder;
pub mod store;
pub mod worker;

use std::sync::Arc;

use anyhow::Result;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{Html, IntoResponse, Response},
    routing::{any, get},
};
use tokio::{net::TcpListener, sync::Notify};
use tracing::{error, info, warn};

use crate::{
    config::Config,
    forwarder::Forwarder,
    store::{Store, Webhook},
    worker::{Backoff, Worker},
};

/// 웹훅을 받는 경로 앞부분. `/h/{slug}` 또는 `/h/{slug}/하위/경로`
const INGEST_PREFIX: &str = "/h/";

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub wake_worker: Arc<Notify>,
    pub admin_token: Arc<str>,
}

/// 서버와 백그라운드 작업을 띄운다. `shutdown`이 끝나면 서버도 종료된다.
pub async fn run(
    config: Config,
    store: Store,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let admin_token = match config.admin_token.clone() {
        Some(t) => t,
        None => {
            let t = random_hex(16);
            warn!("ADMIN_TOKEN이 없어서 임시 토큰을 만들었습니다 (재시작하면 바뀝니다): {t}");
            t
        }
    };

    let wake_worker = Arc::new(Notify::new());
    let worker = Worker {
        store: store.clone(),
        forwarder: Forwarder::new(config.request_timeout)?,
        backoff: Backoff {
            min: config.min_backoff,
            max: config.max_backoff,
        },
        lease: config.request_timeout + std::time::Duration::from_secs(30),
        concurrency: config.max_concurrency,
        wake: wake_worker.clone(),
    };
    let worker_task = tokio::spawn(worker.run());
    let prune_task = tokio::spawn(worker::prune_loop(store.clone(), config.retention));

    let state = AppState {
        store,
        wake_worker,
        admin_token: admin_token.into(),
    };
    let app = Router::new()
        .route("/", get(panel))
        .route("/h/{slug}", any(ingest))
        .route("/h/{slug}/{*rest}", any(ingest))
        .route(
            "/d/{token}/alive",
            get(destination_alive).post(destination_alive),
        )
        .nest("/api", api::router(state.clone()))
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .with_state(state);

    info!(
        "{}에서 대기 중 (패널: /, 웹훅: /h/{{slug}}, 복구 알림: /d/{{token}}/alive)",
        listener.local_addr()?
    );
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await;
    worker_task.abort();
    prune_task.abort();
    Ok(result?)
}

async fn panel() -> Html<&'static str> {
    Html(include_str!("panel.html"))
}

/// 웹훅을 받아 디스크에 저장하고 목적지마다 전달 작업을 만든다.
/// 실제 전달은 워커가 바로 이어서 하고, 실패하면 성공할 때까지 다시 보낸다.
async fn ingest(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let rest = uri.path().strip_prefix(INGEST_PREFIX).unwrap_or_default();
    let (slug, sub_path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let path_and_query = match uri.query() {
        Some(q) => format!("{sub_path}?{q}"),
        None => sub_path.to_string(),
    };

    let (endpoint, destination_ids) = match state.store.endpoint_by_slug(slug.to_string()).await {
        Ok(Some(found)) => found,
        Ok(None) => return (StatusCode::NOT_FOUND, "unknown endpoint").into_response(),
        Err(e) => {
            error!("엔드포인트 조회 실패: {e:#}");
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };

    let webhook = Webhook {
        method: method.to_string(),
        path_and_query,
        headers: headers
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
            .collect(),
        body,
    };
    let destinations = destination_ids.len();

    match state
        .store
        .insert_event(endpoint.id, webhook, destination_ids)
        .await
    {
        Ok(event_id) => {
            state.wake_worker.notify_one();
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "event_id": event_id, "destinations": destinations })),
            )
                .into_response()
        }
        Err(e) => {
            // 저장조차 못 했으면 보낸 쪽이 다시 보낼 수 있도록 에러를 돌려준다.
            error!("웹훅 저장 실패: {e:#}");
            (StatusCode::SERVICE_UNAVAILABLE, "failed to buffer webhook").into_response()
        }
    }
}

/// 목적지 서버가 "다시 살아났다"고 알리는 곳. 목적지마다 받은 복구 토큰으로 호출한다.
/// 응답 없음 상태로 쌓아 둔 웹훅을 바로 보내기 시작한다.
async fn destination_alive(State(state): State<AppState>, Path(token): Path<String>) -> Response {
    match state.store.mark_alive_by_token(token).await {
        Ok(Some((id, queued))) => {
            info!(
                destination = id,
                queued, "목적지 복구 알림 받음, 쌓아 둔 웹훅을 보냅니다"
            );
            state.wake_worker.notify_one();
            Json(serde_json::json!({ "status": "ok", "queued": queued })).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "unknown token").into_response(),
        Err(e) => {
            error!("복구 알림 처리 실패: {e:#}");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

pub fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::fill(&mut buf[..]);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}
