pub mod config;
pub mod forwarder;
pub mod retry;
pub mod store;

use std::sync::Arc;

use anyhow::Result;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
};
use tokio::{net::TcpListener, sync::Notify};
use tracing::{error, info, warn};

use crate::{
    config::Config,
    forwarder::{Forwarder, should_forward_header},
    retry::Backoff,
    store::{Store, Webhook},
};

#[derive(Clone)]
struct AppState {
    store: Store,
    forwarder: Forwarder,
    backoff: Backoff,
    wake_retry: Arc<Notify>,
}

/// 서버와 재시도 작업을 띄운다. `shutdown`이 끝나면 서버도 종료된다.
pub async fn run(
    config: Config,
    store: Store,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let forwarder = Forwarder::new(&config.target_url, config.request_timeout)?;
    let backoff = Backoff {
        min: config.min_backoff,
        max: config.max_backoff,
    };
    let wake_retry = Arc::new(Notify::new());

    let retry_task = tokio::spawn(retry::run(
        store.clone(),
        forwarder.clone(),
        backoff,
        wake_retry.clone(),
    ));

    let state = AppState {
        store,
        forwarder,
        backoff,
        wake_retry,
    };
    let app = Router::new()
        .route("/_buffer/status", get(status))
        .fallback(receive)
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .with_state(state);

    info!(
        "{}에서 웹훅 수신 대기, 목적지: {}",
        listener.local_addr()?,
        config.target_url
    );
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await;
    retry_task.abort();
    Ok(result?)
}

async fn status(State(state): State<AppState>) -> Response {
    match state.store.pending_count().await {
        Ok(pending) => Json(serde_json::json!({ "pending": pending })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// 모든 메서드·경로의 요청을 웹훅으로 받아 목적지로 전달한다.
async fn receive(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let webhook = Webhook {
        method: method.to_string(),
        path_and_query: uri
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "/".into()),
        headers: headers
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.as_bytes().to_vec()))
            .collect(),
        body,
    };

    // 1. 먼저 바로 전달해 본다. 성공하면 목적지 응답을 그대로 돌려주고 끝.
    let err = match state.forwarder.send(&webhook).await {
        Ok(delivered) => {
            let mut resp = Response::builder().status(delivered.status.as_u16());
            for (name, value) in &delivered.headers {
                if should_forward_header(name.as_str()) {
                    resp = resp.header(name.as_str(), value.as_bytes());
                }
            }
            return resp
                .body(Body::from(delivered.body))
                .unwrap_or_else(|_| StatusCode::OK.into_response());
        }
        Err(err) => err,
    };

    // 2. 실패하면 저장해 두고 재시도 작업이 성공할 때까지 다시 보낸다.
    warn!(
        "{} {} 전달 실패, 저장 후 재시도합니다: {err}",
        webhook.method, webhook.path_and_query
    );
    match state
        .store
        .insert(webhook, 1, state.backoff.next_attempt_at(1), err)
        .await
    {
        Ok(id) => {
            state.wake_retry.notify_one();
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "status": "queued", "id": id })),
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
