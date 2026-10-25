//! 웹 패널이 쓰는 관리 API. 모든 요청에 `Authorization: Bearer {ADMIN_TOKEN}`이 필요하다.

use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, patch, post},
};
use serde::Deserialize;
use tracing::error;

use crate::{AppState, random_hex};

pub fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/stats", get(stats))
        .route("/endpoints", get(list_endpoints).post(create_endpoint))
        .route("/endpoints/{id}", axum::routing::delete(delete_endpoint))
        .route("/endpoints/{id}/destinations", post(add_destination))
        .route(
            "/destinations/{id}",
            patch(update_destination).delete(delete_destination),
        )
        .route("/events", get(list_events))
        .route("/events/{id}", get(get_event))
        .route("/events/{id}/replay", post(replay_event))
        .route("/deliveries", get(list_pending))
        .route("/deliveries/{id}", axum::routing::delete(delete_delivery))
        .route("/deliveries/{id}/retry", post(retry_delivery))
        .layer(middleware::from_fn_with_state(state, require_token))
}

async fn require_token(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let given = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default();
    if constant_time_eq(given.as_bytes(), state.admin_token.as_bytes()) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "invalid admin token").into_response()
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

type ApiResult = Result<Response, ApiError>;

struct ApiError(StatusCode, String);

impl ApiError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, msg.into())
    }
    fn not_found() -> Self {
        Self(StatusCode::NOT_FOUND, "not found".into())
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        error!("관리 API 오류: {e:#}");
        Self(StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

fn ok<T: serde::Serialize>(value: T) -> ApiResult {
    Ok(Json(value).into_response())
}

fn found(changed: bool) -> ApiResult {
    if changed {
        Ok(StatusCode::NO_CONTENT.into_response())
    } else {
        Err(ApiError::not_found())
    }
}

async fn stats(State(s): State<AppState>) -> ApiResult {
    ok(s.store.stats().await?)
}

async fn list_endpoints(State(s): State<AppState>) -> ApiResult {
    ok(s.store.list_endpoints().await?)
}

#[derive(Deserialize)]
struct NewEndpoint {
    name: String,
    slug: Option<String>,
}

async fn create_endpoint(State(s): State<AppState>, Json(body): Json<NewEndpoint>) -> ApiResult {
    let name = body.name.trim().to_string();
    if name.is_empty() {
        return Err(ApiError::bad_request("이름을 입력하세요"));
    }
    let slug = match body
        .slug
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        Some(slug) => {
            let valid = slug.len() <= 64
                && slug
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
            if !valid {
                return Err(ApiError::bad_request(
                    "슬러그는 영문, 숫자, -, _ 로 64자 이내여야 합니다",
                ));
            }
            slug
        }
        None => random_hex(12),
    };
    if s.store.slug_exists(slug.clone()).await? {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "이미 있는 슬러그입니다".into(),
        ));
    }
    let endpoint = s.store.create_endpoint(name, slug).await?;
    Ok((StatusCode::CREATED, Json(endpoint)).into_response())
}

async fn delete_endpoint(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    found(s.store.delete_endpoint(id).await?)
}

#[derive(Deserialize)]
struct NewDestination {
    url: String,
}

async fn add_destination(
    State(s): State<AppState>,
    Path(endpoint_id): Path<i64>,
    Json(body): Json<NewDestination>,
) -> ApiResult {
    let url = body.url.trim().to_string();
    match reqwest::Url::parse(&url) {
        Ok(u) if matches!(u.scheme(), "http" | "https") => {}
        _ => {
            return Err(ApiError::bad_request(
                "http:// 또는 https:// 로 시작하는 URL을 입력하세요",
            ));
        }
    }
    match s.store.add_destination(endpoint_id, url).await? {
        Some(d) => Ok((StatusCode::CREATED, Json(d)).into_response()),
        None => Err(ApiError::not_found()),
    }
}

#[derive(Deserialize)]
struct UpdateDestination {
    enabled: bool,
}

async fn update_destination(
    State(s): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<UpdateDestination>,
) -> ApiResult {
    let changed = s.store.set_destination_enabled(id, body.enabled).await?;
    if changed && body.enabled {
        // 꺼져 있는 동안 밀린 전달을 바로 보내도록 깨운다.
        s.wake_worker.notify_one();
    }
    found(changed)
}

async fn delete_destination(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    found(s.store.delete_destination(id).await?)
}

#[derive(Deserialize)]
struct EventQuery {
    endpoint_id: Option<i64>,
    before: Option<i64>,
    limit: Option<usize>,
}

async fn list_events(State(s): State<AppState>, Query(q): Query<EventQuery>) -> ApiResult {
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    ok(s.store.list_events(q.endpoint_id, q.before, limit).await?)
}

async fn get_event(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    match s.store.get_event(id).await? {
        Some(event) => ok(event),
        None => Err(ApiError::not_found()),
    }
}

async fn replay_event(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    match s.store.replay_event(id).await? {
        Some(n) => {
            s.wake_worker.notify_one();
            ok(serde_json::json!({ "destinations": n }))
        }
        None => Err(ApiError::not_found()),
    }
}

async fn list_pending(State(s): State<AppState>) -> ApiResult {
    ok(s.store.list_pending(200).await?)
}

async fn retry_delivery(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    let changed = s.store.retry_now(id).await?;
    if changed {
        s.wake_worker.notify_one();
    }
    found(changed)
}

async fn delete_delivery(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    found(s.store.delete_delivery(id).await?)
}
