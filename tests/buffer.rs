use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
};
use tokio::net::TcpListener;
use webhook_buffer::{config::Config, store::Store};

/// (경로+쿼리, x-signature 헤더, 본문)
type Received = (String, Option<String>, Bytes);

/// 처음 `fail_times`번은 503을 돌려주고 그 뒤로는 받은 요청을 기록하는 가짜 목적지
#[derive(Clone, Default)]
struct Target {
    fail_times: usize,
    hits: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Received>>>,
}

async fn target_handler(
    State(t): State<Target>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, &'static str) {
    if t.hits.fetch_add(1, Ordering::SeqCst) < t.fail_times {
        return (StatusCode::SERVICE_UNAVAILABLE, "down");
    }
    let sig = headers
        .get("x-signature")
        .map(|v| v.to_str().unwrap().to_string());
    t.received
        .lock()
        .unwrap()
        .push((uri.to_string(), sig, body));
    (StatusCode::OK, "target-ok")
}

async fn spawn_target(fail_times: usize) -> (String, Target) {
    let target = Target {
        fail_times,
        ..Default::default()
    };
    let app = Router::new()
        .fallback(target_handler)
        .with_state(target.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, target)
}

async fn spawn_buffer(target_url: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Config {
        listen_addr: addr,
        target_url,
        db_path: ":memory:".into(),
        request_timeout: Duration::from_secs(2),
        min_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(200),
        max_body_bytes: 1024 * 1024,
    };
    let store = Store::open_in_memory().unwrap();
    tokio::spawn(webhook_buffer::run(
        config,
        store,
        listener,
        std::future::pending(),
    ));
    format!("http://{addr}")
}

async fn wait_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..100 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("시간 안에 조건을 만족하지 못했습니다");
}

#[tokio::test]
async fn delivers_directly_when_target_is_up() {
    let (target_url, target) = spawn_target(0).await;
    let buffer = spawn_buffer(target_url).await;

    let resp = reqwest::Client::new()
        .post(format!("{buffer}/hooks/github?a=1"))
        .header("x-signature", "sha256=abc")
        .body("payload")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "target-ok");
    let received = target.received.lock().unwrap().clone();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, "/hooks/github?a=1");
    assert_eq!(received[0].1.as_deref(), Some("sha256=abc"));
    assert_eq!(received[0].2, Bytes::from("payload"));
}

#[tokio::test]
async fn buffers_and_retries_until_success() {
    let (target_url, target) = spawn_target(3).await;
    let buffer = spawn_buffer(target_url).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{buffer}/hooks"))
        .header("x-signature", "sha256=xyz")
        .body("retry-me")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    wait_until(|| target.received.lock().unwrap().len() == 1).await;
    let received = target.received.lock().unwrap().clone();
    assert_eq!(received[0].0, "/hooks");
    assert_eq!(received[0].1.as_deref(), Some("sha256=xyz"));
    assert_eq!(received[0].2, Bytes::from("retry-me"));
    assert_eq!(target.hits.load(Ordering::SeqCst), 4);

    // 성공한 뒤에는 큐가 비어 있어야 한다
    let status: serde_json::Value = client
        .get(format!("{buffer}/_buffer/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["pending"], 0);
}

#[tokio::test]
async fn buffers_when_target_is_unreachable() {
    // 아무도 듣지 않는 포트
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_url = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);
    let buffer = spawn_buffer(dead_url).await;

    let resp = reqwest::Client::new()
        .post(format!("{buffer}/hooks"))
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
}
