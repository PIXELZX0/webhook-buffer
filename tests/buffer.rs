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
use serde_json::{Value, json};
use tokio::net::TcpListener;
use webhook_buffer::{config::Config, store::Store};

const TOKEN: &str = "test-token";

/// (경로+쿼리, x-signature 헤더, 본문)
type Received = (String, Option<String>, Bytes);

/// 처음 `fail_times`번은 503을 돌려주고 그 뒤로는 받은 요청을 기록하는 가짜 목적지
#[derive(Clone, Default)]
struct Target {
    fail_times: usize,
    hits: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Received>>>,
}

impl Target {
    fn received(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }
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
    spawn_target_on(TcpListener::bind("127.0.0.1:0").await.unwrap(), fail_times).await
}

async fn spawn_target_on(listener: TcpListener, fail_times: usize) -> (String, Target) {
    let target = Target {
        fail_times,
        ..Default::default()
    };
    let app = Router::new()
        .fallback(target_handler)
        .with_state(target.clone());
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, target)
}

struct Buffer {
    base: String,
    client: reqwest::Client,
}

impl Buffer {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = Config {
            listen_addr: addr,
            db_path: ":memory:".into(),
            admin_token: Some(TOKEN.into()),
            request_timeout: Duration::from_secs(2),
            min_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
            max_body_bytes: 1024 * 1024,
            max_concurrency: 4,
            retention: Duration::from_secs(3600),
        };
        let store = Store::open_in_memory().unwrap();
        tokio::spawn(webhook_buffer::run(
            config,
            store,
            listener,
            std::future::pending(),
        ));
        Self {
            base: format!("http://{addr}"),
            client: reqwest::Client::new(),
        }
    }

    async fn api(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Value {
        let mut req = self
            .client
            .request(method, format!("{}/api{path}", self.base))
            .bearer_auth(TOKEN);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let resp = req.send().await.unwrap();
        assert!(resp.status().is_success(), "{path}: {}", resp.status());
        resp.json().await.unwrap_or(Value::Null)
    }

    /// 엔드포인트를 만들고 목적지들을 기본 설정으로 붙인다.
    async fn endpoint(&self, slug: &str, destinations: &[&str]) -> Vec<Value> {
        let destinations: Vec<Value> = destinations.iter().map(|u| json!({ "url": u })).collect();
        self.endpoint_with(slug, &destinations).await
    }

    /// 엔드포인트를 만들고 목적지 설정(JSON)들을 붙인다. 만들어진 목적지들을 돌려준다.
    async fn endpoint_with(&self, slug: &str, destinations: &[Value]) -> Vec<Value> {
        let ep = self
            .api(
                reqwest::Method::POST,
                "/endpoints",
                Some(json!({ "name": slug, "slug": slug })),
            )
            .await;
        let mut created = Vec::new();
        for d in destinations {
            created.push(
                self.api(
                    reqwest::Method::POST,
                    &format!("/endpoints/{}/destinations", ep["id"]),
                    Some(d.clone()),
                )
                .await,
            );
        }
        created
    }

    async fn send(&self, slug: &str, body: &'static str) {
        let resp = self
            .client
            .post(format!("{}/h/{slug}", self.base))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
    }

    async fn stats(&self) -> Value {
        self.api(reqwest::Method::GET, "/stats", None).await
    }
}

async fn wait_until(mut cond: impl AsyncFnMut() -> bool) {
    for _ in 0..100 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("시간 안에 조건을 만족하지 못했습니다");
}

#[tokio::test]
async fn fans_out_to_every_destination_and_retries_failures() {
    let (healthy_url, healthy) = spawn_target(0).await;
    let (flaky_url, flaky) = spawn_target(3).await;
    let buffer = Buffer::spawn().await;
    buffer
        .endpoint("github", &[&healthy_url, &format!("{flaky_url}/in?k=1")])
        .await;

    let resp = buffer
        .client
        .post(format!("{}/h/github/push?a=1", buffer.base))
        .header("x-signature", "sha256=abc")
        .body("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["destinations"], 2);

    wait_until(async || healthy.received().len() == 1 && flaky.received().len() == 1).await;

    assert_eq!(
        healthy.received()[0],
        (
            "/push?a=1".to_string(),
            Some("sha256=abc".to_string()),
            Bytes::from("payload")
        )
    );
    let flaky_got = flaky.received();
    assert_eq!(flaky_got[0].0, "/in/push?k=1&a=1");
    assert_eq!(flaky_got[0].2, Bytes::from("payload"));
    assert_eq!(flaky.hits.load(Ordering::SeqCst), 4);
    assert_eq!(healthy.hits.load(Ordering::SeqCst), 1);

    wait_until(async || buffer.stats().await["pending"] == 0).await;
    let stats = buffer.stats().await;
    assert_eq!(stats["events_24h"], 1);
    assert_eq!(stats["delivered_24h"], 2);
}

#[tokio::test]
async fn holds_webhooks_while_unreachable_until_destination_says_alive() {
    // 포트만 잡아 두고 아무도 듣지 않는 목적지
    let port = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = port.local_addr().unwrap();
    drop(port);
    let buffer = Buffer::spawn().await;
    let dest = buffer
        .endpoint("svc", &[&format!("http://{addr}")])
        .await
        .remove(0);
    assert_eq!(dest["max_retries"], 10);

    buffer.send("svc", "first").await;
    wait_until(async || buffer.stats().await["down_destinations"] == 1).await;
    // 응답 없음 상태에서 들어온 웹훅은 보내 보지도 않고 보관만 한다
    buffer.send("svc", "second").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let queue = buffer.api(reqwest::Method::GET, "/deliveries", None).await;
    let queue = queue.as_array().unwrap();
    assert_eq!(queue.len(), 2);
    assert!(queue.iter().all(|d| d["destination_down"] == true));
    let attempts: Vec<_> = queue
        .iter()
        .map(|d| d["attempts"].as_u64().unwrap())
        .collect();
    assert_eq!(
        attempts.iter().sum::<u64>(),
        1,
        "재시도하면 안 됩니다: {attempts:?}"
    );

    // 목적지가 다시 떠서 복구 알림을 보낸다
    let (_, target) = spawn_target_on(TcpListener::bind(addr).await.unwrap(), 0).await;
    let token = dest["recover_token"].as_str().unwrap();
    let resp: Value = buffer
        .client
        .post(format!("{}/d/{token}/alive", buffer.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["queued"], 2);

    wait_until(async || target.received().len() == 2).await;
    let bodies: Vec<_> = target.received().into_iter().map(|r| r.2).collect();
    assert!(bodies.contains(&Bytes::from("first")) && bodies.contains(&Bytes::from("second")));
    let stats = buffer.stats().await;
    assert_eq!(
        (stats["down_destinations"].clone(), stats["pending"].clone()),
        (json!(0), json!(0))
    );

    let resp = buffer
        .client
        .post(format!("{}/d/wrong-token/alive", buffer.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn stops_after_configured_retries() {
    let (url, target) = spawn_target(usize::MAX).await;
    let buffer = Buffer::spawn().await;
    buffer
        .endpoint_with("err", &[json!({ "url": url, "max_retries": 2 })])
        .await;

    buffer.send("err", "x").await;
    wait_until(async || buffer.stats().await["failed"] == 1).await;
    // 첫 시도 1번 + 재시도 2번
    assert_eq!(target.hits.load(Ordering::SeqCst), 3);
    let queue = buffer.api(reqwest::Method::GET, "/deliveries", None).await;
    assert_eq!(queue[0]["state"], "failed");
    assert_eq!(queue[0]["last_status"], 503);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(target.hits.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn unknown_endpoint_is_404_and_api_needs_token() {
    let buffer = Buffer::spawn().await;

    let resp = buffer
        .client
        .post(format!("{}/h/nope", buffer.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    let resp = buffer
        .client
        .get(format!("{}/api/stats", buffer.base))
        .bearer_auth("wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    let resp = buffer.client.get(&buffer.base).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.text().await.unwrap().contains("Webhook Buffer"));
}
