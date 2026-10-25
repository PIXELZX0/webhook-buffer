//! 웹훅을 목적지로 그대로 전달한다.

use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use reqwest::{
    Client, Method,
    header::{HeaderMap, HeaderName, HeaderValue},
};

use crate::store::Webhook;

/// 프록시가 그대로 넘기면 안 되는 헤더 (hop-by-hop 헤더와 reqwest가 다시 계산하는 헤더)
const SKIP_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub fn should_forward_header(name: &str) -> bool {
    !SKIP_HEADERS.iter().any(|h| name.eq_ignore_ascii_case(h))
}

/// 목적지가 돌려준 응답
pub struct Delivered {
    pub status: reqwest::StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

#[derive(Clone)]
pub struct Forwarder {
    client: Client,
    target_url: String,
}

impl Forwarder {
    pub fn new(target_url: &str, timeout: Duration) -> Result<Self> {
        let client = Client::builder()
            .timeout(timeout)
            // 리다이렉트를 따라가면 POST가 GET으로 바뀔 수 있으니 따라가지 않는다.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            target_url: target_url.trim_end_matches('/').to_string(),
        })
    }

    /// 웹훅을 한 번 전달해 본다. 2xx 응답만 성공으로 보고, 그 외에는 실패 사유를 돌려준다.
    pub async fn send(&self, webhook: &Webhook) -> Result<Delivered, String> {
        let method = Method::from_bytes(webhook.method.as_bytes())
            .map_err(|e| format!("잘못된 메서드 {}: {e}", webhook.method))?;
        let url = format!("{}{}", self.target_url, webhook.path_and_query);

        let mut headers = HeaderMap::new();
        for (name, value) in &webhook.headers {
            if !should_forward_header(name) {
                continue;
            }
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_bytes(value),
            ) {
                headers.append(name, value);
            }
        }

        let resp = self
            .client
            .request(method, &url)
            .headers(headers)
            .body(webhook.body.clone())
            .send()
            .await
            .map_err(|e| format!("요청 실패: {e}"))?;

        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.bytes().await.unwrap_or_default();

        if status.is_success() {
            Ok(Delivered {
                status,
                headers,
                body,
            })
        } else {
            Err(format!("목적지 응답 {status}"))
        }
    }
}
