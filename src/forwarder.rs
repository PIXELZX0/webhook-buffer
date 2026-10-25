//! 웹훅을 목적지로 그대로 전달한다.

use std::time::Duration;

use anyhow::Result;
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

fn should_forward_header(name: &str) -> bool {
    !SKIP_HEADERS.iter().any(|h| name.eq_ignore_ascii_case(h))
}

/// 전달 실패 사유. 목적지가 응답했다면 그 상태 코드도 담는다.
#[derive(Debug)]
pub struct SendError {
    pub status: Option<u16>,
    pub message: String,
}

#[derive(Clone)]
pub struct Forwarder {
    client: Client,
}

impl Forwarder {
    pub fn new(timeout: Duration) -> Result<Self> {
        let client = Client::builder()
            .timeout(timeout)
            // 리다이렉트를 따라가면 POST가 GET으로 바뀔 수 있으니 따라가지 않는다.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { client })
    }

    /// 웹훅을 한 번 전달해 본다. 2xx 응답만 성공으로 보고 상태 코드를 돌려준다.
    pub async fn send(&self, destination: &str, webhook: &Webhook) -> Result<u16, SendError> {
        let fail = |status, message| SendError { status, message };

        let method = Method::from_bytes(webhook.method.as_bytes())
            .map_err(|e| fail(None, format!("잘못된 메서드 {}: {e}", webhook.method)))?;
        let url = build_url(destination, &webhook.path_and_query);

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
            .map_err(|e| fail(None, format!("요청 실패: {e}")))?;

        let status = resp.status();
        if status.is_success() {
            Ok(status.as_u16())
        } else {
            Err(fail(Some(status.as_u16()), format!("목적지 응답 {status}")))
        }
    }
}

/// 목적지 URL 뒤에 받은 요청의 하위 경로와 쿼리를 붙인다.
/// `https://a.com/hook?k=1` + `/push?x=2` → `https://a.com/hook/push?k=1&x=2`
pub fn build_url(destination: &str, suffix: &str) -> String {
    let (dest_path, dest_query) = split_query(destination);
    let (suffix_path, suffix_query) = split_query(suffix);

    let mut url = if suffix_path.is_empty() {
        dest_path.to_string()
    } else {
        format!("{}{}", dest_path.trim_end_matches('/'), suffix_path)
    };
    let query: Vec<&str> = [dest_query, suffix_query]
        .into_iter()
        .filter(|q| !q.is_empty())
        .collect();
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query.join("&"));
    }
    url
}

fn split_query(s: &str) -> (&str, &str) {
    s.split_once('?').unwrap_or((s, ""))
}

#[cfg(test)]
mod tests {
    use super::build_url;

    #[test]
    fn joins_paths_and_queries() {
        assert_eq!(build_url("https://a.com/hook", ""), "https://a.com/hook");
        assert_eq!(
            build_url("https://a.com/hook/", "/push"),
            "https://a.com/hook/push"
        );
        assert_eq!(build_url("https://a.com", "?x=1"), "https://a.com?x=1");
        assert_eq!(
            build_url("https://a.com/hook?k=1", "/push?x=2"),
            "https://a.com/hook/push?k=1&x=2"
        );
    }
}
