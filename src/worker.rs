//! 저장된 전달 작업을 목적지마다 보내는 백그라운드 작업

use std::{sync::Arc, time::Duration};

use tokio::sync::{Notify, Semaphore};
use tracing::{debug, info, warn};

use crate::{
    forwarder::Forwarder,
    store::{Job, Store, now_ms},
};

/// 새 웹훅이 들어오지 않아도 이 간격마다 한 번은 DB를 다시 본다.
const MAX_IDLE: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
pub struct Backoff {
    pub min: Duration,
    pub max: Duration,
}

impl Backoff {
    /// `attempts`번 실패한 뒤 다음 시도까지 기다릴 시간. 실패할 때마다 두 배로 늘어난다.
    pub fn delay(&self, attempts: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempts.saturating_sub(1));
        self.min.saturating_mul(factor).min(self.max)
    }
}

#[derive(Clone)]
pub struct Worker {
    pub store: Store,
    pub forwarder: Forwarder,
    pub backoff: Backoff,
    /// 한 번의 요청이 끝나기 전에 같은 작업을 다시 집어가지 않도록 잡아 두는 시간
    pub lease: Duration,
    pub concurrency: usize,
    pub wake: Arc<Notify>,
}

impl Worker {
    pub async fn run(self) {
        let permits = Arc::new(Semaphore::new(self.concurrency));
        loop {
            // 보낼 자리가 하나라도 날 때까지 기다린 뒤, 남은 자리만큼 작업을 가져온다.
            let first = permits
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");
            let limit = 1 + permits.available_permits();
            let now = now_ms();
            let jobs = match self
                .store
                .claim_due(now, limit, now + self.lease.as_millis() as i64)
                .await
            {
                Ok(jobs) => jobs,
                Err(e) => {
                    warn!("전달 작업 조회 실패: {e:#}");
                    Vec::new()
                }
            };

            if jobs.is_empty() {
                drop(first);
                self.sleep_until_next().await;
                continue;
            }

            let mut first = Some(first);
            for job in jobs {
                let permit = match first.take() {
                    Some(p) => p,
                    None => permits
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("semaphore closed"),
                };
                let worker = self.clone();
                tokio::spawn(async move {
                    worker.deliver(job).await;
                    drop(permit);
                    // 실패한 작업의 다음 시도 시각이 바뀌었을 수 있으니 다시 계산하게 깨운다.
                    worker.wake.notify_one();
                });
            }
        }
    }

    async fn sleep_until_next(&self) {
        let sleep_for = match self.store.next_due().await {
            Ok(Some(at)) => Duration::from_millis((at - now_ms()).max(0) as u64).min(MAX_IDLE),
            Ok(None) => MAX_IDLE,
            Err(e) => {
                warn!("다음 전달 시각 조회 실패: {e:#}");
                Duration::from_secs(1)
            }
        };
        // 이미 보낼 시각이 된 작업이 있는데 자리가 없어서 못 가져온 경우가 아니라면 잠깐 쉰다.
        let sleep_for = sleep_for.max(Duration::from_millis(10));
        tokio::select! {
            _ = tokio::time::sleep(sleep_for) => {}
            _ = self.wake.notified() => {}
        }
    }

    async fn deliver(&self, job: Job) {
        let attempts = job.attempts + 1;
        let result = match self.forwarder.send(&job.url, &job.webhook).await {
            Ok(status) => {
                if attempts > 1 {
                    info!(delivery = job.delivery_id, attempts, url = %job.url, "재전달 성공");
                } else {
                    debug!(delivery = job.delivery_id, url = %job.url, "전달 성공");
                }
                self.store
                    .mark_delivered(job.delivery_id, attempts, status)
                    .await
            }
            Err(err) if err.unreachable => {
                // 응답이 아예 없으면 다시 두드리지 않고, 목적지가 살아났다고 알려줄 때까지 쌓아 둔다.
                warn!(
                    delivery = job.delivery_id,
                    url = %job.url,
                    "목적지 응답 없음, 복구 알림이 올 때까지 보관: {}",
                    err.message
                );
                self.store
                    .mark_unreachable(job.delivery_id, job.destination_id, attempts, err.message)
                    .await
            }
            Err(err) => {
                // 에러로 응답한 경우만 정해진 횟수까지 다시 보낸다. 첫 응답은 재시도가 아니므로 세지 않는다.
                let error_responses = job.error_responses + 1;
                let retries_used = error_responses - 1;
                let next_attempt_at = match job.max_retries {
                    Some(max) if retries_used >= max => {
                        warn!(
                            delivery = job.delivery_id,
                            url = %job.url,
                            "재시도 {max}회를 다 써서 멈춤: {}",
                            err.message
                        );
                        None
                    }
                    _ => {
                        let delay = self.backoff.delay(error_responses);
                        warn!(
                            delivery = job.delivery_id,
                            attempts,
                            url = %job.url,
                            "전달 실패, {:.1}초 뒤 다시 시도: {}",
                            delay.as_secs_f32(),
                            err.message
                        );
                        Some(now_ms() + delay.as_millis() as i64)
                    }
                };
                self.store
                    .mark_error_response(
                        job.delivery_id,
                        attempts,
                        error_responses,
                        next_attempt_at,
                        err.status,
                        err.message,
                    )
                    .await
            }
        };
        if let Err(e) = result {
            // 상태를 못 남겨도 잡아 둔 시간이 지나면 다시 보내므로 웹훅은 사라지지 않는다.
            warn!(delivery = job.delivery_id, "전달 상태 저장 실패: {e:#}");
        }
    }
}

/// 다 보낸 지 오래된 웹훅 기록을 주기적으로 지운다.
pub async fn prune_loop(store: Store, retention: Duration) {
    let mut tick = tokio::time::interval(Duration::from_secs(60 * 60));
    loop {
        tick.tick().await;
        match store.prune(now_ms() - retention.as_millis() as i64).await {
            Ok(0) => {}
            Ok(n) => info!("오래된 웹훅 기록 {n}건 정리"),
            Err(e) => warn!("웹훅 기록 정리 실패: {e:#}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        let b = Backoff {
            min: Duration::from_secs(1),
            max: Duration::from_secs(10),
        };
        assert_eq!(b.delay(1), Duration::from_secs(1));
        assert_eq!(b.delay(2), Duration::from_secs(2));
        assert_eq!(b.delay(4), Duration::from_secs(8));
        assert_eq!(b.delay(5), Duration::from_secs(10));
        assert_eq!(b.delay(1000), Duration::from_secs(10));
    }
}
