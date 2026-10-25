//! 저장된 웹훅을 성공할 때까지 계속 재전달하는 백그라운드 작업

use std::{sync::Arc, time::Duration};

use tokio::sync::Notify;
use tracing::{info, warn};

use crate::{
    forwarder::Forwarder,
    store::{Store, now_ms},
};

const BATCH_SIZE: usize = 100;
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

    pub fn next_attempt_at(&self, attempts: u32) -> i64 {
        now_ms() + self.delay(attempts).as_millis() as i64
    }
}

pub async fn run(store: Store, forwarder: Forwarder, backoff: Backoff, wake: Arc<Notify>) {
    loop {
        if let Err(e) = retry_due(&store, &forwarder, backoff).await {
            warn!("재시도 처리 중 오류: {e:#}");
        }

        let sleep_for = match store.next_attempt_at().await {
            Ok(Some(at)) => Duration::from_millis((at - now_ms()).max(0) as u64).min(MAX_IDLE),
            Ok(None) => MAX_IDLE,
            Err(e) => {
                warn!("다음 재시도 시각 조회 실패: {e:#}");
                Duration::from_secs(1)
            }
        };

        tokio::select! {
            _ = tokio::time::sleep(sleep_for) => {}
            _ = wake.notified() => {}
        }
    }
}

async fn retry_due(store: &Store, forwarder: &Forwarder, backoff: Backoff) -> anyhow::Result<()> {
    loop {
        let due = store.due(now_ms(), BATCH_SIZE).await?;
        if due.is_empty() {
            return Ok(());
        }

        for item in due {
            match forwarder.send(&item.webhook).await {
                Ok(_) => {
                    store.delete(item.id).await?;
                    info!(
                        id = item.id,
                        attempts = item.attempts + 1,
                        "저장된 웹훅 재전달 성공"
                    );
                }
                Err(err) => {
                    let attempts = item.attempts + 1;
                    let delay = backoff.delay(attempts);
                    warn!(
                        id = item.id,
                        attempts,
                        "재전달 실패, {}초 뒤 다시 시도: {err}",
                        delay.as_secs_f32()
                    );
                    store
                        .reschedule(item.id, attempts, backoff.next_attempt_at(attempts), err)
                        .await?;
                }
            }
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
