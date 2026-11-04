//! SIGTERM drain for the `movement-events` consume loops (trust-consumer,
//! trust-backlog-consumer, full-coverage-consumer).
//!
//! Without a handler, SIGTERM killed a consumer mid-cycle. That was already
//! safe -- an un-ACKed batch stays in the consumer's pending-entries list
//! and is replayed by the next pod under the same fixed consumer name --
//! but it threw away a batch that was a moment from done, so it was always
//! re-read. With [`Shutdown`] and [`finish_within`], a loop stops at the
//! next cycle boundary and lets the cycle in flight finish (write, then
//! `XACK`) for up to a bound.
//!
//! **Never a gap.** Nothing here ACKs anything: a cycle ACKs only after its
//! own downstream write is confirmed, exactly as before. A cycle that does
//! not finish within the bound is dropped where it stands; whatever it had
//! not `ACKed` is still pending and is replayed after the restart. Dropping
//! it mid-write is the same as the SIGKILL the pod would otherwise get.

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;

/// How long a consumer lets the cycle in flight finish after SIGTERM. A
/// cycle is one `XREADGROUP` (blocking up to 5 s when the stream is quiet)
/// plus its write and `XACK`, so 15 s covers a healthy one with room; an
/// unhealthy one is abandoned, which is safe (see the module docs). The
/// pod's `terminationGracePeriodSeconds` must exceed this plus whatever
/// periodic work (a reference reload, a gap check) is running when SIGTERM
/// lands: the Kubernetes default of 30 s does.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(15);

/// Whether shutdown was requested, and a future that waits for it. Cheap
/// to check between steps of a loop.
pub struct Shutdown {
    requested: watch::Receiver<bool>,
}

/// Requests a [`Shutdown`] (tests, or a caller with its own signal).
pub struct ShutdownTrigger(watch::Sender<bool>);

impl ShutdownTrigger {
    pub fn trigger(&self) {
        self.0.send_replace(true);
    }
}

impl Shutdown {
    /// Installs the SIGTERM (and Ctrl-C) handler now and returns the
    /// signal it raises. From this call on SIGTERM no longer kills the
    /// process, so call it only where the caller's loop starts checking it:
    /// before that, the default action (exit) is the right one.
    pub fn on_sigterm() -> Self {
        let (trigger, shutdown) = Self::manual();
        #[cfg(unix)]
        let sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        tokio::spawn(async move {
            #[cfg(unix)]
            match sigterm {
                Ok(mut sigterm) => {
                    tokio::select! {
                        _ = sigterm.recv() => {}
                        _ = tokio::signal::ctrl_c() => {}
                    }
                }
                Err(err) => {
                    tracing::warn!(error = ?err, "could not install a SIGTERM handler; Ctrl-C only");
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown requested; finishing the cycle in flight");
            trigger.trigger();
        });
        shutdown
    }

    /// A shutdown nothing raises until the trigger is used.
    pub fn manual() -> (ShutdownTrigger, Self) {
        let (sender, requested) = watch::channel(false);
        (ShutdownTrigger(sender), Self { requested })
    }

    pub fn is_requested(&self) -> bool {
        *self.requested.borrow()
    }

    /// Resolves once shutdown is requested; never, if the trigger is gone
    /// without having been used.
    pub async fn requested(&mut self) {
        if self
            .requested
            .wait_for(|requested| *requested)
            .await
            .is_err()
        {
            std::future::pending::<()>().await;
        }
    }

    /// Runs `wait` (a backoff, an idle sleep) unless shutdown comes first.
    /// `false` if shutdown interrupted it.
    pub async fn wait_unless_requested(&mut self, wait: impl Future<Output = ()>) -> bool {
        tokio::select! {
            biased;
            () = self.requested() => false,
            () = wait => true,
        }
    }
}

/// How [`finish_within`] ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Finished<T> {
    /// The work finished and shutdown was not requested meanwhile.
    Ran(T),
    /// Shutdown was requested while it ran; it finished within the bound.
    /// The caller should stop now.
    Drained(T),
    /// Shutdown was requested and the work did not finish within the
    /// bound; it was dropped. The caller should stop now.
    Abandoned,
}

/// Runs `work` (one consume cycle) to completion; if shutdown is requested
/// meanwhile -- or already was -- gives it at most `bound` more, then drops
/// it.
pub async fn finish_within<T>(
    shutdown: &mut Shutdown,
    bound: Duration,
    work: impl Future<Output = T>,
) -> Finished<T> {
    let mut work = std::pin::pin!(work);
    if !shutdown.is_requested() {
        tokio::select! {
            biased;
            out = work.as_mut() => {
                return if shutdown.is_requested() {
                    Finished::Drained(out)
                } else {
                    Finished::Ran(out)
                };
            }
            () = shutdown.requested() => {}
        }
    }
    match tokio::time::timeout(bound, work).await {
        Ok(out) => Finished::Drained(out),
        Err(_elapsed) => Finished::Abandoned,
    }
}

/// Logs how a consume loop stopped. `service` names it in the message.
pub fn log_stop<T>(service: &str, finished: &Finished<T>) {
    match finished {
        Finished::Ran(_) | Finished::Drained(_) => {
            tracing::info!(
                service,
                "shutdown: the cycle in flight finished; anything it ACKed was written first"
            );
        }
        Finished::Abandoned => tracing::warn!(
            service,
            drain_secs = DRAIN_TIMEOUT.as_secs(),
            "shutdown: the cycle in flight did not finish in time and was dropped; whatever it \
             had not ACKed stays pending and is replayed after the restart"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn work_that_finishes_without_a_shutdown_ran() {
        let (_trigger, mut shutdown) = Shutdown::manual();
        let out = finish_within(&mut shutdown, DRAIN_TIMEOUT, async {
            tokio::time::sleep(Duration::from_secs(3)).await;
            7
        })
        .await;
        assert_eq!(out, Finished::Ran(7));
    }

    /// SIGTERM mid-cycle: the cycle is not cancelled, it finishes.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_mid_cycle_lets_it_finish() {
        let (trigger, mut shutdown) = Shutdown::manual();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            trigger.trigger();
        });
        let out = finish_within(&mut shutdown, DRAIN_TIMEOUT, async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            "acked"
        })
        .await;
        assert_eq!(out, Finished::Drained("acked"));
    }

    /// A cycle that outlasts the bound is dropped: its remaining steps
    /// (the XACK among them) never run.
    #[tokio::test(start_paused = true)]
    async fn a_cycle_that_outlasts_the_bound_is_abandoned() {
        let (trigger, mut shutdown) = Shutdown::manual();
        trigger.trigger();
        let acked = std::sync::atomic::AtomicBool::new(false);
        let out = finish_within(&mut shutdown, DRAIN_TIMEOUT, async {
            tokio::time::sleep(DRAIN_TIMEOUT * 2).await;
            acked.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
        assert_eq!(out, Finished::Abandoned);
        assert!(!acked.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn a_backoff_is_cut_short_by_a_shutdown() {
        let (trigger, mut shutdown) = Shutdown::manual();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            trigger.trigger();
        });
        let started = tokio::time::Instant::now();
        let completed = shutdown
            .wait_unless_requested(tokio::time::sleep(Duration::from_secs(60)))
            .await;
        assert!(!completed);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(shutdown.is_requested());
    }

    /// A dropped trigger is not a shutdown.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_trigger_does_not_request_shutdown() {
        let (trigger, mut shutdown) = Shutdown::manual();
        drop(trigger);
        let out = finish_within(&mut shutdown, DRAIN_TIMEOUT, async {
            tokio::time::sleep(Duration::from_secs(1)).await;
        })
        .await;
        assert_eq!(out, Finished::Ran(()));
        assert!(!shutdown.is_requested());
    }
}
