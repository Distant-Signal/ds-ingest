//! Minimal liveness endpoint. Unlike every existing poller (whose health
//! is implicit -- "did the last cron tick succeed") and `crates/enricher`
//! (which has "no HTTP surface" at all per its own deployment templates),
//! a persistent Kafka consumer needs a real connected/disconnected signal
//! a Kubernetes liveness probe can act on: a broker connection that's
//! silently wedged should get restarted, not left running forever.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::http::StatusCode;
use axum::routing::get;

/// Shared with the Kafka consumer loop (Task 14): `true` once the consumer
/// has successfully polled at least one batch (or confirmed group
/// membership) since the last disconnect; `false` from startup and
/// whenever a reconnect is in progress.
pub type ConnectionState = Arc<AtomicBool>;

pub fn spawn(bind_url: String) -> ConnectionState {
    let state: ConnectionState = Arc::new(AtomicBool::new(false));
    let state_for_server = Arc::clone(&state);

    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/healthz",
            get(move || healthz(Arc::clone(&state_for_server))),
        );
        let listener = match tokio::net::TcpListener::bind(&bind_url).await {
            Ok(listener) => listener,
            Err(err) => {
                tracing::error!(error = ?err, bind_url, "failed to bind health endpoint");
                return;
            }
        };
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!(error = ?err, "health endpoint server stopped");
        }
    });

    state
}

async fn healthz(state: ConnectionState) -> (StatusCode, &'static str) {
    if state.load(Ordering::Relaxed) {
        (StatusCode::OK, "connected")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "disconnected")
    }
}

/// Centralizes every `ConnectionState` transition with a matching
/// Prometheus gauge update, so the AtomicBool and
/// `distant_signal_trust_consumer_ready` never drift out of sync across
/// this crate's three flip sites (`feed/kafka.rs`'s own internal update,
/// and `main.rs`'s `ActiveFeed::next_batch` RedisStream branch) -- one
/// place that changes, not three, matching
/// `crates/common/src/metrics.rs::metric_name`'s own stated reasoning for
/// the identical shape of problem.
pub fn set_connected(state: &ConnectionState, connected: bool) {
    state.store(connected, Ordering::Relaxed);
    metrics::gauge!(common::metrics::metric_name("trust_consumer_ready")).set(if connected {
        1.0
    } else {
        0.0
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::set_connected;

    #[test]
    fn set_connected_updates_the_shared_atomic_state() {
        let state = Arc::new(AtomicBool::new(false));

        set_connected(&state, true);
        assert!(state.load(Ordering::Relaxed));

        set_connected(&state, false);
        assert!(!state.load(Ordering::Relaxed));
        // The distant_signal_trust_consumer_ready gauge update inside
        // set_connected is not independently asserted here -- no recorder
        // is installed in this unit test, matching how
        // crates/movement-relay/src/main.rs's own
        // a_clean_batch_commits_and_publishes test already treats its
        // metrics::counter! call.
    }
}
