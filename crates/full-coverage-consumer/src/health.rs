//! Minimal liveness endpoint. Unlike every existing poller (whose health
//! is implicit -- "did the last cron tick succeed") and `crates/enricher`
//! (which has "no HTTP surface" at all per its own deployment templates),
//! a persistent Kafka consumer needs a real connected/disconnected signal
//! a Kubernetes liveness probe can act on: a broker connection that's
//! silently wedged should get restarted, not left running forever.
//!
//! Verbatim copy of `crates/trust-consumer/src/health.rs` -- this is a
//! second, independent persistent Kafka consumer with the exact same
//! need, per Task 8 of
//! docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::http::StatusCode;
use axum::routing::get;

/// Shared with the Kafka consumer loop (Task 13): `true` once the consumer
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
