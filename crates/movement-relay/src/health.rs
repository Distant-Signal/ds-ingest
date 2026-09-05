//! Readiness for `movement-relay` means "confirmed Kafka partition
//! assignment," NOT "the HTTP server answered" and NOT "at least one
//! message has arrived" (contrast with `trust-consumer`/
//! `full-coverage-consumer`'s own `ConnectionState`, which flips on
//! message arrival -- see
//! docs/superpowers/specs/2026-09-04-movement-relay-design.md Decision 5
//! for why the two crates deliberately differ here: this crate's
//! readiness is the exact gate Deploy B's rollout safety depends on
//! (whether the NEW pod has truly taken over group membership before the
//! OLD one is torn down), which message-arrival alone doesn't prove during
//! a genuine lull. Do not "fix" this inconsistency by making the two
//! match -- it is deliberate.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::http::StatusCode;
use axum::routing::get;
use rdkafka::ClientContext;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};

pub type ReadyState = Arc<AtomicBool>;

/// `rdkafka::ClientConfig::create_with_context` target -- flips `ready`
/// true on a non-empty partition assignment (`post_rebalance`'s `Assign`
/// variant), false on any revoke/error path, independent of whether a
/// message has arrived on the assigned partitions yet.
pub struct RelayContext {
    pub ready: ReadyState,
}

impl ClientContext for RelayContext {}

impl ConsumerContext for RelayContext {
    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) if !partitions.elements().is_empty() => {
                self.ready.store(true, Ordering::Relaxed);
                metrics::gauge!(common::metrics::metric_name("movement_relay_ready")).set(1.0);
                tracing::info!(
                    partitions = partitions.elements().len(),
                    "movement-relay: Kafka partition assignment confirmed; readiness now true"
                );
            }
            Rebalance::Revoke(_) => {
                self.ready.store(false, Ordering::Relaxed);
                metrics::gauge!(common::metrics::metric_name("movement_relay_ready")).set(0.0);
                tracing::warn!("movement-relay: Kafka partitions revoked; readiness now false");
            }
            Rebalance::Error(err) => {
                self.ready.store(false, Ordering::Relaxed);
                metrics::gauge!(common::metrics::metric_name("movement_relay_ready")).set(0.0);
                tracing::error!(error = ?err, "movement-relay: Kafka rebalance error; readiness now false");
            }
            _ => {}
        }
    }
}

pub fn spawn(bind_url: String, ready: ReadyState) {
    tokio::spawn(async move {
        let app = axum::Router::new().route("/healthz", get(move || healthz(Arc::clone(&ready))));
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
}

async fn healthz(ready: ReadyState) -> (StatusCode, &'static str) {
    if ready.load(Ordering::Relaxed) {
        (StatusCode::OK, "partitions assigned")
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "no confirmed partition assignment",
        )
    }
}
