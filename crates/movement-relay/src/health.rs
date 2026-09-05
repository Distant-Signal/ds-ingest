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
//!
//! The shared HTTP/gauge plumbing (`spawn_with_state`/`healthz`/
//! `set_connected`) now lives in `health-http`; only this crate's own
//! Kafka-rebalance-driven state transitions stay here.

use rdkafka::ClientContext;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};

/// `rdkafka::ClientConfig::create_with_context` target -- flips `ready`
/// true on a non-empty partition assignment (`post_rebalance`'s `Assign`
/// variant), false on any revoke/error path, independent of whether a
/// message has arrived on the assigned partitions yet.
pub struct RelayContext {
    pub ready: health_http::ConnectionState,
}

impl ClientContext for RelayContext {}

impl ConsumerContext for RelayContext {
    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) if !partitions.elements().is_empty() => {
                health_http::set_connected(&self.ready, "movement_relay_ready", true);
                tracing::info!(
                    partitions = partitions.elements().len(),
                    "movement-relay: Kafka partition assignment confirmed; readiness now true"
                );
            }
            Rebalance::Revoke(_) => {
                health_http::set_connected(&self.ready, "movement_relay_ready", false);
                tracing::warn!("movement-relay: Kafka partitions revoked; readiness now false");
            }
            Rebalance::Error(err) => {
                health_http::set_connected(&self.ready, "movement_relay_ready", false);
                tracing::error!(error = ?err, "movement-relay: Kafka rebalance error; readiness now false");
            }
            _ => {}
        }
    }
}
