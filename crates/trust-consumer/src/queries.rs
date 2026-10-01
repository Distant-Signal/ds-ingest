//! Thin HTTP client wrapper against `crates/api`'s train-tracking
//! endpoints. Kept separate from `process.rs` so the processing loop's
//! tests can run against `FakeMovementFeed` without also needing a live
//! `api` -- these functions are the one part of `process::run_once`'s
//! surrounding loop this plan does NOT unit-test, verified instead by the
//! manual live-stack check, the same posture `crates/enricher`'s
//! DB-touching `queries.rs` takes.

use common::oauth_client::OAuthTokenCache;
use common::{TrackedTrainRef, TrainMovementEventMessage};
use reqwest::Client;

pub(crate) async fn fetch_active_tracked_trains(
    client: &Client,
    url: &str,
    tokens: &OAuthTokenCache,
) -> anyhow::Result<Vec<TrackedTrainRef>> {
    common::ingest::get_json(client, url, tokens).await
}

pub(crate) async fn fetch_stanox_crs(
    client: &Client,
    url: &str,
    tokens: &OAuthTokenCache,
) -> anyhow::Result<Vec<common::StanoxCrsRecord>> {
    common::ingest::get_json(client, url, tokens).await
}

/// POSTs one batch and returns `api`'s reply, including any events it
/// rejected for a data error (DB2-2; the same `rejected` shape as
/// `/private/trust-event-backlog`). A transient failure is `Err` (a 5xx
/// among them), so the caller leaves the batch un-ACKed. An older `api` that
/// answers only `{"upserted": N}` parses as "nothing rejected". An empty
/// batch is not sent.
pub(crate) async fn post_train_events(
    client: &Client,
    url: &str,
    tokens: &OAuthTokenCache,
    events: &[TrainMovementEventMessage],
) -> anyhow::Result<common::TrustBacklogIngestResponse> {
    if events.is_empty() {
        return Ok(common::TrustBacklogIngestResponse::default());
    }
    common::ingest::post_batch_for_response(client, url, tokens, events, "train events").await
}

pub(crate) async fn post_train_forward_signals(
    client: &Client,
    url: &str,
    tokens: &OAuthTokenCache,
    signals: &[common::TrainForwardSignalMessage],
) -> anyhow::Result<()> {
    if signals.is_empty() {
        return Ok(());
    }
    common::ingest::post_batch(client, url, tokens, signals, "train forward signals").await
}
