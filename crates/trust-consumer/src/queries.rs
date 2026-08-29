//! Thin HTTP client wrapper against `crates/api`'s train-tracking
//! endpoints. Kept separate from `process.rs` so the processing loop's
//! tests can run against `FakeMovementFeed` without also needing a live
//! `api` -- these functions are the one part of `process::run_once`'s
//! surrounding loop this plan does NOT unit-test, verified instead by the
//! manual live-stack check, the same posture `crates/enricher`'s
//! DB-touching `queries.rs` takes.

use common::ingest::INTERNAL_TOKEN_HEADER;
use common::{TrackedTrainRef, TrainMovementEventMessage};
use reqwest::Client;

pub async fn fetch_active_tracked_trains(
    client: &Client,
    url: &str,
    internal_token: &str,
) -> anyhow::Result<Vec<TrackedTrainRef>> {
    let response = client
        .get(url)
        .header(INTERNAL_TOKEN_HEADER, internal_token)
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json().await?)
}

pub async fn post_train_events(
    client: &Client,
    url: &str,
    internal_token: &str,
    events: &[TrainMovementEventMessage],
) -> anyhow::Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    common::ingest::post_batch(client, url, internal_token, events, "train events").await
}
