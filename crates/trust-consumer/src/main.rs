//! `trust-consumer`: persistent Kafka consumer for Network Rail's TRUST
//! Train Movements feed (via RDM), filtered to exactly the currently
//! user-tracked `(train_uid, date)` set. NOT a cron-style poller -- see
//! docs/superpowers/plans/2026-08-28-train-tracking.md's Global
//! Constraints for why this crate isn't named `poller-trust`.

mod config;
mod feed;
mod health;
mod schema;
mod matching;
mod journey;
mod eta;
mod dedup;
mod process;
mod queries;
mod stanox_crs;

use std::time::Duration;

use clap::Parser;
use config::Config;
use feed::MovementFeed;
use feed::kafka::KafkaMovementFeed;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::parse();
    let connection_state = health::spawn(config.health_bind_url.clone());
    let http = reqwest::Client::new();

    let mut feed = KafkaMovementFeed::connect(&config, connection_state)?;

    let mut reference = process::Reference { pending: Vec::new() };
    let reload_interval = Duration::from_secs(config.reference_reload_secs);
    let mut last_reference_reload = tokio::time::Instant::now() - reload_interval;

    // The CSV-derived table `config.stanox_crs` already loaded at parse
    // time becomes the shared cell's initial value -- the startup value
    // and the fail-open fallback stay exactly as they were (Decision 3);
    // only the read path (a per-cycle snapshot instead of a bare
    // reference) and the addition of this reload block are new.
    let stanox_crs = std::sync::RwLock::new(config.stanox_crs.clone());
    let stanox_crs_reload_interval = Duration::from_secs(config.stanox_crs_reload_secs);
    let mut last_stanox_crs_reload = tokio::time::Instant::now() - stanox_crs_reload_interval;

    // Owned here, for the whole life of the process: TRUST spreads one
    // train's Activation, origin departure, later movements and any
    // cancellation across many batches, so this state must survive every
    // `run_once` call, not be rebuilt per cycle. See
    // `process::ProcessorState`'s docs.
    let mut state = process::ProcessorState::default();

    loop {
        if last_reference_reload.elapsed() >= reload_interval {
            match queries::fetch_active_tracked_trains(
                &http,
                &config.api_tracked_trains_url,
                &config.internal_token,
            )
            .await
            {
                Ok(refs) => {
                    // Rebuilds the matchable pins AND rehydrates already-resolved
                    // train_ids, so a restart doesn't permanently lose trains
                    // whose origin departure has already been and gone.
                    process::apply_reference_reload(refs, &mut reference, &mut state);
                    // Same cadence, unrelated job: age out parked Activations
                    // for schedules that have already ended, so the national
                    // Activation stream can't grow this map without bound.
                    process::prune_expired_activations(
                        &mut state.pending_activations,
                        chrono::Utc::now().date_naive(),
                    );
                    last_reference_reload = tokio::time::Instant::now();
                }
                Err(err) => {
                    tracing::error!(error = ?err, "failed to reload active tracked trains; retrying next cycle");
                }
            }
        }

        if last_stanox_crs_reload.elapsed() >= stanox_crs_reload_interval {
            let fetched = queries::fetch_stanox_crs(&http, &config.stanox_crs_url, &config.internal_token).await;
            process::apply_stanox_crs_reload(fetched, &stanox_crs);
            last_stanox_crs_reload = tokio::time::Instant::now();
        }

        let outcome = run_cycle(&mut feed, &reference, &mut state, &stanox_crs, async |events| {
            queries::post_train_events(&http, &config.api_ingest_url, &config.internal_token, events).await
        })
        .await;

        if outcome == Cycle::Failed {
            // Nothing here waits on anything: `run_once` returns as soon as
            // the feed hands over a batch, and every failure path above
            // skips the commit, so a persistently-down `api` or an erroring
            // feed would otherwise spin this loop at full speed -- hammering
            // `api` and the log for the whole outage. A flat, short pause is
            // enough to make that a trickle; it deliberately isn't
            // exponential or configurable, because the loop has no backlog
            // to drain (Kafka holds the backlog) and a fixed small delay
            // costs nothing once the outage clears.
            tokio::time::sleep(ERROR_BACKOFF).await;
        }
    }
}

/// How long to wait before retrying after a failed cycle. See its one use
/// site above for why a flat constant is the right shape here.
const ERROR_BACKOFF: Duration = Duration::from_secs(2);

/// What one consume -> post -> commit cycle did. Returned rather than acted
/// on inside `run_cycle` so the caller owns the backoff sleep, and a test of
/// the commit rule doesn't have to wait out a real delay.
#[derive(Debug, PartialEq, Eq)]
enum Cycle {
    /// The batch was posted and its offsets confirmed.
    Committed,
    /// Something failed. Offsets were deliberately left unconfirmed, so the
    /// feed has not advanced past whatever went wrong.
    Failed,
}

/// The `loop` body's consume/post/commit step, extracted so the one rule
/// that matters here -- *never* commit a batch whose post failed -- is
/// unit-testable against `FakeMovementFeed` without a broker or an `api`.
/// `post` is taken as a closure for the same reason: it's the only part of
/// this that needs HTTP.
///
/// The commit is the sole way the consumed position advances (see
/// `MovementFeed::commit`), so skipping it on failure genuinely means "leave
/// this batch to be redelivered", and the `dedup_key` path makes that replay
/// safe.
async fn run_cycle<F, P>(
    feed: &mut F,
    reference: &process::Reference,
    state: &mut process::ProcessorState,
    stanox_crs: &std::sync::RwLock<stanox_crs::StanoxCrsTable>,
    post: P,
) -> Cycle
where
    F: MovementFeed,
    P: AsyncFnOnce(&[common::TrainMovementEventMessage]) -> anyhow::Result<()>,
{
    let snapshot = stanox_crs.read().expect("stanox_crs lock poisoned").clone();

    let events = match process::run_once(feed, reference, state, &snapshot).await {
        Ok(events) => events,
        Err(err) => {
            tracing::error!(error = ?err, "error processing movement feed batch");
            return Cycle::Failed;
        }
    };

    if let Err(err) = post(&events).await {
        tracing::error!(error = ?err, "failed to post train events; not committing this batch's offsets");
        return Cycle::Failed;
    }

    if let Err(err) = feed.commit().await {
        tracing::error!(error = ?err, "failed to commit Kafka offsets");
        return Cycle::Failed;
    }

    Cycle::Committed
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::LazyLock;

    use super::*;
    use crate::feed::FakeMovementFeed;

    /// The real, checked-in `reference-data/stanox-crs.csv`, mirroring
    /// `process.rs`'s own test fixture of the same name -- these tests
    /// depend on the real STANOX `"87212"` translating to `"WAT"` to match
    /// `one_pending_pin`'s pin.
    static TEST_STANOX_CRS: LazyLock<std::sync::RwLock<stanox_crs::StanoxCrsTable>> = LazyLock::new(|| {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../reference-data/stanox-crs.csv");
        std::sync::RwLock::new(stanox_crs::StanoxCrsTable::from_file(&path).expect("reference-data/stanox-crs.csv should parse"))
    });

    const ORIGIN_DEPARTURE: &str = r#"[{"header":{"msg_type":"0003"},"body":{
        "train_id":"221832406","event_type":"DEPARTURE",
        "planned_timestamp":"1787941920000","actual_timestamp":"1787941920000",
        "loc_stanox":"87212","variation_status":"ON TIME"
    }}]"#;

    fn one_pending_pin() -> process::Reference {
        process::Reference {
            pending: vec![matching::PendingPin {
                tracked_train_id: 1,
                pin_origin_crs: "WAT".to_string(),
                pin_scheduled_departure: "2026-08-28T18:32:00Z".parse().unwrap(),
            }],
        }
    }

    /// The regression this guards: while `enable.auto.offset.store` was left
    /// at librdkafka's `true` default, a batch whose post failed still had
    /// its offset stored the moment `recv` returned it, so the *next*
    /// cycle's commit swept it up and the failed batch was never
    /// redelivered. Skipping the commit only preserves the batch if
    /// receiving it never advances anything on its own -- which is exactly
    /// what `FakeMovementFeed` now mirrors.
    #[tokio::test]
    async fn a_failed_post_does_not_commit_the_batch() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome =
            run_cycle(&mut feed, &reference, &mut state, &TEST_STANOX_CRS, async |_| Err(anyhow::anyhow!("api is down"))).await;

        assert_eq!(outcome, Cycle::Failed);
        assert_eq!(feed.committed_count, 0, "a batch that never reached api must not be committed");
    }

    /// And the same batch, posted successfully, does commit -- otherwise the
    /// test above would pass against a `commit` that never worked at all.
    #[tokio::test]
    async fn a_successful_post_commits_the_batch() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(&mut feed, &reference, &mut state, &TEST_STANOX_CRS, async |events| {
            assert_eq!(events.len(), 1, "the pinned train's origin departure");
            Ok(())
        })
        .await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(feed.committed_count, 1);
    }

    /// A cycle that saw nothing has no offset to advance, so it must not
    /// manufacture a commit -- committing an empty poll is how an
    /// unconfirmed offset from a *previous* failed cycle would get swept up.
    #[tokio::test]
    async fn an_empty_poll_commits_nothing() {
        let mut feed = FakeMovementFeed::new(vec![vec![]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(&mut feed, &reference, &mut state, &TEST_STANOX_CRS, async |_| Ok(())).await;

        assert_eq!(outcome, Cycle::Committed);
        assert_eq!(feed.committed_count, 0);
    }

    /// A run of failures must never commit, however long it goes on -- this
    /// is the shape a real `api` outage takes, and it's the case the offset
    /// fix is there for: nothing is confirmed, so a restart resumes from
    /// before the outage.
    #[tokio::test]
    async fn a_sustained_outage_never_commits() {
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
        ]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        for _ in 0..3 {
            let outcome =
                run_cycle(&mut feed, &reference, &mut state, &TEST_STANOX_CRS, async |_| Err(anyhow::anyhow!("api is down"))).await;
            assert_eq!(outcome, Cycle::Failed);
        }
        assert_eq!(feed.committed_count, 0, "nothing reached api, so nothing may be confirmed");
    }

    /// A feed that errors outright is a failed cycle too, and equally must
    /// not confirm anything.
    #[tokio::test]
    async fn a_batch_that_fails_to_parse_is_a_failed_cycle_and_commits_nothing() {
        let mut feed = FakeMovementFeed::new(vec![vec!["not json at all".to_string()]]);
        let reference = one_pending_pin();
        let mut state = process::ProcessorState::default();

        let outcome = run_cycle(&mut feed, &reference, &mut state, &TEST_STANOX_CRS, async |_| Ok(())).await;

        assert_eq!(outcome, Cycle::Failed);
        assert_eq!(feed.committed_count, 0);
    }
}
