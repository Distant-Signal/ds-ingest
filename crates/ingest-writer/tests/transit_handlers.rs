//! Transit plan 1.6's handlers against a live database (`DATABASE_URL`, a
//! migrated database; `cargo test -p ingest-writer --test transit_handlers
//! -- --ignored --test-threads=1`), through [`WriterHandler`] in `apply`
//! mode with the writer's real [`registry`] on `ds:ingest:transit-metrolink`
//! and Nottingham NET's `ds:ingest:transit-net`: `transit-timetable/1` and
//! `transit-timetable-commit/1`. CI's per-service step runs them as the
//! writer's own role, which proves its grants (and its `ingest_freshness`
//! row policy for `transit-net-timetable`).

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code: a panic is the right failure in a test"
)]

mod transit_fixture;

use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use common::transit::{NetworkId, TimetableCommit, TimetablePart, TimetablePartKind};
use ds_store::transit::PartIndex;
use ingest_stream::envelope::format_produced_at;
use ingest_stream::{
    BatchPart, Envelope, Handled, Handler, HandlerError, SchemaId, StreamEntry, streams,
};
use ingest_writer::handlers::registry;
use ingest_writer::handlers::transit::{INCOMPLETE_FEED_VERSION, NETWORK_NOT_FOR_STREAM};
use ingest_writer::stream::{StreamSpec, WriterHandler};
use transit_fixture::{Db, Feed, now_millis};

/// A unique key prefix per test run, so `ingest_dedup` keys never collide
/// with an earlier run's.
fn run_tag() -> String {
    format!("transit-test-{}", now_millis().timestamp_millis())
}

/// The part's envelope as the producer encodes it (`Snapshot::add_capped`:
/// batch `<produced_at>/<kind>`, key `transit-timetable:<batch>:<i>/<n>`),
/// on Metrolink's stream.
fn part_entry(tag: &str, part: &TimetablePart, index: PartIndex, at: DateTime<Utc>) -> StreamEntry {
    part_entry_on(streams::TRANSIT_METROLINK, tag, part, index, at)
}

/// [`part_entry`] on `stream`.
fn part_entry_on(
    stream: &str,
    tag: &str,
    part: &TimetablePart,
    index: PartIndex,
    at: DateTime<Utc>,
) -> StreamEntry {
    let batch = format!("{tag}/{}/{}", format_produced_at(at), part.rows.kind());
    let schema = SchemaId::new("transit-timetable", 1).unwrap();
    let key = Envelope::snapshot_key(&schema, &batch, index.part, index.parts);
    StreamEntry {
        stream: stream.to_owned(),
        id: "1-0".to_owned(),
        envelope: Envelope::new(schema, "test/pod", key, at, part)
            .unwrap()
            .with_batch(BatchPart {
                batch,
                part: index.part,
                parts: index.parts,
            }),
    }
}

fn commit_entry(tag: &str, commit: &TimetableCommit, at: DateTime<Utc>) -> StreamEntry {
    commit_entry_on(streams::TRANSIT_METROLINK, tag, commit, at)
}

/// [`commit_entry`] on `stream`.
fn commit_entry_on(
    stream: &str,
    tag: &str,
    commit: &TimetableCommit,
    at: DateTime<Utc>,
) -> StreamEntry {
    StreamEntry {
        stream: stream.to_owned(),
        id: "1-0".to_owned(),
        envelope: Envelope::new(
            SchemaId::new("transit-timetable-commit", 1).unwrap(),
            "test/pod",
            format!(
                "transit-timetable-commit:{tag}/{}:1/1",
                format_produced_at(at)
            ),
            at,
            commit,
        )
        .unwrap(),
    }
}

async fn handle(db: &Db, entry: &StreamEntry) -> Result<Handled, HandlerError> {
    let spec = StreamSpec::by_stream(&entry.stream).expect("a writer stream");
    WriterHandler::new(db.pool.clone(), Arc::new(registry()), spec)
        .handle(entry)
        .await
}

async fn applied(db: &Db, entry: &StreamEntry) {
    let result = handle(db, entry).await;
    assert!(
        matches!(result, Ok(Handled::Applied)),
        "{}: {result:?}",
        entry.envelope.key
    );
}

fn poisoned(result: &Result<Handled, HandlerError>, reason: &str) -> bool {
    matches!(result, Err(HandlerError::Poison(got)) if got.starts_with(reason))
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_feed_applied_through_the_stream_activates() {
    let db = Db::new().await;
    let tag = run_tag();
    let at = now_millis() - TimeDelta::minutes(10);
    let feed = Feed::new("v1-aaaa", 'a', 5, at);
    for (part, index) in &feed.parts {
        applied(&db, &part_entry(&tag, part, *index, at)).await;
    }
    applied(&db, &commit_entry(&tag, &feed.commit, at)).await;
    assert_eq!(db.active().await.unwrap().0.as_deref(), Some("v1-aaaa"));
    assert_eq!(db.trips("v1-aaaa").await, 5);
    assert_eq!(db.freshness().await, Some(at));
    assert_eq!(
        db.versions().await,
        [("v1-aaaa".to_owned(), "active".to_owned())]
    );

    // A redelivered part is the runtime's duplicate (ingest_dedup).
    let (part, index) = &feed.parts[0];
    assert!(matches!(
        handle(&db, &part_entry(&tag, part, *index, at)).await,
        Ok(Handled::Duplicate)
    ));
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn an_incomplete_feed_is_dead_lettered_and_the_old_one_stays() {
    let db = Db::new().await;
    let tag = run_tag();
    let at = now_millis() - TimeDelta::minutes(10);
    let v1 = Feed::new("v1-aaaa", 'a', 3, at);
    for (part, index) in &v1.parts {
        applied(&db, &part_entry(&tag, part, *index, at)).await;
    }
    applied(&db, &commit_entry(&tag, &v1.commit, at)).await;

    let later = at + TimeDelta::minutes(1);
    let v2 = Feed::new("v2-bbbb", 'b', 4, later);
    for (part, index) in &v2.parts {
        // One trips part was dead-lettered (or trimmed) on the way.
        if part.rows.kind() == TimetablePartKind::Trips && index.part == 2 {
            continue;
        }
        applied(&db, &part_entry(&tag, part, *index, later)).await;
    }
    let result = handle(&db, &commit_entry(&tag, &v2.commit, later)).await;
    assert!(poisoned(&result, INCOMPLETE_FEED_VERSION), "{result:?}");
    assert_eq!(db.active().await.unwrap().0.as_deref(), Some("v1-aaaa"));
    assert_eq!(db.freshness().await, Some(at));
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn another_networks_body_is_poison_before_any_write() {
    let db = Db::new().await;
    let tag = run_tag();
    let at = now_millis() - TimeDelta::minutes(10);
    let mut feed = Feed::new("v1-aaaa", 'a', 2, at);
    let other = NetworkId::parse("net").unwrap();
    // NET's body on Metrolink's stream (the reverse is below).
    for (part, _) in &mut feed.parts {
        part.network = other.clone();
    }
    feed.commit.network = other;
    for (part, index) in &feed.parts {
        let result = handle(&db, &part_entry(&tag, part, *index, at)).await;
        assert!(poisoned(&result, NETWORK_NOT_FOR_STREAM), "{result:?}");
    }
    let result = handle(&db, &commit_entry(&tag, &feed.commit, at)).await;
    assert!(poisoned(&result, NETWORK_NOT_FOR_STREAM), "{result:?}");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM transit_feed_versions")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
    db.cleanup().await;
}

/// NET design §7, §8: a NET feed on NET's own stream activates as network
/// `net` and records `transit-net-timetable` (which the writer's row
/// policy admits under its own role), leaving Metrolink untouched.
#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_net_feed_applies_on_its_own_stream() {
    let net = Db::for_network("net").await;
    let metrolink = Db::new().await;
    let tag = run_tag();
    let at = now_millis() - TimeDelta::minutes(10);
    let feed = Feed::new("net-v1-aaaa", 'a', 3, at).on_network("net");
    for (part, index) in &feed.parts {
        let entry = part_entry_on(streams::TRANSIT_NET, &tag, part, *index, at);
        applied(&net, &entry).await;
    }
    applied(
        &net,
        &commit_entry_on(streams::TRANSIT_NET, &tag, &feed.commit, at),
    )
    .await;
    assert_eq!(
        net.active().await.unwrap().0.as_deref(),
        Some("net-v1-aaaa")
    );
    assert_eq!(net.trips("net-v1-aaaa").await, 3);
    assert_eq!(net.freshness().await, Some(at));
    assert_eq!(metrolink.active().await, None);
    assert_eq!(metrolink.freshness().await, None);
    net.cleanup().await;
}

/// The pin works both ways: a Metrolink body on NET's stream is poison
/// before any write (NET design §7; transit spec §9.1).
#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_metrolink_body_on_nets_stream_is_poison() {
    let db = Db::new().await;
    let net = Db::for_network("net").await;
    let tag = run_tag();
    let at = now_millis() - TimeDelta::minutes(10);
    let feed = Feed::new("v1-aaaa", 'a', 2, at);
    for (part, index) in &feed.parts {
        let result = handle(
            &db,
            &part_entry_on(streams::TRANSIT_NET, &tag, part, *index, at),
        )
        .await;
        assert!(poisoned(&result, NETWORK_NOT_FOR_STREAM), "{result:?}");
    }
    let result = handle(
        &db,
        &commit_entry_on(streams::TRANSIT_NET, &tag, &feed.commit, at),
    )
    .await;
    assert!(poisoned(&result, NETWORK_NOT_FOR_STREAM), "{result:?}");
    assert_eq!(db.active().await, None);
    assert_eq!(net.active().await, None);
    db.cleanup().await;
}
