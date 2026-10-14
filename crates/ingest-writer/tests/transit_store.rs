//! `ds_store::transit`'s write side (transit plan 1.5, spec §9.2) against a
//! live, migrated database (`DATABASE_URL`; `cargo test -p ingest-writer
//! --test transit_store -- --ignored --test-threads=1`). Here rather than in
//! ds-store: CI runs ds-store's DB suite as the api role and this crate's as
//! the writer (`DATABASE_URL_WRITER`, scripts/test-postgres-roles.py
//! --mode per-service), so these prove the writer's grants are enough.
//!
//! Each call runs in its own transaction, as the writer applies an entry.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code: a panic is the right failure in a test"
)]

mod transit_fixture;

use chrono::{DateTime, TimeDelta, Utc};
use common::transit::{TimetableCommit, TimetablePart, TimetablePartKind, TimetableRows};
use ds_store::transit::{CommitOutcome, PartIndex, StageOutcome, commit, stage_part};
use transit_fixture::{Db, Feed, NETWORK, now_millis};

async fn stage(db: &Db, part: &TimetablePart, index: PartIndex, at: DateTime<Utc>) -> StageOutcome {
    let mut tx = db.pool.begin().await.unwrap();
    let outcome = stage_part(&mut tx, part, index, at).await.unwrap();
    tx.commit().await.unwrap();
    outcome
}

async fn commit_at(db: &Db, manifest: &TimetableCommit, at: DateTime<Utc>) -> CommitOutcome {
    let mut tx = db.pool.begin().await.unwrap();
    let outcome = commit(&mut tx, manifest, at).await.unwrap();
    tx.commit().await.unwrap();
    outcome
}

/// Stages every part of `feed` but those of `skip`, at `at`.
async fn stage_feed(
    db: &Db,
    feed: &Feed,
    at: DateTime<Utc>,
    skip: Option<(TimetablePartKind, u32)>,
) {
    for (part, index) in &feed.parts {
        if skip == Some((part.rows.kind(), index.part)) {
            continue;
        }
        let outcome = stage(db, part, *index, at).await;
        assert_eq!(
            outcome,
            StageOutcome::Staged {
                rows: part.rows.len()
            }
        );
    }
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_staged_feed_activates_and_an_unchanged_one_writes_only_freshness() {
    let db = Db::new().await;
    let at = now_millis() - TimeDelta::minutes(10);
    let feed = Feed::new("v1-aaaa", 'a', 7, at);
    stage_feed(&db, &feed, at, None).await;
    // Staged, but nothing is active yet.
    assert_eq!(db.active().await, None);
    assert_eq!(
        commit_at(&db, &feed.commit, at).await,
        CommitOutcome::Activated
    );

    let (active, activated_at) = db.active().await.unwrap();
    assert_eq!(active.as_deref(), Some("v1-aaaa"));
    assert_eq!(db.versions().await, [("v1-aaaa".into(), "active".into())]);
    assert_eq!(db.trips("v1-aaaa").await, 7);
    assert_eq!(db.freshness().await, Some(at));
    let links: Vec<(String, String)> =
        sqlx::query_as("SELECT stop_id, crs FROM transit_rail_links WHERE network = $1")
            .bind(NETWORK)
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(links, [("9400ZZMAPIC".into(), "MAN".into())]);

    // The same feed again a day later (the producer's daily re-send): its
    // parts are not re-staged, and the commit only moves freshness.
    let later = at + TimeDelta::minutes(5);
    for (part, index) in &feed.parts {
        assert_eq!(
            stage(&db, part, *index, later).await,
            StageOutcome::NotStaging
        );
    }
    assert_eq!(
        commit_at(&db, &feed.commit, later).await,
        CommitOutcome::Unchanged
    );
    assert_eq!(db.active().await, Some((active, activated_at)));
    assert_eq!(db.freshness().await, Some(later));

    // The same content under a new source version: unchanged, and its
    // staged copy is deleted.
    let renamed = Feed::new("v1b-aaaa", 'a', 7, at);
    stage_feed(&db, &renamed, later, None).await;
    assert_eq!(
        commit_at(&db, &renamed.commit, later).await,
        CommitOutcome::Unchanged
    );
    assert_eq!(db.versions().await, [("v1-aaaa".into(), "active".into())]);
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_missing_part_leaves_the_old_version_active() {
    let db = Db::new().await;
    let at = now_millis() - TimeDelta::minutes(10);
    let v1 = Feed::new("v1-aaaa", 'a', 4, at);
    stage_feed(&db, &v1, at, None).await;
    assert_eq!(
        commit_at(&db, &v1.commit, at).await,
        CommitOutcome::Activated
    );

    let later = at + TimeDelta::minutes(1);
    let v2 = Feed::new("v2-bbbb", 'b', 6, later);
    stage_feed(&db, &v2, later, Some((TimetablePartKind::Trips, 2))).await;
    let CommitOutcome::Incomplete(why) = commit_at(&db, &v2.commit, later).await else {
        panic!("a missing trips part must be incomplete");
    };
    assert!(why.contains("trips: 1 of 2"), "{why}");
    assert_eq!(db.active().await.unwrap().0.as_deref(), Some("v1-aaaa"));
    assert_eq!(db.freshness().await, Some(at));

    // A commit nothing was staged for.
    let v3 = Feed::new("v3-cccc", 'c', 1, later);
    assert!(matches!(
        commit_at(&db, &v3.commit, later).await,
        CommitOutcome::Incomplete(_)
    ));
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_third_feed_prunes_the_first_and_an_older_commit_changes_nothing() {
    let db = Db::new().await;
    let t1 = now_millis() - TimeDelta::minutes(30);
    let t2 = t1 + TimeDelta::minutes(10);
    let t3 = t2 + TimeDelta::minutes(10);
    let feeds = [
        (Feed::new("v1-aaaa", 'a', 3, t1), t1),
        (Feed::new("v2-bbbb", 'b', 4, t2), t2),
        (Feed::new("v3-cccc", 'c', 5, t3), t3),
    ];
    for (feed, at) in &feeds {
        stage_feed(&db, feed, *at, None).await;
        assert_eq!(
            commit_at(&db, &feed.commit, *at).await,
            CommitOutcome::Activated
        );
    }
    assert_eq!(
        db.versions().await,
        [
            ("v2-bbbb".into(), "previous".into()),
            ("v3-cccc".into(), "active".into()),
        ]
    );
    // v1's rows went with it (the cascade).
    assert_eq!(db.trips("v1-aaaa").await, 0);
    assert_eq!(db.trips("v2-bbbb").await, 4);

    // v1 re-delivered late (produced before the active v3): ignored.
    let (v1, _) = &feeds[0];
    stage_feed(&db, v1, t1, None).await;
    assert_eq!(commit_at(&db, &v1.commit, t1).await, CommitOutcome::Older);
    assert_eq!(db.active().await.unwrap().0.as_deref(), Some("v3-cccc"));
    assert_eq!(db.freshness().await, Some(t3));
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn an_inconsistent_feed_is_refused() {
    let db = Db::new().await;
    let at = now_millis() - TimeDelta::minutes(10);

    // A trip naming a pattern that does not exist.
    let mut dangling = Feed::new("v1-aaaa", 'a', 2, at);
    for (part, _) in &mut dangling.parts {
        if let TimetableRows::Trips(trips) = &mut part.rows {
            for trip in trips.iter_mut() {
                trip.pattern_id = "nope".to_owned();
            }
        }
    }
    stage_feed(&db, &dangling, at, None).await;
    let CommitOutcome::Inconsistent(why) = commit_at(&db, &dangling.commit, at).await else {
        panic!("a dangling pattern must be inconsistent");
    };
    assert!(why.contains("pattern is missing"), "{why}");

    // Row counts off the manifest.
    let mut short = Feed::new("v2-bbbb", 'b', 2, at);
    short.commit.counts.trips = 3;
    stage_feed(&db, &short, at, None).await;
    let CommitOutcome::Inconsistent(why) = commit_at(&db, &short.commit, at).await else {
        panic!("a count mismatch must be inconsistent");
    };
    assert!(why.contains("manifest"), "{why}");

    // No service today (the service days are a week old).
    let week_ago = at - TimeDelta::days(7);
    let stale = Feed::new("v3-cccc", 'c', 2, week_ago);
    stage_feed(&db, &stale, at, None).await;
    let CommitOutcome::Inconsistent(why) = commit_at(&db, &stale.commit, at).await else {
        panic!("no service today must be inconsistent");
    };
    assert!(why.contains("no service on"), "{why}");

    assert_eq!(db.active().await, None);
    assert_eq!(db.freshness().await, None);
    db.cleanup().await;
}
