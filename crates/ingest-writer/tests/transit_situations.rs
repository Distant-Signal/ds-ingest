//! `ds_store::transit::apply_situations` (transit plan 4.3, spec §9.3,
//! §10.3) against a live, migrated database (`DATABASE_URL`; `cargo test
//! -p ingest-writer --test transit_situations -- --ignored
//! --test-threads=1`). Here rather than in ds-store so that CI's
//! per-service step runs it as the writer, proving its grants and its
//! `line_status` row policy are enough.
//!
//! Each call runs in its own transaction, as the writer applies an entry.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test code: a panic is the right failure in a test"
)]

mod transit_fixture;

use chrono::{DateTime, TimeDelta, Utc};
use common::transit::{
    Period, SituationScope, TransitConsequence, TransitSituation, TransitSituations,
};
use ds_store::transit::{
    CommitOutcome, SituationsApplied, SituationsOutcome, apply_situations, commit, stage_part,
};
use transit_fixture::{Db, Feed, NETWORK, network, now_millis};

/// Activates a one-line timetable (`blue`: Piccadilly, Eccles).
async fn timetable(db: &Db, at: DateTime<Utc>) {
    let feed = Feed::new("v1-aaaa", 'a', 2, at);
    for (part, index) in &feed.parts {
        let mut tx = db.pool.begin().await.unwrap();
        stage_part(&mut tx, part, *index, at).await.unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = db.pool.begin().await.unwrap();
    assert_eq!(
        commit(&mut tx, &feed.commit, at).await.unwrap(),
        CommitOutcome::Activated
    );
    tx.commit().await.unwrap();
    assert!(db.active().await.is_some());
    assert_eq!(db.versions().await.len(), 1);
    assert_eq!(db.trips("v1-aaaa").await, 2);
    assert_eq!(db.freshness().await, Some(at));
}

fn delayed(id: &str, at: DateTime<Utc>) -> TransitSituation {
    TransitSituation {
        situation_id: id.to_owned(),
        version: Some(1),
        progress: "open".to_owned(),
        planned: false,
        reason: Some("miscellaneous:unknown".to_owned()),
        summary: format!("Delays ({id})"),
        description: Some("Trams are delayed.".to_owned()),
        info_url: None,
        validity: vec![Period {
            start: at - TimeDelta::hours(1),
            end: None,
        }],
        publication: Vec::new(),
        consequences: vec![TransitConsequence {
            condition: Some("delayed".to_owned()),
            severity: Some("slight".to_owned()),
            all_lines: false,
            line_ids: vec!["blue".to_owned()],
            unmapped_line_refs: Vec::new(),
            stop_ids: Vec::new(),
            advice: None,
        }],
        line_ids: vec!["blue".to_owned()],
        stop_ids: Vec::new(),
        scope: SituationScope::Line,
    }
}

fn snapshot(situations: Vec<TransitSituation>) -> TransitSituations {
    TransitSituations {
        network: network(),
        situations,
    }
}

async fn apply(db: &Db, body: &TransitSituations, at: DateTime<Utc>) -> SituationsOutcome {
    let mut tx = db.pool.begin().await.unwrap();
    let outcome = apply_situations(&mut tx, body, at).await.unwrap();
    tx.commit().await.unwrap();
    outcome
}

fn applied(outcome: SituationsOutcome) -> SituationsApplied {
    match outcome {
        SituationsOutcome::Applied(applied) => applied,
        SituationsOutcome::Older => panic!("expected the snapshot to apply"),
    }
}

/// `(severity of the first status, computed_at, source_updated_at)`.
async fn blue(db: &Db) -> (i64, DateTime<Utc>, Option<DateTime<Utc>>) {
    sqlx::query_as(
        "SELECT (statuses->0->>'severity')::bigint, computed_at, source_updated_at \
           FROM line_status WHERE line_id = 'metrolink:blue' AND source = 'transit'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

async fn history(db: &Db) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT (statuses->0->>'severity')::bigint FROM line_status_history \
          WHERE line_id = 'metrolink:blue' ORDER BY computed_at, id",
    )
    .fetch_all(&db.admin)
    .await
    .unwrap()
}

async fn situation_ids(db: &Db) -> Vec<String> {
    sqlx::query_scalar("SELECT situation_id FROM transit_situations WHERE network = $1 ORDER BY 1")
        .bind(NETWORK)
        .fetch_all(&db.pool)
        .await
        .unwrap()
}

async fn ended_at(db: &Db, id: &str) -> Option<DateTime<Utc>> {
    sqlx::query_scalar(
        "SELECT ended_at FROM transit_situations WHERE network = $1 AND situation_id = $2",
    )
    .bind(NETWORK)
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

const MINOR_DELAYS: i64 = 9;
const NO_REPORTED_DISRUPTION: i64 = 27;

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn a_snapshot_writes_its_situations_and_the_line_statuses() {
    let db = Db::new().await;
    let at = now_millis() - TimeDelta::minutes(20);
    timetable(&db, at).await;

    let first = snapshot(vec![delayed("S1", at)]);
    let done = applied(apply(&db, &first, at).await);
    assert_eq!(done.situations, 1);
    assert_eq!(done.lines, 1);
    assert_eq!(done.history, 1);
    assert_eq!(situation_ids(&db).await, ["S1"]);
    assert_eq!(blue(&db).await, (MINOR_DELAYS, at, Some(at)));
    let (mode, operators, first_seen): (String, Vec<String>, DateTime<Utc>) = sqlx::query_as(
        "SELECT ls.mode_name, ls.operators, ts.first_seen_at \
           FROM line_status ls, transit_situations ts \
          WHERE ls.line_id = 'metrolink:blue' AND ts.situation_id = 'S1'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(mode, NETWORK);
    assert!(operators.is_empty());
    assert_eq!(first_seen, at);
    let freshness: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT fetched_at FROM ingest_freshness WHERE source = 'transit-metrolink-situations'",
    )
    .fetch_optional(&db.pool)
    .await
    .unwrap();
    assert_eq!(freshness, Some(at));

    // The same snapshot five minutes later: no new history, the times move.
    let later = at + TimeDelta::minutes(5);
    let again = applied(apply(&db, &first, later).await);
    assert_eq!(again.history, 0);
    assert_eq!(history(&db).await, [MINOR_DELAYS]);
    assert_eq!(blue(&db).await, (MINOR_DELAYS, later, Some(later)));
    let first_seen_again: DateTime<Utc> = sqlx::query_scalar(
        "SELECT first_seen_at FROM transit_situations WHERE network = $1 AND situation_id = 'S1'",
    )
    .bind(NETWORK)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(first_seen_again, at, "first_seen_at is kept");

    // An older snapshot (an out-of-order redelivery) changes nothing.
    let stale = snapshot(Vec::new());
    assert_eq!(apply(&db, &stale, at).await, SituationsOutcome::Older);
    assert_eq!(situation_ids(&db).await, ["S1"]);
    assert_eq!(blue(&db).await, (MINOR_DELAYS, later, Some(later)));

    // The situation ends: kept, marked ended (for its page), and the line
    // returns to "No reported disruption" with one more history row.
    let end = later + TimeDelta::minutes(5);
    let ended = applied(apply(&db, &snapshot(Vec::new()), end).await);
    assert_eq!(ended.ended, 1);
    assert_eq!(ended.purged, 0);
    assert_eq!(ended.history, 1);
    assert_eq!(situation_ids(&db).await, ["S1"]);
    assert_eq!(ended_at(&db, "S1").await, Some(end));
    assert_eq!(blue(&db).await, (NO_REPORTED_DISRUPTION, end, Some(end)));
    assert_eq!(history(&db).await, [MINOR_DELAYS, NO_REPORTED_DISRUPTION]);
    // ... and stays there without more history, its end time kept.
    let quiet = applied(apply(&db, &snapshot(Vec::new()), end + TimeDelta::minutes(5)).await);
    assert_eq!(quiet.history, 0);
    assert_eq!(quiet.ended, 0);
    assert_eq!(ended_at(&db, "S1").await, Some(end));
    assert_eq!(history(&db).await.len(), 2);

    // Listed again: current again.
    let back_at = end + TimeDelta::minutes(6);
    let back = applied(apply(&db, &first, back_at).await);
    assert_eq!(back.situations, 1);
    assert_eq!(ended_at(&db, "S1").await, None);

    // Ended for over a week: deleted.
    let gone_at = back_at + TimeDelta::minutes(1);
    applied(apply(&db, &snapshot(Vec::new()), gone_at).await);
    sqlx::query(
        "UPDATE transit_situations SET ended_at = ended_at - interval '8 days' WHERE network = $1",
    )
    .bind(NETWORK)
    .execute(&db.pool)
    .await
    .unwrap();
    let purged = applied(apply(&db, &snapshot(Vec::new()), gone_at + TimeDelta::minutes(1)).await);
    assert_eq!(purged.purged, 1);
    assert!(situation_ids(&db).await.is_empty());
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn the_prune_touches_only_this_networks_transit_rows() {
    let db = Db::new().await;
    let at = now_millis() - TimeDelta::minutes(20);
    timetable(&db, at).await;
    // Rows the writer may not or must not delete, written as the owner: an
    // aggregator line, a TfL line, another network's transit line, and a
    // metrolink line gone from the timetable.
    for (id, mode, source) in [
        ("TEST-SIT-AGG", "national-rail", "aggregator"),
        ("tfl-test-sit", "tube", "tfl"),
        ("othernet:blue", "othernet", "transit"),
        ("metrolink:gone", "metrolink", "transit"),
    ] {
        sqlx::query(
            "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
             VALUES ($1, $1, $2, '{}', '[]', $3)",
        )
        .bind(id)
        .bind(mode)
        .bind(source)
        .execute(&db.admin)
        .await
        .unwrap();
    }

    let done = applied(apply(&db, &snapshot(vec![delayed("S1", at)]), at).await);
    assert_eq!(done.pruned, 1);
    let left: Vec<String> = sqlx::query_scalar(
        "SELECT line_id FROM line_status \
          WHERE line_id IN ('TEST-SIT-AGG', 'tfl-test-sit', 'othernet:blue', 'metrolink:gone', \
                            'metrolink:blue') ORDER BY line_id COLLATE \"C\"",
    )
    .fetch_all(&db.admin)
    .await
    .unwrap();
    assert_eq!(
        left,
        [
            "TEST-SIT-AGG",
            "metrolink:blue",
            "othernet:blue",
            "tfl-test-sit"
        ]
    );
    sqlx::query(
        "DELETE FROM line_status WHERE line_id IN ('TEST-SIT-AGG', 'tfl-test-sit', 'othernet:blue')",
    )
    .execute(&db.admin)
    .await
    .unwrap();
    db.cleanup().await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL (a migrated database)"]
async fn with_no_active_timetable_only_the_situations_are_stored() {
    let db = Db::new().await;
    let at = now_millis() - TimeDelta::minutes(20);
    let done = applied(apply(&db, &snapshot(vec![delayed("S1", at)]), at).await);
    assert_eq!(done.situations, 1);
    assert_eq!(done.lines, 0);
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM line_status WHERE starts_with(line_id, 'metrolink:')",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(rows, 0);
    db.cleanup().await;
}
