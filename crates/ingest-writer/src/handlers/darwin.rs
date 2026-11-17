//! `darwin-pushport/1` (stream `ds:ingest:darwin`, DS-152): the body is a
//! [`common::darwin::Batch`], a few seconds of Darwin Push Port messages
//! from darwin-relay, written by [`ds_store::darwin::apply`] and recorded
//! in `ingest_freshness` as `darwin-pushport` at the batch's newest Darwin
//! timestamp, clamped like every other observed time ([`freshness_at`]).
//!
//! Every row is guarded by its Darwin timestamp, so a duplicate entry
//! (which the dedup key absorbs anyway) or an older one writes nothing. That
//! guard (ds-core's `ds_store::darwin`) has no future arm of its own, so the
//! handler keeps a future timestamp out of the table: each row's `ts` is
//! clamped to the writer's `now() + 2 min` ([`clamp_batch`]), and a stored
//! timestamp already further ahead (written before the clamp, or by a
//! producer that bypassed it) is brought back before the batch applies
//! ([`heal_future_rows`]), so one skewed message cannot pin a row and make
//! every later genuine update lose the guard. A
//! batch the relay split for size arrives as several entries, each a valid
//! batch on its own.

use chrono::{DateTime, Utc};
use common::darwin::Batch;
use ingest_stream::{HandlerError, StreamEntry};
use sqlx::PgConnection;

use super::{Applied, BoxFuture, SchemaHandler, classify_anyhow, count_apply, decode};
use crate::observed::{MAX_AHEAD_SQL, Observed};

/// The `darwin-pushport/1` handler.
pub struct DarwinPushport;

impl SchemaHandler for DarwinPushport {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        entry: &'a StreamEntry,
        observed: &'a Observed,
    ) -> BoxFuture<'a, Result<Applied, HandlerError>> {
        Box::pin(async move {
            let mut batch: Batch = decode(entry)?;
            if batch.is_empty() {
                return Ok(Applied::All);
            }
            clamp_batch(&mut batch, observed);
            heal_future_rows(conn, &batch)
                .await
                .map_err(|err| classify_anyhow(&err))?;
            let applied = ds_store::darwin::apply(conn, &batch)
                .await
                .map_err(|err| classify_anyhow(&err))?;
            count_apply(entry, batch.len(), applied.total());
            record_freshness(conn, freshness_at(&batch, observed))
                .await
                .map_err(|err| classify_anyhow(&err))?;
            tracing::debug!(
                key = %entry.envelope.key,
                rows = batch.len(),
                services = applied.services,
                calls = applied.calls,
                associations = applied.associations,
                deactivated = applied.deactivated,
                "applied a Darwin Push Port batch"
            );
            Ok(Applied::All)
        })
    }
}

/// Clamps every row's Darwin `ts` in `batch` to the writer's
/// `now() + 2 min`, each clamp counted in
/// `ingest_stream_observed_at_clamped_total` ([`Observed::observed_at`]).
/// Times at or before the limit are untouched.
pub fn clamp_batch(batch: &mut Batch, observed: &Observed) {
    let clamp = |ts: &mut DateTime<Utc>| *ts = observed.observed_at(Some(*ts));
    for row in &mut batch.services {
        clamp(&mut row.ts);
    }
    for row in &mut batch.schedule_calls {
        clamp(&mut row.ts);
    }
    for row in &mut batch.status_calls {
        clamp(&mut row.ts);
    }
    for row in &mut batch.associations {
        clamp(&mut row.ts);
    }
    for row in &mut batch.deactivated {
        clamp(&mut row.ts);
    }
}

/// Where a healed timestamp is put: this long before the writer's clock, so
/// the genuine update that follows (a message a little behind the clock)
/// wins ds-core's `EXCLUDED.ts >= stored` guard.
const HEAL_BACK_SQL: &str = "interval '1 hour'";

/// The healing arm of the Darwin timestamp guards, for the rows `batch`
/// touches: a stored `message_ts`, `schedule_ts` or `ts_ts` more than
/// 2 minutes in the future (a skewed message from before [`clamp_batch`])
/// is set back to `now() - 1 h`, so this batch's genuine row is not refused
/// by `EXCLUDED.ts >= stored` until the clock catches up. This is what the
/// snapshot handlers get from [`crate::observed::guard`]; the Darwin SQL
/// lives in ds-core, so the healing is a separate `UPDATE` here, scoped to
/// the batch's RIDs, that touches only rows that are in the future.
async fn heal_future_rows(conn: &mut PgConnection, batch: &Batch) -> anyhow::Result<()> {
    let mut rids: Vec<&str> = batch
        .services
        .iter()
        .map(|r| r.rid.as_str())
        .chain(batch.schedule_calls.iter().map(|r| r.key.rid.as_str()))
        .chain(batch.status_calls.iter().map(|r| r.key.rid.as_str()))
        .chain(batch.deactivated.iter().map(|r| r.rid.as_str()))
        .collect();
    rids.sort_unstable();
    rids.dedup();
    let mut main_rids: Vec<&str> = batch
        .associations
        .iter()
        .map(|r| r.main_rid.as_str())
        .collect();
    main_rids.sort_unstable();
    main_rids.dedup();
    let limit = format!("now() + {MAX_AHEAD_SQL}");
    let back = format!("now() - {HEAL_BACK_SQL}");
    let heal = |column: &str| {
        format!("{column} = CASE WHEN {column} > {limit} THEN {back} ELSE {column} END")
    };
    let future = |column: &str| format!("{column} > {limit}");
    let mut healed = 0;
    if !rids.is_empty() {
        healed += sqlx::query(&format!(
            "UPDATE darwin_services SET {}, {} WHERE rid = ANY($1) AND ({} OR {})",
            heal("message_ts"),
            heal("schedule_ts"),
            future("message_ts"),
            future("schedule_ts"),
        ))
        .bind(&rids)
        .execute(&mut *conn)
        .await?
        .rows_affected();
        healed += sqlx::query(&format!(
            "UPDATE darwin_calls SET {}, {} WHERE rid = ANY($1) AND ({} OR {})",
            heal("schedule_ts"),
            heal("ts_ts"),
            future("schedule_ts"),
            future("ts_ts"),
        ))
        .bind(&rids)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    }
    if !main_rids.is_empty() {
        healed += sqlx::query(&format!(
            "UPDATE darwin_associations SET {} WHERE main_rid = ANY($1) AND {}",
            heal("message_ts"),
            future("message_ts"),
        ))
        .bind(&main_rids)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    }
    if healed > 0 {
        tracing::warn!(
            healed,
            "healed Darwin rows stamped in the future so this batch's updates apply"
        );
    }
    Ok(())
}

/// The freshness marker of `batch`: its newest Darwin `ts` (else the
/// entry's `produced_at`), clamped to the writer's `now() + 2 min` and the
/// clamp counted ([`Observed::observed_at`]). Unclamped, one message
/// stamped in the future would hold `darwin-pushport` in the future
/// through `record_ingest`'s `GREATEST`, and the staleness alert would stay
/// silent however long the feed then stopped.
pub fn freshness_at(batch: &Batch, observed: &Observed) -> DateTime<Utc> {
    observed.observed_at(batch.newest_ts())
}

/// Records `darwin-pushport` at `at`. A marker already more than 2 minutes
/// in the future (written before the clamp) is first brought back to `at`,
/// the observed-time guard's healing arm ([`crate::observed::guard`]):
/// `record_ingest`'s `GREATEST` would otherwise keep it until the clock
/// caught up.
async fn record_freshness(conn: &mut PgConnection, at: DateTime<Utc>) -> anyhow::Result<()> {
    sqlx::query(&format!(
        "UPDATE ingest_freshness SET fetched_at = $2 \
         WHERE source = $1 AND fetched_at > now() + {MAX_AHEAD_SQL}"
    ))
    .bind(common::darwin::FRESHNESS_SOURCE)
    .bind(at)
    .execute(&mut *conn)
    .await?;
    ds_store::freshness::record_ingest(conn, common::darwin::FRESHNESS_SOURCE, Some(at)).await
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeDelta};
    use common::darwin::Service;

    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().to_utc()
    }

    fn batch(ts: DateTime<Utc>) -> Batch {
        Batch {
            services: vec![Service {
                rid: "202611168Z80001".to_owned(),
                uid: "Z80001".to_owned(),
                ssd: NaiveDate::from_ymd_opt(2026, 11, 16).unwrap(),
                schedule: None,
                ts,
            }],
            ..Batch::default()
        }
    }

    fn observed(produced_at: DateTime<Utc>, now: DateTime<Utc>) -> Observed {
        Observed::at("ds:ingest:darwin", "darwin-pushport/1", produced_at, now)
    }

    #[test]
    fn a_future_darwin_time_is_clamped() {
        let now = at("2026-11-16T12:00:00Z");
        assert_eq!(
            freshness_at(&batch(now + TimeDelta::days(1)), &observed(now, now)),
            now + TimeDelta::minutes(2)
        );
    }

    #[test]
    fn every_row_time_is_clamped_and_past_ones_kept() {
        let now = at("2026-11-16T12:00:00Z");
        let far = now + TimeDelta::days(400);
        let past = now - TimeDelta::seconds(30);
        let mut b = batch(far);
        b.deactivated.push(common::darwin::Deactivated {
            rid: "202611168Z80001".to_owned(),
            ts: far,
        });
        b.deactivated.push(common::darwin::Deactivated {
            rid: "202611168Z80002".to_owned(),
            ts: past,
        });
        clamp_batch(&mut b, &observed(now, now));
        let limit = now + TimeDelta::minutes(2);
        assert_eq!(b.services[0].ts, limit);
        assert_eq!(b.deactivated[0].ts, limit);
        assert_eq!(b.deactivated[1].ts, past);
        assert_eq!(b.newest_ts(), Some(limit));
    }

    #[test]
    fn a_past_darwin_time_is_kept() {
        let now = at("2026-11-16T12:00:00Z");
        let ts = now - TimeDelta::seconds(30);
        assert_eq!(freshness_at(&batch(ts), &observed(now, now)), ts);
    }

    #[test]
    fn a_batch_without_a_time_uses_produced_at_clamped() {
        let now = at("2026-11-16T12:00:00Z");
        assert_eq!(
            freshness_at(&Batch::default(), &observed(now + TimeDelta::hours(1), now)),
            now + TimeDelta::minutes(2)
        );
    }
}
