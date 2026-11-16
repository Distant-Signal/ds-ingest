//! `darwin-pushport/1` (stream `ds:ingest:darwin`, DS-152): the body is a
//! [`common::darwin::Batch`], a few seconds of Darwin Push Port messages
//! from darwin-relay, written by [`ds_store::darwin::apply`] and recorded
//! in `ingest_freshness` as `darwin-pushport` at the batch's newest Darwin
//! timestamp, clamped like every other observed time ([`freshness_at`]).
//!
//! Every row is guarded by its Darwin timestamp, so a duplicate entry
//! (which the dedup key absorbs anyway) or an older one writes nothing. A
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
            let batch: Batch = decode(entry)?;
            if batch.is_empty() {
                return Ok(Applied::All);
            }
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
