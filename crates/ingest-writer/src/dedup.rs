//! `ingest_dedup`: the writer's idempotency keys (spec §7.4, plan 3a.3).
//!
//! Every entry applied in `apply` mode inserts its envelope `key` here **in
//! the same transaction** as its write ([`claim`]). A key already present
//! means the entry was applied before (a lost XACK, an XAUTOCLAIM after a
//! crash): the writer rolls back, reports `Duplicate` and acks it. Rows
//! older than [`RETENTION`] are pruned every [`PRUNE_INTERVAL`] by the
//! [`prune_loop`], under its advisory lock.

use std::time::Duration;

use common::advisory_locks;
use ds_store::loops::LoopSpec;
use sqlx::{PgConnection, PgPool};

/// How long a key is kept (spec §7.4: 48 h). A redelivery of an applied
/// entry follows its apply closely: the entry stays in this consumer's PEL
/// and is re-read first, or is claimed by the next pod after 5 minutes.
pub const RETENTION: Duration = Duration::from_secs(48 * 3600);

/// How often the prune runs.
pub const PRUNE_INTERVAL: Duration = Duration::from_secs(3600);

/// Inserts `key` for `stream`. `true` if this transaction claimed it,
/// `false` if it was already applied on `stream`.
///
/// A key is scoped to its stream (security review L2): one stream's
/// producer cannot mark another stream's entry applied by sending its key.
/// The conflict target is the unique index
/// `20261009160100_ingest_dedup_stream_key_index.sql` built, which
/// `20261009170100_ingest_dedup_stream_key_primary_key.sql` (the contract
/// step) made the primary key in place of the old one on `key` alone.
/// Before that step, a key already claimed on another stream was a unique
/// violation (dead-lettered); after it, it is claimed on each stream.
pub async fn claim(conn: &mut PgConnection, key: &str, stream: &str) -> sqlx::Result<bool> {
    let claimed: Option<i32> = sqlx::query_scalar(
        "INSERT INTO ingest_dedup (key, stream, applied_at) VALUES ($1, $2, now()) \
         ON CONFLICT (stream, key) DO NOTHING RETURNING 1",
    )
    .bind(key)
    .bind(stream)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(claimed.is_some())
}

/// Deletes the keys applied more than `retention` ago. Returns how many.
pub async fn prune(pool: &PgPool, retention: Duration) -> sqlx::Result<u64> {
    let result = sqlx::query(
        "DELETE FROM ingest_dedup WHERE applied_at < now() - make_interval(secs => $1)",
    )
    .bind(retention.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// The hourly prune, under [`advisory_locks::INGEST_DEDUP_PRUNE`].
pub fn prune_loop(interval: Duration) -> LoopSpec {
    LoopSpec::new(
        advisory_locks::INGEST_DEDUP_PRUNE,
        interval,
        |pool| async move {
            match prune(&pool, RETENTION).await {
                Ok(deleted) => {
                    tracing::info!(deleted, "pruned ingest_dedup");
                    Ok(())
                }
                Err(err) => {
                    tracing::error!(error = ?err, "ingest_dedup prune failed; will retry next interval");
                    Err(err.into())
                }
            }
        },
    )
}
