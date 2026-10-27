//! Row-level security on `ingest_freshness` (security review L4,
//! 2026-10-08), as each role: the migration
//! `20261009170000_ingest_freshness_rls.sql` (RLS on, plus a permissive
//! policy for everyone) and each producer's RESTRICTIVE write policies from
//! `postgres-grants.sql` (db-grants.yaml `row_policies`).
//!
//! Needs the per-service roles, so it runs under
//! `scripts/test-postgres-roles.py --mode per-service` (CI's per-service
//! step runs every `ingest-writer` DB test that way), which sets
//! `DATABASE_URL_<ROLE>` for the writer, stations, incidents, api and
//! aggregator roles. Without them it says so and passes: a plain superuser
//! connection is exempt from the policies, so there is nothing to check.
//!
//! Every write runs in a transaction that is rolled back, so the real
//! freshness rows are never moved.
//!
//! ```text
//! uv run scripts/test-postgres-roles.py --mode per-service -- \
//!   cargo test -p ingest-writer --test freshness_rls -- --ignored
//! ```

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure in a test"
)]

use sqlx::PgPool;

async fn pool(var: &str) -> Option<PgPool> {
    let url = std::env::var(var).ok()?;
    Some(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap_or_else(|err| panic!("connect with {var}: {err}")),
    )
}

/// SQLSTATE of a failed call, or `None` if it succeeded.
fn sqlstate<T>(result: &anyhow::Result<T>) -> Option<String> {
    match result {
        Ok(_) => None,
        Err(err) => Some(
            err.downcast_ref::<sqlx::Error>()
                .and_then(sqlx::Error::as_database_error)
                .and_then(|db| db.code().map(std::borrow::Cow::into_owned))
                .unwrap_or_else(|| format!("not a database error: {err:#}")),
        ),
    }
}

/// `record_ingest(source)` as `pool`'s role, rolled back.
async fn record(pool: &PgPool, source: &str) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let result = ds_store::freshness::record_ingest(&mut tx, source, None).await;
    tx.rollback().await?;
    result
}

/// A plain `UPDATE` of `source`'s row as `pool`'s role, rolled back: the
/// rows it touched.
async fn update(pool: &PgPool, source: &str) -> u64 {
    let mut tx = pool.begin().await.unwrap();
    let done = sqlx::query(
        "UPDATE ingest_freshness SET fetched_at = fetched_at + interval '1 day' WHERE source = $1",
    )
    .bind(source)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    done.rows_affected()
}

async fn visible(pool: &PgPool, source: &str) -> bool {
    sqlx::query_scalar::<_, i32>("SELECT 1 FROM ingest_freshness WHERE source = $1")
        .bind(source)
        .fetch_optional(pool)
        .await
        .unwrap()
        .is_some()
}

/// A source no producer owns, present for the whole test.
const FIXTURE: &str = "TEST-L4-not-yours";

/// Each transit network's timetable and situations sources (transit spec
/// §8.5), which the writer records.
fn transit_sources() -> Vec<String> {
    let sources: Vec<String> = common::transit::NETWORKS
        .iter()
        .map(|id| common::transit::NetworkId::parse(id).unwrap())
        .flat_map(|network| {
            [
                common::transit::timetable_freshness_source(&network),
                common::transit::situations_freshness_source(&network),
            ]
        })
        .collect();
    assert!(sources.contains(&"transit-metrolink-situations".to_owned()));
    // Nottingham NET (NET design §8): both sources, though only the
    // timetable has a producer today.
    assert!(sources.contains(&"transit-net-timetable".to_owned()));
    assert!(sources.contains(&"transit-net-situations".to_owned()));
    sources
}

/// Every source the writer records (`ds_store::freshness::record_ingest`
/// from its handlers). A new one belongs here (the `sources::ALL` lists
/// join automatically) and in db-grants.yaml's writer allow-list.
fn writer_sources() -> Vec<String> {
    let sources: Vec<String> = [
        "tfl",
        "tocs",
        common::tfl_stops::FRESHNESS_SOURCE,
        common::tfl_routes::FRESHNESS_SOURCE,
    ]
    .into_iter()
    .chain(ds_store::samples::sources::ALL.iter().copied())
    .chain(
        ds_store::samples::island_of_ireland::sources::ALL
            .iter()
            .copied(),
    )
    .map(str::to_owned)
    .chain(transit_sources())
    .collect();
    // The tracked-stop-boards/1 handler records it after applying the rows
    // (DS-127); without it every tracked-stop entry dead-letters.
    assert!(sources.contains(&ds_store::samples::sources::TRACKED_STOP_BOARDS.to_owned()));
    sources
}

/// `role`'s `ingest_freshness` `command` row policy expression in the
/// rendered postgres-grants.sql: the row policies' `VALUES` rows carry an
/// expression, unlike the column grants' rows (`''` there).
fn rendered_policy<'a>(sql: &'a str, role: &str, command: &str) -> &'a str {
    let key = format!("('ingest_freshness', '{role}', '{command}', '");
    sql.lines()
        .filter_map(|line| line.trim_start().strip_prefix(key.as_str()))
        .find(|rest| !rest.starts_with('\''))
        .unwrap_or_else(|| panic!("no {role} {command} row policy on ingest_freshness"))
}

/// Without a database: every source each producer records is in its
/// RESTRICTIVE allow-list as rendered from db-grants.yaml (the file the
/// grants job applies; `gen-db-grants.py check` keeps it in step), so a
/// new source cannot ship without its grant.
#[test]
fn each_producer_records_only_allow_listed_sources() {
    let sql = include_str!("../../../charts/distant-signal/files/postgres-grants.sql");
    let producers = [
        ("writer", writer_sources()),
        ("stations", vec!["stations".to_owned()]),
        ("incidents", vec!["incidents".to_owned()]),
    ];
    for (role, sources) in producers {
        for command in ["INSERT", "UPDATE"] {
            let policy = rendered_policy(sql, role, command);
            for source in &sources {
                assert!(
                    policy.contains(&format!("''{source}''")),
                    "{role} records {source:?} but its ingest_freshness {command} \
                     policy (db-grants.yaml row_policies) does not allow it"
                );
            }
        }
    }
}

/// The writer may record `tracked-stop-boards`: the tracked-stop-boards/1
/// handler does after applying each entry's rows, and a refusal (42501,
/// Poison) dead-letters every tracked-stop part.
#[tokio::test]
#[ignore = "requires the per-service roles: run under scripts/test-postgres-roles.py --mode per-service"]
async fn the_writer_records_tracked_stop_boards() {
    let Some(writer) = pool("DATABASE_URL_WRITER").await else {
        eprintln!(
            "DATABASE_URL_WRITER not set: run under scripts/test-postgres-roles.py \
             --mode per-service; nothing checked"
        );
        return;
    };
    // Twice in one transaction: the insert, then the upsert onto its row.
    // Rolled back, so the real row never moves.
    let mut tx = writer.begin().await.unwrap();
    for _ in 0..2 {
        ds_store::freshness::record_ingest(
            &mut tx,
            ds_store::samples::sources::TRACKED_STOP_BOARDS,
            None,
        )
        .await
        .unwrap();
    }
    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the per-service roles: run under scripts/test-postgres-roles.py --mode per-service"]
async fn each_producer_writes_only_its_own_freshness_rows() {
    let (Some(writer), Some(stations), Some(incidents), Some(api), Some(aggregator)) = (
        pool("DATABASE_URL_WRITER").await,
        pool("DATABASE_URL_STATIONS").await,
        pool("DATABASE_URL_INCIDENTS").await,
        pool("DATABASE_URL_API").await,
        pool("DATABASE_URL_AGGREGATOR").await,
    ) else {
        eprintln!(
            "DATABASE_URL_WRITER/_STATIONS/_INCIDENTS/_API/_AGGREGATOR not set: run under \
             scripts/test-postgres-roles.py --mode per-service; nothing checked"
        );
        return;
    };
    let policies: Vec<String> = sqlx::query_scalar(
        "SELECT policyname::text FROM pg_policies \
         WHERE tablename = 'ingest_freshness' AND permissive = 'RESTRICTIVE' ORDER BY 1",
    )
    .fetch_all(&api)
    .await
    .unwrap();
    for role in ["incidents", "stations", "writer"] {
        for command in ["delete", "insert", "update"] {
            let name = format!("ds_grants_{role}_{command}");
            assert!(policies.contains(&name), "{name} missing from {policies:?}");
        }
    }

    // The fixture row, written as the api (unrestricted, as before).
    let mut tx = api.begin().await.unwrap();
    ds_store::freshness::record_ingest(&mut tx, FIXTURE, None)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Each narrow role: its own source(s) succeed ...
    let writer_sources = writer_sources();
    let writer_sources: Vec<&str> = writer_sources.iter().map(String::as_str).collect();
    let own: [(&str, &PgPool, &[&str]); 3] = [
        ("stations", &stations, &["stations"]),
        ("incidents", &incidents, &["incidents"]),
        ("writer", &writer, &writer_sources),
    ];
    for (role, pool, sources) in own {
        for source in sources {
            record(pool, source)
                .await
                .unwrap_or_else(|err| panic!("{role} writing {source}: {err:#}"));
        }
        // ... another feed's new row fails (WITH CHECK) ...
        let other = if role == "stations" {
            "incidents"
        } else {
            "stations"
        };
        let insert = record(pool, other).await;
        assert_eq!(
            sqlstate(&insert).as_deref(),
            Some("42501"),
            "{role} inserting {other}: {insert:?}"
        );
        // ... the upsert onto another feed's existing row fails loudly ...
        let upsert = record(pool, FIXTURE).await;
        assert_eq!(
            sqlstate(&upsert).as_deref(),
            Some("42501"),
            "{role} upserting {FIXTURE}: {upsert:?}"
        );
        // ... a plain UPDATE of it touches nothing ...
        assert_eq!(update(pool, FIXTURE).await, 0, "{role}");
        // ... and every row stays readable (the writer's gauge reads all).
        assert!(visible(pool, FIXTURE).await, "{role} reads every row");
    }

    // The shared island-of-Ireland names from before the per-network split
    // were dropped a release later: the writer may no longer record them.
    for legacy in ["island_of_ireland_stations", "island_of_ireland_lines"] {
        let insert = record(&writer, legacy).await;
        assert_eq!(
            sqlstate(&insert).as_deref(),
            Some("42501"),
            "writer inserting {legacy}: {insert:?}"
        );
    }

    // Every other role is unaffected: the api and the aggregator still
    // update any source's row.
    record(&api, "stations").await.unwrap();
    assert_eq!(update(&api, FIXTURE).await, 1);
    assert_eq!(update(&aggregator, FIXTURE).await, 1);

    let mut tx = api.begin().await.unwrap();
    sqlx::query("DELETE FROM ingest_freshness WHERE source = $1")
        .bind(FIXTURE)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
