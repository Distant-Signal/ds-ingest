//! Thin HTTP client wrappers -- deliberately separate from correlation
//! logic, same reasoning as trust-consumer's own queries.rs module doc:
//! keeps correlation logic unit-testable without a live api.

pub async fn fetch_line_population(
    client: &reqwest::Client,
    url: &str,
    tokens: &common::oauth_client::OAuthTokenCache,
    line_id: &str,
    service_date: chrono::NaiveDate,
) -> anyhow::Result<Option<serde_json::Value>> {
    let token = tokens.get_token(client).await?;
    let response = client
        .get(url)
        .query(&[
            ("line_id", line_id),
            ("service_date", &service_date.to_string()),
        ])
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json().await?)
}

pub async fn fetch_stanox_crs(
    client: &reqwest::Client,
    url: &str,
    tokens: &common::oauth_client::OAuthTokenCache,
) -> anyhow::Result<Vec<common::StanoxCrsRecord>> {
    common::ingest::get_json(client, url, tokens).await
}

pub async fn post_full_coverage_stats(
    client: &reqwest::Client,
    url: &str,
    tokens: &common::oauth_client::OAuthTokenCache,
    rows: &[common::FullCoverageLineStatsRow],
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    common::ingest::post_batch(client, url, tokens, rows, "full-coverage line stats").await
}

/// Posts to the OTHER chain's own endpoint (`POST /private/station-full-coverage-samples`),
/// owned by `docs/superpowers/plans/2026-09-04-per-station-full-coverage-stats-plan.md`
/// -- this crate is only ever an HTTP client of it, never its
/// route/migration owner (see this plan's Non-goals). Takes the real
/// `common::StationFullCoverageSample` (both branches are now merged).
/// Encodes via a local `Wire` struct rather than deriving `Serialize`
/// directly on `common::StationFullCoverageSample`, since that type has no
/// other reason to depend on `serde` derives itself.
pub async fn post_station_full_coverage_samples(
    client: &reqwest::Client,
    url: &str,
    tokens: &common::oauth_client::OAuthTokenCache,
    samples: &[common::StationFullCoverageSample],
) -> anyhow::Result<()> {
    if samples.is_empty() {
        return Ok(());
    }
    #[derive(serde::Serialize)]
    struct Wire<'a> {
        crs: &'a str,
        operator: &'a str,
        resolved_at: chrono::DateTime<chrono::Utc>,
        stats: &'a common::SampleStats,
    }
    let wire: Vec<Wire> = samples
        .iter()
        .map(|s| Wire {
            crs: &s.crs,
            operator: &s.operator,
            resolved_at: s.resolved_at,
            stats: &s.stats,
        })
        .collect();
    common::ingest::post_batch(client, url, tokens, &wire, "station full-coverage samples").await
}
