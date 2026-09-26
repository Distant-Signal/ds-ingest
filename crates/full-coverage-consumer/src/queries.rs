//! Thin HTTP client wrappers -- deliberately separate from correlation
//! logic, same reasoning as trust-consumer's own queries.rs module doc:
//! keeps correlation logic unit-testable without a live api.

/// Outcome of one [`fetch_line_population`] call that reached `api`.
#[derive(Debug, PartialEq, Eq)]
pub enum LinePopulationFetch {
    /// `304 Not Modified`: the population `if_none_match` described is
    /// still current. Only possible when a validator was sent.
    NotModified,
    /// `200`: the response body as JSON text -- `null` when nothing is
    /// published yet for that `(line, date)` -- plus the `ETag` `api` sent
    /// with it, if any (an `api` predating conditional GET sends none).
    ///
    /// Left as text for the caller to deserialize, so a body that fails
    /// to parse is still counted as a deserialize error, not a fetch error.
    Fetched { body: String, etag: Option<String> },
}

/// `GET /private/schedule-line-population` for one `(line_id,
/// service_date)`, conditional on `if_none_match` when given (the `ETag`
/// of the population this process already holds for that key).
///
/// Backward-compatible both ways by construction: against an `api` that
/// predates conditional GET, `If-None-Match` is just an ignored header and
/// the answer is the same `200` it always was, with no `ETag` to remember,
/// so every later call is unconditional again.
pub async fn fetch_line_population(
    client: &reqwest::Client,
    url: &str,
    tokens: &common::oauth_client::OAuthTokenCache,
    line_id: &str,
    service_date: chrono::NaiveDate,
    if_none_match: Option<&str>,
) -> anyhow::Result<LinePopulationFetch> {
    let token = tokens.get_token(client).await?;
    let mut request = client
        .get(url)
        .query(&[
            ("line_id", line_id),
            ("service_date", &service_date.to_string()),
        ])
        .bearer_auth(&token);
    if let Some(etag) = if_none_match {
        request = request.header(reqwest::header::IF_NONE_MATCH, etag);
    }
    let response = request.send().await?.error_for_status()?;
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(LinePopulationFetch::NotModified);
    }
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response.text().await?;
    Ok(LinePopulationFetch::Fetched { body, etag })
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

#[cfg(test)]
mod tests {
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    async fn mock_token_cache(server: &MockServer) -> common::oauth_client::OAuthTokenCache {
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fake-jwt",
                "expires_in": 300,
            })))
            .mount(server)
            .await;
        common::oauth_client::OAuthTokenCache::new(common::oauth_client::OAuthCredentials {
            token_url: format!("{}/token/", server.uri()),
            client_id: "test-client".to_string(),
            scope: "groups".to_string(),
            username: "test-user".to_string(),
            password: "test-password".to_string(),
        })
    }

    const BODY: &str = r#"[{"uid": "C11052", "calling_points": []}]"#;

    fn date() -> chrono::NaiveDate {
        "2026-09-04".parse().unwrap()
    }

    /// A new consumer against a new `api`: the first fetch is unconditional
    /// and remembers the `ETag`; sending it back gets a bodyless 304.
    #[tokio::test]
    async fn a_matching_etag_gets_not_modified_from_an_api_that_supports_it() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .and(query_param("line_id", "waterloo-reading"))
            .and(query_param("service_date", "2026-09-04"))
            .and(header("if-none-match", "\"slp-42\""))
            .respond_with(ResponseTemplate::new(304).insert_header("etag", "\"slp-42\""))
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"slp-42\"")
                    .insert_header("content-type", "application/json")
                    .set_body_string(BODY),
            )
            .with_priority(2)
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let url = format!("{}/private/schedule-line-population", server.uri());

        let first = fetch_line_population(&client, &url, &tokens, "waterloo-reading", date(), None)
            .await
            .expect("first fetch");
        assert_eq!(
            first,
            LinePopulationFetch::Fetched {
                body: BODY.to_string(),
                etag: Some("\"slp-42\"".to_string()),
            }
        );

        let second = fetch_line_population(
            &client,
            &url,
            &tokens,
            "waterloo-reading",
            date(),
            Some("\"slp-42\""),
        )
        .await
        .expect("conditional fetch");
        assert_eq!(second, LinePopulationFetch::NotModified);
    }

    /// A new consumer against an OLD `api` (one predating conditional GET):
    /// it ignores `If-None-Match` and sends no `ETag`, so the consumer gets
    /// the full body every time and has no validator to remember.
    #[tokio::test]
    async fn an_api_without_etag_support_still_returns_the_full_population() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/json")
                    .set_body_string(BODY),
            )
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let url = format!("{}/private/schedule-line-population", server.uri());

        for if_none_match in [None, Some("\"slp-42\"")] {
            let fetched = fetch_line_population(
                &client,
                &url,
                &tokens,
                "waterloo-reading",
                date(),
                if_none_match,
            )
            .await
            .expect("fetch");
            assert_eq!(
                fetched,
                LinePopulationFetch::Fetched {
                    body: BODY.to_string(),
                    etag: None,
                }
            );
        }
    }

    /// A `null` body (nothing published yet) is still a successful fetch.
    #[tokio::test]
    async fn an_unpublished_population_is_a_null_body_not_an_error() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(ResponseTemplate::new(200).set_body_string("null"))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let url = format!("{}/private/schedule-line-population", server.uri());

        let fetched =
            fetch_line_population(&client, &url, &tokens, "waterloo-reading", date(), None)
                .await
                .expect("fetch");
        let LinePopulationFetch::Fetched { body, etag } = fetched else {
            panic!("expected a 200");
        };
        assert_eq!(etag, None);
        let parsed: Option<Vec<schedule_query::LinePopulationEntry>> =
            serde_json::from_str(&body).unwrap();
        assert!(parsed.is_none());
    }
}
