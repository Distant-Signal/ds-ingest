//! The schedule population reload, off the consume loop.
//!
//! **Why a background task** (2026-09-27 full-coverage lag review): the
//! reload used to run inline in the consume loop, fetching 243 lines x 2
//! dates one after another. Before the `ETag` change that blocked
//! consumption for about 2 minutes every ~7 minutes (the 1-1.5k lag
//! sawtooth), and a cold start or an `ETag` miss still does. It now runs
//! here, building each new snapshot aside and swapping it in atomically
//! ([`SharedPopulation`], an `ArcSwap`), so the loop only ever reads a
//! complete snapshot and never waits on `api`.
//!
//! **Why consumption waits for the first load**: the reload used to retry
//! only after the full `population_reload_secs` (300s). When `api` was not
//! up yet at startup (connection refused, seen at 01:24Z on 2026-09-27),
//! up to 5 minutes of events were matched against an empty population and
//! ACKed -- lost for this consumer. The first load is now awaited before
//! anything is consumed (see [`wait_for_first_load`]), and a failed cycle
//! is retried on a short, doubling backoff instead of the full interval --
//! the same idea as the stanox/crs reload's `failed_reload_retry_delay`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::population::{LineGeometry, Population};
use crate::queries;

/// The current population snapshot, swapped whole by the reloader.
pub type SharedPopulation = Arc<ArcSwap<Population>>;
/// The shadow line ids the reloader fetches, refreshed by the consume
/// loop's stanox/crs reload.
pub type SharedLineIds = Arc<ArcSwap<Vec<String>>>;
/// line_id -> [`LineGeometry`], refreshed by the stanox/crs reload. Empty
/// while `FULL_COVERAGE_WINDOWED_STATS` is off, so no trains are reduced
/// (the population is then exactly what it was before windowed stats).
pub type SharedGeometry = Arc<ArcSwap<HashMap<String, Arc<LineGeometry>>>>;

/// What one reload cycle achieved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CycleOutcome {
    /// Every `(line_id, date)` whose fetch failed (transport/HTTP error or
    /// an undeserializable body). Its previous snapshot, if any, was kept.
    pub failed: Vec<(String, chrono::NaiveDate)>,
    /// How many `(line_id, date)` fetches reached `api` and were
    /// understood (200 with a body or `null`, or 304).
    pub succeeded: usize,
}

impl CycleOutcome {
    /// Lines whose fetch for `date` failed.
    pub fn failed_on(&self, date: chrono::NaiveDate) -> Vec<String> {
        self.failed
            .iter()
            .filter(|(_, d)| *d == date)
            .map(|(line, _)| line.clone())
            .collect()
    }
}

/// One reload cycle: `service_date`'s and the next day's population for
/// every line in `line_ids` (Decision 2b -- tomorrow's too, so the rail-day
/// rollover finds it already loaded), each conditional on the `ETag` held
/// in `previous`. Returns a NEW snapshot; `previous` is never mutated, so
/// the consume loop can keep reading it throughout.
///
/// Best-effort per `(line, date)`: a failure keeps that key's previous
/// snapshot and is reported in [`CycleOutcome::failed`]; it never blocks
/// any other line. Every date older than `service_date` is dropped (only
/// today's and tomorrow's are ever read).
pub async fn reload_cycle(
    client: &reqwest::Client,
    url: &str,
    tokens: &common::oauth_client::OAuthTokenCache,
    line_ids: &[String],
    geometry: &HashMap<String, Arc<LineGeometry>>,
    previous: &Population,
    service_date: chrono::NaiveDate,
) -> (Population, CycleOutcome) {
    let mut next = Population::default();
    let mut outcome = CycleOutcome::default();
    let dates = [service_date, service_date + chrono::Duration::days(1)];
    for line_id in line_ids {
        for &date in &dates {
            // The ETag of what we already hold for this key, if `api` sent
            // one: an unchanged population then costs a bodyless 304
            // instead of a full re-download. See `Population::etags`.
            // ...unless the line's geometry changed since that population
            // was reduced: it must then be downloaded and reduced again.
            let geometry_hash = geometry.get(line_id).map_or(0, |g| g.hash);
            let if_none_match = previous.etag_if_current(line_id, date, geometry_hash);
            match queries::fetch_line_population(client, url, tokens, line_id, date, if_none_match)
                .await
            {
                Ok(queries::LinePopulationFetch::NotModified) => {
                    metrics::counter!(
                        common::metrics::metric_name(
                            "full_coverage_consumer_population_reloads_total"
                        ),
                        "result" => "not_modified"
                    )
                    .increment(1);
                    next.carry_over(previous, line_id, date);
                    outcome.succeeded += 1;
                }
                Ok(queries::LinePopulationFetch::Fetched { body, etag }) => {
                    metrics::counter!(
                        common::metrics::metric_name(
                            "full_coverage_consumer_population_reloads_total"
                        ),
                        "result" => "fetched"
                    )
                    .increment(1);
                    // Straight from the body text, one entry at a time,
                    // into uids and (with a geometry) reduced trains -- no
                    // `serde_json::Value` tree and no calling points kept
                    // (see `population::parse_line_population`, which also
                    // leaves rail-replacement buses and ships out). The body
                    // is dropped as soon as it is parsed, so a cold start
                    // holds at most one line's wire payload at a time.
                    let geometry = geometry.get(line_id).map(Arc::as_ref);
                    let parsed = crate::population::parse_line_population(&body, geometry, date);
                    drop(body);
                    match parsed {
                        Ok(Some(pop)) => {
                            next.insert_line_pop(line_id, date, pop, etag);
                            outcome.succeeded += 1;
                        }
                        Ok(None) => {
                            // Nothing published yet for this (line, date) --
                            // Decision 2e's Pending case, upstream of the
                            // rail-day gate. Not an error.
                            outcome.succeeded += 1;
                        }
                        Err(err) => {
                            tracing::error!(error = ?err, line_id = %line_id, %date, "failed to deserialize schedule-line-population response; keeping previous snapshot");
                            metrics::counter!(
                                common::metrics::metric_name("full_coverage_consumer_errors_total"),
                                "operation" => "reload_line_population_deserialize"
                            )
                            .increment(1);
                            next.carry_over(previous, line_id, date);
                            outcome.failed.push((line_id.clone(), date));
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(error = ?err, line_id = %line_id, %date, "failed to fetch schedule line population; keeping previous snapshot");
                    metrics::counter!(
                        common::metrics::metric_name("full_coverage_consumer_errors_total"),
                        "operation" => "reload_line_population_fetch"
                    )
                    .increment(1);
                    next.carry_over(previous, line_id, date);
                    outcome.failed.push((line_id.clone(), date));
                }
            }
        }
    }
    (next, outcome)
}

/// Signalled once, by the first reload cycle that makes the population
/// usable -- see [`Reloader::run`] for exactly when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirstLoad {
    pub service_date: chrono::NaiveDate,
    /// Lines whose population for `service_date` could still not be
    /// fetched when consumption was allowed to begin (only possible after
    /// `initial_wait`). Their rows are marked partial for that day.
    pub missing_lines: Vec<String>,
}

pub struct Reloader {
    pub client: reqwest::Client,
    pub url: String,
    pub tokens: Arc<common::oauth_client::OAuthTokenCache>,
    pub line_ids: SharedLineIds,
    pub geometry: SharedGeometry,
    pub population: SharedPopulation,
    /// Wait after a cycle in which every fetch succeeded.
    pub interval: Duration,
    /// First wait after a cycle with any failure; doubles per consecutive
    /// failing cycle up to `max_retry`.
    pub min_retry: Duration,
    pub max_retry: Duration,
    /// How long the FIRST load may keep failing for some lines (while
    /// others succeed) before consumption is let through anyway, with those
    /// lines marked partial. While EVERY fetch fails (`api` down) the first
    /// load is never signalled, however long that takes.
    pub initial_wait: Duration,
}

impl Reloader {
    /// Spawns the reload loop. The returned receiver turns `Some` once, when
    /// the first load is usable; wait on it with [`wait_for_first_load`].
    pub fn spawn(self) -> tokio::sync::watch::Receiver<Option<FirstLoad>> {
        let (tx, rx) = tokio::sync::watch::channel(None);
        tokio::spawn(self.run(tx));
        rx
    }

    /// The first load is signalled after the first cycle in which every
    /// line's fetch for the CURRENT rail day succeeded -- or, once
    /// `initial_wait` has passed, after a cycle in which at least one did
    /// (some lines persistently failing must not stall every other line
    /// forever; those are reported in [`FirstLoad::missing_lines`]).
    async fn run(self, tx: tokio::sync::watch::Sender<Option<FirstLoad>>) {
        let started = tokio::time::Instant::now();
        let mut retry = self.min_retry;
        loop {
            let service_date = crate::stats::current_rail_service_date(chrono::Utc::now());
            let line_ids = self.line_ids.load_full();
            let geometry = self.geometry.load_full();
            let previous = self.population.load_full();
            let cycle_start = std::time::Instant::now();
            let (next, outcome) = reload_cycle(
                &self.client,
                &self.url,
                &self.tokens,
                &line_ids,
                &geometry,
                &previous,
                service_date,
            )
            .await;
            drop(previous);
            metrics::gauge!(common::metrics::metric_name(
                "full_coverage_consumer_population_uids"
            ))
            .set(next.total_uids() as f64);
            self.population.store(Arc::new(next));
            metrics::histogram!(common::metrics::metric_name(
                "full_coverage_consumer_population_reload_duration_seconds"
            ))
            .record(cycle_start.elapsed().as_secs_f64());

            if tx.borrow().is_none() {
                let missing = outcome.failed_on(service_date);
                let today_ok = line_ids.len().saturating_sub(missing.len());
                let give_up_waiting = started.elapsed() >= self.initial_wait && today_ok > 0;
                if missing.is_empty() || give_up_waiting {
                    if !missing.is_empty() {
                        tracing::error!(
                            %service_date,
                            missing = missing.len(),
                            lines = ?missing,
                            "starting consumption without these lines' populations; their stats are partial for this rail day"
                        );
                    }
                    metrics::gauge!(common::metrics::metric_name(
                        "full_coverage_consumer_population_loaded"
                    ))
                    .set(1.0);
                    tx.send_replace(Some(FirstLoad {
                        service_date,
                        missing_lines: missing,
                    }));
                }
            }

            let wait = if outcome.failed.is_empty() {
                metrics::gauge!(common::metrics::metric_name(
                    "full_coverage_consumer_population_last_success_timestamp_seconds"
                ))
                .set(chrono::Utc::now().timestamp() as f64);
                retry = self.min_retry;
                self.interval
            } else {
                let wait = retry;
                retry = (retry * 2).min(self.max_retry);
                tracing::warn!(
                    failed = outcome.failed.len(),
                    succeeded = outcome.succeeded,
                    retry_in_ms = wait.as_millis() as u64,
                    "population reload cycle had failures; retrying sooner than the normal interval"
                );
                wait
            };
            tokio::time::sleep(wait).await;
        }
    }
}

/// Blocks until the reloader signals its first usable load, beating
/// `progress` meanwhile (the process is alive and retrying; a restart would
/// not make `api` come up any sooner). `Err` only if the reloader task has
/// died, which should be impossible.
pub async fn wait_for_first_load(
    rx: &mut tokio::sync::watch::Receiver<Option<FirstLoad>>,
    progress: &health_http::Progress,
) -> anyhow::Result<FirstLoad> {
    const LOG_EVERY: Duration = Duration::from_secs(10);
    let started = tokio::time::Instant::now();
    loop {
        match tokio::time::timeout(LOG_EVERY, rx.wait_for(Option::is_some)).await {
            Ok(Ok(first)) => {
                return Ok(first.clone().expect("wait_for guarantees Some"));
            }
            Ok(Err(_)) => anyhow::bail!("population reloader task stopped before its first load"),
            Err(_) => {
                progress.beat();
                tracing::info!(
                    waited_secs = started.elapsed().as_secs(),
                    "waiting for the first schedule population load before consuming"
                );
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    pub(crate) async fn mock_token_cache(
        server: &MockServer,
    ) -> common::oauth_client::OAuthTokenCache {
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

    fn url(server: &MockServer) -> String {
        format!("{}/private/schedule-line-population", server.uri())
    }

    /// Conditional reload, end to end through `reload_cycle`: the first
    /// cycle downloads both dates and remembers their `ETag`s; the next
    /// cycle sends them back, gets 304s, and keeps the snapshot it has.
    /// The `.expect(n)` counts are the point -- they are what proves the
    /// second cycle downloaded nothing.
    #[tokio::test]
    async fn reload_cycle_revalidates_with_etags_and_keeps_the_snapshot_on_304() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .and(header("if-none-match", "\"slp-7\""))
            .respond_with(ResponseTemplate::new(304).insert_header("etag", "\"slp-7\""))
            .with_priority(1)
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"slp-7\"")
                    .set_body_string(BODY),
            )
            .with_priority(2)
            .expect(2)
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let lines = vec!["waterloo-reading".to_string()];
        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let tomorrow = today + chrono::Duration::days(1);
        let mut population = Population::default();

        for _ in 0..2 {
            let (next, outcome) = reload_cycle(
                &client,
                &url(&server),
                &tokens,
                &lines,
                &HashMap::new(),
                &population,
                today,
            )
            .await;
            assert!(outcome.failed.is_empty());
            population = next;
            for date in [today, tomorrow] {
                assert_eq!(
                    population.uids_for("waterloo-reading", date),
                    vec!["C11052"]
                );
                assert_eq!(
                    population.etag_for("waterloo-reading", date),
                    Some("\"slp-7\"")
                );
            }
        }
        server.verify().await;
    }

    /// When a line's geometry changes (a stanox/crs reload changed its
    /// TIPLOCs), its held population was reduced against the old one: the
    /// next cycle must download it again, not accept a `304`.
    #[tokio::test]
    async fn a_geometry_change_forces_a_refetch_instead_of_a_304() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .and(header("if-none-match", "\"slp-7\""))
            .respond_with(ResponseTemplate::new(304).insert_header("etag", "\"slp-7\""))
            .with_priority(1)
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"slp-7\"")
                    .set_body_string(BODY),
            )
            .with_priority(2)
            .expect(4)
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let lines = vec!["waterloo-reading".to_string()];
        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let geometry = |tiplocs: &[&str]| {
            let g = LineGeometry::new(
                tiplocs
                    .iter()
                    .map(|t| (t.to_string(), t.to_string()))
                    .collect(),
                Default::default(),
            );
            let mut map = HashMap::new();
            map.insert("waterloo-reading".to_string(), Arc::new(g));
            map
        };
        let before = geometry(&["WATRLMN", "RDNGSTN"]);
        let after = geometry(&["WATRLMN", "RDNGSTN", "WOKING"]);

        let mut population = Population::default();
        // Cycle 1: download. Cycle 2: same geometry, 304s. Cycle 3: the
        // geometry changed, so both dates are downloaded again.
        for g in [&before, &before, &after] {
            let (next, outcome) = reload_cycle(
                &client,
                &url(&server),
                &tokens,
                &lines,
                g,
                &population,
                today,
            )
            .await;
            assert!(outcome.failed.is_empty());
            population = next;
        }
        assert_eq!(
            population
                .line_pop("waterloo-reading", today)
                .unwrap()
                .geometry_hash,
            after["waterloo-reading"].hash
        );
        server.verify().await;
    }

    /// The other compatibility direction: an `api` predating conditional
    /// GET sends no `ETag`, so every cycle is a plain full download, and
    /// the population is still loaded exactly as before.
    #[tokio::test]
    async fn reload_cycle_against_an_api_without_etags_downloads_every_cycle() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .expect(4)
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let lines = vec!["waterloo-reading".to_string()];
        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let mut population = Population::default();

        for _ in 0..2 {
            let (next, _) = reload_cycle(
                &client,
                &url(&server),
                &tokens,
                &lines,
                &HashMap::new(),
                &population,
                today,
            )
            .await;
            population = next;
            assert_eq!(
                population.uids_for("waterloo-reading", today),
                vec!["C11052"]
            );
            assert_eq!(population.etag_for("waterloo-reading", today), None);
        }
        server.verify().await;
    }

    /// Regression test: a rail-replacement bus (`train_status` `5`) or a
    /// ship in the published population is left out, so it can no longer
    /// count as a cancellation; a train, and an entry published without a
    /// status (an older `schedule-reference`), are kept.
    #[tokio::test]
    async fn buses_and_ships_are_left_out_of_the_population() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"[{"uid": "C11052", "calling_points": [], "train_status": "P", "operator_atoc": "SW"},
                    {"uid": "B00001", "calling_points": [], "train_status": "5", "operator_atoc": "SW"},
                    {"uid": "B00002", "calling_points": [], "train_status": "B"},
                    {"uid": "S00001", "calling_points": [], "train_status": "S"},
                    {"uid": "C22222", "calling_points": []}]"#,
            ))
            .mount(&server)
            .await;
        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let (next, outcome) = reload_cycle(
            &reqwest::Client::new(),
            &url(&server),
            &tokens,
            &["waterloo-reading".to_string()],
            &HashMap::new(),
            &Population::default(),
            today,
        )
        .await;
        assert!(outcome.failed.is_empty());
        let mut uids = next.uids_for("waterloo-reading", today);
        uids.sort_unstable();
        assert_eq!(uids, vec!["C11052", "C22222"]);

        let row = crate::stats::build_line_row(
            "waterloo-reading",
            today,
            &uids,
            &std::collections::HashMap::new(),
            true,
            false,
            &common::Defaults::default(),
        );
        assert_eq!(
            row.stats.total, 2,
            "the buses are not in the closed-day row"
        );
    }

    /// A failed fetch keeps the previous snapshot for that key -- a
    /// transient `api` error must not empty a line's population.
    #[tokio::test]
    async fn a_failed_fetch_keeps_the_previous_snapshot_and_is_reported() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let today: chrono::NaiveDate = "2026-09-04".parse().unwrap();
        let mut previous = Population::default();
        previous.insert(
            "waterloo-reading",
            today,
            vec![schedule_query::LinePopulationEntry {
                uid: "C11052".to_string(),
                calling_points: vec![],
                operator_atoc: None,
                train_status: None,
            }],
        );
        let lines = vec!["waterloo-reading".to_string()];
        let (next, outcome) = reload_cycle(
            &reqwest::Client::new(),
            &url(&server),
            &tokens,
            &lines,
            &HashMap::new(),
            &previous,
            today,
        )
        .await;
        assert_eq!(outcome.failed.len(), 2);
        assert_eq!(
            outcome.failed_on(today),
            vec!["waterloo-reading".to_string()]
        );
        assert_eq!(next.uids_for("waterloo-reading", today), vec!["C11052"]);
    }

    fn reloader(
        server: &MockServer,
        tokens: common::oauth_client::OAuthTokenCache,
        lines: &[&str],
    ) -> (Reloader, SharedPopulation) {
        let population: SharedPopulation = Arc::new(ArcSwap::from_pointee(Population::default()));
        let reloader = Reloader {
            client: reqwest::Client::new(),
            url: url(server),
            tokens: Arc::new(tokens),
            line_ids: Arc::new(ArcSwap::from_pointee(
                lines.iter().map(|l| l.to_string()).collect(),
            )),
            geometry: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            population: Arc::clone(&population),
            interval: Duration::from_secs(3600),
            min_retry: Duration::from_millis(20),
            max_retry: Duration::from_millis(100),
            initial_wait: Duration::from_secs(3600),
        };
        (reloader, population)
    }

    /// `api` refusing (here: 503) at startup must not cost a full reload
    /// interval: the reloader retries on its short backoff, and the first
    /// load is only signalled once the fetch really succeeds -- never
    /// against the empty population.
    #[tokio::test]
    async fn a_failed_first_load_retries_quickly_and_is_only_signalled_on_success() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(4) // two failing cycles (today + tomorrow each)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .with_priority(2)
            .mount(&server)
            .await;

        let (reloader, population) = reloader(&server, tokens, &["waterloo-reading"]);
        let mut rx = reloader.spawn();
        let started = std::time::Instant::now();
        let first = wait_for_first_load(
            &mut rx,
            &health_http::Progress::new(Duration::from_secs(60)),
        )
        .await
        .unwrap();

        assert!(first.missing_lines.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "retried on the short backoff, not the {}s interval",
            3600
        );
        let requests = server.received_requests().await.unwrap();
        let gets = requests
            .iter()
            .filter(|r| r.method.as_str() == "GET")
            .count();
        assert_eq!(gets, 6, "two failing cycles, then one that succeeded");
        assert_eq!(
            population
                .load()
                .uids_for("waterloo-reading", first.service_date),
            vec!["C11052"],
            "the population is already in place when the first load is signalled"
        );
    }

    /// While every fetch fails (`api` down), the first load is never
    /// signalled, however long -- consumption must not start against an
    /// empty population.
    #[tokio::test]
    async fn the_first_load_is_never_signalled_while_every_fetch_fails() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let (mut reloader, _) = reloader(&server, tokens, &["waterloo-reading"]);
        reloader.initial_wait = Duration::ZERO;
        let mut rx = reloader.spawn();
        let waited =
            tokio::time::timeout(Duration::from_millis(500), rx.wait_for(Option::is_some)).await;
        assert!(
            waited.is_err(),
            "no first load while api refuses everything"
        );
        let gets = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method.as_str() == "GET")
            .count();
        assert!(
            gets >= 6,
            "and it kept retrying quickly meanwhile (got {gets} GETs)"
        );
    }

    /// One line persistently failing while the others load: once
    /// `initial_wait` has passed, consumption is let through, and that line
    /// is reported so its day can be marked partial.
    #[tokio::test]
    async fn a_persistently_failing_line_is_reported_missing_after_the_initial_wait() {
        let server = MockServer::start().await;
        let tokens = mock_token_cache(&server).await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .and(wiremock::matchers::query_param("line_id", "broken-line"))
            .respond_with(ResponseTemplate::new(500))
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/private/schedule-line-population"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .with_priority(2)
            .mount(&server)
            .await;
        let (mut reloader, _) = reloader(&server, tokens, &["waterloo-reading", "broken-line"]);
        reloader.initial_wait = Duration::from_millis(100);
        let mut rx = reloader.spawn();
        let first = tokio::time::timeout(
            Duration::from_secs(5),
            wait_for_first_load(
                &mut rx,
                &health_http::Progress::new(Duration::from_secs(60)),
            ),
        )
        .await
        .expect("let through after the initial wait")
        .unwrap();
        assert_eq!(first.missing_lines, vec!["broken-line".to_string()]);
    }
}
