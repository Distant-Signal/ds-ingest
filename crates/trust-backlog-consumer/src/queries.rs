//! HTTP client calls to `api`.
//!
//! **Deviation from the plan's own sketch, confirmed directly against
//! this codebase rather than assumed**: the plan's own Task 10 Step 1
//! sketch hand-rolls `.bearer_auth(token)` GET/POST calls and calls a
//! `common::oauth_client::OAuthTokenCache::token()` method. Neither
//! matches this codebase's real, current state:
//! `crates/common/src/ingest.rs` already exists (`get_json`/`post_batch`),
//! extracted specifically to be "the single source of truth for the
//! POST-batch-and-log pattern every real caller repeats" (that module's
//! own doc comment) -- `full-coverage-consumer::queries::fetch_stanox_crs`
//! is now a one-line call into it, not a hand-rolled request, and
//! `OAuthTokenCache`'s real method is `get_token(client)`, not `token()`.
//! Using the shared helpers here instead of duplicating a fourth
//! hand-rolled copy of the same GET/POST-bearer-token shape.

pub(crate) async fn fetch_stanox_crs(
    client: &reqwest::Client,
    url: &str,
    tokens: &common::oauth_client::OAuthTokenCache,
) -> anyhow::Result<Vec<common::StanoxCrsRecord>> {
    common::ingest::get_json(client, url, tokens).await
}

/// POSTs one batch and returns `api`'s reply, including any rows it
/// rejected for a data error (see `common::TrustBacklogIngestResponse`).
/// An empty batch is not sent and counts as a clean success.
pub(crate) async fn post_trust_event_backlog(
    client: &reqwest::Client,
    url: &str,
    tokens: &common::oauth_client::OAuthTokenCache,
    events: &[common::TrustBacklogEventMessage],
) -> anyhow::Result<common::TrustBacklogIngestResponse> {
    if events.is_empty() {
        return Ok(common::TrustBacklogIngestResponse::default());
    }
    common::ingest::post_batch_for_response(
        client,
        url,
        tokens,
        events,
        "trust-event-backlog events",
    )
    .await
}

/// `api`'s `/private/train-reasons` URL, derived from the backlog route's
/// URL (`API_INGEST_URL`, `.../private/trust-event-backlog`) so no new
/// setting is needed: the two routes share a host, a prefix and a
/// service-account group. `None` when that URL does not end in
/// `/trust-event-backlog`, in which case reasons are not sent.
pub(crate) fn train_reasons_url(api_ingest_url: &str) -> Option<String> {
    api_ingest_url
        .trim_end_matches('/')
        .strip_suffix("/trust-event-backlog")
        .map(|prefix| format!("{prefix}/train-reasons"))
}

/// POSTs one batch of reason codes. An empty batch is not sent.
pub(crate) async fn post_train_reasons(
    client: &reqwest::Client,
    url: &str,
    tokens: &common::oauth_client::OAuthTokenCache,
    reasons: &[common::TrainReasonMessage],
) -> anyhow::Result<()> {
    if reasons.is_empty() {
        return Ok(());
    }
    common::ingest::post_batch(client, url, tokens, reasons, "train reasons").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reasons_url_sits_beside_the_backlog_route() {
        assert_eq!(
            train_reasons_url("http://api:8080/private/trust-event-backlog").as_deref(),
            Some("http://api:8080/private/train-reasons")
        );
        assert_eq!(
            train_reasons_url("http://api:8080/private/trust-event-backlog/").as_deref(),
            Some("http://api:8080/private/train-reasons")
        );
        assert_eq!(train_reasons_url("http://api:8080/somewhere-else"), None);
    }
}
