//! `darwin-relay` (DS-152;
//! docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md
//! phase 3, docs/darwin-pushport.md): the one Kafka client of the Rail
//! Data Marketplace's Darwin Push Port JSON topic. It keeps the parts DS
//! uses (schedules, train status platforms and forecasts, associations,
//! deactivations; [`pushport`]), merges a few seconds of messages into one
//! `darwin-pushport/1` batch and produces it on `ds:ingest:darwin`, which
//! ingest-writer applies to the `darwin_*` tables ([`relay`]). Off by
//! default in the chart.
//!
//! Health: `/healthz` is "Kafka partitions assigned" (movement-relay's
//! rule); `/livez` is loop progress, idle while the topic is quiet and
//! while the producer waits out a Redis outage.
//!
//! Not built: the Push Port snapshot product (an hourly full state, for a
//! resync after a long outage). Until then a gap closes as Darwin re-sends
//! each running train's state.

mod config;
mod kafka;
mod metrics;
mod pushport;
mod relay;

use std::num::NonZeroUsize;
use std::time::Duration;

use clap::Parser;
use config::Config;
use ingest_stream::{ProducePolicy, Producer, ProducerConfig, SchemaId};

/// Batches buffered while Redis is unavailable before the loop waits.
const MAX_BUFFERED: NonZeroUsize = NonZeroUsize::MIN.saturating_add(3);

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(run().await)
}

async fn run() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    common::logging::init("darwin-relay");
    let config = Config::parse();
    if config.metrics.metrics_enabled {
        common::metrics::install(config.metrics_port)?;
        metrics::register();
    }

    let (ready, progress) = health_http::spawn_with_progress(
        config.health.health_bind_url.clone(),
        "partitions assigned",
        "no confirmed partition assignment",
        Duration::from_secs(config.health.progress_stall_secs),
    );

    // The result carries the password: pass it on, never log it.
    let redis_url = common::redis_auth::redis_url_with_credentials(
        &config.redis_url,
        config.redis_username.as_deref(),
        config.redis_password.as_ref(),
    )?;
    let stream = ingest_stream::streams::DARWIN;
    let maxlen = match config.darwin_stream_maxlen {
        0 => ingest_stream::budget::decl(stream)
            .map(ingest_stream::budget::StreamDecl::maxlen)
            .ok_or_else(|| anyhow::anyhow!("{stream} has no budget declaration"))?,
        n => n,
    };
    let (producer, producer_task) = Producer::spawn(
        redis::Client::open(redis_url.expose())?,
        ProducerConfig::new(
            stream,
            maxlen,
            ProducePolicy::Event {
                max_buffered: MAX_BUFFERED,
            },
        ),
    );
    let mut sink = relay::ProducerSink {
        producer: producer.clone(),
        schema: SchemaId::new(common::darwin::SCHEMA, 1)?,
        producer_id: ingest_stream::snapshot::producer_id("darwin-relay"),
        progress: progress.clone(),
    };
    let mut source = kafka::KafkaSource::connect(&config, ready, progress.clone())?;
    let limits = relay::Limits {
        max_age: Duration::from_secs(config.batch_max_secs.max(1)),
        max_records: config.batch_max_records.max(1),
    };
    tracing::info!(
        stream,
        maxlen,
        topic = %config.kafka_topic,
        batch_max_secs = config.batch_max_secs,
        "darwin-relay started"
    );

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            () = &mut shutdown => break Ok(()),
            cycle = relay::run_cycle(&mut source, &mut sink, limits) => match cycle {
                Ok(relay::Cycle::Committed { records, rows }) => {
                    tracing::debug!(records, rows, "relayed a batch");
                    progress.beat();
                }
                // A failed receive is retried (Kafka reconnects itself); a
                // failed publish or commit ends the process, which resumes
                // from the last commit (see `relay`).
                Err(err) if err.is::<rdkafka::error::KafkaError>() => {
                    tracing::error!(error = ?err, "Kafka receive or commit failed; retrying");
                    metrics::error("kafka");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(err) => {
                    metrics::error("publish");
                    break Err(err.context("publishing a Push Port batch failed"));
                }
            },
        }
    };
    tracing::info!("darwin-relay stopping; an uncommitted batch is re-read on the next start");
    producer
        .shutdown(producer_task, Duration::from_secs(10))
        .await;
    result
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut sigterm) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
