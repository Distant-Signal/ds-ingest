//! `ingest-writer`: the train-domain background loops (plan 1B.6) and the
//! Redis-stream consumers (spec §10, plan 3a.3).
//!
//! The binary is `main.rs`; this library holds what its tests need to
//! reach. The loop runner and the loops themselves are in
//! `ds_store::loops`, which the api's loops use too (plan 1B.7). The stream
//! runtime's Redis half is `ingest_stream::consumer`; the writer's half is
//! here:
//!
//! - [`stream`]: the per-stream modes (`off`/`shadow`/`apply`), the
//!   consumer tasks and their [`ingest_stream::Handler`], and the hourly
//!   `MINID` trim of the dead-letter streams;
//! - [`handlers`]: the schema registry and the per-row isolation helper;
//! - [`dedup`]: `ingest_dedup`, claimed in each entry's transaction and
//!   pruned hourly;
//! - [`observed`]: the D13 observed-time clamp and ordering guard;
//! - [`telemetry`]: the `ingest_freshness` gauge and the stream-mode info.

pub mod config;
pub mod dedup;
pub mod handlers;
pub mod loops;
pub mod observed;
pub mod stream;
pub mod telemetry;
