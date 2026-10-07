//! `ingest-writer`: the train-domain background loops now (plan 1B.6), and
//! the Redis-stream consumers from phase 3 (spec §10).
//!
//! The binary is `main.rs`; this library holds what its tests need to
//! reach. The loop runner and the loops themselves are in
//! `ds_store::loops`, which the api's loops use too (plan 1B.7).

pub mod config;
pub mod loops;
