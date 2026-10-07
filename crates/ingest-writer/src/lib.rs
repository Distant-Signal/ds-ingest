//! `ingest-writer`: the train-domain background loops now (plan 1B.6), and
//! the Redis-stream consumers from phase 3 (spec §10).
//!
//! The binary is `main.rs`; this library holds what its tests (and, once
//! `ds-store` exists, the api's loops, plan 1B.7) need to reach.

pub mod config;
pub mod loop_runner;
pub mod loops;
