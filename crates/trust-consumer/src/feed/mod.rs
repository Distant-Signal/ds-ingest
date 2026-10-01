//! Re-exports the shared `MovementFeed` trait/fake from `movement-feed`.
//! This crate's own Kafka implementation (`kafka.rs`) was deleted in
//! Deploy C (PL-15a): every consumer reads movement-relay's Redis stream.
//! See docs/superpowers/specs/2026-09-04-movement-relay-design.md
//! Decision 3 for why the trait/fake moved to a shared crate.

#[cfg(test)]
pub(crate) use movement_feed::FakeMovementFeed;
pub(crate) use movement_feed::MovementFeed;
