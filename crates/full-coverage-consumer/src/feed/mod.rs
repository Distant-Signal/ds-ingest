//! Re-exports the shared `MovementFeed` trait/fake from `movement-feed`,
//! plus this crate's own Kafka implementation (`kafka.rs`, unchanged --
//! scheduled for deletion in Deploy C, see
//! docs/superpowers/plans/2026-09-04-movement-relay-plan.md Task 13/14,
//! NOT this task). See
//! docs/superpowers/specs/2026-09-04-movement-relay-design.md Decision 3
//! for why the trait/fake moved to a shared crate.

pub mod kafka;

#[cfg(test)]
pub use movement_feed::FakeMovementFeed;
pub use movement_feed::MovementFeed;
