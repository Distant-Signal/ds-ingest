//! One rail day's in-memory correlation state, and the dispatch of raw
//! `movement-events` payloads into it. Shared by the live consume path and
//! the startup replay (`replay.rs`), so both apply an event identically.

use std::collections::{HashMap, HashSet};

use trust_schema::schema::TrustMessage;

use crate::correlate;
use crate::population::Population;
use crate::stanox_tiploc::StanoxTable;
use crate::station_correlate;

/// The STANOX/TIPLOC lookups every Movement is resolved through, rebuilt
/// on each stanox/crs reload.
#[derive(Debug, Default)]
pub struct Lookups {
    pub stanox: StanoxTable,
    /// TIPLOC -> every shadow line whose catalogue resolves to it.
    pub tiploc_index: HashMap<String, Vec<String>>,
}

/// Why a whole rail day is `partial` -- see [`DayState::partial_reason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialReason {
    /// The process started mid-day and the day's first events had already
    /// been trimmed from `movement-events` (or the replay could not tell).
    DayStartTrimmed,
    /// The process started mid-day on a backend with no replay (Kafka).
    ReplayUnsupported,
}

impl PartialReason {
    pub fn as_str(self) -> &'static str {
        match self {
            PartialReason::DayStartTrimmed => "day_start_trimmed",
            PartialReason::ReplayUnsupported => "replay_unsupported",
        }
    }
}

/// Everything this process knows about `service_date`'s rail day.
#[derive(Debug)]
pub struct DayState {
    pub service_date: chrono::NaiveDate,
    pub correlation: correlate::CorrelationState,
    pub stations: station_correlate::StationCorrelationState,
    /// Set when this process cannot have seen every event of the day, so
    /// "no event seen for a scheduled train" means nothing: the day's rows
    /// then leave unseen trains out instead of counting them as cancelled,
    /// and never read "available". Only ever set for the day a process
    /// STARTS in; a day entered by an in-process rollover saw everything
    /// from its first event.
    pub partial_reason: Option<PartialReason>,
    /// Lines whose population for this day was still missing (every fetch
    /// failing) when consumption began -- their early movements could not
    /// be matched, so their rows are partial for the rest of the day too.
    pub partial_lines: HashSet<String>,
}

impl DayState {
    pub fn new(service_date: chrono::NaiveDate) -> Self {
        Self {
            service_date,
            correlation: correlate::CorrelationState::default(),
            stations: station_correlate::StationCorrelationState::default(),
            partial_reason: None,
            partial_lines: HashSet::new(),
        }
    }

    pub fn is_line_partial(&self, line_id: &str) -> bool {
        self.partial_reason.is_some() || self.partial_lines.contains(line_id)
    }

    /// Parses one raw stream payload and dispatches every message in it.
    /// `Err` only for an unparseable payload (the caller decides whether to
    /// dead-letter it).
    pub fn dispatch_payload(
        &mut self,
        raw: &str,
        lookups: &Lookups,
        population: &Population,
    ) -> anyhow::Result<()> {
        for message in trust_schema::schema::parse_batch(raw)? {
            dispatch_message(
                message,
                &mut self.correlation,
                &mut self.stations,
                &lookups.stanox,
                &lookups.tiploc_index,
                population,
                self.service_date,
            );
        }
        Ok(())
    }
}

/// Dispatches one parsed `TrustMessage` into both running correlation
/// records. `ChangeOfOrigin`/`ChangeOfIdentity`/`Reinstatement`/`Unknown`
/// are deliberately ignored -- `correlate.rs`'s own scope (Decision 2d) only
/// covers Activation/Movement/Cancellation, the same three message types
/// `trust-consumer` itself keys real behaviour on. `Reinstatement` (`0005`,
/// confirmed by the H4 fix of the 2026-09-26 review) doesn't regress this
/// consumer's own "cancelled" line-level state by being ignored here: this
/// module's `apply_movement`/`apply_cancellation` already reuse
/// `trust_schema::journey` directly, so that fix's own `status_rank` change
/// (a fresh Movement can un-stick a `"cancelled"` per-line status without
/// needing a Reinstatement message specifically) already applies here too.
pub fn dispatch_message(
    message: TrustMessage,
    correlation_state: &mut correlate::CorrelationState,
    station_state: &mut station_correlate::StationCorrelationState,
    stanox: &StanoxTable,
    tiploc_index: &HashMap<String, Vec<String>>,
    population: &Population,
    service_date: chrono::NaiveDate,
) {
    match message {
        TrustMessage::Activation(activation) => {
            correlate::apply_activation(correlation_state, &activation);
            // `toc_id` is `Option` as of the 2026-09-25 review's finding #7
            // (a required field with no reader made one absent value drop the
            // WHOLE Activation, costing its far more valuable
            // train_id/train_uid binding). `None` simply means this uid
            // learns no operator here -- `station_correlate` already treats
            // "a UID absent from `activations_by_uid`" as a first-class case
            // and skips station correlation for it, exactly as it does for a
            // Movement whose Activation this process never saw.
            if let Some(toc_id) = activation.toc_id.as_deref() {
                station_correlate::apply_activation(station_state, &activation.train_uid, toc_id);
            } else {
                tracing::debug!(
                    train_uid = %activation.train_uid,
                    "Activation carries no toc_id; skipping station correlation for this uid"
                );
            }
        }
        TrustMessage::Movement(movement) => {
            let result = correlate::apply_movement(
                correlation_state,
                &movement,
                stanox,
                tiploc_index,
                population,
                service_date,
            );
            for (line_id, uid) in &result.matched_lines {
                metrics::counter!(
                    common::metrics::metric_name("full_coverage_consumer_events_matched_total"),
                    "line_id" => line_id.clone()
                )
                .increment(1);

                let Some(crs) = result.loc_crs.as_deref() else {
                    continue;
                };
                let Some(derived) = correlation_state
                    .derived
                    .get(&(line_id.clone(), uid.clone()))
                else {
                    continue;
                };
                let matched_station = station_correlate::apply_movement_station(
                    station_state,
                    &result.train_uid,
                    crs,
                    derived,
                );
                if !matched_station {
                    metrics::counter!(common::metrics::metric_name(
                        "full_coverage_consumer_station_buckets_dropped_total"
                    ))
                    .increment(1);
                }
            }
        }
        TrustMessage::Cancellation(cancellation) => {
            correlate::apply_cancellation(correlation_state, &cancellation);
        }
        TrustMessage::ChangeOfOrigin(_)
        | TrustMessage::ChangeOfIdentity(_)
        | TrustMessage::Reinstatement(_)
        | TrustMessage::Unknown(_) => {}
    }
}
