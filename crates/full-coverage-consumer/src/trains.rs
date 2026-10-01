//! Per-train TRUST state for the windowed stats
//! (docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md
//! section 4.3.1), kept only with `FULL_COVERAGE_WINDOWED_STATS=true`.
//!
//! Unlike the legacy `correlate` state, which only ever learns about a train
//! through a Movement matched at one of a line's stations, this keeps
//! everything TRUST said about each UID of the service date -- its
//! Activation, its reports (wherever they were), and its cancellation,
//! reinstatement and change of origin -- and leaves "what does that mean
//! for line L" to `stats::classify_line_train`, which knows the line's due
//! times.
//!
//! **Keyed by service date.** An Activation says which date its train runs
//! on (`tp_origin_timestamp`, else the `train_id`'s day-of-month digits);
//! one for the next date, received before today's rail day closes (the
//! next day's first trains are activated about an hour before they leave,
//! i.e. before 02:00 London), goes into a separate `next` map that becomes
//! `current` at the rollover instead of being thrown away.
//!
//! **Messages before their Activation.** A 0002/0005/0006 whose `train_id`
//! is not known yet (its Activation predates what this process has seen) is
//! parked by `train_id` and applied when the Activation arrives. Whatever
//! is still parked when the day rolls is counted in
//! `full_coverage_consumer_unattributed_total{msg_type}`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use trust_schema::schema::{Activation, Cancellation, ChangeOfOrigin, Movement, Reinstatement};

use crate::correlate::{activation_service_date, train_id_day_of_month};

/// UTC minutes since the Unix epoch -- the unit every due time is in.
pub fn to_minutes(instant: DateTime<Utc>) -> u32 {
    u32::try_from(instant.timestamp().div_euclid(60)).unwrap_or(0)
}

/// A live 0002.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Canx {
    pub canx_type: Option<String>,
    /// The planned departure at the location the train was cancelled
    /// from: it runs no further. `None` when the 0002 did not say.
    pub dep_min: Option<u32>,
}

/// One 0003, reduced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    pub planned_min: u32,
    /// Minutes late (TRUST `timetable_variation`; 0 on time or early).
    pub delay: i16,
    /// Interned TIPLOC of the report's location ([`TrainState::tiploc_id`]),
    /// [`NO_TIPLOC`] when it did not resolve.
    pub tiploc: u32,
}

pub const NO_TIPLOC: u32 = u32::MAX;

/// Reports kept per train. A long-distance train reports at a few dozen
/// points; this only bounds a pathological feed.
const MAX_REPORTS_PER_TRAIN: usize = 256;
/// Parked messages kept per unknown `train_id`, and unknown `train_id`s.
const MAX_PARKED_PER_TRAIN: usize = 8;
const MAX_PARKED_TRAINS: usize = 50_000;

/// Everything TRUST said about one UID's train(s) for the service date.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrainDay {
    pub activated: bool,
    /// The latest 0002 not undone by a later 0005.
    pub cancel: Option<Canx>,
    /// The planned departure at the new origin of the latest 0006.
    pub origin_change_dep_min: Option<u32>,
    /// In arrival order.
    pub reports: Vec<Report>,
}

impl TrainDay {
    /// The report with the latest planned time: how late the train was
    /// last seen running.
    pub fn last_report(&self) -> Option<&Report> {
        self.reports.iter().max_by_key(|r| r.planned_min)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Which {
    Current,
    Next,
}

#[derive(Debug, Clone)]
enum Parked {
    Cancellation(Cancellation, DateTime<Utc>),
    Reinstatement,
    ChangeOfOrigin(ChangeOfOrigin, DateTime<Utc>),
}

impl Parked {
    fn msg_type(&self) -> &'static str {
        match self {
            Parked::Cancellation(..) => "0002",
            Parked::Reinstatement => "0005",
            Parked::ChangeOfOrigin(..) => "0006",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TrainState {
    pub service_date: chrono::NaiveDate,
    pub current: HashMap<String, TrainDay>,
    pub next: HashMap<String, TrainDay>,
    train_ids: HashMap<String, (Which, String)>,
    parked: HashMap<String, Vec<Parked>>,
    tiplocs: HashMap<String, u32>,
    /// `corrected - raw` of the latest Movement's `actual_timestamp`: the
    /// feed's current local-as-UTC skew (`common::trust_timestamp`), applied
    /// to 0002/0006 `dep_timestamp`s, which are planned times often hours
    /// ahead of receipt and so cannot be plausibility-checked on their own.
    skew: chrono::Duration,
}

impl TrainState {
    pub fn new(service_date: chrono::NaiveDate) -> Self {
        Self {
            service_date,
            current: HashMap::new(),
            next: HashMap::new(),
            train_ids: HashMap::new(),
            parked: HashMap::new(),
            tiplocs: HashMap::new(),
            skew: chrono::Duration::zero(),
        }
    }

    /// The interned id of `tiploc`, if any report was ever at it.
    pub fn tiploc_id(&self, tiploc: &str) -> Option<u32> {
        self.tiplocs.get(tiploc).copied()
    }

    fn intern(&mut self, tiploc: &str) -> u32 {
        if let Some(id) = self.tiplocs.get(tiploc) {
            return *id;
        }
        let id = u32::try_from(self.tiplocs.len()).unwrap_or(NO_TIPLOC);
        self.tiplocs.insert(tiploc.to_string(), id);
        id
    }

    fn day_mut(&mut self, which: Which, uid: &str) -> &mut TrainDay {
        let map = match which {
            Which::Current => &mut self.current,
            Which::Next => &mut self.next,
        };
        map.entry(uid.to_string()).or_default()
    }

    fn lookup(&self, train_id: &str) -> Option<(Which, String)> {
        self.train_ids.get(train_id).cloned()
    }

    /// `lookback`: an Activation from the startup replay's segment before
    /// the rail day started; only one for THIS service date is kept.
    pub fn apply_activation(&mut self, activation: &Activation, lookback: bool) {
        let next_date = self.service_date + chrono::Duration::days(1);
        let candidates = if lookback {
            [
                self.service_date - chrono::Duration::days(1),
                self.service_date,
            ]
        } else {
            [self.service_date, next_date]
        };
        let which = match activation_service_date(activation, &candidates) {
            Some(date) if date == self.service_date => Which::Current,
            Some(date) if date == next_date && !lookback => Which::Next,
            _ => return,
        };
        self.day_mut(which, &activation.train_uid).activated = true;
        self.train_ids.insert(
            activation.train_id.clone(),
            (which, activation.train_uid.clone()),
        );
        if let Some(parked) = self.parked.remove(&activation.train_id) {
            for message in parked {
                self.apply_known(which, &activation.train_uid, message);
            }
        }
    }

    fn apply_known(&mut self, which: Which, uid: &str, message: Parked) {
        match message {
            Parked::Cancellation(c, received_at) => {
                let dep_min = self.dep_minutes(c.dep_timestamp.as_deref(), received_at);
                self.day_mut(which, uid).cancel = Some(Canx {
                    canx_type: c.canx_type,
                    dep_min,
                });
            }
            Parked::Reinstatement => self.day_mut(which, uid).cancel = None,
            Parked::ChangeOfOrigin(o, received_at) => {
                let dep_min = self.dep_minutes(o.dep_timestamp.as_deref(), received_at);
                if dep_min.is_some() {
                    self.day_mut(which, uid).origin_change_dep_min = dep_min;
                }
            }
        }
    }

    fn apply_or_park(&mut self, train_id: &str, message: Parked) {
        use chrono::Datelike;
        if let Some((which, uid)) = self.lookup(train_id) {
            self.apply_known(which, &uid, message);
            return;
        }
        // Only a train of today or tomorrow (by its train_id's day digits)
        // can still be attributed; anything else (yesterday's trains, which
        // are not tracked at all) is not this day's business.
        let next_date = self.service_date + chrono::Duration::days(1);
        let day = train_id_day_of_month(train_id);
        if day != Some(self.service_date.day()) && day != Some(next_date.day()) {
            return;
        }
        if self.parked.len() >= MAX_PARKED_TRAINS && !self.parked.contains_key(train_id) {
            metrics::counter!(
                common::metrics::metric_name("full_coverage_consumer_unattributed_total"),
                "msg_type" => message.msg_type()
            )
            .increment(1);
            return;
        }
        let parked = self.parked.entry(train_id.to_string()).or_default();
        if parked.len() < MAX_PARKED_PER_TRAIN {
            parked.push(message);
        }
    }

    pub fn apply_cancellation(&mut self, cancellation: &Cancellation, received_at: DateTime<Utc>) {
        self.apply_or_park(
            &cancellation.train_id.clone(),
            Parked::Cancellation(cancellation.clone(), received_at),
        );
    }

    pub fn apply_reinstatement(&mut self, reinstatement: &Reinstatement) {
        self.apply_or_park(&reinstatement.train_id, Parked::Reinstatement);
    }

    pub fn apply_change_of_origin(&mut self, change: &ChangeOfOrigin, received_at: DateTime<Utc>) {
        self.apply_or_park(
            &change.train_id.clone(),
            Parked::ChangeOfOrigin(change.clone(), received_at),
        );
    }

    /// Records a 0003 against its train. Returns the report's actual
    /// time (for feed-health), when it parsed. `OFF ROUTE` (and any report
    /// that says nothing about lateness) updates nothing.
    pub fn apply_movement(
        &mut self,
        movement: &Movement,
        tiploc: Option<&str>,
        received_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let pair = common::trust_timestamp::parse_trust_epoch_millis_pair(
            movement.planned_timestamp.as_deref(),
            movement.actual_timestamp.as_deref(),
            received_at,
            true,
        );
        if let (Some(actual), Some(raw)) = (
            pair.actual,
            movement
                .actual_timestamp
                .as_deref()
                .and_then(|a| a.parse::<i64>().ok())
                .and_then(DateTime::from_timestamp_millis),
        ) {
            self.skew = actual - raw;
        }
        let (which, uid) = self.lookup(&movement.train_id)?;
        let delay = trust_schema::schema::movement_delay_minutes(movement)?;
        let planned = pair.planned?;
        let tiploc = tiploc
            .map(|t| self.intern(schedule_query::normalize_tiploc(t)))
            .unwrap_or(NO_TIPLOC);
        let day = self.day_mut(which, &uid);
        if day.reports.len() < MAX_REPORTS_PER_TRAIN {
            day.reports.push(Report {
                planned_min: to_minutes(planned),
                delay: i16::try_from(delay).unwrap_or(i16::MAX),
                tiploc,
            });
        }
        pair.actual
    }

    fn dep_minutes(&self, raw: Option<&str>, _received_at: DateTime<Utc>) -> Option<u32> {
        let millis: i64 = raw?.trim().parse().ok()?;
        let raw = DateTime::from_timestamp_millis(millis)?;
        Some(to_minutes(raw.checked_add_signed(self.skew)?))
    }

    /// The state for the day after this one: `next` becomes `current`, and
    /// the `train_id`s and parked messages that belong to it are kept.
    /// Returns how many parked messages were given up, by `msg_type`.
    pub fn roll(self, next_date: chrono::NaiveDate) -> (TrainState, HashMap<&'static str, u64>) {
        use chrono::Datelike;
        let mut rolled = TrainState::new(next_date);
        let mut unattributed: HashMap<&'static str, u64> = HashMap::new();
        let consecutive = next_date == self.service_date + chrono::Duration::days(1);
        if consecutive {
            rolled.current = self.next;
            rolled.train_ids = self
                .train_ids
                .into_iter()
                .filter(|(_, (which, _))| *which == Which::Next)
                .map(|(id, (_, uid))| (id, (Which::Current, uid)))
                .collect();
        }
        for (train_id, messages) in self.parked {
            if consecutive && train_id_day_of_month(&train_id) == Some(next_date.day()) {
                rolled.parked.insert(train_id, messages);
            } else {
                for message in messages {
                    *unattributed.entry(message.msg_type()).or_default() += 1;
                }
            }
        }
        rolled.tiplocs = self.tiplocs;
        rolled.skew = self.skew;
        (rolled, unattributed)
    }

    /// Parked messages not yet attributed.
    pub fn parked_count(&self) -> usize {
        self.parked.values().map(Vec::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date() -> chrono::NaiveDate {
        "2026-09-27".parse().unwrap()
    }

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn millis(s: &str) -> String {
        at(s).timestamp_millis().to_string()
    }

    fn activation(train_id: &str, uid: &str) -> Activation {
        Activation {
            train_id: train_id.to_string(),
            train_uid: uid.to_string(),
            toc_id: Some("SW".to_string()),
            train_service_code: None,
            schedule_wtt_id: None,
            schedule_start_date: None,
            schedule_end_date: None,
            tp_origin_timestamp: None,
        }
    }

    fn cancellation(train_id: &str, dep: Option<&str>) -> Cancellation {
        Cancellation {
            train_id: train_id.to_string(),
            canx_timestamp: None,
            canx_reason_code: None,
            canx_type: Some("EN ROUTE".to_string()),
            dep_timestamp: dep.map(millis),
            loc_stanox: None,
        }
    }

    fn movement(train_id: &str, planned: &str, status: &str, variation: &str) -> Movement {
        Movement {
            train_id: train_id.to_string(),
            event_type: "DEPARTURE".to_string(),
            gbtt_timestamp: None,
            planned_timestamp: Some(millis(planned)),
            actual_timestamp: Some(millis(planned)),
            reporting_stanox: None,
            loc_stanox: None,
            toc_id: None,
            variation_status: Some(status.to_string()),
            timetable_variation: Some(variation.to_string()),
        }
    }

    // Winter dates keep the local-as-UTC correction a no-op, so the times
    // below read as written.
    fn winter() -> chrono::NaiveDate {
        "2026-01-27".parse().unwrap()
    }

    #[test]
    fn a_cancellation_before_any_movement_is_recorded() {
        let mut s = TrainState::new(date());
        s.apply_activation(&activation("1A01MW27", "C1"), false);
        s.apply_cancellation(&cancellation("1A01MW27", None), at("2026-09-27T08:00:00Z"));
        assert!(s.current["C1"].activated);
        assert!(s.current["C1"].cancel.is_some());
    }

    #[test]
    fn a_reinstatement_after_a_cancellation_clears_it() {
        let mut s = TrainState::new(date());
        s.apply_activation(&activation("1A01MW27", "C1"), false);
        s.apply_cancellation(&cancellation("1A01MW27", None), at("2026-09-27T08:00:00Z"));
        s.apply_reinstatement(&Reinstatement {
            train_id: "1A01MW27".to_string(),
            dep_timestamp: None,
            reinstatement_timestamp: None,
        });
        assert_eq!(s.current["C1"].cancel, None);
    }

    /// A 0002 for a `train_id` whose Activation has not been seen yet is
    /// applied when it arrives.
    #[test]
    fn a_cancellation_for_an_unknown_train_id_is_applied_when_its_activation_arrives() {
        let mut s = TrainState::new(winter());
        s.apply_cancellation(
            &cancellation("1A01MW27", Some("2026-01-27T09:30:00Z")),
            at("2026-01-27T08:00:00Z"),
        );
        assert!(s.current.is_empty());
        assert_eq!(s.parked_count(), 1);
        s.apply_activation(&activation("1A01MW27", "C1"), false);
        assert_eq!(
            s.current["C1"].cancel,
            Some(Canx {
                canx_type: Some("EN ROUTE".to_string()),
                dep_min: Some(to_minutes(at("2026-01-27T09:30:00Z"))),
            })
        );
        assert_eq!(s.parked_count(), 0);
    }

    #[test]
    fn a_late_movements_delay_comes_from_timetable_variation() {
        let mut s = TrainState::new(winter());
        s.apply_activation(&activation("1A01MW27", "C1"), false);
        s.apply_movement(
            &movement("1A01MW27", "2026-01-27T09:00:00Z", "LATE", "12"),
            Some("LLANDUJ"),
            at("2026-01-27T09:13:00Z"),
        );
        s.apply_movement(
            &movement("1A01MW27", "2026-01-27T09:30:00Z", "OFF ROUTE", "0"),
            Some("ELSEWHR"),
            at("2026-01-27T09:31:00Z"),
        );
        let reports = &s.current["C1"].reports;
        assert_eq!(reports.len(), 1, "OFF ROUTE records nothing");
        assert_eq!(reports[0].delay, 12);
        assert_eq!(reports[0].tiploc, s.tiploc_id("LLANDUJ").unwrap());
        assert_eq!(
            reports[0].planned_min,
            to_minutes(at("2026-01-27T09:00:00Z"))
        );
    }

    /// Routing by service date: the next day's train goes to `next` and
    /// becomes `current` at the rollover; yesterday's is dropped.
    #[test]
    fn next_day_activations_survive_the_rollover() {
        let mut s = TrainState::new(winter());
        s.apply_activation(&activation("1A01MW28", "NEXT"), false);
        s.apply_activation(&activation("1A01MW27", "TODAY"), false);
        s.apply_activation(&activation("1A01MW26", "YESTERDAY"), false);
        assert!(s.next.contains_key("NEXT"));
        assert!(s.current.contains_key("TODAY"));
        assert!(!s.current.contains_key("YESTERDAY"));
        // A 0002 for an unknown train of the next day is kept across the
        // rollover; one for an unknown train of today is given up.
        s.apply_cancellation(&cancellation("2B02MW28", None), at("2026-01-27T23:00:00Z"));
        s.apply_cancellation(&cancellation("2B02MW27", None), at("2026-01-27T23:00:00Z"));

        let (mut rolled, unattributed) = s.roll(winter() + chrono::Duration::days(1));
        assert!(rolled.current["NEXT"].activated);
        assert!(!rolled.current.contains_key("TODAY"));
        assert_eq!(unattributed.get("0002"), Some(&1));
        // The next day's movements still resolve by train_id.
        rolled.apply_movement(
            &movement("1A01MW28", "2026-01-28T06:00:00Z", "ON TIME", "0"),
            None,
            at("2026-01-28T06:00:30Z"),
        );
        assert_eq!(rolled.current["NEXT"].reports.len(), 1);
        rolled.apply_activation(&activation("2B02MW28", "LATE_ACT"), false);
        assert!(rolled.current["LATE_ACT"].cancel.is_some());
    }

    #[test]
    fn the_lookback_keeps_only_this_days_activations() {
        let mut s = TrainState::new(winter());
        s.apply_activation(&activation("1A01MW27", "TODAY"), true);
        s.apply_activation(&activation("1A01MW26", "YESTERDAY"), true);
        assert!(s.current.contains_key("TODAY"));
        assert!(s.next.is_empty());
        assert_eq!(s.current.len(), 1);
    }

    /// A 0002's `dep_timestamp` carries the feed's local-as-UTC skew: it is
    /// corrected by the skew the latest Movement showed (BST here).
    #[test]
    fn dep_timestamps_are_corrected_by_the_feeds_current_skew() {
        let mut s = TrainState::new(date());
        s.apply_activation(&activation("1A01MW27", "C1"), false);
        // Movement at true 09:00Z, skewed +1 h on the wire, received 09:00:30Z.
        let skewed = |s: &str| {
            (at(s) + chrono::Duration::hours(1))
                .timestamp_millis()
                .to_string()
        };
        let mut m = movement("1A01MW27", "2026-09-27T09:00:00Z", "ON TIME", "0");
        m.planned_timestamp = Some(skewed("2026-09-27T09:00:00Z"));
        m.actual_timestamp = Some(skewed("2026-09-27T09:00:00Z"));
        s.apply_movement(&m, None, at("2026-09-27T09:00:30Z"));
        assert_eq!(
            s.current["C1"].reports[0].planned_min,
            to_minutes(at("2026-09-27T09:00:00Z"))
        );
        let mut c = cancellation("1A01MW27", None);
        c.dep_timestamp = Some(skewed("2026-09-27T11:00:00Z"));
        s.apply_cancellation(&c, at("2026-09-27T09:01:00Z"));
        assert_eq!(
            s.current["C1"].cancel.as_ref().unwrap().dep_min,
            Some(to_minutes(at("2026-09-27T11:00:00Z")))
        );
    }
}
