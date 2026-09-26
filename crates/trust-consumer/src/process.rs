//! The full consume -> parse -> match/derive -> write -> commit cycle,
//! generic over `MovementFeed` so it's testable against `FakeMovementFeed`
//! without a broker. This is this plan's answer to "no wiremock for
//! Kafka" in practice, not just in the abstract -- see `feed::MovementFeed`'s
//! doc comment for the reasoning.
//!
//! # Known simplification left for follow-up work
//!
//! **STANOX->CRS translation is implemented via a table loaded once at
//! startup from `reference-data/stanox-crs.csv`,
//! `stanox_crs::StanoxCrsTable::stanox_to_crs`.** `loc_crs` in
//! `process_message` is the real translated CRS (or `None` when the
//! STANOX isn't in the table -- see below), and
//! `matching::resolve_origin_departure` is handed that translated CRS,
//! not the raw `loc_stanox`, so a pin's `pin_origin_crs` can now actually
//! compare equal to it. The table itself is generated from a real CIF
//! full-timetable extract's `TI` (TIPLOC Insert) records, not fetched
//! live -- this crate has no CIF SCHEDULE feed connection (a separate,
//! larger, unbuilt ingestion pipeline; see
//! docs/superpowers/specs/2026-08-30-schedule-feed-ingress-design.md) --
//! see `stanox_crs`'s own module doc and `reference-data/stanox-crs.md`
//! for full provenance, the exact record format it was decoded against,
//! and the (small, documented) set of STANOX values deliberately excluded
//! as ambiguous. A lookup miss -- a genuinely unmapped or non-passenger
//! STANOX (freight-only sidings, signals, junctions), or one of the
//! table's excluded ambiguous entries -- still yields `loc_crs = None`,
//! preserving the honest "we don't know" behaviour this module always had:
//! `last_reported_location` falls back to the raw STANOX (per
//! `journey::apply_movement`'s existing fallback), and a pin simply
//! doesn't match on that event.
//!
//! **A tracked train can stay `resolution_status = 'pending'` in the
//! database forever even while this process tracks it correctly.**
//! `crates/api`'s `upsert_train_event` (Task 4) only flips
//! `tracked_trains.resolution_status` to `'resolved'` when an incoming event
//! carries BOTH `resolved_train_uid` and `resolved_train_id`. This module
//! can only supply `resolved_train_uid` when it observed a `0001` Activation
//! for that `train_id` *in this process* before the Movement that resolved
//! the pin -- see `ProcessorState::pending_activations`. If the Activation
//! arrived before this process started, was pruned as expired, or was simply
//! never emitted on the slice of the feed this consumer sees, the resolving
//! Movement goes out with `resolved_train_uid: None`, `api` leaves the row
//! `'pending'`, and it stays that way indefinitely: nothing re-attempts the
//! binding, because `state.resolved` now short-circuits matching for that
//! `train_id` on every later message.
//!
//! The consequence is confined to database-level status staleness and
//! whatever display depends on it. Tracking itself stays correct -- events
//! keep flowing against the right `tracked_train_id`s, because those come
//! from `state.resolved`, not from the DB's status column. Closing the gap
//! properly would mean relaxing `crates/api`'s two-field guard (which
//! reopens already-reviewed Task 4 work); that is still out of scope. The
//! *other* half of what this paragraph used to name as out of scope --
//! restructuring when this module mutates its maps relative to the batch's
//! HTTP post -- is now done, see the next section.
//!
//! **One sub-case of the above IS now closed (Low finding #2 of the
//! 2026-09-25 review): a Cancellation.** A `TrustMessage::Cancellation`
//! below never carries `resolved_train_uid`/`resolved_train_id` either --
//! there is no new identity to report, only the fact the journey is over --
//! so it hits this exact gap even when this process DID observe the
//! Activation (`state.resolved` already attributed the subscription, just
//! never through a resolving Movement). Left alone, a subscription whose
//! train was cancelled before ever departing its origin stayed
//! `resolution_status = 'pending'` forever. `crates/api`'s
//! `upsert_train_event` now reads this module's own `derived.status ==
//! "cancelled"` (already sent on every event below, unchanged) to flip such
//! a subscription to `'unresolved'` instead -- see
//! `train_tracking::mark_subscription_unresolved_on_cancellation`. The
//! general "Activation this process never saw" gap above is unaffected: it
//! is specifically about a resolving MOVEMENT going out without an identity,
//! which a Cancellation was never going to supply anyway.
//!
//! **Every in-memory mutation this module makes is undone if the batch's
//! downstream POST fails (finding #4 of the 2026-09-25 review).**
//! `process_message` still mutates `state.resolved`,
//! `state.pending_activations`, `state.activation_matched_awaiting_movement`
//! and `state.last_derived` as it builds each event -- a later message in
//! the SAME batch has to see an earlier one's effects, so deferring the
//! writes outright isn't an option -- but every one of those mutations is
//! now journaled (`ProcessorState::journal`). `main.rs`'s `run_cycle` calls
//! `ProcessorState::confirm_batch` once, and only once, the POST has
//! actually succeeded, and `ProcessorState::roll_back_batch` on any failure
//! path. A retried batch therefore sees exactly the pre-batch state it saw
//! the first time, and resolves identically.
//!
//! This matters because the claim this paragraph used to make -- "there is
//! no in-process redelivery for these maps to go stale against" -- was
//! FALSE. It reasoned correctly about Kafka (`StreamConsumer::recv` never
//! re-hands a message to a running process, whatever the offset state:
//! committed offsets govern where a *new* consumer session resumes) and
//! then generalized from it, but the production backend is Redis Streams,
//! and `movement_feed::redis_stream::RedisStreamMovementFeed::reclaim_stale`
//! DOES replay an unacked batch to the same running process after 30
//! seconds. On that replay, the resolving Movement used to find
//! `state.resolved` already populated by the first (unacked, unposted)
//! attempt, so `freshly_resolved` came back `false`, the event went out
//! without `resolved_train_uid`/`resolved_train_id`, and `api` left the
//! subscription `'pending'` forever even though the redelivered POST
//! succeeded. Rolling the maps back makes the "freshly resolved" signal
//! idempotent under redelivery, which is the property that whole path needs.
//!
//! **A failed batch is retried, not skipped.** `feed/kafka.rs` stores an
//! offset only when `commit` is called (`enable.auto.offset.store=false`),
//! and, as of finding #3 of the same review, it also `seek`s back to the
//! offset of any record whose cycle never confirmed before its next `recv`
//! -- so a failed cycle re-delivers the same record instead of the next
//! consumer read silently advancing past it and the following commit
//! sweeping it up. Under the Redis backend the equivalent guarantee was
//! already there (the unacked entry is reclaimed and replayed). Either way
//! the replay is safe because of the `dedup_key` path and the rollback
//! described above.
//!
//! Two smaller, deliberate consequences of shapes fixed in earlier tasks,
//! noted here so they aren't mistaken for oversights:
//!
//! - `raw_body` is always `serde_json::json!({})`. `schema::parse_envelope`
//!   deserializes each envelope's body into a typed struct and drops the
//!   original `serde_json::Value`, so there is no raw body left to forward
//!   by the time a message reaches this module.
//! - `eta::propagate_eta` is not called. It only ever returns `Some` when
//!   given a `remaining_scheduled` timestamp, and this crate has no
//!   calling-point schedule to supply one until a future CIF-backed pass --
//!   so `eta_next`/`eta_source` are always `None` here rather than being
//!   filled by a call that could only ever be a no-op.

use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;

use trust_schema::journey::DerivedState;
use trust_schema::schema::TrustMessage;

use crate::feed::MovementFeed;

/// In-memory mirror of what `api`'s active-tracked-trains reference set
/// contains, refreshed on `run_once`'s caller's own schedule (main.rs's
/// reference-reload timer). Kept as a plain argument rather than internal
/// state so `run_once` stays an easy-to-assert function of
/// (feed, reference, state) -> events.
pub struct Reference {
    pub pending: Vec<crate::matching::PendingPin>,
    /// `train_uid -> EVERY subscription that shares it`, for every active
    /// ref whose identity is already known (a schedule match, or an
    /// NR-primary subscription created via `POST /Train/by-uid/.../track`,
    /// Task 20). Checked FIRST on every Activation
    /// (docs/superpowers/specs/2026-09-06-shared-train-identity-design.md §3)
    /// -- strictly more reliable than the +/-20-minute CRS+time heuristic
    /// `matching::resolve_origin_departure` still exists for pins that
    /// genuinely lack this.
    ///
    /// `Vec<i64>`, not `i64` (review finding I6). "Two subscribers sharing
    /// one physical train" is the headline scenario the whole shared-train
    /// redesign exists to support, and one `train_uid` therefore maps to as
    /// many subscriptions as there are subscribers. The old single-valued
    /// map meant a blind `.insert` in `apply_reference_reload` silently
    /// overwrote every subscriber but the last one this loop happened to
    /// visit, so exactly one of them ever flipped to `'resolved'` -- the
    /// rest sat at `'pending'` forever while the train was visibly
    /// running.
    /// `Vec<SharingSubscription>`, not `Vec<i64>`, as of finding #2 of the
    /// 2026-09-25 review: the fast path MUST be able to check which day
    /// each subscription is for before attributing an Activation to it.
    /// See [`SharingSubscription`] and `activation_is_for_service_date`.
    pub by_train_uid: HashMap<String, Vec<SharingSubscription>>,
    /// `tracked_train_id -> trains_id`, for every active ref that has one
    /// (regardless of resolution_status -- an already-`resolved`
    /// subscription still needs its later movements forwarded). Feeds
    /// `build_forward_signals`, below.
    pub trains_id_by_tracked_train_id: HashMap<i64, i64>,
    /// `trains_id -> the shared trains row's own known destination CRS`,
    /// for every active ref whose `trains_id` is known AND whose schedule
    /// match resolved a destination (`common::TrackedTrainRef::destination_crs`).
    /// Feeds `process_message`'s confirmed-terminus-ARRIVAL detection
    /// (`trust_schema::journey::apply_movement`'s `destination_crs` param)
    /// -- a `trains_id` absent from this map means "destination genuinely
    /// unknown," not "no destination," so confirmed-arrival detection is
    /// simply unavailable for it (the same honest-gap posture as every
    /// other `None` in this module).
    pub destination_crs_by_trains_id: HashMap<i64, String>,
}

/// One subscription sharing a `train_uid`, as
/// [`Reference::by_train_uid`]'s value entries -- the subscription id plus
/// **which day's running of that uid it is actually tracking**.
///
/// The `service_date` is the whole point (finding #2 of the 2026-09-25
/// review). A CIF `train_uid` runs EVERY day its schedule is valid, and
/// `api`'s `list_active_tracked_trains` -- the only source of this map --
/// has no date bound in its `WHERE` clause at all: a subscription stays in
/// the active set until its train reaches `completed`/`cancelled` in
/// `train_current_state`, which a subscription whose train was never
/// successfully attributed never does. So yesterday's (or last week's)
/// subscription for uid `C21373` is still in this map when TODAY's
/// Activation for `C21373` arrives, and the fast path used to attribute it
/// without ever comparing the two dates -- binding a subscription to a
/// completely different day's running of the same service, permanently
/// (`ProcessorState::resolved` has no unwind path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharingSubscription {
    pub tracked_train_id: i64,
    /// `common::TrackedTrainRef::service_date` verbatim -- the calendar date
    /// of the pinned/tracked departure, as the subscription was created
    /// with.
    pub service_date: NaiveDate,
    /// `common::TrackedTrainRef::pin_scheduled_departure` verbatim -- `None`
    /// for a subscription with no schedule/pin match yet (the design spec's
    /// own accepted §1 gap; see `apply_reference_reload`'s own comment on
    /// this same field name). Carried through so
    /// `activation_is_for_service_date` can discriminate a legitimate
    /// post-midnight D+1 subscription from an ordinary tomorrow-daytime one
    /// sharing the same `train_uid` (the 2026-09-26 review's finding H3) --
    /// `service_date` alone can't tell those apart, since both carry the
    /// same "one calendar day ahead" relationship to the Activation's rail
    /// day.
    pub pin_scheduled_departure: Option<chrono::DateTime<chrono::Utc>>,
}

/// Cross-batch memory the processing loop accumulates as it observes the
/// feed. Owned by `main.rs` for the whole lifetime of the process and
/// passed in by `&mut`, NOT rebuilt per `run_once` call: every one of these
/// maps exists precisely because a later TRUST message needs something a
/// strictly earlier one carried, and TRUST spreads a single train's
/// Activation / origin Movement / later Movements / Cancellation across
/// many batches.
///
/// Bundled into one struct rather than passed as three `&mut HashMap`
/// parameters so that adding a fourth kind of carried-over state later is a
/// field, not a signature change rippling through every call site and test.
///
/// `trust_timestamp_correction_enabled` (below) is not "cross-batch
/// memory" in the same sense as the maps above -- it's a startup-time
/// config flag (Finding #2's kill switch) that never changes for the life
/// of the process. It's bundled into this struct anyway, rather than
/// threaded as its own parameter through `run_once`/`process_message`,
/// specifically to avoid rippling a signature change through this module's
/// 40+ existing `run_once` test call sites for a value every one of them
/// wants defaulted to `true` -- exactly the "field, not a signature
/// change" tradeoff this doc comment already argues for above. `main.rs`
/// sets it once, right after constructing `ProcessorState::default()`,
/// from `config.trust_timestamp_correction_enabled`.
#[derive(Debug)]
pub struct ProcessorState {
    /// `train_id -> EVERY subscription attributed to it`. Consulted FIRST
    /// by every message type: a train_id in here is already attributed, so
    /// it must never go back through `matching::resolve_origin_departure`
    /// (that function matches *origin departures* against pins; re-running
    /// it on a mid-journey event would at best fail and at worst
    /// mis-attribute).
    ///
    /// `Vec<i64>` for the same reason as `Reference::by_train_uid` above
    /// (review finding I6) -- and it has to change in lockstep with it,
    /// since the Activation fast path writes straight from one into the
    /// other. Every message for a train_id now fans out to one event per
    /// subscription in this list; see `process_message`'s own return type.
    ///
    /// Entries are unioned, never replaced: a subscription already here
    /// stays, and a newly-seen one is appended. The two writers that add to
    /// it (an Activation's `by_train_uid` match, and a Movement's CRS+time
    /// claim) can both legitimately fire for the same train_id at different
    /// times.
    pub resolved: HashMap<String, Vec<i64>>,

    /// `train_id -> parked Activation`, populated by `0001` messages. An
    /// Activation alone can't resolve a pin (per Task 10: this app has no
    /// CIF lookup to bridge `train_uid` to a scheduled departure time), so
    /// it only parks its `train_uid` here to be claimed by whichever
    /// Movement does the resolving. Removed on claim -- one-shot.
    ///
    /// The overwhelming majority of entries are never claimed: this consumer
    /// sees the whole national Activation stream but only ever resolves the
    /// handful of trains its users have pinned. Since the process is
    /// designed to run indefinitely, entries must also age out --
    /// `prune_expired_activations` does that on the reference-reload tick.
    pub pending_activations: HashMap<String, PendingActivation>,

    /// `train_id -> most recently derived state`. Supplies the real
    /// `previous` argument to `journey::apply_movement`/`apply_cancellation`
    /// -- without it a Cancellation would be derived against a blank
    /// `awaiting_activation()` and silently lose the last-known location
    /// that `journey::apply_cancellation` exists to preserve.
    pub last_derived: HashMap<String, DerivedState>,

    /// `train_id`s resolved via `by_train_uid`'s direct-match fast path
    /// (this task) but not yet confirmed by a live Movement. `api`'s own
    /// `upsert_train_event` only flips `resolution_status` on a message
    /// that carries `resolved_train_id` -- since the direct match happens
    /// on the Activation itself (which never posts an event), this set
    /// defers that one-time "freshly resolved" signal to the FIRST
    /// Movement this process sees for the train_id, exactly once.
    pub activation_matched_awaiting_movement: HashSet<String>,

    /// Finding #2's kill switch: whether
    /// `common::trust_timestamp::parse_trust_epoch_millis_pair` may apply
    /// its Europe/London-mislabelling correction at all. See this struct's
    /// own doc comment above for why this lives here rather than as a
    /// `run_once`/`process_message` parameter. Defaults to `true`
    /// (correction on) via this struct's own `Default` impl below, NOT via
    /// `#[derive(Default)]` (which would default a bare `bool` to `false`,
    /// the opposite of this codebase's chosen default of "ship the fix,
    /// give operators an instant off switch").
    pub trust_timestamp_correction_enabled: bool,

    /// Undo log for every mutation the CURRENT, not-yet-confirmed batch has
    /// made to the four maps above -- finding #4's fix. Appended to by the
    /// journaled mutators below (which are the only way `process_message`
    /// touches those maps), cleared by [`ProcessorState::confirm_batch`]
    /// once the batch's downstream POST has succeeded, and replayed in
    /// reverse by [`ProcessorState::roll_back_batch`] on any failure path.
    ///
    /// Why an undo log rather than simply deferring the writes until after
    /// the POST: a batch can legitimately contain an Activation and a later
    /// Movement for the SAME train_id, and that Movement must see the
    /// Activation's attribution (otherwise it falls through to the CRS+time
    /// heuristic and can claim a different pin entirely). The mutations
    /// therefore have to be visible within the batch, and only their
    /// *durability* deferred. Why not a full snapshot/restore of the maps:
    /// `pending_activations` holds the whole national Activation stream and
    /// is routinely tens of thousands of entries, cloned once per cycle
    /// would be pure waste; the journal only ever holds the handful of keys
    /// one batch actually touched.
    journal: Vec<StateChange>,
}

/// One journaled mutation, carrying whatever is needed to undo it. Private:
/// nothing outside this module may construct or interpret these.
#[derive(Debug)]
enum StateChange {
    /// `resolved[train_id]` gained at least one subscription.
    ResolvedAttributed {
        train_id: String,
        previous: Option<Vec<i64>>,
    },
    /// `pending_activations[train_id]` was parked (possibly over a previous
    /// entry for the same recycled train_id).
    ActivationParked {
        train_id: String,
        previous: Option<PendingActivation>,
    },
    /// `pending_activations[train_id]` was consumed by a resolving Movement.
    ActivationClaimed {
        train_id: String,
        claimed: PendingActivation,
    },
    /// `activation_matched_awaiting_movement` gained `train_id`.
    AwaitingMovementMarked { train_id: String },
    /// `activation_matched_awaiting_movement` lost `train_id` (the one-time
    /// "freshly resolved" signal was spent on a Movement).
    AwaitingMovementSpent { train_id: String },
    /// `last_derived[train_id]` was overwritten.
    LastDerivedSet {
        train_id: String,
        previous: Option<DerivedState>,
    },
}

impl Default for ProcessorState {
    fn default() -> Self {
        Self {
            resolved: HashMap::new(),
            pending_activations: HashMap::new(),
            last_derived: HashMap::new(),
            activation_matched_awaiting_movement: HashSet::new(),
            trust_timestamp_correction_enabled: true,
            journal: Vec::new(),
        }
    }
}

impl ProcessorState {
    /// Fresh state with Finding #2's timestamp-correction kill switch set
    /// from config -- `main.rs`'s one constructor call.
    ///
    /// A named constructor rather than `ProcessorState { flag, ..default() }`
    /// at the call site: the functional-update form requires every field to
    /// be visible to the caller, and `journal` (finding #4's undo log) is
    /// deliberately private -- it is this module's own transaction
    /// bookkeeping, mutated only through the journaled helpers below, and
    /// nothing outside may construct or inspect it.
    pub fn new(trust_timestamp_correction_enabled: bool) -> Self {
        Self {
            trust_timestamp_correction_enabled,
            ..Self::default()
        }
    }

    /// The batch's events reached `api`. Its mutations are now durable
    /// facts, so the undo log is dropped. Called by `main.rs`'s `run_cycle`
    /// and by nothing else.
    pub fn confirm_batch(&mut self) {
        self.journal.clear();
    }

    /// The batch failed somewhere between `next_batch` and a confirmed
    /// POST, so it will be redelivered (Redis: `reclaim_stale`'s 30-second
    /// replay or the startup PEL replay; Kafka: `KafkaMovementFeed`'s
    /// seek-back). Undo everything it did, newest change first, so the
    /// replay sees the same state the first attempt saw and can produce the
    /// same events -- including the one-time
    /// `resolved_train_uid`/`resolved_train_id` resolution signal, which was
    /// exactly what used to be lost (finding #4).
    pub fn roll_back_batch(&mut self) {
        while let Some(change) = self.journal.pop() {
            match change {
                StateChange::ResolvedAttributed { train_id, previous } => match previous {
                    Some(ids) => {
                        self.resolved.insert(train_id, ids);
                    }
                    None => {
                        self.resolved.remove(&train_id);
                    }
                },
                StateChange::ActivationParked { train_id, previous } => match previous {
                    Some(activation) => {
                        self.pending_activations.insert(train_id, activation);
                    }
                    None => {
                        self.pending_activations.remove(&train_id);
                    }
                },
                StateChange::ActivationClaimed { train_id, claimed } => {
                    self.pending_activations.insert(train_id, claimed);
                }
                StateChange::AwaitingMovementMarked { train_id } => {
                    self.activation_matched_awaiting_movement.remove(&train_id);
                }
                StateChange::AwaitingMovementSpent { train_id } => {
                    self.activation_matched_awaiting_movement.insert(train_id);
                }
                StateChange::LastDerivedSet { train_id, previous } => match previous {
                    Some(derived) => {
                        self.last_derived.insert(train_id, derived);
                    }
                    None => {
                        self.last_derived.remove(&train_id);
                    }
                },
            }
        }
    }

    /// Unions `tracked_train_ids` into `resolved[train_id]`, journaled.
    /// Returns whether anything was actually added -- the "freshly resolved"
    /// signal is raised only then, so a redelivered Activation for an
    /// already fully-attributed train doesn't make the next Movement
    /// re-announce a resolution.
    fn attribute(&mut self, train_id: &str, tracked_train_ids: &[i64]) -> bool {
        let existing = self.resolved.get(train_id);
        let already_present = |id: &i64| existing.is_some_and(|ids: &Vec<i64>| ids.contains(id));
        if tracked_train_ids.iter().all(already_present) {
            return false;
        }
        let previous = existing.cloned();
        let attributed = self.resolved.entry(train_id.to_string()).or_default();
        for &tracked_train_id in tracked_train_ids {
            if !attributed.contains(&tracked_train_id) {
                attributed.push(tracked_train_id);
            }
        }
        self.journal.push(StateChange::ResolvedAttributed {
            train_id: train_id.to_string(),
            previous,
        });
        true
    }

    /// Parks an Activation's binding for a later Movement to claim,
    /// journaled.
    fn park_activation(&mut self, train_id: &str, activation: PendingActivation) {
        let previous = self
            .pending_activations
            .insert(train_id.to_string(), activation);
        self.journal.push(StateChange::ActivationParked {
            train_id: train_id.to_string(),
            previous,
        });
    }

    /// One-shot claim of a parked Activation's `train_uid` by the Movement
    /// that resolves the pin, journaled.
    fn claim_activation(&mut self, train_id: &str) -> Option<String> {
        let claimed = self.pending_activations.remove(train_id)?;
        let train_uid = claimed.train_uid.clone();
        self.journal.push(StateChange::ActivationClaimed {
            train_id: train_id.to_string(),
            claimed,
        });
        Some(train_uid)
    }

    /// Defers an Activation-fast-path resolution's one-time "freshly
    /// resolved" signal to the first Movement seen for this train_id,
    /// journaled.
    fn mark_awaiting_movement(&mut self, train_id: &str) {
        if self
            .activation_matched_awaiting_movement
            .insert(train_id.to_string())
        {
            self.journal.push(StateChange::AwaitingMovementMarked {
                train_id: train_id.to_string(),
            });
        }
    }

    /// Spends that deferred signal, exactly once, journaled.
    fn spend_awaiting_movement(&mut self, train_id: &str) -> bool {
        if self.activation_matched_awaiting_movement.remove(train_id) {
            self.journal.push(StateChange::AwaitingMovementSpent {
                train_id: train_id.to_string(),
            });
            true
        } else {
            false
        }
    }

    /// Records the state derived for this message, journaled -- so a
    /// redelivered batch derives against the same `previous` state the first
    /// attempt did rather than against its own half-applied result.
    fn set_last_derived(&mut self, train_id: &str, derived: DerivedState) {
        let previous = self.last_derived.insert(train_id.to_string(), derived);
        self.journal.push(StateChange::LastDerivedSet {
            train_id: train_id.to_string(),
            previous,
        });
    }
}

/// What an Activation parks for a later Movement to claim: the `train_uid`
/// the event actually needs, plus enough to know when the entry is safe to
/// forget.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingActivation {
    pub train_uid: String,
    /// `Activation::schedule_end_date` parsed as a date, or `None` when it
    /// was absent or didn't parse. `None` means "expiry unknown", and
    /// `prune_expired_activations` deliberately fails *open* on it: an entry
    /// whose end date can't be read is kept rather than dropped, so a
    /// malformed field costs a little memory instead of silently losing a
    /// binding the feed did send us.
    ///
    /// This is the CIF schedule's own VALIDITY-WINDOW end, months away for
    /// a permanent schedule -- which is precisely why it is no longer the
    /// primary pruning signal (finding #5). See `observed_rail_day`.
    pub schedule_end_date: Option<NaiveDate>,
    /// The Europe/London rail day this Activation was OBSERVED on
    /// (`common::rail_day::current_rail_day(received_at)`) -- i.e. which
    /// day's running it is actually for, and the primary pruning signal.
    ///
    /// TRUST delivers an Activation in real time, for that specific day's
    /// running, so the day it is processed on IS its running day. Nothing
    /// else on the message carries that: `schedule_start_date`/
    /// `schedule_end_date` are the CIF schedule's multi-month validity
    /// window (confirmed the hard way -- see
    /// `trust-backlog-consumer`'s `process.rs` module doc for the live
    /// production bug that came of treating `schedule_start_date` as a
    /// running date), and this crate deliberately does not decode the
    /// `train_id` string's internal structure, which is not part of any
    /// shape this codebase has confirmed.
    pub observed_rail_day: NaiveDate,
}

/// Applies one reference-reload tick's worth of `api` state to the
/// processing loop's own view. Lives here rather than inline in `main.rs`
/// so the rehydration rules below are unit-testable without an HTTP layer.
///
/// Two distinct jobs, both driven off the same fetch:
///
/// 1. `reference.pending` is rebuilt from scratch from the still-`pending`
///    refs -- pins that resolved elsewhere must stop being matchable.
/// 2. `state.resolved` is *seeded* from refs that are already `resolved` and
///    carry a `train_id`. Without this, a restart is permanently lossy: the
///    only other way into `state.resolved` is matching a fresh origin
///    departure, and a train whose origin departure already happened will
///    never emit another one -- so every remaining movement and any
///    cancellation for it would be dropped forever. `TrackedTrainRef`'s own
///    doc comment names this consumer as the reason those already-resolved
///    refs are returned at all.
///
/// Seeding UNIONS into `state.resolved` rather than replacing it: a
/// resolution this process made in memory is strictly fresher than a row
/// that may have been read before it was written, so it is never dropped --
/// but a subscription the reload knows about and this process hasn't seen
/// yet is added alongside it rather than being discarded.
pub fn apply_reference_reload(
    refs: Vec<common::TrackedTrainRef>,
    reference: &mut Reference,
    state: &mut ProcessorState,
) {
    let mut pending = Vec::new();
    let mut by_train_uid: HashMap<String, Vec<SharingSubscription>> = HashMap::new();
    let mut trains_id_by_tracked_train_id = HashMap::new();
    let mut destination_crs_by_trains_id = HashMap::new();

    for tracked in refs {
        if let Some(trains_id) = tracked.trains_id {
            trains_id_by_tracked_train_id.insert(tracked.id, trains_id);
            // Same "regardless of resolution_status" posture as
            // `trains_id_by_tracked_train_id` just above -- an
            // already-`resolved` subscription's later movements still need
            // to recognize a confirmed terminus ARRIVAL, not just a freshly
            // schedule-matched one.
            if let Some(destination_crs) = &tracked.destination_crs {
                destination_crs_by_trains_id.insert(trains_id, destination_crs.clone());
            }
        }
        match tracked.resolution_status.as_str() {
            // `schedule_matched` is treated exactly like `pending` here --
            // it already carries a `train_uid` (now consulted FIRST, via
            // `by_train_uid`, by a live Activation's direct-match fast
            // path -- see `process_message`), but it still has no
            // `train_id`, so it must stay eligible for the same
            // live-Movement claim a plain `pending` row is (Decision 3 of
            // docs/superpowers/specs/2026-09-05-schedule-first-train-tracking-design.md)
            // as a fallback for whichever pins don't get caught by the
            // direct match first.
            "pending" | "schedule_matched" => {
                if let Some(train_uid) = &tracked.train_uid {
                    // Push, never `insert` (review finding I6): a blind
                    // single-value insert kept only whichever subscriber
                    // this loop visited last, which is precisely how two
                    // people tracking the same train ended up with one of
                    // them stuck at `'pending'` forever.
                    //
                    // `service_date` is carried through with the id (finding
                    // #2): `list_active_tracked_trains` has no date bound,
                    // so this map routinely holds subscriptions for several
                    // different days' running of the same uid, and the
                    // Activation fast path has to be able to tell them
                    // apart.
                    // `pin_scheduled_departure` is carried through too (the
                    // 2026-09-26 review's finding H3): `service_date` alone
                    // can't tell a legitimate post-midnight D+1 subscription
                    // from an ordinary tomorrow-daytime one sharing this
                    // uid, and `activation_is_for_service_date` needs the
                    // subscription's own booked time to do that.
                    by_train_uid
                        .entry(train_uid.clone())
                        .or_default()
                        .push(SharingSubscription {
                            tracked_train_id: tracked.id,
                            service_date: tracked.service_date,
                            pin_scheduled_departure: tracked.pin_scheduled_departure,
                        });
                }
                // `pin_origin_crs`/`pin_scheduled_departure` are `None` for
                // an NR-primary subscription (Task 20) whose `trains` row
                // has no schedule data yet (the design spec's own accepted
                // §1 gap) -- there is nothing for the CRS+time heuristic to
                // match against in that case, so such a row is simply never
                // added to `pending` (it may still be caught via
                // `by_train_uid` above, once Task 21's read cutover lands).
                if let (Some(pin_origin_crs), Some(pin_scheduled_departure)) =
                    (tracked.pin_origin_crs, tracked.pin_scheduled_departure)
                {
                    pending.push(crate::matching::PendingPin {
                        tracked_train_id: tracked.id,
                        pin_origin_crs,
                        pin_scheduled_departure,
                        // Carried so `process_message`'s contradiction
                        // filter can refuse a CRS+time claim that a parked
                        // Activation already proves is for a different
                        // schedule -- see that filter's own comment.
                        train_uid: tracked.train_uid,
                    });
                }
            }
            "resolved" => {
                if let Some(train_id) = tracked.train_id {
                    // Union, not `or_insert` of a single value: several
                    // already-`resolved` subscriptions can share one
                    // `train_id`, and every one of them still needs its
                    // later movements attributed after a restart.
                    let ids = state.resolved.entry(train_id).or_default();
                    if !ids.contains(&tracked.id) {
                        ids.push(tracked.id);
                    }
                }
            }
            _ => {}
        }
    }

    reference.pending = pending;
    reference.by_train_uid = by_train_uid;
    reference.trains_id_by_tracked_train_id = trains_id_by_tracked_train_id;
    reference.destination_crs_by_trains_id = destination_crs_by_trains_id;
}

/// How many rail days a parked, unclaimed Activation is kept for before it
/// is aged out. Two, not one: an overnight working activated late on rail
/// day D is still emitting Movements well into rail day D+1, and a pin for
/// it may not be claimed until then. Three or more buys nothing -- the
/// binding is only ever useful to a Movement of the same running.
pub const MAX_PARKED_ACTIVATION_AGE_DAYS: i64 = 2;

/// Drops parked Activations that can no longer be of use to any Movement.
/// Pure, so the caller supplies `today` (the current Europe/London rail
/// day, per `common::rail_day::current_rail_day`) rather than this reading
/// the clock.
///
/// # Why this stopped keying on `schedule_end_date` (finding #5 of the 2026-09-25 review)
///
/// This function used to drop an entry only when its own
/// `schedule_end_date` had passed. For a PERMANENT CIF schedule -- most of
/// the national timetable -- that date is the end of the schedule's
/// multi-month validity window, typically the next timetable change, so
/// almost nothing was ever actually pruned: a map fed by the entire
/// national Activation stream (tens of thousands of entries a day, of which
/// this consumer only ever claims the handful its users have pinned) grew
/// essentially unbounded, and the doc comment promising otherwise was
/// wrong. Parse failures made it worse by failing open. Growth was bounded
/// in practice only by TRUST's own `train_id` reuse cadence, roughly
/// monthly, overwriting entries by key.
///
/// The primary rule is now the Activation's own OBSERVED rail day, which is
/// the day its running actually belongs to (see
/// `PendingActivation::observed_rail_day`): anything older than
/// [`MAX_PARKED_ACTIVATION_AGE_DAYS`] is gone, because no Movement that
/// could still claim it exists. That also incidentally protects the
/// correctness of a claim: a stale entry surviving until TRUST recycled its
/// `train_id` would have handed a completely unrelated train's `train_uid`
/// to whatever Movement claimed it.
///
/// `schedule_end_date` is kept as a cheap secondary signal (a schedule that
/// has genuinely ended can go immediately), still failing open when absent
/// or unparseable.
pub fn prune_expired_activations(
    activations: &mut HashMap<String, PendingActivation>,
    today: NaiveDate,
) {
    let oldest_kept = today - chrono::Duration::days(MAX_PARKED_ACTIVATION_AGE_DAYS);
    activations.retain(|_, activation| {
        let fresh_enough = activation.observed_rail_day >= oldest_kept;
        let schedule_still_running = match activation.schedule_end_date {
            Some(end) => end >= today,
            None => true,
        };
        fresh_enough && schedule_still_running
    });
}

/// Applies one `stanox_crs` reload tick's HTTP result to the shared cell.
/// Pure with respect to the swap-vs-keep *decision* -- given directly what
/// the fetch produced, not performing the fetch itself -- so the fail-open
/// policy below is unit-testable without a live `api`, mirroring
/// `apply_reference_reload`'s own split from `queries::fetch_active_tracked_trains`.
///
/// Fails open in both failure shapes: an `Err` (network/HTTP failure) or an
/// empty `Ok` (fresh environment, or `schedule-reference` has never
/// successfully run) both leave the currently-loaded table (CSV-derived at
/// startup, or a previously-fetched live one) untouched, never swapping in
/// an empty table that would silently stop translating every STANOX. See
/// the spec's Error handling section.
pub fn apply_stanox_crs_reload(
    fetched: anyhow::Result<Vec<common::StanoxCrsRecord>>,
    cell: &std::sync::RwLock<crate::stanox_crs::StanoxCrsTable>,
) {
    match fetched {
        Ok(records) if !records.is_empty() => {
            let count = records.len();
            let table = crate::stanox_crs::StanoxCrsTable::from_records(records);
            *cell.write().expect("stanox_crs lock poisoned") = table;
            tracing::info!(count, "reloaded live stanox/crs table");
        }
        Ok(_) => {
            tracing::warn!("live stanox_crs table is empty; keeping the currently loaded table");
        }
        Err(err) => {
            tracing::error!(error = ?err, "failed to reload stanox_crs table; keeping the currently loaded table");
            metrics::counter!(
                common::metrics::metric_name("trust_consumer_errors_total"),
                "operation" => "reload_stanox_crs"
            )
            .increment(1);
        }
    }
}

/// Pure by design (see this module's own doc comment on why `run_once`
/// itself returns only `Vec<TrainMovementEventMessage>`, untouched by this
/// task): building a forwarding signal is a separate concern from
/// resolving/deriving movement state, and keeping it out of `run_once`
/// means none of that function's own 25+ existing tests need updating for
/// this feature. Filters out any event whose `tracked_train_id` has no
/// known `trains_id` yet -- exactly the same accepted gap named throughout
/// this plan (a subscription whose identity, and therefore trains_id, is
/// still unknown has nothing to forward a signal about).
///
/// Deduplicated by `trains_id`, first occurrence winning. This became
/// load-bearing with review finding I6's fix: `process_message` now fans one
/// TRUST message out to one event per subscriber sharing the train, and
/// every one of those carries the SAME `trains_id`, so an undeduplicated
/// build would enqueue N identical forwarding rows for one real-world
/// event. `notifier` reads the queue with `SELECT DISTINCT trains_id`
/// (`crates/notifier/src/queries.rs`), so the duplicates were never going to
/// produce duplicate notifications -- this keeps them out of the queue
/// table in the first place, where they would otherwise scale with a
/// popular train's subscriber count.
pub fn build_forward_signals(
    events: &[common::TrainMovementEventMessage],
    trains_id_by_tracked_train_id: &HashMap<i64, i64>,
) -> Vec<common::TrainForwardSignalMessage> {
    let mut seen: HashSet<i64> = HashSet::new();
    events
        .iter()
        .filter_map(|event| {
            let trains_id = *trains_id_by_tracked_train_id.get(&event.tracked_train_id)?;
            if !seen.insert(trains_id) {
                return None;
            }
            Some(common::TrainForwardSignalMessage {
                trains_id,
                event_summary: format!(
                    "{} at {}",
                    event.status,
                    event
                        .last_reported_location
                        .as_deref()
                        .unwrap_or("an unknown location")
                ),
            })
        })
        .collect()
}

/// One full cycle: pull whatever the feed has, parse it, resolve/derive
/// against `reference` and `state`, and return the batch of events that
/// would be posted to `api` -- NOT posted here, so tests can assert on the
/// returned `Vec` directly without an HTTP layer in the loop at all.
/// `main.rs`'s real loop posts this return value and only then calls
/// `feed.commit()`.
///
/// `received_at` is the wall-clock time this whole batch is being
/// processed at, supplied by the caller (`main.rs` passes
/// `chrono::Utc::now()`) rather than read from the clock in here, so this
/// function stays a pure function of its arguments -- same posture as
/// `prune_expired_activations`'s own caller-supplied `today`. One value for
/// the whole batch, not one per message, is a deliberate simplification:
/// `next_batch` returns whatever Kafka/Redis Streams has ready right now,
/// and the messages in one batch are processed within, at most, a handful
/// of milliseconds of each other -- far inside
/// `common::trust_timestamp::MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT` -- so a
/// single per-batch timestamp is indistinguishable from a per-message one
/// for the plausibility guard's purposes. Threaded into every TRUST
/// timestamp parsed this cycle (via
/// `common::trust_timestamp::parse_trust_epoch_millis`) and into
/// `matching::resolve_origin_departure`'s own guard.
pub async fn run_once<F: MovementFeed>(
    feed: &mut F,
    reference: &Reference,
    state: &mut ProcessorState,
    stanox_crs: &crate::stanox_crs::StanoxCrsTable,
    received_at: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Vec<common::TrainMovementEventMessage>> {
    let raw_batches = feed.next_batch().await?;
    let mut events = Vec::new();

    for raw in raw_batches {
        // ONE unparseable payload must not take the rest of the batch down
        // with it (finding #8 of the 2026-09-25 review). This used to be a
        // `?`, which aborted the whole `run_once` call: under the Redis
        // backend a batch is up to 100 entries and `commit` acks all or
        // nothing, so a single poison payload left every good entry beside
        // it unacked, to be reclaimed 30 seconds later, fail identically,
        // and be reclaimed again -- trapping real events indefinitely
        // behind one bad one. There is no retry that can ever fix a payload
        // that doesn't parse, so the honest handling is to log it loudly
        // (with the raw bytes) and keep going, exactly as
        // `trust-backlog-consumer`'s own main loop already does.
        //
        // The raw payload is logged, not just the error, because this
        // codebase has already been burned twice by an envelope-shape
        // assumption that looked right on paper (a JSON-array batch, then a
        // bare single-object envelope) but didn't match what a real broker
        // sent; the fix both times was a live payload, not another guess.
        let messages = match trust_schema::schema::parse_batch(&raw) {
            Ok(messages) => messages,
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    raw = %raw,
                    "failed to parse a TRUST payload; dropping just this payload and continuing \
                     with the rest of the batch"
                );
                metrics::counter!(
                    common::metrics::metric_name("trust_consumer_errors_total"),
                    "operation" => "parse_batch"
                )
                .increment(1);
                continue;
            }
        };
        for message in messages {
            // Metrics recording, not a change to this function's return
            // value or any behavior the 25 existing tests in this module
            // assert on -- the same "tolerated side effect inside an
            // otherwise value-returning function" posture this codebase
            // already takes with logging (e.g. `schema.rs`'s own
            // warn-and-drop). Labelled with the SAME raw msg_type string
            // `movement-relay`'s own `movement_relay_events_published_total`
            // counter uses (`crates/movement-relay/src/main.rs`), so the
            // two are directly comparable side by side: published vs.
            // received, per type, answering "is trust-consumer actually
            // seeing what movement-relay is sending it."
            metrics::counter!(
                common::metrics::metric_name("trust_consumer_events_received_total"),
                "msg_type" => msg_type_label(&message)
            )
            .increment(1);
            // One message can now produce MORE than one event -- one per
            // subscription sharing the resolved train (review finding I6).
            // The counter still counts events, not messages, so it stays
            // directly comparable with `trust_consumer_events_received_total`
            // in the same way it always was.
            for event in process_message(&message, reference, state, stanox_crs, received_at) {
                metrics::counter!(common::metrics::metric_name(
                    "trust_consumer_events_matched_total"
                ))
                .increment(1);
                events.push(event);
            }
        }
    }

    Ok(events)
}

/// The raw `msg_type` string this `TrustMessage` was parsed from -- the
/// same strings `trust_schema::schema::parse_envelope`'s own match arms
/// dispatch on ("0001"/"0002"/"0003"/"0006"/"0007"), reconstructed here
/// (rather than carried on the enum itself, which has no reason to know
/// about its own wire tag once parsed) purely so `run_once`'s metrics
/// above can label by the same value `movement-relay` already labels its
/// own publish counter with. `Unknown` already carries its own raw
/// `msg_type` string for exactly this kind of use.
fn msg_type_label(message: &TrustMessage) -> &'static str {
    match message {
        TrustMessage::Activation(_) => "0001",
        TrustMessage::Cancellation(_) => "0002",
        TrustMessage::Movement(_) => "0003",
        TrustMessage::ChangeOfOrigin(_) => "0006",
        TrustMessage::ChangeOfIdentity(_) => "0007",
        // Genuinely reachable under the Kafka backend (`process_message`'s
        // own `Unknown` arm below logs and drops these) -- `parse_batch`
        // does NOT filter them out itself (confirmed against
        // `schema.rs`'s own test asserting `Unknown` surfaces in its
        // output). Under the redis-stream backend this should be rare to
        // absent in practice, since `movement-relay`'s own
        // `confirmed_envelope_bodies` already drops unconfirmed types
        // before ever publishing to Redis -- but this crate can still run
        // against direct Kafka (`MovementFeedBackend::Kafka`), where no
        // such upstream filter exists, so this label stays real rather
        // than theoretical.
        TrustMessage::Unknown(_) => "unknown",
    }
}

/// Whether a subscription tracking `service_date` (and, if known, its own
/// `pin_scheduled_departure`) may be attributed to an Activation observed on
/// `activation_rail_day` -- finding #2's date check, tightened by finding H3
/// of the 2026-09-26 review.
///
/// Two accepted dates, not one:
///
/// - `service_date == activation_rail_day` is the ordinary case, accepted
///   unconditionally: a CIF `train_uid` runs at most once per rail day, so
///   agreement on the day alone already identifies the one running it can
///   mean.
/// - `service_date == activation_rail_day + 1 day` is the legitimate
///   post-midnight case, and it is not an edge case worth losing. A rail day
///   runs 02:00 to 02:00 Europe/London, so a service departing at (say)
///   00:12 has the NEXT calendar date as its `service_date` -- that's the
///   date the departure board it was pinned from showed -- while its
///   Activation, which TRUST emits before the train runs, lands in the
///   earlier rail day.
///
///   **This is no longer accepted on the date arithmetic alone (finding
///   H3).** `service_date + 1` is also exactly the shape of a completely
///   different, not-yet-run recurring service -- e.g. a Mon-Fri commute's
///   subscription for TOMORROW's ordinary daytime departure, already sitting
///   in `by_train_uid` because `list_active_tracked_trains` has no upper
///   date bound. Nothing about `service_date` distinguishes "one day ahead
///   because it's an overnight service" from "one day ahead because it
///   hasn't run yet", so the D+1 branch additionally requires the
///   subscription's own `pin_scheduled_departure` to fall in the SAME rail
///   day as the Activation (via `common::rail_day::current_rail_day`,
///   the same 02:00 Europe/London cutoff this whole module already keys on)
///   -- true for a genuine 00:12 departure, false for an ordinary 17:30 one.
///   A subscription with no `pin_scheduled_departure` at all (no
///   schedule/pin match yet) can't be checked this way, so it is rejected
///   for D+1 rather than trusted -- it stays eligible for the ordinary
///   pin-claim heuristic once it actually runs.
///
/// Everything else is rejected, and note the asymmetry is deliberate: a
/// `service_date` BEHIND the Activation's rail day is never legitimate --
/// that is precisely the stale-subscription shape finding #2 is about
/// (yesterday's, or last week's, running of a daily-repeating uid) -- while
/// one day AHEAD, once confirmed via `pin_scheduled_departure`, is an
/// ordinary overnight service. Being wrong in the rejecting direction costs
/// only the fast path for that subscription (the pin-claim heuristic still
/// runs); being wrong in the accepting direction binds a subscription to the
/// wrong day's train with no way back.
fn activation_is_for_service_date(
    service_date: NaiveDate,
    pin_scheduled_departure: Option<chrono::DateTime<chrono::Utc>>,
    activation_rail_day: NaiveDate,
) -> bool {
    if service_date == activation_rail_day {
        return true;
    }
    if service_date != activation_rail_day + chrono::Duration::days(1) {
        return false;
    }
    // D+1 only: confirm it's a genuine post-midnight departure (same rail
    // day as the Activation) rather than an arbitrary tomorrow-daytime
    // running of the same uid.
    match pin_scheduled_departure {
        Some(departure) => common::rail_day::current_rail_day(departure) == activation_rail_day,
        None => false,
    }
}

/// Returns one event PER SUBSCRIPTION attributed to this message's train
/// -- an empty `Vec` for a message that resolves nothing, one element in
/// the ordinary single-subscriber case, and N for N subscribers sharing one
/// physical train (review finding I6).
///
/// Fanning out here, rather than adding a subscription list to
/// `common::TrainMovementEventMessage`, is deliberate and is the smaller of
/// the two changes: that wire type is shared with `api`'s ingest route,
/// `trust-backlog-consumer`, and `upsert_train_event`'s whole call path, and
/// every one of those already treats a batch of events as the unit of work.
/// The duplicated events are also genuinely cheap and safe on the receiving
/// side: `upsert_train_movement` is keyed on `trains_id` (shared by all of
/// them) with `ON CONFLICT DO NOTHING`/`DO UPDATE`, so the movement row is
/// written once no matter how many arrive, and the only genuinely
/// per-subscription work -- `flip_legacy_resolution`'s
/// `resolution_status` update -- is exactly the thing that needs to happen
/// once per subscriber and previously happened for only one of them.
fn process_message(
    message: &TrustMessage,
    reference: &Reference,
    state: &mut ProcessorState,
    stanox_crs: &crate::stanox_crs::StanoxCrsTable,
    received_at: chrono::DateTime<chrono::Utc>,
) -> Vec<common::TrainMovementEventMessage> {
    match message {
        // An Activation never produces a posted event of its own -- it only
        // parks its train_uid for the Movement that eventually resolves a
        // pin for this train_id to claim.
        TrustMessage::Activation(activation) => {
            // Which day's running this Activation is for. TRUST delivers an
            // Activation in real time for that specific day, so the rail day
            // it is observed on IS its running day -- see
            // `PendingActivation::observed_rail_day` for why nothing on the
            // message itself can be used instead.
            let activation_rail_day = common::rail_day::current_rail_day(received_at);

            // Direct train_uid match FIRST, per the design spec's §3: strictly
            // more reliable than the CRS+time departure heuristic, since it
            // needs no location or timing coincidence at all.
            //
            // BUT only for the subscriptions actually tracking THIS day's
            // running (finding #2 of the 2026-09-25 review). A CIF
            // `train_uid` runs every day its schedule is valid, and
            // `by_train_uid` is built from `list_active_tracked_trains`,
            // whose `WHERE` clause has no date bound at all -- so this map
            // routinely holds a subscription for some earlier day's running
            // of the very same uid, and attributing it here bound that
            // subscription to a completely different day's train,
            // irreversibly. A subscription whose date disagrees is simply
            // left alone: it falls through to the pin-claim heuristic
            // (`matching::resolve_origin_departure`) exactly as a
            // subscription with no known identity does, which is the
            // correct, non-destructive outcome.
            //
            // EVERY date-matching subscription sharing this train_uid is
            // attributed, not just one (review finding I6). Unioned rather
            // than replaced, so an existing resolution -- from an earlier
            // Activation, a CRS+time claim, or the reference reload's
            // rehydration -- is never clobbered; only genuinely new
            // subscriptions are added. The "freshly resolved" flag is
            // raised only if something actually WAS added, so a redelivered
            // Activation for a fully-attributed train doesn't make the next
            // Movement re-announce a resolution.
            if let Some(sharing) = reference.by_train_uid.get(&activation.train_uid) {
                let (for_this_running, for_another_day): (Vec<_>, Vec<_>) =
                    sharing.iter().partition(|subscription| {
                        activation_is_for_service_date(
                            subscription.service_date,
                            subscription.pin_scheduled_departure,
                            activation_rail_day,
                        )
                    });
                if !for_another_day.is_empty() {
                    tracing::info!(
                        train_uid = %activation.train_uid,
                        train_id = %activation.train_id,
                        activation_rail_day = %activation_rail_day,
                        skipped = ?for_another_day
                            .iter()
                            .map(|s| (s.tracked_train_id, s.service_date))
                            .collect::<Vec<_>>(),
                        "not attributing this Activation to subscription(s) tracking a different \
                         day's running of the same train_uid; they stay eligible for the \
                         ordinary pin-claim heuristic instead"
                    );
                }
                let tracked_train_ids: Vec<i64> = for_this_running
                    .iter()
                    .map(|subscription| subscription.tracked_train_id)
                    .collect();
                if !tracked_train_ids.is_empty()
                    && state.attribute(&activation.train_id, &tracked_train_ids)
                {
                    state.mark_awaiting_movement(&activation.train_id);
                }
            }
            state.park_activation(
                &activation.train_id,
                PendingActivation {
                    train_uid: activation.train_uid.clone(),
                    // TRUST's schedule dates are `YYYY-MM-DD`, which is
                    // exactly `NaiveDate`'s own `FromStr` format. An absent
                    // or unreadable value parks with an unknown expiry
                    // rather than failing.
                    schedule_end_date: activation
                        .schedule_end_date
                        .as_deref()
                        .and_then(|raw| raw.parse::<NaiveDate>().ok()),
                    observed_rail_day: activation_rail_day,
                },
            );
            Vec::new()
        }

        TrustMessage::Movement(movement) => {
            // ONE correction decision for both fields, anchored on
            // `actual_timestamp` -- see
            // `common::trust_timestamp::parse_trust_epoch_millis_pair`'s own
            // doc comment for why calling the single-field
            // `parse_trust_epoch_millis` independently on `planned`/`actual`
            // (the pre-fix behavior) could desync them by a full hour
            // (Finding #1).
            let timestamp_pair = common::trust_timestamp::parse_trust_epoch_millis_pair(
                movement.planned_timestamp.as_deref(),
                movement.actual_timestamp.as_deref(),
                received_at,
                state.trust_timestamp_correction_enabled,
            );
            let planned = timestamp_pair.planned;
            let actual = timestamp_pair.actual;
            // Finding #2's operator signal: how often the correction
            // actually fires vs. falls back to raw, so a change in the
            // upstream feed's own behavior (e.g. a vendor fix landing) shows
            // up here rather than only via user complaints. Only counted
            // when a decision was actually made (`Some`) -- `None` means
            // there was no `actual_timestamp` to anchor on at all.
            if let Some(was_corrected) = timestamp_pair.was_corrected {
                metrics::counter!(
                    common::metrics::metric_name("trust_consumer_timestamp_correction_total"),
                    "outcome" => if was_corrected { "corrected" } else { "raw" }
                )
                .increment(1);
            }
            // Real translation now -- see `stanox_crs`'s module doc for
            // where the table comes from and why a miss (`None`) is the
            // honest, expected outcome for a non-passenger or otherwise
            // unmapped STANOX, not a bug.
            let loc_crs: Option<String> = movement
                .loc_stanox
                .as_deref()
                .and_then(|stanox| stanox_crs.stanox_to_crs(stanox));

            // Already-resolved train_ids short-circuit matching entirely;
            // only a genuinely unseen train_id is offered to the pins.
            let (tracked_train_ids, freshly_resolved) =
                match state.resolved.get(&movement.train_id).cloned() {
                    Some(tracked_train_ids) => {
                        // Already resolved -- either by this same branch on
                        // an earlier Movement, by the reference reload's
                        // rehydration, or (this task) by an Activation's
                        // direct train_uid match. That last case never got
                        // to post its own event (an Activation never does),
                        // so the FIRST Movement seen for this train_id after
                        // it is the one that must carry the one-time
                        // "freshly resolved" signal for `api`'s db flip.
                        let freshly_resolved = state.spend_awaiting_movement(&movement.train_id);
                        (tracked_train_ids, freshly_resolved)
                    }
                    None => {
                        // Only a DEPARTURE may claim a pin. `resolve_origin_departure`
                        // knows nothing about event types -- it compares a
                        // location and two times, and TRUST's `event_type` is
                        // one of ARRIVAL / DEPARTURE / PASS (see
                        // `schema::Movement`). At a busy terminus an ARRIVAL
                        // or PASS near a pin's scheduled departure would
                        // otherwise satisfy both tests and claim it, and
                        // a claim is one-way: `state.resolved` has no unwind
                        // path, so the train that should have matched is
                        // locked out for the life of the process. Filtered
                        // here rather than inside `matching`, for the same
                        // reason as the `claimed` filter just below: that
                        // module stays a pure function of its arguments.
                        if movement.event_type != "DEPARTURE" {
                            return Vec::new();
                        }

                        let Some(actual_ts) = actual else {
                            return Vec::new();
                        };
                        // A pin can only ever be claimed by a Movement whose
                        // location translated to a real CRS -- an untranslated
                        // STANOX can never equal a pin's `pin_origin_crs`, so
                        // there's nothing to attempt a match against. This
                        // mirrors the existing early-returns just above for a
                        // missing `event_type`/`actual_timestamp`.
                        let Some(loc_crs_for_match) = loc_crs.as_deref() else {
                            return Vec::new();
                        };

                        // A pin already claimed by some other train_id must not
                        // be offered again. `resolve_origin_departure` has no
                        // notion of "taken", so two different trains
                        // departing the same origin close enough together
                        // would otherwise both resolve to the same
                        // tracked_train_id and flip-flop what the user sees.
                        // Filtering here rather than inside `matching` keeps
                        // that module a pure function of its arguments.
                        let claimed: HashSet<i64> =
                            state.resolved.values().flatten().copied().collect();

                        // CONTRADICTION FILTER -- the fix for a confirmed
                        // production mis-attribution (2026-09-25). A pin
                        // that already knows its own CIF identity
                        // (`PendingPin::train_uid`, set for every
                        // `schedule_matched`/NR-primary subscription) is
                        // still offered to this ±20-minute CRS+time
                        // heuristic, deliberately, as a fallback for
                        // whichever pins the Activation direct match
                        // (`by_train_uid`) didn't catch -- see
                        // `apply_reference_reload`'s own comment and
                        // Decision 3 of
                        // docs/superpowers/specs/2026-09-05-schedule-first-train-tracking-design.md.
                        // But when this process has ALSO parked an
                        // Activation for the `train_id` now trying to claim
                        // a pin, it already knows -- for certain, from
                        // TRUST's own `0001` message -- which `train_uid`
                        // that `train_id` is. A pin naming a DIFFERENT
                        // `train_uid` is then provably not this train, and
                        // must not be claimed however well the location and
                        // the ±20-minute window happen to line up.
                        //
                        // The real case this closes: London Euston, 2026-09-25.
                        // A user tracked `Y80926` (the 18:56 to Birmingham
                        // New Street, `pin_scheduled_departure` 17:56:00Z).
                        // TRUST reported `train_id` `721F34MX25` -- whose
                        // own Activation names `train_uid` `W34058`, the
                        // 18:43 Euston to Liverpool Lime Street -- departing
                        // EUSTON at 17:42:00Z. That is 14 minutes from the
                        // pin's scheduled departure, comfortably inside
                        // `common::MATCH_TOLERANCE`, so the heuristic
                        // claimed the pin, and every subsequent movement of
                        // the Liverpool train (Wembley Central, Harrow &
                        // Wealdstone, Watford Junction) was written onto the
                        // Birmingham train's shared row. The user's journey
                        // read "En route" -- via `trust_schema::journey::
                        // apply_movement`'s `en_route`, faithfully rendered
                        // by `frontend/components/TrackedTrainStatusBadge.tsx`
                        // -- a quarter of an hour before their train had
                        // left, with another train's calling points filled
                        // in behind it.
                        //
                        // Filtered here, alongside the `claimed` filter
                        // below, rather than inside `matching`, for the
                        // reason that filter already gives: that module
                        // stays a pure CRS+time function of its arguments.
                        //
                        // Known residual: `pending_activations` is in-memory
                        // and one-shot, so this guard can only fire when
                        // THIS process saw the Activation for this
                        // `train_id` and no Movement has claimed it yet. A
                        // restart between Activation and origin departure
                        // leaves the old behavior exactly as it was -- this
                        // narrows the mis-attribution window, it does not
                        // close it, and nothing here weakens any path that
                        // resolved correctly before.
                        let movement_train_uid = state
                            .pending_activations
                            .get(&movement.train_id)
                            .map(|activation| activation.train_uid.as_str());

                        let unclaimed: Vec<crate::matching::PendingPin> = reference
                            .pending
                            .iter()
                            .filter(|pin| !claimed.contains(&pin.tracked_train_id))
                            .filter(|pin| match (pin.train_uid.as_deref(), movement_train_uid) {
                                // Both identities known and different: a
                                // provable mismatch, never a claim.
                                (Some(pin_uid), Some(movement_uid)) => {
                                    pin_uid.eq_ignore_ascii_case(movement_uid)
                                }
                                // Either side unknown -- the heuristic is
                                // all there is, exactly as before.
                                _ => true,
                            })
                            .cloned()
                            .collect();

                        // Still a SINGLE claim, deliberately: this is the
                        // CRS+time heuristic, and letting one departure claim
                        // every pin that happens to fall in its tolerance
                        // window would re-open exactly the mis-attribution
                        // the `claimed` filter above exists to prevent.
                        // Subscribers sharing one physical train are
                        // attributed through `by_train_uid` (which knows
                        // their identity for certain), not through this
                        // guess. See this fix's report for the residual
                        // limitation this leaves.
                        //
                        // `planned` -- the Movement's own BOOKED (WTT)
                        // departure time -- is what the match is decided on
                        // now, against each pin's `pin_scheduled_departure`
                        // with a tight tolerance and nearest-wins; `actual`
                        // remains the plausibility guard's anchor and the
                        // fallback for a Movement that carries no booked time
                        // at all. See `matching::resolve_origin_departure`'s
                        // own doc comment for finding #1's full reasoning:
                        // before this, `planned` was parsed here and used
                        // only for the delay calculation below, while the
                        // claim itself was a first-found match inside a
                        // ±20-minute window around `actual` -- which at a
                        // busy terminus routinely claimed a pin for the
                        // wrong train, permanently.
                        let Some(tracked_train_id) = crate::matching::resolve_origin_departure(
                            loc_crs_for_match,
                            planned,
                            actual_ts,
                            &unclaimed,
                            received_at,
                        ) else {
                            return Vec::new();
                        };
                        state.attribute(&movement.train_id, &[tracked_train_id]);
                        (vec![tracked_train_id], true)
                    }
                };

            // Destination lookup: any of this message's `tracked_train_ids`
            // sharing the same physical train also share the same
            // `trains_id` (they're the SAME train), so the first one with a
            // known `trains_id` -> `destination_crs` mapping supplies it --
            // there is no need to check every one of them. `None` when the
            // `trains_id` itself is unknown yet (this Movement is the very
            // one resolving it, so the reference reload hasn't seen it) or
            // when no schedule has ever matched this train -- both honest
            // "destination genuinely unknown" cases, not bugs.
            let destination_crs: Option<String> = tracked_train_ids
                .iter()
                .find_map(|id| reference.trains_id_by_tracked_train_id.get(id))
                .and_then(|trains_id| reference.destination_crs_by_trains_id.get(trains_id))
                .cloned();

            let previous = previous_state(state, &movement.train_id);
            let mut derived = trust_schema::journey::apply_movement(
                &previous,
                movement,
                loc_crs.as_deref(),
                destination_crs.as_deref(),
            );
            if let (Some(p), Some(a), Some("LATE")) =
                (planned, actual, movement.variation_status.as_deref())
            {
                derived.delay_minutes = Some((a - p).num_minutes() as i32);
            }
            state.set_last_derived(&movement.train_id, derived.clone());

            // `resolved_train_uid`/`resolved_train_id` are only ever `Some`
            // on the one message that resolves a pending pin (see
            // `common::TrainMovementEventMessage`'s docs); the train_uid is
            // whatever an earlier Activation parked, or `None` if this
            // process never saw one.
            // Carried on EVERY event this message fans out to, not just
            // the first: each one drives `flip_legacy_resolution` for its
            // own subscription, and that flip is the whole point.
            let (resolved_train_uid, resolved_train_id) = if freshly_resolved {
                (
                    state.claim_activation(&movement.train_id),
                    Some(movement.train_id.clone()),
                )
            } else {
                (None, None)
            };

            let dedup = trust_schema::dedup::dedup_key(
                &movement.train_id,
                "0003",
                Some(&movement.event_type),
                movement.loc_stanox.as_deref(),
                movement.planned_timestamp.as_deref(),
                common::rail_day::current_rail_day(received_at),
            );

            tracked_train_ids
                .into_iter()
                .map(|tracked_train_id| common::TrainMovementEventMessage {
                    tracked_train_id,
                    resolved_train_uid: resolved_train_uid.clone(),
                    resolved_train_id: resolved_train_id.clone(),
                    dedup_key: dedup.clone(),
                    msg_type: "0003".to_string(),
                    event_type: Some(movement.event_type.clone()),
                    loc_stanox: movement.loc_stanox.clone(),
                    loc_crs: loc_crs.clone(),
                    planned_timestamp: planned,
                    actual_timestamp: actual,
                    variation_status: movement.variation_status.clone(),
                    raw_body: serde_json::json!({}),
                    status: derived.status.clone(),
                    last_reported_location: derived.last_reported_location.clone(),
                    last_event_type: derived.last_event_type.clone(),
                    delay_minutes: derived.delay_minutes,
                    next_calling_point: derived.next_calling_point.clone(),
                    eta_next: None,
                    eta_source: None,
                })
                .collect()
        }

        TrustMessage::Cancellation(cancellation) => {
            // A cancellation can only ever arrive for a train_id some
            // earlier Movement already resolved -- it carries no location
            // to match a pin on, so an unresolved one is dropped rather
            // than run through `resolve_origin_departure` a second time.
            let Some(tracked_train_ids) = state.resolved.get(&cancellation.train_id).cloned()
            else {
                return Vec::new();
            };

            let previous = previous_state(state, &cancellation.train_id);
            let derived = trust_schema::journey::apply_cancellation(&previous);
            state.set_last_derived(&cancellation.train_id, derived.clone());

            // The rail day is load-bearing in this one especially: a
            // Cancellation carries nothing else that distinguishes it, so
            // without a date the key was just `(train_id, "0002")` -- and
            // TRUST recycles `train_id`s monthly while `api`'s
            // `trust_event_backlog` enforces a GLOBAL unique `dedup_key`
            // over a 90-day retention. See `trust_schema::dedup::dedup_key`.
            let dedup = trust_schema::dedup::dedup_key(
                &cancellation.train_id,
                "0002",
                None,
                None,
                None,
                common::rail_day::current_rail_day(received_at),
            );

            // TRUST's confirmed `canx_timestamp` is the time the
            // cancellation actually happened; it is the only timestamp this
            // message shape carries, so it lands in the event's generic
            // `actual_timestamp` rather than being dropped. Routed through
            // the same `parse_trust_epoch_millis_pair` decision function as
            // a Movement's fields (with `planned: None`, since a
            // Cancellation has no companion field to keep in sync) so this
            // path gets the same guarding, kill switch, and correction
            // metric as every other TRUST timestamp -- see Finding #1's own
            // note that this path needed checking too.
            let canx_pair = common::trust_timestamp::parse_trust_epoch_millis_pair(
                None,
                cancellation.canx_timestamp.as_deref(),
                received_at,
                state.trust_timestamp_correction_enabled,
            );
            if let Some(was_corrected) = canx_pair.was_corrected {
                metrics::counter!(
                    common::metrics::metric_name("trust_consumer_timestamp_correction_total"),
                    "outcome" => if was_corrected { "corrected" } else { "raw" }
                )
                .increment(1);
            }

            tracked_train_ids
                .into_iter()
                .map(|tracked_train_id| common::TrainMovementEventMessage {
                    tracked_train_id,
                    resolved_train_uid: None,
                    resolved_train_id: None,
                    dedup_key: dedup.clone(),
                    msg_type: "0002".to_string(),
                    event_type: None,
                    loc_stanox: None,
                    loc_crs: None,
                    planned_timestamp: None,
                    actual_timestamp: canx_pair.actual,
                    variation_status: None,
                    raw_body: serde_json::json!({}),
                    status: derived.status.clone(),
                    last_reported_location: derived.last_reported_location.clone(),
                    last_event_type: derived.last_event_type.clone(),
                    delay_minutes: derived.delay_minutes,
                    next_calling_point: derived.next_calling_point.clone(),
                    eta_next: None,
                    eta_source: None,
                })
                .collect()
        }

        TrustMessage::ChangeOfOrigin(change) => {
            passthrough_event(&change.train_id, "0006", state, received_at)
        }
        TrustMessage::ChangeOfIdentity(change) => {
            passthrough_event(&change.train_id, "0007", state, received_at)
        }

        // Not logged by `schema::parse_envelope` itself (parsing an
        // unconfirmed msg_type into `Unknown` always succeeds, so there's
        // no failure to warn about there) -- logged here instead, the one
        // place the captured msg_type is actually read, so a real RDM feed
        // sending `0005`/`0008` (or anything else undocumented) shows up in
        // this crate's logs rather than vanishing silently. There is no
        // confirmed body shape to derive anything from either way.
        TrustMessage::Unknown(msg_type) => {
            tracing::info!(
                msg_type,
                "unconfirmed msg_type observed; dropping without a confirmed shape to parse into"
            );
            Vec::new()
        }
    }
}

/// `0006`/`0007` are recorded, not interpreted. Neither `journey` nor any
/// other module in this crate has a confirmed derivation rule for a change
/// of origin or of identity, and both message shapes carry nothing but a
/// `train_id` (see `schema.rs`), so inventing one would be guesswork. The
/// honest handling is to post the event -- so the change is visible in
/// `train_movement_events` -- with the train's derived state passed through
/// unchanged, exactly as `journey::apply_movement` already passes
/// `next_calling_point` through when it lacks the information to update it.
fn passthrough_event(
    train_id: &str,
    msg_type: &str,
    state: &ProcessorState,
    received_at: chrono::DateTime<chrono::Utc>,
) -> Vec<common::TrainMovementEventMessage> {
    let Some(tracked_train_ids) = state.resolved.get(train_id) else {
        return Vec::new();
    };
    let derived = previous_state(state, train_id);

    // Same recycled-`train_id` hazard as a Cancellation's key, and for the
    // same reason: `(train_id, msg_type)` is genuinely all these message
    // shapes carry. See `trust_schema::dedup::dedup_key`.
    let dedup = trust_schema::dedup::dedup_key(
        train_id,
        msg_type,
        None,
        None,
        None,
        common::rail_day::current_rail_day(received_at),
    );
    tracked_train_ids
        .iter()
        .map(|&tracked_train_id| common::TrainMovementEventMessage {
            tracked_train_id,
            resolved_train_uid: None,
            resolved_train_id: None,
            dedup_key: dedup.clone(),
            msg_type: msg_type.to_string(),
            event_type: None,
            loc_stanox: None,
            loc_crs: None,
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            raw_body: serde_json::json!({}),
            status: derived.status.clone(),
            last_reported_location: derived.last_reported_location.clone(),
            last_event_type: derived.last_event_type.clone(),
            delay_minutes: derived.delay_minutes,
            next_calling_point: derived.next_calling_point.clone(),
            eta_next: None,
            eta_source: None,
        })
        .collect()
}

/// The last state derived for this train, or a blank `awaiting_activation`
/// if this is the first event ever seen for it.
fn previous_state(state: &ProcessorState, train_id: &str) -> DerivedState {
    state
        .last_derived
        .get(train_id)
        .cloned()
        .unwrap_or_else(DerivedState::awaiting_activation)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::LazyLock;

    use super::*;
    use crate::feed::FakeMovementFeed;
    use crate::matching::PendingPin;
    use crate::stanox_crs::StanoxCrsTable;

    /// The real, checked-in `reference-data/stanox-crs.csv` -- not a
    /// synthetic fixture -- loaded once and shared across every test below,
    /// mirroring how `crates/aggregator`/`crates/api`'s own tests load the
    /// real `lines/` directory directly (see e.g.
    /// `crates/aggregator/src/segments.rs`'s `load_all_lines`). These tests
    /// depend on real STANOX values (`"87212"` -> `"WAT"`, `"73000"`,
    /// `"86031"`) actually translating, same as before this table moved out
    /// of a Rust literal.
    static TEST_STANOX_CRS: LazyLock<StanoxCrsTable> = LazyLock::new(|| {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../reference-data/stanox-crs.csv");
        StanoxCrsTable::from_file(&path).expect("reference-data/stanox-crs.csv should parse")
    });

    /// `run_once`'s `received_at` for every test in this module that isn't
    /// specifically exercising the plausibility guard itself (that's
    /// `matching.rs`'s own job, plus the two guard-focused tests at the
    /// bottom of this module). Deliberately set safely AFTER every raw
    /// `planned_timestamp`/`actual_timestamp` fixture used anywhere in this
    /// file (all of them fall on 2026-08-28) -- since these tests are about
    /// matching/derivation correctness, not the guard, a `received_at` this
    /// far past every fixture's event time can never trip
    /// `common::is_plausible_actual_timestamp` by construction, the same way
    /// a real feed's receipt time trails the event it reports.
    fn test_received_at() -> chrono::DateTime<chrono::Utc> {
        "2026-08-29T00:00:00Z".parse().unwrap()
    }

    /// The Europe/London rail day `test_received_at()` falls in -- 00:00 UTC
    /// is 01:00 BST, before the 02:00 cutoff, so it is still 2026-08-28's
    /// rail day. That is the same date every fixture subscription in this
    /// module carries as its `service_date`, which is what makes finding
    /// #2's Activation date check pass for them; the tests that exercise the
    /// check itself deliberately use a different date instead.
    fn test_rail_day() -> NaiveDate {
        common::rail_day::current_rail_day(test_received_at())
    }

    /// One subscription sharing a `train_uid`, dated to the rail day these
    /// tests run in unless a test wants otherwise. No `pin_scheduled_departure`
    /// -- callers that need finding H3's D+1 timing check exercised use
    /// `sharing_on_with_departure` instead.
    fn sharing(tracked_train_id: i64) -> SharingSubscription {
        SharingSubscription {
            tracked_train_id,
            service_date: test_rail_day(),
            pin_scheduled_departure: None,
        }
    }

    fn sharing_on(tracked_train_id: i64, service_date: &str) -> SharingSubscription {
        SharingSubscription {
            tracked_train_id,
            service_date: service_date.parse().unwrap(),
            pin_scheduled_departure: None,
        }
    }

    /// Same as `sharing_on`, but with a real `pin_scheduled_departure` --
    /// for finding H3's tests, which need to distinguish a genuine
    /// post-midnight D+1 subscription from an ordinary tomorrow-daytime one.
    fn sharing_on_with_departure(
        tracked_train_id: i64,
        service_date: &str,
        pin_scheduled_departure: &str,
    ) -> SharingSubscription {
        SharingSubscription {
            tracked_train_id,
            service_date: service_date.parse().unwrap(),
            pin_scheduled_departure: Some(pin_scheduled_departure.parse().unwrap()),
        }
    }

    fn reference_with_one_pending(id: i64, crs: &str, scheduled: &str) -> Reference {
        Reference {
            pending: vec![PendingPin {
                tracked_train_id: id,
                pin_origin_crs: crs.to_string(),
                pin_scheduled_departure: scheduled.parse().unwrap(),
                train_uid: None,
            }],
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        }
    }

    /// A resolving origin departure at WAT, matching the pin every test
    /// below builds with `reference_with_one_pending(1, "WAT", ...)`.
    ///
    /// **Every raw `planned_timestamp`/`actual_timestamp`/`canx_timestamp`
    /// millis literal in this test module is 3,600,000ms (1 hour) LATER
    /// than the UTC instant it's meant to represent** -- e.g. this
    /// constant's `"1787945520000"` is 2026-08-28T19:32:00Z as a raw wire
    /// value, not 18:32:00Z. That's deliberate, not a typo: every one of
    /// these fields is now parsed by
    /// `common::trust_timestamp::parse_trust_epoch_millis`, which
    /// reinterprets a BST-period wire value as Europe/London LOCAL time and
    /// corrects it one hour earlier (see that function's own doc comment).
    /// August is BST, so a literal written as the "plain" UTC instant would
    /// be silently corrected an hour EARLIER than every pin/assertion in
    /// this file expects -- every fixture here instead encodes the wire
    /// value a genuinely-corrected feed would send for the intended
    /// 18:32:00Z-onwards instants these tests actually reason about.
    const ORIGIN_DEPARTURE: &str = r#"[{"header":{"msg_type":"0003"},"body":{
        "train_id":"221832406","event_type":"DEPARTURE",
        "planned_timestamp":"1787945520000","actual_timestamp":"1787945520000",
        "loc_stanox":"87212","variation_status":"ON TIME"
    }}]"#;

    #[tokio::test]
    async fn a_matching_movement_produces_one_event() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(events[0].status, "en_route");
    }

    #[tokio::test]
    async fn a_movement_with_no_matching_pin_produces_no_event() {
        let raw_batch = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"999","event_type":"DEPARTURE",
            "planned_timestamp":"1787945520000","actual_timestamp":"1787945520000",
            "loc_stanox":"73000","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![raw_batch.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn an_empty_batch_produces_no_events_and_is_not_an_error() {
        let mut feed = FakeMovementFeed::new(vec![vec![]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();
        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn an_activation_supplies_the_train_uid_to_the_movement_that_resolves_the_pin() {
        let activation = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
            "train_service_code":"22345000","schedule_wtt_id":"WTT1",
            "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![activation.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let activation_events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(
            activation_events.is_empty(),
            "an Activation alone posts nothing"
        );

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].resolved_train_uid, Some("C21373".to_string()));
        assert_eq!(events[0].resolved_train_id, Some("221832406".to_string()));
    }

    #[tokio::test]
    async fn a_second_movement_reuses_the_resolution_without_re_resolving() {
        // The second movement is at WOK, which matches no pin at all -- the
        // only way it can produce an event is via the resolved-train_id map
        // the first call populated.
        let later_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![later_arrival.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let first = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(first[0].resolved_train_id, Some("221832406".to_string()));

        let second = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(
            second[0].tracked_train_id, 1,
            "same tracked train as the resolving movement"
        );
        assert_eq!(second[0].last_reported_location, Some("WOK".to_string()));
        assert_eq!(
            second[0].resolved_train_uid, None,
            "only the resolving message carries these"
        );
        assert_eq!(second[0].resolved_train_id, None);
    }

    /// The headline scenario this feature exists for, exercised through the
    /// real `run_once` loop end to end (not just `journey::apply_movement`
    /// in isolation): once the reference reload has told this process a
    /// train's own known destination CRS (`Reference::destination_crs_by_trains_id`,
    /// seeded by `apply_reference_reload` from `TrackedTrainRef::destination_crs`),
    /// a later ARRIVAL translating to that SAME CRS produces an event whose
    /// `status` is `"completed"`, not the usual `"en_route"`.
    #[tokio::test]
    async fn an_arrival_at_the_known_destination_produces_a_completed_event() {
        // ARRIVAL at WOK (86031 -- see this test module's own real
        // `reference-data/stanox-crs.csv` STANOX->CRS translations).
        let arrival_at_destination = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![arrival_at_destination.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        let mut state = ProcessorState::default();

        let mut resolved_ref = tracked_ref(1, "resolved", Some("221832406"));
        resolved_ref.trains_id = Some(42);
        resolved_ref.destination_crs = Some("WOK".to_string());
        apply_reference_reload(vec![resolved_ref], &mut reference, &mut state);

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].status, "completed",
            "an ARRIVAL translating to the train's own known destination CRS is confirmed \
             evidence the journey finished"
        );
        assert_eq!(events[0].last_reported_location, Some("WOK".to_string()));
    }

    /// The distinction that matters most, exercised the same end-to-end way:
    /// an ARRIVAL at an INTERMEDIATE calling point (a real, translated CRS,
    /// just not this train's own destination) must NOT be reported as
    /// `"completed"`, even though the train's destination IS known.
    #[tokio::test]
    async fn an_arrival_at_an_intermediate_stop_with_a_known_destination_stays_en_route() {
        // ARRIVAL at PAD (73000), not this train's destination (WOK).
        let arrival_at_intermediate_stop = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"73000","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![arrival_at_intermediate_stop.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        let mut state = ProcessorState::default();

        let mut resolved_ref = tracked_ref(1, "resolved", Some("221832406"));
        resolved_ref.trains_id = Some(42);
        resolved_ref.destination_crs = Some("WOK".to_string());
        apply_reference_reload(vec![resolved_ref], &mut reference, &mut state);

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].status, "en_route",
            "an ARRIVAL at an intermediate stop must not be mistaken for a finished journey, \
             even when the train's real destination is known"
        );
        assert_eq!(events[0].last_reported_location, Some("PAD".to_string()));
    }

    #[tokio::test]
    async fn a_cancellation_after_a_movement_preserves_the_last_known_location() {
        let cancellation = r#"[{"header":{"msg_type":"0002"},"body":{
            "train_id":"221832406","canx_timestamp":"1787947200000","canx_type":"EN ROUTE"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![cancellation.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let movement_events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(
            movement_events[0].last_reported_location,
            Some("WAT".to_string())
        );

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(events[0].msg_type, "0002");
        assert_eq!(events[0].status, "cancelled");
        assert_eq!(
            events[0].last_reported_location,
            Some("WAT".to_string()),
            "the cancellation is derived against the movement's state, not a blank one",
        );
    }

    #[tokio::test]
    async fn a_cancellation_for_an_unresolved_train_produces_no_event() {
        let cancellation = r#"[{"header":{"msg_type":"0002"},"body":{
            "train_id":"221832406","canx_timestamp":"1787947200000","canx_type":"AT ORIGIN"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![cancellation.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(
            events.is_empty(),
            "nothing to attribute the cancellation to"
        );
    }

    #[tokio::test]
    async fn a_change_of_origin_passes_the_derived_state_through_unchanged() {
        let change_of_origin =
            r#"[{"header":{"msg_type":"0006"},"body":{"train_id":"221832406"}}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![change_of_origin.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].msg_type, "0006");
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(events[0].status, "en_route");
        assert_eq!(events[0].last_reported_location, Some("WAT".to_string()));
    }

    #[tokio::test]
    async fn a_change_of_identity_for_an_unresolved_train_produces_no_event() {
        let change_of_identity =
            r#"[{"header":{"msg_type":"0007"},"body":{"train_id":"221832406"}}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![change_of_identity.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn an_unknown_message_type_produces_no_event() {
        let unknown = r#"[{"header":{"msg_type":"0005"},"body":{"anything":"goes"}}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![unknown.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(events.is_empty());
    }

    // --- Reference-reload rehydration (fix 1) ---

    fn tracked_ref(id: i64, status: &str, train_id: Option<&str>) -> common::TrackedTrainRef {
        common::TrackedTrainRef {
            id,
            service_date: "2026-08-28".parse().unwrap(),
            pin_origin_crs: Some("WAT".to_string()),
            pin_scheduled_departure: Some("2026-08-28T18:32:00Z".parse().unwrap()),
            resolution_status: status.to_string(),
            train_uid: None,
            train_id: train_id.map(str::to_string),
            trains_id: None,
            destination_crs: None,
        }
    }

    /// A train whose origin departure happened before this process started
    /// will never emit another one, so without reload rehydration its
    /// remaining movements would be dropped forever.
    #[tokio::test]
    async fn an_already_resolved_ref_is_rehydrated_from_the_reference_reload() {
        // Mid-journey arrival at WOK -- matches no pin, so the only possible
        // route to an event is the rehydrated `resolved` map.
        let later_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![later_arrival.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        let mut state = ProcessorState::default();

        apply_reference_reload(
            vec![tracked_ref(7, "resolved", Some("221832406"))],
            &mut reference,
            &mut state,
        );
        assert!(
            reference.pending.is_empty(),
            "a resolved ref is not a matchable pin"
        );

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(
            events.len(),
            1,
            "the restart-surviving train is still tracked"
        );
        assert_eq!(events[0].tracked_train_id, 7);
        assert_eq!(
            events[0].resolved_train_id, None,
            "rehydration is not a fresh resolution"
        );
    }

    /// The exact scenario this task exists for: a schedule-matched pin (a
    /// real `train_uid` already known, no `train_id` yet) must be rehydrated
    /// into `reference.pending` -- NOT `state.resolved` -- so a live TRUST
    /// Movement can still claim it via the ordinary CRS+time heuristic.
    #[tokio::test]
    async fn a_schedule_matched_ref_is_treated_as_pending_for_rehydration_and_can_still_be_claimed()
    {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        let mut state = ProcessorState::default();

        let mut schedule_matched_ref = tracked_ref(1, "schedule_matched", None);
        schedule_matched_ref.train_uid = Some("C88888".to_string()); // known from the schedule match
        schedule_matched_ref.pin_origin_crs = Some("WAT".to_string());
        schedule_matched_ref.pin_scheduled_departure =
            Some("2026-08-28T18:32:00Z".parse().unwrap());

        apply_reference_reload(vec![schedule_matched_ref], &mut reference, &mut state);
        assert_eq!(
            reference.pending.len(),
            1,
            "a schedule_matched ref must be rehydrated as a matchable pending pin"
        );

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(
            events[0].resolved_train_id,
            Some("221832406".to_string()),
            "the live Movement still claims it via the ordinary heuristic, unchanged"
        );
    }

    /// The reload row may have been read before an in-flight resolution was
    /// written, so it must never overwrite a live one.
    #[tokio::test]
    async fn a_live_resolution_is_not_clobbered_by_a_stale_reload_row() {
        let later_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![later_arrival.to_string()],
        ]);
        let mut reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let first = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(first[0].tracked_train_id, 1);

        apply_reference_reload(
            vec![tracked_ref(99, "resolved", Some("221832406"))],
            &mut reference,
            &mut state,
        );

        let second = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(
            second[0].tracked_train_id, 1,
            "the in-process resolution wins over the reload row"
        );
    }

    // --- stanox_crs live reload (Task 5) ---

    #[test]
    fn a_successful_reload_replaces_the_table_for_subsequent_lookups() {
        let initial = StanoxCrsTable::from_records(vec![common::StanoxCrsRecord {
            stanox: "72410".to_string(),
            crs: "EUS".to_string(),
            tiploc: "EUSTON".to_string(),
            station_name: "LONDON EUSTON".to_string(),
            source_sequence: 940,
            change_time_minutes: None,
        }]);
        let cell = std::sync::RwLock::new(initial);

        let fresh = vec![common::StanoxCrsRecord {
            stanox: "72410".to_string(),
            crs: "EU2".to_string(),
            tiploc: "EUSTON".to_string(),
            station_name: "LONDON EUSTON".to_string(),
            source_sequence: 942,
            change_time_minutes: None,
        }];
        apply_stanox_crs_reload(Ok(fresh), &cell);

        assert_eq!(
            cell.read().unwrap().stanox_to_crs("72410"),
            Some("EU2".to_string())
        );
    }

    #[test]
    fn a_failed_or_empty_reload_does_not_clear_the_currently_loaded_table() {
        let initial = StanoxCrsTable::from_records(vec![common::StanoxCrsRecord {
            stanox: "72410".to_string(),
            crs: "EUS".to_string(),
            tiploc: "EUSTON".to_string(),
            station_name: "LONDON EUSTON".to_string(),
            source_sequence: 940,
            change_time_minutes: None,
        }]);
        let cell = std::sync::RwLock::new(initial);

        apply_stanox_crs_reload(Err(anyhow::anyhow!("api is down")), &cell);
        assert_eq!(
            cell.read().unwrap().stanox_to_crs("72410"),
            Some("EUS".to_string()),
            "a failed fetch must not clear the table"
        );

        apply_stanox_crs_reload(Ok(Vec::new()), &cell);
        assert_eq!(
            cell.read().unwrap().stanox_to_crs("72410"),
            Some("EUS".to_string()),
            "an empty live table must not clear the table either"
        );
    }

    // --- Double-claimed pins (fix 2) ---

    /// Two services can leave the same origin close enough together to be
    /// inside one pin's tolerance window; only the first may claim it.
    #[tokio::test]
    async fn an_already_claimed_pin_cannot_be_stolen_by_a_second_train() {
        // Same station, different train_id, and a booked departure only two
        // minutes from the pin's own -- deliberately INSIDE
        // `matching::SCHEDULED_DEPARTURE_TOLERANCE`, so that the `claimed`
        // filter is what rejects this train rather than finding #1's tighter
        // time comparison rejecting it first. (The old fixture was 8 minutes
        // out, which the new tolerance excludes on its own, leaving this test
        // asserting nothing about double-claiming.)
        let other_train_departure = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832407","event_type":"DEPARTURE",
            "planned_timestamp":"1787945640000","actual_timestamp":"1787945640000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![other_train_departure.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let first = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(first[0].tracked_train_id, 1);
        assert_eq!(first[0].resolved_train_id, Some("221832406".to_string()));

        let second = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(
            second.is_empty(),
            "the only pin is already claimed by 221832406; 221832407 must not take it too",
        );
        assert_eq!(
            state.resolved.len(),
            1,
            "and it is not recorded as resolved either"
        );
    }

    // --- Only a DEPARTURE may claim a pin ---

    /// An ARRIVAL at the pinned origin, inside the pin's tolerance window --
    /// everything `resolve_origin_departure` looks at says "match". A
    /// terminus sees plenty of these, and claiming on one would bind the pin
    /// to the wrong train irreversibly.
    #[tokio::test]
    async fn an_arrival_at_the_pinned_origin_does_not_claim_the_pin() {
        let arrival_at_origin = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832499","event_type":"ARRIVAL",
            "planned_timestamp":"1787945520000","actual_timestamp":"1787945520000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![arrival_at_origin.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(events.is_empty(), "an arrival is not an origin departure");
        assert!(
            !state.resolved.contains_key("221832499"),
            "and it leaves no resolution behind either",
        );

        // The pin must still be there for the train that really departs.
        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(events[0].resolved_train_id, Some("221832406".to_string()));
    }

    /// Same for a PASS -- a train running through the pinned origin without
    /// stopping is emphatically not the pinned service departing it.
    #[tokio::test]
    async fn a_pass_at_the_pinned_origin_does_not_claim_the_pin() {
        let pass_at_origin = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832499","event_type":"PASS",
            "planned_timestamp":"1787945520000","actual_timestamp":"1787945520000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![pass_at_origin.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(events.is_empty());
        assert!(state.resolved.is_empty(), "the pin stays unclaimed");
    }

    // --- Finding #1: which pin a live departure may claim ---

    /// **Finding #1 end to end.** A different service out of the pinned
    /// terminus, booked 12 minutes after the pin and running on time. Its
    /// ACTUAL departure is inside the old ±20-minute window, so the old
    /// first-found match claimed the pin for it -- irreversibly, locking the
    /// correct train out for the life of the process. Now the claim is
    /// decided on the message's own booked time, so it doesn't.
    #[tokio::test]
    async fn a_busy_terminus_neighbour_does_not_claim_the_pin_it_used_to_steal() {
        // Booked (and running) 12 minutes after the pinned 18:32 departure:
        // raw 1787945520000 + 720000ms.
        let neighbour = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221899999","event_type":"DEPARTURE",
            "planned_timestamp":"1787946240000","actual_timestamp":"1787946240000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![neighbour.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(
            events.is_empty(),
            "a train booked 12 minutes from the pin is not the pinned train"
        );
        assert!(
            state.resolved.is_empty(),
            "and it must leave no claim behind -- a claim has no unwind path"
        );

        // The pin is therefore still there for the train that really is it.
        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(events[0].resolved_train_id, Some("221832406".to_string()));
    }

    /// **Finding #1's second failure, end to end.** Two open pins at the same
    /// terminus, offered in `tracked_at` order. The departing train is booked
    /// 1 minute from pin 2 and 13 from pin 1, so the old first-found scan
    /// claimed pin 1 -- the worse match -- and pin 2's own train then found
    /// nothing left to claim.
    #[tokio::test]
    async fn the_closest_pin_is_claimed_not_the_earliest_created_one() {
        // Booked 18:45 (raw 1787945520000 + 780000ms), on time.
        let departure_at_1845 = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1787946300000","actual_timestamp":"1787946300000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![departure_at_1845.to_string()]]);
        let reference = Reference {
            pending: vec![
                PendingPin {
                    tracked_train_id: 1, // created first, 13 minutes away
                    pin_origin_crs: "WAT".to_string(),
                    pin_scheduled_departure: "2026-08-28T18:32:00Z".parse().unwrap(),
                    train_uid: None,
                },
                PendingPin {
                    tracked_train_id: 2, // created second, 1 minute away
                    pin_origin_crs: "WAT".to_string(),
                    pin_scheduled_departure: "2026-08-28T18:46:00Z".parse().unwrap(),
                    train_uid: None,
                },
            ],
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].tracked_train_id, 2,
            "the closest pin must be claimed, not whichever was pinned first"
        );
    }

    // --- Finding #2: an Activation may only attribute its OWN day's running ---

    /// **Finding #2's regression test.** `api`'s `list_active_tracked_trains`
    /// has no date bound, and a subscription stays in the active set until
    /// its train completes -- which a never-attributed one never does. So
    /// yesterday's subscription for a daily-running uid is still in
    /// `by_train_uid` when TODAY's Activation for that uid arrives, and the
    /// fast path used to bind it, permanently, to a completely different
    /// day's train.
    #[tokio::test]
    async fn an_activation_is_not_attributed_to_another_days_subscription_for_the_same_uid() {
        let mut feed = FakeMovementFeed::new(vec![vec![SHARED_ACTIVATION.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        // This process is handling 2026-08-28's rail day (see
        // `test_rail_day`), but the only subscription for this uid is
        // tracking the PREVIOUS day's running of it.
        reference
            .by_train_uid
            .insert("C88888".to_string(), vec![sharing_on(1, "2026-08-27")]);
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert!(
            state.resolved.is_empty(),
            "yesterday's subscription must NOT be bound to today's running of the same train_uid"
        );
        assert!(
            state.activation_matched_awaiting_movement.is_empty(),
            "and no resolution signal may be queued for it either"
        );
        // The Activation is still parked, so the correct day's subscription
        // can still be resolved by the ordinary heuristic and pick up the uid.
        assert_eq!(
            state
                .pending_activations
                .get("221832406")
                .map(|activation| activation.train_uid.as_str()),
            Some("C88888")
        );
    }

    /// The same-day subscription still resolves immediately -- otherwise the
    /// test above could pass by breaking the fast path outright.
    #[tokio::test]
    async fn an_activation_is_attributed_to_the_subscription_for_its_own_service_date() {
        let mut feed = FakeMovementFeed::new(vec![vec![SHARED_ACTIVATION.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        reference.by_train_uid.insert(
            "C88888".to_string(),
            // One subscription for this rail day, one for a week ago.
            vec![sharing_on(1, "2026-08-28"), sharing_on(2, "2026-08-21")],
        );
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(
            state.resolved.get("221832406"),
            Some(&vec![1]),
            "only the subscription actually tracking this day's running is attributed"
        );
    }

    /// A post-midnight service (`service_date` one calendar day AFTER the
    /// Activation's rail day, because a rail day ends at 02:00) is a real,
    /// ordinary case and must still resolve -- the date check is deliberately
    /// asymmetric about this. Its own `pin_scheduled_departure` (00:15,
    /// still inside the SAME rail day as the Activation) is what finding
    /// H3 now requires to accept the D+1 branch at all.
    #[tokio::test]
    async fn an_activation_still_resolves_a_post_midnight_services_subscription() {
        let mut feed = FakeMovementFeed::new(vec![vec![SHARED_ACTIVATION.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        // Activation handled inside 2026-08-28's rail day; the subscription
        // is for a 00:15 departure, which the departure board dates 08-29,
        // and which is itself still inside 2026-08-28's rail day (before the
        // 02:00 Europe/London cutoff).
        reference.by_train_uid.insert(
            "C88888".to_string(),
            vec![sharing_on_with_departure(
                1,
                "2026-08-29",
                "2026-08-29T00:15:00Z",
            )],
        );
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(
            state.resolved.get("221832406"),
            Some(&vec![1]),
            "an overnight service's next-calendar-day service_date is legitimate"
        );
    }

    // --- Finding H3 (2026-09-26 review): D+1 needs its own timing, not just its date ---

    /// The false-positive case from finding H3: a same-uid subscription for
    /// TOMORROW's ORDINARY DAYTIME running (e.g. a Mon-Fri commute's "track
    /// tomorrow's departure" subscription, already sitting in `by_train_uid`
    /// because `list_active_tracked_trains` has no upper date bound) must
    /// NOT be claimed by TODAY's Activation just because its `service_date`
    /// happens to be one day ahead. Before the fix, `activation_is_for_service_date`
    /// accepted this on the date arithmetic alone and permanently bound the
    /// wrong day's subscription to today's train.
    #[tokio::test]
    async fn an_activation_is_not_attributed_to_tomorrows_ordinary_daytime_subscription() {
        let mut feed = FakeMovementFeed::new(vec![vec![SHARED_ACTIVATION.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        // Activation handled inside 2026-08-28's rail day; the subscription
        // is for the SAME uid's 08-29 running, but booked at an ordinary
        // 17:30 daytime departure -- not a post-midnight one.
        reference.by_train_uid.insert(
            "C88888".to_string(),
            vec![sharing_on_with_departure(
                1,
                "2026-08-29",
                "2026-08-29T16:30:00Z",
            )],
        );
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert!(
            state.resolved.is_empty(),
            "tomorrow's ordinary daytime subscription must NOT be bound to today's train just \
             because its service_date is one day ahead"
        );
        assert!(
            state.activation_matched_awaiting_movement.is_empty(),
            "and no resolution signal may be queued for it either"
        );
        // The Activation is still parked for the pin-claim heuristic, same
        // as the other-day rejection case above.
        assert_eq!(
            state
                .pending_activations
                .get("221832406")
                .map(|activation| activation.train_uid.as_str()),
            Some("C88888")
        );
    }

    /// The other half of finding H3: once the false positive above is
    /// closed, tomorrow's REAL Activation (arriving on its own, correct
    /// rail day) must still be able to claim that same subscription
    /// normally, via the ordinary `service_date == activation_rail_day`
    /// case -- this fix must not cost the subscription its eventual,
    /// correct resolution.
    #[tokio::test]
    async fn an_activation_still_claims_tomorrows_subscription_when_tomorrow_actually_arrives() {
        let mut feed = FakeMovementFeed::new(vec![vec![SHARED_ACTIVATION.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        reference.by_train_uid.insert(
            "C88888".to_string(),
            vec![sharing_on_with_departure(
                1,
                "2026-08-29",
                "2026-08-29T16:30:00Z",
            )],
        );
        let mut state = ProcessorState::default();

        // A day later: this Activation is now handled inside 2026-08-29's
        // rail day, which is exactly the subscription's own service_date.
        let tomorrows_received_at: chrono::DateTime<chrono::Utc> =
            "2026-08-30T00:00:00Z".parse().unwrap();
        assert_eq!(
            common::rail_day::current_rail_day(tomorrows_received_at),
            "2026-08-29".parse::<NaiveDate>().unwrap(),
            "sanity check on the fixture: this instant must fall in 2026-08-29's rail day"
        );

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            tomorrows_received_at,
        )
        .await
        .unwrap();

        assert_eq!(
            state.resolved.get("221832406"),
            Some(&vec![1]),
            "tomorrow's real Activation must still claim the subscription once it actually \
             arrives, ordinary daytime booking and all"
        );
    }

    // --- Finding #4: the in-memory state is a transaction ---

    /// **Finding #4's unit-level regression test**, in the exact shape the
    /// Redis backend produces: an Activation fast-path resolution whose batch
    /// was never confirmed is rolled back, so the REPLAY of that batch raises
    /// the one-time "freshly resolved" signal again instead of deciding the
    /// train was already resolved and dropping it forever.
    #[tokio::test]
    async fn a_rolled_back_activation_resolution_is_re_announced_on_the_replay() {
        let later_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        // The whole batch -- Activation and the Movement that carries its
        // resolution signal -- delivered twice, which is what an unacked
        // Redis batch's `reclaim_stale` replay looks like from here.
        let batch = vec![SHARED_ACTIVATION.to_string(), later_arrival.to_string()];
        let mut feed = FakeMovementFeed::new(vec![batch.clone(), batch]);
        let reference = shared_ref("C88888", vec![1]);
        let mut state = ProcessorState::default();

        let first = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].resolved_train_id, Some("221832406".to_string()));
        assert_eq!(first[0].resolved_train_uid, Some("C88888".to_string()));

        // The POST failed, so nothing this batch did may survive.
        state.roll_back_batch();
        assert!(state.resolved.is_empty(), "the attribution is undone");
        assert!(
            state.pending_activations.is_empty(),
            "so is the parked Activation this batch added"
        );
        assert!(
            state.last_derived.is_empty(),
            "and the derived state it recorded"
        );

        let replay = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(
            replay[0].resolved_train_id,
            Some("221832406".to_string()),
            "the replay must re-announce the resolution -- this is the signal that flips the \
             subscription to 'resolved' in the database, and nothing else ever sends it"
        );
        assert_eq!(
            replay[0].resolved_train_uid,
            Some("C88888".to_string()),
            "including the train_uid the claimed Activation carried, which must have been \
             un-claimed by the rollback"
        );
    }

    /// A rollback must restore what was there BEFORE the batch, not just
    /// remove what the batch added: an already-resolved train's attribution,
    /// and a previously parked Activation, both have to survive intact.
    #[tokio::test]
    async fn a_rollback_restores_pre_existing_state_rather_than_clearing_it() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();
        // Pre-existing state from an earlier, CONFIRMED batch.
        state.resolved.insert("other-train".to_string(), vec![99]);
        state
            .pending_activations
            .insert("221832406".to_string(), parked("C21373", None));
        state.confirm_batch();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        state.roll_back_batch();

        assert_eq!(
            state.resolved.get("other-train"),
            Some(&vec![99]),
            "an unrelated, already-confirmed attribution must not be touched"
        );
        assert!(
            !state.resolved.contains_key("221832406"),
            "the failed batch's own claim is gone"
        );
        assert_eq!(
            state
                .pending_activations
                .get("221832406")
                .map(|activation| activation.train_uid.as_str()),
            Some("C21373"),
            "the Activation this batch claimed must be back for the replay to claim again"
        );
    }

    /// And `confirm_batch` must make the mutations permanent -- a later
    /// rollback (of a subsequent, unrelated failed batch) may not undo them.
    #[tokio::test]
    async fn a_confirmed_batch_survives_a_later_batchs_rollback() {
        let later_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![later_arrival.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        state.confirm_batch();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        state.roll_back_batch();

        assert_eq!(
            state.resolved.get("221832406"),
            Some(&vec![1]),
            "the confirmed resolution is a fact and must survive a later failure"
        );
    }

    // --- Finding #8: one bad payload must not take its batch down ---

    /// `run_once` no longer aborts on an unparseable payload: the good
    /// entries sharing its batch are still processed. Under Redis a batch is
    /// up to 100 entries acked all-or-nothing, so the old `?` trapped every
    /// one of them behind the bad one, forever.
    #[tokio::test]
    async fn an_unparseable_payload_does_not_stop_the_rest_of_its_batch() {
        let mut feed = FakeMovementFeed::new(vec![vec![
            "not json at all".to_string(),
            ORIGIN_DEPARTURE.to_string(),
        ]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .expect("a bad payload is no longer an error for the whole batch");

        assert_eq!(events.len(), 1, "the good entry still resolves its pin");
        assert_eq!(events[0].tracked_train_id, 1);
    }

    // --- Activation map growth (fix 4, and finding #5 of the 2026-09-25 review) ---

    /// A parked Activation observed on `observed` (default: the rail day
    /// these tests run in), with the given `schedule_end_date`.
    fn parked(train_uid: &str, end: Option<&str>) -> PendingActivation {
        parked_on(train_uid, end, "2026-08-28")
    }

    fn parked_on(train_uid: &str, end: Option<&str>, observed: &str) -> PendingActivation {
        PendingActivation {
            train_uid: train_uid.to_string(),
            schedule_end_date: end.map(|e| e.parse().unwrap()),
            observed_rail_day: observed.parse().unwrap(),
        }
    }

    #[test]
    fn pruning_drops_ended_schedules_and_keeps_current_ones() {
        let today: NaiveDate = "2026-08-28".parse().unwrap();
        let mut activations = HashMap::from([
            ("ended".to_string(), parked("C00001", Some("2026-08-27"))),
            (
                "ends_today".to_string(),
                parked("C00002", Some("2026-08-28")),
            ),
            (
                "ends_later".to_string(),
                parked("C00003", Some("2026-09-30")),
            ),
            ("unknown_expiry".to_string(), parked("C00004", None)),
        ]);

        prune_expired_activations(&mut activations, today);

        assert!(
            !activations.contains_key("ended"),
            "yesterday's schedule is forgettable"
        );
        assert!(
            activations.contains_key("ends_today"),
            "a schedule ending today is still live"
        );
        assert!(activations.contains_key("ends_later"));
        assert!(
            activations.contains_key("unknown_expiry"),
            "an unreadable end date fails open -- kept, not silently dropped",
        );
    }

    /// **Finding #5's regression test.** The old rule keyed ONLY on
    /// `schedule_end_date`, so a permanent CIF schedule -- most of the
    /// national timetable, validity window months out -- was never pruned at
    /// all, however long ago its Activation was actually observed. This is
    /// that exact entry: activated a month ago, schedule valid until
    /// December. It must go.
    #[test]
    fn a_month_old_activation_for_a_permanent_schedule_is_pruned_despite_its_end_date() {
        let today: NaiveDate = "2026-09-25".parse().unwrap();
        let mut activations = HashMap::from([(
            "221832406".to_string(),
            // The shape that made pruning inert: a real permanent schedule.
            parked_on("L83673", Some("2026-12-11"), "2026-08-25"),
        )]);

        prune_expired_activations(&mut activations, today);

        assert!(
            activations.is_empty(),
            "a month-old Activation cannot be claimed by any live Movement, whatever its \
             schedule's validity window says -- keeping it is both an unbounded-growth leak and \
             a mis-binding risk once TRUST recycles its train_id"
        );
    }

    /// The other side of the same rule: today's and yesterday's entries are
    /// exactly the ones a live Movement can still claim (an overnight
    /// working activated late on one rail day still moves on the next), so
    /// they must survive -- a prune that dropped them would break resolution
    /// rather than fix a leak.
    #[test]
    fn pruning_keeps_activations_a_live_movement_could_still_claim() {
        let today: NaiveDate = "2026-09-25".parse().unwrap();
        let mut activations = HashMap::from([
            (
                "today".to_string(),
                parked_on("C00001", Some("2026-12-11"), "2026-09-25"),
            ),
            (
                "yesterday".to_string(),
                parked_on("C00002", Some("2026-12-11"), "2026-09-24"),
            ),
            (
                "two_days_ago".to_string(),
                parked_on("C00003", Some("2026-12-11"), "2026-09-23"),
            ),
            (
                "three_days_ago".to_string(),
                parked_on("C00004", Some("2026-12-11"), "2026-09-22"),
            ),
        ]);

        prune_expired_activations(&mut activations, today);

        assert!(activations.contains_key("today"));
        assert!(
            activations.contains_key("yesterday"),
            "an overnight working activated yesterday is still running"
        );
        assert!(
            activations.contains_key("two_days_ago"),
            "exactly at MAX_PARKED_ACTIVATION_AGE_DAYS, still kept"
        );
        assert!(
            !activations.contains_key("three_days_ago"),
            "past the window, nothing can claim it"
        );
    }

    /// An unreadable `schedule_end_date` still fails open -- but only within
    /// the age window, so "unknown expiry" can no longer mean "kept
    /// forever".
    #[test]
    fn an_unknown_expiry_fails_open_but_still_ages_out() {
        let today: NaiveDate = "2026-09-25".parse().unwrap();
        let mut activations = HashMap::from([
            ("fresh".to_string(), parked_on("C00001", None, "2026-09-25")),
            ("stale".to_string(), parked_on("C00002", None, "2026-09-01")),
        ]);

        prune_expired_activations(&mut activations, today);

        assert!(activations.contains_key("fresh"));
        assert!(
            !activations.contains_key("stale"),
            "failing open on an unreadable date must not mean keeping it indefinitely"
        );
    }

    /// The Activation's observed rail day is taken from `received_at`'s
    /// Europe/London rail day, not a bare UTC date -- so an Activation
    /// handled at 01:00 BST belongs to the rail day that is still running,
    /// and isn't a day "ahead" of the movements that will claim it.
    #[tokio::test]
    async fn a_parked_activation_records_the_rail_day_it_was_observed_on() {
        let activation = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
            "train_service_code":"22345000","schedule_wtt_id":"WTT1",
            "schedule_start_date":"2026-08-28","schedule_end_date":"2026-12-11"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![activation.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        // 00:30 UTC on the 29th = 01:30 BST, still inside the rail day that
        // began on the 28th.
        let received_at: chrono::DateTime<chrono::Utc> = "2026-08-29T00:30:00Z".parse().unwrap();
        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            received_at,
        )
        .await
        .unwrap();

        assert_eq!(
            state
                .pending_activations
                .get("221832406")
                .map(|a| a.observed_rail_day),
            Some("2026-08-28".parse().unwrap()),
            "the rail day, not the UTC calendar date"
        );
    }

    /// A malformed `schedule_end_date` must cost memory, not data: the
    /// binding still has to reach the resolving Movement.
    #[tokio::test]
    async fn an_activation_with_an_unparseable_end_date_survives_pruning_and_still_binds() {
        let activation = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
            "train_service_code":"22345000","schedule_wtt_id":"WTT1",
            "schedule_start_date":"2026-08-28","schedule_end_date":"not-a-date"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![activation.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(
            state
                .pending_activations
                .get("221832406")
                .map(|a| a.schedule_end_date),
            Some(None),
            "an unreadable date parks with an unknown expiry rather than failing",
        );

        // Pruned against the rail day this batch was actually processed on.
        // (This used to pass a far-future `2099-01-01`, which only made sense
        // while `schedule_end_date` was the ONLY pruning signal and an
        // unreadable one meant "keep forever". Since finding #5 the primary
        // rule is the Activation's own age, so a 73-year-old entry is
        // correctly dropped and asserting otherwise would assert the leak
        // back into existence -- see
        // `an_unknown_expiry_fails_open_but_still_ages_out` for the boundary
        // behaviour this test's intent now lives in.)
        prune_expired_activations(&mut state.pending_activations, test_rail_day());

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events[0].resolved_train_uid, Some("C21373".to_string()));
    }

    #[tokio::test]
    async fn a_parked_activation_is_pruned_once_its_schedule_has_ended() {
        let activation = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
            "train_service_code":"22345000","schedule_wtt_id":"WTT1",
            "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![activation.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(state.pending_activations.len(), 1);

        prune_expired_activations(
            &mut state.pending_activations,
            "2026-08-29".parse().unwrap(),
        );
        assert!(
            state.pending_activations.is_empty(),
            "unclaimed national-stream activations must not accumulate forever",
        );
    }

    // --- Direct train_uid match on Activation (Task 16) ---

    #[tokio::test]
    async fn an_activation_with_a_known_train_uid_resolves_the_pin_immediately_without_waiting_for_a_movement()
     {
        let activation = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C88888","toc_id":"SW",
            "train_service_code":"22345000","schedule_wtt_id":"WTT1",
            "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![activation.to_string()]]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        reference
            .by_train_uid
            .insert("C88888".to_string(), vec![sharing(1)]);
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(
            events.is_empty(),
            "an Activation never posts an event of its own"
        );
        assert_eq!(
            state.resolved.get("221832406"),
            Some(&vec![1]),
            "resolved immediately on Activation, before any Movement at all"
        );
    }

    #[tokio::test]
    async fn the_first_movement_after_an_activation_direct_match_still_carries_resolved_train_id_for_the_db_flip()
     {
        let activation = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C88888","toc_id":"SW",
            "train_service_code":"22345000","schedule_wtt_id":"WTT1",
            "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
        }}]"#;
        let later_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![activation.to_string()],
            vec![later_arrival.to_string()],
        ]);
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        reference
            .by_train_uid
            .insert("C88888".to_string(), vec![sharing(1)]);
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(events[0].resolved_train_uid, Some("C88888".to_string()));
        assert_eq!(events[0].resolved_train_id, Some("221832406".to_string()));

        // A SECOND movement for the same train_id must not re-report resolution.
        let second_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946700000","actual_timestamp":"1787946700000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed2 = FakeMovementFeed::new(vec![vec![second_arrival.to_string()]]);
        let second_events = run_once(
            &mut feed2,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(second_events[0].resolved_train_id, None);
    }

    // --- Notifier-forwarding queue write side (Task 17) ---

    #[test]
    fn build_forward_signals_only_forwards_events_with_a_known_trains_id() {
        let mut trains_id_by_tracked_train_id = HashMap::new();
        trains_id_by_tracked_train_id.insert(1i64, 42i64);

        let events = vec![
            common::TrainMovementEventMessage {
                tracked_train_id: 1,
                resolved_train_uid: None,
                resolved_train_id: None,
                dedup_key: "d1".to_string(),
                msg_type: "0003".to_string(),
                event_type: Some("DEPARTURE".to_string()),
                loc_stanox: None,
                loc_crs: None,
                planned_timestamp: None,
                actual_timestamp: None,
                variation_status: None,
                raw_body: serde_json::json!({}),
                status: "en_route".to_string(),
                last_reported_location: Some("WAT".to_string()),
                last_event_type: Some("DEPARTURE".to_string()),
                delay_minutes: Some(3),
                next_calling_point: None,
                eta_next: None,
                eta_source: None,
            },
            common::TrainMovementEventMessage {
                tracked_train_id: 2, // no trains_id known for this one
                resolved_train_uid: None,
                resolved_train_id: None,
                dedup_key: "d2".to_string(),
                msg_type: "0003".to_string(),
                event_type: None,
                loc_stanox: None,
                loc_crs: None,
                planned_timestamp: None,
                actual_timestamp: None,
                variation_status: None,
                raw_body: serde_json::json!({}),
                status: "en_route".to_string(),
                last_reported_location: None,
                last_event_type: None,
                delay_minutes: None,
                next_calling_point: None,
                eta_next: None,
                eta_source: None,
            },
        ];

        let signals = build_forward_signals(&events, &trains_id_by_tracked_train_id);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].trains_id, 42);
        assert!(signals[0].event_summary.contains("WAT"));
    }

    /// `apply_reference_reload` must seed `trains_id_by_tracked_train_id`
    /// from every active ref that carries a `trains_id`, regardless of
    /// `resolution_status` -- an already-`resolved` subscription still
    /// needs its later movements forwarded (see `Reference`'s own doc
    /// comment on this field).
    #[test]
    fn apply_reference_reload_seeds_trains_id_by_tracked_train_id_for_resolved_refs() {
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        let mut state = ProcessorState::default();

        let mut resolved_ref = tracked_ref(7, "resolved", Some("221832406"));
        resolved_ref.trains_id = Some(99);

        apply_reference_reload(vec![resolved_ref], &mut reference, &mut state);

        assert_eq!(
            reference.trains_id_by_tracked_train_id.get(&7),
            Some(&99),
            "an already-resolved ref's trains_id must still be seeded for forwarding"
        );
    }

    // --- Two subscribers sharing one physical train (review finding I6) ---

    fn shared_ref(train_uid: &str, ids: Vec<i64>) -> Reference {
        let sharing: Vec<SharingSubscription> = ids.into_iter().map(sharing).collect();
        Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::from([(train_uid.to_string(), sharing)]),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        }
    }

    const SHARED_ACTIVATION: &str = r#"[{"header":{"msg_type":"0001"},"body":{
        "train_id":"221832406","train_uid":"C88888","toc_id":"SW",
        "train_service_code":"22345000","schedule_wtt_id":"WTT1",
        "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
    }}]"#;

    /// The headline scenario the whole shared-train redesign exists to
    /// support, and the one finding I6 said was broken: TWO subscriptions
    /// on ONE physical train. Before the fix, `by_train_uid` was
    /// `HashMap<String, i64>` and `resolved` was `HashMap<String, i64>`, so
    /// an Activation attributed the train to exactly one of them and the
    /// other never received a `resolved_train_id` -- meaning `api`'s
    /// `flip_legacy_resolution` never ran for it and it sat at `'pending'`
    /// forever while the train was visibly running.
    ///
    /// Also the test `routes::train::enrich_shared_train`'s doc comment in
    /// `crates/api` points at for the live-data half of finding I1.
    #[tokio::test]
    async fn nr_primary_subscriptions_resolve_from_a_live_activation_and_movement() {
        let later_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![SHARED_ACTIVATION.to_string()],
            vec![later_arrival.to_string()],
        ]);
        let reference = shared_ref("C88888", vec![1, 2]);
        let mut state = ProcessorState::default();

        let activation_events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert!(activation_events.is_empty());
        assert_eq!(
            state.resolved.get("221832406"),
            Some(&vec![1, 2]),
            "BOTH subscriptions sharing this train_uid must be attributed, not just one"
        );

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(
            events.len(),
            2,
            "one event per sharing subscriber, so each one's own \
             resolution_status can be flipped"
        );
        let mut ids: Vec<i64> = events.iter().map(|e| e.tracked_train_id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
        for event in &events {
            assert_eq!(
                event.resolved_train_id,
                Some("221832406".to_string()),
                "every sharing subscriber needs the resolution signal, not just the first"
            );
            assert_eq!(event.resolved_train_uid, Some("C88888".to_string()));
            assert_eq!(event.status, "en_route");
        }
        // Same real-world event -> same dedup key on both, which is exactly
        // what makes `upsert_train_movement`'s `ON CONFLICT (trains_id,
        // dedup_key) DO NOTHING` collapse them to one stored movement row.
        assert_eq!(events[0].dedup_key, events[1].dedup_key);
    }

    /// The one-time resolution signal must not repeat: a SECOND movement
    /// for the same train still fans out to both subscribers, but neither
    /// event re-announces a resolution.
    #[tokio::test]
    async fn a_later_movement_still_fans_out_to_both_subscribers_without_re_resolving() {
        let first = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946600000","actual_timestamp":"1787946600000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let second = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787946700000","actual_timestamp":"1787946700000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![SHARED_ACTIVATION.to_string()],
            vec![first.to_string()],
            vec![second.to_string()],
        ]);
        let reference = shared_ref("C88888", vec![1, 2]);
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        let later = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(later.len(), 2, "both subscribers keep receiving movements");
        for event in &later {
            assert_eq!(event.resolved_train_id, None);
            assert_eq!(event.resolved_train_uid, None);
        }
    }

    /// A Cancellation carries no location to match on, so it can only ever
    /// reach subscribers through `state.resolved` -- which means it had the
    /// same single-value bug, and the same fix.
    #[tokio::test]
    async fn a_cancellation_reaches_every_subscriber_sharing_the_train() {
        let cancellation = r#"[{"header":{"msg_type":"0002"},"body":{
            "train_id":"221832406","canx_timestamp":"1787946600000"
        }}]"#;
        let departure = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1787945520000","actual_timestamp":"1787945520000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![SHARED_ACTIVATION.to_string()],
            vec![departure.to_string()],
            vec![cancellation.to_string()],
        ]);
        let reference = shared_ref("C88888", vec![7, 8]);
        let mut state = ProcessorState::default();

        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 2);
        let mut ids: Vec<i64> = events.iter().map(|e| e.tracked_train_id).collect();
        ids.sort();
        assert_eq!(ids, vec![7, 8]);
        for event in &events {
            assert_eq!(event.status, "cancelled");
        }
    }

    /// The reference-reload half of the same bug. `apply_reference_reload`
    /// used a blind `by_train_uid.insert(...)`, so of N subscriptions
    /// sharing a `train_uid`, only whichever this loop visited LAST
    /// survived -- every other one was silently dropped from the
    /// direct-match fast path on every single reload tick.
    #[test]
    fn apply_reference_reload_keeps_every_subscription_sharing_one_train_uid() {
        fn subscription(id: i64, status: &str, train_uid: Option<&str>) -> common::TrackedTrainRef {
            common::TrackedTrainRef {
                id,
                service_date: "2026-09-12".parse().unwrap(),
                pin_origin_crs: None,
                pin_scheduled_departure: None,
                resolution_status: status.to_string(),
                train_uid: train_uid.map(str::to_string),
                train_id: None,
                trains_id: Some(99),
                destination_crs: None,
            }
        }

        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        let mut state = ProcessorState::default();

        apply_reference_reload(
            vec![
                subscription(1, "pending", Some("C88888")),
                subscription(2, "pending", Some("C88888")),
                subscription(3, "schedule_matched", Some("C88888")),
                subscription(4, "pending", Some("OTHER1")),
            ],
            &mut reference,
            &mut state,
        );

        let mut sharing = reference
            .by_train_uid
            .get("C88888")
            .cloned()
            .expect("the shared train_uid must be present");
        sharing.sort_by_key(|subscription| subscription.tracked_train_id);
        assert_eq!(
            sharing
                .iter()
                .map(|subscription| subscription.tracked_train_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
            "every subscription sharing this train_uid must survive the reload"
        );
        // And each one carries its own `service_date` through (finding #2):
        // without it the Activation fast path has nothing to check against.
        for subscription in &sharing {
            assert_eq!(
                subscription.service_date,
                "2026-09-12".parse::<NaiveDate>().unwrap(),
                "the subscription's own service_date must survive the reload too"
            );
        }
        assert_eq!(
            reference.by_train_uid.get("OTHER1"),
            Some(&vec![sharing_on(4, "2026-09-12")])
        );
    }

    /// The already-`resolved` rehydration path, same shape: after a
    /// restart, EVERY subscription that shares a resolved `train_id` must
    /// come back into `state.resolved`, or the ones that don't stop
    /// receiving movements entirely for the rest of the process's life.
    #[test]
    fn apply_reference_reload_rehydrates_every_resolved_subscription_sharing_one_train_id() {
        fn resolved(id: i64) -> common::TrackedTrainRef {
            common::TrackedTrainRef {
                id,
                service_date: "2026-08-28".parse().unwrap(),
                pin_origin_crs: None,
                pin_scheduled_departure: None,
                resolution_status: "resolved".to_string(),
                train_uid: Some("C88888".to_string()),
                train_id: Some("221832406".to_string()),
                trains_id: Some(99),
                destination_crs: None,
            }
        }

        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        let mut state = ProcessorState::default();
        apply_reference_reload(vec![resolved(1), resolved(2)], &mut reference, &mut state);

        let mut attributed = state
            .resolved
            .get("221832406")
            .cloned()
            .expect("the resolved train_id must be rehydrated");
        attributed.sort();
        assert_eq!(attributed, vec![1, 2]);

        // And a second reload tick must not duplicate them.
        apply_reference_reload(vec![resolved(1), resolved(2)], &mut reference, &mut state);
        assert_eq!(state.resolved.get("221832406").map(Vec::len), Some(2));
    }

    /// One real-world event fanned out to N subscribers must still enqueue
    /// ONE forwarding signal, not N identical ones -- they all carry the
    /// same `trains_id` by construction.
    #[test]
    fn build_forward_signals_deduplicates_one_trains_id_fanned_out_to_many_subscribers() {
        fn event(tracked_train_id: i64) -> common::TrainMovementEventMessage {
            common::TrainMovementEventMessage {
                tracked_train_id,
                resolved_train_uid: None,
                resolved_train_id: None,
                dedup_key: "shared-dedup".to_string(),
                msg_type: "0003".to_string(),
                event_type: Some("ARRIVAL".to_string()),
                loc_stanox: None,
                loc_crs: None,
                planned_timestamp: None,
                actual_timestamp: None,
                variation_status: None,
                raw_body: serde_json::json!({}),
                status: "en_route".to_string(),
                last_reported_location: Some("WAT".to_string()),
                last_event_type: Some("ARRIVAL".to_string()),
                delay_minutes: None,
                next_calling_point: None,
                eta_next: None,
                eta_source: None,
            }
        }

        let trains_id_by_tracked_train_id = HashMap::from([(1i64, 42i64), (2i64, 42i64)]);
        let signals = build_forward_signals(&[event(1), event(2)], &trains_id_by_tracked_train_id);
        assert_eq!(signals.len(), 1, "one shared train, one forwarding signal");
        assert_eq!(signals[0].trains_id, 42);
    }

    // --- Contradiction filter: a parked Activation's own train_uid vetoes a
    // CRS+time claim on a pin that names a different schedule (the
    // 2026-09-25 London Euston mis-attribution; see `process_message`'s own
    // comment on the filter for the full production case) ---

    /// An Activation binding `train_id` `221832406` to `train_uid`
    /// `"W34058"` -- the schedule that train_id REALLY is. Deliberately a
    /// different `train_uid` from `SHARED_ACTIVATION`'s `"C88888"`, so a
    /// test using this one can't accidentally satisfy `by_train_uid` too.
    const OTHER_SCHEDULES_ACTIVATION: &str = r#"[{"header":{"msg_type":"0001"},"body":{
        "train_id":"221832406","train_uid":"W34058","toc_id":"VT",
        "train_service_code":"22345000","schedule_wtt_id":"WTT1",
        "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
    }}]"#;

    /// An origin DEPARTURE at WAT 3 minutes BEFORE the pin every test here
    /// builds is scheduled to leave -- well inside
    /// `matching::SCHEDULED_DEPARTURE_TOLERANCE` (5 minutes, matched on
    /// `planned_timestamp` -- the booked time -- since this crate's own
    /// 2026-09-25 fix), so the CRS+time heuristic alone says "claim it".
    /// Raw wire value is one hour later than the true instant, per this
    /// test module's own timestamp convention (see `ORIGIN_DEPARTURE`).
    const EARLIER_ORIGIN_DEPARTURE: &str = r#"[{"header":{"msg_type":"0003"},"body":{
        "train_id":"221832406","event_type":"DEPARTURE",
        "planned_timestamp":"1787945340000","actual_timestamp":"1787945340000",
        "loc_stanox":"87212","variation_status":"ON TIME"
    }}]"#;

    fn reference_with_one_schedule_matched_pin(train_uid: Option<&str>) -> Reference {
        Reference {
            pending: vec![PendingPin {
                tracked_train_id: 1,
                pin_origin_crs: "WAT".to_string(),
                pin_scheduled_departure: "2026-08-28T18:32:00Z".parse().unwrap(),
                train_uid: train_uid.map(str::to_string),
            }],
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        }
    }

    /// THE REGRESSION TEST for the confirmed production bug: a pin that
    /// already knows its own schedule identity must not be claimed by
    /// another train's origin departure just because the two leave the same
    /// station inside the CRS+time heuristic's tolerance window. Before the
    /// contradiction filter this produced a `freshly_resolved` event, bound
    /// `state.resolved` for the life of the process, and made the user's
    /// not-yet-departed train read "en_route" with a different train's
    /// movements behind it.
    #[tokio::test]
    async fn a_parked_activation_for_a_different_schedule_cannot_claim_a_pin_by_crs_and_time() {
        let mut feed = FakeMovementFeed::new(vec![vec![
            OTHER_SCHEDULES_ACTIVATION.to_string(),
            EARLIER_ORIGIN_DEPARTURE.to_string(),
        ]]);
        // The pin tracks `Y80926`; the Activation says this train_id is
        // `W34058`. Nothing in `by_train_uid`, so the direct match can't
        // fire and the CRS+time heuristic is the only path left.
        let reference = reference_with_one_schedule_matched_pin(Some("Y80926"));
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert!(
            events.is_empty(),
            "a train TRUST itself says is W34058 must not claim a pin tracking Y80926, however              well its origin departure lines up: got {events:?}"
        );
        assert!(
            !state.resolved.contains_key("221832406"),
            "the claim must not be recorded either -- state.resolved has no unwind path, so a              wrong binding here locks the pin out for the life of the process"
        );
    }

    /// The other side of the same filter: when the parked Activation names
    /// the SAME schedule the pin is tracking, the CRS+time fallback must
    /// still resolve it. This is the real path for a subscription created
    /// AFTER its train's Activation was already parked -- `by_train_uid`
    /// never saw it at Activation time, so the heuristic is what rescues it.
    #[tokio::test]
    async fn a_parked_activation_for_the_same_schedule_still_allows_the_crs_and_time_claim() {
        let mut feed = FakeMovementFeed::new(vec![vec![
            OTHER_SCHEDULES_ACTIVATION.to_string(),
            EARLIER_ORIGIN_DEPARTURE.to_string(),
        ]]);
        let reference = reference_with_one_schedule_matched_pin(Some("W34058"));
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1, "the matching pin must still be claimed");
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(events[0].resolved_train_id, Some("221832406".to_string()));
        assert_eq!(events[0].resolved_train_uid, Some("W34058".to_string()));
        assert_eq!(events[0].status, "en_route");
    }

    /// The comparison is case-insensitive on both sides, same posture as
    /// every other identity comparison in this codebase -- a lowercased
    /// `train_uid` must not read as a contradiction.
    #[tokio::test]
    async fn the_contradiction_filter_compares_train_uids_case_insensitively() {
        let mut feed = FakeMovementFeed::new(vec![vec![
            OTHER_SCHEDULES_ACTIVATION.to_string(),
            EARLIER_ORIGIN_DEPARTURE.to_string(),
        ]]);
        let reference = reference_with_one_schedule_matched_pin(Some("w34058"));
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1, "casing alone is not a contradiction");
        assert_eq!(events[0].tracked_train_id, 1);
    }

    /// A pin that genuinely has no schedule identity of its own is
    /// unaffected: the heuristic is all it has ever had, and this filter
    /// must not take it away.
    #[tokio::test]
    async fn a_pin_with_no_known_train_uid_is_still_claimable_when_an_activation_names_one() {
        let mut feed = FakeMovementFeed::new(vec![vec![
            OTHER_SCHEDULES_ACTIVATION.to_string(),
            EARLIER_ORIGIN_DEPARTURE.to_string(),
        ]]);
        let reference = reference_with_one_schedule_matched_pin(None);
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(events[0].resolved_train_uid, Some("W34058".to_string()));
    }

    /// With NO Activation parked at all (this process started after it was
    /// delivered, or the feed never sent one), there is nothing to
    /// contradict and the pre-fix behavior is preserved exactly -- the
    /// filter's named residual limitation, pinned by a test so a future
    /// change to it is deliberate.
    #[tokio::test]
    async fn without_a_parked_activation_the_heuristic_is_unchanged_even_for_a_named_pin() {
        let mut feed = FakeMovementFeed::new(vec![vec![EARLIER_ORIGIN_DEPARTURE.to_string()]]);
        let reference = reference_with_one_schedule_matched_pin(Some("Y80926"));
        let mut state = ProcessorState::default();

        let events = run_once(
            &mut feed,
            &reference,
            &mut state,
            &TEST_STANOX_CRS,
            test_received_at(),
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(
            events[0].resolved_train_uid, None,
            "no Activation was ever parked for this train_id"
        );
    }

    /// `apply_reference_reload` must actually carry a ref's `train_uid` onto
    /// the `PendingPin` it builds -- without this the filter above can never
    /// fire in production, however correct it is in isolation.
    #[test]
    fn apply_reference_reload_carries_a_refs_train_uid_onto_its_pending_pin() {
        let tracked = common::TrackedTrainRef {
            id: 9,
            service_date: "2026-08-28".parse().unwrap(),
            pin_origin_crs: Some("WAT".to_string()),
            pin_scheduled_departure: Some("2026-08-28T18:32:00Z".parse().unwrap()),
            resolution_status: "schedule_matched".to_string(),
            train_uid: Some("Y80926".to_string()),
            train_id: None,
            trains_id: None,
            destination_crs: None,
        };
        let mut reference = Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        };
        let mut state = ProcessorState::default();

        apply_reference_reload(vec![tracked], &mut reference, &mut state);

        assert_eq!(reference.pending.len(), 1);
        assert_eq!(
            reference.pending[0].train_uid,
            Some("Y80926".to_string()),
            "the pin must remember the schedule it is tracking"
        );
        assert_eq!(
            reference.by_train_uid.get("Y80926").map(Vec::as_slice),
            Some(
                [SharingSubscription {
                    tracked_train_id: 9,
                    service_date: "2026-08-28".parse().unwrap(),
                    pin_scheduled_departure: Some("2026-08-28T18:32:00Z".parse().unwrap()),
                }]
                .as_slice()
            ),
            "and must still be reachable by the Activation direct match, with its own \
             pin_scheduled_departure carried through too (finding H3)"
        );
    }
}
