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
//! properly means either relaxing `crates/api`'s two-field guard (which
//! reopens already-reviewed Task 4 work) or restructuring when this module
//! mutates its maps relative to the batch's HTTP post; both were judged out
//! of scope for the task that introduced this file, and are deliberately
//! left for a follow-up rather than half-done here.
//!
//! **A failed post is recovered by a restart, not by an in-process retry --
//! and only if that restart happens before the next successful commit.**
//! `process_message` mutates `state.resolved`, `state.pending_activations`
//! and `state.last_derived` as it builds each event -- *before* `main.rs`
//! has attempted, let alone confirmed, the `post_train_events` call for that
//! batch. This paragraph used to describe that ordering as costing the
//! resolution binding on a Kafka *redelivery* of the same messages. That
//! premise was wrong, and why is worth stating precisely.
//!
//! `StreamConsumer::recv` never hands the same message to a running process
//! twice, whatever the offset state -- committed offsets govern where a
//! *new* consumer session resumes, not what an existing one replays
//! (verified against rdkafka 0.39.0's consumer API and librdkafka 2.12.1's
//! `rd_kafka_offset_store` contract in `rdkafka.h`, which is about the
//! stored/committed position only). So there is no in-process redelivery for
//! these maps to go stale against: a batch whose post fails is simply
//! dropped from this process's view. Recovery is a restart, and a restart
//! rebuilds every one of these maps from scratch -- `main.rs` constructs a
//! fresh `ProcessorState::default()`, seeds `resolved` from the reference
//! reload, and re-parks Activations from whatever the feed replays. The
//! replayed Movement therefore resolves its pin as if for the first time,
//! and the pin is still `pending` in the database, because the post that
//! would have changed that is precisely the one that failed.
//!
//! What remains is narrower but real. `feed/kafka.rs` now stores an offset
//! only when `commit` is called (`enable.auto.offset.store=false`), so a
//! failed post confirms nothing -- but the *next* message received
//! overwrites the offset being held, and committing that one commits past
//! the failed one. A sustained `api` outage therefore confirms nothing at
//! all and is fully recoverable by a restart (asserted by `main.rs`'s
//! `a_sustained_outage_never_commits`), while a single transient failure
//! sandwiched between successes is still lost once the following batch
//! commits. Closing that last gap means retrying the unposted batch
//! in-process rather than pulling the next one, *with* a retry bound so a
//! permanently-unpostable batch can't stall the consumer forever -- a
//! design decision of its own, deliberately left for follow-up rather than
//! half-done here.
//!
//! The Activation-binding consequence above survives in one form: if a
//! `0001` Activation's offset was already committed before the failure, the
//! restart's replay begins after it, so the resolving Movement goes out with
//! `resolved_train_uid: None` and lands in the previous paragraph's
//! stuck-`pending` state.
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

use anyhow::Context;
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
    pub by_train_uid: HashMap<String, Vec<i64>>,
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
#[derive(Debug, Default)]
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
}

/// What an Activation parks for a later Movement to claim: the `train_uid`
/// the event actually needs, plus enough to know when the entry is safe to
/// forget.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingActivation {
    pub train_uid: String,
    /// `Activation::schedule_end_date` parsed as a date, or `None` when it
    /// didn't parse. `None` means "expiry unknown", and
    /// `prune_expired_activations` deliberately fails *open* on it: an entry
    /// whose end date can't be read is kept rather than dropped, so a
    /// malformed field costs a little memory instead of silently losing a
    /// binding the feed did send us.
    pub schedule_end_date: Option<NaiveDate>,
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
    let mut by_train_uid: HashMap<String, Vec<i64>> = HashMap::new();
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
                    by_train_uid
                        .entry(train_uid.clone())
                        .or_default()
                        .push(tracked.id);
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

/// Drops parked Activations whose schedule has already ended. Pure, so the
/// caller supplies `today` rather than this reading the clock. Entries whose
/// `schedule_end_date` didn't parse are kept -- see `PendingActivation`.
pub fn prune_expired_activations(
    activations: &mut HashMap<String, PendingActivation>,
    today: NaiveDate,
) {
    activations.retain(|_, activation| match activation.schedule_end_date {
        Some(end) => end >= today,
        None => true,
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
pub async fn run_once<F: MovementFeed>(
    feed: &mut F,
    reference: &Reference,
    state: &mut ProcessorState,
    stanox_crs: &crate::stanox_crs::StanoxCrsTable,
) -> anyhow::Result<Vec<common::TrainMovementEventMessage>> {
    let raw_batches = feed.next_batch().await?;
    let mut events = Vec::new();

    for raw in raw_batches {
        // The raw payload is attached to the error via `.with_context` (not
        // just propagated with `?`) so a real-world parse failure's log line
        // shows the actual bytes that didn't match either shape
        // `schema::parse_batch` understands -- this codebase has already
        // been burned twice by an envelope-shape assumption that looked
        // right on paper (a JSON-array batch, then a bare single-object
        // envelope) but didn't match what a real broker sent; the fix both
        // times was a live payload, not another guess. Without the raw
        // string in the error, `main.rs`'s `tracing::error!(error = ?err,
        // ...)` only ever showed the parse failure's shape (e.g. "missing
        // field `header`"), never the payload that produced it.
        let messages = trust_schema::schema::parse_batch(&raw)
            .with_context(|| format!("raw payload: {raw}"))?;
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
            for event in process_message(&message, reference, state, stanox_crs) {
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
) -> Vec<common::TrainMovementEventMessage> {
    match message {
        // An Activation never produces a posted event of its own -- it only
        // parks its train_uid for the Movement that eventually resolves a
        // pin for this train_id to claim.
        TrustMessage::Activation(activation) => {
            // Direct train_uid match FIRST, per the design spec's §3: strictly
            // more reliable than the ±20-minute CRS+time heuristic, since it
            // needs no location or timing coincidence at all. Only a
            // train_id not already resolved is eligible -- this must never
            // clobber an existing resolution.
            // EVERY subscription sharing this train_uid is attributed,
            // not just one (review finding I6). Unioned rather than
            // replaced, so an existing resolution -- from an earlier
            // Activation, a CRS+time claim, or the reference reload's
            // rehydration -- is never clobbered; only genuinely new
            // subscriptions are added. The "freshly resolved" flag is
            // raised only if something actually WAS added, so a redelivered
            // Activation for a fully-attributed train doesn't make the next
            // Movement re-announce a resolution.
            if let Some(sharing) = reference.by_train_uid.get(&activation.train_uid) {
                let attributed = state
                    .resolved
                    .entry(activation.train_id.clone())
                    .or_default();
                let mut added_any = false;
                for &tracked_train_id in sharing {
                    if !attributed.contains(&tracked_train_id) {
                        attributed.push(tracked_train_id);
                        added_any = true;
                    }
                }
                if added_any {
                    state
                        .activation_matched_awaiting_movement
                        .insert(activation.train_id.clone());
                }
            }
            state.pending_activations.insert(
                activation.train_id.clone(),
                PendingActivation {
                    train_uid: activation.train_uid.clone(),
                    // TRUST's schedule dates are `YYYY-MM-DD`, which is
                    // exactly `NaiveDate`'s own `FromStr` format. Anything
                    // else parks with an unknown expiry rather than failing.
                    schedule_end_date: activation.schedule_end_date.parse::<NaiveDate>().ok(),
                },
            );
            Vec::new()
        }

        TrustMessage::Movement(movement) => {
            let planned = movement
                .planned_timestamp
                .as_deref()
                .and_then(parse_epoch_millis);
            let actual = movement
                .actual_timestamp
                .as_deref()
                .and_then(parse_epoch_millis);
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
                        let freshly_resolved = state
                            .activation_matched_awaiting_movement
                            .remove(&movement.train_id);
                        (tracked_train_ids, freshly_resolved)
                    }
                    None => {
                        // Only a DEPARTURE may claim a pin. `resolve_origin_departure`
                        // knows nothing about event types -- it compares a
                        // location and a ±20-minute window, and TRUST's
                        // `event_type` is one of ARRIVAL / DEPARTURE / PASS
                        // (see `schema::Movement`). At a busy terminus an
                        // ARRIVAL or PASS near a pin's scheduled departure
                        // would otherwise satisfy both tests and claim it, and
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
                        // be offered again. `resolve_origin_departure` is a
                        // first-match scan with no notion of "taken", so two
                        // different trains departing the same origin inside the
                        // same tolerance window would otherwise both resolve to
                        // the same tracked_train_id and flip-flop what the user
                        // sees. Filtering here rather than inside `matching`
                        // keeps that module a pure function of its arguments.
                        let claimed: HashSet<i64> =
                            state.resolved.values().flatten().copied().collect();
                        let unclaimed: Vec<crate::matching::PendingPin> = reference
                            .pending
                            .iter()
                            .filter(|pin| !claimed.contains(&pin.tracked_train_id))
                            .cloned()
                            .collect();

                        // Still a SINGLE claim, deliberately: this is the
                        // legacy CRS+time heuristic, and letting one
                        // departure claim every pin that happens to fall in
                        // its tolerance window would re-open exactly the
                        // mis-attribution the `claimed` filter above exists
                        // to prevent. Subscribers sharing one physical train
                        // are attributed through `by_train_uid` (which knows
                        // their identity for certain), not through this
                        // guess. See this fix's report for the residual
                        // limitation this leaves.
                        let Some(tracked_train_id) = crate::matching::resolve_origin_departure(
                            loc_crs_for_match,
                            actual_ts,
                            &unclaimed,
                        ) else {
                            return Vec::new();
                        };
                        state
                            .resolved
                            .insert(movement.train_id.clone(), vec![tracked_train_id]);
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
            state
                .last_derived
                .insert(movement.train_id.clone(), derived.clone());

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
                    state
                        .pending_activations
                        .remove(&movement.train_id)
                        .map(|activation| activation.train_uid),
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
            state
                .last_derived
                .insert(cancellation.train_id.clone(), derived.clone());

            let dedup =
                trust_schema::dedup::dedup_key(&cancellation.train_id, "0002", None, None, None);

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
                    // TRUST's confirmed `canx_timestamp` is the time the
                    // cancellation actually happened; it is the only timestamp
                    // this message shape carries, so it lands in the event's
                    // generic `actual_timestamp` rather than being dropped.
                    actual_timestamp: cancellation
                        .canx_timestamp
                        .as_deref()
                        .and_then(parse_epoch_millis),
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

        TrustMessage::ChangeOfOrigin(change) => passthrough_event(&change.train_id, "0006", state),
        TrustMessage::ChangeOfIdentity(change) => {
            passthrough_event(&change.train_id, "0007", state)
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
) -> Vec<common::TrainMovementEventMessage> {
    let Some(tracked_train_ids) = state.resolved.get(train_id) else {
        return Vec::new();
    };
    let derived = previous_state(state, train_id);

    let dedup = trust_schema::dedup::dedup_key(train_id, msg_type, None, None, None);
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

fn parse_epoch_millis(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let millis: i64 = raw.parse().ok()?;
    chrono::DateTime::from_timestamp_millis(millis)
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

    fn reference_with_one_pending(id: i64, crs: &str, scheduled: &str) -> Reference {
        Reference {
            pending: vec![PendingPin {
                tracked_train_id: id,
                pin_origin_crs: crs.to_string(),
                pin_scheduled_departure: scheduled.parse().unwrap(),
            }],
            by_train_uid: HashMap::new(),
            trains_id_by_tracked_train_id: HashMap::new(),
            destination_crs_by_trains_id: HashMap::new(),
        }
    }

    /// A resolving origin departure at WAT, matching the pin every test
    /// below builds with `reference_with_one_pending(1, "WAT", ...)`.
    const ORIGIN_DEPARTURE: &str = r#"[{"header":{"msg_type":"0003"},"body":{
        "train_id":"221832406","event_type":"DEPARTURE",
        "planned_timestamp":"1787941920000","actual_timestamp":"1787941920000",
        "loc_stanox":"87212","variation_status":"ON TIME"
    }}]"#;

    #[tokio::test]
    async fn a_matching_movement_produces_one_event() {
        let mut feed = FakeMovementFeed::new(vec![vec![ORIGIN_DEPARTURE.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787941920000","actual_timestamp":"1787941920000",
            "loc_stanox":"73000","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![raw_batch.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn an_empty_batch_produces_no_events_and_is_not_an_error() {
        let mut feed = FakeMovementFeed::new(vec![vec![]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();
        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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

        let activation_events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert!(
            activation_events.is_empty(),
            "an Activation alone posts nothing"
        );

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787943000000","actual_timestamp":"1787943000000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![later_arrival.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let first = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert_eq!(first[0].resolved_train_id, Some("221832406".to_string()));

        let second = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787943000000","actual_timestamp":"1787943000000",
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

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787943000000","actual_timestamp":"1787943000000",
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

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "train_id":"221832406","canx_timestamp":"1787943600000","canx_type":"EN ROUTE"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![cancellation.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let movement_events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert_eq!(
            movement_events[0].last_reported_location,
            Some("WAT".to_string())
        );

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "train_id":"221832406","canx_timestamp":"1787943600000","canx_type":"AT ORIGIN"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![cancellation.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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

        run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787943000000","actual_timestamp":"1787943000000",
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

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787943000000","actual_timestamp":"1787943000000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![later_arrival.to_string()],
        ]);
        let mut reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let first = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert_eq!(first[0].tracked_train_id, 1);

        apply_reference_reload(
            vec![tracked_ref(99, "resolved", Some("221832406"))],
            &mut reference,
            &mut state,
        );

        let second = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
        }]);
        let cell = std::sync::RwLock::new(initial);

        let fresh = vec![common::StanoxCrsRecord {
            stanox: "72410".to_string(),
            crs: "EU2".to_string(),
            tiploc: "EUSTON".to_string(),
            station_name: "LONDON EUSTON".to_string(),
            source_sequence: 942,
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

    /// Two services can leave the same origin inside one pin's ±20min
    /// tolerance window; only the first may claim it.
    #[tokio::test]
    async fn an_already_claimed_pin_cannot_be_stolen_by_a_second_train() {
        // Same station, same window, different train_id.
        let other_train_departure = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832407","event_type":"DEPARTURE",
            "planned_timestamp":"1787942400000","actual_timestamp":"1787942400000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![ORIGIN_DEPARTURE.to_string()],
            vec![other_train_departure.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let first = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert_eq!(first[0].tracked_train_id, 1);
        assert_eq!(first[0].resolved_train_id, Some("221832406".to_string()));

        let second = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787941920000","actual_timestamp":"1787941920000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![arrival_at_origin.to_string()],
            vec![ORIGIN_DEPARTURE.to_string()],
        ]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert!(events.is_empty(), "an arrival is not an origin departure");
        assert!(
            !state.resolved.contains_key("221832499"),
            "and it leaves no resolution behind either",
        );

        // The pin must still be there for the train that really departs.
        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787941920000","actual_timestamp":"1787941920000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![vec![pass_at_origin.to_string()]]);
        let reference = reference_with_one_pending(1, "WAT", "2026-08-28T18:32:00Z");
        let mut state = ProcessorState::default();

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert!(events.is_empty());
        assert!(state.resolved.is_empty(), "the pin stays unclaimed");
    }

    // --- Activation map growth (fix 4) ---

    fn parked(train_uid: &str, end: Option<&str>) -> PendingActivation {
        PendingActivation {
            train_uid: train_uid.to_string(),
            schedule_end_date: end.map(|e| e.parse().unwrap()),
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

        run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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

        prune_expired_activations(
            &mut state.pending_activations,
            "2099-01-01".parse().unwrap(),
        );

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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

        run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
        reference.by_train_uid.insert("C88888".to_string(), vec![1]);
        let mut state = ProcessorState::default();

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787943000000","actual_timestamp":"1787943000000",
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
        reference.by_train_uid.insert("C88888".to_string(), vec![1]);
        let mut state = ProcessorState::default();

        run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tracked_train_id, 1);
        assert_eq!(events[0].resolved_train_uid, Some("C88888".to_string()));
        assert_eq!(events[0].resolved_train_id, Some("221832406".to_string()));

        // A SECOND movement for the same train_id must not re-report resolution.
        let second_arrival = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787943100000","actual_timestamp":"1787943100000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed2 = FakeMovementFeed::new(vec![vec![second_arrival.to_string()]]);
        let second_events = run_once(&mut feed2, &reference, &mut state, &TEST_STANOX_CRS)
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
        Reference {
            pending: Vec::new(),
            by_train_uid: HashMap::from([(train_uid.to_string(), ids)]),
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
            "planned_timestamp":"1787943000000","actual_timestamp":"1787943000000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![SHARED_ACTIVATION.to_string()],
            vec![later_arrival.to_string()],
        ]);
        let reference = shared_ref("C88888", vec![1, 2]);
        let mut state = ProcessorState::default();

        let activation_events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        assert!(activation_events.is_empty());
        assert_eq!(
            state.resolved.get("221832406"),
            Some(&vec![1, 2]),
            "BOTH subscriptions sharing this train_uid must be attributed, not just one"
        );

        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "planned_timestamp":"1787943000000","actual_timestamp":"1787943000000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let second = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"ARRIVAL",
            "planned_timestamp":"1787943100000","actual_timestamp":"1787943100000",
            "loc_stanox":"86031","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![SHARED_ACTIVATION.to_string()],
            vec![first.to_string()],
            vec![second.to_string()],
        ]);
        let reference = shared_ref("C88888", vec![1, 2]);
        let mut state = ProcessorState::default();

        run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        let later = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
            "train_id":"221832406","canx_timestamp":"1787943000000"
        }}]"#;
        let departure = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1787941920000","actual_timestamp":"1787941920000",
            "loc_stanox":"87212","variation_status":"ON TIME"
        }}]"#;
        let mut feed = FakeMovementFeed::new(vec![
            vec![SHARED_ACTIVATION.to_string()],
            vec![departure.to_string()],
            vec![cancellation.to_string()],
        ]);
        let reference = shared_ref("C88888", vec![7, 8]);
        let mut state = ProcessorState::default();

        run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
            .await
            .unwrap();
        let events = run_once(&mut feed, &reference, &mut state, &TEST_STANOX_CRS)
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
                service_date: "2026-08-28".parse().unwrap(),
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
        sharing.sort();
        assert_eq!(
            sharing,
            vec![1, 2, 3],
            "every subscription sharing this train_uid must survive the reload"
        );
        assert_eq!(reference.by_train_uid.get("OTHER1"), Some(&vec![4]));
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
}
