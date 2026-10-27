# Darwin Push Port and arrival boards (DS-152)

Pre-departure prediction phases 2 and 3
([spec](superpowers/specs/2026-10-21-pre-departure-prediction-design.md)):
live platforms with Darwin's confirmed and suppressed flags at every stop,
Darwin's live next-working links for the late-start prediction, the
timetable's joins and divides as a cap on the inferred links, and arrival
boards for the user's alighting stops. Everything that reads a new feed is
off by default.

## What runs

```text
RDM Push Port JSON topic (Kafka, SASL_SSL)
  -> darwin-relay            decode, keep, merge 30 s of messages
  -> ds:ingest:darwin        Redis stream, schema darwin-pushport/1
  -> ingest-writer           ds_store::darwin::apply
  -> darwin_services, darwin_calls, darwin_associations
  -> api (JourneyStop platforms), api + notifier (predictedStart)
```

- **darwin-relay** (`crates/darwin-relay`) is the one Kafka client of the
  topic, in movement-relay's shape: the RDM-issued consumer group, manual
  offset store and commit, readiness from the partition assignment. A
  record's value is a JSON envelope whose `bytes` field holds the Push Port
  document (the `Pport` element as JSON); either may be gzipped, and
  `bytes` may be base64. It keeps `schedule`, `TS`, `association` and
  `deactivated` from each update (`uR`, and a snapshot's `sR`), merges up
  to 30 seconds of them so each key appears once, and produces one entry.
  Offsets are committed only once Redis has the entry: a restart re-reads
  at most one batch, and every stored row is guarded by its Darwin
  timestamp, so a duplicate changes nothing. A record that cannot be
  decoded is logged, counted and skipped.
- **ingest-writer** applies `darwin-pushport/1` (`ingestWriter.streams.darwin`)
  and records `ingest_freshness` source `darwin-pushport` at the batch's
  newest Darwin time.
- **The api** lays each tracked train's Push Port locations over its stops
  (`crates/api/src/data/pushport_platform.rs`): a stop matches the Darwin
  location with its TIPLOC and working time. A confirmed platform is served
  as `platformBasis: "confirmed"`; a suppressed one is not served
  (`platformSuppressed: true`); `platformChanged` compares with the
  timetable's booked platform, else Darwin's planned one.
- **The late-start prediction** (`ds_store::trains::predicted_start`) takes
  a train's previous working from Darwin's live next-working (NP) link
  (`darwin_previous_workings`) when there is one, and from the platform
  sequence otherwise. An inferred link where either train joins or divides
  that day (`schedule_associations`, from the CIF `AA` records) is capped
  at medium, so it never pushes.

## Data model

| Table | Key | What |
|---|---|---|
| `darwin_services` | `rid` | RID to CIF UID and scheduled start date (`ssd`), operator, headcode, RSID, deleted, deactivated |
| `darwin_calls` | `rid, tiploc, wta, wtd, wtp` | per location: Darwin's planned platform and cancelled flag (schedule), the current platform with `platform_confirmed`, `platform_suppressed`, `platform_cis_suppressed`, `platform_source`, the generated `platform_basis` (`suppressed`, `confirmed`, `altered`, `planned`), forecasts and actuals (TS) |
| `darwin_associations` | `main_rid, assoc_rid, tiploc, category` | NP/JJ/VV/LK links, cancelled and deleted flags, both trains' working times there |
| `darwin_previous_workings` (view) | `service_date, uid` | each train's previous working by its newest live NP link, with the booked turnaround |
| `schedule_associations` | `service_date, main_uid, assoc_uid, tiploc` | the timetable's JJ/VV (and NP) associations per date, after the STP overlay |

A row joins DS's trains by `(uid, ssd)` = `(trains.train_uid,
trains.service_date)`. A TS location replaces only the groups it carries
(platform, arrival, departure, pass). The aggregator prunes all three
Darwin tables two days after their service date.

## Switching on the Push Port

Prerequisites, already true in production: the ingest-writer connects as
its own role (`postgresql.roles.perService.writer.connect`), and the Redis
ACL rollout is at `stage: narrow` with `defaultUser: "on-unshared"`. The
Secret `distant-signal-darwin-pushport` exists in namespace
`distant-signal` (Ranma deploy tu99) with the keys `username`, `password`,
`consumer-group`, `kafka-bootstrap-server`, `sasl-mechanism`,
`security-protocol`, `sasl-config` and `kafka-topic`; the chart reads each
one by name and renders none of them.

### Ranma steps

1. **The relay's Redis password.** Add a key `darwin-relay-password` to
   the Redis users Secret (`redis.acl.existingSecret`, a SealedSecret in
   Ranma-Config): letters and digits only, sealed byte-exact from a file
   with no trailing newline (`kubeseal --from-file`), and check its length
   after sealing.
2. **The broker port** (only if `networkPolicy.egress.internetPorts` is
   set): read the port of the Secret's `kafka-bootstrap-server` and set
   `darwinRelay.kafka.brokerPorts` to it (default `[9092]`).
3. **One release** with these values:

   ```yaml
   redis:
     acl:
       clients:
         darwinRelay: true      # renders the darwin-relay ACL user; Redis restarts once
   ingestWriter:
     streams:
       darwin: apply
   darwinRelay:
     enabled: true
   ```

   The render refuses `darwinRelay.enabled` without the other two, and
   `darwin: apply` without the writer's own role.
4. **Check** (within a few minutes):
   - the `darwin-relay` pod is Ready (partitions assigned) and its log shows
     no SASL error;
   - `distant_signal_darwin_relay_records_total{outcome="document"}` rises
     and `outcome="malformed"` stays near 0;
   - `redis-cli --user ds-admin XLEN ds:ingest:darwin` grows (it is capped
     near 240 entries) and the writer's `ds:ingest:darwin` lag stays low;
   - in Postgres: `SELECT count(*), max(updated_at) FROM darwin_calls;` and
     `SELECT fetched_at FROM ingest_freshness WHERE source =
     'darwin-pushport';` are current;
   - a tracked train's `GET /Train/{id}` shows `platformBasis: "confirmed"`
     at a stop once its platform is confirmed.
5. **Alerts** come with it: `DistantSignalDarwinPushPortStale`,
   `DistantSignalDarwinConsumerLagHigh` and
   `DistantSignalIngestSourceStale{source="darwin-pushport"}`
   ([runbooks](alerts.md#darwin-push-port-darwin-relay)).

**Rollback:** `darwinRelay.enabled: false`. The api keeps serving the
stored rows until the aggregator prunes them, then falls back to the
boards. `ingestWriter.streams.darwin` and the ACL user may stay on.

**First start** reads from the newest message (`autoOffsetReset: latest`):
trains already running fill in as Darwin re-sends their state, schedules
for later trains as Darwin publishes them. The Push Port's snapshot
product (a full state, for a resync after a long outage) is not built;
it is the later option for a gap longer than Kafka's retention.

## Switching on the arrival boards

poller-ldbws's tracked-stop lane can also poll the LDBWS arrivals product
for each tracked train's alighting stops (a journey leg's destination, or
the subscription's), where the train has a booked arrival: a terminating
or set-down-only stop, which departure boards never list. The boards ride
`ds:ingest:station-samples` as `tracked-stop-arrival-boards/1` into
`tracked_stop_arrival_boards` (targets: the `ingest_tracked_stop_arrival_targets`
view, which only `ldbws_ro` reads). The api sets `platform` (basis
`board`) on such a stop when it has no live platform yet; a Push Port
platform still wins. It has its own budget (`maxRequestsPerCycle`, 6 a
cycle, and 3 s of the cycle, taken from the departure lane's 10 s so the
45 s station rotation and both lanes stay under the 60 s interval) and is
switched on independently of the departure lane
(`pollers.ldbws.trackedStops`).

The connection is the Secret `distant-signal-ldbws-arrivals`: `url`,
`consumer-key` (sent as `x-apikey`, as every RDM API product takes it) and
`consumer-secret` (wired, optional, not sent). The request URL is built
from `url`: a `{crs}` placeholder is replaced by the station; a URL whose
last path segment starts with `Get` gets `/<CRS>` appended; any other gets
`/GetArrBoardWithDetails/<CRS>`.

### Ranma steps

1. Check the Secret `distant-signal-ldbws-arrivals` has `url` and
   `consumer-key` (`consumer-secret` is optional), and which of the three
   URL shapes above `url` is.
2. `ingestWriter.streams.station-samples` is `apply` (the default), and
   the writer runs this release (it has the new handler).
3. Set `pollers.ldbws.trackedStopArrivals.enabled: true` (and
   `maxRequestsPerCycle` if 6 a cycle is not right; it only has 3 s of
   the cycle).
4. If `url` is not on port 443 and `networkPolicy.egress.internetPorts`
   is set, add the port to `networkPolicy.components.poller-ldbws.internetPorts`.
5. Check: `distant_signal_ldbws_tracked_stop_arrival_polls_total{outcome="ok"}`
   rises, rows appear in `tracked_stop_arrival_boards`, and near arrival a
   tracked train's destination shows `platformBasis: "board"`.
