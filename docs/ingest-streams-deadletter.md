# `ds:dlq:*`: ingest stream dead letters and how to recover them

The ingest-writer applies the `ds:ingest:*` streams to Postgres (ingest spec
§7; plan phase 3a). An entry that can never succeed as it stands goes to
its stream's dead-letter stream, `ds:dlq:<domain>` (`ds:ingest:tfl` →
`ds:dlq:tfl`), and is then ACKed, so it does not block the stream. This
runbook is modelled on [movement-events-deadletter](movement-events-deadletter.md),
which covers the separate `movement-events` pipeline.

Code: `crates/ingest-stream/src/consumer.rs` (`dead_letter`,
`rejected_fields`) and the writer's handlers. Alerts:
[DistantSignalIngestDeadLetters](alerts.md#distantsignalingestdeadletters)
and [DistantSignalIngestDeadLetterExpiring](alerts.md#distantsignalingestdeadletterexpiring).

## What is dead-lettered, and what never is

| `reason` | When | What the record holds |
| --- | --- | --- |
| `poison` | the handler refused the whole entry: an unknown schema *name*, or a data error (SQLSTATE class 22 or 23) for the whole entry | the source entry, unchanged |
| `rejected_rows` | the entry was applied, but some rows were refused for a data error; the rest committed | the source envelope with `body` replaced by only the refused rows, and `rejected_rows=true` |
| `undecodable` | the envelope could not be decoded: a missing or malformed field, or a `body` that does not gunzip | the source entry's fields, as they were |
| `oversize` | a `body` over 1 MiB, or one that gunzips to over 16 MiB | the source entry's fields |

Every record also carries `error` (the handler's or decoder's message),
`reason`, `failed_at`, `deliveries`, `source_stream` and `source_id` (the
source entry's id).

**Never dead-lettered:**

- **A transient failure** (the database down, a pool timeout, a
  serialization failure, a deadlock, a lock or statement timeout). The
  entry stays pending and is retried forever, in order, with backoff;
  [DistantSignalIngestStreamBacklog](alerts.md#distantsignalingeststreambacklog)
  alerts on its age. An outage must not empty a stream into its
  dead-letter stream.
- **An unknown schema *version*, or envelope version `v`** (a producer
  newer than the writer). The
  entry stays pending and
  [DistantSignalIngestUnsupportedSchema](alerts.md#distantsignalingestunsupportedschema)
  fires. Roll the writer forward; do not `XACK` it by hand.

## Visibility

- Each dead letter logs a warning ("ingest entry dead-lettered") with the
  stream, id, reason and error, and increments
  `distant_signal_ingest_stream_dead_lettered_total{stream, reason}`
  (every reason registered at 0).
- The writer reads, every 30s, `distant_signal_ingest_stream_dlq_length{stream}`
  (`XLEN`) and `distant_signal_ingest_stream_dlq_oldest_age_seconds{stream}`
  (the oldest record's age, 0 when there is none).
- `distant_signal_ingest_stream_bytes{stream}` includes the dead-letter
  stream's memory.

## Retention

Each dead-letter stream is capped twice:

- **By age:** the writer trims it hourly with `XTRIM ds:dlq:<domain> MINID
  ~ <now - 7 days>` (spec §7.1; plan 3a.3).
  `DistantSignalIngestDeadLetterExpiring` fires when the oldest record is
  within 4 hours of that (`metrics.prometheusRule.ingestDeadLetterExpiring`).
- **By count:** `MAXLEN ~` the source stream's cap (720 for
  station-samples; `crates/ingest-stream/src/budget.rs`), applied on every
  dead-letter write. A flood of dead letters keeps one outage window's
  worth and drops the oldest.

A trimmed record is gone. For the snapshot streams that usually loses
nothing current: a newer snapshot has replaced it. Re-inject or discard
records before then.

## Inspecting

`<fullname>` is the chart's full name (`distant-signal` for a release named
`distant-signal`; see movement-events-deadletter.md). The Redis container's
`redis-cli` authenticates by itself when `redis.auth` is on.

```sh
REDIS="kubectl -n <namespace> exec deploy/<fullname>-redis -- redis-cli"
$REDIS XLEN ds:dlq:station-samples
$REDIS XRANGE ds:dlq:station-samples - + COUNT 5
```

The `body` is JSON, or gzipped JSON when `enc` is `json+gzip`. To read a
gzipped one, fetch the record with `redis-cli --raw` and pipe the `body`
through `gunzip`. The writer's warn log for the `source_id` has the
`error` in full.

## Fix the cause first

- `poison` / `rejected_rows` with a data error: a producer sends a value the
  table refuses (a check or foreign-key violation, an out-of-range value).
  Fix the producer or the handler, or decide the rows are wrong and delete
  the record. An unknown schema *name* means a producer writes a schema the
  writer never had: deploy the writer that has it.
- `undecodable` / `oversize`: a producer bug. Re-injecting the same bytes
  dead-letters them again; fix the producer, and let it send fresh data.
  These records are evidence, not something to replay.

## Re-injecting

Re-injection copies a record back onto its source stream as a new entry,
then deletes it from the dead-letter stream. Two rules (spec §7.3, §7.8):

- **It keeps the original `produced_at`.** `produced_at` is the entry's
  observed time (D13): the writer's ordering guard compares it, and the
  writer stores it as the rows' observed time and as the feed's freshness.
  A re-injected snapshot older than what the table already holds therefore
  changes nothing: newer data wins, as it should. Never re-stamp
  `produced_at` to "make it apply": that would overwrite newer rows with
  older data and mark the feed fresher than it is.
- **It gets a new `key`.** The writer records each applied `key` in
  `ingest_dedup` for 48 hours. A `rejected_rows` record's original key was
  applied (the rest of its entry committed), so reusing it would be skipped
  as a duplicate. The script appends `:reinjected:<dead-letter id>` to it.

The script drops the dead-letter fields (`error`, `reason`, `failed_at`,
`deliveries`, `source_stream`, `source_id`, `rejected_rows`) and keeps
every envelope field (`v`, `schema`, `producer`, `produced_at`, `enc`,
`batch`, `part`, `parts`, `body`). It runs atomically inside Redis. Save it
locally as `ingest-reinject.lua`:

```lua
-- KEYS[1] = dead-letter stream (ds:dlq:<domain>)
-- KEYS[2] = its source stream (ds:ingest:<domain>)
-- ARGV[1] = first id, ARGV[2] = last id (inclusive; "-" / "+" for all)
-- ARGV[3] = the source stream's MAXLEN (budget.rs: station-samples 720,
--           full-coverage 360, tfl 288, reference 30, island-of-ireland 2000)
-- ARGV[4] = reason filter ("*" for poison and rejected_rows)
-- Returns {reinjected, skipped_by_reason, skipped_other_source}.
local drop = {error = true, reason = true, failed_at = true, deliveries = true,
              source_stream = true, source_id = true, rejected_rows = true}
local reinjected, by_reason, other = 0, 0, 0
for _, e in ipairs(redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[2])) do
  local rec = {}
  for i = 1, #e[2], 2 do rec[e[2][i]] = e[2][i + 1] end
  local wanted = rec['reason'] == 'poison' or rec['reason'] == 'rejected_rows'
  if not wanted or (ARGV[4] ~= '*' and rec['reason'] ~= ARGV[4]) then
    by_reason = by_reason + 1
  elseif rec['source_stream'] ~= KEYS[2] or rec['key'] == nil then
    other = other + 1
  else
    local fields = {}
    for i = 1, #e[2], 2 do
      local k, v = e[2][i], e[2][i + 1]
      if k == 'key' then v = v .. ':reinjected:' .. e[1] end
      if not drop[k] then
        fields[#fields + 1] = k
        fields[#fields + 1] = v
      end
    end
    redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*', unpack(fields))
    redis.call('XDEL', KEYS[1], e[1])
    reinjected = reinjected + 1
  end
end
return {reinjected, by_reason, other}
```

It only replays `poison` and `rejected_rows` records; `undecodable` and
`oversize` ones are left in place (see above).

```sh
# Every poison and rejected_rows record of station-samples:
$REDIS EVAL "$(cat ingest-reinject.lua)" 2 ds:dlq:station-samples ds:ingest:station-samples - + 720 '*'

# One record, by its dead-letter id (first = last):
$REDIS EVAL "$(cat ingest-reinject.lua)" 2 ds:dlq:tfl ds:ingest:tfl 1791380000000-0 1791380000000-0 288 '*'
```

Then watch the writer's log and `ingest_stream_consumed_total{stream}`: a
re-injected entry is `applied` (or `skipped` in shadow mode). If it is
dead-lettered again, the cause is not fixed.

Re-inject soon after the fix. Re-injection is safe for every schema: the
snapshot handlers are idempotent upserts behind the ordering guard, and a
dead-lettered entry was either never applied (`poison`) or carries only
the rows that were refused (`rejected_rows`). The script deletes each
record in the same step as it copies it, so a second run cannot replay it;
do not copy records by hand as well, since the writer's dedup cannot catch
a second copy under a different key (`tocs/1` is not idempotent).

## Deleting

After you have recovered or deliberately discarded a record:

```sh
$REDIS XDEL ds:dlq:<domain> <id> [<id> ...]
```
