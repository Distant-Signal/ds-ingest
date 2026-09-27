# `movement-events-deadletter`: poison entries and how to recover them

The three `movement-events` consumer groups (`trust-consumer`,
`full-coverage-consumer`, `trust-event-backlog`) set aside records that can
never succeed as they stand, instead of retrying them forever. They go to one
shared Redis stream, `movement-events-deadletter`, next to `movement-events`.

Code: `crates/movement-feed/src/redis_stream.rs` (`reject_batch`,
`dead_letter`), and each consumer's `main.rs`.

## What is dead-lettered, and what never is

Only records with specific evidence that they are poison are dead-lettered:

| `reason` | When | `source_id` | `payload` |
| --- | --- | --- | --- |
| `rejected_by_api` (whole entry) | `api` answered 400, 413 or 422 for a batch, and, once the batch was narrowed down one entry at a time, for that single entry | the `movement-events` entry id | the entry's `payload`, unchanged |
| `rejected_by_api` (row) | `trust-backlog-consumer` only: `api` answered 2xx but listed the row in its per-row `rejected` list | empty | the rejected row, as `TrustBacklogEventMessage` JSON |
| `malformed_entry` | the stream entry has no usable `payload` field | the entry id | its fields, as `name=value` lines |
| `unparseable_payload` | the payload is not a TRUST envelope the consumer can parse | empty | the raw payload |

**A transient failure is never dead-lettered, however long it lasts.** If
`api` is unreachable, times out (the consumers' HTTP client has a 5s connect
and 60s request timeout), or answers 5xx, 401/403, 404, 408 or 429, the entry
stays pending in its group and is retried until it succeeds. A delivery count
is no longer a reason to dead-letter: an entry that has been redelivered more
than 240 times (about an hour of failure) is only reported, as a warn log and
`distant_signal_movement_feed_long_pending_total{group}`.

When `api` rejects a batch of several entries, the consumer cannot tell which
entry was bad. It then re-reads its pending entries one at a time
("isolation"). Healthy entries are committed on their own, and only the entry
that is rejected alone is dead-lettered.

## Visibility

- Every dead-lettered record logs a warning ("dead-lettered a poison record
  ...") with its group, reason, detail and payload, and increments
  `distant_signal_movement_feed_deadlettered_total{group, reason}`.
- `distant_signal_movement_feed_deadletter_length{stream}` is the stream's
  length after the last write.
- `trust-backlog-consumer` also keeps its own
  `distant_signal_trust_backlog_consumer_deadlettered_total{reason}`.

Alert on any increase of `deadlettered_total`. It should normally stay at 0.

## Capacity: never trimmed

The stream is capped at 10,000 records (well under 1KB each, so about 10MB)
but is **never trimmed**. Trimming would silently lose the oldest poison
record. When a write would go over the cap, it is refused instead: an error
log ("dead-letter stream is full ..."), and
`distant_signal_movement_feed_deadletter_full_total{group}` goes up. The
affected entry stays pending in its group, so it is delayed but not lost.
To free space, re-inject or delete records (see below).

## Inspecting

```sh
REDIS="kubectl -n <namespace> exec deploy/<release>-redis -- redis-cli"
$REDIS XLEN movement-events-deadletter
$REDIS XRANGE movement-events-deadletter - + COUNT 20
```

Each record has the fields `group`, `consumer`, `reason`, `source_id`,
`delivery_count`, `detail` (the `api` status and body, or the parse error)
and `payload`.

## Re-injecting whole entries

Fix the cause first (deploy the `api` or consumer fix). Then use this Lua
script, which runs atomically inside Redis, so there are no shell quoting
issues with JSON payloads. Save it locally as `reinject.lua`:

```lua
-- KEYS[1] = dead-letter stream, KEYS[2] = target stream
-- ARGV[1] = first id, ARGV[2] = last id (inclusive; "-" / "+" for all)
-- ARGV[3] = group filter ("*" for any), ARGV[4] = reason filter ("*" for any)
-- Returns {reinjected, skipped_row_level, skipped_by_filter}.
local reinjected, row_level, filtered = 0, 0, 0
for _, e in ipairs(redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[2])) do
  local rec = {}
  for i = 1, #e[2], 2 do rec[e[2][i]] = e[2][i + 1] end
  if (ARGV[3] ~= '*' and rec['group'] ~= ARGV[3]) or (ARGV[4] ~= '*' and rec['reason'] ~= ARGV[4]) then
    filtered = filtered + 1
  elseif rec['source_id'] == nil or rec['source_id'] == '' or rec['reason'] == 'malformed_entry' then
    row_level = row_level + 1
  else
    redis.call('XADD', KEYS[2], '*', 'payload', rec['payload'], 'msg_type', 'reinjected')
    redis.call('XDEL', KEYS[1], e[1])
    reinjected = reinjected + 1
  end
end
return {reinjected, row_level, filtered}
```

It copies each matching record's `payload` back onto `movement-events` as a
new entry, then `XDEL`s the record from the dead-letter stream. It skips
records that are not whole stream entries: row-level `rejected_by_api` with
an empty `source_id`, `unparseable_payload`, and `malformed_entry`. It returns
`{reinjected, skipped_row_level, skipped_by_filter}`.

```sh
# Everything from trust-event-backlog, any reason:
$REDIS EVAL "$(cat reinject.lua)" 2 movement-events-deadletter movement-events - + trust-event-backlog '*'

# One record, by its dead-letter id (first = last):
$REDIS EVAL "$(cat reinject.lua)" 2 movement-events-deadletter movement-events 1790472442957-0 1790472442957-0 '*' '*'
```

**A re-injected entry is delivered to all three groups**, not only the one
that dead-lettered it: a stream entry cannot target one group. That is safe
for `trust-consumer` and `trust-backlog-consumer`, whose writes are
idempotent on `dedup_key`. `full-coverage-consumer` re-applies the event to
its in-memory correlation state, where last-write-wins means an old movement
can briefly overwrite a newer one for that train. Prefer re-injecting soon
after the fix, and treat that rail day's full-coverage shadow stats as
approximate if you re-inject many entries late.

## Re-posting row-level `trust-event-backlog` rejections

A row-level `rejected_by_api` record (empty `source_id`) holds one
already-processed `trust_event_backlog` row. Send it straight to `api` as a
one-element JSON array: `POST /private/trust-event-backlog` with a bearer
token for the `svc-trust-backlog-consumer` identity. Then `XDEL` the record.

## Deleting

After you have recovered or deliberately discarded a record:

```sh
$REDIS XDEL movement-events-deadletter <id> [<id> ...]
```
