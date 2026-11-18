# `trust-backlog-consumer`: which Movements are stored

`trust-backlog-consumer` keeps a short backlog of TRUST events in
`trust_event_backlog`, so a train that is tracked late can be matched against
what it already did. It reads the `movement-events` stream as its own
consumer group and writes Postgres directly.

## What is stored

- Activation (`0001`) and Cancellation (`0002`): always.
- Movement (`0003`): only `ARRIVAL` and `DEPARTURE`, never `PASS`, and only
  at a station that passes the scope below.
- ChangeOfOrigin, ChangeOfIdentity and unknown types: never.
- A Movement whose STANOX has no CRS in `stanox_to_crs` is dropped in every
  scope.

## Scope: `TRUST_BACKLOG_ALL_STATIONS`

By default (unset or `false`) a Movement is stored only at a CRS in the
line catalogue's station index. That was Decision 2 of the 2026-09-05 trust
event backlog design, a scope and storage-ceiling choice rather than a
correctness one.

With `TRUST_BACKLOG_ALL_STATIONS=true` a Movement is stored at any station
whose STANOX resolves through `stanox_to_crs`. Only the CRS check changes:
the event-type filter, the unmapped-STANOX drop, retention and the metrics
are the same. The consumer logs the active scope once at startup
(`TRUST backlog Movement scope`).

Measured extra volume: about 7% more rows, roughly 40k rows a day, or
0.25 to 0.4 GB at the current retention.

## Trial and rollback

- Trial: set `TRUST_BACKLOG_ALL_STATIONS=true` in the trust-backlog-consumer
  environment through Ranma-Config and let Flux roll the pod. No migration
  or grant change is needed. A named chart value is a later ds-deploy
  change.
- Rollback: unset it (or set it to `false`) and restart. Nothing is deleted;
  the extra rows age out with `TRUST_EVENT_BACKLOG_RETENTION_DAYS`.
- Watch `trust_event_backlog` row growth and the table size over the trial.
