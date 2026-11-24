# Island-of-Ireland board history retention

`ingest-writer` records the Iarnrod Eireann boards by train
(`island_of_ireland_trains`, `island_of_ireland_train_calls`) and prunes them
at the end of every `ioi-station-samples/1` apply, at most 2,000 rows per
table per entry. A row's `train_date` decides how long it stays:

| Train | Kept until | Setting (default) |
|---|---|---|
| Not tracked | `train_date` older than the window | `IOI_TRAIN_HISTORY_RETENTION_DAYS` (14) |
| Tracked (an `ie:` `trains` row with a subscription) | `train_date` older than the window | `IOI_TRACKED_TRAIN_HISTORY_RETENTION_DAYS` (30) |
| Tracked and bound to a journey leg | the leg is deleted, but never past the hard bound | `IOI_TRAINS_SNAPSHOT_GUARD_MAX_DAYS` (600) |

A leg is bound when `journey_legs.train_subscription_id` points at the train's
subscription (`train_subscriptions.trains_id`). This is the same rule, and the
same variable name and default, as ds-workers' aggregator holds a GB `trains`
row by, so a journey keeps an Irish train's history for as long as it keeps a
GB train's. The api deletes a leg at its 548-day journey retention, which
releases the train; 600 days is the bound for a retention that stopped
running. `IOI_TRAINS_SNAPSHOT_GUARD_MAX_DAYS=0` turns the hold off (a tracked
train then goes at the tracked window). A value under the tracked window is
treated as that window.

The writer's role needs SELECT on `journey_legs (train_subscription_id)` and
`train_subscriptions (id, trains_id)` (ds-core v0.17.6 or later; the writer
already has both).

No licence ceiling for these tables is written in this repository or in
ds-core: Iarnrod Eireann publishes no terms for the feed (the Irish rail
train-tracking design, section 7), so the 600-day bound is the only cap. The
300-day cap that applies to LDBWS-derived rows does not apply here.
