# Ingest architecture, phase 0: what Ranma rolls out, in order

Phase 0 of [the ingest architecture plan](superpowers/plans/2026-10-06-ingest-architecture-plan.md)
(spec: [design](superpowers/specs/2026-10-06-ingest-architecture-design.md)).
Everything here is a Ranma-Config values change; the chart ships every
switch **off**, and with them off it renders exactly what it did before.
Agents only read production.

| Step | What | Runbook | Restarts | Rollback |
|---|---|---|---|---|
| 0a | The Postgres role split, Stages A–C (in progress) | [postgres-app-role.md](postgres-app-role.md), "Rollout runbook" | Stage B: api, aggregator, enricher, notifier | its "Rollback" section |
| 0b | One Postgres role per service (`postgresql.roles.perService`), then 7 days of `pg_stat_statements` and the `observe-role-usage.py` report | [postgres-app-role.md](postgres-app-role.md#stage-0b-one-role-per-service-ingest-architecture-phase-0b), "Stage 0b" | one service per release | `perService.<service>.connect: false` |
| 0c | Redis ACL users (`redis.acl`), four releases | [redis-acl.md](redis-acl.md) | Redis (steps 1, 3, 4), one client per release (step 2) | the previous step's values |
| 0d | Narrow `allow-ingress-same-namespace` (Ranma's NetworkPolicy) | below | none | restore the old policy |
| prep for 1B | Raise the HelmRelease `timeout` to 20 minutes | below | none | the old timeout |

0b needs 0a's Stage B (the services on `distant_signal_app`). 0c is
independent of 0a/0b and can run in parallel.

## Prep for phase 1B: the HelmRelease timeout (decision D2)

The migrations move into a Helm `pre-upgrade,post-install` hook Job in
phase 1B (it needs the `ds-migrate` binary from 1B.1, so the Job, the
schema gate and `api.migrateOnStartup` ship then, not in phase 0). A hook
runs inside the release's timeout, and the Job's own deadline is 15
minutes, so the HelmRelease must allow more than that. This can be done any
time; it changes nothing else:

```yaml
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: distant-signal, namespace: distant-signal}
spec:
  timeout: 20m
```

Verify: `kubectl -n distant-signal get helmrelease distant-signal
-o jsonpath='{.spec.timeout}'` prints `20m`, and the next reconcile is
Ready. Rollback: the previous value (the default is 5m).

## 0d: NetworkPolicy narrowing (DS checks)

Ranma's `allow-ingress-same-namespace` (`podSelector: {}`) admits every
pod in the namespace to every other, so the chart's own policies
(`networkPolicy.enabled`) do not restrict anything yet. Before Ranma
narrows it, DS checks that every real flow into api, postgres and redis is
in the chart's lists (from Loki, or a short flow log), and fixes the chart
first where one is not. Rollback: restore the old policy.

## Phase 0 exit

- every DB service connects as its own role (`pg_stat_activity.usename`),
  none as `distant_signal`;
- the 7-day role-usage report exists, and its differences from
  `db-grants.yaml` are explained or fixed;
- every Redis connection has its own user, `default` is off, `ACL LOG` is
  empty for 7 days;
- `gen-db-grants.py check` is green in CI;
- the same-namespace allow is gone or narrowed.
