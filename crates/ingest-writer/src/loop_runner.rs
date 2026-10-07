//! Named background loops, each guarded by a session advisory lock (spec
//! §10 and §12.3, plan 1B.6).
//!
//! Every [`LoopSpec`] runs on its own interval. Before each tick the loop
//! asks the runner's [`LockSession`] for `pg_try_advisory_lock(key)`:
//!
//! - **held** (taken now, or still held from an earlier tick): the body
//!   runs;
//! - **held by another session** (a second writer replica, or the api's own
//!   loops during the 1B cutover): the tick is skipped and logged at debug;
//! - **error** (the lock connection is gone and could not be reopened): the
//!   tick is skipped and logged at warn.
//!
//! # Holding, not taking per tick
//!
//! A lock, once taken, is held for the life of the [`LockSession`]'s
//! connection; it is never released between ticks. So one runner owns a
//! loop until its process exits (the connection closes and Postgres
//! releases the lock), and a standby takes it over on its next tick. That
//! is what lets the writer and the api overlap safely during the cutover
//! (spec §12.3, "turn the writer on before the api off").
//!
//! # One connection for every lock
//!
//! The session is ONE dedicated connection, opened from the pool's own
//! connect options (same `application_name` and timeouts) but outside the
//! pool, for all of the runner's locks. So the runner costs one connection
//! beyond the pool's `max_connections` however many loops it runs (spec
//! §6.6: the writer's pool is 6, its role limit 7). The bodies use the
//! pool. Postgres advisory locks are re-entrant per session, so the
//! session remembers which keys it holds rather than asking again: two
//! runners in one process must each have their own session.
//!
//! Before a tick of a lock it already holds, the session pings its
//! connection. If the connection has died, Postgres has released every
//! lock on it, so the session reconnects and tries again at once. A
//! connection dying in the middle of a body can let another runner start
//! the same sweep before this body finishes; every sweep is idempotent, so
//! that window costs at most one duplicate pass.
//!
//! # Metrics
//!
//! With `service` the runner's metric prefix (`ingest_writer`):
//!
//! - `distant_signal_<service>_cycles_total{cycle, result}` and
//!   `distant_signal_<service>_last_success_timestamp_seconds{cycle}`
//!   ([`common::metrics::register_cycle`]): the body's outcomes;
//! - `distant_signal_<service>_loop_ticks_total{loop, outcome}`, outcome
//!   `ran`, `failed`, `skipped` (lock held elsewhere) or `lock_error`;
//! - `distant_signal_<service>_loop_lock_held{loop}`: 1 while this process
//!   holds the loop's lock;
//! - `distant_signal_<service>_loop_seconds{loop}`: body duration.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use common::advisory_locks::LoopLock;
use common::metrics::metric_name;
use common::progress::Progress;
use sqlx::{ConnectOptions, Connection, PgConnection, PgPool};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

/// A loop body's future.
pub type LoopFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// A loop body: called once per tick it holds the lock for, with the pool.
pub type LoopBody = Arc<dyn Fn(PgPool) -> LoopFuture + Send + Sync>;

/// One named loop: its lock, its interval and its body.
#[derive(Clone)]
pub struct LoopSpec {
    pub lock: LoopLock,
    pub interval: Duration,
    body: LoopBody,
}

impl LoopSpec {
    pub fn new<F, Fut>(lock: LoopLock, interval: Duration, body: F) -> Self
    where
        F: Fn(PgPool) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            lock,
            interval,
            body: Arc::new(move |pool| Box::pin(body(pool))),
        }
    }

    pub fn name(&self) -> &'static str {
        self.lock.name
    }
}

impl fmt::Debug for LoopSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoopSpec")
            .field("lock", &self.lock)
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

/// What one tick did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickOutcome {
    /// Held the lock; the body succeeded.
    Ran,
    /// Held the lock; the body returned an error (logged).
    Failed,
    /// Another session holds the lock; the body did not run.
    Skipped,
    /// The lock session could not be (re)opened; the body did not run.
    LockError,
}

impl TickOutcome {
    const ALL: [Self; 4] = [Self::Ran, Self::Failed, Self::Skipped, Self::LockError];

    fn label(self) -> &'static str {
        match self {
            Self::Ran => "ran",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::LockError => "lock_error",
        }
    }
}

/// The dedicated connection holding a runner's loop locks. See the module
/// docs.
pub struct LockSession {
    pool: PgPool,
    application_name: String,
    state: Mutex<SessionState>,
}

#[derive(Default)]
struct SessionState {
    conn: Option<PgConnection>,
    held: HashSet<i64>,
}

impl SessionState {
    /// Forgets the connection and every lock on it. Dropping a
    /// `PgConnection` closes its socket, so Postgres releases the locks.
    fn reset(&mut self) {
        self.conn = None;
        self.held.clear();
    }
}

impl LockSession {
    /// Connections are opened from `pool`'s connect options, reported in
    /// `pg_stat_activity` as `application_name`.
    pub fn new(pool: PgPool, application_name: &str) -> Self {
        Self {
            pool,
            application_name: application_name.to_owned(),
            state: Mutex::new(SessionState::default()),
        }
    }

    /// Whether this session holds `key` now: already held (and the
    /// connection still answers), or newly taken with
    /// `pg_try_advisory_lock`. `Ok(false)` means another session holds it.
    pub async fn try_hold(&self, key: i64) -> Result<bool> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;
        if state.held.contains(&key) {
            let alive = match state.conn.as_mut() {
                Some(conn) => conn.ping().await.is_ok(),
                None => false,
            };
            if alive {
                return Ok(true);
            }
            tracing::warn!(
                key,
                "the advisory-lock connection was lost, and every lock with it; reconnecting"
            );
            state.reset();
        }
        let result = self.take(state, key).await;
        if result.is_err() {
            state.reset();
        }
        result
    }

    async fn take(&self, state: &mut SessionState, key: i64) -> Result<bool> {
        if state.conn.is_none() {
            let options = (*self.pool.connect_options())
                .clone()
                .application_name(&self.application_name);
            let conn = options
                .connect()
                .await
                .context("could not open the advisory-lock connection")?;
            state.held.clear();
            state.conn = Some(conn);
        }
        let Some(conn) = state.conn.as_mut() else {
            anyhow::bail!("the advisory-lock connection was just opened");
        };
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&mut *conn)
            .await
            .context("pg_try_advisory_lock failed")?;
        if taken {
            state.held.insert(key);
        }
        Ok(taken)
    }

    /// Whether this session currently believes it holds `key`.
    pub async fn holds(&self, key: i64) -> bool {
        self.state.lock().await.held.contains(&key)
    }

    /// Releases every lock and closes the connection (graceful shutdown).
    /// The explicit `pg_advisory_unlock_all` makes the release synchronous:
    /// a closed socket alone frees the locks only once the server backend
    /// has noticed and exited, a moment later.
    pub async fn close(&self) {
        let mut state = self.state.lock().await;
        if let Some(mut conn) = state.conn.take() {
            if let Err(err) = sqlx::query("SELECT pg_advisory_unlock_all()")
                .execute(&mut conn)
                .await
            {
                tracing::debug!(error = ?err, "releasing the loop locks failed; closing anyway");
            }
            if let Err(err) = conn.close().await {
                tracing::debug!(error = ?err, "closing the advisory-lock connection failed");
            }
        }
        state.held.clear();
    }
}

/// A set of loops sharing one [`LockSession`].
pub struct LoopRunner {
    service: &'static str,
    pool: PgPool,
    session: Arc<LockSession>,
    loops: Vec<LoopSpec>,
}

impl LoopRunner {
    /// `service` is the metric prefix (`ingest_writer`).
    pub fn new(service: &'static str, pool: PgPool, session: LockSession) -> Self {
        Self {
            service,
            pool,
            session: Arc::new(session),
            loops: Vec::new(),
        }
    }

    /// Adds a loop and registers its metrics at 0. Refuses a zero interval
    /// (`tokio::time::interval` panics on one) and a second loop with the
    /// same name or key.
    pub fn register(&mut self, spec: LoopSpec) -> Result<()> {
        anyhow::ensure!(
            !spec.interval.is_zero(),
            "loop {:?} has a zero interval",
            spec.name()
        );
        if let Some(existing) = self
            .loops
            .iter()
            .find(|l| l.name() == spec.name() || l.lock.key == spec.lock.key)
        {
            anyhow::bail!(
                "loop {:?} clashes with the registered loop {:?} (same name or lock key)",
                spec.name(),
                existing.name()
            );
        }
        common::metrics::register_cycle(self.service, spec.name());
        for outcome in TickOutcome::ALL {
            metrics::counter!(
                metric_name(&format!("{}_loop_ticks_total", self.service)),
                "loop" => spec.name(),
                "outcome" => outcome.label()
            )
            .increment(0);
        }
        metrics::gauge!(
            metric_name(&format!("{}_loop_lock_held", self.service)),
            "loop" => spec.name()
        )
        .set(0.0);
        self.loops.push(spec);
        Ok(())
    }

    pub fn loops(&self) -> &[LoopSpec] {
        &self.loops
    }

    pub fn session(&self) -> &Arc<LockSession> {
        &self.session
    }

    /// One tick of `self.loops()[index]`, now: try the lock, then run the
    /// body if this session holds it.
    pub async fn tick(&self, index: usize) -> TickOutcome {
        tick(self.service, &self.pool, &self.session, &self.loops[index]).await
    }

    /// Spawns every loop on its own task. Each gets its own [`Progress`]
    /// (stalled once one body runs past `stall_after`); the caller polls
    /// [`RunningLoops::stalled`] to drive its health endpoint.
    pub fn spawn(self, stall_after: Duration) -> RunningLoops {
        let mut tasks = JoinSet::new();
        let mut progress = Vec::with_capacity(self.loops.len());
        for spec in self.loops {
            let loop_progress = Progress::new(stall_after);
            progress.push((spec.name(), loop_progress.clone()));
            tasks.spawn(run_loop(
                self.service,
                self.pool.clone(),
                Arc::clone(&self.session),
                spec,
                loop_progress,
            ));
        }
        RunningLoops {
            tasks,
            progress,
            session: self.session,
        }
    }
}

/// The spawned loops of a [`LoopRunner`].
pub struct RunningLoops {
    tasks: JoinSet<()>,
    progress: Vec<(&'static str, Progress)>,
    session: Arc<LockSession>,
}

impl RunningLoops {
    /// The loops whose current body has run past the stall budget, or whose
    /// task has died (it stops beating, so it goes stalled too).
    pub fn stalled(&self) -> Vec<&'static str> {
        self.progress
            .iter()
            .filter(|(_, progress)| progress.is_stalled())
            .map(|(name, _)| *name)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.progress.len()
    }

    pub fn is_empty(&self) -> bool {
        self.progress.is_empty()
    }

    /// Stops every loop (a body in flight is dropped mid-await; each is
    /// idempotent and transactional) and closes the lock session, so a
    /// standby can take the locks on its next tick.
    pub async fn shutdown(mut self) {
        self.tasks.shutdown().await;
        self.session.close().await;
    }
}

async fn run_loop(
    service: &'static str,
    pool: PgPool,
    session: Arc<LockSession>,
    spec: LoopSpec,
    progress: Progress,
) {
    let mut interval = tokio::time::interval(spec.interval);
    // A slow tick delays the next one rather than bursting the missed ones
    // (the api's `sweep_interval` does the same).
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        progress.idle(interval.tick()).await;
        tick(service, &pool, &session, &spec).await;
        progress.beat();
    }
}

async fn tick(
    service: &'static str,
    pool: &PgPool,
    session: &LockSession,
    spec: &LoopSpec,
) -> TickOutcome {
    let name = spec.name();
    let outcome = match session.try_hold(spec.lock.key).await {
        Ok(true) => {
            set_lock_held(service, name, true);
            let started = tokio::time::Instant::now();
            let result = (spec.body)(pool.clone()).await;
            metrics::histogram!(
                metric_name(&format!("{service}_loop_seconds")),
                "loop" => name
            )
            .record(started.elapsed().as_secs_f64());
            common::metrics::record_cycle(service, name, result.is_ok());
            match result {
                Ok(()) => TickOutcome::Ran,
                Err(err) => {
                    tracing::error!(
                        loop_name = name,
                        error = ?err,
                        "background loop failed; will retry next interval"
                    );
                    TickOutcome::Failed
                }
            }
        }
        Ok(false) => {
            set_lock_held(service, name, false);
            tracing::debug!(
                loop_name = name,
                key = spec.lock.key,
                "advisory lock held by another session; skipping this tick"
            );
            TickOutcome::Skipped
        }
        Err(err) => {
            set_lock_held(service, name, false);
            tracing::warn!(
                loop_name = name,
                error = ?err,
                "could not check the loop's advisory lock; skipping this tick"
            );
            TickOutcome::LockError
        }
    };
    metrics::counter!(
        metric_name(&format!("{service}_loop_ticks_total")),
        "loop" => name,
        "outcome" => outcome.label()
    )
    .increment(1);
    outcome
}

fn set_lock_held(service: &str, name: &'static str, held: bool) {
    metrics::gauge!(
        metric_name(&format!("{service}_loop_lock_held")),
        "loop" => name
    )
    .set(if held { 1.0 } else { 0.0 });
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCK: LoopLock = LoopLock {
        name: "test",
        key: 1,
    };

    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@localhost/none")
            .unwrap()
    }

    fn noop(lock: LoopLock, interval: Duration) -> LoopSpec {
        LoopSpec::new(lock, interval, |_pool| async { Ok(()) })
    }

    #[tokio::test]
    async fn register_refuses_a_zero_interval_and_clashes() {
        let pool = lazy_pool();
        let mut runner = LoopRunner::new(
            "ingest_writer_test",
            pool.clone(),
            LockSession::new(pool, "t"),
        );
        assert!(runner.register(noop(LOCK, Duration::ZERO)).is_err());
        runner.register(noop(LOCK, Duration::from_secs(1))).unwrap();
        let same_key = LoopLock {
            name: "other",
            key: LOCK.key,
        };
        let same_name = LoopLock {
            name: LOCK.name,
            key: 2,
        };
        assert!(
            runner
                .register(noop(same_key, Duration::from_secs(1)))
                .is_err()
        );
        assert!(
            runner
                .register(noop(same_name, Duration::from_secs(1)))
                .is_err()
        );
        assert_eq!(runner.loops().len(), 1);
    }

    #[tokio::test]
    async fn an_unreachable_database_is_a_lock_error_and_the_body_does_not_run() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            // Port 1 on localhost: refused at once.
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .unwrap();
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let mut runner = LoopRunner::new(
            "ingest_writer_test",
            pool.clone(),
            LockSession::new(pool, "t"),
        );
        runner
            .register(LoopSpec::new(LOCK, Duration::from_secs(1), move |_pool| {
                let flag = Arc::clone(&flag);
                async move {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            }))
            .unwrap();
        assert_eq!(runner.tick(0).await, TickOutcome::LockError);
        assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!runner.session().holds(LOCK.key).await);
    }

    #[test]
    fn spawning_no_loops_reports_nothing_stalled() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = lazy_pool();
            let runner = LoopRunner::new(
                "ingest_writer_test",
                pool.clone(),
                LockSession::new(pool, "t"),
            );
            let running = runner.spawn(Duration::from_secs(60));
            assert!(running.is_empty());
            assert!(running.stalled().is_empty());
            running.shutdown().await;
        });
    }
}
