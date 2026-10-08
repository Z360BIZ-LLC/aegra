# Enqueue Multitask Strategy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make omitted and explicit `enqueue` stateful runs execute one at a
time per thread in durable FIFO order, without losing state and without
breaking org-level concurrency limits, worker recovery, or either executor
mode.

**Architecture:** PostgreSQL is the queue authority. A durable queue position
and effective strategy are stored on every run. One shared admission service
serializes `pending → running` under thread-then-org advisory locks; both local
and Redis executors use it. A generalized promoter dispatches eligible pending
runs, while leases and periodic scans provide recovery.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy async, PostgreSQL
advisory locks, Alembic, asyncio, Redis lists, pytest, Ruff, ty.

**Spec:**
[`docs/superpowers/specs/2026-10-07-enqueue-multitask-strategy-design.md`](../specs/2026-10-07-enqueue-multitask-strategy-design.md)

## Global Constraints

- Work only in the Aegra submodule on `codex/feat-multitask-enqueue`; do not
  modify parent `agent-layer` files except for the eventual submodule pointer.
- Follow `AGENTS.md` and `CLAUDE.md`: complete types, top-level imports unless a
  documented cycle requires otherwise, 88-character Python formatting, and no
  broad exception swallowing.
- Preserve `reject`, `interrupt`, and `rollback` behavior as-is. Only omitted
  and explicit `enqueue` gain thread serialization in this change.
- Acquire advisory locks in exactly one order: thread first, organization
  second. Never add a path that reverses this order.
- Keep PostgreSQL authoritative. Redis entries and in-process wakeups are
  delivery hints and may be duplicated or lost without changing correctness.
- Do not mutate queued input, command, or explicit checkpoint. Scheduling may
  delay execution but must not reinterpret the request.
- Make every schema change through one linear, reversible Alembic migration
  from revision `a3f7c1d9e2b4`.
- Bump `aegra-api` and `aegra-cli` together from `0.10.6` to `0.10.7` only
  after behavior and documentation are complete.
- Use test-first steps and keep each task's commit focused and conventional.
- Before implementation, restore a valid Aegra uv workspace containing
  `packages/common` (or run in a standalone checkout that has it). The current
  embedded checkout cannot resolve `agent-layer-common`; do not “fix” this
  feature by committing a dependency-layout workaround.

## Review Focus

1. **Creation/dispatch inversion:** a later request can be delivered before an
   earlier commit. Task 4 pins this with a concurrent creation-order test; Task
   9 proves it end-to-end.
2. **Thread/org deadlock or quota overflow:** all claims must lock thread then
   org and recheck capacity. Task 3 pins lock order; Task 9 covers simultaneous
   claims and composed queueing against real PostgreSQL.
3. **Crash wedges successors:** a crashed predecessor must retain precedence
   while retrying and release it when retries exhaust. Task 7 pins both cases.
4. **Wrong queue expiry:** time spent behind a thread predecessor must never be
   treated as org-limit queue time. Tasks 3 and 6 pin `pending_reason`
   transitions and expiry filtering.
5. **Duplicate/lost delivery:** multiple promoters, Redis duplicates, and a
   commit-before-enqueue crash must not double-run or lose work. Tasks 5–7 pin
   guarded claims, periodic recovery, and fallback polling.

## Task 1: Tighten and Default the Public Strategy Contract

**Files:**

- Modify: `libs/aegra-api/src/aegra_api/models/runs.py`
- Modify: `libs/aegra-api/src/aegra_api/models/run_job.py`
- Modify: `libs/aegra-api/tests/unit/test_models/test_runcreate_validation.py`
- Modify: `libs/aegra-api/tests/unit/test_services/test_run_job.py`

- [ ] Add failing model tests proving the four protocol values validate,
  arbitrary strings fail, the request field remains wire-optional, and
  `RunBehavior()` defaults to `enqueue`. Add a legacy execution-params case
  where the stored JSON contains `null` and reconstruction resolves it from
  the ORM column or falls back to `enqueue`.

- [ ] Run the focused tests and confirm the new assertions fail:

  ```bash
  uv run --package aegra-api pytest \
    libs/aegra-api/tests/unit/test_models/test_runcreate_validation.py \
    libs/aegra-api/tests/unit/test_services/test_run_job.py -q
  ```

- [ ] Define one exported strategy type, for example
  `MultitaskStrategy = Literal["reject", "interrupt", "rollback", "enqueue"]`,
  use it in `RunCreate` and `RunBehavior`, and set only `RunBehavior`'s default
  to `enqueue`. Normalize legacy missing/`null` behavior during
  `RunJob.from_run_orm()`.

- [ ] Add `multitask_strategy: MultitaskStrategy` to the `Run` response model so
  API responses expose the effective persisted value, with an `enqueue`
  default for compatibility with legacy rows and existing test doubles.

- [ ] Run the focused tests and confirm they pass.

- [ ] Commit the contract change:

  ```bash
  git add libs/aegra-api/src/aegra_api/models \
    libs/aegra-api/tests/unit/test_models/test_runcreate_validation.py \
    libs/aegra-api/tests/unit/test_services/test_run_job.py
  git commit -m "feat(api): default multitask strategy to enqueue"
  ```

## Task 2: Add Durable Run-Admission Columns

**Files:**

- Create: `libs/aegra-api/alembic/versions/20261007000000_add_run_admission_fields.py`
- Modify: `libs/aegra-api/src/aegra_api/core/orm.py`
- Modify: `libs/aegra-api/tests/unit/test_core/test_migration_chain.py`
- Modify: `libs/aegra-api/tests/unit/test_migrations.py`

- [ ] Add failing tests that require one Alembic head, the new revision's
  correct `down_revision`, deterministic backfill, sequence ownership/default,
  strategy and pending-reason checks, `pending_reason_at`, active-thread
  partial index, and complete downgrade cleanup.

- [ ] Run the migration tests and confirm failure:

  ```bash
  uv run --package aegra-api pytest \
    libs/aegra-api/tests/unit/test_core/test_migration_chain.py \
    libs/aegra-api/tests/unit/test_migrations.py -q
  ```

- [ ] Implement the migration in resumable phases: add nullable columns and
  sequence, backfill `queue_position` ordered by `(created_at, run_id)`, advance
  the sequence, apply defaults/nullability/checks, then create
  `idx_runs_thread_active_queue` on `(thread_id, queue_position)` where status
  is `pending` or `running`.

- [ ] Map `multitask_strategy`, `queue_position`, `pending_reason`, and
  `pending_reason_at` in `RunORM`, including complete `Mapped[...]` types and
  matching index metadata.

- [ ] Run migration tests, then exercise both directions against a disposable
  PostgreSQL database:

  ```bash
  uv run --package aegra-api alembic -c libs/aegra-api/alembic.ini upgrade head
  uv run --package aegra-api alembic -c libs/aegra-api/alembic.ini downgrade -1
  uv run --package aegra-api alembic -c libs/aegra-api/alembic.ini upgrade head
  ```

- [ ] Commit the schema change:

  ```bash
  git add libs/aegra-api/alembic/versions/20261007000000_add_run_admission_fields.py \
    libs/aegra-api/src/aegra_api/core/orm.py \
    libs/aegra-api/tests/unit/test_core/test_migration_chain.py \
    libs/aegra-api/tests/unit/test_migrations.py
  git commit -m "feat(api): persist run admission order"
  ```

## Task 3: Build the Unified Admission Gate

**Files:**

- Create: `libs/aegra-api/src/aegra_api/services/run_admission.py`
- Create: `libs/aegra-api/tests/unit/test_services/test_run_admission.py`
- Modify: `libs/aegra-api/src/aegra_api/services/run_limits.py`
- Modify: `libs/aegra-api/tests/unit/test_services/test_run_limits.py`

**Required interface:**

```python
class AdmissionOutcome(enum.Enum):
    CLAIMED = "claimed"
    ALREADY_TAKEN = "already_taken"
    THREAD_BLOCKED = "thread_blocked"
    ORG_AT_CAPACITY = "org_at_capacity"


async def lock_thread(session: AsyncSession, thread_id: str) -> None: ...


async def try_start_run(
    session: AsyncSession,
    run_id: str,
    *,
    claimed_by: str | None = None,
    lease_expires_at: datetime | None = None,
) -> AdmissionOutcome: ...
```

- [ ] Write failing admission tests for: missing/non-pending rows; enqueue with
  no predecessor; enqueue behind older pending and running rows; terminal older
  rows; a non-enqueue incoming run; org free/full/shadow/off modes; exact
  thread-then-org lock order; conditional-claim race loss; and clearing or
  changing `pending_reason`.

- [ ] Run the new tests and confirm they fail because the service is absent.

- [ ] Implement a dependency-light admission service that reads the candidate,
  acquires the thread advisory lock, checks for any older active row when the
  incoming strategy is `enqueue`, then acquires/evaluates the org lock before
  conditionally claiming the row.

- [ ] Move only claim orchestration out of `run_limits.py`; retain org ID
  resolution, limit settings, locking, active counts, and capacity evaluation
  there.

- [ ] Ensure blocked outcomes update `pending_reason` without committing and
  set `pending_reason_at` only when the reason changes. A successful claim
  clears both fields; callers own commit/rollback.

- [ ] Run admission and run-limit tests and confirm all pass.

- [ ] Commit the gate:

  ```bash
  git add libs/aegra-api/src/aegra_api/services/run_admission.py \
    libs/aegra-api/src/aegra_api/services/run_limits.py \
    libs/aegra-api/tests/unit/test_services/test_run_admission.py \
    libs/aegra-api/tests/unit/test_services/test_run_limits.py
  git commit -m "feat(api): serialize enqueue run admission"
  ```

## Task 4: Persist Effective Strategy in Stable Creation Order

**Files:**

- Modify: `libs/aegra-api/src/aegra_api/services/run_preparation.py`
- Modify: `libs/aegra-api/tests/unit/test_api/test_runs.py`
- Modify: `libs/aegra-api/tests/integration/test_api/test_runs_crud.py`

- [ ] Add failing tests proving `_prepare_run()` resolves `None` to `enqueue`,
  writes the same value to `RunORM`, `RunJob.behavior`, and the returned `Run`,
  and acquires the thread creation lock before adding/committing the row.

- [ ] Add a concurrent creation test that deliberately delays the first
  request around commit and verifies queue positions follow serialized commit
  order, not executor submission order.

- [ ] Run the focused API tests and confirm failure.

- [ ] In `_prepare_run()`, compute `effective_strategy`, take the thread lock
  before the run insert, populate the dedicated strategy column, preserve the
  DB-generated queue position, commit, refresh the ORM row, then submit.

- [ ] Include effective strategy and queue position in creation logs/metrics;
  do not expose `pending_reason` as a public protocol field.

- [ ] Run the focused tests and confirm they pass.

- [ ] Commit creation ordering:

  ```bash
  git add libs/aegra-api/src/aegra_api/services/run_preparation.py \
    libs/aegra-api/tests/unit/test_api/test_runs.py \
    libs/aegra-api/tests/integration/test_api/test_runs_crud.py
  git commit -m "feat(api): order stateful run creation"
  ```

## Task 5: Route Both Executors Through Admission

**Files:**

- Modify: `libs/aegra-api/src/aegra_api/services/local_executor.py`
- Modify: `libs/aegra-api/src/aegra_api/services/worker_executor.py`
- Modify: `libs/aegra-api/tests/unit/test_services/test_executor.py`
- Modify: `libs/aegra-api/tests/unit/test_services/test_worker_executor.py`

- [ ] Replace run-limit-specific test expectations with failing admission tests
  for `CLAIMED`, `THREAD_BLOCKED`, `ORG_AT_CAPACITY`, and `ALREADY_TAKEN` in
  both executor modes.

- [ ] Add tests proving duplicate Redis deliveries cannot execute a run twice,
  blocked-reason updates are committed, and local execution leases every run
  even when org limits are off.

- [ ] Add a local wait test proving `wait_for_completion()` keeps waiting via
  terminal-state polling when a queued run has not spawned an in-process task,
  then returns after the promoted task finishes.

- [ ] Run executor tests and confirm the new expectations fail.

- [ ] Make `LocalExecutor.submit()` and `promote()` always claim through
  `run_admission.try_start_run()`. Spawn only `CLAIMED`; commit blocked reason
  changes; rollback only `ALREADY_TAKEN` or actual errors.

- [ ] Make worker `_acquire_and_load()` use the same admission service and the
  same transaction rules. Keep Redis enqueue/dequeue transport unchanged.

- [ ] Remove settings-based bypasses that allow local runs to start without a
  claim, and heartbeat/release every local lease.

- [ ] Change local `wait_for_completion()` to use the active task when present
  and database terminal-state polling otherwise, under the caller's timeout.

- [ ] Make PostgreSQL fallback polling call the shared promotable-candidate
  selector rather than selecting the oldest pending row directly.

- [ ] Run executor tests and confirm they pass.

- [ ] Commit executor integration:

  ```bash
  git add libs/aegra-api/src/aegra_api/services/local_executor.py \
    libs/aegra-api/src/aegra_api/services/worker_executor.py \
    libs/aegra-api/tests/unit/test_services/test_executor.py \
    libs/aegra-api/tests/unit/test_services/test_worker_executor.py
  git commit -m "feat(api): enforce admission in all executors"
  ```

## Task 6: Generalize Pending-Run Promotion

**Files:**

- Create: `libs/aegra-api/src/aegra_api/services/run_queue_signal.py`
- Modify: `libs/aegra-api/src/aegra_api/services/run_admission.py`
- Modify: `libs/aegra-api/src/aegra_api/services/run_promoter.py`
- Modify: `libs/aegra-api/tests/unit/test_services/test_run_admission.py`
- Modify: `libs/aegra-api/tests/unit/test_services/test_run_promoter.py`

- [ ] Add failing candidate-selection tests for one eligible head per enqueue
  thread, concurrent eligibility across different threads, non-enqueue pending
  runs, projected org capacity, fairness across orgs, batch limits, and stale
  unscoped recovery.

- [ ] Add failing expiry tests proving only `pending_reason == "org"` can
  expire and the cutoff uses `pending_reason_at`, not run creation time. Pin a
  thread-to-org transition so prior thread wait does not consume org wait time.

- [ ] Add failing promoter-loop tests for immediate wakeup, periodic fallback,
  wakeups arriving during a tick, duplicate dispatch tolerance, and one failed
  dispatch not aborting the batch.

- [ ] Implement `find_promotable_runs()` in the admission service with a
  `NOT EXISTS` predecessor rule and projected org capacity. Keep the final
  admission claim authoritative.

- [ ] Generalize `RunPromoter` to run regardless of org-limit settings and use
  an `asyncio.Event` signal plus timeout. Clear the event before each tick so a
  signal arriving during work remains set for the next iteration.

- [ ] Restrict org queue expiry scans and guarded updates to
  `pending_reason == "org"`; after expiry, signal another promotion pass.

- [ ] Run admission/promoter tests and confirm they pass.

- [ ] Commit promotion changes:

  ```bash
  git add libs/aegra-api/src/aegra_api/services/run_admission.py \
    libs/aegra-api/src/aegra_api/services/run_promoter.py \
    libs/aegra-api/src/aegra_api/services/run_queue_signal.py \
    libs/aegra-api/tests/unit/test_services/test_run_admission.py \
    libs/aegra-api/tests/unit/test_services/test_run_promoter.py
  git commit -m "feat(api): promote durable thread queues"
  ```

## Task 7: Close Recovery and Terminal-Transition Gaps

**Files:**

- Modify: `libs/aegra-api/src/aegra_api/services/lease_reaper.py`
- Modify: `libs/aegra-api/src/aegra_api/services/run_status.py`
- Modify: `libs/aegra-api/src/aegra_api/api/runs.py`
- Modify: `libs/aegra-api/src/aegra_api/main.py`
- Modify: `libs/aegra-api/tests/unit/test_services/test_lease_reaper.py`
- Modify: `libs/aegra-api/tests/unit/test_services/test_run_executor.py`
- Modify: `libs/aegra-api/tests/unit/test_api/test_runs.py`
- Modify: `libs/aegra-api/tests/unit/test_main.py`

- [ ] Add failing tests proving a crashed predecessor returns to pending with
  its queue position unchanged, successors remain blocked during retry, retry
  exhaustion releases the next run, and Redis delivery failure is recovered by
  a later promoter scan.

- [ ] Add failing tests proving successful/error/interrupted finalization,
  unowned cancellation, queued-run cancellation, forced deletion, and org
  expiry all signal promotion only after their database transition commits.

- [ ] Add lifecycle tests requiring promoter and lease reaper startup/shutdown
  in local mode with org limits off.

- [ ] Remove the lease reaper's generic stuck-pending redispatch; the promoter
  now owns all unclaimed pending recovery. Keep expired-running lease recovery
  and retry accounting.

- [ ] Signal the queue after committed retry reset or exhausted terminal
  transition. A retrying predecessor retains its original queue position.

- [ ] Have `finalize_run()` and cancellation/deletion call sites notify through
  the dependency-free signal after a successful commit. Do not let signal
  failure alter the already-committed API outcome.

- [ ] Start and stop promoter/reaper unconditionally in the application
  lifespan, preserving shutdown order before executor drain.

- [ ] Run all touched unit tests and confirm they pass.

- [ ] Commit recovery behavior:

  ```bash
  git add libs/aegra-api/src/aegra_api/services/lease_reaper.py \
    libs/aegra-api/src/aegra_api/services/run_status.py \
    libs/aegra-api/src/aegra_api/api/runs.py \
    libs/aegra-api/src/aegra_api/main.py \
    libs/aegra-api/tests/unit/test_services/test_lease_reaper.py \
    libs/aegra-api/tests/unit/test_services/test_run_executor.py \
    libs/aegra-api/tests/unit/test_api/test_runs.py \
    libs/aegra-api/tests/unit/test_main.py
  git commit -m "fix(api): recover and release queued runs"
  ```

## Task 8: Add HTTP Integration Coverage

**Files:**

- Modify: `libs/aegra-api/tests/integration/test_api/test_runs_crud.py`

- [ ] Add HTTP integration tests for omitted/explicit strategy responses and
  invalid-strategy 422 errors across create, stream, and wait entry points.

- [ ] Add fixture-backed assertions that the effective strategy reaches the
  returned run and persisted execution parameters for each HTTP entry point.

- [ ] Run the integration tests and confirm they pass:

  ```bash
  uv run --package aegra-api pytest \
    libs/aegra-api/tests/integration/test_api/test_runs_crud.py -q
  ```

- [ ] Commit integration coverage:

  ```bash
  git add libs/aegra-api/tests/integration/test_api/test_runs_crud.py
  git commit -m "test(api): cover enqueue admission integration"
  ```

## Task 9: Prove Stateful FIFO End to End in Both Modes

**Files:**

- Create: `libs/aegra-api/tests/e2e/harness/enqueue/aegra.json`
- Create: `libs/aegra-api/tests/e2e/harness/enqueue/queue_graph.py`
- Create: `libs/aegra-api/tests/e2e/test_runs/test_multitask_enqueue.py`
- Modify: `Makefile`

- [ ] Build a deterministic graph whose state has an append reducer plus
  request-controlled delay, error, and interrupt behavior. It must not call an
  external model.

- [ ] Add a dedicated Make harness that runs the same E2E file with
  `LocalExecutor` and with Redis workers, using separate ports and guaranteed
  teardown. Add an aggregate `e2e-enqueue-both` target.

- [ ] Write the primary failing E2E: create a slow first run and immediate
  second run on one thread with omitted strategies; assert first is running,
  second is pending, both succeed in queue order, and final thread state
  contains both appended values.

- [ ] Add E2E cases for explicit enqueue, independent-thread concurrency,
  predecessor error, interruption, queued cancellation, and explicit
  checkpoint preservation. Keep the creation requests back-to-back without
  waiting for the first run's status so worker scheduling remains adversarial.

- [ ] Add an org-limit harness case proving same-thread FIFO and cross-thread
  org capacity compose without exceeding either constraint. The internal
  thread-to-org reason transition remains pinned by Task 6's unit tests.

- [ ] Run the dedicated suite in both modes:

  ```bash
  make e2e-enqueue-both
  ```

- [ ] Commit E2E coverage:

  ```bash
  git add Makefile libs/aegra-api/tests/e2e/harness/enqueue \
    libs/aegra-api/tests/e2e/test_runs/test_multitask_enqueue.py
  git commit -m "test(e2e): verify enqueue multitask strategy"
  ```

## Task 10: Document, Version, and Verify the Release

**Files:**

- Modify: `docs/feature-support.mdx`
- Modify: `docs/guides/threads-and-state.mdx`
- Modify: `docs/guides/worker-architecture.mdx`
- Modify: `docs/openapi.json`
- Modify: `libs/aegra-api/pyproject.toml`
- Modify: `libs/aegra-cli/pyproject.toml`

- [ ] Document effective `None → enqueue`, FIFO and terminal-release semantics,
  explicit checkpoint behavior, supported versus not-yet-implemented
  strategies, PostgreSQL authority, and multi-replica recovery.

- [ ] Update the feature-support table to mark `enqueue` supported while
  leaving the other multitask strategies unsupported.

- [ ] Regenerate OpenAPI and inspect the strategy enum/default and additive run
  response field:

  ```bash
  make openapi
  git diff -- docs/openapi.json
  ```

- [ ] Bump both package versions to `0.10.7` and verify they match.

- [ ] Run formatting, lint, type checking, security checks, and the full unit/
  integration suites:

  ```bash
  make format
  make lint
  make type-check
  make security
  make test
  ```

- [ ] Run the complete repository E2E suites in both standard modes, followed
  by the deterministic enqueue harness:

  ```bash
  make e2e-both
  make e2e-enqueue-both
  ```

- [ ] Inspect migration state and working-tree scope:

  ```bash
  uv run --package aegra-api alembic -c libs/aegra-api/alembic.ini heads
  git diff --check
  git status --short
  git log --oneline origin/main..HEAD
  ```

- [ ] Commit release metadata and docs:

  ```bash
  git add docs/feature-support.mdx docs/guides/threads-and-state.mdx \
    docs/guides/worker-architecture.mdx docs/openapi.json \
    libs/aegra-api/pyproject.toml libs/aegra-cli/pyproject.toml
  git commit -m "chore: prepare enqueue strategy release"
  ```

- [ ] Invoke `superpowers:requesting-code-review`, address findings with
  dedicated tests, and rerun every affected verification command.

- [ ] Invoke `superpowers:verification-before-completion` before reporting the
  branch ready for integration.
