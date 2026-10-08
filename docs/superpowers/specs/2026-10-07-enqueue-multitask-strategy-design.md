# Enqueue Multitask Strategy Design

**Status:** Approved for implementation planning

## Context

Aegra accepts `multitask_strategy` on stateful run requests and persists it
inside `RunJob.behavior`, but neither executor uses it. Two runs created for
the same thread can therefore execute at the same time from the same parent
checkpoint. Both run records may finish, but their checkpoints form sibling
branches and the checkpoint with the newest ID becomes the thread's current
state. The thread's materialized state is also whichever concurrent run
finalizes last.

LangGraph Deployments treats an omitted strategy as `enqueue`. Aegra will
follow that effective default and implement `enqueue` first. Implementing the
other accepted strategies (`reject`, `interrupt`, and `rollback`) is outside
this change.

## Goals

- Resolve an omitted stateful-run strategy to `enqueue`.
- Start same-thread enqueue runs one at a time in durable FIFO order.
- Make the behavior identical in local and Redis-worker execution modes.
- Compose thread serialization with the existing per-organization concurrency
  limit without races or deadlocks.
- Preserve queued work across Redis loss, API restarts, worker crashes, and
  multiple Aegra replicas.
- Keep explicit checkpoint selection intact while still respecting admission
  order.
- Expose and document the effective strategy returned for each run.

## Non-goals

- Implementing `reject`, `interrupt`, or `rollback` semantics.
- Merging checkpoints produced by runs that were already executed
  concurrently before this migration.
- Applying multitask strategies to stateless runs, which have isolated
  ephemeral threads.
- Reinterpreting a queued run's submitted input or command after an earlier
  run interrupts. Admission changes when the run starts, not what it means.

## Public Contract

`RunCreate.multitask_strategy` becomes a literal union of the four protocol
values. It remains optional at the wire boundary for SDK compatibility, but
run preparation resolves `None` to `enqueue` before constructing or
persisting the job.

The run response includes the effective strategy. Existing clients remain
compatible because this is an additive response field. Invalid strings return
HTTP 422 instead of being silently accepted.

For an enqueue run, “FIFO” means ascending durable queue position within one
thread. A later run cannot enter `running` while an older enqueue run on that
thread is `pending` or `running`. Terminal predecessors (`success`, `error`,
or `interrupted`) release the next run. Cancellation and queue expiry are
terminal for this purpose.

The rule belongs to the incoming enqueue run: it waits behind every older
active run on its thread. Explicit non-enqueue strategies retain their current
behavior until their semantics are implemented separately.

An explicit checkpoint remains the run's selected starting checkpoint. Queue
ordering does not replace it with the predecessor's final checkpoint. Without
an explicit checkpoint, normal LangGraph checkpoint lookup occurs when the
run actually starts, so the run sees its predecessor's committed state.

## Persistence Model

Add these columns to `runs`:

- `multitask_strategy TEXT NOT NULL DEFAULT 'enqueue'`
- `queue_position BIGINT NOT NULL`, populated from a PostgreSQL sequence
- `pending_reason TEXT NULL`, constrained to `thread` or `org`
- `pending_reason_at TIMESTAMPTZ NULL`

Add a partial index ordered by `(thread_id, queue_position)` for rows whose
status is `pending` or `running`. This supports predecessor checks and the
promoter's per-thread head selection.

The Alembic migration is linear from the current head. It creates and owns the
sequence, backfills existing rows deterministically by `(created_at, run_id)`,
sets the sequence above the backfill maximum, applies defaults and nullability,
then creates constraints and indexes. Downgrade removes the added objects.

`execution_params.behavior.multitask_strategy` remains populated for worker
reconstruction and protocol observability. Dedicated columns are the source of
truth for admission; scheduling must not query JSONB.

## Creation Ordering

Run creation takes a transaction-scoped PostgreSQL advisory lock keyed by the
thread before inserting the run. The lock remains held through commit. This
serializes queue-position assignment and commit order for one thread while
allowing unrelated threads to proceed concurrently.

The API commits the run before asking an executor to submit it, as today. A
later request may reach an executor first, but the admission gate will see its
older pending predecessor and leave it queued.

## Unified Admission Gate

Introduce a dependency-light `run_admission.py` service. Both executors call
its `try_start_run()` for every stateful run, independent of whether org limits
are enabled. The service owns:

- thread and organization advisory-lock acquisition;
- enqueue predecessor checks;
- organization-capacity checks through the existing run-limit helpers;
- `pending_reason` updates for blocked runs; and
- the conditional `pending → running` claim.

Locks are always acquired in thread-then-organization order. The thread lock
protects same-thread eligibility. The organization lock protects the capacity
count. A conditional update still closes races with cancellation, expiry, and
another worker claiming the same row.

Admission returns one of `CLAIMED`, `ALREADY_TAKEN`, `THREAD_BLOCKED`, or
`ORG_AT_CAPACITY`. A blocked result leaves the row pending and unclaimed.
`pending_reason` is cleared on a successful claim and recalculated on every
attempt, so a run can move from thread-blocked to org-blocked after its
predecessor completes.

`pending_reason_at` changes only when the reason changes and is cleared on
claim. Org queue expiry measures from the transition to `org`, so time spent
waiting behind a thread predecessor does not consume the org queue-wait budget.

The current `run_limits.try_start_run()` claim logic moves into the admission
service. Capacity calculation and org-specific settings remain in
`run_limits.py` to preserve a focused service boundary.

## Promotion and Delivery

Generalize `RunPromoter` from an org-limit promoter into the durable pending-run
promoter. It runs whenever Aegra runs, not only when org limits are enabled.

On each tick it:

1. Expires only runs currently blocked by an enforced org limit and older than
   `ORG_RUN_MAX_QUEUE_WAIT_SECONDS`.
2. Selects pending, unclaimed candidates that are the oldest active enqueue run
   for their thread.
3. Applies projected org capacity for fair batch selection.
4. Dispatches selected IDs through `executor.promote()`.

Selection is advisory. The executor's admission claim repeats all checks under
locks, making duplicate dispatches and multi-replica promoter races harmless.
PostgreSQL remains the source of truth; Redis contains delivery hints only.

A small dependency-free queue signal wakes the local process's promoter after
a terminal transition, cancellation, expiry, or recovered worker failure.
The periodic scan remains the cross-process and lost-signal safety net.

In Redis mode, PostgreSQL fallback polling uses the same candidate selector.
In local mode, promoted runs are reconstructed from `execution_params` before
claiming and spawning, as they are today.

Local wait/join behavior polls terminal database state when a queued run has no
in-process task yet. It must not treat “not spawned” as “already complete.”

Because enqueue is active even when org limits and Redis are disabled, the
promoter and lease reaper run in every mode. Local execution acquires and
heartbeats a lease for every claimed run, giving a restarted development
server the same recovery path instead of leaving a successor blocked forever.

## Recovery Semantics

- A crashed running worker keeps its successors blocked until the lease reaper
  either returns it to `pending` for retry or marks it terminal after exhausting
  retries.
- A retried predecessor keeps its original queue position.
- The lease reaper never blindly dispatches an intentionally thread-blocked or
  org-blocked pending run. It signals the promoter instead.
- A pending row lost between database commit and Redis enqueue is discovered by
  the promoter without waiting for org-limit mode.
- Deleting or cancelling a queued run makes the next eligible run promotable.
- Thread status remains `busy` while any run on that thread is pending or
  running. Finalization uses the existing active-run check and therefore does
  not incorrectly mark a thread idle between queued runs.

## Observability

Add structured log fields and metrics for effective strategy, queue position,
blocked reason, queue wait duration, and promotion. Keep the existing org-limit
metrics for org-capacity events; add thread-queue metrics rather than
relabeling org metrics.

## Testing Strategy

Unit tests cover strategy validation/defaulting, migration-chain integrity,
admission outcomes, lock ordering, predecessor selection, org-limit
composition, promoter wakeups, expiry scoping, and lease-reaper behavior.

Integration tests exercise the HTTP strategy contract and run-preparation
wiring through the repository's existing application fixtures.

End-to-end tests use real PostgreSQL plus a deterministic graph with a
controllable first run and a state-appending second run. The same test runs in
dev and production modes and asserts:

- omitted strategy behaves as enqueue;
- the second run stays pending while the first runs;
- different threads can still run concurrently;
- both outputs are retained in order in final thread state;
- error, interruption, and cancellation unblock correctly; and
- org capacity and thread FIFO compose.

Worker-retry and Redis-delivery-loss paths are covered at the service boundary
with deterministic tests, while the existing run-reconciliation E2E suite
continues to exercise real worker recovery.

## Documentation and Versioning

Update the run and worker-architecture documentation, feature-support table,
and generated OpenAPI schema. Bump both `aegra-api` and `aegra-cli` from
`0.10.6` to the same patch version, following repository policy.
