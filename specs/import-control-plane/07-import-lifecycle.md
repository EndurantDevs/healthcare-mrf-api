# 07 Import Lifecycle

## Purpose

Define the unified run state machine and engine control behavior.

## State Machine

Allowed statuses:

- `queued`
- `starting`
- `running`
- `finalizing`
- `succeeded`
- `failed`
- `canceling`
- `canceled`
- `dead_letter`
- `waiting`
- `blocked`

Transitions:

- `queued -> starting`
- `starting -> running`
- `running -> finalizing`
- `finalizing -> succeeded`
- Any active state -> `failed`
- `queued|starting|running -> canceling -> canceled`
- Repeated failure -> `dead_letter`

## Auto-Finalize

Existing import families that require human `stop/finish` need an engine-side watcher:

- Track total chunks and completed chunks.
- Detect no more chunks are pending.
- Enqueue the appropriate finish/finalize queue once.
- Update `import_run.phase_detail`.
- Mark final result and metrics.

## Cancellation

Cooperative cancellation:

- Control endpoint sets Redis `cancel:{run_id}` with TTL.
- Start/fanout loops check before enqueueing more work.
- Chunk workers check at safe chunk boundaries.
- Finish is skipped if canceled before finalization.
- Long PTG scans check between files and terminate scanner subprocess only at safe file boundaries.
- `/importers` reports `cancelable:false` until implemented for that importer.

## Retry

- Retry creates a new run id.
- Retry copies original params and records `retry_of_run_id`.
- Idempotency key must differ unless prior run is terminal.
- Retry cannot mutate completed source/file placements unless the new run succeeds.

## Heartbeats

- Engine updates `heartbeat_at` periodically while active.
- Missing heartbeat marks mirrored run stale.
- Stale run does not automatically fail until timeout policy is met.

## Persisted Latest State

`import-control` persists one importer/node latest-state row outside the active
run list. The admin status table must survive service restarts and show:

- current active run, if any;
- latest run;
- latest terminal run;
- latest successful run.

The persisted payload includes run params, progress, estimate, metrics, error,
and timestamps. History purges may remove old `run_mirror` rows, but must not
erase the latest-state summary.

## Resource Metadata

PTG and MRF runs carry resource metadata in run metrics. PTG source-file imports
also mirror the selected resource class and queue in their metrics so admin UI,
worker repair, and retry logic keep the same lane.

## Acceptance Criteria

- A test import can be started, monitored, canceled, retried, and finalized without CLI intervention.
- Finish queue is enqueued exactly once.
- Cancellation is honest per importer capability.
- Latest completed run state is still visible after import-control/api-app restart.
