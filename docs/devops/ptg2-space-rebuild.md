# Strict PTG V3 Dev Reimport Runbook

Use this runbook to replace obsolete PTG snapshots with the only supported
architecture: `postgres_binary_v3` and `shared_blocks_v3`.

## Safety Rules

- Pause new PTG dispatch and let active publication transactions finish.
- Apply the strict V3 database migration before deploying the writer or reader.
- Reimport through the normal external orchestration path from stored source
  options; do not copy private source URLs into tickets or chat.
- Keep original JSON or gzip inputs only as bounded validation scratch. Strict
  serving is PostgreSQL-only.
- Do not remove an old snapshot until the new snapshot passes direct source
  exactness, API latency, storage, and pointer checks.

## 1. Capture Baseline

Record database size, import scratch usage, active PTG runs, and current source
and plan pointers. Use `pg_total_relation_size` for PostgreSQL evidence; pod
filesystem size is not a substitute.

## 2. Deploy Migration And Writer First

Run the healthcare MRF migration Job and verify one Alembic head. Update the
worker Job image, but leave the currently serving API Deployment on its prior
image while active plans are rebuilt. Strict V3 deliberately has no old-snapshot
reader, so switching the API first would make any remaining old pointer
unservable.

Confirm the worker's effective configuration reports:

- `HLTHPRT_PTG2_SNAPSHOT_ARCH=postgres_binary_v3`;
- `storage_generation=shared_blocks_v3`;
- PostgreSQL shared-block serving;
- no filesystem or in-process serving cache.

## 3. Reimport Normally

Use the authenticated operator API to dispatch a fresh import. When rebuilding from an existing
run, `scripts/devops/ptg2_rebuild_snapshot_from_options.py` loads stored options
and invokes the current strict importer. Do not hand-edit a published snapshot.

Record scanner, staging, finalization, publication, and total wall time. A
published state alone is not sufficient evidence.

## 4. Run Exact Source Audit

Before deleting validation scratch, run the independent audit against the
pinned published snapshot:

```bash
/opt/venv/bin/python /opt/scripts/validation/ptg2_v3_source_api_audit.py \
  /work/validation/source-01.json.gz \
  --report /work/validation/strict-v3-audit.json \
  --api-base-url http://healthcare-mrf-api:8080 \
  --plan-id "${PLAN_ID}" \
  --snapshot-id "${SNAPSHOT_ID}" \
  --source-key "${SOURCE_KEY}" \
  --profile release
```

Pass every release floor and require zero exactness, multiplicity, source
attribution, unresolved-reference, invalid-value, and negative-query failures.
The report must show at least 3,000 observed standard API HTTP requests and cold
p95 below 40 ms for the configured release gate.

## 5. Verify Storage and Independence

Confirm the logical snapshot and plan bindings are independent. Physical layout
reuse is allowed only when the complete physical input set and all semantic
options match. Verify shared-reference counts before removing any binding.

Record the strict relation split, unique shared bytes, logical referenced bytes,
and total PostgreSQL allocation. Confirm API requests create no local serving
cache files.

## 6. Cut Over The API

Pause PTG dispatch again and require all current source, plan, and global
pointers to resolve to published snapshots bound to sealed
`shared_blocks_v3` layouts. The cutover must fail if an import is still active
or any current pointer remains on an old generation.

Promote the already-tested writer image to the API Deployment. Require the
readiness endpoint to return HTTP 200 with both PostgreSQL and strict-V3 schema
status `OK`, then repeat representative pinned pricing requests. Do not rebuild
a different image for this step.

## 7. Remove Obsolete Snapshots

Dry-run `scripts/devops/ptg2_remove_source_snapshot.py`, review pointer and
shared-layout reference checks, then execute one snapshot at a time. Cleanup may
recognize old generation metadata solely to delete it safely.

The bulk legacy cleanup command fails closed while any current pointer still
references an old snapshot. It must never delete current pointer rows to make a
cleanup plan executable.

After each removal, verify current source/plan pointers, shared-layout reference
counts, garbage-collection eligibility, database size, and a pinned strict V3
pricing request.

## 8. Resume Dispatch

Restore capacity only after migration, import, exact audit, cold-latency,
storage, removal, and GC checks pass. Keep evidence under ignored report paths
with private identifiers redacted.
