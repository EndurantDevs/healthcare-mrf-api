# PTG2 Dev Space Rebuild Runbook

This runbook covers the dev-server workflow for replacing a large current
legacy PTG2 source snapshot with the current lean storage layout, then removing
only the verified old snapshot.

Use this for current snapshots that are still referenced by
`mrf.ptg2_current_source_snapshot` or `mrf.ptg2_current_plan_source`. Do not use
the non-current GC runbook for these; current snapshots must be rebuilt,
compared, repointed by the importer, and only then removed.

## Safety Rules

- Pause PTG dispatch before starting, or the scheduler can immediately consume
  reclaimed space with unrelated planned imports.
- Rebuild from stored `mrf.ptg2_import_run.options`; do not hand-copy payer URLs
  into tickets or chat.
- Compare old and new snapshots before removal.
- Treat row samples and retained sidecar hashes as required checks.
- Remove one old source snapshot at a time.
- Keep the job output JSON and post-cleanup metrics in the incident/report
  folder.

## 1. Pause PTG Dispatch

Set the dev import-control PTG capacity to zero and confirm active slots drain:

```bash
curl -sS -X PATCH \
  -H "Authorization: Bearer ${HP_IMPORT_CONTROL_API_TOKEN}" \
  -H "Content-Type: application/json" \
  --data '{"ptg_slot_capacity":0}' \
  http://127.0.0.1:8095/v1/nodes/local_mrf
```

Keep it at zero while disk is tight.

## 2. Capture Baseline Metrics

Capture filesystem and database size before rebuilding:

```bash
df -h /data
sudo du -xhd1 /data 2>/dev/null | sort -h | tail -30
```

Inside an API environment with DB variables:

```sql
SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size;
```

For candidate selection, prefer the largest current legacy source snapshots.
Current references come from:

- `mrf.ptg2_current_source_snapshot`
- `mrf.ptg2_current_plan_source`

## 3. Rebuild Snapshot

Run the rebuild script inside the deployed API image or a Kubernetes Job using
the same image and the `import-workdir` PVC:

```bash
/opt/venv/bin/python /opt/scripts/devops/ptg2_rebuild_snapshot_from_options.py \
  --snapshot-id ptg2:202606:old_snapshot_id \
  --import-id space_rebuild_source_suffix_yyyymmddhhmmss
```

The script loads the old snapshot's stored import options from Postgres and
runs `process.ptg.main` with the current code/storage layout. The output JSON
includes the new snapshot id, file counts, and serving-rate count.

## 4. Compare Old and New

After the rebuild publishes the new snapshot, compare before any cleanup:

```bash
/opt/venv/bin/python /opt/scripts/devops/ptg2_compare_snapshots.py \
  --old-snapshot-id ptg2:202606:old_snapshot_id \
  --new-snapshot-id ptg2:202606:new_snapshot_id \
  --sample-limit 500 \
  --sample-pct 0.1
```

The compare must pass all of these checks:

- old and new snapshots are published
- source key matches
- serving-rate count matches
- processed-file count matches
- sampled serving rows have zero misses
- sampled price atoms have zero misses
- sampled provider group members have zero misses
- sidecar byte counts and sha256 hashes match

Do not remove the old snapshot if any check fails.

## 5. Remove Old Snapshot

Dry-run first:

```bash
/opt/venv/bin/python /opt/scripts/devops/ptg2_remove_source_snapshot.py \
  --snapshot-id ptg2:202606:old_snapshot_id \
  --source-key ptg_source_key
```

Execute only after confirming `removable: true` and reviewing the table list:

```bash
/opt/venv/bin/python /opt/scripts/devops/ptg2_remove_source_snapshot.py \
  --snapshot-id ptg2:202606:old_snapshot_id \
  --source-key ptg_source_key \
  --execute
```

The remove script refuses snapshots still referenced by global, source, or plan
pointers. It drops only manifest-declared PTG2 snapshot tables and deletes the
matching `ptg2_artifact_manifest` and `ptg2_snapshot` metadata.

## 6. Verify After Cleanup

Refresh storage metrics:

```bash
df -h /data
sudo du -xhd1 /data 2>/dev/null | sort -h | tail -30
```

Check the old snapshot is gone and the new snapshot remains current:

```sql
SELECT snapshot_id, source_key
FROM mrf.ptg2_current_source_snapshot
WHERE source_key = 'ptg_source_key';

SELECT snapshot_id, status
FROM mrf.ptg2_snapshot
WHERE snapshot_id IN ('ptg2:202606:old_snapshot_id', 'ptg2:202606:new_snapshot_id');
```

Run a pricing smoke with a real plan for the source:

```bash
curl -sS \
  "http://127.0.0.1:8080/api/v1/pricing/group-plan-providers?plan_id=${PLAN_ID}&market_type=${MARKET_TYPE}&source_key=${SOURCE_KEY}&limit=1"
```

The response should resolve the rebuilt snapshot id.

## 7. Resume Capacity

Only restore PTG dispatch capacity after:

- old snapshot cleanup completed
- storage metrics are recorded
- pricing smoke resolved the rebuilt snapshot
- no unrelated planned backlog should start immediately

When dev storage is still tight, leave `ptg_slot_capacity` at `0`.
