# PTG2 Source Snapshot GC DevOps

This runbook covers cleanup of old source-scoped PTG2 serving snapshots. It is
for retained `manifest_snapshot` rows in `mrf.ptg2_snapshot` whose physical
tables are no longer referenced by the current PTG2 pointer tables.

## What It Cleans

The maintenance command targets only snapshots that satisfy all of these
conditions:

- `mrf.ptg2_snapshot.status = 'published'`
- `manifest->'serving_index'->>'storage' = 'manifest_snapshot'`
- the snapshot id is absent from all current pointer tables:
  - `mrf.ptg2_current_snapshot`
  - `mrf.ptg2_current_source_snapshot`
  - `mrf.ptg2_current_plan_source`

For those snapshots it drops only manifest-declared PTG2 tables with known
snapshot-table prefixes, then deletes matching `ptg2_artifact_manifest` and
`ptg2_snapshot` metadata rows. It does not remove raw payer downloads or
sidecar artifact files.

## Command

Dry-run first:

```bash
./venv314/bin/python -m process.ptg_parts.ptg2_source_snapshot_gc \
  --schema mrf \
  --max-snapshots 400 \
  --max-tables 2000 \
  --max-bytes-gb 80
```

Execute only after reviewing the dry-run counts:

```bash
./venv314/bin/python -m process.ptg_parts.ptg2_source_snapshot_gc \
  --schema mrf \
  --max-snapshots 400 \
  --max-tables 2000 \
  --max-bytes-gb 80 \
  --lock-timeout 5s \
  --execute
```

The execute path recomputes the candidate set inside one database transaction
before dropping tables. It refuses to run if the recomputed snapshot count,
table count, or byte total exceeds the supplied bounds.

## Dev-Server Access Pattern

On the current dev server, PostgreSQL is host-level PostgreSQL on port `5440`;
the Kubernetes service points to `postgres-host:5440`.

From a deployed API pod, run the command with the normal app environment. From
the host, connect as the `postgres` system user for ad hoc verification:

```bash
ssh ubuntu@<dev-node-host>
sudo -u postgres psql -p 5440 -d healthporta
```

Do not paste database credentials into tickets, PRs, or chat. Prefer running
the maintenance command inside an environment that already has the app DB
variables.

## Verification Queries

After cleanup, non-current manifest snapshots should be zero:

```sql
WITH current_refs AS (
    SELECT snapshot_id FROM mrf.ptg2_current_snapshot WHERE snapshot_id IS NOT NULL
    UNION
    SELECT snapshot_id FROM mrf.ptg2_current_source_snapshot WHERE snapshot_id IS NOT NULL
    UNION
    SELECT snapshot_id FROM mrf.ptg2_current_plan_source WHERE snapshot_id IS NOT NULL
), manifest_snapshots AS (
    SELECT snapshot_id, manifest->'serving_index'->>'source_key' AS source_key
    FROM mrf.ptg2_snapshot
    WHERE status = 'published'
      AND COALESCE(manifest->'serving_index'->>'storage', '') = 'manifest_snapshot'
), non_current AS (
    SELECT ms.*
    FROM manifest_snapshots ms
    LEFT JOIN current_refs cr ON cr.snapshot_id = ms.snapshot_id
    WHERE cr.snapshot_id IS NULL
), current_missing AS (
    SELECT cr.snapshot_id
    FROM current_refs cr
    LEFT JOIN mrf.ptg2_snapshot s ON s.snapshot_id = cr.snapshot_id
    WHERE s.snapshot_id IS NULL
)
SELECT 'manifest_snapshots' AS metric, count(*)::text AS value FROM manifest_snapshots
UNION ALL
SELECT 'non_current_manifest_snapshots', count(*)::text FROM non_current
UNION ALL
SELECT 'current_refs_missing_snapshot', count(*)::text FROM current_missing;
```

Current manifest table references should all point at existing tables:

```sql
WITH current_refs AS (
    SELECT snapshot_id FROM mrf.ptg2_current_snapshot WHERE snapshot_id IS NOT NULL
    UNION
    SELECT snapshot_id FROM mrf.ptg2_current_source_snapshot WHERE snapshot_id IS NOT NULL
    UNION
    SELECT snapshot_id FROM mrf.ptg2_current_plan_source WHERE snapshot_id IS NOT NULL
), current_table_refs AS (
    SELECT DISTINCT regexp_replace(table_value, '^.*\.', '') AS table_name
    FROM mrf.ptg2_snapshot s
    JOIN current_refs cr ON cr.snapshot_id = s.snapshot_id
    CROSS JOIN LATERAL (VALUES
        (s.manifest->'serving_index'->>'table'),
        (s.manifest->'serving_index'->>'price_atom_table'),
        (s.manifest->'serving_index'->>'provider_group_member_table'),
        (s.manifest->'serving_index'->>'provider_npi_scope_table'),
        (s.manifest->'serving_index'->>'serving_binary_table'),
        (s.manifest->'serving_index'->>'provider_group_location_table'),
        (s.manifest->'serving_index'->>'provider_set_component_table'),
        (s.manifest->'serving_index'->>'code_count_table')
    ) AS tables(table_value)
    WHERE COALESCE(s.manifest->'serving_index'->>'storage', '') = 'manifest_snapshot'
      AND table_value IS NOT NULL
      AND table_value <> ''
), missing_current_tables AS (
    SELECT ctr.table_name
    FROM current_table_refs ctr
    LEFT JOIN pg_class c ON c.relname = ctr.table_name
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'mrf'
    WHERE n.oid IS NULL
)
SELECT 'current_manifest_table_refs' AS metric, count(*)::text AS value FROM current_table_refs
UNION ALL
SELECT 'missing_current_manifest_tables', count(*)::text FROM missing_current_tables;
```

Track disk and database size before and after:

```bash
df -hT /data
sudo du -sh /data/healthporta/postgres /data/healthporta-rwx /data/healthporta/k3s 2>/dev/null
```

```sql
SELECT pg_size_pretty(pg_database_size('healthporta')) AS db_size;
```

## Known Follow-Up

If a source still has more than one manifest snapshot after GC, check whether an
older snapshot remains referenced by `ptg2_current_plan_source`. Do not delete
those rows through GC. First repair or repromote the source pointer path so
plan rows and source rows agree on the current snapshot, then rerun the dry-run.

## 2026-07-03 Dev Evidence

Before cleanup, the dev database had `268` old published manifest snapshots
across `1,074` physical tables, totaling about `30GB`:

- `ptg2_serving_*`: `268` tables, about `23GB`
- `ptg2_price_atom_*`: `268` tables, about `4.3GB`
- `ptg2_provider_group_member_*`: `268` tables, about `2.5GB`
- `ptg2_code_count_*`: `268` tables, about `436MB`
- `ptg2_provider_group_location_*`: `2` tables, about `329MB`

The live cleanup dropped the `1,074` tables and deleted `268` snapshot metadata
rows. Post-checks showed:

- `non_current_manifest_snapshots = 0`
- `current_refs_missing_snapshot = 0`
- `missing_current_manifest_tables = 0`
- `/data` increased from about `905G` free to about `934G` free during this GC

The remaining pressure was not old snapshots. It was current PTG2 serving data:
`mrf.ptg2_serving_*` was still about `4.3T`.
