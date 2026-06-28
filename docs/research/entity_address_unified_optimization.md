# Entity Address Unified Optimization Harness

This workflow reuses the PTG2 experiment harness for entity-address-unified
pilots. It keeps the evidence under ignored `reports/` paths and captures the
final import-control run, progress samples, elapsed time, and published metrics
from the importer.

## Dev Pilot Usage

Run a dry-run plan:

```bash
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/entity_address_unified_benchmark_suite.example.json \
  --dry-run
```

Run the 1k-per-source bounded dev smoke through import-control:

```bash
HLTHPRT_IMPORT_CONTROL_URL=http://127.0.0.1:8095 \
HLTHPRT_IMPORT_CONTROL_API_TOKEN=<token> \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/entity_address_unified_benchmark_suite.example.json \
  --case dev-bounded-smoke-1k
```

Bounded smoke cases pass `publish=false`, so they validate staging, support-table
builds, publish validation, and phase timings without replacing the live serving
tables.

Run the full dev pilot only when PTG/openaddress jobs are not competing for the
same database CPU and temp I/O:

```bash
HLTHPRT_IMPORT_CONTROL_URL=http://127.0.0.1:8095 \
HLTHPRT_IMPORT_CONTROL_API_TOKEN=<token> \
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/entity_address_unified_benchmark_suite.example.json \
  --case dev-full-run
```

Reports are written to:

```text
reports/ptg2-experiments/run-<timestamp>/report.json
reports/ptg2-experiments/run-<timestamp>/report.md
```

Inspect `import_run.final_run.metrics.phase_timings` for the real phase costs.
For the 2026-06-26 dev baseline, the latest successful full refresh took about
38.8 minutes wall time. The 10x target for the optimized dev refresh is under
4 minutes, with these run metrics proving the intended path was active:

```text
inline_source_evidence = true
split_array_aggregates = true
serving_only_refresh = true
support_stage_skipped = true
stage_index_concurrency = 12
enrich_concurrency = 24
raw_group_index_profile = shard
source_table_shards = 64
source_select_count > 100
raw_location_key_index_skipped = true
stage_index_profile = none
post_publish_index_profile = serving
post_publish_index_concurrently = true
post_publish_index_concurrency = 1
```

## Local Harness Proof

The 2026-06-27 local self-harness run used `healthporta_test` on
`127.0.0.1:5440`, full-mode source discovery, and `HLTHPRT_IMPORT_ID_OVERRIDE`
`codexeauserve16`. It staged 45,369 rows in 2.824 seconds with 514 source
selects, 32 aggregate shards, `inline_source_evidence=true`,
`split_array_aggregates=true`, `serving_only_refresh=true`, and
`raw_location_key_index_skipped=true`.

The exact row comparison against the old-path baseline table
`mrf.entity_address_unified_codexeauoldpath1`, excluding only `updated_at` and
`last_seen_at`, returned:

```text
new_count=45369 old_count=45369 new_minus_old=0 old_minus_new=0
```

## Dev Proof Notes

Disposable dev proofs on 2026-06-27 ran in a temporary `eau-proof-codex` pod
with `publish=false`; each proof removed its stage artifacts and left the live
serving table unchanged at 35,553,296 rows.

Measured wall times:

```text
baseline optimized path with all stage indexes: 7:07.2
serving index profile, publish validation still before skip: 6:58.6
serving index profile, publish validation skipped for publish=false: 5:52.1
tightened serving profile + 8GB maintenance_work_mem: 5:38.6
diagnostic with all additional stage indexes deferred: 4:19.6
main-stage heap-load experiment: 5:59.4 (worse; do not enable)
serving profile + source_record_ids aggregation skipped: 5:31.4
diagnostic with additional stage indexes deferred + shard raw group index: 3:59.3
serving profile + shard raw group index + stage index concurrency 4: 5:15.4
serving profile + shard raw group index + stage index concurrency 8: 5:11.6
aggregate concurrency 24 experiment: 5:17.6 (worse; keep 16)
source concurrency 32 experiment: 5:11.7 (neutral/slightly worse; keep 24)
enrich concurrency 24 + stage index concurrency 8: 5:09.1
enrich concurrency 24 + stage index concurrency 12: 5:01.6
enrich concurrency 24 + stage index concurrency 14: 5:05.4 (worse; keep 12)
enrich concurrency 24 + stage index concurrency 16: 5:04.1 (worse; keep 12)
enrich concurrency 32 + stage index concurrency 12: 5:04.4 (worse; keep 24)
main-stage heap-load retest with current profile: 5:18.1 (worse; do not enable)
aggregate shards 16 + aggregate concurrency 16: 5:08.2 (worse; keep 32 shards)
primary-only taxonomy/procedure/medication GIN predicates: 4:59.8
work_mem 512MB with accepted profile: 5:01.5 (worse; keep 128MB)
drop primary_npi from serving profile: 5:06.8 (worse; keep primary_npi)
aggregate shards 64 + aggregate concurrency 16: 5:06.9 (worse; keep 32 shards)
geo_bbox serving index instead of geo_idx GiST: 4:31.3
primary_phone_npi serving index + raw unified phone filter: 4:31.6
aggregate concurrency 8 experiment: 5:26.6 (worse; keep 16)
skip standalone zip5 in serving profile: 4:21.5
source table shards 64 + serving profile: 4:13.6
source table shards 32 experiment: 4:25.9 (worse; keep 64)
max_parallel_workers_per_gather 0 experiment: 4:24.3 (worse; leave unset)
source table shards 64 + stage index profile none: 3:52.4 (10x boundary; data complete, serving indexes deferred)
one-pass terminal stage counts + shutdown count reuse: implemented, proof pending
one-pass counts full dev proof with serving profile: 4:22.6 (35,553,296 rows, no artifacts; slower dev run, not accepted as 10x)
skip final detailed summary counts for full serving refreshes: implemented, proof pending
skip final detailed summary counts full dev proof with serving profile: 4:13.7 (35,553,296 rows, no artifacts; exact staged rowcount from aggregate INSERT)
aggregate concurrency 32 with reused raw stage: aggregation wall 90.6s (better than ~103-104s; promote 32)
aggregate concurrency 32 full serving proof: 4:26.9 (worse overall due dev-load/enrichment and geo_bbox index outliers; keep as aggregate-only improvement, not full proof)
```

Current interpretation: the row-building path can get just under 4 minutes on
full dev data when serving indexes are deferred. The deployable
serving profile is now about 4:13, down from the 38.8 minute baseline, with the
remaining wall time concentrated in aggregation (~103s), serving index builds
(~24-26s), source loading (~64s), and raw enrichment (~33-34s).

Strict 10x is now proven for the data-build path only: `stage_index_profile=none`
finished in 3:52.4 with 35,553,296 staged rows, matching live row count, and no
leftover artifacts. That keeps row quality but does not leave the published table
serving-ready by itself. A deployable strict-10x path should publish first and
warm serving indexes separately, or explicitly accept temporarily slower queries
until index warmup completes.

The publish-first path is now explicit: use
`HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE=none` with
`HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE=serving`. The import
publishes and marks the row-complete table successful before warming the serving
indexes on the live table. This preserves row quality and makes the sub-4-minute
target a publish-time target; serving index warmup remains a separate
post-publish performance phase. Use `metrics.published_elapsed_seconds` for the
publish-time SLA when post-publish index warmup is enabled; total worker wall
time can still include warmup. Import-control waiters can observe terminal
status immediately after publish with
`metrics.post_publish_index_pending=true`, a nonzero
`metrics.post_publish_index_total`, and
`metrics.post_publish_index_completed=0`; if the proof also needs warmup
timings, poll the same run again after the worker finishes and confirm
`metrics.post_publish_index_pending=false`,
`metrics.post_publish_index_completed=metrics.post_publish_index_total`, and
`metrics.post_publish_index_timings` is populated. Live-table warmup uses
`CREATE INDEX CONCURRENTLY` by default and forces sequential index builds because
PostgreSQL can deadlock multiple concurrent index builds on the same table. It
also drops invalid leftover indexes before retrying, so a canceled or deadlocked
warmup cannot leave a name collision that makes `IF NOT EXISTS` skip the rebuild.

The importer now computes final staged/NPI/inferred/multi-source counts in one
aggregate scan and reuses the staged row count during shutdown. This removes
several full scans over the 35M-row stage table from the critical path and is
expected to close part of the remaining serving-ready gap. Full dev proof
`codexeauproofcount27` completed in 4:22.6 with 35,553,296 rows and no leftover
artifacts; that run was slower than the previous best, with source loading at
70.2s and aggregation at 107.0s, so it is not enough by itself.

`HLTHPRT_ENTITY_ADDRESS_UNIFIED_FINAL_SUMMARY_COUNTS=false` now lets full
serving refreshes reuse the exact aggregate INSERT rowcount for staged rows and
skip the final detailed NPI/inferred/multi-source count scan. Partial refreshes
still fall back to a stage-table count because they can mix reused and newly
aggregated rows.

Full dev proof `codexeauprooffastcnt27` with final summary counts disabled
finished in 4:13.7 with 35,553,296 rows, `aggregating.rows=35553296`, and no
leftover artifacts. This restores the prior best serving-ready range but still
does not meet strict 10x while all serving indexes are built before completion.

Reused-raw proof against `entity_address_unified_codexrawreuse27_raw` showed
`HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_CONCURRENCY=32` improves aggregation:
wall time was 90.6s for 35,553,296 rows versus the 103-104s range at
concurrency 16. The dev overlay now uses 32.

Full serving proof `codexeauproofagg32` did not validate as a full wall-clock
win: it finished in 4:26.9. Aggregation was 93.8s, but enrichment regressed to
43.1s and `geo_bbox` index build regressed to 37.0s in that run. Treat
concurrency 32 as an accepted aggregation-local improvement that still needs a
clean full-run confirmation.

Post-publish success updates now preserve the `finished_at` recorded when the
row-complete table is published. This makes import-control duration match
`metrics.published_elapsed_seconds` for publish-first refreshes while still
recording deferred validation and index warmup metrics afterward. Validation
failure remains terminal and updates the run to failed. A follow-up proof
attempt, `run_5c7f1deb7cdb4abaa44e21a5fd30859b`, was canceled and cleaned up
because it entered the old non-inline source-evidence path and added heavy dev
load; do not use it as speed evidence.

Rollback-transaction geo index checks on the live dev table confirm the current
serving `geo_bbox` shape is the right tradeoff. A single-column `lat` candidate
built in 11.1s but made the Jacksonville 25-mile near query scan a large
latitude band and execute in 616 ms. The accepted `(lat, long)` candidate built
in 13.4s and executed the same query in 46 ms while avoiding a sequential scan.
Both checks rolled back, leaving only the live `geo_idx` index in dev.

Local smoke on `healthporta_test` (PostgreSQL 5440) with the serving profile
staged 3,504 rows in 0.47s, published nothing, and left no proof artifacts.
With `HLTHPRT_ENTITY_ADDRESS_UNIFIED_FINAL_SUMMARY_COUNTS=false`, staged rows
came from the aggregate INSERT rowcount (`aggregating.rows=3504`) and no final
summary count scan was required. This verifies the code path but is not a useful
wall-clock benchmark because the local base is intentionally small.

The accepted serving index profile keeps exact query semantics but avoids two
expensive per-refresh index builds:

- `/npi/near/` now uses a partial `(lat, long)` `geo_bbox` B-tree in the serving
  profile. The query already applies a bounding box before exact `ST_DWithin`
  filtering. Staged `EXPLAIN ANALYZE` checks used `geo_bbox`, avoided a
  sequential scan, and returned in the low tens of milliseconds for the
  Jacksonville 25-mile smoke query when dev was otherwise quiet.
- unified-table phone filtering uses the already-normalized `telephone_number`
  column plus `primary_phone_npi`; live dev data had 9,284,476 primary phone
  rows and 0 non-digit primary phone rows. A staged phone-count smoke used an
  index-only scan on `primary_phone_npi` and returned in ~0.4 ms.
- the standalone broad `zip5` index is skipped in the serving profile; primary
  provider ZIP searches still keep `primary_zip5_npi`, and near-zip searches
  also have the bounded geo path.
- source table sharding is set to 64 in the dev overlay. This reduced source
  SELECTs from 514 to 258 and gave the best serving-ready proof so far; 32
  shards made the stage-index phase unstable and regressed total time.

`HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SOURCE_RECORD_IDS=false` is accepted
for serving-only compacted refreshes. The final serving table already has
`source_record_ids` compacted to empty arrays, so skipping the high-cardinality
string `ARRAY_AGG(DISTINCT source_record_id ...)` preserves final row content
for that mode.

`HLTHPRT_ENTITY_ADDRESS_UNIFIED_RAW_GROUP_INDEX_PROFILE=shard` is accepted for
the current sharded inline-evidence path. It reduces the raw group index phase
from about 36s to about 8.5s; aggregation gets somewhat slower, but the net
serving-ready run still improves.

Before accepting the dev result, compare the run against the latest baseline:

```sql
SELECT
    run_id,
    status,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM finished_at - started_at)::int AS elapsed_seconds,
    metrics->>'rows' AS rows,
    metrics->>'inline_source_evidence' AS inline_source_evidence,
    metrics->>'split_array_aggregates' AS split_array_aggregates,
    metrics->>'serving_only_refresh' AS serving_only_refresh,
    metrics->>'support_stage_skipped' AS support_stage_skipped,
    metrics->>'source_table_shards' AS source_table_shards,
    metrics->>'source_select_count' AS source_select_count,
    metrics->>'raw_location_key_index_skipped' AS raw_location_key_index_skipped,
    metrics->>'stage_index_concurrency' AS stage_index_concurrency,
    metrics->'phase_timings' AS phase_timings
  FROM mrf.import_run
 WHERE importer = 'entity-address-unified'
 ORDER BY started_at DESC NULLS LAST
 LIMIT 5;
```

The live serving row count should stay in the same order of magnitude as the
baseline full refresh. If `serving_only_refresh=true`, support/provenance
tables are intentionally preserved from the previous publish rather than
rebuilt.

For concurrent phases, prefer `metrics->'phase_timings'->phase->>'wall_seconds'`
over `seconds` when estimating wall-clock contribution. `seconds` is accumulated
SQL time across shards and can exceed real elapsed time by design.

The importer uses the same bounded-concurrency pattern as the PTG experiments
for the long independent phases. Dev can tune the final support tail with:

```text
HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CONCURRENCY
HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_INDEX_CONCURRENCY
```
