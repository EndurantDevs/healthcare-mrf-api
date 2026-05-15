# PTG Import

## Purpose
Imports Transparency in Coverage file structures through the PTG2 importer. PTG2 keeps the existing `ptg` command name while adding content-addressed raw artifact retention, canonical source identities, monthly snapshot bookkeeping, and dynamic PTG2 table creation.

## Source Websites
- Transparency in Coverage files published by payers and referenced from public TOC locations
- CMS transparency ecosystem context: <https://www.cms.gov/healthplan-price-transparency>

## Start Command
```bash
python main.py start ptg
```

Heartland Dental full-run example:
```bash
HLTHPRT_PTG2_ARTIFACT_DIR="/Volumes/Data/data" \
HLTHPRT_PTG2_KEEP_PARTIAL_ARTIFACTS=true \
python main.py start ptg \
  --toc-url "<SIGNED_UHC_HEARTLAND_DENTAL_INDEX_URL>" \
  --plan-id 010854205 \
  --plan-market-type group \
  --import-month 2026-05-01 \
  --import-id heartland_dental_202605_full \
  --source-key heartland_dental
```

## Common Options
```bash
python main.py start ptg --test
python main.py start ptg --toc-url <URL>
python main.py start ptg --toc-list ./toc_urls.txt
python main.py start ptg --toc-url <URL> --plan-market-type group --plan-id <PLAN_ID> --test
python main.py start ptg --toc-url <URL> --plan-id <PLAN_ID> --import-month 2026-04-01 --max-files 1 --max-items 1000
python main.py start ptg --in-network-url <URL>
python main.py start ptg --allowed-url <URL>
python main.py start ptg --provider-ref-url <URL>
```

TOC imports can be scoped with repeatable filters:
- `--plan-id <PLAN_ID>` matches exact `reporting_plans[].plan_id`.
- `--plan-name-contains <TEXT>` matches plan, sponsor, issuer, or reporting entity text.
- `--plan-market-type <TYPE>` matches values such as `group` or `individual`.
- `--import-month <YYYY-MM-DD>` records the snapshot month; the day is normalized to the first day of the month.
- `--max-files <N>` limits discovered rate files for guarded pilot imports.
- `--max-items <N>` limits top-level in-network or allowed-amount items parsed per file.
- `--reuse-raw-artifacts/--no-reuse-raw-artifacts` controls retained raw file reuse. Reuse is enabled by default.
- `--keep-artifacts-on-failure` is an alias for keeping partial failed downloads so reruns do not redownload large files.

When filters match a TOC structure containing multiple plans, only the matched plan metadata is attached to imported rows.

## Main Outputs
- PTG2 control/canonical tables such as `ptg2_import_run`, `ptg2_snapshot`, `ptg2_current_snapshot`, `ptg2_source_catalog`, `ptg2_source_identity`, `ptg2_source_file_version`, `ptg2_content_identity`, `ptg2_artifact_manifest`, `ptg2_provider_set`, `ptg2_price_set`, `ptg2_rate_pack`, `ptg2_fact_chunk`, and `ptg2_rate_set`
- Snapshot serving artifact: `snapshot_index/<snapshot_id>.json`, referenced by `ptg2_artifact_manifest`
- `ptg_file`
- `ptg_in_network_item`
- `ptg_negotiated_rate`
- `ptg_negotiated_price`
- `ptg_allowed_item`
- `ptg_allowed_payment`
- `ptg_allowed_provider_payment`
- `ptg_provider_group`
- `ptg_billing_code`

## Notes
- Raw artifacts are retained under `HLTHPRT_PTG2_ARTIFACT_DIR` when set, otherwise under the system temp directory. Files are stored by SHA-256 prefix.
- Before downloading, PTG2 checks server metadata. Strong ETag plus content length allows reuse; length plus last-modified is allowed for metadata reuse; otherwise the local SHA-256 is verified before reuse.
- Gzip and ZIP inputs are streamed to logical JSON files; decompressed members are not loaded into memory.
- Full UHC-scale serving imports use the Rust compact scanner by default. It writes rotating COPY shard files under the PTG2 temp directory and Python streams those shards into unlogged Rust stage tables.
- Serving-rate Rust worker shards are routed to separate PostgreSQL stage tables (`worker0000` -> base stage table, `worker0001+` -> lane stage tables). This allows parallel COPY across tables without multiple COPY sessions extending the same large heap.
- `HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES` defaults to 128 MiB. Shards are deleted after successful COPY.
- `HLTHPRT_PTG2_COMPACT_COPY_TASKS` controls global shard COPY concurrency (default `4`).
- `HLTHPRT_PTG2_COMPACT_COPY_KIND_TASKS` controls per-target-table COPY concurrency (default `1`). Keep this low for large `price_set` loads to avoid PostgreSQL table-extension contention.
- `HLTHPRT_PTG2_COMPACT_SERVING_COPY_TASKS` controls per-serving-stage-table COPY concurrency (default `1`). Keep this at `1`; parallelism comes from worker lane tables plus the global `HLTHPRT_PTG2_COMPACT_COPY_TASKS` cap.
- `HLTHPRT_PTG2_INDEX_TASKS` controls parallel post-load index creation (default `4`).
- `HLTHPRT_PTG2_RUST_EVENT_QUEUE` controls Rust scanner copy-event buffering (default `32`). This bounds `.ready` shard backlog when PostgreSQL COPY is slower than parsing.
- Rust scanner progress defaults to every 256 MiB or 2,000,000 parsed objects. Override with `HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES` or `HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS` when interactive visibility needs to change.
- `PTG2_COPY_SHARD_START` and `PTG2_COPY_SHARD_DONE` should appear while `PTG2_SCANNER_PROGRESS` is still advancing. A growing `.ready` backlog without active PostgreSQL `COPY` means the scanner/COPY handoff is unhealthy.
- Successful imports print a final `PTG2_IMPORT_DONE` line with processed/failed file counts, serving row count, total seconds, data seconds, publish seconds, index seconds, and analyze seconds. The same timing data is stored in `ptg2_import_run.report.timings`.
- Parallel serving-only imports can still batch top-level `in_network` objects per worker via `HLTHPRT_PTG2_WORKER_CHUNK_ITEMS` for the Python parser path.
- PTG2 semantic key hashing mode is configurable via `HLTHPRT_PTG2_HASH_MODE`:
  - `checksum64` (default, compact 16-hex keys),
  - `sha256` (64-hex keys),
  - `blake2_128` (32-hex keys).
- Snapshot publish and rollback are pointer operations through `ptg2_current_snapshot`.
- Source-scoped PTG2 serving publishes snapshot-specific tables and updates current pointers. This preserves the NPI/claims-style rollback property: a bad import should not replace the current source snapshot until publish completes.
- Plan-filtered `/pricing/providers/by-procedure` requests route to the PTG2 snapshot index when `plan_id`, `plan_external_id`, or `snapshot_id` is provided. No-plan requests keep the existing claims-pricing behavior.
- Warm-cache fixture benchmark coverage lives in `tests/test_ptg2_serving.py`; default p95 threshold is 50 ms (`HLTHPRT_PTG2_WARM_P95_MAX_MS`).
- This pipeline is payer-file oriented and structurally different from CMS Medicare claims imports.
- It is useful when you need raw transparency file ingestion rather than CMS summarized public use files.

## Operational Checks

Use these checks during large PTG2 imports:

```bash
# Import and scanner processes
ps -axo pid,ppid,stat,etime,%cpu,%mem,rss,command | rg 'main.py start ptg|ptg2_scanner'

# COPY shard backlog
find /Volumes/Data/data/tmp -maxdepth 1 -type f -name '*.ready' | wc -l
find /Volumes/Data/data/tmp -maxdepth 1 -type f -name '*.ready' -exec du -k {} + \
  | awk '{s+=$1} END{printf "%.2f GiB\n", s/1024/1024}'

# Active PostgreSQL COPY sessions
psql -h 127.0.0.1 -p 5440 -U nick -d healthporta -Atc "
select pid,state,wait_event_type,wait_event,now()-query_start,substring(query,1,180)
from pg_stat_activity
where datname='healthporta'
  and (query ilike '%copy%' or query ilike '%ptg2_rust_stage%' or query ilike '%ptg%')
order by backend_start desc;"

# Rust stage table growth
psql -h 127.0.0.1 -p 5440 -U nick -d healthporta -Atc "
select table_name, pg_size_pretty(pg_total_relation_size(format('%I.%I', table_schema, table_name)::regclass))
from information_schema.tables
where table_schema='mrf' and table_name like 'ptg2_rust_stage_%'
order by table_name;"
```

Healthy import behavior:
- `ptg2_scanner` stays CPU-bound while parsing.
- PostgreSQL shows active `COPY` sessions while `.ready` files exist.
- `.ready` backlog may fluctuate, but should not grow unbounded for many minutes.
- Stage table sizes should grow while scanner progress advances.

Known remaining validation gap:
- Full Heartland-size runs have been validated through scanner and COPY behavior, but the final publish/index phase still needs a complete observed run. Watch for `PTG2_PUBLISH_*` and `PTG2_INDEX_*` output once scanner and shard COPY complete.

## Cleanup After Stopped Runs

If a PTG2 import is stopped before publish, remove only PTG/PTG2 temp artifacts and tables from that failed run. Do not delete retained raw artifacts under `raw/`; they allow reruns to avoid redownloading large UHC files.

```bash
# Remove PTG2 COPY shards from the configured temp directory
find /Volumes/Data/data/tmp -maxdepth 1 -type f \
  \( -name 'ptg2_*.copy*' -o -name 'ptg2_compact_serving_*' \) -delete

# Drop PTG/PTG2 tables in the local development DB
psql -h 127.0.0.1 -p 5440 -U nick -d healthporta -v ON_ERROR_STOP=1 -c "
DO \$\$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT format('%I.%I', table_schema, table_name) AS fqtn
    FROM information_schema.tables
    WHERE table_schema='mrf'
      AND table_type='BASE TABLE'
      AND (table_name LIKE 'ptg\\_%' ESCAPE '\\' OR table_name LIKE 'ptg2\\_%' ESCAPE '\\')
  LOOP
    EXECUTE 'DROP TABLE IF EXISTS ' || r.fqtn || ' CASCADE';
  END LOOP;
END \$\$;"
```
