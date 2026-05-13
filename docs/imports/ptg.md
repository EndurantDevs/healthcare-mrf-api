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
- Parallel serving-only imports can batch top-level `in_network` objects per worker via `HLTHPRT_PTG2_WORKER_CHUNK_ITEMS` (default `128`).
- PTG2 semantic key hashing mode is configurable via `HLTHPRT_PTG2_HASH_MODE`:
  - `checksum64` (default, compact 16-hex keys),
  - `sha256` (64-hex keys),
  - `blake2_128` (32-hex keys).
- Snapshot publish and rollback are pointer operations through `ptg2_current_snapshot`.
- Plan-filtered `/pricing/providers/by-procedure` requests route to the PTG2 snapshot index when `plan_id`, `plan_external_id`, or `snapshot_id` is provided. No-plan requests keep the existing claims-pricing behavior.
- Warm-cache fixture benchmark coverage lives in `tests/test_ptg2_serving.py`; default p95 threshold is 50 ms (`HLTHPRT_PTG2_WARM_P95_MAX_MS`).
- This pipeline is payer-file oriented and structurally different from CMS Medicare claims imports.
- It is useful when you need raw transparency file ingestion rather than CMS summarized public use files.
