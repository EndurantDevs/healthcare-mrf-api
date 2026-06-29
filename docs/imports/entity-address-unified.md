# Entity Address Unified Import

## Purpose
Builds one unified address/entity search table from already imported internal datasets.
It consolidates provider, facility, and enrollment-side address evidence and applies deterministic inferred-NPI ranking rules.

## Command

```bash
python main.py start entity-address-unified
python main.py worker process.EntityAddressUnified --burst
```

PTG/TiC price files do not carry authoritative provider locations. Source
snapshot promotions can optionally trigger this importer through import-control,
but that runs a normal unified-address refresh from address-bearing sources
rather than rebuilding a PTG-specific address cache.

## Source Inputs

This importer does not fetch an external website directly. It materializes from canonical local tables built by other imports, including:

- `npi`
- `provider-enrichment`
- `cms-doctors`
- `facility-anchors`

## Main Table

- `mrf.entity_address_unified`

## Data Notes

- Keeps both direct `npi` and deterministic `inferred_npi` outcomes.
- For facility anchors, maps NPI in source order:
  - HRSA `FQHC Site NPI Number` is kept as direct `npi`.
  - CMS PECOS hospital/FQHC enrollment rows are normalized into a CCN->NPI crosswalk and populate `inferred_npi` only when a facility CCN has exactly one distinct NPI.
  - Optional PECOS additional-NPI rows are included in the same uniqueness check when `provider_enrollment_ffs_additional_npi` is available.
  - Additional-NPI matches are also written as review candidates.
  - When NPPES `npi_taxonomy` is available, CCNs with multiple distinct NPIs are disambiguated by facility taxonomy: among the conflicting NPIs the one whose NPPES taxonomy matches the facility type (`261QF0400X` for FQHC, `HOSPITAL_FACILITY_TAXONOMY_CODES` for hospitals) is selected — preferring the NPI that carries it as the primary taxonomy, otherwise the unique any-taxonomy match. These resolve as `inferred_npi` with method `fqhc_pecos_ccn_taxonomy`/`hospital_pecos_ccn_taxonomy` (confidence `0.95`).
  - CCNs where taxonomy does not isolate a single NPI (multiple NPIs all carry the facility taxonomy, or none does) remain unresolved instead of being auto-selected.
- Uses facility-aware ranking logic for ambiguous organization matches (for example hospital/FQHC contexts).
- Uses canonical `address_key` and primary phone+ZIP matches against NPPES for facility-anchor candidates with hospital/FQHC/clinic taxonomy evidence.
- Adds review-only NPPES DBA candidates for unresolved facility anchors:
  - FQHC DBA name + primary phone + ZIP, gated to FQHC taxonomy.
  - Hospital DBA name + ZIP + state, gated to the expanded NUCC hospital taxonomy set.
  These DBA matches are not auto-inferred; they require review/approval before promotion.
- Writes unresolved facility anchors to `facility_anchor_npi_candidate` with single-candidate, conflict, or no-candidate review status. Rows marked `review_status='approved'` are promoted into `facility_anchor_npi_override` before the next refresh and then participate in inference.
- Keeps a single primary direct/inferred NPI on `entity_address_unified`; additional NPIs remain review candidates/overrides until a separate serving bridge is introduced.
- Supports `primary`, `secondary`, and `practice` address evidence where applicable.
- Canonical dedupe runs inside import using normalized address keys (punctuation/case/zip formatting differences collapse into one row).
- Keeps source evidence on each unified address:
  - `address_sources`
  - `source_record_ids`
  - `source_count`
  - `multi_source_confirmed` (`true` when the same canonical address is observed from 2+ sources)

## Test Mode

```bash
python main.py start entity-address-unified --test
python main.py start entity-address-unified --test --limit-per-source 1000
python main.py start entity-address-unified --test --limit-per-source 1000 --publish
python main.py worker process.EntityAddressUnified --burst
```

Test mode is stage-only by default and does not replace the live serving tables
unless `--publish` or import-control `publish=true` is supplied.

## Key Environment Variables

- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_BATCH_SIZE`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_TABLE_SHARDS`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_SHARDS`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SHARDS`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_SHARDS`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_CONCURRENCY`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_INLINE_SOURCE_EVIDENCE` (default `true`; computes multi-source evidence during raw aggregation and skips the separate evidence work-table update pass for full rebuilds)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_INLINE_SOURCE_EVIDENCE` (default `false`; fail fast if the optimized inline evidence path is not active, used by the dev serving-only speed profile to avoid silently falling back to the old evidence work-table pass)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SPLIT_ARRAY_AGGREGATES` (default `false`; aggregates plan/source arrays in a separate pass to avoid multiplying grouped rows when raw rows carry many array values)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_CONCURRENCY` (default `4`; fan-out for independent main-stage index builds)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE` (default `all`; set `serving` to keep provider lookup, plan/network, taxonomy/procedure/medication, address-key, primary ZIP/city, and bounded geo serving indexes while skipping provenance/debug indexes and the broad standalone ZIP/GiST geo indexes)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE` (default `none`; set `serving` with `STAGE_INDEX_PROFILE=none` to publish the row-complete table first, mark the import published, then warm the same serving indexes on the live table outside the critical publish path)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_CONCURRENCY` (default: `STAGE_INDEX_CONCURRENCY`; fan-out for the optional live-table post-publish index warmup)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_CONCURRENTLY` (default `true`; uses `CREATE INDEX CONCURRENTLY` for live-table warmup. Set `false` for the publish-swap/read-mostly serving table to build normal indexes in parallel; reads stay available, but writes to the published table are blocked while each index builds.)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_PUBLISH_VALIDATION` (default `false`; for full serving-only refreshes, publish after row-count and table constraints, then run the same deep integrity validation on the live table before final warmup success)

When post-publish index warmup is enabled, use `metrics.published_elapsed_seconds`
as the publish-time measurement. Successful post-publish validation and warmup
updates preserve the original publish `finished_at`, so import-control duration
also reflects the row-complete publish time while metrics retain the post-publish
tail timings. A post-publish validation failure still marks the run failed at the
failure time. Total worker wall time can still include live index warmup after
the row-complete table has been published. `CREATE INDEX
CONCURRENTLY` warmup is forced to run one index at a time because PostgreSQL can
deadlock multiple concurrent index builds on the same table. For
entity-address-unified full serving refreshes the published table is read-mostly,
so dev uses non-concurrent warmup with parallelism to keep reads available while
finishing the serving indexes much faster. Before each warmup build, the importer
drops any invalid leftover index with the target name, so a canceled previous
warmup does not make `IF NOT EXISTS` skip a missing serving index. During the
publish-first window,
`metrics.post_publish_index_pending=true` means the live row-complete table is
published while serving indexes are still warming; the same status includes the
planned `metrics.post_publish_index_total` and current
`metrics.post_publish_index_completed`. After warmup,
`metrics.post_publish_index_completed` should match
`metrics.post_publish_index_total` and `metrics.post_publish_index_pending`
should be `false`. Deferred validation keeps all integrity checks, but uses the
valid primary key on `location_key` as catalog proof for non-null/uniqueness and
runs independent archive, coordinate, practice, and fallback checks in parallel.

- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_RAW_GROUP_INDEX_PROFILE` (default `group`; set `shard` for sharded inline-evidence full refreshes to index only `evidence_shard` before aggregation instead of the full grouping key)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CONCURRENCY` (default `4`; fan-out for independent support-table inserts after the unified stage is built)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_INDEX_CONCURRENCY` (default `2`; fan-out for support-table index builds)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_CODE_BRIDGES` (default `true`; set `false` for serving-focused rebuilds that do not need procedure/medication support bridge refresh)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES` (default `true`; set `false` for serving-focused rebuilds that do not need facility-anchor candidate refresh)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SERVING_ONLY` (default `false`; for full refreshes, publish only the denormalized serving table and leave existing support/provenance tables unchanged)
- `serving_only_refresh` import-control param / `--serving-only-refresh` CLI option (task-level override for `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SERVING_ONLY`; used by the daily Provider Directory serving projection)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE` (default `false`; dev-speed option for full rebuilds that builds the main stage table without WAL, trading crash durability for faster refreshes)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SOURCE_RECORD_IDS` (default `true`; set `false` for compacted serving refreshes to skip the expensive `source_record_ids` aggregation when final rows intentionally keep this array empty)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_FINAL_SUMMARY_COUNTS` (default `true`; set `false` for full serving refreshes to use the exact aggregate INSERT rowcount for staged rows and skip the final detailed count scan)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEEP_RAW_STAGE` (default `false`; experiment/retry aid that leaves the raw stage table in place for reuse; do not enable for normal scheduled refreshes)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES` (default `false`; when `true`, skips all non-primary-key stage indexes and records them as skipped; intended only for bounded proof runs)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE` (default `false`; retry only, resumes from an already-materialized stage table for the current import id)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_WORK_MEM`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAINTENANCE_WORK_MEM`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEMP_FILE_LIMIT` (best effort; skipped automatically when the database role cannot set it)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_LOCK_TIMEOUT`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_STATEMENT_TIMEOUT`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SYNCHRONOUS_COMMIT`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_JIT`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAX_PARALLEL_WORKERS_PER_GATHER`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_LIMIT_PER_SOURCE` (bounded pilots only; import-control can also pass `limit_per_source`)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_PUBLISH` (overrides publish decision; by default test mode skips publish and full mode publishes)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_SKIP_PUBLISH`
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_REFRESH_MODE` (`full` or `provider-directory-partial`; import-control can also pass `refresh_mode`)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_ARCHIVE_COORDINATES` (default `false`; when `true`, publish fails if archive rows still lack coordinates)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE` (default `false`; enables automatic NPI inference updates; review candidates are still populated when this is off)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NAME_FALLBACK_INFERENCE` (default `false`; enables expensive broad name+ZIP+street automatic inference after deterministic facility-anchor matches)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_NAME_INFERENCE` (default `false`; enables expensive NPPES organization-name fallback inference; otherwise those rows flow to the candidate review table)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_BROAD_INFERENCE` (default `false`; enables broad automatic NPPES exact-address and phone+ZIP inference; otherwise those rows remain review candidates)
- `HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPI_OTHER_IDENTIFIER_INFERENCE` (default `false`; enables automatic `npi_other_identifier` regex matching for facility anchors)
- `HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT` (default `25`)
- `HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES` (default `false`; broad text-based NPPES review-candidate expansion beyond the default indexed `address_key` and primary phone+ZIP matching)
- `HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_OTHER_IDENTIFIER` (default `false`; includes `npi_other_identifier` regex evidence in the review-candidate table)

## Experiment Harness

Entity-address pilots reuse the PTG research harness reporting flow:

```bash
./venv314/bin/python scripts/research/ptg2_experiment.py run \
  --suite docs/research/entity_address_unified_benchmark_suite.example.json \
  --dry-run
```

See `docs/research/entity_address_unified_optimization.md` for dev import-control
pilot commands.
