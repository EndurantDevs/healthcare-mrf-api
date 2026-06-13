# Address Canonical (Deduplicated Address Archive)

**Status:** IN PROGRESS — Phase 0 fixes and Phase 1 foundation implemented locally.
**Date:** 2026-06-11

## Implementation checkpoint — 2026-06-11

- Added Alembic foundation migration for `address_archive_v2`, canonical
  `addr_*_v1` SQL functions, checksum bridge/quarantine tables, and nullable
  `address_key` columns.
- Added `process/ext/address_canon.py` with SQL-backed stage stamping and
  archive resolve helpers, including an explicit `address_key`/`identity_key`
  collision guard.
- Wired canonical address stamping behind `HLTHPRT_ADDRESS_CANON_SOURCES` for
  CMS doctors, facility anchors, provider-enrichment FFS, NPPES, marketplace MRF,
  pharmacy license, and Part D pharmacy activity.
- Updated `entity_address_unified` to carry `address_key`, support the
  flag-gated v2 grouping key, include `mrf_address` as a source, and fall back
  to `NULL address_key` if the canonical SQL functions are not installed yet.
- Updated NPI geocode archive reads/writes to prefer v2 archive tables when
  available and fall back to the legacy checksum archive otherwise.
- Added DB-backed checks for checksum-bridge cardinality, Python/SQL parity
  over a frozen 200+ case corpus, reason buckets, synthetic key-collision abort,
  MRF-source unified rebuild, and live NPI detail compatibility before the
  address migration is applied.
- Added phase-based live progress events for canonical address stamping and
  archive resolve; events reuse the existing async Redis/import-control progress
  path and no-op when no controlled import context is active.
- Added `HLTHPRT_ADDRESS_CANON_STAMP_SHARDS` as an ops override for canonical
  address stamp sharding, with focused unit coverage and runbook guidance.
- Added `HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY` as an explicit dev/prod
  rebuild throttle for parallel shard updates. Default remains serial for small
  databases; 24-core dev rebuilds can use 8.
- Added shared Phase 2 gate telemetry to `resolve_into_archive`: eligible
  street+state+ZIP rows, eligible rows still missing `address_key`, normalized
  gate violations, bounded multi-source sample rows for eyeball review, and
  live warning events for unstamped eligible rows.
- Materialized the golden corpus into 279 explicit frozen fixture cases; SQL
  and Python address-key parity is checked against that fixture in Postgres.
- Added an optional Rust-backed resolve materializer behind
  `HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE=true`. Python exports the deduped raw
  address projection with asyncpg COPY, `ptg2_scanner
  --address-canonicalize-copy` computes the keyed COPY rows, and Python loads
  the same transaction-local temp table before running the existing mismatch,
  collision, insert, provenance, and gate checks.
- Added static USPS Pub. 28 normalization constants from official Postal
  Explorer C1/C2/Appendix B/I4 pages and compiled them into the SQL functions;
  DB tests now verify function volatility/parallel-safety and expanded Pub. 28
  examples.
- Added the `address-archive-v2-migrate` controlled importer/CLI for Phase 3.1.
  A disposable real-copy run over 916,949 local legacy archive rows produced
  780,672 canonical rows in 88.608s with zero represented/geocode misses; see
  `reports/address_archive_v2_migration_smoke_2026-06-11.md`.
- Verification so far: disposable Postgres `5440` is at Alembic head
  `20260612205000_reapply_address_canonical_nbsp_functions`; expanded impacted
  importer/API/control suite `346 passed`; full local suite `931 passed`;
  post-suite schema check
  confirmed `address_archive_v2`, `address_checksum_map`, and
  `addr_key_v1(text,text,text,text,text,text)` remain installed; `git diff
  --check` and `python -m compileall -q api process db alembic tests` passed.
  PostGIS migration matrix was validated in
  `healthporta_address_canon_postgis_test` with PostGIS `3.6.1`, including a
  downgrade/upgrade cycle and the conditional `address_archive_v2_geo_idx`.
  Current live local healthcare-mrf-api smoke on `127.0.0.1:8085` returned
  `200` for healthcheck, `npi/all`, `npi/id`, and pricing score against an
  unmigrated `healthporta` DB, proving fallback readers.
- Current focused post-review verification: `tests/test_address_canonical_unit.py`
  plus `tests/test_address_checksum_schema.py` `40 passed`;
  `tests/test_address_canonical_db.py` `14 passed` against disposable Postgres
  `5440`; `support/ptg2_scanner` Rust tests `37 passed` after adding the
  actual NPI `2ND FLOOR-PULMANARY` parity regression; NPI v2 cutover tests
  `5 passed`; pharmacy
  canonical failure tests `2 passed`; full local suite `964 passed`; `git diff
  --check` and `python -m compileall -q api process db alembic tests` passed.
- Disposable dev rebuild verification completed with
  `HLTHPRT_ADDRESS_CANON_SOURCES=all` and
  `HLTHPRT_ADDRESS_ARCHIVE_TABLE=address_archive_v2`: geo, NUCC, CMS doctors,
  facility anchors, provider enrichment, NPPES, marketplace MRF, Part D,
  pharmacy license, archive migration, and `entity-address-unified` were run in
  order on a wiped `healthporta` dev schema. Resulting source-key coverage:
  CMS doctors `5000/5000`, facility anchors `501/501`, FFS addresses
  `1408/1408`, NPI addresses `2818/2818`, MRF addresses `376/376`, pharmacy
  license `10/10`, unified addresses `8688/10174`, archive rows `4169`, and
  checksum collisions `0`. Part D smoke activity rows had ZIP/state but no
  street and no overlapping NPI primary address in the small NPPES sample, so
  no Part D keys were expected in this run; the importer now fills state from
  ZIP and street/city/state/ZIP from primary NPI rows when available.
- Facility anchors were rerun on the dev DB after the source-coordinate archive
  refresh was added; `address_archive_v2` now has 500 facility-source rows with
  `lat`/`long` and `geocode_source='facility_anchor'`.
- Dev-server Rust materializer verification on image
  `ghcr.io/endurantdevs/healthcare-mrf-api:dev-address-canon-rust-20260612214227`
  passed on actual data after fixing Rust unit parsing parity for
  `2ND FLOOR-PULMANARY`. A 1M-row NPI scratch resolve inserted 487,574
  canonical rows with 0 key mismatches and 0 source-bit misses in 9.752s wall
  time; Rust transformed 45.3 MB of COPY input to 143.8 MB in 3.106s. A
  multi-source actual-data smoke also passed with 0 key mismatches, 0
  source-bit misses, and no gate violations for NPI 250k rows, marketplace MRF
  500k rows, CMS doctors 250k rows, and all keyed facility-anchor rows. Live
  dev API/control smoke returned expected statuses for healthcheck, `npi/all`,
  `npi/id`, NPI full taxonomy, coverage, pharmacy-license, Part D, and control
  registry endpoints, with no `address_key` in checked public NPI payloads.
- Dev deploy was later advanced through
  `ghcr.io/endurantdevs/healthcare-mrf-api:dev-address-canon-rust-20260613020811`
  and finally to
  `ghcr.io/endurantdevs/healthcare-mrf-api:dev-address-canon-rust-20260613050521`
  for the NPPES timing follow-up and MRF recovery work. The newer images add
  configurable NPI worker concurrency, conflict-safe MRF formulary aggregate
  upserts, `HLTHPRT_MRF_QUEUE_READ_LIMIT`, DB-pool sizing for parallel address
  stamping, BigInteger ORM coverage for MRF provider NPI/network-checksum
  staging plus NPPES NPI identifiers, and Rust parity fixes for actual NPI
  `2ND FLOOR-PULMANARY` and `STE T .` addresses. The MRF parallelism follow-up
  showed that multiple normal `process.MRF --burst` processes with a larger
  read window are safer than one very wide ARQ process, which can exhaust the
  per-process DB pool. The current dev profile pins
  `HLTHPRT_ADDRESS_CANON_STAMP_SHARDS=24`,
  `HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY=16`, and
  `HLTHPRT_DB_POOL_MAX_SIZE=32` for canonical address stamping on the 24-core
  dev host.
- The controlled Python 3.14 MRF run for `addrcanon_mrf_20260612215839`
  published successfully on 2026-06-13. Patched finish
  `hp-mrf-finish-patched-20260613020811` took 3208.32s; total import delta was
  5:51:04.280138. Published counts included 15,500 plans, 25,672,487 plan NPI
  rows, 16,302,076 MRF address rows, 85,859,271 MRF address evidence rows, and
  15,500 plan search summary rows. `address_archive_v2` had 1,322,413 rows with
  the MRF source bit. The only two null MRF `address_key` rows were malformed
  Intermountain source rows with `postal_code='.'`.
- Full NPPES evidence for `addrcanon_npi_20260613040046`: the actual June 2026
  data load completed in 1369.91s, then patched shutdown
  `hp-npi-shutdown-patched-20260613050521` published the staged import in
  26m12s. `canonical_address_resolve` took 780.512s and represented 5,351,601
  NPPES source-bit keys in `address_archive_v2`. Published counts were
  9,606,683 NPI rows, 12,019,685 taxonomy rows, 19,707,579 NPI address rows,
  and 154,583 phone-staffing rows. Healthcheck, `npi/id/1154324382`, and import
  API smoke passed, with NPI queues empty afterward. Remaining NPI shutdown
  hotspots are SQL restamping, `do_business_as`, taxonomy-array enrichment, and
  `npi_address` vacuum/analyze.

## The idea in one paragraph

Today, six different imports (NPPES, CMS doctors, marketplace MRF, enrollment,
pharmacy license, Part D) each write the same real-world addresses into their own
tables, each using their own incompatible "code" (checksum) for the address. The
same clinic at "123 Main St, Suite 200" can exist as five different rows with five
different codes that can never be matched to each other. This project turns the
existing `mrf.address_archive` table into the single shared "address book" of the
whole system: every unique physical address is stored there exactly once, with one
stable fingerprint, no matter which import saw it. Imports keep their own tables
and their fast loading paths — they just learn to also stamp each address with the
shared fingerprint and register it in the address book at the end of each run.

## Documents

| Doc | What it covers |
|---|---|
| [01-problem-and-goal.md](01-problem-and-goal.md) | What is broken today, explained simply, with evidence |
| [02-algorithm.md](02-algorithm.md) | The deduplication algorithm, step by step, with examples |
| [03-data-model.md](03-data-model.md) | New and changed tables, columns, indexes |
| [04-import-integration.md](04-import-integration.md) | What changes in each importer, one by one |
| [05-migration-and-safety.md](05-migration-and-safety.md) | Rollout phases, geocode preservation, rollback, audits |
| [06-task-list.md](06-task-list.md) | The full task list with acceptance criteria |

## Ground rules (decided with the product owner)

1. The center of deduplication and address history is **`mrf.address_archive`**,
   evolved in place — not a brand-new table.
2. The ~3M geocoded addresses already in production `address_archive` are
   **migrated wholesale**; zero geocoded unique addresses may be lost
   (verified by query before anything legacy is dropped).
3. Imports must not slow down: no per-row database lookups, COPY fast paths
   untouched; all dedup work happens in a few set-based SQL statements per import.
4. Healthcare-grade correctness: when in doubt the algorithm **splits** addresses
   (keeps two rows) rather than merging them. Automatic merging of "almost equal"
   addresses is forbidden; near-matches go to a human review queue.
