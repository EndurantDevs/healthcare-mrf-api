# Address Archive v2 Migration Smoke

Date: 2026-06-11  
Database: disposable `healthporta_address_canon_test` on `127.0.0.1:5440`,
schema `mrf`, with follow-up full NPPES rebuild checks on local and dev
`healthporta` databases  
Source data: `mrf.address_archive_realcopy_test`, loaded from local
`healthporta.mrf.address_archive`  
Initial migration smoke mutation: none on main `healthporta`

## Result

The one-time `address-archive-v2-migrate` helper was run against a real local
legacy archive snapshot copied into the disposable database.

| Metric | Value |
|---|---:|
| Legacy rows | 916,949 |
| Keyable rows | 916,417 |
| Non-keyable rows | 532 |
| Canonical rows inserted | 780,372 |
| Canonical rows updated | 0 |
| Checksum bridge rows | 916,417 |
| Checksum collision rows | 0 |
| Distinct collision checksums | 0 |
| Missing represented keys | 0 |
| Legacy geocoded keys | 780,372 |
| Missing geocoded keys | 0 |
| Runtime | 88.644 seconds |

The smoke was rerun after the 2026-06-11 review fixes for SQL/Python identity
parity, shared unit extraction, lowercase country handling, and the non-US
exclusion rule. Earlier pre-review numbers were intentionally discarded because
they were generated with invalid keys.

## Resolve Performance Smoke

`resolve_into_archive` was also run against a 916,417-row real-copy staging
table built from the same legacy archive snapshot and checksum bridge. The
archive target was the disposable scratch table
`mrf.address_archive_resolve_perf_v2`; the main `healthporta` database was not
mutated.

| Metric | Value |
|---|---:|
| Staged rows | 916,417 |
| Eligible key rows | 916,352 |
| Eligible null-key rows | 0 |
| Distinct address keys | 780,372 |
| Inserted archive rows | 780,372 |
| Provenance updates | 0 |
| Null key rows | 0 |
| Gate violations | 0 |
| Identity collisions | 0 |
| Runtime | 66.809 seconds |

Reason buckets:

| Reason | Rows |
|---|---:|
| Ambiguous unit | 101,611 |
| Missing street | 65 |
| Unit conflicts | 45,628 |
| Missing state | 0 |
| Missing ZIP | 0 |
| Unsupported country | 0 |

## Notes

- The first SQL-only resolve materialization attempt was canceled after it
  stayed in `addr_street_norm_v1` normalization for more than 6 minutes on the
  same 916,417-row staging input.
- The implemented archive migration path materializes canonical keys in Python
  batches using the parity-tested oracle, then uses SQL for upsert,
  checksum-map rebuild, collision quarantine, and V1/V2/V3 verification.
- The implemented resolve path materializes raw staging rows once, builds the
  keyed projection in Python batches, indexes the temp projection, and reuses it
  for coverage, reason buckets, collision checks, inserts, and provenance
  updates.
- The run exposed real unmapped legacy `state_name` values longer than eight
  characters. `address_archive_v2.state_code` was widened from `varchar(8)` to
  `varchar(32)` in the foundation migration and specs.

## Full NPPES Rebuild Follow-Up Notes

An in-progress full local `npi` rebuild was started on the main dev
`healthporta` database with canonical dual-write enabled:

```bash
HLTHPRT_ADDRESS_CANON_SOURCES=nppes \
HLTHPRT_ADDRESS_ARCHIVE_TABLE=address_archive_v2 \
HLTHPRT_ADDRESS_ARCHIVE_CUTOVER=1 \
./venv314/bin/python main.py start npi
./venv314/bin/python main.py worker process.NPI --burst
```

Final local result:

| Phase / metric | Observed value |
|---|---:|
| Raw `process_data` job runtime before shutdown hooks | 1,613.87 seconds |
| Staged `npi_20260611` rows | 9,606,683 |
| Published `npi` rows | 9,606,683 |
| Published `npi_address` rows | 19,707,579 |
| Published `npi_address.address_key` populated during load | 19,694,747 |
| Published `npi_address` null `address_key` rows | 12,832 |
| Normalized-eligible US street rows still missing `address_key` | 0 |
| Published `npi_address` geocoded rows | 7,405,679 |
| Published `npi_taxonomy` rows | 12,019,685 |
| Published `npi_other_identifier` rows | 3,477,937 |
| Published `npi_taxonomy_group` rows | 1,105,012 |
| Published `npi_phone_staffing` rows | 154,583 |
| `address_archive_v2` rows after NPI resolve committed | 5,386,849 |
| `address_archive_v2` rows with NPPES source bit | 5,386,849 |
| `address_archive_v2` geocoded rows | 780,372 |
| `address_checksum_map` rows | 916,417 |
| `address_checksum_collision` rows | 0 |
| Staged `npi_address_20260611` size before taxonomy rewrite | ~7.1 GiB |
| Staged `npi_address_20260611` size after taxonomy rewrite | ~12 GiB |
| Staged `npi_address_20260611` size during geocode rewrite | ~13 GiB |
| Full local NPI rebuild wall time | 3:40:36.049770 |

The run proves the load-time `address_key` optimization works: address keys are
available before shutdown, so NPI skips the previous expensive SQL stamping
phase. It also exposed performance follow-ups that must be addressed before the
Phase 2.4 NPPES wall-time AC can be claimed:

- `resolve_into_archive` still materialized the 19.7M-row staged address table
  through a Python batch loop before SQL collision/upsert checks. This is
  correct but slow; future work should dedupe earlier or push more of the
  materialization into set-based SQL while preserving Python/SQL key parity.
- NPI shutdown rewrites large staging tables after load. The DBA reset/update
  rewrote `npi_20260611`, and `taxonomy_array` enrichment rewrites
  `npi_address_20260611`. In this run, the taxonomy rewrite over 19.7M staged
  address rows took more than one hour and grew the staged table from ~7.1 GiB
  to ~12 GiB.
- Geocode enrichment currently performs another large staged address rewrite
  by joining `npi_address_20260611.address_key` to `address_archive_v2`; the
  table was ~13 GiB and the statement was still active after 30+ minutes.
- Autovacuum repeatedly started on transient staging tables during import
  shutdown and competed for WAL/IO. Future work should use temporary staging
  reloptions or controlled analyze points without publishing permanent
  autovacuum-disabled tables.
- NPI needs durable per-phase shutdown timers/live progress so this report can
  be generated from run output instead of manual `pg_stat_activity` sampling.
- The published `npi_address` index set needs a query-plan audit. Historical
  geo/coverage indexes on the previous published table consumed tens of GiB;
  every retained index should map to an active endpoint/query plan and have
  build time captured in the smoke report.
  Initial read-only audit flags: `npi_address_idx_primary` may duplicate the
  table PK; `type_state_city_npi` and `primary_state_city_npi` overlap for
  primary-only traffic; multicolumn GIN `(taxonomy_array, plans_network_array)`
  may be replaceable by bitmap-AND over the single-column GIN indexes;
  published `address_key` has no known active API/helper query; and ZIP callers
  should use the same expression as the retained ZIP index.
- Procedure and medication enrichment are additional post-load full staged-table
  rewrites. After geocode enrichment, local NPI shutdown ran
  `procedures_array` from `pricing_provider_procedure`, then
  `medications_array` from `pricing_provider_prescription`; these need the same
  load-time/side-table/CTAS treatment as taxonomy and geocode enrichment.
- API smoke after publish passed for healthcheck, `/api/v1/npi/id/1326442542`,
  `/api/v1/npi/all?limit=1&include_total=false`, and
  `/api/v1/pricing/providers/1003000480/score`. The live payload redaction
  check found and then fixed an `address_key` leak in flattened `/npi/all`
  rows and nested provider-enrichment data; focused tests now assert no
  `address_key` appears anywhere in public NPI payloads.

## Dev Server Validation Notes

The same current source tree was copied to the dev server under an ephemeral
folder and run with the existing healthcare image's Python 3.14 environment
against host PostgreSQL 18 on `ns1033171`.

Foundation setup:

| Metric | Value |
|---|---:|
| Legacy `mrf.address_archive` rows | 2,299,958 |
| Canonical `mrf.address_archive_v2` rows after backfill | 1,634,735 |
| `mrf.address_checksum_map` rows after backfill | 2,296,683 |
| `mrf.address_checksum_collision` rows after backfill | 0 |
| Canonical geocoded rows | 1,634,735 |
| Canonical rows with `premise_key` | 1,634,649 |

Full dev NPI rebuild was then started with
`HLTHPRT_ADDRESS_CANON_SOURCES=nppes`,
`HLTHPRT_ADDRESS_ARCHIVE_TABLE=address_archive_v2`, and
`HLTHPRT_ADDRESS_ARCHIVE_CUTOVER=1`. Early progress confirmed the faster dev
host was actively loading: staged `npi_20260611` reached 300,001 rows and
staged `npi_address_20260611` reached 546,202 rows shortly after download and
parse began.

The first standalone dev NPI rebuild completed in `0:56:40.864599` and
published correct address-canonical coverage, but dev had an empty
`nucc_taxonomy` table, so the derived `npi_phone_staffing` table published with
0 rows. An ordered `nucc` -> `npi` run without explicit address-canonical
environment flags then proved the NUCC dependency and produced 154,583
staffing rows, but published 0 `npi_address.address_key` values because
canonical writes are gated by `HLTHPRT_ADDRESS_CANON_SOURCES`.

The final dev validation therefore ran full `nucc` first, then full `npi`,
with the address-canonical flags passed explicitly:

```bash
HLTHPRT_ADDRESS_CANON_SOURCES=nppes
HLTHPRT_ADDRESS_ARCHIVE_TABLE=address_archive_v2
HLTHPRT_ADDRESS_ARCHIVE_CUTOVER=true
```

Final ordered dev result:

| Phase / metric | Observed value |
|---|---:|
| NUCC runtime | 1.094 seconds |
| Full ordered NUCC + NPI wall time | 1:01:10.151387 |
| Published `nucc_taxonomy` rows | 883 |
| Published NUCC pharmacist taxonomy rows | 18 |
| Published `npi` rows | 9,606,683 |
| Published `npi_address` rows | 19,707,579 |
| Published `npi_address.address_key` populated | 19,694,747 |
| Published `npi_address` null `address_key` rows | 12,832 |
| Normalized-eligible US street rows still missing `address_key` | 0 |
| Published `npi_address` geocoded rows | 12,432,865 |
| Published `npi_address` rows with taxonomy arrays | 19,707,579 |
| Published `npi_taxonomy` rows | 12,019,685 |
| Published `npi_other_identifier` rows | 3,477,937 |
| Published `npi_taxonomy_group` rows | 1,105,012 |
| Published `npi_phone_staffing` rows | 154,583 |
| Published `npi_phone_staffing.pharmacist_count` sum | 327,504 |
| `address_archive_v2` rows after ordered run | 5,420,285 |
| `address_archive_v2` rows with NPPES source bit | 5,420,285 |
| `address_archive_v2` geocoded rows | 1,634,735 |
| `address_checksum_map` rows | 2,296,683 |
| `address_checksum_collision` rows | 0 |
| Staged `npi_address_20260611` leftover after publish | none |

Log evidence from the final dev run includes:

- `NPI canonical address keys already populated during load: 19694747`
- `NPI canonical address resolve complete: ResolveStats(staged=19707579, distinct_keys=5352155, inserted=0, ...)`
- `Updating NPI Addresses Geo from Archive address_archive_v2...`
- `Materializing phone staffing table mrf.npi_phone_staffing_20260611 from mrf.npi_address_20260611...`

The dev run confirms correctness for the dependency-ordered full import, but
also reinforces the same performance follow-ups observed locally: address
resolve materialization, taxonomy/geocode full-table rewrites, heavy index
builds, and `VACUUM FULL` dominate the shutdown tail.
