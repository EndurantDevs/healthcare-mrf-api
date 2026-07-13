# PTG2 v3 source-to-API audit

`ptg2_v3_source_api_audit.py` independently compares original TiC
`*.json.gz` in-network sources with the public pricing response for one pinned
plan and snapshot. It does not read Rust scanner output, PTG staging tables, or
serving artifacts.

## Required precondition

Confirm through deployment/import metadata that the supplied snapshot uses the
strict `postgres_binary_v3` contract before running. The harness also requires
the normal pricing response and persisted-audit endpoint to report
`postgres_binary_v3`, `shared_blocks_v3`, PostgreSQL storage, the pinned
snapshot, and plan-scoped pricing. A missing or mismatched marker fails the run.

## Run

Use environment variables for secrets and target identifiers so they do not
appear in shell history:

```bash
export PTG_AUDIT_API_BASE_URL='https://api.example.invalid'
export PTG_AUDIT_AUTH_TOKEN='...'
export PTG_AUDIT_PLAN_ID='...'
export PTG_AUDIT_SNAPSHOT_ID='...'

python3 scripts/validation/ptg2_v3_source_api_audit.py \
  --api-base-url "$PTG_AUDIT_API_BASE_URL" \
  --auth-token "$PTG_AUDIT_AUTH_TOKEN" \
  --plan-id "$PTG_AUDIT_PLAN_ID" \
  --snapshot-id "$PTG_AUDIT_SNAPSHOT_ID" \
  --report reports/ptg2-v3-source-audit.json \
  /data/source-001.json.gz /data/source-002.json.gz
```

The normal pricing endpoint defaults to
`/api/v1/pricing/providers/search-by-procedure`; the independently selected
served-occurrence sample defaults to
`/api/v1/pricing/providers/audit-occurrences`. Override them with `--api-path`
and `--api-audit-path` when calling the service directly. `--header NAME=VALUE`
supports API keys or other non-Bearer auth. Values from auth arguments and
headers are never emitted to the report.

New imports are not public until this audit passes. To audit a `validated`
candidate, also provide its exact logical source and market selectors and use
`--validated-candidate`:

```bash
export PTG_AUDIT_SOURCE_KEY='...'
export PTG_AUDIT_PLAN_MARKET_TYPE='group'

python3 scripts/validation/ptg2_v3_source_api_audit.py \
  --validated-candidate \
  --api-base-url "$PTG_AUDIT_API_BASE_URL" \
  --auth-token "$PTG_AUDIT_AUTH_TOKEN" \
  --plan-id "$PTG_AUDIT_PLAN_ID" \
  --plan-market-type "$PTG_AUDIT_PLAN_MARKET_TYPE" \
  --source-key "$PTG_AUDIT_SOURCE_KEY" \
  --snapshot-id "$PTG_AUDIT_SNAPSHOT_ID" \
  --report reports/ptg2-v3-candidate-audit.json \
  /data/source-001.json.gz /data/source-002.json.gz
```

Candidate mode forces the control-authenticated
`/api/v1/pricing/providers/audit-search-by-procedure` path. Supplying the same
header to a public pricing route does not grant candidate access. After a
passing run, submit the parsed report object to
`POST /control/v1/ptg/source-snapshots/attest` with the exact four selectors,
then call `POST /control/v1/ptg/source-snapshots/promote`. Promotion rechecks
and consumes the attestation in the same PostgreSQL transaction as all pointer
changes; it does not accept a report path or local cache file. Routine
deployments should have an authenticated, bounded audit worker perform these
steps asynchronously. The contract does not depend on a particular
orchestration product.

Attestation accepts only the configured audit tool/version and a report whose
completion time is within
`HLTHPRT_PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES` (120 minutes by default).
It binds the report to the candidate's sealed persisted-sample digest and
complete source-set digest. The resulting single-use PostgreSQL receipt is
eligible for promotion for
`HLTHPRT_PTG2_CANDIDATE_ATTESTATION_TTL_HOURS` (24 hours by default). These are
separate windows: an old report cannot be resubmitted to renew an attestation.

The persisted endpoint must expose the
`persisted_served_occurrence_sample_v2` contract, format version 2, source
multiset multiplicity, and source-key-aware occurrence identity. Both API
surfaces must return the selected occurrence's flat source identity and exact
source trace. When `--source-key` is supplied, both response contracts must
echo that logical key and the audit endpoint rejects another logical snapshot
owner. The endpoint recomputes the complete bounded persisted-sample digest
from PostgreSQL before returning each page.

The release profile requires at least 2,500 independently selected source
occurrences, 2,500 independently selected persisted API occurrences, 2,500
positive pseudo-random standard-pricing API requests, and 250 negative code/NPI
recombinations. Defaults request 500 negative candidates. Release mode rejects
lower `--random-api-calls` or `--min-random-api-calls` values, rejects
configurations whose random request target plus negative target is below 3,000,
and separately requires at least 3,000 observed HTTP requests to the standard
pricing endpoint.

The random request phase draws from source occurrence samples with replacement
using `--seed`. It reports distinct query keys, repeated draws, response
occurrences, failures, retries, cold/warm latency distributions, and only
SHA-256 fingerprints of its request plan and ordered responses. Each planned
logical request chooses a deterministic page size from 1 through
`--random-api-max-limit`, traverses every page, and compares the complete
response tuple Counter with the source Counter. The report separates planned
logical requests from actual HTTP page requests, including retries.
`--random-api-max-limit` cannot exceed the validated `--page-size` ceiling.
Pagination total/limit/offset/has-more metadata and the full
architecture/provenance
contract are checked on every page. The audit does not infer item count,
grouping, or ordering from tuple count, so distinct arrangements, physical
sources, or locations may produce multiple items without weakening exact tuple
multiplicity checks. Any missing, fabricated, altered, or multiplicity-changed
tuple fails the release.

Every normal pricing query is fully offset-paged. Its first traversal is
classified as cold and its immediate repeat as warm. The audit fails when
first-page first-observation p95 exceeds 40 ms or complete logical-query p95
exceeds 1,000 ms. "Cold" does not imply database or operating-system cache
eviction, so the release run must start from fresh API pods and use sampled
distinct keys rather than one warmed fixture.
Unresolved provider references, invalid prices, and invalid NPIs default to a
maximum of zero; the corresponding `--max-*` options can relax those gates only
when the source exception is understood.

Exit status is `0` for a passing audit, `1` for completed audit failures, and
`2` for configuration, source, HTTP setup, or internal fatal errors. A JSON
report is written for completed and runtime-fatal runs.

## Source and memory model

The source is read twice with `ijson` events:

1. Stream all top-level `provider_references` into a temporary SQLite index.
2. Stream `in_network` rates and negotiated prices and link references or
   inline `provider_groups`.

This supports `provider_references` before or after `in_network`. No complete
provider-reference record, in-network item, or gzip payload is materialized.
One negotiated price is held at a time, with `--max-list-values` providing a
hard bound for service-code and modifier arrays. SQLite stores normalized
relationships instead of expanding every provider-price cross product.
In-memory sampling is fixed-size seeded SHA-256 bottom-k sampling.
Sampled queries and random requests are audited in batches of twice the
configured concurrency. Random request planning uses bounded source occurrence
samples; each batch holds only the source Counters needed by that batch.
`--max-tuples-per-query` (default 10,000) is a hard bound on each source
counter; exceeding it is a coverage failure rather than an unbounded read.

## Exact canonical comparison

The report embeds the canonicalization version and rules. Version 2 applies:

* Null and stripped empty scalar strings become null.
* General scalar strings are stripped and otherwise remain case-sensitive.
* Code systems, billing codes, arrangements, and modifiers are upper-cased.
  Public API aliases and catalog widths are applied to code fields.
* NPIs must be integral decimals from `1000000000` through `9999999999`.
  The schema-defined TIN-only marker `[0]` is counted separately and never
  becomes an API NPI. The release profile fails when any TIN-only rate is
  present because the current NPI-addressed API cannot verify it exactly.
* Negotiated rates are finite base-10 decimals. Exponents are expanded,
  insignificant zeroes are removed, and negative zero becomes zero.
* Expiration dates must be exact ISO calendar dates (`YYYY-MM-DD`).
* Service-code and modifier scalar/list values become sorted unique canonical
  lists.
* Negotiated type, billing class, setting, and additional information preserve
  case after trimming.
* Negotiated-price multiplicity is retained after provider-membership union.
* Each occurrence includes the SHA-256 of the exact original JSON or gzip
  container bytes. Files with identical price tuples but different container
  bytes remain distinct occurrences.

The full compared occurrence is code system, code, NPI, negotiation arrangement,
negotiated type, negotiated rate, expiration date, service-code list, billing
class, setting, modifier list, additional information, and exact original
container SHA-256. A value change is an `altered` failure. Absent source tuples are `missing`; API-only tuples are
`extra_fake`; unequal counts for an otherwise exact tuple are
`duplicate_count`.

The harness requires detailed API output. It always sends
`mode=exact_source`, `include_details=true`, `include_providers=true`,
`include_sources=true`, `include_allowed_amounts=false`, the configured plan, and the configured
snapshot. An API response that omits negotiation arrangement will therefore
fail exact comparison when the source arrangement is non-null.

Every returned occurrence must contain a nonnegative `source_artifact_key`,
may contain the selected logical string `source_key`, and must contain
`source_type=in_network`, valid lowercase SHA-256 identity fields, a consistent
logical-versus-deferred identity mode, and a nonempty exact `source_trace`
whose rows contain `source_file_version_id`. Across the report, one source key
may have multiple trace rows, but it must map to exactly one raw container and
one exact identity/trace set. A raw container or source-wide trace union cannot
be attributed to multiple selected keys. The direct parser links container
hashes through its own SQLite `file_id`; it never assumes that local path names
match API metadata.

## Report privacy

Reports contain source fingerprints, redacted target SHA-256 values,
query/code/NPI hashes, occurrence fingerprints, mismatch field names,
counts, and latency summaries. Custom seeds are also represented by SHA-256.
They exclude source paths and file names, raw
plan/snapshot values, auth material, HTTP bodies, network/reporting entity
names, provider names, raw source hashes, source URLs, and arbitrary source/API strings. This keeps reports
machine-actionable without embedding company or client names.

The temporary SQLite index contains canonical source fields and is deleted at
the end of a run. `--work-dir` changes only the parent directory for that
temporary index; it does not preserve the index.

## Monthly capacity gate

`ptg2_v3_capacity_gate.py` evaluates aggregate production-like measurements
for the 2,000-logical-imports-per-month objective. It is separate from the
source audit because it needs a measurement window that includes concurrent
imports, normal API traffic, PostgreSQL write and WAL rates, scratch use,
retained storage, and garbage collection.

Create the fixed-schema example without target or customer identifiers:

```bash
python3 scripts/validation/ptg2_v3_capacity_gate.py \
  --write-example reports/ptg2-v3-capacity-input.json
```

Replace the example measurements with observed aggregates, then evaluate them:

```bash
python3 scripts/validation/ptg2_v3_capacity_gate.py \
  reports/ptg2-v3-capacity-input.json \
  --report reports/ptg2-v3-capacity-report.json
```

The gate fails unless the evidence covers at least 2,000 logical imports per
month, every qualifying unique build finishes within 15 minutes, worst-case
lane utilization is at most 70 percent, peak arrivals fit the observed queue
window, scratch cleanup succeeds, and PostgreSQL connection, write, WAL,
storage-retention, and GC capacity remain within their declared headroom. It
also requires at least 3,000 error-free API requests during the configured
concurrent import load, at least 2,500 cold first-page observations from fresh
processes, and cold p95 at or below 40 ms.

Reuse reduces the capacity projection only when the measurement proves a
complete physical-input fingerprint match, an independently published logical
binding, and production-like observation for every claimed hit. The report is
aggregate-only and rejects unknown input fields; it does not echo paths,
identifiers, URLs, arbitrary JSON values, or exception text.

Exit status is `0` for a passing capacity gate, `1` for a completed gate
failure, and `2` for invalid or unavailable evidence. A passing example proves
only that the gate works; release approval requires measurements from the
candidate deployment under representative concurrency.
