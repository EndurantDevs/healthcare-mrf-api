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

The imported layout must have passed the strict source-run and code-dictionary
source contracts. Those contracts use deterministic dense physical-source keys
and exact per-file SHA-256, row, and byte descriptors; they are build-time
completeness checks and are separate from this independent source-to-API audit.

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

This full-source harness is a qualification and forensic gate after scanner,
publisher, or serving changes. It is deliberately slower and is not the
synchronous activation gate for every import. After V4-capable readers are
fully deployed, automatic activation uses the bounded PostgreSQL source-witness
audit documented in `docs/imports/ptg.md`; that path submits one authenticated
V4 report assembled from exact max-100 partitions. It derives up to 10,000
exact pricing challenges and 1,000 provider records, validates the complete
served sample, starts no more than two requests per second, and applies a
55-second fail-closed deadline to each endpoint request.

To exercise this deep audit against a still-`validated` candidate, provide its
exact logical source and market selectors and use `--validated-candidate`:

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
header to a public pricing route does not grant candidate access. The deep
audit report is evidence but is not accepted by the automatic activation
attestation. In V4 mode, the generic `ptg-candidate-audit` worker submits sealed
candidate coordinates and digests; the authenticated API endpoint loads and
derives the import-time PostgreSQL witness, returns the bounded aggregate
result, and the worker stores its canonical redacted report before atomically
promoting the exact predecessor. No report path, retained source path, or local cache file is
accepted by promotion. The contract does not depend on a particular
orchestration product.

Activation attestation accepts only the bounded audit tool/version, explicit
`aiohttp`/`uvloop` runtime evidence, and a report whose
completion time is within
`HLTHPRT_PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES` (120 minutes by default).
It binds the report to the candidate's sealed source-witness digest,
persisted-sample digest, and complete source-set digest. The resulting
single-use PostgreSQL receipt is
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

This deep audit's release profile requires at least 2,500 independently selected source
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
maximum of zero. A candidate audit may set `--max-invalid-npis` from the sealed
snapshot's bounded quarantine count, but activation also requires the
source-derived values, counts, and digest to match that snapshot exactly. The
numeric ceiling alone does not authorize publication. Other `--max-*` options
can relax their gates only when the source exception is understood.

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

The report embeds the canonicalization version and rules. Version 5 applies:

* Null and stripped empty scalar strings become null.
* General scalar strings are stripped and otherwise remain case-sensitive.
* Code systems, billing codes, arrangements, and modifiers are upper-cased.
  Modifier array entries containing payer-packed comma-separated codes are split
  into individual modifiers before sorting and deduplication.
  Public API aliases and catalog widths are applied to code fields.
* NPIs must be integral decimals from `1000000000` through `9999999999`.
  Out-of-range nonzero integral values are excluded from NPI membership and reported
  through the bounded provider-identifier quarantine contract; they are never
  padded or coerced into synthetic NPIs. Valid NPIs in the same provider group
  remain eligible for exact API verification.
  Only the singleton array `[0]` is the schema-defined TIN-only marker; zero
  mixed with another value or repeated zero values are rejected. The marker is
  counted separately and never becomes an API NPI. The release profile fails
  when any TIN-only rate is present because the current NPI-addressed API cannot
  verify it exactly.
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

`ptg2_v3_capacity_gate.py` evaluates authenticated, fixed-schema measurements
for the 2,000-logical-imports-per-month objective. Schema version 7 rejects
older reports. Numeric assertions alone are not release evidence: every release
report embeds the redacted import-lifecycle, audit-result, raw peak-arrival,
peak-window, and HTTP sample rows used by the arithmetic and needs a fresh
Ed25519 collector receipt.
The receipt binds the expected release digest, environment identity, collector
id/version, contention run, observation interval, and cryptographic commitments
to those rows.

Create the fixed-schema example without target or customer identifiers:

```bash
python3 scripts/validation/ptg2_v3_capacity_gate.py \
  --write-example reports/ptg2-v3-capacity-input.json
```

The command deliberately exits `1`, writes `release_evidence: false`, and does
not create a receipt. It is a shape example only. Editing its values cannot
make it releasable.

The deployed collector, not this evaluator, creates the signed report. Release
automation installs the verifier trust policy at the non-overridable path
`/etc/healthporta/ptg2-capacity-trust.json`. It must be a root-owned regular
file with no group or world write permission. The expected release,
environment, key, and collector identities come from this protected file, not
from report contents or caller-controlled environment variables:

```json
{
  "version": 2,
  "algorithm": "ed25519",
  "key_id": "capacity-collector-2026-01",
  "public_key_hex": "<64-lowercase-hex-ed25519-public-key>",
  "api_evidence_key_id": "capacity-api-2026-01",
  "api_evidence_public_key_hex": "<64-lowercase-hex-ed25519-public-key>",
  "release_digest": "<64-lowercase-hex-release-digest>",
  "environment_id": "<64-lowercase-hex-environment-id>",
  "collector_id": "ptg2-capacity-collector",
  "collector_version": "7.0.0"
}
```

```bash
python3 scripts/validation/ptg2_v3_capacity_gate.py \
  reports/ptg2-v3-capacity-receipt.json \
  --output reports/ptg2-v3-capacity-report.json
```

The receipt is valid for at most 120 minutes, may trail the observation by at
most 15 minutes, and is rejected when expired, future-dated, signed by another
key, or bound to another release, environment, or collector. The signature
covers every measurement field and all commitment metadata. The canonical
signing bytes are exposed by `receipt_signing_payload()` so an independently
deployed collector can implement the same canonical JSON/Ed25519 contract; the
evaluator has no signing CLI, private key, or caller-selected trust path.
The API evidence key is separate from the collector key. The isolated API
process signs each eligible first-page response status, body digest, semantic
query digest, challenge, unique process identity, request start and completion,
monotonic server duration, contention run, semantic cohort, selection ordinal,
and result count. The gate derives latency from that signed duration and
requires the complete request interval to remain inside every configured import
and audit lane. A process can contribute only its signed ordinal-zero request as
cold evidence. In isolated evidence mode the process is single-use across the
entire HTTP application: its first request must be the challenged canonical
pricing path. A readiness request, alias route, missing challenge, or second
request is rejected and consumes that process, so the collector must launch one
fresh API process directly for each sample rather than place evidence workers
behind ordinary readiness or shared-service traffic. The source/API audit must receive
the same 64-character contention digest through
`PTG_AUDIT_CAPACITY_CONTENTION_RUN_ID` (or
`--capacity-contention-run-id`) that the resource collector records.

The release profile requires all of the following:

- At least 30 unique large builds must use fresh fingerprints, complete input
  coverage, the release scanner, durable publication, and persisted audit
  creation. At least 30 reuse imports are measured separately. A committed
  per-logical-import stage sample joins build/reuse, audit queue, audit,
  attestation, and activation timestamps for the same import. Every one of at
  least 60 samples must be complete and at or below 15 minutes; sums of
  unrelated aggregate maxima are never release evidence.
- Reuse-only work has at least 30 timing samples. Its measured duration is
  charged to reuse-adjusted worker hours and combined with the same audited
  activation path; a reuse hit is discounted only after audited activation is
  verified. Failed-attempt worker time must be nonzero for every reported
  retry, reconciles across unique and reuse imports, and is charged across all
  logical samples. Both unreused and reuse-adjusted lane utilization are hard
  release gates.
- Candidate audits have at least 30 successful samples and zero errors. Their
  lane count and availability are independent from build lanes, monthly and
  peak utilization must stay at or below 70 percent, maximum queue age is 30
  minutes, and every executed audit attempt is charged its exact max-100
  partition count. Observed planned, started, completed, failed, and retried
  request totals, duration, concurrency, and start rate must reconcile with each
  activation and the contention interval. Monthly audit HTTP demand is the sum
  of those measured per-audit partition counts; server-side occurrence, sample,
  block, witness, and projection work is measured separately and is not
  projected away.
- Peak evidence is a gap-free, fully covered sequence of individually
  timestamped windows spanning at least seven days; every window is at least 30
  minutes. Each window's logical, unique-build, reuse, audit, and queue counts
  are recomputed from separately committed raw arrival events. Summary counts,
  span, peaks, queue maxima, the window commitment, and the raw-event commitment
  must exactly reconcile. Capacity is evaluated from each observed window's
  actual workload mixture; maxima from different windows are never combined.
  The observed peak must meet the target's prorated average floor, maximum
  import queue delay is 30 minutes, and both weighted import work and
  candidate-audit work must fit their queue SLOs.
- Simultaneous import, candidate-audit, and normal API contention lasts at
  least 30 minutes. It covers every configured import and audit lane, at least
  3,000 API requests, at least 1 request/second, and one candidate-audit V4 POST
  per executed audit attempt within its measured duration. The evidence also
  reconciles server-side challenge and once-only processing ledgers, plus any
  explicit operator retries.
  Redacted timestamps must cover at least 99 percent of the contention interval
  with no import, audit, or HTTP observation gap greater than five seconds.
- Fresh processes produce separate cold first-page p95 values at or below 40
  ms for matched-positive, negative, and deterministic-random requests. The
  floors are 100, 250, and 2,500 samples respectively. Every sample in each
  class must use a distinct committed query-key digest; this deliberately
  exceeds a minimum-distinct-count rule so repeated fast keys cannot dilute a
  slow tail. Sample floors and p95 are computed only from observations made
  while every configured unique-build lane and candidate-audit lane is active.
  Aggregate p95, partially contended samples, out-of-window samples, and
  repeated-key evidence are not accepted. HTTP and per-class errors must all
  be zero.
- Scratch and PostgreSQL temp space retain at least 20 percent measured
  headroom. Pool-wait evidence covers the combined normal and candidate-audit
  request rate over the contention interval with p95 at most 10 ms, maximum at
  most 100 ms, and zero timeouts. At least two non-requested
  checkpoints complete with 20 percent time headroom; connection, write, WAL,
  and autovacuum worker capacity also retain 20 percent headroom. Resource,
  storage, and GC summaries are accepted only when their signed observation
  metadata binds them to the same contention run and fresh adjacent GC window.
- GC evidence spans at least 24 hourly cycles and 30 eligible/deleted layouts,
  has zero overlaps or reference-check failures, drains eligible bytes with 20
  percent throughput headroom, and keeps backlog within its declared bound.
  Retained storage must still fit after the unreused projection, and its
  logical sample must cover the population used to claim the reuse ratio.

Reuse reduces the capacity projection only when the measurement proves a
complete physical-input fingerprint match, an independently published logical
binding, production-like observation, and audited activation for every claimed
hit. Embedded rows remain redacted: identities and query keys are SHA-256
values, while lifecycle rows contain only fixed booleans, timestamps, counts,
and durations. Reports reject unknown fields and do not echo paths, target
identifiers, URLs, arbitrary JSON values, auth material, or exception text.
The evaluator target must exactly match the objective signed into the evidence;
a caller cannot substitute a smaller target on the command line.

Exit status is `0` only for authenticated passing release evidence, `1` for a
completed gate failure or `--write-example`, and `2` for invalid, unsigned,
stale, unauthenticated, or unavailable evidence.
