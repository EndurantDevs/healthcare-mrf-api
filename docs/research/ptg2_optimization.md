# Strict PTG V3 Optimization And Measurement

## Scope

This document defines the evidence required to optimize and release the only
supported PTG architecture: `postgres_binary_v3` with
`storage_generation=shared_blocks_v3`.

Optimization work may change implementation details only when it preserves the
published contract. Historical architecture comparisons and filesystem serving
measurements are outside this scope.

## Non-Negotiable Invariants

Every candidate must preserve all of the following:

- PostgreSQL is the only serving store.
- The API serves only sealed `shared_blocks_v3` layouts through an explicit
  logical snapshot binding and logical plan scope.
- PTG block, graph, dictionary, and price payloads are not materialized into a
  runtime filesystem cache or retained in a process serving cache.
- `source_multiset_v1` preserves duplicate source serving occurrences.
- `multiset_v1` preserves duplicate price-atom memberships and ordinals.
- One physical layout represents one complete distinct physical input set plus
  the exact scanner/finalizer and semantic contracts.
- Equal complete physical input sets may share a layout while plan, source,
  snapshot, scope, and pointer state remain independent.
- New physical publication is all-or-nothing, persists the audit sample, and
  seals immutable mappings before logical cutover.
- Scratch and unlogged PostgreSQL stages are disposable and removed on success
  and failure.
- Replacement creates a new immutable layout or a new logical binding; sealed
  state is never rewritten in place.
- GC cannot release a layout with a live binding or delete a block with a live
  mapping.
- Dense publication is fenced by a unique build token and a row lock, and
  finalization validates a source-bound complete run partition contract.

An optimization that relaxes one of these properties is an architecture
change, not a performance candidate.

## Current Implementation Shape

The scanner emits partitioned serving runs, source-bound code-dictionary
shards, and temporary provider-membership inputs. Every dictionary shard is
authenticated by SHA-256 and exact row/byte metadata before parsing, and its
resident-memory estimate is charged before allocation. A Rust finalizer performs bounded-memory external
sorting and emits dense block streams. Publication stores content-addressed
payloads and fixed support rows in PostgreSQL, validates the complete mapping,
persists the audit sample, and atomically changes the layout from `building` to
`sealed`.

Completeness is source-bound rather than inferred from top-level totals. Each
physical source has one deterministic dense key, one complete partition-run
contract, and one complete code-dictionary shard contract. Python constructs
those contracts from scanner-reported exact totals and canonical file digests;
Rust independently validates their schemas, canonical hashes, identities,
aggregate counts, descriptor sets, uniqueness, and source-key ordering. This
closes the coherent-subset case where a caller could otherwise omit one shard
and merely recompute aggregate manifest fields.

The physical semantic fingerprint is order-independent and covers the complete
artifact set and scanner/finalizer identity. Logical plan metadata is excluded
from physical identity and stored in snapshot-specific scope rows. This allows
cross-plan physical reuse without allowing one plan to observe another plan's
logical scope.

At request time the API resolves a logical snapshot, validates its strict
manifest, follows the sealed physical binding, and fetches only addressed
PostgreSQL fragments. Block readers validate stored hashes and metadata and do
not retain payloads between requests.

The release scanner enables indexed `rapidgzip` decompression by default. A
late `provider_references` array is sought and decoded first, after which 1, 8,
or 16 parser workers consume indexed in-network ranges with byte-identical
results. The finalizer schedules only nonempty partitions, so sparse layouts do
not dilute the process-wide sort-memory budget across idle workers.

The authoritative by-code writer emits only `by_code_provider_shard_v1`.
Provider-set keys are partitioned into 1,024-key shards, where
`shard_id = provider_set_key // 1024` and
`block_key = (code_key << 31) | shard_id`; every block's fragments are dense
and contiguous from `0`. The separate `by_code_price_page_v4` projection holds
at most 64 rows and exists only for fast qualifying first-page reads.

Large logical provider groups are emitted as bounded adjacent continuation
chunks. The reader accepts equal provider keys only while occurrences remain
globally ordered across the continuation, preserving duplicate multiplicity.
Provider-to-code logical bytes remain canonical while block assembly uses a
reusable import spool and bounded fragment buffer instead of a second complete
block allocation.

Standard cost-ordered provider pagination uses progressive exact selection for
pages that cannot be satisfied by the projection. It expands the forward
prefix only until the requested window plus one row is proven, then performs
sparse reverse completion for the selected NPIs and provider sets. Therefore
`has_more` remains exact without forcing exhaustive expansion. Pagination may
add optional `total_is_exact` and `total_lower_bound` fields when the complete
total is not yet known; a false `total_is_exact` marks `total` and
`total_lower_bound` as proven lower bounds.

## Evidence Status

| Gate | Status | Required evidence |
| --- | --- | --- |
| Strict architecture selection | Implemented in repository | Unsupported architecture values and incompatible manifests fail closed. |
| Complete-set physical identity | Implemented in repository | Order independence, content sensitivity, scanner identity, and one-plan-per-snapshot tests. |
| Independent logical binding | Implemented in repository | Two logical snapshots can share one layout while plan filtering and removal remain isolated. |
| Temporary scratch cleanup | Implemented in repository | Scanner and graph/finalizer failure tests leave no serving scratch. |
| Source and dictionary completeness | Implemented in repository | Per-source run and dictionary contracts bind exact file descriptors to canonical physical identities; Python/Rust parity and coherent-omission tests fail closed. |
| Exact multiplicity | Implemented in repository | Duplicate source candidates and duplicate atom ordinals survive publication and audit. |
| Persisted audit sample | Implemented in repository | Publish-time sample metadata, digest, row bound, and API readback tests. |
| Independent release audit | Tooling implemented | A qualifying release report must meet the fixed sample and latency floors. |
| Candidate attestation and atomic activation | Implemented in repository | Fresh report and sealed-sample binding, immutable validated row, exact predecessor CAS, wall-clock expiry, single-use receipt, and transaction rollback tests. |
| Automatic candidate audit orchestration | Implemented in repository | The generic `ptg-candidate-audit` job resolves one validated candidate, leases and verifies retained inputs, runs the release audit, records the attestation, and atomically promotes the exact predecessor. |
| Class-specific cold first-page p95 <= 40 ms | Pending release measurement | Fresh API processes, distinct keys, and complete first-page observations measured separately for matched-positive, negative, and deterministic-random requests. |
| Unique large import in 10-15 minutes | Pending dev measurement | Complete fresh build, logged PostgreSQL publication, audit, seal, and resource report. |
| 2,000 imports/month | Authenticated schema-v7 gate implemented; measurement pending | `ptg2_v3_capacity_gate.py` requires a fresh Ed25519 collector receipt, committed per-import end-to-end timings, 30 qualifying large builds and reuse samples, reconciled retry and audit HTTP cost, signed raw import and audit arrivals behind gap-free seven-day peaks, independently server-signed fully contended cold API samples, zero errors, and raw contention-bound resource telemetry. |

There is no accepted dev large-import proof for the strict architecture yet.
Do not promote the target or a projection to a measured result.

## Focused Repository Checks

The following tests cover the highest-risk architecture boundaries:

```bash
python -m pytest -q \
  tests/test_ptg2_shared_reuse.py \
  tests/test_ptg2_v3_cross_plan_reuse.py \
  tests/test_ptg2_strict_v3_import_scratch.py \
  tests/test_ptg2_shared_audit.py \
  tests/test_ptg2_shared_gc.py \
  tests/test_ptg2_candidate_attestation.py \
  tests/test_ptg2_strict_v3_snapshot_validation.py \
  tests/test_ptg2_v3_source_api_audit.py \
  tests/test_ptg2_v3_source_api_audit_capacity.py \
  tests/test_ptg2_capacity_evidence.py \
  tests/test_ptg2_v3_capacity_gate_adversarial.py
```

PostgreSQL-backed cases that are environment-gated must be run in a disposable
test database before release. A skipped database case is not evidence that the
database behavior passed.

The independent audit tool documents its release invocation and privacy model
in [the validation guide](../../scripts/validation/README.md). Inspect its
arguments without recording target details:

```bash
python scripts/validation/ptg2_v3_source_api_audit.py --help
```

## Correctness Measurement

Correctness is occurrence-based, not set-based. The canonical comparison tuple
contains code system, code, NPI, negotiation arrangement, negotiated type and
rate, expiration date, service codes, billing class, setting, modifiers, and
additional information. Counts must match for otherwise identical tuples.

The release profile requires all of these floors:

| Check | Floor or ceiling |
| --- | ---: |
| Source-selected exact occurrences | default 2,500; minimum 2,500 |
| Independently API-selected persisted occurrences | default 2,500; minimum 2,500 |
| Deterministic pseudo-random complete pricing requests | default 2,500; minimum 2,500 |
| Observed standard-API HTTP requests | minimum 3,000 |
| Negative code/NPI recombinations | default 500; hard minimum 250 |
| Cold first-page p95 per request class | at most 40 ms |
| Resolved-rate fraction | exactly 1.0 |
| Unresolved provider references | 0 |
| Invalid prices | 0 |
| Invalid NPIs | 0 |
| Invalid field types | 0 |

The source-selected sample is derived independently from original in-network
files and includes the exact raw container SHA in occurrence identity. The
API-selected sample comes from the persisted publish-time served
occurrences and receives no source-selected query keys. Negative checks combine
individually positive codes and NPIs that must not produce a false membership.
TIN-only provider groups use the schema-defined `[0]` marker and never create
a fake NPI. The audit reports them separately from NPI-addressable rates and
the release profile fails if any are present until a TIN-addressed API audit is
available.

The physical layout itself stores a deterministic, stratified publication
sample of at most 2,560 occurrences. It is generated before sealing and includes
source-candidate and atom ordinals, so `source_multiset_v1` and `multiset_v1`
multiplicity can be checked. This sample supports the API-selected release
floor but does not replace source-side validation.

## Latency Measurement

The release ceiling is cold first-page p95 <= 40 ms. For this contract, "cold"
means the first observed traversal for each distinct sampled key from fresh API
processes. It does not claim that PostgreSQL buffers, the operating-system page
cache, or upstream network caches were evicted.

Measure and report separately:

- first-page first-observation latency for matched-positive, negative, and
  deterministic-random requests, each with its own gate;
- complete logical query latency across all pages, also split by request class;
- immediate repeat latency for diagnosis only;
- page count, tuple count, concurrency, and timeout/error counts; and
- block kinds and PostgreSQL round trips used by each lookup class when
  profiling is enabled.

Warm measurements cannot satisfy the cold gate. Repeating one key cannot stand
in for the distinct-key release distribution. Report p95 only when the release
sample floor was completed without excluded errors.

For plans backed by multiple published network snapshots, measure both every
pinned snapshot and the unpinned merged-plan API. The merged path reads
immutable snapshots through separate PostgreSQL sessions with bounded
concurrency and must match the union of the individually audited snapshots
without partial success.

## Large Import Gate

The target is 10 to 15 minutes end-to-end for a unique large physical build.
The timer starts before source processing and ends only after durable
PostgreSQL publication, persisted audit creation, sealing, candidate-audit
queueing and execution, attestation, exact-predecessor pointer activation, and
cleanup complete. A release capacity claim requires at least 30 such builds;
one successful build is diagnostic evidence only.

A qualifying dev measurement must:

1. Use capacity schema version 7 and collect at least 30 representative large
   builds with the exact release scanner/finalizer and migrated schema intended
   for deployment.
2. Select a complete representative input set without diagnostic truncation.
3. Force a fresh physical semantic fingerprint for every qualifying build;
   collect at least 30 reuse-only durations as a different workload.
4. Use logged durable shared relations and the normal PostgreSQL transaction
   path.
5. Exercise bounded temporary scratch and prove cleanup after completion.
6. Persist and validate the publish-time audit sample.
7. Run the independent release audit at 2,500 source occurrences, 2,500 API
   occurrences, 2,500 pseudo-random complete pricing requests, at least 3,000
   observed standard-API HTTP requests, and 500 negative checks by default with
   a hard minimum of 250.
8. Collect at least 30 successful candidate audits with zero errors. Record
   audit lane count and availability, duration, queue age, at least 3,000 HTTP
   requests per activation, attestation, and exact-predecessor activation.
9. Start fresh API processes and pass cold first-page p95 <= 40 ms separately
   for matched-positive, negative, and deterministic-random requests.
10. Run at least 30 minutes of simultaneous configured import lanes,
    candidate-audit lanes, and normal API traffic. Record enough resource data
    to prove coexistence rather than extrapolating from tiny positive load.

The report must separate at least these stages:

- discovery and download;
- scan and temporary run generation;
- provider graph conversion;
- finalizer sort/merge and block encoding;
- PostgreSQL COPY and support-row publication;
- price and graph publication;
- persisted audit generation;
- mapping validation and seal;
- candidate-audit queue wait and audit execution;
- attestation, logical binding, and exact-predecessor pointer cutover; and
- scratch cleanup.

Also record peak process memory, peak scratch and PostgreSQL temporary bytes,
PostgreSQL relation and index bytes, WAL bytes, database CPU and I/O,
connection and pool-wait usage, checkpoint time, autovacuum workers, row/block
counts, retry count, and GC candidates created. Redact source names, plan and
snapshot identifiers, URLs, paths, target addresses, credentials, and exact
client-specific source sizes from durable reports.

Status: **pending**. Until a report satisfies this protocol, the 10-to-15-minute
number is a target only.

## Reclamation

Logical snapshot cleanup removes bindings first. An hourly, non-overlapping
shared-layout GC job reclaims unbound layouts and then unreferenced immutable
blocks under explicit row, byte, and layout limits. Stale building snapshots
are eligible only after their import run is absent, terminal, or has stopped
heartbeating beyond the configured threshold. This recurring sweep is part of
the 2,000-import/month capacity contract, not an operator-only maintenance
step.

## Reuse Measurement

Measure fresh physical builds and logical reuse separately:

- **Fresh build:** no matching semantic fingerprint exists. All scan,
  finalization, publication, audit, and seal work runs.
- **Preflight reuse:** an identical complete input set and scanner identity
  resolve to an existing sealed layout. The import records provenance and
  publishes independent logical scope, binding, and pointers without scanning.
- **Seal-time deduplication:** independently started work converges on an
  identical mapping and support digest; the already sealed layout wins and the
  redundant building layout is released.

A failed fresh build is abandoned immediately only when the row is still in
the building state, has no logical binding, and carries the failing attempt's
fencing token. Interrupted cleanup remains covered by the recurring stale-build
sweep, but ordinary retries do not wait for that interval.

For reuse evidence, verify that logical snapshots have different ids and plan
scope rows, share the intended physical layout key, return only their own
logical plan, and remain available when another binding is removed. A content,
artifact-set, or scanner identity change must miss preflight reuse.

## Capacity For 2,000 Imports Per Month

Use a 30-day month for comparable arithmetic. With no reuse:

| Build-lane time | Build hours/month | One-lane utilization | Theoretical one-lane maximum |
| --- | ---: | ---: | ---: |
| 10 minutes | 333.3 | 46.3% | 4,320/month |
| 15 minutes | 500.0 | 69.4% | 2,880/month |

These are steady-state build-lane bounds, not an operations plan or an
end-to-end latency claim. At 15 minutes, one lane has only 30.6 percent time
headroom before burstiness, retries, deployment drains, database maintenance,
or GC. Candidate audits consume separately configured lanes, while their queue
age, execution, and activation durations still count toward end-to-end import
latency.

Let `U` be the observed fraction of logical imports that require a unique
physical build. The gate calculates build-lane and audit-lane hours separately:

```text
build_lane_hours = 2,000 * (
    U * retry_adjusted_unique_build_minutes
    + (1 - U) * reuse_only_minutes
) / 60

audit_lane_hours = 2,000 * (
    candidate_audit_minutes + activation_minutes
) / 60
```

Queue age consumes end-to-end latency but not lane service time. Do not assume
a reuse discount until production-like fingerprints and audited activation
demonstrate it. Track logical-import, unique-layout, and audited-activation
throughput.

Schema-v7 release evidence uses these fixed representativeness policies:

- At least 30 fully qualifying representative large builds, 30 reuse-only
  samples, and a directly observed end-to-end stage record for every logical
  sample. Each record joins the same import's build/reuse, audit queue, audit,
  attestation, and activation timestamps. Every committed record completes in
  at most 15 minutes; unrelated aggregate maxima cannot be added to claim that
  result. All candidate-audit and API error counts are zero.
- Candidate-audit queue age and import queue delay are each at most 30 minutes;
  import and audit lane utilization are at most 70 percent.
- Timestamped peak windows of at least 30 minutes form a gap-free, fully
  covered sequence spanning at least seven days. Counts, span, peaks, and queue
  maxima reconcile to the committed redacted window list. The observed peak
  meets the target's prorated average and both import and audit demand fit the
  fixed queue SLOs.
- Simultaneous configured build lanes, audit lanes, and API traffic run for at
  least 30 minutes with at least 3,000 normal requests and 1 request/second.
  Candidate-audit request totals, duration, and derived rate reconcile both in
  the sample population and contention interval, covering at least 3,000 HTTP
  calls per active audit and 6,000,000 projected calls per 2,000 activations.
  The signed raw timestamps cover at least 99 percent of the contention interval
  with no import, audit, or HTTP observation gap above five seconds.
- Fresh-process cold first-page p95 is at most 40 ms independently for at
  least 100 matched-positive, 250 negative, and 2,500 deterministic-random
  samples and distinct keys. All three committed sample sets are collected
  inside the simultaneous contention interval.
- PostgreSQL pool wait covers the combined normal and candidate-audit request
  rate over the contention interval with p95 at most 10 ms, maximum at most 100
  ms, and no timeout. At least two non-requested checkpoints complete.
- Connections, COPY/write, WAL, checkpoint time, PostgreSQL temp, autovacuum
  workers, scratch, and GC throughput retain at least 20 percent headroom.
- GC spans at least 24 hourly cycles and 30 eligible/deleted layouts with no
  overlap or reference-check failure. Storage and backlog projections fit
  their declared retention and reserve bounds.

The report also retains the measured reuse hit rate, retry and failed-build
work, shared-block growth after content-addressed deduplication, and bounded
layout release/block-sweep throughput.

Numeric JSON is not a release credential. The deployed collector signs the
canonical schema-v7 measurement with Ed25519. Each challenged HTTP observation
is separately signed by the isolated API process using a distinct Ed25519 key.
That API signature covers the server-received and completion timestamps,
monotonic duration, contention run, semantic class, selection ordinal,
ordinal-zero cold-process claim, result count, status, and body digest. Latency
and full-contention membership are derived only from these signed fields, and
every accepted cold sample must have a unique process identity. Isolated
evidence mode is application-wide and single-use: the first request must be the
challenged canonical pricing path, while readiness probes, route aliases,
missing challenges, and second requests fail closed and consume the process.
The evaluator loads both public keys only from a fixed, root-owned trust file;
the collector cannot manufacture API latency evidence. Its receipt is
valid for at most two hours and binds the expected release digest, environment
digest, collector id/version, contention run, observation timestamps, and
SHA-256 commitments to embedded redacted import, audit, peak-window, and cold
request rows. The evaluator rejects examples, unsigned reports, identity
mismatches, stale receipts, tampering, and incoherent raw rows or aggregates
before running capacity arithmetic.

Adding worker lanes is acceptable only while PostgreSQL and scratch stay within
measured limits and the API keeps the cold first-page gate. A scheduler should
cap concurrent unique builds independently from fast logical reuse jobs.

## GC And Replacement Experiments

Optimization experiments must preserve immutable replacement:

1. Build and seal a new layout or reuse an existing exact physical match.
2. Publish a new logical snapshot and scope.
3. Move pointers only after correctness and serving validation.
4. Retain the prior binding for the required rollback window.
5. Remove obsolete logical snapshots through the normal snapshot-removal path.
6. Release only layouts with no remaining binding and expired leases.
7. Queue mapped block hashes with a grace period, then sweep only unreferenced
   hashes in row- and byte-bounded batches.

GC measurement must report dry-run and executed layout counts, candidate and
deleted hashes, stored bytes, grace timing, batch limits, and final reference
rechecks. Demonstrate that a concurrent new binding prevents layout release and
that a concurrent mapping prevents block deletion.

Run the metadata-only plan first:

```bash
python -m process.ptg_parts.ptg2_shared_gc
```

Execution is a separate reviewed action:

```bash
python -m process.ptg_parts.ptg2_shared_gc --execute
```

## Promotion Decision

Promote a PTG optimization only when the same candidate passes focused tests,
PostgreSQL-backed isolation tests, exact source-to-API release audit, cold
latency, unique large-import timing, scratch cleanup, durability, and capacity
headroom. A smaller fixture can reject a candidate but cannot prove the large
import or 2,000-import monthly gates.

The promotion record should contain only redacted digests, aggregate counts,
timings, resource metrics, and pass/fail reasons. It must not contain company,
client, plan, network, source, environment, path, host, or credential details.
