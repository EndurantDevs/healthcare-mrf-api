# PTG Import

## Purpose

The PTG importer publishes two distinct Transparency in Coverage data domains
for the pricing API:

- negotiated in-network rates; and
- historical allowed-amount evidence from allowed-amount files.

The only supported negotiated-rate architecture is `postgres_binary_v3` with
the `shared_blocks_v3` storage generation. Allowed-amount evidence is stored in
fixed, snapshot-scoped PostgreSQL tables under the same strict-V3 lifecycle,
but it is not encoded as negotiated serving blocks.

The import contract is intentionally strict:

- Serving data is durable PostgreSQL state.
- A physical layout is built from one complete selected set of in-network
  inputs and is sealed before any logical snapshot can use it.
- Identical complete physical input sets can reuse one sealed layout.
- Logical plan scope, snapshot identity, source pointers, and publication
  history remain independent even when physical bytes are shared.
- Source and price multiplicity are preserved exactly.
- Historical allowed amounts never replace or impersonate negotiated rates.
- Negotiated and allowed-evidence current pointers remain independent.
- Publication persists a bounded audit sample before sealing the layout.
- A sealed layout is first exposed as a non-serving `validated` candidate;
  audited activation is the only operation that may change live pointers.
- The API fails closed when any required architecture marker, block mapping,
  logical binding, scope row, or audit metadata is missing or inconsistent.

PTG rate files are price evidence. Provider locations are resolved from the
separate canonical address data used by the API; PTG does not publish a second
address-serving dataset.

## Public Service Boundary

This repository is a tenant-neutral public data service. It defines generic
import, audit, operator-control, and pricing contracts, but has no dependency
on a particular commercial dashboard, gateway, automation implementation,
subscription system, or orchestration product. External software may consume
the same authenticated control and public pricing APIs. Customer names,
subscription ownership, private service names, proprietary schemas, and
deployment-specific URLs must not appear in source, fixtures, reports, or
public documentation.

## Deployment Contract

Dev releases are deployed only through GitHub Actions from an exact `main`
commit whose `CI` workflow passed. Strict V3 writer staging and audited reader
promotion are phases of that CI-controlled path. Operator commands may inspect
the deployment and run smoke checks, but must not patch live workloads or
bypass the source and desired-state repositories.

## Start Command

```bash
python main.py start ptg
```

A release import normally supplies a table-of-contents URL and one unambiguous
plan scope. Several plan records may reference the same physical files when
their plan ID, ID type, and market resolve to that same logical scope:

```bash
HLTHPRT_PTG2_SNAPSHOT_ARCH=postgres_binary_v3 \
python main.py start ptg \
  --toc-url "<TOC_URL>" \
  --plan-id "<PLAN_ID>" \
  --plan-market-type "<MARKET_TYPE>" \
  --import-month "<YYYY-MM-01>" \
  --import-id "<IMPORT_ID>" \
  --source-key "<SOURCE_KEY>"
```

`HLTHPRT_PTG2_SNAPSHOT_ARCH` defaults to `postgres_binary_v3`. Any other value
is rejected. A direct `--in-network-url <URL>` is supported when the job still
provides enough metadata to establish one logical plan scope.

Useful diagnostic options include `--test`, `--max-files`, and `--max-items`.
They are for bounded fixtures and investigations, not release publication.
Release evidence must cover the complete selected source without truncation.

Strict imports accept in-network files, allowed-amount files, or a selected
table-of-contents set containing both. Provider references embedded in, or
referenced by, in-network files are resolved by the scanner. A separate
provider-reference input lane is not supported.

An allowed-only import must contain payment evidence. It publishes a durable
evidence snapshot with zero negotiated serving rates and advances only the
isolated allowed-evidence current pointer. A mixed import writes both domains
under one logical snapshot; activation advances the negotiated and allowed
pointers atomically. Failure of either compare-and-swap rolls back both pointer
changes. Reusing an existing negotiated physical layout does not discard or
reuse the new import's allowed evidence.

Allowed-amount files describe historical payments, not a current negotiated
network contract. Their default network status is therefore
`out_of_network_or_not_confirmed_in_network`. Only explicit source metadata may
mark them `in_network`; the importer never infers in-network status from a
matching plan, code, provider, or negotiated snapshot.

## Complete Input Identity

The importer downloads every selected in-network file before reserving a
physical layout. A download failure, missing artifact, unresolved plan scope,
or failed file aborts the import; strict publication never cuts over a partial
set.

Physical identity is deterministic and order-independent. It includes:

- the complete distinct set of selected in-network artifact identities;
- each artifact's source type, identity kind, digest, and identity byte count;
- `postgres_binary_v3`, `shared_blocks_v3`, the cold-read contract, and both
  multiplicity contracts; and
- the Rust scanner/finalizer binary plus the Python publication modules that
  define canonical output and physical layout.

When a decompressed logical JSON digest is available, equivalent containers
can share a physical identity. When that digest is deliberately deferred,
reuse is restricted to byte-identical containers. Adding, removing, or
changing any member of the complete physical input set changes the semantic
fingerprint.

URLs, source keys, plan identifiers, and descriptive ownership fields do not
define physical equality. They remain logical metadata. One negotiated import
can contain only one unambiguous logical plan scope, but two logical plans may
bind to the same sealed physical layout when their complete physical input sets
and scanner semantics are identical.

Allowed-amount rows are snapshot-scoped logical evidence and do not contribute
to negotiated shared-layout equality. An allowed file may cover multiple plans;
plan coverage is stored separately from its shared file-level item rows so one
physical source item does not need to be copied once per plan.

## Physical And Logical Ownership

`shared_blocks_v3` separates immutable physical storage from logical serving
ownership:

- `ptg2_v3_snapshot_layout` owns one building or sealed physical layout.
- `ptg2_v3_layout_fingerprint` maps a semantic fingerprint to that layout.
- `ptg2_v3_block` stores immutable content-addressed PostgreSQL payloads.
- `ptg2_v3_snapshot_block` maps logical block coordinates to payload hashes.
- Fixed PostgreSQL support tables store dense code, provider, price, graph,
  NPI-scope, and audit metadata for the physical layout.
- `ptg2_v3_snapshot_binding` binds each logical snapshot id to a sealed layout.
- `ptg2_v3_snapshot_scope` stores one immutable physical coverage scope for
  the logical snapshot.
- `ptg2_v3_snapshot_plan_scope` binds every logical plan record represented by
  that file set to the snapshot. Several employers or groups may legitimately
  reference the same complete physical file set.

A reused layout does not merge plan ownership. The importer stores and scans
one physical layout for an identical complete file set, then creates one
logical binding per plan record without copying rates. Each API request
resolves the requested logical snapshot and plan binding, follows the shared
layout binding, and joins physical code metadata to the snapshot's coverage
scope. Source-current and plan-current pointers remain logical and can move
independently.

Allowed evidence uses four fixed PostgreSQL relations for plan coverage,
billable items, payments, and provider payments. Each row carries its logical
snapshot id. The manifest binds the storage contract, counts, data domain,
source identity, and isolated current-pointer key. The API accepts the evidence
only when the snapshot is published and every one of those bindings agrees; it
does not fall back to a negotiated pointer or to another plan's evidence.

Reservation is serialized by semantic fingerprint. A sealed match is reused;
a matching live build is not duplicated. Publication also deduplicates an
identical completed mapping/support digest if two builds converge on the same
physical result.

Each build attempt has a unique fencing token. Dense writers hold a key-share
lock on the building layout, sealing holds an update lock through validation,
and the final state transition is token-conditional. GC therefore cannot
delete a layout underneath a writer, and a retry cannot reset or seal another
attempt's validated contents.

If publication fails before pointer cutover, failure handling immediately
abandons only the still-building, unbound layout owned by that attempt's token.
A token mismatch, sealed state, or logical binding makes abandonment a no-op;
transient cleanup failures are retried three times, and the bounded recurring
GC remains the fallback for a persistent database outage.

## PostgreSQL-Only Serving

A valid serving manifest must declare all of the following:

| Field | Required value |
| --- | --- |
| `arch_version` | `postgres_binary_v3` |
| `storage_generation` | `shared_blocks_v3` |
| `cold_lookup_contract` | `ptg_v3_cold_v2` |
| `price_membership_semantics` | `multiset_v1` |
| `serving_multiplicity_semantics` | `source_multiset_v1` |
| `provider_scope_strategy` | `postgres_shared_graph` |
| `shared_block_layout` | `dense_shared_blocks_v3` |

The manifest must point to a positive shared layout key and a canonical
coverage-scope digest. The layout must be sealed and the logical snapshot must
have matching binding and scope rows.

The PTG serving path reads only the required immutable PostgreSQL fragments for
each request and validates kind, coordinates, sizes, compression, hashes, and
dictionary bounds. It does not materialize a filesystem serving cache and does
not retain PTG block, graph, dictionary, or price payloads in a process serving
cache. PostgreSQL and operating-system page caches may still affect observed
latency; they are not alternate serving stores.

Serving manifests do not contain filesystem artifact URIs, retained serving
files, per-snapshot serving tables, or materialized-table fallbacks. Missing or
incompatible metadata requires a new import; the API does not infer another
layout.

### Forward Blocks And Cost Pagination

The authoritative by-code writer emits only `by_code_provider_shard_v1`
objects. It does not emit a monolithic or legacy by-code membership object.
Each shard covers a 1,024-key `provider_set_key` interval, with
`shard_id = provider_set_key // 1024` and
`block_key = (code_key << 31) | shard_id`. Fragments within each block are
numbered contiguously from `0`; a missing, repeated, or out-of-order fragment
fails the strict reader.

One `(code_key, provider_set_key)` may contain more occurrences than one block
can hold. The writer splits that logical group into adjacent continuation
chunks before its configured payload bound is reached. Equal provider keys are
valid only for those contiguous chunks, and `(price_key, source_key)` remains
globally ordered across the boundary. Readers reject a decreasing provider,
noncontiguous provider reuse, decreasing occurrences, or an oversized single
occurrence. Duplicate occurrences remain duplicate source facts.

Provider-to-code blocks retain their canonical wire format, but the finalizer
spools encoded entries through bounded import scratch and streams physical
fragments instead of retaining a complete 1,024-provider block twice in
memory. Import scratch is disposable; API serving still reads only PostgreSQL.

`by_code_price_page_v4` is a separate 64-row fast first-page projection. It is
not the authoritative membership stream and cannot replace the provider-shard
blocks. Standard cost-ordered provider searches use that projection for a
qualifying shallow page. Deeper pages use progressive exact selection: they
widen the forward prefix until they prove the requested window, then perform
sparse reverse completion only for the selected NPIs and provider sets.

Pagination always reports an exact `has_more`, including when obtaining a full
total would require exhaustive provider expansion. Such responses may also
include the optional `total_is_exact` and `total_lower_bound` fields. A false
`total_is_exact` means `total` and `total_lower_bound` are proven lower bounds,
not an estimate; clients must use `has_more` to decide whether to request the
next page.

### Geographic Filtering And Ordering

Provider locations come from the canonical address projection and are joined
to strict-V3 provider membership at request time. Nearest-first queries use a
PostGIS KNN prefix ordered by distance and numeric NPI. Duplicate address rows
cannot prove source exhaustion: the reader tracks whether the raw SQL probe,
not the deduplicated NPI list, was exhausted.

Taxonomy and price predicates are applied before a geographic page is declared
complete. Price-filtered searches first resolve matching provider-set keys and
then scan locations only for those sets. Cost ordering, descending-distance
ordering, and other non-monotone orders require an exhaustive bounded
selection; the API fails closed when the configured exactness bound is reached
before exhaustion. It never returns a page whose global order has not been
proved. Location responses use the same `has_more`, `total_is_exact`, and
`total_lower_bound` contract as forward and reverse pricing responses.

## Exactness Contracts

`source_multiset_v1` preserves source serving occurrences. Two source
occurrences that canonicalize to the same code, provider set, and price set are
still two occurrences. Exact validation compares multiplicity rather than
collapsing the source to a set.

`multiset_v1` preserves price membership multiplicity. Repeated atom
memberships remain distinct through atom ordinals, so equal price payloads can
be compared with their exact counts.

These contracts are part of physical identity and the serving manifest. A
change to either contract creates a different physical result and must be
published as an immutable replacement.

## Import Scratch And Publication

The scanner and finalizer use bounded-memory, temporary scratch rather than
durable serving files. The main temporary products are partitioned scanner
runs, code dictionaries, provider-graph conversion files, external-sort runs,
and PostgreSQL COPY inputs. Worker queues, input limits, decompression limits,
and finalizer record limits bound the work; capacity planning must still
reserve scratch for every concurrent unique physical build.

Each source-file scan owns a unique scratch directory. Manifest spill files
and late sidecar outputs use that same attempt scope, and the scanner recreates
an output parent immediately before finalization. Recovery cleanup applies its
retention age to empty directories as well as files: an empty but fresh
directory can be the reserved destination of a long-running scan and must not
be pruned.

The finalizer extracts and sorts provider identities inside the same bounded
nonempty-partition worker pool used for the source scan. It then performs a
sorted merge of those partition-local unique streams instead of globally
sorting one provider-identity record per source row. After dense assignment,
the block encoder consumes the fixed-width assigned records directly; it does
not synthesize a PostgreSQL COPY stream only to parse it again. The committed
block output remains PostgreSQL binary COPY and is byte-identical to the
reference encoder.

Strict V3 scanner workers emit canonical partition runs directly. They do not
construct legacy serving-row identifiers or retain a process-wide uniqueness
set for rows that V3 never publishes. The dedupe summary still reports the
exact attempted serving-row count from the completed worker runs, sets
`serving_rate_dedupe_enabled` to `false`, and leaves the legacy uniqueness,
duplicate, and reduction fields null rather than presenting unmeasured values
as zero. Any diagnostic path that emits a serving row retains its dedupe
invariant.

Negotiated-rate parsing remains bounded and streamed. The gzip reader and
top-level scanner frame rate objects into the existing bounded raw-chunk
queue; they never materialize the full file or `in_network` array. Workers use
a typed parser for the supported rate shape and preserve the same canonical
money, provider-reference, NPI, modifier, service-code, and optional-text
contracts as the strict streaming parser. A shape the typed parser cannot
represent is retried through that strict parser rather than accepted with
weaker validation. `typed_rate_parses` and
`streaming_rate_parse_fallbacks` in the scanner summary expose which path
handled every successfully parsed rate.

Gzip scanning uses `rapidgzip` by default. When `provider_references` appears
after `in_network`, the scanner builds a temporary seek index, reads the
provider range first, and then processes indexed in-network ranges in parallel.
The explicit disable switch remains diagnostic only. Release tests require
byte-identical COPY rows, serving runs, sidecars, and dedupe summaries at 1, 8,
and 16 workers. The finalizer likewise produces identical committed bytes and
support digest at those worker counts; its active-worker memory divisor counts
only nonempty partition jobs.

Code dictionaries remain worker-sharded. Every shard carries an exact byte and
row count, SHA-256 digest, dense physical-source key, and source-run contract
digest. Rust verifies these before allocating or decoding variable-length
fields and charges the conservative dictionary resident estimate to the
process identity-map limit.

Physical-source keys are not caller-selected identifiers. Python normalizes
each source identity as a lowercase ASCII token, one supported digest kind,
and a 32-byte digest; sorting that tuple assigns the contiguous keys. Rust
independently rejects unsupported identity kinds, duplicate identities,
noncanonical source types, and any key-to-identity ordering that differs from
that deterministic assignment.

The scanner summary also authenticates the complete code-dictionary shard set
for each physical source. Python turns its exact file, row, and byte totals into
a source-bound contract containing every shard digest. Each dictionary entry
carries that contract digest, while the finalizer manifest carries the complete
per-source contract set and its canonical SHA-256. Rust recomputes both levels
and compares the observed descriptors exactly. Recomputing only aggregate
manifest totals after omitting a shard therefore cannot produce an acceptable
finalizer input.

Scratch lives under temporary directories selected by the import runtime and
is removed after success or failure. Disposable PostgreSQL stages are unlogged
and dropped during publication. A scanner, graph-conversion, finalizer, COPY,
audit, or sealing failure cannot promote a partial layout.

Content-addressed source downloads may be retained by the input artifact
policy for provenance and download reuse. They are input records, not serving
artifacts, and the API never reads them.

### Retained Input Artifact Leases

Each PTG import creates an atomic marker under
`$HLTHPRT_PTG2_ARTIFACT_DIR/leases`. The marker is heartbeated every five
minutes by default and expires after six hours without a heartbeat. It holds
exact raw and resumable-partial paths plus logical-directory prefixes used by
that import. Multiple imports can reference the same content-addressed file
independently; releasing one marker does not make the file collectible while
another valid marker still references it. A normal return, failure, or
cancellation removes the marker, and a new publication releases it immediately
after PostgreSQL pointer cutover. Loss of the durable marker or a failed
heartbeat cancels the import before its inputs can become collectible.

Raw publication, logical expansion, lease updates, deletion, and
`manifest.jsonl` replacement share a cross-process store lock. Per-URL and
per-raw-digest locks serialize identical downloads and expansions without
copying the resulting file per import. Expanded logical JSON is atomically
renamed into its retained content path only after decompression and hashing
complete.

The input collector is dry-run by default:

```bash
python -m process.ptg_parts.ptg2_input_artifact_gc
python -m process.ptg_parts.ptg2_input_artifact_gc --execute
```

The default policy expires raw, logical, resumable-partial, and range-sidecar
files after 24 hours without a valid reference and can remove them under
capacity pressure after a one-hour last-unleased grace (and a one-hour file-age
floor) when their combined size exceeds 300 GiB. One cycle removes at most
2,000 files or 100 GiB, except that one oversized file may be removed so it
cannot become immortal. Every executing cycle also removes expired lease
markers, prunes obsolete unleased metadata, and atomically compacts the
manifest to its latest live records. A malformed or crash-torn manifest makes
cleanup fail closed; a later append isolates a torn tail so it cannot consume
the next complete record. Valid active references always win over age and
capacity pressure; if active inputs alone exceed the target, the collector
reports the remaining overage instead of deleting them. A legacy file without
retention metadata receives a durable first-observed timestamp and the same
grace instead of being deleted from an old filesystem mtime on the collector's
first run.

Negotiated or mixed publication proceeds in this order:

1. Discover, filter, download, and validate the complete selected input set.
2. Compute physical identity and reserve or reuse a layout.
3. For a reuse hit, publish only the new logical snapshot, scope, and pointers.
4. For a new layout, scan to bounded runs and disposable PostgreSQL stages.
5. Validate every source-bound scanner run and code-dictionary contract,
   including complete partition and shard vectors, aggregate rows and bytes,
   file hashes, deterministic dense source keys, physical source identity, and
   exact malformed-provider-identifier quarantine evidence.
6. Finalize dense blocks, dictionaries, prices, and all provider-graph
   directions.
7. Build and persist the served-occurrence audit sample and exact source
   witnesses captured during the normal scanner pass.
8. Validate every block mapping and support digest, then atomically seal the
   immutable layout.
9. Bind the logical snapshot and scope as a `validated` candidate without
   changing source, plan, or global serving pointers.
10. Queue the bounded PostgreSQL-witness-to-API release audit.
11. Persist a fresh passing attestation and atomically activate the candidate.
12. Remove temporary files and stages.

For a mixed import, allowed evidence is parsed and persisted before negotiated
layout publication. Its manifest index, metrics, source-file versions, and
predecessor pointer are retained whether the negotiated layout is newly built
or reused. Candidate activation changes both current pointers in one database
transaction. An allowed-only import skips the negotiated scanner, block build,
candidate audit, and negotiated pointer entirely; it publishes the evidence
snapshot and advances only its isolated pointer.

## Allowed-Amount API Semantics

The standard pricing endpoint consults allowed evidence only after the
negotiated query has produced no priced items and only when the request can be
represented without changing its meaning. A fallback request must identify a
plan and billing code. Supported evidence predicates include billing-code
system, NPI, place of service, modifier, plan market, source, and geographic
filters. A request for a negotiated rate or rate tolerance does not use the
fallback.

The response identifies the records as historical allowed amounts and includes
their conservative network status and semantics. Payment and provider rows
retain source multiplicity. Plan coverage is resolved through the dedicated
plan table, including a shared file that covers several plans; nullable
item-level plan metadata cannot make such evidence disappear or leak it to an
uncovered plan.

Provider locations are still joined from the canonical address projection at
request time. When `include_unverified_addresses=false`, location predicates
may be evaluated internally, but address, city, state, postal code, contact,
location, and distance fields are omitted from returned allowed-evidence
items.

Logical source traces and their source-trace-set identities always use full
domain-separated SHA-256, independent of the compact semantic-hash mode used by
other PTG values. This keeps source provenance acceptable to the shared-source
dictionary and release attestation without widening compact serving keys.

## Persisted Source Witnesses

The scanner captures exact source evidence while bytes are already being
parsed. It does not reopen, seek, decompress, or reparse an input after the
build. Selection happens only after the strict V3 writer accepts a rate, so the
authoritative population is the emitted, API-queryable price/provider
occurrence population rather than raw JSON row count. Each source contributes
deterministic local bottom-k candidates with contract
`bottom_k_atomic_occurrence_exponential_priority_v2`:

- negotiated-rate JSON plus its procedure fields, exact emitted price ordinal,
  exact provider-set NPI ordinal, and linked raw provider evidence;
- independently selected provider-reference JSON for provider parsing checks;
  and
- the verified raw-container SHA-256 and source-stable object/rate coordinate
  for every token. Worker identity and partition layout are not part of the
  selection key.

Publication selects exactly `min(total_population, 2,048)` records across all
sources. Up to 48 records are reserved for provider references; queryable
occurrences fill the remainder, and either cohort deterministically backfills
unused capacity. Both local and global population-derived counts are checked,
so an omitted queryable occurrence or provider candidate fails publication.
Per-source scanner candidates use a separate bounded intermediate bundle
(currently 512 MiB maximum) because a local bottom-k set can be larger than the
final cross-source selection. The immutable PostgreSQL witness remains capped
at 128 MiB; exceeding that final bound still fails publication.

The exact raw JSON token is preserved, individually zlib-compressed, and
protected by its own SHA-256. Publication merges all per-source bottom-k sets
deterministically, verifies complete source coverage and framing, then stores
one bounded payload in `ptg2_v3_source_audit_witness`. The layout and logical
snapshot manifests bind the payload digest, source-set digest, sample digest,
counts, and selection contract before sealing. Scanner bundle files are
temporary publication scratch and are deleted; PostgreSQL is the only durable
copy and API pods do not materialize a filesystem cache.

This witness is intentionally independent of the compact serving encoding.
The source half contains the exact original rate token and, for referenced
providers, the cryptographically linked provider token. Scanner metadata
retains only coordinates and contract identifiers. The release audit derives
the code, selected price, selected NPI, and required network names again from
the raw evidence before making an API request; it does not trust a
scanner-authored expected tuple.

## Persisted Served Audit Sample

Every new physical layout persists a deterministic publish-time sample in
PostgreSQL before sealing. The contract is
`persisted_served_occurrence_sample_v2` with method
`publish_time_stratified_v1` and contains at most 2,560 served occurrences.
Occurrence
identity includes source-candidate and atom ordinals, so duplicate source rows
and duplicate atom memberships remain independently auditable. The manifest
records the sample count, maximum, digest, method, format, and
`source_multiset_v1` marker. Reused logical snapshots expose the same physical
sample through their independent binding.

This served sample is publication evidence and a one-row preflight for the
release gate. A completed import is first stored as a `validated` candidate;
public pricing resolution remains `published`-only. The auditor may read
exactly one candidate through the control-authenticated audit aliases only when
snapshot, source, plan, and market selectors all match.

The activation audit loads the sealed source-witness payload from PostgreSQL,
checks all framing and manifest digests, reparses every selected raw token,
validates provider evidence, and then runs one exact standard pricing challenge
for every selected occurrence witness. With the normal 48-record provider
quota, a dense import runs 2,000 pricing challenges plus one served-sample
preflight: 2,001 no-retry HTTP requests. The absolute maximum is 2,049 when no
provider-reference records exist. Exact filters normally resolve in one page;
pagination is bounded to eight pages and all retries and extra requests are
counted rather than hidden.

One `aiohttp.ClientSession` and connection pool serves 32 concurrent requests
(hard maximum 64). Each request has a four-second timeout and one transient
retry. The complete audit has a fail-closed 55-second deadline that cancels all
unfinished requests. The candidate-audit ARQ worker and audit core both require
`uvloop`; the canonical attestation accepts only runtime evidence declaring
`aiohttp` on `uvloop`. A timeout, cancellation, source mismatch, API mismatch,
or unsupported runtime leaves the prior snapshot active.

Run the exact audit once for every pinned physical snapshot participating in a
multi-network plan. Then run an additional unpinned plan-level probe that checks
the merged response against the union of those audited snapshots and measures
cold first-page p95. Multi-network reads use separate PostgreSQL sessions with
bounded concurrency (`HLTHPRT_PTG2_MULTI_NETWORK_CONCURRENCY`, default 8); a
failed network read fails the whole request rather than returning a partial
union.

The exact tuple includes the source artifact's raw container SHA, so a correct
price attributed to the wrong input file still fails. The bounded activation
audit records request p50, p95, maximum, retries, and actual HTTP count, but it
does not claim the separate cold-process 40 ms capacity gate. Audit-only
requests have a 250 ms p95 operational budget because they are bounded release
proofs, not public serving traffic. Run the 40 ms standard-API gate from fresh
API processes with distinct keys under representative concurrent import and API
load; it does not imply database or operating-system cache eviction.

Published strict-V3 requests do not inflate the full snapshot manifest on the
hot path. Snapshot resolution and serving-table discovery validate the sealed
layout, logical scope, dense source dictionary, and consumed release
attestation through compact relational rows. The full snapshot manifest remains
authoritative for pre-activation candidate validation, but it is not a
per-request serving dependency after activation. A standard exact-NPI request
without geographic or taxonomy filters stays in dense graph keys, intersects
provider-set keys directly with the requested code shards, and never scans the
generic provider-page projection. Provider expansion enriches only the requested
NPI. Exact-NPI requests that also carry geographic or taxonomy filters retain
the full address or taxonomy validation path. The internal candidate-audit route
materializes only the challenged NPI and does not require provider-directory or
address enrichment.

Per-request audit latency starts after the request obtains its bounded
concurrency slot, so p50/p95 describe HTTP and API execution rather than local
semaphore wait. Complete audit wall time still includes queueing and is enforced
by the fail-closed deadline.

An out-of-range nonzero integral value in an NPI array is never padded, coerced, or
published as an NPI. The scanner excludes it from NPI membership, includes it
in provider-group identity so anomalous and clean groups cannot collapse, and
stores a bounded canonical quarantine summary in the immutable PostgreSQL
manifest. The summary records exact occurrence counts for at most 1,024
distinct malformed integer values plus a domain-separated SHA-256 digest. A
strict import fails if that bound is exceeded or if any scanner omits the
evidence. Publication validates the complete aggregate across scanner runs,
and the bounded release audit reparses the selected provider-reference tokens
against that sealed evidence. Mixed groups retain their valid NPI memberships
and remain auditable through the NPI API; TIN-only groups remain preserved but
do not create a fake NPI challenge.

Only the singleton array `[0]` is accepted as the schema-defined TIN-only
marker. Zero mixed with another value or repeated zero values are rejected so
an ambiguous provider identifier cannot silently become a TIN-only group.

The audit endpoint recomputes the complete bounded sample digest from
PostgreSQL before returning a page. A supplied logical `source_key` must match
the pinned snapshot, so the audit cannot silently validate another logical
owner that happens to share physical bytes.

After a passing release audit, the authenticated operator API stores the canonical redacted
report in `ptg2_v3_candidate_audit_attestation`. Attestation rechecks the sealed
layout binding, logical scope, complete PostgreSQL source-set digest, sealed
audit-sample digest, exact candidate selectors, audit-tool version, transport,
sample floors, and zero failures. A report must have completed within
`HLTHPRT_PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES` (120 minutes by default)
when it is accepted. The resulting single-use attestation expires after
`HLTHPRT_PTG2_CANDIDATE_ATTESTATION_TTL_HOURS` (24 hours by default); submitting
an old report cannot refresh that window.

Release audits use verified HTTPS by default. The validated-candidate lane may
instead use explicit authenticated cluster HTTP only when the configured
origin is a Kubernetes service DNS name and
`HLTHPRT_PTG2_CANDIDATE_AUDIT_TRUSTED_CLUSTER_HTTP=true`. That exception is
recorded in the attestation as `authenticated_cluster_service_v1`; it is not
available to published/public audit targets and does not permit arbitrary
private IPs, localhost, or external HTTP hosts. The deployment must restrict
the candidate Service with a NetworkPolicy to the audit workers.

Activation then holds the shared pointer/GC advisory lock and, in one database
transaction, locks and rereads the authoritative candidate, revalidates the
unexpired attestation against `clock_timestamp()`, compares and swaps the
source pointer against the candidate's exact immutable predecessor, publishes
the logical snapshot, reconciles global and plan pointers, and marks the exact
report consumed. Caller-supplied manifests are not authoritative. Any mismatch
rolls back all pointer and lifecycle changes, and generic publication helpers
cannot rewrite or repoint a strict candidate.

Routine orchestration is asynchronous and implemented by the generic
`ptg-candidate-audit` job: validation queues the authenticated audit, a passing
audit records the attestation, and promotion consumes it. These are generic
HTTP/control contracts; this repository neither knows nor depends on the
product operating the worker. Until an audit run has a durable control-run
record and heartbeat, generic age-based GC deliberately excludes every `validated`
candidate. Source-artifact leases are not part of activation because the audit
reads only the sealed PostgreSQL witness. Abandoned candidates require
explicit authenticated removal rather than an unsafe time-only guess.

The bounded PostgreSQL witness audit is the sole automated release verifier.
Activation never rereads or decompresses complete source files. The witness
contains authenticated raw source fragments captured at the actual V3 emission
point, so source-to-API comparison remains independent of serving storage while
its work stays fixed.

## Performance And Capacity Gates

The target for a new, unique large physical build is 10 to 15 minutes
end-to-end. The timer starts before source processing and ends only after
durable PostgreSQL publication and sealing, candidate-audit queueing and
execution, attestation, exact-predecessor pointer activation, and cleanup.
Reuse-only logical publication is measured separately and cannot be used to
satisfy the fresh-build target, but its nonzero duration is charged to the
reuse-adjusted capacity projection.

Current status: **the large-import measured gate is pending**. Repository unit
and integration coverage establish architecture behavior, but there is not yet
accepted dev evidence for a complete large import under this strict contract.
Do not cite historical runs from another layout or an incomplete measurement
as proof.

A qualifying measurement uses authenticated capacity schema version 7 and at
least 30 unique large builds. Every build must use the deployed release scanner
and schema,
logged durable relations, a fresh physical fingerprint, complete source
coverage, bounded scratch, persisted audit publication, and the release audit
floors above. The same report needs at least 30 reuse-only timing samples and
enough successful candidate-audit samples to cover every end-to-end sample.
The collector commits one joined stage record per logical import and signs the
measurement with a short-lived receipt bound to the exact release,
environment, and collector. Record build, reuse, audit queue, audit,
activation, and cleanup timestamps for the same logical import.

For 2,000 logical imports per 30-day month:

- At 10 minutes of build-lane work per unique build, the unreused workload is
  about 333 worker hours per month and 46 percent of one continuously available
  build lane.
- At 15 minutes of build-lane work per unique build, it is 500 worker hours and
  69 percent of one continuously available build lane. A passing end-to-end
  15-minute measurement necessarily leaves less than 15 minutes for that build
  stage because audit queueing, audit, and activation are included.
- One build lane therefore has little burst, retry, and maintenance headroom
  near that bound even though its theoretical steady-state capacity is 2,880
  builds per 30-day month. Candidate audits use separately measured lanes and
  availability. A full 2,048-record activation normally has 2,000 occurrence
  challenges plus one preflight, or 4,002,000 calls at the monthly objective.
  The provider-empty maximum is 2,049 calls per activation and 4,098,000 per
  month; measured retries and bounded pagination must be added rather than
  projected away.
- Physical reuse reduces build work only when the complete-set fingerprint
  matches. Capacity models must measure the reuse hit rate and keep an
  unreused scenario.
- Concurrent lanes multiply peak scratch, database connections, COPY traffic,
  WAL, I/O, and GC demand. Increase concurrency only from measured database
  headroom, not from CPU count alone.

Peak-arrival evidence must contain at least 30 timestamped, non-overlapping
windows of at least 30 minutes spread across at least seven days. Their
redacted list and cryptographic commitment must reconcile exactly. Maximum
import queue delay and
candidate-audit queue age are each fixed at 30 minutes. Both build/reuse demand
and audit demand must fit those SLOs; an observed low queue age does not expand
capacity.

The contention run lasts at least 30 minutes with every configured build and
audit lane active. It includes at least 3,000 requests and 1 request/second of
normal API traffic plus observed candidate-audit request totals whose duration
and derived rate reconcile with each audit's occurrence-witness count plus one
preflight (normally 2,001 requests) and actual retry/page counts. It must use
fresh API processes and separate error-free cold p95 measurements at or below
40 ms for at least 100 distinct matched-positive, 250 distinct negative, and
2,500 distinct deterministic-random requests. Every cold sample must fall
inside the signed contention interval.

Release evidence also includes pool wait, checkpoints, PostgreSQL temp space,
autovacuum, scratch, and GC. Connection, write, WAL, checkpoint time,
PostgreSQL temp, autovacuum workers, scratch, and GC throughput each retain at
least 20 percent headroom under the contention profile. GC covers at least 24
hourly cycles and 30 eligible/deleted layouts; positivity alone is not capacity
evidence.

## Immutable Replacement And GC

Sealed physical layouts and content-addressed blocks are immutable. To change
inputs, canonicalization, multiplicity semantics, block encoding, or required
support data, build and seal a new physical layout, publish a new logical
snapshot, and move pointers only after validation. Keep the prior snapshot
available until replacement and rollback requirements are satisfied.

Removing one logical snapshot removes only its binding and scope. A shared
layout remains protected while any other binding references it. Cleanup uses a
two-phase, bounded process:

1. Release an unbound sealed layout, or a stale expired build, only after lock
   and lease checks; queue its block hashes with a grace period.
2. Sweep only expired candidate hashes that have no remaining layout mapping,
   rechecking references under lock and enforcing row and byte limits.

The import failure path applies the same ownership and binding checks to remove
its own unpublished building layout immediately, so a normal retry does not
wait for the stale-build interval.

The cleanup command is dry-run by default:

```bash
python -m process.ptg_parts.ptg2_shared_gc
```

Apply one bounded cleanup cycle only after reviewing the plan:

```bash
python -m process.ptg_parts.ptg2_shared_gc --execute
```

Normal source-snapshot removal coordinates logical deletion with shared-layout
release. Do not delete shared PostgreSQL tables or block rows directly.

## Operational Acceptance Checklist

- The manifest contains only the strict architecture and exactness markers.
- The physical layout is sealed and its logical binding/scope agree with the
  manifest.
- No release import used truncation or partial-source options.
- Temporary scanner/finalizer files and disposable stages are gone.
- The PostgreSQL source-witness payload exists, validates against the sealed
  manifests and complete source set, contains exactly 2,048 combined witnesses
  for a large population (normally 2,000 queryable rate occurrences and 48
  provider references), and has no API-pod filesystem copy. Repeated linked
  provider JSON is stored once in the payload's SHA-256-keyed compressed
  dictionary; every occurrence reference, dictionary digest, byte count, and
  dictionary coverage set must validate before the audit can run.
- The persisted audit sample exists, validates, and contains no more than 2,560
  rows.
- The bounded candidate audit reparsed every sealed source witness, passed all
  selected API challenges and its served-sample preflight, ran on
  `aiohttp`/`uvloop`, completed within 55 seconds, and recorded its actual HTTP
  and retry counts.
- Cold first-page p95 is at or below 40 ms separately for matched-positive,
  negative, and deterministic-random requests.
- Audit-only source-witness endpoints may use the separate 250 ms p95 ceiling.
  That allowance does not relax the 40 ms cold ceiling for standard pricing
  endpoints.
- The authenticated schema-v7 monthly capacity report passes with at least 30
  qualifying large builds, 30 reuse-only samples, committed end-to-end timing
  for every logical sample, reconciled candidate-audit traffic, signed raw
  arrivals behind the timestamped seven-day peak profile, the 30-minute
  contention run, contention-bound resource observations, and all fixed
  resource headroom gates.
- Every accepted cold request is API-signed over its start, completion,
  monotonic duration, contention run, semantic class, selection ordinal,
  ordinal-zero process state, result count, status, and response digest. Its
  complete request interval remains inside all required build and audit lanes,
  and no API process identity is reused by another cold sample. An isolated
  evidence process accepts exactly one HTTP request: it must be the challenged
  canonical pricing route. Readiness traffic, aliases, and unchallenged or
  subsequent requests fail closed, so each cold sample starts a dedicated
  process outside the ordinary service load-balancer lifecycle.
- A claimed 10-to-15-minute large import is backed by a current complete
  measured report; until then the gate remains pending.
- Replacement and rollback bindings are retained as intended, and GC dry-run
  output is reviewed before execution.
- The strict-reader cutover preflight blocks fresh pending/running/building
  activity. Abandoned rows older than `HLTHPRT_PTG2_STALE_BUILD_SECONDS`
  (six hours by default) are reported separately as stale cleanup debt; they
  do not masquerade as live work forever.
