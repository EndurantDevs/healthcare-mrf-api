# PTG V4 Provider-Graph Improvement

Status: additive dev rollout. This is not a replacement or rewrite of the PTG
V3 review material.

## Compatibility boundary

PTG V3 remains the reviewable production baseline for at least the next one to
two months. This release does not rename or remove V3 tables, alter the V3
logical manifest contract, rewrite V3 documentation, or garbage-collect the
retained V3 snapshots used for review.

V4 is a separately authenticated provider-graph projection under
`ptg2_v4_*`. It deliberately reuses the immutable V3 snapshot binding,
provider-set dictionary, exact cold NPI/group relations, and content-addressed
block store. Sharing those immutable foundations avoids duplicating storage;
disabling V4 leaves the V3 reader and stored snapshot unchanged.

The reviewed V3 candidate and oracle remain pinned to their original immutable
image. A new-image V3 import is intentionally assigned the new scanner and
publisher physical fingerprint; it must not reuse an older layout merely
because V4 is disabled. This preserves the retained review artifact without
weakening physical reuse safety.

## Representation

The scanner retains exact common facts:

```text
provider set -> source component -> canonical provider group <-> NPI
```

The compiler derives a snapshot-local incidence quotient:

```text
group -> pattern <-> provider set
NPI -> pattern
```

Pattern identifiers are physical, snapshot-relative coordinates. They never
participate in logical provider identity or cross-snapshot digests.

For each sealed snapshot compilation, one deterministic chooser compares complete
`encoded_persistent_projection_v1` costs and publishes one graph
representation:

- direct layout for small, low-fanout graphs;
- pattern layout for fragmented graphs;
- exact source-component traversal only for pattern-overflow owners that remain
  within the declared online work limits.

The choice is automatic and shape-driven. Each candidate cost includes the
post-bitmap member and locator blocks, dictionaries, ordered-prefix projection,
the exact packed coordinate-map payload, its pack/root rows, relation
manifests, and heavy-owner metadata. This is an exact versioned
application-encoded persistence projection, not an estimate of PostgreSQL
heap, index, or TOAST allocation. The chooser seals the complete generic
compiler-option vector and rejects a candidate whose online-work,
prefix-storage, factor-edge, or model-memory eligibility proof fails. It fails
closed if neither candidate is eligible and selects direct only on an exact
eligible-cost tie.

The decision does not inspect a source, carrier, plan, subscription, import,
or canary identity. Equal graph geometry and compiler options with different
source metadata therefore have the same representation decision, costs, and
decision digest; a different incidence shape or option vector may choose
differently. The selected and losing candidate geometry, eligibility reasons,
encoded costs, option vector, policy, and decision digest are retained in
progress, the graph summary, and the sealed manifest. Publication reconstructs
the packed map and rejects any selected-candidate coordinate, pack, object-kind,
payload-byte, or total-cost drift.

The direct flat set/group expansion is not published for a pattern-shaped file.
The compiler uses component and component-tuple memoization, so deriving
patterns does not recreate the logical set/group expansion in scratch.

## Bounded provider prefixes

The public provider page is bounded to 200 results. The compiler targets an
ordered 201-NPI prefix, including one continuation sentinel, for every provider
set. It proves every candidate prefix from exact factor counts and bounded
group-first traversal before choosing a representation.

The two candidate layouts account for and publish that projection differently:

- `direct_v1` includes one authenticated prefix metadata row and ordered vector
  for every provider set, including empty and short sets. This makes bounded
  provider serving complete before direct set-to-group traversal.
- `pattern_v1` retains the smaller sparse projection. It publishes exact
  prefixes only for owners that cannot prove the requested page within the
  online group, source-owner, source-member, page, byte, or second-hop limits.

Pattern overflow normally uses the bounded source-component first hop. An owner
whose component degree exceeds that fallback cap is pattern-eligible only when
the sparse projection contains its exact authenticated target-length prefix.
Serving applies and removes those owners before loading set-pattern or
set-component relations, so a covered owner never enters the capped component
walk. An uncovered over-cap owner makes pattern serving ineligible.

Every stored prefix is an exact ordered vector, not a bitmap. It is
content-addressed, digest-checked, and globally owner/member-byte capped.
Requests at or below the online target use the bounded hot path. Larger or
unbounded internal requests use the separately metered exact cold V4 traversal;
they do not treat a bounded prefix as the complete provider set.

New direct manifests prove complete prefix coverage with all of the following:

- `selected_layout=direct`;
- `direct_layout_complete_prefix_eligible=true`;
- `observe.npi_prefix_override_owner_count` equal to
  `observe.provider_set_count`;
- the prefix metadata COPY row count equal to that same provider-set count; and
- prefix relation member geometry equal to the authenticated aggregate count.

Pattern manifests retain sparse owner counts and authenticate separate
component-over-cap, exact-prefix-covered, and uncovered-unsafe counters. Summary
validation recomputes the eligibility equation before publication.

Existing sparse `direct_v1` snapshots remain readable. When a set has no stored
prefix metadata, the reader uses the existing bounded exact direct fallback;
it does not infer completeness or rewrite the snapshot. Complete direct prefixes
and the new diagnostics are produced by reimport, not by relabeling or
repacking an older layout.

Provider-expanded CPT serving reads rates in sealed 64-row pages and loads only
the provider sets mathematically needed for the requested ordered prefix. Once
the prefix is selected, it intersects each selected NPI with the compact exact
CPT-to-provider-set scope and completes every selected NPI across later
matching sets and rates. This avoids a fast but incomplete first-page answer.
The sealed request caps are 256 rate rows, 64 distinct provider sets, and 64
graph batches; exceeding a cap fails closed and requires an explicit cold
request.

Exact cold NPI-to-group and group-to-NPI indexes remain online in this release.
Their removal, if ever justified, requires an API-usage audit after rollout.

## Packed serving and storage

Snapshot coordinate-to-hash mappings are packed into authenticated immutable
pages. Member and locator pages use the existing content-addressed block store,
and readers fetch each distinct page once per request before native
intersection/decoding. Caches and per-request database work are bounded.

There is no blanket gzip layer. Compression may be added later to a measured
relation only when its physical `pg_total_relation_size` reduction exceeds its
decode and CPU cost.

Physical storage acceptance separately measures tables, indexes, TOAST, packed
maps, every map-reachable CAS block, ordered prefixes, prefix metadata and
digests, and diagnostics with `pg_total_relation_size` plus owned/shared
attribution. The compiler's direct candidate includes the complete all-set
prefix relation and metadata; the pattern candidate includes only its sparse
unsafe-owner prefix relation and metadata. Shared relations are counted once.
The canary reports the signed residual and ratio between the selected
application-encoded projection and the measured physical graph footprint.
Activation fails when either the generic physical ceiling or the reviewed
estimator-drift budget is exceeded. Acceptance reports both the V4 graph
footprint and the whole coexisting snapshot footprint; neither number is
relabelled as the compiler cost.

The storage ceiling is not a canary command-line input. A source-controlled
policy binds each rollout case only to its frozen V3 snapshot, authenticated
raw source-set digest, source count, retained base-layout logical bytes, and
physical ceilings. It does not bind or predict a V4 representation. The sealed
adaptive decision is the sole representation authority. Unknown or changed
source sets fail closed until a reviewed policy change records the new
immutable physical baseline, while source renaming alone cannot alter the
shape decision. Individual raw source hashes are never stored in the policy.

The first release deliberately marks all three roster entries as
measurement-only. It records exact `graph_gate_bytes` and
`snapshot_gate_bytes` from `pg_total_relation_size`, but cannot pass the
promotion gate because no V4 physical result has been approved yet. Input-size
or factor-edge formulas are not evidence that storage is minimal, and they are
not used to self-approve promotion.

Approval is a two-step, source-controlled workflow:

1. Run the exact source-bound V4 canary and retain its snapshot id, import-run
   id, immutable image identity, graph gate bytes, and whole-snapshot gate
   bytes. This run must fail promotion with a clear unapproved-measurement
   result.
2. Review those physical measurements, check in both absolute byte ceilings
   plus a small explicit basis-point tolerance and the measurement provenance,
   then build a second image and reimport/reaccept. The checked-in ceiling must
   equal the measured value plus exactly that tolerance; it cannot contain
   hidden extra headroom.

For this rollout the tolerance is fixed at exactly 200 basis points (2%).
Both physical-storage and graph-read first passes emit a canonical,
case-bound measurement-evidence object and SHA-256. A checked-in approval must
name the same frozen reference snapshot, reproduce that digest, and derive
every ceiling from the measured value plus exactly 2%. Operator-provided graph
ceilings are not accepted.

The compiler-authenticated factor resources remain in the acceptance report as
scale evidence. The retained base-layout value is bound only to the frozen V3
reference. The V4 factored logical byte count is measured independently and
must reconcile across the sealed layout, completed packed-map root, and exact
map rows. The report also records both values, the policy digest, and exact
snapshot/import identity. Widening a limit therefore requires a reviewed image
change; extra `accept` arguments cannot change it.

Runtime metrics separately report physical graph bytes/pages/lookups, second-hop
group-to-NPI work, and the actual provider-expansion rate rows, distinct sets,
graph batches, and cap rejections. This keeps a low-latency answer auditable
against the sealed work model.

## Provider-group tax-identity sidecar

V4 retains one version-neutral tax-identity sidecar row for every canonical
provider group, keyed by the shared
`(snapshot_key, provider_group_global_id_128)` identity. The scanner emits this
row while the source TIN is still available, including for TIN-only groups with
an empty NPI array. It classifies each row as `matched_ein`, `missing`,
`malformed`, or `unsupported_type`; only an exact EIN expressed as nine ASCII
digits or `NN-NNNNNNN`, with outer ASCII whitespace allowed, is matchable.
Unavailable states remain valid billing-identity diagnostics and do not make a
snapshot unpriceable.

Raw TINs and business names are not retained in graph artifacts or PostgreSQL.
For `matched_ein`, the scanner computes a policy-scoped HMAC from the canonical
TIN type and nine digits using a file-mounted 32-byte secret. The snapshot
stores the full 256-bit HMAC as authority, its first 128 bits only as a
candidate locator, and a dense snapshot-local `tin_key`. The key and locator
must never be treated as durable cross-snapshot identities. Cross-source
connectors join by the manifest `token_policy_id` plus the full HMAC and verify
the full value in constant time after candidate lookup.

Release 1 freezes the exact wire contract. The HMAC message is
`healthporta.ptg.tin.v1`, one NUL byte, the ASCII `ein` length as unsigned
16-bit big-endian plus `ein`, then the nine-digit value length in the same
format plus the value. The policy ID is
`ptg-tin-hmac-sha256-v1:release-1`. Its descriptor is
`PTG2V4TINPOLICY\x01` followed by five independently unsigned-32-bit
big-endian-length-prefixed ASCII fields: policy ID, normalization contract,
HMAC contract, candidate-prefix contract, and full-authority contract. The
descriptor is 208 bytes and its SHA-256 is
`a0c06f5494f80663686be6861038a8804d9509d0fdc2d2c8cc56c259e53d761c`.

The manifest authenticates the policy descriptor, normalization and HMAC
contracts, per-state counts, a deterministic sorted source-shard ordinal map,
its digest, and the sidecar content digest. Each group also carries the exact
source-shard bitmap under that ordinal map. A partial reverse index on
`(snapshot_key, tin_key, provider_group_global_id_128)` contains only
`matched_ein` rows so TIN-to-group lookup is bounded without making unavailable
states matchable.

Policy secrets are supplied only through scoped worker file mounts; they never
enter arguments, environment values, logs, manifests, artifacts, or config
dumps. Every token policy referenced by a live layout must remain computable by
the trusted connector until that layout is retired, or the layout must be
reimported before the old policy secret is removed.

The sidecar publishes atomically with the V4 provider-group dictionary and is
immutable under the same building-to-complete lifecycle. Completion proves
exactly one sidecar row per provider group, exact state and token counts,
contiguous source ordinals, nonempty correctly sized bitmaps with no unused
high bits, and no unmatched group in the reverse index. Snapshot removal and
layout cleanup cascade through the sidecar without a separate destructive
path. Pre-sidecar V4 snapshots remain readable and removable.

This foundation does not change the V4 factoring or traversal algorithm, but
it does change physical storage and import work. Therefore every V4 storage
baseline, import ceiling, and canary approval is remeasured after the sidecar
is present. The direct baseline, provider-fragmented, and reference-extreme
cases remain neutrally identified in source control. No earlier V4 measurement
can be used as the final approval value.

## Source-scoped taxonomy selection

V4 chooses inferred-taxonomy projection geometry from the exact NPIs reachable
in the source being compiled, rather than from the installation-wide NPI
catalog. Before the graph compiler runs, a Rust prepass writes a sorted,
deduplicated binary PostgreSQL COPY stream of `(npi_key, npi)` rows and binds it
to the authenticated dense NPI-to-group reciprocal graph, its shard vector,
byte count, and SHA-256. The file lives only under a randomized, mode-private,
run-owned scratch directory. It is not placed in CAS, a shared layout,
snapshot metadata, or the publication manifest.

The taxonomy lookup pins one database connection. A short first transaction
creates a session-local TEMP scope table, and a second transaction starts with
`REPEATABLE READ READ ONLY`, authenticates and COPYs the same open file
descriptor, and performs only the bounded joins needed for the selected
source scope. Closing the connection removes the TEMP table. Successful,
failed, canceled, and hard-death lifecycle tests require zero persistent
relation, CAS, packed-map, or manifest delta.

The compiler receives only the resulting authenticated candidate vector. It
computes direct and pattern costs from the same immutable source scope, selects
one layout, and emits one selected taxonomy COPY. Publication authenticates
that COPY immediately before and after ingestion, stages it in a transaction-
local TEMP table, and publishes only those selected rows with the graph maps.
It never re-queries the mutable taxonomy catalog after compiler selection.
Changing a catalog row after the prepass therefore cannot silently change the
published representation or members.

## Import progress and timing

The importer publishes weighted progress from download, scan, graph compile,
publish, audit, and activation. Movement and heartbeat are separate:

- `progress_seq` and `progressed_at` prove work advanced;
- `event_seq` and `observed_at` prove the process is alive;
- a healthy heartbeat without progress movement still fails the stuck-import
  gate.

The V4 graph segment exposes distinct progress phases for authenticated NPI
scope extraction, taxonomy input preparation, layout/taxonomy selection, and
selected-COPY publication. Each phase reports completed bytes or rows only
after the corresponding authenticated boundary completes. A heartbeat during
COPY or bounded database work does not advance those counters. The dashboard
can therefore distinguish a long source-scope read from compiler work and
publication instead of appearing frozen at one generic graph percentage.

Import ceilings are calculated from compressed input bytes and exact component
fact work. A fixed ceiling copied from a smaller file is not an acceptance
criterion. The first source-controlled dev policy is 300 fixed seconds, plus
30 seconds per compressed GiB, plus three seconds per million factor edges.
Operators cannot override the sealed byte/fact counts or these coefficients at
canary time.

Relational dictionary publication uses the authenticated
`ptg2_v4_dictionary_publication_adaptive_v1` runtime contract. It starts with
100,000 ordered rows per database operation, reserves fixed row-work overhead,
and keeps the encoded/estimated row-work envelope at or below 16 MiB. This is
an admission estimate for inserted and replay-validated rows, not the literal
SQL text or network payload size. An operation taking four seconds or longer
halves the next range toward the 10,000-row fallback; operations at or below
two seconds recover geometrically. The widest tax sidecar estimate includes
its source bitmap and fixed bind/materialization work. A four-second heartbeat
repeats only the last completed counters: rows and batches advance only after
the range has been inserted and replay verified. The dashboard therefore shows
a healthy heartbeat with unchanged completed-work counters while a bounded
operation is still executing; it must not render that heartbeat as additional
rows completed. Final completeness is also bounded: target keys are enumerated
through adaptive indexed pages, every group-tax row performs an indexed group
dictionary lookup, and a compact exact bitset proves that every tax token is
referenced. No full-snapshot `COUNT(DISTINCT ...)` or anti-join belongs in this
lane. Batch adaptation changes neither canonical ordering, transaction scope,
content digests, nor graph layout.

V4 also memoizes byte-identical inline provider-group arrays before JSON
deserialization and normalization. A cache entry retains the exact raw bytes,
their digest, parsed groups, normalized transform, and audit counts; digest
matches are verified by full byte comparison. The cache is bounded to 256 MiB,
and eviction can affect speed only, never output. This specifically removes
repeated parsing work from jumbo inline-source shapes while leaving the V3
parser and logical output unchanged.

Publisher-invalid empty `npi` arrays are handled as an explicit compatibility
case. The scanner retains the TIN-scoped group and its rates, emits no invented
NPI membership, canonicalizes the empty array like the existing TIN-only zero
marker, and records the normalization count in authenticated import evidence.
Bounded noncanonical NPI strings are quarantined; other malformed shapes remain fail-closed.

## Release gates

The candidate audit completes exactness and integrity checks before activation.
Normal imports use `candidate_audit_mode=audit_and_activate`, preserving the
existing audit-plus-promotion behavior. V4 comparison imports explicitly use
`candidate_audit_mode=audit_only`: they record the same passing attestation but
return a `validated` snapshot with `activation_status=deferred`, never enter
the promotion phase, and leave every current pointer unchanged. The control
plane must verify that the engine advertises this mode before enqueueing work.
Unknown modes, missing audit-only result markers, an activated import ID, or an
equivalent-layout reuse fail closed.

The partition request contract continues to accept at most 100 items for
reader compatibility. Current writers emit deterministic, code-aware
partitions targeting at most 25 items to reduce observed dense-provider-graph
retention without raising the existing 512 MiB decoded-retention or default
1 GiB per-process admission limits. Those caps remain fail-closed; only a
complete candidate audit proves every emitted partition for a snapshot fits.
Changing the partition boundary does not change the sealed plan digest,
ordered item ordinals, or cohort counts. Progress reports the resulting
dynamic `partition_count` and exact completed count; it must not assume the
older 50-item target.

V4 audit traversal is selected from the sealed graph representation before
forward occurrence payloads are read. `pattern_v1` proves
NPI-to-pattern-to-set membership first and supplies those exact provider-set
keys as the forward-read filter. If the graph proof cannot fit the existing
decoded-retention and physical-read limits, the request fails closed; it does
not switch to code/source-first after reading graph blocks.

`direct_v1` is adaptive before forward I/O. The authenticated code dictionary
supplies the exact sealed `rate_count` for every selected code key. The reader
sums each distinct key once and applies a conservative full-peak bound, not
only decoded-row/index coefficients. The bound includes code-key
normalization, source projection, mandatory request and filter maps, and the
worst-case sealed-rate allowance for discovered shards, fragment views,
coordinate buckets, occurrence workspaces, mutable rows, and the frozen
result. It also multiplies each code's rate count by the largest distinct-NPI
fanout of any requested source, then includes the code/source-to-NPI map, NPI
provider candidates, persisted-sample memberships, and the conservative V4
result and coordinate-reservation workspace. Exact capacity selects
code/source-first only when that complete peak fits; a one-byte-short budget
selects graph-first before forward I/O. If the broad code/source-first path
cannot fit, the audit proves the requested challenge and persisted-sample NPIs
through the direct graph first, then supplies those exact provider-set and
occurrence coordinates to one filtered forward read. Small direct partitions
retain the existing code/source-first path.

The same direct-layout rule applies to exact-NPI online serving. It resolves
the requested NPI through the bounded direct graph first, then reads only that
proved provider-set scope for the requested code. A heavy NPI that reaches the
sealed graph-member cap falls back before forward I/O to the bounded
code-first scope; it never pays for a failed broad forward read first.

Only a typed graph-capacity outcome on the first requested NPI may fall back
to one code/source-first attempt, and that decision occurs before any forward
block or earlier NPI graph result is retained. A capacity failure on a later
NPI after an earlier coordinate was proven remains fail-closed, preventing a
retry from processing the earlier graph coordinate twice. Graph integrity,
manifest, authentication, or mapping errors remain fail-closed.
Missing, boolean, string, or negative sealed rate cardinalities also fail
before traversal. Both orders must preserve exact price atoms, occurrence
identity, provenance, persisted-sample proof, and result ordering; the
request-scoped physical-read ledger must prove the selected forward block is
read and decoded once.

The selector does not widen a byte, member, request, or process cap and does
not change a stored relation, packed map, block, layout, or manifest identity.
Any mapping or block loaded while selecting or traversing is retained for its
downstream consumer within the request; it is not decoded or logically
processed a second time. The two traversal orders must produce the same
matched occurrence and persisted-sample result.

Concurrent completion progress is a count, not a partition ordinal. A
partition failure must retain the exact immutable request identity:
`plan_digest`, zero-based `partition_index`, `partition_count`,
`partition_digest`, and `request_digest`. This makes one failing partition
reproducible without relabeling the last completed count as its index.
Traversal selection is internal to that partition and does not introduce an
extra progress phase. If both admissible direct traversal orders exceed their
existing limits, the terminal failure retains the same partition identity and
the exact capacity classification; it is never reported as a successful
completed partition.

The attestation persists `activation_intent` and a digest binding that intent
to the audited report. Generic promotion and attestation consumption accept
only `audit_and_activate`; an `audit_only` attestation is a durable hold and
requires the authenticated reviewed-activation operation. That operation
freezes the source import, candidate snapshot, predecessor snapshot, audit run,
full attestation digest, and actor under one stable operation ID. It pins the
published predecessor for rollback, consumes the exact hold while switching
engine pointers, changes only the matching control-plane source routes, and
requires every affected current serving release to carry the exact candidate
source/import/snapshot binding before recording completion. Every lifecycle
transition is append-only, and exact request replay resumes or verifies that
operation without widening its coordinates. The paired reviewed
rollback accepts only the stable operation ID and restores the pinned
predecessor; it cannot select an arbitrary snapshot.

Malformed, mismatched, expired, already-consumed, route-drifted, or
release-incomplete operations fail closed. Redelivery reuses an unexpired held
attestation without public audit I/O or generic promotion. Its terminal
progress is `candidate audit-only complete`, with exact request counts retained
through the control wrapper instead of being replaced by a generic success
phase.

Dev-only operational latency, price-comparison, and physical-storage gates run
against the inactive attested candidate. A separate reviewed activation step
is required after those gates; an audit-only source import cannot enter the
generic promotion or route/release reconciliation paths.

1. Exact counts, digests, packed-map roots, relation manifests, prefix
   metadata/vectors, layout eligibility, component-over-cap coverage, and
   diagnostics must reconcile against PostgreSQL. A new direct layout must have
   exactly one authenticated prefix owner per provider set; a pattern layout
   must match its sparse prefix count.
2. Exact sampled V4 traversals must match the retained V3 truth.
3. The public no-NPI, provider-expanded, cost-ordered CPT page of 25 results
   must match an independently captured frozen-V3 semantic page and have cold
   and warm p95 at or below the current 70 ms release ceiling. Cold p95 requires
   at least 20 distinct fresh API processes; API headers and metrics must
   identify the same process and exact image. The 50 ms bound remains the
   follow-up optimization target and is not a blocker for this release.
4. The compiler-declared worst stored-prefix owner and, when the selected
   representation retains ordinary online owners, the worst non-prefix owner
   must each return the exact 201-member prefix within the same 70 ms release
   ceiling, cold and warm, without exceeding physical read limits. Complete
   direct layouts have no ordinary online owner; the 50 ms follow-up target is
   measured and retained separately.
5. Storage must pass both snapshot-attributed and positive import-delta gates
   against the source-controlled, source-set-bound ceiling. The report must
   retain the selected encoded projection, measured physical
   `pg_total_relation_size` attribution, and estimator residual; generic
   ceiling or residual drift blocks activation.
6. Progress must be visible from dispatch through terminal 100%, with polling
   no slower than five seconds and no unreported movement gap.
7. Rerunning identical input must choose the same representation and produce
   the same authenticated logical roots. Synthetic identity-invariance proof
   must also show that changing only source metadata leaves layout and
   representation byte totals unchanged, while a changed incidence shape may
   select another layout.

The dev canary order covers three frozen source/evidence baselines without
preselecting their layouts. Each canary is independently accepted; a failure
stops the sequence and triggers rollback to the retained V3 snapshot.

During this isolated window, generic planned-import dispatch is fenced while
candidate-audit dispatch remains active. An exact reimport is first created
under a node-independent deterministic identity; node selection happens only
inside the atomic dispatch reservation, so concurrent requests cannot start
two attempts. Rollback is a separate authenticated operation that accepts only
the exact pinned predecessor and reverses source, plan, same-source global, and
declared allowed-amount pointers in one lifecycle-locked transaction. It
validates the retained snapshot's sealed scope and activated audit attestation
before changing any pointer, and an exact retry performs no writes.

When a retained source wrapper no longer resolves but its active stored direct
projection is revalidated from fresh bytes, exact-period planning may consume
an immutable future-only attestation. The proof binds the exact source-file and
content version, direct-dispatch CAS, semantic month, current source and plan
lineage, HTTP validators, byte count and hashes, and the validating code image.
It is idempotent by proof digest, consumed at most once, and can be revoked
without rewriting history. It does not repair historical provenance, authorize
catalog refresh, or change an import, snapshot, release, route, or V3 artifact.

The V3 oracle is pinned to its reviewed image. Reference capture first attests
the singular ready Deployment and Pod, immutable image digest, V3-only
ConfigMaps, and exact Service target. In the current dev topology that same
Deployment also serves candidate-audit traffic, so it remains at one replica
through both canary passes. It may become scale-to-zero only after those roles
are separated in source control and the canary attests the dedicated oracle.

## Metadata-only stale-build reconciliation

An interrupted V4 attempt can leave only its internal import-run row and an
empty-manifest `building` snapshot. Operators repair that exact pair through
two authenticated capabilities:

- `POST /control/v1/ptg/v4/stale-metadata/reconcile-plan` accepts
  `snapshot_id` and `internal_run_id` and returns a redacted plan digest.
- `POST /control/v1/ptg/v4/stale-metadata/reconcile` accepts the same exact
  coordinates plus `expected_plan_digest`.

The stale interval is server-owned through
`HLTHPRT_PTG2_V4_METADATA_STALE_SECONDS` and cannot be shortened in a request.
Execution holds the shared lifecycle lock, a pair-qualified advisory lock, and
the snapshot row, run row, and durable exact-pair attempt fence before
rechecking the plan. It is eligible only for a V4 internal run, an exact
snapshot/run association, an empty snapshot manifest, and no row in the
authoritative attempt-attachment registry. That registry covers layout
bindings, logical scope, artifacts, allowed amounts, rates, source metadata,
pointers, plan months, child jobs, and deterministic manifest-stage tables.
The plan digest also binds the canonical complete import-run report. Any
physical-layout or failed-layout recovery field makes metadata reconciliation
ineligible and leaves the target for exact physical recovery. That recovery
must prove the same attempt fence is still active and writable immediately
before releasing physical ownership.

Every registered application writer guards the same fence in the transaction
that performs its write. Low-volume relations also have database triggers as a
backstop; bulk/COPY paths guard once per transaction to avoid a per-row trigger
cost. After reconciliation, the immutable fence rejects resumed heartbeats,
writes, cleanup, attachment, and storage-generation changes.

The operation changes only the snapshot and run metadata rows to `failed` and
seals the fence with their identical audit-safe marker, plan digest, target
digest, marker digest, and reconciliation timestamp. It never deletes rows,
releases a layout, queues or sweeps a block, starts GC, removes an artifact, or
changes a pointer. An exact retry requires the two row markers and the fence
audit to agree, then performs no writes.

## Deferred work

Rate-schedule factoring remains observe-only. The importer records distinct
schedule digests and potential edge reduction, but rates continue to use the
existing exact terminal representation until measured storage and serving
evidence justify a separate release.

Source-component-only serving is not a global layout. It is an exact bounded
fallback for manifest-listed pattern-overflow owners.

The first release keeps the fail-closed transaction-scoped lifecycle lock for
all guarded bulk writes. Serial canaries retain database wait samples beside
the existing import-stage timings. A V3-specific bulk fast path is deferred
until those measurements justify it; that path must validate and row-lock the
V3 snapshot and run without weakening the global-first fence for V4, unknown,
or reconciled attempts.

V3 retirement, documentation consolidation, and retained-snapshot cleanup are
explicitly deferred for one to two months and require a separate approval.
