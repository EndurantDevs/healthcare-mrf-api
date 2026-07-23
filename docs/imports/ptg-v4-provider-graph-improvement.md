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

For each file, one deterministic chooser compares complete encoded sizes and
publishes one graph representation:

- direct layout for small, low-fanout graphs;
- pattern layout for fragmented graphs;
- exact source-component traversal only for pattern-overflow owners that remain
  within the declared online work limits.

The direct flat set/group expansion is not published for a pattern-shaped file.
The compiler uses component and component-tuple memoization, so deriving
patterns does not recreate the logical set/group expansion in scratch.

## Bounded provider prefixes

The public provider page is bounded to 200 results. The compiler targets an
ordered 201-NPI prefix, including one continuation sentinel, for every provider
set. It first proves the prefix from exact factor counts and bounded traversal.
Only owners that cannot prove it within the online group, source-owner,
source-member, page, and byte limits receive a manifest-listed ordered prefix
override.

The override is an exact ordered vector, not a bitmap. It is content-addressed,
digest-checked, globally size-capped, and published only for failing owners.
Requests at or below the online target use the bounded hot path. Larger or
unbounded internal requests use the separately metered exact cold V4 traversal;
they do not fall back to an incomplete prefix.

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

Physical storage acceptance includes tables, indexes, TOAST, packed maps, every
map-reachable CAS block, ordered prefix overrides, and diagnostics. It reports
both the V4 graph footprint and the whole coexisting snapshot footprint.

Runtime metrics separately report physical graph bytes/pages/lookups, second-hop
group-to-NPI work, and the actual provider-expansion rate rows, distinct sets,
graph batches, and cap rejections. This keeps a low-latency answer auditable
against the sealed work model.

## Import progress and timing

The importer publishes weighted progress from download, scan, graph compile,
publish, audit, and activation. Movement and heartbeat are separate:

- `progress_seq` and `progressed_at` prove work advanced;
- `event_seq` and `observed_at` prove the process is alive;
- a healthy heartbeat without progress movement still fails the stuck-import
  gate.

Import ceilings are calculated from compressed input bytes and exact component
fact work. A fixed ceiling copied from a smaller file is not an acceptance
criterion. The first source-controlled dev policy is 300 fixed seconds, plus
30 seconds per compressed GiB, plus three seconds per million factor edges.
Operators cannot override the sealed byte/fact counts or these coefficients at
canary time.

Publisher-invalid empty `npi` arrays are handled as an explicit compatibility
case. The scanner retains the TIN-scoped group and its rates, emits no invented
NPI membership, canonicalizes the empty array like the existing TIN-only zero
marker, and records the normalization count in authenticated import evidence.
Other malformed NPI shapes remain fail-closed.

## Release gates

The existing candidate audit completes exactness and integrity checks before
activation. Dev-only operational latency and physical-storage gates run
immediately after that audited activation; a failure reactivates the retained
V3 snapshot and blocks promotion beyond dev.

1. Exact counts, digests, packed-map roots, relation manifests, prefix
   overrides, and diagnostics must reconcile against PostgreSQL.
2. Exact sampled V4 traversals must match the retained V3 truth.
3. The public no-NPI, provider-expanded, cost-ordered CPT page of 25 results
   must match an independently captured frozen-V3 semantic page and have cold
   and warm p95 at or below 50 ms. Cold p95 requires at least 20 distinct fresh
   API processes; API headers and metrics must identify the same process and
   exact image.
4. The compiler-declared worst override owner and worst non-override online
   owner must each return the exact 201-member prefix within 50 ms, cold and
   warm, without exceeding physical read limits.
5. Storage must pass both snapshot-attributed and positive import-delta gates.
6. Progress must be visible from dispatch through terminal 100%, with polling
   no slower than five seconds and no unreported movement gap.
7. Rerunning identical input must choose the same representation and produce
   the same authenticated logical roots.

The dev canary order covers a low-fanout/direct shape, a
reference-fragmented/pattern shape, and a jumbo fragmented shape. Each canary
is independently accepted; a failure stops the sequence and triggers rollback
to the retained V3 snapshot.

The V3 oracle is a separately deployed, scale-to-zero candidate pinned to its
reviewed image. Reference capture first attests the singular ready Deployment
and Pod, immutable image digest, V3-only ConfigMaps, and exact Service target.
The V3 oracle is scaled back to zero after capture.

## Deferred work

Rate-schedule factoring remains observe-only. The importer records distinct
schedule digests and potential edge reduction, but rates continue to use the
existing exact terminal representation until measured storage and serving
evidence justify a separate release.

Source-component-only serving is not a global layout. It is an exact bounded
fallback for manifest-listed pattern-overflow owners.

V3 retirement, documentation consolidation, and retained-snapshot cleanup are
explicitly deferred for one to two months and require a separate approval.
