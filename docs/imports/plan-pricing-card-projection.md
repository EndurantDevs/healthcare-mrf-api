# Plan pricing card projection

Release-scoped `view=card` reads use the immutable
`plan_pricing_card_v2` projection. With `view=card`,
`include_providers=false` selects its compact rate aggregates. Omitted or
explicit `view=full` retains the existing PTG serving path and response shape.

## Release contract

Before opening its release transaction, the release publisher creates the
idempotent `plan-pricing-projection` control import, ensures the existing
single-job ARQ worker, and polls its durable run. The request contains the
exact physical binding manifest and its digest. The candidate identity binds
that digest to the locked provider relation generation. A retry reuses the
active run or the ready candidate. The publisher waits at most 840 seconds;
if the build is still running, the release stays unpublished and the error
retains the run ID for a later replay.

The builder uses a repeatable-read transaction and holds access-share locks on
the provider, taxonomy, address, ZIP, and geo-assurance relations. It requires
the active geo-assurance signature, reads only sealed in-network bindings,
applies the serving reader's inferred-taxonomy eligibility rules for ruled
numeric CPT/HCPCS families, and writes in one database transaction:

- one pre-rendered provider-card fragment per release, code, ZIP cell, and NPI;
- one pre-rendered aggregate fragment per release, code, and ZIP cell; and
- row, byte, content-digest, and build-time receipts.

Ready candidates and child fragments have database mutation and truncate
guards; the ready transition also verifies row and byte counts. The publisher
re-reads the ready candidate before atomically placing its
receipt in the serving revision manifest. A request accepts the link only when
the candidate is still ready and its contract, binding digest, provider
signature, and content digest match that immutable receipt. Current-release
replay attaches the same receipt without rebuilding or replacing it.

ZIP centroids are the cell catalog. A bounding-box index reduces a radius to a
bounded ZIP `IN` list; the card and aggregate indexes then fetch stored JSON
bytes. The request path does not expand provider sets or serialize individual
rows.

## Response lanes

Provider cards contain NPI, provider name, credential, primary taxonomy and
specialty, classification, city/state/ZIP, minimum and maximum negotiated
rate, and rate count. One assured address is projected per NPI and ZIP; a
radius result keeps the nearest selected ZIP for an NPI that appears in more
than one cell. Aggregate rows contain ZIP cell, provider count, rate count,
and minimum/median/maximum negotiated rate; even-sized populations use the
mean of the two middle rates. Empty bounded geography is reported explicitly
as `no_match_in_radius`.

The projection supports exact ZIP and coordinate/radius geography. For
provider cards, city/state, unverified-address display, non-cost ordering, and
other incompatible filters are rejected. With
`view=card&include_providers=false`, filters not represented by the projection
retain the existing aggregate reader so their semantics do not change. The
omitted or explicit `view=full&include_providers=false` path also always
retains that reader.

## Cold-path attribution and cache boundary

The old full-view miss is query-bound. On the valid 32,000-address synthetic
fixture, the original membership join took 30.01 seconds directly (39.35
seconds under `EXPLAIN`) and accumulated about 2.8 million shared-buffer hits;
JSON serialization of its 113 KiB response took 1.75 ms. The correlated
snapshot-key probe removed repeated scope scans. Across 32 nationally
distributed dense, medium, sparse, and zero shapes, ordered parity was 32/32;
direct execution p95 changed from 941.751 ms to 62.812 ms and max from
1,013.389 ms to 69.644 ms.

The old warm effect is not a release-agnostic response cache. Full provider
expansion uses a process-local 1,024-entry/32 MiB selection LRU with no TTL. Its key
includes the immutable shared snapshot key, snapshot ID, code rows, full
request arguments, network identity, order, and target count, so a republished
release cannot reuse the prior release's entry. ZIP-radius rows have a separate
24-hour process cache and contain only ZIP geography.

A separate top-N warm-up queue was not added. The existing provider-expansion
selection cache is process-local, so a release worker cannot warm every serving pod and any
claim that it did would be false. The card and aggregate 70 ms lanes use the
complete database projection and do not depend on process cache state; the
full lane receives the independent membership-plan repair. Add pod-addressed warm-up only if
source-bound live proof shows `view=full` approaching the 30-second timeout.

## Synthetic PostgreSQL receipt

The measurement receipt is
[`docs/research/plan_pricing_card_projection_benchmark_20260825.json`](../research/plan_pricing_card_projection_benchmark_20260825.json).
That receipt records the original `plan_pricing_card_v1` lookup layout; it is
performance evidence only, not proof of the v2 inferred-taxonomy build contract.
It used native arm64 PostgreSQL 18.2, 342,500 realistic card fragments, 13,700
aggregate fragments, and a 33,179-row ZIP centroid catalog. The timed dense
queries selected 151–199 cells and exercised the final nearest-cell NPI
de-duplication query. Each lane used 100 previously unqueried code-by-ZIP shapes
(20 codes by five nearby anchors) plus an immediate paired repeat in one
long-lived process and connection pool.

Card unseen-shape total p99 was 9.330 ms with a 46.435 ms maximum (9,224
bytes); its paired-repeat p99 was 9.102 ms. Aggregate unseen-shape p99 was
2.268 ms with a 2.276 ms maximum (4,125 bytes); its paired-repeat p99 was 1.891
ms. The card/aggregate tables occupied 416,849,920 and 7,946,240 bytes. These
are component results through release resolution, cell selection, fragment
fetch, envelope assembly, and serialization; they do not stand in for
origin/API-layer acceptance. Origin p99 and real-release build runtime/storage
must be recorded after the exact source images are deployed.

`endurant-provenance:v7:1665272cdeb7b2f7db4facac2ebeb30cfb8df044443d63534976508d2396b130`
