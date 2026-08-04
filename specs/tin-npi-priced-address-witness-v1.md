# TIN/NPI-Scoped Priced-Address Witness V1

**Status:** Accepted architecture contract; implementation inactive · **Date:** 2026-08-04
**Scope:** source-neutral PTG pricing, billing-identity evidence, provider-address
geo search, publication, and serving boundaries

This contract defines the first public-source implementation slice. It separates
the target architecture from capabilities that happen to exist in the current
repository. The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are
normative.

---

## 1. Current state versus target state

### Current state at acceptance

- Procedure pricing is exposed through existing GET handlers, including
  `/api/v1/pricing/providers/search-by-procedure`. Their behavior is a backward-
  compatibility boundary and MUST remain unchanged by this work.
- The repository can emit output-side `be1_` billing-entity references for some
  sealed PTG tax-identity associations.
- TIN/NPI connector, PTG address, and table-swap components exist as partial
  foundations. Their presence does not prove the end-to-end witness in this
  contract.
- There is no accepted, active POST billing-identity search on this baseline,
  no proven request-wide PTG/connector/address generation pin, and no accepted
  benchmark proving the latency objective below.

These observations are baselines to revalidate before implementation. They are
not statements about a deployed environment or a published dataset.

### Target state

V1 adds a POST procedure-pricing operation that restricts results by exactly one
billing identity before provider-address geo filtering. Every result retains an
exact, immutable pricing witness and a separately sourced NPI-address witness.
The target remains inactive until its migrations, publication path, API,
security controls, and runtime proofs have passed their independent gates.

Relevant current foundations include the
[pricing endpoint](../api/endpoint/pricing.py),
[billing-reference encoder](../api/ptg2_billing_entity_refs.py),
[TIN/NPI connector](../process/tin_npi_connector.py), and
[import swap policy](import_swap_backup_policy.md).

## 2. Frozen decisions

The following decisions are fixed for V1:

1. The new semantic operation uses POST procedure pricing. Existing GET
   behavior is unchanged.
2. One request contains exactly one billing-identity selector:
   - one exact raw EIN, or
   - one opaque `billing_entity_ref` beginning with `be1_`.
3. Multi-identity OR semantics are not part of V1.
4. The server resolves the entitled immutable release and its PTG, connector,
   and address generations. A client cannot select arbitrary source files,
   snapshots, or internal generations.
5. Billing restriction is TIN first. It is not implemented by finding every NPI
   related to a TIN and then invoking an unrestricted NPI pricing search.
6. Geo matching applies to independently sourced addresses belonging to each
   eligible NPI after the exact billing/rate restriction.
7. Phase 1 uses only credential-free public inputs through source-neutral
   adapters. Restricted-source adapters remain disabled and separately gated.
8. The first hard latency target is warm exact-ZIP search p95 at or below
   40 ms for service plus database time, excluding external network transit.
9. Radius search receives a separate SLO only after the read model is measured.

## 3. Deliberately deferred API details

This architecture contract freezes operation semantics, not an unreviewed wire
schema. The implementation ADR/OpenAPI change MUST reconcile the canonical
mounted route before freezing:

- the exact POST URI and operation ID;
- JSON envelope and field names beyond the selector concepts defined here;
- procedure, modifier, place-of-service, geo, and page object shapes;
- response field names, successful no-match states, and HTTP error mapping;
- optional rendering-provider NPI filter syntax;
- keyset cursor encoding, expiry, and key-rotation representation;
- the authorization claim vocabulary and quota values.

The final request schema MUST still express a mutually exclusive union: exact
raw EIN versus `billing_entity_ref`. The raw EIN field MUST be marked sensitive
and `writeOnly` in OpenAPI. It MUST NOT be accepted through a URL, query string,
or header.

The evidence graph MUST preserve a source-reported billing identifier whose TiC
type is `npi`, including Type-1 or Type-2 membership around it. Enabling a raw
NPI as a public billing-identity selector is not frozen by this contract and
requires a separate API/security decision. A valid `be1_` reference MAY resolve
to a supported typed identity without exposing its raw value.

If a rendering-provider NPI filter is later included in V1, it MUST be an exact
intersection inside the selected group and rate witness. It is not a second
billing identity and cannot start an independent provider search.

## 4. Terms and non-claims

- **Billing identity:** a typed identifier reported by a source for a billing
  group. It does not by itself identify legal ownership.
- **Provider group (`G`):** one snapshot-local TiC provider group associated
  with the selected billing identity.
- **Provider set (`S`):** the immutable internal set participating in one
  code-specific pricing occurrence. It is not sufficient evidence of a
  provider's billing identity without an exact group intersection.
- **Atomic rate option (`R`):** one source-preserved negotiated price atom,
  including its code, modifiers, place-of-service values, source semantics, and
  lineage. Distinct source atoms are not collapsed merely because their visible
  prices match.
- **Provider (`P`):** an exact NPI member of `G` that participates in `S` for
  `R`.
- **Provider address (`A`):** an address attached to `P` by address evidence
  independent of the selected TIN-to-group membership.
- **Generation bundle:** the mutually compatible PTG price, billing connector,
  address evidence, and serving projections pinned for one request.

The following claims are forbidden unless a separate direct source proves them:

- legal ownership, employment, tax ownership, or facility ownership;
- that an organization candidate owns a provider group;
- that a provider address is an address owned by the TIN;
- that a matched address is the exact service location for the rate;
- that a rate's place-of-service code identifies the returned postal address;
- that same-address or same-premise evidence establishes an affiliation.

## 5. Exact pricing witness

Every returned provider/rate pair MUST preserve this path within one pinned
generation bundle:

```text
entitled plan release
  -> exactly one request selector
  -> verified typed identity token
  -> snapshot-local billing identity
  -> exact provider group G
  -> exact code-specific provider set S
  -> exact atomic rate option R
  -> exact group member NPI P
```

For a raw EIN, normalization and tokenization happen transiently after plan
entitlement. For a `billing_entity_ref`, authenticated decoding or lookup yields
the same typed, policy-bound identity token. The raw selector is never persisted
as part of the witness.

The internal witness MUST retain at least:

- plan release and generation-bundle identity;
- source release, snapshot, network, and source-record lineage;
- token policy and typed billing identity;
- provider-group and provider-set identities;
- atomic rate-option identity and its full pricing semantics;
- group-member NPI and membership lineage.

The provider/rate relation is valid only when all of these facts intersect in
the same pinned scope. Conceptually:

```text
G is in groups(selected billing identity)
G is in groups(S for R)
P is in members(G)
P participates in S for R under G
```

An implementation MUST NOT flatten this to:

```text
billing identity -> all related NPIs -> any rate for those NPIs
```

That shortcut can disclose a provider's rate under another billing identity.
It also makes provider-set membership appear to prove a billing relationship
that it does not prove.

If one provider belongs to several billing identities, each matching path stays
independent. An organization or reassignment relationship observed elsewhere
cannot be attached to a group that lacks that exact source witness. A group with
only Type-1 members remains searchable, but remains organizationally unresolved.

## 6. Independent provider-address and geo witness

The pricing witness and address witness meet only at the exact NPI `P`:

```text
pricing witness -> P
                   |
independent address source -> NPI-address evidence -> A -> exact-ZIP match
```

An eligible address MUST retain:

- the NPI to which the source attached it;
- immutable source release and source-record provenance;
- address purpose, observed/effective interval, and freshness state;
- canonical `address_key`;
- `address_site_key` or premise key when supported;
- geocode/ZIP derivation version and quality state;
- selected-address rule version.

The selected billing identity, its TiC group, and the queried provider's own TiC
membership MUST NOT be recycled as an independent address source. This prevents
the circular claim "the TIN is at this address because its provider is here,
therefore the provider is confirmed under the TIN at this address."

An optional billing-entity site comparison may use only direct taxpayer/entity
address evidence, an independently linked organization NPI's address, or direct
plan/network directory evidence. It produces one deterministic comparison:

- `exact_address` for canonical address equality;
- `same_site` for premise equality with a unit-level difference;
- `different`;
- `not_comparable`.

Comparison output MUST retain its two evidence inputs, rule version,
independence decision, freshness, and conflicts. A match proves only the
supported co-location. It does not relocate the provider, prove ownership, or
bind the negotiated rate to that site.

Exact-ZIP search compares each eligible NPI address to the requested ZIP only
after the billing and rate witness is complete. Missing address evidence cannot
be filled from a candidate organization's address. A TIN-only group with no
eligible NPI/address path returns no priced-address result rather than a
fabricated location.

## 7. Public/source-neutral phase-1 boundary

Phase 1 supports adapters for evidence that is both approved for use and
available without source credentials:

- public TiC artifacts for typed billing identities, exact groups, membership,
  and negotiated-rate lineage;
- public Provider Directory or Plan-Net FHIR releases for exact resource and
  plan/network location evidence when the source is actually enumerable;
- NPPES for NPI type and NPI-address enrichment;
- applicable public Hospital Price Transparency artifacts when they contain a
  traceable organization/tax/address witness.

The schemas and adapter contract are source-neutral: no source-specific names,
identifiers, or assumptions appear in public code paths or synthetic fixtures.
Public availability is not sufficient by itself; artifact identity, integrity,
terms, completeness, and evidence semantics still require validation.

NPPES is not an EIN-to-NPI crosswalk. TiC group membership is not ownership.
Provider-directory name similarity is not an exact plan/network bridge. Public
organizational and reassignment datasets may corroborate a candidate but cannot
create a missing TiC pricing witness.

The following inputs are disabled and outside the phase-1 critical path:

- contracted payer/administrator rosters and W-9 feeds;
- CAQH data;
- claims or 837 data;
- credential-gated provider-directory data;
- any other licensed, restricted, or PHI-governed source.

Their source-neutral interfaces and synthetic fixtures MAY exist, but ingestion
requires separate rights, retention, security, and operational approval. A
partial or targeted source contributes positive evidence only and MUST NOT erase
unrelated evidence. Only a source contract explicitly declared complete may
replace its own previous complete generation.

## 8. Identity, reference, and privacy boundary

### Raw EIN handling

The service MUST authenticate the caller, resolve tenant capability, and verify
plan entitlement before tokenizing or searching. EIN normalization accepts only
the reviewed exact form and produces nine ASCII digits. Matching is exact; fuzzy,
prefix, substring, and enumeration searches are forbidden.

The normalized value is transformed using a domain-separated,
policy-versioned HMAC-SHA-256. A bounded locator may index the lookup, but the
full HMAC is authoritative and MUST be compared before a match is accepted.
Policy descriptors and key versions are part of the generation contract.

Raw or masked EINs MUST NOT enter:

- URLs, headers, logs, traces, exception text, validation echoes, or debug data;
- response bodies, signed cursors, or billing references;
- metrics labels, cache keys, Redis keys, manifests, or public fixtures.

Caches use only policy-versioned pseudonymous identities and immutable scope.
Ordinary API workers do not receive raw-identity vault access. Key rotation uses
explicit dual-read/rebuild and retirement gates; missing policy material fails
closed.

### `be1_` reference boundary

`billing_entity_ref` is the only public identifier for a resolved billing
identity. It is opaque, versioned, authenticated, and bound to enough policy,
snapshot/release, and tenant/entitlement scope to prevent substitution or
cross-snapshot confusion. The exact encoding remains an implementation detail.

A `be1_` value may be emitted in an entitled response and reused as the single
selector in a later entitled request. It MUST NOT reveal a raw or masked TIN,
an internal group key, or a reversible database identifier. Input verification
must authenticate the complete reference before lookup; prefix checking alone
is not verification.

Unknown, expired, malformed, and unentitled references receive
non-enumerating behavior. Detailed provenance requires a stronger capability
than ordinary billing search. Access is audited, fanout and page size are
bounded, and responses use `Cache-Control: private, no-store`.

## 9. Immutable evidence and serving generations

The implementation stores source facts and derived projections separately. At
minimum, the logical model represents:

- immutable evidence-source releases and digests;
- typed policy-versioned tax identities;
- tax-identity-to-NPI/group evidence with explicit relationship class;
- source-reported business-name evidence separate from identity derivation;
- NPI/entity address evidence;
- non-circular site comparisons;
- evidence-generation build, publish, predecessor, and rollback state.

Affiliation, observed billing, provider attestation, same-organization FHIR
evidence, and legal-entity evidence remain distinct relationship classes. They
MUST NOT be reduced to one confidence scalar or a last-write-wins row.

One serving bundle binds compatible identities for:

```text
PTG price generation
+ TIN/NPI connector generation
+ address-evidence generation
+ derived priced-address read generation
```

Every request pins one bundle for its entire read transaction. A result cannot
combine a newer price snapshot with an older connector or address projection.
A cursor is signed and bound to the request fingerprint, authorization scope,
generation bundle, stable sort tuple, expiry, and key version. Offset pagination
is not permitted for this operation.

## 10. Build, atomic publication, and rollback

Each source release is immutable and digest-identified. The importer performs
separate resumable phases:

```text
register -> acquire -> validate -> stage -> project -> reduce
         -> constrain/index/analyze -> validate -> publish -> verify
```

Builds use run-scoped staging relations, bounded streaming/COPY batches,
checkpoints, deterministic counts/digests, rejection reasons, and resource
ceilings. Repeating the same source vector and artifact digest is a no-op or
resume. Producing a different result digest for the same immutable inputs is a
hard failure.

The mutually dependent serving bundle MUST publish together:

1. Build every staging relation, constraint, index, and statistic before the
   swap.
2. If a stage began `UNLOGGED`, convert it to `LOGGED` and verify persistence
   before admission.
3. Validate row/fanout ceilings, full-HMAC collision checks, source completeness,
   no plaintext TIN, referential integrity, witness parity, address coverage,
   deterministic digests, and query-plan ceilings.
4. Capture the expected predecessor generation, source fences, source and stage
   relation OIDs, and source-vector digest.
5. In one short transaction, acquire scoped locks, recheck the compare-and-swap
   inputs, rename live relations/indexes to deterministic `_old` names, rename
   the complete stage to canonical live names, and update the current bundle and
   rollback pointers.
6. If the commit result is ambiguous, determine the outcome from relation OIDs
   and the generation pointer before any retry.
7. Run post-publish verification. A failure invokes the recorded atomic reverse
   swap.

A pre-swap failure leaves live data unchanged. `_old` is the immediately
previous validated rollback asset and is not routine temporary data. It is
retained according to the [import swap policy](import_swap_backup_policy.md).
Orphan recovery MUST NOT drop an active, leased, current, previous, or last-
valid generation. Retention/garbage collection is a separate capacity-checked,
explicitly authorized operation.

## 11. TIN-first serving shape

The hot path is selective in this order:

```text
selector/ref
  -> exact identity token
  -> exact provider groups
  -> code-matching provider sets and atomic rates
  -> exact member NPIs
  -> bounded independent NPI addresses
  -> exact-ZIP filtering
  -> response hydration
```

The path MUST NOT begin by decoding every provider set for a procedure code.
The read model therefore needs generation-scoped indexes or immutable sidecars
for identity-to-group, group-to-set, group-and-NPI membership, code/set/rate,
NPI-to-address, exact ZIP, and reverse NPI-to-identity lookup. It preserves
source/rate multiplicity without materializing an unbounded
code-by-provider-by-location cross product.

Candidate, group, address, and rate fanout are bounded. Pathological groups use
an explicit slow-path or fail-closed response; they do not silently escape the
billing scope.

## 12. Performance contract

The V1 release gate is:

```text
warm exact-ZIP procedure pricing, default page size,
agreed concurrency: service + database p95 <= 40 ms
```

The interval starts when the application accepts the request and ends when the
response is serialized. It includes identity resolution, database work, rate
and address hydration, authorization already local to the service, and
serialization. It excludes caller-to-service network transit and all external
source access. No external data source may be called on the request path.

"Warm" means required processes, database pages/indexes, and sealed immutable
read sidecars are initialized under a documented repeatable protocol. It does
not permit precomputing one benchmark request outside the normal cache policy.
Proof records default page size, concurrency, dataset/generation, hardware,
sample count, cache state, and per-stage p50/p95/p99/max.

The 40 ms objective is not achieved by one SQL timing or by a database-only
sample. Cold exact-ZIP behavior is measured and reported separately. Radius
search is benchmarked independently after geo indexes and the read model exist;
its SLO is deliberately deferred.

## 13. Required proof

Implementation is not active until all applicable gates prove:

### Correctness

- One provider under multiple TINs returns only the rates witnessed through the
  selected identity's exact group and rate option.
- Type-1 members, Type-2 members, a source-reported NPI billing identifier, and
  a group without an organization NPI retain distinct semantics.
- No group, set, rate, source, snapshot, NPI, or address cross-product is
  possible under property and integration tests.
- A TIN-only group cannot fabricate a provider or address.
- Exact-address, same-site, different, missing, stale, circular, and conflicting
  evidence have deterministic outcomes.
- Existing GET behavior and OpenAPI remain backward compatible.

### Privacy and authorization

- Frozen normalization/HMAC vectors pass across implementation languages.
- Raw-identity redaction probes cover logs, errors, traces, responses, cursors,
  caches, metrics, manifests, Redis, and fixtures.
- `be1_` tampering, scope mismatch, expiry, generation mismatch, and key rotation
  fail closed without identity enumeration.
- Entitlement, capability, quotas, fanout ceilings, and auditing work in the
  running service.

### Publication and runtime

- Disposable PostgreSQL tests exercise staging, OID/CAS fences, atomic swap,
  ambiguous commit resolution, `_old` retention, concurrent reads, reverse
  swap, and orphan recovery.
- A shadow build from authorized retained public artifacts has deterministic
  counts, digests, provenance, rejects, and forward/reverse parity.
- Concurrent requests observe wholly the old generation bundle or wholly the
  new one.
- Exact-ZIP latency passes the defined warm service-plus-database p95 gate.
- Radius results are measured without claiming the deferred SLO.

## 14. Deferred work and activation boundary

The following require later contracts or explicit approval:

- bounded multi-identity OR requests;
- a raw NPI billing-identity input contract;
- a radius-search latency SLO;
- licensed roster, CAQH, W-9, credentialed-directory, claims, or 837 ingestion;
- any ownership, employment, facility, or exact rate-site assertion;
- production activation and restricted-source publication.

Implementation, migration, data publication, deployment, latency proof, and
production activation are separate states. Merging code or observing a healthy
endpoint does not by itself activate this architecture contract.
