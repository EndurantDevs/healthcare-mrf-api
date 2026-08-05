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

V1 adds `POST /api/v1/pricing/providers/search-by-procedure`. It restricts
results by exactly one billing identity before provider-address geo filtering.
Every result retains an exact, immutable pricing witness and a separately
sourced NPI-address witness. The target remains inactive until its migrations,
publication path, API, security controls, and runtime proofs have passed their
independent gates.

Relevant current foundations include the
[pricing endpoint](../api/endpoint/pricing.py),
[billing-reference encoder](../api/ptg2_billing_entity_refs.py),
[TIN/NPI connector](../process/tin_npi_connector.py), and
[import swap policy](import_swap_backup_policy.md).

## 2. Frozen decisions

The following decisions are fixed for V1:

1. The new operation is
   `POST /api/v1/pricing/providers/search-by-procedure`. Existing GET behavior
   on that resource is unchanged.
2. One request contains exactly one billing-identity selector:
   - one exact raw tax identity whose type is `ein` or `npi`, or
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

## 3. Public API contract

### Request

The operation is:

```text
POST /api/v1/pricing/providers/search-by-procedure
Content-Type: application/json
```

The raw-tax-identity form is:

```json
{
  "healthporta_plan_id": "<entitled-plan-id>",
  "billing_identity": {
    "tax_identity": {
      "type": "ein",
      "value": "<exact-tax-identity>"
    }
  },
  "procedure": {
    "code_system": "CPT",
    "code": "<procedure-code>",
    "modifiers": [],
    "place_of_service": []
  },
  "geo": {
    "zip5": "<five-digit-ZIP>",
    "radius_miles": 0
  },
  "provider_npi": "<optional-checksum-valid-NPI>",
  "page": {
    "limit": 25,
    "cursor": null
  }
}
```

`billing_identity.tax_identity.type` is exactly `ein` or `npi`. The reference
form replaces the complete `tax_identity` member with:

```json
"billing_identity": {
  "billing_entity_ref": "be1_<opaque-reference>"
}
```

`tax_identity` and `billing_entity_ref` are mutually exclusive and exactly one
is required. The request therefore carries one billing identity even when
`provider_npi` is present. `provider_npi` is optional; when present it is an
exact same-group intersection:

```text
G is in groups(selected billing identity)
  AND groups(provider_npi)
  AND groups(provider set for the returned rate)
```

It MUST NOT initiate an independent NPI pricing lookup.

`geo.zip5` is required. `geo.radius_miles` is between 0 and 100 inclusive; zero
means exact ZIP. Deployments MAY enforce a lower maximum but MUST NOT silently
widen or clamp a request. `page.limit` is between 1 and 200 inclusive and
defaults to 25. Offset pagination is forbidden. Procedure code, modifiers, and
place-of-service values are exact normalized filters.

The server resolves the caller's entitled immutable plan release, revision,
snapshot set, and compatible serving generation bundle. Requests cannot name
an internal snapshot, source file, generation, or token policy.

`billing_identity.tax_identity.value` is sensitive and `writeOnly` in OpenAPI.
The value MUST NOT be accepted through a URL, query string, or header.
Validation errors do not echo it.

### Response and errors

The response reuses the existing provider, rate, and address envelope where it
is compatible. It adds `pricing_scope=plan_scoped_ptg_tax_identity`,
`billing_association_scope=tax_identity_match_only`,
`geo_match_scope=provider_address_evidence`, the matched `be1_` reference,
per-provider/per-rate witness, site-comparison dimensions, and resolved public
release/revision references. It never returns raw or masked tax identities,
internal group keys, or source-record identifiers.

An entitled, well-formed search returns HTTP `200` with exactly one of these
`match_state` values:

- `matched`;
- `no_matching_tax_identity`;
- `tax_identity_unavailable_for_snapshot`;
- `no_matching_rates`;
- `no_match_in_radius`;
- `no_snapshot_for_plan`.

Unknown or unentitled plans and unknown, expired, malformed, or unentitled
billing references use one indistinguishable generic `404` response. A request
that has no validated compatible serving generation uses `503`. A signed cursor
whose retained generation has expired uses `409`. Malformed request structure
or raw typed-identity syntax uses a generic `400` without echoing the sensitive
selector.

Pagination is signed keyset pagination. Every cursor is bound to the normalized
request fingerprint, authorization scope, generation bundle, snapshot set,
stable total-order sort tuple, expiry, and key version. Equal-distance and
equal-rate rows MUST NOT be duplicated or skipped. Cursor serialization and
signature-key storage are implementation details, but these bindings are not.

The caller needs the dedicated `pricing:billing-search` capability; detailed
provenance requires a stronger capability.
Billing search has stricter per-tenant and per-principal quotas than ordinary
procedure search, plus bounded page, group, address, and rate fanout. Exact
quota numbers and the stronger provenance capability name remain deployment
configuration, but capability enforcement, auditing, and anti-enumeration
limits are mandatory.

### Wire details still deferred

The OpenAPI implementation may still freeze the POST operation ID, optional
detailed-provenance sub-schema, cursor byte encoding, signing-key provider, and
deployment-specific quota values. Those choices cannot weaken the route,
envelope, selector union, bounds, states, status codes, cursor bindings, or
capability requirements above.

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

For a raw EIN or billing NPI, normalization and tokenization happen transiently
after plan entitlement. For a `billing_entity_ref`, authenticated decoding or
lookup yields the same typed, policy-bound identity token. The raw selector is
never persisted as part of the witness.

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

When identical provider groups are deduplicated across source shards, a merged
group-level tax-identity state or source bitmap is not a source-local pricing
witness. The serving projection MUST prove that the selected atomic rate
occurrence and the selected billing-identity state came from the same admitted
source occurrence. If the retained generation cannot prove that relationship,
the path fails closed and MUST NOT authorize rate serving.

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

Site comparison and evidence confidence are separate dimensions. Every
comparison also emits exactly one confidence value:

- `confirmed` for direct, authoritative evidence supporting the compared
  relationship and address;
- `corroborated` for compatible evidence from independent sources;
- `candidate` for a plausible but not independently established relationship;
- `conflict` when applicable evidence materially disagrees;
- `unknown` when the evidence is absent, excluded, stale beyond policy, or
  otherwise insufficient.

The comparison outcome (`exact_address`, `same_site`, `different`, or
`not_comparable`) MUST NOT be inferred from the confidence label, or vice versa.
Comparison output MUST retain its two evidence inputs, rule version,
independence decision, `independent_source_count`, freshness, conflicts, and a
machine-readable `circularity_exclusion_reason`. That reason is null only when
no candidate evidence was excluded as circular. A match proves only the
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

The normalized evidence graph, serving projection, and public API are
source-neutral. Adapter tests MAY use fully synthetic fixtures shaped like the
TiC, FHIR/Plan-Net, NPPES, or HPT source formats and MAY exercise rules specific
to those formats. Source-format assumptions stay inside the owning adapter and
are translated into the normalized evidence contract. Fixtures, test names,
branches, commits, and public documentation MUST NOT contain real customer,
payer, provider, plan, or source identities or copied private payloads. Public
availability is not sufficient by itself; artifact identity, integrity, terms,
completeness, and evidence semantics still require validation.

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

### Raw tax-identity handling

The service MUST authenticate the caller, resolve tenant capability, and verify
plan entitlement before tokenizing or searching. The type is normalized first
and accepts only `ein` or `npi`. EIN normalization accepts only reviewed display
forms and produces nine ASCII digits. NPI normalization accepts exactly ten
ASCII digits and requires the CMS 80840/Luhn checksum. Matching is exact; fuzzy,
prefix, substring, and enumeration searches are forbidden.

The normalized value is transformed using a domain-separated,
policy-versioned HMAC-SHA-256 whose message binds the normalized type and value.
A bounded locator may index the lookup, but the full HMAC is authoritative and
MUST be compared before a match is accepted. Policy descriptors and key versions
are part of the generation contract.

The value inside `billing_identity.tax_identity` is sensitive even when its type
is `npi`. That taint is distinct from an optional rendering `provider_npi` used
as a same-group filter. Raw or masked tax-identity values MUST NOT enter:

- URLs, headers, logs, traces, exception text, validation echoes, or debug data;
- response bodies, signed cursors, or billing references;
- metrics labels, cache keys, Redis keys, manifests, or public fixtures.

Caches use only policy-versioned pseudonymous identities and immutable scope.
Ordinary API workers do not receive raw-identity vault access. Key rotation uses
explicit dual-read/rebuild and retirement gates; missing policy material fails
closed.

### `be1_` reference boundary

`billing_entity_ref` is the only reusable and response-visible tax-identity
identifier. A raw EIN or billing NPI is allowed only as transient sensitive
request input. The reference is opaque, versioned, authenticated, and bound to
enough policy, snapshot/release, and tenant/entitlement scope to prevent
substitution or cross-snapshot confusion. The exact encoding remains an
implementation detail.

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
hard failure. One publisher holds the scoped session-level build lock while it
constructs and admits the complete bundle; a competing builder cannot prepare a
second bundle for the same publication scope.

The mutually dependent serving bundle MUST publish together:

1. Build every staging relation, constraint, index, and statistic before the
   swap.
2. If a stage began `UNLOGGED`, convert it to `LOGGED` and verify persistence
   before admission.
3. Verify live/stage schema, column, constraint, index, and ownership parity for
   every relation in the bundle.
4. Validate row/fanout ceilings, full-HMAC collision checks, source completeness,
   no plaintext TIN, referential integrity, witness parity, address coverage,
   deterministic digests, and query-plan ceilings.
5. Capture the expected predecessor generation, source fences, source-vector
   digest, and the exact relation OIDs for every canonical live and staged
   relation. Source relation OIDs used by a fence are captured too.
6. Publish in one short transaction:
   1. set bounded `lock_timeout` and `statement_timeout` values;
   2. acquire the scoped advisory transaction lock;
   3. recheck source fences, expected predecessor, source OIDs, canonical live
      OIDs, staged OIDs, and generation state;
   4. lock all live and staged bundle relations in one deterministic order;
   5. rename canonical live relations and indexes to deterministic `_old`
      names;
   6. rename the validated staged relations and indexes to canonical live names;
   7. update the current generation bundle, source-release pointers, and
      rollback metadata before commit.
7. If the transaction result is ambiguous after a timeout or connection loss,
   determine commit status from the captured canonical/staged relation OIDs and
   generation pointer before any retry.
8. Run post-publish verification. A failure invokes the recorded atomic reverse
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
- Match classification and confidence remain independent, and
  `independent_source_count` plus `circularity_exclusion_reason` survive
  materialization and response shaping.
- Raw EIN, raw billing-NPI, and `be1_` requests exercise the same exact witness;
  an optional `provider_npi` only narrows the same-group intersection.
- All six successful match states and generic `404`, `503`, and `409` behavior
  match the frozen API contract without leaking inaccessible plan or reference
  existence.
- Existing GET behavior and OpenAPI remain backward compatible.

### Privacy and authorization

- Frozen EIN and checksum-valid NPI normalization/HMAC vectors pass across
  implementation languages.
- Raw-identity redaction probes cover logs, errors, traces, responses, cursors,
  caches, metrics, manifests, Redis, and fixtures.
- `be1_` tampering, scope mismatch, expiry, generation mismatch, and key rotation
  fail closed without identity enumeration.
- Entitlement, capability, quotas, fanout ceilings, and auditing work in the
  running service.

### Publication and runtime

- Disposable PostgreSQL tests exercise the session build lock, relation parity,
  bounded timeouts, advisory transaction lock, deterministic relation locking,
  OID/CAS fences, atomic swap, ambiguous commit resolution, `_old` retention,
  concurrent reads, reverse swap, and orphan recovery.
- A shadow build from authorized retained public artifacts has deterministic
  counts, digests, provenance, rejects, and forward/reverse parity.
- Concurrent requests observe wholly the old generation bundle or wholly the
  new one.
- Exact-ZIP latency passes the defined warm service-plus-database p95 gate.
- Radius results are measured without claiming the deferred SLO.

## 14. Deferred work and activation boundary

The following require later contracts or explicit approval:

- bounded multi-identity OR requests;
- a radius-search latency SLO;
- licensed roster, CAQH, W-9, credentialed-directory, claims, or 837 ingestion;
- any ownership, employment, facility, or exact rate-site assertion;
- production activation and restricted-source publication.

Implementation, migration, data publication, deployment, latency proof, and
production activation are separate states. Merging code or observing a healthy
endpoint does not by itself activate this architecture contract.
