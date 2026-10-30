# FHIR network catalog contract

This is the shared implementation contract for the importer and both API layers.
The schema is defined in `db/registry_fhir_catalog.py`; the canonical wire
fixture is `tests/fixtures/fhir_network_catalog.json`.

## Identity and retention

- Allocate a positive int32 network ID from the existing registry identity
  allocator. Match the physical FHIR endpoint and Organization resource ID;
  preserve existing approved bindings and aliases. Names and metadata hashes
  never allocate or merge identities.
- Sync source metadata only after the existing Source admission and native
  custody checks succeed. Partial or unavailable imports do not replace a
  previously accepted source.
- Never delete or archive canonical networks, companies or groups. Preserve
  approved manual fields and relationships.
- Source-reported insurer/administrator Organizations become usable catalog
  companies immediately, without approving existing draft records. Their IDs
  bind the physical endpoint and Organization ID. Explicit current mappings
  take precedence; display-name changes never change identity. A same-source
  `partOf` reference may supply a reported parent group, never verified ownership.
- When a network disappears from a later import, keep its last accepted
  metadata searchable and retain its previous serving membership and addresses.
  Metadata publication cannot replace the membership generation.

Ordinary forward directory publication preserves a disappeared whole network
before catalog synchronization. Its authenticated predecessor recipe vector and
complete admitted Organization coverage determine absence; omitted recipes or
zero membership do not. Copy the previous raw membership facts, exact office
bindings and complete address rows into the replacement candidate, then apply
the current approved additions, removals and mappings. Independent sealing
rederives the selection and verifies complete row parity before publication.
Subsequent rebuilds preserve this carry; a reappearing current identity uses
the current source facts. Present-but-empty networks and approved remappings
must not resurrect old membership.

Ambiguous source subjects or conflicting address versions refuse replacement
and leave the current generation serving. Explicit, independently authenticated
historical reactivation retains its reviewed target semantics. Source rollback
recomposes the reviewed historical source universe with the current approved
overlay. Neither operation gains authority from a differing head or a
caller-supplied preservation receipt.

## Storage and publication

- `registry_fhir_catalog_source` records immutable accepted source descriptors.
- `registry_fhir_catalog_observation` retains compact Organization and plan-edge
  observations for provenance and subsequent reconciliation.
- `registry_fhir_catalog_network`, `registry_fhir_catalog_relationship`, and
  `registry_fhir_catalog_binding` contain derived searchable data.
- Those five tables belong to an isolated candidate schema. The control-schema
  `registry_fhir_catalog_snapshot` records its sealed native custody, source
  vector, approved mapping fingerprints and an optional directory attachment.
  `network_generation` and `manifest_sha256` are either both present or both
  null. A catalog-only snapshot must not create or imply a serving manifest.
- Use bounded COPY and set-based SQL. Build candidate indexes before closing
  writes, validate complete candidate relationships, and publish through the
  existing publication queue under the source fence and exact predecessor check.
- Failed validation or publication leaves the current snapshot serving. A
  repeated accepted source vector and mapping version is a no-op.
- The declared Source reader also needs catalog-column reads: `source_id` and
  `endpoint_id` on `provider_directory_source`, plus `endpoint_id` and
  `canonical_api_base` on `provider_directory_api_endpoint`. These lookups do
  not require access to other registration fields or any Source write grant.
- A standing grant may list exact reviewed retained capture recipes in
  `retained_sources`. These use the existing capture custody and read pin;
  they require no directory publication. A published-recipe grant alone never
  admits an unpublished capture. Raw legacy tables still need verified capture
  preparation, and draft company records are not approved mappings.

## Imported metadata observations

The existing `process.RegistryFHIRCatalogSync` worker uses the server-owned
`HLTHPRT_REGISTRY_FHIR_CATALOG_SYNC` configuration. Its dedicated image may be
selected with `HLTHPRT_REGISTRY_FHIR_CATALOG_WORKER_IMAGE`; other worker classes
keep their existing image selection.

An authenticated Source runtime may supply catalog exports instead of a separate
Source reader connection. This worker mode sets `source_reader_dsn` to null and
requires the image to provide the fixed
`directory_catalog_runtime.catalog_source_export_runtime` interface. Startup
owns that runtime and shutdown closes it. Each export remains a fresh native
read-only snapshot; source contexts close before independent sealing. Registry
Loader, Sealer, lifecycle and execution logins remain distinct and retain their
existing authorization checks. Job parameters cannot supply connections or
runtime factories. The standalone command retains its direct-reader contract.

Catalog role preparation is an explicit operator transaction on existing roles.
Run `install_fhir_catalog_preparation_permissions` before
`install_fhir_catalog_publication_permissions`, using the same metadata-only
consumer policy. Preparation adds the enumerated control reads, bounded allocator
and queue inserts, Sealer snapshot inserts excluding `published_at`, and the
new ledger's API SELECT. It refuses unexpected effective authority and preserves
existing API grants. It creates no roles or credentials and grants no Source
access. Alias values remain trusted application validation, not row-scoped ACLs.
The worker verifies the prepared privileges before creating a candidate.

The Sealer keeps a separate authenticated, read-only repeatable-read connection
open through candidate closure and commit. That connection verifies the exact
directory attachment and retained inputs; current candidate reads and temporary
projection replay remain on the fresh read-committed Sealer transaction.

The metadata-only standing grant may select
`registry-fhir-catalog-observation-v1` as its `observation_contract`. This
collects normalized Organization, InsurancePlan, OrganizationAffiliation and
PractitionerRole facts from a registered published edition in one genuine
read-only repeatable-read snapshot. It does not copy providers or addresses,
approve old registry drafts, or confer provider membership eligibility.

The existing candidate stores immutable observations. Actual family counts and
canonical row digests determine the source generation before any replay/no-op
decision. The trusted sealer verifies the closed candidate's observations
against an authenticated SourceReader before sealing. Missing historical import proofs
remain missing; old proof-contract fields are recorded only as provenance.

Publication authorizes the current standing grant and mapping revision and uses
the existing metadata predecessor fence. These are point-in-time observations:
a later Source replacement does not invalidate a sealed catalog. A newer
published catalog prevents an older candidate from overwriting it. Observation
time is audit data and does not create a new generation for an identical read.
The source binding's existing `accepted_at` keeps its publication-time meaning;
collection/reconciliation time belongs to snapshot and job audit data.

The independent Reader needs only `SELECT(tableoid, source_id, endpoint_id)`
on the source-registration table and
`SELECT(tableoid, endpoint_id, canonical_api_base)` on the endpoint table.
Column reads pin those catalog relations with a bounded lock wait; they do not
require table-wide catalog reads. The transaction keeps the locks and verifies
the same physical relation families and row table OIDs before sealing.

The closed observation receipt contains `version: 1`,
`kind: catalog_observation`, `contract: registry-fhir-catalog-observation-v1`,
`publication`, and `families`. Publication provenance has exactly
`root_run_id`, `published_at`, `resource_hash_contract`,
`original_admission_version`, `original_admission_kind`,
`original_admission_sha256`, `original_metadata_sha256`, and
`claimed_resource_count`; nullable original-proof fields preserve absence.
`published_at` is the observed publication's UTC instant, not collection time.
Each of the four fixed family names maps to `{rows, sha256}`, including zeros.

Family hashes start with canonical JSON
`{"contract":"registry-fhir-catalog-observation-v1","resource_type":TYPE}`
and a newline. Append each canonical source row and a newline in strict
`(resource_type, resource_id)` byte order. A source row has exactly
`resource_type`, `resource_id`, `payload_hash` and `payload_json`; historical
payload hashes are included as claims, while the new digest binds the actual
observed normalized bytes. Hashing additional normalized fields is intentional
so a consumed field cannot change unnoticed.

Retain the descriptor's existing closed field set. Its observation receipt ID
is `registry-fhir-catalog-observation-v1:DIGEST`; the digest is SHA-256 of its
canonical document excluding only `accepted_receipt_id` and
`accepted_receipt_sha256`. This binds physical identity, policy and facts
without a circular checksum. Neither checksum alone establishes authority.

### Published-leaf reader scope

An optional `published_read_scope` extends the existing native read profile;
profiles without it retain their original four-field encoding. The closed v1
scope pins the own-login role and OID, database identity, fixed metadata
projections, exact resource root and published leaf OIDs, source aliases and
the binding relation. Each admitted endpoint/dataset includes its content hash
and stable publication provenance hash. The scope is bounded by encoded bytes
and aggregate dataset, leaf and alias counts.

Scoped reads use only authenticated physical leaves under an exact dataset LIST
partition, with complete ancestry and leaf coverage. Metadata reads use fixed
column projections from `ONLY` relations. A native read handle belongs to one
connection, task and transaction; extraction, SQL reference aggregation and
legacy identity lookup all require the same handle. Resource traversal has one
global keyset and byte/row limits across leaves. Mixed history/hash heaps and
unadmitted binding aliases refuse; a row filter cannot narrow a database grant.

The full Source runtime roles remain distinct from this login. Scoped semantic
admission refuses until the Source policy/checker supports the exact additional
reader; explicit metadata observation grants do not bypass that contract.
Issuance must qualify binding-writer closure, compatibility with every routable
Source checker and retained-capture entitlement. Existing sealed captures and
their roles, ACLs and receipts remain unchanged.

An issued finite scope covers only its declared editions. Completing unattended
reimport synchronization also requires accepted owner scope renewal after each
publication, without per-network maintenance or automatic privilege widening.
Local scoped-reader tests alone do not establish issuance or renewal authority.

Inside an owned read-only repeatable-read transaction,
`fhir_catalog_published_reads(connection)` reuses complete scope validation for
the exact connection, task, native transaction and canonical profile. Binding
disclosure is checked once per scope, while every granted edition still has its
publication and registration verified. Every handle and page retains native
identity, effective ACL and held-lock checks. An admitted zero-row header
projection is reopened through the simple query protocol before privilege
checks, rather than relying on previously prepared probes. Concurrent authority
changes and newly created roles or relations still require owner qualification.

Context exit clears proofs and owner references; a failed collector savepoint
invalidates cached proofs before another endpoint proceeds. Naked reads remain
uncached. Iterators inherit an explicit outer context without installing one
across their public yields. For N granted editions, one owned scope validation
performs N header and 2N registration queries plus one binding-disclosure scan;
this accounting is not a pipeline timing claim.

## Existing retained sources

The standing grant can also select existing verified retained recipes. Their
preparation and custody remain with the Source owner. Catalog reconciliation
does not prepare new full-provider captures or extend importer privileges.

### Reviewed product selectors

An existing canonical product may select one complete pinned InsurancePlan
through the existing binding approval flow. Its closed `source_scope` is:

```json
{"insurance_plan_id":"plan-one","capture_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","product_network_id":7}
```

The exact `source_key` is `InsurancePlan/plan-one`. Require the retained producer
`registry-retained-normalized-fhir-v1`, a source other than `cms-npd`, all six
exact source coordinates, and the genuine capture's custody. The positive int32
`product_network_id` must equal the enclosing canonical `network_id`. Keep one
product selector per target within those coordinates; combining it with an
Organization selector for that target is refused.

Distinct reviewed products may select the same plan. Expand every witnessed
root network reference set-wise, preserving each target's original binding UUID
and review evidence. Membership and preview deduplicate the same effective
target/Organization pairs. Automatic Organization identity allocation remains
unchanged, and product selectors create no Organization identity aliases.

Catalog projection emits target-specific relationships and a `last_good`
retained binding. Before sealing, independently replay the complete capture
observations, including carried facts, and compare the full derived product
sets. Approved selectors require this verification even when candidate product
output is empty. Product projection pages contain at most 128 edges and obey
the existing byte bounds; the complete Organization witness digest is bounded
separately. Prepare the retained-reference TEMP workspace before the Sealer's
read-only repeatable-read transaction and retain it through sealing commit.

Both catalog decoders require the enclosing target when validating this scope.
Capture proof and an explicit product approval remain necessary; catalog sync
does not approve a product assignment or publish provider membership.

## Search and transport

- The enriched wire view is `fhir-plans-v1`. Public `company_id`, `group_id`, and
  `plan_name` selectors choose it automatically; Layer sends the explicit view
  and canonical `plan_search` field to Core. Existing default responses remain
  compatible. Both API layers use the same selected view and strict decoder.
- `company_id` means insurance company; `group_id` means its corporate parent or
  NAIC group. IDs are UUIDs. Filters apply to the same visible relationship.
- A plan-name search is a literal, case-insensitive substring, with at least
  three characters. `%` and `_` do not become SQL wildcards.
- Each network includes company/group relationships and source bindings;
  nested pages contain at most five items. Unknown mappings remain explicitly
  unresolved, never inferred from similar names. Imported company/group facts
  serve network search without modifying the manual organization registry.
- Reads always pin metadata. When a real directory attachment exists, reads
  also verify and pin that exact membership generation. An unattached enriched
  response has null `generation` and `approved_custom_revision`, no
  `X-Network-Generation` header, and false `directory_available` values.
  Continuations require `metadata_generation` and `continuation_scope_sha256`.
  `network_generation` is an optional extra equality constraint, rejected for
  an unattached snapshot. The scope binds
  the actual optional attachment, visibility policy and filters. Stale pins
  require restart. Default catalog/provider routes retain their existing pins.
- A `last_good` binding remains usable and does not imply archive or deletion.

## Connected acceptance

One test must execute: admitted import, sync, Core and Layer query, replay,
replacement import omitting a network, sync and another query. Assert stable
existing IDs, allocation for new IDs, no duplicates, retained manual data, and
unchanged usable membership/address data. Component mocks cannot establish this
acceptance. Measure the complete pipeline separately from COPY throughput.
