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
