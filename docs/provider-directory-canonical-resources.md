# Provider Directory Canonical Resource Storage

## Problem

Provider Directory imports currently store normalized FHIR resources under
`(source_id, resource_id)`. This preserves per-payer/source provenance and keeps
existing joins simple, but it duplicates the same remote FHIR resources when
multiple catalog rows point to the same canonical API base.

Live dev evidence from `run_aba92496eb0d4567ba9caeec8862b0dc` showed the
largest case:

- `https://fhir.humana.com/api`
- `Practitioner`
- `18` source aliases
- `4,230,203` source/resource rows
- `235,203` distinct remote resource IDs
- about `18x` fan-out

The deployed coverage audit now reports this as `alias_fanout_summary`.

## Current Correctness Contract

Do not remove source fan-out from the existing tables until downstream joins are
made canonical-aware.

Current address-unified paths require source-local rows:

- `process/entity_address_unified.py`
  - Practitioner path joins `provider_directory_practitioner_role` to
    `provider_directory_practitioner` on `source_id` and normalized practitioner
    reference.
  - Practitioner path joins `provider_directory_location` on `source_id` and
    normalized location reference.
  - Organization path joins `provider_directory_organization_affiliation` to
    `provider_directory_organization` and `provider_directory_location` on the
    same `source_id`.
- `process/provider_directory_fhir.py`
  - PTG corroboration payloads retain `provider_directory_source_id`,
    location resource ID, role/affiliation IDs, and network refs.

This means source-level tables are still the compatibility layer for current API
serving, address archive publication, and PTG corroboration.

## Target Shape

Add canonical remote-resource storage beside the current source-level tables.
The source-level tables remain available until every consumer is migrated.

Proposed tables:

- `provider_directory_canonical_resource`
  - `canonical_api_base`
  - `resource_type`
  - `resource_id`
  - `resource_url`
  - `payload_hash`
  - normalized payload columns or a `payload_json`
  - `first_seen_run_id`
  - `last_seen_run_id`
  - `observed_at`
  - `updated_at`
- `provider_directory_source_resource`
  - `source_id`
  - `canonical_api_base`
  - `resource_type`
  - `resource_id`
  - `last_seen_run_id`
  - `observed_at`
  - primary key on `(source_id, resource_type, resource_id)`

The alias edge table preserves provenance without copying the same canonical
resource body once per source alias.

## Migration Strategy

1. Publish read-only diagnostics first.
   - Done: `provider_directory_coverage_audit.py` reports
     `alias_fanout_summary`.
   - Gate: dev audit shows which API bases/resources are worth canonicalizing.

2. Add canonical tables without changing serving behavior.
   - Populate canonical rows during import.
   - Populate source-resource edge rows during import.
   - Keep existing per-source tables exactly as they are.
   - Gate: row counts prove canonical distinct IDs and edge counts match the
     existing source-level table semantics.

3. Add canonical-aware resolver views.
   - Views should expose the current source-level shape by joining source edges
     to canonical resource rows.
   - The view must be bit-for-bit equivalent for:
     - role to practitioner lookup
     - role to location lookup
     - affiliation to organization lookup
     - affiliation to location lookup

4. Switch address-unified and PTG corroboration to the compatibility views.
   - Keep `source_id` in emitted records and payloads.
   - Keep `source_record_id` stable enough for provenance debugging.
   - Gate: generated SQL tests plus dev row-count parity for Provider Directory
     unified rows, keyed rows, phone rows, and PTG corroboration rows.

5. Stop writing duplicated source-level payload tables only after parity.
   - Existing tables can become compatibility views or retained rollback tables.
   - Gate: API provider/address search latency remains within the existing
     target envelope and coverage audit stays green.

## Self-Harness

Each implementation step should have a local harness before dev deployment:

- Unit SQL tests:
  - canonical table DDL contains expected primary keys and indexes.
  - source-resource edge DDL contains `(source_id, resource_type, resource_id)`.
  - compatibility views retain `source_id`, `resource_id`, and joinable refs.
- Fixture import:
  - two source aliases share one `canonical_api_base`.
  - both aliases point to the same Practitioner, Location, and Role resources.
  - canonical resource count is `1`.
  - source-resource edge count is `2`.
  - source-compatible view count is `2`.
- Address-unified smoke:
  - Provider Directory practitioner address rows are unchanged before and after
    switching to compatibility views.
  - `source_record_id` still contains source and role/location identifiers.
- Dev audit:
  - `alias_fanout_summary.excess_source_resource_rows` drops after switching
    write paths.
  - `provider_directory_rows`, keyed percentage, phone percentage, and retained
    source-record-ID percentage remain stable.

## API-Side Performance Notes

API serving should not query canonical resource tables directly for address
search. It should continue to use the published address archive and
entity-address-unified serving tables. Canonical storage is an import/storage
optimization and a provenance improvement; serving latency should remain tied to
the existing indexed serving tables.

Any endpoint that exposes source provenance should read from the edge table or
from compatibility views, not from canonical resource rows alone.
