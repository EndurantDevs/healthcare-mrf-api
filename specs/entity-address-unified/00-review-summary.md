# 00 Review Summary

## Review Outcome

The proposed direction is sound with one important adjustment: reuse
`entity_address_unified` as the new internal serving/search table instead of
adding a second similarly named serving table.

This adjustment is safe because the existing `entity_address_unified` schema is
not a client-facing contract. We only need to preserve public API response
behavior.

## Accepted Decisions

- Keep raw source tables as source truth.
- Keep `address_archive_v2` as canonical physical address truth.
- Replace the current `entity_address_unified` shape completely.
- Add `ptg_address` as a fast logical node-scoped PTG provider-location and
  coverage projection. In the current one-PostgreSQL deployment, this is
  ownership/routing metadata rather than a separate database requirement.
- Add `entity_address_evidence` for provenance and audit.
- Rebuild `entity_address_unified` from published data instead of mutating it
  from every source import.
- Treat `entity_address_evidence` as compact append-only/source-run evidence,
  not as a nightly rebuilt hot table.
- Keep the serving/evidence tables compact: no raw JSON payload copies, no raw
  source URLs, no broad text duplication where an ID or normalized address key is
  enough.
- Bind serving rows to the `address_archive_v2` identity contract: archive
  identity version, merge resolution, precision policy, and deterministic
  `location_key`.
- Keep procedure and medication filters in bridge tables from day one.
- Publish with the staged-table rewrite/swap pattern.
- Move APIs that currently rely on `npi_address` for serving behavior to
  `entity_address_unified`.
- Treat PTG/group-plan data primarily as coverage/provider-membership evidence,
  not as direct postal-address evidence.

## Main Benefits

- One serving source for NPPES, ACA MRF, PTG/group, CMS doctors, facility
  anchors, provider enrollment, pharmacy license, and Part D pharmacy address
  search.
- Fast PTG refresh path that avoids rebuilding the large shared NPI/MRF base for
  every new PTG snapshot.
- Better ranking via source confidence and independent source counts.
- ACA, group/PTG, and no-insurance searches can share one model.
- Smaller hot tables by separating compact search columns from source payloads
  and high-cardinality coverage bridges.
- Failed rebuilds do not corrupt the live serving table.
- Raw source audit remains intact.
- API contracts can stay stable while internals improve.

## Main Risks

### Query-plan risk

`entity_address_unified` and `ptg_address` will be large and filter-heavy.
GIN/GIST/bridge indexes must be validated on production-like data before broad
API cutover.

Mitigation:

- run query-plan checks before publish
- cut over API groups gradually
- keep procedure and medication filters bridge-only
- move any bloated plan/network/PTG arrays to bridges if query plans degrade

### Address identity coupling risk

The serving table inherits address quality from `address_archive_v2`.
Over-merged `city_zip` rows, stale keys, or unresolved merge redirects can place
providers incorrectly.

Mitigation:

- record `archive_identity_version` on every serving row
- follow `merged_into` redirects before publish
- exclude `city_zip` precision from exact radius searches
- fail publish when base and PTG overlay archive versions differ

### Source attribution risk

PTG coverage can be attached to an address inferred from NPPES or another source.
The model must not imply PTG supplied the postal address unless it did.

Mitigation:

- separate `source_systems` from address source/confidence
- use explicit `location_confidence_code`
- keep evidence rows for explainability

### Rebuild timing risk

Full rebuilds may be expensive after NPI, MRF, and claims imports. PTG imports
may happen more often and must use the faster `ptg_address` plus overlay path.

Mitigation:

- schedule at night in America/Chicago
- coalesce dirty markers into one rebuild
- keep NPI/MRF base rebuild full and deterministic
- use PTG-only overlay refresh for new PTG snapshots
- consider incremental rebuild only after production timings are known

### API regression risk

Search results may improve but still differ from old `npi_address` behavior.

Mitigation:

- quantified dual-read comparison before cutover
- cut over one API family at a time
- preserve public response shapes
- keep temporary migration fallback

## Team Decisions Needed

1. Which optional dependencies should block production rebuild?
2. What row-count and geo-coverage thresholds are acceptable for publish?
3. Should clients see `confidence_score`, or only categorical fields first?
4. Should PTG arrays store internal IDs, public plan IDs, source keys, or all of
   them?
5. Should `ptg_address` store only current snapshots, or keep a configurable
   history window for debugging and rollback?
6. Should `entity_address_evidence` be served internally only, or exposed through
   admin/debug APIs?
7. What retention window should compact source-run evidence keep?
8. Which bounded plan/network/PTG filters must stay as arrays on the hot row?

## Recommendation

Proceed with the staged rebuild implementation. Keep the old raw source tables,
add `ptg_address`, replace `entity_address_unified`, add
`entity_address_evidence`, and cut APIs over gradually after dual-read
validation.
