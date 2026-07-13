# 04 API Cutover

## Principle

Public API response contracts should remain stable while the data source changes
from `npi_address`-centered reads to `entity_address_unified` reads.

The table schema is internal. API payloads are the client contract.

## Primary API Surfaces

Move these read paths to `entity_address_unified` where they need provider,
pharmacy, address, coverage, or geo behavior:

```text
/api/v1/providers
/api/v1/providers/{npi}
/api/v1/providers/geo
/api/v1/providers/{npi}/nearby

/api/v1/pharmacies
/api/v1/pharmacies/{npi}
/api/v1/pharmacies/geo
/api/v1/pharmacies/{npi}/nearby

/api/v1/provider-services-claims/*
/api/v1/drug-claims/*
/api/v1/pricing/providers/search-by-procedure
/api/v1/pricing/providers/search-by-drug

PTG/group-plan provider search
site-intelligence provider/pharmacy geo logic
pharmacy market/report endpoints
```

## Cutover Order

### Phase 1: Read-only dual comparison

Build `entity_address_unified` but keep APIs on current sources.

Compare:

- provider by NPI
- pharmacy by NPI
- provider geo by ZIP
- pharmacy geo by ZIP
- procedure provider search
- drug provider search
- PTG/group-plan provider search

### Phase 2: Provider/pharmacy detail reads

Move low-risk detail enrichment first:

- provider address list
- pharmacy address list
- source/confidence optional metadata

Fallback to old source can remain behind an environment flag during migration.

Detail reads must assemble one public location per provider/pharmacy/address
after reading base and PTG overlay rows. If the same provider/address appears in
both `row_origin = base` and `row_origin = ptg_overlay`, merge them by:

```text
entity_type
entity_id or npi
address_role
resolved address_key, or premise_key when detail grouping is premise-level
```

The response should union coverage filters, source masks, evidence summaries,
and confidence metadata from both rows. It must not return duplicate location
items only because the PTG overlay adds coverage for the same physical address.
`location_key` remains an internal rebuild-scoped row key and is not the public
dedupe key.

### Phase 3: Geo search

Move geo search to `entity_address_unified`:

- provider geo
- pharmacy geo
- nearby provider/pharmacy

Validate latency and result shape carefully because these paths are visible and
query-plan sensitive.

### Phase 4: Pricing/provider searches

Move procedure and drug provider searches:

- procedure providers
- prescription/drug providers
- provider quality/location enrichment

### Phase 5: PTG/group-plan search

Move group-plan provider filtering to the PTG filters and evidence in
`entity_address_unified`.

This phase should verify:

- ACA-only plan search
- PTG/group-plan search
- no-insurance general search
- plan/source-specific PTG routing behavior
- PTG-only refresh behavior after `ptg_address` swaps

### Phase 6: Reports and site intelligence

Move aggregate/reporting readers after core search is stable.

## Migration Flag

Use a short-lived safety flag:

```text
HLTHPRT_ADDRESS_SERVING_SOURCE=entity_address_unified|legacy
```

This is a migration safety control only. It is not a long-term legacy support
mode.

## Response Additions

Do not remove existing fields during cutover.

Optional new fields can be added after API review. These should be derived from
compact lookup IDs/bitmasks, not stored as repeated string arrays on every hot
row unless query plans require it:

```json
{
  "address_sources": ["nppes", "aca_mrf", "ptg"],
  "source_systems": ["nppes", "aca_mrf", "ptg"],
  "multi_source_confirmed": true,
  "location_confidence_code": "multi_source_confirmed",
  "confidence_score": 91
}
```

Private/internal data must not leak:

- raw private source URLs
- signed URLs
- credentials
- internal node names
- raw crawler payloads
- client-specific private import metadata

## PTG API Behavior

For PTG/group-plan searches:

- plan/source filters should use `ptg_plan_array`, `ptg_source_array`, or PTG
  bridge tables where applicable.
- PTG plan/source filters should be populated from `ptg_address`, not by
  repeatedly scanning raw PTG2 snapshots in the API path.
- provider location should come from the best known canonical address.
- address source should not be labeled PTG unless PTG supplied the address.
- coverage source can include PTG even when address source is NPPES or MRF.
- APIs should tolerate a stale PTG overlay by surfacing old live data until the
  next successful `ptg-address` and overlay publish.
- procedure and medication provider searches should join bridge tables, not
  filter on wide hot-row arrays.
- Result assembly should dedupe by provider plus resolved address identity, then
  union PTG/group coverage onto the public location.

## Compatibility Checks

Every cutover phase needs before/after checks:

```text
status code
top-level payload shape
pagination metadata
item count range
required fields
geo sorting/distance when applicable
filter behavior
authorization behavior through an external API gateway
```

## External Client Impact

External gateways and automation clients should not need large contract changes
if the upstream response shape is preserved.

Expected dependent work:

- update e2e expectations if optional source/confidence fields are added
- add smoke tests for ACA vs PTG/group vs no-insurance searches
- ensure provider, pharmacy, and pricing clients can benefit from richer coverage
  without changing required arguments

## Legacy Table Reads

After cutover, direct `npi_address` reads should remain only for:

- NPI source-specific endpoints
- import diagnostics
- source audit/debug
- temporary fallback during migration

Normal provider/pharmacy/location serving should read `entity_address_unified`.
