# 08 Address Identity Contract

## Purpose

`entity_address_unified` and `ptg_address` depend on `address_archive_v2` for
physical address identity. This contract defines the minimum fields and behavior
the serving rebuild must enforce so API search does not serve superseded,
over-merged, or low-precision address identities as exact provider locations.

## Required Archive Fields

The rebuild expects `address_archive_v2` or a companion metadata table/view to
provide:

```text
address_key uuid primary key
premise_key uuid null
identity_version varchar not null
precision varchar not null
merged_into uuid null
zip5 varchar null
state_code varchar(2) null
city_norm varchar null
county_fips varchar null
lat numeric null
long numeric null
```

If names differ in the implementation, adapters should map them to this logical
contract before `ptg_address` or `entity_address_unified` reads the archive.

## Identity Version

Every published base build must record:

```text
archive_identity_version
base_address_version
```

`archive_identity_version` identifies the address archive keying rules. If the
archive re-keys, changes its normalization identity, or changes merge behavior,
the current base serving table and all PTG overlays are stale.

Rules:

- A PTG-only overlay refresh is allowed only when its stored
  `archive_identity_version` equals the current archive version.
- A full base rebuild is required when `archive_identity_version` changes.
- `ptg_address` must be rerun after a base rebuild because better canonical
  matches can change PTG address attachments.
- The operator health view should show stale archive identity
  versions as a blocker, not as a warning.

## Merge Resolution

If `address_archive_v2.merged_into` is present, rebuilds must resolve to the
terminal canonical key before publishing:

```text
source address_key -> merged_into -> ... -> terminal address_key
```

Rules:

- Do not publish serving rows that point to a superseded `address_key`.
- Detect and fail validation on merge cycles.
- Preserve the original source record key in evidence, not in the hot row.
- When an archive merge changes the terminal key, rebuild the base table and
  refresh PTG overlays.

If the archive later exposes a materialized `canonical_address_key`, use it
instead of recursive resolution at rebuild time.

## Precision Policy

The archive precision controls whether a row can be used for exact geo serving.

Current production/dev archive reality is simpler than the forward model:

```text
street
city_zip
```

Today, `street` is the only non-fallback precision class and can serve normal
provider/pharmacy location search when coordinates are present. `city_zip`
remains broad fallback only. Finer tiers such as `rooftop`, `parcel`,
`premise`, `interpolated`, and `range` are forward-compatible classes and should
not be used as hard gates until the archive actually emits them from real
geocode precision metadata.

Recommended precision classes:

```text
rooftop
parcel
premise
street_address
interpolated
range
city_zip
unknown
```

Rules:

- `rooftop`, `parcel`, `premise`, and `street_address` can serve normal
  provider/pharmacy distance searches.
- `interpolated` and `range` can serve distance searches with lower confidence.
- `city_zip` is broad fallback only. It must not place a provider at a shared
  ZIP/city centroid for exact nearby searches.
- `unknown` cannot serve geo radius queries unless explicitly requested as a
  broad fallback.

Rows with `city_zip` precision can still support:

- provider/pharmacy detail display when no better address exists;
- broad ZIP/city search fallback;
- diagnostics showing missing precise geocode.

They must be down-ranked and excluded from precise radius results by default.

## Premise Versus Unit Grain

`address_key` remains the unit-level or most specific canonical address identity
available. `premise_key` groups multiple units/suites/department lockboxes at the
same physical premise.

Rules:

- Provider/pharmacy serving rows use `address_key` when available.
- `premise_key` is used for grouping, dedupe display, conflict detection, and
  multi-source confirmation.
- Do not collapse different unit-level `address_key` values into one serving row
  unless the API intentionally requests premise-level grouping.
- Department lockboxes and city/ZIP-only fallbacks should not collapse unrelated
  providers into one precise search location.

## `location_key`

`location_key` is a stable serving-row identity. It is not a replacement for
`address_key`.

When a resolved `address_key` exists, compute:

```text
sha256(
  'v1|' ||
  entity_type || '|' ||
  entity_id || '|' ||
  coalesce(npi::text, '') || '|' ||
  coalesce(tin, '') || '|' ||
  address_role_id::text || '|' ||
  row_origin || '|' ||
  resolved_address_key::text
)
```

When no `address_key` exists, compute a fallback key from stable source and
normalized non-geocode fields:

```text
sha256(
  'v1|fallback|' ||
  entity_type || '|' ||
  entity_id || '|' ||
  coalesce(npi::text, '') || '|' ||
  coalesce(tin, '') || '|' ||
  address_role_id::text || '|' ||
  row_origin || '|' ||
  source_id::text || '|' ||
  coalesce(source_record_key, '') || '|' ||
  coalesce(zip5, '') || '|' ||
  coalesce(state_code, '') || '|' ||
  coalesce(city_norm, '')
)
```

Fallback keys must carry lower confidence and must not be treated as canonical
address identity. Do not include latitude/longitude buckets in fallback identity:
geocode coordinates can improve or move between rebuilds, which would create
avoidable identity churn for otherwise stable source rows.

`location_key` is stable only within the same archive identity version, merge
state, `row_origin`, and source inputs. It legitimately changes when:

- an archive rekey changes the terminal `address_key`;
- a `merged_into` redirect changes the resolved terminal key;
- a row moves between `base` and `ptg_overlay` origin;
- a fallback row later resolves to a real `address_key`.

All bridge tables that reference `location_key` must be rebuilt and published in
lockstep with `entity_address_unified`. APIs must not expose `location_key` as a
client-stable identifier, and clients must not be expected to cache it across
rebuilds.

## Build-Time Validation

A publish must fail if:

- archive identity version is missing;
- any live serving row points to an address with unresolved `merged_into`;
- a merge cycle is detected;
- `city_zip` rows are included in precise radius-search candidate sets;
- `location_key` is null or not deterministic for a stable input sample;
- any bridge row references a `location_key` outside the same staged publish;
- base and PTG overlay rows report different archive identity versions.

This validation belongs in both staged build checks and operator
health checks.
