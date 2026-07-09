# 02 Data Model

## Overview

The model has four layers:

1. `address_archive_v2`: canonical physical address identity.
2. source/projection tables: `npi_address`, `mrf_address`, CMS/provider,
   pharmacy, Part D, and `ptg_address`.
3. `entity_address_evidence`: compact source assertions and provenance.
4. `entity_address_unified`: denormalized API serving/search table.

The serving tables are derived models. Raw payloads stay in source import tables
or retained artifacts.

## `address_archive_v2`

Existing canonical address table. It remains address-only and must not become a
provider/plan coverage table.

Responsibilities:

- dedupe normalized addresses;
- store stable `address_key`;
- store geocode and TIGER/PostGIS-derived fields;
- preserve source-bit coverage for address systems;
- publish an address identity version used by derived models;
- expose or allow resolution of `merged_into` redirects when canonical keys are
  merged or superseded.

The serving model is coupled to the archive identity contract in
[08 Address Identity Contract](08-address-identity-contract.md). A base rebuild
must record the archive identity version it used, resolve `merged_into` chains
to terminal keys, and treat `city_zip` precision as broad fallback only.

## Storage Principles

The serving tables are hot API/search structures. Keep them as small as
possible:

- no raw JSON payload copies;
- no raw source URLs;
- no duplicated full source records;
- prefer small integer source/type IDs over repeated strings where practical;
- keep display address text in `address_archive_v2` when `address_key` is
  available;
- use fixed-width IDs, bitmasks, and bridge tables for high-cardinality filters;
- duplicate only fields needed for fast API search or stable display;
- keep raw payloads in original source tables/artifacts.

## Source And Projection Tables

Address-bearing source tables remain source-specific and are not replaced:

- `npi_address`
- `mrf_address`
- `mrf_address_evidence`
- `doctor_clinician_address`
- `facility_anchor`
- `provider_enrollment_ffs`
- `provider_enrollment_ffs_address`
- `pharmacy_license_record_v1`
- `partd_pharmacy_activity_v2`

See [07 Source Inventory](07-source-inventory.md) for the complete
classification, including enrichment-only and derived tables.

`ptg_address` is different: it is a derived PTG provider-location and coverage
projection. PTG2 usually supplies provider identity and coverage, not direct
postal addresses, so `ptg_address` records the best address attached to PTG
coverage for a current snapshot.

### `ptg_address`

One row per current PTG source/snapshot/plan/provider/address attachment.

Recommended columns:

```text
node_id varchar null
source_key varchar not null
snapshot_id varchar not null
plan_id varchar null
ptg_plan_id varchar null
market_type varchar null

provider_group_id varchar null
provider_set_id varchar null
npi bigint null
tin varchar null

location_key varchar not null
address_key uuid null
premise_key uuid null
archive_identity_version varchar not null
address_precision varchar not null
address_source_id smallint not null
address_source_record_key varchar null
address_role_id smallint not null
location_confidence_id smallint not null

zip5 varchar null
state_code varchar(2) null
city_norm varchar null
county_fips varchar null
lat numeric null
long numeric null

ptg_plan_array varchar[] not null default '{}'
ptg_source_array varchar[] not null default '{}'
group_plan_array varchar[] not null default '{}'

base_address_version varchar null
ptg_snapshot_published_at timestamptz null
observed_at timestamptz null
updated_at timestamptz not null
```

Recommended primary key:

```text
(source_key, snapshot_id, coalesce(plan_id, ''), location_key)
```

`location_key` is a serving-row key, not a canonical address key. Its input
contract is defined in [08 Address Identity Contract](08-address-identity-contract.md).

Recommended indexes:

```text
btree(source_key, snapshot_id)
btree(plan_id)
btree(ptg_plan_id)
btree(npi)
btree(tin)
btree(address_key)
btree(premise_key)
btree(location_key)
btree(zip5)
btree(state_code, city_norm)
gist(Geography(ST_MakePoint(long::double precision, lat::double precision)))
gin(ptg_plan_array)
gin(ptg_source_array)
gin(group_plan_array)
```

`ptg_address` can be large, but it should be much cheaper to rebuild than the
full NPI/MRF base because it only covers current PTG snapshots.

## `entity_address_evidence`

Compact relational provenance rows. This table is for source/scoring/debug
metadata only. It must not duplicate raw source payloads.

Recommended columns:

```text
evidence_id bigserial primary key
location_key varchar not null
address_key uuid null
premise_key uuid null
archive_identity_version varchar null

entity_type varchar not null
entity_id varchar not null
npi bigint null
tin varchar null

source_id smallint not null
source_record_key varchar null
source_run_id varchar not null
source_snapshot_id varchar null
node_id varchar null

plan_id varchar null
network_id varchar null
ptg_plan_id varchar null
ptg_source_key varchar null
ptg_snapshot_id varchar null
market_type varchar null

address_role_id smallint null
location_confidence_id smallint not null
address_precision varchar null
observed_at timestamptz null
last_seen_at timestamptz null
retired_at timestamptz null
```

Evidence is append-only by source run or maintained in partitions keyed by
`source_run_id`. A nightly unified rebuild must not rebuild and swap all
evidence rows for unchanged source runs. New source runs upsert or append compact
evidence, then the serving rebuild reads only the current active source-run set.

Recommended primary access patterns:

- by `npi`
- by `location_key`
- by `address_key`
- by `source_id`
- by `plan_id`
- by `ptg_plan_id`
- by `source_run_id`
- by `source_snapshot_id`

Recommended indexes:

```text
btree(npi)
btree(location_key)
btree(address_key)
btree(entity_type, entity_id)
btree(source_id)
btree(source_run_id)
btree(plan_id)
btree(ptg_plan_id)
btree(ptg_source_key)
btree(ptg_snapshot_id)
```

No `jsonb` column belongs in this table. If a debugger needs full source data,
join back to the original source table using `source_id` and
`source_record_key`.

## `entity_address_unified`

Denormalized search table consumed by APIs.

Recommended columns:

```text
entity_type varchar not null
entity_id varchar not null
npi bigint null
tin varchar null
entity_name varchar null
entity_subtype_id smallint null
row_origin varchar not null

location_key varchar not null
address_key uuid null
premise_key uuid null
archive_identity_version varchar not null
address_precision varchar not null
address_role_id smallint not null
zip5 varchar null
state_code varchar(2) null
city_norm varchar null
county_fips varchar null
lat numeric null
long numeric null

taxonomy_array int[] not null default '{}'
aca_plan_array varchar[] not null default '{}'
aca_network_array varchar[] not null default '{}'
ptg_plan_array varchar[] not null default '{}'
ptg_source_array varchar[] not null default '{}'
group_plan_array varchar[] not null default '{}'

source_mask bigint not null default 0
address_source_mask bigint not null default 0
node_id varchar null
base_address_version varchar null
ptg_address_version varchar null

source_count int not null default 0
independent_source_count int not null default 0
multi_source_confirmed boolean not null default false
location_confidence_id smallint not null
confidence_score smallint null
freshness_score smallint null
last_seen_at timestamptz null

updated_at timestamptz not null
```

Recommended primary key:

```text
(location_key)
```

Recommended uniqueness guard:

```text
(entity_type, entity_id, address_role_id, location_key)
```

The hot row intentionally omits full address display fields when `address_key`
is present. API detail readers can join to `address_archive_v2` for display
address text. The hot row keeps `zip5`, `state_code`, `city_norm`, `lat`, and
`long` because those are search-critical.

Procedure and medication hot-row arrays are intentionally absent. Coverage can
be very high cardinality for one provider/location, so those dimensions are
bridge-only from day one.

## Physical Layout For Fast PTG Refresh

The logical serving name is `entity_address_unified`. Internally, implement it in
one of these ways:

1. Partitioned table, preferred:
   - base partition for non-PTG rows
   - PTG overlay partition for PTG rows
   - PTG-only refresh swaps only the overlay partition

2. Stable view:
   - `entity_address_unified_base`
   - `entity_address_unified_ptg_overlay`
   - `entity_address_unified` view as `UNION ALL`

3. Full physical table:
   - acceptable only if production timings prove PTG-only rebuild is fast enough

The first implementation should prefer partitioned table or stable view. A full
physical rewrite of all NPI/MRF-derived rows on every PTG import is not
acceptable.

Recommended `row_origin` values:

```text
base
ptg_overlay
```

Today the dev/prod deployment is one PostgreSQL host. The partition/view split
still gives the fast PTG win on a single database. Cross-node PTG overlays are a
future topology concern, not a requirement for the initial deployment.

## Address Roles

Recommended role values:

```text
primary
mail
secondary
practice
site
billing
inferred
unknown
```

API queries should usually prefer:

```text
practice, primary, site, secondary
```

## Confidence Codes

Recommended values:

```text
exact_source_address
canonical_address_match
multi_source_confirmed
inferred_from_nppes
inferred_from_cms_doctors
zip_city_only
source_conflict
unknown
```

The confidence code should be a categorical explanation. `confidence_score` can
be used for ranking, but it should not replace the code.

For compact storage, implement confidence codes as a small lookup table or enum
ID. The spec uses names for readability.

## Source Scoring

`source_count` is row support. `independent_source_count` is family support.

Count independent source families, not duplicate rows. Initial families:

```text
nppes
aca_mrf
ptg_or_group_plan
cms_doctors
facility_anchor
provider_enrollment
pharmacy_license
partd_pharmacy_activity
claims_pricing
drug_claims
```

NPPES and CMS doctors are separate operational feeds but may both trace to
self-reported provider data. Count them as separate families for coverage, but
cap the confidence gain from self-reported families so they do not equal a
direct payer/network or license confirmation.

Initial `confidence_score` formula should be deterministic and documented in
code:

```text
score =
  precision_points
  + directness_points
  + independent_family_points
  + freshness_points
  + geocode_points
  - conflict_penalty
  - stale_penalty
  - inferred_coverage_penalty
  - city_zip_penalty
```

Recommended starting weights:

```text
precision_points:
  street: 35
  exact / rooftop / parcel / street_address: 35
  premise: 30
  interpolated / range: 20
  city_zip: 5
  unknown: 0

directness_points:
  source supplied postal address for this entity: 25
  canonical match from another direct source: 18
  inferred from NPI/TIN identity only: 8
  coverage-only attachment: 3

independent_family_points:
  +8 for first independent family
  +5 for each additional independent family, capped at +20

freshness_points:
  observed within 12 months: 10
  observed within 24 months: 5
  older or unknown: 0

geocode_points:
  archive geocode verified and non-null: 10
  centroid/fallback only: 2

penalties:
  conflicting direct source at same role: -20
  stale source older than 36 months: -10
  inferred coverage-only address: -10
  city_zip precision used for broad fallback: -25
```

Clamp final score to `0..100`. `multi_source_confirmed` is true only when two or
more independent source families support the same resolved address key or
premise key without a direct conflict.

Current archive precision is expected to be only `street` and `city_zip` until
the geocoder starts emitting finer classes. Treat `street` as the exact serving
class and `city_zip` as broad fallback. The finer scoring tiers above are
forward-compatible and must not become publish blockers until real precision
metadata exists.

## Filter Semantics

Arrays are allowed only for low-cardinality hot-row filters. Bridge tables are
the default for high-cardinality dimensions.

Recommended hot-row arrays:

- `taxonomy_array`: NUCC taxonomy integer IDs/codes represented as current local
  system expects.
- `aca_plan_array`: ACA/marketplace plan filters when cardinality remains
  bounded.
- `aca_network_array`: ACA network filters when cardinality remains bounded.
- `ptg_plan_array`: group/PTG plan filters for current snapshots.
- `ptg_source_array`: PTG source/subscription routing filters.
- `group_plan_array`: group-plan filters when bounded.
- `source_mask`: compact source-system bitmask for source provenance.

Bridge-only dimensions:

- procedures/pricing;
- medications/drug-provider search;
- any plan/network dimension whose array cardinality or row width crosses the
  publish gate.

Recommended dictionary and bridge tables:

```text
procedure_concept(
  procedure_concept_id bigint primary key,
  code_system varchar not null,
  code varchar not null,
  active boolean not null
)

medication_concept(
  medication_concept_id bigint primary key,
  code_system varchar not null,
  code varchar not null,
  active boolean not null
)

entity_address_plan_bridge(location_key, entity_type, entity_id, plan_id, market_type)
entity_address_network_bridge(location_key, entity_type, entity_id, network_id)
entity_address_ptg_bridge(location_key, entity_type, entity_id, source_key, snapshot_id, ptg_plan_id)
entity_address_procedure_bridge(location_key, npi, procedure_concept_id)
entity_address_medication_bridge(location_key, npi, medication_concept_id)
```

`procedure_concept_id` and `medication_concept_id` must be stable across
rebuilds. If stable concept dictionaries do not exist yet, use natural keys
`(code_system, code)` directly in the bridge until the dictionary is available.
Do not use rebuild-local surrogate IDs in hot rows or bridges.

Bridge rows are tied to the currently published `location_key` set. Rebuild and
swap all bridge tables in lockstep with `entity_address_unified`; do not treat
`location_key` as a stable public ID across archive rekeys, merge redirects, or
base/PTG overlay reshaping.

## Indexes

Minimum serving indexes:

```text
btree(npi)
btree(entity_type, npi)
btree(entity_type, entity_id)
btree(location_key)
btree(address_key)
btree(premise_key)
btree(archive_identity_version)
btree(zip5)
btree(state_code, city_norm)
gist(Geography(ST_MakePoint(long::double precision, lat::double precision)))
gin(taxonomy_array)
gin(aca_plan_array)
gin(aca_network_array)
gin(ptg_plan_array)
gin(ptg_source_array)
```

Deployed serving refreshes also require model-defined partial indexes for the
API predicates that cannot rely on generic NPI or geo indexes:

```text
btree(npi) where type in service-location roles and address_key is not null
btree(npi) where type in service-location roles and premise_key is not null
btree(
  COALESCE(NULLIF(phone_number, ''), normalized_telephone_digits),
  npi
) where type in service-location roles and the normalized phone is present
gist(Geography(ST_MakePoint(long::double precision, lat::double precision)))
  where type in service-location roles and precise coordinates are present
```

These indexes must be declared on the model and created on stage tables before
the publish swap. A reimport must not depend on one-off migrations or manual
live indexes, because `entity_address_unified` is replaced by table swap.

Minimum bridge indexes:

```text
btree(entity_address_plan_bridge.plan_id, location_key)
btree(entity_address_network_bridge.network_id, location_key)
btree(entity_address_ptg_bridge.ptg_plan_id, source_key, location_key)
btree(entity_address_procedure_bridge.procedure_concept_id, location_key)
btree(entity_address_medication_bridge.medication_concept_id, location_key)
```

Query-plan checks decide whether a bounded plan/network array stays on the hot
row or moves to bridge-only storage. Procedure and medication dimensions are not
eligible for hot-row arrays.
