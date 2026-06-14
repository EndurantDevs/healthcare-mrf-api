# 07 Source Inventory

## Purpose

This document is the implementation checklist for address-related data sources.
The unified serving model must not accidentally include only NPPES, MRF, and PTG.

Each source is classified as:

- **Address evidence:** source can directly support a provider/pharmacy/facility
  address row.
- **Coverage evidence:** source links an entity/address to plan, network,
  payer, drug, procedure, or PTG coverage.
- **Geo enrichment:** source enriches existing addresses with coordinates,
  county/FIPS, ZIP/ZCTA, census, or catchment metadata.
- **Derived/do-not-source:** source is already derived from other data and should
  not be treated as raw evidence.

## First-Class Address Evidence

| Source | Table(s) | Entity | Use |
|---|---|---|---|
| NPPES | `npi_address` | provider, organization, pharmacy by NPI | Core NPI address evidence. Preserve `primary`, `mail`, and `secondary` roles. |
| ACA/MRF provider-network addresses | `mrf_address`, `mrf_address_evidence` | provider/location/network | Core ACA network address and payer/network evidence. |
| CMS doctors/clinicians | `doctor_clinician_address` | provider | Independent doctor/clinician location and coordinates. |
| Facility anchors | `facility_anchor` | facility | Hospital, FQHC, and facility location evidence with coordinates. |
| CMS provider enrollment | `provider_enrollment_ffs`, `provider_enrollment_ffs_address` | provider, organization | Enrollment/PECOS-style address and provider-type evidence. |
| Pharmacy license | `pharmacy_license_record_v1`, optionally current history/stage during validation | pharmacy | State board license address, status, and verification evidence. |
| Part D pharmacy activity | `partd_pharmacy_activity_v2` | pharmacy | Medicare Part D pharmacy activity, plan activity, and address evidence. |

## Coverage Evidence

| Source | Table(s) | Coverage Contribution |
|---|---|---|
| ACA/MRF plans and networks | `mrf_address_evidence`, plan/network tables | ACA plan/network/issuer arrays. |
| PTG/group plan imports | PTG2 source/snapshot/provider/group tables, `ptg_address` | PTG plan/source/group filters and logical node-scoped provider membership. |
| Claims procedure imports | claims/pricing provider procedure tables | Procedure bridge rows for procedure/provider search. |
| Drug claims imports | drug/provider prescription tables | Medication bridge rows for drug/provider search. |
| Part D pharmacy imports | `partd_pharmacy_activity_v2`, Part D medication/cost tables | Medicare pharmacy activity and plan/drug context. |
| Provider enrichment | provider-enrichment summary and linked tables | provider type, enrollment, specialty, chain/organization context. |

## Geo Enrichment Sources

These sources enrich existing address rows. They should not independently
increase address-source confidence unless they also carry a real address
assertion.

| Source | Table(s) | Use |
|---|---|---|
| TIGER/PostGIS geocoder | PostGIS/TIGER tables | Geocode and spatial normalization. |
| ZIP reference | `geo_zip_lookup` | ZIP centroid, city/state fallback, ZIP validation. |
| Census/ZCTA/LODES/places | geo, LODES, places/census imports | County/FIPS, ZCTA, market/catchment and demographic enrichment. |
| Address archive | `address_archive_v2` | Canonical address identity and geocode metadata. |

## Derived Or Do-Not-Source Tables

These can be useful for comparison or migration, but should not be treated as
raw address evidence in the new model.

| Source | Table(s) | Reason |
|---|---|---|
| Existing unified table | current `entity_address_unified` | It is being replaced. Use only for migration comparison. |
| Existing PTG location projection | `ptg2_provider_location` | Derived PTG location table. New `ptg_address` should replace/supersede it. |
| Procedure location aggregate | `pricing_provider_procedure_location` | Derived/aggregated location context. Use for validation/search context, not raw address evidence. |
| Pharmacy license stage/history | `pharmacy_license_record_stage_v1`, `pharmacy_license_record_history_v1` | Useful for validation and audit; live serving should prefer current `pharmacy_license_record_v1`. |

## Source Priority Guidance

Priority should be query-specific, but default source confidence should roughly
prefer:

1. direct source address with exact NPI/entity match;
2. multi-source confirmed address;
3. direct MRF/provider-network address with plan/network context;
4. NPPES practice/primary address;
5. CMS doctor/facility/provider-enrollment address;
6. pharmacy license or Part D pharmacy address for pharmacy-specific queries;
7. inferred PTG attachment through NPI/TIN;
8. ZIP/city-only fallback.

Conflicts should remain visible in `entity_address_evidence`; do not overwrite
conflicting source assertions silently.

## Implementation Checklist

The first full rebuild is not complete until the builder has explicit source
adapters or explicit exclusions for:

- `npi_address`
- `mrf_address`
- `mrf_address_evidence`
- `doctor_clinician_address`
- `facility_anchor`
- `provider_enrollment_ffs`
- `provider_enrollment_ffs_address`
- `pharmacy_license_record_v1`
- `partd_pharmacy_activity_v2`
- `ptg_address`
- claims/procedure provider bridges
- drug/provider medication bridges
- geo enrichment tables

Each adapter should report:

- source table availability;
- row count considered;
- evidence rows emitted;
- rows with `address_key`;
- rows with coordinates;
- source freshness;
- skipped rows and reason buckets.

## Compactness Rule

Adapters must emit normalized IDs and source keys, not raw payload copies. Raw
source JSON, raw source URLs, and large text blobs remain in the original import
tables or artifacts. Derived tables store enough relational keys to rejoin or
debug the source when needed.
