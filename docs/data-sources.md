# Data Sources

This page is the canonical source registry for `healthcare-mrf-api`.
It documents every active import source family, which importer consumes it, and what data it powers.

Most importers resolve current files/distributions at runtime (from catalog APIs or source landing pages) instead of pinning one static artifact forever.

## Active Source Registry

| Source website | Dataset families used in this repo | Importers using it | Main outputs |
| --- | --- | --- | --- |
| <https://data.cms.gov/> | Medicare physician claims, Medicare Part D prescriber/spending, Part D formulary/pharmacy files, Medicare enrollment dashboard data APIs, provider-enrollment datasets | `claims-pricing`, `claims-procedures`, `drug-claims`, `partd-formulary-network`, `medicare-enrollment`, `provider-enrichment` | procedure pricing, prescription claims, pharmacy activity/formulary coverage, Medicare enrollment stats, PECOS sidecar enrichment |
| <https://data.cms.gov/provider-data/> | Provider Data Catalog distributions (Doctors & Clinicians, Hospital General Information) | `cms-doctors`, `facility-anchors` | clinician-practice location rows, hospital anchor rows |
| <https://www.cms.gov/marketplace/resources/data/public-use-files> | Federally facilitated marketplace PUF families (plans, issuers, rates, transparency metadata) | `mrf`, `plan-attributes` | marketplace plan/issuer/formulary/network/rates datasets |
| <https://www.cms.gov/marketplace/resources/data/state-based-public-use-files> | State-based exchange plan/rate/attribute files | `mrf`, `plan-attributes` | state-exchange plan and pricing coverage where configured |
| <https://download.cms.gov/nppes/NPI_Files.html> | NPPES NPI dissemination files | `npi` (canonical), `provider-enrichment` (NPPES field-mapping support) | provider identities, names, addresses, taxonomies, identifiers |
| <https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-fee-for-service-public-provider-enrollment> | Medicare FFS provider enrollment subfiles (additional NPIs, reassignment, address, secondary specialty) | `provider-enrichment` | Medicare enrollment flags, reassignment relationships, summary sidecars |
| <https://www.cms.gov/Medicare/Quality-Payment-Program/Quality-Payment-Program.html> | Public QPP measure files/feeds used by quality modeling | `provider-quality` | provider quality measures/domain scores and rating inputs |
| <https://www.atsdr.cdc.gov/place-health/php/svi/index.html> | CDC/ATSDR Social Vulnerability Index inputs | `provider-quality` | socioeconomic risk overlays for provider quality scoring |
| <https://chronicdata.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-ZCTA-Data-2025/qnzd-25i4/about_data> | CDC PLACES ZCTA public health indicators | `places-zcta` | normalized ZIP/ZCTA health indicator tables |
| <https://api.census.gov/data.html> | ACS 5-year, Decennial, and CBP ZIP/ZCTA profile APIs | `geo-census` | `geo_zip_census_profile` ZIP socioeconomic/demographic metrics |
| <https://lehd.ces.census.gov/data/> | LEHD/LODES WAC files | `lodes` | tract-to-ZIP workplace demand aggregates |
| <https://www.huduser.gov/hudapi/public/usps?type=2&query=All> | HUD USPS tract-to-ZIP crosswalk API | `lodes` | tract-to-ZIP allocation weights (token-backed path) |
| <https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_tract20_natl.txt> | Census tract↔ZCTA relationship file | `lodes` | public fallback tract-to-ZIP mapping when HUD token is unavailable |
| <https://data.hrsa.gov/data/download> | HRSA health-center service delivery/look-alike files | `facility-anchors` | FQHC facility anchor coverage |
| <https://data.medicaid.gov/> | SDUD, NADAC, FUL datasets | `pharmacy-economics` | state/NDC reimbursement and gross-margin reference tables |
| <https://www.nucc.org/index.php/21-provider-taxonomy> | NUCC provider taxonomy code set | `nucc` | taxonomy lookups used by NPI/provider filtering |
| <https://www.fda.gov/drugs/besaferx-your-source-online-pharmacy-information/locate-state-licensed-online-pharmacy> | FDA BeSafeRx board directory for discovery | `pharmacy-license` | state board source discovery, coverage metadata |
| State board endpoints (see below) | State-specific machine-readable exports/APIs/search adapters | `pharmacy-license` | pharmacy license status, discipline, and activity evidence |
| repo support file `support/zip/geo_city_public.csv` | local ZIP/city/state/lat/lng seed table | `geo` | baseline `geo_zip_lookup` data and local geo expansion |
| Configured payer TOC/rate/provider-reference URLs (TiC) | Transparency in Coverage machine-readable files | `ptg` | TiC TOC index plus in-network/allowed/provider-reference ingestion |
| Internal repository tables only (no external fetch) | unified address/entity materialization from existing imported tables | `entity-address-unified` | `entity_address_unified` search layer across NPI/providers/facilities |

## State Board Sources With Explicit Adapters

`pharmacy-license` currently includes concrete adapters for these machine-readable endpoints:

- TX: <https://www.pharmacy.texas.gov/downloads/phydsk.csv>
- FL: `mqa-internet.doh.state.fl.us` CSV export endpoint (`ExportToCsv`)
- CO Socrata: <https://data.colorado.gov/resource/7s5z-vewr.csv>
- WA Socrata: <https://data.wa.gov/resource/qxh8-f4bd.csv>
- NY ROSA API: <https://api.nysed.gov/rosa/V2/findPharmacies>
- MA board export API: <https://healthprofessionlicensing-api.mass.gov/api-public/export/data/board>
- NJ verification search adapter: <https://newjersey.mylicense.com/Verification_Bulk/Search.aspx?facility=Y>

Other states use discovery-driven machine-readable extraction via FDA BeSafeRx links where a direct adapter is not hardcoded.

## Notes

- External websites are import-time dependencies; the API serves local PostgreSQL tables at request time.
- Some imports can optionally use local override files/URLs via `HLTHPRT_*` environment variables.
- Dataset freshness is determined by each importer’s latest successful publish/swap cycle.
