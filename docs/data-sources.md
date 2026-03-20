# Data Sources

This page lists the public source websites used by `healthcare-mrf-api`.
The project usually resolves current files or distributions from these websites at import time; it does not depend on a single hard-coded artifact forever.

## CMS and Medicare Sources

### CMS Data Portal
Website: <https://data.cms.gov/>

Used for:

- Medicare physician and other practitioners claims summaries
- Medicare Part D prescriber and drug spending datasets
- Quality Payment Program related public files
- Medicare Part D formulary and pharmacy network public files
- Medicare provider enrollment public files

### Health Insurance Exchange Public Use Files
Website: <https://www.cms.gov/marketplace/resources/data/public-use-files>

Used for:

- marketplace machine-readable URL manifests
- plan transparency public use files
- marketplace plan and issuer metadata
- rate and benefits-oriented plan imports

### U.S. Census Data API
Website: <https://api.census.gov/data.html>

Used for:

- ZIP/ZCTA-level Census profile metrics used by geo ZIP enrichment
- ACS 5-year socioeconomic/labor/housing/commute/broadband metrics
- Decennial population and race/ethnicity counts used for ZIP race shares
- ZIP-level County Business Patterns establishment/employment/payroll metrics

### State-based Exchange Public Use Files
Website: <https://www.cms.gov/marketplace/resources/data/state-based-public-use-files>

Used for:

- state exchange plan attributes when those state-specific files are configured

### NPPES NPI Files
Website: <https://download.cms.gov/nppes/NPI_Files.html>

Used for:

- provider identity and directory data
- addresses, taxonomies, other identifiers, and business names

### Medicare Fee-for-Service Public Provider Enrollment
Website: <https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-fee-for-service-public-provider-enrollment>

Used for:

- PECOS / Medicare enrollment enrichment
- additional NPIs, reassignment, address, and secondary specialty subfiles

### Quality Payment Program
Website: <https://www.cms.gov/Medicare/Quality-Payment-Program/Quality-Payment-Program.html>

Used for:

- public quality and cost performance inputs used by the provider quality model

### Medicare Plan Finder
Website: <https://www.medicare.gov/>

Used indirectly for:

- understanding the business source behind monthly and quarterly Part D formulary and pharmacy network public files

## Public Health and Taxonomy Sources

### CDC / ATSDR Social Vulnerability Index
Website: <https://www.atsdr.cdc.gov/place-health/php/svi/index.html>

Used for:

- ZIP/ZCTA-level socioeconomic adjustment in provider quality imports

### CDC PLACES Local Data for Better Health (ZCTA)
Website: <https://chronicdata.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-ZCTA-Data-2025/qnzd-25i4/about_data>

Used for:

- ZIP/ZCTA-level public health indicator metrics
- long-form PLACES measure rows consumed by geo ZIP PLACES endpoints

### National Uniform Claim Committee (NUCC)
Website: <https://www.nucc.org/index.php/21-provider-taxonomy>

Used for:

- provider taxonomy code system and lookups
- specialty normalization and provider filtering support

## Pharmacy Regulation Sources

### FDA BeSafeRx Online Pharmacy Resources
Website: <https://www.fda.gov/drugs/besaferx-your-source-online-pharmacy-information/locate-state-licensed-online-pharmacy>

Used for:

- discovering state pharmacy board sources for pharmacy license imports

### State Pharmacy Boards
Websites: discovered from FDA BeSafeRx and then fetched per state

Used for:

- pharmacy license status, discipline, and state coverage imports

## Repository-bundled Support Data

### ZIP / City Lookup Support Files
Location in repo: `support/zip/`

Used for:

- ZIP-to-city/state/lat/long lookup
- local geographic expansion and radius matching

This import is repository-backed rather than fetched from an external source during runtime.
