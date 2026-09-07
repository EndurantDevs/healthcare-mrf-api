# CMS Doctors Import

## Purpose
Imports CMS Doctors & Clinicians National Downloadable data, retaining practice
addresses and independently sourced medical-school/graduation-year assertions.

## Command
```bash
python main.py start cms-doctors [--test]
python main.py worker process.CMSDoctors --burst
```

Full publication is handled during worker shutdown. A bounded `--test` run
discards its staging tables and never publishes either dataset.

## Source Website
- <https://data.cms.gov/provider-data/>

## Main Table
- `mrf.doctor_clinician_address`
- `mrf.cms_doctor_education`

## Data Notes
- Preserves multiple practice locations per NPI.
- Dedupe key is `npi + address_checksum` (not NPI-only collapse).
- Supports CSV and ZIP source distributions.
- Education is read from `Med_sch` and `Grd_yr`, independently of missing
  practice addresses. Distinct NPI/school/year assertions are retained even when
  they disagree; repeated practice rows do not duplicate education facts.
- `OTHER` means no named school was provided. School-only and year-only records
  remain useful. Future years are retained with a `graduation_year_in_future`
  flag relative to acquisition time; they establish neither completed education
  nor student status. No years-of-practice value is inferred.
- Each education assertion retains the resolved source URL, file SHA-256,
  dataset ID, generation identity, acquisition timestamp, source row number and
  raw fields.
- Missing/duplicate headers, malformed CSV, invalid NPIs or malformed years
  abort acquisition. A full education publication requires at least 10,000
  assertions, one matching generation, and at least 80% of the incumbent
  assertion count. Both table swaps occur in one transaction.
- Failed acquisition or publication removes the unpublished education stage
  created by that worker. Existing stages belonging to another run are retained.
- Live tables retain one `_old` rollback generation. Florida and FHIR profile
  publications are not modified. The education table is created by its importer;
  API integration is delivered separately.
- CMS covers Medicare-listed clinicians of multiple professions. It is not an
  exhaustive physician roster or a source of residency/employment history.

## Key Environment Variables
- `HLTHPRT_CMS_DOCTORS_DATASET_ID` (default `mj5m-pzi6`)
- `HLTHPRT_CMS_DOCTORS_BATCH_SIZE`
- `HLTHPRT_CMS_DOCTORS_TEST_ROWS`
