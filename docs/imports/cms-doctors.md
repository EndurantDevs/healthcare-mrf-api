# CMS Doctors Import

## Purpose
Imports CMS Doctors & Clinicians National Downloadable data and maps NPIs to real practice addresses for supply-intensity and site-scoring workflows.

## Command
```bash
python main.py start cms-doctors [--test]
python main.py worker process.CMSDoctors --burst
```

Publish is handled during worker shutdown.

## Source Website
- <https://data.cms.gov/provider-data/>

## Main Table
- `mrf.doctor_clinician_address`

## Data Notes
- Preserves multiple practice locations per NPI.
- Dedupe key is `npi + address_checksum` (not NPI-only collapse).
- Supports CSV and ZIP source distributions.

## Key Environment Variables
- `HLTHPRT_CMS_DOCTORS_DATASET_ID` (default `mj5m-pzi6`)
- `HLTHPRT_CMS_DOCTORS_BATCH_SIZE`
- `HLTHPRT_CMS_DOCTORS_TEST_ROWS`

