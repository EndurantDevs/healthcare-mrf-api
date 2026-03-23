# Facility Anchors Import

## Purpose
Builds facility-anchor coverage for site-intelligence scoring using FQHC and hospital location datasets.

## Command
```bash
python main.py start facility-anchors [--test]
python main.py worker process.FacilityAnchors --burst
```

Publish is handled during worker shutdown.

## Source Websites
- <https://data.hrsa.gov/data/download>
- <https://data.cms.gov/provider-data/>

## Main Table
- `mrf.facility_anchor`

## Data Notes
- `FQHC` rows from HRSA service-delivery sites.
- `Hospital` rows from CMS Hospital General Information.
- Hospital coordinates fallback to ZIP centroid (`mrf.geo_zip_lookup`) when lat/lng is missing in source rows.

## Quality Guards
- Minimum staged row threshold.
- Minimum hospital geocode coverage ratio before publish.

## Key Environment Variables
- `HLTHPRT_FACILITY_ANCHORS_BATCH_SIZE`
- `HLTHPRT_FACILITY_ANCHORS_MIN_HOSPITAL_COORD_RATIO` (default `0.90`)
- `HLTHPRT_CMS_HOSPITAL_DATASET_ID` (default `xubh-q36u`)
- `HLTHPRT_CMS_HOSPITAL_CSV_URL` (optional override)

