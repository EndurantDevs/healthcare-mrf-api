# Medicare Enrollment Import

## Purpose
Imports annual county-level Medicare enrollment from CMS and publishes:
- county-canonical enrollment table
- ZIP-allocated enrollment table for query-time ZIP trade-area scoring

## Command
```bash
python main.py start medicare-enrollment [--test]
python main.py worker process.MedicareEnrollment --burst
```

Publish is handled during worker shutdown.

## Source Website
- <https://data.cms.gov/>

## Main Tables
- `mrf.medicare_enrollment_county_stats` (live county canonical)
- `mrf.medicare_enrollment_stats` (live ZIP-allocated)
- dated staging tables for both models

## Allocation Method
- CMS annual county rows are loaded by county FIPS.
- Counties are allocated to ZIP using `mrf.geo_zip_lookup` county/ZIP mapping.
- ZIP allocation uses population-weighted integer distribution.

## Quality Guards
- Minimum county and ZIP staged row thresholds.
- Maximum unmatched-county ratio threshold before publish.

## Key Environment Variables
- `HLTHPRT_MEDICARE_ENROLLMENT_BATCH_SIZE`
- `HLTHPRT_MEDICARE_ENROLLMENT_PAGE_SIZE`
- `HLTHPRT_MEDICARE_ENROLLMENT_TEST_ROWS`
- `HLTHPRT_MEDICARE_ENROLLMENT_MAX_UNMATCHED_COUNTY_RATIO` (default `0.10`)

