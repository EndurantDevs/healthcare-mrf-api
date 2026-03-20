# PLACES ZCTA Import

## Purpose
Imports CDC PLACES ZIP/ZCTA health indicator measures into a normalized long table used by geo ZIP APIs.

## Source Website
- CDC PLACES ZCTA dataset:
  <https://chronicdata.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-ZCTA-Data-2025/qnzd-25i4/about_data>

## Start Command
```bash
python main.py start places-zcta
```

## Workers
```bash
python main.py worker process.PlacesZcta --burst
python main.py worker process.PlacesZcta_finish --burst
```

## Test Mode
```bash
python main.py start places-zcta --test
```

## Main Outputs
- `pricing_places_zcta`

## Notes
- Import scope is latest available PLACES year only.
- Storage is normalized by `(zcta, year, measure_id)`.
- Publish uses staging table swap to preserve rollback via `_old` table/index assets.
- Optional tuning env keys:
  - `HLTHPRT_PLACES_ZCTA_DOWNLOAD_URL`
  - `HLTHPRT_PLACES_ZCTA_BATCH_SIZE`
  - `HLTHPRT_PLACES_ZCTA_TEST_ROWS`
  - `HLTHPRT_PLACES_ZCTA_MIN_ROWS`
