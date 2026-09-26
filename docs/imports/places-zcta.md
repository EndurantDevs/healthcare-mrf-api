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
- Controlled jobs use a separate stage for each attempt and finalize before reporting success. Manual jobs retain the load/finish worker sequence above and support `HLTHPRT_IMPORT_ID_OVERRIDE`.
- `HLTHPRT_PLACES_ZCTA_PROTECTED_PUBLICATION=true` requires a compatible trusted publisher. In this mode, complete controlled jobs record an attempt-bound `places-stage-handoff-v1` receipt in `import_run.metrics.places_handoff` and remain `finalizing`. The ordinary worker cannot publish that handoff; the trusted publisher must validate and freeze the exact stage, preserve the predecessor, and commit publication with the terminal run state. Manual and test-mode loads are rejected. This setting defaults to `false` and must be enabled only after the publisher is ready.
- Optional tuning env keys:
  - `HLTHPRT_PLACES_ZCTA_DOWNLOAD_URL`
  - `HLTHPRT_PLACES_ZCTA_BATCH_SIZE`
  - `HLTHPRT_PLACES_ZCTA_TEST_ROWS`
  - `HLTHPRT_PLACES_ZCTA_MIN_ROWS`
