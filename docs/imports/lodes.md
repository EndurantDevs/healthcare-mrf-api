# LODES Workplace Demand Import

## Purpose
Builds ZIP/ZCTA workplace-demand aggregates from Census LEHD/LODES WAC files for site-intelligence and local demand scoring.

## Command
```bash
python main.py start lodes [--test]
python main.py worker process.LODES --burst
```

Publish is handled during worker shutdown.

## Source Website
- <https://lehd.ces.census.gov/data/>

## Main Tables
- `mrf.lodes_workplace_aggregate` (live)
- `mrf.lodes_workplace_aggregate_<import_id>` (staging)

## Data Guarantees
- Requires tract-to-ZIP crosswalk mapping (HUD token, local crosswalk file, or Census tract/ZCTA relationship fallback).
- Rejects coarse county-FIPS fallback publishing.
- Supports per-state year fallback (`<= HLTHPRT_LODES_YEAR`).
- Enforces minimum row, distinct-ZIP, and geo-match thresholds before publish.

## Key Environment Variables
- `HLTHPRT_LODES_YEAR` (default `2021`)
- `HLTHPRT_LODES_MIN_YEAR` (default `2010`)
- `HLTHPRT_LODES_TEST_STATES` (comma-separated 2-char states, optional)
- `HLTHPRT_LODES_CROSSWALK_FILE` (optional local tract->ZIP CSV)
- `HLTHPRT_HUD_API_TOKEN` (optional HUD API token)
- `HLTHPRT_LODES_REQUIRE_CROSSWALK` (default `true`)

