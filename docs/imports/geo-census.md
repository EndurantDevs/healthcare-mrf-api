# Geo Census ZIP Profile Import

## Purpose
Loads a compact Census ZIP profile table used to enrich `/geo/zip/{zip_code}` responses with local socioeconomic metrics.

## Source
- U.S. Census API (`api.census.gov`)
  - ACS 5-year Subject/Profile/Detailed datasets
  - Decennial Census DHC dataset
  - County Business Patterns (CBP)

## Start Command
```bash
python main.py start geo-census
```

## Test Mode
```bash
python main.py start geo-census --test
```

## Main Outputs
- `geo_zip_census_profile`

## Notes
- Storage is ZCTA-scoped (5-digit ZIP keys) and intentionally compact.
- Stored fields include baseline demographics plus poverty/income/education, labor metrics,
  housing/rent/value, commute patterns, broadband, race/ethnicity counts+shares, and CBP
  establishments/employment/payroll.
- Year defaults are controlled by env vars:
  - `HLTHPRT_CENSUS_ACS5_YEAR` (default `2024`)
  - `HLTHPRT_CENSUS_DECENNIAL_YEAR` (default `2020`)
  - `HLTHPRT_CENSUS_CBP_YEAR` (default `2023`)
- API key is read from `HLTHPRT_CENSUS_API_KEY`.
