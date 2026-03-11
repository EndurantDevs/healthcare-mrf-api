# Geo Lookup Import

## Purpose
Loads the repository-bundled ZIP lookup support table used for city/state/ZIP normalization and local radius workflows.

## Source
- Repository support file under `support/zip/`

## Start Command
```bash
python main.py start geo
```

## Optional File Override
```bash
python main.py start geo --file /path/to/geo_city_public.csv
```

## Main Outputs
- `geo_zip_lookup`

## Notes
- This import does not fetch external data during runtime.
- It is used by ZIP-aware provider search, local benchmark expansion, and other geo features.
