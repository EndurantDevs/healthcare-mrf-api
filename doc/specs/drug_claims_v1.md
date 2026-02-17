# CMS Drug Claims v1 (HealthPorta-style)

## Scope

Separate Part D prescription import and API surface, independent from procedures/services claims.

## Data Sources

Importer resolves latest CSV URLs from the CMS portal (`https://data.cms.gov/`) for year `2023` by default:

1. Medicare Part D Prescribers by Provider and Drug
2. Medicare Part D Spending by Drug

## Commands

Start:

```bash
python main.py start drug-claims
```

Test mode:

```bash
python main.py start drug-claims --test
```

Finish queued run:

```bash
python main.py finish drug-claims --import-id 20260216 --run-id <RUN_ID>
```

## Canonical Tables

- `pricing_provider_prescription`
- `pricing_prescription`
- `code_catalog`
- `code_crosswalk`

## HP_RX_CODE External Crosswalk Enrichment

`drug-claims` finalize stage enriches `code_crosswalk` with external medication codes:

- `HP_RX_CODE <-> RXNORM`
- `HP_RX_CODE <-> NDC` (normalized NDC11 when available)

Source strategy is env-driven:

- `HLTHPRT_RX_CROSSWALK_SOURCE=hybrid|snapshot|live` (default `hybrid`)
- Snapshot mode reads normalized source rows from drug-api tables (`rx_data.product` + `rx_data.package` by default).
- Hybrid mode optionally performs bounded live fallback against drug-api for unresolved internal codes.

Confidence and volume controls:

- `HLTHPRT_RX_CROSSWALK_CONFIDENCE_MIN` (default `0.85`)
- `HLTHPRT_RX_CROSSWALK_MAX_NDC_PER_CODE` (default `5`)
- `HLTHPRT_RX_CROSSWALK_MAX_RXNORM_PER_CODE` (default `5`)

## API Endpoints

1. `GET /api/v1/pricing/providers/{npi}/prescriptions`
2. `GET /api/v1/pricing/providers/{npi}/prescriptions/{rx_code_system}/{rx_code}`
3. `GET /api/v1/pricing/prescriptions/{rx_code_system}/{rx_code}/providers`
4. `GET /api/v1/pricing/prescriptions/{rx_code_system}/{rx_code}/benchmarks`
5. `GET /api/v1/pricing/prescriptions/autocomplete`
6. `GET /api/v1/pricing/drugs/autocomplete` (alias)
7. `GET /api/v1/pricing/providers/by-prescription`
8. `GET /api/v1/pricing/providers/by-drug` (alias)

Aliases:

- `GET /api/v1/pricing/physicians/{npi}/prescriptions`
- `GET /api/v1/pricing/physicians/{npi}/prescriptions/{rx_code_system}/{rx_code}`
- `GET /api/v1/pricing/physicians/by-prescription`
- `GET /api/v1/pricing/physicians/by-drug`
