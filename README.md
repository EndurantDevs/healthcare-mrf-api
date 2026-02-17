# healthcare-mrf-api
US Healthcare Machine Readable Files API with Importer

## Data provenance

Primary upstream sources used by this service:

- CMS data portal: `https://data.cms.gov/`
- CMS Physician & Other Practitioners (provider): `https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider`
- CMS Physician & Other Practitioners (provider and service): `https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service`
- CMS Physician & Other Practitioners (geography and service): `https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-geography-and-service`
- CMS Part D Prescribers (provider and drug): `https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug`
- CMS Part D Spending by Drug: `https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-spending-by-drug/medicare-part-d-spending-by-drug`
- CMS NPPES provider directory files: `https://download.cms.gov/nppes/NPI_Files.html`
- NUCC provider taxonomy: `https://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40`

Claims freshness policy:

- Claims/pricing APIs publish from the most recent fully imported CMS reporting year.
- Current freshest claims year in production docs/default import window: **2023**.
- Defaults are controlled by `HLTHPRT_CLAIMS_MIN_YEAR/MAX_YEAR` and `HLTHPRT_DRUG_CLAIMS_MIN_YEAR/MAX_YEAR` (currently `2023`).

Docker Dev builds can be pulled from:
```shell
docker pull dmytronikolayev/healthcare-mrf-api
```

Please use .env.example file for the configuration reference.
You will need to have PostgreSQL and Redis to run this project.
To use API you will need to execute the import for the first time after the container start.

Crontab example to run weekly updates of the data from FDA API:
```shell
15 5 * * 1 docker exec CHANGE_TO_THE_NAME_OF_DRUG_API_CONTAINER /bin/bash -c 'source venv/bin/activate && python main.py start mrf && python main.py worker process.MRF --burst' > /dev/null 2>&1
```

Once you do the first import(the comand that you added to crontab) - the data will be available on 8080 port of your Docker image.

## CMS Claims Pricing Imports

Schema bootstrap note:

- `claims-pricing` / `claims-procedures` and `drug-claims` automatically create required canonical tables and indexes (`checkfirst`) during import startup.
- Alembic migrations are optional for these import pipelines; imports are migration-independent for table creation.

Queue procedures/services claims import:

```shell
python main.py start claims-pricing
# alias:
python main.py start claims-procedures
```

Queue prescriptions (Part D) import:

```shell
python main.py start drug-claims
```

Run reduced deterministic smoke import in test mode:

```shell
python main.py start claims-pricing --test
python main.py start drug-claims --test
```

Run workers (burst mode) to process procedures chunks and finalize publish:

```shell
python main.py worker process.ClaimsPricing --burst
python main.py worker process.ClaimsPricing_finish --burst
```

Run workers for drug claims:

```shell
python main.py worker process.DrugClaims --burst
python main.py worker process.DrugClaims_finish --burst
```

Drug code crosswalk enrichment (`HP_RX_CODE -> NDC/RXNORM`):

- `HLTHPRT_RX_CROSSWALK_SOURCE=hybrid|snapshot|live` (default: `hybrid`)
- Snapshot source defaults to drug-api tables in the same database:
  - `HLTHPRT_RX_CROSSWALK_SNAPSHOT_SCHEMA=rx_data`
  - `HLTHPRT_RX_CROSSWALK_SNAPSHOT_PRODUCT_TABLE=product`
  - `HLTHPRT_RX_CROSSWALK_SNAPSHOT_PACKAGE_TABLE=package`
- Optional live fallback for unresolved rows (enabled in `hybrid` by default):
  - `HLTHPRT_RX_CROSSWALK_LIVE_BASE_URL=http://127.0.0.1:8087`
  - `HLTHPRT_RX_CROSSWALK_LIVE_MAX_CODES=400`
  - `HLTHPRT_RX_CROSSWALK_LIVE_MAX_PRODUCTS_PER_CODE=12`
  - `HLTHPRT_RX_CROSSWALK_LIVE_CONCURRENCY=8`
  - `HLTHPRT_RX_CROSSWALK_LIVE_TIMEOUT_SECONDS=12`
- Match-quality controls:
  - `HLTHPRT_RX_CROSSWALK_CONFIDENCE_MIN=0.85`
  - `HLTHPRT_RX_CROSSWALK_MAX_NDC_PER_CODE=5`
  - `HLTHPRT_RX_CROSSWALK_MAX_RXNORM_PER_CODE=5`

Manual finalize re-queue (if needed):

```shell
python main.py finish claims-pricing --import-id 20260215 --run-id <run_id> --test
python main.py finish drug-claims --import-id 20260215 --run-id <run_id> --test
```

Useful test-mode tuning for huge CMS files:

```shell
HLTHPRT_CLAIMS_TEST_MAX_DOWNLOAD_BYTES=26214400 \
HLTHPRT_CLAIMS_TEST_PROVIDER_ROWS=5000 \
HLTHPRT_CLAIMS_TEST_PROVIDER_DRUG_ROWS=10000 \
HLTHPRT_CLAIMS_TEST_DRUG_SPENDING_ROWS=2000 \
python main.py start claims-pricing --test
```

Tune chunk splitting and queue behavior:

```shell
HLTHPRT_CLAIMS_CHUNK_TARGET_MB=128 \
HLTHPRT_CLAIMS_DOWNLOAD_CONCURRENCY=3 \
HLTHPRT_MAX_CLAIMS_JOBS=20 \
python main.py start claims-pricing
```

High-throughput mode (recommended for full imports):

```shell
HLTHPRT_CLAIMS_DEFER_STAGE_INDEXES=true \
HLTHPRT_CLAIMS_IMPORT_BATCH_SIZE=100000 \
python main.py start claims-pricing
```

Detailed v1 pricing scope and endpoint notes:

- `doc/specs/cms_claims_pricing_v1.md`
- `doc/specs/drug_claims_v1.md`
- `specs/import_swap_backup_policy.md` (required `_old` table rollback policy)


## Examples

The status of your import will be seen here: ``http(s)://YourHost:8080/api/v1/import``

Some plan info (Put your Plan ID): ``http://127.0.0.1:8085/api/v1/plan/id/99999GG1234005``

## Additional Info

Please check [Healthcare Machine Readable Files Import Data](https://www.healthporta.com/healthcare-data/) Status Page. It shows the work of the Importer with status of Data from Healthcare Issuers (Insurance companies/agents).

Project by [Pharmacy Near Me](https://pharmacy-near-me.com/) & [HealthPorta](https://www.healthporta.com/about-us)

For testing purposes the API works and getting heavily tested in [Drugs Discount Card](https://pharmacy-near-me.com/drug-discount-card/)
