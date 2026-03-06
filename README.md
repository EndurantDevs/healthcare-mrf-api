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

- `claims-pricing` / `claims-procedures`, `drug-claims`, and `provider-quality` automatically create required canonical tables and indexes (`checkfirst`) during import startup.
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

Queue provider quality import (QPP + SVI + claims-derived quality model):

```shell
python main.py start provider-quality
```

Provider quality source safety defaults:

- `HLTHPRT_PROVIDER_QUALITY_FAIL_ON_SOURCE_ERROR=true` (default): abort run if QPP/SVI source download fails.
- `HLTHPRT_PROVIDER_QUALITY_ALLOW_DEGRADED_TEST_ONLY=true` (default): allow empty-source degraded fallback only in `--test`.
- `HLTHPRT_PROVIDER_QUALITY_BENCHMARK_MODE=national|state|zip` (default: `national`).
- `HLTHPRT_PROVIDER_QUALITY_MIN_STATE_PEER_N=30` (minimum peers to keep state benchmark before national fallback).
- ZIP-local mode settings:
  - `HLTHPRT_PROVIDER_QUALITY_ZIP_MIN_PEER_N=30`
  - `HLTHPRT_PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES=50`
  - `HLTHPRT_PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES=10`
- ZIP mode fallback chain: `zip -> state -> national`
- Materialization fan-out by mode:
  - `national` -> stores `national`
  - `state` -> stores `state`, `national`
  - `zip` -> stores `zip`, `state`, `national`
- Sharded materialization (finalize acceleration):
  - `HLTHPRT_PROVIDER_QUALITY_MATERIALIZE_SHARDED_ENABLED=false` (canary default; keeps legacy monolith path when false)
  - `HLTHPRT_PROVIDER_QUALITY_MATERIALIZE_SHARD_QUEUE=arq:ProviderQuality_finish` (default; keeps finalize+shards on same worker queue)
  - `HLTHPRT_PROVIDER_QUALITY_FINALIZE_MAX_TRIES=720` (avoid finalize retry exhaustion while shards are running)
  - `HLTHPRT_PROVIDER_QUALITY_MATERIALIZE_SHARD_MAX_TRIES=20`
  - `HLTHPRT_PROVIDER_QUALITY_LSH_SHARDS=8`
  - `HLTHPRT_PROVIDER_QUALITY_MEASURE_SHARDS=8`
  - `HLTHPRT_PROVIDER_QUALITY_DOMAIN_SHARDS=8`
  - `HLTHPRT_PROVIDER_QUALITY_SCORE_SHARDS=8`
  - `HLTHPRT_PROVIDER_QUALITY_SHARD_PARALLELISM=8`
  - `HLTHPRT_PROVIDER_QUALITY_SHARD_WORK_MEM=64MB`
  - `HLTHPRT_PROVIDER_QUALITY_SHARD_JIT=off`
  - `HLTHPRT_PROVIDER_QUALITY_SHARD_MAX_PARALLEL_GATHER=0`

Run reduced deterministic smoke import in test mode:

```shell
python main.py start claims-pricing --test
python main.py start drug-claims --test
python main.py start provider-quality --test
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

Run workers for provider quality:

```shell
python main.py worker process.ProviderQuality --burst
python main.py worker process.ProviderQuality_finish --burst
```

For sharded materialization, `ProviderQuality_finish` can now process shard jobs itself (same queue), which avoids finalize stalls if `ProviderQuality --burst` exits after chunk load.

Finalize concurrency guardrail:

- Do **not** run `ClaimsPricing_finish`, `DrugClaims_finish`, and `ProviderQuality_finish` in parallel.
- `ClaimsPricing_finish` and `DrugClaims_finish` both touch shared `code_catalog`/`code_crosswalk`; `ProviderQuality_finish` should run in a separate finalize window.
- `ProviderQuality_finish` also acquires a global finalize mutex key (`imports:finalize_mutex`) to avoid publish overlap.

Provider ranking option:

- `GET /api/v1/pricing/providers?order_by=tier_relevance` (or `/pricing/physicians`) ranks by quality tier (`high`, `acceptable`, `low`) and then by `score_0_100`.

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
python main.py finish provider-quality --import-id 20260215 --run-id <run_id> --test
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

- `specs/cms_claims_pricing_v1.md`
- `specs/drug_claims_v1.md`
- `specs/import_swap_backup_policy.md` (required `_old` table rollback policy)


## Examples

The status of your import will be seen here: ``http(s)://YourHost:8080/api/v1/import``

Some plan info (Put your Plan ID): ``http://127.0.0.1:8085/api/v1/plan/id/99999GG1234005``

## Additional Info

Please check [Healthcare Machine Readable Files Import Data](https://www.healthporta.com/healthcare-data/) Status Page. It shows the work of the Importer with status of Data from Healthcare Issuers (Insurance companies/agents).

Project by [Pharmacy Near Me](https://pharmacy-near-me.com/) & [HealthPorta](https://www.healthporta.com/about-us)

For testing purposes the API works and getting heavily tested in [Drugs Discount Card](https://pharmacy-near-me.com/drug-discount-card/)
