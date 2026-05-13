# MRF Import

## Purpose
Imports marketplace-oriented issuer, plan, formulary, plan network, and transparency metadata used by the ACA / exchange side of the API.

## Source Websites
- CMS Exchange Public Use Files: <https://www.cms.gov/marketplace/resources/data/public-use-files>
- CMS state-based exchange public use files: <https://www.cms.gov/marketplace/resources/data/state-based-public-use-files>

## Start Command
```bash
python main.py start mrf
```

## Workers
```bash
python main.py worker process.MRF --burst
python main.py worker process.MRF_finish --burst
```

Concurrency notes:

- `HLTHPRT_MAX_MRF_JOBS=10` means one `process.MRF` worker process can run up to 10 jobs concurrently.
- It does not start 10 separate OS worker processes.
- To launch 10 actual worker processes locally, use the helper script:

```bash
support/run_mrf_parallel.sh --workers 10 --import-id 20260405
```

Test-schema smoke run:

```bash
support/run_mrf_parallel.sh --test --workers 10 --import-id 20260405
```

## Manual Finish
```bash
python main.py finish mrf
```

## Test Mode
```bash
python main.py start mrf --test
```

## Main Outputs
Examples of canonical outputs populated by this pipeline include:

- `issuer`
- `plan`
- `plan_formulary`
- `plan_benefits_marketplace`
- `plan_transparency`
- `plan_drug_raw`
- `plan_npi_raw`
- `plan_network_tier_raw`
- `mrf_address`
- `mrf_address_evidence`
- import history and import logs

## Notes
- This is one of the original marketplace ingestion pipelines in the repo.
- It works from configured CMS marketplace PUF families rather than one fixed file.
- It is broader than the dedicated `plan-attributes` import and is responsible for several marketplace-facing objects.
- `plans.json benefits[*]` are normalized into `plan_benefits_marketplace` while the original raw payload remains on `plan.benefits`.
- `providers.json addresses[*]` are normalized into `mrf_address` using the same `(npi, type, checksum)` address identity pattern as `npi_address`.
- Exact marketplace address provenance is preserved in `mrf_address_evidence`, including issuer id, issuer name, import id, import date, source file URL, and source record id.
- Aggregated address rows in `mrf_address` expose summary provenance arrays such as `source_issuer_ids`, `source_issuer_names`, `source_import_ids`, `source_import_dates`, and `source_urls`.
