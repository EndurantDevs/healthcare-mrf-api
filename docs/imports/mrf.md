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

- `HLTHPRT_MAX_MRF_JOBS=8` means one `process.MRF` worker process can run up to 8 jobs concurrently.
- It does not start 8 separate OS worker processes.
- Production-style/dev-server runs should launch separate `process.MRF --burst`
  worker processes as well as setting the per-process concurrency limit.
- `HLTHPRT_MAX_MRF_FINISH_JOBS=1` should be used for `process.MRF_finish`;
  the finish queue has one shutdown job and extra ARQ slots only contend on the
  same lock.
- On full-source runs, older finish code spent most of its time recomputing
  canonical keys on `mrf_address_evidence`. The current path stamps
  `mrf_address` first, then propagates matching evidence keys from the
  aggregate table via `(npi, type, checksum)` and identical address fields.
  Residual evidence rows fall back to direct stamping.
- `HLTHPRT_ADDRESS_CANON_REPAIR_EXISTING=true` is reserved for one-time
  canonicalizer repairs. Normal finish runs leave it unset/false so parent and
  evidence stamping only fills missing `address_key` values.
- To launch 8 actual worker processes locally, use the helper script:

```bash
support/run_mrf_parallel.sh --workers 8 --import-id 20260405
```

Test-schema smoke run:

```bash
support/run_mrf_parallel.sh --test --workers 8 --import-id 20260405
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
