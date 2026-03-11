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
- `plan_transparency`
- `plan_drug_raw`
- `plan_npi_raw`
- `plan_network_tier_raw`
- import history and import logs

## Notes
- This is one of the original marketplace ingestion pipelines in the repo.
- It works from configured CMS marketplace PUF families rather than one fixed file.
- It is broader than the dedicated `plan-attributes` import and is responsible for several marketplace-facing objects.
