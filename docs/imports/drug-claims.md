# Drug Claims Import

## Purpose
Imports Medicare Part D provider-drug and drug spending public files to build prescription-level provider benchmarking and drug pricing aggregates.

## Source Websites
- CMS data portal: <https://data.cms.gov/>
- Medicare Part D public datasets on CMS Data

## Start Command
```bash
python main.py start drug-claims
```

## Workers
```bash
python main.py worker process.DrugClaims --burst
python main.py worker process.DrugClaims_finish --burst
```

## Manual Finish
```bash
python main.py finish drug-claims --import-id <IMPORT_ID> --run-id <RUN_ID>
```

## Test Mode
```bash
python main.py start drug-claims --test
```

## Main Outputs
- `pricing_provider_prescription`
- `pricing_prescription`
- shared `code_catalog` and `code_crosswalk`

## Notes
- This import is independent from procedures/services claims.
- It also drives HP internal medication code crosswalk enrichment to NDC and RxNorm where configured.
- Do not run `DrugClaims_finish` in parallel with `ClaimsPricing_finish`.

## Related Specs
- [`../../specs/drug_claims_v1.md`](../../specs/drug_claims_v1.md)
- [`../../specs/import_swap_backup_policy.md`](../../specs/import_swap_backup_policy.md)
