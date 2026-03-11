# Claims Pricing Import

## Purpose
Imports Medicare physician and other practitioner public use files to build provider, procedure, peer-stat, and geography-aware pricing data.

## Source Websites
- CMS data portal: <https://data.cms.gov/>
- Medicare Physician & Other Practitioners program pages on CMS Data

## Start Commands
```bash
python main.py start claims-pricing
```

Alias:

```bash
python main.py start claims-procedures
```

## Workers
```bash
python main.py worker process.ClaimsPricing --burst
python main.py worker process.ClaimsPricing_finish --burst
```

## Manual Finish
```bash
python main.py finish claims-pricing --import-id <IMPORT_ID> --run-id <RUN_ID>
```

## Test Mode
```bash
python main.py start claims-pricing --test
```

## Main Outputs
- `pricing_provider`
- `pricing_procedure`
- `pricing_provider_procedure`
- `pricing_provider_procedure_location`
- `pricing_provider_procedure_cost_profile`
- `pricing_procedure_peer_stats`
- `pricing_procedure_geo_benchmark`
- shared `code_catalog` and `code_crosswalk`

## Notes
- This is the main Medicare procedures/services pricing import.
- It resolves current CMS distributions from the CMS data catalog rather than depending on one fixed CSV.
- Publish uses swap/rename semantics with `_old` rollback backups.
- Do not run `ClaimsPricing_finish` in parallel with `DrugClaims_finish`.

## Related Specs
- [`../../specs/cms_claims_pricing_v1.md`](../../specs/cms_claims_pricing_v1.md)
- [`../../specs/import_swap_backup_policy.md`](../../specs/import_swap_backup_policy.md)
