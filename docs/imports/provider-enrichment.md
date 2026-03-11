# Provider Enrichment Import

## Purpose
Imports CMS provider enrollment data and builds a Medicare-enrollment sidecar that enriches NPI records with PECOS-related context.

## Source Websites
- Medicare Fee-for-Service Public Provider Enrollment: <https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-fee-for-service-public-provider-enrollment>
- CMS data portal: <https://data.cms.gov/>
- NPPES NPI files: <https://download.cms.gov/nppes/NPI_Files.html>

## Start Command
```bash
python main.py start provider-enrichment
```

## Workers
```bash
python main.py worker process.ProviderEnrichment --burst
python main.py worker process.ProviderEnrichment_finish --burst
```

## Test Mode
```bash
python main.py start provider-enrichment --test
```

## Main Outputs
- `provider_enrollment_ffs`
- `provider_enrollment_ffs_additional_npi`
- `provider_enrollment_ffs_address`
- `provider_enrollment_ffs_secondary_specialty`
- `provider_enrollment_ffs_reassignment`
- `provider_enrichment_summary`
- additional enrollment tables for hospital, HHA, hospice, FQHC, RHC, and SNF where available

## Notes
- NPPES remains the canonical provider directory.
- This import adds a Medicare-enrollment sidecar; it does not rewrite `npi_address` or `npi_taxonomy`.
- Latest-quarter FFS enrollment imports are the primary current-state focus.
- The shutdown/finalize phase materializes `provider_enrichment_summary` and publishes the staged tables.
