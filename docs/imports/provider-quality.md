# Provider Quality Import

## Purpose
Builds provider quality and cost scoring inputs from public CMS and CDC sources, then materializes provider-level measures, domain scores, and benchmarked rating outputs.

## Source Websites
- CMS Quality Payment Program: <https://www.cms.gov/Medicare/Quality-Payment-Program/Quality-Payment-Program.html>
- CMS data portal: <https://data.cms.gov/>
- CDC / ATSDR Social Vulnerability Index: <https://www.atsdr.cdc.gov/place-health/php/svi/index.html>

## Start Command
```bash
python main.py start provider-quality
```

## Workers
```bash
python main.py worker process.ProviderQuality --burst
python main.py worker process.ProviderQuality_finish --burst
```

## Manual Finish
```bash
python main.py finish provider-quality --import-id <IMPORT_ID> --run-id <RUN_ID>
```

## Test Mode
```bash
python main.py start provider-quality --test
```

## Main Outputs
- `pricing_qpp_provider`
- `pricing_svi_zcta`
- `pricing_provider_quality_feature`
- `pricing_provider_quality_measure`
- `pricing_provider_quality_domain`
- `pricing_provider_quality_score`
- supporting run and cohort tables

## Notes
- This pipeline depends on claims-derived provider tables already existing.
- Benchmark modes can materialize national, state, and ZIP-aware variants.
- Sharded finalize is supported for large runs.
- Run `ProviderQuality_finish` in its own finalize window.
