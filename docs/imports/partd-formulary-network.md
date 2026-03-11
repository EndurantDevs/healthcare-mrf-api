# Part D Formulary and Pharmacy Network Import

## Purpose
Imports monthly and quarterly Medicare Part D formulary, pharmacy network, and pricing public files to support pharmacy activity, medication cost, and plan-network queries.

## Source Websites
- CMS data portal: <https://data.cms.gov/>
- Medicare.gov Plan Finder: <https://www.medicare.gov/>

## Start Command
```bash
python main.py start partd-formulary-network
```

## Workers
```bash
python main.py worker process.PartDFormularyNetwork --burst
python main.py worker process.PartDFormularyNetwork_finish --burst
```

## Manual Finish
```bash
python main.py finish partd-formulary-network --import-id <IMPORT_ID> --run-id <RUN_ID>
```

## Test Mode
```bash
python main.py start partd-formulary-network --test
```

## Main Outputs
- `partd_import_run`
- `partd_formulary_snapshot`
- `partd_pharmacy_activity`
- `partd_medication_cost`
- stage tables used during publish

## Notes
- The importer prefers quarterly context where needed but also ingests monthly releases.
- It is the main source behind Part D pharmacy activity and medication cost endpoints in this repo.
