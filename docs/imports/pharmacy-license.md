# Pharmacy License Import

## Purpose
Discovers state pharmacy board source files and normalizes pharmacy license status into a queryable canonical table.

## Source Websites
- FDA BeSafeRx online pharmacy resources: <https://www.fda.gov/drugs/besaferx-your-source-online-pharmacy-information/locate-state-licensed-online-pharmacy>
- State pharmacy board websites discovered from the FDA directory

## Start Command
```bash
python main.py start pharmacy-license
```

## Workers
```bash
python main.py worker process.PharmacyLicense --burst
python main.py worker process.PharmacyLicense_finish --burst
```

## Manual Finish
```bash
python main.py finish pharmacy-license --import-id <IMPORT_ID> --run-id <RUN_ID>
```

## Test Mode
```bash
python main.py start pharmacy-license --test
```

## Main Outputs
- `pharmacy_license_import_run`
- `pharmacy_license_snapshot`
- `pharmacy_license_record`
- `pharmacy_license_record_history`
- `pharmacy_license_state_coverage`

## Notes
- This pipeline is state-source driven and less uniform than CMS imports.
- The importer normalizes varying board formats into one canonical structure.
- NPI matching is a core part of the normalization logic.
