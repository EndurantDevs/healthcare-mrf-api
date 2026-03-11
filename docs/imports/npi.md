# NPI Import

## Purpose
Imports the National Plan and Provider Enumeration System (NPPES) provider directory and related address, taxonomy, and other-identifier structures.

## Source Website
- NPPES NPI files: <https://download.cms.gov/nppes/NPI_Files.html>

## Start Command
```bash
python main.py start npi
```

## Workers
```bash
python main.py worker process.NPI --burst
python main.py worker process.NPI_finish --burst
```

## Test Mode
```bash
python main.py start npi --test
```

## Main Outputs
- `npi`
- `npi_address`
- `npi_data_taxonomy`
- `npi_data_taxonomy_group`
- `npi_data_other_identifier`
- `address_archive`

## Notes
- This is the canonical provider directory import for names, addresses, phones, and NUCC-linked taxonomy declarations.
- Other imports such as provider enrichment add sidecar data, but do not replace NPPES as the core directory source.
- The shutdown phase also refreshes `npi_address.procedures_array` and `npi_address.medications_array` from canonical pricing tables.
