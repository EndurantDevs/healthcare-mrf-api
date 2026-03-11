# NUCC Import

## Purpose
Imports the NUCC provider taxonomy code set used for provider specialty normalization, filtering, and taxonomy metadata.

## Source Website
- NUCC provider taxonomy: <https://www.nucc.org/index.php/21-provider-taxonomy>

## Start Command
```bash
python main.py start nucc
```

## Test Mode
```bash
python main.py start nucc --test
```

## Main Outputs
- `nucc_taxonomy`

## Notes
- This import is small compared with claims or NPPES imports, but it is foundational for classification and specialty filtering.
- It is typically run before or alongside NPI-driven workflows.
