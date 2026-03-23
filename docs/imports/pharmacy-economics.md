# Pharmacy Economics Import

## Purpose
Builds state/NDC pharmacy economics references for reimbursement-pressure and gross-margin context in pharmacy/site intelligence workflows.

## Command
```bash
python main.py start pharmacy-economics [--test]
python main.py worker process.PharmacyEconomics --burst
```

Publish is handled during worker shutdown.

## Source Website
- <https://data.medicaid.gov/>

## Main Table
- `mrf.pharmacy_economics_summary`

## Data Inputs
- SDUD (State Drug Utilization Data)
- NADAC (National Average Drug Acquisition Cost)
- ACA Federal Upper Limits (FUL)
- state dispensing-fee reference mapping

## Data Notes
- SDUD volumes are based on prescription counts (not reimbursed-dollar fallback).
- Invalid/unmapped state codes are dropped (for example `XX`).
- Gross margin proxy:
  - `((min(NADAC, FUL) * qty) + dispensing_fee) - (NADAC * qty)`

## Key Environment Variables
- `HLTHPRT_PHARMACY_ECON_BATCH_SIZE`
- `HLTHPRT_PHARMACY_ECON_SDUD_URL` (optional override)
- `HLTHPRT_PHARMACY_ECON_NADAC_URL` (optional override)
- `HLTHPRT_PHARMACY_ECON_FUL_URL` (optional override)

