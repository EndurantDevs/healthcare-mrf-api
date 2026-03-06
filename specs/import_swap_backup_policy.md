# Import Swap & Backup Policy (`_old` Tables)

## Purpose

Define mandatory behavior for importer table swaps and rollback safety.

## Policy

- Tables with `_old` suffix are **required rollback backups**.
- `_old` tables are **not cleanup candidates** during normal operations.
- Each successful import rewrites the backup snapshot:
  - current live table is renamed to `<table>_old`
  - staged import table is renamed to live table name
  - therefore `<table>_old` always represents the immediately previous live dataset

## Operational Rules

- Keep `_old` tables and their indexes available until the next successful import.
- Do not add automated jobs that drop `_old` tables by age.
- Storage planning must account for approximately 2x footprint for swapped datasets.
- Performance investigations must treat `_old` presence as expected behavior, not data drift.

## Index Rename Safety Rules

- Index archive rename must be idempotent:
  - drop existing archive index name before renaming current live index;
  - then rename staged index to live index name.
- Archive index names must be PostgreSQL-identifier-safe (63-char max).
  - For long names, use deterministic truncated+hash archive naming to avoid collisions.
- `ALTER INDEX ... RENAME TO ..._old` without target pre-drop is not allowed in finalize paths.

## NPI Import Reference

Current NPI swap implementation follows this policy in `process/npi.py`:

- rename live to `_old`
- rename staged import table to live
- rename indexes accordingly
- archive index naming is truncation-safe and collision-tolerant

Claims/drug finalize in `process/claims_pricing.py` and `process/drug_claims.py` follows the same guardrail.

This is intentional rollback design, not temporary migration debris.

## Rollback Procedure (Manual)

If a newly published table is bad, rollback is performed by renaming live and `_old` tables/indexes back to previous names inside a controlled maintenance window.

## Guardrail

Any proposal to purge `_old` tables requires explicit approval and a replacement rollback mechanism.
