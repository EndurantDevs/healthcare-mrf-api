# Terminology Synonyms Import

`terminology-synonyms` materializes a fast search/resolve table for provider types, procedure names, and medication names.

The import is intentionally derived from data the service already imports:

- NUCC taxonomy rows from `nucc`
- CMS/provider-service rows from `claims-pricing`
- Part D prescription rows from `drug-claims`
- reference rows from `code-sets` and `clinical-reference`
- curated non-license-restricted aliases for common provider/procedure search terms

It does not download or load official proprietary CPT/CDT synonym files. If those licensed sources are ever approved, add them to the upstream reference import first and keep their source attribution/license status explicit.

## Run

```bash
python -m process start terminology-synonyms
```

The importer builds a staged `mrf.terminology_synonym_<import_id>` table and indexes it. Publication rotates the live table to `mrf.terminology_synonym_old`, promotes the stage, and validates the live row count in one transaction. A failed publication rolls the complete rotation back, and the importer removes only its exact staging table on exit. The retained `_old` table is the immediately previous live dataset and is not normal cleanup material.

## Roll Back the Published Snapshot

First confirm that no `terminology-synonyms` import is running, and keep new imports paused through verification. Inventory the exact live and retained relation identities:

```sql
SELECT c.relname, c.oid::bigint AS relation_oid
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'mrf'
  AND c.relname IN ('terminology_synonym', 'terminology_synonym_old')
ORDER BY c.relname;

SELECT EXISTS (SELECT 1 FROM mrf.terminology_synonym_old) AS predecessor_has_rows;
```

Run the operator command once with those exact OIDs:

```bash
python main.py manage rollback-terminology-synonyms \
  --expected-live-oid <live-oid> \
  --expected-old-oid <old-oid>
```

The command locks both relations, rechecks their OIDs, rejects an empty predecessor, swaps their names in one transaction, and verifies that the OIDs reversed before commit. Missing, duplicate, or stale identities fail without a swap. After success, rerun the inventory query, confirm the OIDs are reversed, and replay the affected API checks before resuming imports. Never retry with previously captured OIDs; inventory again after any failure or later publication. The rollback command is operator-only and is never invoked automatically.

## API Use

- Provider type autocomplete: `GET /api/v1/pricing/provider-types/autocomplete?q=family`
- Provider type resolve: `GET /api/v1/pricing/provider-types/resolve?q=207Q00000X`
- Procedure resolve: `GET /api/v1/pricing/procedures/resolve?q=office%20visit`
- Medication resolve: `GET /api/v1/pricing/medications/resolve?q=atorvastatin`

Main pricing provider searches use the table when present and fall back to the old text search behavior when it is missing.
