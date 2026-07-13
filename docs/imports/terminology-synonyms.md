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

The importer builds a staged `mrf.terminology_synonym_<import_id>` table, indexes it, and atomically promotes it to `mrf.terminology_synonym`.

## API Use

- Provider type autocomplete: `GET /api/v1/pricing/provider-types/autocomplete?q=family`
- Provider type resolve: `GET /api/v1/pricing/provider-types/resolve?q=207Q00000X`
- Procedure resolve: `GET /api/v1/pricing/procedures/resolve?q=office%20visit`
- Medication resolve: `GET /api/v1/pricing/medications/resolve?q=atorvastatin`

Main pricing provider searches use the table when present and fall back to the old text search behavior when it is missing.
