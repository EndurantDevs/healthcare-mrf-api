# Clinical Reference DevOps

## Smoke Run

```bash
python main.py start clinical-reference --test --import-id smoke
```

This publishes tiny `mrf.clinical_*` tables and keeps previous live tables as `_old`.

## Full Run

```bash
export HLTHPRT_UMLS_API_KEY=...
python main.py start clinical-reference --import-id 20260525
```

The importer downloads or reuses retained terminology artifacts, creates stage tables, bulk loads rows, builds indexes, validates minimum row counts, then swaps stage tables into live names in one publish step.

Default artifact root:

```bash
/Volumes/Data/data/artifacts/terminology
```

Useful source controls:

```bash
python main.py start clinical-reference --sources icd10cm,mesh,rxnorm,snomed,medrt
python main.py start clinical-reference --force-download
HLTHPRT_MEDRT_RXCUI_LIMIT=5000 python main.py start clinical-reference --sources medrt
```

Leave `HLTHPRT_MEDRT_RXCUI_LIMIT` unset for full non-test MED-RT coverage. Test mode still defaults to a tiny RxCUI sample.

## Area Validation

After a full run, verify that the MeSH-derived area tables are populated:

```sql
SELECT COUNT(*) FROM mrf.clinical_area;
SELECT COUNT(*) FROM mrf.clinical_area_condition;
SELECT COUNT(*) FROM mrf.clinical_area_treatment;
```

Spot-check broad areas through the API:

```bash
curl 'http://127.0.0.1:8085/api/v1/clinical/clinical-areas?q=cardiovascular'
curl 'http://127.0.0.1:8085/api/v1/clinical/clinical-areas/mesh%3AC14/conditions?system=MESH'
curl 'http://127.0.0.1:8085/api/v1/clinical/clinical-areas/mesh%3AC14/treatments?system=RXNORM'
```

## Required Attribution

Any product surface using NLM-derived data must include:

> This product uses publicly available data from the U.S. National Library of Medicine (NLM), National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the product and does not endorse or recommend this or any other product.
