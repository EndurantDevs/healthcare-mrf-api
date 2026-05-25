# Clinical Reference Import

`clinical-reference` builds local terminology lookup, synonym, crosswalk, and relationship tables for Ribbon-replacement APIs.

## Command

```bash
python main.py start clinical-reference --test
python main.py start clinical-reference
```

Optional:

```bash
python main.py start clinical-reference --import-id 20260525
python main.py start clinical-reference --sources icd10cm,mesh,rxnorm,snomed,medrt
```

## Sources

The importer uses official coding systems as canonical keys and retains downloaded artifacts under `/Volumes/Data/data/artifacts/terminology` by default:

- `ICD10CM`: CDC ICD-10-CM code descriptions for conditions.
- `MESH`: NLM MeSH descriptor and supplemental XML.
- `RXNORM`: NLM RxNorm monthly release for drug concepts and aliases.
- `SNOMEDCT_US`: NLM/UMLS SNOMED CT US Edition plus SNOMED-to-ICD-10-CM map.
- `MEDRT`: RxClass MED-RT relationships for RxNorm-to-condition/treatment links.

SNOMED and RxNorm release downloads require `HLTHPRT_UMLS_API_KEY` or `UMLS_API_KEY`. NLM-derived rows carry the required NLM attribution string in `source_attribution`.

By default, non-test MED-RT loading checks every distinct RxCUI present in `rx_data.product`. Use `HLTHPRT_MEDRT_RXCUI_LIMIT=<n>` only for controlled partial runs.

Clinical areas are derived from official MeSH tree roots, not from a proprietary specialty taxonomy:

- `C##` disease roots and `F03` mental-disorder roots become condition areas.
- `E##` procedure/service roots become treatment areas.
- Direct MeSH descendants are mapped from their tree numbers.
- RxNorm drugs are mapped into disease areas through RxClass/MED-RT `may_treat` relationships to MeSH conditions.

CPT/HCPCS procedure-to-clinical-area mappings are intentionally not imported by default because a complete official AMA/CMS mapping is not available in the public source set.

## Tables

The importer stages new tables, builds indexes, validates row counts, and swaps only after validation succeeds:

- `mrf.clinical_code_catalog`
- `mrf.clinical_code_crosswalk`
- `mrf.clinical_code_synonym`
- `mrf.clinical_code_relationship`
- `mrf.clinical_area`
- `mrf.clinical_area_condition`
- `mrf.clinical_area_treatment`

## API Coverage

The live tables power:

- `GET /api/v1/clinical/conditions`
- `GET /api/v1/clinical/conditions/{system}/{code}`
- `GET /api/v1/clinical/treatments`
- `GET /api/v1/clinical/treatments/{system}/{code}`
- `GET /api/v1/clinical/concepts`
- `GET /api/v1/clinical/concepts/{system}/{code}`
- `GET /api/v1/clinical/relationships`
- `GET /api/v1/clinical/clinical-areas`
- `GET /api/v1/clinical/clinical-areas/{clinical_area_id}`
- `GET /api/v1/clinical/clinical-areas/{clinical_area_id}/conditions`
- `GET /api/v1/clinical/clinical-areas/{clinical_area_id}/treatments`
- `GET /api/v1/clinical/crosswalk`

## Attribution

If NLM data is loaded, API responses and downstream products must preserve:

> This product uses publicly available data from the U.S. National Library of Medicine (NLM), National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the product and does not endorse or recommend this or any other product.
