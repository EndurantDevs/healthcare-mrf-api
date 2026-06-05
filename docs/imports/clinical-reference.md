# Clinical Reference Import

`clinical-reference` builds local terminology lookup, synonym, crosswalk, and relationship tables for Ribbon-replacement APIs. `mrf.code_synonym` is the only synonym storage; `mrf.code_catalog` stores one row per canonical code without duplicate synonym arrays or checksum columns.

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
- `ICD10PCS` is not loaded by this importer. CMS MS-DRG-specific ICD-10-PCS rows are loaded by the separate `ms-drg` importer.
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

CPT/HCPCS/CDT procedure-to-clinical-area mappings are intentionally not imported by default because a complete official licensed mapping is not available in the public source set.

The platform may store CPT, CDT, and HCPCS procedure codes and source-provided labels observed in claims, PTG, or MRF files. Those rows are marked as source-observed text and are not an AMA CPT, ADA CDT, or UMLS CPT/CDT reference import. Do not load official CPT/CDT descriptors, synonyms, or licensed reference dictionaries into `code_catalog` unless the deployment has explicit AMA/ADA licensing and the importer is updated to record that licensed source.

MS-DRG to ICD-10-CM/PCS relationships are maintained by `ms-drg` as grouping relationships in `code_relationship`; they are not CPT/HCPCS equivalence crosswalks.

## Tables

The importer stages terminology rows, source-scopes its unified-table merge, builds indexes, validates row counts, and replaces clinical-area live tables only after validation succeeds. It does not keep `_old` rollback tables.

- `mrf.code_catalog`
- `mrf.code_synonym`
- `mrf.code_crosswalk`
- `mrf.code_relationship`
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
