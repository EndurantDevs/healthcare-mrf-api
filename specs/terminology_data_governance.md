# Terminology Data Governance

This repo imports reference terminology into `CodeCatalog`, `CodeSynonym`,
`CodeCrosswalk`, and `CodeRelationship`. These tables can feed public API
responses, so source rights and attribution must be handled before adding or
enabling new sources.

## Source Classes

- Public/open operational sources: CDC ICD-10-CM descriptions, CMS place of
  service codes, CMS revenue center references, and local HCPCS modifier
  references.
- Attribution-required NLM sources: MeSH and RxClass/MED-RT derived mappings.
  Preserve `source_attribution` on every published row.
- Restricted-license sources: SNOMED CT US and SNOMED-to-ICD-10-CM mapping
  resources delivered through UMLS. Do not enable these for anonymous/public
  redistribution until license terms and downstream serving behavior have been
  reviewed.

## Import Guardrails

- The clinical-reference importer defaults to
  `icd10cm,mesh,rxnorm,medrt`; SNOMED is intentionally excluded.
- To import SNOMED-derived data, operators must set both:
  - `HLTHPRT_CLINICAL_REFERENCE_SOURCES` including `snomed`
  - `HLTHPRT_ENABLE_RESTRICTED_TERMINOLOGIES=1`
- Public code/clinical endpoints hide `SNOMEDCT_US` rows by default even if
  they already exist in the database. Public exposure requires the separate
  `HLTHPRT_PUBLIC_RESTRICTED_TERMINOLOGIES=1` flag after legal/compliance
  review.
- UMLS download automation follows the official NLM UTS download API, which
  requires `url` and `apiKey` parameters on the download request. Because that
  protocol exposes the API key in the request URL at the upstream service, local
  code must never log the full UTS download URL. The importer redacts `apiKey`
  in raised errors and artifact manifests store only the original release URL,
  byte count, timestamp, and SHA-256.

Reference: https://documentation.uts.nlm.nih.gov/automating-downloads.html

## Release Checklist

Before exposing terminology-derived rows through public endpoints:

1. Confirm the source license permits the exact redistribution shape.
2. Confirm attribution text survives through the public API payload.
3. Confirm restricted source systems are either excluded from public endpoints
   or served only to entitled clients.
4. Keep the release file manifest and SHA-256 with the import artifacts.
5. Record the selected sources and restricted-data opt-in setting in the import
   run parameters.
