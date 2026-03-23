# CMS Sources For Treatment Cost Planning

Last updated: 2026-02-17

This document keeps the canonical CMS source list used by `healthcare-mrf-api` for treatment-cost
planning features (provider procedures, drug claims, pricing context, and supporting code systems).

For the full cross-import source registry (beyond treatment-cost scope), see:
- `specs/data_source_registry.md`
- `docs/data-sources.md`

## Primary Claims Sources (core for cost/rating logic)

- Medicare Physician & Other Practitioners - by Provider and Service  
  https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service
- Medicare Physician & Other Practitioners - by Geography and Service  
  https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-geography-and-service
- Medicare Outpatient Hospitals - by Provider and Service  
  https://data.cms.gov/provider-summary-by-type-of-service/medicare-outpatient-hospitals/medicare-outpatient-hospitals-by-provider-and-service
- Medicare Part D Prescribers - by Provider and Drug  
  https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug
- Medicare Part D Prescribers - by Geography and Drug  
  https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-geography-and-drug

## Drug Spending Context

- Medicare Part D Spending by Drug  
  https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-spending-by-drug/medicare-part-d-spending-by-drug
- Medicare Part B Spending by Drug  
  https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-spending-by-drug/medicare-part-b-spending-by-drug

## Coding/Normalization Sources

- CMS ICD-10 landing page  
  https://www.cms.gov/medicare/coding-billing/icd-10-codes
- CMS HCPCS quarterly updates  
  https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update
- CMS Physician Fee Schedule (PFS) relative value files  
  https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files

## Marketplace/Commercial Context (non-Medicare)

- Plan Attributes PUF (2026)  
  https://download.cms.gov/marketplace-puf/2026/plan-attributes-puf.zip
- Rate PUF (2026)  
  https://download.cms.gov/marketplace-puf/2026/rate-puf.zip
- Benefits and Cost Sharing PUF (2026)  
  https://download.cms.gov/marketplace-puf/2026/benefits-and-cost-sharing-puf.zip
- Hospital Price Transparency initiative  
  https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency
- Medical Bill Rights / Good Faith Estimate  
  https://www.cms.gov/medical-bill-rights/help/guides/good-faith-estimate

## National/Local Coverage Policy Downloads (NCD/LCD)

- MCD Downloads (National Coverage Determinations and Local Coverage Determinations)  
  https://www.cms.gov/medicare-coverage-database/downloads/downloadable-databases.aspx

Notes:
- Use these for coverage policy logic (medical necessity, code coverage constraints, local policy differences).
- Do not use these as a replacement for claims-based cost benchmarks; they are policy/coverage artifacts, not paid-amount distributions.

## Transparency in Coverage Implementation Reference (PDT)

- CMS + NAIC Transparency in Coverage webinar (June 27, 2022, PDF)  
  https://www.cms.gov/files/document/transparency-coverage-webinar-naic-06-27-22-508.pdf

Notes:
- Keep this as a baseline policy/implementation reference for PDT implementation and future PDT improvements.
- Use alongside machine-readable claims/pricing datasets; this webinar is guidance context, not a paid-claims benchmark feed.

## MDP/LDS Clarification

- Medicaid Drug Programs (MDP/MDRP) pages are program/system references and rebate operations, not direct public provider cost benchmark feeds:  
  https://www.medicaid.gov/medicaid/prescription-drugs/medicaid-drug-rebate-program/index.html
- CMS Limited Data Set (LDS) files are access-controlled and require DUA workflow; treat as optional future ingestion, not default public-source ingestion:  
  https://www.cms.gov/LimitedDataSets/

## Freshness Policy

- For claims-based provider and drug benchmark features, use the freshest complete CMS service year
  available in the selected datasets.
- Current standard in this project: **2023** (freshest complete year currently used).
- API docs should explicitly state the active claims year when returning claims-derived costs/ratings.

## Currently Active Import Scope (Implemented)

The current importer code actively ingests:

1. Medicare Physician & Other Practitioners - by Provider
2. Medicare Physician & Other Practitioners - by Provider and Service
3. Medicare Physician & Other Practitioners - by Geography and Service
4. Medicare Part D Prescribers - by Provider and Drug
5. Medicare Part D Spending by Drug

Other sources listed in this document are roadmap/reference inputs, not yet active import feeds.

## Data Catalog Root

- CMS public catalog root: https://data.cms.gov/
