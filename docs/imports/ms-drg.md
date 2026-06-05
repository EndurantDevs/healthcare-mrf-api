# MS-DRG Reference Import

## Purpose

Loads CMS Medicare Severity Diagnosis Related Group reference data for TiC/PTG rates that report `billing_code_type` as `MS-DRG`, `MSDRG`, or `DRG`.

The import treats MS-DRG as its own code system:

- `MS_DRG` rows in `mrf.code_catalog`
- `MS_DRG` aliases in `mrf.code_synonym`
- optional MS-DRG to ICD-10-CM/PCS grouping relationships in `mrf.code_relationship`
- `ICD10PCS` rows observed in the CMS MS-DRG procedure index

It does not write MS-DRG to CPT/HCPCS rows into `mrf.code_crosswalk`.

## Source Websites

- CMS MS-DRG Classifications and Software: <https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/ms-drg-classifications-and-software>
- Current CMS ICD-10-CM/PCS MS-DRG definitions manual linked from that page.

The importer resolves the current non-draft definitions manual table of contents from the CMS landing page. Override only for controlled testing or emergency source changes:

```bash
HLTHPRT_MS_DRG_CMS_PAGE_URL=<CMS_MS_DRG_PAGE_URL>
HLTHPRT_MS_DRG_MANUAL_TOC_URL=<CMS_MANUAL_TOC_URL>
HLTHPRT_MS_DRG_CONCURRENCY=10
```

## Start Command

```bash
python main.py start ms-drg
```

Smoke run:

```bash
python main.py start ms-drg --test
```

Controlled partial relationship run:

```bash
python main.py start ms-drg --relationship-page-limit 10 --concurrency 10
```

Catalog-only run:

```bash
python main.py start ms-drg --skip-relationships
```

Catalog-only runs refresh only `MS_DRG` `code_catalog` and `code_synonym` rows. They leave existing CMS MS-DRG relationship rows and ICD-10-PCS rows unchanged.

This is a direct reference import. There is no separate finish step.

## Main Outputs

- `mrf.code_catalog`
  - `MS_DRG` rows from CMS Appendix A.
  - `ICD10PCS` rows observed in the CMS procedure-code/MS-DRG index.
- `mrf.code_synonym`
  - aliases such as `MS-DRG 470` and `DRG 470`.
- `mrf.code_relationship`
  - `MS_DRG -> ICD10CM` with `relationship = uses_icd10cm`.
  - `ICD10CM -> MS_DRG` with `relationship = groups_to_ms_drg`.
  - `MS_DRG -> ICD10PCS` with `relationship = uses_icd10pcs`.
  - `ICD10PCS -> MS_DRG` with `relationship = groups_to_ms_drg`.

MS-DRG relationships are grouping evidence, not procedure-code equivalence.

## API Usage After Import

```bash
curl "$API_BASE/api/v1/codes/MS_DRG/470"
curl "$API_BASE/api/v1/codes/DRG/470"
curl "$API_BASE/api/v1/clinical/relationships?from_system=MS_DRG&from_code=470&relationship=uses_icd10pcs"
curl "$API_BASE/api/v1/pricing/providers/by-procedure?plan_id=<plan>&code_system=MS_DRG&code=470&include_code_details=true"
```

`code_system=DRG`, `MSDRG`, and `MS-DRG` are normalized to `MS_DRG`.

## Crosswalk Policy

Do not auto-crosswalk MS-DRG to CPT/HCPCS. CMS MS-DRGs are inpatient case/payment groups assigned from ICD-10-CM diagnoses, ICD-10-PCS procedures, and sometimes patient/discharge attributes. A CPT/HCPCS code is not equivalent to an MS-DRG.

CMS also publishes an informational HCPCS-MS-DRG package for a limited surgical set under the 21st Century Cures Act. That package includes CPT content and AMA notices, so this importer does not ingest it by default. If a deployment has explicit licensing approval, add a separate curated relationship importer and keep those rows in `code_relationship`, not `code_crosswalk`.

## Production Verification

```bash
psql "$DATABASE_URL" -P pager=off -c "
SELECT code_system, code, display_name, source_release
FROM mrf.code_catalog
WHERE code_system = 'MS_DRG' AND code IN ('001', '470')
ORDER BY code;"
```

Expected key rows include:

- `MS_DRG 001` = Heart transplant or implant of heart assist system with MCC.
- `MS_DRG 470` = Major hip and knee joint replacement or reattachment of lower extremity without MCC.
