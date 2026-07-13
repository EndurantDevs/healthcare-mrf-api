# MS-DRG Import Operations

## Schedule

Run `ms-drg` as a reference import after CMS MS-DRG releases change, or at least weekly to detect CMS page/link updates:

```bash
python main.py start ms-drg
```

For an external scheduler, configure importer `ms-drg` with default parameters:

```json
{
  "test_mode": false,
  "include_relationships": true,
  "concurrency": 10
}
```

## Smoke

```bash
python main.py start ms-drg --test
```

Expected smoke behavior:

- Loads a small `MS_DRG` catalog sample.
- Loads only a small number of ICD-10 index pages.
- Does not write `code_crosswalk` rows.

## Operational Notes

- Override `HLTHPRT_MS_DRG_MANUAL_TOC_URL` only if CMS changes the landing-page structure.
- Keep HCPCS-MS-DRG package ingestion disabled unless licensing policy explicitly allows CPT-containing CMS package content.
- MS-DRG rows should support direct PTG lookup by reported code system, not CPT/HCPCS expansion.
