# RC/POS Code Sets Import

## Purpose
Loads official readable code labels for Transparency in Coverage billing and service-code display:

- Revenue Center codes into `mrf.code_catalog` as `code_system = 'RC'`
- CMS Place of Service codes into `mrf.code_catalog` as `code_system = 'POS'`
- Common billing-code modifiers into `mrf.code_catalog` as `code_system = 'MODIFIER'`

The import is standalone reference data. PTG imports do not fetch these sources automatically; run this import before relying on PTG `include_code_details=true`.

## Source Websites
- CMS Place of Service Code Set: <https://www.cms.gov/medicare/coding-billing/place-of-service-codes/code-sets>
- CMS Blue Button Claim Revenue Center CodeSystem: <https://bluebutton.cms.gov/fhir/CodeSystem/CLM-REV-CNTR-CD/>

## Start Command
```bash
python main.py start code-sets
```

Smoke run:
```bash
python main.py start code-sets --test
```

This is a direct import, not an ARQ queue. There is no separate worker or finish step.

## Environment Overrides
```bash
HLTHPRT_CODE_SETS_POS_URL=<CMS_POS_URL>
HLTHPRT_CODE_SETS_RC_URL=<CMS_BLUE_BUTTON_RC_URL>
```

Use overrides only for controlled source testing or if CMS changes the canonical landing URL.

## Main Outputs
- `mrf.code_catalog`
  - `RC` rows are canonicalized to four digits, for example `0450`.
  - `POS` rows are canonicalized to two digits, for example `23`.
  - `MODIFIER` includes common observed TiC modifiers: `26`, `95`, `GQ`, `NU`, `QW`, `RR`, `TC`, and `UE`.
  - `source` is set to `cms_bluebutton_revenue_center_code`, `cms_place_of_service_code_set`, or `cms_hcpcs_modifier_reference`.

No new table or migration is required.

## API Usage After Import
```bash
curl "$API_BASE/api/v1/codes/RC/450"
curl "$API_BASE/api/v1/codes/POS/23"
curl "$API_BASE/api/v1/codes/PLACE_OF_SERVICE/23"
curl "$API_BASE/api/v1/codes/MODIFIER/TC"
curl "$API_BASE/api/v1/codes/CPT_MODIFIER/26"
curl "$API_BASE/api/v1/codes/HCPCS_MODIFIER/RR"
```

PTG code detail enrichment is opt-in:
```bash
curl "$API_BASE/api/v1/pricing/providers/by-procedure?plan_id=TESTPLAN001&plan_market_type=group&source_key=example_dental_rust_publish_check&code_system=RC&code=450&limit=1&include_code_details=true"
```

When enabled, PTG responses may include:
- `billing_code_detail` for the item billing/reported code.
- `service_code_details` for POS values inside each price `service_code` array.
- `billing_code_modifier_details` for modifier values inside each price `billing_code_modifier` array. Unknown modifiers still return a normalized detail with `catalog_status = "missing"` so the API shape stays stable.

## Production Verification
```bash
psql "$DATABASE_URL" -P pager=off -c "
SELECT code_system, code, display_name, source
FROM mrf.code_catalog
WHERE (code_system = 'RC' AND code = '0450')
   OR (code_system = 'POS' AND code = '23')
   OR (code_system = 'MODIFIER' AND code IN ('26', '95', 'GQ', 'NU', 'QW', 'RR', 'TC', 'UE'))
ORDER BY code_system, code;"
```

Expected key rows:
- `RC 0450` = `EMERGENCY ROOM - GENERAL CLASSIFICATION`
- `POS 23` = `Emergency Room - Hospital`
- `MODIFIER 26` = `Professional component`
- `MODIFIER NU` = `New equipment`
- `MODIFIER RR` = `Rental`
- `MODIFIER TC` = `Technical component`

If the API is already running, restart/reload it after deploying code changes so `/codes/RC/450` alias normalization and PTG `include_code_details=true` are available.
