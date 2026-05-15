# RC/POS Code Sets Import

## Purpose
Loads official readable code labels for Transparency in Coverage billing and service-code display:

- Revenue Center codes into `mrf.code_catalog` as `code_system = 'RC'`
- CMS Place of Service codes into `mrf.code_catalog` as `code_system = 'POS'`

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
  - `source` is set to `cms_bluebutton_revenue_center_code` or `cms_place_of_service_code_set`.

No new table or migration is required.

## API Usage After Import
```bash
curl "$API_BASE/api/v1/codes/RC/450"
curl "$API_BASE/api/v1/codes/POS/23"
curl "$API_BASE/api/v1/codes/PLACE_OF_SERVICE/23"
```

PTG code detail enrichment is opt-in:
```bash
curl "$API_BASE/api/v1/pricing/providers/by-procedure?plan_id=010854205&plan_market_type=group&source_key=heartland_dental_rust_publish_check&code_system=RC&code=450&limit=1&include_code_details=true"
```

When enabled, PTG responses may include:
- `billing_code_detail` for the item billing/reported code.
- `service_code_details` for POS values inside each price `service_code` array.

## Production Verification
```bash
psql "$DATABASE_URL" -P pager=off -c "
SELECT code_system, code, display_name, source
FROM mrf.code_catalog
WHERE (code_system = 'RC' AND code = '0450')
   OR (code_system = 'POS' AND code = '23')
ORDER BY code_system, code;"
```

Expected key rows:
- `RC 0450` = `EMERGENCY ROOM - GENERAL CLASSIFICATION`
- `POS 23` = `Emergency Room - Hospital`

If the API is already running, restart/reload it after deploying code changes so `/codes/RC/450` alias normalization and PTG `include_code_details=true` are available.
