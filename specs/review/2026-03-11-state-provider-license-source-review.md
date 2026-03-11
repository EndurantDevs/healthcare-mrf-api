# State Provider-License Data Source Review (2026-03-11)

## Purpose
Capture verified opportunities for expanding from pharmacy-only state adapters to broader provider/doctor license pipelines.

This note is intentionally internal to `specs/review/` and is not linked from public READMEs.

## Current Scope
Explicit state adapters currently implemented in `process/pharmacy_license.py`:
- TX
- FL
- CO
- WA
- NY
- MA
- NJ

Those adapters currently normalize pharmacy-license oriented rows. The sources below can support broader provider/license pipelines.

## Findings by State

### Texas (TX)
Primary sources:
- TMB all-license dataset: `https://data.texas.gov/resource/tm3v-pfq9.csv`
- TMB prescriptive delegation dataset: `https://data.texas.gov/resource/3fg6-5a53.csv`

What is available beyond pharmacies:
- Broad TMB license inventory (many provider types), with columns such as:
  - `license_type`
  - `license_number`
  - `registration_status`
  - `disciplinary_status`
  - `degree`
  - `specialties`
  - `practice_address/city/state/zip`
  - current board action date/description
- Physician delegation relationships to PA/APRN from the second dataset.

Pipeline opportunities:
- `tx_provider_licenses`
- `tx_prescriptive_delegation`

### Florida (FL)
Primary sources:
- Public search/export surface:
  - `https://mqa-internet.doh.state.fl.us/MQASearchServices/HealthCareProviders`
  - `.../HealthCareProviders/ExportToCsvLVP?jsonModel=...`
- MQA guide:
  - `https://mqa-internet.doh.state.fl.us/MQASearchServices/Content/HelpFile/MQA%20SearchServices%20and%20DataDownload%20UserGuide.pdf`

What is available beyond pharmacies:
- Practitioner/provider exports by board + profession codes, including Medical Doctor and many other professions.
- Search results can be exported to CSV from the public service.
- User guide confirms broader licensure/profile/license-status datasets and daily updates in data download workflows.

Pipeline opportunities:
- `fl_provider_licenses`
- `fl_provider_discipline` (from discipline/admin action sources)
- optional provider-profile sidecar where available

### Colorado (CO)
Primary source:
- `https://data.colorado.gov/resource/7s5z-vewr.csv`

What is available beyond pharmacies:
- Dataset includes many provider and non-provider license types.
- Useful columns include:
  - `licensetype`
  - `licensenumber`
  - `licensestatusdescription`
  - `specialty`
  - `title`, `degrees`
  - discipline columns (`casenumber`, `programaction`, `disciplineeffectivedate`, `disciplinecompletedate`)
  - verification/profile links.

Pipeline opportunities:
- `co_provider_licenses`
- `co_provider_discipline`

### Washington (WA)
Primary source:
- `https://data.wa.gov/resource/qxh8-f4bd.csv`

What is available beyond pharmacies:
- Dataset name: "Health Care Provider Credential Data".
- Includes high-volume provider credential types (physician, PA, RN, LPN, dentist, etc.) in addition to pharmacy.
- Key columns:
  - `credentialnumber`
  - `credentialtype`
  - `status`
  - `firstissuedate`, `lastissuedate`, `expirationdate`
  - `actiontaken`
  - name fields.

Limitation:
- No robust address/location fields in this dataset.

Pipeline opportunities:
- `wa_provider_credentials`
- discipline/status timeline enrichment from lifecycle fields

### Massachusetts (MA)
Primary sources:
- Export index: `https://healthprofessionlicensing-api.mass.gov/api-public/export/all`
- License export download: `https://healthprofessionlicensing-api.mass.gov/api-public/export/data/license/{licenseMetaId}`
- Board bundle export (example pharmacy): `.../export/data/board/BOARD_OF_REGISTRATION_IN_PHARMACY`

What is available beyond pharmacies:
- Large board-level catalog covering many health professions.
- Board and license exports exist for PA, RN, psychology, social work, dentistry, respiratory care, EMS, controlled substance registrations, etc.
- CSV payloads include rich status + address + issue/renewal/expiration fields.

Pipeline opportunities:
- `ma_provider_licenses`
- `ma_controlled_substance_registrations`
- board-specific subsets when needed

### New Jersey (NJ)
Primary source:
- MyLicense search:
  - Facility mode: `https://newjersey.mylicense.com/Verification_Bulk/Search.aspx?facility=Y`
  - Person mode: `https://newjersey.mylicense.com/Verification_Bulk/Search.aspx?facility=N`

What is available beyond pharmacies:
- Person mode includes professions/license types such as Medical Doctor, DO, PA, nursing, dental, pharmacy, etc.
- Search is ASP.NET form-post based (ViewState/EventValidation workflow), similar to existing NJ adapter pattern.

Pipeline opportunities:
- `nj_provider_licenses` adapter by profession/license type

Operational caveat:
- Requires robust ASP.NET stateful form workflow and throttling/retry handling.

### New York (NY)
Primary sources:
- NYSED OP verification landing page:
  - `https://www.op.nysed.gov/services/verifications/online-verification-searches`
- Existing pharmacy API in use:
  - `https://api.nysed.gov/rosa/V2/findPharmacies`

What is available beyond pharmacies:
- Public verification service states coverage for 1.5M+ licensees across 50+ professions.
- Information includes name, profession, license number, location, original license date, registration status.
- Discipline note: physician/PA discipline is referenced through NY DOH OPMC.

Pipeline opportunities:
- `ny_provider_licenses` (scrape/search workflow or approved API if available)
- `ny_provider_discipline` split by OP + OPMC sources

Operational caveat:
- A clear bulk/API endpoint for non-pharmacy records was not confirmed in this pass.

## Recommended Build Order
1. TX provider licenses + TX delegation (high-value structured bulk).
2. MA provider licenses (broad normalized exports with rich fields).
3. WA provider credentials + CO provider licenses/discipline.
4. FL provider licenses via search/export pathway.
5. NJ and NY provider pipelines (higher complexity/stateful or less explicit API surfaces).

## Proposed Canonical Extensions
If we split provider licensing out from pharmacy licensing, a separate canonical model is recommended:
- `provider_license_snapshot`
- `provider_license_record`
- `provider_license_record_history`
- `provider_license_state_coverage`

Suggested normalized fields:
- state, profession, license_type, license_number
- person/entity name
- status (normalized + raw)
- issue/renewal/expiration dates
- disciplinary flag + summary
- practice/mailing location fields (when available)
- source identifiers, source url, import metadata
- resolved NPI(s) where deterministic matching succeeds

## Validation Notes
This review used live endpoint checks on 2026-03-11 and sample payload verification for:
- FL provider search + CSV export
- MA export index + license exports
- WA dataset metadata + sample provider rows
- CO dataset metadata + sample rows
- TX Socrata dataset metadata + sample rows
- NJ person/facility option surfaces
- NY OP verification page content

