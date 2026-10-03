# CMS Doctors Import

## Purpose
Imports CMS Doctors & Clinicians National Downloadable data, retaining practice
addresses, source-grain group/site observations and reported medical-school/graduation-year assertions.

## Command
```bash
python main.py start cms-doctors [--test]
python main.py start cms-doctors --retained-source-manifest source-manifest.json
python main.py worker process.CMSDoctors --burst
```

Full publication is handled during worker shutdown. A bounded `--test` run
discards its staging tables and never publishes the replacement family.

## Source Website
- <https://data.cms.gov/provider-data/>

## Tables
- `mrf.doctor_clinician_address`
- `mrf.cms_doctor_education`
- `mrf.cms_doctor_group_site`

## Data Notes
- Preserves multiple practice locations per NPI.
- Dedupe key is `npi + address_checksum` (not NPI-only collapse).
- Supports CSV and ZIP source distributions.
- Retains the complete raw CSV/ZIP bytes by content hash, including the `Cred`
  column. Group/site observations retain every physical row before address
  deduplication: NPI, `Ind_enrl_ID`, `Org_PAC_ID`, the full `adrs_id`, facility name
  and reported member count. Distinct groups sharing an address and one group
  at multiple sites remain separate. Missing PAC IDs remain unresolved;
  observation time is not an employment or membership effective date.
- Education is read from `Med_sch` and `Grd_yr`, independently of missing
  practice addresses. Distinct NPI/school/year assertions are retained even when
  they disagree; repeated practice rows do not duplicate education facts.
- `OTHER` means no named school was provided. School-only and year-only records
  remain useful. Future years are retained with a `graduation_year_in_future`
  flag relative to acquisition time; they establish neither completed education
  nor student status. No years-of-practice value is inferred.
- Each education assertion retains the resolved source URL, file SHA-256,
  dataset ID, generation identity, acquisition timestamp, source row number and
  raw fields.
- Missing/duplicate headers, malformed CSV, invalid NPIs or malformed years
  abort acquisition. A full education publication requires at least 10,000
  assertions, one matching generation, and at least 80% of the incumbent
  assertion count. All three table swaps occur in one transaction.
- Failed acquisition or publication removes the unpublished education stage
  created by that worker. Existing stages belonging to another run are retained.
- Live tables retain one `_old` rollback generation. Florida and FHIR profile
  publications are not modified. The education table is created by its importer.
- `GET /api/v1/npi/id/{npi}/profile` composes these assertions in the `education`
  category alongside Florida and Provider Directory facts. CMS facts use the
  `education_history` type with `institution` and `graduation_year` values and
  `cms_reported` assertion metadata; neither value implies verified completion.
  Source quality flags are retained, and future years are labeled as reported
  future years. No clinical experience is calculated from graduation.
- Profile composition groups education assertions with the same institution
  after Unicode, case and whitespace normalization. Partial school/year matches
  require a known matching year, compatible reported details and one reciprocal
  candidate after exact duplicates are grouped. Punctuation, aliases, conflicting
  dates or programs, ambiguous events, and differing visibility remain separate.
  The richer original value remains the display value, and every source retains
  its original value, display, record IDs and quality flags in `assertions`.
  `corroborated_fields` reports only shared institution/year claims; agreement
  does not verify a degree, exact graduation day or completed education.
  Composer v7 introduces new education item IDs and fences this normalization
  change with a new profile generation. Within v7, an unambiguous institution/year
  identity stays stable when richer corroborating source details arrive.
- CMS evidence is under `provider_profile_evidence.sources.cms_doctors` when
  `include_evidence=true`, filtered to the facts on the returned page. Profile
  generation IDs include the CMS source generation, so stale category-page
  requests receive the existing generation-conflict response. Before the first
  CMS import, profiles continue to serve available Florida and FHIR data.
- CMS covers Medicare-listed clinicians of multiple professions. It is not an
  exhaustive physician roster or a source of residency/employment history.
- Nonblank `Cred` labels use the existing `credential` fact type in the
  `certifications` category, including clinicians with no medical-school/year
  assertion. Equal reported labels are grouped while preserving every physical
  source-row assertion and page-filtered evidence. They do not establish license
  validity or board certification. Issuer and period evidence from other
  qualification sources remains intact; missing CMS issuer/period values are not
  invented. Composer v12 fences this projection change with a new composed
  profile generation while retaining the original CMS source generation.

## Native generation and archive boundary

The `cms-doctors` replacement family contains `doctor_clinician_address`,
`cms_doctor_education` and `cms_doctor_group_site`, in that generation-authority order. A successful full
publication advances `reference_family_result_generation` after all three table swaps
in the same transaction, recording all three live relation OIDs. A failed swap or
authority write rolls back the entire publication. Bounded test runs do not
advance this authority. Migration installs a generation-less row; it does not
adopt pre-existing data as a historical publication.

The existing `reference-replacement-family.postgres.v1` archive helpers accept
this closed family. Source generation metadata must be captured with
`capture_reference_family_serving_generation` through the archive preparation
metadata callback, under the same source snapshot and family locks. Its portable
lineage/counter is distinct from the education rows' source-content generation.
Restore precreates all three tables and their indexes before loading native data.
Historical two-table archives remain unchanged; their restore creates an
independently verified empty group/site relation rather than inventing observations.

The entity-address dependency role `doctor_clinician_address` therefore resolves
to the address relation of this three-table generation, not a separately owned
address-only package. Shared canonical address tables and other sources'
contributions are outside this archive. This producer contract does not enable
destination automatic activation, retention relocation, or canonical resolution;
those require a separately integrated destination profile.

## Retained raw-source replay

The replay option consumes the original closed canonical
`cms-doctors-prepared-source.v1` manifest captured during successful source
preparation. It uses the configured durable artifact root, verifies the original
CSV/ZIP hash and byte length before and after parsing, and never resolves or
downloads a current source URL. Extra fields, inconsistent source/generation
bindings, changed retained bytes or changed completed source/address/education/group/site
counts fail the replay before it can complete.

Replay keeps the original dataset, source generation, acquisition timestamp,
row numbers and raw fields. Education future-year flags remain relative to that
original observation, even when the replay runs in a later calendar year.
Group/site observation timestamps and address timestamps use the same original
acquisition time. Replay does not infer employment, membership dates or verified
qualifications. A bounded `--test` replay remains unpublished and does not claim
the full manifest's counts. Normal acquisition still records a new observation.

## Key Environment Variables
- `HLTHPRT_CMS_DOCTORS_DATASET_ID` (default `mj5m-pzi6`)
- `HLTHPRT_CMS_DOCTORS_BATCH_SIZE`
- `HLTHPRT_CMS_DOCTORS_TEST_ROWS`
