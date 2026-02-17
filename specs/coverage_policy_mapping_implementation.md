# Coverage-Policy Mapping (ICD-10-CM ↔ HCPCS/CPT) Implementation Spec

## 1. Objective
Build a **coverage-policy mapping subsystem** in `healthcare-mrf-api` that answers:
- Which diagnosis codes are supported for a procedure code under a given policy context?
- Which procedure codes are supported for a diagnosis code under a given policy context?

This is **not** a universal clinical crosswalk. It is a policy-evidence mapping based on payer policy artifacts (LCD/NCD/article-style coding relationships).

## 2. Scope
### In scope
- New normalized data model for policy documents, rules, and code links.
- Import pipeline for policy mapping datasets.
- API endpoints for lookup and evidence-backed responses.
- Context-aware filtering (`effective_date`, `jurisdiction`, `policy_scope`, retired/current).
- Test coverage for parser, DB writes, and API behavior.

### Out of scope
- Clinical equivalence mapping independent of policy context.
- Medical necessity inference beyond source policy evidence.
- Free redistribution of copyrighted code descriptions.

## 3. Compliance / Licensing Guardrails
1. `ICD-10-CM` source data is CMS-hosted and can be used for this subsystem.
2. `HCPCS` alpha-numeric source data is CMS-hosted and can be used.
3. `CPT` handling in this subsystem is **codes-only unless licensed descriptions are explicitly available**.
4. Add `source_license` and `description_rights` metadata on imported datasets.
5. API must avoid returning CPT descriptions unless `description_rights=true` for the loaded dataset.

## 4. Proposed Data Sources (v1)
- CMS MCD downloadable datasets (LCD/NCD/article relationships) as primary mapping evidence.
- Existing local code source snapshots (ICD-10-CM/HCPCS) for normalization and validation.
- CMS + NAIC Transparency in Coverage webinar (June 27, 2022, PDF) for implementation guidance:
  https://www.cms.gov/files/document/transparency-coverage-webinar-naic-06-27-22-508.pdf

## 5. Data Model (new tables under `mrf` schema)
### `coverage_policy_document`
- `policy_id` (PK, text) - stable id from source system.
- `policy_type` (text) - `lcd | ncd | article | other`.
- `title` (text)
- `jurisdiction` (text, nullable)
- `contractor` (text, nullable)
- `effective_start` (date, nullable)
- `effective_end` (date, nullable)
- `status` (text) - `active | retired | draft | unknown`.
- `source_system` (text)
- `source_url` (text)
- `source_license` (text)
- `description_rights` (boolean, default `false`)
- `checksum` (bigint)
- `updated_at` (timestamp)

Indexes:
- `(policy_type, jurisdiction, status)`
- `(effective_start, effective_end)`
- unique `(policy_id)`

### `coverage_policy_rule`
- `rule_id` (PK, bigint/bigserial)
- `policy_id` (FK -> `coverage_policy_document.policy_id`)
- `rule_name` (text, nullable)
- `rule_kind` (text) - `covered | non_covered | conditional | unknown`.
- `place_of_service` (text, nullable)
- `age_min` (int, nullable)
- `age_max` (int, nullable)
- `gender` (text, nullable)
- `evidence_text` (text, nullable)
- `evidence_ref` (text, nullable)
- `checksum` (bigint)

Indexes:
- `(policy_id, rule_kind)`
- `(place_of_service)`

### `coverage_policy_rule_code`
- `rule_code_id` (PK, bigint/bigserial)
- `rule_id` (FK -> `coverage_policy_rule.rule_id`)
- `code_system` (text) - `icd10cm | hcpcs | cpt`
- `code_value` (text)
- `code_role` (text) - `diagnosis | procedure`
- `is_primary` (boolean, default `false`)
- `raw_value` (text, nullable) - unparsed source token/range

Indexes:
- `(code_system, code_value, code_role)`
- `(rule_id, code_role)`
- partial index for fast pair lookup:
  - `(rule_id, code_system, code_value)`

### Optional denormalized helper table (recommended for speed)
`coverage_policy_pair`
- flattened diagnosis/procedure pair rows materialized from rules.
- key columns: `policy_id`, `jurisdiction`, `effective_start`, `effective_end`, `diagnosis_system`, `diagnosis_code`, `procedure_system`, `procedure_code`, `rule_kind`, `rule_id`.
- use for high-throughput lookup endpoint.

## 6. API Contract (v1)
### `GET /api/v1/coverage/mappings`
Query params:
- `diagnosis_code` (optional)
- `diagnosis_system` (`icd10cm`, default)
- `procedure_code` (optional)
- `procedure_system` (`hcpcs|cpt`, optional)
- `policy_type` (optional)
- `jurisdiction` (optional)
- `effective_date` (optional, defaults to today)
- `include_retired` (`0|1`, default `0`)
- `limit` (default 50, max 200)
- `page` (default 1)

Validation:
- require at least one of `diagnosis_code` or `procedure_code`.
- strict code format validation per `code_system`.

Response shape:
- `query_context`
- `items[]` each containing:
  - `diagnosis` `{system, code}`
  - `procedure` `{system, code}`
  - `coverage_status` (`covered|non_covered|conditional|unknown`)
  - `policy` `{policy_id, policy_type, title, jurisdiction, effective_start, effective_end, status}`
  - `evidence` `{rule_id, rule_name, evidence_text, source_url}`
- `pagination`

### `GET /api/v1/coverage/policies/:policy_id`
- returns one policy + rules + code links for auditability.

### `GET /api/v1/coverage/stats`
- counts by policy type, status, jurisdiction, and code system coverage.

## 7. Import Pipeline Design
Add new importer module:
- `process/coverage_policy.py`

Responsibilities:
1. Download/ingest source files.
2. Parse policy metadata.
3. Parse coverage rule sections and code relationships.
4. Normalize codes/ranges into atomic rows.
5. Upsert into `coverage_policy_document`, `coverage_policy_rule`, `coverage_policy_rule_code`.
6. Refresh denormalized `coverage_policy_pair` table.

Queue isolation:
- default queue key: `HLTHPRT_QUEUE_COVERAGE_POLICY`.
- no queue sharing with `NUCC/NPI/MRF` importers.

CLI integration in `main.py`:
- `python main.py start coverage-policy`
- `python main.py worker process.CoveragePolicy --burst`

## 8. Implementation Plan (5-minute slices)
> Each line is one 5-minute slot of focused implementation work.

### Phase A: Scaffolding (50 min)
1. **00:00-00:05** Create branch + verify clean target files in `healthcare-mrf-api`.
2. **00:05-00:10** Create `specs/` entry and add TODO marker in README for new coverage module.
3. **00:10-00:15** Add new module skeleton `process/coverage_policy.py`.
4. **00:15-00:20** Add module skeleton `api/endpoint/coverage.py`.
5. **00:20-00:25** Add placeholder routes registration in API bootstrap/router.
6. **00:25-00:30** Add model class stubs in `db/models.py`.
7. **00:30-00:35** Add migration stub in `alembic/versions/*_coverage_policy_tables.py`.
8. **00:35-00:40** Wire importer command stub into `main.py` CLI.
9. **00:40-00:45** Wire worker class stub into `main.py worker` dispatch.
10. **00:45-00:50** Add queue env var defaults and docs in `.env.example` comments.

### Phase B: Schema + Migration (70 min)
11. **00:50-00:55** Implement `coverage_policy_document` SQLAlchemy model.
12. **00:55-01:00** Implement `coverage_policy_rule` model + FK.
13. **01:00-01:05** Implement `coverage_policy_rule_code` model + FK.
14. **01:05-01:10** Add compound indexes on code lookup fields.
15. **01:10-01:15** Add optional `coverage_policy_pair` model.
16. **01:15-01:20** Create Alembic table DDL for all new tables.
17. **01:20-01:25** Add PK/FK/index constraints in migration.
18. **01:25-01:30** Add migration downgrade path.
19. **01:30-01:35** Run migration locally and inspect schema.
20. **01:35-01:40** Add smoke SQL checks for row insert/select.
21. **01:40-01:45** Confirm no naming collisions in `mrf` schema.
22. **01:45-02:00** Add helper views/materialized table SQL for pair lookup (if enabled).
23. **02:00-02:05** Validate migration idempotence in fresh DB.
24. **02:05-02:10** Validate migration on populated DB clone.

### Phase C: Parser + Normalization (80 min)
25. **02:10-02:15** Add source fetch helper with retry/backoff.
26. **02:15-02:20** Add parser for policy document headers/metadata.
27. **02:20-02:25** Add parser for rule blocks.
28. **02:25-02:30** Add code token extraction utility.
29. **02:30-02:35** Add ICD code validation utility.
30. **02:35-02:40** Add HCPCS/CPT code validation utility.
31. **02:40-02:45** Add code-range expansion helper.
32. **02:45-02:50** Add normalization to `(code_system, code_value, code_role)` rows.
33. **02:50-02:55** Add checksum generation for dedupe/idempotent upserts.
34. **02:55-03:00** Add importer stage logging + counters.
35. **03:00-03:05** Add upsert for policy docs.
36. **03:05-03:10** Add upsert for rules.
37. **03:10-03:15** Add upsert for rule-code links.
38. **03:15-03:20** Add cleanup of stale rule links on reimport.
39. **03:20-03:25** Add denormalized pair table refresh routine.
40. **03:25-03:30** Add importer transaction boundaries and batch sizes.
41. **03:30-03:35** Add importer dry-run mode.
42. **03:35-03:40** Add importer `--test` mode compatibility (`*_test` DB suffix).

### Phase D: API Endpoints (70 min)
43. **03:40-03:45** Implement query arg parsing for `/coverage/mappings`.
44. **03:45-03:50** Add strict validation and error payloads.
45. **03:50-03:55** Build diagnosis-first lookup query.
46. **03:55-04:00** Build procedure-first lookup query.
47. **04:00-04:05** Build bidirectional lookup query (both inputs provided).
48. **04:05-04:10** Apply policy-type/jurisdiction/effective-date filters.
49. **04:10-04:15** Add pagination support.
50. **04:15-04:20** Build response serializer with evidence fields.
51. **04:20-04:25** Implement `/coverage/policies/:policy_id` endpoint.
52. **04:25-04:30** Implement `/coverage/stats` endpoint.
53. **04:30-04:35** Add CPT description guard (`description_rights` check).
54. **04:35-04:40** Add route registration and smoke test with curl.
55. **04:40-04:45** Add OpenAPI doc entries for new endpoints.
56. **04:45-04:50** Verify endpoint behavior under empty dataset.

### Phase E: Tests + Hardening (80 min)
57. **04:50-04:55** Add parser unit tests for metadata extraction.
58. **04:55-05:00** Add parser unit tests for rule extraction.
59. **05:00-05:05** Add code normalization tests (ICD/HCPCS/CPT).
60. **05:05-05:10** Add code-range expansion tests.
61. **05:10-05:15** Add importer idempotency test.
62. **05:15-05:20** Add queue isolation test (`HLTHPRT_QUEUE_COVERAGE_POLICY`).
63. **05:20-05:25** Add endpoint validation tests (400 cases).
64. **05:25-05:30** Add endpoint positive tests (diagnosis->procedure).
65. **05:30-05:35** Add endpoint positive tests (procedure->diagnosis).
66. **05:35-05:40** Add context filter tests (date/jurisdiction).
67. **05:40-05:45** Add CPT-description suppression tests.
68. **05:45-05:50** Add stats endpoint tests.
69. **05:50-05:55** Run `pytest` full suite and fix regressions.
70. **05:55-06:00** Run lint/static checks and fix style issues.
71. **06:00-06:05** Run importer + API local end-to-end smoke.
72. **06:05-06:10** Capture test artifacts and sample payloads in `doc/`.

### Phase F: Rollout (30 min)
73. **06:10-06:15** Prepare migration + importer runbook.
74. **06:15-06:20** Stage rollout on dev DB (`--test` first).
75. **06:20-06:25** Validate API latency and index usage.
76. **06:25-06:30** Promote to prod with post-deploy verification queries.
77. **06:30-06:35** Backfill monitoring dashboards and alert thresholds.
78. **06:35-06:40** Final signoff + publish operations notes.

## 9. Acceptance Criteria
1. Import job loads policy mappings with deterministic row counts.
2. API answers both lookup directions with policy evidence references.
3. Context filtering by date and jurisdiction works.
4. CPT descriptions are not exposed without rights.
5. Queue isolation confirmed (`coverage-policy` jobs never mix with other importer queues).
6. Full test suite passes with new tests included.

## 10. Operational Queries (post-deploy checks)
- Count documents:
```sql
select count(*) from mrf.coverage_policy_document;
```
- Count rules:
```sql
select count(*) from mrf.coverage_policy_rule;
```
- Count code links by system:
```sql
select code_system, count(*)
from mrf.coverage_policy_rule_code
group by code_system
order by count(*) desc;
```
- Validate pair coverage sample:
```sql
select diagnosis_code, procedure_code, count(*)
from mrf.coverage_policy_pair
group by diagnosis_code, procedure_code
order by count(*) desc
limit 20;
```

## 11. Risks and Mitigations
- **Risk:** source schema drift.
  - **Mitigation:** parser version adapters + strict tests on fixtures.
- **Risk:** over-expansion of code ranges causing row explosion.
  - **Mitigation:** cap range expansion + logging + alerting.
- **Risk:** policy ambiguity across jurisdictions.
  - **Mitigation:** require explicit jurisdiction in strict mode; return confidence + evidence.
- **Risk:** licensing mistakes around CPT descriptors.
  - **Mitigation:** code-only default, explicit rights flag, automated response guard.
