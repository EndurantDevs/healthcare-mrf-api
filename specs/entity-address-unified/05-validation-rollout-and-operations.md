# 05 Validation Rollout And Operations

## Validation Gates

Do not publish a rebuilt table unless validation passes.

Required publish gates:

```text
row_count >= configured minimum
row_count >= previous_publish_count * configured_floor
eligible NPI direct-address match >= 99.0% in sampled dual-read set
legacy top-50 geo results included in new top-100 >= 95.0%
top-50 geo result Jaccard >= 0.75 for provider search
top-50 geo result Jaccard >= 0.80 for pharmacy search
empty-result rate increase <= 0.5 percentage points
address_key coverage >= configured threshold
geo coverage >= configured threshold
pharmacy taxonomy rows present
provider taxonomy rows present
CMS doctors/source rows included when cms-doctors exists
facility-anchor rows included when facility-anchors exists
provider-enrollment rows included when provider-enrichment exists
pharmacy-license rows included when pharmacy-license exists
Part D pharmacy activity rows included when Part D data exists
procedure bridge rows present when claims-procedures exists
medication bridge rows present when drug-claims exists
ACA coverage arrays or bridges present when MRF exists
PTG coverage arrays or bridges present when PTG sources exist
ptg_address version is current when active PTG snapshots exist
archive_identity_version is current
no serving rows point to unresolved merged_into address keys
city_zip precision is excluded from precise radius candidates
no invalid latitude/longitude values
required indexes exist
service phone lookup uses the expression index and not a broad OR scan
hot table average row size below configured limit
bridge table counts within expected ranges
ANALYZE completed
smoke queries pass under timeout
```

Recommended default row-count floor:

```text
new_count >= 80% of previous_count
```

The exact threshold should be environment-specific during dev because small
sample imports may intentionally have lower coverage.

## Dual-Read Acceptance

Before API cutover, run old and new readers over fixed samples:

```text
sample_npi_detail: at least 10,000 NPIs or all rows in small dev data
sample_provider_geo: at least 500 ZIP/radius or lat/long searches
sample_pharmacy_geo: at least 500 ZIP/radius or lat/long searches
sample_procedure: at least 100 common procedure searches
sample_drug: at least 100 common drug searches
sample_ptg: all active dev PTG plans, then production sample before cutover
```

Pass/fail:

- Provider/pharmacy detail: for eligible rows with exact or better source
  precision, the served primary/practice address must equal the legacy
  `npi_address` selected address for at least 99.0% of sampled NPIs. Differences
  are allowed when the new model has stronger direct evidence and the delta is
  recorded.
- Geo search: at least 95.0% of legacy top-50 NPIs must appear in the new top
  100 for the same query unless they were excluded by precision policy.
- Geo ranking: top-50 Jaccard must meet the thresholds in Validation Gates.
- Empty result rate: new reader cannot increase empty responses by more than
  0.5 percentage points for equivalent filters.
- Procedure/drug search: legacy top-50 inclusion in new top-100 must be at least
  95.0% where the legacy source had the same procedure/drug coverage.
- PTG/group search: active PTG plan/provider counts must be within 2.0% of
  `ptg_address` control counts after overlay refresh.

All accepted deltas must be bucketed by reason:

```text
stronger_non_nppes_address
legacy_city_zip_excluded
archive_merge_redirected
source_conflict_lowered_rank
new_coverage_source_added
missing_source_adapter
query_plan_or_timeout
unknown
```

## Smoke Queries

Run these after staging build and before publish:

```text
provider by NPI
provider geo by ZIP
provider geo by lat/long
provider nearby
pharmacy by NPI
pharmacy geo by ZIP
pharmacy nearby
procedure provider search
drug provider search
PTG group-plan provider search
PTG group-plan provider search immediately after ptg_address swap
site-intelligence score
archive merge redirect sample
city_zip precision exclusion sample
```

Each smoke should record:

```text
status/result
row count
latency
query plan summary where relevant
source table version
archive_identity_version
```

## Query Plan Checks

Geo and filter-heavy searches need explicit plan review.

Plan checks should include:

- GIST geo index usage for radius searches;
- GIN index usage for bounded taxonomy/ACA/PTG arrays;
- expression index usage for service-phone/provider-directory lookups;
- bridge index usage for procedure and medication searches;
- no unexpected sequential scan on full table for common searches;
- acceptable latency on production-like data.

Initial warm-cache latency targets:

```text
provider/pharmacy detail p95 <= 250 ms
provider/pharmacy geo p95 <= 1000 ms
procedure/drug provider search p95 <= 1500 ms
PTG/group-plan search p95 <= 1500 ms
```

If production-like data shows these targets are unrealistic, update the targets
before cutover and record the reason. Do not leave the pass/fail undefined.

## Address Identity Validation

Run these checks on every staged build:

- current archive identity version is present;
- all staged rows have `archive_identity_version`;
- no staged row points to an address key whose `merged_into` is non-null;
- no merge cycles exist in the archive resolution query;
- staged `location_key` is stable for a fixed sample across two dry runs;
- fallback `location_key` does not include latitude/longitude buckets;
- bridge rows use only `location_key` values from the same staged publish;
- `city_zip` rows are not returned by precise radius-search smoke queries;
- base and PTG overlay rows use the same archive identity version.

See [08 Address Identity Contract](08-address-identity-contract.md).

## Observability

Import-control should display:

- current live version/run ID;
- current archive identity version;
- current `ptg_address` version/run ID;
- last successful rebuild time;
- last successful PTG address rebuild time;
- last failed rebuild time and reason;
- dirty sources waiting for rebuild;
- stale PTG overlay marker;
- stale archive identity marker;
- active dependencies causing wait;
- row counts by source system;
- geo coverage;
- address-key coverage;
- source-confidence summary;
- validation failures.

Worker logs should include:

- source table discovery;
- active evidence source-run manifest;
- PTG snapshot/source discovery;
- `ptg_address` row counts by source/snapshot;
- evidence row counts by source run;
- unified row counts by entity type;
- bounded array coverage counts;
- hot table row width and total size;
- bridge table row counts and index sizes;
- index build timings;
- validation timings;
- swap timing.

## Rollback

Rollback must use the existing swap backup model for serving tables.

On successful publish:

```text
entity_address_unified_old = previous live table
entity_address_unified = new staged table
entity_address_*_bridge_old = previous live bridge table
entity_address_*_bridge = new staged bridge table
ptg_address_old = previous live table, when ptg-address publishes
ptg_address = new staged table, when ptg-address publishes
```

Evidence rollback uses the active source-run manifest:

```text
active_source_run_manifest_old = previous active manifest
active_source_run_manifest = new validated manifest
```

Manual rollback:

1. pause address-unified rebuilds;
2. rename current live serving/bridge tables aside or restore view pointer;
3. rename `_old` serving/bridge tables back to live names;
4. restore the previous active source-run manifest if evidence activation
   changed;
5. restore live index names;
6. run smoke queries;
7. resume after root cause is understood.

## Dev Rollout

1. Implement schema and builder behind feature flag.
2. Run full base rebuild on dev database.
3. Run `ptg-address` on current dev PTG snapshots.
4. Run PTG overlay refresh without rebuilding the full base.
5. Compare against current `npi_address`-backed behavior with quantified
   dual-read gates.
6. Run focused healthcare-mrf-api tests.
7. Run api-layer e2e smoke against dev.
8. Run dashboard/import-control rebuild flow.
9. Review query plans and row counts with the team.

## Production Rollout

1. Deploy code with APIs still on legacy source.
2. Run `entity-address-unified` rebuild in build-only mode.
3. Validate staged tables.
4. Enable read-only dual comparison.
5. Cut over one API group at a time.
6. Monitor latency, error rate, row counts, and result deltas.
7. Keep rollback table available.
8. Remove temporary fallback only after stable production evidence.

## Test Plan

### Unit tests

- source availability detection;
- source inventory classification;
- evidence row generation by `source_run_id`;
- active evidence selection uses latest successful source run per source scope;
- source confidence scoring;
- independent source family counting;
- PTG inferred-address handling;
- `ptg_address` source priority;
- `ptg_address` stale base-version detection;
- archive identity version mismatch blocks publish;
- `merged_into` resolution and cycle detection;
- city/ZIP-only precision exclusion from precise geo;
- deterministic `location_key`;
- `location_key` churn after archive rekey/merge forces bridge rebuild;
- address role selection;
- provider/pharmacy detail dedupes base and PTG overlay rows by address identity;
- bridge aggregation for procedures and medications;
- row-count guard failure;
- dependency wait behavior;
- swap failure leaves live table untouched.

### SQL tests

- indexes are generated with safe names;
- geo index exists when PostGIS exists;
- bounded array indexes exist;
- procedure and medication bridge indexes exist;
- no duplicate primary keys;
- source arrays are deterministic;
- bridge dictionary IDs are stable or natural keys are used;
- confidence score handles source conflicts;
- no unresolved `merged_into` address keys are published.

### API tests

- provider detail compatibility;
- provider geo compatibility;
- pharmacy detail compatibility;
- pharmacy geo compatibility;
- procedure provider search compatibility;
- drug provider search compatibility;
- PTG/group-plan provider search;
- PTG/group-plan search after PTG-only overlay refresh;
- no-insurance provider search;
- authorization/bundle behavior through api-layer.

### E2E tests

- run source imports;
- run evidence update for active source runs;
- run PTG import and `ptg-address`;
- run `entity-address-unified`;
- run PTG-only overlay refresh;
- verify dashboard status;
- verify API calls against actual data;
- verify rollback path on intentionally bad rebuild.

## Open Questions For Team Review

1. Should API payloads expose `confidence_score` immediately or only
   `location_confidence_code` and decoded `address_sources`?
2. What is the minimum acceptable geo coverage for production publish by entity
   type?
3. Which optional dependencies should block rebuild in production?
4. Should PTG group-plan filters use internal plan IDs, public plan IDs, source
   keys, or all of them?
5. What retention window should compact evidence partitions keep after a source
   run is no longer active?
6. What is the final admin UI wording for dirty/waiting dependency states?
7. What average row-size threshold should block publish for
   `entity_address_unified` and `ptg_address`?

## Implementation Phases

### Phase 1: schema and builder

- redefine `EntityAddressUnified` ORM model;
- add compact append-only `EntityAddressEvidence`;
- add stage-table builder;
- add bridge-table builder;
- add index creation;
- add validation gates.

### Phase 2: rebuild importer

- wire importer lifecycle;
- add dependency wait logic;
- add dirty markers;
- add import-control status output;
- add manual rebuild support.

### Phase 3: source coverage

- NPPES/NPI source;
- MRF/ACA source;
- CMS doctors source;
- facility-anchor source;
- provider-enrollment/provider-enrichment source;
- pharmacy-license source;
- Part D pharmacy activity source;
- claims/procedure bridges;
- drug/provider medication bridges;
- PTG/group-plan arrays and bridges.

### Phase 4: API cutover

- provider/pharmacy details;
- geo/nearby search;
- pricing/provider searches;
- PTG/group-plan searches;
- reports/site-intelligence.

### Phase 5: production hardening

- production-like rebuild timing;
- query-plan tuning;
- rollback drill;
- API e2e suite;
- remove temporary migration fallback.
