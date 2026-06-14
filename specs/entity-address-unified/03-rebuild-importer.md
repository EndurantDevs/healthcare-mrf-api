# 03 Rebuild Importer

## Importer Name

Use two controlled importers:

```text
ptg-address
entity-address-unified
```

`ptg-address` rebuilds PTG provider-location coverage from current PTG2
snapshots.

`entity-address-unified` rebuilds the API serving/search model from currently
published source/projection tables.

Neither importer downloads external data.

## High-Level Flow

```text
source imports publish raw data
        |
        v
source adapters append/upsert compact evidence by source_run_id
        |
        v
PTG imports publish current snapshots
        |
        v
ptg-address rebuilds and swaps ptg_address
        |
        v
import-control marks address unified dirty or PTG-overlay dirty
        |
        v
entity-address-unified waits for active dependencies
        |
        v
build unified/bridge stage tables from current evidence and projections
create indexes
validate
atomic swap serving tables or overlay
```

Evidence is not part of the nightly full-table swap path. It is compact,
append-only, and keyed by `source_run_id` so unchanged provenance is not rebuilt
every night.

## Staged Tables

For run ID `run_abc123`, build:

```text
ptg_address_run_abc123
entity_address_unified_run_abc123
entity_address_plan_bridge_run_abc123
entity_address_network_bridge_run_abc123
entity_address_ptg_bridge_run_abc123
entity_address_procedure_bridge_run_abc123
entity_address_medication_bridge_run_abc123
```

After validation, publish to:

```text
ptg_address
entity_address_unified
entity_address_plan_bridge
entity_address_network_bridge
entity_address_ptg_bridge
entity_address_procedure_bridge
entity_address_medication_bridge
```

Evidence publish behavior:

```text
entity_address_evidence partition/source_run_id append or upsert
active_source_run manifest update
```

Follow the existing import swap policy for serving tables: keep `_old` rollback
tables for the immediately previous live dataset.

## Dependency Classes

### Required for useful build

```text
geo
npi
address-archive-v2-migrate
```

`address-archive-v2-migrate` means the canonical address archive functions and
tables are available, populated for the current environment, and exposing a
current `identity_version`.

### Strongly recommended coverage sources

```text
claims-pricing
claims-procedures
drug-claims
partd-formulary-network
provider-enrichment
provider-enrollment
cms-doctors
facility-anchors
ptg
ptg-address
pharmacy-license
partd-pharmacy-activity
```

### Dependency behavior

- If a dependency is currently active, wait.
- If several dependencies finish during the same window, rebuild once.
- If a dependency has never run in the environment, build without it and record
  a missing-source warning.
- If a dependency failed, allow admin policy:
  - block rebuild
  - rebuild without failed source
  - retry failed dependency first

Default production policy should block on required dependencies and warn on
optional dependencies.

## Dirty Markers

Source imports should mark address-unified dirty when they publish data that can
change provider/pharmacy search.

Dirty marker fields:

```text
source_importer
source_run_id
published_at
reason
requires_rebuild boolean
```

Example reasons:

```text
npi_addresses_changed
mrf_addresses_changed
ptg_provider_membership_changed
ptg_address_changed
procedure_bridge_changed
drug_bridge_changed
taxonomy_changed
provider_enrichment_changed
provider_enrollment_changed
cms_doctors_changed
facility_anchors_changed
pharmacy_license_changed
partd_pharmacy_activity_changed
geo_changed
archive_identity_version_changed
```

## Scheduling

Default schedule:

```text
nightly at configured time in America/Chicago
```

Admin controls:

- set preferred launch time;
- trigger manual rebuild;
- choose rebuild policy for optional failed dependencies;
- view waiting dependencies;
- view archive identity version and stale overlay state;
- view last publish summary.

NPI should be weekly. If weekly NPI and MRF/PTG imports overlap, the derived
rebuild should wait until active dependencies finish and publish once.

PTG-only imports should not force the full NPI/MRF base rebuild. They should run
`ptg-address`, then refresh only the PTG overlay portion of the combined serving
model when the archive identity version is unchanged.

## Address Identity Checks

Every build that reads `address_archive_v2` must:

1. read the current archive identity version;
2. resolve `merged_into` chains to terminal canonical keys;
3. fail on merge cycles;
4. carry `archive_identity_version`, `address_precision`, `address_key`,
   `premise_key`, and `location_key` into staged rows;
5. exclude or down-rank `city_zip` precision according to
   [08 Address Identity Contract](08-address-identity-contract.md);
6. fail publish if base rows and PTG overlay rows use different archive identity
   versions.

## Build Steps

### Evidence update steps

Evidence adapters run after source imports publish or before the next unified
build:

1. Resolve the source run and source snapshot IDs.
2. Map raw source rows to compact relational evidence keys.
3. Resolve available address keys through `address_archive_v2`.
4. Write evidence rows keyed by `source_run_id`.
5. Mark superseded source-run partitions inactive, or set `retired_at` for rows
   no longer present in the active source-run manifest.
6. Emit source row counts and skipped reason buckets.

The unified serving rebuild reads only active evidence rows.

Current evidence means the latest successful `source_run_id` per source identity
and scope, as recorded in the active source-run manifest. The source identity
must include at least `source_id` plus the relevant snapshot/source key/node
scope for that adapter. Older successful runs remain retained for audit but are
inactive and must not contribute to `source_count`, `independent_source_count`,
confidence scoring, or bridge rows unless an admin explicitly pins that run for
rollback/debug.

### `ptg-address` build steps

1. Resolve current PTG2 snapshots and source routes.
2. Extract provider identities from PTG provider groups, provider sets, NPI
   sidecars, and TIN data where available.
3. Resolve best address candidates from published base sources:
   - direct PTG postal address if ever available
   - `mrf_address` / `mrf_address_evidence`
   - `npi_address`
   - `doctor_clinician_address`
   - `facility_anchor`
   - `provider_enrollment_ffs` / `provider_enrollment_ffs_address`
   - `pharmacy_license_record_v1`
   - `partd_pharmacy_activity_v2`
   - existing base unified rows where useful
4. Resolve archive identity fields and merge redirects.
5. Assign `location_key`, `address_key`, `premise_key`,
   `address_source_id`, and `location_confidence_id`.
6. Attach bounded PTG plan/source/group arrays.
7. Create `ptg_address_run_id`.
8. Build indexes and analyze.
9. Validate PTG row counts, source coverage, archive version, and geo coverage.
10. Atomically swap `ptg_address`.
11. Mark `entity-address-unified` PTG overlay dirty.

### `entity-address-unified` full build steps

1. Resolve available source tables, evidence partitions, and active source-run
   manifest.
2. Create serving and bridge stage tables.
3. Attach `address_key`, `premise_key`, archive precision, and resolved terminal
   keys where possible.
4. Build `entity_address_unified_run_id` from evidence and source projection
   tables.
5. Attach taxonomy/specialty arrays.
6. Attach bounded ACA plan/network/issuer arrays or plan/network bridges.
7. Attach bounded PTG/group-plan/source arrays or PTG bridges.
8. Build procedure bridge rows.
9. Build medication bridge rows.
10. Attach pharmacy-license and Part D pharmacy evidence.
11. Attach provider-enrollment, CMS doctor, and facility-anchor evidence.
12. Calculate source counts, independent source counts, and confidence fields.
13. Create indexes.
14. Analyze stage tables.
15. Run validation gates.
16. Swap live serving and bridge tables.
17. Update active publish metadata in import-control.

### `entity-address-unified` PTG-fast refresh steps

For PTG-only changes:

1. Confirm the heavy base address version has not changed.
2. Confirm `archive_identity_version` matches current archive metadata.
3. Read the newly published `ptg_address`.
4. Rebuild only the PTG overlay rows and PTG bridge rows.
5. Validate PTG search smoke queries and archive version consistency.
6. Swap only the overlay/partition, or publish a new stable view pointer.

This path must avoid scanning and rewriting the full `npi_address` and
`mrf_address` base.

## PTG Contribution

PTG/group-plan imports generally contribute provider membership and coverage,
not direct postal addresses.

Build behavior:

- Join PTG provider/group membership to NPI/TIN where available in
  `ptg-address`.
- Add `ptg_plan_array`, `ptg_source_array`, `group_plan_array`, and PTG bridge
  rows where needed.
- If no direct PTG address exists, attach PTG coverage to the best known
  provider address and mark location as inferred.
- Preserve PTG projection/evidence rows in `ptg_address` and
  `entity_address_evidence`.

Do not label the address source as PTG unless PTG supplied the actual address.

## Compactness Rules

- Do not copy raw JSON payloads into derived tables.
- Do not copy raw source URLs into derived tables.
- Do not duplicate full source rows; store source IDs and source record keys.
- Do not store full address display text in `entity_address_unified` when
  `address_key` is available.
- Use `address_archive_v2` for canonical address display fields.
- Use small lookup IDs or bitmasks for source systems, address roles,
  confidence codes, and entity subtypes where practical.
- Keep procedure and medication dimensions in bridges, not hot-row arrays.
- Move plan/network/PTG filters to bridge tables if arrays bloat the hot row or
  query plans degrade.

## Deployment Topology

Current dev/prod topology is one PostgreSQL host. That means the initial
implementation does not need cross-database coordination. The fast PTG win comes
from table partitioning or a base-plus-overlay view inside the same database.

Future multi-node behavior remains compatible:

- shared NPI, MRF, address archive, and other base data can use one base build;
- PTG imports can remain node-scoped if nodes own different PTG
  snapshots/routes;
- each node can refresh only its PTG overlay after new PTG imports;
- when base address version changes, mark PTG overlays stale and rerun
  `ptg-address`.

`ptg_address` should store `base_address_version` and
`archive_identity_version` so stale overlays are visible.

## Publish Strategy

Publish must be atomic from the API reader perspective.

Allowed approaches:

1. Physical table swap:
   - live table renamed to `_old`
   - staged table renamed to live name
   - staged indexes renamed to live names

2. Stable view pointer:
   - versioned physical table remains
   - stable view points to latest validated version

The physical table swap matches current importer patterns and is preferred for
the first implementation. Evidence partitions/manifests are updated separately
from serving-table swaps.

## Failure Behavior

If any validation or publish step fails:

- leave current live `entity_address_unified` untouched;
- leave current live bridges untouched;
- leave current active evidence manifest untouched;
- mark run failed with reason;
- keep failed stage tables only if configured for debugging;
- keep dirty markers so a retry can run.

## Rebuild Frequency

First production version should use deterministic full rebuilds for the heavy
base and for `ptg_address` over current PTG snapshots.

PTG-only changes should then use the PTG overlay refresh path. This is still a
bounded rebuild of derived PTG rows, not a row-by-row incremental updater.

Incremental serving-row rebuild can be considered later after:

- API cutover is complete;
- query plans are stable;
- source dependency semantics are proven;
- table size and rebuild time are measured on production-like data.
