# 01 Scope And Decisions

## Problem

The service currently has multiple address-bearing source tables:

- NPPES provider addresses in `npi_address`
- ACA/MRF provider-network addresses in `mrf_address`
- CMS doctors and clinician addresses
- facility anchor addresses
- provider-enrichment addresses
- CMS provider-enrollment/PECOS-style addresses
- pharmacy license and Part D pharmacy activity addresses
- PTG/group-plan provider membership and coverage data

`npi_address` is a strong NPPES source, but it is not enough for the serving
model. It does not explain whether an address is confirmed by many sources, does
not represent all plan coverage types, and cannot directly support group/PTG
coverage filtering.

The desired table should preserve the useful search shape of `npi_address`
while adding multi-source address evidence and ACA/group/PTG coverage.

## Decision

Replace the existing internal `entity_address_unified` shape with a new
production serving/search table.

The table is allowed to change completely because no existing client depends on
the old table schema. Public API response compatibility is still required.

Add `ptg_address` as a separate PTG provider-location and coverage projection.
It is derived from current PTG2 snapshots plus best-known base addresses, and is
rebuilt/swapped quickly whenever logical node-scoped PTG data changes. In the
current deployment, node scope is routing/ownership metadata in one PostgreSQL
database.

## Source Of Truth Boundaries

### Raw/import truth

These tables remain source-specific and are not replaced:

- `npi_address`
- `mrf_address`
- `mrf_address_evidence`
- PTG2 source/snapshot/provider/group tables
- CMS doctors tables
- provider-enrichment tables
- provider-enrollment tables
- facility-anchor tables
- pharmacy license/activity tables
- claims/drug/pricing source tables

### Address identity truth

`address_archive_v2` remains the canonical physical address book:

- one normalized `address_key` per physical address identity
- archive identity version and merge/redirect semantics
- geocode metadata
- TIGER/PostGIS/ZIP/census enrichment
- address-source coverage bits

### Serving/search truth

`entity_address_unified` becomes the derived search table consumed by APIs.

`entity_address_evidence` becomes the companion provenance table that explains
why a searchable row exists.

### PTG projection truth

`ptg_address` becomes the logical node-scoped PTG projection used between raw PTG2
snapshots and the combined serving model.

It answers: for each current PTG source/snapshot/plan/provider identity on this
node, which best-known address can we attach for search, and how confident is
that attachment?

`ptg_address` is not a raw postal-address source unless PTG actually supplied
the postal address.

## Why Rebuild Instead Of Direct Mutation

Directly updating the unified serving table from each source import would create
mixed states, partial updates, and difficult rollback. The unified table should
be rebuilt from published facts.

This gives:

- deterministic rebuilds
- atomic publish
- simple rollback to the previous version
- clear dependency handling in the external orchestrator
- simpler debugging when a source import is wrong

## PTG Rule

PTG/group-plan data usually contains provider identity and coverage data, not
postal addresses. Therefore:

- PTG contributes plan/source/network/provider membership filters.
- PTG can attach coverage to the best known provider address through NPI/TIN.
- PTG-derived address rows should be marked as inferred unless PTG supplied the
  address directly.
- `ptg_address` should be rebuilt from all current logical node-scoped PTG2
  snapshots and atomically switched before the combined search table consumes it.

Example confidence code:

```text
location_confidence_code = inferred_from_nppes
source_systems includes ptg
```

## Out Of Scope For First Cut

- Replacing `address_archive_v2`
- Dropping raw source tables
- Removing `npi_address`
- Changing public API response contracts without a separate product review
- Building a fully incremental delta updater
- Human review tooling for near-match address conflicts
- Rebuilding the full NPI/MRF base on every PTG-only import

## Success Criteria

- `entity-address-unified` can rebuild from existing published data without
  external downloads.
- `ptg_address` can rebuild from existing current PTG2 snapshots without
  external downloads.
- The rebuild can run after several source imports and publish once.
- Failed rebuilds do not affect live APIs.
- APIs that previously depended on `npi_address` keep working from the new
  serving table.
- ACA, group/PTG, and no-insurance searches are supported from the same model.
- Source masks, compact evidence, and confidence fields make address confidence
  visible.
- Procedure and medication search use bridge tables instead of hot-row arrays.
- Source inventory coverage is explicit enough that implementation can audit
  which address-bearing tables are included, excluded, or enrichment-only.
