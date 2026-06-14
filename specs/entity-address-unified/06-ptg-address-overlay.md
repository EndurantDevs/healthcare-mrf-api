# 06 PTG Address Overlay

## Purpose

`ptg_address` is a fast projection that connects current PTG2 snapshots to the
best known provider addresses and PTG coverage filters.

It exists so frequent PTG imports do not require rebuilding the full combined
address/search table from large shared NPI and MRF data.

## Current Deployment Topology

Today dev/prod uses one PostgreSQL host. That means node scope is logical
placement metadata for routing and ownership, not a requirement for separate
databases.

The fast refresh requirement still applies:

- base NPI/MRF/address rows can stay untouched;
- only `ptg_address` and the PTG overlay partition/view need to refresh after a
  PTG-only import;
- import-control can show node ownership even when all nodes write to the same
  database.

Future multi-node deployment can split PTG ownership by node without changing
the serving contract.

## Why This Is Needed

NPI and MRF address data are large and mostly shared. PTG imports are more
frequent and can be source/node-specific.

If every new PTG import forced a full rebuild from `npi_address` and
`mrf_address`, refresh would be too slow and operationally risky.

Instead:

```text
shared base data:
  npi_address + mrf_address + cms/facility/provider sources
        |
        v
  entity_address_unified base rows

current PTG data:
  current PTG2 snapshots
        |
        v
  ptg_address
        |
        v
  entity_address_unified PTG overlay
```

## Table Meaning

`ptg_address` means:

> For this PTG source/snapshot/plan/provider identity, this is the best address
> currently known for search, and this is the confidence/provenance of that
> attachment.

It does not automatically mean PTG supplied the postal address.

## Source Priority

Recommended address candidate priority:

1. Direct PTG postal address, if a PTG source ever supplies one.
2. MRF address evidence matched to the provider and payer/network context.
3. NPPES primary/practice address from `npi_address`.
4. CMS doctors/facility/provider-enrichment address.
5. ZIP/city-only fallback, if useful for broad search.

The selected row must carry:

```text
location_key
address_source_id
address_source_record_key
location_confidence_id
address_key
premise_key
archive_identity_version
address_precision
```

`city_zip` precision is broad fallback only. It must not be used as exact
provider placement for radius or nearby searches.

## Importer

Importer name:

```text
ptg-address
```

Input:

- current PTG2 snapshots owned by the deployment/node;
- current PTG2 routes/catalog state;
- published base address tables;
- current `address_archive_v2` identity version.

Output:

```text
ptg_address
```

Publish style:

```text
ptg_address_<run_id> -> validate -> swap to ptg_address
```

Keep `ptg_address_old` as immediate rollback.

## Fast Refresh Contract

After a PTG import publishes:

1. Run `ptg-address`.
2. Resolve `address_archive_v2` identity fields and merge redirects.
3. Swap `ptg_address`.
4. Mark the combined search table PTG overlay dirty.
5. Refresh only the PTG overlay or partition of `entity_address_unified`.

Do not rescan/rewrite the large NPI/MRF base for PTG-only changes.

PTG fast refresh is blocked if:

- archive identity version changed;
- base address version changed;
- `ptg_address` contains unresolved merged archive keys;
- `ptg_address` has a different archive identity version than the base serving
  rows.

## Multi-Node Contract

Forward-looking contract for future split-node deployments:

Each serving node may own:

- its local PTG snapshots;
- its local `ptg_address`;
- its local PTG overlay in `entity_address_unified`.

All nodes can share the same base address version:

- `npi_address`
- `mrf_address`
- `address_archive_v2`
- non-PTG base search rows

When the base version or archive identity version changes:

1. mark local `ptg_address` stale;
2. rerun `ptg-address` on each node;
3. refresh the PTG overlay.

The current single-Postgres deployment should implement the same metadata and
health checks even though all overlays live in one database.

## Performance Rules

- Rebuild from current PTG snapshots only, not historical inactive snapshots.
- Resolve address candidates with indexed joins by NPI, TIN, address key,
  source key, plan ID, and snapshot ID.
- Keep a configurable history window only for rollback/debugging.
- Avoid per-row remote calls.
- Avoid per-row Python lookups for large PTG projections.
- Prefer set-based SQL or COPY-based staging.
- Keep `ptg_address` compact: do not copy raw PTG JSON, raw URLs, or full
  address text when `address_key` can join back to `address_archive_v2`.
- Keep procedure and medication coverage in bridges, not in `ptg_address`
  hot-row arrays.

## Validation

`ptg-address` publish requires:

```text
current snapshot count matches control metadata
provider identity count is nonzero for active PTG snapshots
ptg_address row count is within expected range
plan/source arrays populated for active PTG snapshots
address_source_id populated
location_confidence_id populated
archive_identity_version populated and current
base_address_version populated
no unresolved merged_into address keys
city_zip rows flagged as broad fallback only
indexes exist
PTG provider search smoke query passes
```

## Failure Behavior

If `ptg-address` fails:

- keep existing `ptg_address` live;
- do not refresh the PTG overlay;
- mark PTG overlay stale in import-control;
- allow retry after source/base/archive issue is fixed.

The existing non-PTG base searches should continue to work.
