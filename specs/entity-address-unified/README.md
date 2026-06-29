# Entity Address Unified Serving Model

**Status:** Historical design package; PTG overlay portions superseded
**Date:** 2026-06-13

**Superseded note, 2026-06-29:** The separate `ptg_address` projection and
`ptg-address` importer described in these specs were retired. PTG/TiC files do
not normally provide authoritative provider locations or contact numbers, so
runtime serving now uses `entity_address_unified` plus Provider Directory/NPPES
corroboration. PTG contributes price/provider/group/plan identity and network
context, not a standalone postal-address cache.

## Purpose

`entity_address_unified` becomes the authoritative provider/pharmacy address
serving table for HealthPorta APIs. It is a derived search model rebuilt from
published import tables, not a raw source table.

The current `entity_address_unified` table is not a client-facing contract. The
schema can be replaced completely as long as public API responses remain stable.

## Reviewed Direction

The reviewed design keeps source truth separate from serving truth:

- Raw imports keep their own tables and provenance.
- `address_archive_v2` remains the canonical physical address book.
- PTG/TiC contributes provider/group/plan identity and network context; it no
  longer publishes a separate `ptg_address` cache.
- `entity_address_evidence` records every source assertion.
- `entity_address_unified` is rebuilt from published source tables and optimized
  for API search.
- APIs that currently depend on `npi_address` for provider/pharmacy address
  behavior move to `entity_address_unified`.

## Spec Map

- [00 Review Summary](00-review-summary.md)
- [01 Scope And Decisions](01-scope-and-decisions.md)
- [02 Data Model](02-data-model.md)
- [03 Rebuild Importer](03-rebuild-importer.md)
- [04 API Cutover](04-api-cutover.md)
- [05 Validation Rollout And Operations](05-validation-rollout-and-operations.md)
- [06 PTG Address Overlay](06-ptg-address-overlay.md)
- [07 Source Inventory](07-source-inventory.md)
- [08 Address Identity Contract](08-address-identity-contract.md)

## Non-Negotiable Rules

- Do not mutate `entity_address_unified` incrementally from individual source
  imports.
- Rebuild from currently published source/projection tables only.
- Publish atomically using the existing staged-table rewrite/swap pattern.
- Keep raw source tables for audit, source-specific imports, and rollback.
- A failed rebuild must leave the previous published serving table active.
- Preserve public API response contracts during cutover.
- Do not treat PTG as a postal-address source unless PTG actually supplied the
  address. PTG usually contributes provider/group/plan coverage and uses NPI/TIN
  identity to inherit best-known addresses from other sources.
- PTG-only changes can trigger a normal `entity_address_unified` refresh, but
  they do not create or patch a PTG address projection.
- `entity_address_unified` must obey the `address_archive_v2` identity
  contract: record archive identity version, follow merges, and never treat
  city/ZIP-only precision as exact placement.

## Primary Outcome

After implementation, provider and pharmacy search can answer:

- "Find providers near this point or ZIP."
- "Find pharmacies in-network for this ACA plan."
- "Find providers for this procedure and group/PTG plan."
- "Find providers for this drug."
- "Show generally available providers without an insurance filter."
- "Explain which source systems support this address."

The result should be richer than `npi_address` while keeping the same public API
surfaces working.
