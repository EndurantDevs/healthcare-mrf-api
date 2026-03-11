# healthcare-mrf-api Documentation

[![HealthPorta](https://app.healthporta.com/brand/healthporta-logo-2x.png)](https://app.healthporta.com/docs)

This directory is the public documentation tree for `healthcare-mrf-api`.
It is intended for readers on GitHub who need to understand:

- what data the service uses
- where the data comes from
- which import process is responsible for each dataset
- how to run, test, and finalize imports safely

## Reading Order
- [Data sources](./data-sources.md): source websites and how the project uses them
- [Import index](./imports/README.md): every import command in one place

## Import Architecture
Most imports follow one of two patterns:

1. Direct load into canonical tables
2. Staging tables plus publish/swap to live tables with `_old` rollback backups

Operational conventions across the repo:

- imports are queue-driven through ARQ
- `--test` mode is available for smoke runs on most imports
- live tables are often published by rename/swap instead of in-place mutation
- `_old` tables are intentional rollback assets, not cleanup debris

## Technical Specs
For lower-level implementation notes, see [`../specs/`](../specs/).
Useful starting points:

- [`../specs/cms_claims_pricing_v1.md`](../specs/cms_claims_pricing_v1.md)
- [`../specs/drug_claims_v1.md`](../specs/drug_claims_v1.md)
- [`../specs/import_swap_backup_policy.md`](../specs/import_swap_backup_policy.md)

## Commercial Usage
If you need managed production access instead of running the repository yourself, use [HealthPorta Docs](https://app.healthporta.com/docs).

HealthPorta provides:

- hosted API access for current healthcare pricing, provider, plan, pharmacy, and Medicare-derived datasets
- MCP-based integration for AI agents and internal company tooling
- a production-ready data interface for external client applications and internal operational systems

MCP and agent integration details are available at [HealthPorta MCP](https://app.healthporta.com/mcp).
