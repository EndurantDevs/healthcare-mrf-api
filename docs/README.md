# healthcare-mrf-api Documentation

[![HealthPorta](https://app.healthporta.com/brand/healthporta-logo-2x.png)](https://app.healthporta.com/docs)

This directory is the public documentation tree for `healthcare-mrf-api`.
It is intended for readers on GitHub who need to understand:

- what data the service uses
- where the data comes from
- which import process is responsible for each dataset
- how to run, test, and finalize imports safely

## Reading Order
- [Architecture overview](./architecture.md): one-screen system flow
- [Data sources](./data-sources.md): source websites and how the project uses them
- [Import index](./imports/README.md): every import command in one place
- [MRF source discovery DevOps](./devops/mrf-source-discovery.md): payer/TPA source catalog schedules, smokes, and troubleshooting
- [Address canonical DevOps](./devops/address-canonical.md): deduplicated address archive rollout, ARQ restart, and smoke checks
- [MS-DRG DevOps](./devops/ms-drg.md): CMS MS-DRG reference refresh and smoke checks
- [Clinical reference DevOps](./devops/clinical-reference.md): direct-replace runbook and attribution rule

## Import Architecture
Imports follow one of three publish patterns. Check the per-import runbook before cleanup or rollback work:

1. Direct load into canonical tables
2. Validated staging followed by direct replacement of live tables
3. Staging tables plus publish/swap to live tables with `_old` rollback backups, or snapshot-pointer publish for PTG

Operational conventions across the repo:

- imports are queue-driven through ARQ
- `--test` mode is available for smoke runs on most imports
- live tables are not mutated in place during large loads
- `_old` tables are intentional only for importers whose runbook says they keep rollback backups

## Technical Specs
For lower-level implementation notes, see [`../specs/`](../specs/).
Useful starting points:

- [`../specs/data_source_registry.md`](../specs/data_source_registry.md)
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
