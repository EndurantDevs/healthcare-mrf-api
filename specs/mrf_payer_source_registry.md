# MRF Payer Source Registry & Storage Design

Status: implementation spec for the source-neutral `mrf-source-discovery` importer.

The deduped payer/source roster lives in
[`mrf_payer_master_list.md`](mrf_payer_master_list.md). Runtime imports read that master list only;
external research inputs can be used during a manual refresh, but are not stored as provider
configuration or active repository references.

## Core Facts

There is no federal master index of payer Transparency in Coverage MRF URLs. CMS defines the rule
and schema, while payers, issuers, employers, TPAs, and platform vendors publish their own entry
points. The durable catalog layer is therefore a curated source list plus periodic metadata
recrawls.

Key consequences:

- BCBS is many separate legal entities. Never collapse Blue licensees into one payer.
- One payer can expose many TOCs: fully insured, self insured, marketplace, brand, EIN, and
  state-specific variants can all be separate source rows.
- TPA-hosted sources are first-class discovery targets. The regulated plan or issuer may delegate
  publishing, but the TPA landing page or metadata index is still part of the public source layer.
- Discovery indexes TOCs and probes body-file headers. It never downloads full in-network or
  allowed-amount rate bodies.

## Runtime Source Model

`specs/mrf_payer_master_list.md` is the only default provider for `mrf-source-discovery`.
`specs/mrf_source_discovery_sources.json` stores reusable platform resolvers, not third-party
registry URLs.

Master-list rows store:

| Field | Purpose |
|---|---|
| `Payer` | Searchable payer, issuer, network, or TPA display name. |
| `Type` | `national`, `blue`, `regional`, `medicaid_mco`, `tpa`, `network/tpa`, `provider_sponsored`, `dtc`, or `medicare_advantage`. |
| `Public MRF TOC / landing URL` | Stable payer-owned, TPA-owned, government, or otherwise public source entry point. Blank rows are allowed for known payers needing research. |
| `Notes` | Operational note only. Do not store third-party provenance or temporary signed body-file URLs. |

Rows without a URL, and rows marked unsupported or archived, are skipped by importer runs. Stale
rows are still checked because stale can mean the source is out of cadence rather than unreachable.

## Hosting Platform Resolvers

The configured resolvers convert stable landing URLs into crawlable TOC/index targets:

- HealthSparq and Aetna Health1 public app URLs into `latest_metadata.json` maps.
- UHC/Optum public blob listing URLs into monthly TOC/index JSON targets.
- BCBS Massachusetts current-month issuer TOCs from deterministic issuer filename suffixes.
- Highmark HMHS landing pages into state/licensee TOC/index JSON targets.
- Sapphire MRF Hub pages into server-rendered `/tocs/YYYYMM/*_index.json` links.
- Cigna compliance pages into `/static/mrf/*.json` lookup files and current TOC/index targets.
- Generic HTML compliance pages into linked TOC JSON files, metadata text indexes, and direct
  body-file references when exposed.

Add a resolver only when the URL pattern is reusable across multiple sources or stable enough to
maintain. One-off payer URLs belong in the master list.

## Storage Design

The importer writes these local catalog tables:

| Table | Stores |
|---|---|
| `mrf_payer` | Canonical payer, aliases, parent group, entity type, states, EINs, lifecycle, and source tags. |
| `mrf_source` | Source URL, hosting platform, access model, freshness metadata, crawl status, and observed counts. |
| `mrf_plan` | Plan IDs, names, market types, reporting entity metadata, and payer/source links from TOCs. |
| `mrf_file` | TOC child file URLs, file type, size, ETag, Last-Modified, container format, plan links, and last-seen metadata. |
| `mrf_url_observation` | URL check/probe history for stale, broken, redirected, gated, and changed URLs. |
| `mrf_crawl_run` | Discovery run summary, counts, status, and errors. |
| `mrf_payer_scorecard` | Optional quality/compliance scorecard rows when an approved enrichment source is configured. |

## Checks Worth Running

- Reachability: source URL `HEAD`/metadata fetch is public and not private-network.
- Freshness: ETag, Last-Modified, content length, and latest index date change tracking.
- Schema validity: TOC files validate against the supported CMS schema version.
- Identity: stable plan identity across URL reorganizations and duplicate source URLs.
- Scale: plan/file counts, compressed size, and body-file container format.
- Privacy: public/client surfaces must not expose signed URLs, raw source payloads, node IDs, or
  private client-submitted URLs.

## Import Strategy

1. Read curated source candidates from `mrf_payer_master_list.md`.
2. Filter by `--source-entity-types` and `--source-payer-query` when requested.
3. Upsert payer/source rows.
4. Optionally `HEAD` source URLs with SSRF protections.
5. Optionally resolve platform URLs and crawl TOC/index metadata.
6. Upsert plan/file rows and URL observations.
7. Optionally expose public source seeds and discovered plans to an external catalog.
8. Use `--probe-files` for metadata-only body-file `HEAD` checks.

## Maintenance

- Add payer-owned, TPA-owned, government, or public source URLs to the master list.
- Do not add temporary signed body-file URLs. Store stable TOC, index, lookup, or landing URLs.
- Use one-time external research to refresh the master list, then remove provider-specific scripts,
  URLs, and docs references from runtime code.
- Configure cadence in an external scheduler, not Python defaults.
- Keep `docs/data-sources.md`, `docs/imports/mrf-source-discovery.md`, and this spec in sync when
  source-discovery behavior changes.

Operational runbook: [`docs/devops/mrf-source-discovery.md`](../docs/devops/mrf-source-discovery.md).
