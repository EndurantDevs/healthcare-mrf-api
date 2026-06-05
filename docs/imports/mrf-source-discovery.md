# MRF Source Discovery Import

`mrf-source-discovery` maintains a lightweight catalog of payer Transparency in Coverage MRF sources.
It discovers source URLs, checks freshness, parses TOC/index metadata, and stores searchable payer,
plan, and file rows without downloading full in-network or allowed-amount rate bodies.

## Run

```bash
python main.py start mrf-source-discovery --test
python main.py start mrf-source-discovery --provider accessmrf --limit 25 --check-urls
python main.py start mrf-source-discovery --provider all --limit 25 --check-urls --crawl --crawl-target-limit 5 --concurrency 10 --sync-import-control
python main.py start mrf-source-discovery --provider master-list --source-entity-types tpa --check-urls --crawl --concurrency 10
python main.py start mrf-source-discovery --probe-files --file-probe-limit 500 --concurrency 25
python main.py start mrf-source-discovery --probe-files --file-probe-entity-types tpa --file-probe-limit 100 --concurrency 10
```

The importer is also exposed through `/control/v1/importers` as `mrf-source-discovery`, so
`import-control` and `api-app` can schedule it, run it now, retry it, and show status/progress.
The operational runbook is [MRF source discovery DevOps](../devops/mrf-source-discovery.md).

Source URLs are configured in `specs/mrf_source_discovery_sources.json`, not in Python code.
Set `HLTHPRT_MRF_DISCOVERY_SOURCE_CONFIG` to point at another JSON registry for deployment-specific
source lists.

Platform resolvers are also configured in that file. The importer currently resolves:

- Aetna Health1 / HealthSparq public URLs into `mrf.healthsparq.com/.../latest_metadata.json`
  metadata maps, then stores direct referenced file URLs and plan metadata.
- UHC/Optum public blob listing pages into direct monthly TOC/index JSON targets.
- Highmark HMHS landing pages into the current-month state/licensee TOC/index JSON targets.
- Sapphire MRF Hub pages into direct `/tocs/YYYYMM/*_index.json` targets from server-rendered HTML.
- Simple compliance landing pages into linked metadata text indexes when they expose `.txt` metadata
  files; those metadata files are parsed only for direct body-file URLs and never downloaded as bodies.

## TPA Layer

TPA-hosted sources are first-class discovery targets. The TiC rule applies to group health plans and
health insurance issuers, but those entities may contract with another party, including a TPA, to
publish required MRFs. See [45 CFR 147.212](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-B/part-147/section-147.212)
and [CMS technical clarification Q36](https://www.cms.gov/healthplan-price-transparency/resources/technical-clarification).

Current curated TPA seeds live in `specs/mrf_payer_master_list.md` and include Allied Benefit
Systems, AmeriBen, BRMS, Collective Health, HPI, MedCost, PBA, WebTPA, HealthScope, Aetna Signature
Administrators, and Meritain. Meritain uses a configured HealthSparq tenant override in
`specs/mrf_source_discovery_sources.json`.

Resolved TOC targets are stored as `mrf_file.file_type = table-of-contents` rows even when
`--crawl-target-limit` limits how many target TOCs are parsed in a smoke run.
When the same canonical source URL is discovered by multiple registries, candidates are still stored,
but the crawler resolves/parses that source URL once per run and prefers curated master-list rows.

The importer remains Python/async because the slow path is external HTTP plus batched Postgres
upserts, not CPU-bound body parsing. Rust remains appropriate for full PTG body import/parsing, where
large compressed rate files dominate runtime.

## Options

- `--provider`: `all`, `master-list`, `accessmrf`, `payerset`, `mrfdatasolutions`, `cms-guide`, `tpafs`, or `bcbs-roster`.
- `--limit`: cap deduped candidates for smoke/bounded runs.
- `--source-entity-types`: comma-separated payer entity types to seed/check/crawl; `tpa` also matches `network/tpa`.
- `--source-payer-query`: case-insensitive payer-name substring for seed/check/crawl.
- `--dry-run`: collect and dedupe candidates without writing catalog tables.
- `--check-urls`: run SSRF-safe HEAD checks against discovered source URLs.
- `--crawl`: fetch and parse TOC/index metadata only.
- `--probe-files`: run metadata-only `HEAD` probes against stored MRF body-file URLs.
- `--file-probe-limit`: cap body-file HEAD probes for representative smoke runs; omit it to probe all stored body-file URLs.
- `--file-probe-types`: comma-separated file types to probe; defaults to `in-network,allowed-amounts`.
- `--file-probe-entity-types`: comma-separated payer entity types to probe; `tpa` also matches `network/tpa`.
- `--file-probe-payer-query`: case-insensitive payer-name substring for focused probes, for example `Collective`.
- `--concurrency`: maximum concurrent URL checks/TOC fetches; defaults to 10.
- `--crawl-target-limit`: cap resolved TOC targets after platform expansion; use it for smoke runs and omit it for full metadata recrawls.
- `--sync-import-control`: push discovered source seeds to `import-control` when configured.
- `--test`: use the curated master-list sample and skip external URL checks.

Useful environment knobs for full crawls:

- `HLTHPRT_MRF_DISCOVERY_HTTP_TIMEOUT`: total timeout for provider/TOC listing fetches; defaults to 300 seconds.
- `HLTHPRT_MRF_DISCOVERY_READ_TIMEOUT`: socket read timeout for provider/TOC listing fetches; defaults to 120 seconds.
- `HLTHPRT_MRF_DISCOVERY_WRITE_BATCH_SIZE`: parsed-row upsert batch size; defaults to 2000 rows.

## Stored Data

The importer writes local MRF catalog tables:

- `mrf_payer`: canonical payer, aliases, parent group, entity type, states, EINs, source coverage.
- `mrf_source`: source/index URL, hosting platform, access model, freshness, status, counts, provenance.
- `mrf_plan`: observed plan IDs, names, market types, and reporting entity metadata from TOCs.
- `mrf_file`: TOC child file URLs, file type, size/freshness metadata, plan links, network/file labels.
- `mrf_url_observation`: URL check history for stale, broken, redirected, gated, and changed URLs.
- `mrf_crawl_run`: discovery run summaries and errors.
- `mrf_payer_scorecard`: optional public/licensed vendor scorecard enrichment.

## Safety Rules

- Discovery never downloads full rate bodies.
- File probing uses `HEAD` only and stores `size_bytes`, `etag`, and `last_modified` when the
  origin provides them. Each probe is also recorded as a `body_file_head` URL observation.
- Full PTG import remains `python main.py start ptg` and should be launched from selected catalog IDs or explicit source URLs.
- TOC crawl mode parses configured platform targets and direct `.json` TOC URLs. Unresolved HTML/app landing
  pages are marked `crawl_skipped` instead of being fetched as JSON.
- URL checks reject private, loopback, link-local, multicast, reserved, and local hosts.
- Public/client surfaces must not expose signed URLs, raw vendor payloads, node IDs, or private client-submitted sources.

## Cadence

Recommended schedules:

- Daily: `--check-urls --provider master-list`
- Weekly: `--provider all --limit 500 --check-urls --concurrency 10 --sync-import-control`
- Smoke: `--provider master-list --limit 15 --check-urls --crawl --crawl-target-limit 2 --concurrency 5`
- Monthly: `--provider all --limit 500 --check-urls --crawl --concurrency 10 --sync-import-control`
- TPA freshness smoke: `--probe-files --file-probe-entity-types tpa --file-probe-limit 100 --concurrency 10`

Configure these in `import-control` so admins can change cadence, run now, disable, retry, and view
status from `api-app`.
