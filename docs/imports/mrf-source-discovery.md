# MRF Source Discovery Import

`mrf-source-discovery` maintains a lightweight catalog of payer Transparency in Coverage MRF sources.
It discovers source URLs, checks freshness, parses TOC/index metadata, and stores searchable payer,
plan, and file rows without downloading full in-network or allowed-amount rate bodies.

## Run

```bash
python main.py start mrf-source-discovery --test
python main.py start mrf-source-discovery --provider all --limit 25 --check-urls --crawl --crawl-target-limit 5 --concurrency 10
python main.py start mrf-source-discovery --provider master-list --source-entity-types tpa --check-urls --crawl --concurrency 10
python main.py start mrf-source-discovery --probe-files --file-probe-limit 500 --concurrency 25
python main.py start mrf-source-discovery --probe-files --file-probe-entity-types tpa --file-probe-limit 100 --concurrency 10
```

The importer is also exposed through the authenticated operator API as
`mrf-source-discovery`, so an external orchestrator can schedule it, retry it,
and observe status and progress.
The operational runbook is [MRF source discovery DevOps](../devops/mrf-source-discovery.md).

Source URLs are curated in `specs/mrf_payer_master_list.md`, not in Python code. The JSON registry
at `specs/mrf_source_discovery_sources.json` defines the master-list provider and reusable platform
resolvers only. Set `HLTHPRT_MRF_DISCOVERY_SOURCE_CONFIG` to point at another resolver registry for
deployment-specific behavior.
Master-list rows without a URL, or rows marked unsupported/archived, are retained as research
backlog but skipped by importer runs.

Platform resolvers are also configured in that file. The importer currently resolves:

- Aetna Health1 / HealthSparq public URLs into `mrf.healthsparq.com/.../latest_metadata.json`
  metadata maps, then stores direct referenced file URLs and plan metadata.
- UHC/Optum public blob listing pages into direct monthly TOC/index JSON targets.
- BCBS Massachusetts monthly issuer TOCs by generating current-month URLs for the two public issuer
  suffixes exposed by the payer app bundle.
- Highmark HMHS landing pages into the current-month state/licensee TOC/index JSON targets.
- Sapphire MRF Hub pages into direct `/tocs/YYYYMM/*_index.json` targets from server-rendered HTML.
- MyMedicalShopper/TALON MRF search pages into current employer plan TOC/index JSON targets by
  enumerating enabled entity employers through the public DDP API and keeping only the newest
  generated month per plan.
- Meritain MRF search pages into group/client HealthSparq links, preserving the group ID as
  `plan_info` so client-level rows are indexed even when the target remains an app-scoped link.
- Healthcare Bluebook MRF listing pages into stable tenant/file endpoints, with grid labels and
  EINs captured as group plan metadata for BRMS and Lucent delegated sources.
- HTML TPA pages that include both direct MRF links and Healthcare Bluebook delegations, including
  BRMS and Lucent landing pages. Direct CSV links are cataloged for visibility but excluded from
  automated catalog promotion.
- ASR Health Benefits group-number MRF pages into deterministic TOC URLs using the named
  `asr_health_benefits_groups` seed list and active rows from
  `specs/mrf_seed_lists/asr_health_benefits_groups.csv`.
- Cigna compliance landing pages into current `/static/mrf/*.json` lookup files, then into the
  current signed federal and Colorado TOC/index JSON targets. The resolver carries a larger
  per-target TOC byte cap because Cigna federal indexes can exceed the generic 25 MB default.
- IBX/QCC keyed Transparency in Coverage links, including public employer landing pages such as
  Reliance Matrix, into stable TOC targets while following the current monthly redirect at fetch
  time.
- Simple compliance landing pages into linked TOC JSON files, metadata text indexes, and direct
  body-file references when the HTML exposes them. Metadata text files are parsed only for direct
  body-file URLs. ZIP, gzip, and plain JSON body references are stored as `mrf_file` rows and are
  never downloaded during discovery.

## TPA Layer

TPA-hosted sources are first-class discovery targets. The TiC rule applies to group health plans and
health insurance issuers, but those entities may contract with another party, including a TPA, to
publish required MRFs. See [45 CFR 147.212](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-B/part-147/section-147.212)
and [CMS technical clarification Q36](https://www.cms.gov/healthplan-price-transparency/resources/technical-clarification).

Current curated TPA seeds live in `specs/mrf_payer_master_list.md` and include Allied Benefit
Systems, AmeriBen, BRMS, Collective Health, HPI, Lucent, MedCost, PBA, Varipro, WebTPA,
HealthScope, Aetna Signature Administrators, and Meritain. Meritain uses both the aggregate
HealthSparq tenant override and the public Meritain group search resolver. Standard/AHL searches
are routed to the public Allied Benefit Systems and Meritain delegated sources named by Standard's
AHL resources page, because the Standard page itself can present bot interstitials to automated
fetchers. BRMS and Lucent delegate some client MRF rows to Healthcare Bluebook, where discovery
stores the stable listing/file endpoints rather than any signed storage redirect. Varipro uses the
configured MyMedicalShopper/TALON resolver.

Resolved TOC targets are stored as `mrf_file.file_type = table-of-contents` rows even when
`--crawl-target-limit` limits how many target TOCs are parsed in a smoke run.
When the same canonical source URL appears more than once in the master list, the crawler
resolves/parses that source URL once per run and prefers the curated row.

The importer remains Python/async because the slow path is external HTTP plus batched Postgres
upserts, not CPU-bound body parsing. Rust remains appropriate for full PTG body import/parsing, where
large compressed rate files dominate runtime.

## Options

- `--provider`: `all` or `master-list`. Both use the curated master list.
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
- `--test`: use the configured test providers and skip external URL checks.

Useful environment knobs for full crawls:

- `HLTHPRT_MRF_DISCOVERY_HTTP_TIMEOUT`: total timeout for provider/TOC listing fetches; defaults to 300 seconds.
- `HLTHPRT_MRF_DISCOVERY_READ_TIMEOUT`: socket read timeout for provider/TOC listing fetches; defaults to 120 seconds.
- `HLTHPRT_MRF_DISCOVERY_WRITE_BATCH_SIZE`: parsed-row upsert batch size; defaults to 2000 rows.
- `HLTHPRT_MRF_CRAWL_ROW_WRITE_TIMEOUT_SECONDS`: optional per-batch database write timeout;
  defaults to `0`, which lets full-catalog crawls finish slow write batches instead of aborting.
- `HLTHPRT_MRF_DISCOVERY_MAX_TOC_BYTES`: generic TOC/metadata fetch limit; defaults to 25 MB.
  Resolver-specific caps in `specs/mrf_source_discovery_sources.json` can raise this for known
  larger index files without changing the global default.

ASR Health Benefits does not expose an all-groups catalog. Keep monthly imports constrained to the
`asr_health_benefits_groups` seed list, and refresh the CSV only through an intentional maintenance
sweep:

```bash
python scripts/research/discover_asr_health_benefits_groups.py --start 1 --end 9999 --concurrency 2 --write
```

## Stored Data

The importer writes local MRF catalog tables:

- `mrf_payer`: canonical payer, aliases, parent group, entity type, states, EINs, source tags.
- `mrf_source`: source/index URL, hosting platform, access model, freshness, status, counts, provenance.
- `mrf_plan`: observed plan IDs, names, market types, and reporting entity metadata from TOCs.
- `mrf_file`: TOC child file URLs, file type, size/freshness metadata, plan links, network/file labels.
- `mrf_url_observation`: URL check history for stale, broken, redirected, gated, and changed URLs.
- `mrf_crawl_run`: discovery run summaries and errors.
- `mrf_payer_scorecard`: optional scorecard enrichment.

## External Catalog Integration

The local catalog tables are the public integration boundary. An external
orchestrator may read eligible source, plan, and file records through an
authenticated operator API and mirror them into its own catalog. Integrations
must preserve stable plan and source-file identities and keep large
`plan_info` payloads bounded.

The authenticated control API exposes deterministic keyset pages for this
purpose:

- `GET /control/v1/mrf/discovery/sources?cursor=...&limit=...&run_id=...` lists
  stored sources and payer identity context. `run_id` scopes reconciliation to
  sources written by one discovery run. The maximum page size is 250.
- `GET /control/v1/mrf/discovery/sources/{source_id}/files?cursor=...&limit=...`
  lists normalized file and `plan_info` records for one source. The maximum
  page size is 500.

Both responses return `items` and `next_cursor`. Consumers should continue
until `next_cursor` is null, checkpoint only after a page is durably ingested,
and treat replayed pages as idempotent.

### Reader performance notes

File pages are bounded by both file count and 10,000 expanded plan references.
The database reader must apply both budgets while rows are streamed; fetching
the requested 500 rows before applying the plan budget can materialize far more
JSON than the response needs.

A July 2026 dense-source measurement found 619 remaining rows carrying about
128 MB of metadata JSON and 36 MB of plan arrays. The old 501-row query loaded
most of that data even though the plan budget needed about two rows. Streaming
in 16-row database batches reduced the first materialization batch to roughly
5 MB. A server-side serialization check on the same cursor fell from 0.70
seconds for 501 rows to 0.05 seconds for 16 rows; application-side savings are
larger because unused JSON no longer needs async driver decoding or Python
object construction.

When investigating a slow catalog refresh, record these separately:

- database row materialization and JSON decoding;
- API response construction and transfer;
- catalog write time;
- derived catalog refresh time;
- idle worker time caused by source-page barriers.

## Safety Rules

- Discovery never downloads full rate bodies.
- File probing uses `HEAD` only and stores `size_bytes`, `etag`, and `last_modified` when the
  origin provides them. Each probe is also recorded as a `body_file_head` URL observation.
- ZIP files are cataloged as compressed body-file references (`metadata_json.container_format =
  "zip"`). Full PTG imports are responsible for streaming/decompressing ZIP members when a selected
  plan/source is imported.
- Full PTG import remains `python main.py start ptg` and should be launched from selected catalog IDs or explicit source URLs.
- TOC crawl mode parses configured platform targets and direct `.json` TOC URLs. Unresolved HTML/app landing
  pages are marked `crawl_skipped` instead of being fetched as JSON.
- URL checks reject private, loopback, link-local, multicast, reserved, and local hosts.
- Public/client surfaces must not expose signed URLs, raw vendor payloads, node IDs, or private client-submitted sources.

## Cadence

Recommended schedules:

- Daily: `--check-urls --provider master-list`
- Weekly: `--provider master-list --check-urls --crawl --concurrency 10`
- Smoke: `--provider master-list --limit 15 --check-urls --crawl --crawl-target-limit 2 --concurrency 5`
- Monthly bounded audit: `--provider master-list --limit 500 --check-urls --crawl --concurrency 10`
- TPA freshness smoke: `--probe-files --file-probe-entity-types tpa --file-probe-limit 100 --concurrency 10`

Configure these in an external scheduler so operators can change cadence,
disable or retry runs, and inspect status through the authenticated operator
API.
