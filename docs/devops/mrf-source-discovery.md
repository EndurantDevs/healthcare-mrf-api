# MRF Source Discovery DevOps

`mrf-source-discovery` is the lightweight MRF catalog importer. It is safe to run on every
`healthcare-mrf-api` node because it fetches source indexes and metadata only; it does not download
full in-network or allowed-amount rate bodies.

## Operational Role

- Keep issuer, network, and TPA-hosted MRF source URLs searchable for PTG import targeting.
- Track source freshness through `HEAD` checks, ETag, Last-Modified, and content-length metadata.
- Parse TOC/index metadata into local `mrf_payer`, `mrf_source`, `mrf_plan`, `mrf_file`,
  `mrf_url_observation`, and `mrf_crawl_run` tables.
- Push discovered seeds to `import-control` when `--sync-import-control` is configured.

The source registry is data-driven:

- Resolver configuration: `specs/mrf_source_discovery_sources.json`
- Curated payer/source roster: `specs/mrf_payer_master_list.md`
- Design/spec notes: `specs/mrf_payer_source_registry.md`

Do not put payer/TPA source URLs in Python code or provider config. Add or correct them in the
master list.

## Required Workers

Start the ARQ worker class when running through queues/import-control:

```bash
python main.py worker MRFSourceDiscovery
```

Run directly for local smokes or one-off repairs:

```bash
python main.py start mrf-source-discovery --test
```

## Recommended Schedules

Configure these in `import-control`, not hard-coded cron:

| Cadence | Params | Purpose |
| --- | --- | --- |
| Daily | `--provider master-list --check-urls --concurrency 10` | catch broken/stale curated sources |
| Weekly | `--provider master-list --limit 500 --check-urls --concurrency 10 --sync-import-control` | refresh curated source universe and central control catalog |
| Weekly TPA | `--provider master-list --source-entity-types tpa --check-urls --crawl --concurrency 10` | keep TPA-hosted metadata and file references current |
| Weekly TPA probe | `--probe-files --file-probe-entity-types tpa --file-probe-limit 100 --concurrency 10` | sample TPA body-file freshness without full PTG import |
| Monthly | `--provider master-list --limit 500 --check-urls --crawl --concurrency 10 --sync-import-control` | full metadata refresh within bounded source universe |

Use `--crawl-target-limit` for production smoke/canary runs before unbounded monthly crawls.

## Local Smoke Commands

Local dev uses `.env`; in the shared dev setup this points PostgreSQL at `127.0.0.1:5440`.

```bash
./venv314/bin/python main.py start mrf-source-discovery --provider master-list --source-entity-types tpa --check-urls --crawl --concurrency 10
./venv314/bin/python main.py start mrf-source-discovery --probe-files --file-probe-entity-types tpa --file-probe-limit 100 --concurrency 10
./venv314/bin/python main.py start mrf-source-discovery --provider master-list --source-payer-query Meritain --crawl --concurrency 3
./venv314/bin/python main.py start mrf-source-discovery --probe-files --file-probe-payer-query Meritain --file-probe-limit 20 --concurrency 5
./venv314/bin/python main.py start mrf-source-discovery --provider master-list --source-payer-query Cigna --crawl --concurrency 3
```

Useful verification queries:

```sql
select crawl_run_id, mode, status, sources_discovered, urls_checked, plans_discovered,
       files_discovered, errors, finished_at
from mrf.mrf_crawl_run
order by started_at desc
limit 10;

select p.canonical_name, p.entity_type,
       count(distinct s.source_id) sources,
       count(distinct case when f.file_type = 'metadata-index' then f.mrf_file_id end) metadata_indexes,
       count(distinct case when f.metadata_json->>'container_format' = 'zip' then f.mrf_file_id end) zip_files,
       count(distinct case when f.file_type in ('in-network', 'allowed-amounts') then f.mrf_file_id end) body_files,
       count(distinct case when f.file_type in ('in-network', 'allowed-amounts') and f.size_bytes is not null then f.mrf_file_id end) body_files_with_size
from mrf.mrf_payer p
left join mrf.mrf_source s on s.payer_id = p.payer_id
left join mrf.mrf_file f on f.payer_id = p.payer_id
where lower(coalesce(p.entity_type, '')) like '%tpa%'
group by 1, 2
order by body_files desc, sources desc, 1;

with latest as (
  select crawl_run_id
  from mrf.mrf_crawl_run
  where mode = 'probe_files'
  order by started_at desc
  limit 1
)
select o.status, count(*) probes, count(o.etag) with_etag,
       count(o.last_modified) with_last_modified, count(o.content_length) with_size
from mrf.mrf_url_observation o, latest
where o.metadata_json->>'run_id' = latest.crawl_run_id
  and o.url_type = 'body_file_head'
group by o.status
order by o.status;
```

## 2026-06-05 Local Evidence

Validated against local PostgreSQL on `127.0.0.1:5440`:

- TPA slice crawl: `mrfcrawl_676311cd49de837d`, `10` sources, `92` plans, `100` files, no run errors.
- TPA file probe after placeholder filtering: `mrfcrawl_0fb58b9bec47b927`, `97/97` `HEAD` probes OK with ETag, Last-Modified, and size.
- Aetna Signature crawl: `mrfcrawl_0a4f6816eb7fed3d`, `22` plans, `30` files; file probe `7/7` OK.
- Meritain crawl with configured tenant override: `mrfcrawl_5bf49692b577a39d`, `8,691` plans, `4,045` files; file-probe sample `20/20` OK.

## Known Edge Cases

- Some landing pages have incomplete TLS chains. Treat the source URL `HEAD` failure as an
  observation, not as a crawl blocker, when the configured platform resolver can fetch a canonical
  metadata URL successfully.
- Some TOCs contain placeholder values such as `Missing file`; probe target loading filters to
  `http://` and `https://` URLs only.
- TPA pages are heterogeneous: Sapphire pages expose TOCs, Collective exposes `.txt` metadata
  indexes, and other TPAs may point to carrier/EIN search pages. Add new resolver rules only when a
  pattern is generic enough to be reused.
- Cigna uses HTML-discovered `/static/mrf/*.json` lookup files, not a direct TOC URL on the
  compliance page. Keep those lookup paths in `specs/mrf_source_discovery_sources.json`.
- BCBS Massachusetts uses deterministic current-month issuer TOC filenames on
  `transparency-in-coverage.bluecrossma.com`; keep the suffixes in
  `specs/mrf_source_discovery_sources.json`. If Python reports certificate-chain failures while
  curl succeeds, treat it as an upstream TLS-chain issue rather than disabling verification.
- Some payers publish body files as ZIPs containing JSON. Discovery records the ZIP URL and probes
  headers only; full PTG import owns ZIP streaming/decompression.
- Public/client surfaces must not show signed URLs, raw source payloads, internal node IDs, or
  private client-submitted source URLs.

## Post-Deploy Checks

```bash
./venv314/bin/python -m pytest tests/test_mrf_source_discovery.py tests/test_control_imports_api.py -q
./venv314/bin/python -m pytest -q
git diff --check
```

After deploying a node:

1. Confirm `/control/v1/importers` exposes `mrf-source-discovery`.
2. Run the TPA source smoke with `--source-entity-types tpa`.
3. Run the TPA probe smoke with `--file-probe-entity-types tpa`.
4. Confirm latest `mrf_crawl_run.status` is `succeeded` or `succeeded_with_errors`.
5. Confirm probe observations have nonzero ETag/Last-Modified/Content-Length coverage.
