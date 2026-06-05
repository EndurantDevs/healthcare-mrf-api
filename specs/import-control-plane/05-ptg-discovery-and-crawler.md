# 05 PTG Discovery And Crawler

## Purpose

Populate and refresh the MRF coverage catalog so users can search sources/plans without knowing raw URLs.

## Source Intake

- Admin manual source entry.
- Client private source submission.
- Third-party seed import for review.
- Scheduled recrawl of active sources.
- MRF source discovery seed import from AccessMRF, Payerset docs, MRF Data Solutions, TPAFS,
  BCBS roster, and the curated master list.
- TPA-hosted source discovery for self-funded group plan MRFs.

Third-party directories may be used as seed inputs only after provenance/license review. Official payer URLs and crawled TOCs are the durable truth.

## Crawl Flow

1. Select source candidates by status, priority, and next crawl time.
2. HEAD source URL where useful.
3. Compare ETag, Last-Modified, content length, and known content version.
4. Skip unchanged sources.
5. Stream GET changed sources.
6. Parse TOC/reporting structures using existing PTG parsing rules.
7. Upsert source versions, files, plans, aliases, and plan-file links.
8. Materialize coverage metrics.
9. Record crawl run summary and source status.

Crawler must never download in-network or allowed-amount file bodies during discovery. Metadata
discovery may fetch TOCs, HealthSparq/Sapphire platform metadata, UHC/Optum blob listings, Highmark
script-derived index links, and small TPA `.txt` metadata indexes. Body files are probed with `HEAD`
only to record size, ETag, and Last-Modified.

## Streaming Requirements

- Do not buffer multi-GB index files in memory.
- Parse incrementally where format permits.
- Checkpoint at payer/source boundaries.
- Persist crawl progress before starting the next source.
- Rate-limit by source domain and global crawler concurrency.

## Limited Production Crawl

Before full crawl:

- Max 25 payer indexes or max 10 GB of index/TOC bytes, whichever comes first.
- Verify ETag skip behavior.
- Verify resume after interruption.
- Verify source dedupe.
- Verify no private/signed URLs leak to public coverage APIs.

## Staleness Rules

- `active`: recent successful crawl and valid files.
- `stale`: no successful crawl within configured freshness window.
- `broken`: repeated HTTP/parsing failures.
- `needs_review`: seed/client source awaiting admin validation.
- `disabled`: admin-disabled.
- `duplicate`: source superseded by another canonical source.

## Admin Update Procedure

Daily:

- Recrawl priority active sources.
- Retry broken sources with backoff.
- Refresh coverage metrics.

Weekly:

- Import source seeds.
- Dedupe seeds against existing sources.
- Review new candidate sources.
- Run TPA source crawl: `mrf-source-discovery --provider master-list --source-entity-types tpa --check-urls --crawl --concurrency 10`.
- Run TPA body-file probe smoke: `mrf-source-discovery --probe-files --file-probe-entity-types tpa --file-probe-limit 100 --concurrency 10`.

Monthly:

- Full active-source recrawl.
- Coverage delta report.
- Stale/broken source review.

## Acceptance Criteria

- Client can search a plan without supplying a URL.
- Admin can promote/reject a seed.
- Re-crawl with unchanged ETag produces no duplicate plans/files.
- Plan identity remains stable across source URL reorganization.
- Admin can filter source-discovery by entity type or payer query, including TPA-only runs.
- TPA metadata crawls produce searchable plan/file references without full body downloads.
