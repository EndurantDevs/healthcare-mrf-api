# 11 Observability Ops

## Purpose

Define operational metrics, logs, dashboards, alerts, and procedures.

## Metrics

- Active runs by status/importer/node.
- Queue depth by engine queue.
- Run duration by importer.
- Import bytes/files/rows.
- PTG slot usage.
- PTG/MRF resource lane by run.
- Planned memory budget and observed peak RSS/cgroup memory.
- Scanner data seconds, publish seconds, index seconds, analyze seconds.
- Worker launcher start failures by queue/class.
- Source-file imports blocked by node memory budget.
- Node heartbeat age.
- Node disk free.
- Base dataset freshness by node.
- Route-index size/staleness.
- Crawl success/failure counts.
- MRF source-discovery counts by provider, entity type, hosting platform, and run mode.
- File-probe OK/error counts, plus ETag, Last-Modified, and content-length coverage.
- Seed review backlog.
- Client quota usage.

## Logs

Every service logs with:

- `request_id`
- `run_id`
- `source_file_import_id`
- `client_id` where applicable
- `node_id`
- `source_id`
- `discovered_plan_id`
- `mrf_file_id`
- `entity_type`
- `hosting_platform`

## Alerts

- Run failed.
- Run active with stale heartbeat.
- Node unhealthy.
- Node disk below threshold.
- PTG route index missing for imported file.
- Crawl failures above threshold.
- TPA source-discovery has zero body-file references after crawl.
- File-probe sample has low ETag/Last-Modified/content-length coverage.
- Source-discovery run succeeds with zero `urls_checked` or zero `files_discovered` when those are expected by schedule.
- Public coverage metrics stale.
- Scheduler not firing.
- Lane saturated with planned work waiting longer than threshold.
- A huge PTG import running while node memory is below safety margin.
- Worker class/queue mismatch rejected by engine guard.

## Operational Procedures

Daily:

- Review failed/stale runs.
- Review node disk, PTG lanes, and memory headroom.
- Recrawl priority sources.

Weekly:

- Import/review source seeds.
- Check source duplicates.
- Run TPA source-discovery crawl and TPA file-probe smoke.
- Review quota anomalies.

Monthly:

- Full active-source crawl.
- Base dataset freshness audit across nodes.
- Public coverage delta report.

## Acceptance Criteria

- On-call can answer: what is running, where, why, and what is blocked.
- Dashboard can show node drift and route gaps.
- Failed imports retain enough diagnostics for retry decisions.
- Operators can see whether a run is waiting for memory, waiting for lane
  capacity, active, stale, failed, or completed.
- Operators can answer which TPA-hosted sources currently have searchable body-file references and
  whether their sampled files changed by ETag or Last-Modified.
