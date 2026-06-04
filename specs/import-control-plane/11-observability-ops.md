# 11 Observability Ops

## Purpose

Define operational metrics, logs, dashboards, alerts, and procedures.

## Metrics

- Active runs by status/importer/node.
- Queue depth by engine queue.
- Run duration by importer.
- Import bytes/files/rows.
- PTG slot usage.
- Node heartbeat age.
- Node disk free.
- Base dataset freshness by node.
- Route-index size/staleness.
- Crawl success/failure counts.
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

## Alerts

- Run failed.
- Run active with stale heartbeat.
- Node unhealthy.
- Node disk below threshold.
- PTG route index missing for imported file.
- Crawl failures above threshold.
- Public coverage metrics stale.
- Scheduler not firing.

## Operational Procedures

Daily:

- Review failed/stale runs.
- Review node disk/PTG slots.
- Recrawl priority sources.

Weekly:

- Import/review source seeds.
- Check source duplicates.
- Review quota anomalies.

Monthly:

- Full active-source crawl.
- Base dataset freshness audit across nodes.
- Public coverage delta report.

## Acceptance Criteria

- On-call can answer: what is running, where, why, and what is blocked.
- Dashboard can show node drift and route gaps.
- Failed imports retain enough diagnostics for retry decisions.
