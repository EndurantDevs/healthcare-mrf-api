# 09 UI Spec

## Purpose

Define the `api-app` user experience for admins, clients, and public coverage.

## Admin Pages

### Imports Overview

- Active runs table.
- Status, importer, node, progress, duration, queue age, requester.
- Actions: cancel, retry, view details.

### Schedules

- Importer schedules.
- Cron/cadence editor.
- Enable/disable.
- Run now.
- Last/next run.
- Conflict/lock indicators.

### MRF Sources

- Search/filter sources.
- Source detail with crawl history.
- Add source.
- Validate URL.
- Crawl now.
- Promote/reject seeds.
- Mark duplicate/broken/disabled.

### Group Plans

- Search by payer, sponsor, issuer, state, market, name, plan id.
- Freshness badges.
- Discovered/imported/queued/stale status.
- Subscribe/unsubscribe.
- Preview source files and estimated bytes.

### Nodes

- Node health.
- Base dataset freshness.
- Disk/RAM/PTG slots.
- Queue depth.
- PTG placements.
- Drain/enable/disable.

## Client Pages

- Coverage search.
- Submitted sources.
- Plan subscriptions.
- Import requests and status.
- Quota usage.
- Clear async state: requested, queued, running, imported, failed.

## Public Pages

- `/coverage/mrf`
- `/coverage/mrf/sources`
- `/coverage/mrf/sources/[source_key]`
- `/coverage/mrf/plans`
- `/coverage/mrf/methodology`

Public pages show coverage and freshness, not control actions.

## UI Rules

- Never show private/signed URLs to unauthorized users.
- Distinguish "your request" from "shared import".
- Explain that imported CMS pricing data is shared.
- Show staleness before users subscribe.
- Prefer dense admin tables over marketing layouts for operational screens.

## Acceptance Criteria

- Admin can manage a source from seed to active crawl.
- Client can submit a source and track review/import status.
- Public user can understand coverage without logging in.
