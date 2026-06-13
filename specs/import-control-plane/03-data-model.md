# 03 Data Model

## Purpose

Define the central catalog/control schema and the engine-side run registry. Names are logical; implementations may use SQLAlchemy naming conventions already present in each repo.

## Engine Schema: `mrf`

### `import_run`

Unified run registry in each engine.

Required columns:

- `run_id varchar(64) primary key`
- `engine varchar(64)` default `healthcare-mrf-api`
- `node_id varchar(64)`
- `importer varchar(64)`
- `family varchar(64)`
- `status varchar(32)`
- `phase_detail varchar(128)`
- `params jsonb`
- `idempotency_key varchar(160)`
- `triggered_by varchar(32)`
- `schedule_id varchar(64) null`
- `subscription_id varchar(64) null`
- `source_file_import_id varchar(64) null`
- `created_at timestamptz`
- `started_at timestamptz null`
- `finished_at timestamptz null`
- `heartbeat_at timestamptz null`
- `progress jsonb`
- `metrics jsonb`
- `error jsonb null`
- `snapshot_id varchar(96) null`
- `import_id varchar(64) null`

Indexes:

- `(status, heartbeat_at)`
- `(importer, created_at desc)`
- `(idempotency_key)` unique for active statuses
- `(schedule_id)`
- `(subscription_id)`
- `(source_file_import_id)`

Legacy run/log tables remain readable during rollout.

### PTG2 Engine Identity Tables

The engine owns PTG2 content identities. The control plane must not derive these
from its catalog hashes after the fact; it stores explicit nullable link columns
populated from TOC preview metadata and terminal run metrics.

- `ptg2_plan.plan_hash`: content identity for plan fields.
- `ptg2_plan_month.plan_month_id`: `(snapshot_id, plan_hash, import_month)`.
- `ptg2_source_catalog.source_catalog_id`: TOC catalog entry identity.
- `ptg2_source_identity.source_identity_hash`: `(source_type, canonical_url)`.
- `ptg2_source_file_version.source_file_version_id`: observed file version
  identity based on source identity, content hashes, ETag, length, and
  Last-Modified.
- `ptg2_snapshot.snapshot_id`: published snapshot.
- `ptg2_current_source_snapshot.source_key -> snapshot_id`: current snapshot
  pointer for a source-scoped PTG import.
- `ptg2_current_plan_source(plan_id, market_type, source_key, snapshot_id)`:
  current source-scoped lookup for a plan.

Engine `import_run.metrics` for PTG terminal runs should include:

- `import_run_id`
- `snapshot_id`
- `source_key`
- `import_month`
- `files_attempted`, `files_processed`, `files_failed`, `files_skipped`
- `serving_rates`, `rate_count`
- `source_file_versions[]` with `canonical_url`,
  `engine_source_identity_hash`, and `engine_source_file_version_id`

## Control Schema: `hp_import_control`

### `import_node`

Registered serving/import node.

- `node_id primary key`
- `display_name`
- `base_url`
- `control_url`
- `region`
- `roles text[]`
- `enabled bool`
- `drain_mode bool`
- `status`
- `last_heartbeat_at`
- `version`
- `feature_flags jsonb`
- `ram_bytes`
- `disk_total_bytes`
- `disk_free_bytes`
- `ptg_slot_capacity`
- `ptg_slots_used`
- `queue_depth jsonb`
- `metadata jsonb`

### `node_dataset_state`

Local dataset freshness per node.

- `node_id`
- `importer`
- `dataset_version`
- `snapshot_id`
- `status`
- `last_run_id`
- `last_refreshed_at`
- unique `(node_id, importer)`

### `mrf_source`

Catalog source record.

- `source_id primary key`
- `source_key unique`
- `display_name`
- `payer_name`
- `payer_group`
- `source_type`
- `official_url`
- `index_url`
- `canonical_index_url`
- `domain`
- `status`
- `visibility`
- `created_by`
- `verified_by`
- `last_verified_at`
- `last_crawled_at`
- `last_changed_at`
- `metadata jsonb`

### `mrf_source_seed`

Unreviewed seed from external lists or admin uploads.

- `seed_id primary key`
- `seed_provider`
- `seed_url`
- `raw_payload jsonb`
- `normalized_url`
- `confidence`
- `license_status`
- `review_status`
- `promoted_source_id null`
- `created_at`
- `reviewed_at null`

### `mrf_source_version`

Observed source/index version.

- `source_version_id primary key`
- `source_id`
- `content_version`
- `etag`
- `last_modified`
- `content_length`
- `sha256`
- `http_status`
- `fetched_at`
- `crawl_run_id`
- unique `(source_id, content_version)`

### `discovered_plan`

Stable group plan identity.

- `discovered_plan_id primary key`
- `reporting_entity_name`
- `plan_id`
- `plan_id_type`
- `market_type`
- `plan_name_current`
- `issuer_name`
- `sponsor_name`
- `engine_plan_hash null`
- `reporting_entity_type`
- `states text[]`
- `first_seen`
- `last_seen`
- `status`

Identity rule:

`discovered_plan_id = hash(reporting_entity_name, plan_id, plan_id_type, market_type)`.

Do not include plan name, TOC URL, file URL, source URL, node id, snapshot id, or content version.

### `discovered_plan_alias`

Mutable names/aliases for plan search.

- `alias_id primary key`
- `discovered_plan_id`
- `alias_type`
- `alias_value`
- `source_id`
- `first_seen`
- `last_seen`

### `source_file`

Stable file URL identity.

- `source_file_id primary key`
- `canonical_url unique`
- `domain`
- `content_version`
- `engine_source_identity_hash null`
- `engine_source_file_version_id null`
- `content_length`
- `last_modified`
- `etag`
- `status`
- `first_seen`
- `last_seen`

Identity rule:

`source_file_id = hash(canonical_url)`.

### `discovered_plan_file`

Plan-to-file mapping.

- `discovered_plan_id`
- `source_file_id`
- `source_id`
- `content_version`
- `file_domain`
- `engine_source_catalog_id null`
- `first_seen`
- `last_seen`
- primary key `(discovered_plan_id, source_file_id, content_version)`

### `coverage_metric`

Precomputed counts.

- `source_id primary key`
- `plans_count`
- `files_count`
- `in_network_files_count`
- `allowed_files_count`
- `drug_files_count`
- `total_bytes`
- `latest_file_date`
- `imported_plans_count`
- `imported_files_count`
- `last_crawl_status`
- `updated_at`

### `ptg_plan_subscription`

Client/admin plan subscription.

- `subscription_id primary key`
- `client_id`
- `discovered_plan_id`
- `cadence`
- `enabled bool`
- `last_request_id`
- `last_content_version`
- `next_run_at`
- `created_by`
- `created_at`
- unique `(client_id, discovered_plan_id)`

### `source_file_import`

Shared file-level execution unit.

- `source_file_import_id primary key`
- `source_file_id`
- `content_version`
- `import_month`
- `assigned_node_id`
- `status`
- `engine_run_id`
- `snapshot_id`
- `source_key`
- `engine_source_identity_hash null`
- `engine_source_file_version_id null`
- `created_at`
- `started_at`
- `finished_at`
- `metrics jsonb`
- `error jsonb`
- unique `(source_file_id, content_version, import_month, assigned_node_id)`

### `source_file_import_request`

Requester fan-out record.

- `request_id primary key`
- `source_file_import_id`
- `client_id`
- `subscription_id null`
- `triggered_by`
- `status`
- `quota_policy`
- `created_at`
- `finished_at`
- `error jsonb`

### `ptg_file_placement`

Imported file placement.

- `source_file_id`
- `content_version`
- `import_month`
- `node_id`
- `run_id`
- `snapshot_id`
- `source_key`
- `status`
- `row_count`
- `db_bytes`
- `artifact_bytes`
- `last_served_at`
- primary key `(source_file_id, content_version, import_month, node_id)`

### `ptg_route_index`

Routing lookup for api-layer.

- `route_id primary key`
- `discovered_plan_id`
- `plan_id`
- `plan_id_type`
- `market_type`
- `source_file_id`
- `content_version`
- `source_key`
- `node_id`
- `snapshot_id`
- `priority`
- `status`
- `freshness_at`
- `updated_at`

### Other Control Tables

- `schedule`: importer schedules and next run state.
- `run_mirror`: mirrored engine `import_run` rows.
- `quota_usage`: byte/file/runtime/PTG-slot usage counters.
- `import_lock`: active distributed locks.
- `crawl_run`: crawl execution summary.
- `crawl_source`: ETag/Last-Modified and crawl state per source URL.
- `audit_log`: append-only mutation audit.

## Retention

- Keep run/audit records at least 18 months.
- Keep source catalog history indefinitely unless legal deletion is required.
- Keep raw third-party seed payloads only while license/review state requires them.
- Keep stale route-index rows marked stale until no cached gateway route can reference them.

## Acceptance Criteria

- Stable plan subscriptions survive payer URL reorganization and plan name edits.
- File-level execution can be shared across clients.
- Route lookup is possible without joining engine-local databases.
