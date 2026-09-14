# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Create migration-owned generic custom-import v1 storage.

Revision ID: 20260914120000_custom_import_v1_schema
Revises: 20260907220000_hospital_price_missing_plan

The revision is strictly schema-only.  It creates no dataset, definition,
capture, execution, generation, or pointer rows.
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260914120000_custom_import_v1_schema"
down_revision = "20260907220000_hospital_price_missing_plan"
branch_labels = None
depends_on = None


_DDL_SCHEMA_TOKEN = "mrf."

_TABLE_DDL = (
    """\
CREATE TABLE mrf.custom_import_dataset (
	dataset_id BIGSERIAL NOT NULL,
	dataset_key VARCHAR(63) NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_dataset_pkey PRIMARY KEY (dataset_id),
	CONSTRAINT custom_import_dataset_key UNIQUE (dataset_key),
	CONSTRAINT custom_import_dataset_key_check CHECK (dataset_key ~ '^[a-z][a-z0-9_]{0,62}$')
)
    """,
    """\
CREATE TABLE mrf.custom_import_entity_binding (
	entity_binding_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	adapter_id VARCHAR(63) NOT NULL,
	canonical_value VARCHAR(512) NOT NULL,
	value_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_entity_binding_pkey PRIMARY KEY (entity_binding_id),
	CONSTRAINT custom_import_entity_binding_value_key UNIQUE (dataset_id, adapter_id, canonical_value),
	CONSTRAINT custom_import_entity_binding_owner_key UNIQUE (entity_binding_id, dataset_id),
	CONSTRAINT custom_import_entity_binding_shape_check CHECK (adapter_id ~ '^[a-z][a-z0-9_]{0,62}$' AND octet_length(canonical_value) > 0 AND octet_length(canonical_value) <= 512 AND octet_length(value_sha256) = 32),
	CONSTRAINT custom_import_entity_binding_dataset_fkey FOREIGN KEY(dataset_id) REFERENCES mrf.custom_import_dataset (dataset_id) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_field_slot (
	dataset_id BIGINT NOT NULL,
	field_slot SMALLINT NOT NULL,
	field_id VARCHAR(63) NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_field_slot_pkey PRIMARY KEY (dataset_id, field_slot),
	FOREIGN KEY(dataset_id) REFERENCES mrf.custom_import_dataset (dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_field_slot_shape_check CHECK (field_slot > 0 AND field_slot < 32768 AND field_id ~ '^[a-z][a-z0-9_]{0,62}$'),
	CONSTRAINT custom_import_field_slot_id_key UNIQUE (dataset_id, field_id)
)
    """,
    """\
CREATE TABLE mrf.custom_import_root_record (
	root_record_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	key_contract_sha256 BYTEA NOT NULL,
	canonical_logical_key TEXT NOT NULL,
	logical_key_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_root_record_pkey PRIMARY KEY (root_record_id),
	FOREIGN KEY(dataset_id) REFERENCES mrf.custom_import_dataset (dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_root_record_key UNIQUE (dataset_id, key_contract_sha256, logical_key_sha256),
	CONSTRAINT custom_import_root_record_owner_key UNIQUE (root_record_id, dataset_id),
	CONSTRAINT custom_import_root_record_shape_check CHECK (octet_length(key_contract_sha256) = 32 AND octet_length(logical_key_sha256) = 32)
)
    """,
    """\
CREATE TABLE mrf.custom_import_schema_revision (
	schema_revision_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	revision_number INTEGER NOT NULL,
	canonical_schema TEXT NOT NULL,
	schema_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_schema_rev_pkey PRIMARY KEY (schema_revision_id),
	CONSTRAINT custom_import_schema_rev_hash_key UNIQUE (dataset_id, schema_sha256),
	CONSTRAINT custom_import_schema_rev_number_key UNIQUE (dataset_id, revision_number),
	CONSTRAINT custom_import_schema_rev_owner_key UNIQUE (schema_revision_id, dataset_id),
	FOREIGN KEY(dataset_id) REFERENCES mrf.custom_import_dataset (dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_schema_rev_shape_check CHECK (revision_number > 0 AND octet_length(schema_sha256) = 32)
)
    """,
    """\
CREATE TABLE mrf.custom_import_child_collection (
	schema_revision_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	collection_slot SMALLINT NOT NULL,
	collection_name VARCHAR(63) NOT NULL,
	canonical_key_shape TEXT NOT NULL,
	key_shape_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_child_collection_pkey PRIMARY KEY (schema_revision_id, collection_slot),
	CONSTRAINT custom_import_child_collection_schema_fkey FOREIGN KEY(schema_revision_id, dataset_id) REFERENCES mrf.custom_import_schema_revision (schema_revision_id, dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_child_collection_shape_check CHECK (collection_slot > 0 AND collection_name ~ '^[a-z][a-z0-9_]{0,62}$'),
	CONSTRAINT custom_import_child_collection_name_key UNIQUE (schema_revision_id, collection_name),
	CONSTRAINT custom_import_child_collection_owner_key UNIQUE (schema_revision_id, dataset_id, collection_slot)
)
    """,
    """\
CREATE TABLE mrf.custom_import_definition_revision (
	definition_revision_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	revision_number INTEGER NOT NULL,
	contract_version VARCHAR(32) NOT NULL,
	refresh_mode VARCHAR(16) NOT NULL,
	canonical_definition TEXT NOT NULL,
	definition_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_definition_rev_pkey PRIMARY KEY (definition_revision_id),
	CONSTRAINT custom_import_definition_rev_hash_key UNIQUE (dataset_id, definition_sha256),
	CONSTRAINT custom_import_definition_rev_shape_check CHECK (contract_version = 'custom-import/v1' AND revision_number > 0 AND refresh_mode IN ('upsert', 'snapshot') AND octet_length(definition_sha256) = 32),
	CONSTRAINT custom_import_definition_rev_number_key UNIQUE (dataset_id, revision_number),
	CONSTRAINT custom_import_definition_rev_owner_key UNIQUE (definition_revision_id, dataset_id, schema_revision_id),
	CONSTRAINT custom_import_definition_rev_schema_fkey FOREIGN KEY(schema_revision_id, dataset_id) REFERENCES mrf.custom_import_schema_revision (schema_revision_id, dataset_id) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_field (
	schema_revision_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	field_slot SMALLINT NOT NULL,
	collection_slot SMALLINT DEFAULT 0 NOT NULL,
	field_name VARCHAR(63) NOT NULL,
	field_type VARCHAR(16) NOT NULL,
	is_nullable BOOLEAN NOT NULL,
	projection_slot SMALLINT DEFAULT 0 NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_field_pkey PRIMARY KEY (schema_revision_id, field_slot),
	CONSTRAINT custom_import_field_projection_owner_key UNIQUE (schema_revision_id, dataset_id, field_slot, field_type, collection_slot, projection_slot),
	CONSTRAINT custom_import_field_shape_check CHECK (collection_slot >= 0 AND field_name ~ '^[a-z][a-z0-9_]{0,62}$' AND field_type IN ('string', 'integer', 'decimal', 'boolean', 'date', 'timestamp') AND projection_slot >= 0 AND projection_slot <= 20),
	CONSTRAINT custom_import_field_schema_fkey FOREIGN KEY(schema_revision_id, dataset_id) REFERENCES mrf.custom_import_schema_revision (schema_revision_id, dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_field_owner_key UNIQUE (schema_revision_id, dataset_id, field_slot),
	CONSTRAINT custom_import_field_name_key UNIQUE (schema_revision_id, collection_slot, field_name),
	CONSTRAINT custom_import_field_slot_fkey FOREIGN KEY(dataset_id, field_slot) REFERENCES mrf.custom_import_field_slot (dataset_id, field_slot) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_capture_bundle (
	capture_bundle_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	snapshot_token TEXT NOT NULL,
	snapshot_token_sha256 BYTEA NOT NULL,
	canonical_manifest TEXT NOT NULL,
	manifest_sha256 BYTEA NOT NULL,
	stream_count SMALLINT NOT NULL,
	sealed_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_capture_bundle_pkey PRIMARY KEY (capture_bundle_id),
	CONSTRAINT custom_import_capture_bundle_owner_key UNIQUE (capture_bundle_id, dataset_id, definition_revision_id, schema_revision_id),
	CONSTRAINT custom_import_capture_bundle_shape_check CHECK (octet_length(snapshot_token) > 0 AND stream_count > 0 AND octet_length(snapshot_token_sha256) = 32 AND octet_length(manifest_sha256) = 32),
	CONSTRAINT custom_import_capture_bundle_definition_fkey FOREIGN KEY(definition_revision_id, dataset_id, schema_revision_id) REFERENCES mrf.custom_import_definition_revision (definition_revision_id, dataset_id, schema_revision_id) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_selection_profile (
	definition_revision_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	profile_slot SMALLINT NOT NULL,
	profile_id VARCHAR(63) NOT NULL,
	context_collection_slot SMALLINT,
	canonical_profile TEXT NOT NULL,
	profile_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_selection_profile_pkey PRIMARY KEY (definition_revision_id, profile_slot),
	CONSTRAINT custom_import_selection_profile_definition_fkey FOREIGN KEY(definition_revision_id, dataset_id, schema_revision_id) REFERENCES mrf.custom_import_definition_revision (definition_revision_id, dataset_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_selection_profile_owner_key UNIQUE (definition_revision_id, dataset_id, schema_revision_id, profile_slot),
	CONSTRAINT custom_import_selection_profile_id_key UNIQUE (definition_revision_id, profile_id),
	CONSTRAINT custom_import_selection_profile_shape_check CHECK (profile_slot > 0 AND profile_slot <= 4 AND profile_id ~ '^[a-z][a-z0-9_]{0,62}$' AND octet_length(profile_sha256) = 32),
	CONSTRAINT custom_import_selection_profile_context_fkey FOREIGN KEY(schema_revision_id, dataset_id, context_collection_slot) REFERENCES mrf.custom_import_child_collection (schema_revision_id, dataset_id, collection_slot) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_source_stream (
	definition_revision_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	stream_slot SMALLINT NOT NULL,
	stream_id VARCHAR(63) NOT NULL,
	record_kind VARCHAR(8) NOT NULL,
	collection_slot SMALLINT,
	decoder VARCHAR(16) NOT NULL,
	compression VARCHAR(8) NOT NULL,
	snapshot_token_selector VARCHAR(255) NOT NULL,
	record_path VARCHAR(63),
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_source_stream_pkey PRIMARY KEY (definition_revision_id, stream_slot),
	CONSTRAINT custom_import_source_stream_id_key UNIQUE (definition_revision_id, stream_id),
	CONSTRAINT custom_import_source_stream_collection_fkey FOREIGN KEY(schema_revision_id, dataset_id, collection_slot) REFERENCES mrf.custom_import_child_collection (schema_revision_id, dataset_id, collection_slot) ON DELETE RESTRICT,
	CONSTRAINT custom_import_source_stream_shape_check CHECK (stream_slot > 0 AND stream_id ~ '^[a-z][a-z0-9_]{0,62}$' AND record_kind IN ('root', 'child') AND decoder IN ('csv', 'tsv', 'json', 'ndjson', 'xml', 'parquet') AND compression IN ('none', 'gzip') AND ((record_kind = 'root' AND collection_slot IS NULL) OR (record_kind = 'child' AND collection_slot IS NOT NULL))),
	CONSTRAINT custom_import_source_stream_record_path_check CHECK ((decoder = 'xml' AND record_path IS NOT NULL AND record_path ~ '^[a-z][a-z0-9_]{0,62}$') OR (decoder <> 'xml' AND record_path IS NULL)),
	CONSTRAINT custom_import_source_stream_owner_key UNIQUE (definition_revision_id, dataset_id, schema_revision_id, stream_slot),
	CONSTRAINT custom_import_source_stream_definition_fkey FOREIGN KEY(definition_revision_id, dataset_id, schema_revision_id) REFERENCES mrf.custom_import_definition_revision (definition_revision_id, dataset_id, schema_revision_id) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_capture (
	capture_bundle_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	stream_slot SMALLINT NOT NULL,
	content_sha256 BYTEA NOT NULL,
	byte_count BIGINT NOT NULL,
	canonical_manifest TEXT NOT NULL,
	manifest_sha256 BYTEA NOT NULL,
	sealed_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_capture_pkey PRIMARY KEY (capture_bundle_id, stream_slot),
	CONSTRAINT custom_import_capture_shape_check CHECK (byte_count >= 0 AND octet_length(content_sha256) = 32 AND octet_length(manifest_sha256) = 32),
	CONSTRAINT custom_import_capture_bundle_fkey FOREIGN KEY(capture_bundle_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_capture_bundle (capture_bundle_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_capture_stream_fkey FOREIGN KEY(definition_revision_id, dataset_id, schema_revision_id, stream_slot) REFERENCES mrf.custom_import_source_stream (definition_revision_id, dataset_id, schema_revision_id, stream_slot) ON DELETE RESTRICT,
	CONSTRAINT custom_import_capture_owner_key UNIQUE (capture_bundle_id, definition_revision_id, stream_slot)
)
    """,
    """\
CREATE TABLE mrf.custom_import_execution (
	execution_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	idempotency_key VARCHAR(128) NOT NULL,
	mechanism VARCHAR(16) NOT NULL,
	state VARCHAR(16) NOT NULL,
	capture_bundle_id BIGINT,
	terminal_reason VARCHAR(64),
	started_at TIMESTAMP WITH TIME ZONE,
	finished_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_execution_pkey PRIMARY KEY (execution_id),
	CONSTRAINT custom_import_execution_bundle_key UNIQUE (execution_id, dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id),
	CONSTRAINT custom_import_execution_bundle_fkey FOREIGN KEY(capture_bundle_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_capture_bundle (capture_bundle_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_execution_definition_fkey FOREIGN KEY(definition_revision_id, dataset_id, schema_revision_id) REFERENCES mrf.custom_import_definition_revision (definition_revision_id, dataset_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_execution_request_key UNIQUE (definition_revision_id, idempotency_key),
	CONSTRAINT custom_import_execution_state_check CHECK (mechanism IN ('local', 'queued', 'external') AND state IN ('queued', 'running', 'canceling', 'canceled', 'failed', 'completed', 'no_change')),
	CONSTRAINT custom_import_execution_owner_key UNIQUE (execution_id, dataset_id, definition_revision_id, schema_revision_id)
)
    """,
    """\
CREATE TABLE mrf.custom_import_field_alias (
	definition_revision_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	stream_slot SMALLINT NOT NULL,
	alias_name VARCHAR(255) NOT NULL,
	field_slot SMALLINT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_field_alias_pkey PRIMARY KEY (definition_revision_id, stream_slot, alias_name),
	CONSTRAINT custom_import_field_alias_field_fkey FOREIGN KEY(schema_revision_id, dataset_id, field_slot) REFERENCES mrf.custom_import_field (schema_revision_id, dataset_id, field_slot) ON DELETE RESTRICT,
	CONSTRAINT custom_import_field_alias_shape_check CHECK (octet_length(alias_name) > 0 AND octet_length(alias_name) <= 255 AND alias_name !~ '[[:cntrl:]]'),
	CONSTRAINT custom_import_field_alias_stream_fkey FOREIGN KEY(definition_revision_id, dataset_id, schema_revision_id, stream_slot) REFERENCES mrf.custom_import_source_stream (definition_revision_id, dataset_id, schema_revision_id, stream_slot) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_generation (
	generation_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	execution_id BIGINT NOT NULL,
	capture_bundle_id BIGINT NOT NULL,
	base_generation_id BIGINT,
	base_dataset_id BIGINT,
	source_bundle_sha256 BYTEA NOT NULL,
	generation_sha256 BYTEA NOT NULL,
	root_count BIGINT NOT NULL,
	family_count BIGINT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_generation_pkey PRIMARY KEY (generation_id),
	CONSTRAINT custom_import_generation_execution_key UNIQUE (execution_id),
	CONSTRAINT custom_import_generation_dataset_key UNIQUE (generation_id, dataset_id),
	CONSTRAINT custom_import_generation_owner_key UNIQUE (generation_id, dataset_id, definition_revision_id, schema_revision_id),
	CONSTRAINT custom_import_generation_content_key UNIQUE (dataset_id, generation_sha256),
	CONSTRAINT custom_import_generation_execution_fkey FOREIGN KEY(execution_id, dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id) REFERENCES mrf.custom_import_execution (execution_id, dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_generation_base_fkey FOREIGN KEY(base_generation_id, base_dataset_id) REFERENCES mrf.custom_import_generation (generation_id, dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_generation_shape_check CHECK (root_count >= 0 AND family_count >= 0 AND octet_length(source_bundle_sha256) = 32 AND octet_length(generation_sha256) = 32 AND ((base_generation_id IS NULL AND base_dataset_id IS NULL) OR (base_generation_id IS NOT NULL AND base_dataset_id IS NOT NULL AND base_dataset_id = dataset_id)))
)
    """,
    """\
CREATE TABLE mrf.custom_import_lease (
	execution_id BIGINT NOT NULL,
	fence BIGINT DEFAULT 0 NOT NULL,
	token_sha256 BYTEA,
	heartbeat_at TIMESTAMP WITH TIME ZONE,
	expires_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_lease_pkey PRIMARY KEY (execution_id),
	CONSTRAINT custom_import_lease_shape_check CHECK (fence >= 0 AND ((fence = 0 AND token_sha256 IS NULL AND expires_at IS NULL) OR (fence > 0 AND octet_length(token_sha256) = 32 AND expires_at IS NOT NULL))),
	FOREIGN KEY(execution_id) REFERENCES mrf.custom_import_execution (execution_id) ON DELETE CASCADE
)
    """,
    """\
CREATE TABLE mrf.custom_import_pack (
	pack_id BIGSERIAL NOT NULL,
	execution_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	stream_slot SMALLINT NOT NULL,
	pack_ordinal INTEGER NOT NULL,
	capture_bundle_id BIGINT NOT NULL,
	record_count BIGINT NOT NULL,
	pack_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_pack_pkey PRIMARY KEY (pack_id),
	CONSTRAINT custom_import_pack_execution_fkey FOREIGN KEY(execution_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_execution (execution_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_pack_capture_fkey FOREIGN KEY(capture_bundle_id, definition_revision_id, stream_slot) REFERENCES mrf.custom_import_capture (capture_bundle_id, definition_revision_id, stream_slot) ON DELETE RESTRICT,
	CONSTRAINT custom_import_pack_owner_key UNIQUE (pack_id, dataset_id, definition_revision_id, schema_revision_id),
	CONSTRAINT custom_import_pack_execution_key UNIQUE (execution_id, stream_slot, pack_ordinal),
	CONSTRAINT custom_import_pack_execution_bundle_fkey FOREIGN KEY(execution_id, dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id) REFERENCES mrf.custom_import_execution (execution_id, dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_pack_shape_check CHECK (pack_ordinal >= 0 AND record_count >= 0 AND octet_length(pack_sha256) = 32)
)
    """,
    """\
CREATE TABLE mrf.custom_import_child_revision (
	child_revision_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	root_record_id BIGINT NOT NULL,
	collection_slot SMALLINT NOT NULL,
	pack_id BIGINT NOT NULL,
	source_ordinal BIGINT NOT NULL,
	canonical_parent_key TEXT NOT NULL,
	parent_key_sha256 BYTEA NOT NULL,
	canonical_child_key TEXT NOT NULL,
	child_key_sha256 BYTEA NOT NULL,
	canonical_payload TEXT NOT NULL,
	payload_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_child_revision_pkey PRIMARY KEY (child_revision_id),
	CONSTRAINT custom_import_child_revision_record_fkey FOREIGN KEY(root_record_id, dataset_id) REFERENCES mrf.custom_import_root_record (root_record_id, dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_child_revision_shape_check CHECK (source_ordinal >= 0 AND octet_length(parent_key_sha256) = 32 AND octet_length(child_key_sha256) = 32 AND octet_length(payload_sha256) = 32),
	CONSTRAINT custom_import_child_revision_collection_fkey FOREIGN KEY(schema_revision_id, dataset_id, collection_slot) REFERENCES mrf.custom_import_child_collection (schema_revision_id, dataset_id, collection_slot) ON DELETE RESTRICT,
	CONSTRAINT custom_import_child_revision_pack_fkey FOREIGN KEY(pack_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_pack (pack_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_child_revision_owner_key UNIQUE (child_revision_id, dataset_id, schema_revision_id, root_record_id, collection_slot)
)
    """,
    """\
CREATE TABLE mrf.custom_import_current_generation (
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	generation_id BIGINT NOT NULL,
	pointer_version BIGINT NOT NULL,
	changed_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_current_generation_pkey PRIMARY KEY (dataset_id),
	CONSTRAINT custom_import_current_generation_fkey FOREIGN KEY(generation_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_generation (generation_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_current_generation_version_check CHECK (pointer_version > 0)
)
    """,
    """\
CREATE TABLE mrf.custom_import_publication_event (
	publication_event_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	execution_id BIGINT NOT NULL,
	event_kind VARCHAR(16) NOT NULL,
	from_generation_id BIGINT,
	to_generation_id BIGINT NOT NULL,
	expected_pointer_version BIGINT NOT NULL,
	committed_pointer_version BIGINT NOT NULL,
	canonical_event TEXT NOT NULL,
	event_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_publication_event_pkey PRIMARY KEY (publication_event_id),
	CONSTRAINT custom_import_publication_event_execution_fkey FOREIGN KEY(execution_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_execution (execution_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_publication_event_to_fkey FOREIGN KEY(to_generation_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_generation (generation_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_publication_event_shape_check CHECK (event_kind IN ('activated', 'rolled_back', 'no_change') AND expected_pointer_version >= 0 AND committed_pointer_version >= 0 AND ((event_kind IN ('activated', 'rolled_back') AND committed_pointer_version = expected_pointer_version + 1) OR (event_kind = 'no_change' AND from_generation_id IS NOT NULL AND from_generation_id = to_generation_id AND committed_pointer_version = expected_pointer_version)) AND octet_length(event_sha256) = 32),
	CONSTRAINT custom_import_publication_event_from_fkey FOREIGN KEY(from_generation_id, dataset_id) REFERENCES mrf.custom_import_generation (generation_id, dataset_id) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_rejection (
	execution_id BIGINT NOT NULL,
	rejection_ordinal BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	pack_id BIGINT,
	root_key_sha256 BYTEA,
	canonical_root_key TEXT,
	collection_slot SMALLINT,
	source_ordinal BIGINT,
	code VARCHAR(63) NOT NULL,
	field_slot SMALLINT,
	canonical_evidence TEXT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_rejection_pkey PRIMARY KEY (execution_id, rejection_ordinal),
	CONSTRAINT custom_import_rejection_pack_fkey FOREIGN KEY(pack_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_pack (pack_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_rejection_execution_fkey FOREIGN KEY(execution_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_execution (execution_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_rejection_shape_check CHECK (rejection_ordinal >= 0 AND code ~ '^[a-z][a-z0-9_]{0,62}$' AND (source_ordinal IS NULL OR source_ordinal >= 0) AND (field_slot IS NULL OR field_slot > 0))
)
    """,
    """\
CREATE TABLE mrf.custom_import_root_revision (
	root_revision_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	root_record_id BIGINT NOT NULL,
	pack_id BIGINT NOT NULL,
	source_ordinal BIGINT NOT NULL,
	canonical_payload TEXT NOT NULL,
	payload_sha256 BYTEA NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_root_revision_pkey PRIMARY KEY (root_revision_id),
	CONSTRAINT custom_import_root_revision_owner_key UNIQUE (root_revision_id, dataset_id, schema_revision_id, root_record_id),
	CONSTRAINT custom_import_root_revision_pack_fkey FOREIGN KEY(pack_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_pack (pack_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_root_revision_schema_fkey FOREIGN KEY(schema_revision_id, dataset_id) REFERENCES mrf.custom_import_schema_revision (schema_revision_id, dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_root_revision_record_fkey FOREIGN KEY(root_record_id, dataset_id) REFERENCES mrf.custom_import_root_record (root_record_id, dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_root_revision_shape_check CHECK (source_ordinal >= 0 AND octet_length(payload_sha256) = 32)
)
    """,
    """\
CREATE TABLE mrf.custom_import_child_scalar (
	child_revision_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	root_record_id BIGINT NOT NULL,
	collection_slot SMALLINT NOT NULL,
	field_slot SMALLINT NOT NULL,
	field_collection_slot SMALLINT NOT NULL,
	projection_slot SMALLINT NOT NULL,
	field_type VARCHAR(16) NOT NULL,
	value_state VARCHAR(8) NOT NULL,
	string_value VARCHAR(4096),
	integer_value BIGINT,
	decimal_value NUMERIC(30, 12),
	boolean_value BOOLEAN,
	date_value DATE,
	timestamp_value TIMESTAMP WITH TIME ZONE,
	CONSTRAINT custom_import_child_scalar_pkey PRIMARY KEY (child_revision_id, field_slot),
	CONSTRAINT custom_import_child_scalar_revision_fkey FOREIGN KEY(child_revision_id, dataset_id, schema_revision_id, root_record_id, collection_slot) REFERENCES mrf.custom_import_child_revision (child_revision_id, dataset_id, schema_revision_id, root_record_id, collection_slot) ON DELETE RESTRICT,
	CONSTRAINT custom_import_child_scalar_shape_check CHECK (field_collection_slot = collection_slot AND projection_slot > 0 AND value_state IN ('value', 'null') AND ((value_state = 'value' AND ((field_type = 'string' AND string_value IS NOT NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL) OR (field_type = 'integer' AND string_value IS NULL AND integer_value IS NOT NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL) OR (field_type = 'decimal' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NOT NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL) OR (field_type = 'boolean' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NOT NULL AND date_value IS NULL AND timestamp_value IS NULL) OR (field_type = 'date' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NOT NULL AND timestamp_value IS NULL) OR (field_type = 'timestamp' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NOT NULL))) OR (value_state = 'null' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL))),
	CONSTRAINT custom_import_child_scalar_field_fkey FOREIGN KEY(schema_revision_id, dataset_id, field_slot, field_type, field_collection_slot, projection_slot) REFERENCES mrf.custom_import_field (schema_revision_id, dataset_id, field_slot, field_type, collection_slot, projection_slot) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_family_revision (
	family_revision_id BIGSERIAL NOT NULL,
	dataset_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	root_record_id BIGINT NOT NULL,
	root_revision_id BIGINT NOT NULL,
	entity_binding_id BIGINT NOT NULL,
	family_sha256 BYTEA NOT NULL,
	child_count BIGINT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL,
	CONSTRAINT custom_import_family_revision_pkey PRIMARY KEY (family_revision_id),
	CONSTRAINT custom_import_family_revision_owner_key UNIQUE (family_revision_id, dataset_id, schema_revision_id, root_record_id),
	CONSTRAINT custom_import_family_revision_content_key UNIQUE (dataset_id, schema_revision_id, root_record_id, family_sha256),
	CONSTRAINT custom_import_family_revision_shape_check CHECK (child_count >= 0 AND octet_length(family_sha256) = 32),
	CONSTRAINT custom_import_family_revision_entity_fkey FOREIGN KEY(entity_binding_id, dataset_id) REFERENCES mrf.custom_import_entity_binding (entity_binding_id, dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_family_revision_entity_key UNIQUE (family_revision_id, entity_binding_id),
	CONSTRAINT custom_import_family_revision_root_fkey FOREIGN KEY(root_revision_id, dataset_id, schema_revision_id, root_record_id) REFERENCES mrf.custom_import_root_revision (root_revision_id, dataset_id, schema_revision_id, root_record_id) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_root_scalar (
	root_revision_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	root_record_id BIGINT NOT NULL,
	field_slot SMALLINT NOT NULL,
	field_collection_slot SMALLINT DEFAULT 0 NOT NULL,
	projection_slot SMALLINT NOT NULL,
	field_type VARCHAR(16) NOT NULL,
	value_state VARCHAR(8) NOT NULL,
	string_value VARCHAR(4096),
	integer_value BIGINT,
	decimal_value NUMERIC(30, 12),
	boolean_value BOOLEAN,
	date_value DATE,
	timestamp_value TIMESTAMP WITH TIME ZONE,
	CONSTRAINT custom_import_root_scalar_pkey PRIMARY KEY (root_revision_id, field_slot),
	CONSTRAINT custom_import_root_scalar_revision_fkey FOREIGN KEY(root_revision_id, dataset_id, schema_revision_id, root_record_id) REFERENCES mrf.custom_import_root_revision (root_revision_id, dataset_id, schema_revision_id, root_record_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_root_scalar_field_fkey FOREIGN KEY(schema_revision_id, dataset_id, field_slot, field_type, field_collection_slot, projection_slot) REFERENCES mrf.custom_import_field (schema_revision_id, dataset_id, field_slot, field_type, collection_slot, projection_slot) ON DELETE RESTRICT,
	CONSTRAINT custom_import_root_scalar_shape_check CHECK (field_collection_slot = 0 AND projection_slot > 0 AND value_state IN ('value', 'null') AND ((value_state = 'value' AND ((field_type = 'string' AND string_value IS NOT NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL) OR (field_type = 'integer' AND string_value IS NULL AND integer_value IS NOT NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL) OR (field_type = 'decimal' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NOT NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL) OR (field_type = 'boolean' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NOT NULL AND date_value IS NULL AND timestamp_value IS NULL) OR (field_type = 'date' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NOT NULL AND timestamp_value IS NULL) OR (field_type = 'timestamp' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NOT NULL))) OR (value_state = 'null' AND string_value IS NULL AND integer_value IS NULL AND decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL)))
)
    """,
    """\
CREATE TABLE mrf.custom_import_family_child (
	family_revision_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	root_record_id BIGINT NOT NULL,
	collection_slot SMALLINT NOT NULL,
	child_revision_id BIGINT NOT NULL,
	CONSTRAINT custom_import_family_child_pkey PRIMARY KEY (family_revision_id, collection_slot, child_revision_id),
	CONSTRAINT custom_import_family_child_family_fkey FOREIGN KEY(family_revision_id, dataset_id, schema_revision_id, root_record_id) REFERENCES mrf.custom_import_family_revision (family_revision_id, dataset_id, schema_revision_id, root_record_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_family_child_revision_fkey FOREIGN KEY(child_revision_id, dataset_id, schema_revision_id, root_record_id, collection_slot) REFERENCES mrf.custom_import_child_revision (child_revision_id, dataset_id, schema_revision_id, root_record_id, collection_slot) ON DELETE RESTRICT
)
    """,
    """\
CREATE TABLE mrf.custom_import_generation_family (
	generation_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	root_record_id BIGINT NOT NULL,
	family_revision_id BIGINT NOT NULL,
	CONSTRAINT custom_import_generation_family_pkey PRIMARY KEY (generation_id, root_record_id),
	CONSTRAINT custom_import_generation_family_family_fkey FOREIGN KEY(family_revision_id, dataset_id, schema_revision_id, root_record_id) REFERENCES mrf.custom_import_family_revision (family_revision_id, dataset_id, schema_revision_id, root_record_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_generation_family_generation_fkey FOREIGN KEY(generation_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_generation (generation_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_generation_family_member_key UNIQUE (generation_id, dataset_id, family_revision_id)
)
    """,
    """\
CREATE TABLE mrf.custom_import_winner (
	generation_id BIGINT NOT NULL,
	dataset_id BIGINT NOT NULL,
	definition_revision_id BIGINT NOT NULL,
	schema_revision_id BIGINT NOT NULL,
	profile_slot SMALLINT NOT NULL,
	entity_binding_id BIGINT NOT NULL,
	family_revision_id BIGINT NOT NULL,
	context_collection_slot SMALLINT DEFAULT 0 NOT NULL,
	context_key_sha256 BYTEA NOT NULL,
	context_child_revision_id BIGINT,
	CONSTRAINT custom_import_winner_pkey PRIMARY KEY (generation_id, profile_slot, entity_binding_id, context_key_sha256),
	CONSTRAINT custom_import_winner_profile_fkey FOREIGN KEY(definition_revision_id, dataset_id, schema_revision_id, profile_slot) REFERENCES mrf.custom_import_selection_profile (definition_revision_id, dataset_id, schema_revision_id, profile_slot) ON DELETE RESTRICT,
	CONSTRAINT custom_import_winner_binding_fkey FOREIGN KEY(entity_binding_id, dataset_id) REFERENCES mrf.custom_import_entity_binding (entity_binding_id, dataset_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_winner_family_fkey FOREIGN KEY(generation_id, dataset_id, family_revision_id) REFERENCES mrf.custom_import_generation_family (generation_id, dataset_id, family_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_winner_context_check CHECK (octet_length(context_key_sha256) = 32 AND ((context_collection_slot = 0 AND context_child_revision_id IS NULL) OR (context_collection_slot > 0 AND context_child_revision_id IS NOT NULL))),
	CONSTRAINT custom_import_winner_generation_fkey FOREIGN KEY(generation_id, dataset_id, definition_revision_id, schema_revision_id) REFERENCES mrf.custom_import_generation (generation_id, dataset_id, definition_revision_id, schema_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_winner_context_fkey FOREIGN KEY(family_revision_id, context_collection_slot, context_child_revision_id) REFERENCES mrf.custom_import_family_child (family_revision_id, collection_slot, child_revision_id) ON DELETE RESTRICT,
	CONSTRAINT custom_import_winner_family_entity_fkey FOREIGN KEY(family_revision_id, entity_binding_id) REFERENCES mrf.custom_import_family_revision (family_revision_id, entity_binding_id) ON DELETE RESTRICT
)
    """,
)

_INDEX_DDL = (
    """\
CREATE INDEX custom_import_capture_content_idx ON mrf.custom_import_capture (content_sha256)
    """,
    """\
CREATE INDEX custom_import_rejection_execution_idx ON mrf.custom_import_rejection (execution_id, code)
    """,
    """\
CREATE INDEX custom_import_root_scalar_text_idx ON mrf.custom_import_root_scalar (schema_revision_id, field_slot, string_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_root_scalar_int_idx ON mrf.custom_import_root_scalar (schema_revision_id, field_slot, integer_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_root_scalar_number_idx ON mrf.custom_import_root_scalar (schema_revision_id, field_slot, decimal_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_root_scalar_date_idx ON mrf.custom_import_root_scalar (schema_revision_id, field_slot, date_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_root_scalar_time_idx ON mrf.custom_import_root_scalar (schema_revision_id, field_slot, timestamp_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_child_scalar_text_idx ON mrf.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, string_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_child_scalar_int_idx ON mrf.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, integer_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_child_scalar_number_idx ON mrf.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, decimal_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_child_scalar_date_idx ON mrf.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, date_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_child_scalar_time_idx ON mrf.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, timestamp_value) WHERE value_state = 'value'
    """,
    """\
CREATE INDEX custom_import_winner_lookup_idx ON mrf.custom_import_winner (generation_id, profile_slot, entity_binding_id)
    """,
)

_TABLE_NAMES = (
    'custom_import_dataset',
    'custom_import_schema_revision',
    'custom_import_field_slot',
    'custom_import_child_collection',
    'custom_import_field',
    'custom_import_definition_revision',
    'custom_import_source_stream',
    'custom_import_field_alias',
    'custom_import_selection_profile',
    'custom_import_execution',
    'custom_import_lease',
    'custom_import_capture_bundle',
    'custom_import_capture',
    'custom_import_pack',
    'custom_import_rejection',
    'custom_import_root_record',
    'custom_import_root_revision',
    'custom_import_child_revision',
    'custom_import_family_revision',
    'custom_import_family_child',
    'custom_import_generation',
    'custom_import_generation_family',
    'custom_import_root_scalar',
    'custom_import_child_scalar',
    'custom_import_entity_binding',
    'custom_import_winner',
    'custom_import_current_generation',
    'custom_import_publication_event',
)

_TABLE_CREATION_ORDER = (
    "custom_import_dataset",
    "custom_import_entity_binding",
    "custom_import_field_slot",
    "custom_import_root_record",
    "custom_import_schema_revision",
    "custom_import_child_collection",
    "custom_import_definition_revision",
    "custom_import_field",
    "custom_import_capture_bundle",
    "custom_import_selection_profile",
    "custom_import_source_stream",
    "custom_import_capture",
    "custom_import_execution",
    "custom_import_field_alias",
    "custom_import_generation",
    "custom_import_lease",
    "custom_import_pack",
    "custom_import_child_revision",
    "custom_import_current_generation",
    "custom_import_publication_event",
    "custom_import_rejection",
    "custom_import_root_revision",
    "custom_import_child_scalar",
    "custom_import_family_revision",
    "custom_import_root_scalar",
    "custom_import_family_child",
    "custom_import_generation_family",
    "custom_import_winner",
)

_IMMUTABLE_TABLES = tuple(
    table_name
    for table_name in _TABLE_NAMES
    if table_name
    not in {
        "custom_import_execution",
        "custom_import_lease",
        "custom_import_current_generation",
    }
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _in_schema(statement: str, schema: str) -> str:
    """Bind revision-owned DDL to the configured target schema."""

    return statement.replace(_DDL_SCHEMA_TOKEN, f"{_quote(schema)}.")


def _immutable_function_sql(schema: str) -> str:
    qualified = f"{_quote(schema)}.guard_custom_import_immutable_row"
    return f"""
    CREATE FUNCTION {qualified}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    BEGIN
        RAISE EXCEPTION 'custom_import_immutable_row'
            USING ERRCODE = 'P0001';
    END;
    $function$;
    """


def _revoke_immutable_function_sql(schema: str) -> str:
    """Return the function permission statement as a separate operation."""

    qualified = f"{_quote(schema)}.guard_custom_import_immutable_row"
    return f"REVOKE ALL ON FUNCTION {qualified}() FROM PUBLIC"


def _immutable_trigger_sql(schema: str, table_name: str) -> str:
    qualified_table = f"{_quote(schema)}.{_quote(table_name)}"
    qualified_function = f"{_quote(schema)}.guard_custom_import_immutable_row()"
    trigger = _quote(f"{table_name}_immutable_row_guard")
    return f"""
    CREATE TRIGGER {trigger}
    BEFORE UPDATE OR DELETE ON {qualified_table}
    FOR EACH ROW EXECUTE FUNCTION {qualified_function};
    """


def _drop_table_sql(schema: str, table_name: str) -> str:
    return f"DROP TABLE IF EXISTS {_quote(schema)}.{_quote(table_name)}"


def _drop_immutable_function_sql(schema: str) -> str:
    qualified = f"{_quote(schema)}.guard_custom_import_immutable_row"
    return f"DROP FUNCTION IF EXISTS {qualified}()"


def upgrade() -> None:
    """Install the v1 schema, indexes, and immutable-content guards."""

    schema = _schema()
    for statement in _TABLE_DDL:
        op.execute(_in_schema(statement, schema))
    for statement in _INDEX_DDL:
        op.execute(_in_schema(statement, schema))
    # asyncpg prepares each Alembic operation independently and rejects a
    # string containing more than one command.  Keep creation and revocation
    # as distinct operations so the migration works with the runtime driver.
    op.execute(_immutable_function_sql(schema))
    op.execute(_revoke_immutable_function_sql(schema))
    for table_name in _IMMUTABLE_TABLES:
        op.execute(_immutable_trigger_sql(schema, table_name))


def downgrade() -> None:
    """Remove only the v1 relations, in reverse dependency order."""

    schema = _schema()
    for table_name in reversed(_TABLE_CREATION_ORDER):
        op.execute(_drop_table_sql(schema, table_name))
    op.execute(_drop_immutable_function_sql(schema))
