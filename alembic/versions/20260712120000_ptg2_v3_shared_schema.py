"""Add fixed shared_blocks_v3 storage for postgres_binary_v3.

Revision ID: 20260712120000_ptg2_v3_shared_schema
Revises: 20260713236000_provider_directory_dataset_insurance_plan
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260712120000_ptg2_v3_shared_schema"
down_revision = "20260713236000_provider_directory_dataset_insurance_plan"
branch_labels = None
depends_on = None


_PARTITION_COUNT = 32


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _create_hash_partitions(schema: str, parent: str) -> None:
    for remainder in range(_PARTITION_COUNT):
        child = f"{parent}_p{remainder:02d}"
        op.execute(
            f"""
            CREATE TABLE {_qt(schema, child)}
                PARTITION OF {_qt(schema, parent)}
                FOR VALUES WITH (
                    MODULUS {_PARTITION_COUNT},
                    REMAINDER {remainder}
                );
            """
        )


def _execute_ddl(*statements: str) -> None:
    for statement in statements:
        op.execute(statement)


def _create_runtime_control_prerequisites(schema: str) -> None:
    """Make a baseline-stamped database usable before application startup."""
    _execute_ddl(
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "import_run")} (
            run_id varchar(64) NOT NULL,
            engine varchar(64) NOT NULL,
            node_id varchar(64),
            importer varchar(64) NOT NULL,
            family varchar(64),
            status varchar(32) NOT NULL,
            phase_detail varchar(128),
            params json,
            idempotency_key varchar(160),
            triggered_by varchar(32),
            schedule_id varchar(64),
            subscription_id varchar(64),
            source_file_import_id varchar(64),
            created_at timestamp without time zone,
            started_at timestamp without time zone,
            finished_at timestamp without time zone,
            heartbeat_at timestamp without time zone,
            progress json,
            metrics json,
            error json,
            snapshot_id varchar(96),
            import_id varchar(64),
            retry_of_run_id varchar(64),
            CONSTRAINT {_q("import_run_pkey")} PRIMARY KEY (run_id)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("import_run_status_heartbeat_idx")}
            ON {_qt(schema, "import_run")} (status, heartbeat_at)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("import_run_importer_created_idx")}
            ON {_qt(schema, "import_run")} (importer, created_at)
        """,
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {_q("import_run_active_idempotency_idx")}
            ON {_qt(schema, "import_run")} (idempotency_key)
            WHERE status IN (
                'queued', 'starting', 'running', 'finalizing', 'canceling'
            )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("import_run_schedule_idx")}
            ON {_qt(schema, "import_run")} (schedule_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("import_run_subscription_idx")}
            ON {_qt(schema, "import_run")} (subscription_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("import_run_source_file_import_idx")}
            ON {_qt(schema, "import_run")} (source_file_import_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_import_run")} (
            import_run_id varchar(96) NOT NULL,
            import_month date,
            status varchar(32),
            started_at timestamp without time zone,
            finished_at timestamp without time zone,
            heartbeat_at timestamp without time zone,
            options json,
            report json,
            error text,
            CONSTRAINT {_q("ptg2_import_run_pkey")} PRIMARY KEY (import_run_id)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_import_run_status_idx")}
            ON {_qt(schema, "ptg2_import_run")} (status)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_import_run_month_idx")}
            ON {_qt(schema, "ptg2_import_run")} (import_month)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_import_run_status_heartbeat_idx")}
            ON {_qt(schema, "ptg2_import_run")} (status, heartbeat_at)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "code_catalog")} (
            code_system varchar(32) NOT NULL,
            code varchar(128) NOT NULL,
            code_type varchar(32),
            display_name varchar,
            short_description varchar,
            long_description text,
            is_active boolean,
            source varchar(128),
            source_release varchar(64),
            source_attribution text,
            updated_at timestamp without time zone,
            CONSTRAINT {_q("code_catalog_pkey")} PRIMARY KEY (code_system, code)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("code_catalog_code_system_idx")}
            ON {_qt(schema, "code_catalog")} (code, code_system)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("code_catalog_system_display_idx")}
            ON {_qt(schema, "code_catalog")} (code_system, display_name)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("code_catalog_system_display_lower_idx")}
            ON {_qt(schema, "code_catalog")} (code_system, lower(display_name))
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("code_catalog_code_type_system_idx")}
            ON {_qt(schema, "code_catalog")} (code_type, code_system)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("code_catalog_display_lower_idx")}
            ON {_qt(schema, "code_catalog")} (lower(display_name))
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("code_catalog_short_description_lower_idx")}
            ON {_qt(schema, "code_catalog")} (lower(short_description))
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("code_catalog_source_idx")}
            ON {_qt(schema, "code_catalog")} (source)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("code_catalog_system_source_idx")}
            ON {_qt(schema, "code_catalog")} (code_system, source)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("code_catalog_source_system_display_lower_idx")}
            ON {_qt(schema, "code_catalog")}
            (source, code_system, lower(display_name))
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_current_snapshot")} (
            slot varchar(32) NOT NULL,
            snapshot_id varchar(96),
            previous_snapshot_id varchar(96),
            updated_at timestamp without time zone,
            CONSTRAINT {_q("ptg2_current_snapshot_pkey")} PRIMARY KEY (slot)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_current_source_snapshot")} (
            source_key varchar(96) NOT NULL,
            snapshot_id varchar(96),
            previous_snapshot_id varchar(96),
            import_month date,
            updated_at timestamp without time zone,
            CONSTRAINT {_q("ptg2_current_source_snapshot_pkey")}
                PRIMARY KEY (source_key)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_current_source_snapshot_idx")}
            ON {_qt(schema, "ptg2_current_source_snapshot")} (snapshot_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_current_source_month_idx")}
            ON {_qt(schema, "ptg2_current_source_snapshot")} (import_month)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_current_plan_source")} (
            plan_source_key varchar(96) NOT NULL,
            plan_id varchar(64),
            plan_market_type varchar(32),
            import_month date,
            source_key varchar(96),
            snapshot_id varchar(96),
            previous_snapshot_id varchar(96),
            updated_at timestamp without time zone,
            CONSTRAINT {_q("ptg2_current_plan_source_pkey")}
                PRIMARY KEY (plan_source_key)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_current_plan_source_plan_idx")}
            ON {_qt(schema, "ptg2_current_plan_source")} (plan_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_current_plan_source_lookup_idx")}
            ON {_qt(schema, "ptg2_current_plan_source")}
            (plan_id, plan_market_type, import_month)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_current_plan_source_source_idx")}
            ON {_qt(schema, "ptg2_current_plan_source")} (source_key)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_current_plan_source_snapshot_idx")}
            ON {_qt(schema, "ptg2_current_plan_source")} (snapshot_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_plan")} (
            plan_hash varchar(64) NOT NULL,
            hash_prefix varchar(16),
            plan_id varchar(64),
            plan_id_type varchar(32),
            plan_name varchar,
            plan_market_type varchar(32),
            issuer_name varchar,
            plan_sponsor_name varchar,
            canonical_payload json,
            created_at timestamp without time zone,
            CONSTRAINT {_q("ptg2_plan_pkey")} PRIMARY KEY (plan_hash)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_plan_prefix_idx")}
            ON {_qt(schema, "ptg2_plan")} (hash_prefix)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_plan_id_idx")}
            ON {_qt(schema, "ptg2_plan")} (plan_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_plan_name_idx")}
            ON {_qt(schema, "ptg2_plan")} (plan_name)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_plan_month")} (
            plan_month_id varchar(96) NOT NULL,
            snapshot_id varchar(96),
            plan_hash varchar(64),
            import_month date,
            created_at timestamp without time zone,
            CONSTRAINT {_q("ptg2_plan_month_pkey")} PRIMARY KEY (plan_month_id)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_plan_month_snapshot_idx")}
            ON {_qt(schema, "ptg2_plan_month")} (snapshot_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_plan_month_plan_idx")}
            ON {_qt(schema, "ptg2_plan_month")} (plan_hash, import_month)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_artifact_manifest")} (
            artifact_id varchar(96) NOT NULL,
            snapshot_id varchar(96),
            import_run_id varchar(96),
            artifact_kind varchar(64),
            storage_uri varchar,
            sha256 varchar(64),
            byte_count bigint,
            payload json,
            created_at timestamp without time zone,
            CONSTRAINT {_q("ptg2_artifact_manifest_pkey")} PRIMARY KEY (artifact_id)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_artifact_kind_idx")}
            ON {_qt(schema, "ptg2_artifact_manifest")} (artifact_kind)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_artifact_snapshot_idx")}
            ON {_qt(schema, "ptg2_artifact_manifest")} (snapshot_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_artifact_sha_idx")}
            ON {_qt(schema, "ptg2_artifact_manifest")} (sha256)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_source_trace")} (
            source_trace_hash varchar(64) NOT NULL,
            source_file_version_id varchar(96),
            original_url varchar,
            canonical_url varchar,
            json_pointer varchar,
            line_number integer,
            created_at timestamp without time zone,
            CONSTRAINT {_q("ptg2_source_trace_pkey")}
                PRIMARY KEY (source_trace_hash)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_source_trace_file_idx")}
            ON {_qt(schema, "ptg2_source_trace")} (source_file_version_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_source_trace_set")} (
            source_trace_set_hash varchar(64) NOT NULL,
            source_trace_hashes varchar[],
            created_at timestamp without time zone,
            CONSTRAINT {_q("ptg2_source_trace_set_pkey")}
                PRIMARY KEY (source_trace_set_hash)
        )
        """,
    )


def upgrade():
    schema = _schema()

    _create_runtime_control_prerequisites(schema)

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "ptg2_snapshot")} (
            snapshot_id varchar(96) NOT NULL,
            import_run_id varchar(96),
            import_month date,
            status varchar(32),
            created_at timestamp without time zone,
            validated_at timestamp without time zone,
            published_at timestamp without time zone,
            previous_snapshot_id varchar(96),
            manifest json,
            CONSTRAINT {_q("ptg2_snapshot_pkey")} PRIMARY KEY (snapshot_id)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_snapshot_status_idx")}
            ON {_qt(schema, "ptg2_snapshot")} (status);
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {_q("ptg2_snapshot_month_idx")}
            ON {_qt(schema, "ptg2_snapshot")} (import_month);
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_snapshot_layout")} (
            snapshot_key bigint GENERATED BY DEFAULT AS IDENTITY,
            storage_shard_id smallint NOT NULL DEFAULT 0,
            build_token varchar(96) NOT NULL,
            generation varchar(32) NOT NULL,
            state varchar(16) NOT NULL,
            mapping_digest bytea,
            support_digest bytea,
            layout_manifest jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            logical_byte_count bigint NOT NULL DEFAULT 0,
            created_at timestamptz NOT NULL DEFAULT now(),
            heartbeat_at timestamptz NOT NULL DEFAULT now(),
            lease_until timestamptz,
            published_at timestamptz,
            CONSTRAINT {_q("ptg2_v3_snapshot_layout_pkey")}
                PRIMARY KEY (snapshot_key),
            CONSTRAINT {_q("ptg2_v3_snapshot_layout_state_check")}
                CHECK (state IN ('building', 'sealed')),
            CONSTRAINT {_q("ptg2_v3_snapshot_layout_mapping_digest_check")}
                CHECK (mapping_digest IS NULL OR octet_length(mapping_digest) = 32),
            CONSTRAINT {_q("ptg2_v3_snapshot_layout_support_digest_check")}
                CHECK (support_digest IS NULL OR octet_length(support_digest) = 32),
            CONSTRAINT {_q("ptg2_v3_snapshot_layout_logical_byte_count_check")}
                CHECK (logical_byte_count >= 0)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_snapshot_layout_state_idx")}
            ON {_qt(schema, "ptg2_v3_snapshot_layout")}
            (state, lease_until, heartbeat_at);
        """
    )
    op.execute(
        f"""
        CREATE UNIQUE INDEX {_q("ptg2_v3_snapshot_layout_sealed_mapping_idx")}
            ON {_qt(schema, "ptg2_v3_snapshot_layout")}
            (generation, mapping_digest, support_digest)
            WHERE state = 'sealed'
              AND mapping_digest IS NOT NULL
              AND support_digest IS NOT NULL;
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_layout_fingerprint")} (
            semantic_fingerprint bytea NOT NULL,
            snapshot_key bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q("ptg2_v3_layout_fingerprint_pkey")}
                PRIMARY KEY (semantic_fingerprint),
            CONSTRAINT {_q("ptg2_v3_layout_fingerprint_snapshot_key_fkey")}
                FOREIGN KEY (snapshot_key)
                REFERENCES {_qt(schema, "ptg2_v3_snapshot_layout")} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q("ptg2_v3_layout_fingerprint_digest_check")}
                CHECK (octet_length(semantic_fingerprint) = 32)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_layout_fingerprint_snapshot_key_idx")}
            ON {_qt(schema, "ptg2_v3_layout_fingerprint")} (snapshot_key);
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_snapshot_binding")} (
            snapshot_id varchar(96) NOT NULL,
            snapshot_key bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q("ptg2_v3_snapshot_binding_pkey")}
                PRIMARY KEY (snapshot_id),
            CONSTRAINT {_q("ptg2_v3_snapshot_binding_snapshot_id_fkey")}
                FOREIGN KEY (snapshot_id)
                REFERENCES {_qt(schema, "ptg2_snapshot")} (snapshot_id)
                ON DELETE CASCADE,
            CONSTRAINT {_q("ptg2_v3_snapshot_binding_snapshot_key_fkey")}
                FOREIGN KEY (snapshot_key)
                REFERENCES {_qt(schema, "ptg2_v3_snapshot_layout")} (snapshot_key)
                ON DELETE RESTRICT
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_snapshot_binding_snapshot_key_idx")}
            ON {_qt(schema, "ptg2_v3_snapshot_binding")} (snapshot_key);
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_snapshot_scope")} (
            snapshot_id varchar(96) NOT NULL,
            plan_id varchar(64) NOT NULL,
            plan_market_type varchar(32) NOT NULL DEFAULT '',
            coverage_scope_id bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q("ptg2_v3_snapshot_scope_pkey")}
                PRIMARY KEY (snapshot_id),
            CONSTRAINT {_q("ptg2_v3_snapshot_scope_snapshot_id_fkey")}
                FOREIGN KEY (snapshot_id)
                REFERENCES {_qt(schema, "ptg2_snapshot")} (snapshot_id)
                ON DELETE CASCADE,
            CONSTRAINT {_q("ptg2_v3_snapshot_scope_coverage_scope_id_check")}
                CHECK (octet_length(coverage_scope_id) = 32)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_snapshot_scope_lookup_idx")}
            ON {_qt(schema, "ptg2_v3_snapshot_scope")}
            (snapshot_id, coverage_scope_id);
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_snapshot_source")} (
            snapshot_id varchar(96) NOT NULL,
            source_key integer NOT NULL,
            source_type varchar(32) NOT NULL,
            identity_kind varchar(64) NOT NULL,
            identity_sha256 varchar(64) NOT NULL,
            raw_container_sha256 varchar(64) NOT NULL,
            logical_json_sha256 varchar(64),
            logical_hash_deferred boolean NOT NULL,
            source_trace_set_hash varchar(64) NOT NULL,
            CONSTRAINT {_q("ptg2_v3_snapshot_source_pkey")}
                PRIMARY KEY (snapshot_id, source_key),
            CONSTRAINT {_q("ptg2_v3_snapshot_source_identity_key")}
                UNIQUE (snapshot_id, source_type, identity_kind, identity_sha256),
            CONSTRAINT {_q("ptg2_v3_snapshot_source_snapshot_id_fkey")}
                FOREIGN KEY (snapshot_id)
                REFERENCES {_qt(schema, "ptg2_v3_snapshot_scope")} (snapshot_id)
                ON DELETE CASCADE,
            CONSTRAINT {_q("ptg2_v3_snapshot_source_trace_set_hash_fkey")}
                FOREIGN KEY (source_trace_set_hash)
                REFERENCES {_qt(schema, "ptg2_source_trace_set")} (source_trace_set_hash)
                ON DELETE RESTRICT,
            CONSTRAINT {_q("ptg2_v3_snapshot_source_source_key_check")}
                CHECK (source_key >= 0),
            CONSTRAINT {_q("ptg2_v3_snapshot_source_source_type_check")}
                CHECK (source_type <> ''),
            CONSTRAINT {_q("ptg2_v3_snapshot_source_identity_kind_check")}
                CHECK (identity_kind <> ''),
            CONSTRAINT {_q("ptg2_v3_snapshot_source_identity_sha256_check")}
                CHECK (identity_sha256 ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT {_q("ptg2_v3_snapshot_source_raw_sha256_check")}
                CHECK (raw_container_sha256 ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT {_q("ptg2_v3_snapshot_source_logical_sha256_check")}
                CHECK (
                    (logical_hash_deferred AND logical_json_sha256 IS NULL)
                    OR (
                        NOT logical_hash_deferred
                        AND logical_json_sha256 ~ '^[0-9a-f]{{64}}$'
                    )
                ),
            CONSTRAINT {_q("ptg2_v3_snapshot_source_identity_evidence_check")}
                CHECK (
                    (
                        identity_kind = 'logical_json_sha256_v1'
                        AND NOT logical_hash_deferred
                        AND logical_json_sha256 = identity_sha256
                    )
                    OR (
                        identity_kind = 'raw_container_sha256_v1'
                        AND logical_hash_deferred
                        AND raw_container_sha256 = identity_sha256
                    )
                ),
            CONSTRAINT {_q("ptg2_v3_snapshot_source_trace_set_hash_check")}
                CHECK (source_trace_set_hash ~ '^[0-9a-f]{{64}}$')
        );
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_block")} (
            block_hash bytea NOT NULL,
            format_version smallint NOT NULL,
            object_kind varchar(64) NOT NULL,
            codec varchar(16) NOT NULL,
            entry_count bigint NOT NULL,
            raw_byte_count bigint NOT NULL,
            stored_byte_count bigint NOT NULL,
            payload bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q("ptg2_v3_block_pkey")}
                PRIMARY KEY (block_hash),
            CONSTRAINT {_q("ptg2_v3_block_hash_check")}
                CHECK (octet_length(block_hash) = 32),
            CONSTRAINT {_q("ptg2_v3_block_format_version_check")}
                CHECK (format_version = 2),
            CONSTRAINT {_q("ptg2_v3_block_codec_check")}
                CHECK (codec IN ('none', 'zlib')),
            CONSTRAINT {_q("ptg2_v3_block_entry_count_check")}
                CHECK (entry_count >= 0),
            CONSTRAINT {_q("ptg2_v3_block_raw_byte_count_check")}
                CHECK (raw_byte_count >= 0),
            CONSTRAINT {_q("ptg2_v3_block_stored_byte_count_check")}
                CHECK (stored_byte_count >= 0),
            CONSTRAINT {_q("ptg2_v3_block_payload_size_check")}
                CHECK (octet_length(payload) = stored_byte_count)
        ) PARTITION BY HASH (block_hash);
        """
    )
    _create_hash_partitions(schema, "ptg2_v3_block")

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_snapshot_block")} (
            snapshot_key bigint NOT NULL,
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL,
            fragment_no integer NOT NULL,
            entry_count bigint NOT NULL,
            block_hash bytea NOT NULL,
            CONSTRAINT {_q("ptg2_v3_snapshot_block_pkey")}
                PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no),
            CONSTRAINT {_q("ptg2_v3_snapshot_block_snapshot_key_fkey")}
                FOREIGN KEY (snapshot_key)
                REFERENCES {_qt(schema, "ptg2_v3_snapshot_layout")} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q("ptg2_v3_snapshot_block_block_hash_fkey")}
                FOREIGN KEY (block_hash)
                REFERENCES {_qt(schema, "ptg2_v3_block")} (block_hash),
            CONSTRAINT {_q("ptg2_v3_snapshot_block_fragment_no_check")}
                CHECK (fragment_no >= 0),
            CONSTRAINT {_q("ptg2_v3_snapshot_block_block_key_check")}
                CHECK (block_key >= 0),
            CONSTRAINT {_q("ptg2_v3_snapshot_block_entry_count_check")}
                CHECK (entry_count >= 0)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_snapshot_block_block_hash_idx")}
            ON {_qt(schema, "ptg2_v3_snapshot_block")} (block_hash);
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_snapshot_block_lookup_idx")}
            ON {_qt(schema, "ptg2_v3_snapshot_block")}
            (snapshot_key, object_kind, block_key);
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_graph_owner")} (
            snapshot_key bigint NOT NULL,
            direction smallint NOT NULL,
            owner_key bigint NOT NULL,
            first_chunk integer NOT NULL,
            member_offset integer NOT NULL,
            member_count bigint NOT NULL,
            CONSTRAINT {_q("ptg2_v3_graph_owner_pkey")}
                PRIMARY KEY (snapshot_key, direction, owner_key),
            CONSTRAINT {_q("ptg2_v3_graph_owner_direction_check")}
                CHECK (direction BETWEEN 1 AND 4),
            CONSTRAINT {_q("ptg2_v3_graph_owner_first_chunk_check")}
                CHECK (first_chunk >= 0),
            CONSTRAINT {_q("ptg2_v3_graph_owner_member_offset_check")}
                CHECK (member_offset >= 0 AND member_offset < 65536),
            CONSTRAINT {_q("ptg2_v3_graph_owner_member_count_check")}
                CHECK (member_count >= 0)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_graph_owner_lookup_idx")}
            ON {_qt(schema, "ptg2_v3_graph_owner")}
            (snapshot_key, direction, owner_key)
            INCLUDE (first_chunk, member_offset, member_count);
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_code")} (
            snapshot_key bigint NOT NULL,
            code_key integer NOT NULL,
            code_global_id_128 bytea NOT NULL,
            coverage_scope_id bytea NOT NULL,
            reported_code_system text,
            reported_code text,
            negotiation_arrangement text,
            billing_code_type_version text,
            source_name text,
            source_description text,
            rate_count bigint NOT NULL,
            CONSTRAINT {_q("ptg2_v3_code_pkey")}
                PRIMARY KEY (snapshot_key, code_key),
            CONSTRAINT {_q("ptg2_v3_code_identity_key")}
                UNIQUE NULLS NOT DISTINCT (
                    snapshot_key,
                    coverage_scope_id,
                    reported_code_system,
                    reported_code,
                    negotiation_arrangement,
                    billing_code_type_version,
                    source_name,
                    source_description
                ),
            CONSTRAINT {_q("ptg2_v3_code_global_id_key")}
                UNIQUE (snapshot_key, code_global_id_128),
            CONSTRAINT {_q("ptg2_v3_code_global_id_check")}
                CHECK (octet_length(code_global_id_128) = 16),
            CONSTRAINT {_q("ptg2_v3_code_coverage_scope_id_check")}
                CHECK (octet_length(coverage_scope_id) = 32),
            CONSTRAINT {_q("ptg2_v3_code_rate_count_check")}
                CHECK (rate_count >= 0),
            CONSTRAINT {_q("ptg2_v3_code_code_key_check")}
                CHECK (code_key >= 0)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_code_lookup_idx")}
            ON {_qt(schema, "ptg2_v3_code")}
            (snapshot_key, coverage_scope_id, reported_code_system, reported_code)
            INCLUDE (code_key, negotiation_arrangement, rate_count);
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_provider_group")} (
            snapshot_key bigint NOT NULL,
            provider_group_key integer NOT NULL,
            provider_group_global_id_128 bytea NOT NULL,
            CONSTRAINT {_q("ptg2_v3_provider_group_pkey")}
                PRIMARY KEY (snapshot_key, provider_group_key),
            CONSTRAINT {_q("ptg2_v3_provider_group_global_id_key")}
                UNIQUE (snapshot_key, provider_group_global_id_128),
            CONSTRAINT {_q("ptg2_v3_provider_group_global_id_check")}
                CHECK (octet_length(provider_group_global_id_128) = 16),
            CONSTRAINT {_q("ptg2_v3_provider_group_key_check")}
                CHECK (provider_group_key >= 0)
        );
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_provider_set")} (
            snapshot_key bigint NOT NULL,
            provider_set_key integer NOT NULL,
            provider_set_global_id_128 bytea NOT NULL,
            provider_count bigint NOT NULL,
            network_names text[] NOT NULL DEFAULT ARRAY[]::text[],
            CONSTRAINT {_q("ptg2_v3_provider_set_pkey")}
                PRIMARY KEY (snapshot_key, provider_set_key),
            CONSTRAINT {_q("ptg2_v3_provider_set_global_id_key")}
                UNIQUE (snapshot_key, provider_set_global_id_128),
            CONSTRAINT {_q("ptg2_v3_provider_set_global_id_check")}
                CHECK (octet_length(provider_set_global_id_128) = 16),
            CONSTRAINT {_q("ptg2_v3_provider_set_provider_count_check")}
                CHECK (provider_count >= 0),
            CONSTRAINT {_q("ptg2_v3_provider_set_key_check")}
                CHECK (provider_set_key >= 0)
        );
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_price_attr")} (
            snapshot_key bigint NOT NULL,
            attribute_kind varchar(32) NOT NULL,
            attribute_key integer NOT NULL,
            value text,
            CONSTRAINT {_q("ptg2_v3_price_attr_pkey")}
                PRIMARY KEY (snapshot_key, attribute_kind, attribute_key),
            CONSTRAINT {_q("ptg2_v3_price_attr_value_key")}
                UNIQUE NULLS NOT DISTINCT (snapshot_key, attribute_kind, value)
        );
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_npi_scope")} (
            snapshot_key bigint NOT NULL,
            npi bigint NOT NULL,
            CONSTRAINT {_q("ptg2_v3_npi_scope_pkey")}
                PRIMARY KEY (snapshot_key, npi),
            CONSTRAINT {_q("ptg2_v3_npi_scope_npi_check")}
                CHECK (npi > 0)
    ) PARTITION BY HASH (snapshot_key);
    """
    )
    _create_hash_partitions(schema, "ptg2_v3_npi_scope")

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_audit_occurrence")} (
            snapshot_key bigint NOT NULL,
            occurrence_id bytea NOT NULL,
            code_key integer NOT NULL,
            provider_set_key integer NOT NULL,
            price_key bigint NOT NULL,
            source_key integer NOT NULL,
            npi bigint NOT NULL,
            atom_ordinal bigint NOT NULL,
            atom_key bigint NOT NULL,
            CONSTRAINT {_q("ptg2_v3_audit_occurrence_pkey")}
                PRIMARY KEY (snapshot_key, occurrence_id),
            CONSTRAINT {_q("ptg2_v3_audit_occurrence_id_check")}
                CHECK (octet_length(occurrence_id) = 32),
            CONSTRAINT {_q("ptg2_v3_audit_occurrence_code_key_check")}
                CHECK (code_key >= 0),
            CONSTRAINT {_q("ptg2_v3_audit_occurrence_provider_set_key_check")}
                CHECK (provider_set_key >= 0),
            CONSTRAINT {_q("ptg2_v3_audit_occurrence_price_key_check")}
                CHECK (price_key >= 0),
            CONSTRAINT {_q("ptg2_v3_audit_occurrence_source_key_check")}
                CHECK (source_key >= 0),
            CONSTRAINT {_q("ptg2_v3_audit_occurrence_npi_check")}
                CHECK (npi BETWEEN 1000000000 AND 9999999999),
            CONSTRAINT {_q("ptg2_v3_audit_occurrence_atom_ordinal_check")}
                CHECK (atom_ordinal >= 0),
            CONSTRAINT {_q("ptg2_v3_audit_occurrence_atom_key_check")}
                CHECK (atom_key >= 0)
        ) PARTITION BY HASH (snapshot_key);
        """
    )
    _create_hash_partitions(schema, "ptg2_v3_audit_occurrence")

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_candidate_audit_attestation")} (
            snapshot_id varchar(96) NOT NULL,
            snapshot_key bigint NOT NULL,
            source_key varchar(96) NOT NULL,
            plan_id varchar(64) NOT NULL,
            plan_market_type varchar(32) NOT NULL,
            coverage_scope_id bytea NOT NULL,
            source_set_digest bytea NOT NULL,
            audit_sample_digest bytea NOT NULL,
            contract varchar(64) NOT NULL,
            tool_name varchar(64) NOT NULL,
            tool_version varchar(32) NOT NULL,
            report_digest bytea NOT NULL,
            report jsonb NOT NULL,
            attested_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL,
            activated_at timestamptz,
            CONSTRAINT {_q("ptg2_v3_candidate_audit_attestation_pkey")}
                PRIMARY KEY (snapshot_id),
            CONSTRAINT {_q("ptg2_v3_candidate_audit_attestation_snapshot_id_fkey")}
                FOREIGN KEY (snapshot_id)
                REFERENCES {_qt(schema, "ptg2_v3_snapshot_scope")} (snapshot_id)
                ON DELETE CASCADE,
            CONSTRAINT {_q("ptg2_v3_candidate_audit_attestation_snapshot_key_fkey")}
                FOREIGN KEY (snapshot_key)
                REFERENCES {_qt(schema, "ptg2_v3_snapshot_layout")} (snapshot_key)
                ON DELETE RESTRICT,
            CONSTRAINT {_q("ptg2_v3_candidate_audit_attestation_scope_check")}
                CHECK (octet_length(coverage_scope_id) = 32),
            CONSTRAINT {_q("ptg2_v3_candidate_audit_attestation_source_set_check")}
                CHECK (octet_length(source_set_digest) = 32),
            CONSTRAINT {_q("ptg2_v3_candidate_audit_attestation_sample_check")}
                CHECK (octet_length(audit_sample_digest) = 32),
            CONSTRAINT {_q("ptg2_v3_candidate_audit_attestation_report_check")}
                CHECK (octet_length(report_digest) = 32),
            CONSTRAINT {_q("ptg2_v3_candidate_audit_attestation_expiry_check")}
                CHECK (expires_at > attested_at)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_candidate_audit_attestation_snapshot_key_idx")}
            ON {_qt(schema, "ptg2_v3_candidate_audit_attestation")}
            (snapshot_key);
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_candidate_audit_attestation_expiry_idx")}
            ON {_qt(schema, "ptg2_v3_candidate_audit_attestation")}
            (expires_at, activated_at);
        """
    )

    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_gc_candidate")} (
            block_hash bytea NOT NULL,
            eligible_at timestamptz NOT NULL,
            queued_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q("ptg2_v3_gc_candidate_pkey")}
                PRIMARY KEY (block_hash),
            CONSTRAINT {_q("ptg2_v3_gc_candidate_block_hash_fkey")}
                FOREIGN KEY (block_hash)
                REFERENCES {_qt(schema, "ptg2_v3_block")} (block_hash)
                ON DELETE CASCADE
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q("ptg2_v3_gc_candidate_eligible_at_idx")}
            ON {_qt(schema, "ptg2_v3_gc_candidate")} (eligible_at);
        """
    )


def downgrade():
    schema = _schema()
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_gc_candidate')};")
    op.execute(
        f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_candidate_audit_attestation')};"
    )
    op.execute(
        f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_audit_occurrence')} CASCADE;"
    )
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_npi_scope')} CASCADE;")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_price_attr')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_provider_set')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_provider_group')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_code')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_graph_owner')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_snapshot_block')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_snapshot_source')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_snapshot_scope')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_snapshot_binding')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_layout_fingerprint')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_block')} CASCADE;")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_snapshot_layout')};")
