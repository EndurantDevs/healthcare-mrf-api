# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic PostgreSQL schema used by stale V4 metadata tests."""

from __future__ import annotations


SCHEMA_DDL = (
    """
    CREATE TABLE {schema}.ptg2_snapshot (
        snapshot_id varchar(96) PRIMARY KEY,
        import_run_id varchar(96),
        import_month date,
        status varchar(32),
        created_at timestamp without time zone,
        validated_at timestamp without time zone,
        published_at timestamp without time zone,
        previous_snapshot_id varchar(96),
        manifest json
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_import_run (
        import_run_id varchar(96) PRIMARY KEY,
        import_month date,
        status varchar(32),
        started_at timestamp without time zone,
        finished_at timestamp without time zone,
        heartbeat_at timestamp without time zone,
        options json,
        report json,
        error text
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
        snapshot_id varchar(96),
        snapshot_key bigint
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_scope (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_plan_scope (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_source (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_candidate_audit_attestation (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_snapshot_pin (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.plan_release_snapshot_binding (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_plan_month (
        plan_month_id varchar(96) PRIMARY KEY,
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_artifact_manifest (
        artifact_id varchar(96) PRIMARY KEY,
        snapshot_id varchar(96),
        import_run_id varchar(96),
        artifact_kind varchar(64),
        storage_uri text,
        sha256 varchar(64),
        byte_count bigint,
        payload json,
        created_at timestamp without time zone
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_allowed_amount_plan (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_allowed_amount_item (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_allowed_amount_payment (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_allowed_amount_provider_payment (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_snapshot (
        slot varchar(32),
        snapshot_id varchar(96),
        previous_snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_source_snapshot (
        source_key varchar(96),
        snapshot_id varchar(96),
        previous_snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_plan_source (
        plan_source_key varchar(96),
        snapshot_id varchar(96),
        previous_snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_import_job (
        import_job_id varchar(96) NOT NULL,
        import_run_id varchar(96),
        source_catalog_id varchar(96),
        source_type varchar(64),
        status varchar(32),
        attempts integer,
        lease_owner varchar,
        lease_expires_at timestamp without time zone,
        heartbeat_at timestamp without time zone,
        error text,
        payload json,
        created_at timestamp without time zone,
        updated_at timestamp without time zone,
        CONSTRAINT ptg2_import_job_pkey PRIMARY KEY (import_job_id)
    )
    """,
    """
    CREATE INDEX ptg2_import_job_run_idx
    ON {schema}.ptg2_import_job (import_run_id)
    """,
    """
    CREATE INDEX ptg2_import_job_status_idx
    ON {schema}.ptg2_import_job (status)
    """,
    """
    CREATE INDEX ptg2_import_job_type_idx
    ON {schema}.ptg2_import_job (source_type)
    """,
    """
    CREATE TABLE {schema}.ptg2_source_catalog (
        source_catalog_id varchar(96),
        import_run_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_serving_rate (
        serving_rate_id varchar(96),
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_serving_rate_compact (
        serving_rate_id varchar(96),
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_price_set_stage (
        snapshot_id varchar(96),
        price_set_hash varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_serving_rate_stage (
        serving_rate_id varchar(96),
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
        snapshot_key bigint PRIMARY KEY,
        state varchar(16)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_block (
        block_hash bytea PRIMARY KEY,
        payload bytea
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_block (
        snapshot_key bigint,
        block_hash bytea
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_gc_candidate (
        block_hash bytea PRIMARY KEY,
        eligible_at timestamptz
    )
    """,
)
