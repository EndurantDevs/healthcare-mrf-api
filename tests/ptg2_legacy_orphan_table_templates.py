# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen lifecycle-table templates for legacy sweeper PostgreSQL tests."""

MRF_TABLE_TEMPLATES = (
    """
    CREATE TABLE {schema}.ptg2_snapshot (
        snapshot_id text PRIMARY KEY,
        import_run_id text,
        status text NOT NULL,
        manifest jsonb NOT NULL DEFAULT '{{}}'::jsonb
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_import_run (
        import_run_id text PRIMARY KEY,
        status text NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_import_job (
        import_job_id text PRIMARY KEY,
        import_run_id text NOT NULL,
        status text NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.import_run (
        run_id text PRIMARY KEY,
        source_file_import_id text,
        status text NOT NULL,
        snapshot_id text
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_snapshot (
        slot text PRIMARY KEY,
        snapshot_id text,
        previous_snapshot_id text
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_source_snapshot (
        source_key text PRIMARY KEY,
        snapshot_id text,
        previous_snapshot_id text
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_plan_source (
        plan_source_key text PRIMARY KEY,
        snapshot_id text,
        previous_snapshot_id text
    )
    """,
    "CREATE TABLE {schema}.ptg2_snapshot_pin (snapshot_id text PRIMARY KEY)",
    """
    CREATE TABLE {schema}.plan_release_snapshot_binding (
        snapshot_id text PRIMARY KEY
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
        snapshot_id text PRIMARY KEY
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_scope (
        snapshot_id text PRIMARY KEY
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_plan_scope (
        snapshot_id text PRIMARY KEY
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_source (
        snapshot_id text PRIMARY KEY
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_candidate_audit_attestation (
        snapshot_id text PRIMARY KEY
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_artifact_manifest (
        artifact_id text PRIMARY KEY,
        snapshot_id text,
        import_run_id text
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_artifact_blob_chunk (
        artifact_id text NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v4_attempt_fence (
        snapshot_id text NOT NULL,
        internal_run_id text NOT NULL,
        state text NOT NULL
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_legacy_v3_metadata_reconcile_audit (
        snapshot_id text NOT NULL,
        internal_run_id text NOT NULL
    )
    """,
    "CREATE TABLE {schema}.ptg2_plan_month (snapshot_id text)",
    "CREATE TABLE {schema}.ptg2_allowed_amount_plan (snapshot_id text)",
    "CREATE TABLE {schema}.ptg2_allowed_amount_item (snapshot_id text)",
    "CREATE TABLE {schema}.ptg2_allowed_amount_payment (snapshot_id text)",
    """
    CREATE TABLE {schema}.ptg2_allowed_amount_provider_payment (
        snapshot_id text
    )
    """,
    "CREATE TABLE {schema}.ptg2_source_catalog (import_run_id text)",
    "CREATE TABLE {schema}.ptg2_serving_rate (snapshot_id text)",
    "CREATE TABLE {schema}.ptg2_serving_rate_compact (snapshot_id text)",
    "CREATE TABLE {schema}.ptg2_price_set_stage (snapshot_id text)",
    "CREATE TABLE {schema}.ptg2_serving_rate_stage (snapshot_id text)",
    """
    CREATE TABLE {schema}.ptg2_v4_attempt_stage (
        snapshot_id text,
        internal_run_id text
    )
    """,
)


