# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DDL contract for the reviewed-subset bounded-drift profile."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260810000000_provider_directory_reviewed_subset_bounded_drift.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_reviewed_subset_bounded_drift_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


class _Recorder:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement):
        self.statements.append(str(statement))


def _capture(monkeypatch, operation):
    migration = _load_migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bounded_drift_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    getattr(migration, operation)()
    statements = [
        " ".join(statement.split()) for statement in recorder.statements
    ]
    return migration, statements


def test_upgrade_replaces_only_profile_bearing_objects_without_evidence_dml(
    monkeypatch,
):
    migration, statements = _capture(monkeypatch, "upgrade")
    normalized_sql = " ".join(statements)

    assert migration.revision == (
        "20260810000000_provider_directory_reviewed_subset_bounded_drift"
    )
    assert migration.down_revision == (
        "20260809030000_provider_directory_reviewed_root_policy"
    )
    assert statements[0].startswith("LOCK TABLE")
    assert "ACCESS EXCLUSIVE MODE" in statements[0]
    replacements = [
        statement
        for statement in statements
        if statement.startswith("CREATE OR REPLACE FUNCTION")
    ]
    assert len(replacements) == 4
    replaced_function_prefixes = {
        statement.split("(", 1)[0] for statement in replacements
    }
    assert replaced_function_prefixes == {
        "CREATE OR REPLACE FUNCTION "
        '"bounded_drift_test".'
        '"provider_directory_subset_completion_proof_shape_valid"',
        "CREATE OR REPLACE FUNCTION "
        '"bounded_drift_test".'
        '"guard_tin_npi_connector_endpoint_dataset"',
        "CREATE OR REPLACE FUNCTION "
        '"bounded_drift_test".'
        '"guard_provider_directory_subset_published_source"',
        "CREATE OR REPLACE FUNCTION "
        '"bounded_drift_test".'
        '"provider_directory_reviewed_subset_activation_valid"',
    }
    assert normalized_sql.count(
        'DROP CONSTRAINT "pd_endpoint_dataset_completion_shape_check"'
    ) == 1
    assert normalized_sql.count(
        'ADD CONSTRAINT "pd_endpoint_dataset_completion_shape_check"'
    ) == 1
    assert "advertised-count-stability" in normalized_sql
    assert (
        "advertised-count-monotone-decrease-at-most-one" in normalized_sql
    )
    assert "advertised_post > advertised_pre" in normalized_sql
    assert "returned_unique > advertised_post" in normalized_sql
    assert "IS DISTINCT FROM TRUE" in normalized_sql
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )


def test_historical_renderers_remain_exact_profile_only_by_default():
    migration = _load_migration()
    subset = migration._subset()
    activation = migration._activation()
    schema = "bounded_drift_test"

    default_sql = " ".join(
        (
            subset._proof_shape_valid_function_sql(schema),
            subset._subset_proof_shape_check(schema),
            subset._subset_endpoint_dataset_guard_sql(
                schema,
                use_configured_endpoint_identity=True,
                reviewed_root_policy_aware=True,
            ),
            subset._subset_published_source_guard_sql(
                schema,
                use_configured_endpoint_identity=True,
                replace_existing=True,
                reviewed_root_policy_aware=True,
            ),
            activation._activation_valid_function_sql(
                schema,
                use_configured_endpoint_identity=True,
                replace_existing=True,
                reviewed_root_policy_aware=True,
            ),
        )
    )
    aware_sql = subset._proof_shape_valid_function_sql(
        schema,
        replace_existing=True,
        reviewed_subset_profile_aware=True,
    )

    assert migration._LEGACY_STRATEGY_VERSION in default_sql
    assert migration._BOUNDED_STRATEGY_VERSION not in default_sql
    assert "max_advertised_count_decrease := 0" in default_sql
    assert migration._LEGACY_STRATEGY_VERSION in aware_sql
    assert migration._BOUNDED_STRATEGY_VERSION in aware_sql
    assert "max_advertised_count_decrease := 1" in aware_sql


def test_terminal_window_renderer_preserves_prior_modes():
    """Keep v5 opt-in while preserving historical renderer modes."""

    migration = _load_migration()
    subset = migration._subset()
    schema = "bounded_drift_test"
    v4_sql = subset._proof_shape_valid_function_sql(
        schema,
        replace_existing=True,
        reviewed_subset_profile_aware=True,
    )
    v5_sql = subset._proof_shape_valid_function_sql(
        schema,
        replace_existing=True,
        reviewed_subset_profile_aware=True,
        reviewed_subset_terminal_window_profile_aware=True,
    )
    assert "traversal-subset-v5" not in v4_sql
    assert "terminal-logical-window-covers-advertised-pre" not in v4_sql
    assert "traversal-subset-v5" in v5_sql
    assert "pg_catalog.ceil(advertised_pre / 100::numeric)" in v5_sql
    assert "page_count * 20" in v5_sql
    assert "terminal_count_window_required" in v5_sql
    rendered_objects = (
        subset._subset_proof_shape_check(
            schema,
            reviewed_subset_profile_aware=True,
            reviewed_subset_terminal_window_profile_aware=True,
        ),
        subset._subset_endpoint_dataset_guard_sql(
            schema,
            use_configured_endpoint_identity=True,
            reviewed_root_policy_aware=True,
            reviewed_subset_profile_aware=True,
            reviewed_subset_terminal_window_profile_aware=True,
        ),
        subset._subset_published_source_guard_sql(
            schema,
            use_configured_endpoint_identity=True,
            replace_existing=True,
            reviewed_root_policy_aware=True,
            reviewed_subset_profile_aware=True,
            reviewed_subset_terminal_window_profile_aware=True,
        ),
        migration._activation()._activation_valid_function_sql(
            schema,
            use_configured_endpoint_identity=True,
            replace_existing=True,
            reviewed_root_policy_aware=True,
            reviewed_subset_profile_aware=True,
            reviewed_subset_terminal_window_profile_aware=True,
        ),
        migration._identity()._adoption_state_fence_sql(
            schema,
            reviewed_root_policy_aware=True,
            reviewed_subset_profile_aware=True,
            reviewed_subset_terminal_window_profile_aware=True,
        ),
    )
    assert all("traversal-subset-v5" in sql for sql in rendered_objects)


def test_effective_endpoint_fence_accepts_policy_and_profile_awareness():
    """Render adoption checks for policy-one v3/v4 lifecycle state."""

    migration = _load_migration()
    identity = migration._identity()
    fence_sql = " ".join(
        identity._adoption_state_fence_sql(
            "bounded_drift_test",
            reviewed_root_policy_aware=True,
            reviewed_subset_profile_aware=True,
        ).split()
    )

    assert "provider_directory_reviewed_root_policy_v1" in fence_sql
    assert "provider_directory_reviewed_subset_activation_v2" in fence_sql
    assert "verified_reviewed_subset_acquisition" in fence_sql
    assert migration._LEGACY_STRATEGY_VERSION in fence_sql
    assert migration._BOUNDED_STRATEGY_VERSION in fence_sql


def test_downgrade_is_reversible_only_behind_bounded_evidence_fence(
    monkeypatch,
):
    migration, statements = _capture(monkeypatch, "downgrade")
    normalized_sql = " ".join(statements)
    fence_position = normalized_sql.index(
        "provider_directory_reviewed_subset_profile_downgrade_blocked"
    )
    replacement_position = normalized_sql.index("CREATE OR REPLACE FUNCTION")

    assert fence_position < replacement_position
    assert migration._BOUNDED_STRATEGY_VERSION in normalized_sql
    assert migration._BOUNDED_COMPLETION_SCOPES_JSON in normalized_sql
    assert normalized_sql.count("CREATE OR REPLACE FUNCTION") == 4
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )
