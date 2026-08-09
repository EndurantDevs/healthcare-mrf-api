# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DDL contract for reviewed Provider Directory root policy."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260809030000_provider_directory_reviewed_root_policy.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_reviewed_root_policy_migration",
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
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "reviewed_root_policy_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    getattr(migration, operation)()
    statements = [
        " ".join(statement.split()) for statement in recorder.statements
    ]
    return migration, recorder, statements


def _assert_upgrade_identity(migration, statements):
    assert migration.revision == (
        "20260809030000_provider_directory_reviewed_root_policy"
    )
    assert migration.down_revision == (
        "20260809020000_nppes_lifecycle_date_tolerance"
    )
    assert statements[0].startswith("LOCK TABLE")
    for relation_name in (
        "provider_directory_endpoint_dataset",
        "provider_directory_dataset_resource",
        "provider_directory_source",
        "provider_directory_dataset_proof_shard",
        "provider_directory_pagination_checkpoint",
        "provider_directory_bulk_acquisition_checkpoint",
    ):
        assert relation_name in statements[0]


def _assert_replay_constraint(statements):
    replay_check = "pd_endpoint_dataset_subset_replay_evidence_check"
    dropped_checks = [
        statement for statement in statements
        if statement.startswith("ALTER TABLE")
        and f'DROP CONSTRAINT "{replay_check}"' in statement
    ]
    added_checks = [
        statement for statement in statements
        if statement.startswith("ALTER TABLE")
        and f'ADD CONSTRAINT "{replay_check}"' in statement
    ]
    assert len(dropped_checks) == len(added_checks) == 1
    assert "required_root_count" in added_checks[0]
    assert "THEN 'not_required' ELSE" in added_checks[0]


def test_upgrade_replaces_policy_lifecycle_functions_without_evidence_dml(
    monkeypatch,
):
    """Require policy-aware function replacement without evidence mutation."""

    migration, recorder, statements = _capture(monkeypatch, "upgrade")
    normalized_sql = " ".join(statements)
    _assert_upgrade_identity(migration, statements)
    replacements = [
        statement
        for statement in statements
        if statement.startswith("CREATE OR REPLACE FUNCTION")
    ]
    assert len(replacements) == 8
    assert sum(
        statement.startswith("CREATE FUNCTION") for statement in statements
    ) == 0
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 8
    assert "provider_directory_subset_content_proof_valid" in normalized_sql
    assert "provider_directory_subset_coverage_shape_valid" in normalized_sql
    assert "provider_directory_reviewed_subset_activation_valid" in normalized_sql
    assert "guard_provider_directory_subset_abandonment_dataset" in normalized_sql
    assert "provider_directory_reviewed_root_policy_v1" in normalized_sql
    assert "pending_reviewed_subset_acquisition" in normalized_sql
    assert "verified_reviewed_subset_acquisition" in normalized_sql
    assert "provider_directory_reviewed_subset_activation_v2" in normalized_sql
    assert "not_required" in normalized_sql
    _assert_replay_constraint(statements)
    assert "provider_directory_reviewed_root_policy_adoption_blocked" in (
        normalized_sql
    )
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )
    assert not any(
        statement.startswith(("CREATE TRIGGER", "DROP TRIGGER"))
        for statement in statements
    )
    assert recorder.statements


def test_policy_source_and_dataset_guards_are_closed_across_marker_versions():
    migration = _load_migration()
    activation = migration._activation()
    abandonment = migration._abandonment()
    source_sql = " ".join(
        activation._source_guard_function_sql(
            "reviewed_root_policy_test",
            allow_effective_endpoint_cutover=True,
            replace_existing=True,
            reviewed_root_policy_aware=True,
        ).split()
    )
    dataset_sql = " ".join(
        activation._dataset_guard_function_sql(
            "reviewed_root_policy_test",
            replace_existing=True,
            reviewed_root_policy_aware=True,
        ).split()
    )
    abandonment_sql = " ".join(
        abandonment._dataset_guard_sql(
            "reviewed_root_policy_test",
            reviewed_root_policy_aware=True,
        ).split()
    )

    for rendered_sql in (source_sql, dataset_sql, abandonment_sql):
        assert "provider_directory_reviewed_subset_activation_v1" in rendered_sql
        assert "provider_directory_reviewed_subset_activation_v2" in rendered_sql
    assert "pending_reviewed_subset_acquisition" in source_sql
    assert "verified_reviewed_subset_acquisition" in source_sql
    assert "pending_reviewed_subset_acquisition" in abandonment_sql
    assert "provider_directory_reviewed_root_policy_v1" in abandonment_sql
    assert "?| ARRAY" in source_sql
    assert "provider_directory_reviewed_subset_activation_dataset_invalid" in (
        dataset_sql
    )


def test_content_proof_adoption_fence_accepts_only_absent_or_exact_function():
    migration = _load_migration()
    adoption_sql = " ".join(
        migration._content_proof_shape_fence_sql(
            "reviewed_root_policy_test",
            expect_installed=None,
        ).split()
    )
    installed_sql = " ".join(
        migration._content_proof_shape_fence_sql(
            "reviewed_root_policy_test",
            expect_installed=True,
        ).split()
    )

    assert "signature_oid IS NOT NULL AND function_count <> 1" in adoption_sql
    assert "signature_oid IS NULL OR function_count <> 1" in installed_sql
    assert "provider_directory_reviewed_root_policy_function_changed" in adoption_sql


def test_downgrade_preserves_hardened_policy_bodies(monkeypatch):
    _migration, recorder, statements = _capture(monkeypatch, "downgrade")

    assert statements == []
    assert recorder.statements == []
