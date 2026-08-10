# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""DDL contract for the terminal scope-binding successor."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260810020000_provider_directory_terminal_scope_binding.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_terminal_scope_binding_migration",
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
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "terminal_scope_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    getattr(migration, operation)()
    statements = [
        " ".join(statement.split()) for statement in recorder.statements
    ]
    return migration, statements, " ".join(statements)


def test_upgrade_replaces_only_the_two_installed_validator_bodies(monkeypatch):
    migration, statements, normalized_sql = _capture(monkeypatch, "upgrade")

    assert migration.revision == (
        "20260810020000_provider_directory_terminal_scope_binding"
    )
    assert migration.down_revision == (
        "20260810010000_provider_directory_reviewed_subset_terminal_disposition"
    )
    assert "SHARE ROW EXCLUSIVE MODE NOWAIT" in statements[0]
    assert "1..150" in statements[0]
    assert "attempt = 150" in statements[0]
    assert "pg_catalog.pg_sleep(0.2)" in statements[0]
    replacement_statements = [
        statement
        for statement in statements
        if statement.startswith("CREATE OR REPLACE FUNCTION")
    ]
    assert len(replacement_statements) == 2
    assert "provider_directory_subset_terminal_disposition_valid" in (
        replacement_statements[0]
    )
    assert "guard_provider_directory_subset_abandonment_dataset" in (
        replacement_statements[1]
    )
    assert "guard_provider_directory_subset_abandonment_checkpoint" not in (
        " ".join(replacement_statements)
    )
    assert "CREATE TRIGGER" not in normalized_sql
    assert "CREATE CONSTRAINT TRIGGER" not in normalized_sql
    assert "DROP TRIGGER" not in normalized_sql
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )


def test_stable_and_transition_sql_bind_exact_serial_diagnostics():
    migration = _load_migration()
    valid_sql = " ".join(
        migration._valid_function_sql("terminal_scope_test").split()
    )
    guard_sql = " ".join(
        migration._dataset_guard_sql("terminal_scope_test").split()
    )

    for sql in (valid_sql, guard_sql):
        for field_name in migration._SERIAL_CONCURRENCY_FIELDS:
            assert field_name in sql
            assert f"-> '{field_name}' #>> '{{}}' IS DISTINCT FROM '1'" in sql
        assert "?& ARRAY['absence_semantics'" in sql
        assert "resource_scan_concurrency_effective" in sql
        assert "resource_scan_concurrency_requested" in sql


def test_transition_recomputed_scope_binds_candidate_verification_domain():
    migration = _load_migration()
    guard_sql = " ".join(
        migration._dataset_guard_sql("terminal_scope_test").split()
    )
    predecessor = migration._predecessor()
    marker_scope_target = (
        "NEW.publication_metadata_json::jsonb #>> "
        f"'{{{predecessor._MARKER},source_scope_sha256}}'"
    )

    assert "provider_directory_subset_canonical_sha256" in guard_sql
    assert (
        "NEW.publication_metadata_json::jsonb ->> "
        "'verification_source_scope_hash'"
    ) in guard_sql
    assert marker_scope_target not in guard_sql


def test_clean_downgrade_restores_exact_predecessor_bodies(monkeypatch):
    migration, statements, normalized_sql = _capture(monkeypatch, "downgrade")
    predecessor = migration._predecessor()
    replacement_statements = [
        statement
        for statement in statements
        if statement.startswith("CREATE OR REPLACE FUNCTION")
    ]

    assert len(replacement_statements) == 2
    assert "resource_scan_concurrency_requested" not in (
        " ".join(replacement_statements)
    )
    assert "provider_directory_terminal_scope_binding_evidence_blocked" in (
        normalized_sql
    )
    assert "CREATE TRIGGER" not in normalized_sql
    assert predecessor._MARKER in normalized_sql
