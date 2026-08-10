# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DDL contract for the reviewed terminal-window drift profile."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260810130000_provider_directory_reviewed_subset_terminal_window.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_reviewed_subset_terminal_window_migration",
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
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "terminal_window_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setattr(migration._bounded(), "op", recorder)
    getattr(migration, operation)()
    statements = [
        " ".join(statement.split()) for statement in recorder.statements
    ]
    return migration, statements


def test_upgrade_replaces_only_profile_objects_without_evidence_dml(
    monkeypatch,
):
    migration, statements = _capture(monkeypatch, "upgrade")
    normalized_sql = " ".join(statements)

    assert migration.revision == (
        "20260810130000_provider_directory_reviewed_subset_terminal_window"
    )
    assert migration.down_revision == (
        "20260810120000_provider_directory_terminal_root_retirement_v2"
    )
    assert statements[0].startswith("LOCK TABLE")
    replacements = [
        statement
        for statement in statements
        if statement.startswith("CREATE OR REPLACE FUNCTION")
    ]
    assert len(replacements) == 4
    assert normalized_sql.count(
        'DROP CONSTRAINT "pd_endpoint_dataset_completion_shape_check"'
    ) == 1
    assert normalized_sql.count(
        'ADD CONSTRAINT "pd_endpoint_dataset_completion_shape_check"'
    ) == 1
    assert "traversal-subset-v3" in normalized_sql
    assert "traversal-subset-v4" in normalized_sql
    assert "traversal-subset-v5" in normalized_sql
    assert "page_count * 20" in normalized_sql
    assert "pg_catalog.ceil(advertised_pre / 100::numeric)" in normalized_sql
    assert "logical_terminal_offset" in normalized_sql
    assert "logical_window_end_offset" in normalized_sql
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )


def test_downgrade_retains_v3_v4_and_fences_v5_evidence(monkeypatch):
    migration, statements = _capture(monkeypatch, "downgrade")
    normalized_sql = " ".join(statements)

    fence_position = normalized_sql.index(
        "provider_directory_reviewed_subset_terminal_window_downgrade_blocked"
    )
    replacement_position = normalized_sql.index("CREATE OR REPLACE FUNCTION")
    assert fence_position < replacement_position
    assert migration._TERMINAL_WINDOW_STRATEGY_VERSION in normalized_sql
    replacement_sql = " ".join(
        statement
        for statement in statements
        if statement.startswith("CREATE OR REPLACE FUNCTION")
    )
    assert "traversal-subset-v3" in replacement_sql
    assert "traversal-subset-v4" in replacement_sql
    assert "traversal-subset-v5" not in replacement_sql
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )


def test_profile_adoption_fence_switches_from_v4_to_v5():
    migration = _load_migration()
    before_sql = migration._profile_adoption_fence_sql(
        "terminal_window_test",
        allow_terminal_window=False,
    )
    after_sql = migration._profile_adoption_fence_sql(
        "terminal_window_test",
        allow_terminal_window=True,
    )

    assert "traversal-subset-v5" not in before_sql
    assert "traversal-subset-v5" in after_sql
    assert "IS DISTINCT FROM TRUE" in after_sql
