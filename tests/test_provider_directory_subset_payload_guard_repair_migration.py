# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""DDL contract for the subset payload guard repair."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260808210000_provider_directory_subset_payload_guard_repair.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_subset_payload_guard_repair_migration",
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


def test_upgrade_reinstalls_the_guard_with_jsonb_payload_normalization(
    monkeypatch,
):
    migration = _load_migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "payload_guard_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    normalized_sql = " ".join(
        " ".join(statement.split()) for statement in recorder.statements
    )
    assert migration.revision == (
        "20260808210000_provider_directory_subset_payload_guard_repair"
    )
    assert migration.down_revision == (
        "20260808200000_provider_directory_reviewed_subset_activation"
    )
    assert normalized_sql.startswith("LOCK TABLE")
    assert "child.payload_json::jsonb - 'resource_url'" in normalized_sql
    assert "child.payload_json - 'resource_url'" not in normalized_sql
    assert normalized_sql.count("CREATE OR REPLACE FUNCTION") == 1
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 1
    assert normalized_sql.count(
        "tin_npi_connector_endpoint_dataset_guard_changed"
    ) == 2
    assert normalized_sql.count(
        "provider_directory_reviewed_subset_activation_trigger_changed"
    ) == 2


def test_downgrade_preserves_the_safe_guard_body(monkeypatch):
    migration = _load_migration()
    recorder = _Recorder()
    monkeypatch.setattr(migration, "op", recorder)

    migration.downgrade()

    assert recorder.statements == []
