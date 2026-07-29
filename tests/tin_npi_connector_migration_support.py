# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Loader and statement capture for connector migration contract tests."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260729110000_tin_npi_connector.py"
)


def load_connector_migration():
    module_spec = importlib.util.spec_from_file_location(
        "tin_npi_connector_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def capture_upgrade(monkeypatch) -> tuple[object, list[str], str]:
    migration = load_connector_migration()
    sql_statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tin_connector")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", sql_statements.append)

    migration.upgrade()

    normalized_sql = " ".join(" ".join(sql_statements).split())
    return migration, sql_statements, normalized_sql
