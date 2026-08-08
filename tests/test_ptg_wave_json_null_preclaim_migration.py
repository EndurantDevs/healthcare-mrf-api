"""Migration contract for ORM JSON-null parity in preclaim recovery."""

from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260808140000_ptg_import_wave_json_null_preclaim.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg_import_wave_json_null_preclaim_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_upgrade_replaces_only_the_exact_json_null_predicate(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "wave_recovery_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    sql = " ".join(statements[0].split())
    assert migration.down_revision == (
        "20260808130000_fhir_formulary_publication_guards"
    )
    assert '"wave_recovery_test".' in sql
    assert "OR run.error IS NOT NULL" in sql
    assert (
        "run.error::jsonb IS DISTINCT FROM ''null''::jsonb" in sql
    )
    assert "pg_get_functiondef" in sql
    assert "PTG_IMPORT_WAVE_PRECLAIM_JSON_NULL_PATCH_PRECONDITION_FAILED" in sql


def test_downgrade_has_the_same_single_replacement_precondition(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "wave_recovery_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    sql = " ".join(statements[0].split())
    assert "pg_get_functiondef" in sql
    assert "PTG_IMPORT_WAVE_PRECLAIM_JSON_NULL_PATCH_PRECONDITION_FAILED" in sql
    assert (
        "OR (run.error IS NOT NULL AND run.error::jsonb IS DISTINCT FROM "
        "''null''::jsonb)" in sql
    )
