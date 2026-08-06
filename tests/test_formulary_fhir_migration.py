# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from db.models import FHIRFormularyCheckpoint, FHIRFormularyCoveragePlan


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260806100000_fhir_formulary_generations.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "fhir_formulary_generations_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _upgrade_sql(monkeypatch) -> tuple[object, str, list[str]]:
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "formulary_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    return migration, " ".join(" ".join(statements).split()), statements


def test_formulary_migration_is_copy_on_write_and_inactive(monkeypatch):
    migration, sql, statements = _upgrade_sql(monkeypatch)

    assert migration.down_revision == "20260806100000_ptg2_tax_identity_source"
    for table in (
        "fhir_formulary_source",
        "fhir_formulary_dataset",
        "fhir_formulary_current",
        "fhir_formulary_coverage_plan",
        "fhir_formulary_coverage_plan_version",
        "fhir_formulary_dataset_coverage_plan",
        "fhir_formulary_drug_plan_alias",
        "fhir_formulary_drug_plan_alias_version",
        "fhir_formulary_dataset_alias",
        "fhir_formulary_medication",
        "fhir_formulary_alias_membership",
        "fhir_formulary_alternative",
        "fhir_formulary_checkpoint",
    ):
        assert f'CREATE TABLE "formulary_test"."{table}"' in sql
    assert "public_id ~ '^fhir_[a-z2-7]{26}$'" in sql
    assert "canonical_identity text NOT NULL" in sql
    assert "upstream_date timestamptz" in sql
    assert "UNIQUE (source_id, canonical_identity)" in sql
    assert "PRIMARY KEY (source_id, alias_id, run_id)" in sql
    assert "fhir_formulary_checkpoint_stale_fence" in sql
    assert "fhir_formulary_checkpoint_owner_immutable" in sql
    assert "enabled, metadata_json" in sql
    assert "false" in sql
    assert '"automation_enabled": false' in sql
    assert "legacy" not in sql.lower()

    table_statements = [
        statement
        for statement in statements
        if statement.lstrip().startswith("CREATE TABLE")
    ]
    assert len(table_statements) == 13
    assert all("CREATE INDEX" not in statement for statement in table_statements)
    function_statement = next(
        statement
        for statement in statements
        if statement.lstrip().startswith("CREATE FUNCTION")
    )
    assert "CREATE TRIGGER" not in function_statement
    assert any(
        statement.lstrip().startswith("CREATE TRIGGER")
        for statement in statements
    )


def test_formulary_models_keep_public_identity_and_alias_checkpoint_distinct():
    assert tuple(FHIRFormularyCoveragePlan.__table__.primary_key.columns.keys()) == (
        "public_id",
    )
    assert tuple(FHIRFormularyCheckpoint.__table__.primary_key.columns.keys()) == (
        "source_id",
        "alias_id",
        "run_id",
    )
    assert FHIRFormularyCheckpoint.__table__.c.source_plan_identifier.nullable is False


def test_formulary_migration_schema_alias_conflict_fails_closed(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration.upgrade()
