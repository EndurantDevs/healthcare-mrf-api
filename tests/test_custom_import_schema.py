# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa

from db import maintenance
from db.models import (
    CustomImportDataset,
    CustomImportField,
    CustomImportGeneration,
    CustomImportRootScalar,
    CustomImportSchemaRevision,
)
from db.models import CustomImportWinner


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / "20260914120000_custom_import_v1_schema.py"


def _migration():
    spec = importlib.util.spec_from_file_location("custom_import_v1_migration", MIGRATION_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_models_keep_schema_identity_distinct_from_definition_and_runtime_sync():
    assert CustomImportDataset.__runtime_schema_sync__ is False
    assert isinstance(CustomImportSchemaRevision.__table__.c.canonical_schema.type, sa.Text)
    assert CustomImportSchemaRevision.__table__.c.schema_sha256.type.length == 32
    assert CustomImportField.__table__.c.projection_slot.nullable is False
    assert "state" not in CustomImportGeneration.__table__.c
    assert "custom_import_generation_execution_key" in {
        constraint.name for constraint in CustomImportGeneration.__table__.constraints
    }
    assert tuple(CustomImportWinner.__table__.primary_key.columns.keys()) == (
        "generation_id", "profile_slot", "entity_binding_id", "context_key_sha256"
    )
    foreign_key_columns_by_name = {
        foreign_key.name: tuple(element.parent.name for element in foreign_key.elements)
        for foreign_key in CustomImportRootScalar.__table__.foreign_key_constraints
    }
    assert foreign_key_columns_by_name["custom_import_root_scalar_field_fkey"][-1] == "projection_slot"


def test_migration_is_schema_only_and_installs_content_immutability(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "custom_import_test")
    migration = _migration()
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    normalized = "\n".join(" ".join(statement.split()) for statement in statements)
    schema = migration._quote(migration._schema())
    assert migration.down_revision == "20260911100000_hospital_price_tall_notes"
    assert all(f"CREATE TABLE {schema}.{name}" in normalized for name in migration._TABLE_NAMES)
    assert "INSERT INTO" not in normalized
    assert "guard_custom_import_immutable_row" in normalized
    assert normalized.count("BEFORE UPDATE OR DELETE") == len(migration._IMMUTABLE_TABLES)
    assert len(migration._TABLE_DDL) == len(migration._TABLE_NAMES)
    assert all("mrf." in statement for statement in migration._TABLE_DDL)
    index_ddl = "\n".join(migration._INDEX_DDL)
    assert "custom_import_pack_execution_stream_idx" not in index_ddl
    assert "custom_import_generation_family_lookup_idx" not in index_ddl
    assert "custom_import_entity_binding_lookup_idx" not in index_ddl
    assert "custom_import_winner_lookup_idx" in index_ddl
    function_statement = next(
        statement for statement in statements if "CREATE FUNCTION" in statement
    )
    revoke_statement = next(
        statement for statement in statements if "REVOKE ALL ON FUNCTION" in statement
    )
    assert "REVOKE ALL ON FUNCTION" not in function_statement
    assert "CREATE FUNCTION" not in revoke_statement


def test_migration_downgrade_removes_only_v1_relations_in_reverse_dependency_order(
    monkeypatch,
):
    migration = _migration()
    assert not hasattr(migration, "models")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "custom_import_test")
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    schema = migration._quote(migration._schema())
    assert statements[0] == (
        f"DROP TABLE IF EXISTS {schema}.\"custom_import_winner\""
    )
    assert statements[-2] == (
        f"DROP TABLE IF EXISTS {schema}.\"custom_import_dataset\""
    )
    assert statements[-1] == (
        f"DROP FUNCTION IF EXISTS {schema}.guard_custom_import_immutable_row()"
    )
    assert len(statements) == len(migration._TABLE_CREATION_ORDER) + 1


def test_runtime_sync_skips_migration_owned_table_before_inspection(monkeypatch):
    metadata = sa.MetaData()
    table = sa.Table("custom_import_dataset", metadata, sa.Column("dataset_id", sa.BigInteger))
    calls: list[str] = []

    class Inspector:
        def has_table(self, *_args, **_kwargs):
            calls.append("has_table")
            return False

    monkeypatch.setattr(maintenance.Base, "metadata", metadata)
    monkeypatch.setattr(maintenance, "inspect", lambda _connection: Inspector())
    monkeypatch.setattr(
        maintenance,
        "_model_by_table_fullname",
        lambda: {table.fullname: CustomImportDataset},
    )
    monkeypatch.setattr(maintenance, "_managed_schemas", lambda: ("public",))
    sync_results_by_kind = {
        kind: []
        for kind in ("tables", "columns", "indexes", "constraints", "skipped_columns", "retired")
    }

    maintenance._sync_structure(None, sync_results_by_kind, add_columns=True, add_indexes=True)

    assert calls == []
    assert all(not values for values in sync_results_by_kind.values())
