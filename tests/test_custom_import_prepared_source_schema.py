# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Migration-owned replay metadata, narrow DDL and fail-closed removal."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db import maintenance, models
from db.models.custom_import import CustomImportSourceBindingRevision
from db.models.custom_import_prepared_source import CustomImportPreparedSource, CustomImportPreparedSourcePart
from tests.custom_import_postgres_support import _migration

_MIGRATION = Path(__file__).resolve().parents[1] / "alembic/versions/20261023010000_custom_import_prepared_source.py"
_MODELS = (CustomImportPreparedSource, CustomImportPreparedSourcePart)


def _operations(monkeypatch, *, retained=False, isolation="read committed"):
    migration = _migration(_MIGRATION, "synthetic_prepared_source_schema")
    statements = []
    connection = SimpleNamespace(
        dialect=postgresql.dialect(),
        scalars=lambda *_args: ["writer", 'odd"role', "PUBLIC"],
        scalar=lambda statement: isolation if "transaction_isolation" in str(statement) else retained,
    )
    operations = SimpleNamespace(
        get_bind=lambda: connection,
        execute=lambda statement: statements.append(str(statement.compile(dialect=connection.dialect))),
        drop_table=lambda name, schema: statements.append(f"DROP {schema}.{name}"),
    )
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_prepared")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    return migration, statements


@pytest.mark.parametrize("model", _MODELS)
def test_prepared_models_are_registered_migration_owned_and_have_no_foreign_keys(model):
    table = model.__table__
    assert getattr(models, model.__name__) is model
    assert model.__runtime_schema_sync__ is False
    assert maintenance._model_by_table_fullname()[table.fullname] is model
    assert not table.foreign_keys
    assert not any(column.autoincrement is True for column in table.columns)
    assert {tuple(index.columns.keys()) for index in table.indexes} == {("dataset_id",)}


def test_prepared_header_retains_identity_progress_and_explicit_sealed_nullability():
    table = CustomImportPreparedSource.__table__
    assert tuple(table.primary_key.columns.keys()) == ("execution_id",)
    assert {column.name for column in table.columns if column.nullable} == {
        "canonical_summary",
        "prepared_sha256",
        "sealed_at",
    }
    checks_by_name = {
        constraint.name: str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, sa.CheckConstraint)
    }
    assert len(checks_by_name) == 3
    state = checks_by_name["custom_import_prepared_source_state_check"]
    for column in ("canonical_summary", "prepared_sha256", "sealed_at"):
        assert f"{column} IS NULL" in state
        assert f"{column} IS NOT NULL" in state
    assert "prepared_sha256 = pg_catalog.sha256" in state
    assert table.c.preparation_deadline_at.nullable is False
    assert table.c.producer_token_sha256.type.length == 32


def test_prepared_part_keeps_global_positions_and_independent_payload_manifest_hashes():
    table = CustomImportPreparedSourcePart.__table__
    assert tuple(table.primary_key.columns.keys()) == ("execution_id", "stream_slot", "part_ordinal")
    assert all(not column.nullable for column in table.columns)
    checks = " ".join(str(item.sqltext) for item in table.constraints if isinstance(item, sa.CheckConstraint))
    assert "first_source_ordinal > 0" in checks
    assert "record_count = 0 AND part_ordinal = 1 AND first_source_ordinal = 1 AND byte_count = 0" in checks
    assert "payload_sha256 = pg_catalog.sha256(payload)" in checks
    assert "manifest_sha256 = pg_catalog.sha256(convert_to(canonical_manifest, 'UTF8'))" in checks


def test_runtime_structure_sync_skips_both_prepared_tables_before_inspection(monkeypatch):
    metadata = sa.MetaData()
    model_by_table = {}
    for model in _MODELS:
        table = model.__table__.to_metadata(metadata)
        model_by_table[table.fullname] = model

    def unexpected_inspection(*_args, **_kwargs):
        raise AssertionError("migration-owned tables must not enter runtime DDL")

    monkeypatch.setattr(maintenance.Base, "metadata", metadata)
    monkeypatch.setattr(maintenance, "inspect", lambda _connection: SimpleNamespace(has_table=unexpected_inspection))
    monkeypatch.setattr(maintenance, "_model_by_table_fullname", lambda: model_by_table)
    monkeypatch.setattr(maintenance, "_managed_schemas", lambda: tuple({model.__table__.schema for model in _MODELS}))
    summary_by_kind = {
        kind: [] for kind in ("tables", "columns", "indexes", "constraints", "skipped_columns", "retired")
    }
    maintenance._sync_structure(None, summary_by_kind, add_columns=True, add_indexes=True)
    assert all(not entries for entries in summary_by_kind.values())


def test_upgrade_creates_only_empty_native_storage_and_quotes_all_acl_grantees(monkeypatch):
    migration, statements = _operations(monkeypatch)
    migration.upgrade()
    sql = "\n".join(statements)
    assert sum(statement.lstrip().startswith("CREATE TABLE ") for statement in statements) == 2
    for model in _MODELS:
        relation = f"synthetic_prepared.{model.__tablename__}"
        assert f"CREATE TABLE {relation}" in sql
        assert f"REVOKE ALL ON TABLE {relation} FROM PUBLIC" in sql
        assert f"REVOKE ALL ON TABLE {relation} FROM writer" in sql
        assert f'REVOKE ALL ON TABLE {relation} FROM "odd""role"' in sql
        assert f'REVOKE ALL ON TABLE {relation} FROM "PUBLIC"' in sql
    assert all(
        fragment not in sql.upper()
        for fragment in ("CREATE FUNCTION", "CREATE TRIGGER", "FOREIGN KEY", "INSERT INTO", "UPDATE ", "GRANT ")
    )
    assert "current_setting('lock_timeout')::interval < interval '2 seconds'" in sql
    assert migration.down_revision == "20261024050000_provider_directory_first_retirement_owner_lineage"


def test_binding_model_retains_owner_foreign_key_and_domain_separated_digest():
    table = CustomImportSourceBindingRevision.__table__
    shape = next(
        str(item.sqltext)
        for item in table.constraints
        if item.name == "custom_import_source_binding_revision_shape_check"
    )
    assert "connector_kind = 'snowflake_bundle'" in shape
    assert "connector_kind IN ('https', 's3', 'upload')" in shape
    assert "binding_sha256 = pg_catalog.sha256" in shape
    owner = next(item for item in table.foreign_key_constraints if item.name.endswith("definition_fkey"))
    assert tuple(element.parent.name for element in owner.elements) == (
        "definition_revision_id",
        "dataset_id",
        "schema_revision_id",
    )
    assert owner.ondelete == "RESTRICT"


@pytest.mark.parametrize("retained", [False, True])
def test_downgrade_locks_then_refuses_any_retention_before_shape_or_table_removal(monkeypatch, retained):
    migration, statements = _operations(monkeypatch, retained=retained)
    if retained:
        with pytest.raises(RuntimeError, match="custom_import_prepared_source_downgrade_blocked"):
            migration.downgrade()
        assert not any(statement.startswith(("DROP ", "ALTER TABLE ")) for statement in statements)
    else:
        migration.downgrade()
        assert statements[-2:] == [
            "DROP synthetic_prepared.custom_import_prepared_source_part",
            "DROP synthetic_prepared.custom_import_prepared_source",
        ]
    assert any("ACCESS EXCLUSIVE MODE" in statement for statement in statements)


@pytest.mark.parametrize("operation", ["upgrade", "downgrade"])
def test_schema_disagreement_prevents_any_migration_statement(monkeypatch, operation):
    migration, statements = _operations(monkeypatch)
    monkeypatch.setenv("DB_SCHEMA", "different_schema")
    with pytest.raises(RuntimeError, match="must match"):
        getattr(migration, operation)()
    assert statements == []


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
def test_non_read_committed_downgrade_is_rejected_before_any_ddl(monkeypatch, isolation):
    migration, statements = _operations(monkeypatch, isolation=isolation)
    with pytest.raises(RuntimeError, match="custom_import_prepared_source_downgrade_isolation"):
        migration.downgrade()
    assert statements == []
