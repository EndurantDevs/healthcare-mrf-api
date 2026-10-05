# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed registry/model and migration contracts; native proofs are separate."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRelation
from process.custom_import import storage_layout


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261005030000_custom_import_snapshot_storage.py"
    spec = importlib.util.spec_from_file_location("snapshot_storage_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_frozen_control_and_hot_layout_matches_the_declared_models():
    """The migration freezes the actual fixed shape, not runtime-generated DDL."""

    migration = _migration()
    assert migration.down_revision == "20261005130000_provider_dataset_candidates"
    models = CustomImportSnapshotFamily, CustomImportSnapshotRelation
    expected_table_statements = tuple(
        str(CreateTable(model.__table__).compile(dialect=postgresql.dialect())).replace("mrf.", "__SCHEMA__.")
        for model in models
    )
    assert migration._TABLE_DDL == expected_table_statements
    assert all(model.__runtime_schema_sync__ is False for model in models)
    assert migration._RELATION_NAMES == tuple(model.__tablename__ for model in storage_layout.SNAPSHOT_MODELS)
    expected_load_statements = tuple(
        statement.replace("ci_snapshot_1.", "__LEAF__.").replace("mrf.", "__SCHEMA__.")
        for statement in storage_layout.snapshot_load_statements(1)[1:]
    )
    assert migration._LOAD_DDL == expected_load_statements
    assert not any("FOREIGN KEY" in statement or "TRIGGER" in statement for statement in migration._LOAD_DDL)


def test_upgrade_is_schema_only_and_does_not_grant_default_entry_points(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_schema")
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    assert len([statement for statement in statements if statement.strip().startswith("CREATE TABLE")]) == 2
    functions = [statement for statement in statements if statement.strip().startswith("CREATE FUNCTION")]
    assert len(functions) == 10
    assert all("SECURITY DEFINER SET search_path=pg_catalog" in statement for statement in functions)
    assert not any(statement.strip().startswith(("INSERT", "UPDATE", "DELETE")) for statement in statements)
    assert not any("DROP TRIGGER" in statement or "DISABLE TRIGGER" in statement for statement in statements)
    assert sum("REVOKE ALL ON FUNCTION" in statement for statement in statements) == 10
    assert (
        sum("aclexplode(coalesce(c." in statement for statement in statements if statement.strip().startswith("DO "))
        == 13
    )
    assert not any(
        "__SCHEMA__" in statement or "__DDL__" in statement or "__NAMES__" in statement for statement in statements
    )


def test_snapshot_relation_resolution_and_freeze_have_no_verified_shortcut():
    migration = _migration()
    relation_body = migration._RELATIONS_BODY
    assert "r.table_oid::oid" in relation_body and "r.table_owner" in relation_body
    assert "custom_import_snapshot_columns_sha256(r.table_oid)" in relation_body
    assert "contype='f'" in relation_body and "NOT tgisinternal" in relation_body
    assert "IN SHARE MODE" in migration._FREEZE_BODY
    assert migration._FREEZE_BODY.count("resolve_custom_import_snapshot_relations") == 2
    assert "f.generation_id IS NULL" in migration._FREEZE_BODY
    assert "b.output_frozen_at IS NULL" in migration._FREEZE_BODY
    assert "verified_at=" not in migration._FREEZE_BODY
    assert "custom_import_generation_seal" not in migration._FREEZE_BODY
    assert migration._CREATE_BODY.count("lock_custom_import_snapshot_attempt") == 2
    assert "ddl" not in CustomImportSnapshotFamily.__table__.c
    assert "namespace" not in CustomImportSnapshotFamily.__table__.c


def test_downgrade_refuses_retained_snapshots_instead_of_cascading(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.downgrade()
    assert "ACCESS EXCLUSIVE MODE" in statements[0]
    assert "custom_import_snapshot_storage_downgrade_blocked" in statements[0]
    assert not any("CASCADE" in statement or "DROP SCHEMA" in statement for statement in statements)


def test_pinned_read_binding_rejects_broken_snapshots_instead_of_legacy_fallback():
    """Only an exact sealed generation may resolve a frozen, OID-pinned leaf."""

    body = _migration()._READ_BINDING_BODY
    assert "custom_import_generation_seal" in body and "s.seal_contract=" in body
    assert "p_dataset_id,p_definition_revision_id,p_schema_revision_id" in body
    assert "execution_id=g.execution_id AND producing_fence=g.producing_fence" in body
    assert "IF f.family_id IS NULL THEN RETURN NULL;" in body
    assert "f.capture_bundle_id,f.producing_fence,f.producing_token_sha256" in body
    assert "f.frozen_at IS NULL" in body
    assert "FOR SHARE" not in body and "FOR UPDATE" not in body
    assert "IN ACCESS SHARE MODE" in body
    assert body.count("resolve_custom_import_snapshot_relations") == 2


def test_writer_binding_requires_a_live_open_family_and_cannot_fall_back():
    """Every future batch writer must share the freeze row and exact leaf locks."""

    body = _migration()._WRITE_BINDING_BODY
    assert "FOR UPDATE" in body and "f.frozen_at IS NOT NULL" in body
    assert "custom_import_generation_seal" in body and "custom_import_no_change_seal" in body
    assert "custom_import_snapshot_write_binding_mismatch" in body
    assert "IN ROW EXCLUSIVE MODE" in body
    assert body.count("resolve_custom_import_snapshot_relations") == 2
    assert body.count("lock_custom_import_snapshot_attempt") == 2
    assert "RETURN NULL" not in body


def test_finality_binding_pins_an_exact_frozen_candidate_without_a_seal_shortcut():
    """New sealing must resolve complete producer identity with writes closed."""

    body = _migration()._FINALITY_BINDING_BODY
    assert "f.family_id IS NULL OR g.generation_id IS NULL OR f.frozen_at IS NULL" in body
    assert "p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256" in body
    assert "p_generation_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id" in body
    assert "IN ACCESS SHARE MODE" in body
    assert body.count("resolve_custom_import_snapshot_relations") == 2
    assert body.count("lock_custom_import_snapshot_attempt") == 2
    assert "RETURN NULL" not in body
    assert "lock_custom_import_writable_snapshot" not in body
    assert "resolve_custom_import_generation_snapshot" not in body
    assert "custom_import_generation_seal" not in body
