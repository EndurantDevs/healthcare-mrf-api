# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual legacy registration migration and fixed provenance ABI contracts."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _migration():
    path = (
        Path(__file__).resolve().parents[1] / "alembic/versions/20261005050000_custom_import_legacy_snapshot_writers.py"
    )
    spec = importlib.util.spec_from_file_location("legacy_snapshot_writers_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_upgrade_registers_fixed_default_denied_functions_without_creating_data(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_schema")
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    expected = (*migration._AUTHORITY_FUNCTIONS, *(helper[:3] for helper in migration._HELPERS), *migration._WRITERS)
    functions = [statement for statement in statements if "CREATE FUNCTION" in statement]
    assert len(functions) == len(expected)
    assert all(
        "SECURITY DEFINER SET search_path=pg_catalog" in statement.replace("search_path = ", "search_path=")
        for statement in functions
    )
    for name, arguments, _result in expected:
        identity = f'"synthetic_schema".{name}({migration._bulk()._argument_types(arguments)})'
        assert f"REVOKE ALL ON FUNCTION {identity} FROM PUBLIC" in statements
    assert not any(
        statement.lstrip().startswith(("INSERT", "UPDATE", "DELETE", "CREATE TABLE")) for statement in statements
    )
    assert not any("__CONTROL__" in statement or "__SCHEMA__" in statement for statement in statements)
    assert not any("DISABLE TRIGGER" in statement or "ENABLE TRIGGER" in statement for statement in statements)


def test_registration_binds_actual_generation_and_refuses_split_storage():
    migration = _migration()
    authority = migration._resource("legacy_authority.sql")
    assert len(authority) == len(migration._AUTHORITY_FUNCTIONS)
    assert "lock_custom_import_snapshot_attempt" in authority[0]
    assert "sealed_at IS NOT NULL" in authority[0]
    assert "custom_import_build_attempt" in authority[0]
    assert "custom_import_no_change_seal" in authority[0]
    assert "lock_custom_import_sealed_snapshot_base" in authority[1]
    assert "f.frozen_at IS NOT NULL" in authority[1]
    assert "custom_import_materialization_not_open_legacy_attempt" in authority[3]
    assert "custom_import_root_record" not in migration._RESOLVE_BODY
    for relation in ("pack", "rejection", "family_revision", "generation_family", "winner"):
        assert f"__CONTROL__.custom_import_{relation}" in migration._RESOLVE_BODY
    assert "create_custom_import_snapshot_family" in migration._RESOLVE_BODY
    assert "bind_custom_import_snapshot_generation" in migration._RESOLVE_BODY
    assert "install_custom_import_legacy_snapshot_writers" in migration._RESOLVE_BODY
    assert "f.frozen_at IS NOT NULL" in migration._RESOLVE_BODY


def test_fixed_origin_has_native_shape_and_no_record_guard():
    migration = _migration()
    table, root_index, child_index = migration._resource("legacy_origin.sql")
    assert "PRIMARY KEY (kind,revision_id)" in table
    assert "revision_id=root_revision_id" in table
    assert "base_child_revision_id IS NOT NULL AND base_child_revision_id>0" in table
    assert "WHERE kind='root'" in root_index and "WHERE kind='child'" in child_index
    assert not any(
        "FOREIGN KEY" in statement or "TRIGGER" in statement for statement in (table, root_index, child_index)
    )
    writers = migration._resource("legacy_writers.sql")
    assert len(writers) == len(migration._WRITERS)
    root, child = writers[1:3]
    for statement, page_limit in ((root, 64), (child, 85)):
        assert f"n NOT BETWEEN 1 AND {page_limit}" in statement
        assert "work_bytes>8388608 AND n<>1" in statement
        assert statement.count("append_custom_import_revision_home") == 1
        assert "fresh_revision_ids" in statement
        assert "origin_replay_mismatch" in statement and "origin_stored_mismatch" in statement
        assert "__BASE__.custom_import_generation_family" in statement
        assert "__BASE__.custom_import_family_revision" in statement
        assert "WHERE x.base_family IS NOT NULL AND x.prior_revision IS NULL" in statement
        assert "record_kind" not in statement.replace("s.record_kind", "")
    assert "p_single_payload" in root and "p_single_parent_key" in child and "p_single_child_key" in child
    assert "__CONTROL__.custom_import_root_record" in writers[0]
    assert "__CANDIDATE__.custom_import_root_record" in writers[0]
    assert "__CONTROL__.custom_import_entity_binding" in writers[0]
    assert "__CANDIDATE__.custom_import_entity_binding" in writers[0]


def test_dispatchers_repeat_authority_after_the_leaf_call():
    migration = _migration()
    for name, arguments, result in migration._WRITERS:
        body = migration._dispatcher(name, arguments, result)
        helper = (
            "check_custom_import_generation_materialization_authority"
            if name.endswith("generation_family_set")
            else "check_custom_import_materialization_authority"
        )
        assert body.count(helper) == 2
        assert body.index(helper) < body.index("EXECUTE format") < body.rindex(helper)
        assert body.count("lock_custom_import_legacy_generation_snapshot") == 2


def test_rejection_set_allocates_global_ids_only_for_missing_durable_ordinals():
    """Snapshot copies have no ID default; exact replay must not allocate again."""

    migration = _migration()
    rejection_writer = next(
        body
        for body in migration._resource("legacy_writers.sql")
        if ".persist_custom_import_legacy_rejection_set(" in body
    )
    assert "custom_import_rejection(rejection_id,execution_id,rejection_ordinal" in rejection_writer
    assert "nextval('__CONTROL__.custom_import_rejection_rejection_id_seq'::regclass)" in rejection_writer
    assert "WHERE NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_rejection r" in rejection_writer
    assert "ORDER BY x.ordinal;" in rejection_writer
    assert "custom_import_legacy_rejection_set_replay_mismatch" in rejection_writer
    assert "custom_import_legacy_rejection_set_stored_mismatch" in rejection_writer


def test_downgrade_refuses_registered_origin_and_unknown_resources(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_schema")
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.downgrade()
    assert "ACCESS EXCLUSIVE MODE" in statements[0]
    assert "origin_table_oid IS NOT NULL" in statements[0]
    assert not any("CASCADE" in statement or "DROP TABLE" in statement for statement in statements)
    with pytest.raises(ValueError, match="unknown legacy writer resource"):
        migration._resource("unregistered.sql")
