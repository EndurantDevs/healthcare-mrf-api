# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact guard rendering and fail-closed refresh of existing protected writers."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def _migration():
    path = (
        Path(__file__).resolve().parents[1] / "alembic/versions/20261009000000_custom_import_child_presence_decode.py"
    )
    spec = importlib.util.spec_from_file_location("child_presence_decode_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_guard_changes_only_expected_rows_and_materializes_before_hot_join():
    migration = _migration()
    bulk = migration._bulk()
    previous = migration._leaf(bulk, "synthetic_control", corrected=False)
    corrected = migration._leaf(bulk, "synthetic_control", corrected=True)
    old = bulk._control_sql(bulk._storage(), "synthetic_control", migration._OLD_EXPECTED)
    new = bulk._control_sql(bulk._storage(), "synthetic_control", migration._NEW_EXPECTED)
    assert corrected == previous.replace(old, new)
    assert previous.count(old) == corrected.count(new) == 1
    assert new.count("AS MATERIALIZED") == 2
    assert new.count("json_array_elements(") == 1
    admitted, decoded = new.split("), decoded_fields AS MATERIALIZED (")
    assert "json_array_elements" not in admitted
    assert "FROM unnest(p_child_ids)" in admitted
    assert "WHERE EXISTS(SELECT 1" in admitted
    for predicate in (
        "f.dataset_id=b.dataset_id",
        "f.schema_revision_id=b.schema_revision_id",
        "f.collection_slot=c.collection_slot",
        "f.projection_slot>0",
    ):
        assert predicate in admitted and predicate in decoded
    assert "FROM admitted_children c\n        CROSS JOIN LATERAL json_array_elements(" in decoded
    assert "replace(c.canonical_payload,chr(92)||'u0000',chr(92)||'u0001')::json->'fields'" in decoded
    assert "c.value_state IS DISTINCT FROM 'missing'" in decoded
    assert "DISTINCT " not in new.replace("IS DISTINCT FROM", "")
    assert "GROUP BY" not in new and "jsonb" not in new
    assert corrected.count("SELECT * FROM expected EXCEPT ALL SELECT * FROM supplied") == 2
    assert corrected.count("SELECT * FROM supplied EXCEPT ALL SELECT * FROM expected") == 2
    assert corrected.index("'graph_children_scalar_mismatch'") < corrected.index(new)
    assert corrected.index(new) < corrected.index("'graph_children_scalar_presence'")
    assert corrected.index("'graph_children_scalar_presence'") < corrected.index("'graph_children_context_mismatch'")


def test_historical_installer_and_other_leaf_bodies_remain_exact():
    migration = _migration()
    bulk = migration._bulk()
    storage = bulk._storage()
    previous = migration._installer(bulk, "synthetic_control", corrected=False)
    corrected = migration._installer(bulk, "synthetic_control", corrected=True)
    assert (
        previous.split("$bulk_snapshot$")[1] == " " + bulk._body(storage, "synthetic_control", bulk._INSTALL_BODY) + " "
    )
    assert corrected == previous.replace(
        storage._literal(migration._leaf(bulk, "synthetic_control", corrected=False)),
        storage._literal(migration._leaf(bulk, "synthetic_control", corrected=True)),
    )
    assert migration.down_revision == "20261006010000_nucc_reference_result_generation"


def test_registered_namespace_uses_installer_body_spelling_and_quoted_header():
    migration = _migration()
    bulk = migration._bulk()
    statement = migration._leaf(bulk, 'synthetic "control', corrected=True, namespace="ci_snapshot_123")
    assert statement.startswith('CREATE FUNCTION "ci_snapshot_123".')
    assert "JOIN ci_snapshot_123.custom_import_child_revision" in statement
    assert '"synthetic ""control".custom_import_field' in statement
    assert "__CANDIDATE__" not in statement and "__CONTROL__" not in statement
    with pytest.raises(RuntimeError, match="namespace_mismatch"):
        migration._leaf(bulk, "synthetic_control", corrected=True, namespace="ci_snapshot_0")


@pytest.mark.parametrize("already_updated", (False, True))
def test_refresh_preserves_all_nonbody_metadata(monkeypatch, already_updated):
    migration = _migration()
    metadata = dict(oid=101, proowner=10, proacl=["owner=X/owner"], proconfig=["search_path=pg_catalog"])
    before = SimpleNamespace(prosrc="new" if already_updated else "old", metadata=metadata)
    after = SimpleNamespace(prosrc="new", metadata=dict(metadata))
    states = iter((before, after))
    statements = []
    monkeypatch.setattr(migration, "_installed", lambda *_args, **_kwargs: next(states))
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration._refresh(
        object(),
        "identity",
        "owner_table",
        "bigint",
        "CREATE FUNCTION $fn$old$fn$",
        "CREATE FUNCTION $fn$new$fn$",
        "$fn$",
    )
    assert statements == ["CREATE OR REPLACE FUNCTION $fn$new$fn$"]


@pytest.mark.parametrize("before", (None, SimpleNamespace(prosrc="unknown", metadata={})))
def test_missing_or_unknown_body_is_never_recreated(monkeypatch, before):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_installed", lambda *_args, **_kwargs: before)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    with pytest.raises(RuntimeError, match="identity_mismatch"):
        migration._refresh(object(), "identity", "owner_table", "bigint", "$fn$old$fn$", "$fn$new$fn$", "$fn$")
    assert statements == []


@pytest.mark.parametrize(
    "after",
    (
        None,
        SimpleNamespace(prosrc="new", metadata={"oid": 102}),
        SimpleNamespace(prosrc="wrong", metadata={"oid": 101}),
    ),
)
def test_post_refresh_catalog_or_body_drift_fails(monkeypatch, after):
    migration = _migration()
    states = iter((SimpleNamespace(prosrc="old", metadata={"oid": 101}), after))
    monkeypatch.setattr(migration, "_installed", lambda *_args, **_kwargs: next(states))
    monkeypatch.setattr(migration.op, "execute", lambda _: None)
    with pytest.raises(RuntimeError, match="refresh_mismatch"):
        migration._refresh(object(), "identity", "owner_table", "bigint", "$fn$old$fn$", "$fn$new$fn$", "$fn$")


def test_catalog_query_pins_result_security_cost_and_owner_only_acl():
    migration = _migration()
    calls = []
    bind = SimpleNamespace(
        execute=lambda sql, parameters: calls.append((str(sql), parameters)) or SimpleNamespace(first=lambda: None)
    )
    assert migration._installed(bind, "identity", "owner_table", "bigint", returns_set=False) is None
    sql, parameters = calls[0]
    assert parameters == dict(identity="identity", owner_table="owner_table", result="bigint", returns_set=False)
    for term in (
        "to_jsonb(p)-'prosrc'",
        "p.prokind='f' AND p.prosecdef",
        "language.lanname='plpgsql'",
        "pg_get_function_result(p.oid)",
        "p.proretset=:returns_set",
        "NOT p.proisstrict AND NOT p.proleakproof AND p.prosqlbody IS NULL",
        "p.provolatile='v' AND p.proparallel='u' AND p.pronargdefaults=0 AND p.prosupport=0",
        "p.procost=100 AND p.prorows=CASE WHEN :returns_set THEN 1000 ELSE 0 END",
        "p.proconfig=ARRAY['search_path=pg_catalog']",
        "p.proowner=owner_table.relowner",
        "privilege.grantee<>p.proowner",
    ):
        assert term in sql


@pytest.mark.parametrize("valid", (True, False, None))
def test_registered_storage_identity_is_required_without_writable_filter(valid):
    migration = _migration()
    calls = []
    row = SimpleNamespace(family_id=123, nspname="ci_snapshot_123", valid=valid)
    bind = SimpleNamespace(execute=lambda sql, parameters: calls.append((str(sql), parameters)) or [row])
    if valid:
        assert list(migration._registered_namespaces(bind, 'synthetic "control', migration._bulk())) == [
            "ci_snapshot_123"
        ]
    else:
        with pytest.raises(RuntimeError, match="storage_mismatch"):
            list(migration._registered_namespaces(bind, 'synthetic "control', migration._bulk()))
    sql, parameters = calls[0]
    assert parameters["owner_table"] == '"synthetic ""control".custom_import_generation'
    assert "f.frozen_at" not in sql and "lock_custom_import_writable_snapshot" not in sql
    for term in (
        "f.landing_table_oid::oid",
        "f.landing_table_owner",
        "f.landing_columns_sha256",
        "n.nspowner=c.relowner",
        "privilege.privilege_type='CREATE'",
    ):
        assert term in sql


def test_upgrade_refreshes_only_installer_and_each_registered_source_leaf(monkeypatch):
    migration = _migration()
    statements, refreshed = [], []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_control")
    monkeypatch.setattr(migration.op, "get_bind", lambda: object())
    monkeypatch.setattr(migration.op, "execute", statements.append)
    monkeypatch.setattr(migration, "_registered_namespaces", lambda *_: ("ci_snapshot_1", "ci_snapshot_2"))
    monkeypatch.setattr(migration, "_refresh", lambda *args: refreshed.append(args))
    migration.upgrade()
    assert statements == ['LOCK TABLE "synthetic_control".custom_import_snapshot_family IN SHARE ROW EXCLUSIVE MODE']
    assert len(refreshed) == 3
    assert refreshed[0][1] == '"synthetic_control".install_custom_import_snapshot_writers(bigint)'
    assert all(
        f'"ci_snapshot_{index}".append_custom_import_build_source_families_page(' in args[1]
        for index, args in enumerate(refreshed[1:], 1)
    )


def test_source_drift_and_downgrade_fail_closed():
    migration = _migration()
    for source in ("missing", "old old"):
        with pytest.raises(RuntimeError, match="source_mismatch"):
            migration._replace_once(source, "old", "new")
    with pytest.raises(RuntimeError, match="forward_migration"):
        migration.downgrade()
