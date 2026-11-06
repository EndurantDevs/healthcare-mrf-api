# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Future scratch persistence and exact compatibility of protected SQL guards."""

import importlib.util
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261104010000_custom_import_unlogged_landing.py"
    spec = importlib.util.spec_from_file_location("unlogged_landing_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_landing_migration_extends_the_unique_current_head():
    migration = _migration()
    root = Path(__file__).resolve().parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == [migration.revision]
    assert migration.down_revision == "20261102040000_custom_import_post_seal_audit"
    assert script.get_revision(migration.revision).down_revision == migration.down_revision


@pytest.mark.parametrize("schema", ("synthetic_control", 'synthetic "control'))
def test_only_future_landing_ddl_and_its_exact_persistence_guard_change(schema):
    definitions = list(_migration()._definitions(schema))
    assert len(definitions) == 3
    signature, result, old, new, delimiter, grants = definitions[0]
    assert signature == "install_custom_import_snapshot_writers(bigint)"
    assert result == "bigint" and delimiter == "$bulk_snapshot$" and grants is False
    expected = old.replace(
        "CREATE TABLE __CANDIDATE__.source_bulk_landing",
        "CREATE UNLOGGED TABLE __CANDIDATE__.source_bulk_landing",
    ).replace(
        "c.relname='source_bulk_landing' AND c.relkind='r' AND c.relpersistence='p'",
        "c.relname='source_bulk_landing' AND c.relkind='r' AND c.relpersistence='u'",
    )
    assert new == expected and new != old
    assert new.count("CREATE UNLOGGED TABLE") == 1
    assert "ALTER TABLE" not in new and "SET UNLOGGED" not in new
    assert "IF f.landing_table_oid IS NOT NULL THEN" in new
    assert "PERFORM" in new and "lock_custom_import_writable_snapshot" in new


def test_only_registered_landing_accepts_old_logged_and_new_unlogged_storage():
    definitions = list(_migration()._definitions("synthetic_control"))
    _, _, old, new, _, grants = definitions[1]
    assert grants is True
    assert new == old.replace(
        "c.relkind='r' AND c.relpersistence='p' AND n.nspname=namespace\n          AND c.relname='source_bulk_landing'",
        "c.relkind='r' AND c.relpersistence IN ('p','u') AND n.nspname=namespace\n"
        "          AND c.relname='source_bulk_landing'",
    )
    assert new.count("c.relpersistence IN ('p','u')") == 1
    assert "c.relpersistence <> 'p'" in new
    assert "c.relowner=n.nspowner AND c.relkind='r' AND c.relpersistence='p'" in new
    assert "c.oid=f.landing_table_oid::oid AND c.relowner::bigint=f.landing_table_owner" in new
    assert "c.relname='source_bulk_landing'" in new
    _, _, old, new, _, grants = definitions[2]
    assert grants is True
    assert new == old.replace(
        "c.relkind='r' AND c.relpersistence='p' AND c.relowner::bigint=f.landing_table_owner",
        "c.relkind='r' AND c.relpersistence IN ('p','u') AND c.relowner::bigint=f.landing_table_owner",
    )
    for guard in (
        "a.transaction_id<>pg_current_xact_id()",
        "a.opened_by<>session_user",
        "lock_custom_import_writable_snapshot",
        "source_bulk_cursor_changed",
        "custom_import_snapshot_columns_sha256",
        "pg_inherits",
        "pg_constraint",
        "pg_trigger",
    ):
        assert guard in new


def test_current_installer_includes_continuation_and_child_presence_corrections():
    migration = _migration()
    continuation = migration._continuation()
    bulk = continuation._bulk()
    child = continuation._previous("20261009000000_custom_import_child_presence_decode")
    source = migration._installer(continuation, child, bulk, "synthetic_control")
    for name in continuation._LEAF_NAMES:
        assert (
            bulk._storage()._literal(continuation._leaf_statement(name, "synthetic_control", corrected=True)) in source
        )
    assert "admitted_children AS MATERIALIZED" in source


def test_downgrade_does_not_make_existing_unlogged_candidates_unreadable():
    with pytest.raises(RuntimeError, match="requires_forward_migration"):
        _migration().downgrade()
