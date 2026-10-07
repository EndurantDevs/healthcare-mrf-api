# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve historical installation and exact validator identity during refresh."""

import importlib.util
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

import pytest

_Function = namedtuple("Function", "oid proowner proacl prosrc")


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261007000000_custom_import_rejection_anti_joins.py"
    spec = importlib.util.spec_from_file_location("rejection_anti_join_migration_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _body(migration, corrected):
    return " " + migration._body(migration._finality(), "synthetic_control", corrected=corrected) + " "


def test_previous_renderer_stays_exact_and_new_body_changes_only_the_probe(monkeypatch):
    migration = _migration()
    finality = migration._finality()
    statements = []
    finality._schema = lambda: "synthetic_control"
    monkeypatch.setattr(finality.op, "execute", statements.append)
    finality.upgrade()
    statement = next(
        sql
        for sql in statements
        if sql.startswith("CREATE FUNCTION") and ".verify_custom_import_snapshot_structure(" in sql
    )
    assert statement.split("$snapshot_finality$")[1] == _body(migration, False)
    old_query = finality._resource("source_lineage")
    expected = old_query.replace(migration._OLD_PROBE, migration._NEW_PROBE)
    expected = expected.replace("ON event.build_id=o.build_id AND event.origin='source'", "ON event.origin='source'")
    expected = expected.replace(
        "WHERE r.rejection_id=o.resolved_rejection_id",
        "WHERE r.rejection_id=o.resolved_rejection_id AND event.build_id=o.build_id",
    )
    start = expected.index("        SELECT 'occurrence_owner'::text")
    end = expected.index("        UNION ALL\n        SELECT 'source_occurrence_position'")
    expected = expected[:start] + migration._owner_query(expected[start:end]) + expected[end:]
    assert migration._source_query() == expected
    assert migration.down_revision == "20261005080000_custom_import_writer_cutover"


@pytest.mark.parametrize("already_updated", (False, True))
def test_refresh_preserves_existing_function_and_only_replaces_one_body(monkeypatch, already_updated):
    migration = _migration()
    functions = iter(
        (
            _Function(101, 10, "owner-only", _body(migration, already_updated)),
            _Function(101, 10, "owner-only", _body(migration, True)),
        )
    )
    statements = []
    monkeypatch.setattr(migration, "_installed", lambda *_: next(functions))
    monkeypatch.setattr(migration.op, "get_bind", lambda: object())
    monkeypatch.setattr(migration.op, "execute", statements.append)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_control")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.upgrade()
    assert len(statements) == 2
    assert statements[0].startswith(
        'CREATE OR REPLACE FUNCTION "synthetic_control".verify_custom_import_snapshot_structure('
    )
    assert statements[0].split("$snapshot_finality$")[1] == _body(migration, True)
    assert (
        statements[1]
        == 'REVOKE ALL ON FUNCTION "synthetic_control".verify_custom_import_snapshot_structure(bigint) FROM PUBLIC'
    )


@pytest.mark.parametrize("existing", (None, _Function(101, 10, "owner-only", "unrecognized")))
def test_missing_or_unfamiliar_validator_is_not_replaced(monkeypatch, existing):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_installed", lambda *_: existing)
    monkeypatch.setattr(migration.op, "get_bind", lambda: object())
    monkeypatch.setattr(migration.op, "execute", statements.append)
    with pytest.raises(RuntimeError, match="identity_mismatch"):
        migration.upgrade()
    assert statements == []


@pytest.mark.parametrize("changes", ({"oid": 102}, {"proowner": 11}, {"proacl": "changed"}, {"prosrc": "changed"}))
def test_refresh_metadata_or_body_drift_rolls_back(monkeypatch, changes):
    migration = _migration()
    original = _Function(101, 10, "owner-only", _body(migration, False))
    updated = original._replace(prosrc=_body(migration, True))._replace(**changes)
    functions = iter((original, updated))
    monkeypatch.setattr(migration, "_installed", lambda *_: next(functions))
    monkeypatch.setattr(migration.op, "get_bind", lambda: object())
    monkeypatch.setattr(migration.op, "execute", lambda *_: None)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_control")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    with pytest.raises(RuntimeError, match="refresh_mismatch"):
        migration.upgrade()


def test_metadata_query_binds_quoted_namespace_and_enforces_owner_only():
    migration = _migration()
    calls = []
    bind = SimpleNamespace(
        execute=lambda statement, parameters: (
            calls.append((str(statement), parameters)) or SimpleNamespace(first=lambda: None)
        )
    )
    assert migration._installed(bind, 'synthetic "schema') is None
    statement, parameters = calls[0]
    assert parameters["identity"] == '"synthetic ""schema".verify_custom_import_snapshot_structure(bigint)'
    assert parameters["owner_table"] == '"synthetic ""schema".custom_import_generation'
    assert all(
        term in statement
        for term in (
            "p.prosecdef",
            "search_path=pg_catalog",
            "p.proowner=owner_table.relowner",
            "privilege.grantee<>p.proowner",
            "NOT p.proretset AND NOT p.proisstrict AND NOT p.proleakproof",
            "p.provolatile='v' AND p.proparallel='u' AND p.pronargdefaults=0",
            "p.procost=100 AND p.prorows=0",
        )
    )


def test_source_drift_and_downgrade_are_not_silently_accepted(monkeypatch):
    migration = _migration()
    monkeypatch.setattr(migration, "_finality", lambda: SimpleNamespace(_resource=lambda _: "changed"))
    with pytest.raises(RuntimeError, match="source_mismatch"):
        migration._source_query()
    with pytest.raises(RuntimeError, match="forward_migration"):
        migration.downgrade()
