# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Snapshot-name migrations keep bodies and tables that already hold their result."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

VERSIONS = Path(__file__).resolve().parents[1] / "alembic/versions"


def _load(name):
    spec = importlib.util.spec_from_file_location(f"idempotency_{name}", VERSIONS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _installed_function(monkeypatch, migration, prosrc):
    definition = f"CREATE OR REPLACE FUNCTION synthetic.f() RETURNS integer AS $body${prosrc}$body$ LANGUAGE plpgsql"
    installed = SimpleNamespace(prosrc=prosrc, definition=definition)
    result = SimpleNamespace(one_or_none=lambda: installed)
    monkeypatch.setattr(migration.op, "get_bind", lambda: SimpleNamespace(execute=lambda *_args: result))
    statements = []
    monkeypatch.setattr(migration.op, "execute", lambda statement: statements.append(str(statement)))
    return statements


def test_refresh_keeps_current_body(monkeypatch):
    names = _load("20261114010000_custom_import_snapshot_names")
    statements = _installed_function(monkeypatch, names, "BEGIN RETURN 2; END;")
    names._refresh("synthetic.f()", lambda _source: pytest.fail("transform ran"), current=lambda s: "2" in s)
    names._install("synthetic.f()", "BEGIN RETURN 2; END;")
    assert statements == []
    with pytest.raises(RuntimeError, match="function_unchanged"):
        names._refresh("synthetic.f()", lambda source: source, current=lambda _source: False)
    names._refresh("synthetic.f()", lambda source: source.replace("2", "3"), current=lambda s: "3" in s)
    assert len(statements) == 1 and "RETURN 3" in statements[0]


def test_relation_and_index_checks_recognize_results():
    names = _load("20261114010000_custom_import_snapshot_names")
    retirement = names._previous("20261102010000_custom_import_snapshot_retirement")
    read = next(
        new for name, _, _, new in retirement._definitions("s") if name == "resolve_custom_import_generation_snapshot"
    )
    assert not names._is_relations_current(read)
    assert names._is_relations_current(names._optional_relations(read))
    assert not names._is_relations_current("BEGIN RETURN 1; END;")
    old_index = (
        "AND c.relowner=heap.relowner AND c.relpersistence='p' "
        "FROM __CONTROL__.custom_import_snapshot_relation WHERE family_id=f.family_id"
    )
    assert not names._is_index_current(old_index)
    assert names._is_index_current(names._index_body(old_index))


@pytest.mark.parametrize(
    ("revision", "profile"),
    (
        ("20261121050000_custom_import_rooted_text", "'rooted_child_text_v1'"),
        ("20261123182000_custom_import_root_boolean_index", "'root_boolean_true_v1'"),
    ),
)
def test_index_profiles_skip_scheduled_bodies(monkeypatch, revision, profile):
    migration = _load(revision)
    names = migration._names()
    calls = []
    monkeypatch.setattr(migration, "_names", lambda: names)
    monkeypatch.setattr(names, "_refresh", lambda identity, _transform, current=None: calls.append(current))
    migration._install(enabled=True)
    assert len(calls) == 2 and all(check(f"x={profile}") and not check("x") for check in calls)
    migration._install(enabled=False)
    assert calls[2:] == [None, None]


def _layout_connection(shapes, regclass):
    shape_list = list(shapes)
    return SimpleNamespace(
        scalar=lambda *_args: regclass,
        execute=lambda *_args: SimpleNamespace(one=lambda: shape_list.pop(0)),
    )


@pytest.mark.parametrize(
    ("regclass", "shapes", "error"),
    (
        (None, (), None),
        ("layout", ((["a"], ["c"]), (["a"], ["c"])), None),
        ("layout", ((["a"], ["c"]), (["b"], ["c"])), "layout_mismatch"),
    ),
)
def test_layout_table_created_or_verified(monkeypatch, regclass, shapes, error):
    migration = _load("20261104020000_custom_import_family_blocks")
    storage = migration._previous("20261005030000_custom_import_snapshot_storage")
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    connection = _layout_connection(shapes, regclass)
    if error:
        with pytest.raises(RuntimeError, match=error):
            migration._create_layout(connection, storage, "control")
    else:
        migration._create_layout(connection, storage, "control")
    created_tables = [statement.split("(")[0].strip() for statement in statements if "CREATE TABLE" in statement]
    expected = '"control"' if regclass is None else '"pg_temp"'
    assert created_tables == [f"CREATE TABLE {expected}.custom_import_family_block_layout"]
    assert (regclass is not None) == ("DROP TABLE pg_temp.custom_import_family_block_layout" in statements)
