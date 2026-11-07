# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The aggregate witness keeps the exact verifier refresh and rollback boundary."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def _migration():
    path = (
        Path(__file__).resolve().parents[1]
        / "alembic/versions/20261107010000_custom_import_child_reference_aggregate.py"
    )
    spec = importlib.util.spec_from_file_location("child_reference_aggregate_unit", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_refresh_changes_only_one_exact_query_literal():
    migration = _migration()
    previous, updated = migration._bodies("synthetic_control")
    ranges = migration._ranges()
    staged = ranges._arms()._staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    old_literal = storage._literal(bulk._control_sql(storage, "synthetic_control", migration._PREVIOUS_QUERY))
    assert ranges._arms()._queries(staged)[18] == migration._PREVIOUS_QUERY
    assert previous == " " + ranges._body("synthetic_control") + " "
    query = migration._UPDATED_QUERY
    new_literal = storage._literal(bulk._control_sql(storage, "synthetic_control", query))
    assert previous.count(old_literal) == updated.count(new_literal) == 1
    assert previous.replace(old_literal, new_literal, 1) == updated
    assert old_literal not in updated and new_literal not in previous
    assert "min(c.child_revision_id)" in query and "HAVING count(*)>0" in query


@pytest.mark.parametrize("rollback", (False, True))
@pytest.mark.parametrize("installed", ("old", "new", "unknown", None))
def test_refresh_refuses_unknown_body_before_ddl_and_uses_existing_custody(monkeypatch, rollback, installed):
    migration = _migration()
    refreshes, statements = [], []
    continuation = SimpleNamespace(_refresh=lambda *args: refreshes.append(args))
    staged = SimpleNamespace(
        _previous=lambda _name: continuation,
        _finality=lambda: SimpleNamespace(
            _bulk=lambda: SimpleNamespace(_storage=lambda: SimpleNamespace(_quote=lambda s: f'"{s}"'))
        ),
    )
    ranges = SimpleNamespace(
        _schema=lambda: "synthetic_control",
        _arms=lambda: SimpleNamespace(_staged=lambda: staged),
    )
    monkeypatch.setattr(migration, "_ranges", lambda: ranges)
    monkeypatch.setattr(migration, "_bodies", lambda schema: ("old", "new"))
    monkeypatch.setattr(
        migration.op,
        "get_bind",
        lambda: SimpleNamespace(execute=lambda *args: SimpleNamespace(scalar_one_or_none=lambda: installed)),
    )
    monkeypatch.setattr(migration.op, "execute", lambda sql: statements.append(str(sql)))
    if installed not in ({"new"} if rollback else {"old", "new"}):
        with pytest.raises(RuntimeError, match="function_identity_mismatch"):
            migration._refresh(rollback=rollback)
        assert statements == refreshes == []
        return
    migration._refresh(rollback=rollback)
    assert statements == ['LOCK TABLE "synthetic_control".custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE']
    assert refreshes == [
        (
            '"synthetic_control".verify_custom_import_build_structure(bigint)',
            installed,
            "old" if rollback else "new",
        )
    ]
    assert continuation.op is migration.op and continuation._schema() == "synthetic_control"
