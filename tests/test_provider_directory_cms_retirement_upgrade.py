# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The six-body installer requires original Admin pins and owns only its savepoint."""

from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from db import migration_cms_retirement_resolution as migration


@pytest.fixture
def upgrade_case(monkeypatch):
    pairs = migration.retirement_resolution_bodies("sample")
    snapshots_by_name = {
        name: dict(
            body=old,
            definition="CREATE OR REPLACE FUNCTION trusted() AS $function$" + old + "$function$;",
            identity={"oid": str(100 + index), "proowner": "200", "proacl": ["unchanged"]},
            description="retained comment",
        )
        for index, (name, (old, _new)) in enumerate(pairs.items())
    }
    state = SimpleNamespace(rows=snapshots_by_name, path="pg_catalog", admin=True, writes=0, fail_after=None)
    state.pins = {
        name: dict(oid=int(function_row["identity"]["oid"]), owner_oid=200) for name, function_row in state.rows.items()
    }

    @contextmanager
    def savepoint():
        before = deepcopy(state.rows)
        try:
            yield
        except BaseException:
            state.rows = before
            raise

    def execute(query):
        if query == "SHOW search_path":
            return SimpleNamespace(scalar_one=lambda: state.path)
        state.writes += 1
        for name, (old, new) in pairs.items():
            if query == state.rows[name]["definition"].replace(old, new, 1):
                state.rows[name]["body"] = new
                state.rows[name]["definition"] = query
                break
        else:
            raise AssertionError("Unrecognized DDL")
        if state.fail_after == state.writes:
            raise RuntimeError("after actual body replacement")

    state.connection = SimpleNamespace(
        in_transaction=Mock(return_value=True),
        exec_driver_sql=Mock(side_effect=execute),
        execute=Mock(side_effect=lambda *args: SimpleNamespace(scalar_one_or_none=lambda: state.admin)),
        begin_nested=Mock(side_effect=savepoint),
    )
    monkeypatch.setattr(migration, "_snapshot", lambda _connection, _schema, name: deepcopy(state.rows[name]))
    return state


def _upgrade(case):
    migration.upgrade_cms_retirement_resolution(case.connection, "sample", expected=case.pins)


def test_all_six_install_then_repeat_without_any_new_write(upgrade_case):
    case = upgrade_case
    before = deepcopy(case.rows)
    _upgrade(case)
    assert case.writes == 6
    for name, original in before.items():
        assert case.rows[name]["body"] == migration.retirement_resolution_bodies("sample")[name][1]
        assert case.rows[name]["identity"] == original["identity"]
        assert case.rows[name]["description"] == original["description"]
    _upgrade(case)
    assert case.writes == 6
    assert not hasattr(case.connection, "commit")


@pytest.mark.parametrize("fault", ["transaction", "admin", "path", "missing", "extra", "oid", "owner", "body"])
def test_every_precondition_refuses_before_any_replacement(upgrade_case, fault):
    case = upgrade_case
    last = next(reversed(case.pins))
    match fault:
        case "transaction":
            case.connection.in_transaction.return_value = False
        case "admin":
            case.admin = False
        case "path":
            case.path = "public"
        case "missing":
            case.pins.pop(last)
        case "extra":
            case.pins["unreviewed"] = case.pins[last]
        case "oid" | "owner":
            case.pins[last]["oid" if fault == "oid" else "owner_oid"] += 1
        case "body":
            case.rows[last]["body"] += "unreviewed"
        case _:
            raise AssertionError(fault)
    with pytest.raises(RuntimeError):
        _upgrade(case)
    assert case.writes == 0


def test_partial_replacement_is_rolled_back_inside_the_existing_transaction(upgrade_case):
    case = upgrade_case
    before = deepcopy(case.rows)
    case.fail_after = 2
    with pytest.raises(RuntimeError, match="after actual body replacement"):
        _upgrade(case)
    assert case.rows == before and case.writes == 2
