# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Plan release storage is located by name: share locks first, then one catalog check."""

import json
from types import SimpleNamespace

import pytest

from api import plan_release_read_storage as storage

_SHARED_TABLES = ("plan_release_serving_revision", "ptg2_snapshot", "ptg2_v3_snapshot_plan_scope", "ptg2_snapshot_pin")


class _Session:
    def __init__(self, *, readable=(True, True), is_admitted=True, lock_error=None):
        self.readable, self.is_admitted, self.lock_error = readable, is_admitted, lock_error
        self.calls = []

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if "AS readable" in sql:
            return [SimpleNamespace(_mapping={"readable": value}) for value in self.readable]
        if sql.startswith("LOCK TABLE"):
            if self.lock_error is not None:
                raise self.lock_error
            return []
        assert "AS admitted" in sql
        return SimpleNamespace(scalar_one=lambda: self.is_admitted)

    def checked_tables(self):
        return [json.loads(parameters["tables"]) for sql, parameters in self.calls if "AS admitted" in sql]


async def test_readable_roots_choose_native_and_hold_shared_tables_by_name():
    session = _Session()
    assert await storage.is_retained_session_read(session, "copy") is False
    names = ['"copy"."' + name + '"' for name in _SHARED_TABLES]
    assert session.calls[1] == (
        "LOCK TABLE " + ", ".join("ONLY " + name for name in names) + " IN ACCESS SHARE MODE",
        None,
    )
    assert session.checked_tables() == [[{"name": name} for name in names]]
    assert "row_security_active" in session.calls[2][0] and "oid" not in session.calls[2][1]["tables"]


async def test_unreadable_roots_choose_retained_storage():
    assert await storage.is_retained_session_read(_Session(readable=(False, False)), "copy") is True


@pytest.mark.parametrize(
    ("readable", "reason"),
    [
        ((True, None), "read_roots_missing"),
        ((True,), "read_roots_missing"),
        ((True, False), "retained_read_not_admitted"),
    ],
)
async def test_missing_or_mixed_roots_refuse_before_any_lock(readable, reason):
    session = _Session(readable=readable)
    with pytest.raises(ValueError, match="^plan_release_storage_" + reason + "$"):
        await storage.is_retained_session_read(session, "copy")
    assert len(session.calls) == 1


async def test_row_filtered_or_non_plain_table_is_refused():
    session = _Session(is_admitted=False)
    with pytest.raises(ValueError, match="^plan_release_storage_read_table_refused$"):
        await storage.lock_binding_storage(session, "copy", retained=False)


async def test_lock_failure_refuses_before_catalog_check():
    session = _Session(lock_error=RuntimeError("lock timeout"))
    with pytest.raises(RuntimeError, match="lock timeout"):
        await storage.is_retained_session_read(session, "copy")
    assert [sql for sql, _ in session.calls if "AS admitted" in sql] == []


async def test_native_route_holds_only_the_partitioned_roots():
    session = _Session()
    await storage.lock_binding_storage(session, "copy", retained=False)
    roots = ['"copy"."plan_release_snapshot_binding"', '"copy"."plan_release_snapshot_pin"']
    assert session.calls[0][0] == f"LOCK TABLE ONLY {roots[0]}, ONLY {roots[1]} IN ACCESS SHARE MODE"
    assert session.checked_tables() == [[{"name": name} for name in roots]]
    assert "c.relkind='p' AND c.relname IN ('plan_release_snapshot_binding'" in session.calls[1][0]


async def test_retained_leaf_requires_its_root_and_read_columns_by_type():
    session = _Session()
    await storage.lock_binding_storage(session, "copy", retained=True)
    legacy, header = session.checked_tables()[0]
    assert legacy["name"] == '"copy"."plan_release_snapshot_binding_legacy"'
    assert legacy["parent"] == '"copy"."plan_release_snapshot_binding"'
    assert legacy["columns"]["snapshot_id"] == "character varying(128)"
    assert header == {
        "name": '"copy"."plan_release_serving_revision"',
        "columns": {column[0]: column[1] for column in storage.HEADER_COLUMNS},
    }
