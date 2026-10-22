# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG checker execution follows the complete existing writer authority."""

import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_ptg_snapshot_candidates_postgres import _migration


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self.rows)

    def one(self):
        return self.rows[0]

    def all(self):
        return self.rows


def _fixture():
    migration = _migration()
    body = migration.Path(migration.__file__).parents[2] / "db/sql/ptg_snapshot_candidates.sql"
    definition_by_field = dict(
        oid=100,
        proowner=10,
        prosrc=body.read_text()
        .split("CREATE FUNCTION __S__.check_ptg_snapshot_write", 1)[1]
        .split("$body$", 2)[1]
        .replace("__S__", '"synthetic_schema"')
        .replace("__Q__", '"synthetic_schema"'),
        prosecdef=True,
        proconfig=["search_path=pg_catalog"],
        provolatile="v",
        prorettype=2278,
        proargtypes="26 25 19",
        lanname="plpgsql",
        prosupport=0,
        provariadic=0,
        pronargdefaults=0,
        input_only=True,
    )
    state = SimpleNamespace(
        definition=definition_by_field,
        closed=True,
        role_count=3,
        rows=[dict(table_name=table, writer_roles=["writer_base"]) for table in migration.TABLES],
        calls=[],
    )

    def execute(sql, parameters=None):
        statement = str(sql)
        if "SELECT p.oid,p.proowner" in statement:
            return _Result([state.definition.copy()])
        if "SELECT table_name,writer_roles" in statement:
            return _Result(state.rows)
        state.calls.append(statement)
        return _Result([])

    def read_count(sql, parameters=None):
        return state.role_count if "count(*) FROM pg_roles" in str(sql) else state.closed

    def read_role_names(sql, parameters=None):
        values = ["table_owner", "begin_owner"] if "WITH RECURSIVE family" in str(sql) else ["unapproved_reader"]
        return _Result(values)

    bind = SimpleNamespace(execute=execute, scalar=read_count, scalars=read_role_names)
    migration.op = SimpleNamespace(get_bind=lambda: bind, execute=lambda sql: state.calls.append(str(sql)))
    return migration, state


def test_checker_grants_verified_roles():
    migration, state = _fixture()
    migration._close_checker_execution("synthetic_schema")
    grants = [sql for sql in state.calls if sql.startswith("GRANT EXECUTE")]
    assert len(grants) == 3
    assert any('TO "writer_base"' in sql for sql in grants)
    assert any('TO "table_owner"' in sql for sql in grants)
    assert any('TO "begin_owner"' in sql for sql in grants)
    assert not any("TO PUBLIC" in sql for sql in grants)
    assert any("FROM PUBLIC" in sql for sql in state.calls)
    assert any('FROM "unapproved_reader"' in sql for sql in state.calls)


@pytest.mark.parametrize("change", ("missing_table", "extra_table", "metadata_write", "missing_role", "changed_body"))
def test_incomplete_authority_or_changed_definition_refuses_before_any_acl_change(change):
    migration, state = _fixture()
    if change == "missing_table":
        state.rows.pop()
    elif change == "extra_table":
        state.rows.append(dict(table_name="unexpected_parent", writer_roles=["writer_base"]))
    elif change == "metadata_write":
        state.closed = False
    elif change == "missing_role":
        state.role_count = 2
    else:
        state.definition["prosrc"] = "BEGIN RETURN; END"
    with pytest.raises(RuntimeError, match="ptg_snapshot_checker_"):
        migration._close_checker_execution("synthetic_schema")
    assert not any(sql.startswith(("GRANT", "REVOKE")) for sql in state.calls)


def test_fresh_install_uses_the_same_checker_closure(monkeypatch):
    migration, state = _fixture()
    monkeypatch.setattr(migration, "_install_sql", lambda *_: None)
    permissions = []
    monkeypatch.setattr(
        migration, "_function_permissions", lambda schema, signature, roles: permissions.append((signature, set(roles)))
    )
    migration._install_candidate_functions("synthetic_schema", {"writer_base"})
    checker_roles = [
        roles for signature, roles in permissions if signature == "check_ptg_snapshot_write(oid,text,name)"
    ]
    assert checker_roles == [{"writer_base", "table_owner", "begin_owner"}]


def test_applied_upgrade_uses_the_same_authority_and_transactional_permission_helper(monkeypatch):
    migration, state = _fixture()
    path = Path(migration.__file__).with_name("20261022110000_ptg_checker_execution.py")
    spec = importlib.util.spec_from_file_location("checker_execution_upgrade", path)
    wrapper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wrapper)
    assert wrapper.revision == "20261022110000_ptg_checker_execution"
    assert wrapper.down_revision == "20261020210000_registry_manual_acceptance_join"
    wrapper.op = migration.op
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_schema")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    wrapper.upgrade()
    assert any('TO "writer_base"' in sql for sql in state.calls)
    assert any("FROM PUBLIC" in sql for sql in state.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_primary,cancel_count", ((True, 1), (True, 2), (False, 2)))
async def test_checker_cleanup_keeps_primary(monkeypatch, has_primary, cancel_count):
    from tests import ptg_checker_execution_postgres_support as support

    started = asyncio.Event()
    release = asyncio.Event()
    completed = asyncio.Event()
    cleanup_calls = []
    primary = ValueError("Synthetic primary failure") if has_primary else None

    async def create_roles(*_):
        return None

    async def cleanup_roles(engine, schema, role_names, engines, ownership):
        cleanup_calls.append((role_names, engines, ownership))
        started.set()
        await release.wait()
        completed.set()

    monkeypatch.setattr(support, "_create_login_roles", create_roles)
    monkeypatch.setattr(support, "_cleanup_roles", cleanup_roles)
    monkeypatch.setattr(support, "create_async_engine", lambda *_: object())
    engine = SimpleNamespace(url=SimpleNamespace(set=lambda **_: None))

    async def run_case():
        async with support._authenticated_roles(engine, "synthetic_schema", "writer"):
            if primary is not None:
                raise primary

    task = asyncio.create_task(run_case())
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        for _ in range(cancel_count):
            task.cancel()
            await asyncio.sleep(0)
        release.set()
        outcome = (await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=1))[0]
        if primary is not None:
            assert outcome is primary
        else:
            assert isinstance(outcome, asyncio.CancelledError)
        assert completed.is_set()
        assert len(cleanup_calls) == 1
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
