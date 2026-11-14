# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named replacement grants preserve actual PostgreSQL readers and rollback."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process import provider_directory_artifact_read_access as access
from process import provider_directory_serving_recovery_receipts as receipts
from tests.provider_directory_entities_postgres_support import _database_url
from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case


def _quoted(name):
    return '"' + name.replace('"', '""') + '"'


@asynccontextmanager
async def _reader_roles():
    administrator = create_async_engine(_database_url())
    suffix = uuid4().hex
    roles_by_kind = {kind: "read_" + kind + "_" + suffix for kind in ("table", "column", "writer", "delegate", "late")}
    created_roles = []
    try:
        async with administrator.begin() as connection:
            for role in roles_by_kind.values():
                await connection.execute(text(f"CREATE ROLE {_quoted(role)} NOLOGIN"))
                created_roles.append(role)
        yield administrator, roles_by_kind
    finally:
        try:
            async with administrator.begin() as connection:
                for role in created_roles:
                    await connection.execute(text(f"DROP ROLE IF EXISTS {_quoted(role)}"))
        finally:
            await administrator.dispose()


async def _seed_reads(case, roles_by_kind):
    schema, database = case.schema, case.database
    for live, stage in (("read_live", "read_stage"), ("public_live", "public_stage")):
        for name, marker in ((live, "old"), (stage, "new")):
            await database.status(f'CREATE TABLE {schema}.{name}(marker text,"selected value" integer)')
            await database.status(f"INSERT INTO {schema}.{name} VALUES ('{marker}',2)")
    roles = ",".join(_quoted(role) for role in roles_by_kind.values())
    await database.status(f"GRANT USAGE ON SCHEMA {schema} TO {roles}")
    await database.status(f"GRANT SELECT ON {schema}.read_live TO {_quoted(roles_by_kind['table'])}")
    await database.status(
        f'GRANT SELECT("selected value") ON {schema}.read_live TO {_quoted(roles_by_kind["column"])} WITH GRANT OPTION'
    )
    await database.status(f"GRANT INSERT,UPDATE(marker) ON {schema}.read_live TO {_quoted(roles_by_kind['writer'])}")
    await database.status(f"GRANT SELECT ON {schema}.public_live TO PUBLIC")
    await database.status(f"GRANT SELECT,INSERT,UPDATE ON {schema}.read_stage TO {_quoted(roles_by_kind['writer'])}")
    await database.status(f"GRANT SELECT(marker) ON {schema}.read_stage TO {_quoted(roles_by_kind['column'])}")


async def _as_reader(administrator, role, statement):
    async with administrator.connect() as connection, connection.begin():
        await connection.execute(text(f"SET LOCAL ROLE {_quoted(role)}"))
        await connection.execute(text("SET LOCAL statement_timeout='200ms'"))
        result = await connection.execute(text(statement))
        return result.scalar() if result.returns_rows else None


async def _refused(administrator, role, statement):
    with pytest.raises(DBAPIError) as failure:
        await _as_reader(administrator, role, statement)
    assert failure.value.orig.sqlstate == "42501"


def _stages(case):
    return tuple(
        SimpleNamespace(schema=case.schema, target_relation=live, stage_table=stage)
        for live, stage in (("read_live", "read_stage"), ("public_live", "public_stage"))
    )


async def _replay(case, stages, path):
    if path == "ordinary":
        proofs = await access.prepare_ordinary_artifact_reads(case.database, case.schema, stages)
        await access.recheck_ordinary_read_access(case.database, proofs)
    else:
        _session, driver, _actor = await receipts.ordinary(case.database)
        await access.preserve_artifact_read_access(
            SimpleNamespace(policy=SimpleNamespace(schema=case.schema), connection=driver), stages
        )


async def _assert_published_reads(case, administrator, roles_by_kind):
    schema = case.schema
    assert await _as_reader(administrator, roles_by_kind["table"], f"SELECT marker FROM {schema}.read_live") == "new"
    assert (
        await _as_reader(administrator, roles_by_kind["column"], f'SELECT "selected value" FROM {schema}.read_live')
        == 2
    )
    assert (
        await _as_reader(administrator, roles_by_kind["delegate"], f'SELECT "selected value" FROM {schema}.read_live')
        == 2
    )
    await _refused(administrator, roles_by_kind["column"], f"SELECT marker FROM {schema}.read_live")
    await _refused(administrator, roles_by_kind["writer"], f"INSERT INTO {schema}.read_live VALUES ('blocked',3)")
    await _refused(administrator, roles_by_kind["writer"], f"UPDATE {schema}.read_live SET marker='blocked'")
    await _refused(administrator, roles_by_kind["writer"], f"SELECT marker FROM {schema}.read_live")
    assert await _as_reader(administrator, roles_by_kind["writer"], f"SELECT marker FROM {schema}.public_live") == "new"
    await _as_reader(
        administrator,
        roles_by_kind["column"],
        f'GRANT SELECT("selected value") ON {schema}.read_live TO {_quoted(roles_by_kind["late"])}',
    )
    assert (
        await _as_reader(administrator, roles_by_kind["late"], f'SELECT "selected value" FROM {schema}.read_live') == 2
    )


@pytest.mark.parametrize("path", ["ordinary", "capability"])
@pytest.mark.parametrize("rollback", [False, True])
async def test_native_select_replay_preserves_readers_and_atomic_rollback(monkeypatch, path, rollback):
    async with _reader_roles() as (administrator, roles_by_kind), ordinary_serving_case(monkeypatch) as case:
        await _seed_reads(case, roles_by_kind)
        await _as_reader(
            administrator,
            roles_by_kind["column"],
            f'GRANT SELECT("selected value") ON {case.schema}.read_live TO {_quoted(roles_by_kind["delegate"])}',
        )

        async def publish():
            async with case.database.transaction():
                proofs = await access.prepare_ordinary_artifact_reads(case.database, case.schema, _stages(case))
                assert (
                    await _as_reader(
                        administrator, roles_by_kind["table"], f"SELECT marker FROM {case.schema}.read_live"
                    )
                    == "old"
                )
                if path == "ordinary":
                    await access.recheck_ordinary_read_access(case.database, proofs)
                else:
                    await _replay(case, _stages(case), path)
                for stage in _stages(case):
                    await case.database.status(
                        f"ALTER TABLE {case.schema}.{stage.target_relation} RENAME TO {stage.target_relation}_previous"
                    )
                    await case.database.status(
                        f"ALTER TABLE {case.schema}.{stage.stage_table} RENAME TO {stage.target_relation}"
                    )
                if rollback:
                    raise RuntimeError("rollback after read replay and rename")

        if rollback:
            with pytest.raises(RuntimeError, match="rollback after read replay"):
                await publish()
            assert (
                await _as_reader(administrator, roles_by_kind["table"], f"SELECT marker FROM {case.schema}.read_live")
                == "old"
            )
            await _refused(administrator, roles_by_kind["table"], f"SELECT marker FROM {case.schema}.read_stage")
        else:
            await publish()
            await _assert_published_reads(case, administrator, roles_by_kind)


async def test_native_final_replay_uses_latest_explicit_select(monkeypatch):
    async with _reader_roles() as (administrator, roles_by_kind), ordinary_serving_case(monkeypatch) as case:
        await _seed_reads(case, roles_by_kind)
        stages = _stages(case)[:1]
        async with case.database.transaction():
            proofs = await access.prepare_ordinary_artifact_reads(case.database, case.schema, stages)
            await _refused(administrator, roles_by_kind["table"], f"SELECT marker FROM {case.schema}.read_stage")
            await _as_reader(administrator, roles_by_kind["table"], f"SELECT marker FROM {case.schema}.read_live")
        await case.database.status(f"REVOKE SELECT ON {case.schema}.read_live FROM {_quoted(roles_by_kind['table'])}")
        await case.database.status(f"GRANT SELECT ON {case.schema}.read_live TO {_quoted(roles_by_kind['late'])}")
        async with case.database.transaction():
            await access.recheck_ordinary_read_access(case.database, proofs)
        assert (
            await _as_reader(administrator, roles_by_kind["late"], f"SELECT marker FROM {case.schema}.read_stage")
            == "new"
        )
        await _refused(administrator, roles_by_kind["table"], f"SELECT marker FROM {case.schema}.read_stage")


async def test_native_unauthorized_grant_is_refused_by_postgres(monkeypatch):
    async with _reader_roles() as (administrator, roles_by_kind), ordinary_serving_case(monkeypatch) as case:
        await _seed_reads(case, roles_by_kind)
        async with administrator.connect() as connection, connection.begin():
            await connection.execute(text(f"SET LOCAL ROLE {_quoted(roles_by_kind['late'])}"))
            driver = (await connection.get_raw_connection()).driver_connection
            with pytest.raises(asyncpg.InsufficientPrivilegeError) as failure:
                await access._replay_select(driver, case.schema, "read_live", "read_stage")
            assert failure.value.sqlstate == "42501"
        await _refused(administrator, roles_by_kind["table"], f"SELECT marker FROM {case.schema}.read_stage")
