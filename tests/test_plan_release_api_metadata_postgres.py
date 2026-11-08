# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine retained-family metadata and guarded rows without eager reader locks."""

import asyncio

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.network_read_execution import bounded_network_read_rows, execute_network_read
from api.plan_release_pricing_projection import plan_release_serving_sql
from api.plan_release_read_storage import is_retained_session_read, read_retained_binding_metadata
from process.network_read_identity import verify_consumed_network_relations
from tests.plan_release_storage_postgres_support import NEW_REVISION, OLD_REVISION, RELEASE, _database_config
from tests.test_plan_release_retained_reads_postgres import retained_case

pytestmark = pytest.mark.asyncio


def _assert_no_eager_locks(statements):
    queries = [event.query.upper() for event in statements]
    assert queries and not any(
        sql.lstrip().startswith("LOCK ") or "PG_ADVISORY" in sql or "LIMIT 0" in sql or "FOR SHARE" in sql
        for sql in queries
    )


async def test_native_retained_metadata_and_session_payload_match_original(monkeypatch):
    async with retained_case(monkeypatch) as (case, _admin, reader, before):
        sql = plan_release_serving_sql(case.schema, include_pricing_projection=False, retained=True)
        native_sql = sql.replace(":plan_release_id", "$1").replace(":pin_owner_type", "$2")
        statements = []
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with reader.query_logger(statements.append):
                retained, pins = await read_retained_binding_metadata(reader, case.schema)
                assert retained and len(pins) == 3
                backend_pid = reader.get_server_pid()
                binding_rows = await reader.fetch(native_sql, RELEASE, "plan_release_serving_revision")
                await verify_consumed_network_relations(reader, pins, expected_backend_pid=backend_pid)
                assert binding_rows == before
                await asyncio.sleep(0)
        _assert_no_eager_locks(statements)
        url = URL.create(
            "postgresql+asyncpg",
            username=case.reader,
            password=case.role_passwords[case.reader],
            host=case.config["host"],
            port=case.config["port"],
            database=case.config["database"],
        )
        engine = create_async_engine(url, isolation_level="REPEATABLE READ")
        try:
            async with async_sessionmaker(engine)() as session, session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                driver = (await (await session.connection()).get_raw_connection()).driver_connection
                statements = []
                with driver.query_logger(statements.append):
                    assert await is_retained_session_read(session, case.schema, api_read=True)
                    binding_rows = await execute_network_read(
                        session,
                        text(sql),
                        {"plan_release_id": RELEASE, "pin_owner_type": "plan_release_serving_revision"},
                    )
                    assert [dict(binding_row._mapping) for binding_row in binding_rows] == [
                        dict(binding_row) for binding_row in before
                    ]
                    projected = await bounded_network_read_rows(
                        session,
                        f"SELECT binding_ordinal,snapshot_id FROM ONLY {case.schema}.plan_release_snapshot_binding_legacy "
                        "WHERE serving_revision_id=$1 ORDER BY binding_ordinal",
                        before[0]["serving_revision_id"],
                    )
                    assert projected == [
                        {"binding_ordinal": binding_row["binding_ordinal"], "snapshot_id": binding_row["snapshot_id"]}
                        for binding_row in before
                    ]
                    await asyncio.sleep(0)
                _assert_no_eager_locks(statements)
        finally:
            await engine.dispose()


async def test_native_retained_metadata_refuses_original_fk_removal(monkeypatch):
    async with retained_case(monkeypatch) as (case, admin, reader, _before):
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert (await read_retained_binding_metadata(reader, case.schema))[0]
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        constraint = await admin.fetchval(
            "SELECT conname FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='f'",
            legacy,
        )
        assert constraint
        quoted = await admin.fetchval("SELECT quote_ident($1::text)", constraint)
        await admin.execute(f"ALTER TABLE {legacy} DROP CONSTRAINT {quoted}")
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(ValueError, match="legacy_authority"):
                await read_retained_binding_metadata(reader, case.schema)


async def _assert_legacy_payload_waits(reader, legacy):
    with pytest.raises(asyncpg.LockNotAvailableError):
        async with reader.transaction():
            await reader.fetch(f"SELECT * FROM ONLY {legacy}")


@pytest.mark.parametrize(
    "locked_relation",
    [
        "plan_release_snapshot_binding_legacy",
        "plan_release_snapshot_binding",
        "plan_release_snapshot_pin",
        "plan_release_serving_revision",
        "ptg2_snapshot_pin",
    ],
)
async def test_retained_metadata_does_not_open_locked_application_relations(monkeypatch, locked_relation):
    """A held exclusive lock exposes even transient locks hidden inside deparsers."""
    async with retained_case(monkeypatch) as (case, admin, reader, before):
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        sql = plan_release_serving_sql(case.schema, include_pricing_projection=False, retained=True)
        sql = sql.replace(":plan_release_id", "$1").replace(":pin_owner_type", "$2")
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await reader.execute("SET LOCAL lock_timeout='250ms'")
            async with admin.transaction():
                await admin.execute(f"LOCK TABLE ONLY {case.relation(locked_relation)} IN ACCESS EXCLUSIVE MODE")
                retained, pins = await read_retained_binding_metadata(reader, case.schema)
                assert retained and len(pins) == 3
                assert not await admin.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
                    "WHERE l.pid=$1 AND c.relnamespace=$2::regnamespace)",
                    reader.get_server_pid(),
                    case.schema,
                )
                if locked_relation == "plan_release_snapshot_binding_legacy":
                    await _assert_legacy_payload_waits(reader, legacy)
            assert await reader.fetch(sql, RELEASE, "plan_release_serving_revision") == before
            await verify_consumed_network_relations(reader, pins, expected_backend_pid=reader.get_server_pid())


@pytest.mark.parametrize("damage", ["check", "system_var", "whole_row", "default", "predicate", "bound"])
async def test_native_metadata_still_refuses_altered_expressions(monkeypatch, damage):
    async with retained_case(monkeypatch) as (case, admin, reader, _before):
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        if damage in {"check", "system_var", "whole_row"}:
            header = case.relation("plan_release_serving_revision")
            expression_by_damage = {
                "check": "expected_binding_count >= 0",
                "system_var": "tableoid <> 0",
                "whole_row": "plan_release_serving_revision IS DISTINCT FROM NULL",
            }
            await admin.execute(f"ALTER TABLE {header} DROP CONSTRAINT plan_release_serving_binding_count_check")
            await admin.execute(
                f"ALTER TABLE {header} ADD CONSTRAINT plan_release_serving_binding_count_check "
                f"CHECK ({expression_by_damage[damage]})"
            )
        elif damage == "default":
            await admin.execute(f"ALTER TABLE {legacy} ALTER COLUMN required SET DEFAULT false")
        elif damage == "predicate":
            header = case.relation("plan_release_serving_revision")
            index = await admin.fetchval(
                "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.relname) "
                "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE i.indrelid=to_regclass($1) AND i.indisunique AND i.indpred IS NOT NULL "
                "AND c.relname='plan_release_serving_current_release_uidx'",
                header,
            )
            assert index
            await admin.execute(f"DROP INDEX {index}")
            await admin.execute(
                f"CREATE UNIQUE INDEX changed_current ON {header}(plan_release_id) WHERE NOT is_current"
            )
        else:
            await admin.execute(f"ALTER TABLE {legacy} DROP CONSTRAINT plan_release_legacy_cohort_bound")
            await admin.execute(
                f"ALTER TABLE {legacy} ADD CONSTRAINT plan_release_legacy_cohort_bound "
                f"CHECK (serving_revision_id IN ('{OLD_REVISION}','{NEW_REVISION}'))"
            )
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(ValueError, match="plan_release_storage_"):
                await read_retained_binding_metadata(reader, case.schema)


@pytest.mark.parametrize("adapter", ["buffered", "aggregate"])
async def test_native_result_guard_rejects_swap_after_metadata_without_eager_locks(monkeypatch, adapter):
    async with retained_case(monkeypatch) as (case, admin, _reader, _before):
        url = URL.create(
            "postgresql+asyncpg",
            username=case.reader,
            password=case.role_passwords[case.reader],
            host=case.config["host"],
            port=case.config["port"],
            database=case.config["database"],
        )
        engine = create_async_engine(url, isolation_level="REPEATABLE READ")
        try:
            async with async_sessionmaker(engine)() as session, session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                assert await is_retained_session_read(session, case.schema, api_read=True)
                await admin.execute("SET lock_timeout='250ms'")
                pin = case.relation("ptg2_snapshot_pin")
                await admin.execute(f"ALTER TABLE {pin} RENAME TO prior_snapshot_pin")
                await admin.execute(f'CREATE TABLE {pin} (LIKE "{case.schema}".prior_snapshot_pin INCLUDING ALL)')
                await admin.execute(f'GRANT SELECT ON {pin} TO "{case.reader}"')
                guarded_read = (
                    execute_network_read(session, text(f"SELECT owner_id FROM ONLY {pin}"))
                    if adapter == "buffered"
                    else bounded_network_read_rows(session, f"SELECT owner_id FROM ONLY {pin}")
                )
                with pytest.raises(ServiceUnavailable):
                    await guarded_read
        finally:
            await engine.dispose()


@pytest.mark.parametrize("suffix", ["0" * 32, "a" * 32, "", "0" * 31, "g" * 32, "0" * 32 + "_extra"])
async def test_native_storage_accepts_only_exact_owned_ci_parent(monkeypatch, suffix):
    database = "hc_network_registry_" + suffix
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", database)
    if suffix in {"0" * 32, "a" * 32}:
        assert _database_config()["database"] == database
    else:
        with pytest.raises(pytest.skip.Exception, match="requires an explicit test database"):
            _database_config()
