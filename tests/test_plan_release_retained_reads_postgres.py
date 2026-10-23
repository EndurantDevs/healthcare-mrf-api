# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Restricted-reader parity across the structural-only migration boundary."""

import secrets
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from api import plan_release_serving as serving
from api import plan_release_serving_resolution as resolution
from api.plan_release_pricing_projection import plan_release_serving_queries, plan_release_serving_sql
from api.plan_release_read_storage import is_retained_binding_read
from process import registry_pricing_release_binding_lookup as lookup
from tests.plan_release_storage_postgres_support import NEW_REVISION, OLD_REVISION, PLAN, RELEASE, storage_case


@asynccontextmanager
async def retained_case(monkeypatch, *, reader_mode="single"):
    async with storage_case(migrated=False) as case:
        admin = await case.connect()
        await admin.execute(f"DELETE FROM {case.relation('plan_release_snapshot_binding')} WHERE binding_ordinal>=2")
        await admin.execute(f"UPDATE {case.relation('plan_release_serving_revision')} SET expected_binding_count=2")
        # This is the existing reader enrollment, before the structural migration.
        await admin.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{case.schema}" TO "{case.reader}"')
        if reader_mode != "single":
            role = case.reader + ("_other" if reader_mode == "two" else "_group")
            has_login = reader_mode == "two"
            password = ""
            if has_login:
                case.role_passwords[role] = secrets.token_hex(32)
                password = " PASSWORD " + await admin.fetchval(
                    "SELECT quote_literal($1::text)", case.role_passwords[role]
                )
            case.created_roles.append(role)
            await admin.execute(f'CREATE ROLE "{role}" ' + ("LOGIN" if has_login else "NOLOGIN") + password)
            await admin.execute(f'GRANT USAGE ON SCHEMA "{case.schema}" TO "{role}"')
            await admin.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{case.schema}" TO "{role}"')
            if not has_login:
                await admin.execute(f'GRANT "{role}" TO "{case.reader}"')
                await admin.execute(f'REVOKE SELECT ON ALL TABLES IN SCHEMA "{case.schema}" FROM "{case.reader}"')
        reader = await case.connect(case.reader)
        query = plan_release_serving_sql(case.schema, include_pricing_projection=False, retained=True)
        query = query.replace(
            f"ONLY {case.schema}.plan_release_snapshot_binding_legacy", f"{case.schema}.plan_release_snapshot_binding"
        )
        query = query.replace(":plan_release_id", "$1").replace(":pin_owner_type", "$2")
        before = await reader.fetch(query, RELEASE, "plan_release_serving_revision")
        await case.migrate()
        for module in (serving, resolution, lookup):
            monkeypatch.setattr(module, "PTG2_SCHEMA", case.schema)
        full, plain = plan_release_serving_queries(case.schema)
        monkeypatch.setattr(serving, "_PLAN_RELEASE_SERVING_SQL", full)
        monkeypatch.setattr(serving, "_PLAN_RELEASE_SERVING_WITHOUT_PROJECTION_SQL", plain)
        monkeypatch.setattr(
            resolution,
            "_PLAN_RELEASE_EXISTS_SQL",
            resolution._PLAN_RELEASE_EXISTS_SQL.replace("mrf.", case.schema + "."),
        )
        monkeypatch.setattr(resolution, "_PLAN_RELEASE_GUARD_SQL", plain)
        monkeypatch.setattr(resolution, "_PLAN_RELEASE_SERVING_SQL", plain + "\n LIMIT 9")
        yield case, admin, reader, before


@asynccontextmanager
async def reader_session(case, role=None):
    role = role or case.reader
    url = URL.create(
        "postgresql+asyncpg",
        username=role,
        password=case.role_passwords[role],
        host=case.config["host"],
        port=case.config["port"],
        database=case.config["database"],
    )
    engine = create_async_engine(url, isolation_level="REPEATABLE READ")
    try:
        async with engine.connect() as connection:
            async with connection.begin():
                await connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                assert tuple((await connection.exec_driver_sql("SELECT session_user,current_user")).one()) == (
                    role,
                    role,
                )
                yield connection
    finally:
        await engine.dispose()


async def _catalog(admin, case):
    return await admin.fetch(
        "SELECT oid,relname,relowner,relacl::text FROM pg_class WHERE relnamespace=$1::regnamespace ORDER BY oid",
        case.schema,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("reader_mode", ["single", "two", "inherited"])
async def test_retained_readers_resolve_the_exact_pre_migration_release_without_grants(monkeypatch, reader_mode):
    async with retained_case(monkeypatch, reader_mode=reader_mode) as (case, admin, reader, before):
        pin = case.relation("ptg2_snapshot_pin")
        await admin.execute(f'GRANT SELECT ON {pin} TO "{case.publisher}"')
        async with reader.transaction(readonly=True):
            assert await is_retained_binding_read(reader.fetch, case.schema) is True
        await admin.execute(f'GRANT INSERT,UPDATE,DELETE ON {pin} TO "{case.publisher}"')
        catalog = await _catalog(admin, case)
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await is_retained_binding_read(reader.fetch, case.schema) is True
            bulk = await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
            assert serving._selection_from_rows(RELEASE, bulk) == serving._selection_from_rows(RELEASE, before)
        async with reader_session(case) as session:
            guard = await resolution.resolve_plan_release_guard_selection(session, RELEASE)
            full = await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True)
            capped = await resolution.resolve_plan_release_serving_resolution(session, RELEASE, projection_only=True)
            assert guard == full == capped.selection == serving._selection_from_rows(RELEASE, before)
            assert capped.state == "ready" and full.binding_set_digest == "a" * 64 and len(full.bindings) == 2
        if reader_mode == "two":
            other = await case.connect(case.reader + "_other")
            async with other.transaction(isolation="repeatable_read", readonly=True):
                assert await is_retained_binding_read(other.fetch, case.schema) is True
                bulk = await lookup._fetch_release_binding_rows(other, [RELEASE], 1024 * 1024)
                assert serving._selection_from_rows(RELEASE, bulk) == serving._selection_from_rows(RELEASE, before)
            async with reader_session(case, case.reader + "_other") as session:
                assert await resolution.resolve_plan_release_guard_selection(session, RELEASE) == full
        assert await _catalog(admin, case) == catalog
        for name in ("plan_release_snapshot_binding", "plan_release_snapshot_pin"):
            assert not await reader.fetchval(
                "SELECT has_table_privilege(current_user,$1,'SELECT')", case.relation(name)
            )


@pytest.mark.asyncio
async def test_provisioned_roots_keep_the_normal_reader_path(monkeypatch):
    async with retained_case(monkeypatch) as (case, admin, reader, before):
        await case.provision()
        async with reader.transaction(readonly=True):
            assert await is_retained_binding_read(reader.fetch, case.schema) is False
        async with reader_session(case) as session:
            assert await resolution.resolve_plan_release_guard_selection(
                session, RELEASE
            ) == serving._selection_from_rows(RELEASE, before)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "one_root_grant",
        "root_column_grant",
        "root_owner",
        "detached",
        "bound",
        "foreign_key",
        "legacy_write",
        "pin_write",
        "pin_read",
        "root_shape",
        "root_check",
        "other_reader_write",
        "public_reader",
        "reader_grantable",
    ],
)
async def test_retained_storage_drift_is_refused_without_fallback(monkeypatch, damage):
    reader_mode = "two" if damage == "other_reader_write" else "single"
    async with retained_case(monkeypatch, reader_mode=reader_mode) as (case, admin, reader, _before):
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        binding = case.relation("plan_release_snapshot_binding")
        pin = case.relation("ptg2_snapshot_pin")
        statements_by_damage = {
            "one_root_grant": f'GRANT SELECT ON {binding} TO "{case.reader}"',
            "root_column_grant": f'GRANT SELECT(snapshot_id) ON {binding} TO "{case.reader}"',
            "root_owner": f'ALTER TABLE {binding} OWNER TO "{case.owner}"',
            "detached": f"ALTER TABLE {binding} DETACH PARTITION {legacy}",
            "bound": f"ALTER TABLE {legacy} DROP CONSTRAINT plan_release_legacy_cohort_bound",
            "foreign_key": f"ALTER TABLE {legacy} DROP CONSTRAINT plan_release_snapshot_binding_revision_fkey",
            "legacy_write": f'GRANT UPDATE(snapshot_id) ON {legacy} TO "{case.reader}"',
            "pin_write": f'GRANT DELETE ON {pin} TO "{case.reader}"',
            "pin_read": f'REVOKE SELECT ON {pin} FROM "{case.reader}"',
            "root_shape": f"ALTER TABLE {case.relation('plan_release_snapshot_pin')} ALTER COLUMN reason SET DEFAULT 'changed'",
            "root_check": f"ALTER TABLE {case.relation('plan_release_snapshot_pin')} ADD CHECK(snapshot_id <> 'changed')",
            "other_reader_write": f'GRANT UPDATE(snapshot_id) ON {legacy} TO "{case.reader}_other"',
            "public_reader": f"GRANT SELECT ON {legacy} TO PUBLIC",
            "reader_grantable": f'GRANT SELECT ON {legacy} TO "{case.reader}" WITH GRANT OPTION',
        }
        await admin.execute(statements_by_damage[damage])
        catalog = await _catalog(admin, case)
        async with reader.transaction(readonly=True):
            with pytest.raises((ValueError, asyncpg.InsufficientPrivilegeError)):
                await is_retained_binding_read(reader.fetch, case.schema)
        assert await _catalog(admin, case) == catalog


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["unpinned", "not_current", "incomplete", "unpublished_snapshot", "outside_legacy"])
async def test_retained_release_semantics_still_fail_closed(monkeypatch, damage):
    async with retained_case(monkeypatch) as (case, admin, _reader, _before):
        header = case.relation("plan_release_serving_revision")
        if damage == "outside_legacy":
            await admin.execute(f"UPDATE {header} SET is_current=false")
            await admin.execute(
                f"INSERT INTO {header}(serving_revision_id,plan_release_id,healthporta_plan_id,release_month,release_status,serving_status,is_current,expected_binding_count,binding_set_digest) VALUES($1,$2,$3,'2026-10','published','published',true,2,$4)",
                NEW_REVISION,
                RELEASE,
                PLAN,
                "b" * 64,
            )
        else:
            statements_by_damage = {
                "unpinned": f"DELETE FROM {case.relation('ptg2_snapshot_pin')}",
                "not_current": f"UPDATE {header} SET is_current=false",
                "incomplete": f"UPDATE {header} SET expected_binding_count=3",
                "unpublished_snapshot": f"UPDATE {case.relation('ptg2_snapshot')} SET status='candidate'",
            }
            await admin.execute(statements_by_damage[damage])
        async with reader_session(case) as session:
            assert await resolution.resolve_plan_release_guard_selection(session, RELEASE) is None
            assert await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True) is None
            assert (
                await resolution.resolve_plan_release_serving_resolution(session, RELEASE, projection_only=True)
            ).state == "unavailable"


@pytest.mark.asyncio
async def test_retained_reader_holds_the_original_leaf_until_transaction_end(monkeypatch):
    async with retained_case(monkeypatch) as (case, admin, reader, _before):
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await is_retained_binding_read(reader.fetch, case.schema) is True
            await admin.execute("SET lock_timeout='100ms'")
            with pytest.raises(asyncpg.LockNotAvailableError):
                await admin.execute(
                    f"ALTER TABLE {case.relation('plan_release_snapshot_binding')} DETACH PARTITION {case.relation('plan_release_snapshot_binding_legacy')}"
                )
            assert (
                await reader.fetchval(f"SELECT count(*) FROM {case.relation('plan_release_snapshot_binding_legacy')}")
                == 2
            )
        await admin.execute(
            f"ALTER TABLE {case.relation('plan_release_snapshot_binding')} DETACH PARTITION {case.relation('plan_release_snapshot_binding_legacy')}"
        )
