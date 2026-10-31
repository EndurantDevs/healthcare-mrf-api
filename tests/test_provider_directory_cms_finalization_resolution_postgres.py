# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual guard/validator resolution and checked body replacement on a disposable catalog."""

import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db import migration_cms_snapshot_allocator as migration
from db.plan_release_storage import storage_identifier, storage_relation
from tests.test_provider_directory_cms_canonical_layout_postgres import native_layout
from tests.test_provider_directory_cms_legacy_history_postgres import _reader

pytestmark = pytest.mark.asyncio(loop_scope="module")
_GUARD = storage_relation("mrf", "guard_pd_generic_dataset_finalization") + "()"
_VALIDATOR = storage_relation("mrf", "validate_pd_dataset_bulk_relationships")


async def _snapshot(driver):
    row_by_field = dict(
        await driver.fetchrow(
            "SELECT p.oid::bigint,p.proowner::bigint AS owner_oid,p.prosrc,"
            "pg_catalog.to_jsonb(p)-'prosrc' AS identity,"
            "pg_catalog.pg_get_functiondef(p.oid) AS definition,"
            "pg_catalog.obj_description(p.oid,'pg_proc') AS description "
            "FROM pg_catalog.pg_proc p WHERE p.oid=$1::regprocedure",
            _GUARD,
        )
    )
    if type(row_by_field["identity"]) is str:
        row_by_field["identity"] = json.loads(row_by_field["identity"])
    return row_by_field


async def _upgrade(session, expected):
    connection = await session.connection()
    await connection.run_sync(
        lambda sync: migration.upgrade_cms_generic_finalization_guard(sync, "mrf", expected=expected)
    )


@asynccontextmanager
async def _overload(admin, kind):
    role = "pd_resolution_" + uuid4().hex
    signature = _VALIDATOR + ("(varchar,text)" if kind == "varchar" else "(text,text,boolean)")
    is_created = False
    attacker = None
    try:
        await admin.execute(f"CREATE ROLE {storage_identifier(role)} LOGIN")
        is_created = True
        await admin.execute(f"GRANT USAGE,CREATE ON SCHEMA mrf TO {storage_identifier(role)}")
        url = make_url(os.environ["HLTHPRT_CMS_LAYOUT_TEST_DSN"]).set(
            drivername="postgresql", username=role, database=await admin.fetchval("SELECT current_database()")
        )
        attacker = await asyncpg.connect(url.render_as_string(hide_password=False))
        assert await attacker.fetchval("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=session_user") is False
        arguments = (
            "requested varchar, kind text"
            if kind == "varchar"
            else "requested text, kind text, extra boolean DEFAULT false"
        )
        await attacker.execute(
            f"CREATE FUNCTION {_VALIDATOR}({arguments}) RETURNS void LANGUAGE plpgsql AS $probe$ "
            "BEGIN PERFORM pg_catalog.set_config('cms.resolution_attack',current_user,true); END $probe$"
        )
        yield
    finally:
        if attacker is not None:
            await attacker.close()
        if is_created:
            await admin.execute("DROP FUNCTION IF EXISTS " + signature)
            await admin.execute(f"REVOKE USAGE,CREATE ON SCHEMA mrf FROM {storage_identifier(role)}")
            await admin.execute("DROP ROLE " + storage_identifier(role))
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE rolname=$1)", role)


async def _publish(driver, attack):
    if attack != "defaults":
        await driver.execute("UPDATE cms_resolution_header SET status='validated'")
        return
    await driver.execute("SAVEPOINT overload_refusal")
    with pytest.raises(asyncpg.AmbiguousFunctionError):
        await driver.execute("UPDATE cms_resolution_header SET status='validated'")
    await driver.execute("ROLLBACK TO SAVEPOINT overload_refusal")
    await driver.execute("RELEASE SAVEPOINT overload_refusal")
    assert await driver.fetchval("SELECT status FROM cms_resolution_header") == "acquiring"


@pytest.mark.parametrize("typed,attack", [(False, "varchar"), (True, "varchar"), (True, "defaults"), (True, None)])
async def test_real_guard_never_executes_the_overload_after_typed_binding(native_layout, typed, attack):
    admin = native_layout
    original = await _snapshot(admin)
    try:
        async with AsyncExitStack() as scopes:
            if attack is not None:
                await scopes.enter_async_context(_overload(admin, attack))
            async with _reader(admin, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
                expected_by_field = {key: original[key] for key in ("oid", "owner_oid")}
                if typed:
                    await _upgrade(session, expected_by_field)
                    after = await _snapshot(driver)
                    assert after["prosrc"] == migration.generic_finalization_bodies("mrf")[1]
                    assert {key: after[key] for key in ("identity", "description")} == {
                        key: original[key] for key in ("identity", "description")
                    }
                    await _upgrade(session, expected_by_field)
                    assert await _snapshot(driver) == after
                await driver.execute(
                    "CREATE TEMP TABLE cms_resolution_header(dataset_id varchar(96),status text,publication_metadata_json jsonb);"
                    "INSERT INTO cms_resolution_header VALUES('resolution-example','acquiring','{}');"
                    "CREATE TRIGGER resolve_guard BEFORE UPDATE OF status ON cms_resolution_header "
                    f"FOR EACH ROW EXECUTE FUNCTION {_GUARD}"
                )
                await _publish(driver, attack)
                observed = await driver.fetchval("SELECT pg_catalog.current_setting('cms.resolution_attack',true)")
                assert bool(observed) is (not typed and attack == "varchar")
                if observed:
                    assert observed == await driver.fetchval(
                        "SELECT pg_catalog.pg_get_userbyid($1::oid)::text", original["owner_oid"]
                    )
    finally:
        await admin.execute(original["definition"])
        assert await _snapshot(admin) == original


@pytest.mark.parametrize("field", ["oid", "owner_oid"])
async def test_wrong_protected_identity_refuses_before_body_replacement(native_layout, field):
    original = await _snapshot(native_layout)
    expected_by_field = {key: original[key] for key in ("oid", "owner_oid")}
    expected_by_field[field] += 1
    async with _reader(native_layout, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
        with pytest.raises(RuntimeError, match="function_changed"):
            await _upgrade(session, expected_by_field)
        assert await _snapshot(driver) == original
