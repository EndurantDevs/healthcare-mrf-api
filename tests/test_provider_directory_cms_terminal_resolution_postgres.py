# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real terminal guard dispatch after an ordinary creator adds hostile siblings."""

import json
from uuid import uuid4

import asyncpg
import pytest

from db import migration_cms_terminal_resolution as migration
from db.plan_release_storage import storage_relation
from process import provider_directory_source_callable_custody as source
from tests.test_provider_directory_cms_canonical_layout_postgres import native_layout as native_layout
from tests.test_provider_directory_cms_creator_dispatch_postgres import _creator
from tests.test_provider_directory_cms_legacy_history_postgres import _reader
from tests.test_provider_directory_cms_replay_resolution_postgres import _attacker_locks
from tests.test_provider_directory_cms_terminal_guard_resolution_postgres import (
    _assert_properties,
    _hooks,
    _routines,
)

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def _sibling(creator, callee, kind, key):
    connection, _stage, signatures, _stages = creator
    function = storage_relation("mrf", callee)
    declaration_by_kind = {
        "varchar": "candidate pg_catalog.varchar",
        "defaults": "candidate pg_catalog.text,extra pg_catalog.bool DEFAULT false",
        "variadic": "candidate pg_catalog.text,VARIADIC extras pg_catalog.bool[]",
    }
    kinds_by_kind = {"varchar": "varchar", "defaults": "text,bool", "variadic": "text,bool[]"}
    signature = function + "(" + kinds_by_kind[kind] + ")"
    assert await connection.fetchval("SELECT pg_catalog.to_regprocedure($1) IS NULL", signature)
    signatures.append(signature)
    await connection.execute(
        f"CREATE FUNCTION {function}({declaration_by_kind[kind]}) RETURNS pg_catalog.bool LANGUAGE plpgsql AS $attack$ "
        f"BEGIN PERFORM pg_catalog.pg_advisory_lock({key}::pg_catalog.int8); "
        "PERFORM pg_catalog.set_config('cms.terminal_actor',current_user,true); RETURN true; END $attack$"
    )


async def _invoke(driver, callee, *, typed, kind, key):
    metadata = (
        {"provider_directory_reviewed_subset_terminal_disposition_v1": {}} if callee == migration.CALLEES[0] else {}
    )
    await driver.execute(
        "CREATE TEMP TABLE terminal_header(dataset_id varchar(96),status text,publication_metadata_json jsonb);"
        "INSERT INTO terminal_header VALUES('terminal-probe','acquiring','{}');"
        "CREATE TRIGGER terminal_guard AFTER UPDATE ON terminal_header "
        "FOR EACH ROW EXECUTE FUNCTION mrf.guard_provider_directory_subset_abandonment_dataset()"
    )
    await driver.execute("SAVEPOINT terminal_probe")
    if typed:
        expected = asyncpg.AmbiguousFunctionError if kind == "defaults" else asyncpg.ObjectNotInPrerequisiteStateError
        with pytest.raises(expected):
            await driver.execute(
                "UPDATE terminal_header SET status='acquisition_abandoned',publication_metadata_json=$1::jsonb",
                json.dumps(metadata),
            )
    else:
        await driver.execute(
            "UPDATE terminal_header SET status='acquisition_abandoned',publication_metadata_json=$1::jsonb",
            json.dumps(metadata),
        )
        assert await driver.fetchval("SELECT status FROM terminal_header") == "acquisition_abandoned"
        assert await driver.fetchval(
            "SELECT pg_catalog.current_setting('cms.terminal_actor')"
        ) == await driver.fetchval(
            "SELECT pg_catalog.pg_get_userbyid(proowner)::text FROM pg_catalog.pg_proc "
            "WHERE oid='mrf.guard_provider_directory_subset_abandonment_dataset()'::regprocedure"
        )
    await driver.execute("ROLLBACK TO SAVEPOINT terminal_probe")
    assert await driver.fetchval("SELECT status FROM terminal_header") == "acquiring"
    assert await _attacker_locks(driver, [key]) == ([] if typed else [key])
    if not typed:
        assert await driver.fetchval("SELECT pg_catalog.pg_advisory_unlock($1::pg_catalog.int8)", key)
    await driver.execute("RELEASE SAVEPOINT terminal_probe")
    await driver.execute("DROP TABLE pg_temp.terminal_header")


@pytest.mark.parametrize("callee", migration.CALLEES)
@pytest.mark.parametrize("typed,kind", [(False, "varchar"), (True, "varchar"), (True, "variadic"), (True, "defaults")])
async def test_real_deferred_guard_never_executes_hostile_sibling_after_typing(native_layout, callee, typed, kind):
    admin, key = native_layout, int(uuid4().hex[:15], 16)
    async with _creator(admin) as creator:
        before, hooks = await _routines(admin), await _hooks(admin)
        original = next(row for row in before if row["native"]["definition"]["proname"] == migration.GUARD)
        try:
            async with _reader(admin, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
                if typed:
                    connection = await session.connection()
                    expected_by_field = dict(
                        oid=original["oid"], owner_oid=int(original["native"]["definition"]["proowner"])
                    )
                    await connection.run_sync(
                        lambda sync: migration.upgrade_cms_terminal_guard_resolution(
                            sync, "mrf", expected=expected_by_field
                        )
                    )
                checked = await _routines(driver)
                guard = next(row for row in checked if row["native"]["definition"]["proname"] == migration.GUARD)
                assert source._is_approved_body(guard["native"]["definition"], "mrf", migration.ORIGINAL_SHA256)
                await _sibling(creator, callee, kind, key)
                assert await _routines(driver) == checked
                await _invoke(driver, callee, typed=typed, kind=kind, key=key)
                _assert_properties(before, await _routines(driver))
                assert await _hooks(driver) == hooks
        finally:
            await admin.execute(original["native"]["body"])
            assert await _routines(admin) == before and await _hooks(admin) == hooks
