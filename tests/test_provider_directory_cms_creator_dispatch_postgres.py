# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Late creator attacks against real guards; no full protected-operation acceptance."""

import os
from contextlib import asynccontextmanager
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_callable_custody as callables
from tests.test_provider_directory_cms_legacy_history_postgres import _reader
from tests.test_provider_directory_cms_replay_resolution_postgres import _attacker_locks, _header
from tests.test_provider_directory_cms_terminal_guard_resolution_postgres import (
    _PREPARATION_SCHEMA,
    _assert_properties,
    _hooks,
    _routines,
    _upgrade_guards,
)
from tests.test_provider_directory_cms_terminal_guard_resolution_postgres import (
    core_layout as core_layout,
)
from tests.test_provider_directory_cms_terminal_guard_resolution_postgres import (
    native_layout as native_layout,
)

pytestmark = pytest.mark.asyncio(loop_scope="module")
_TARGETS = {
    "generic": ("validate_pd_dataset_bulk_relationships", ["text", "text"], 0, "void"),
    "replay": (
        "provider_directory_subset_replay_evidence_shape_valid",
        ["jsonb", "text", "jsonb", "text"],
        3,
        "bool",
    ),
    "coverage": ("provider_directory_subset_coverage_shape_valid", ["jsonb", "jsonb", "text", "text"], 2, "bool"),
}


@asynccontextmanager
async def _creator(admin):
    """Register exact cleanup before provisioning this fixture's ordinary actor."""
    role, stage = "creator_" + uuid4().hex, "creator_stage_" + uuid4().hex
    connection, has_role, signatures, stages = None, False, [], []
    try:
        assert await admin.fetchval("SELECT pg_catalog.to_regrole($1) IS NULL", role)
        has_role = True
        await admin.execute(f"CREATE ROLE {storage_identifier(role)} LOGIN")
        await admin.execute(f"GRANT USAGE,CREATE ON SCHEMA mrf TO {storage_identifier(role)}")
        url = make_url(os.environ["HLTHPRT_CMS_LAYOUT_TEST_DSN"]).set(
            drivername="postgresql", username=role, database=await admin.fetchval("SELECT current_database()")
        )
        connection = await asyncpg.connect(url.render_as_string(hide_password=False), command_timeout=2)
        assert not await connection.fetchval("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=session_user")
        assert not await connection.fetchval(
            "SELECT pg_catalog.has_table_privilege(current_user,'mrf.provider_directory_endpoint_dataset',"
            "'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') "
            "OR pg_catalog.has_any_column_privilege(current_user,'mrf.provider_directory_endpoint_dataset',"
            "'INSERT,UPDATE,REFERENCES')"
        )
        yield connection, stage, signatures, stages
    finally:
        if connection is not None:
            await connection.close()
        for signature in reversed(signatures):
            await admin.execute("DROP FUNCTION IF EXISTS " + signature)
        for name in stages:
            await admin.execute("DROP TABLE IF EXISTS " + storage_relation("mrf", name))
        if has_role and await admin.fetchval("SELECT pg_catalog.to_regrole($1) IS NOT NULL", role):
            await admin.execute(f"REVOKE USAGE,CREATE ON SCHEMA mrf FROM {storage_identifier(role)}")
            await admin.execute("DROP ROLE " + storage_identifier(role))
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE rolname=$1)", role)


async def _late_overload(creator, target, kind, key):
    connection, stage, signatures, stages = creator
    name, types, varchar_position, returns = _TARGETS[target]
    types = list(types)
    if kind == "varchar":
        types[varchar_position] = "varchar"
    declarations = [f"p{index} pg_catalog.{typename}" for index, typename in enumerate(types)]
    if kind != "varchar":
        types.append("bool[]" if kind == "variadic" else "bool")
        declarations.append(
            "VARIADIC extras pg_catalog.bool[]" if kind == "variadic" else "extra pg_catalog.bool DEFAULT false"
        )
    function = storage_relation("mrf", name)
    return_statement = "RETURN;" if returns == "void" else "RETURN true;"
    signature = function + "(" + ",".join("pg_catalog." + kind for kind in types) + ")"
    assert await connection.fetchval("SELECT pg_catalog.to_regprocedure($1) IS NULL", signature)
    signatures.append(signature)
    await connection.execute(
        f"CREATE FUNCTION {function}({','.join(declarations)}) RETURNS pg_catalog.{returns} LANGUAGE plpgsql "
        f"AS $probe$ BEGIN PERFORM pg_catalog.pg_advisory_lock({key}::pg_catalog.int8); {return_statement} END $probe$"
    )
    assert await connection.fetchval("SELECT pg_catalog.to_regclass($1) IS NULL", storage_relation("mrf", stage))
    stages.append(stage)
    await connection.execute(f"CREATE TABLE {storage_relation('mrf', stage)}(id pg_catalog.int4 PRIMARY KEY)")
    await connection.copy_records_to_table(stage, schema_name="mrf", records=[(101,), (202,)], columns=["id"])
    assert await connection.fetchval("SELECT count(*) FROM " + storage_relation("mrf", stage)) == 2


async def _generic_header(driver):
    await driver.execute(
        "CREATE TEMP TABLE cms_creator_header(dataset_id varchar(96),status text,publication_metadata_json jsonb);"
        "INSERT INTO cms_creator_header VALUES('creator-probe','acquiring','{}');"
        "CREATE TRIGGER creator_guard BEFORE UPDATE OF status ON cms_creator_header FOR EACH ROW "
        "EXECUTE FUNCTION mrf.guard_pd_generic_dataset_finalization()"
    )


async def _invoke(driver, target, kind, key):
    if target == "generic":
        await _generic_header(driver)
    else:
        await _header(driver, valid=True)
    table = "cms_creator_header" if target == "generic" else "cms_replay_header"
    for status in ("validated", "published"):
        await driver.execute("SAVEPOINT creator_probe")
        if kind == "defaults":
            with pytest.raises(asyncpg.AmbiguousFunctionError):
                await driver.execute(f"UPDATE {table} SET status=$1", status)
            await driver.execute("ROLLBACK TO SAVEPOINT creator_probe")
            assert await driver.fetchval(f"SELECT status FROM {table}") == "acquiring"
        else:
            await driver.execute(f"UPDATE {table} SET status=$1", status)
            assert await driver.fetchval(f"SELECT status FROM {table}") == status
        await driver.execute("RELEASE SAVEPOINT creator_probe")
        assert await _attacker_locks(driver, [key]) == []
    await driver.execute("DROP TABLE pg_temp." + table)


@pytest.mark.parametrize("target", list(_TARGETS))
@pytest.mark.parametrize("kind", ["varchar", "defaults", "variadic"])
async def test_late_creator_preserves_guard_binding(native_layout, target, kind):
    """Source-body checks precede real sibling creation and disjoint staging COPY."""
    admin, key = native_layout, int(uuid4().hex[:15], 16)
    incumbent_count = await admin.fetchval("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset")
    async with _creator(admin) as creator:
        before, hooks = await _routines(admin), await _hooks(admin)
        try:
            async with _reader(admin, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
                await _upgrade_guards(session, before, (True, True), retirement_typed=True, cms_parent=True)
                checked = await _routines(driver)
                callables.require_cms_terminal_guard_resolution(
                    checked,
                    catalog_schema="mrf",
                    catalog_schema_oid=await driver.fetchval("SELECT 'mrf'::regnamespace::oid::bigint"),
                    preparation_schema=_PREPARATION_SCHEMA,
                )
                await _late_overload(creator, target, kind, key)
                assert await _routines(driver) == checked
                await _invoke(driver, target, kind, key)
                assert (
                    await admin.fetchval("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset")
                    == incumbent_count
                )
                _assert_properties(before, await _routines(driver))
                assert await _hooks(driver) == hooks
        finally:
            for row in before:
                await admin.execute(row["native"]["body"])
            assert await _routines(admin) == before and await _hooks(admin) == hooks
