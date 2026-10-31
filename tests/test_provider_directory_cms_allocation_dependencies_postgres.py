# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Existing native dependency links; no protected issuance or cut acceptance."""

import os

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.engine import make_url

from process import provider_directory_cms_preparation_custody as custody
from process.provider_directory_cms_control_custody import _require_native_foreign_functions
from tests.cms_npd_admission_postgres_support import _owned_database

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def dependency_catalog():
    raw = os.getenv("HLTHPRT_CMS_LAYOUT_TEST_DSN")
    if not raw:
        pytest.skip("set HLTHPRT_CMS_LAYOUT_TEST_DSN for native allocation dependency checks")
    database_url = make_url(raw)
    assert database_url.host in {"127.0.0.1", "localhost"} and database_url.port and not database_url.query
    async with _owned_database(database_url) as (owned, _admin):
        connection = await asyncpg.connect(owned.set(drivername="postgresql").render_as_string(hide_password=False))
        try:
            await connection.execute("CREATE SCHEMA allocation_fixture")
            await connection.execute("CREATE TABLE allocation_fixture.parent(id text PRIMARY KEY)")
            await connection.execute("CREATE TABLE allocation_fixture.admission(id text PRIMARY KEY)")
            # Reproduce existing legacy FK metadata, never a new product relationship.
            await connection.execute(
                "CREATE TABLE allocation_fixture.child(id text PRIMARY KEY,parent_id text,"
                "CONSTRAINT existing_relationship FOREIGN KEY(parent_id) REFERENCES allocation_fixture.parent(id))"
            )
            await connection.execute(
                "CREATE FUNCTION allocation_fixture.known_guard() RETURNS trigger LANGUAGE plpgsql "
                "AS $$BEGIN RETURN NEW; END$$"
            )
            for root in ("parent", "admission"):
                await connection.execute(
                    "CREATE CONSTRAINT TRIGGER known_guard AFTER INSERT ON allocation_fixture."
                    + root
                    + " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION allocation_fixture.known_guard()"
                )
            yield connection
        finally:
            await connection.close()


@pytest_asyncio.fixture(loop_scope="module")
async def dependencies(dependency_catalog):
    transaction = dependency_catalog.transaction()
    await transaction.start()
    try:
        await dependency_catalog.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        yield dependency_catalog
    finally:
        await transaction.rollback()


async def _arguments(connection):
    parent, admission, namespace = await connection.fetchrow(
        "SELECT 'allocation_fixture.parent'::regclass::oid::bigint,"
        "'allocation_fixture.admission'::regclass::oid::bigint,'allocation_fixture'::regnamespace::oid::bigint"
    )
    relations = [parent, admission]
    routines = await connection.fetch(custody._ALLOCATION_ROUTINES_SQL, relations, [], [admission])
    native_oids = [
        row["oid"] for row in routines if custody._decoded(row["native"])["definition"]["pronamespace"] == "11"
    ]
    await _require_native_foreign_functions(connection, set(native_oids))
    return [row["oid"] for row in routines], relations, [11, namespace], [admission]


async def test_native_constraint_trigger_links_preserve_admission_exclusion(dependencies):
    await dependencies.execute("ALTER TABLE allocation_fixture.child DROP CONSTRAINT existing_relationship")
    arguments = await _arguments(dependencies)
    assert await dependencies.fetchval(custody._ALLOCATION_DEPENDENCIES_SQL, *arguments) is True
    guard = await dependencies.fetchval("SELECT 'allocation_fixture.known_guard()'::regprocedure::oid::bigint")
    arguments[0].remove(guard)
    assert await dependencies.fetchval(custody._ALLOCATION_DEPENDENCIES_SQL, *arguments) is False
    await dependencies.execute("DROP TRIGGER known_guard ON allocation_fixture.parent")
    assert await dependencies.fetchval(custody._ALLOCATION_DEPENDENCIES_SQL, *arguments) is True


async def test_native_external_fk_is_only_a_parent_action_link(dependencies):
    await dependencies.execute("DROP TRIGGER known_guard ON allocation_fixture.parent")
    arguments = await _arguments(dependencies)
    child = await dependencies.fetchval("SELECT 'allocation_fixture.child'::regclass::oid::bigint")
    assert child not in arguments[1]
    assert await dependencies.fetchval(custody._ALLOCATION_DEPENDENCIES_SQL, *arguments) is True
    arguments[0].pop()
    assert await dependencies.fetchval(custody._ALLOCATION_DEPENDENCIES_SQL, *arguments) is False


@pytest.mark.parametrize(
    "damage",
    [
        "tgisinternal=false",
        "tgtype=5",
        "tgenabled='D'",
        "tgnargs=1",
        "tgattr='1'::pg_catalog.int2vector",
        "tgconstrrelid=0",
        "tgconstrindid=0",
        "tgconstraint=0",
    ],
)
async def test_native_external_fk_link_shape_changes_refuse(dependencies, damage):
    arguments = await _arguments(dependencies)
    assert await dependencies.fetchval(custody._ALLOCATION_DEPENDENCIES_SQL, *arguments) is True
    await dependencies.execute(
        "UPDATE pg_catalog.pg_trigger SET " + damage + " WHERE oid=(SELECT min(oid) FROM pg_catalog.pg_trigger "
        "WHERE tgrelid='allocation_fixture.parent'::regclass AND tgisinternal)"
    )
    assert await dependencies.fetchval(custody._ALLOCATION_DEPENDENCIES_SQL, *arguments) is False


async def test_native_constraint_trigger_link_must_match_the_same_relation(dependencies):
    arguments = await _arguments(dependencies)
    await dependencies.execute(
        "UPDATE pg_catalog.pg_constraint SET conname='misplaced_guard',conrelid='allocation_fixture.admission'::regclass "
        "WHERE oid=(SELECT tgconstraint FROM pg_catalog.pg_trigger "
        "WHERE tgrelid='allocation_fixture.parent'::regclass AND tgname='known_guard')"
    )
    assert await dependencies.fetchval(custody._ALLOCATION_DEPENDENCIES_SQL, *arguments) is False
