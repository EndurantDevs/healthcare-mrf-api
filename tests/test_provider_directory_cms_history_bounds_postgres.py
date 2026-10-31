# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native CHECK mechanics only; no protected profile, full finisher or cut acceptance."""

import os

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.engine import make_url

from process import provider_directory_cms_history_bounds as bounds
from tests.cms_npd_admission_postgres_support import _owned_database

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def native_tables():
    raw = os.getenv("HLTHPRT_CMS_LAYOUT_TEST_DSN")
    if not raw:
        pytest.skip("set HLTHPRT_CMS_LAYOUT_TEST_DSN for the native bound prerequisite proof")
    source = make_url(raw)
    assert source.host in {"127.0.0.1", "localhost"} and source.port and not source.query
    async with _owned_database(source) as (owned, _admin):
        connection = await asyncpg.connect(owned.set(drivername="postgresql").render_as_string(hide_password=False))
        try:
            await connection.execute("CREATE SCHEMA bound_fixture")
            await connection.execute("CREATE TABLE bound_fixture.history_heap(dataset_id varchar(96) NOT NULL)")
            yield connection, owned.set(drivername="postgresql").render_as_string(hide_password=False)
        finally:
            await connection.close()


@pytest_asyncio.fixture(loop_scope="module")
async def bound_connection(native_tables):
    connection = native_tables[0]
    transaction = connection.transaction()
    await transaction.start()
    try:
        await connection.execute("SET LOCAL search_path=pg_catalog")
        yield connection
    finally:
        await transaction.rollback()


async def _row(connection):
    oid = await connection.fetchval("SELECT 'bound_fixture.history_heap'::regclass::oid")
    rows = await connection.fetch(bounds._READ_SQL, [oid])
    assert len(rows) == 1
    return rows[0]["bound"]


async def _install(connection, dataset_ids, *, validate):
    literals = bounds.history._bounds(dataset_ids)
    expression = "dataset_id IN (" + literals + ")" if literals else "false"
    await connection.execute(
        "ALTER TABLE bound_fixture.history_heap ADD CONSTRAINT cms_history_bound CHECK (" + expression + ") NOT VALID"
    )
    if validate:
        await connection.execute("ALTER TABLE bound_fixture.history_heap VALIDATE CONSTRAINT cms_history_bound")


@pytest.mark.parametrize("standard_strings", [True, False])
@pytest.mark.parametrize("dataset_ids", [[], ["one"], ["one", "two"], ["a'quote", "b\\slash", "c\nline", "é"]])
async def test_native_empty_single_multi_and_escaped_source_bounds(bound_connection, standard_strings, dataset_ids):
    connection = bound_connection
    await _install(connection, dataset_ids, validate=True)
    await connection.execute("SET LOCAL standard_conforming_strings=" + ("on" if standard_strings else "off"))
    proof = bounds.require_cms_history_bound(await _row(connection), dataset_ids)
    assert proof["constraint"]["validated"] is True and proof["constraint"]["enforced"] is True
    assert proof["constraint"]["oid"] > 0


async def test_native_missing_unvalidated_and_different_bounds_refuse(bound_connection):
    connection = bound_connection
    with pytest.raises(ValueError, match="validated_bound_required"):
        bounds.require_cms_history_bound(await _row(connection), ["one"])
    await _install(connection, ["one"], validate=False)
    with pytest.raises(ValueError, match="validated_bound_changed"):
        bounds.require_cms_history_bound(await _row(connection), ["one"])
    await connection.execute("ALTER TABLE bound_fixture.history_heap VALIDATE CONSTRAINT cms_history_bound")
    with pytest.raises(ValueError, match="validated_bound_changed"):
        bounds.require_cms_history_bound(await _row(connection), ["two"])


async def test_native_attachment_retains_exact_constraint_tree_and_identity(bound_connection):
    connection = bound_connection
    await _install(connection, ["one", "two"], validate=True)
    before = bounds.require_cms_history_bound(await _row(connection), ["one", "two"])
    await connection.execute(
        "CREATE TABLE bound_fixture.snapshot_parent(dataset_id varchar(96) NOT NULL) PARTITION BY LIST(dataset_id)"
    )
    await connection.execute(
        "ALTER TABLE bound_fixture.snapshot_parent ATTACH PARTITION bound_fixture.history_heap FOR VALUES IN ('one','two')"
    )
    assert bounds.require_cms_history_bound(await _row(connection), ["one", "two"]) == before


async def test_native_read_is_readonly_and_does_not_change_catalog(native_tables):
    connection, dsn = native_tables
    await _install(connection, ["one"], validate=True)
    observer = await asyncpg.connect(dsn)
    try:
        before = await _row(connection)
        async with observer.transaction(readonly=True):
            proof = bounds.require_cms_history_bound(await _row(observer), ["one"])
            assert proof["constraint"]["oid"] > 0
            assert await observer.fetchval("SELECT pg_current_xact_id_if_assigned()::text") is None
            assert await connection.fetchval("SELECT count(*) FROM bound_fixture.history_heap") == 0
        assert bounds._decoded(before) == bounds._decoded(await _row(connection))
    finally:
        await observer.close()
        await connection.execute("ALTER TABLE bound_fixture.history_heap DROP CONSTRAINT cms_history_bound")
