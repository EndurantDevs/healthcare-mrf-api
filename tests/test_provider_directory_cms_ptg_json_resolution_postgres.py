# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual helper dispatch/parity and updater custody; never CMS launch authority."""

import json
import os
from contextlib import asynccontextmanager
from hashlib import sha256
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from db import migration_cms_ptg_json_resolution as migration
from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_source_policy import source_transaction
from process.provider_directory_rooted_graph_single_root_contract import derive_single_root_identity
from process.provider_directory_rooted_graph_twin_store import _ADMISSION_COLUMNS, admit_rooted_graph_single_root
from tests.provider_directory_rooted_graph_rotation_pg_support import publish_legacy_root
from tests.test_provider_directory_cms_canonical_layout_postgres import native_layout as native_layout
from tests.test_provider_directory_cms_creator_dispatch_postgres import _creator
from tests.test_provider_directory_rooted_graph_acquisition_postgres import _complete_success
from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope

pytestmark = pytest.mark.asyncio(loop_scope="module")
_SIGNATURE = '"mrf"."ptg_wave_canonical_json_ascii_v1"(pg_catalog.jsonb)'
_MARKER = "cms_ptg_resolution_probe"
_JSON_COLUMNS = {"reviewed_root_policy_json", "request_failure_coverage"}
_PAYLOADS = (
    "null",
    "true",
    "false",
    "17",
    "-23",
    "1.25",
    '"plain"',
    "[]",
    "{}",
    '{"z":1,"a":2,"z":3}',
    '{"z":[3,2,1],"a":{"b":false,"a":null}}',
    json.dumps({"é": "漢字", "😀": ["é", "😀"], "a": "ASCII"}, ensure_ascii=False),
    json.dumps({'quote"slash\\tab\t': 'line\n\r\t"\\\b\f', "control": "\x01\x7f", "literal": "\\u0000"}),
)


async def _snapshot(driver, signature=_SIGNATURE):
    observed_by_field = dict(
        await driver.fetchrow(
            "SELECT p.oid::bigint,pg_catalog.to_jsonb(p)-'prosrc' AS identity,p.prosrc,"
            "pg_catalog.pg_get_functiondef(p.oid) AS definition,pg_catalog.obj_description(p.oid,'pg_proc') AS description,"
            "(SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) ORDER BY t.oid) FROM pg_catalog.pg_trigger t "
            "WHERE t.tgfoid=p.oid) AS hooks,(SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(d) "
            "ORDER BY d.classid,d.objid,d.objsubid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype) "
            "FROM pg_catalog.pg_depend d WHERE (d.classid='pg_catalog.pg_proc'::regclass AND d.objid=p.oid) "
            "OR (d.refclassid='pg_catalog.pg_proc'::regclass AND d.refobjid=p.oid)) AS dependencies "
            "FROM pg_catalog.pg_proc p WHERE p.oid=pg_catalog.to_regprocedure($1)",
            signature,
        )
    )
    for field in ("identity", "hooks", "dependencies"):
        if isinstance(observed_by_field[field], str):
            observed_by_field[field] = json.loads(observed_by_field[field])
    return observed_by_field


def _pins(snapshot):
    return dict(oid=snapshot["oid"], owner_oid=int(snapshot["identity"]["proowner"]))


@asynccontextmanager
async def _operator(admin, isolation="READ COMMITTED"):
    url = make_url(os.environ["HLTHPRT_CMS_LAYOUT_TEST_DSN"]).set(
        drivername="postgresql+asyncpg", database=await admin.fetchval("SELECT current_database()")
    )
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                await connection.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL " + isolation)
                await connection.exec_driver_sql("SET LOCAL search_path=pg_catalog,pg_temp")
                await connection.exec_driver_sql("SET LOCAL jit=off")
                driver = (await connection.get_raw_connection()).driver_connection
                yield connection, driver
            finally:
                if transaction.is_active:
                    await transaction.rollback()
    finally:
        await engine.dispose()


async def _settings(connection, mode):
    path = "pg_catalog,mrf" + (",pg_temp" if mode == "target" else "")
    for signature in (_SIGNATURE, '"mrf"."ptg_wave_json_ascii_text_v1"(pg_catalog.text)'):
        await connection.exec_driver_sql("ALTER FUNCTION " + signature + " SET search_path TO " + path)


async def _upgrade(connection, pins):
    await connection.run_sync(lambda sync: migration.upgrade_cms_ptg_json_resolution(sync, "mrf", expected=pins))


async def _canonical(driver, payload):
    value = await driver.fetchval("SELECT " + _SIGNATURE.split("(")[0] + "($1::pg_catalog.jsonb)", payload)
    digest = await driver.fetchval(
        "SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to($1::text,'UTF8')),'hex')", value
    )
    return value, digest


def _require_properties(before, after):
    assert after["identity"] == before["identity"]
    assert all(after[field] == before[field] for field in ("oid", "description", "hooks", "dependencies"))
    old, new = migration.ptg_json_bodies("mrf")
    assert before["prosrc"] == old and after["prosrc"] == new
    assert after["definition"] == before["definition"].replace(old, new, 1)


async def _marker_seen(observer, pid, key):
    return await observer.fetchval(
        "SELECT EXISTS(SELECT FROM pg_catalog.pg_locks WHERE pid=$1 AND locktype='advisory' AND granted "
        "AND classid=($2::bigint>>32)::oid AND objid=($2::bigint&4294967295)::oid AND objsubid=1)",
        pid,
        key,
    )


async def _install_hostile(creator, key):
    connection, _stage, signatures, _stages = creator
    signature = '"mrf"."to_jsonb"(pg_catalog.text)'
    assert await connection.fetchval("SELECT pg_catalog.to_regprocedure($1) IS NULL", signature)
    signatures.append(signature)
    await connection.execute(
        'CREATE FUNCTION "mrf"."to_jsonb"(pg_catalog.text) RETURNS pg_catalog.jsonb LANGUAGE plpgsql '
        f"AS $probe$ BEGIN PERFORM pg_catalog.pg_advisory_lock({key}::pg_catalog.int8); "
        f"RAISE EXCEPTION '{_MARKER}'; END $probe$"
    )


async def _hostile_call(admin, driver, key, *, hardened):
    savepoint = driver.transaction()
    await savepoint.start()
    try:
        if hardened:
            assert await _canonical(driver, '{"nested":{"a":1}}') == (
                '{"nested":{"a":1}}',
                sha256(b'{"nested":{"a":1}}').hexdigest(),
            )
        else:
            with pytest.raises(asyncpg.RaiseError, match=_MARKER):
                await _canonical(driver, '{"nested":{"a":1}}')
    finally:
        await savepoint.rollback()
    assert await _marker_seen(admin, driver.get_server_pid(), key) is not hardened
    if not hardened:
        assert await driver.fetchval("SELECT pg_catalog.pg_advisory_unlock($1::bigint)", key)


@pytest.mark.parametrize("mode", ["legacy", "target"])
@pytest.mark.parametrize("isolation", ["READ COMMITTED", "SERIALIZABLE"])
async def test_real_helper_old_and_new_cold_warm_dispatch(native_layout, mode, isolation):
    admin, key = native_layout, int(uuid4().hex[:15], 16)
    original = await _snapshot(admin)
    async with _creator(admin) as creator:
        await _install_hostile(creator, key)
        async with _operator(admin, isolation) as (connection, driver):
            await _settings(connection, mode)
            before = await _snapshot(driver)
            for _temperature in ("cold", "warm"):
                await _hostile_call(admin, driver, key, hardened=False)
            await _upgrade(connection, _pins(before))
            after = await _snapshot(driver)
            _require_properties(before, after)
            for _temperature in ("cold", "warm"):
                await _hostile_call(admin, driver, key, hardened=True)
            assert connection.in_transaction()
    assert await _snapshot(admin) == original


@pytest.mark.parametrize("mode", ["legacy", "target"])
@pytest.mark.parametrize("isolation", ["READ COMMITTED", "SERIALIZABLE"])
async def test_canonical_bytes_hashes_and_installer_properties(native_layout, mode, isolation):
    admin = native_layout
    original = await _snapshot(admin)
    ascii_signature = '"mrf"."ptg_wave_json_ascii_text_v1"(pg_catalog.text)'
    ascii_original = await _snapshot(admin, ascii_signature)
    sibling = await _snapshot(admin, '"mrf"."ptg_wave_canonical_json_ascii_v1"(pg_catalog.json)')
    async with _operator(admin, isolation) as (connection, driver):
        await _settings(connection, mode)
        await connection.exec_driver_sql("COMMENT ON FUNCTION " + _SIGNATURE + " IS 'canonical parity fixture'")
        before = await _snapshot(driver)
        expected_values = [
            json.dumps(json.loads(payload), sort_keys=True, ensure_ascii=True, separators=(",", ":"))
            for payload in _PAYLOADS
        ]
        previous = [await _canonical(driver, payload) for payload in _PAYLOADS]
        assert previous == [(value, sha256(value.encode()).hexdigest()) for value in expected_values]
        assert await _canonical(driver, None) == (None, None)
        await _upgrade(connection, _pins(before))
        after = await _snapshot(driver)
        _require_properties(before, after)
        await _upgrade(connection, _pins(before))
        assert await _snapshot(driver) == after
        for _temperature in ("cold", "warm"):
            assert [await _canonical(driver, payload) for payload in _PAYLOADS] == previous
        assert await _snapshot(driver, '"mrf"."ptg_wave_canonical_json_ascii_v1"(pg_catalog.json)') == sibling
        assert connection.in_transaction()
    assert await _snapshot(admin) == original
    assert await _snapshot(admin, ascii_signature) == ascii_original


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "SERIALIZABLE"])
async def test_installer_savepoint_preserves_caller_after_real_ddl(native_layout, isolation):
    admin = native_layout
    original = await _snapshot(admin)
    failure = RuntimeError("after actual canonical helper replacement")
    async with _operator(admin, isolation) as (connection, driver):

        def interrupted(sync):
            class InterruptedConnection:
                def __getattr__(self, name):
                    return getattr(sync, name)

                def exec_driver_sql(self, statement):
                    value = sync.exec_driver_sql(statement)
                    if statement.startswith("CREATE OR REPLACE FUNCTION"):
                        raise failure
                    return value

            migration.upgrade_cms_ptg_json_resolution(InterruptedConnection(), "mrf", expected=_pins(original))

        with pytest.raises(RuntimeError) as observed:
            await connection.run_sync(interrupted)
        assert observed.value is failure and connection.in_transaction()
        assert await _snapshot(driver) == original
    assert await _snapshot(admin) == original


@pytest.mark.parametrize("field", ["oid", "owner_oid"])
async def test_installer_refuses_changed_protected_identity(native_layout, field):
    admin = native_layout
    original = await _snapshot(admin)
    pins = _pins(original)
    pins[field] += 1
    async with _operator(admin, "SERIALIZABLE") as (connection, driver):
        with pytest.raises(RuntimeError, match="function_changed"):
            await _upgrade(connection, pins)
        assert connection.in_transaction() and await _snapshot(driver) == original
    assert await _snapshot(admin) == original


async def _genuine_admission(context):
    current = await publish_legacy_root(context.database)
    operation = "7" * 64
    identity = derive_single_root_identity(current, operation_key=operation)
    sealed = await _complete_success(context.database, identity.candidate)
    failure = RuntimeError("retain genuine admission inputs then roll back")
    columns = tuple(name for name in _ADMISSION_COLUMNS if name != "admitted_at")
    async with source_transaction(context.database):
        try:
            async with context.database.transaction():
                admission = await admit_rooted_graph_single_root(
                    identity.candidate.acquisition_id, acquisition_operation_key=operation, database=context.database
                )
                assert admission.rooted_graph_sha256 == sealed.rooted_graph_sha256
                observed = await context.database.first(
                    "SELECT " + ",".join(columns) + " FROM mrf.provider_directory_rooted_graph_twin_admission "
                    "WHERE admission_id=:identity",
                    identity=admission.admission_id,
                )
                values_by_column = dict(observed._mapping)
                raise failure
        except RuntimeError as observed:
            assert observed is failure
    assert not await context.connection.fetchval(
        "SELECT EXISTS(SELECT FROM mrf.provider_directory_rooted_graph_twin_admission)"
    )
    return values_by_column


async def _admission_probe(context, publisher, values_by_column, key, *, hardened):
    transaction = publisher.transaction()
    await transaction.start()
    try:
        await publisher.execute("SET LOCAL ROLE " + storage_identifier(context.component.policy.owner_role))
        assert not await publisher.fetchval("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user")
        columns = tuple(values_by_column)
        arguments = [
            json.dumps(column_value)
            if name in _JSON_COLUMNS and column_value is not None and not isinstance(column_value, str)
            else column_value
            for name, column_value in values_by_column.items()
        ]
        sql = (
            "INSERT INTO mrf.provider_directory_rooted_graph_twin_admission ("
            + ",".join(columns)
            + ") VALUES ("
            + ",".join("$" + str(index) for index in range(1, len(columns) + 1))
            + ")"
        )
        if hardened:
            assert await publisher.execute(sql, *arguments) == "INSERT 0 1"
        else:
            with pytest.raises(asyncpg.RaiseError, match=_MARKER):
                await publisher.execute(sql, *arguments)
    finally:
        await transaction.rollback()
    assert await _marker_seen(context.connection, publisher.get_server_pid(), key) is not hardened
    if not hardened:
        assert await publisher.fetchval("SELECT pg_catalog.pg_advisory_unlock($1::bigint)", key)
    assert not await context.connection.fetchval(
        "SELECT EXISTS(SELECT FROM mrf.provider_directory_rooted_graph_twin_admission)"
    )


async def _rooted_dispatch(context, values_by_column, key, original):
    publisher = await asyncpg.connect(
        context.url.set(drivername="postgresql", username=context.component.publisher).render_as_string(
            hide_password=False
        )
    )
    try:
        for _temperature in ("cold", "warm"):
            await _admission_probe(context, publisher, values_by_column, key, hardened=False)
        async with _operator(context.connection, "SERIALIZABLE") as (connection, driver):
            await _upgrade(connection, _pins(original))
            _require_properties(original, await _snapshot(driver))
            await connection.commit()
        for _temperature in ("cold", "warm"):
            await _admission_probe(context, publisher, values_by_column, key, hardened=True)
    finally:
        await publisher.close()


async def test_actual_rooted_trigger_keeps_existing_ordinary_actor(monkeypatch):
    """DB-trigger replay of genuine rolled-back inputs; no Source-successor adoption."""
    async with _lifecycle_scope(monkeypatch) as context:
        values_by_column = await _genuine_admission(context)
        original = await _snapshot(context.connection)
        key = int(uuid4().hex[:15], 16)
        try:
            async with _creator(context.connection) as creator:
                await _install_hostile(creator, key)
                await _rooted_dispatch(context, values_by_column, key, original)
        finally:
            await context.connection.execute(original["definition"])
            assert await _snapshot(context.connection) == original
