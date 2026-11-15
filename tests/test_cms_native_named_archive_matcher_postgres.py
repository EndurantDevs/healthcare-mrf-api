# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual named archive matching, independent of full CMS publication."""

import hashlib
import json
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from process import provider_directory_cms_native_registration as registration
from tests.test_cms_archive_transaction_migration import _load
from tests.test_cms_native_input_named_revision_migration import _migration
from tests.test_reference_family_archive_postgres import _database_url


async def _install_and_merge(connection, schema):
    statements = []
    historical = _load("20260930120000")
    historical._schema = lambda: f'"{schema}"'
    historical.op = SimpleNamespace(execute=statements.append)
    historical.upgrade()
    receipt = _load("20260930100000")
    receipt.op = SimpleNamespace(execute=statements.append)
    receipt._create_archive_check(f'"{schema}"')
    for statement in statements:
        await connection.execute(statement)
    repair = _load("20261019130000")
    await connection.execute(repair._counter_sql(schema))
    await connection.execute(repair._repair_sql(schema))
    named = _migration()
    check, key = named._key_sql(schema)
    await connection.execute(check)
    await connection.execute(
        named._function_sql(
            schema,
            names=("cms_native_input_advance", "cms_native_input_revision_guard", "cms_serving_archive_result_matches"),
        )
    )
    await connection.execute(key)
    await connection.execute(
        f'CREATE TABLE "{schema}".provider_directory_cms_serving_receipt(receipt_id text PRIMARY KEY,payload jsonb)'
    )
    await connection.execute(f'CREATE TABLE "{schema}".address_archive_v2(address_key int PRIMARY KEY,value text)')
    await connection.execute(f"INSERT INTO \"{schema}\".address_archive_v2 VALUES (1,'old')")
    candidate = registration.NativeInputCandidate(schema, "address_archive_v2", schema, "address_archive_v2")
    evidence = await registration.register_native_input_candidates(
        connection, ledger_schema=schema, candidates=(candidate,)
    )
    await connection.execute(
        f"INSERT INTO \"{schema}\".address_archive_v2 VALUES (1,'new') ON CONFLICT(address_key) DO UPDATE SET value=EXCLUDED.value"
    )
    assert await connection.fetchval(f'SELECT revision FROM "{schema}".cms_native_input_revision') == 2
    row_document = await connection.fetchval(
        f'SELECT to_jsonb(archived)::text FROM "{schema}".address_archive_v2 archived'
    )
    archive_result_by_field = dict(
        target_schema=schema,
        target_table="address_archive_v2",
        from_revision=0,
        to_revision=2,
        native_input_hash=registration._native_enrollment_digest(evidence),
        delta_rows=1,
        delta_sha256=hashlib.sha256(row_document.encode()).hexdigest(),
    )
    return dict(predecessor_receipt_id=None, archive=archive_result_by_field, cms={"dataset_id": "current"})


async def _assert_current_result(connection, query, payload_by_field):
    assert await connection.fetchval(query, json.dumps(payload_by_field)) is True
    for changes in (
        dict(target_schema="other"),
        dict(target_table="other"),
        dict(to_revision=3),
        dict(delta_rows=-1),
        dict(delta_sha256="invalid"),
    ):
        assert (
            await connection.fetchval(
                query, json.dumps({**payload_by_field, "archive": {**payload_by_field["archive"], **changes}})
            )
            is False
        )


async def _assert_retained_result(connection, schema, query, payload_by_field):
    assert await connection.fetchval(query, json.dumps(payload_by_field)) is False
    legacy_result_by_field = {
        key: edition
        for key, edition in payload_by_field["archive"].items()
        if key not in {"target_schema", "target_table"}
    }
    legacy_result_by_field["target_oid"] = 17
    prior_by_field = dict(archive=legacy_result_by_field, cms={"dataset_id": "retained"})
    encoded = json.dumps(prior_by_field, sort_keys=True, separators=(",", ":"))
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_cms_serving_receipt VALUES ($1,$2::jsonb)', "previous", encoded
    )
    retained_by_field = dict(
        predecessor_receipt_id="previous", archive=legacy_result_by_field, cms=prior_by_field["cms"]
    )
    assert await connection.fetchval(query, json.dumps(retained_by_field)) is True
    assert (
        await connection.fetchval(query, json.dumps({**retained_by_field, "cms": {"dataset_id": "different"}})) is False
    )
    assert (
        json.loads(
            await connection.fetchval(f'SELECT payload::text FROM "{schema}".provider_directory_cms_serving_receipt')
        )
        == prior_by_field
    )


@pytest.mark.asyncio
async def test_named_archive_matcher_preserves_revision_transaction_and_opaque_history():
    url = make_url(_database_url()).set(drivername="postgresql")
    connection = await asyncpg.connect(url.render_as_string(hide_password=False), command_timeout=10)
    schema = "archive_" + uuid4().hex
    is_created = False
    try:
        async with connection.transaction():
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            is_created = True
            payload_by_field = await _install_and_merge(connection, schema)
            query = f'SELECT "{schema}".cms_serving_archive_result_matches($1::jsonb)'
            await _assert_current_result(connection, query, payload_by_field)
        async with connection.transaction():
            await _assert_retained_result(connection, schema, query, payload_by_field)
    finally:
        try:
            if is_created:
                await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await connection.close()
