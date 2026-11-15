# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual scalar serving functions retain history, logical data checks and rollback."""

import json

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_serving_receipt as receipts
from tests.test_provider_directory_cms_serving_receipt_postgres import (
    _advance_native,
    _apply,
    _database,
    _payload,
    _seed_profile,
    _seed_source,
)


async def _publish_legacy(engine, schema):
    async with engine.begin() as connection:
        await _seed_source(connection, schema)
        await _seed_profile(connection, schema)
        await _advance_native(connection, schema, doctors=True)
        await _advance_native(connection, schema)
        payload = await _payload(connection, schema)
        receipt_id = await connection.scalar(
            text(f"""INSERT INTO {schema}.provider_directory_cms_serving_receipt(receipt_id,payload)
            SELECT encode(sha256(convert_to(p::text,'UTF8')),'hex'),p FROM (SELECT CAST(:payload AS jsonb) p) document
            RETURNING receipt_id"""),
            dict(payload=json.dumps(payload)),
        )
    return dict(receipt_id=receipt_id, payload=payload)


async def _named_migrations(connection):
    for prefix in ("20261016030000", "20261114180000", "20260930120000", "20261019130000", "20261114190000"):
        await connection.run_sync(lambda sync: _apply(sync, prefix))


async def _named_doctors_advance(connection, schema):
    await connection.execute(
        text(f"""UPDATE {schema}.reference_family_result_generation SET
        local_generation=local_generation+1,origin_lineage_id=local_lineage_id,
        origin_generation=local_generation+1,published_at=clock_timestamp() WHERE importer_id='cms-doctors'""")
    )


async def _matches(connection, schema, name, payload):
    return await connection.scalar(
        text(f"SELECT {schema}.{name}(CAST(:payload AS jsonb))"), dict(payload=json.dumps(payload))
    )


@pytest.mark.asyncio
async def test_actual_named_serving_upgrade_replays_and_preserves_opaque_legacy_history(monkeypatch):
    async with _database(monkeypatch, named=False) as (engine, schema):
        legacy = await _publish_legacy(engine, schema)
        async with engine.begin() as connection:
            async with connection.begin_nested() as rollback:
                await _named_migrations(connection)
                assert "relations" in (await receipts.capture_native_dependencies(connection, schema))["doctors"]
                await rollback.rollback()
            assert await connection.scalar(
                text(
                    f"SELECT EXISTS(SELECT FROM information_schema.columns WHERE table_schema=:schema AND table_name='reference_family_result_generation' AND column_name='relation_oids')"
                ),
                dict(schema=schema),
            )
            assert await receipts.read_current_receipt(connection, schema) == legacy
            await _named_migrations(connection)
        async with engine.begin() as connection:
            await connection.run_sync(lambda sync: _apply(sync, "20261114190000"))
            snapshot = await receipts.capture_native_dependencies(connection, schema)
            assert "overlay" in snapshot and "overlay_oid" not in snapshot
            assert "relations" in snapshot["doctors"] and "relation_oids" not in snapshot["doctors"]
            assert await receipts.read_current_receipt(connection, schema) == legacy
            assert await receipts.read_serving_receipt(connection, schema) == legacy
            assert not await _matches(connection, schema, "cms_serving_receipt_matches", legacy["payload"])
            assert await receipts.verify_historical_receipt(connection, schema, legacy["receipt_id"], legacy["payload"])
            async with connection.begin_nested() as copied:
                await connection.execute(
                    text(f"CREATE TABLE {schema}.doctor_copy (LIKE {schema}.doctor_clinician_address INCLUDING ALL)")
                )
                await connection.execute(
                    text(f"INSERT INTO {schema}.doctor_copy SELECT * FROM {schema}.doctor_clinician_address")
                )
                await connection.execute(
                    text(f"ALTER TABLE {schema}.doctor_clinician_address RENAME TO retained_doctor")
                )
                await connection.execute(text(f"ALTER TABLE {schema}.doctor_copy RENAME TO doctor_clinician_address"))
                assert await receipts.capture_native_dependencies(connection, schema) == snapshot
                assert await receipts.read_current_receipt(connection, schema) == legacy
                await copied.rollback()
            for kind, field, replacement_value in (
                ("doctors", "local_generation", 99),
                ("profile", "evidence_rows", 99),
            ):
                changed_by_field = {**snapshot, kind: {**snapshot[kind], field: replacement_value}}
                assert not await _matches(connection, schema, "cms_serving_native_matches", changed_by_field)


@pytest.mark.asyncio
async def test_named_serving_functions_require_fresh_semantic_result_and_rollback(monkeypatch):
    async with _database(monkeypatch, named=False) as (engine, schema):
        legacy = await _publish_legacy(engine, schema)
        async with engine.begin() as connection:
            await _named_migrations(connection)
        with pytest.raises(DBAPIError, match="cms_serving_fresh_receipt_required"):
            async with engine.begin() as connection:
                await _named_doctors_advance(connection, schema)
        async with engine.begin() as connection:
            assert await receipts.read_current_receipt(connection, schema) == legacy
            await _named_doctors_advance(connection, schema)
            await _advance_native(connection, schema)
            await connection.execute(
                text(
                    f"UPDATE {schema}.provider_directory_profile_serving_generation SET generation_id=:generation,control_generation=control_generation+1"
                ),
                dict(generation="pdprofile_" + "2" * 32),
            )
            payload = await _payload(connection, schema, legacy)
            assert await _matches(connection, schema, "cms_serving_receipt_matches", payload)
            for field in ("dataset_hash", "acquisition_root_run_id"):
                changed_by_field = {**payload, "cms": {**payload["cms"], field: "changed"}}
                assert not await _matches(connection, schema, "cms_serving_receipt_matches", changed_by_field)
            receipt_id = await receipts.append_serving_receipt(connection, schema, payload)
        async with engine.connect() as connection:
            assert await receipts.read_current_receipt(connection, schema) == dict(
                receipt_id=receipt_id, payload=payload
            )
            assert await receipts.verify_historical_receipt(connection, schema, legacy["receipt_id"], legacy["payload"])
