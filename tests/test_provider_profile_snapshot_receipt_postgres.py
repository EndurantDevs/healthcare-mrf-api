# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read accepted common generations through actual receipt guards and read snapshots."""

import asyncio
import json

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api import provider_profile_snapshot as snapshot
from process import provider_directory_cms_serving_receipt as receipts
from tests import test_provider_directory_cms_serving_receipt_postgres as receipt_fixture
from tests.provider_profile_snapshot_postgres_support import (
    create_composite_families,
    grant_profile_reader,
    snapshot_database,
)


async def _publish_dependency(database, schema, prior):
    """Include a real non-CMS dataset pin in the next accepted source vector."""
    async with database.engine.begin() as connection:
        await connection.execute(
            text(f"INSERT INTO {schema}.provider_directory_source VALUES ('other','other-endpoint')")
        )
        await connection.execute(
            text(f"""INSERT INTO {schema}.provider_directory_endpoint_dataset
            SELECT 'other-dataset','other-endpoint',dataset_hash,acquisition_root_run_id,status,is_current,
                published_at,'{{"source_ids":["other"]}}',content_proof_admission_sha256,publication_metadata_sha256
            FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id='dataset'""")
        )
        pins = [
            receipt_fixture._PIN,
            {
                **receipt_fixture._PIN,
                "source_id": "other",
                "endpoint_id": "other-endpoint",
                "dataset_id": "other-dataset",
            },
        ]
        await connection.execute(
            text(f"""UPDATE {schema}.provider_directory_profile_serving_generation
            SET generation_id=:generation,source_vector_json=:vector"""),
            {
                "generation": "pdprofile_" + "2" * 32,
                "vector": json.dumps([{key: pin[key] for key in ("source_id", "dataset_id")} for pin in pins]),
            },
        )
        receipt_payload = await receipt_fixture._payload(connection, schema, prior)
        receipt_payload["desired_datasets"] = pins
        receipt_id = await receipts.append_serving_receipt(connection, schema, receipt_payload)
    return {"receipt_id": receipt_id, "payload": receipt_payload}


async def test_dependency_drift_keeps_accepted_generation_readable(monkeypatch):
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        await create_composite_families(database, schema)
        initial = await receipt_fixture._publish_initial(database.engine, schema)
        accepted = await _publish_dependency(database, schema, initial)
        await database.status(f"UPDATE {schema}.address_alias_state_v1 SET generation=1")
        await database.status(f"""UPDATE {schema}.provider_directory_endpoint_dataset
            SET status='superseded',is_current=false WHERE dataset_id='other-dataset'""")
        async with snapshot.provider_profile_read_snapshot(database, schema) as session:
            assert session.info["provider_profile_cms_serving_receipt"] == accepted
            assert set(session.info["provider_profile_native_authorities"]) == {"cms-doctors", "entity-address"}
            assert not await session.scalar(
                text(f"SELECT {schema}.cms_serving_receipt_matches(CAST(:payload AS jsonb))"),
                {"payload": json.dumps(accepted["payload"])},
            )
        assert "provider_profile_cms_serving_receipt" not in session.info


@pytest.mark.parametrize("install_receipt", [False, True])
async def test_cms_profile_requires_a_committed_common_proof(monkeypatch, install_receipt):
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        await create_composite_families(database, schema, install_receipt=False)
        async with database.engine.begin() as connection:
            await receipt_fixture._seed_profile(connection, schema)
            if install_receipt:
                await connection.run_sync(lambda sync: receipt_fixture._apply(sync, "20260930100000"))
        with pytest.raises(ServiceUnavailable):
            async with snapshot.provider_profile_read_snapshot(database, schema):
                pytest.fail("CMS Profile cannot be read without its common serving proof")
        assert database._transaction_binding() is None and snapshot._SNAPSHOT.get() is None


@pytest.mark.parametrize("table", ["provider_directory_profile", "provider_directory_address_overlay"])
@pytest.mark.parametrize("include_detail", [False, True])
async def test_replaced_projection_cannot_reuse_common_proof(monkeypatch, table, include_detail):
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        await create_composite_families(database, schema)
        await receipt_fixture._publish_initial(database.engine, schema)
        await database.status(f"ALTER TABLE {schema}.{table} RENAME TO abandoned_projection")
        await database.status(f"CREATE TABLE {schema}.{table} (synthetic_id int)")
        await grant_profile_reader(database, schema, (table,))
        with pytest.raises(ServiceUnavailable):
            async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=include_detail):
                pytest.fail("A different physical projection cannot inherit the accepted proof")


async def test_withdrawal_still_requires_its_common_proof(monkeypatch):
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        await create_composite_families(database, schema)
        initial = await receipt_fixture._publish_initial(database.engine, schema)
        async with database.engine.begin() as connection:
            await connection.execute(
                text(f"""UPDATE {schema}.provider_directory_profile_serving_generation
                SET generation_id='pdprofile_{"3" * 32}',status='purged',operation='purge',source_vector_json='[]'""")
            )
            payload = await receipt_fixture._payload(connection, schema, initial)
            payload["desired_datasets"] = []
            receipt_id = await receipts.append_serving_receipt(connection, schema, payload)
        async with snapshot.provider_profile_read_snapshot(database, schema) as session:
            assert session.info["provider_profile_cms_serving_receipt"]["receipt_id"] == receipt_id
        await database.status(f"ALTER TABLE {schema}.provider_directory_address_overlay RENAME TO abandoned_overlay")
        await database.status(f"CREATE TABLE {schema}.provider_directory_address_overlay (synthetic_id int)")
        await grant_profile_reader(database, schema, ("provider_directory_address_overlay",))
        with pytest.raises(ServiceUnavailable):
            async with snapshot.provider_profile_read_snapshot(database, schema):
                pytest.fail("Removing CMS from Profile does not remove the accepted address proof")


async def _replace_overlay(database, schema, prior, started):
    """Commit a successor only after the old response releases its physical relations."""
    async with database.session_factory() as writer, writer.begin():
        await writer.execute(text("SET LOCAL statement_timeout='5s'"))
        started.set()
        await writer.execute(text(f"ALTER TABLE {schema}.provider_directory_address_overlay RENAME TO old_overlay"))
        await writer.execute(text(f"ALTER TABLE {schema}.next_overlay RENAME TO provider_directory_address_overlay"))
        await receipt_fixture._advance_native(writer, schema)
        payload = await receipt_fixture._payload(writer, schema, prior)
        receipt_id = await receipts.append_serving_receipt(writer, schema, payload)
    return {"receipt_id": receipt_id, "payload": payload}


async def test_common_proof_and_projection_cross_cutover_together(monkeypatch):
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        await create_composite_families(database, schema)
        initial = await receipt_fixture._publish_initial(database.engine, schema)
        await database.status(f"CREATE TABLE {schema}.next_overlay (synthetic_id int)")
        await grant_profile_reader(database, schema, ("next_overlay",))
        started = asyncio.Event()
        publication = None
        try:
            async with snapshot.provider_profile_read_snapshot(database, schema) as session:
                publication = asyncio.create_task(_replace_overlay(database, schema, initial, started))
                await asyncio.wait_for(started.wait(), 3)
                assert session.info["provider_profile_cms_serving_receipt"] == initial
                assert await receipts.read_serving_receipt(session, schema) == initial
            accepted = await asyncio.wait_for(publication, 3)
        finally:
            if publication is not None:
                publication.cancel()
                await asyncio.gather(publication, return_exceptions=True)
        async with snapshot.provider_profile_read_snapshot(database, schema) as session:
            assert session.info["provider_profile_cms_serving_receipt"] == accepted
            assert accepted["payload"]["overlay_oid"] != initial["payload"]["overlay_oid"]
