# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native cross-query publication, cleanup, and optional legacy-read regressions."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api import provider_profile_snapshot as snapshot
from api.endpoint import npi
from tests.provider_profile_snapshot_postgres_support import publish_family, snapshot_database

NPI = 1000000004


async def test_profile_route_keeps_both_loaders_before_family_rename(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        reached_first_read = asyncio.Event()
        writer_started = asyncio.Event()
        loader_tasks = []

        async def state_loader(_npi):
            loader_tasks.append(asyncio.current_task())
            marker = await database.scalar(f'SELECT marker FROM "{schema}".cms_doctor_education')
            reached_first_read.set()
            await writer_started.wait()
            return {"marker": marker}

        async def profile_loader(_npis, **_options):
            loader_tasks.append(asyncio.current_task())
            marker = await database.scalar(f'SELECT marker FROM "{schema}".provider_directory_profile')
            return {NPI: {"profile": {"marker": marker}}}

        def compose(_npi, state, profile, _query):
            return {"generation_id": "a" * 64, "cms": state["marker"], "directory": profile["profile"]["marker"]}

        monkeypatch.setattr(npi, "fetch_provider_profile_projection", state_loader)
        monkeypatch.setattr(npi, "_fetch_provider_directory_profile_map", profile_loader)
        monkeypatch.setattr(npi, "_compose_requested_provider_profile", compose)
        async with database.session_factory() as writer:
            reader = asyncio.create_task(npi.get_provider_profile(SimpleNamespace(args={}), str(NPI)))
            await asyncio.wait_for(reached_first_read.wait(), 3)
            publication = asyncio.create_task(publish_family(writer, schema, writer_started))
            try:
                operation_result = await asyncio.wait_for(reader, 3)
                await asyncio.wait_for(publication, 3)
            finally:
                for task in (reader, publication):
                    task.cancel()
                await asyncio.gather(reader, publication, return_exceptions=True)
        profile = json.loads(operation_result.body)["provider_profile"]
        assert profile["cms"] == profile["directory"] == "old"
        assert len(set(loader_tasks)) == 1
        next_result = await npi.get_provider_profile(SimpleNamespace(args={}), str(NPI))
        next_profile = json.loads(next_result.body)["provider_profile"]
        assert next_profile["cms"] == next_profile["directory"] == "new"


async def test_snapshot_pins_rows_when_independent_updates_commit(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True) as session:
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
            authority = session.info["provider_profile_native_authorities"]
            assert set(authority) == {"cms-doctors", "entity-address"}
            first = await database.scalar(f'SELECT marker FROM "{schema}".cms_doctor_education')
            async with database.session_factory() as writer, writer.begin():
                await writer.execute(text(f"UPDATE \"{schema}\".provider_directory_profile SET marker='changed'"))
            second = await database.scalar(f'SELECT marker FROM "{schema}".provider_directory_profile')
            assert first == second == "old"
        assert snapshot._SNAPSHOT.get() is None and not session.in_transaction()


async def test_cancelled_read_releases_all_relation_locks(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        reached_read = asyncio.Event()
        sessions = []

        async def read():
            async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True) as session:
                sessions.append(session)
                reached_read.set()
                await asyncio.Future()

        reader = asyncio.create_task(read())
        await asyncio.wait_for(reached_read.wait(), 3)
        reader.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reader
        assert not sessions[0].in_transaction()
        async with database.session_factory() as writer:
            await asyncio.wait_for(publish_family(writer, schema), 3)


async def test_cutover_lock_timeout_releases_snapshot(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        async with database.session_factory() as writer, writer.begin():
            await writer.execute(text(f'LOCK TABLE "{schema}".cms_doctor_education IN ACCESS EXCLUSIVE MODE'))
            with pytest.raises(ServiceUnavailable) as caught:
                async with snapshot.provider_profile_read_snapshot(database, schema):
                    pytest.fail("A contended family cannot be read")
            assert "cms_doctor_education" not in str(caught.value)
            assert database._transaction_binding() is None and snapshot._SNAPSHOT.get() is None
        async with snapshot.provider_profile_read_snapshot(database, schema):
            assert await database.scalar("SELECT 1") == 1


async def test_drifted_native_family_cannot_enter_read_scope(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        await database.status(f'ALTER TABLE "{schema}".cms_doctor_education RENAME TO abandoned_education')
        await database.status(f'CREATE TABLE "{schema}".cms_doctor_education (marker text)')
        with pytest.raises(ServiceUnavailable):
            async with snapshot.provider_profile_read_snapshot(database, schema):
                pytest.fail("A drifted native receipt cannot be read")


async def test_rename_setup_retries_only_a_fresh_owned_snapshot(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        read_oids = snapshot._snapshot_relation_oids
        observed_snapshots = []
        sessions = []

        async def rotate_after_inventory(session, schema_name, table_names):
            relation_oids = await read_oids(session, schema_name, table_names)
            observed_snapshots.append(relation_oids)
            sessions.append(session)
            if len(observed_snapshots) == 1:
                async with database.session_factory() as writer:
                    await publish_family(writer, schema)
            return relation_oids

        monkeypatch.setattr(snapshot, "_snapshot_relation_oids", rotate_after_inventory)
        async with snapshot.provider_profile_read_snapshot(database, schema):
            assert await database.scalar(f'SELECT marker FROM "{schema}".cms_doctor_education') == "new"
        assert len(sessions) == 2 and sessions[0] is not sessions[1]
        assert observed_snapshots[0] != observed_snapshots[1]
        assert database._transaction_binding() is None and snapshot._SNAPSHOT.get() is None


async def test_borrowed_read_transaction_is_not_retried(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        setup = AsyncMock(wraps=snapshot._lock_serving_relations)
        monkeypatch.setattr(snapshot, "_lock_serving_relations", setup)
        async with database.transaction():
            with pytest.raises(ServiceUnavailable):
                async with snapshot.provider_profile_read_snapshot(database, schema):
                    pytest.fail("A borrowed transaction cannot establish a fresh repeatable-read snapshot")
        setup.assert_not_awaited()
        assert database._transaction_binding() is None


async def test_setup_retry_never_replays_a_yielded_loader(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        loader_calls = 0
        with pytest.raises(snapshot._SnapshotSetupChanged):
            async with snapshot.provider_profile_read_snapshot(database, schema):
                loader_calls += 1
                raise snapshot._SnapshotSetupChanged("synthetic loader failure")
        assert loader_calls == 1 and database._transaction_binding() is None


async def test_missing_legacy_profile_returns_existing_not_found(monkeypatch):
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        # A stale process cache must not invent an installed projection.
        monkeypatch.setattr(npi, "_PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN", {f"{schema}.provider_directory_profile"})
        result = await npi.get_provider_profile(SimpleNamespace(args={}), str(NPI))
        assert result.status == 404
        assert json.loads(result.body)["error"] == "provider_profile_not_found"
        assert database._transaction_binding() is None


async def test_legacy_education_without_native_ledger_remains_available(monkeypatch):
    from tests.test_provider_profile_cms import _education_row

    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        await database.status(f'''CREATE TABLE "{schema}".cms_doctor_education (
            npi bigint,education_key text,medical_school text,graduation_year integer,
            generation_id text,source_json jsonb)''')
        education_by_field = _education_row()
        await database.status(
            f'''INSERT INTO "{schema}".cms_doctor_education VALUES
                (:npi,:education_key,:medical_school,:graduation_year,:generation_id,CAST(:source_json AS jsonb))''',
            npi=NPI,
            **{**education_by_field, "source_json": json.dumps(education_by_field["source_json"])},
        )
        result = await npi.get_provider_profile(SimpleNamespace(args={"category": "education"}), str(NPI))
        assert result.status == 200
        profile = json.loads(result.body)["provider_profile"]
        assert profile["source_generations"] == {"cms_doctors": education_by_field["generation_id"]}
        assert profile["categories"]["education"]["items"]


async def test_optional_status_failure_rolls_back_only_its_savepoint(monkeypatch):
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True) as session:
            status = await npi._fetch_location_status_by_record_id(
                ["provider_directory_fhir:practitioner_role:example:role:location"],
                session=session,
                use_request_session=True,
            )
            assert status == {}
            assert await database.scalar("SELECT 1") == 1


def _detail_loaders(database, schema):
    """Read every response component through the handler's real bound snapshot."""
    marker_reads = []

    async def read_marker(table, session):
        assert database._transaction_binding().session is session
        value = await session.scalar(text(f'SELECT marker FROM "{schema}".{table}'))
        marker_reads.append(value)
        return value

    async def profile_loader(_npis, *, session, **_options):
        marker = await read_marker("provider_directory_profile", session)
        return {NPI: {"profile": {"generation_id": marker}}}

    async def detail_loader(_npi, *, session, **_options):
        await read_marker("doctor_clinician_address", session)
        return {"npi": NPI, "address_list": []}

    async def address_loader(_npi, *, session, **_options):
        marker = await read_marker("entity_address_unified", session)
        return [
            {
                "npi": NPI,
                "checksum": 1,
                "type": "practice",
                "first_line": marker + " Example Street",
                "city_name": "Example",
                "state_name": "IL",
                "postal_code": "60001",
                "lat": None,
                "long": None,
                "address_key": "00000000-0000-0000-0000-000000000001",
            }
        ]

    async def enrichment_loader(_npi, *, session, **_options):
        return {"summary": {"marker": await read_marker("cms_doctor_group_site", session)}}

    async def hydrate(_npi, addresses, **_options):
        return addresses

    return marker_reads, profile_loader, detail_loader, address_loader, enrichment_loader, hydrate


async def test_detail_reads_finish_before_geocoding(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        marker_reads, profile_loader, detail_loader, address_loader, enrichment_loader, hydrate = _detail_loaders(
            database, schema
        )

        async def geocode(*_args, **_options):
            assert database._transaction_binding() is None and snapshot._SNAPSHOT.get() is None
            async with database.session_factory() as writer:
                await asyncio.wait_for(publish_family(writer, schema), 3)
            return json.dumps({"features": [{"geometry": {"coordinates": [-87, 41]}}]})

        monkeypatch.setattr(npi, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0)
        monkeypatch.setattr(npi, "_fetch_provider_directory_profile_map", profile_loader)
        monkeypatch.setattr(npi, "_build_npi_details", detail_loader)
        monkeypatch.setattr(npi, "_fetch_npi_location_candidates", address_loader)
        monkeypatch.setattr(npi, "_fetch_provider_directory_address_overlay", AsyncMock(return_value=[]))
        monkeypatch.setattr(npi, "_hydrate_selected_provider_locations", hydrate)
        monkeypatch.setattr(npi, "_fetch_other_names", AsyncMock(return_value=[]))
        monkeypatch.setattr(npi, "_fetch_provider_enrichment_detail", enrichment_loader)
        monkeypatch.setattr(npi, "download_it", geocode)
        request = SimpleNamespace(
            args={"sync_geocode": "1"},
            app=SimpleNamespace(
                config={
                    "GEOCODE_MAPBOX_STYLE_KEY_PARAM": "token",
                    "GEOCODE_MAPBOX_STYLE_KEY": '["synthetic"]',
                    "GEOCODE_MAPBOX_STYLE_URL": "https://example.invalid/",
                }
            ),
        )
        operation_result = await npi.get_npi(request, str(NPI))
        response_by_field = json.loads(operation_result.body)
        assert marker_reads == ["old"] * 4
        assert response_by_field["provider_directory_profile"]["generation_id"] == "old"
        assert response_by_field["provider_enrichment"]["summary"]["marker"] == "old"
        assert response_by_field["address_list"][0]["lat"] == 41
        assert await database.scalar(f'SELECT marker FROM "{schema}".entity_address_unified') == "new"
