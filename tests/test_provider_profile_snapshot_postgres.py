# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native cross-query publication, cleanup, and optional legacy-read regressions."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api import network_read_execution
from api import provider_profile_snapshot as snapshot
from api.endpoint import npi
from api.reference_family_reads import pin_claims_reader
from db.connection import current_session
from tests.npi_near_snapshot_postgres_support import check_near_cleanup, check_near_cutover
from tests.provider_profile_snapshot_postgres_support import (
    create_directory_partitions,
    grant_profile_reader,
    publish_family,
    snapshot_database,
)

NPI = 1000000004


async def test_reader_starts_native_transaction_before_first_raw_query(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, _schema):
        async with database.reader_session() as session:
            connection = await session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            assert driver.is_in_transaction()
            modes = await driver.fetchrow(
                "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"
            )
            assert tuple(modes) == ("repeatable read", "on")
            assert "api_reader_identity" not in session.info


async def test_profile_route_keeps_both_loaders_before_family_rename(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        reached_first_read = asyncio.Event()
        writer_started = asyncio.Event()
        loader_tasks = []

        async def state_loader(_npi):
            loader_tasks.append(asyncio.current_task())
            marker = await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education')
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
        reader = asyncio.create_task(npi.get_provider_profile(SimpleNamespace(args={}), str(NPI)))
        await asyncio.wait_for(reached_first_read.wait(), 3)
        publication = asyncio.create_task(publish_family(database, schema, writer_started))
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
            assert tuple((await session.execute(text("SELECT session_user,current_user"))).one()) == (
                database._reader_database._reader_login[0],
                database._reader_database._reader_login[0],
            )
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
            authority = session.info["provider_profile_native_authorities"]
            assert set(authority) == {"cms-doctors", "entity-address"}
            first = await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education')
            async with database.session_factory() as writer, writer.begin():
                await writer.execute(text(f"UPDATE \"{schema}\".provider_directory_profile SET marker='changed'"))
            second = await database.scalar(f'SELECT marker FROM "{schema}".provider_directory_profile')
            assert first == second == "old"
        assert snapshot._SNAPSHOT.get() is None and not session.in_transaction()


async def test_cancelled_read_releases_all_relation_locks(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        await create_directory_partitions(database, schema)
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
        async with database.engine.begin() as connection:
            roots = ", ".join(f'"{schema}"."{name}"' for name in sorted(snapshot._DIRECTORY_PARTITION_ROOTS))
            await connection.execute(text(f"LOCK TABLE {roots} IN ACCESS EXCLUSIVE MODE NOWAIT"))
        await asyncio.wait_for(publish_family(database, schema), 3)


async def test_cutover_lock_timeout_releases_snapshot(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        async with database.session_factory() as writer, writer.begin():
            await writer.execute(text(f'LOCK TABLE "{schema}".cms_doctor_education IN ACCESS EXCLUSIVE MODE'))
            with pytest.raises(ServiceUnavailable) as caught:
                async with snapshot.provider_profile_read_snapshot(database, schema) as session:
                    await snapshot.execute_network_read(
                        session, text(f'SELECT medical_school FROM "{schema}".cms_doctor_education')
                    )
                    pytest.fail("A contended family cannot be read")
            assert "cms_doctor_education" not in str(caught.value)
            assert database._transaction_binding() is None and snapshot._SNAPSHOT.get() is None
        async with snapshot.provider_profile_read_snapshot(database, schema):
            assert await database.scalar("SELECT 1") == 1


async def test_copied_named_family_serves_without_rewriting_legacy_metadata(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        before = await database.scalar(f'SELECT relation_oids::text FROM "{schema}".reference_family_result_generation')
        async with database.engine.begin() as connection:
            await connection.execute(
                text(f'CREATE TABLE "{schema}".copied_education (LIKE "{schema}".cms_doctor_education INCLUDING ALL)')
            )
            await connection.execute(
                text(f'INSERT INTO "{schema}".copied_education SELECT * FROM "{schema}".cms_doctor_education')
            )
            await connection.execute(text(f'ALTER TABLE "{schema}".cms_doctor_education RENAME TO previous_education'))
            await connection.execute(text(f'ALTER TABLE "{schema}".copied_education RENAME TO cms_doctor_education'))
        await grant_profile_reader(database, schema, ("cms_doctor_education",))
        async with snapshot.provider_profile_read_snapshot(database, schema):
            assert await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education') == "old"
            assert (
                await database.scalar(f'SELECT relation_oids::text FROM "{schema}".reference_family_result_generation')
                == before
            )


async def test_lock_conflict_retries_a_fresh_snapshot_after_real_publication(monkeypatch):
    """Close the failed Reader before a retry observes the atomically renamed family."""
    async with snapshot_database(monkeypatch) as (database, schema):
        entered, release = asyncio.Event(), asyncio.Event()
        sessions = []
        lock_names = snapshot.lock_reader_relations

        async def publish():
            async with database.session_factory() as writer:
                async with writer.begin():
                    await writer.execute(
                        text(f'LOCK TABLE "{schema}".reference_family_result_generation IN ACCESS EXCLUSIVE MODE')
                    )
                    entered.set()
                    await release.wait()
                await publish_family(database, schema)

        publication = asyncio.create_task(publish())

        async def lock(session, names):
            sessions.append(session)
            if len(sessions) > 1:
                await asyncio.wait_for(publication, 3)
            try:
                return await lock_names(session, names)
            except DBAPIError as error:
                assert snapshot.postgres_sqlstate(error) == "55P03" and len(sessions) == 1
                release.set()
                raise

        monkeypatch.setattr(snapshot, "lock_reader_relations", lock)
        try:
            await asyncio.wait_for(entered.wait(), 3)
            async with snapshot.provider_profile_read_snapshot(database, schema):
                assert await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education') == "new"
                assert await database.scalar(f'SELECT marker FROM "{schema}".provider_directory_profile') == "new"
            assert len(sessions) == 2 and sessions[0] is not sessions[1]
            assert all(not session.in_transaction() for session in sessions)
        finally:
            release.set()
            publication.cancel()
            await asyncio.gather(publication, return_exceptions=True)


async def test_new_optional_name_retries_only_a_fresh_owned_snapshot(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        lock_names = snapshot.lock_reader_relations
        sessions = []

        async def install_after_locks(session, names):
            locked = await lock_names(session, names)
            sessions.append(session)
            if len(sessions) == 1:
                async with database.engine.begin() as connection:
                    await connection.execute(text(f'CREATE TABLE "{schema}".provider_profile_fact (marker text)'))
                await grant_profile_reader(database, schema, ("provider_profile_fact",))
            return locked

        monkeypatch.setattr(snapshot, "lock_reader_relations", install_after_locks)
        async with snapshot.provider_profile_read_snapshot(database, schema):
            assert snapshot.snapshot_relation_available(f"{schema}.provider_profile_fact") is True
            assert await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education') == "old"
        assert len(sessions) == 2 and sessions[0] is not sessions[1]
        assert all(not session.in_transaction() for session in sessions)


async def test_frozen_unpinned_reader_refuses_mutable_aliases(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        async with database.reader_session() as session:
            await session.execute(text("SELECT 1"))
            with pytest.raises(ServiceUnavailable, match="fresh or already pinned"):
                async with snapshot.provider_profile_read_snapshot(database, schema):
                    pytest.fail("a frozen unpinned Reader cannot adopt mutable names")


async def test_prebound_http_reader_pins_profiles_before_first_data_snapshot(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        request = SimpleNamespace(path="/api/v1/npi/geo", ctx=SimpleNamespace())
        async with database.reader_session() as session:
            assert session.in_transaction()
            await database._pin_request_reader(request, session)
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            async with snapshot.provider_geo_read_snapshot(database, schema):
                assert await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education') == "old"
            async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True):
                assert await database.scalar(f'SELECT marker FROM "{schema}".provider_directory_profile') == "old"


async def test_writer_transaction_cannot_enter_reader_snapshot(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        setup = AsyncMock(wraps=snapshot._inventory_serving_relations)
        monkeypatch.setattr(snapshot, "_inventory_serving_relations", setup)
        async with database.transaction():
            with pytest.raises(RuntimeError, match="Cannot open a Reader inside a Writer transaction"):
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
        await grant_profile_reader(database, schema, ("cms_doctor_education",))
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


@pytest.mark.parametrize("use_request_session", [False, True])
@pytest.mark.parametrize("fail_closed", [False, True])
async def test_optional_status_failure_rolls_back_only_its_savepoint(monkeypatch, use_request_session, fail_closed):
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True) as session:
            transaction = session.get_transaction()
            pinned = snapshot._SNAPSHOT.get()
            assert pinned and database.has_reader_session() and current_session() is session
            assert network_read_execution._installed_read_inventory(session) is None
            assert network_read_execution.has_network_read_inventory(session)

            lookup = npi._fetch_location_status_by_record_id(
                ["provider_directory_fhir:practitioner_role:example:role:location"],
                session=session,
                use_request_session=use_request_session,
                fail_closed=fail_closed,
            )
            if fail_closed:
                with pytest.raises(DBAPIError) as caught:
                    await lookup
                assert snapshot.postgres_sqlstate(caught.value) == "42P01"
            else:
                assert await lookup == {}

            assert database.has_reader_session() and current_session() is session
            assert session.get_transaction() is transaction and transaction.is_active
            assert session.get_nested_transaction() is None
            assert snapshot._SNAPSHOT.get() is pinned
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
            assert (await snapshot.execute_network_read(session, "SELECT 1")).scalar_one() == 1


def _detail_loaders(database, schema):
    """Read every response component through the handler's real bound snapshot."""
    marker_reads = []

    async def read_marker(table, session):
        assert database.has_reader_session() and current_session() is session
        column = {"doctor_clinician_address": "city", "cms_doctor_group_site": "org_pac_id"}.get(table, "marker")
        value = await session.scalar(text(f'SELECT {column} FROM "{schema}".{table}'))
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
    """Keep external geocoding outside the Reader transaction and all named locks."""
    async with snapshot_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "true")
        pin = AsyncMock(wraps=pin_claims_reader)
        monkeypatch.setattr("api.reference_family_reads.pin_claims_reader", pin)
        callbacks_by_kind = {}
        app = SimpleNamespace(
            listener=lambda _name: lambda callback: callback,
            middleware=lambda name: lambda callback: callbacks_by_kind.setdefault(name, callback),
        )
        database.init_app(app)
        geocode_reader_scopes = []
        marker_reads, profile_loader, detail_loader, address_loader, enrichment_loader, hydrate = _detail_loaders(
            database, schema
        )

        async def geocode(*_args, **_options):
            geocode_reader_scopes.append(database.has_reader_session())
            assert not database.has_reader_session() and database._transaction_binding() is None
            assert snapshot._SNAPSHOT.get() is None
            await asyncio.wait_for(publish_family(database, schema), 3)
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
            path=f"/api/v1/npi/id/{NPI}",
            ctx=SimpleNamespace(),
            args={"sync_geocode": "1"},
            app=SimpleNamespace(
                config={
                    "GEOCODE_MAPBOX_STYLE_KEY_PARAM": "token",
                    "GEOCODE_MAPBOX_STYLE_KEY": '["synthetic"]',
                    "GEOCODE_MAPBOX_STYLE_URL": "https://example.invalid/",
                }
            ),
        )
        await callbacks_by_kind["request"](request)
        pin.assert_not_awaited()
        try:
            operation_result = await npi.get_npi(request, str(NPI))
        finally:
            await callbacks_by_kind["response"](request, SimpleNamespace(status=200))
        pin.assert_not_awaited()
        assert geocode_reader_scopes == [False]
        response_by_field = json.loads(operation_result.body)
        assert marker_reads == ["old"] * 4
        assert response_by_field["provider_directory_profile"]["generation_id"] == "old"
        assert response_by_field["provider_enrichment"]["summary"]["marker"] == "old"
        assert response_by_field["address_list"][0]["lat"] == 41
        assert await database.scalar(f'SELECT marker FROM "{schema}".entity_address_unified') == "new"


async def test_near_read_snapshot_blocks_cutover(monkeypatch):
    """Keep page, count, metadata and encoding on the original serving heap."""
    await check_near_cutover(monkeypatch)


async def test_near_read_failure_releases_snapshot(monkeypatch):
    """A native query error must release the Reader before publication."""
    await check_near_cleanup(monkeypatch, cancelled=False)


async def test_near_cancellation_releases_snapshot(monkeypatch):
    """Cancellation must release the Reader before publication."""
    await check_near_cleanup(monkeypatch, cancelled=True)


@pytest.mark.parametrize(
    "populated,partition_kind", [(False, "single"), (True, "single"), (True, "history"), (True, "default")]
)
async def test_directory_partition_snapshot_reads_native_models(monkeypatch, populated, partition_kind):
    """The actual catalog admits empty roots and ordinary leaves under the same Reader."""
    async with snapshot_database(monkeypatch) as (database, schema):
        await create_directory_partitions(database, schema, populated=populated, partition_kind=partition_kind)
        async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True) as session:
            for name in sorted(snapshot._DIRECTORY_PARTITION_ROOTS):
                result = await snapshot.execute_network_read(session, text(f'SELECT count(*) FROM "{schema}"."{name}"'))
                assert result.scalar() == int(populated)
                assert snapshot.snapshot_relation_available(f"{schema}.{name}") is True
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
        assert not session.in_transaction() and snapshot._SNAPSHOT.get() is None


@pytest.mark.parametrize("topology", ["wrong-key", "nested", "inheritance"])
async def test_directory_partition_snapshot_refuses_unsupported_shapes(monkeypatch, topology):
    async with snapshot_database(monkeypatch) as (database, schema):
        name = "provider_directory_dataset_resource"
        root = f'"{schema}"."{name}"'
        partitioning = (
            ""
            if topology == "inheritance"
            else f" PARTITION BY LIST ({'marker' if topology == 'wrong-key' else 'dataset_id'})"
        )
        await database.status(f"CREATE TABLE {root} (dataset_id text, marker text){partitioning}")
        if topology == "nested":
            await database.status(
                f"CREATE TABLE \"{schema}\".leaf PARTITION OF {root} FOR VALUES IN ('fixture') PARTITION BY LIST(marker)"
            )
        elif topology == "inheritance":
            await database.status(f'CREATE TABLE "{schema}".leaf () INHERITS ({root})')
        await grant_profile_reader(database, schema, (name,))
        with pytest.raises(ServiceUnavailable):
            async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True):
                pytest.fail("unsupported partition shape reached a loader")
        assert database._transaction_binding() is None and snapshot._SNAPSHOT.get() is None


async def test_directory_partition_snapshot_blocks_leaf_detach(monkeypatch):
    """Legacy recursive locks preserve existing leaf semantics until the Reader exits."""
    async with snapshot_database(monkeypatch) as (database, schema):
        await create_directory_partitions(database, schema)
        root = f'"{schema}".provider_directory_dataset_resource'
        detach = text(f'ALTER TABLE {root} DETACH PARTITION "{schema}".provider_directory_dataset_resource_leaf')
        async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True) as session:
            assert (await snapshot.execute_network_read(session, text(f"SELECT count(*) FROM {root}"))).scalar() == 1
            with pytest.raises(DBAPIError) as caught:
                async with database.engine.begin() as writer:
                    await writer.execute(text("SET LOCAL lock_timeout='50ms'"))
                    await writer.execute(detach)
            assert snapshot.postgres_sqlstate(caught.value) == "55P03"
            assert (await snapshot.execute_network_read(session, text(f"SELECT count(*) FROM {root}"))).scalar() == 1
        async with database.engine.begin() as writer:
            await writer.execute(text("SET LOCAL lock_timeout='250ms'"))
            await writer.execute(detach)
        async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True) as session:
            assert (await snapshot.execute_network_read(session, text(f"SELECT count(*) FROM {root}"))).scalar() == 0


async def test_unpublished_logical_generations_allow_an_empty_reader_cohort(monkeypatch):
    """Seeded unpublished counters do not require a family that has never served."""
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        columns = (
            "local_lineage_id uuid NOT NULL, local_generation bigint NOT NULL, "
            "origin_lineage_id uuid, origin_generation bigint, published_at timestamptz"
        )
        async with database.engine.begin() as connection:
            for table, selector, value in (
                (snapshot.reference_generation.TABLE_NAME, "importer_id text", "'cms-doctors'"),
                (snapshot.address_generation.TABLE_NAME, "singleton boolean", "true"),
            ):
                await connection.execute(text(f'CREATE TABLE "{schema}".{table} ({selector},{columns})'))
                await connection.execute(
                    text(
                        f'INSERT INTO "{schema}".{table} VALUES ({value},'
                        "'10000000-0000-4000-8000-000000000001',0,NULL,NULL,NULL)"
                    )
                )
        await grant_profile_reader(
            database, schema, (snapshot.reference_generation.TABLE_NAME, snapshot.address_generation.TABLE_NAME)
        )
        async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True) as session:
            assert all(
                authority.serving_generation is None
                for authority in session.info["provider_profile_native_authorities"].values()
            )
            assert snapshot.snapshot_relation_available(f"{schema}.entity_address_unified") is False
