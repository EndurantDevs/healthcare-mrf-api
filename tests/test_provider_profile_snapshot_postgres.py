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

from api import provider_profile_snapshot as snapshot
from api.endpoint import npi
from db.connection import current_session
from tests.npi_near_snapshot_postgres_support import (
    _middleware_near_reader,
    check_near_cleanup,
    check_near_cutover,
    near_request,
)
from tests.provider_profile_snapshot_postgres_support import (
    grant_profile_reader,
    held_family_publication,
    near_snapshot_database,
    publish_family,
    release_after_snapshot_wait,
    snapshot_database,
    wait_for_snapshot_lock,
)

NPI = 1000000004


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
        await asyncio.wait_for(publish_family(database, schema), 3)


async def test_cutover_lock_timeout_releases_snapshot(monkeypatch):
    """One native five-second refusal closes the owned Reader without replaying setup."""
    async with snapshot_database(monkeypatch) as (database, schema):
        setup = AsyncMock(wraps=snapshot._lock_serving_relations)
        monkeypatch.setattr(snapshot, "_lock_serving_relations", setup)
        async with held_family_publication(database, schema):
            started = asyncio.get_running_loop().time()
            with pytest.raises(ServiceUnavailable) as caught:
                async with snapshot.provider_profile_read_snapshot(database, schema):
                    pytest.fail("A contended family cannot be read")
            elapsed = asyncio.get_running_loop().time() - started
            assert 4.5 <= elapsed < 8
            assert isinstance(caught.value.__cause__, DBAPIError)
            assert snapshot.postgres_sqlstate(caught.value.__cause__) in {"55P03", "57014"}
            assert "cms_doctor_education" not in str(caught.value)
            setup.assert_awaited_once()
            assert not setup.await_args.args[0].in_transaction()
            assert database._transaction_binding() is None and snapshot._SNAPSHOT.get() is None
        async with snapshot.provider_profile_read_snapshot(database, schema):
            assert await database.scalar("SELECT 1") == 1


async def test_drifted_native_family_cannot_enter_read_scope(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        await database.status(f'ALTER TABLE "{schema}".cms_doctor_education RENAME TO abandoned_education')
        await database.status(f'CREATE TABLE "{schema}".cms_doctor_education (marker text)')
        await grant_profile_reader(database, schema, ("cms_doctor_education",))
        with pytest.raises(ServiceUnavailable):
            async with snapshot.provider_profile_read_snapshot(database, schema):
                pytest.fail("A drifted native receipt cannot be read")


async def test_owned_snapshot_waits_once_for_real_publication(monkeypatch):
    """A genuine restricted Publisher holds AX beyond the former total retry window."""
    async with snapshot_database(monkeypatch) as (database, schema):
        setup = AsyncMock(wraps=snapshot._lock_serving_relations)
        monkeypatch.setattr(snapshot, "_lock_serving_relations", setup)
        async with held_family_publication(database, schema) as release:
            releaser = asyncio.create_task(release_after_snapshot_wait(database, schema, release))
            try:
                async with snapshot.provider_profile_read_snapshot(database, schema, include_detail=True) as session:
                    assert await session.scalar(text("SELECT pg_backend_pid()")) == await releaser
                    await _assert_reader_snapshot(database, schema, session)
                    assert await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education') == "new"
                    assert await database.scalar(f'SELECT marker FROM "{schema}".provider_directory_profile') == "new"
                setup.assert_awaited_once()
                assert not session.in_transaction() and not database.has_reader_session()
            finally:
                releaser.cancel()
                await asyncio.gather(releaser, return_exceptions=True)
        assert database._transaction_binding() is None and snapshot._SNAPSHOT.get() is None


async def _assert_reader_snapshot(database, schema, session, *, include_address=True):
    reader = database._reader_database._reader_login[0]
    assert tuple((await session.execute(text("SELECT session_user,current_user"))).one()) == (reader, reader)
    assert await session.scalar(text("SHOW transaction_read_only")) == "on"
    assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
    assert await session.scalar(text("SHOW lock_timeout")) == "250ms"
    assert await session.scalar(text("SHOW statement_timeout")) == "5s"
    assert current_session() is session and session.info["api_reader_verified"]
    expected = {"cms-doctors", "entity-address"} if include_address else {"cms-doctors"}
    assert set(session.info["provider_profile_native_authorities"]) == expected
    pinned = snapshot._SNAPSHOT.get()
    names = tuple(reference.split(".", 1)[1] for reference in pinned)
    assert pinned == await snapshot._relation_oids(session, schema, names)


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
                await publish_family(database, schema)
            return relation_oids

        monkeypatch.setattr(snapshot, "_snapshot_relation_oids", rotate_after_inventory)
        async with snapshot.provider_profile_read_snapshot(database, schema):
            assert await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education') == "new"
        assert len(sessions) == 2 and sessions[0] is sessions[1]
        assert observed_snapshots[0] != observed_snapshots[1]
        assert database._transaction_binding() is None and snapshot._SNAPSHOT.get() is None


async def test_writer_transaction_cannot_enter_reader_snapshot(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        setup = AsyncMock(wraps=snapshot._lock_serving_relations)
        monkeypatch.setattr(snapshot, "_lock_serving_relations", setup)
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
    async with snapshot_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "true")
        pin = AsyncMock()
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


@pytest.mark.parametrize("middleware", (False, True))
async def test_near_read_snapshot_blocks_cutover(monkeypatch, middleware):
    """Keep page, count, metadata and encoding on the original serving heap."""
    await check_near_cutover(monkeypatch, middleware=middleware)


@pytest.mark.parametrize("middleware", (False, True))
async def test_near_read_failure_releases_snapshot(monkeypatch, middleware):
    """A native query error must release the Reader before publication."""
    await check_near_cleanup(monkeypatch, cancelled=False, middleware=middleware)


@pytest.mark.parametrize("middleware", (False, True))
async def test_near_cancellation_releases_snapshot(monkeypatch, middleware):
    """Cancellation must release the Reader before publication."""
    await check_near_cleanup(monkeypatch, cancelled=True, middleware=middleware)


async def test_cancellation_while_waiting_drains_the_owned_reader(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        setup = AsyncMock(wraps=snapshot._lock_serving_relations)
        monkeypatch.setattr(snapshot, "_lock_serving_relations", setup)

        async def read():
            async with snapshot.provider_profile_read_snapshot(database, schema):
                pytest.fail("The writer still holds the family locks")

        async with held_family_publication(database, schema):
            reader = asyncio.create_task(read())
            try:
                pid = await wait_for_snapshot_lock(database, schema)
                reader.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(reader, 3)
                setup.assert_awaited_once()
                session = setup.await_args.args[0]
                assert not session.in_transaction() and not session.info.get("api_reader_verified")
                assert not await database.scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid AND mode='AccessShareLock')", pid=pid
                )
                assert not database.has_reader_session() and snapshot._SNAPSHOT.get() is None
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)


async def _refuse_borrowed_snapshot(session, database, schema):
    with pytest.raises(ServiceUnavailable):
        async with session.begin_nested(), snapshot.provider_profile_read_snapshot(database, schema):
            pytest.fail("A borrowed Reader cannot wait beyond its original lock limit")


async def test_borrowed_snapshot_retains_250ms_without_rolling_back_its_owner(monkeypatch):
    async with snapshot_database(monkeypatch) as (database, schema):
        setup = AsyncMock(wraps=snapshot._lock_serving_relations)
        monkeypatch.setattr(snapshot, "_lock_serving_relations", setup)
        async with database.reader_session() as session:
            rollback = AsyncMock(wraps=session.rollback)
            monkeypatch.setattr(session, "rollback", rollback)
            async with held_family_publication(database, schema):
                started = asyncio.get_running_loop().time()
                await _refuse_borrowed_snapshot(session, database, schema)
                assert 0.2 <= asyncio.get_running_loop().time() - started < 2
                setup.assert_awaited_once()
                assert setup.await_args.args[0] is session and not setup.await_args.kwargs.get("fresh_snapshot")
                rollback.assert_not_awaited()
                assert current_session() is session and session.in_transaction()
                assert await session.scalar(text("SELECT 1")) == 1
                assert snapshot._SNAPSHOT.get() is None


async def test_profile_middleware_leaves_snapshot_setup_with_endpoint(monkeypatch):
    """The real Profile route authenticates its own Reader before either native loader."""
    async with snapshot_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "true")
        claims = AsyncMock()
        monkeypatch.setattr("api.reference_family_reads.pin_claims_reader", claims)
        callbacks_by_kind = {}
        app = SimpleNamespace(
            listener=lambda _name: lambda callback: callback,
            middleware=lambda name: lambda callback: callbacks_by_kind.setdefault(name, callback),
        )
        database.init_app(app)
        request = SimpleNamespace(path=f"/api/v1/npi/id/{NPI}/profile", args={}, ctx=SimpleNamespace())
        markers = []

        async def projection(_npi):
            assert database.has_reader_session() and current_session().info["api_reader_verified"]
            marker = await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education')
            markers.append(marker)
            return {"marker": marker}

        async def directory(_npis, **_options):
            assert database.has_reader_session()
            marker = await database.scalar(f'SELECT marker FROM "{schema}".provider_directory_profile')
            markers.append(marker)
            return {NPI: {"profile": {"marker": marker}}}

        monkeypatch.setattr(npi, "fetch_provider_profile_projection", projection)
        monkeypatch.setattr(npi, "_fetch_provider_directory_profile_map", directory)
        monkeypatch.setattr(npi, "_compose_requested_provider_profile", lambda *_: {"generation_id": "a" * 64})
        await callbacks_by_kind["request"](request)
        try:
            assert not database.has_reader_session() and not request.ctx.sa_session.in_transaction()
            claims.assert_not_awaited()
            http_response = await npi.get_provider_profile(request, str(NPI))
            assert http_response.status == 200 and markers == ["old", "old"]
            assert not database.has_reader_session() and snapshot._SNAPSHOT.get() is None
        finally:
            await callbacks_by_kind["response"](request, SimpleNamespace(status=200))
        assert request.ctx.sa_session is None


def _waited_profile_reader(database, schema, monkeypatch):
    async def projection(_npi):
        await _assert_reader_snapshot(database, schema, current_session(), include_address=False)
        return {"marker": await database.scalar(f'SELECT medical_school FROM "{schema}".cms_doctor_education')}

    async def directory(_npis, **_options):
        return {
            NPI: {
                "profile": {
                    "marker": await database.scalar(f'SELECT marker FROM "{schema}".provider_directory_profile')
                }
            }
        }

    monkeypatch.setattr(npi, "fetch_provider_profile_projection", projection)
    monkeypatch.setattr(npi, "_fetch_provider_directory_profile_map", directory)
    monkeypatch.setattr(
        npi,
        "_compose_requested_provider_profile",
        lambda _npi, state, profile, _query: {
            "generation_id": "a" * 64,
            "cms": state["marker"],
            "directory": profile["profile"]["marker"],
        },
    )
    return lambda: npi.get_provider_profile(SimpleNamespace(args={}), str(NPI))


def _waited_detail_reader(database, schema, monkeypatch):
    loaders = _detail_loaders(database, schema)
    _, profile, detail, addresses, enrichment, hydrate = loaders

    async def located_addresses(*args, **kwargs):
        await _assert_reader_snapshot(database, schema, current_session())
        return [dict(row, lat=41, long=-87) for row in await addresses(*args, **kwargs)]

    monkeypatch.setattr(npi, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0)
    monkeypatch.setattr(npi, "_fetch_provider_directory_profile_map", profile)
    monkeypatch.setattr(npi, "_build_npi_details", detail)
    monkeypatch.setattr(npi, "_fetch_npi_location_candidates", located_addresses)
    monkeypatch.setattr(npi, "_fetch_provider_directory_address_overlay", AsyncMock(return_value=[]))
    monkeypatch.setattr(npi, "_hydrate_selected_provider_locations", hydrate)
    monkeypatch.setattr(npi, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(npi, "_fetch_provider_enrichment_detail", enrichment)
    request = SimpleNamespace(path=f"/api/v1/npi/id/{NPI}", ctx=SimpleNamespace(), args={}, app=SimpleNamespace())
    return lambda: npi.get_npi(request, str(NPI))


def _waited_route_reader(kind, database, schema, monkeypatch):
    if kind == "profile":
        return _waited_profile_reader(database, schema, monkeypatch)
    if kind == "detail":
        return _waited_detail_reader(database, schema, monkeypatch)
    read = _middleware_near_reader(database, schema, monkeypatch)
    return lambda: read(near_request())


def _assert_successor_response(kind, response):
    document = json.loads(response.body)
    assert response.status == 200
    if kind == "profile":
        profile = document["provider_profile"]
        assert profile["cms"] == profile["directory"] == "new"
    elif kind == "detail":
        assert document["provider_directory_profile"]["generation_id"] == "new"
        assert document["provider_enrichment"]["summary"]["marker"] == "new"
        assert document["address_list"][0]["first_line"] == "new Example Street"
    else:
        assert document["total_count"] == 2
        assert [row["first_line"] for row in document["items"]] == ["new"]


@pytest.mark.parametrize("kind", ("profile", "detail", "near"))
async def test_owned_routes_wait_for_native_publication(monkeypatch, kind):
    fixture = near_snapshot_database if kind == "near" else snapshot_database
    async with fixture(monkeypatch) as (database, schema):
        read = _waited_route_reader(kind, database, schema, monkeypatch)
        setup = AsyncMock(wraps=snapshot._lock_serving_relations)
        monkeypatch.setattr(snapshot, "_lock_serving_relations", setup)
        async with held_family_publication(database, schema) as release:
            releaser = asyncio.create_task(release_after_snapshot_wait(database, schema, release))
            try:
                response = await asyncio.wait_for(read(), 8)
                assert await releaser > 0
                _assert_successor_response(kind, response)
                fresh_calls = [call for call in setup.await_args_list if call.kwargs.get("fresh_snapshot")]
                assert len(fresh_calls) == 1
                assert not fresh_calls[0].args[0].in_transaction()
                assert not database.has_reader_session() and snapshot._SNAPSHOT.get() is None
            finally:
                releaser.cancel()
                await asyncio.gather(releaser, return_exceptions=True)
