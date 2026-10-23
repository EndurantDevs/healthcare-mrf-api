# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native ordinary geo responses pin the serving heap until encoding finishes."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import MetaData, text
from sqlalchemy.sql.elements import TextClause

from api import provider_profile_snapshot as snapshot
from api.endpoint import npi
from db.connection import ConnectionProxy, current_session
from db.models import EntityAddressUnified, NPIData, NPIDataTaxonomy, NUCCTaxonomy
from tests.provider_profile_snapshot_postgres_support import grant_profile_reader, snapshot_database


@asynccontextmanager
async def near_database(monkeypatch):
    async with snapshot_database(monkeypatch, populated=False) as (database, schema):
        monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
        monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
        tables_by_name = {}
        async with database.engine.begin() as connection:
            await connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            for model in (NPIData, NPIDataTaxonomy, NUCCTaxonomy, EntityAddressUnified):
                table = model.__table__.to_metadata(MetaData(), schema=schema)
                await connection.run_sync(lambda sync, table=table: table.create(sync))
                tables_by_name[table.name] = table
            successor = EntityAddressUnified.__table__.to_metadata(
                MetaData(), schema=schema, name="entity_address_unified_next"
            )
            await connection.run_sync(successor.create)
            await connection.execute(tables_by_name["npi"].insert(), {"npi": 1000000004, "entity_type_code": 1})
            for table, marker, count in ((tables_by_name["entity_address_unified"], "old", 1), (successor, "new", 2)):
                await connection.execute(
                    table.insert(),
                    [
                        {
                            "entity_type": "provider",
                            "entity_id": "1000000004",
                            "npi": 1000000004,
                            "location_key": str(number).zfill(64),
                            "checksum": number,
                            "type": "primary",
                            "address_key": UUID(int=number),
                            "address_precision": "street",
                            "first_line": marker,
                            "lat": 41.0,
                            "long": -87.0,
                        }
                        for number in range(1, count + 1)
                    ],
                )
        await grant_profile_reader(database, schema, (*tables_by_name, successor.name))
        # The geo templates retain their documented canonical namespace; only
        # relocate that namespace into this fixture's UUID-owned schema.
        for name in ("_build_nearby_sql", "_build_nearby_count_sql"):
            build = getattr(npi, name)

            def in_schema(*args, build=build, **kwargs):
                return build(*args, **kwargs).replace("mrf.", f'"{schema}".')

            monkeypatch.setattr(npi, name, in_schema)
        execute = npi._execute_stmt

        async def execute_in_schema(statement, **kwargs):
            if isinstance(statement, TextClause):
                statement = text(str(statement).replace("'mrf'", f"'{schema}'"))
            return await execute(statement, **kwargs)

        monkeypatch.setattr(npi, "_execute_stmt", execute_in_schema)
        yield database, schema


def near_request():
    return SimpleNamespace(
        args={"lat": "41", "long": "-87", "include_total": "1", "limit": "1"},
        ctx=SimpleNamespace(sa_session=None),
        app=SimpleNamespace(),
    )


async def _publish_address(database, schema, started=None, identity=None):
    async with database.engine.begin() as connection:
        if identity is not None:
            identity["pid"] = await connection.scalar(text("SELECT pg_backend_pid()"))
        if started is not None:
            started.set()
        await connection.execute(text(f'ALTER TABLE "{schema}".entity_address_unified RENAME TO retired_address'))
        await connection.execute(
            text(f'ALTER TABLE "{schema}".entity_address_unified_next RENAME TO entity_address_unified')
        )


async def _wait_for_read(reader, reached):
    waiter = asyncio.create_task(reached.wait())
    try:
        done, _pending = await asyncio.wait((reader, waiter), timeout=3, return_when=asyncio.FIRST_COMPLETED)
        if waiter not in done:
            if reader in done:
                await reader
                raise AssertionError("Reader completed before the test barrier")
            raise AssertionError("Reader did not reach the test barrier")
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def _wait_for_writer_lock(database, schema, pid):
    async def wait():
        async with database.engine.connect() as observer:
            while not await observer.scalar(
                text(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid AND mode='AccessExclusiveLock' "
                    "AND NOT granted AND relation=to_regclass(:table))"
                ),
                {"pid": pid, "table": f'"{schema}".entity_address_unified'},
            ):
                await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), 3)


def _observe_near_reads(database, schema, monkeypatch):
    state = SimpleNamespace(
        page_read=asyncio.Event(),
        writer_started=asyncio.Event(),
        writer_identity={},
        read_stages=[],
        reader_sessions=[],
        is_pinned_phase=True,
        publication=None,
    )
    original_all, original_metadata, original_json = (
        ConnectionProxy.all,
        npi._project_provider_network_metadata,
        npi.response.json,
    )

    async def observed_all(connection, statement, **parameters):
        assert database.has_reader_session() and connection._connection is current_session()
        state.reader_sessions.append(current_session())
        rows = await original_all(connection, statement, **parameters)
        if "COUNT(DISTINCT" in str(statement):
            state.read_stages.append("count")
        else:
            state.read_stages.append("page")
            if state.is_pinned_phase and not state.page_read.is_set():
                state.page_read.set()
                await state.writer_started.wait()
                await _wait_for_writer_lock(database, schema, state.writer_identity["pid"])
        return rows

    async def observed_metadata(rows, *, session, **options):
        assert database.has_reader_session() and session is current_session()
        state.read_stages.append("metadata")
        if state.is_pinned_phase:
            assert await session.scalar(text(f'SELECT first_line FROM "{schema}".entity_address_unified')) == "old"
            assert not state.publication.done()
        return await original_metadata(rows, session=session, **options)

    def observed_json(*args, **kwargs):
        assert database.has_reader_session() and current_session().in_transaction()
        state.read_stages.append("encode")
        if state.is_pinned_phase:
            assert not state.publication.done()
        return original_json(*args, **kwargs)

    monkeypatch.setattr(ConnectionProxy, "all", observed_all)
    monkeypatch.setattr(npi, "_project_provider_network_metadata", observed_metadata)
    monkeypatch.setattr(npi.response, "json", observed_json)
    return state


async def check_near_cutover(monkeypatch):
    async with near_database(monkeypatch) as (database, schema):
        state = _observe_near_reads(database, schema, monkeypatch)
        reader = asyncio.create_task(npi.get_near_npi(near_request()))
        try:
            await _wait_for_read(reader, state.page_read)
            state.publication = asyncio.create_task(
                _publish_address(database, schema, state.writer_started, state.writer_identity)
            )
            http_response = await asyncio.wait_for(reader, 5)
            await asyncio.wait_for(state.publication, 3)
        finally:
            tasks = [task for task in (reader, state.publication) if task is not None]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        document = json.loads(http_response.body)
        assert document["total_count"] == 1 and [row["first_line"] for row in document["items"]] == ["old"]
        assert state.read_stages[-3:] == ["count", "metadata", "encode"]
        assert len({id(session) for session in state.reader_sessions}) == 1
        assert not state.reader_sessions[0].in_transaction() and snapshot._SNAPSHOT.get() is None
        state.is_pinned_phase = False
        next_document = json.loads((await npi.get_near_npi(near_request())).body)
        assert next_document["total_count"] == 2 and [row["first_line"] for row in next_document["items"]] == ["new"]
        assert not database.has_reader_session() and database._transaction_binding() is None


async def check_near_cleanup(monkeypatch, *, cancelled):
    async with near_database(monkeypatch) as (database, schema):
        reached, sessions = asyncio.Event(), []

        async def unavailable_metadata(_rows, *, session, **_options):
            assert database.has_reader_session() and session is current_session()
            sessions.append(session)
            reached.set()
            if cancelled:
                await asyncio.Future()
            await session.execute(text("SELECT 1/0"))
            raise AssertionError("The native read must fail")

        monkeypatch.setattr(npi, "_project_provider_network_metadata", unavailable_metadata)
        reader = asyncio.create_task(npi.get_near_npi(near_request()))
        try:
            await _wait_for_read(reader, reached)
            if cancelled:
                reader.cancel()
            with pytest.raises(asyncio.CancelledError if cancelled else ServiceUnavailable):
                await reader
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
        assert not sessions[0].in_transaction() and snapshot._SNAPSHOT.get() is None
        assert not database.has_reader_session() and database._transaction_binding() is None
        await asyncio.wait_for(_publish_address(database, schema), 3)
