# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native signed HTTP ownership, fixed workspace and cancellation cleanup."""

import asyncio
import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api import custom_import_provider_http as providers
from api import custom_import_provider_service_http as services
from api import custom_import_read_http as transport
from api import signed_request_reader as boundary
from api.custom_import_plan_pricing import search_imported_plan_providers
from api.endpoint import extension_reads
from db.connection import current_session
from tests import test_custom_import_plan_pricing as plan_fixture
from tests import test_custom_import_provider_http as provider_fixture
from tests.signed_request_reader_postgres_support import (
    assert_fixed_preparation,
    assert_reader_write_denials,
    legacy_plan_result,
    native_reader_witness,
    observe_reader_phases,
    signed_reader_case,
)


async def test_signed_reader_failure_records_same_native_principal_and_target(signed_reader_case, caplog):
    prepared = signed_reader_case
    case, database = prepared.case, prepared.database
    await prepared.admin.execute(f"CREATE TABLE {case.relation('unread_payload')}(value text)")
    expected = await prepared.admin.fetchrow(
        "SELECT (SELECT oid FROM pg_roles WHERE rolname=$1), "
        "(SELECT oid FROM pg_database WHERE datname=current_database()), "
        "host(inet_server_addr()),inet_server_port()",
        case.reader,
    )
    phases = await observe_reader_phases(database)
    ordinary = SimpleNamespace()
    request_id = "00000000-0000-4000-8000-000000000003"
    request = SimpleNamespace(
        headers={"X-Request-ID": request_id},
        id=UUID(request_id),
        ctx=SimpleNamespace(sa_session=ordinary, session=ordinary),
    )
    with pytest.raises(DBAPIError) as caught:
        async with database.reader_session() as reader:
            backend = await native_reader_witness(reader, case.reader)
            with boundary._bind_request_reader(request, reader):
                await reader.execute(text(f"SELECT * FROM {case.relation('unread_payload')}"))
    assert reader.info == {} and not database.has_reader_session()
    assert request.ctx.sa_session is ordinary and request.ctx.session is ordinary
    transport._log_failure(caught.value)
    fields = json.loads(caplog.records[-1].getMessage().partition(" failed ")[2])
    identity = fields["reader"]
    assert tuple(identity[key] for key in ("role_oid", "database_oid", "server_address", "server_port")) == tuple(
        expected
    )
    assert identity["backend_pid"] == backend
    assert fields["sqlstate"] == "42501" and fields["request_id"] == request_id
    assert sum(statement.startswith("SELECT session_user, current_user,") for statement, *_ in phases) == 1


@pytest.mark.asyncio
async def test_signed_provider_route_uses_genuine_reader_and_restores_ordinary_context(signed_reader_case, monkeypatch):
    prepared = signed_reader_case
    case, database = prepared.case, prepared.database
    app = Sanic("signed-reader-" + uuid4().hex)
    database.init_app(app)
    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    witnesses = []
    failures = []
    monkeypatch.setattr(transport, "_log_failure", lambda failure: failures.append(repr(failure)))

    async def payload(request, session, parsed, verified):
        assert verified.credential and request.ctx.sa_session is session and request.ctx.session is session
        witnesses.append(await native_reader_witness(session, case.reader))
        assert await session.scalar(text(f"SELECT value FROM {case.relation('reader_payload')}")) == "old"
        await prepared.admin.execute(f"UPDATE {case.relation('reader_payload')} SET value='new'")
        assert await session.scalar(text(f"SELECT value FROM {case.relation('reader_payload')}")) == "old"
        return {"rows": []}

    monkeypatch.setattr(providers, "_read_provider_payload", payload)
    original_encode = transport._canonical_json_bytes

    def encode(document):
        if document == {"rows": []}:
            assert current_session().info.get("api_reader_verified") is True
        return original_encode(document)

    monkeypatch.setattr(transport, "_canonical_json_bytes", encode)
    request = provider_fixture._request()
    _, reply = await app.asgi_client.post(request.path, data=request.body, headers=request.headers)
    assert reply.status == 200, (reply.text, failures)
    assert reply.json == {"rows": []} and len(witnesses) == 1
    assert not database.has_reader_session()
    assert (
        await prepared.admin.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE usename=$1 AND xact_start IS NOT NULL", case.reader
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_signed_reader_restores_context_and_keeps_active_writer_untouched(signed_reader_case, failure):
    prepared = signed_reader_case
    async with prepared.database.session_factory() as ordinary:
        request = SimpleNamespace(
            path=providers.CUSTOM_IMPORT_PROVIDERS_PATH, ctx=SimpleNamespace(sa_session=ordinary, session=ordinary)
        )
        with pytest.raises(failure):
            async with boundary.signed_request_reader(request, ordinary) as reader:
                assert reader is not ordinary and request.ctx.sa_session is reader
                await native_reader_witness(reader, prepared.case.reader)
                raise failure("synthetic reader interruption")
        assert request.ctx.sa_session is ordinary and request.ctx.session is ordinary
        assert not ordinary.in_transaction() and not prepared.database.has_reader_session()
        await ordinary.execute(text("SELECT 1"))
        with pytest.raises(transport.CustomImportReadUnavailableError, match="unused caller"):
            async with boundary.signed_request_reader(request, ordinary):
                pytest.fail("an active ordinary writer was adopted")
        assert ordinary.in_transaction()
        await ordinary.rollback()


@pytest.mark.asyncio
async def test_fixed_plan_workspace_keeps_complete_computation_on_same_read_only_connection(
    signed_reader_case, monkeypatch
):
    prepared = signed_reader_case
    case = prepared.case
    plan_fixture._native_fixture(monkeypatch)
    monkeypatch.setattr(
        plan_fixture.serving, "_ptg2_npi_scope_table", lambda _tables: case.relation("native_npi_fixture")
    )
    phases = await observe_reader_phases(prepared.database)
    async with prepared.database.session_factory() as ordinary:
        request = SimpleNamespace(
            path=providers.CUSTOM_IMPORT_PROVIDERS_PATH, ctx=SimpleNamespace(sa_session=ordinary, session=ordinary)
        )
        async with boundary.signed_request_reader(request, ordinary, plan_workspace=True) as reader:
            backend = await native_reader_witness(reader, case.reader)
            assert reader.info["native_plan_workspace"] is True
            temp_namespace_oid = await reader.scalar(text("SELECT pg_my_temp_schema()"))
            assert await reader.scalar(text("SELECT count(*) FROM pg_temp.custom_import_plan_imported")) == 0
            pricing_response = await search_imported_plan_providers(
                reader,
                {},
                SimpleNamespace(limit=2, offset=0, page=1),
                plan_fixture._selection(),
                plan_fixture._import_context(),
            )
            assert pricing_response["pagination"]["total"] == 6
            assert len(pricing_response["items"]) == 2
            assert await native_reader_witness(reader, case.reader) == backend
            await assert_reader_write_denials(reader, case)
            encoded = transport._canonical_json_bytes(pricing_response)
            assert encoded and request.ctx.sa_session is reader
        assert request.ctx.sa_session is ordinary
    assert pricing_response == await legacy_plan_result(case)
    assert_fixed_preparation(phases, backend)
    assert not await prepared.admin.fetchval("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1)", backend)
    assert not await prepared.admin.fetchval(
        "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=$1)", temp_namespace_oid
    )


@pytest.mark.asyncio
async def test_workspace_task_cancellation_retires_only_its_physical_connection(signed_reader_case):
    prepared = signed_reader_case
    entered = asyncio.Event()
    identifiers = []
    async with prepared.database.session_factory() as ordinary:
        request = SimpleNamespace(
            path=providers.CUSTOM_IMPORT_PROVIDERS_PATH, ctx=SimpleNamespace(sa_session=ordinary, session=ordinary)
        )

        async def canceled_read():
            async with boundary.signed_request_reader(request, ordinary, plan_workspace=True) as reader:
                identifiers.append(await native_reader_witness(reader, prepared.case.reader))
                await reader.execute(text("INSERT INTO pg_temp.custom_import_plan_imported VALUES(1000000001,1)"))
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(canceled_read())
        await asyncio.wait_for(entered.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert request.ctx.sa_session is ordinary and not ordinary.in_transaction()
        assert not await prepared.admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1)", identifiers[0]
        )
        async with boundary.signed_request_reader(request, ordinary) as reader:
            assert await native_reader_witness(reader, prepared.case.reader) != identifiers[0]


@pytest.mark.asyncio
async def test_missing_distinct_reader_never_uses_ordinary_payload_connection(signed_reader_case, monkeypatch):
    prepared = signed_reader_case
    monkeypatch.setenv("HLTHPRT_DB_READER_USER", prepared.case.config["user"])
    async with prepared.database.session_factory() as ordinary:
        ordinary.info["api_reader_verified"] = True
        request = SimpleNamespace(
            path=providers.CUSTOM_IMPORT_PROVIDERS_PATH, ctx=SimpleNamespace(sa_session=ordinary, session=ordinary)
        )
        with pytest.raises(RuntimeError, match="distinct Reader"):
            async with boundary.signed_request_reader(request, ordinary):
                pytest.fail("ordinary session marker bypassed genuine Reader login")
        assert not ordinary.in_transaction() and request.ctx.sa_session is ordinary
        assert not prepared.database.has_reader_session()


@pytest.mark.asyncio
async def test_missing_existing_temp_privilege_fails_without_ordinary_fallback(signed_reader_case, monkeypatch):
    from db.connection import Database

    prepared = signed_reader_case
    isolated_name = "reader_privilege_" + uuid4().hex
    database = Database()
    database._database_override = isolated_name
    is_created = False
    try:
        await prepared.admin.execute(f'CREATE DATABASE "{isolated_name}"')
        is_created = True
        await prepared.admin.execute(f'REVOKE TEMP ON DATABASE "{isolated_name}" FROM PUBLIC')
        monkeypatch.setattr(boundary, "db", database)
        phases = await observe_reader_phases(database)
        async with prepared.database.session_factory() as ordinary:
            request = SimpleNamespace(
                path=providers.CUSTOM_IMPORT_PROVIDERS_PATH, ctx=SimpleNamespace(sa_session=ordinary, session=ordinary)
            )
            with pytest.raises(DBAPIError):
                async with boundary.signed_request_reader(request, ordinary, plan_workspace=True):
                    pytest.fail("missing TEMP privilege used an ordinary fallback")
            assert request.ctx.sa_session is ordinary and not ordinary.in_transaction()
            backend = phases[0][2]
            assert not await prepared.admin.fetchval(
                "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1)", backend
            )
            async with boundary.signed_request_reader(request, ordinary) as reader:
                await native_reader_witness(reader, prepared.case.reader)
    finally:
        await database.disconnect()
        if is_created:
            await prepared.admin.execute(f'DROP DATABASE "{isolated_name}"')


@pytest.mark.asyncio
async def test_signed_plan_service_route_prepares_workspace_before_its_reader_payload(signed_reader_case, monkeypatch):
    from tests import test_custom_import_provider_service_http as service_signature

    prepared = signed_reader_case
    plan_fixture._native_fixture(monkeypatch)
    monkeypatch.setattr(
        plan_fixture.serving, "_ptg2_npi_scope_table", lambda _tables: prepared.case.relation("native_npi_fixture")
    )
    witnesses = []

    async def payload(request, session, parsed, _verified):
        assert parsed.native_args.get("plan_release_id") and request.ctx.sa_session is session
        assert session.info["native_plan_workspace"] is True
        witnesses.append(await native_reader_witness(session, prepared.case.reader))
        return await search_imported_plan_providers(
            session,
            {},
            SimpleNamespace(limit=2, offset=0, page=1),
            plan_fixture._selection(),
            plan_fixture._import_context(),
        )

    monkeypatch.setattr(services, "_read_service_payload", payload)
    request = service_signature._request(
        service_signature._body(
            native_query={"code_system": "CPT", "code": "99213", "plan_release_id": "hprelease_" + "0" * 26}
        )
    )
    async with prepared.database.session_factory() as ordinary:
        request.ctx = SimpleNamespace(sa_session=ordinary, session=ordinary)
        reply = await services.serve_custom_import_provider_service(request, ordinary)
        assert reply.status == 200 and witnesses
        assert request.ctx.sa_session is ordinary and not ordinary.in_transaction()
    assert not await prepared.admin.fetchval("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=$1)", witnesses[0])
