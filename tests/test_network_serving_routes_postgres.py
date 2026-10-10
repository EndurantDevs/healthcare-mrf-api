# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real manifest HTTP reads through full serving gates and native reader authority."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.endpoint import network_serving as serving
from process.network_address_projection import project_network_address_arrays
from process.network_membership_candidate_indexes import prepare_network_candidate_indexes
from process.network_membership_validation import validate_network_membership_candidate
from tests.test_network_address_projection_postgres import projection_db
from tests.test_network_membership_publication_postgres import (
    _candidate_writer_roles,
    _clone_raw_inputs,
    _freeze_fixture,
    _publish,
    _sequence_state,
    _snapshot,
    _write_report,
)
from tests.test_network_membership_serving_indexes_postgres import _prepare, serving_indexes_db
from tests.test_network_membership_validation_postgres import validation_db
from tests.test_network_serving_schema_postgres import serving_schema

_AUTH = {"Authorization": "Bearer synthetic-network-serving-token"}
_PATH = "/api/v1/registry/serving/manifest"
_IDENTITY_FIELDS = (
    "generation_id",
    "schema_revision",
    "source_generations",
    "approved_custom_revision",
    "manifest_sha256",
)


def _reader_engine(source_engine, reader_role):
    engine = create_async_engine(source_engine.url)

    @event.listens_for(engine.sync_engine, "connect")
    def use_reader(dbapi_connection, _connection_record):
        """Apply the actual limited reader role when a lazy request connection opens."""
        dbapi_connection.run_async(lambda connection: connection.execute(f'SET ROLE "{reader_role}"'))

    return engine


@pytest.fixture
async def manifest_app(serving_indexes_db, serving_schema, monkeypatch):
    fixture = serving_indexes_db
    _, _, source_engine = serving_schema
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-network-serving-token")
    async with fixture.connection.transaction():
        await _prepare(fixture)
    try:
        async with _candidate_writer_roles(fixture):
            async with fixture.connection.transaction():
                fixture.published_manifest = await _publish(fixture)
            engine = _reader_engine(source_engine, fixture.writer_roles["reader"])
            sessions = async_sessionmaker(engine)
            app = Sanic("network_serving_http_" + uuid4().hex)
            app.blueprint(Blueprint.group([serving.blueprint], version_prefix="/api/v"))

            @app.middleware("request")
            async def open_session(request):
                """Give each request its own lazily connected reader session."""
                request.ctx.sa_session = sessions()

            @app.middleware("response")
            async def close_session(request, _response):
                """Release the native transaction and reader connection after this response."""
                await request.ctx.sa_session.close()

            try:
                yield app, fixture, engine
            finally:
                await engine.dispose()
    finally:
        for schema_name in getattr(fixture, "additional_schemas", ()):
            await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema_name) is None


async def _next_full_candidate(fixture):
    candidate_id = uuid4()
    copy_target = replace(
        fixture.copy_target, candidate_id=str(candidate_id), schema_name="network_candidate_" + candidate_id.hex
    )
    fixture.additional_schemas.append(copy_target.schema_name)
    next_fixture = SimpleNamespace(**vars(fixture) | {"copy_target": copy_target})
    namespace = f'"{fixture.control_schema}"'
    async with fixture.connection.transaction():
        await fixture.connection.execute(
            f"INSERT INTO {namespace}.network_membership_candidate "
            "(candidate_id,dataset_id,schema_id,producer_id,schema_name,state,source_generations,"
            "approved_custom_revision,expected_head,expected_rows,accepted_rows) "
            f"SELECT $1,dataset_id,schema_id,producer_id,$2,'sealed',source_generations,approved_custom_revision,$3,expected_rows,accepted_rows "
            f"FROM {namespace}.network_membership_candidate WHERE candidate_id=$4",
            candidate_id,
            copy_target.schema_name,
            fixture.published_manifest["generation_id"],
            UUID(fixture.copy_target.candidate_id),
        )
        await _clone_raw_inputs(fixture, next_fixture)
        await validate_network_membership_candidate(
            fixture.connection, copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        await project_network_address_arrays(
            fixture.connection, copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        await prepare_network_candidate_indexes(
            fixture.connection, copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        await _prepare(next_fixture)
    await _freeze_fixture(next_fixture)
    return next_fixture


def _identity(manifest_by_name):
    return {field: manifest_by_name[field] for field in _IDENTITY_FIELDS}


@pytest.mark.asyncio
async def test_current_retained_identity_and_native_reader_without_durable_writes(manifest_app):
    app, fixture, engine = manifest_app
    before = await _snapshot(fixture)
    sequence_before = await _sequence_state(fixture)
    _, current = await app.asgi_client.get(_PATH, headers=_AUTH)
    assert current.status == 200 and current.json == _identity(fixture.published_manifest)
    assert current.headers["Cache-Control"] == "private, no-store"
    assert current.headers["X-Network-Generation"] == str(current.json["generation_id"])
    assert set(current.json) == set(_IDENTITY_FIELDS)
    assert all(
        type(current.json[field]) is int for field in ("generation_id", "schema_revision", "approved_custom_revision")
    )
    assert type(current.json["source_generations"]) is dict and type(current.json["manifest_sha256"]) is str
    async with engine.connect() as connection:
        assert await connection.scalar(text("SELECT current_user")) == fixture.writer_roles["reader"]
        assert (
            await connection.scalar(
                text("SELECT pg_has_role(current_user,CAST(:owner AS name),'SET')"),
                {"owner": fixture.writer_roles["owner"]},
            )
            is False
        )
    _, retained = await app.asgi_client.get(
        _PATH + f"?network_generation={current.json['generation_id']}", headers=_AUTH
    )
    assert retained.status == 200 and retained.json == current.json
    assert await _snapshot(fixture) == before and await _sequence_state(fixture) == sequence_before
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_revision_control SET draft_revision=7'
    )
    _, pending = await app.asgi_client.get(_PATH, headers=_AUTH)
    assert pending.json == current.json


@pytest.mark.asyncio
async def test_current_head_advance_keeps_explicit_old_generation(manifest_app, monkeypatch):
    app, fixture, _ = manifest_app
    _, first = await app.asgi_client.get(_PATH, headers=_AUTH)
    next_fixture = await _next_full_candidate(fixture)
    resolve_manifest = serving.resolve_network_serving_manifest
    next_manifests = []

    async def resolve_then_publish(connection, **kwargs):
        """Advance real publication after this request has resolved its native pin."""
        manifest = await resolve_manifest(connection, **kwargs)
        async with fixture.connection.transaction():
            next_manifests.append(await _publish(next_fixture))
        return manifest

    monkeypatch.setattr(serving, "resolve_network_serving_manifest", resolve_then_publish)
    _, pinned = await app.asgi_client.get(_PATH, headers=_AUTH)
    assert pinned.status == 200 and pinned.json == first.json
    monkeypatch.setattr(serving, "resolve_network_serving_manifest", resolve_manifest)
    _, current = await app.asgi_client.get(_PATH, headers=_AUTH)
    assert current.status == 200 and current.json == _identity(next_manifests[0])
    assert current.json["generation_id"] != first.json["generation_id"]
    _, retained = await app.asgi_client.get(_PATH + f"?network_generation={first.json['generation_id']}", headers=_AUTH)
    assert retained.status == 200 and retained.json == first.json


@pytest.mark.asyncio
async def test_control_auth_and_invalid_inputs_precede_native_reads(manifest_app, monkeypatch):
    app, fixture, engine = manifest_app
    statements = []
    event.listen(engine.sync_engine, "before_cursor_execute", lambda *arguments: statements.append(arguments[2]))
    for headers in ({}, {"Authorization": "Bearer incorrect-token"}):
        _, denied = await app.asgi_client.get(_PATH + "?network_generation=0", headers=headers)
        assert denied.status == 403
    for query in (
        "network_generation=",
        "network_generation=0",
        "network_generation=01",
        "network_generation=-1",
        "network_generation=+1",
        "network_generation=true",
        "network_generation=1.0",
        "network_generation=9223372036854775808",
        "network_generation=1&network_generation=1",
        "network_generation=1&extra=1",
        "draft_revision=1",
        "network_generation",
        "network_generation=" + "1" * 129,
        "network_generation=%EF%BC%91",
    ):
        _, invalid = await app.asgi_client.get(_PATH + "?" + query, headers=_AUTH)
        assert invalid.status == 400 and invalid.json == {"error": {"code": "network_serving_request_invalid"}}
        assert invalid.headers["Cache-Control"] == "private, no-store"
    _, body = await app.asgi_client.request("GET", _PATH, headers=_AUTH, content=b"{}")
    assert body.status == 400
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    _, unconfigured = await app.asgi_client.get(_PATH, headers=_AUTH)
    assert unconfigured.status == 403 and statements == []
    assert (await _snapshot(fixture))["manifests"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    (
        "missing_head",
        "missing_generation",
        "retired",
        "digest",
        "source",
        "custom",
        "physical_oid",
        "catalog_permission",
    ),
)
async def test_native_unavailability_is_sanitized_and_never_falls_back(manifest_app, damage):
    app, fixture, _ = manifest_app
    namespace = f'"{fixture.control_schema}"'
    query = ""
    match damage:
        case "missing_head":
            await fixture.connection.execute(f"UPDATE {namespace}.network_serving_control SET generation_id=NULL")
        case "missing_generation":
            query = "?network_generation=9223372036854775807"
        case "retired" | "digest" | "source" | "custom":
            changes_by_name = {
                "retired": "eligible=false",
                "digest": "manifest_sha256='" + "0" * 64 + "'",
                "source": "source_generations='{}'::jsonb",
                "custom": "approved_custom_revision=1",
            }
            await fixture.connection.execute(
                f"UPDATE {namespace}.network_serving_manifest SET {changes_by_name[damage]}"
            )
        case "physical_oid":
            report_by_name = json.loads(
                await fixture.connection.fetchval(
                    f"SELECT validation_json FROM {namespace}.network_membership_candidate"
                )
            )
            report_by_name["writer_closure"]["relation_oids"]["entity_address_unified"] = 1
            await _write_report(fixture, report_by_name)
        case _:
            await fixture.connection.execute(
                f'REVOKE SELECT ON {namespace}.network_serving_manifest FROM "{fixture.writer_roles["reader"]}"'
            )
    _, unavailable = await app.asgi_client.get(_PATH + query, headers=_AUTH)
    assert unavailable.status == 503 and unavailable.json == {"error": {"code": "network_serving_unavailable"}}
    assert unavailable.headers["Cache-Control"] == "private, no-store"
    assert "X-Network-Generation" not in unavailable.headers
    assert fixture.copy_target.schema_name not in unavailable.text


@pytest.mark.asyncio
async def test_native_catalog_lock_failure_is_bounded_and_sanitized(manifest_app):
    app, fixture, _ = manifest_app
    async with fixture.observer.transaction():
        await fixture.observer.execute(
            f'LOCK TABLE "{fixture.control_schema}".network_serving_manifest IN ACCESS EXCLUSIVE MODE'
        )
        _, unavailable = await asyncio.wait_for(app.asgi_client.get(_PATH, headers=_AUTH), timeout=3)
    assert unavailable.status == 503 and unavailable.json == {"error": {"code": "network_serving_unavailable"}}
