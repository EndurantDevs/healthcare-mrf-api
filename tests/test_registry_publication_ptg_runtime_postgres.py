# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine retained PTG office review, bounded fenced composition and publication."""

import json
import os
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_address_projection import PinnedAddressSource
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_witness import _is_retained_review_command_match
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_publication_worker_permissions import install_registry_publication_worker_permissions
from process.registry_record_store import RegistryActor, RegistryRecordCommand, apply_registry_record_command
from process.registry_source_recipe_store import decode_registry_source_recipes
from tests.test_network_custom_address_source_postgres import _draft
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create
from tests.test_registry_ptg_office_coordinator_postgres import (
    coordinator_db as coordinator_db,
)
from tests.test_registry_ptg_office_coordinator_postgres import (
    custom_db as custom_db,
)
from tests.test_registry_ptg_office_coordinator_postgres import (
    office_db as office_db,
)
from tests.test_registry_ptg_office_coordinator_postgres import (
    office_publisher as office_publisher,
)
from tests.test_registry_ptg_office_coordinator_postgres import (
    serving_schema as serving_schema,
)


@pytest.mark.parametrize("damage", [None, "current", "future", "bool", "field", "source", "fresh"])
def test_retained_command_relaxes_only_monotonic_approval_revision(damage):
    original_by_name = {"source": {"approved_revision": 3, "binding": "exact"}, "other": "unchanged"}
    expected = deepcopy(original_by_name)
    expected["source"]["approved_revision"] = 5
    current = 5
    if damage == "current":
        current = 4
    if damage == "future":
        original_by_name["source"]["approved_revision"] = 6
    if damage == "bool":
        original_by_name["source"]["approved_revision"] = True
    if damage == "field":
        expected["other"] = "changed"
    if damage == "source":
        expected["source"]["binding"] = "changed"
    if damage == "fresh":
        current = None
    assert _is_retained_review_command_match(expected, original_by_name, current) is (damage is None)


async def _review_offices(native):
    prepared = await native.service.prepare(native.request)
    command = prepared["preparation"]["command"]
    source_service = native.service.preparation.scope_service
    for table in ("registry_ptg_office_approval", "registry_ptg_office_hold"):
        await native.custom.connection.execute(
            f'GRANT SELECT,INSERT ON "{native.custom.control_schema}"."{table}" TO "{source_service.store.approval_role}"'
        )
    await native.custom.connection.execute(
        f'GRANT "{native.custom.roles["reader"]}" TO "{source_service.store.approval_role}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
    )
    envelope_by_name = {name: native.request[name] for name in ("actor", "session_token_sha256", "read_limits")}
    from process.registry_company_approval_fence import registry_company_approval_transaction
    from process.registry_ptg_producer_scope import _protected_store
    from process.registry_ptg_published_plan_scope import _read_protected_published_scope

    async with registry_company_approval_transaction(
        source_service.approval_sessions, control_schema=native.custom.control_schema
    ) as session:
        table = await _protected_store(
            session, source_service.store, write=True, table_name="registry_ptg_published_plan_scope"
        )
        with pytest.raises(ValueError, match="published_lock_privileges_unprotected"):
            await _read_protected_published_scope(
                session,
                table,
                native.request["selector"]["scope_id"],
                native.request["selector"]["client_id"],
                native.request["selector"]["scope_approval_sha256"],
            )
    approval = await source_service.approve_offices({**envelope_by_name, "command": command})
    recipes = decode_registry_source_recipes(json.dumps(approval["source_recipes"]))
    return source_service, recipes, command["source"]["approved_revision"]


async def _worker(native, execution_role, physical_owner, execution_password):
    source_service = native.service.preparation.scope_service
    await native.custom.connection.execute(
        f'GRANT "{native.fixture.source.roles[2]}" TO "{execution_role}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
    )
    async with native.custom.connection.transaction():
        await install_registry_publication_worker_permissions(
            native.custom.connection,
            lifecycle_role=native.roles_by_kind["queue"],
            execution_role=execution_role,
            api_role=native.roles_by_kind["management"],
            queue_owner_role=native.custom.roles["owner"],
            physical_owner_role=physical_owner,
            control_schema=native.custom.control_schema,
        )
    engine = create_async_engine(
        native.custom.engine.url.set(username=execution_role, password=execution_password),
        pool_size=1,
        max_overflow=0,
        hide_parameters=True,
    )
    runtime = RegistryPublicationPTGRuntime(
        async_sessionmaker(engine), source_service.store, RegistryPTGGraphReadBudget(16 * 1024 * 1024)
    )
    connection = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"], user=execution_role, password=execution_password
    )
    return engine, runtime, connection


async def _retained_pin(native):
    async with native.custom.connection.transaction(isolation="repeatable_read", readonly=True):
        return await resolve_network_serving_manifest(
            native.custom.connection, control_schema=native.custom.control_schema
        )


async def _unrelated_approval(native):
    record = await _draft(native.custom, _create(), _actor())
    approval = await _approve(
        native.custom.connection,
        native.custom.control_schema,
        await _command(native.custom.connection, native.custom.control_schema, record),
        _actor(),
    )
    return approval["approved_revision"]


async def _compose(runtime, connection, native, recipes, approved_revision, source_pin, physical_owner):
    return await runtime.compose(
        connection,
        request_id=uuid4(),
        approved_revision=approved_revision,
        expected_head=source_pin.generation_id,
        source_manifest=source_pin,
        address_sources=RegistryCompositionAddressSources(
            PinnedAddressSource(source_pin.schema_name, "entity_address_unified", source_pin.manifest_sha256),
            replacement_source_recipes=recipes,
        ),
        writer_roles=dict(
            owner_role=physical_owner,
            loader_roles=(),
            reader_roles=(native.custom.roles["reader"], native.roles_by_kind["publisher"]),
        ),
        control_schema=native.custom.control_schema,
    )


async def _archive_company(native):
    actor = RegistryActor("platform_admin", UUID(native.request["actor"]["user_id"]), "system")
    company_id = UUID(native.request["source_command"]["legal_company_id"])
    async with native.service.management_sessions() as session, session.begin():
        revision = await session.scalar(
            text(f'SELECT revision FROM "{native.custom.control_schema}".company_registry_links WHERE company_id=:id'),
            {"id": company_id},
        )
        record = await apply_registry_record_command(
            session,
            RegistryRecordCommand(
                "company_links", company_id, "archive", revision, {}, "Archive association", uuid4().hex
            ),
            actor,
            schema=native.custom.control_schema,
        )
    approval = await _approve(
        native.custom.connection,
        native.custom.control_schema,
        await _command(native.custom.connection, native.custom.control_schema, record),
        actor,
    )
    return approval["approved_revision"]


async def _cleanup_worker(native, connection, engine, created_roles):
    if connection is not None:
        await connection.close()
    if engine is not None:
        await engine.dispose()
    for role in reversed(created_roles):
        schemas = await native.custom.connection.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=$1)", role
        )
        for schema in schemas:
            assert schema["nspname"].startswith(("network_candidate_", "network_composition_"))
            await native.custom.connection.execute('DROP SCHEMA "' + schema["nspname"] + '" CASCADE')
        await native.custom.connection.execute('DROP OWNED BY "' + role + '"')
        await native.custom.connection.execute('DROP ROLE "' + role + '"')


@pytest.mark.asyncio
async def test_genuine_ptg_runtime_reuses_old_capture_then_refuses_archived_company(coordinator_db):
    native = coordinator_db
    execution_role, physical_owner = "ptg_execution_" + uuid4().hex, "ptg_network_owner_" + uuid4().hex
    execution_password = uuid4().hex
    created_roles, engine, connection = [], None, None
    try:
        service, recipes, historical_revision = await _review_offices(native)
        original = await native.custom.connection.fetchrow(
            f'SELECT a.approval_sha256,a.approval_json,h.hold_sha256,h.hold_json FROM "{native.custom.control_schema}".registry_ptg_office_approval a '
            f'JOIN "{native.custom.control_schema}".registry_ptg_office_hold h USING(capture_id,approval_sha256)'
        )
        for role in (physical_owner, execution_role):
            created_roles.append(role)
            await native.custom.connection.execute(
                'CREATE ROLE "'
                + role
                + '" '
                + (f"LOGIN PASSWORD '{execution_password}'" if role == execution_role else "NOLOGIN")
            )
        engine, runtime, connection = await _worker(native, execution_role, physical_owner, execution_password)
        current_revision = await _unrelated_approval(native)
        assert current_revision > historical_revision
        source_pin = await _retained_pin(native)
        copy_target, address_source = await _compose(
            runtime, connection, native, recipes, current_revision, source_pin, physical_owner
        )
        assert (
            await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership_source')
            == 1
        )
        await prepare_and_publish_network_candidate(
            connection,
            copy_target,
            address_source,
            owner_role=physical_owner,
            loader_roles=(),
            reader_roles=(native.custom.roles["reader"], native.roles_by_kind["publisher"]),
            control_schema=native.custom.control_schema,
        )
        published = await _retained_pin(native)
        assert published.generation_id != source_pin.generation_id
        assert await connection.fetchval(f'SELECT count(*) FROM "{published.schema_name}".network_membership') == 1
        assert (
            await native.custom.connection.fetchrow(
                f'SELECT a.approval_sha256,a.approval_json,h.hold_sha256,h.hold_json FROM "{native.custom.control_schema}".registry_ptg_office_approval a '
                f'JOIN "{native.custom.control_schema}".registry_ptg_office_hold h USING(capture_id,approval_sha256)'
            )
            == original
        )
        revoked_revision = await _archive_company(native)
        with pytest.raises(ValueError, match="company_unapproved"):
            await _compose(runtime, connection, native, recipes, revoked_revision, published, physical_owner)
        assert (await _retained_pin(native)).generation_id == published.generation_id
    finally:
        await _cleanup_worker(native, connection, engine, created_roles)


def _worker_configuration(native, execution_role, physical_owner, service, execution_password):
    from sqlalchemy.engine import make_url

    from process.registry_publication_worker import parse_registry_publication_worker_config

    url = make_url(os.environ["NETWORK_REGISTRY_TEST_DSN"])
    configured_by_name = {
        "lifecycle_dsn": url.set(
            username=native.roles_by_kind["queue"], password=native.fixture.consumer_password
        ).render_as_string(hide_password=False),
        "execution_dsn": url.set(username=execution_role, password=execution_password).render_as_string(
            hide_password=False
        ),
        "app_origin": "https://authority.example.test",
        "service_token": "synthetic-service-token",
        "consumer": {
            "control_schema": native.custom.control_schema,
            "lifecycle_role": native.roles_by_kind["queue"],
            "execution_role": execution_role,
            "api_role": native.roles_by_kind["management"],
            "queue_owner_role": native.custom.roles["owner"],
            "physical_owner_role": physical_owner,
            "reader_roles": [native.custom.roles["reader"], native.roles_by_kind["publisher"]],
        },
        "ptg": {
            "owner_role": service.store.owner_role,
            "approval_role": service.store.approval_role,
            "read_limits": {"maximum_bytes": 16 * 1024 * 1024, "maximum_pages": 256, "maximum_coordinates": 4096},
        },
    }
    return parse_registry_publication_worker_config(json.dumps(configured_by_name))


def _worker_authority(monkeypatch, *, refuse_after=None):
    import hashlib

    import httpx

    from process import registry_publication_worker as worker
    from process.registry_publication_authority import RegistryPublicationAuthorityClient
    from tests.test_registry_publication_authority import _reply

    calls = []

    async def authorize(request):
        payload = json.loads(request.content)
        calls.append(payload)
        if refuse_after is not None and len(calls) >= refuse_after:
            return httpx.Response(403, json={"error": "refused"})
        field = "selection" if "selection" in payload else "command"
        digest = hashlib.sha256(json.dumps(payload[field], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return _reply(
            {"authorized": True, "actor": payload["actor"], field + "_sha256": digest, "policy_revision": len(calls)}
        )

    monkeypatch.setattr(
        worker,
        "RegistryPublicationAuthorityClient",
        lambda origin, token: RegistryPublicationAuthorityClient(
            origin, token, transport=httpx.MockTransport(authorize)
        ),
    )
    return calls


async def _enqueue_worker_publication(native, head):
    from process.registry_publication_queue import enqueue_registry_publication

    draft_record = await _draft(native.custom, _create(), _actor())
    command = await _command(native.custom.connection, native.custom.control_schema, draft_record)
    async with native.service.management_sessions() as session, session.begin():
        await session.execute(text("SELECT 1"))
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        return await enqueue_registry_publication(
            driver,
            command,
            _actor(),
            session_token_sha256="a" * 64,
            expected_serving_generation=head,
            control_schema=native.custom.control_schema,
        )


async def _run_ptg_worker_requests(native, config, original, monkeypatch):
    from process.registry_publication_queue import (
        enqueue_registry_source_rollback_preview,
        read_registry_publication_request,
    )
    from process.registry_publication_worker import run_registry_publication_worker

    calls = _worker_authority(monkeypatch)
    queued = await _enqueue_worker_publication(native, original.generation_id)
    completed = await run_registry_publication_worker(config)
    assert completed == {"request_id": queued["request_id"], "state": "completed"}
    status = await read_registry_publication_request(
        native.custom.connection, UUID(queued["request_id"]), control_schema=native.custom.control_schema
    )
    published = await _retained_pin(native)
    assert (
        published.generation_id == status["result"]["serving_generation"]
        and published.generation_id != original.generation_id
    )
    command_by_name = dict(
        operation="prepare_source_rollback",
        target_generation=original.generation_id,
        expected_serving_generation=published.generation_id,
        expected_approved_revision=status["result"]["approved_revision"],
        reason="Review retained PTG source",
        idempotency_key=uuid4().hex,
    )
    actor = RegistryActor("platform_admin", UUID(native.request["actor"]["user_id"]), "system")
    async with native.service.management_sessions() as session, session.begin():
        await session.execute(text("SELECT 1"))
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        queued = await enqueue_registry_source_rollback_preview(
            driver, command_by_name, actor, session_token_sha256="a" * 64, control_schema=native.custom.control_schema
        )
    assert await run_registry_publication_worker(config) == {"request_id": queued["request_id"], "state": "completed"}
    preview = await read_registry_publication_request(
        native.custom.connection, UUID(queued["request_id"]), control_schema=native.custom.control_schema
    )
    assert preview["result"]["validation"]["state"] == "ready" and (await _retained_pin(native)) == published
    assert len(calls) >= 10
    return published


@pytest.fixture
async def ptg_worker_db(coordinator_db):
    native = coordinator_db
    execution_role, physical_owner = "ptg_worker_" + uuid4().hex, "ptg_worker_owner_" + uuid4().hex
    execution_password = uuid4().hex
    created_roles, engine, connection = [], None, None
    try:
        service, recipes, _ = await _review_offices(native)
        for role in (physical_owner, execution_role):
            created_roles.append(role)
            await native.custom.connection.execute(
                'CREATE ROLE "'
                + role
                + '" '
                + (f"LOGIN PASSWORD '{execution_password}'" if role == execution_role else "NOLOGIN")
            )
        engine, runtime, connection = await _worker(native, execution_role, physical_owner, execution_password)
        revision = await _unrelated_approval(native)
        source_pin = await _retained_pin(native)
        copy_target, addresses = await _compose(
            runtime, connection, native, recipes, revision, source_pin, physical_owner
        )
        await prepare_and_publish_network_candidate(
            connection,
            copy_target,
            addresses,
            owner_role=physical_owner,
            loader_roles=(),
            reader_roles=(native.custom.roles["reader"], native.roles_by_kind["publisher"]),
            control_schema=native.custom.control_schema,
        )
        original = await _retained_pin(native)
        await engine.dispose()
        config = _worker_configuration(native, execution_role, physical_owner, service, execution_password)
        yield SimpleNamespace(native=native, config=config, original=original, connection=connection)
    finally:
        await _cleanup_worker(native, connection, engine, created_roles)


@pytest.mark.asyncio
async def test_actual_worker_ordinary_and_source_preview_use_genuine_ptg_runtime(ptg_worker_db, monkeypatch):
    fixture = ptg_worker_db
    published = await _run_ptg_worker_requests(fixture.native, fixture.config, fixture.original, monkeypatch)
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{published.schema_name}".network_membership') == 1
    assert (
        await fixture.native.custom.connection.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE usename=$1", fixture.config.consumer.execution_role
        )
        == 1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["no_runtime", "archived_company", "stale_approval", "authority"])
async def test_actual_ptg_worker_refuses_before_publication(ptg_worker_db, monkeypatch, fault):
    from process.registry_publication_queue import read_registry_publication_request
    from process.registry_publication_worker import run_registry_publication_worker

    fixture = ptg_worker_db
    native, config = fixture.native, fixture.config
    if fault == "no_runtime":
        config = replace(config, ptg=None)
    if fault == "archived_company":
        await _archive_company(native)
    calls = _worker_authority(monkeypatch, refuse_after=5 if fault == "authority" else None)
    queued = await _enqueue_worker_publication(native, fixture.original.generation_id)
    if fault == "stale_approval":
        await _unrelated_approval(native)
    with pytest.raises(ValueError, match="registry_publication_request_refused"):
        await run_registry_publication_worker(config)
    status = await read_registry_publication_request(
        native.custom.connection, UUID(queued["request_id"]), control_schema=native.custom.control_schema
    )
    assert status["state"] == "rejected" and await _retained_pin(native) == fixture.original
    if fault == "authority":
        assert len(calls) == 5
        assert (
            await native.custom.connection.fetchval(
                f'SELECT state FROM "{native.custom.control_schema}".network_membership_candidate WHERE dataset_id=$1',
                uuid5(UUID(queued["request_id"]), "dataset"),
            )
            == "ready"
        )
