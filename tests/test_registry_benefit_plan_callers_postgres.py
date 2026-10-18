# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual management routes and queued approval use trusted selected-plan metadata."""

import asyncio
import json
import os
from dataclasses import replace
from uuid import UUID, uuid4

import asyncpg
import pytest

from api.endpoint import registry_management as management
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_company_approval_fence import _fence_key
from process.registry_manual_undo import prepare_registry_manual_undo
from process.registry_publication_consumer import consume_registry_publication_once
from process.registry_publication_execution import approve_queued_registry_publication
from process.registry_publication_queue import claim_registry_publication_request, enqueue_registry_publication
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _command
from tests.test_registry_benefit_plan_metadata import _document, _reply
from tests.test_registry_benefit_plan_writers_postgres import _client, _selected_record, _service, _write
from tests.test_registry_company_links_postgres import _AUTH
from tests.test_registry_company_links_postgres import company_links_app as company_links_app
from tests.test_registry_publication_consumer_postgres import _consumer, _head
from tests.test_registry_publication_execution_postgres import _authority
from tests.test_registry_publication_worker_permissions_postgres import _login
from tests.test_registry_publication_worker_permissions_postgres import custom_db as custom_db
from tests.test_registry_publication_worker_permissions_postgres import worker_permissions as worker_permissions

pytestmark = pytest.mark.asyncio


def _body(command, actor):
    return {
        "record_id": str(command.record_id),
        "operation": command.operation,
        "expected_revision": command.expected_revision,
        "fields": command.fields,
        "reason": command.reason,
        "idempotency_key": command.idempotency_key,
        "actor": {"kind": actor.kind, "user_id": str(actor.user_id), "client_id": actor.client_id},
    }


async def test_management_edit_batch_undo_authority_and_replay(company_links_app, monkeypatch):
    app, fixture = company_links_app
    _, schema, _ = fixture
    actor = _actor()
    company, networks, command = await _selected_record(fixture, actor)
    path = "/api/v1/registry/manage/company_links"
    body = _body(command, actor)
    _, denied = await app.asgi_client.post(path, json=body)
    assert denied.status == 403
    _, missing = await app.asgi_client.post(path, json=body, headers=_AUTH)
    assert missing.status == 503
    service_state_dict = {"status": "active"}
    async with _service(service_state_dict) as client:
        app.ctx.registry_plan_metadata_client = client
        _, created = await app.asgi_client.post(path, json=body, headers=_AUTH)
        assert created.status == 200 and len(service_state_dict["requests"]) == 1
        app.ctx.registry_plan_metadata_client = None
        _, replay = await app.asgi_client.post(path, json=body, headers=_AUTH)
        assert replay.status == 200 and replay.json == created.json
        target_by_name = {
            "company_id": company["record_id"],
            "expected_revision": 1,
            "network_ids": [network["record_id"] for network in networks],
            "group_id": None,
            "network_assertions": command.fields["network_assertions"],
        }
        batch_by_name = {
            "targets": [target_by_name],
            "reason": "Reviewed selection",
            "idempotency_key": uuid4().hex,
            "selection_group_id": None,
            "actor": body["actor"],
        }
        _, unavailable = await app.asgi_client.post(path + "/batch", json=batch_by_name, headers=_AUTH)
        assert unavailable.status == 503
        app.ctx.registry_plan_metadata_client = client
        _, retained = await app.asgi_client.post(path + "/batch", json=batch_by_name, headers=_AUTH)
        assert retained.status == 200 and len(service_state_dict["requests"]) == 2
        await _restore_choices(app, schema, command, actor, client, service_state_dict, monkeypatch)


@pytest.mark.parametrize("outcome", ["accepted", "stale", "denied", "expired"])
async def test_queued_approval_metadata_precedes_locks_and_final_authority(serving_schema, outcome):
    connection, schema, _ = serving_schema
    actor = _actor()
    command, queued, lease = await _queue_selected(serving_schema, actor, expires_soon=outcome == "expired")
    service_state_dict = {"denied": False, "http_calls": 0}
    calls = []
    allowed = _authority(calls)

    async def authorize(**request):
        if service_state_dict["denied"]:
            raise PermissionError("registry_publication_denied")
        if outcome == "accepted" and len(calls) in {2, 5}:
            other = await asyncpg.connect(os.environ["NETWORK_REGISTRY_TEST_DSN"])
            try:
                assert await other.fetchval("SELECT pg_try_advisory_xact_lock($1)", _fence_key(schema)) is False
                with pytest.raises(asyncpg.LockNotAvailableError):
                    await other.fetchrow(
                        f'SELECT id FROM "{schema}".registry_revision_control WHERE id=1 FOR UPDATE NOWAIT'
                    )
            finally:
                await other.close()
        return await allowed(**request)

    metadata = _queue_metadata(serving_schema, command, actor, outcome, service_state_dict)

    async with _client(metadata) as client:
        options_by_name = dict(authorize=authorize, control_schema=schema, plan_metadata_client=client)
        if outcome == "accepted":
            approved_receipt = await approve_queued_registry_publication(
                connection, UUID(queued["request_id"]), lease["lease_id"], **options_by_name
            )
            assert approved_receipt["replayed"] is False and len(calls) == 3
            replay = await approve_queued_registry_publication(
                connection, UUID(queued["request_id"]), lease["lease_id"], authorize=authorize, control_schema=schema
            )
            assert replay["replayed"] is True and len(calls) == 6
            service_state_dict["denied"] = True
            with pytest.raises(PermissionError):
                await approve_queued_registry_publication(
                    connection,
                    UUID(queued["request_id"]),
                    lease["lease_id"],
                    authorize=authorize,
                    control_schema=schema,
                )
        else:
            with pytest.raises(PermissionError if outcome == "denied" else RegistryApprovalConflict):
                await approve_queued_registry_publication(
                    connection, UUID(queued["request_id"]), lease["lease_id"], **options_by_name
                )
            assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0
            assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0
    assert service_state_dict["http_calls"] == 1


async def _restore_choices(app, schema, command, actor, client, service_state_dict, monkeypatch):
    path = "/api/v1/registry/manage/company_links"
    clear = replace(
        command,
        operation="correct",
        expected_revision=2,
        idempotency_key=uuid4().hex,
        fields=command.fields | {"network_assertions": []},
    )
    app.ctx.registry_plan_metadata_client = None
    _, cleared = await app.asgi_client.post(path, json=_body(clear, actor), headers=_AUTH)
    assert cleared.status == 200

    async def prepare(*args, **kwargs):
        return await prepare_registry_manual_undo(*args, **kwargs, schema=schema)

    monkeypatch.setattr(management, "prepare_registry_manual_undo", prepare)
    undo_by_name = {
        "expected_revision": 3,
        "target_revision": 1,
        "reason": "Restore reviewed choices",
        "idempotency_key": uuid4().hex,
        "actor": _body(command, actor)["actor"],
    }
    undo_path = path + "/" + str(command.record_id) + "/undo"
    _, unavailable = await app.asgi_client.post(undo_path, json=undo_by_name, headers=_AUTH)
    assert unavailable.status == 503
    app.ctx.registry_plan_metadata_client = client
    _, restored = await app.asgi_client.post(undo_path, json=undo_by_name, headers=_AUTH)
    assert restored.status == 200 and len(service_state_dict["requests"]) == 3
    app.ctx.registry_plan_metadata_client = None
    _, replay = await app.asgi_client.post(undo_path, json=undo_by_name, headers=_AUTH)
    assert replay.status == 200 and replay.json == restored.json


async def _queue_selected(fixture, actor, *, expires_soon=False):
    connection, schema, _ = fixture
    company, networks, command = await _selected_record(fixture, actor)
    async with _service({"status": "active"}) as client:
        selected_record = await _write(fixture, command, actor, plan_metadata_client=client)
    approval = await _command(connection, schema, company, *networks, selected_record)
    async with connection.transaction():
        queued = await enqueue_registry_publication(
            connection,
            approval,
            actor,
            session_token_sha256="a" * 64,
            expected_serving_generation=0,
            control_schema=schema,
        )
        lease = await claim_registry_publication_request(
            connection, operation_family="publication", control_schema=schema
        )
        if expires_soon:
            await connection.execute(
                f'UPDATE "{schema}".registry_publication_request '
                "SET lease_expires_at=clock_timestamp()+interval '1 second' WHERE request_id=$1",
                lease["request_id"],
            )
    return command, queued, lease


@pytest.mark.parametrize("status", ["active", "retired", "unconfigured"])
async def test_dedicated_login_consumer_publishes_with_configured_metadata(worker_permissions, status):
    fixture = worker_permissions
    database = fixture.fixture
    triple = (database.connection, database.control_schema, database.engine)
    actor = fixture.seed.actor
    company, networks, command = await _selected_record(triple, actor)
    service_state_dict = {"status": "active"}
    async with _service(service_state_dict) as client:
        selected_record = await _write(triple, command, actor, plan_metadata_client=client)
        approval = await _command(database.connection, database.control_schema, company, *networks, selected_record)
        async with _consumer(fixture) as (arguments_by_name, _):
            async with _login(fixture, "api") as connection, connection.transaction():
                queued = await enqueue_registry_publication(
                    connection,
                    approval,
                    actor,
                    session_token_sha256="a" * 64,
                    expected_serving_generation=0,
                    control_schema=database.control_schema,
                )
            previous_head = await _head(fixture)
            service_state_dict["status"] = status
            arguments_by_name["plan_metadata_client"] = None if status == "unconfigured" else client
            if status == "active":
                receipt = await consume_registry_publication_once(**arguments_by_name)
                assert receipt["request_id"] == queued["request_id"] and receipt["state"] == "completed"
                assert await _head(fixture) > 0
            else:
                with pytest.raises(RuntimeError if status == "unconfigured" else ValueError):
                    await consume_registry_publication_once(**arguments_by_name)
                assert await _head(fixture) == previous_head
                assert (
                    await database.connection.fetchval(
                        f'SELECT approved_revision FROM "{database.control_schema}".registry_revision_control'
                    )
                    == fixture.seed.revision
                )
            assert len(service_state_dict["requests"]) == (1 if status == "unconfigured" else 2)


def _queue_metadata(fixture, command, actor, outcome, service_state_dict):
    _, schema, _ = fixture

    async def metadata(request):
        service_state_dict["http_calls"] += 1
        if outcome == "expired":
            await asyncio.sleep(1.1)
        # A separate real writer must acquire both existing approval locks during HTTP.
        other = await asyncpg.connect(os.environ["NETWORK_REGISTRY_TEST_DSN"])
        try:
            async with other.transaction():
                await other.execute("SET LOCAL lock_timeout='300ms'")
                await other.fetchrow(f'SELECT id FROM "{schema}".registry_revision_control WHERE id=1 FOR UPDATE')
                assert await other.fetchval("SELECT pg_try_advisory_xact_lock($1)", _fence_key(schema))
            if outcome == "stale":
                clear = replace(
                    command,
                    operation="correct",
                    expected_revision=1,
                    idempotency_key=uuid4().hex,
                    fields=command.fields | {"network_assertions": []},
                )
                await _write(fixture, clear, actor)
            service_state_dict["denied"] = outcome == "denied"
        finally:
            await other.close()
        return _reply(_document(json.loads(request.content)["healthporta_plan_ids"]))

    return metadata
