# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Assemble the management blueprint with native persistence and real HTTP."""

from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic

from api.endpoint import registry_management as management
from process import registry_record_store
from tests.test_registry_record_store_postgres import record_db

_AUTH = {"Authorization": "Bearer synthetic-registry-service-token"}


@pytest.fixture
async def registry_app(record_db, monkeypatch):
    _, schema, sessions = record_db
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-registry-service-token")
    for name in (
        "apply_registry_record_command",
        "get_registry_record",
        "list_registry_records",
        "verify_registry_targets",
        "prepare_registry_manual_undo",
        "read_registry_manual_history",
    ):
        original = getattr(management, name)

        def mapped_call(original):
            async def invoke(*args, **kwargs):
                return await original(*args, **kwargs, schema=schema)

            return invoke

        monkeypatch.setattr(management, name, mapped_call(original))
    for name in ("apply_network_source_binding_batch", "list_network_source_bindings"):
        original = getattr(management, name)

        def mapped_native(original):
            async def invoke(*args, **kwargs):
                return await original(*args, **kwargs, control_schema=schema)

            return invoke

        monkeypatch.setattr(management, name, mapped_native(original))
    app = Sanic("registry_http_test_" + uuid4().hex)
    app.blueprint(Blueprint.group([management.blueprint], version_prefix="/api/v"))

    @app.middleware("request")
    async def open_session(request):
        request.ctx.sa_session = sessions()

    @app.middleware("response")
    async def close_session(request, response):
        await request.ctx.sa_session.close()

    yield app, record_db


def _network_create():
    return {
        "record_id": None,
        "operation": "create",
        "expected_revision": 0,
        "fields": {"display_name": "Example National Network", "aliases": ["Example Preferred"]},
        "reason": "Create a reviewed manual network",
        "idempotency_key": "manual-create-one",
        "allocation_key": str(uuid4()),
        "actor": {"kind": "platform_admin", "user_id": str(uuid4()), "client_id": "example-client"},
    }


@pytest.mark.asyncio
async def test_native_network_http_create_replay_read_and_exact_verify(registry_app):
    app, (connection, schema, _) = registry_app
    command = _network_create()
    endpoint = "/api/v1/registry/manage"
    _, created = await app.asgi_client.post(endpoint + "/network", headers=_AUTH, json=command)
    assert created.status == 200
    assert created.headers["Cache-Control"] == "private, no-store"
    network_id = created.json["record_id"]
    assert type(network_id) is int and 0 < network_id <= 2147483647
    _, replay = await app.asgi_client.post(endpoint + "/network", headers=_AUTH, json=command)
    assert replay.status == 200 and replay.json == created.json
    _, readback = await app.asgi_client.get(endpoint + f"/network/{network_id}", headers=_AUTH)
    assert readback.json["record"] == created.json["record"]
    _, listed = await app.asgi_client.get(endpoint + "/network?limit=1", headers=_AUTH)
    assert listed.json["records"] == [created.json["record"]]
    _, verified = await app.asgi_client.post(
        endpoint + "/targets/verify",
        headers=_AUTH,
        json={"targets": [{"record_kind": "network", "action": "edit", "target_key": str(network_id)}]},
    )
    assert verified.status == 200 and verified.json == {
        "targets": {f"network:edit:{network_id}": {"known": True, "archived": False}}
    }
    changed_command_by_name = {**command, "reason": "Different request under the same replay key"}
    _, conflict = await app.asgi_client.post(endpoint + "/network", headers=_AUTH, json=changed_command_by_name)
    assert conflict.status == 409
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 1
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


@pytest.mark.asyncio
async def test_auth_body_rejection_and_typed_integer_namespace(registry_app, monkeypatch):
    app, (connection, schema, _) = registry_app
    endpoint = "/api/v1/registry/manage/network"
    command = _network_create()
    for auth_headers in ({}, {"Authorization": "Bearer incorrect-token"}):
        _, denied = await app.asgi_client.post(endpoint, headers=auth_headers, json=command)
        assert denied.status == 403
    for altered in (
        {**command, "record_id": str(uuid4())},
        {**command, "expected_revision": True},
        {**command, "fields": {"display_name": "Example", "aliases": [], "checksum_network": 42}},
        {**command, "actor": {**command["actor"], "unsupported": True}},
    ):
        _, rejected = await app.asgi_client.post(endpoint, headers=_AUTH, json=altered)
        assert rejected.status == 400
    _, duplicate = await app.asgi_client.post(endpoint, headers=_AUTH, content=b'{"record_id":null,"record_id":42}')
    assert duplicate.status == 400
    _, invalid = await app.asgi_client.get(endpoint + "/0", headers=_AUTH)
    assert invalid.status == 400
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    _, unavailable = await app.asgi_client.post(endpoint, headers=_AUTH, json=command)
    assert unavailable.status == 403
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_record') == 0


@pytest.mark.asyncio
async def test_filtered_management_page_cannot_return_unselected_records(registry_app):
    app, _ = registry_app
    endpoint = "/api/v1/registry/manage/network"
    first = _network_create()
    second_command_by_name = {
        **_network_create(),
        "idempotency_key": "second-network",
        "fields": {"display_name": "Other Network", "aliases": []},
    }
    _, created = await app.asgi_client.post(endpoint, headers=_AUTH, json=first)
    _, other = await app.asgi_client.post(endpoint, headers=_AUTH, json=second_command_by_name)
    first_id, other_id = created.json["record_id"], other.json["record_id"]
    assert first_id != other_id
    _, selected = await app.asgi_client.get(endpoint + f"?record_ids={first_id}", headers=_AUTH)
    assert selected.status == 200 and selected.json["records"] == [created.json["record"]]
    _, absent = await app.asgi_client.get(endpoint + "?record_ids=2147483647", headers=_AUTH)
    assert absent.status == 200 and absent.json["records"] == []
    for selector in (f"{first_id},{first_id}", "01", "0", "-1", "true", str(uuid4()), "1.0", ""):
        _, invalid = await app.asgi_client.get(endpoint + f"?record_ids={selector}", headers=_AUTH)
        assert invalid.status == 400
    _, noncanonical_detail = await app.asgi_client.get(endpoint + "/01", headers=_AUTH)
    assert noncanonical_detail.status == 400


async def _changed_manual_network(app):
    endpoint = "/api/v1/registry/manage/network"
    create = _network_create()
    _, first = await app.asgi_client.post(endpoint, headers=_AUTH, json=create)
    network_id = first.json["record_id"]
    correction_by_field = {
        **create,
        "record_id": network_id,
        "operation": "correct",
        "expected_revision": 1,
        "fields": {"display_name": "Later network", "aliases": []},
        "idempotency_key": "later-edit",
    }
    correction_by_field.pop("allocation_key")
    _, second = await app.asgi_client.post(endpoint, headers=_AUTH, json=correction_by_field)
    assert second.status == 200
    return create, first, second, correction_by_field


@pytest.mark.asyncio
async def test_manual_history_and_undo_http_retains_stable_draft_retry(registry_app):
    """Historical correction stays unapproved and exact retry survives later edits."""
    app, (connection, schema, _) = registry_app
    endpoint = "/api/v1/registry/manage/network"
    create, first, second, correction_by_field = await _changed_manual_network(app)
    network_id = first.json["record_id"]
    detail = endpoint + f"/{network_id}"
    _, history = await app.asgi_client.get(detail + "/history?limit=1", headers=_AUTH)
    assert history.status == 200 and history.json == {
        "record_kind": "network",
        "record_id": network_id,
        "current_revision": 2,
        "limit": 1,
        "offset": 0,
        "has_more": True,
        "records": [{"revision": 2, "custom_revision": 2, "reason": create["reason"], "record": second.json["record"]}],
    }
    undo_by_field = {
        "expected_revision": 2,
        "target_revision": 1,
        "reason": "Reviewed previous name",
        "idempotency_key": "undo-edit",
        "actor": create["actor"],
    }
    _, restored = await app.asgi_client.post(detail + "/undo", headers=_AUTH, json=undo_by_field)
    assert restored.status == 200 and restored.json["receipt"]["revision"] == 3
    assert restored.json["receipt"]["record"]["display_name"] == first.json["record"]["display_name"]
    proof = restored.json["undo"]
    assert set(proof) == {
        "target_revision",
        "target_custom_revision",
        "target_request_sha256",
        "target_archived",
        "reason",
    }
    assert proof["reason"] == f"Undo revision 1 [{proof['target_request_sha256']}]: Reviewed previous name"
    _, later = await app.asgi_client.post(
        endpoint,
        headers=_AUTH,
        json={
            **correction_by_field,
            "expected_revision": 3,
            "idempotency_key": "later-after-undo",
        },
    )
    assert later.status == 200
    _, replay = await app.asgi_client.post(detail + "/undo", headers=_AUTH, json=undo_by_field)
    assert replay.status == 200 and replay.json == restored.json
    _, stale = await app.asgi_client.post(
        detail + "/undo", headers=_AUTH, json={**undo_by_field, "idempotency_key": "stale-undo"}
    )
    assert stale.status == 409
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (4, 0)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 4


@pytest.mark.asyncio
async def test_manual_history_http_rejects_invalid_bounds_and_limits_payload(registry_app):
    app, (connection, schema, _) = registry_app
    endpoint = "/api/v1/registry/manage/network"
    _, created = await app.asgi_client.post(endpoint, headers=_AUTH, json=_network_create())
    network_id = created.json["record_id"]
    history_path = endpoint + f"/{network_id}/history"
    for query in (
        "limit=0",
        "limit=51",
        "limit=true",
        "offset=-1",
        "offset=1000001",
        "limit=1&limit=2",
        "record_ids=1",
    ):
        _, invalid = await app.asgi_client.get(history_path + "?" + query, headers=_AUTH)
        assert invalid.status == 400
    _, with_body = await app.asgi_client.request("GET", history_path, headers=_AUTH, content=b"{}")
    assert with_body.status == 400
    _, denied = await app.asgi_client.get(history_path)
    assert denied.status == 403
    _, absent = await app.asgi_client.get(endpoint + "/2147483647/history", headers=_AUTH)
    assert absent.status == 404
    _, forbidden = await app.asgi_client.get(
        "/api/v1/registry/manage/network_binding/" + str(uuid4()) + "/history", headers=_AUTH
    )
    assert forbidden.status == 400
    await connection.execute(
        f'''UPDATE "{schema}".registry_record_history SET record_json=jsonb_set(
        record_json,'{{display_name}}',to_jsonb(repeat('x',1048576))) WHERE record_kind='network' AND record_key=$1''',
        str(network_id),
    )
    _, oversized = await app.asgi_client.get(history_path, headers=_AUTH)
    assert oversized.status == 503 and oversized.json == {"error": {"code": "registry_unavailable"}}
    assert oversized.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
async def test_manual_undo_http_closed_actor_and_source_binding_boundary(registry_app):
    app, (connection, schema, _) = registry_app
    undo_by_field = {
        "expected_revision": 2,
        "target_revision": 1,
        "reason": "Undo reviewed change",
        "idempotency_key": "undo-example",
        "actor": _network_create()["actor"],
    }
    endpoint = "/api/v1/registry/manage/network/1/undo"
    _, denied = await app.asgi_client.post(endpoint, json=undo_by_field)
    assert denied.status == 403
    for body in (
        {**undo_by_field, "extra": True},
        {**undo_by_field, "target_revision": True},
        {**undo_by_field, "actor": {**undo_by_field["actor"], "extra": True}},
    ):
        _, invalid = await app.asgi_client.post(endpoint, headers=_AUTH, json=body)
        assert invalid.status == 400
    _, binding = await app.asgi_client.post(
        "/api/v1/registry/manage/network_binding/" + str(uuid4()) + "/undo", headers=_AUTH, json=undo_by_field
    )
    assert binding.status == 400
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["provider", "location"])
async def test_manual_directory_http_identity_and_readback(registry_app, kind):
    app, (connection, schema, _) = registry_app
    command = _network_create()
    command.pop("allocation_key")
    command["record_id"] = str(uuid4())
    command["fields"] = {"display_name": "Example manual record", "aliases": []}
    if kind == "provider":
        command["fields"].update(provider_kind="individual", npi=None)
    else:
        command["fields"]["address_json"] = {
            "first_line": "123 Example Street",
            "second_line": None,
            "city": "Sample City",
            "state": "CA",
            "zip": "90210",
            "country": "US",
        }
    endpoint = "/api/v1/registry/manage/" + kind
    _, created = await app.asgi_client.post(endpoint, headers=_AUTH, json=command)
    assert created.status == 200 and created.json["record_id"] == command["record_id"]
    _, readback = await app.asgi_client.get(endpoint + "/" + command["record_id"], headers=_AUTH)
    assert readback.status == 200 and readback.json["record"] == created.json["record"]
    _, listed = await app.asgi_client.get(endpoint, headers=_AUTH)
    assert listed.status == 200 and listed.json == {"records": [created.json["record"]]}
    _, replay = await app.asgi_client.post(endpoint, headers=_AUTH, json=command)
    assert replay.status == 200 and replay.json == created.json
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0


@pytest.mark.asyncio
async def test_location_native_dependency_failure_is_sanitized_and_atomic(registry_app, monkeypatch):
    app, (connection, schema, _) = registry_app
    command = _network_create()
    command.pop("allocation_key")
    command["record_id"] = str(uuid4())
    command["fields"] = {
        "display_name": "Example site",
        "aliases": [],
        "address_json": {
            "first_line": "123 Example Street",
            "second_line": None,
            "city": "Sample City",
            "state": "CA",
            "zip": "90210",
            "country": "US",
        },
    }
    monkeypatch.setattr(registry_record_store, "_fast_module", lambda: None)
    _, failed = await app.asgi_client.post("/api/v1/registry/manage/location", headers=_AUTH, json=command)
    assert failed.status == 503 and failed.json == {"error": {"code": "registry_unavailable"}}
    assert failed.headers["Cache-Control"] == "private, no-store"
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_location_registry') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 0
