# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real authenticated HTTP preserves one atomic native source-binding batch."""

from datetime import datetime

import pytest

from process import network_source_binding_store
from tests.test_registry_management_routes_postgres import _AUTH, _network_create, registry_app
from tests.test_registry_network_binding_approval_postgres import _binding
from tests.test_registry_record_store_postgres import record_db

pytestmark = pytest.mark.asyncio
_PATH = "/api/v1/registry/manage"


async def _network(app):
    command = _network_create()
    _, created = await app.asgi_client.post(_PATH + "/network", headers=_AUTH, json=command)
    assert created.status == 200
    row = _binding(created.json["record_id"])
    return row, {
        "rows": [row],
        "reason": "Reviewed exact network source",
        "idempotency_key": "source-binding-one",
        "actor": command["actor"],
    }


async def test_binding_http_batch_replay_exact_query_and_pending_detail(registry_app):
    app, (connection, schema, _) = registry_app
    binding_row, command = await _network(app)
    _, created = await app.asgi_client.post(_PATH + "/source_bindings/batch", headers=_AUTH, json=command)
    assert created.status == 200 and created.headers["Cache-Control"] == "private, no-store"
    assert created.json["records"] == [
        {
            "record_kind": "network_binding",
            "record_id": binding_row["binding_id"],
            "revision": 1,
            "network_id": binding_row["network_id"],
            "archived": False,
        }
    ]
    _, replay = await app.asgi_client.post(_PATH + "/source_bindings/batch", headers=_AUTH, json=command)
    assert replay.status == 200 and replay.json == created.json
    query_by_field = {
        "network_id": binding_row["network_id"],
        "source_system": "ptg",
        "binding_key": None,
        "limit": 50,
        "offset": 0,
    }
    _, readback = await app.asgi_client.post(_PATH + "/source_bindings/query", headers=_AUTH, json=query_by_field)
    assert readback.status == 200 and len(readback.json["records"]) == 1
    binding_record = readback.json["records"][0]
    assert binding_record["source_scope_json"] == binding_row["source_scope_json"]
    _, detail = await app.asgi_client.get(_PATH + "/network_binding/" + binding_row["binding_id"], headers=_AUTH)
    assert detail.status == 200
    detailed = detail.json["record"]
    assert {key: field_value for key, field_value in detailed.items() if key != "created_at"} == {
        key: field_value for key, field_value in binding_record.items() if key != "created_at"
    }
    assert datetime.fromisoformat(detailed["created_at"]) == datetime.fromisoformat(binding_record["created_at"])
    _, absent = await app.asgi_client.post(
        _PATH + "/source_bindings/query", headers=_AUTH, json={**query_by_field, "binding_key": "a" * 64}
    )
    assert absent.status == 200 and absent.json == {"records": []}
    _, conflict = await app.asgi_client.post(
        _PATH + "/source_bindings/batch", headers=_AUTH, json={**command, "reason": "Changed review"}
    )
    assert conflict.status == 409
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (2, 0)


async def test_binding_http_invalid_last_row_and_generic_writer_bypass_are_rejected(registry_app):
    app, (connection, schema, _) = registry_app
    row, command = await _network(app)
    for invalid in (
        {**command, "rows": [row, {**row, "network_id": True}]},
        {**command, "rows": [row, row]},
        {**command, "rows": []},
        {**command, "actor": {**command["actor"], "role": "administrator"}},
    ):
        _, rejected = await app.asgi_client.post(_PATH + "/source_bindings/batch", headers=_AUTH, json=invalid)
        assert rejected.status == 400
    for operation in ("create", "correct", "archive", "restore"):
        _, rejected = await app.asgi_client.post(
            _PATH + "/network_binding",
            headers=_AUTH,
            json={
                "record_id": row["binding_id"],
                "operation": operation,
                "expected_revision": 0 if operation == "create" else 1,
                "fields": {},
                "reason": "Generic bypass attempt",
                "idempotency_key": operation,
                "actor": command["actor"],
            },
        )
        assert rejected.status == 400
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 1


async def test_binding_http_auth_and_query_fields_are_exact(registry_app):
    app, _ = registry_app
    row, command = await _network(app)
    for headers in ({}, {"Authorization": "Bearer incorrect-token"}):
        _, denied = await app.asgi_client.post(_PATH + "/source_bindings/batch", headers=headers, json=command)
        assert denied.status == 403
    query_by_field = {
        "network_id": row["network_id"],
        "source_system": None,
        "binding_key": None,
        "limit": 50,
        "offset": 0,
    }
    for patch in (
        {"network_id": True},
        {"source_system": "any"},
        {"binding_key": "A" * 64},
        {"offset": True},
        {"scope": "any"},
    ):
        _, invalid = await app.asgi_client.post(
            _PATH + "/source_bindings/query", headers=_AUTH, json={**query_by_field, **patch}
        )
        assert invalid.status == 400
    _, invalid = await app.asgi_client.post(
        _PATH + "/source_bindings/batch", headers=_AUTH, content=b'{"rows":[],"rows":[]}'
    )
    assert invalid.status == 400


async def test_binding_http_native_unavailability_is_sanitized_and_atomic(registry_app, monkeypatch):
    app, (connection, schema, _) = registry_app
    _, command = await _network(app)
    monkeypatch.setattr(network_source_binding_store, "_fast_module", lambda: None)
    _, failed = await app.asgi_client.post(_PATH + "/source_bindings/batch", headers=_AUTH, json=command)
    assert failed.status == 503 and failed.json == {"error": {"code": "registry_unavailable"}}
    assert failed.headers["Cache-Control"] == "private, no-store"
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 0
