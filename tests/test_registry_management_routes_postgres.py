# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Assemble the management blueprint with native persistence and real HTTP."""

import copy
import json
from dataclasses import asdict, replace
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import text

from api.endpoint import registry_management as management
from process import registry_record_store
from process.registry_management_permissions import _INSERT_COLUMNS
from tests.test_company_registry_assertions_postgres import (
    _accounting as assertion_accounting,
)
from tests.test_company_registry_assertions_postgres import (
    _create as create_company,
)
from tests.test_company_registry_assertions_postgres import (
    _declared_group as declared_group,
)
from tests.test_company_registry_assertions_postgres import (
    _install as install_assertion_permissions,
)
from tests.test_company_registry_assertions_postgres import (
    assertion_db,
)
from tests.test_network_membership_draft_store_postgres import membership_db
from tests.test_network_serving_schema_postgres import serving_schema
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
        "search_registry_companies",
        "search_registry_groups",
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


@pytest.fixture
async def membership_registry_app(membership_db, monkeypatch):
    connection, schema, sessions, _, _ = membership_db
    fixture = registry_app.__wrapped__((connection, schema, sessions), monkeypatch)
    app, _ = await anext(fixture)
    try:
        yield app, membership_db
    finally:
        await fixture.aclose()


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


@pytest.fixture
async def company_registry_app(assertion_db, monkeypatch):
    fixture = assertion_db
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-registry-service-token")
    for name in (
        "apply_registry_record_command",
        "get_registry_record",
        "list_registry_records",
        "search_registry_companies",
        "read_registry_manual_history",
    ):
        original = getattr(management, name)

        def mapped_company_call(original):
            async def invoke(*args, **kwargs):
                return await original(*args, **kwargs, schema=fixture.schema)

            return invoke

        monkeypatch.setattr(management, name, mapped_company_call(original))
    async with fixture.sessions() as session:
        assert await session.scalar(text("SELECT current_user")) == fixture.roles_by_kind["draft"]
    app = Sanic("registry_company_http_test_" + uuid4().hex)
    app.blueprint(Blueprint.group([management.blueprint], version_prefix="/api/v"))

    @app.middleware("request")
    async def open_company_session(request):
        request.ctx.sa_session = fixture.sessions()

    @app.middleware("response")
    async def close_company_session(request, response):
        await request.ctx.sa_session.close()

    yield app, fixture


def _company_http_command(group_id):
    command = create_company()
    command.fields["assertions"]["group_assertions"] = [declared_group(command.record_id, group_id)]
    document_by_field = json.loads(json.dumps(asdict(command), default=str))
    del document_by_field["record_kind"]
    del document_by_field["allocation_key"]
    del document_by_field["_undo_provenance_required"]
    document_by_field["actor"] = _network_create()["actor"]
    return document_by_field


@pytest.mark.asyncio
async def test_company_search_http_preserves_assertion_record(company_registry_app):
    app, fixture = company_registry_app
    command = _company_http_command(await _http_group_target(app))
    command["fields"]["aliases"] = ["RéSeau %_\\ Alias"]
    endpoint = "/api/v1/registry/manage/company"
    _, created = await app.asgi_client.post(endpoint, headers=_AUTH, json=command)
    assert created.status == 200
    body_by_field = {"q": "%_\\", "limit": 1, "offset": 0, "record_ids": [command["record_id"]]}
    _, found = await app.asgi_client.post(endpoint + "/search", headers=_AUTH, json=body_by_field)
    assert found.status == 200 and found.json == {"records": [created.json["record"]]}
    assert found.headers["Cache-Control"] == "private, no-store"
    assert found.json["records"][0]["group_assertions"]
    _, empty = await app.asgi_client.post(endpoint + "/search", headers=_AUTH, json=body_by_field | {"record_ids": []})
    assert empty.status == 200 and empty.json == {"records": []}
    _, admin = await app.asgi_client.post(
        endpoint + "/search", headers=_AUTH, json=body_by_field | {"record_ids": None}
    )
    assert admin.json == found.json
    assert (await assertion_accounting(fixture))["history"] == 2


@pytest.mark.asyncio
async def test_company_search_http_auth_and_closed_validation(registry_app):
    app, (connection, schema, _) = registry_app
    endpoint = "/api/v1/registry/manage/company/search"
    body_by_field = {"q": "Example", "limit": 50, "offset": 0, "record_ids": None}
    _, denied = await app.asgi_client.post(endpoint, json=body_by_field)
    assert denied.status == 403
    for altered in (
        body_by_field | {"q": "é" * 65},
        body_by_field | {"limit": True},
        body_by_field | {"record_ids": [str(uuid4())] * 2},
        body_by_field | {"extra": True},
    ):
        _, invalid = await app.asgi_client.post(endpoint, headers=_AUTH, json=altered)
        assert invalid.status == 400 and invalid.json == {"error": {"code": "registry_request_invalid"}}
        assert invalid.headers["Cache-Control"] == "private, no-store"
    _, duplicate = await app.asgi_client.post(
        endpoint, headers=_AUTH, content=b'{"q":"x","q":"y","limit":1,"offset":0,"record_ids":null}'
    )
    assert duplicate.status == 400
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry') == 0


def _company_http_correction(command, revision):
    corrected_by_field = copy.deepcopy(command)
    corrected_by_field.update(operation="correct", expected_revision=revision, idempotency_key=uuid4().hex)
    corrected_by_field["fields"]["assertions"]["expected_revision"] = revision
    return corrected_by_field


async def _http_group_target(app):
    command_by_field = _network_create()
    del command_by_field["allocation_key"]
    command_by_field.update(
        record_id=str(uuid4()),
        fields={"group_kind": "corporate_parent", "display_name": "Example Group", "aliases": []},
    )
    _, created = await app.asgi_client.post("/api/v1/registry/manage/group", headers=_AUTH, json=command_by_field)
    assert created.status == 200
    return command_by_field["record_id"]


@pytest.mark.asyncio
async def test_group_search_http_uses_native_fixture_schema_and_selector(registry_app):
    app, _ = registry_app
    group_id = await _http_group_target(app)
    endpoint = "/api/v1/registry/manage/group/search"
    body_by_field = {"q": "Example", "limit": 1, "offset": 0, "record_ids": [group_id]}
    _, denied = await app.asgi_client.post(endpoint, json=body_by_field)
    assert denied.status == 403
    _, found = await app.asgi_client.post(endpoint, headers=_AUTH, json=body_by_field)
    assert found.status == 200 and [record["group_id"] for record in found.json["records"]] == [group_id]
    assert found.headers["Cache-Control"] == "private, no-store"
    _, excluded = await app.asgi_client.post(endpoint, headers=_AUTH, json=body_by_field | {"record_ids": []})
    assert excluded.status == 200 and excluded.json == {"records": []}


async def _http_company_read(app, command):
    path = "/api/v1/registry/manage/company/" + command["record_id"]
    _, readback = await app.asgi_client.get(path, headers=_AUTH)
    assert readback.status == 200 and readback.headers["Cache-Control"] == "private, no-store"
    return readback.json["record"]


@pytest.mark.asyncio
async def test_company_group_http_roundtrip_history_and_omitted_preservation(company_registry_app):
    app, fixture = company_registry_app
    command = _company_http_command(await _http_group_target(app))
    endpoint = "/api/v1/registry/manage/company"
    before = await assertion_accounting(fixture)
    _, denied = await app.asgi_client.post(endpoint, json=command)
    assert denied.status == 403 and await assertion_accounting(fixture) == before
    _, created = await app.asgi_client.post(endpoint, headers=_AUTH, json=command)
    assert created.status == 200 and created.headers["Cache-Control"] == "private, no-store"
    group_assertions = command["fields"]["assertions"]["group_assertions"]
    assert created.json["record"]["group_assertions"] == group_assertions
    assert await _http_company_read(app, command) == created.json["record"]
    for revision, omission in ((1, "group_assertions"), (2, "assertions")):
        correction = _company_http_correction(command, revision)
        del correction["fields"]["assertions"]["group_assertions"]
        if omission == "assertions":
            del correction["fields"]["assertions"]
        _, preserved = await app.asgi_client.post(endpoint, headers=_AUTH, json=correction)
        assert preserved.status == 200 and preserved.json["record"]["group_assertions"] == group_assertions
    clear = _company_http_correction(command, 3)
    clear["fields"]["assertions"]["group_assertions"] = []
    _, cleared = await app.asgi_client.post(endpoint, headers=_AUTH, json=clear)
    assert cleared.status == 200 and cleared.json["record"]["group_assertions"] == []
    assert await _http_company_read(app, command) == cleared.json["record"]
    _, history = await app.asgi_client.get(endpoint + "/" + command["record_id"] + "/history", headers=_AUTH)
    assert history.status == 200 and history.headers["Cache-Control"] == "private, no-store"
    assert [entry["revision"] for entry in history.json["records"]] == [4, 3, 2, 1]
    assert [entry["record"]["group_assertions"] for entry in history.json["records"]] == [
        [],
        group_assertions,
        group_assertions,
        group_assertions,
    ]
    _, replay = await app.asgi_client.post(endpoint, headers=_AUTH, json=command)
    assert replay.status == 200 and replay.json == created.json
    actor = json.loads(
        await fixture.connection.fetchval(
            f"SELECT actor_json::text FROM {fixture.namespace}.registry_record_history WHERE record_kind='company' AND revision=1"
        )
    )
    assert actor == command["actor"] | {"impersonator_id": None}
    assert (
        await fixture.connection.fetchval(
            f"SELECT approved_revision FROM {fixture.namespace}.registry_revision_control"
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("case,status", [("company", 400), ("source", 409), ("target", 409)])
async def test_company_group_http_refusal_rolls_back_every_family(company_registry_app, case, status):
    app, fixture = company_registry_app
    command = _company_http_command(await _http_group_target(app))
    endpoint = "/api/v1/registry/manage/company"
    _, created = await app.asgi_client.post(endpoint, headers=_AUTH, json=command)
    assert created.status == 200
    correction = _company_http_correction(command, 1)
    group_assertion = correction["fields"]["assertions"]["group_assertions"][0]
    match case:
        case "company":
            group_assertion["company_id"] = str(uuid4())
        case "source":
            group_assertion["provenance"] = {
                "kind": "source_reference",
                "evidence_ref": "Unretained occurrence",
                "snapshot_id": str(uuid4()),
                "source_record_key": "missing-occurrence",
            }
        case "target":
            group_assertion["group_id"] = str(uuid4())
    before = await assertion_accounting(fixture)
    _, refused = await app.asgi_client.post(endpoint, headers=_AUTH, json=correction)
    assert refused.status == status and refused.headers["Cache-Control"] == "private, no-store"
    assert await assertion_accounting(fixture) == before
    assert await _http_company_read(app, command) == created.json["record"]


@pytest.mark.asyncio
async def test_company_group_http_live_insert_revocation_rolls_back(company_registry_app):
    app, fixture = company_registry_app
    command = _company_http_command(await _http_group_target(app))
    endpoint = "/api/v1/registry/manage/company"
    _, created = await app.asgi_client.post(endpoint, headers=_AUTH, json=command)
    assert created.status == 200
    correction = _company_http_correction(command, 1)
    correction["fields"]["display_name"] = "Changed company"
    before = await assertion_accounting(fixture)
    columns = ",".join('"' + name + '"' for name in _INSERT_COLUMNS["company_registry_group_assertion"])
    try:
        await fixture.connection.execute(
            f'REVOKE INSERT({columns}) ON {fixture.namespace}.company_registry_group_assertion FROM "{fixture.roles_by_kind["draft"]}"'
        )
        _, refused = await app.asgi_client.post(endpoint, headers=_AUTH, json=correction)
        assert refused.status == 503 and refused.json == {"error": {"code": "registry_unavailable"}}
        assert refused.headers["Cache-Control"] == "private, no-store"
        assert await assertion_accounting(fixture) == before
        assert await _http_company_read(app, command) == created.json["record"]
    finally:
        await install_assertion_permissions(fixture)
    _, restored = await app.asgi_client.post(endpoint, headers=_AUTH, json=correction)
    assert restored.status == 200 and restored.json["record"]["display_name"] == "Changed company"


@pytest.mark.asyncio
async def test_membership_http_custom_revision_propagates_without_head_fallback(membership_registry_app):
    """An old exact office snapshot survives later clearing and unrelated revisions."""
    from tests.test_network_membership_draft_store_postgres import _apply, _command, _member, _seed

    app, database = membership_registry_app
    actor, network_id, provider_id, sites = await _seed(database)
    first = await _apply(database, _command(network_id, [_member(network_id, provider_id, sites[0])]), actor)
    cleared = await _apply(
        database,
        replace(_command(network_id, []), operation="correct", expected_revision=1, idempotency_key=uuid4().hex),
        actor,
    )
    endpoint = f"/api/v1/registry/manage/membership/{network_id}"
    _, historical = await app.asgi_client.get(endpoint + f"?custom_revision={first['custom_revision']}", headers=_AUTH)
    assert historical.status == 200 and historical.json == {"record": first["record"]}
    _, head = await app.asgi_client.get(endpoint, headers=_AUTH)
    assert head.status == 200 and head.json == {"record": cleared["record"]}
    _, absent = await app.asgi_client.get(endpoint + "?custom_revision=0", headers=_AUTH)
    assert absent.status == 404 and absent.json == {"error": {"code": "registry_record_not_found"}}
    _, selected = await app.asgi_client.get(
        f"/api/v1/registry/manage/membership?custom_revision={first['custom_revision']}&record_ids={network_id}&limit=1&offset=0",
        headers=_AUTH,
    )
    assert selected.status == 200 and selected.json == {"records": [first["record"]]}
    _, unauthorized = await app.asgi_client.get(endpoint + "?custom_revision=0")
    assert unauthorized.status == 403
    for query in (
        "custom_revision=",
        "custom_revision=01",
        "custom_revision=-1",
        "custom_revision=true",
        "custom_revision=1.0",
        "custom_revision=1&custom_revision=2",
        "custom_revision=9223372036854775808",
        f"custom_revision={cleared['custom_revision'] + 1}",
        "source_revision=1",
        "offset=1",
    ):
        _, invalid = await app.asgi_client.get(endpoint + "?" + query, headers=_AUTH)
        assert invalid.status == 400
    _, wrong_kind = await app.asgi_client.get(
        f"/api/v1/registry/manage/network/{network_id}?custom_revision=0", headers=_AUTH
    )
    assert wrong_kind.status == 400
