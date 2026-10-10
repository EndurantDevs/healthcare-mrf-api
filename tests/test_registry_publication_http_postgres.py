# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publication HTTP lifecycle on actual migrated storage and the ordinary API role."""

import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.endpoint import registry_publication as publication
from process.registry_management_permissions import verify_registry_management_permissions
from tests.test_manual_provider_identity_store_postgres import provider_db
from tests.test_network_address_projection_postgres import projection_db as projection_db
from tests.test_network_membership_candidate_indexes_postgres import indexed_db as indexed_db
from tests.test_network_membership_publication_postgres import _published_pair
from tests.test_network_membership_publication_postgres import publication_db as publication_db
from tests.test_network_membership_validation_postgres import validation_db as validation_db
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create
from tests.test_registry_management_permissions_postgres import _arguments, _draft, _install, permissions_db

pytestmark = pytest.mark.asyncio
_AUTH = {"Authorization": "Bearer synthetic-publication-control-token"}
_PATH = "/api/v1/registry/publication"
_NO_STORE = "private, no-store"


def _reactivation_body(**changes):
    return {
        "operation": "reactivate_generation",
        "target_generation": 1,
        "expected_serving_generation": 2,
        "expected_approved_revision": 0,
        "reason": "Reviewed generation rollback",
        "idempotency_key": uuid4().hex,
        "actor": {"kind": "platform_admin", "user_id": str(uuid4()), "client_id": "system"},
        "session_token_sha256": "a" * 64,
        **changes,
    }


async def test_reactivation_http_queues_distinct_arm_and_retains_completed_status_shape(
    publication_app, publication_db
):
    fixture = publication_app
    assert publication_db.control_schema == fixture.fixture.schema
    original, current = await _published_pair(publication_db)
    document_by_name = _reactivation_body(
        target_generation=original["generation_id"], expected_serving_generation=current["generation_id"]
    )
    _, queued = await fixture.app.asgi_client.post(_PATH + "/reactivate", headers=_AUTH, json=document_by_name)
    assert queued.status == 200 and queued.json["state"] == "queued"
    _, replay = await fixture.app.asgi_client.post(_PATH + "/reactivate", headers=_AUTH, json=document_by_name)
    assert replay.json == {**queued.json, "replayed": True} and replay.headers["Cache-Control"] == _NO_STORE
    _, detail = await fixture.app.asgi_client.get(_PATH + "/requests/" + queued.json["request_id"], headers=_AUTH)
    assert detail.status == 200 and detail.json["operation"] == "reactivate_generation"
    assert set(detail.json) == {
        "request_id",
        "state",
        "actor",
        "result",
        "created_at",
        "updated_at",
        "operation",
        "command_sha256",
    }
    for private_field in ("reason", "idempotency_key", "session_token_sha256", "command_json", "selection"):
        assert private_field not in detail.json
    _, conflict = await fixture.app.asgi_client.post(
        _PATH + "/reactivate", headers=_AUTH, json={**document_by_name, "reason": "Different reason"}
    )
    _assert_error(conflict, 409)
    assert (
        await fixture.fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.fixture.schema}".network_serving_control'
        )
        == current["generation_id"]
    )


@pytest.mark.parametrize(
    "change",
    [
        "owner",
        "impersonator",
        "extra",
        "zero",
        "bool",
        "overflow",
        "reason_bytes",
        "reason_padded",
        "reason_control",
        "unknown_operation",
    ],
)
async def test_reactivation_http_strict_input_precedes_native_connection(publication_app, change):
    fixture = publication_app
    document_by_name = _reactivation_body()
    changes_by_case = {
        "owner": {"actor": {**document_by_name["actor"], "kind": "client_owner"}},
        "impersonator": {"actor": {**document_by_name["actor"], "impersonator_id": str(uuid4())}},
        "extra": {"selection": []},
        "zero": {"target_generation": 0},
        "bool": {"expected_approved_revision": True},
        "overflow": {"expected_serving_generation": 2**63},
        "reason_bytes": {"reason": "é" * 257},
        "reason_padded": {"reason": " padded "},
        "reason_control": {"reason": "Review\nreason"},
        "unknown_operation": {"operation": "publish_records"},
    }
    document_by_name.update(changes_by_case[change])
    checkouts = []
    event.listen(fixture.engine.sync_engine, "checkout", lambda *arguments: checkouts.append(arguments))
    _, invalid = await fixture.app.asgi_client.post(_PATH + "/reactivate", headers=_AUTH, json=document_by_name)
    _assert_error(invalid, 400)
    assert not checkouts


async def test_reactivation_http_requires_control_auth_and_exact_body(publication_app):
    fixture = publication_app
    document_by_name = _reactivation_body()
    checkouts = []
    event.listen(fixture.engine.sync_engine, "checkout", lambda *arguments: checkouts.append(arguments))
    _, denied = await fixture.app.asgi_client.post(_PATH + "/reactivate", json=document_by_name)
    assert denied.status == 403 and denied.headers["Cache-Control"] == _NO_STORE
    _, duplicate = await fixture.app.asgi_client.post(
        _PATH + "/reactivate",
        headers=_AUTH,
        content=json.dumps(document_by_name)[:-1] + ',"operation":"reactivate_generation"}',
    )
    _assert_error(duplicate, 400)
    _, query = await fixture.app.asgi_client.post(_PATH + "/reactivate?extra=1", headers=_AUTH, json=document_by_name)
    _assert_error(query, 400)
    assert not checkouts


@pytest.fixture
async def publication_app(permissions_db, serving_schema, monkeypatch):
    fixture = permissions_db
    _, _, source_engine = serving_schema
    receipt = await _install(fixture)
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-publication-control-token")
    engine = create_async_engine(source_engine.url)
    sessions = async_sessionmaker(engine)

    @event.listens_for(engine.sync_engine, "connect")
    def use_api_role(dbapi_connection, _connection_record):
        """Request connections use the native role granted only draft and queue columns."""
        dbapi_connection.run_async(lambda connection: connection.execute(f'SET ROLE "{fixture.roles_by_kind["api"]}"'))

    app = Sanic("registry_publication_http_" + uuid4().hex)
    app.blueprint(Blueprint.group([publication.blueprint], version_prefix="/api/v"))

    @app.middleware("request")
    async def open_session(request):
        """Open a lazy request-owned session, with no database reads before authorization."""
        request.ctx.sa_session = sessions()

    @app.middleware("response")
    async def close_session(request, _response):
        """Release each request's actual transaction and native connection."""
        await request.ctx.sa_session.close()

    try:
        yield SimpleNamespace(app=app, fixture=fixture, engine=engine, permissions=receipt)
    finally:
        await engine.dispose()


def _body(command, actor, **changes):
    actor_by_field = {key: str(value) if isinstance(value, UUID) else value for key, value in asdict(actor).items()}
    if actor_by_field["impersonator_id"] is None:
        actor_by_field.pop("impersonator_id")
    return {
        **asdict(command),
        "selection": list(command.selection),
        "actor": actor_by_field,
        "expected_serving_generation": 0,
        "session_token_sha256": "a" * 64,
        **changes,
    }


async def _seed(fixture):
    actor = _actor()
    creation = _create()
    original = await _draft(fixture, creation, actor)
    approval = await _approve(
        fixture.connection, fixture.schema, await _command(fixture.connection, fixture.schema, original), actor
    )
    correction = replace(
        creation,
        operation="correct",
        expected_revision=1,
        fields={**creation.fields, "display_name": "Reviewed Group Correction"},
        idempotency_key=uuid4().hex,
    )
    corrected = await _draft(fixture, correction, actor)
    other = await _draft(fixture, _create("company"), _actor("client_other"))
    network = await _draft(fixture, _create("network"), actor)
    command = await _command(fixture.connection, fixture.schema, corrected, network)
    return SimpleNamespace(
        actor=actor,
        original=original,
        corrected=corrected,
        other=other,
        network=network,
        command=command,
        approval=approval,
    )


async def _durable_snapshot(fixture):
    """Capture every durable registry heap, including approval, queue, and serving control."""
    table_names = sorted(fixture.permissions["table_oids"])
    parts = [
        f"SELECT '{table_name}' AS table_name,coalesce(jsonb_agg(to_jsonb(record) ORDER BY to_jsonb(record)::text),'[]'::jsonb) AS records FROM \"{fixture.fixture.schema}\".\"{table_name}\" record"
        for table_name in table_names
    ]
    return await fixture.fixture.connection.fetch(" UNION ALL ".join(parts))


def _assert_error(reply, status):
    codes_by_status = {
        400: "registry_request_invalid",
        404: "registry_record_not_found",
        409: "registry_revision_conflict",
        503: "registry_unavailable",
    }
    assert reply.status == status and reply.json == {"error": {"code": codes_by_status[status]}}
    assert reply.headers["Cache-Control"] == _NO_STORE


async def test_state_and_selected_preview_preserve_every_durable_table(publication_app):
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    before = await _durable_snapshot(fixture)
    _, state = await fixture.app.asgi_client.get(_PATH + "/state", headers=_AUTH)
    assert state.status == 200 and state.json == {
        "draft_revision": seed.command.expected_draft_revision,
        "approved_revision": seed.approval["approved_revision"],
        "serving_generation": 0,
    }
    assert state.headers["Cache-Control"] == _NO_STORE
    _, preview = await fixture.app.asgi_client.post(
        _PATH + "/preview", headers=_AUTH, json=_body(seed.command, seed.actor)
    )
    assert preview.status == 200 and preview.headers["Cache-Control"] == _NO_STORE
    assert {key: count for key, count in preview.json.items() if key != "records"} == {
        "expected_draft_revision": seed.command.expected_draft_revision,
        "expected_approved_revision": seed.command.expected_approved_revision,
        "selected_count": 2,
        "additions_count": 1,
        "corrections_count": 1,
        "archives_count": 0,
        "restores_count": 0,
        "unresolved_count": 0,
    }
    assert preview.json["records"] == [
        {
            "record_kind": "group",
            "record_id": seed.corrected["record_id"],
            "record_revision": 2,
            "before": seed.original["record"],
            "after": seed.corrected["record"],
        },
        {
            "record_kind": "network",
            "record_id": seed.network["record_id"],
            "record_revision": 1,
            "before": None,
            "after": seed.network["record"],
        },
    ]
    _, repeated = await fixture.app.asgi_client.post(
        _PATH + "/preview", headers=_AUTH, json=_body(seed.command, seed.actor)
    )
    assert repeated.status == 200 and repeated.json == preview.json
    assert await _durable_snapshot(fixture) == before


async def test_explicit_null_impersonator_matches_absent_actor_in_preview_and_queue(publication_app):
    """Canonical null actor metadata preserves exact normal actor replay identity."""
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    document = _body(seed.command, seed.actor)
    document["actor"]["impersonator_id"] = None
    before = await _durable_snapshot(fixture)
    _, preview = await fixture.app.asgi_client.post(_PATH + "/preview", headers=_AUTH, json=document)
    assert preview.status == 200 and preview.headers["Cache-Control"] == _NO_STORE
    assert await _durable_snapshot(fixture) == before
    _, queued = await fixture.app.asgi_client.post(_PATH + "/queue", headers=_AUTH, json=document)
    assert queued.status == 200
    _, replay = await fixture.app.asgi_client.post(
        _PATH + "/queue", headers=_AUTH, json=_body(seed.command, seed.actor)
    )
    assert replay.json == {**queued.json, "replayed": True}
    _, status = await fixture.app.asgi_client.get(_PATH + "/requests/" + queued.json["request_id"], headers=_AUTH)
    assert status.status == 200 and status.json["actor"] == _body(seed.command, seed.actor)["actor"]


async def test_queue_replay_after_later_drafts_keeps_actor_selection_and_no_publication(publication_app):
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    actor = replace(seed.actor, impersonator_id=uuid4())
    document = _body(seed.command, actor, selection=list(reversed(seed.command.selection)))
    before = await _durable_snapshot(fixture)
    _, queued = await fixture.app.asgi_client.post(_PATH + "/queue", headers=_AUTH, json=document)
    assert queued.status == 200 and queued.headers["Cache-Control"] == _NO_STORE
    assert set(queued.json) == {"request_id", "state", "created_at", "replayed"}
    assert queued.json["state"] == "queued" and queued.json["replayed"] is False
    after = await _durable_snapshot(fixture)
    assert [
        durable_table for durable_table in after if durable_table["table_name"] != "registry_publication_request"
    ] == [durable_table for durable_table in before if durable_table["table_name"] != "registry_publication_request"]
    await _draft(fixture.fixture, _create("company"), _actor("client_later"))
    _, replay = await fixture.app.asgi_client.post(_PATH + "/queue", headers=_AUTH, json=_body(seed.command, actor))
    assert replay.status == 200 and replay.json == {**queued.json, "replayed": True}
    _, detail = await fixture.app.asgi_client.get(_PATH + "/requests/" + queued.json["request_id"], headers=_AUTH)
    assert detail.status == 200 and detail.headers["Cache-Control"] == _NO_STORE
    assert set(detail.json) == {"request_id", "state", "actor", "selection", "result", "created_at", "updated_at"}
    assert detail.json["actor"] == _body(seed.command, actor)["actor"]
    assert detail.json["selection"] == list(seed.command.selection) and detail.json["result"] is None
    assert detail.json["state"] == "queued" and detail.json["request_id"] == queued.json["request_id"]
    assert "session_token_sha256" not in json.dumps(detail.json)
    assert (
        await fixture.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.fixture.schema}".registry_publication_request'
        )
        == 1
    )


async def test_conflicting_replay_and_all_three_head_cas_fail_closed(publication_app):
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    document = _body(seed.command, seed.actor)
    _, queued = await fixture.app.asgi_client.post(_PATH + "/queue", headers=_AUTH, json=document)
    assert queued.status == 200
    for changes in (
        {"reason": "Changed reason"},
        {"session_token_sha256": "b" * 64},
        {"selection": document["selection"][:1]},
        {"expected_serving_generation": 1},
    ):
        _, conflict = await fixture.app.asgi_client.post(_PATH + "/queue", headers=_AUTH, json={**document, **changes})
        _assert_error(conflict, 409)
    for operation in ("preview", "queue"):
        for changes in (
            {"expected_draft_revision": seed.command.expected_draft_revision - 1},
            {"expected_approved_revision": 0},
            {"expected_serving_generation": 1},
        ):
            _, stale = await fixture.app.asgi_client.post(
                _PATH + "/" + operation, headers=_AUTH, json={**document, "idempotency_key": uuid4().hex, **changes}
            )
            _assert_error(stale, 409)
    await _draft(fixture.fixture, _create("company"), seed.actor)
    _, stale = await fixture.app.asgi_client.post(
        _PATH + "/queue", headers=_AUTH, json={**document, "idempotency_key": uuid4().hex}
    )
    _assert_error(stale, 409)
    assert (
        await fixture.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.fixture.schema}".registry_publication_request'
        )
        == 1
    )


async def test_authorization_precedes_lazy_native_connection_even_for_replay(publication_app, monkeypatch):
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    document = _body(seed.command, seed.actor)
    _, queued = await fixture.app.asgi_client.post(_PATH + "/queue", headers=_AUTH, json=document)
    assert queued.status == 200
    checkouts = []
    event.listen(fixture.engine.sync_engine, "checkout", lambda *arguments: checkouts.append(arguments))
    for headers in ({}, {"Authorization": "Bearer incorrect-token"}):
        for method, path in (
            ("GET", "/state"),
            ("POST", "/preview"),
            ("POST", "/queue"),
            ("GET", "/requests/" + queued.json["request_id"]),
        ):
            _, denied = await fixture.app.asgi_client.request(
                method, _PATH + path, headers=headers, json=document if method == "POST" else None
            )
            assert denied.status == 403
            assert denied.headers.get("Cache-Control") == _NO_STORE
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    _, disabled = await fixture.app.asgi_client.post(_PATH + "/queue", headers=_AUTH, json=document)
    assert disabled.status == 403 and not checkouts
    assert disabled.headers.get("Cache-Control") == _NO_STORE
    assert (
        await fixture.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.fixture.schema}".registry_publication_request'
        )
        == 1
    )


async def test_missing_request_and_strict_get_inputs(publication_app):
    fixture = publication_app
    _, missing = await fixture.app.asgi_client.get(_PATH + "/requests/" + str(uuid4()), headers=_AUTH)
    _assert_error(missing, 404)
    for path in (
        "/state?extra=1",
        "/state?extra=",
        "/requests/" + str(uuid4()) + "?extra=1",
        "/requests/" + str(uuid4()) + "?extra=",
        "/requests/00000000-0000-0000-0000-000000000000",
        "/requests/not-a-uuid",
    ):
        _, invalid = await fixture.app.asgi_client.get(_PATH + path, headers=_AUTH)
        _assert_error(invalid, 400)
    for path in ("/state", "/requests/" + str(uuid4())):
        _, body = await fixture.app.asgi_client.request("GET", _PATH + path, headers=_AUTH, content=b"{}")
        _assert_error(body, 400)


async def test_actual_api_role_cannot_publish_or_change_controller_queue_columns(publication_app):
    fixture = publication_app
    async with fixture.engine.connect() as reader:
        assert await reader.scalar(text("SELECT current_user")) == fixture.fixture.roles_by_kind["api"]
        assert (
            await reader.scalar(
                text("SELECT pg_has_role(current_user,CAST(:owner AS name),'SET')"),
                {"owner": fixture.fixture.roles_by_kind["owner"]},
            )
            is False
        )
    async with fixture.engine.connect() as connection:
        raw = (await connection.get_raw_connection()).driver_connection
        for statement in (
            f'UPDATE "{fixture.fixture.schema}".registry_revision_control SET approved_revision=approved_revision',
            f'UPDATE "{fixture.fixture.schema}".network_serving_control SET generation_id=generation_id',
            f'UPDATE "{fixture.fixture.schema}".registry_publication_request SET state=state',
            f'DELETE FROM "{fixture.fixture.schema}".registry_publication_request',
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await raw.execute(statement)
    assert (
        await verify_registry_management_permissions(fixture.fixture.connection, **_arguments(fixture.fixture))
        == fixture.permissions
    )


def _invalid_envelopes(document):
    actor = document["actor"]
    return [
        {**document, "unexpected": True},
        {key: value for key, value in document.items() if key != "actor"},
        {**document, "expected_draft_revision": True},
        {**document, "expected_approved_revision": -1},
        {**document, "expected_draft_revision": 2**63},
        {**document, "expected_serving_generation": True},
        {**document, "expected_serving_generation": -1},
        {**document, "expected_serving_generation": 2**63},
        {**document, "expected_serving_generation": "0"},
        {**document, "reason": " "},
        {**document, "reason": "x" * 1001},
        {**document, "idempotency_key": " key "},
        {**document, "idempotency_key": "x" * 129},
        {**document, "session_token_sha256": "A" * 64},
        {**document, "session_token_sha256": None},
        {**document, "actor": None},
        {**document, "actor": {**actor, "kind": "browser"}},
        {**document, "actor": {**actor, "user_id": "00000000-0000-0000-0000-000000000000"}},
        {**document, "actor": {**actor, "user_id": "not-a-uuid"}},
        {**document, "actor": {**actor, "client_id": True}},
        {**document, "actor": {**actor, "client_id": "invalid\nclient"}},
        {**document, "actor": {**actor, "client_id": "x" * 65}},
        {**document, "actor": {**actor, "impersonator_id": "not-a-uuid"}},
        {**document, "actor": {**actor, "untrusted": "field"}},
    ]


def _invalid_selections(document):
    selected = document["selection"][-1]
    variants = [
        [],
        "all",
        [None],
        [selected, selected],
        [{**selected, "record_kind": "legacy_checksum"}],
        [{**selected, "record_id": str(selected["record_id"])}],
        [{**selected, "record_id": True}],
        [{**selected, "record_id": 0}],
        [{**selected, "record_id": -1}],
        [{**selected, "record_id": 1.5}],
        [{**selected, "record_id": 2147483648}],
        [{**selected, "record_id": str(uuid4())}],
        [{**selected, "revision": True}],
        [{**selected, "revision": 0}],
        [{**selected, "revision": 1.0}],
        [{**selected, "revision": 2**63}],
        [{**selected, "revision": "1"}],
        [{**selected, "revision": float("nan")}],
        [{**selected, "extra": 1}],
        [{key: value for key, value in selected.items() if key != "revision"}],
        [{**selected, "record_id": network_id} for network_id in range(1, 102)],
        [{"record_kind": "group", "record_id": 42, "revision": 1}],
        [{"record_kind": "group", "record_id": "00000000-0000-0000-0000-000000000000", "revision": 1}],
    ]
    return [{**document, "selection": variant} for variant in variants]


async def test_strict_post_body_selection_actor_and_bounds_leave_no_durable_writes(publication_app):
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    document = _body(seed.command, seed.actor)
    before = await _durable_snapshot(fixture)
    malformed_documents = _invalid_envelopes(document) + _invalid_selections(document)
    for operation in ("preview", "queue"):
        for malformed in malformed_documents:
            _, rejected = await fixture.app.asgi_client.post(
                _PATH + "/" + operation, headers=_AUTH, content=json.dumps(malformed).encode()
            )
            _assert_error(rejected, 400)
        for raw in (
            b"",
            b"null",
            b"[]",
            b"{",
            b"\xff",
            b" " * 65537,
            b'{"actor":{},"actor":{}}',
            b'{"selection":[{"revision":1,"revision":2}]}',
        ):
            _, rejected = await fixture.app.asgi_client.post(_PATH + "/" + operation, headers=_AUTH, content=raw)
            _assert_error(rejected, 400)
        for query_string in ("extra=1", "extra=", "extra"):
            _, query = await fixture.app.asgi_client.post(
                _PATH + "/" + operation + "?" + query_string, headers=_AUTH, json=document
            )
            _assert_error(query, 400)
    _, operation = await fixture.app.asgi_client.post(_PATH + "/approve", headers=_AUTH, json=document)
    _assert_error(operation, 400)
    assert await _durable_snapshot(fixture) == before


async def test_preview_rejects_stale_selected_version_and_missing_history(publication_app):
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    correction = replace(
        _create(),
        record_id=UUID(seed.corrected["record_id"]),
        operation="correct",
        expected_revision=2,
        fields={**_create().fields, "display_name": "Later Pending Group"},
    )
    await _draft(fixture.fixture, correction, seed.actor)
    command = await _command(fixture.fixture.connection, fixture.fixture.schema, seed.corrected, seed.network)
    before = await _durable_snapshot(fixture)
    _, stale = await fixture.app.asgi_client.post(_PATH + "/preview", headers=_AUTH, json=_body(command, seed.actor))
    _assert_error(stale, 409)
    assert await _durable_snapshot(fixture) == before
    command = await _command(fixture.fixture.connection, fixture.fixture.schema, seed.network)
    await fixture.fixture.connection.execute(
        f"DELETE FROM \"{fixture.fixture.schema}\".registry_record_history WHERE record_kind='network'"
    )
    before = await _durable_snapshot(fixture)
    _, missing = await fixture.app.asgi_client.post(_PATH + "/preview", headers=_AUTH, json=_body(command, seed.actor))
    _assert_error(missing, 409)
    assert await _durable_snapshot(fixture) == before


async def test_native_catalog_failure_is_sanitized_and_request_session_released(publication_app):
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    schema, api_role = fixture.fixture.schema, fixture.fixture.roles_by_kind["api"]
    await fixture.fixture.connection.execute(f'REVOKE SELECT ON "{schema}".registry_revision_control FROM "{api_role}"')
    for method, path in (("GET", "/state"), ("POST", "/preview"), ("POST", "/queue")):
        _, failed = await fixture.app.asgi_client.request(
            method, _PATH + path, headers=_AUTH, json=_body(seed.command, seed.actor) if method == "POST" else None
        )
        _assert_error(failed, 503)
        assert schema not in failed.text and api_role not in failed.text
    await fixture.fixture.connection.execute(
        f'REVOKE SELECT ON "{schema}".registry_publication_request FROM "{api_role}"'
    )
    _, failed = await fixture.app.asgi_client.get(_PATH + "/requests/" + str(uuid4()), headers=_AUTH)
    _assert_error(failed, 503)
    assert fixture.engine.pool.checkedout() == 0
    assert (
        await fixture.fixture.connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_publication_request') == 0
    )


async def test_preview_pins_repeatable_read_before_reading_actual_registry(publication_app, monkeypatch):
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    real_preview = publication.preview_registry_approval
    isolation_levels = []

    async def observed_preview(connection, command, actor):
        isolation_levels.append(await connection.fetchval("SHOW transaction_isolation"))
        return await real_preview(connection, command, actor)

    monkeypatch.setattr(publication, "preview_registry_approval", observed_preview)
    before = await _durable_snapshot(fixture)
    _, preview = await fixture.app.asgi_client.post(
        _PATH + "/preview", headers=_AUTH, json=_body(seed.command, seed.actor)
    )
    assert preview.status == 200 and isolation_levels == ["repeatable read"]
    assert await _durable_snapshot(fixture) == before
    assert fixture.engine.pool.checkedout() == 0


async def test_imported_preview_guard_failure_is_sanitized_without_durable_writes(publication_app, monkeypatch):
    fixture = publication_app
    seed = await _seed(fixture.fixture)

    async def unavailable_preview(connection, _command, _actor):
        assert await connection.fetchval("SHOW transaction_isolation") == "repeatable read"
        raise publication.RegistryImportedSelectionUnavailable("private-source-catalog-details")

    monkeypatch.setattr(publication, "preview_registry_approval", unavailable_preview)
    before = await _durable_snapshot(fixture)
    _, failed = await fixture.app.asgi_client.post(
        _PATH + "/preview", headers=_AUTH, json=_body(seed.command, seed.actor)
    )
    _assert_error(failed, 503)
    assert "private-source-catalog-details" not in failed.text
    assert await _durable_snapshot(fixture) == before
    assert fixture.engine.pool.checkedout() == 0


async def test_status_wire_actor_without_impersonator_and_full_actor_replay_scope(publication_app):
    fixture = publication_app
    seed = await _seed(fixture.fixture)
    document = _body(seed.command, seed.actor)
    _, first = await fixture.app.asgi_client.post(_PATH + "/queue", headers=_AUTH, json=document)
    assert first.status == 200
    _, detail = await fixture.app.asgi_client.get(_PATH + "/requests/" + first.json["request_id"], headers=_AUTH)
    assert detail.status == 200 and detail.json["actor"] == document["actor"]
    assert set(detail.json["actor"]) == {"kind", "user_id", "client_id"}
    other_actor = replace(seed.actor, client_id="client_other")
    _, other = await fixture.app.asgi_client.post(
        _PATH + "/queue", headers=_AUTH, json=_body(seed.command, other_actor)
    )
    assert other.status == 200 and other.json["request_id"] != first.json["request_id"] and not other.json["replayed"]
    _, other_detail = await fixture.app.asgi_client.get(_PATH + "/requests/" + other.json["request_id"], headers=_AUTH)
    assert other_detail.json["actor"] == _body(seed.command, other_actor)["actor"]
    stored_actors = await fixture.fixture.connection.fetch(
        f'SELECT actor_json FROM "{fixture.fixture.schema}".registry_publication_request'
    )
    assert all(json.loads(stored["actor_json"])["impersonator_id"] is None for stored in stored_actors)


async def test_source_rollback_http_ordinary_api_queues_preview_without_head_change(publication_app, publication_db):
    fixture = publication_app
    original, current = await _published_pair(publication_db)
    command = _reactivation_body(
        operation="prepare_source_rollback",
        target_generation=original["generation_id"],
        expected_serving_generation=current["generation_id"],
    )
    _, queued = await fixture.app.asgi_client.post(_PATH + "/source-rollback/preview", headers=_AUTH, json=command)
    assert queued.status == 200 and queued.json["state"] == "queued"
    _, detail = await fixture.app.asgi_client.get(_PATH + "/requests/" + queued.json["request_id"], headers=_AUTH)
    assert detail.status == 200 and detail.json["operation"] == "prepare_source_rollback"
    assert detail.headers["Cache-Control"] == _NO_STORE and "reason" not in detail.json
    assert (
        await fixture.fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.fixture.schema}".network_serving_control'
        )
        == current["generation_id"]
    )


@pytest.mark.parametrize("change", ["auth", "owner", "operation", "query", "duplicate", "bool", "key"])
async def test_source_rollback_http_auth_and_strict_shape_precede_database(publication_app, change):
    fixture = publication_app
    command = _reactivation_body(operation="prepare_source_rollback")
    if change == "owner":
        command["actor"]["kind"] = "client_owner"
    elif change == "operation":
        command["operation"] = "reactivate_generation"
    elif change == "bool":
        command["target_generation"] = True
    elif change == "key":
        command["idempotency_key"] = "bad\x00key"
    body = json.dumps(command)
    if change == "duplicate":
        body = body[:-1] + ',"operation":"prepare_source_rollback"}'
    checkouts = []
    event.listen(fixture.engine.sync_engine, "checkout", lambda *arguments: checkouts.append(arguments))
    path = _PATH + "/source-rollback/preview" + ("?extra=1" if change == "query" else "")
    _, result = await fixture.app.asgi_client.post(
        path, headers={"Accept": "application/json"} if change == "auth" else _AUTH, content=body
    )
    if change == "auth":
        assert result.status == 403 and result.headers["Cache-Control"] == _NO_STORE
    else:
        _assert_error(result, 400)
    assert not checkouts


def _source_publication_body(**changes):
    return {
        "operation": "publish_source_rollback",
        "preview_request_id": str(uuid4()),
        "candidate_sha256": "a" * 64,
        "expected_serving_generation": 2,
        "expected_approved_revision": 0,
        "reason": "Reviewed source publication",
        "idempotency_key": uuid4().hex,
        "actor": {"kind": "platform_admin", "user_id": str(uuid4()), "client_id": "system"},
        "session_token_sha256": "a" * 64,
        **changes,
    }


async def test_source_publication_http_ordinary_api_queues_distinct_explicit_arm(publication_app, publication_db):
    fixture = publication_app
    _original, current = await _published_pair(publication_db)
    document_by_name = _source_publication_body(expected_serving_generation=current["generation_id"])
    _, queued = await fixture.app.asgi_client.post(
        _PATH + "/source-rollback/publish", headers=_AUTH, json=document_by_name
    )
    assert queued.status == 200 and queued.json["state"] == "queued"
    _, detail = await fixture.app.asgi_client.get(_PATH + "/requests/" + queued.json["request_id"], headers=_AUTH)
    assert detail.status == 200 and detail.json["operation"] == "publish_source_rollback"
    assert detail.headers["Cache-Control"] == _NO_STORE and "preview_request_id" not in detail.json
    assert (
        await fixture.fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.fixture.schema}".network_serving_control'
        )
        == current["generation_id"]
    )


@pytest.mark.parametrize(
    "change", ["auth", "owner", "impersonator", "operation", "uuid", "digest", "query", "duplicate"]
)
async def test_source_publication_http_strict_auth_body_precedes_database(publication_app, change):
    fixture = publication_app
    document_by_name = _source_publication_body()
    changed_fields_by_name = {
        "owner": {"actor": {**document_by_name["actor"], "kind": "client_owner"}},
        "impersonator": {"actor": {**document_by_name["actor"], "impersonator_id": str(uuid4())}},
        "operation": {"operation": "prepare_source_rollback"},
        "uuid": {"preview_request_id": "invalid"},
        "digest": {"candidate_sha256": "A" * 64},
    }
    document_by_name.update(changed_fields_by_name.get(change, {}))
    body = json.dumps(document_by_name)
    if change == "duplicate":
        body = body[:-1] + ',"operation":"publish_source_rollback"}'
    checkouts = []
    event.listen(fixture.engine.sync_engine, "checkout", lambda *arguments: checkouts.append(arguments))
    path = _PATH + "/source-rollback/publish" + ("?extra=1" if change == "query" else "")
    _, reply = await fixture.app.asgi_client.post(
        path, headers={"Accept": "application/json"} if change == "auth" else _AUTH, content=body
    )
    if change == "auth":
        assert reply.status == 403 and reply.headers["Cache-Control"] == _NO_STORE
    else:
        _assert_error(reply, 400)
    assert not checkouts
