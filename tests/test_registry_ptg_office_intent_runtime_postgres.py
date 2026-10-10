# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Installed native startup/list/prepare; only upstream HTTP authority is synthetic."""

from copy import deepcopy
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sanic import Sanic

from api import control_registry_ptg_scope as routes
from process import registry_ptg_office_coordinator as coordinator
from process import registry_ptg_scope_runtime as runtime
from process.registry_ptg_office_capture import _digest
from tests.test_registry_ptg_office_coordinator_postgres import (
    _registered,
)
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

pytestmark = pytest.mark.asyncio
_PREFIX = "/control/v1/registry/ptg-source-scopes"
_AUTH = {"Authorization": "Bearer synthetic-control"}


def _configuration(native):
    """Derive credentials only in memory from exact already-owned login factories."""
    scope = native.service.preparation.scope_service
    return {
        "ptg_schema": scope.schema_name,
        "control_schema": scope.store.control_schema,
        "owner_role": scope.store.owner_role,
        "approval_role": scope.store.approval_role,
        "reader_dsn": scope.reader_sessions.kw["bind"].url.render_as_string(hide_password=False),
        "approver_dsn": scope.approval_sessions.kw["bind"].url.render_as_string(hide_password=False),
        "app_origin": scope.authority.base_url,
        "authority_token": scope.authority.token,
        "office_custody": {
            "owner_role": scope.office_custody.owner_role,
            "publisher_role": scope.office_custody.publisher_role,
        },
        "publisher_dsn": native.service.preparation.publisher_sessions.kw["bind"].url.render_as_string(
            hide_password=False
        ),
        "office_intents": {
            "management_dsn": native.service.management_sessions.kw["bind"].url.render_as_string(hide_password=False),
            "queue_dsn": native.custom.engine.url.set(
                drivername="postgresql",
                username=native.roles_by_kind["queue"],
                password=native.fixture.consumer_password,
            ).render_as_string(hide_password=False),
        },
    }


async def _role_snapshot(native):
    scope = native.service.preparation.scope_service
    role_names = sorted(
        set(
            [
                *native.roles_by_kind.values(),
                *native.fixture.source.roles,
                *native.fixture.roles,
                *native.custom.roles.values(),
            ]
        )
    )
    roles = await native.custom.connection.fetch(
        "SELECT rolname::text,oid::bigint,rolcanlogin FROM pg_catalog.pg_roles WHERE rolname=ANY($1::text[]) ORDER BY rolname",
        role_names,
    )
    assert len(roles) == len(role_names)
    identities_by_role = {record["rolname"]: dict(record) for record in roles}
    login_roles = {
        scope.reader_sessions.kw["bind"].url.username,
        scope.store.approval_role,
        *native.roles_by_kind.values(),
    }
    assert all(identities_by_role[role]["rolcanlogin"] is True for role in login_roles)
    assert identities_by_role[scope.store.owner_role]["rolcanlogin"] is False
    return identities_by_role


async def _close_native_runtime(app, native):
    """Continue exact owned pool/engine disposal; never kill sessions or change roles."""
    failures = []
    try:
        await runtime._stop_runtime(app)
    except BaseException as error:
        failures.append(type(error).__name__)
    try:
        await native.service.queue_pool.close()
    except BaseException as error:
        failures.append(type(error).__name__)
    scope = native.service.preparation.scope_service
    engines = {
        sessions.kw["bind"]
        for sessions in (
            native.service.management_sessions,
            native.service.preparation.publisher_sessions,
            scope.reader_sessions,
            scope.approval_sessions,
        )
    }
    for engine in engines:
        try:
            assert engine.pool.checkedout() == 0
            await engine.dispose()
        except BaseException as error:
            failures.append(type(error).__name__)
    return failures


async def _verify_parent_cleanup_boundary(native, identities_by_role, native_identity, journal, failures):
    """Keep OID/LOGIN evidence and refuse role teardown if identity or sessions drift."""
    role_records = await native.custom.connection.fetch(
        "SELECT rolname::text,oid::bigint,rolcanlogin FROM pg_catalog.pg_roles WHERE rolname=ANY($1::text[]) ORDER BY rolname",
        list(identities_by_role),
    )
    observed_by_role = {role_record["rolname"]: dict(role_record) for role_record in role_records}
    observed_identity = await coordinator._native_identity(native.custom.connection)
    refusals = []
    sessions = await native.custom.connection.fetchval(
        "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE usename=ANY($1::text[])",
        list(identities_by_role),
    )
    captures = []
    for schema_name in sorted(set(native.fixture.schemas)):
        schema_record = await native.custom.connection.fetchrow(
            "SELECT oid::bigint,nspowner::bigint FROM pg_catalog.pg_namespace WHERE nspname=$1",
            schema_name,
        )
        if schema_record is not None:
            captures.append({"schema": schema_name, **dict(schema_record)})
            if schema_record["nspowner"] != identities_by_role[native.fixture.roles[0]]["oid"]:
                refusals.append("capture_owner_changed:" + schema_name)
    _registered(
        journal,
        {
            "phase": "closed_before_parent_cleanup",
            "roles": identities_by_role,
            "observed_roles": observed_by_role,
            "native_identity": native_identity,
            "observed_identity": observed_identity,
            "sessions": sessions,
            "captures": captures,
            "close_failures": failures,
            "refusals": refusals,
        },
    )
    assert observed_by_role == identities_by_role and observed_identity == native_identity
    assert sessions == 0 and not failures and not refusals


@pytest.fixture
async def installed_office_runtime(coordinator_db, tmp_path, monkeypatch):
    """Real runtime construction/proofs; no native identity/permission seam is mocked."""
    native = coordinator_db
    identities_by_role = await _role_snapshot(native)
    native_identity = await coordinator._native_identity(native.custom.connection)
    journal = tmp_path / "office-intent-runtime-cleanup-registration.json"
    _registered(
        journal, {"phase": "runtime_registered", "roles": identities_by_role, "native_identity": native_identity}
    )
    app = Sanic("native_office_intents_" + uuid4().hex)
    app.blueprint(routes.blueprint, url_prefix=_PREFIX)
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    try:
        app.config[runtime.CONFIG_KEY] = _configuration(native)
        await runtime._start_runtime(app)
        service = app.ctx.registry_ptg_office_coordinator
        assert service is not native.service and service.queue_pool is not native.service.queue_pool
        assert service.queue_pool.get_max_size() == 1
        async with service.queue_pool.acquire() as connection:
            assert await coordinator._native_identity(connection) == native_identity
        yield SimpleNamespace(app=app, service=service, native=native, journal=journal, roles=identities_by_role)
    finally:
        failures = await _close_native_runtime(app, native)
        try:
            await _verify_parent_cleanup_boundary(native, identities_by_role, native_identity, journal, failures)
        finally:
            Sanic.unregister_app(app)


def _list_request(document, *, after=None, limit=20):
    return {name: deepcopy(document[name]) for name in ("actor", "session_token_sha256", "source_command")} | {
        "selector": {
            **{name: document["selector"][name] for name in ("scope_id", "client_id", "scope_approval_sha256")},
            "after": after,
            "limit": limit,
        }
    }


async def _post(installed, suffix, document, *, authenticated=True):
    _, reply = await installed.app.asgi_client.post(
        _PREFIX + "/" + suffix,
        json=document,
        headers=_AUTH if authenticated else {},
    )
    return reply


async def _queue_rows(installed):
    schema = installed.native.custom.control_schema
    return await installed.native.custom.connection.fetch(
        f'SELECT * FROM "{schema}".registry_publication_request ORDER BY request_id'
    )


async def test_native_installed_prepare_list_actor_filter_and_original_audit_replay(installed_office_runtime):
    installed = installed_office_runtime
    document = deepcopy(installed.native.request)
    document["selector"]["reason"] = "Reviewed native café office"
    denied = await _post(installed, "prepare-offices", document, authenticated=False)
    assert denied.status_code == 401 and not await _queue_rows(installed)
    prepared = await _post(installed, "prepare-offices", document)
    assert prepared.status_code == 200
    original = prepared.json
    assert original["preparation"]["capture_id"] == original["intent"]["selector"]["capture_id"]
    assert original["source_command_sha256"] == _digest(document["source_command"])
    original_audit_by_field = dict((await _queue_rows(installed))[0])
    another_actor = deepcopy(document)
    another_actor["actor"]["user_id"] = str(uuid4())
    another_actor["session_token_sha256"] = "c" * 64
    other = await installed.service.prepare(another_actor)
    assert other["intent"]["request_id"] != original["intent"]["request_id"]
    assert len(await _queue_rows(installed)) == 2
    renewed = deepcopy(document)
    renewed["session_token_sha256"] = "b" * 64
    page = await _post(installed, "office-intents", _list_request(renewed, limit=1))
    assert page.status_code == 200 and page.json["items"] == [original["intent"]]
    assert page.json["next_cursor"] is None and page.json["source_command_sha256"] == _digest(
        document["source_command"]
    )
    assert all(name not in page.text for name in ("session_token_sha256", "actor_json"))
    replay = await _post(installed, "prepare-offices", renewed)
    assert replay.status_code == 200 and replay.json == original
    current = next(
        request_record
        for request_record in await _queue_rows(installed)
        if request_record["request_id"] == UUID(original["intent"]["request_id"])
    )
    assert dict(current) == original_audit_by_field
    assert installed.native.calls.count("genuine_prepare") == 2 and installed.native.calls[-1] == "genuine_read"
    changed = _list_request(document)
    changed["source_command"]["reason"] = "Changed routing source"
    assert (await _post(installed, "office-intents", changed)).status_code == 400
    assert len(await _queue_rows(installed)) == 2
    _, wrong_method = await installed.app.asgi_client.get(_PREFIX + "/office-intents")
    assert wrong_method.status_code == 405


async def test_native_startup_global_custody_refuses_foreign_audit_writer(installed_office_runtime):
    installed = installed_office_runtime
    native, schema = installed.native, installed.native.custom.control_schema
    role = native.fixture.source.roles[2]
    relation = f'"{schema}".registry_publication_request'
    assert not await native.custom.connection.fetchval(
        "SELECT pg_catalog.has_column_privilege($1,$2,'command_json','UPDATE')",
        role,
        relation,
    )
    _registered(
        installed.journal.with_name("office-intent-runtime-mutation.json"),
        {
            "phase": "foreign_audit_grant_registered",
            "roles": installed.roles,
            "role": role,
            "relation": relation,
            "revoke": "UPDATE(command_json)",
        },
    )
    await native.custom.connection.execute(f'GRANT UPDATE(command_json) ON {relation} TO "{role}"')
    try:
        with pytest.raises(ValueError, match="queue_permissions"):
            await runtime._check_office_coordinator(installed.service)
        with pytest.raises(ValueError, match="queue_permissions"):
            await installed.service.list(_list_request(native.request))
        assert not await _queue_rows(installed)
        async with installed.service.queue_pool.acquire() as queue:
            assert not await queue.fetchval(
                "SELECT pg_catalog.has_column_privilege(current_user,$1,'command_json','UPDATE')",
                relation,
            )
    finally:
        await native.custom.connection.execute(f'REVOKE UPDATE(command_json) ON {relation} FROM "{role}"')
    await runtime._check_office_coordinator(installed.service)


async def test_native_list_denies_revoked_source_read_and_current_upstream_authority(installed_office_runtime):
    installed = installed_office_runtime
    native = installed.native
    prepared = await _post(installed, "prepare-offices", native.request)
    assert prepared.status_code == 200
    original_requests = [dict(request_record) for request_record in await _queue_rows(installed)]
    document = _list_request(native.request)
    native.app.authorized = False
    try:
        denied = await _post(installed, "office-intents", document)
        assert denied.status_code == 403 and denied.json == {"error": {"code": "registry_access_denied"}}
    finally:
        native.app.authorized = True
    reader = native.fixture.source.roles[2]
    relation = f'"{native.custom.control_schema}".registry_ptg_published_plan_scope'
    _registered(
        installed.journal.with_name("office-intent-runtime-mutation.json"),
        {
            "phase": "source_select_revocation_registered",
            "roles": installed.roles,
            "reader": reader,
            "relation": relation,
            "restore": "SELECT",
        },
    )
    await native.custom.connection.execute(f'REVOKE SELECT ON {relation} FROM "{reader}"')
    try:
        denied = await _post(installed, "office-intents", document)
        assert denied.status_code == 503 and denied.json == {"error": {"code": "registry_unavailable"}}
    finally:
        await native.custom.connection.execute(f'GRANT SELECT ON {relation} TO "{reader}"')
    recovered = await _post(installed, "office-intents", document)
    assert recovered.status_code == 200 and recovered.json["items"] == [prepared.json["intent"]]
    assert [dict(request_record) for request_record in await _queue_rows(installed)] == original_requests
