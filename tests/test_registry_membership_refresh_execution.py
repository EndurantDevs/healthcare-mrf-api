# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Final refresh fences compare complete registered recipes and live leases."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from sqlalchemy.engine import make_url

from process import registry_membership_refresh_execution as refresh
from process import registry_publication_execution as publication
from process.network_address_projection import PinnedAddressSource
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_membership_refresh_execution import _require_prepared_recipes
from process.registry_membership_refresh_queue import RegistryMembershipRefreshAddressProfile
from process.registry_publication_consumer import _lease_authority
from tests.test_registry_source_recipe_store_postgres import _candidate, _cms_epoch_recipe


def recipe_for(index=0):
    recipe = _cms_epoch_recipe()
    return replace(
        recipe,
        source_pin=replace(
            recipe.source_pin, retained_epoch=replace(recipe.source_pin.retained_epoch, content_sha256=str(index) * 64)
        ),
    )


def source_inputs(index=0):
    return RegistryCompositionAddressSources(
        PinnedAddressSource("address_example", "entity_address_unified", "a" * 64),
        initial_source_recipes=(recipe_for(index),),
        compact=True,
    )


def test_final_source_fence_compares_whole_recipe_documents():
    original = source_inputs()
    _require_prepared_recipes(_candidate((recipe_for(),)), original, original)
    with pytest.raises(RegistryApprovalConflict, match="candidate_recipes_changed"):
        _require_prepared_recipes(_candidate((recipe_for(1),)), original, original)
    with pytest.raises(RegistryApprovalConflict, match="sources_changed"):
        _require_prepared_recipes(_candidate((recipe_for(),)), original, source_inputs(1))
    with pytest.raises(RegistryApprovalConflict, match="sources_changed"):
        _require_prepared_recipes(
            _candidate((recipe_for(),)),
            original,
            replace(original, base_source=replace(original.base_source, generation_id="b" * 64)),
        )


@pytest.mark.asyncio
async def test_commit_authority_never_outlives_a_lost_lease():
    stopped = asyncio.Event()
    authorize = AsyncMock(return_value={"policy_revision": 1})
    stopped.set()
    with pytest.raises(RegistryApprovalConflict, match="lease_conflict"):
        await _lease_authority(authorize, stopped, {"actor": {}, "command": {}})
    authorize.assert_not_awaited()
    stopped.clear()

    async def revoked_during_http(**_request):
        stopped.set()
        return {"policy_revision": 2}

    with pytest.raises(RegistryApprovalConflict, match="lease_conflict"):
        await _lease_authority(revoked_during_http, stopped, {"actor": {}, "command": {}})


@pytest.mark.asyncio
async def test_source_workers_recover_compact_base_without_changing_legacy_inputs(monkeypatch):
    original = source_inputs()
    connection = object()
    pin = SimpleNamespace(schema_name="retained", manifest_sha256="b" * 64, compact_layout=None)
    recover = AsyncMock(return_value=original)
    monkeypatch.setattr(publication, "_source_rollback_address_sources", recover)
    legacy = await publication.source_worker_address_sources(connection, pin, "registry_example")
    assert legacy.base_source == PinnedAddressSource("retained", "entity_address_unified", "b" * 64)
    assert legacy.compact is False
    recover.assert_not_awaited()
    pin.compact_layout = object()
    assert await publication.source_worker_address_sources(connection, pin, "registry_example") is original
    recover.assert_awaited_once_with(connection, pin, "registry_example")


def _runtime(authorize):
    return refresh.RegistryMembershipRefreshRuntime(
        authorize, RegistryMembershipRefreshAddressProfile("addresses", ("reader",)), AsyncMock(), AsyncMock()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["authority", "profile", "resolve", "pin"])
async def test_refresh_runtime_rejects_missing_host_capabilities(fault):
    from tests.test_registry_membership_refresh_authority import client

    async with client(lambda _request: pytest.fail("configuration attempted HTTP")) as authorize:
        runtime = _runtime(authorize)
        changes = {
            "authority": {"authorize": {}},
            "profile": {"address_profile": {}},
            "resolve": {"resolve_sources": None},
            "pin": {"pin_sources": None},
        }[fault]
        with pytest.raises(ValueError, match="runtime_invalid"):
            replace(runtime, **changes)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["untyped", "noncompact", "replacement_on_initial"])
async def test_fresh_sources_cannot_change_phase_or_drop_compact_custody(monkeypatch, fault):
    from tests.test_registry_membership_refresh_authority import client
    from tests.test_registry_membership_refresh_observations import published_rows

    _, record, _, _ = published_rows()
    sources = source_inputs()
    if fault == "noncompact":
        sources = replace(sources, compact=False)
    elif fault == "replacement_on_initial":
        sources = replace(sources, initial_source_recipes=(), replacement_source_recipes=sources.initial_source_recipes)
    monkeypatch.setattr(refresh, "_current_heads", AsyncMock(return_value={}))
    monkeypatch.setattr(refresh, "_require_address", AsyncMock())
    authorize = AsyncMock()
    async with client(lambda _request: pytest.fail("unexpected transport")) as configured:
        runtime = replace(
            _runtime(configured), resolve_sources=AsyncMock(return_value={} if fault == "untyped" else sources)
        )
        with pytest.raises(ValueError, match="refresh_sources_invalid|refresh_source_phase_invalid"):
            await refresh._fresh_inputs(object(), record, runtime, "registry", authorize)
    authorize.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "head", "address"])
async def test_final_candidate_must_keep_exact_approval_head_and_address_epoch(fault):
    from process.registry_candidate_composition import _target
    from tests.test_registry_membership_refresh_contract import command

    selected = command()
    target = _target(UUID(int=1), selected["expected_approved_revision"], selected["expected_serving_generation"], {})
    candidate_by_field = {
        "approved_custom_revision": selected["expected_approved_revision"],
        "expected_head": selected["expected_serving_generation"],
        "source_generations": {"network_address_origin": selected["address_artifact_sha256"]},
    }
    if fault == "head":
        candidate_by_field["expected_head"] += 1
    elif fault == "address":
        candidate_by_field["source_generations"]["network_address_origin"] = "f" * 64
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=None if fault == "missing" else candidate_by_field))
    with pytest.raises(RegistryApprovalConflict, match="candidate_changed|address_changed"):
        await refresh._candidate_scope(driver, target, selected, "registry")
    assert driver.fetchrow.await_args.args[1] == UUID(target.candidate_id)


@pytest.mark.asyncio
async def test_refresh_refuses_nested_execution_transaction_before_touching_request():
    from tests.test_registry_membership_refresh_authority import client

    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock())
    async with client(lambda _request: pytest.fail("unexpected transport")) as authorize:
        with pytest.raises(ValueError, match="runtime_invalid"):
            await refresh.complete_queued_membership_refresh(
                connection,
                {},
                runtime=_runtime(authorize),
                writer_context=None,
                authorize=AsyncMock(),
                authorize_commit=AsyncMock(),
            )
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_changed_locked_request_stops_before_candidate_locks_or_final_grant(monkeypatch):
    events = []

    @asynccontextmanager
    async def locked(*_arguments):
        try:
            yield {"request_sha256": "b" * 64}
        finally:
            events.append("released")

    monkeypatch.setattr(refresh, "_locked_request_transaction", locked)
    controls = AsyncMock()
    monkeypatch.setattr(refresh, "_lock_publication_controls", controls)
    final_authority = AsyncMock()
    with pytest.raises(RegistryApprovalConflict, match="queue_request_changed"):
        await refresh._finish_refresh(
            object(),
            (UUID(int=1), UUID(int=2), {"request_sha256": "a" * 64}),
            ({"command": {}}, None, None),
            None,
            SimpleNamespace(control_schema="registry"),
            final_authority,
        )
    assert events == ["released"]
    controls.assert_not_awaited()
    final_authority.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError("invalid rows"), asyncio.CancelledError()])
async def test_refresh_fixture_uses_named_preparation_before_after_seal_hook(failure):
    from tests.registry_membership_refresh_execution_support import RefreshExecution

    fixture = RefreshExecution(SimpleNamespace(admin=SimpleNamespace(kw={"bind": object()})), (None,) * 5)
    target = object()
    fixture.prepare_candidate = AsyncMock(side_effect=failure)
    fixture.after_seal = AsyncMock()
    if failure is None:
        await fixture.seal(target)
        fixture.after_seal.assert_awaited_once_with(target)
    else:
        with pytest.raises(type(failure)) as error:
            await fixture.seal(target)
        assert error.value is failure
        fixture.after_seal.assert_not_awaited()
    fixture.prepare_candidate.assert_awaited_once_with(target)


@pytest.mark.asyncio
async def test_refresh_fixture_installs_configured_runtime_and_administrator_freeze(monkeypatch):
    from tests import registry_membership_refresh_execution_support as support

    fixture = support.RefreshExecution(
        SimpleNamespace(
            admin=SimpleNamespace(
                kw={"bind": SimpleNamespace(url=make_url("postgresql+asyncpg://admin@localhost/test"))}
            ),
            names=SimpleNamespace(reader="source_reader"),
        ),
        (None, None, "registry_control", None, []),
    )
    fixture.roles_by_kind = {
        name: "refresh_" + name
        for name in ("queue", "physical", "custody", "api", "lifecycle", "execution", "custodian")
    }
    fixture.password = "test"
    connect = AsyncMock(side_effect=lambda *_args, **_kwargs: SimpleNamespace(close=AsyncMock()))
    pools = AsyncMock(side_effect=lambda **_kwargs: SimpleNamespace(close=AsyncMock()))
    monkeypatch.setattr(support.asyncpg, "connect", connect)
    monkeypatch.setattr(support.asyncpg, "create_pool", pools)
    monkeypatch.setattr(support.RegistryPublicationSQLLimits, "verify", AsyncMock())
    installed, callback = SimpleNamespace(preparation_roles=object()), AsyncMock()
    configure = AsyncMock(return_value=installed)
    freeze = Mock(return_value=callback)
    monkeypatch.setattr(support, "_configured_worker_runtime", configure)
    monkeypatch.setattr(support, "administrative_freeze_callback", freeze)
    async with AsyncExitStack() as stack:
        await support._connect_workers(fixture, stack)
        settings, actual_stack = configure.await_args.args
        assert actual_stack is stack
        assert {**settings.consumer.permission_roles, "control_schema": settings.consumer.control_schema} == (
            fixture.permission_roles
        )
        assert settings.consumer.reader_roles == ("source_reader",)
        assert settings.execution_url.username == fixture.roles_by_kind["execution"]
        assert settings.preparation_roles == {
            "namespace_owner_role": "refresh_custody",
            "publisher_role": "refresh_execution",
            "application_roles": sorted({"source_reader", *fixture.roles_by_kind.values()} - {"refresh_custody"}),
        }
        assert fixture.preparation_roles is installed.preparation_roles
        assert fixture.prepare_candidate is callback
        freeze.assert_called_once_with(fixture, installed)
    assert connect.await_count == pools.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("write_allowed", [False, True])
async def test_refresh_published_write_probe_refuses_an_application_write_grant(monkeypatch, write_allowed):
    from tests import registry_membership_refresh_execution_support as support

    statements = []

    class Connection:
        @asynccontextmanager
        async def transaction(self):
            yield

        async def execute(self, statement):
            statements.append(statement)
            if not write_allowed:
                raise asyncpg.InsufficientPrivilegeError("published generation is read only")

    connection = Connection()

    @asynccontextmanager
    async def acquire():
        yield connection

    fixture = support.RefreshExecution(SimpleNamespace(admin=SimpleNamespace(kw={"bind": object()})), (None,) * 5)
    fixture.connection = fixture.api = fixture.custodian = connection
    fixture.execution_pool = fixture.lifecycle_pool = SimpleNamespace(acquire=acquire)
    handoff_by_field = {
        "tables": [
            {"schema": "named_snapshot", "name": name} for name in ("network_membership", "provider_location_binding")
        ]
    }
    loader = AsyncMock(return_value=handoff_by_field)
    monkeypatch.setattr(support, "load_named_network_preparation", loader)
    candidate_id = str(uuid4())
    if write_allowed:
        with pytest.raises(pytest.fail.Exception, match="DID NOT RAISE"):
            await fixture.assert_published_readonly(candidate_id)
        assert len(statements) == 1
    else:
        await fixture.assert_published_readonly(candidate_id)
        assert len(statements) == 28
        assert {statement.split()[0] for statement in statements} == {"UPDATE", "DELETE", "INSERT", "TRUNCATE"}
    loader.assert_awaited_once_with(connection, candidate_id, control_schema=fixture.control_schema)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["allow", "deny", "cancel"])
async def test_refresh_commit_http_does_not_require_source_locks(outcome):
    from tests import registry_membership_refresh_execution_support as support
    from tests.test_registry_membership_refresh_authority import envelope

    fixture = support.RefreshExecution(SimpleNamespace(admin=SimpleNamespace(kw={"bind": object()})), (None,) * 5)
    fixture._assert_source_fenced = AsyncMock()
    fixture.denied_commit, fixture.cancel_commit = outcome == "deny", outcome == "cancel"
    request = httpx.Request(
        "POST",
        "https://authority.example.invalid" + support.COMMIT_AUTHORITY_PATH,
        headers={"Authorization": "Bearer synthetic-refresh-only"},
        json=envelope(),
    )
    if outcome == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await fixture.http(request)
    else:
        reply = await fixture.http(request)
        assert reply.status_code == (403 if outcome == "deny" else 200)
    fixture._assert_source_fenced.assert_not_awaited()
    assert fixture.calls == [support.COMMIT_AUTHORITY_PATH]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("final CAS refused"), asyncio.CancelledError()])
async def test_refresh_source_probe_runs_only_inside_original_pin_and_releases(failure):
    from tests import registry_membership_refresh_execution_support as support

    fixture = support.RefreshExecution(SimpleNamespace(admin=SimpleNamespace(kw={"bind": object()})), (None,) * 5)
    connection, envelope, source_pin, final_sources = object(), object(), object(), object()
    events, state = [], SimpleNamespace(is_pinned=False)

    @asynccontextmanager
    async def pin(*args):
        assert args == (connection, envelope, source_pin)
        events.append("pin")
        state.is_pinned = True
        try:
            yield final_sources
        finally:
            state.is_pinned = False
            events.append("release")

    async def prove():
        assert state.is_pinned
        events.append("writer_refused")

    fixture.bundle = SimpleNamespace(runtime=SimpleNamespace(pin_sources=pin))
    fixture._assert_source_fenced = prove

    async def finish():
        async with fixture._pinned_sources(connection, envelope, source_pin) as sources:
            assert sources is final_sources and state.is_pinned
            events.append("final_CAS")
            if failure is not None:
                raise failure

    if failure is None:
        await finish()
    else:
        with pytest.raises(type(failure)) as caught:
            await finish()
        assert caught.value is failure
    assert events == ["pin", "writer_refused", "final_CAS"] + (
        ["writer_refused", "release"] if failure is None else ["release"]
    )
    assert state.is_pinned is False


@pytest.mark.asyncio
async def test_refresh_consumer_wraps_final_pins_without_changing_server_runtime(monkeypatch):
    from tests import registry_membership_refresh_execution_support as support
    from tests.test_registry_membership_refresh_authority import client

    async with client(lambda _request: pytest.fail("configuration attempted HTTP")) as authorize:
        original = _runtime(authorize)
        fixture = support.RefreshExecution(
            SimpleNamespace(
                admin=SimpleNamespace(kw={"bind": object()}), names=SimpleNamespace(reader="source_reader")
            ),
            (None, None, "registry_control", None, []),
        )
        fixture.roles_by_kind = {
            name: "refresh_" + name
            for name in ("queue", "physical", "custody", "api", "lifecycle", "execution", "custodian")
        }
        fixture.preparation_roles = object()
        fixture.bundle = SimpleNamespace(runtime=original)
        fixture.lifecycle_pool, fixture.execution_pool = object(), object()
        consume = AsyncMock(return_value=None)
        monkeypatch.setattr(support, "consume_registry_publication_once", consume)
        assert await fixture.consume(str(uuid4())) is None
        selected = consume.await_args.kwargs["config"]
        assert selected.membership_refresh_runtime.pin_sources == fixture._pinned_sources
        assert selected.membership_refresh_runtime.authorize is original.authorize
        assert selected.membership_refresh_runtime.resolve_sources is original.resolve_sources
        assert selected.preparation_roles is fixture.preparation_roles
        assert fixture.bundle.runtime is original and original.pin_sources is not fixture._pinned_sources
