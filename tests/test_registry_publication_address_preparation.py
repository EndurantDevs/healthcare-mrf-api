# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named preparation, queue authorization, resource budgets and rollback scope."""

import asyncio
import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import registry_candidate_composition as composition
from process import registry_publication_consumer as consumer
from process import registry_publication_execution as execution
from process import registry_publication_worker as worker
from process.network_address_projection import PinnedAddressSource
from process.registry_approval_store import RegistryApprovalConflict
from tests.test_registry_publication_worker import _CandidateSealConnection, configuration
from tests.test_registry_publication_worker import isolated_ptg_runtime as isolated_ptg_runtime


def _settings(reader_count=None):
    configured = configuration()
    if reader_count is not None:
        configured["consumer"]["reader_roles"] = [f"registry_reader_{index:03d}" for index in range(reader_count)]
    return worker.parse_registry_publication_worker_config(json.dumps(configured))


def _sources():
    return composition.RegistryCompositionAddressSources(
        PinnedAddressSource("retained_base", "entity_address_unified", "a" * 64)
    )


def test_address_callback_is_private_callable_configuration():
    settings = _settings()
    callback = AsyncMock()
    assert replace(settings.consumer, prepare_address_sources=callback).prepare_address_sources is callback
    assert "AsyncMock" not in repr(replace(settings.consumer, prepare_address_sources=callback))
    with pytest.raises(ValueError, match="consumer_config_invalid"):
        replace(settings.consumer, prepare_address_sources={})
    configured = configuration()
    configured["consumer"]["prepare_address_sources"] = {}
    with pytest.raises(ValueError, match="worker_config_invalid"):
        worker.parse_registry_publication_worker_config(json.dumps(configured))


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["legacy", "complete", "prepare_failure", "lease", "revoked"])
async def test_address_preparation_rechecks_lease_and_authority_before_composition(monkeypatch, outcome):
    events = []
    address_sources, prepared = _sources(), _sources()
    source_pin = object()
    request_by_name = {
        "request_id": uuid4(),
        "lease_id": uuid4(),
        "request_sha256": "a" * 64,
        "command_json": {"expected_serving_generation": 7},
    }

    async def prepare(**options):
        assert options == dict(
            request_id=request_by_name["request_id"],
            approved_revision=4,
            source_manifest=source_pin,
            expected_head=7,
            address_sources=address_sources,
        )
        events.append("prepare")
        if outcome == "prepare_failure":
            raise ValueError("preparation_refused")
        return prepared

    async def request(*_args):
        events.append("lease")
        if outcome == "lease" and events.count("lease") == 2:
            raise RegistryApprovalConflict("registry_queue_lease_conflict")
        return request_by_name

    async def authorize(*_args):
        events.append("authority")
        if outcome == "revoked" and events.count("authority") == 2:
            raise PermissionError("authority_denied")

    monkeypatch.setattr(consumer, "_request", request)
    monkeypatch.setattr(consumer, "_fresh_authority", authorize)
    config = replace(_settings().consumer, prepare_address_sources=None if outcome == "legacy" else prepare)
    operation = consumer._prepared_address_sources(
        object(), config, request_by_name, 4, source_pin, address_sources, object()
    )
    if outcome in {"legacy", "complete"}:
        assert await operation is (address_sources if outcome == "legacy" else prepared)
        assert events == ([] if outcome == "legacy" else ["lease", "authority", "prepare", "lease", "authority"])
    else:
        with pytest.raises(PermissionError if outcome == "revoked" else ValueError):
            await operation
        assert (
            events
            == {
                "prepare_failure": ["lease", "authority", "prepare"],
                "lease": ["lease", "authority", "prepare", "lease"],
                "revoked": ["lease", "authority", "prepare", "lease", "authority"],
            }[outcome]
        )


def _named_settings():
    settings = _settings()
    return replace(
        settings,
        preparation_roles={
            "namespace_owner_role": "frozen_snapshot_owner",
            "publisher_role": settings.consumer.execution_role,
            "application_roles": list(worker._application_role_names(settings)),
        },
    )


class _PreparationPool:
    def __init__(self):
        self.connection = object()
        self.active = False
        self.closed = False
        self.timeouts = []

    @asynccontextmanager
    async def acquire(self, *, timeout):
        assert not self.active
        self.timeouts.append(timeout)
        self.active = True
        try:
            yield self.connection
        finally:
            self.active = False

    async def close(self):
        assert not self.active
        self.closed = True


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError("invalid source rows"), asyncio.CancelledError()])
async def test_configured_runtime_uses_bounded_named_preparation_and_cleans_pool(monkeypatch, failure):
    from contextlib import AsyncExitStack

    from process import network_membership_publication as publication
    from tests.test_named_network_preparation import TARGET

    settings = _named_settings()
    pool = _PreparationPool()
    create = AsyncMock(return_value=pool)
    marker = object()
    validator = AsyncMock(return_value=marker, side_effect=failure)
    monkeypatch.setattr(worker.asyncpg, "create_pool", create)
    monkeypatch.setattr(publication, "validate_named_network_preparation", validator)
    async with AsyncExitStack() as stack:
        runtime = await worker._configured_worker_runtime(settings, stack)
        assert runtime.prepare_address_sources is None
        assert runtime.preparation_roles == settings.preparation_roles
        if failure is None:
            assert await runtime.seal_candidate(TARGET) is marker
        else:
            with pytest.raises(type(failure)) as observed:
                await runtime.seal_candidate(TARGET)
            assert observed.value is failure
        validator.assert_awaited_once_with(
            pool.connection,
            TARGET,
            control_schema=settings.consumer.control_schema,
            roles=settings.preparation_roles,
            source_witness_runtime=None,
            validation_snapshot=None,
        )
        assert not pool.active and not pool.closed
    assert pool.closed and pool.timeouts == [settings.sql_limits.connect_timeout_seconds]
    options = create.await_args.kwargs
    assert options["min_size"] == options["max_size"] == 1
    assert options["user"] == settings.consumer.execution_role
    assert options["server_settings"] == settings.sql_limits.server_settings()
    assert options["init"] == options["setup"] == settings.sql_limits.verify


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError, asyncio.CancelledError], ids=["held", "lost", "cancelled"])
async def test_ptg_preparation_retains_client_scope_driver_and_fresh_budget(monkeypatch, failure, isolated_ptg_runtime):
    from process import registry_publication_ptg_runtime as runtime_module

    runtime = isolated_ptg_runtime
    driver = object()
    raw = SimpleNamespace(driver_connection=driver)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw))
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    events = []

    @asynccontextmanager
    async def transaction(factory, **options):
        assert factory is runtime.sessions and options == {"control_schema": runtime.store.control_schema}
        events.append("begin_snapshot")
        try:
            yield session
        finally:
            events.append("release_snapshot")

    async def require(actual, schema):
        assert actual is session and schema == runtime.store.control_schema
        session.connection.assert_not_awaited()
        events.append("client_scope")
        if failure is not None:
            raise failure

    monkeypatch.setattr(runtime_module, "registry_company_approval_transaction", transaction)
    monkeypatch.setattr(runtime_module, "require_registry_company_approval_fence", require)
    if failure is not None:
        with pytest.raises(failure):
            async with runtime.validation_snapshot():
                pytest.fail("Refused client scope reached data access")
        session.connection.assert_not_awaited()
    else:
        async with runtime.validation_snapshot() as (actual, authority):
            assert actual is driver and authority[:2] == (session, runtime.store)
            budget = authority[2]
            assert budget is not runtime.read_budget
            assert tuple(getattr(budget, name) for name in runtime_module._LIMIT_FIELDS) == runtime._limits
            assert events == ["begin_snapshot", "client_scope"]
    assert events == ["begin_snapshot", "client_scope", "release_snapshot"]


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["legacy", "complete", "prepare_failure", "revoked", "lease", "digest"])
async def test_retained_operation_preparation_fences_exact_request_and_purpose(monkeypatch, outcome):
    events = []
    address_sources, prepared = _sources(), _sources()
    source_pin = object()
    request_by_name = {
        "request_id": uuid4(),
        "lease_id": uuid4(),
        "request_sha256": "a" * 64,
        "command_json": {"expected_serving_generation": 7, "expected_approved_revision": 4},
    }

    async def request(*_args):
        events.append("lease")
        if outcome == "lease" and events.count("lease") == 2:
            raise RegistryApprovalConflict("registry_queue_lease_conflict")
        return {**request_by_name, "request_sha256": "b" * 64} if outcome == "digest" else request_by_name

    async def authority(_authorize, actual, operation):
        assert actual == request_by_name and operation == "prepare_source_rollback"
        events.append("authority")
        if outcome == "revoked" and events.count("lease") == 2:
            raise PermissionError("authority_denied")

    async def prepare(**options):
        assert options == dict(
            request_id=request_by_name["request_id"],
            approved_revision=4,
            source_manifest=source_pin,
            expected_head=7,
            address_sources=address_sources,
        )
        events.append("prepare")
        if outcome == "prepare_failure":
            raise ValueError("preparation_refused")
        return prepared

    monkeypatch.setattr(execution, "_request", request)
    monkeypatch.setattr(execution, "_fresh_operation_authority", authority)
    operation = execution._prepared_operation_addresses(
        object(),
        request_by_name,
        source_pin,
        address_sources,
        None if outcome == "legacy" else prepare,
        object(),
        "registry",
        "prepare_source_rollback",
    )
    if outcome in {"legacy", "complete"}:
        assert await operation is (address_sources if outcome == "legacy" else prepared)
        assert events == ([] if outcome == "legacy" else ["lease", "authority", "prepare", "lease", "authority"])
    else:
        with pytest.raises(PermissionError if outcome == "revoked" else ValueError):
            await operation
        assert ("prepare" in events) == (outcome != "digest")


@pytest.mark.asyncio
@pytest.mark.parametrize("callbacks", [{"unknown": None}, {"prepare_address_sources": {}}])
async def test_rollback_preparation_rejects_unknown_or_noncallable_callbacks(callbacks):
    with pytest.raises(ValueError, match="registry_publication_address_preparation_invalid"):
        await execution.complete_queued_registry_source_rollback_preview(
            SimpleNamespace(is_in_transaction=lambda: False),
            uuid4(),
            uuid4(),
            authorize=object(),
            writer_roles={},
            **callbacks,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["prepare_source_rollback", "publish_source_rollback"])
@pytest.mark.parametrize("configured_runtime", [False, True])
async def test_rollback_dispatch_uses_only_server_runtime(monkeypatch, operation, configured_runtime):
    from tests.test_registry_source_witness_worker import _runtime

    runtime, untrusted = _runtime(), object()
    address_sources = replace(_sources(), source_witness_runtime=runtime) if configured_runtime else None
    connection, authorize = object(), AsyncMock()
    config = _settings().consumer
    request_by_field = {
        "request_id": uuid4(),
        "lease_id": uuid4(),
        "command_json": {"operation": operation, "source_witness_runtime": untrusted},
        "source_witness_runtime": untrusted,
    }

    @asynccontextmanager
    async def acquire():
        yield connection

    preview, publish = AsyncMock(), AsyncMock()
    monkeypatch.setattr(consumer, "complete_queued_registry_source_rollback_preview", preview)
    monkeypatch.setattr(consumer, "complete_queued_registry_source_rollback_publication", publish)
    async with AsyncExitStack() as cleanup:
        await consumer._execute_publication_request(
            SimpleNamespace(acquire=acquire), config, request_by_field, authorize, address_sources, cleanup
        )
    expected_by_field = dict(
        authorize=authorize, control_schema=config.control_schema, writer_roles=config.writer_roles
    )
    if operation == "prepare_source_rollback":
        expected_by_field.update(composition_runtime=config.composition_runtime, seal_candidate=config.seal_candidate)
        if configured_runtime:
            expected_by_field["source_witness_runtime"] = runtime
        selected, unused = preview, publish
    else:
        expected_by_field["preparation_roles"] = config.preparation_roles
        selected, unused = publish, preview
    selected.assert_awaited_once_with(
        connection, request_by_field["request_id"], request_by_field["lease_id"], **expected_by_field
    )
    unused.assert_not_awaited()
    assert address_sources is None or address_sources.source_witness_runtime is runtime


def _bind_rollback_composition(
    monkeypatch, connection, retained, runtime, source_pin, request_by_field, copy_target, addresses
):
    events = []
    loader = AsyncMock(return_value=retained)
    monkeypatch.setattr(execution, "_source_rollback_address_sources", loader)

    def require_inputs(actual):
        assert actual == replace(retained, source_witness_runtime=runtime)
        assert actual.base_source is retained.base_source
        assert actual.source_witness_runtime is runtime
        loader.assert_awaited_once_with(connection, source_pin, "registry")
        assert not connection.active

    async def prepare_addresses(**options):
        require_inputs(options["address_sources"])
        assert options == dict(
            request_id=request_by_field["request_id"],
            approved_revision=4,
            source_manifest=source_pin,
            expected_head=7,
            address_sources=options["address_sources"],
        )
        events.append("addresses")
        return replace(options["address_sources"])

    async def compose(*args, **options):
        require_inputs(options["address_sources"])
        assert options["source_manifest"] is source_pin
        assert options["request_id"] == request_by_field["request_id"]
        assert options["approved_revision"] == 4 and options["expected_head"] == 7
        events.append("compose")
        return copy_target, addresses

    monkeypatch.setattr(execution, "compose_registry_membership_candidate", compose)
    return events, prepare_addresses


def _bind_rollback_preview(monkeypatch, connection, events):
    async def preview(*args):
        assert connection.active and events[-1] == "seal"
        return {"candidate_sha256": "b" * 64}

    monkeypatch.setattr(execution, "_source_rollback_preview_result", preview)
    finish = AsyncMock(return_value={"state": "completed"})
    monkeypatch.setattr(execution, "_finish_source_rollback_preview", finish)
    return finish


@pytest.mark.asyncio
@pytest.mark.parametrize("configured_runtime", [False, True])
@pytest.mark.parametrize("address_callback", [False, True])
async def test_rollback_preview_retains_authenticated_inputs_and_runtime_through_preparation(
    monkeypatch, configured_runtime, address_callback
):
    from tests.test_registry_source_witness_worker import _runtime

    connection = _CandidateSealConnection()
    runtime = _runtime() if configured_runtime else None
    retained = _sources()
    source_pin, current_pin, copy_target, addresses = object(), object(), object(), object()
    request_by_field = {
        "request_id": uuid4(),
        "lease_id": uuid4(),
        "request_sha256": "a" * 64,
        "command_json": {"expected_approved_revision": 4, "expected_serving_generation": 7},
    }
    command = SimpleNamespace(expected_approved_revision=4, expected_serving_generation=7)
    monkeypatch.setattr(execution, "_request", AsyncMock(return_value=request_by_field))
    monkeypatch.setattr(execution, "_fresh_retained_operation_authority", AsyncMock(return_value=command))
    monkeypatch.setattr(execution, "_fresh_operation_authority", AsyncMock())
    monkeypatch.setattr(execution, "_source_rollback_pins", AsyncMock(return_value=(source_pin, current_pin)))
    events, prepare_addresses = _bind_rollback_composition(
        monkeypatch, connection, retained, runtime, source_pin, request_by_field, copy_target, addresses
    )

    prepare = AsyncMock()
    monkeypatch.setattr(execution, "prepare_registry_publication_candidate", prepare)

    async def seal(actual):
        assert actual is copy_target and not connection.active
        assert events == (["addresses"] if address_callback else []) + ["compose"]
        prepare.assert_awaited_once_with(
            connection,
            copy_target,
            addresses,
            writer_roles={},
            control_schema="registry",
            composition_runtime=None,
            **({"source_witness_runtime": runtime} if runtime is not None else {}),
        )
        events.append("seal")

    finish = _bind_rollback_preview(monkeypatch, connection, events)
    assert await execution.complete_queued_registry_source_rollback_preview(
        connection,
        request_by_field["request_id"],
        request_by_field["lease_id"],
        authorize=object(),
        writer_roles={},
        control_schema="registry",
        source_witness_runtime=runtime,
        seal_candidate=seal,
        **({"prepare_address_sources": prepare_addresses} if address_callback else {}),
    ) == {"state": "completed"}
    assert retained.source_witness_runtime is None
    assert not connection.active
    finish.assert_awaited_once()


@pytest.mark.asyncio
async def test_rollback_preview_without_runtime_keeps_typed_source_failure_closed(monkeypatch):
    from process.registry_source_recipe_composition import (
        RegistrySourceCompositionError,
        _typed_source_read_scope,
    )

    connection = _CandidateSealConnection()
    request_by_field = {"request_id": uuid4(), "lease_id": uuid4()}
    command = SimpleNamespace(expected_approved_revision=4, expected_serving_generation=7)
    monkeypatch.setattr(execution, "_request", AsyncMock(return_value=request_by_field))
    monkeypatch.setattr(execution, "_fresh_retained_operation_authority", AsyncMock(return_value=command))
    monkeypatch.setattr(execution, "_source_rollback_pins", AsyncMock(return_value=(object(), object())))
    monkeypatch.setattr(execution, "_source_rollback_address_sources", AsyncMock(return_value=_sources()))

    async def compose(*args, **options):
        await _typed_source_read_scope(
            connection, object(), object(), "registry", options["address_sources"].source_witness_runtime, True
        )

    monkeypatch.setattr(execution, "compose_registry_membership_candidate", compose)
    prepare, seal, finish = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(execution, "prepare_registry_publication_candidate", prepare)
    monkeypatch.setattr(execution, "_finish_source_rollback_preview", finish)
    with pytest.raises(RegistrySourceCompositionError, match="registry_source_typed_runtime_unavailable"):
        await execution.complete_queued_registry_source_rollback_preview(
            connection,
            request_by_field["request_id"],
            request_by_field["lease_id"],
            authorize=object(),
            writer_roles={},
            control_schema="registry",
            seal_candidate=seal,
        )
    prepare.assert_not_awaited()
    seal.assert_not_awaited()
    finish.assert_not_awaited()
