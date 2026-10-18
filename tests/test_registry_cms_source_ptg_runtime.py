# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed optional PTG wiring and real adapter dispatch without database acceptance claims."""

import asyncio
import json
from contextlib import AsyncExitStack
from dataclasses import fields, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from process import registry_publication_ptg_runtime as adapter
from process import registry_publication_worker as generic_worker
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_source_recipe_composition import _require_recipe_custody
from tests import test_registry_cms_source_consumer as consumer_support
from tests import test_registry_cms_source_worker as worker_support
from tests.test_registry_ptg_office_membership_recipe import _recipe as ptg_recipe
from tests.test_registry_publication_worker import _observe_ptg_engine

consumer, worker = consumer_support.consumer, worker_support.worker


def configuration():
    configured = worker_support.configuration()
    configured["ptg"] = {
        "owner_role": "ptg_scope_owner",
        "approval_role": "ptg_scope_approver",
        "read_limits": {"maximum_bytes": 1048576, "maximum_pages": 256, "maximum_coordinates": 4096},
    }
    return configured


def parsed():
    return worker.parse_registry_cms_source_worker_config(json.dumps(configuration()))


@pytest.mark.parametrize("damage", ["null", "unknown", "dsn", "owner", "same_roles", "bool", "zero", "limits"])
def test_operator_ptg_config_is_closed_and_bounded(damage):
    configured = configuration()
    if damage == "null":
        configured["ptg"] = None
    elif damage in {"unknown", "dsn"}:
        configured["ptg"][damage] = "synthetic-sensitive-marker"
    elif damage == "owner":
        configured["ptg"]["owner_role"] = "invalid role"
    elif damage == "same_roles":
        configured["ptg"]["approval_role"] = configured["ptg"]["owner_role"]
    else:
        configured["ptg"]["read_limits"]["extra" if damage == "limits" else "maximum_bytes"] = (
            True if damage == "bool" else 0
        )
    with pytest.raises(ValueError, match="registry_cms_source_worker_config_invalid"):
        worker.parse_registry_cms_source_worker_config(json.dumps(configured))


def test_old_positional_contracts_and_direct_ptg_validation():
    original = worker_support.parsed()
    assert (
        worker.RegistryCMSSourceWorkerConfig(*(getattr(original, item.name) for item in fields(original)[:-1])).ptg
        is None
    )
    old = original.consumer
    assert (
        consumer.RegistryCMSSourceConsumerConfig(
            *(getattr(old, item.name) for item in fields(old)[:-1])
        ).composition_runtime
        is None
    )
    config = parsed()
    assert config.ptg.store.control_schema == config.consumer.control_schema
    assert config.ptg.read_budget.maximum_bytes == 1048576
    for changed in [
        object(),
        replace(config.ptg, store=object()),
        replace(config.ptg, store=replace(config.ptg.store, control_schema="other")),
    ]:
        with pytest.raises(ValueError):
            replace(config, ptg=changed)
    config.ptg.read_budget.maximum_bytes = True
    with pytest.raises(ValueError):
        replace(config, ptg=config.ptg)
    with pytest.raises(ValueError, match="registry_cms_source_consumer_invalid"):
        replace(old, composition_runtime=object())


def _attempt_inputs(monkeypatch):
    connection = consumer_support._PhaseConnection()
    notice, old, retained_ptg = consumer_support._notice(), consumer_support._recipe("old", "old-edition"), ptg_recipe()
    pin = SimpleNamespace(generation_id=9, manifest_sha256="d" * 64)
    approved = SimpleNamespace(generation_id="e" * 64)
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=pin))
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=approved))
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", AsyncMock(return_value=1))
    monkeypatch.setattr(
        consumer, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(old, retained_ptg))
    )
    return connection, notice, retained_ptg, pin


@pytest.mark.asyncio
async def test_mixed_attempt_dispatches_real_adapter_after_snapshot_and_keeps_raw_publish(monkeypatch):
    connection, notice, retained_ptg, pin = _attempt_inputs(monkeypatch)
    async with AsyncExitStack() as stack:
        configured = generic_worker._ptg_runtime(parsed(), stack)
        config = replace(configured, coordinates=notice.pair.recipe.binding_coordinates)
        runtime = config.composition_runtime
        identity = AsyncMock()
        monkeypatch.setattr(adapter.RegistryPublicationPTGRuntime, "_same_execution_database", identity)
        factory_calls = []

        async def compose(sessions, **arguments):
            assert not connection.active and type(sessions) is async_sessionmaker
            assert sessions is runtime.sessions
            supplied = arguments["address_sources"]
            assert type(supplied) is RegistryCompositionAddressSources
            assert consumer_support.canonical_registry_source_recipes(supplied.replacement_source_recipes) == (
                consumer_support.canonical_registry_source_recipes((notice.pair.recipe, retained_ptg))
            )
            assert supplied.office_scope_store is runtime.store
            assert supplied.office_read_budget is not runtime.read_budget
            assert supplied.office_read_budget.maximum_bytes == runtime.read_budget.maximum_bytes
            assert arguments["expected_head"] == pin.generation_id and arguments["approved_revision"] == 1
            factory_calls.append(arguments)
            return consumer._target(arguments["request_id"], 1, pin.generation_id, {}), notice.pair.address_source

        async def prepare(raw, *arguments, **keywords):
            assert raw is connection and not raw.active

        async def publish(raw, *arguments, **keywords):
            assert raw is connection and raw.active and raw.checkpoints
            return {"published": True}

        monkeypatch.setattr(adapter, "compose_registry_membership_candidate", compose)
        monkeypatch.setattr(
            consumer, "compose_registry_membership_candidate", AsyncMock(side_effect=AssertionError("raw path"))
        )
        monkeypatch.setattr(consumer, "prepare_network_candidate", prepare)
        monkeypatch.setattr(consumer, "publish_network_candidate", publish)
        publication = await consumer._attempt(
            connection, notice, consumer._event(notice, config), config, "endpoint-one"
        )
        assert publication["state"] == "completed" and not connection.active and len(factory_calls) == 1
        identity.assert_awaited_once_with(connection, config.control_schema)
        connection.active = True
        with pytest.raises(ValueError, match="registry_publication_ptg_runtime_invalid"):
            await runtime.compose(
                connection,
                address_sources=RegistryCompositionAddressSources(notice.pair.address_source),
                control_schema=config.control_schema,
            )
        identity.assert_awaited_once()


@pytest.mark.asyncio
async def test_missing_runtime_keeps_ptg_owner_refusal_before_prepare_or_publish(monkeypatch):
    connection, notice, retained_ptg, _pin = _attempt_inputs(monkeypatch)
    config = replace(consumer_support._config(), coordinates=notice.pair.recipe.binding_coordinates)

    async def compose(raw, **arguments):
        assert not raw.active
        await _require_recipe_custody(raw, retained_ptg)
        raise AssertionError("PTG source without owner must refuse")

    prepare, publish = AsyncMock(), AsyncMock()
    monkeypatch.setattr(consumer, "compose_registry_membership_candidate", compose)
    monkeypatch.setattr(consumer, "prepare_network_candidate", prepare)
    monkeypatch.setattr(consumer, "publish_network_candidate", publish)
    with pytest.raises(ValueError, match="registry_ptg_office_recipe_owner_unavailable"):
        await consumer._attempt(connection, notice, consumer._event(notice, config), config, "endpoint-one")
    prepare.assert_not_awaited()
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["completed", "error", "cancel", "qualification"])
async def test_worker_qualifies_genuine_execution_factory_and_disposes_it_after_drain(monkeypatch, outcome):
    actual_listen = worker.event.listen
    events, options, _verify, sessions = worker_support.resources(monkeypatch)
    reader_listen = worker.event.listen

    def listen(target, name, callback, **options):
        if target is worker_support.Engine.sync_engine:
            return reader_listen(target, name, callback)
        return actual_listen(target, name, callback, **options)

    monkeypatch.setattr(worker.event, "listen", listen)
    config = parsed()
    engines = _observe_ptg_engine(monkeypatch, config, events, "{synthetic-password}")

    async def qualify(runtime):
        assert type(runtime) is adapter.RegistryPublicationPTGRuntime
        assert runtime.sessions.kw["bind"] is engines[0]
        assert engines[0].pool.size() == 1 and engines[0].pool._max_overflow == 0
        events.append("ptg qualify")
        if outcome == "qualification":
            raise RuntimeError("synthetic qualification refusal")

    async def consume(**arguments):
        runtime = arguments["config"].composition_runtime
        assert type(runtime) is adapter.RegistryPublicationPTGRuntime
        assert arguments["notification_sessions"] is sessions
        assert runtime.store is config.ptg.store and runtime.sql_limits is config.sql_limits
        assert not arguments["connection"].is_in_transaction()
        events.append("consume")
        try:
            if outcome == "error":
                raise RuntimeError("synthetic refusal")
            if outcome == "cancel":
                raise asyncio.CancelledError
            return {"state": "completed"}
        finally:
            events.append("consumer drained")

    monkeypatch.setattr(adapter.RegistryPublicationPTGRuntime, "qualify", qualify)
    monkeypatch.setattr(worker, "consume_registry_cms_source_notification_once", consume)
    if outcome == "completed":
        assert await worker.run_registry_cms_source_worker(config) == {"state": "completed"}
    else:
        with pytest.raises(RuntimeError if outcome in {"error", "qualification"} else asyncio.CancelledError):
            await worker.run_registry_cms_source_worker(config)
    if outcome == "qualification":
        assert "reader connect" not in events and "consume" not in events
        assert (
            events.index("ptg qualify")
            < events.index("release")
            < events.index("sqlalchemy close")
            < events.index("pool close")
        )
        return
    assert events.index("ptg qualify") < events.index("reader connect") < events.index("consume")
    assert events.index("consumer drained") < events.index("release") < events.index("reader dispose")
    assert events.index("reader dispose") < events.index("sqlalchemy close") < events.index("pool close")
    assert options["pool"]["max_size"] == 1
    assert options["engine"]["connect_args"]["server_settings"]["default_transaction_read_only"] == "on"


@pytest.mark.asyncio
async def test_configured_runtime_does_not_move_published_replay_after_freshness(monkeypatch):
    notice = consumer_support._notice()
    async with AsyncExitStack() as stack:
        config = generic_worker._ptg_runtime(parsed(), stack)
        config = replace(config, coordinates=notice.pair.recipe.binding_coordinates)
        monkeypatch.setattr(consumer, "_notification", AsyncMock(return_value=notice))
        monkeypatch.setattr(consumer, "_periodic_config", AsyncMock(return_value=(config, ["saved-candidate"])))
        replay = AsyncMock(return_value={"state": "replayed"})
        current, attempt = AsyncMock(), AsyncMock()
        monkeypatch.setattr(consumer, "_replay", replay)
        monkeypatch.setattr(consumer, "_current_source", current)
        monkeypatch.setattr(consumer, "_attempt", attempt)
        result = await consumer._consume(
            object(), object(), object(), "source", "receipt", "digest", config, "endpoint-one"
        )
        assert result == {"state": "replayed"}
        replay.assert_awaited_once()
        current.assert_not_awaited()
        attempt.assert_not_awaited()


@pytest.mark.asyncio
async def test_unapproved_new_mapping_never_reaches_configured_ptg_runtime(monkeypatch):
    connection, notice, _retained_ptg, _pin = _attempt_inputs(monkeypatch)
    async with AsyncExitStack() as stack:
        config = generic_worker._ptg_runtime(parsed(), stack)
        config = replace(config, coordinates=notice.pair.recipe.binding_coordinates)
        compose = AsyncMock(side_effect=AssertionError("unapproved mapping cannot compose"))
        monkeypatch.setattr(adapter.RegistryPublicationPTGRuntime, "compose", compose)
        monkeypatch.setattr(consumer, "require_approved_network_source_bindings", AsyncMock(return_value=0))
        result = await consumer._attempt(connection, notice, consumer._event(notice, config), config, "endpoint-one")
        assert result["state"] == "review_required" and not connection.active
        compose.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_refuses_external_runtime_before_owning_resources():
    config = parsed()
    async with AsyncExitStack() as stack:
        supplied_consumer = generic_worker._ptg_runtime(config, stack)
        with pytest.raises(ValueError, match="registry_cms_source_worker_config_invalid"):
            replace(config, consumer=supplied_consumer)


@pytest.mark.asyncio
async def test_runtime_identity_refusal_has_no_raw_composer_fallback(monkeypatch):
    connection, notice, _retained_ptg, _pin = _attempt_inputs(monkeypatch)
    async with AsyncExitStack() as stack:
        config = generic_worker._ptg_runtime(parsed(), stack)
        config = replace(config, coordinates=notice.pair.recipe.binding_coordinates)
        identity = AsyncMock(side_effect=ValueError("registry_publication_ptg_execution_changed"))
        raw_compose, prepare = AsyncMock(), AsyncMock()
        monkeypatch.setattr(adapter.RegistryPublicationPTGRuntime, "_same_execution_database", identity)
        monkeypatch.setattr(consumer, "compose_registry_membership_candidate", raw_compose)
        monkeypatch.setattr(consumer, "prepare_network_candidate", prepare)
        with pytest.raises(ValueError, match="registry_publication_ptg_execution_changed"):
            await consumer._attempt(connection, notice, consumer._event(notice, config), config, "endpoint-one")
        assert not connection.active
        raw_compose.assert_not_awaited()
        prepare.assert_not_awaited()
