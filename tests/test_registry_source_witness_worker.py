# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured Source data services pass through the normal named preparation path."""

import asyncio
from contextlib import AsyncExitStack
from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest

from process import network_membership_publication as publication
from process import registry_publication_worker as worker
from process.network_address_projection import PinnedAddressSource
from process.registry_composition_addresses import RegistryCompositionAddressSources
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile
from process.registry_source_recipe_composition import SourceWitnessRuntime
from tests.test_named_network_preparation import TARGET
from tests.test_registry_publication_address_preparation import _named_settings, _PreparationPool
from tests.test_registry_publication_worker import _runtime_configuration, ptg_configuration

_ACTORS = (
    "protected_control_owner",
    "registry_api",
    "registry_queue_owner",
    "registry_reader",
    "registry_snapshot_owner",
    "sealed_operator",
    "worker_execution",
    "worker_lifecycle",
)


def _runtime(roles=_ACTORS):
    return SourceWitnessRuntime(
        Mock(side_effect=AssertionError("Source must not open in a worker wiring test")),
        (FHIRCatalogSourceReadProfile("source_payload", "source_catalog"),),
        "capture_owner",
        tuple(sorted(roles)),
    )


def _sources(runtime):
    return RegistryCompositionAddressSources(
        PinnedAddressSource("address_source", "address_rows", "generation-one"),
        compact=True,
        source_witness_runtime=runtime,
    )


@pytest.mark.asyncio
async def test_preparation_forwards_configured_source_runtime_before_data_refusal(monkeypatch):
    runtime = _runtime(("source_reader",))
    sources = _sources(runtime)
    config = replace(_named_settings(), initial_address_sources=sources)
    pool = _PreparationPool()
    monkeypatch.setattr(worker.asyncpg, "create_pool", AsyncMock(return_value=pool))
    validate = AsyncMock(side_effect=ValueError("unit_source_data_refused"))
    monkeypatch.setattr(publication, "validate_named_network_preparation", validate)
    async with AsyncExitStack() as stack:
        consumer = await worker._configured_worker_runtime(config, stack)
        with pytest.raises(ValueError, match="unit_source_data_refused"):
            await consumer.seal_candidate(TARGET)
        assert validate.await_args.args == (pool.connection, TARGET)
        assert validate.await_args.kwargs["source_witness_runtime"] is runtime
        assert sources.source_witness_runtime is runtime
        assert not pool.active
    assert pool.closed
    runtime.source_export.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("with_snapshot", [False, True])
@pytest.mark.parametrize("failure", [None, ValueError("Source data refused"), asyncio.CancelledError()])
async def test_named_preparation_keeps_source_and_approval_services_through_every_exit(
    monkeypatch, with_snapshot, failure
):
    runtime = _runtime()
    settings = _runtime_configuration(ptg_configuration()) if with_snapshot else _named_settings()
    config = replace(settings, initial_address_sources=_sources(runtime))
    pool = _PreparationPool()
    monkeypatch.setattr(worker.asyncpg, "create_pool", AsyncMock(return_value=pool))
    marker = object()
    validate = AsyncMock(return_value=marker, side_effect=failure)
    monkeypatch.setattr(publication, "validate_named_network_preparation", validate)
    async with AsyncExitStack() as stack:
        consumer = await worker._configured_worker_runtime(config, stack)
        if failure is None:
            assert await consumer.seal_candidate(TARGET) is marker
        else:
            with pytest.raises(type(failure)) as observed:
                await consumer.seal_candidate(TARGET)
            assert observed.value is failure
        assert consumer.prepare_address_sources is None
        validate.assert_awaited_once_with(
            pool.connection,
            TARGET,
            control_schema=consumer.control_schema,
            roles=consumer.preparation_roles,
            source_witness_runtime=runtime,
            validation_snapshot=consumer.composition_runtime.validation_snapshot if with_snapshot else None,
        )
        if with_snapshot:
            assert consumer.composition_runtime.store.control_schema == consumer.control_schema
            assert consumer.composition_runtime.sessions.kw["bind"].pool.checkedout() == 0
        assert not pool.active and not pool.closed
    assert pool.closed and pool.timeouts == [config.sql_limits.connect_timeout_seconds]
    runtime.source_export.assert_not_called()
