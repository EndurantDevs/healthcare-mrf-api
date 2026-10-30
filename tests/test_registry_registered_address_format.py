# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Component replay-format checks; doubles do not establish native Source authority."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_candidate_custody as custody
from process import registry_candidate_composition as composition
from process import registry_composition_resume as resume
from process import registry_publication_worker as worker
from process.network_address_projection import PinnedAddressSource
from tests.test_registry_publication_address_preparation import _PreparationConnection, _settings, _sources


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "inventory,physical,requested,recognized,refuses",
    [
        (None, None, False, False, False),
        (False, False, False, True, False),
        (False, True, True, False, False),
        (True, True, True, False, False),
        (False, False, True, True, True),
        (False, True, False, False, True),
    ],
)
async def test_composer_preserves_registered_policy_and_refuses_preparation_races(
    monkeypatch, inventory, physical, requested, recognized, refuses
):
    connection, request = object(), UUID(int=8)
    source_pin = PinnedAddressSource("address_fixture", "entity_address_unified", "a" * 64)
    source_descriptors = composition.RegistryCompositionAddressSources(source_pin, compact=requested)
    default_inputs, legacy_inputs = (source_pin, ("default_reader",)), (source_pin, ("legacy_reader",))
    approved = SimpleNamespace(approved_revision=3)
    inputs = composition._CompositionInputs(connection, None, approved, None, None, {}, default_inputs)
    monkeypatch.setattr(composition, "_composition_inputs", AsyncMock(return_value=inputs))
    monkeypatch.setattr(composition, "resolve_registered_source_inventory", AsyncMock(return_value=inventory))
    format_pin = AsyncMock(return_value=physical)
    legacy_pin = AsyncMock(return_value=legacy_inputs if recognized else None)
    monkeypatch.setattr(composition, "resolve_registered_compact_format", format_pin)
    monkeypatch.setattr(composition, "resolve_registered_legacy_office_inputs", legacy_pin)
    monkeypatch.setattr(resume, "resolve_registered_verified_office_inputs", AsyncMock(return_value=None))
    prepare = AsyncMock(return_value=(({}, 0, (), object()), None))
    monkeypatch.setattr(composition, "_prepare_composition_sources_for_format", prepare)
    monkeypatch.setattr(composition, "_composition_retention", AsyncMock(return_value=(object(), {})))
    monkeypatch.setattr(composition, "registry_source_retention_sha256", lambda _: "b" * 64)
    register = AsyncMock(return_value="prepared")
    monkeypatch.setattr(composition, "_register_composition_snapshot", register)
    operation = composition._prepare_composition_snapshot(
        connection, request, 3, 0, source_descriptors, None, {}, "registry"
    )
    if refuses:
        with pytest.raises(composition.RegistryCompositionError, match="prepared_format_changed"):
            await operation
        prepare.assert_not_awaited()
        legacy_pin.assert_not_awaited()
        register.assert_not_awaited()
        return
    assert await operation == "prepared"
    selected = prepare.await_args.args[0].verified_office_inputs
    assert prepare.await_args.kwargs["inventory"] is (requested if inventory is None else inventory)
    assert selected is (default_inputs if inventory is None else legacy_inputs if recognized else None)
    assert register.await_args.args[3] is source_descriptors
    if inventory is None:
        format_pin.assert_not_awaited()
        legacy_pin.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "registered,requested,expected",
    [(None, True, True), (False, True, False), (True, True, True), (True, False, None)],
)
async def test_qualified_preparer_freezes_format_before_preparing_addresses(
    monkeypatch, registered, requested, expected
):
    settings, connection = _settings(1), _PreparationConnection()
    identity_by_field = {
        "schema_owner_role": settings.consumer.queue_owner_role,
        "custody_owner_role": "registry_custody_owner",
    }
    source_descriptors = replace(_sources(), compact=requested)
    format_pin = AsyncMock(return_value=registered)
    monkeypatch.setattr(resume, "resolve_registered_compact_format", format_pin)
    monkeypatch.setattr(worker, "_custody_identity", AsyncMock(return_value=identity_by_field))
    monkeypatch.setattr(custody, "_q_reach_closed", AsyncMock())
    prepared_sources = []

    async def prepare(actual, **options):
        assert actual is connection and connection.active
        prepared_sources.append(options["address_sources"])
        assert options["address_sources"].compact is expected
        return None

    monkeypatch.setattr(composition, "prepare_registry_composition_address_artifact", prepare)

    @asynccontextmanager
    async def acquire():
        yield connection

    callback = worker._address_preparer(
        settings, settings.consumer, SimpleNamespace(acquire=acquire), identity_by_field, "custodian_login"
    )
    request = UUID(int=9)
    operation = callback(
        request_id=request,
        approved_revision=4,
        source_manifest=None,
        expected_head=0,
        address_sources=source_descriptors,
    )
    if expected is None:
        with pytest.raises(ValueError, match="registered_compact_format_required"):
            await operation
        assert not prepared_sources
        assert not connection.committed
    else:
        operation_result = await operation
        assert operation_result is prepared_sources[0] and operation_result.compact is expected
        assert connection.committed
    format_pin.assert_awaited_once_with(connection, request, 4, 0, control_schema="registry")
