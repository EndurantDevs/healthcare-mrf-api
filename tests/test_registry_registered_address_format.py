# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Component replay-format checks; doubles do not establish native Source authority."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_candidate_composition as composition
from process import registry_composition_resume as resume
from process.network_address_projection import PinnedAddressSource


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "inventory,physical,requested,recognized,refuses",
    [
        (None, None, False, False, False),
        (None, None, True, False, False),
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
