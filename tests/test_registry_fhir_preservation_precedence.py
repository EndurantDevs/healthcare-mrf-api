# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Approved office precedence never changes the authenticated historical receipt."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_custom_address_source as custom
from process import registry_fhir_serving_preservation as preservation
from process import registry_membership_composition_stage as commands


@pytest.mark.parametrize("context", [(True, "registry"), (-1, "registry"), (2**63, "registry"), (1,), [1, "registry"]])
def test_projection_rejects_unbounded_or_ambiguous_approval_context(context):
    proof = SimpleNamespace(address_query="SELECT * FROM old_addresses", binding_query="SELECT * FROM old_bindings")
    with pytest.raises(preservation.FHIRServingPreservationError):
        preservation._effective_carry_queries(proof, context)


def test_raw_scratch_projection_stays_exact_and_approved_projection_keeps_native_binds():
    proof = SimpleNamespace(
        address_query="SELECT * FROM old_addresses WHERE epoch=$1", binding_query="SELECT * FROM old_bindings"
    )
    assert preservation._effective_carry_queries(proof, None) == (proof.address_query, proof.binding_query)
    addresses, bindings = preservation._effective_carry_queries(proof, (4, "registry"))
    assert "epoch=$1" in addresses and "approved_revision=4" in addresses
    assert "custom.location_key=old.location_key" in addresses
    assert "LEFT JOIN custom USING(provider_system,provider_id,location_id)" in bindings
    assert "coalesce(custom.location_key,expected.location_key)" in bindings


@pytest.mark.asyncio
@pytest.mark.parametrize("writing", [False, True])
async def test_custom_artifact_write_and_replay_supply_the_same_approved_context(monkeypatch, writing):
    carry = AsyncMock(return_value="digest")
    monkeypatch.setattr(preservation, "carry_fhir_preservation_addresses", carry)
    proof = object()
    recipe = SimpleNamespace(approved_revision=4, control_schema="registry")
    assert (
        await custom._fhir_address_carry(object(), ((), None, proof), recipe, "candidate", writing=writing) == "digest"
    )
    assert carry.await_args.args[1:] == (proof, "candidate")
    assert carry.await_args.kwargs == {"writing": writing, "approved_context": (4, "registry")}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError("invalid overlay"), asyncio.CancelledError()])
async def test_legacy_overlay_cleanup_preserves_the_original_refusal_or_cancellation(monkeypatch, failure):
    monkeypatch.setattr(commands, "stage_approved_membership_commands", AsyncMock(return_value=object()))
    monkeypatch.setattr(commands, "verify_registry_composition_rows", AsyncMock(side_effect=failure))
    cleanup_failure = RuntimeError("drop failed")
    monkeypatch.setattr(commands, "drop_registry_command_stage", AsyncMock(side_effect=cleanup_failure))
    with pytest.raises(type(failure)) as observed:
        await preservation._verify_current_overlay(object(), object(), {}, object(), "registry")
    assert observed.value is failure
    assert observed.value._registry_preservation_cleanup_error is cleanup_failure
