# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail closed before native row transfer and exercise consumed exact-office dispatch."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from process import registry_ptg_office_membership as offices
from process import registry_source_recipe_composition as composition
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_membership_copy import MAX_INPUT_BYTES
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)


def _recipe():
    coordinates = RegistryNetworkSourceCoordinates(
        "ptg", "synthetic", "source_fixture", "dataset", "producer", "edition"
    )
    pin = PinnedPTGOfficeMembershipSource(
        str(UUID(int=1)), "synthetic-client", "a" * 64, "b" * 64, "c" * 64, coordinates, 71, 3
    )
    return RegistrySourceMembershipRecipe(pin, coordinates)


def test_closed_ptg_recipe_roundtrip_and_complete_scope():
    recipe = _recipe()
    assert decode_registry_source_recipes(canonical_registry_source_recipes((recipe,))) == (recipe,)
    with pytest.raises(ValueError):
        RegistrySourceMembershipRecipe(recipe.source_pin, replace(recipe.binding_coordinates, producer_id="other"))
    document = json.loads(canonical_registry_source_recipes((recipe,)))
    document[0]["source_pin"]["authority"] = True
    with pytest.raises(ValueError):
        decode_registry_source_recipes(json.dumps(document))


def _owner():
    driver = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"rows": 0, "bytes": 2}),
        fetch=AsyncMock(return_value=[]),
        is_in_transaction=lambda: True,
    )
    transaction = object()
    session = SimpleNamespace(in_transaction=lambda: True, get_transaction=lambda: transaction)
    approved = SimpleNamespace(approved_revision=4, generation_id="d" * 64)
    verified = offices._VerifiedOfficeSource(
        session,
        driver,
        transaction,
        _recipe().source_pin,
        object(),
        SimpleNamespace(input_row_count=0),
        SimpleNamespace(schema_name="office_fixture"),
        object(),
    )
    return driver, verified, approved


@pytest.mark.asyncio
async def test_page_bounds_precede_row_transfer(monkeypatch):
    driver, verified, approved = _owner()
    driver.fetchrow.return_value = {"rows": 1, "bytes": MAX_INPUT_BYTES + 1}
    monkeypatch.setattr(offices, "pin_approved_membership_source", AsyncMock(return_value=approved))
    with pytest.raises(offices.PTGOfficeBatchBoundsError):
        await offices.read_ptg_office_membership_batch(driver, verified, approved, None, 1000, "control_fixture")
    driver.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_census_does_not_hide_missing_offices(monkeypatch):
    driver, verified, approved = _owner()
    verified = replace(verified, request=SimpleNamespace(input_row_count=1))
    monkeypatch.setattr(offices, "pin_approved_membership_source", AsyncMock(return_value=approved))
    with pytest.raises(ValueError, match="page_changed"):
        await offices.read_ptg_office_membership_batch(driver, verified, approved, None, 1000, "control_fixture")


@pytest.mark.asyncio
async def test_native_owner_and_latest_approved_map_are_required(monkeypatch):
    driver, verified, approved = _owner()
    monkeypatch.setattr(offices, "pin_approved_membership_source", AsyncMock(return_value=object()))
    with pytest.raises(ValueError):
        await offices.read_ptg_office_membership_batch(driver, verified, approved, None, 1, "control_fixture")
    driver.fetchrow.assert_not_awaited()
    with pytest.raises(ValueError, match="owner_unavailable"):
        await composition._require_recipe_custody(driver, _recipe(), approved, "control_fixture", None)


@pytest.mark.asyncio
async def test_consumed_stager_selects_ptg_and_preserves_cancel(monkeypatch):
    driver, verified, approved = _owner()

    async def cancel(*args):
        raise BaseException("synthetic interruption")

    reader = AsyncMock(side_effect=cancel)
    monkeypatch.setattr(composition, "read_ptg_office_membership_batch", reader)
    monkeypatch.setattr(composition, "read_aca_membership_batch", AsyncMock())
    with pytest.raises(BaseException, match="synthetic interruption"):
        await composition._read_recipe_page(driver, _recipe(), approved, None, "control_fixture", 10, verified)
    reader.assert_awaited_once()
    composition.read_aca_membership_batch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "hold", "witness", "source_pin"])
async def test_consumed_proof_reaches_real_whole_office_witness(monkeypatch, changed):
    from process import network_serving_read, registry_ptg_office_approval
    from process.ptg_parts import result_archive_published_authority
    from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
    from process.registry_ptg_office_retention import office_hold_document
    from tests.test_registry_ptg_office_retention import _verified

    prepared, witness = await _verified(monkeypatch)
    pin = replace(
        _recipe().source_pin,
        capture_id=str(prepared.request.capture_id),
        manifest_sha256=prepared.descriptor.manifest_sha256,
    )
    source_pin_by_field = {"operation_id": "registry_ptg_office_review_" + prepared.request.capture_id.hex}
    document_by_field = {
        "command": {"source": {"approved_revision": 4}},
        "witness": witness.as_dict(),
        "source_pin": source_pin_by_field,
    }
    hold = office_hold_document(prepared.descriptor, witness, pin.approval_sha256)
    if changed == "hold":
        hold["schema_name"] = "other_fixture"
    if changed == "witness":
        document_by_field["witness"]["canonical_input_sha256"] = "0" * 64
    returned_pin = {**source_pin_by_field, "different": True} if changed == "source_pin" else source_pin_by_field
    monkeypatch.setattr(offices, "_retained_documents", AsyncMock(return_value=(document_by_field, hold)))
    monkeypatch.setattr(offices, "_recipe_context", AsyncMock(return_value=(prepared.context, prepared.request)))
    monkeypatch.setattr(registry_ptg_office_approval, "_descriptor", AsyncMock(return_value=prepared.descriptor))
    monkeypatch.setattr(
        result_archive_published_authority,
        "lock_ptg_published_result_for_clone",
        AsyncMock(return_value=SimpleNamespace(as_dict=lambda: returned_pin)),
    )
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", AsyncMock(return_value=object()))
    if changed:
        with pytest.raises(ValueError):
            await offices._verify_recipe_source(
                prepared.session,
                prepared.driver,
                pin,
                SimpleNamespace(approved_revision=4),
                object(),
                "synthetic_control",
                RegistryPTGGraphReadBudget(1048576),
                prepared.session.get_transaction(),
            )
    else:
        actual = await offices._verify_recipe_source(
            prepared.session,
            prepared.driver,
            pin,
            SimpleNamespace(approved_revision=4),
            object(),
            "synthetic_control",
            RegistryPTGGraphReadBudget(1048576),
            prepared.session.get_transaction(),
        )
        assert actual.descriptor is prepared.descriptor
        assert prepared.session.witness_calls[-1] == prepared.request.input_row_count


@pytest.mark.asyncio
async def test_office_binding_dispatch_uses_selected_tuple_and_real_copy(monkeypatch):
    from process import registry_ptg_office_bindings as bindings
    from process.network_address_projection import PinnedAddressSource
    from process.network_membership_copy import MembershipCopyTarget
    from process.registry_retained_site_adoption import RetainedSiteAdoption

    driver, verified, approved = _owner()
    adoption = RetainedSiteAdoption("npi", "1234567893", str(UUID(int=8)), "e" * 64, "npi", "1234567893", "f" * 64)
    batch = offices.PTGOfficeMembershipBatch(
        verified.pin, approved, verified, 1, 1, 0, 0, b"[]", "1" * 64, 1, (adoption,)
    )
    driver.execute = AsyncMock()
    driver.fetchval = AsyncMock(return_value=False)
    driver.copy_records_to_table = AsyncMock(return_value="COPY 1")
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    driver.transaction = lambda: transaction
    monkeypatch.setattr(bindings, "read_ptg_office_membership_batch", AsyncMock(return_value=batch))
    monkeypatch.setattr(bindings, "_source_identity", AsyncMock(return_value=(1, 2, 3, "a" * 64)))
    monkeypatch.setattr(bindings, "_candidate", AsyncMock())
    address = PinnedAddressSource("address_fixture", "entity_address_unified", "b" * 64)
    copy_target = MembershipCopyTarget(
        *(str(UUID(int=number)) for number in range(1, 5)), "network_candidate_" + UUID(int=4).hex
    )
    pin = await bindings.pin_ptg_office_address_source(
        driver, batch, address, runtime_roles=("reader_fixture",), control_schema="control_fixture"
    )
    receipt = await bindings.copy_ptg_office_bindings(
        driver, batch, copy_target, pin, runtime_roles=("reader_fixture",), control_schema="control_fixture"
    )
    assert receipt == {"office_count": 1}
    assert driver.copy_records_to_table.await_args.kwargs["records"] == [
        ("npi", "1234567893", UUID(int=8), "e" * 64, "npi", "1234567893")
    ]
    assert all("group" not in str(call.args[0]) for call in driver.execute.await_args_list)
