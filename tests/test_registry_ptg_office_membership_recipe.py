# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail closed before native row transfer and exercise consumed exact-office dispatch."""

import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from process import registry_ptg_office_membership as offices
from process import registry_source_recipe_composition as composition
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_copy import MAX_INPUT_BYTES
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_ptg_office_retention import office_hold_document
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
    monkeypatch.setattr(composition, "read_aca_raw_fact_batch", AsyncMock())
    with pytest.raises(BaseException, match="synthetic interruption"):
        await composition._read_recipe_page(driver, _recipe(), approved, None, "control_fixture", 10, verified)
    reader.assert_awaited_once()
    composition.read_aca_raw_fact_batch.assert_not_awaited()


async def _prepared_recipe_case(monkeypatch, historical):
    from tests.test_registry_ptg_office_retention import _verified

    if historical is None:
        prepared, witness = await _verified(monkeypatch)
    else:
        from process import registry_ptg_office_capture as capture
        from tests import test_registry_ptg_office_witness as fixture

        prepared = fixture.arrange.__wrapped__(monkeypatch)()
        manifest = json.loads(prepared.descriptor.manifest_json)
        manifest["command"]["retained_serving"].pop("address_relation")
        manifest["command"]["retained_serving"]["address_table_oid"] = 17
        manifest["command_sha256"] = "9" * 64
        prepared.descriptor = replace(
            prepared.descriptor,
            manifest_json=capture._canonical(manifest),
            manifest_sha256="8" * 64,
            storage_json=capture._canonical(capture.office_storage(prepared.descriptor.schema_name, "8" * 64)),
        )
        prepared.driver.descriptor = prepared.descriptor
        witness = await fixture._verify(prepared)
    return prepared, witness


def _legacy_recipe_documents(document_by_field, hold, historical):
    from process import registry_ptg_office_capture as capture

    if historical is not None:
        from process.registry_ptg_office_review_contract import named_office_review_command

        evidence = document_by_field["witness"]
        evidence["retained_serving"] = dict(document_by_field["command"]["retained_serving"])
        evidence["custody"] = dict.fromkeys(capture._LEGACY_CUSTODY_FIELDS)
        evidence.pop("storage")
        hold.pop("storage")
        hold["custody"] = dict(evidence["custody"])
        hold["retained_serving"] = dict(evidence["retained_serving"])
        hold["witness_sha256"] = capture._digest(evidence)
        if historical == "decoded":
            document_by_field["command"] = named_office_review_command(document_by_field["command"])


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "hold", "witness", "source_pin"])
@pytest.mark.parametrize("historical", [None, "stored", "decoded"])
async def test_consumed_proof_reaches_real_whole_office_witness(monkeypatch, changed, historical):
    """Rebuild the retained office witness using its actual prepared approval revision."""
    from process import network_serving_read, registry_ptg_office_approval
    from process.ptg_parts import result_archive_published_authority

    prepared, witness = await _prepared_recipe_case(monkeypatch, historical)
    approved_revision = json.loads(prepared.descriptor.manifest_json)["command"]["source"]["approved_revision"]
    pin = replace(
        _recipe().source_pin,
        capture_id=str(prepared.request.capture_id),
        manifest_sha256=prepared.descriptor.manifest_sha256,
    )
    source_pin_by_field = {"operation_id": "registry_ptg_office_review_" + prepared.request.capture_id.hex}
    document_by_field = {
        "command": json.loads(prepared.descriptor.manifest_json)["command"],
        "manifest_sha256": prepared.descriptor.manifest_sha256,
        "witness": witness.as_dict(),
        "source_pin": source_pin_by_field,
    }
    hold = office_hold_document(prepared.descriptor, witness, pin.approval_sha256)
    _legacy_recipe_documents(document_by_field, hold, historical)
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
                SimpleNamespace(approved_revision=approved_revision),
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
            SimpleNamespace(approved_revision=approved_revision),
            object(),
            "synthetic_control",
            RegistryPTGGraphReadBudget(1048576),
            prepared.session.get_transaction(),
        )
        assert actual.descriptor is prepared.descriptor
        assert prepared.session.witness_calls[-1] == prepared.request.input_row_count


@pytest.mark.asyncio
@pytest.mark.parametrize("compact", [False, True])
async def test_office_binding_dispatch_uses_selected_tuple_and_real_copy(monkeypatch, compact):
    """Dispatch the selected office tuple and COPY its unchanged retained adoption fields."""
    from process import registry_ptg_office_bindings as bindings
    from process.network_address_projection import PinnedAddressSource
    from process.network_membership_copy import MembershipCopyTarget
    from process.registry_source_office_origin import source_office_pin_document

    driver, batch, adoption = _office_binding_batch(compact=compact)
    driver.execute = AsyncMock()
    driver.fetchval = AsyncMock(return_value=False)
    driver.copy_records_to_table = AsyncMock(return_value="COPY 1")
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    driver.transaction = lambda: transaction
    monkeypatch.setattr(bindings, "read_ptg_office_membership_batch", AsyncMock(return_value=batch))
    resolve = AsyncMock(side_effect=_office_binding_origin)
    monkeypatch.setattr(bindings, "resolve_source_office_origin", resolve)
    monkeypatch.setattr(bindings, "_candidate", AsyncMock())
    address = PinnedAddressSource("address_fixture", "entity_address_unified", "b" * 64)
    copy_target = MembershipCopyTarget(
        *(str(UUID(int=number)) for number in range(1, 5)), "network_candidate_" + UUID(int=4).hex
    )
    pin = await bindings.pin_ptg_office_address_source(
        driver, batch, address, runtime_roles=("reader_fixture",), control_schema="control_fixture"
    )
    assert "origin_manifest_sha256" not in source_office_pin_document(pin)
    receipt = await bindings.copy_ptg_office_bindings(
        driver, batch, copy_target, pin, runtime_roles=("reader_fixture",), control_schema="control_fixture"
    )
    assert receipt == {"office_count": 1}
    assert driver.copy_records_to_table.await_args.kwargs["records"] == [
        ("npi", "1234567893", UUID(int=8), "e" * 64, "npi", "1234567893")
    ]
    assert all("group" not in str(call.args[0]) for call in driver.execute.await_args_list)
    retained_sql, retained_arguments = bindings._source_addresses(batch.verified_source.serving, '"retained_fixture"')
    office_queries = [
        call.args
        for call in driver.fetchval.await_args_list
        if "jsonb_to_recordset($1::jsonb) AS office(" in call.args[0]
    ]
    assert len(office_queries) == 2
    query, encoded_offices, *arguments = office_queries[-1]
    assert bool(retained_arguments) is compact
    assert retained_sql in query and arguments == [*retained_arguments, "npi"]
    assert json.loads(encoded_offices) == [asdict(adoption)]
    assert pin.address_identity == (address.schema_name, address.table_name, address.generation_id, None)
    assert all(call.kwargs["first_parameter"] == 2 + len(retained_arguments) for call in resolve.await_args_list)
    assert f"${2 + len(retained_arguments)}::text" in query
    assert "matches<>1 OR differs" in query and "count(DISTINCT (provider_system,provider_id,location_id))" in query
    assert "to_regnamespace" not in query and "pg_class" not in query


def _with_retained_office_serving(verified, *, compact=False):
    """Attach the same retained serving DTO to the existing owner fixture."""
    from process.network_serving_read import PinnedNetworkServingManifest

    verified = replace(
        verified,
        serving=PinnedNetworkServingManifest(
            3,
            str(UUID(int=9)),
            "retained_fixture",
            1,
            {},
            4,
            "d" * 64,
            CompactAddressRelation("retained_fixture", "entity_address_unified"),
        ),
    )
    if compact:
        from process.network_compact_layout import CompactEdgeArtifact, CompactNetworkLayout

        layout = CompactNetworkLayout(
            CompactAddressRelation("retained_fixture", "network_slice_ref"),
            CompactAddressRelation("base_fixture", "address_rows"),
            (CompactEdgeArtifact(str(UUID(int=10)), "network_candidate_" + UUID(int=10).hex),),
        )
        serving = replace(verified.serving, address_relation=layout.base)
        object.__setattr__(serving, "_compact_layout", layout)
        verified = replace(verified, serving=serving)
    return verified


def _office_binding_batch(*, compact=False):
    from process.registry_retained_site_adoption import RetainedSiteAdoption

    driver, verified, approved = _owner()
    verified = _with_retained_office_serving(verified, compact=compact)
    adoption = RetainedSiteAdoption("npi", "1234567893", str(UUID(int=8)), "e" * 64, "npi", "1234567893", "f" * 64)
    batch = offices.PTGOfficeMembershipBatch(
        verified.pin, approved, verified, 1, 1, 0, 0, b"[]", "1" * 64, 1, (adoption,)
    )
    return driver, batch, adoption


def _office_binding_origin(_connection, address_source, **selection):
    from process.registry_source_office_origin import ResolvedSourceOfficeOrigin

    relation = f'(SELECT * FROM "address_fixture".entity_address_unified WHERE entity_type=${selection["first_parameter"]}::text)'
    return ResolvedSourceOfficeOrigin(address_source, relation, ("npi",), None)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["page", "unresolved", "matches", "unknown", "count"])
async def test_office_binding_rejects_changed_page_and_incomplete_correspondence(monkeypatch, damage):
    from process import registry_ptg_office_bindings as bindings
    from process.network_address_projection import PinnedAddressSource

    driver, batch, _adoption = _office_binding_batch()
    if damage == "unresolved":
        batch = replace(batch, unresolved_rows=1)
    if damage == "count":
        batch = replace(batch, membership_rows=2)
    replay = replace(batch, input_sha256="2" * 64) if damage == "page" else batch
    reader = AsyncMock(return_value=replay)
    monkeypatch.setattr(bindings, "read_ptg_office_membership_batch", reader)
    driver.fetchval = AsyncMock(return_value=True if damage == "matches" else None if damage == "unknown" else False)
    expected = "page_changed" if damage == "page" else "input_invalid" if damage == "unresolved" else "unresolved"
    with pytest.raises(ValueError, match="registry_ptg_office_binding_" + expected):
        await bindings._offices(
            driver,
            batch,
            PinnedAddressSource("address_fixture", "entity_address_unified", "b" * 64),
            None,
            "control_fixture",
        )
    if damage in {"page", "unresolved"}:
        driver.fetchval.assert_not_awaited()
    if damage == "unresolved":
        reader.assert_not_awaited()
