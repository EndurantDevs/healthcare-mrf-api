# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed leaf configuration and bounded grouping; native authority has separate proof."""

import json
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_aca_refresh_anchors as aca
from process import registry_cms_refresh_anchors as cms
from process import registry_cms_source_notifications as notifications
from process import registry_membership_refresh_aca as accepted_aca
from process import registry_membership_refresh_cms as accepted_cms
from process import registry_refresh_anchor_service as service
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import (
    GENERIC_FHIR_PRODUCER,
    ApprovedRefreshBinding,
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorUnavailable,
)
from process.registry_refresh_anchor_service import _merge_anchors, _verified_receipt
from tests.test_network_fhir_membership_successor import _successor
from tests.test_registry_source_recipe_store_postgres import _cms_epoch_recipe


def _binding(system="fhir", index=1, **changes):
    identity = str(UUID(int=index))
    record_by_field = dict(
        binding_id=identity,
        revision=1,
        network_id=index,
        source_system=system,
        source_id="cms-npd" if system == "fhir" else "accepted-aca",
        dataset_schema="original_source",
        dataset_id=str(UUID(int=100)),
        producer_id="producer",
        edition_id="edition",
        source_key="network-one",
        source_scope_json={},
        archived=False,
    )
    record_by_field.update(changes)
    return ApprovedRefreshBinding("network_binding", identity, 1, 1, record_by_field, "a" * 64)


@pytest.mark.parametrize("adapter,system", [(cms, "fhir"), (aca, "aca")])
def test_bindings_share_native_capture_but_duplicate_ids_and_unbounded_captures_refuse(adapter, system):
    first, second = _binding(system), _binding(system, 2)
    grouped = adapter._capture_groups((first, second))
    assert len(grouped) == 1 and next(iter(grouped.values())) == [first, second]
    with pytest.raises(RegistryRefreshAnchorMismatch):
        adapter._capture_groups((first, first))
    many_bindings = tuple(_binding(system, index + 1, dataset_id=str(UUID(int=index + 100))) for index in range(101))
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        adapter._capture_groups(many_bindings)


def test_generic_fhir_cannot_invent_a_cms_anchor():
    binding = _binding(source_id="generic-directory")
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        cms._capture_groups((binding,))


def test_generic_native_scope_requires_exact_binding_fingerprint_and_retained_policy():
    binding = _binding(source_id="generic-directory", producer_id=GENERIC_FHIR_PRODUCER)
    native_scope_by_field = {
        **{
            name: binding.record_json[name]
            for name in ("source_system", "source_id", "dataset_schema", "producer_id", "source_key")
        },
        "approved_binding_sha256": binding.approved_binding_sha256,
        "capture_generation": "b" * 64,
    }
    anchors_by_binding = {}
    _merge_anchors(anchors_by_binding, (binding,), {binding.binding_id: native_scope_by_field})
    scope_by_field = {
        "binding_id": binding.binding_id,
        "binding_revision": binding.binding_revision,
        "network_id": binding.network_id,
        "continuity_policy": "retained_successor",
        "source_scope": native_scope_by_field,
    }
    approved = SimpleNamespace(approved_revision=1, generation_id="c" * 64)
    assert _verified_receipt({"enrolled_scopes": [scope_by_field]}, approved, anchors_by_binding)[
        "enrolled_scopes"
    ] == [scope_by_field]
    for field, changed in (("source_key", "other-network"), ("approved_binding_sha256", "d" * 64)):
        with pytest.raises(RegistryRefreshAnchorUnavailable):
            _merge_anchors({}, (binding,), {binding.binding_id: {**native_scope_by_field, field: changed}})
    with pytest.raises(RegistryRefreshAnchorMismatch):
        _verified_receipt(
            {"enrolled_scopes": [{**scope_by_field, "continuity_policy": "accepted_edition"}]},
            approved,
            anchors_by_binding,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter,argument", [(cms, "reader"), (aca, "store")])
async def test_typed_scope_documents_do_not_grant_native_session_or_store_authority(adapter, argument):
    resolve = adapter.resolve_cms_refresh_anchors if adapter is cms else adapter.resolve_aca_refresh_anchors
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await resolve(object(), (), **{argument: object()}, read_budget=RegistryPTGGraphReadBudget(1000))


@pytest.mark.asyncio
async def test_cms_reader_cannot_enter_the_writer_transaction_path(monkeypatch):
    reader = notifications.RegistryCMSSourceNotificationReader("control", "sealed_owner", "publisher", ("reader",))
    native = AsyncMock()
    monkeypatch.setattr(notifications, "native_driver", native)
    with pytest.raises(ValueError, match="source_notification_invalid"):
        await reader._connection(object(), writing=True)
    native.assert_not_awaited()
    assert not hasattr(reader, "source_attempt") and not hasattr(reader, "notify_receipt")


def _pair():
    pin = PinnedFHIRMembershipSource(
        "original_source",
        "cms-npd",
        "endpoint",
        str(UUID(int=100)),
        "b" * 64,
        "release",
        100,
        "alias",
        "2026-01-01",
    )
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", pin.source_id, pin.schema_name, pin.dataset_id, "producer", "edition"
    )
    proof = SimpleNamespace(
        source_pin=pin,
        binding_coordinates=coordinates,
        cms_authority=(pin.source_id, pin.endpoint_id, pin.dataset_id, pin.dataset_sha256, "root", pin.release_id, 2),
        expected_admission_sha256="c" * 64,
        expected_metadata_sha256="d" * 64,
    )
    return SimpleNamespace(proof=proof, recipe=SimpleNamespace(source_pin=pin))


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_every_notification_must_agree_on_original_capture_and_scope(monkeypatch, conflict):
    first, second = _pair(), _pair()
    if conflict:
        second.proof = SimpleNamespace(**{**vars(second.proof), "expected_admission_sha256": "e" * 64})
    reader = SimpleNamespace(
        matching_headers=AsyncMock(
            return_value=({"receipt_id": "1", "raw_byte_count": 100}, {"receipt_id": "2", "raw_byte_count": 100})
        ),
        read=AsyncMock(side_effect=[SimpleNamespace(pair=first), SimpleNamespace(pair=second)]),
    )
    organization_read = AsyncMock(return_value={"network-one": "40000000-0000-4000-8000-000000000001"})
    monkeypatch.setattr(cms, "_organizations", organization_read)
    binding = _binding(
        source_scope_json=dict(
            organization_id="network-one", alias_scope="alias", legacy_uuid="40000000-0000-4000-8000-000000000001"
        )
    )
    arguments = (
        object(),
        object(),
        reader,
        first.proof.binding_coordinates,
        (binding,),
        RegistryPTGGraphReadBudget(100000),
    )
    if conflict:
        with pytest.raises(RegistryRefreshAnchorUnavailable):
            await cms._capture_scopes(*arguments)
    else:
        derived = await cms._capture_scopes(*arguments)
        assert derived[binding.binding_id]["endpoint_id"] == first.proof.source_pin.endpoint_id
    assert reader.read.await_count == 2 and organization_read.await_count == 2
    assert asdict(first.proof.source_pin) == asdict(second.proof.source_pin)


def _retained_pair(recipe=None):
    recipe = recipe or _cms_epoch_recipe()
    pin = replace(recipe.source_pin, retained_epoch=None)
    return SimpleNamespace(
        recipe=recipe,
        proof=SimpleNamespace(
            source_pin=pin,
            binding_coordinates=recipe.binding_coordinates,
            cms_authority=(
                pin.source_id,
                pin.endpoint_id,
                pin.dataset_id,
                pin.dataset_sha256,
                "root",
                pin.release_id,
                2,
            ),
            expected_admission_sha256="c" * 64,
            expected_metadata_sha256="d" * 64,
        ),
    )


def _retained_binding(pair):
    return _binding(
        **asdict(pair.proof.binding_coordinates),
        source_scope_json={
            "organization_id": "network-one",
            "alias_scope": pair.proof.source_pin.alias_scope,
            "legacy_uuid": "40000000-0000-4000-8000-000000000001",
        },
    )


@pytest.mark.asyncio
async def test_accepted_cms_notification_requires_retained_bytes_before_decode():
    reader = SimpleNamespace(control_schema="control", read=AsyncMock())
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=None))
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="accepted_cms_unavailable"):
        await accepted_cms._accepted_pair(
            object(), driver, reader, "source", "receipt", RegistryPTGGraphReadBudget(100)
        )
    reader.read.assert_not_awaited()
    driver.fetchval.return_value = 101
    with pytest.raises(ValueError, match="graph_budget"):
        await accepted_cms._accepted_pair(
            object(), driver, reader, "source", "receipt", RegistryPTGGraphReadBudget(100)
        )
    reader.read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [{"receipt_id": "a"}, {"receipt_id": "b"}], [{"receipt_id": "a", "cms": "{}"}]])
async def test_current_cms_header_requires_one_complete_serving_authority(rows):
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="current_cms_unavailable"):
        await accepted_cms._current_header(SimpleNamespace(fetch=AsyncMock(return_value=rows)), "source")


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "coordinates", "identity"])
async def test_original_cms_selection_never_ranks_missing_foreign_or_conflicting_receipts(monkeypatch, fault):
    first, second = _retained_pair(), _retained_pair()
    binding = _retained_binding(first)
    if fault == "coordinates":
        first.proof.binding_coordinates = replace(first.proof.binding_coordinates, producer_id="different-producer")
    if fault == "identity":
        second.proof.expected_admission_sha256 = "f" * 64
    headers = [] if fault == "missing" else [{"receipt_id": "a"}, {"receipt_id": "b"}]
    reader = SimpleNamespace(
        control_schema="control",
        matching_headers=AsyncMock(return_value=headers),
        read=AsyncMock(side_effect=[SimpleNamespace(pair=first), SimpleNamespace(pair=second)]),
    )
    monkeypatch.setattr(
        accepted_cms,
        "_organizations",
        AsyncMock(return_value={"network-one": binding.record_json["source_scope_json"]["legacy_uuid"]}),
    )
    with pytest.raises((RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable), match="original_cms"):
        await accepted_cms._original_pair(
            object(),
            SimpleNamespace(fetchval=AsyncMock(return_value=100)),
            reader,
            second.recipe.binding_coordinates,
            (binding,),
            RegistryPTGGraphReadBudget(100000),
        )
    assert reader.read.await_count == {"missing": 0, "coordinates": 1, "identity": 2}[fault]


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [[], [{"receipt_id": str(index)} for index in range(101)]])
async def test_current_cms_refuses_empty_or_unbounded_accepted_capture_set(monkeypatch, headers):
    pair = _retained_pair()
    monkeypatch.setattr(accepted_cms, "_current_header", AsyncMock(return_value=("current", pair.proof.cms_authority)))
    reader = SimpleNamespace(control_schema="control", _connection=AsyncMock(), read=AsyncMock())
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="current_cms_unavailable"):
        await accepted_cms._current_pair(
            object(),
            SimpleNamespace(fetch=AsyncMock(return_value=headers)),
            reader,
            "source",
            RegistryPTGGraphReadBudget(100000),
        )
    reader.read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection", ["current_receipt", "same_pin_replay", "conflicting_identity", "different_retention"]
)
async def test_current_cms_uses_serving_receipt_and_requires_unambiguous_physical_pin(monkeypatch, selection):
    first, second = _retained_pair(), _retained_pair()
    if selection == "conflicting_identity":
        second.proof.expected_metadata_sha256 = "f" * 64
    if selection == "different_retention":
        epoch_id = UUID(int=17)
        epoch = replace(
            second.recipe.source_pin.retained_epoch, epoch_id=epoch_id, schema_name="registry_cms_epoch_" + epoch_id.hex
        )
        second.recipe = replace(second.recipe, source_pin=replace(second.recipe.source_pin, retained_epoch=epoch))
    head = "b" if selection == "current_receipt" else "no-current-copy"
    monkeypatch.setattr(accepted_cms, "_current_header", AsyncMock(return_value=(head, first.proof.cms_authority)))
    driver = SimpleNamespace(
        fetch=AsyncMock(return_value=[{"receipt_id": "a"}, {"receipt_id": "b"}]), fetchval=AsyncMock(return_value=100)
    )
    reader = SimpleNamespace(
        control_schema="control",
        _connection=AsyncMock(),
        read=AsyncMock(side_effect=[SimpleNamespace(pair=first), SimpleNamespace(pair=second)]),
    )
    budget = RegistryPTGGraphReadBudget(100000)
    if selection in {"conflicting_identity", "different_retention"}:
        with pytest.raises(RegistryRefreshAnchorUnavailable, match="current_cms_ambiguous"):
            await accepted_cms._current_pair(object(), driver, reader, "source", budget)
    else:
        selected = await accepted_cms._current_pair(object(), driver, reader, "source", budget)
        assert selected is second
        assert selected.recipe == first.recipe
    assert reader.read.await_count == 2 and budget.read_bytes == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["organization", "producer"])
async def test_cms_successor_cannot_change_the_enrolled_organization_or_producer(monkeypatch, changed):
    original = _retained_pair()
    physical = _successor(original.recipe.source_pin)
    coordinates = replace(original.recipe.binding_coordinates, dataset_id=physical.dataset_id)
    if changed == "producer":
        coordinates = replace(coordinates, producer_id="different-producer")
    successor = _retained_pair(replace(original.recipe, source_pin=physical, binding_coordinates=coordinates))
    binding = _retained_binding(original)
    scopes = cms._scopes(original, (binding,), {"network-one": binding.record_json["source_scope_json"]["legacy_uuid"]})
    monkeypatch.setattr(accepted_cms, "_original_pair", AsyncMock(return_value=(original, scopes)))
    monkeypatch.setattr(accepted_cms, "_current_pair", AsyncMock(return_value=successor))
    changed_organization = AsyncMock(
        return_value={
            "network-one": "40000000-0000-4000-8000-000000000002"
            if changed == "organization"
            else binding.record_json["source_scope_json"]["legacy_uuid"]
        }
    )
    monkeypatch.setattr(accepted_cms, "_organizations", changed_organization)
    with pytest.raises(RegistryRefreshAnchorMismatch):
        await accepted_cms.resolve_accepted_cms_recipes(
            object(), object(), (binding,), reader=object(), read_budget=RegistryPTGGraphReadBudget(100000)
        )
    changed_organization.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", [None, "absent", "pending", "generation", "relations", "summary_inputs", "summary_oid"]
)
async def test_aca_current_publication_requires_completed_generation_and_exact_relations(monkeypatch, fault):
    from process.mrf_publication_receipt import SUMMARY_INPUTS
    from process.reference_family_result_generation import (
        ReferenceFamilyResultGenerationAuthority,
        validate_reference_family_serving_generation,
    )
    from tests.test_registry_aca_global_epoch import _global_completion

    completion = _global_completion()
    fields = completion["generation"]
    generation = ReferenceFamilyResultGenerationAuthority(
        fields["importer_id"],
        fields["local_lineage_id"],
        fields["local_generation"],
        validate_reference_family_serving_generation(fields["serving_generation"]),
        tuple(fields["relation_oids"]),
    )
    publication_by_field = {
        "contract_version": 1,
        "state": "complete",
        **{key: completion[key] for key in ("attempt_id", "import_id", "summary_oid")},
        "generation": json.dumps(generation.as_dict()),
        "summary_inputs": json.dumps(completion["summary_inputs"]),
    }
    current_relations = list({**completion["summary_inputs"], "plan_search_summary": completion["summary_oid"]}.items())
    relation_oids = generation.relation_oids
    match fault:
        case "absent":
            publication_by_field = None
        case "pending":
            publication_by_field["state"] = "preparing"
        case "generation":
            publication_by_field["generation"] = json.dumps(
                {**generation.as_dict(), "local_generation": generation.local_generation + 1}
            )
        case "relations":
            relation_oids = tuple(oid + 1 for oid in relation_oids)
        case "summary_inputs":
            publication_by_field["summary_inputs"] = json.dumps({name: 10 for name in SUMMARY_INPUTS if name != "plan"})
        case "summary_oid":
            current_relations[-1] = ("plan_search_summary", completion["summary_oid"] + 1)
    driver = SimpleNamespace(
        fetchrow=AsyncMock(return_value=publication_by_field), fetch=AsyncMock(return_value=current_relations)
    )
    monkeypatch.setattr(
        accepted_aca, "read_reference_family_result_generation_authority", AsyncMock(return_value=generation)
    )
    monkeypatch.setattr(accepted_aca, "current_reference_family_relation_oids", AsyncMock(return_value=relation_oids))
    if fault is None:
        completed, serving = await accepted_aca._current_publication(object(), driver, "source")
        assert completed == {
            name: completion[name]
            for name in ("attempt_id", "import_id", "summary_oid", "generation", "summary_inputs")
        }
        assert serving == generation.serving_generation.as_dict()
    else:
        with pytest.raises(RegistryRefreshAnchorUnavailable, match="current_aca_unavailable"):
            await accepted_aca._current_publication(object(), driver, "source")


@pytest.mark.asyncio
@pytest.mark.parametrize("header_count", [0, 101])
async def test_aca_accepted_capture_set_is_nonempty_and_bounded_before_decode(header_count):
    driver = SimpleNamespace(
        fetch=AsyncMock(
            return_value=[{"capture_id": str(index), "raw_byte_count": 100} for index in range(header_count)]
        )
    )
    store = SimpleNamespace(control_schema="control", qualify=AsyncMock(), accepted=AsyncMock())
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="current_aca_unavailable"):
        await accepted_aca._current_accepted(
            object(), driver, store, {"attempt_id": "attempt"}, {}, RegistryPTGGraphReadBudget(100000)
        )
    store.accepted.assert_not_awaited()


@pytest.mark.asyncio
async def test_aca_replay_deduplicates_one_successor_but_refuses_competing_editions(monkeypatch):
    from tests.test_registry_aca_successor_transport import recipe_pair, source_recipe

    original, current, expected = await recipe_pair(monkeypatch)
    assert accepted_aca._current_recipe(original, (current, current)) == expected
    assert accepted_aca._current_recipe(original, (original,)) == original
    newer = await source_recipe(monkeypatch, 3)
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="current_aca_unavailable"):
        accepted_aca._current_recipe(original, (current, newer))
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="current_aca_unavailable"):
        accepted_aca._current_recipe(original, ())
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="current_aca_unavailable"):
        accepted_aca._successor_for_subject(original, {})


@pytest.mark.parametrize("damage", ["missing", "extra", "invalid_scope"])
def test_refresh_receipt_requires_every_exact_valid_native_scope(damage):
    from tests.test_registry_refresh_anchor_contract import enrolled_scope

    scope = enrolled_scope()
    scopes_by_binding = {scope["binding_id"]: deepcopy(scope["source_scope"])}
    if damage == "missing":
        scopes_by_binding.clear()
    elif damage == "extra":
        scopes_by_binding[str(UUID(int=17))] = scope["source_scope"]
    else:
        scopes_by_binding[scope["binding_id"]]["resource_id"] = "not/an/organization-id"
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="incomplete_source_proof|native_source_scope_invalid"):
        _verified_receipt({"enrolled_scopes": [scope]}, ApprovedMembershipSource(1, "a" * 64), scopes_by_binding)


@pytest.mark.asyncio
@pytest.mark.parametrize("system", ["fhir", "aca"])
async def test_source_derivation_requires_the_installed_family_reader(system):
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="cms_unconfigured|aca_unconfigured"):
        await service._derive_sources(
            SimpleNamespace(cms_reader=None, aca_store=None),
            object(),
            (_binding(system),),
            RegistryPTGGraphReadBudget(100000),
        )
