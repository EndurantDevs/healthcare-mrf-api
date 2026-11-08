# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure office orchestration/refusal checks; mocks establish no Source or address custody."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import network_initial_cms_office_bindings as legacy
from process import registry_source_contribution_store as facts
from process import registry_source_recipe_composition as composition
from process import registry_verified_witness_offices as offices
from process import registry_verified_witness_stage as stage
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2
from process.network_fhir_source_witness_capture import SourceWitnessRead
from process.network_fhir_source_witness_contract import SourceWitnessContractError
from tests.test_network_fhir_source_witness_recipe import _reader_recipe, _source_recipe

APPROVED = ApprovedMembershipSource(7, "a" * 64)
ADDRESS = PinnedAddressSource("synthetic_addresses", "entity_address_unified", "b" * 64)


def _witness_source(recipe=None):
    recipe = _source_recipe() if recipe is None else recipe
    identity, capture = facts.verified_office_fact_identity(recipe)
    descriptor_by_field = {
        **dict.fromkeys(facts._WITNESS_COUNTERS_V2, 0),
        **dict.fromkeys(facts.FACT_ANCHOR_FIELDS),
        "descriptor_version": 2,
        "transform_version": FHIR_SOURCE_WITNESS_TRANSFORM_V2,
        "recipe_sha256": identity,
        "capture_sha256": capture,
        "mapping_sha256": "c" * 64,
        "fact_sha256": "d" * 64,
        "source_generation": facts._digest([capture, "c" * 64]),
    }
    return offices.WitnessOfficeSource(recipe, descriptor_by_field, "synthetic_members")


def test_verified_scope_accepts_only_normalized_source_and_reader_shapes():
    source, reader = _source_recipe(), _reader_recipe()
    stage._require_witness_scope((source,), APPROVED, (ADDRESS, ("reader",)))
    stage._require_witness_scope((source, reader), APPROVED, (ADDRESS, ("reader",)))
    assert offices._descriptor(_witness_source()) == _witness_source().descriptor
    assert offices._descriptor(_witness_source(reader)) == _witness_source(reader).descriptor


@pytest.mark.parametrize("damage", ["identity", "capture", "extra"])
def test_office_descriptor_keeps_the_complete_source_identity(damage):
    selected = _witness_source()
    descriptor_by_field = dict(selected.descriptor)
    field = {"identity": "recipe_sha256", "capture": "capture_sha256", "extra": "untrusted"}[damage]
    descriptor_by_field[field] = "0" * 64
    with pytest.raises(ValueError):
        offices._descriptor(offices.WitnessOfficeSource(selected.recipe, descriptor_by_field, selected.table_name))


@pytest.mark.asyncio
async def test_source_census_reads_the_capture_and_includes_empty_families():
    recipe = _source_recipe()
    expected_by_kind = dict(recipe.source_pin.source_witness_capture.origin.resource_counts)
    connection = SimpleNamespace(
        fetch=AsyncMock(
            return_value=[{"resource_type": kind, "rows": count} for kind, count in expected_by_kind.items() if count]
        )
    )
    assert await stage._native_witness_headers(connection, recipe) == 0
    query, dataset = connection.fetch.await_args.args
    assert recipe.source_pin.read_schema_name in query and dataset == recipe.source_pin.dataset_id
    connection.fetch.return_value.append({"resource_type": "PractitionerRole", "rows": 1})
    with pytest.raises(composition.RegistrySourceCompositionError, match="accounting_invalid"):
        await stage._native_witness_headers(connection, recipe)


@pytest.mark.asyncio
@pytest.mark.parametrize("read", [None, SimpleNamespace(verify=AsyncMock())])
async def test_source_office_read_refuses_missing_or_duck_capabilities(read):
    connection = SimpleNamespace(fetch=AsyncMock(), fetchval=AsyncMock())
    with pytest.raises(legacy.InitialCMSOfficeBindingError):
        await legacy._verify_retained_office_read(connection, _source_recipe().source_pin, read)
    connection.fetch.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    if read is not None:
        read.verify.assert_not_awaited()


@pytest.mark.asyncio
async def test_actual_source_read_type_still_requires_its_native_brand():
    connection = SimpleNamespace(fetch=AsyncMock(), fetchval=AsyncMock())
    recipe = _source_recipe()
    unissued = SourceWitnessRead(
        connection, recipe.source_pin.source_witness_capture, asyncio.current_task(), "1/1", object()
    )
    with pytest.raises(SourceWitnessContractError):
        await legacy._verify_retained_office_read(connection, recipe.source_pin, unissued)
    connection.fetch.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["source", "reader"])
async def test_authorizer_passes_source_runtime_without_reusing_producer_reads(monkeypatch, kind):
    recipe = _source_recipe() if kind == "source" else _reader_recipe()
    runtime, read = object(), object()  # callback protocol sentinels, never native capability proofs
    cached_by_generation = {recipe.source_pin.generation_id: object()}
    custody = AsyncMock(return_value=read)
    checked = AsyncMock()
    monkeypatch.setattr(stage, "_require_recipe_custody", custody)
    monkeypatch.setattr(stage, "_require_source_recipe", checked)
    assert (
        await stage._authorize_witness_recipe(
            object(), recipe, APPROVED, "registry_example", cached_by_generation, source_witness_runtime=runtime
        )
        is read
    )
    if kind == "source":
        assert custody.await_args.kwargs == {"retained_fhir_reads": None, "source_witness_runtime": runtime}
    else:
        assert custody.await_args.kwargs == {"retained_fhir_reads": cached_by_generation}
    assert checked.await_args.args[-1] is read


def test_stage_context_preserves_closed_legacy_options():
    runtime = object()
    context = composition.SourceRecipeReadContext(
        retained_fhir_reads={}, verified_office_inputs=(ADDRESS, ("reader",)), source_witness_runtime=runtime
    )
    assert stage._witness_read_context(context, {}) is context
    legacy_context = stage._witness_read_context(
        None,
        {
            "retained_fhir_reads": context.retained_fhir_reads,
            "verified_office_inputs": context.verified_office_inputs,
            "source_witness_runtime": runtime,
        },
    )
    assert legacy_context == context
    for supplied, options in ((context, {"source_witness_runtime": runtime}), (None, {"extra": True}), (object(), {})):
        with pytest.raises(composition.RegistrySourceCompositionError):
            stage._witness_read_context(supplied, options)


@pytest.mark.asyncio
async def test_mapping_reauthorizes_source_using_only_the_supplied_runtime(monkeypatch):
    selected = _witness_source()
    runtime = object()
    options = offices._WitnessOfficeContext(APPROVED, "registry_example", "e" * 64, runtime)
    connection = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(offices, "_require_transaction", AsyncMock())
    monkeypatch.setattr(offices, "_policy_context", AsyncMock(return_value=({}, (1, 2, 3, "f" * 64))))
    marker = RuntimeError("unit_source_refused")
    authorizer = AsyncMock(side_effect=marker)
    monkeypatch.setattr(composition, "_require_recipe_custody", authorizer)
    with pytest.raises(RuntimeError) as failure:
        await offices._map_witness_offices(connection, ADDRESS, ("reader",), (selected,), "members", options)
    assert failure.value is marker
    assert authorizer.await_args.kwargs == {"source_witness_runtime": runtime}


@pytest.mark.asyncio
async def test_source_office_savepoint_preserves_the_original_failure(monkeypatch):
    transaction = SimpleNamespace(__aenter__=AsyncMock(), __aexit__=AsyncMock(return_value=False))

    class Savepoint:
        async def __aenter__(self):
            return await transaction.__aenter__()

        async def __aexit__(self, *arguments):
            return await transaction.__aexit__(*arguments)

    connection = SimpleNamespace(transaction=Mock(return_value=Savepoint()))
    monkeypatch.setattr(offices, "_require_transaction", AsyncMock())
    marker = RuntimeError("unit_source_failure")
    monkeypatch.setattr(offices, "_map_witness_offices", AsyncMock(side_effect=marker))
    options = offices._WitnessOfficeContext(APPROVED, "registry_example", "e" * 64, object())
    with pytest.raises(RuntimeError) as failure:
        await offices._prepare_source_witness_mapping(connection, ADDRESS, ("reader",), (), "members", options)
    assert failure.value is marker and transaction.__aexit__.await_args.args[1] is marker
