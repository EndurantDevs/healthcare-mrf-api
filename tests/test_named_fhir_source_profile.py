# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog configuration and published payload names have independent scopes."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import network_fhir_source_witness_capture as capture
from process import network_fhir_typed_source as typed
from process import registry_source_recipe_composition as composition
from process.network_fhir_source_witness_contract import (
    CONTRACT,
    SourceWitnessCaptureAnchor,
    SourceWitnessContractError,
    source_origin,
)
from process.network_fhir_typed_contract import TypedFHIRGraphError, TypedFHIRGraphStage
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile
from tests.test_named_fhir_typed_graph import COUNTS, GraphConnection
from tests.test_network_fhir_source_witness_contract import captured, origin_fields


@pytest.fixture(params=[True, False], ids=["matching-catalog", "wrong-catalog"])
def named_source(request):
    """Keep the configured namespace distinct from a complete published payload."""
    origin = source_origin(
        origin_fields()
        | {
            "schema_name": "published_generation",
            "header_relation": {"schema": "source_catalog", "name": "provider_directory_endpoint_dataset"},
            "resource_relation": {"schema": "published_generation", "name": "frozen_resources"},
        }
    )
    profile = FHIRCatalogSourceReadProfile(
        "configured_storage" if request.param else origin.schema_name,
        "source_catalog" if request.param else "wrong_catalog",
    )
    events = []

    async def pages():
        try:
            yield tuple(
                {
                    "dataset_id": origin.dataset_id,
                    "resource_type": kind,
                    "resource_id": identity,
                    "payload_hash": "a" * 64,
                    "payload_json": {"resource_id": identity},
                    "acquired_resource_sha256": None,
                }
                for kind, identity in (("Location", "office-one"), ("Organization", "network-one"))
            )
            events.append("source.eof")
        finally:
            events.append("source.closed")

    @asynccontextmanager
    async def export(**selection):
        assert selection.pop("source_profile") is profile
        assert selection in (
            {"origin": origin},
            {key: getattr(origin, key) for key in ("source_id", "endpoint_id", "dataset_id")},
        )
        events.append("source.open")
        yield {"contract": CONTRACT, "origin": origin, "resources": pages}
        events.append("source.exit")

    original = captured()
    anchor = SourceWitnessCaptureAnchor(original.capture_id, original.release_id, original.observed_at)
    return SimpleNamespace(
        origin=origin, profile=profile, export=export, events=events, anchor=anchor, matches=request.param
    )


def _configured_profiles(origin, state):
    catalog = origin.header_relation["schema"]
    if state == "wrong":
        return (FHIRCatalogSourceReadProfile(origin.schema_name, "wrong_catalog"),)
    profiles = (FHIRCatalogSourceReadProfile("configured_storage", catalog),)
    if state == "ambiguous":
        profiles += (FHIRCatalogSourceReadProfile("another_configuration", catalog),)
    return profiles


def _source_runtime(origin, state):
    return composition.SourceWitnessRuntime(
        Mock(), _configured_profiles(origin, state), "capture_owner", ("capture_reader",)
    )


@pytest.mark.asyncio
async def test_typed_graph_accepts_named_payload(named_source, monkeypatch):
    """Consume the complete Source stream while binding configuration to its catalog."""
    case = named_source
    connection = GraphConnection()
    connection.fetchval = AsyncMock(return_value=True)
    connection.execute = AsyncMock(wraps=connection.execute)
    stage = TypedFHIRGraphStage(
        {"cells": "source_cells", "addresses": "source_addresses", "pairs": "source_pairs"},
        schema_name="candidate_generation",
    )
    canonicalize, pairs, counts = AsyncMock(), AsyncMock(), AsyncMock(return_value=COUNTS)
    monkeypatch.setattr(typed, "_canonicalize", canonicalize)
    monkeypatch.setattr(typed, "_pairs", pairs)
    monkeypatch.setattr(typed, "_counts", counts)
    arguments_by_name = dict(source_profile=case.profile, reviewed_origin=case.origin, membership_as_of="2026-01-01")
    if case.matches:
        document = await typed.produce_typed_source_graph(case.export, connection, stage, **arguments_by_name)
        assert document["origin"]["resource_relation"] == case.origin.resource_relation
        assert document["counts"]["source_rows"] == 2
        assert case.events == ["source.open", "source.eof", "source.closed", "source.exit"]
        assert connection.execute.await_count == 1
        canonicalize.assert_awaited_once_with(connection, stage)
        counts.assert_awaited_once_with(connection, stage, 2)
    else:
        with pytest.raises(TypedFHIRGraphError, match="source_profile"):
            await typed.produce_typed_source_graph(case.export, connection, stage, **arguments_by_name)
        assert case.events == [] and stage.document is None
        connection.fetchval.assert_not_awaited()
        connection.execute.assert_not_awaited()
        canonicalize.assert_not_awaited()
        pairs.assert_not_awaited()
        counts.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_replay_accepts_named_payload(named_source, monkeypatch):
    """Preserve second-export EOF and context exit after catalog scope validation."""
    case = named_source
    compare, finish = AsyncMock(), AsyncMock()
    monkeypatch.setattr(capture, "_compare_source_page", compare)
    monkeypatch.setattr(capture, "_finish_source_replay", finish)
    arguments_by_name = dict(source_profile=case.profile, reviewed_origin=case.origin, expected_anchor=case.anchor)
    connection = object()
    if case.matches:
        await capture._verify_source_rows(case.export, connection, **arguments_by_name)
        assert case.events == ["source.open", "source.eof", "source.closed", "source.exit"]
        compare.assert_awaited_once()
        assert compare.await_args.args[0] is connection
        assert compare.await_args.args[1].origin == case.origin
        assert {row["dataset_id"] for row in compare.await_args.args[2]} == {case.origin.dataset_id}
        finish.assert_awaited_once_with(connection, compare.await_args.args[1])
    else:
        with pytest.raises(SourceWitnessContractError):
            await capture._verify_source_rows(case.export, connection, **arguments_by_name)
        assert case.events == []
        compare.assert_not_awaited()
        finish.assert_not_awaited()


@pytest.mark.asyncio
async def test_capture_catalog_gate_precedes_io(named_source):
    """The capture gate accepts matching catalogs and rejects others before writer IO."""
    case = named_source
    boundary = RuntimeError("first capture transaction")
    writer = SimpleNamespace(transaction=Mock(side_effect=boundary))
    arguments_by_name = dict(
        source_profile=case.profile,
        origin=case.origin,
        capture_id=UUID(case.anchor.capture_id),
        owner_role="capture_owner",
        runtime_roles=("capture_reader",),
        expected_anchor=case.anchor,
    )
    if case.matches:
        with pytest.raises(RuntimeError) as raised:
            await capture.capture_source_fhir_witness(case.export, writer, **arguments_by_name)
        assert raised.value is boundary
        writer.transaction.assert_called_once_with()
    else:
        with pytest.raises(SourceWitnessContractError):
            await capture.capture_source_fhir_witness(case.export, writer, **arguments_by_name)
        writer.transaction.assert_not_called()
    assert case.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["matching", "wrong", "ambiguous"])
async def test_typed_recipe_selects_configured_catalog(monkeypatch, state):
    """Preserve approved binding verification after one unambiguous catalog selection."""
    from process import network_approved_source_bindings as bindings
    from tests.test_registry_typed_source_recipe import _recipe

    recipe = _recipe()
    origin = source_origin(recipe.source_pin.capture["graph"]["origin"])
    runtime = _source_runtime(origin, state)
    verify = AsyncMock()
    monkeypatch.setattr(bindings, "require_approved_network_source_bindings", verify)
    connection, approved = object(), object()
    arguments = (connection, recipe, approved, "control_example", runtime, False)
    if state == "matching":
        pin, profile, selected = await composition._typed_source_read_scope(*arguments)
        assert pin is recipe.source_pin and selected == origin
        assert profile is runtime.source_profiles[0] and profile.schema_name != origin.schema_name
        verify.assert_awaited_once_with(
            connection, approved, recipe.binding_coordinates, control_schema="control_example"
        )
    else:
        with pytest.raises(composition.RegistrySourceCompositionError, match="profile_unavailable"):
            await composition._typed_source_read_scope(*arguments)
        verify.assert_not_awaited()
    runtime.source_export.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["matching", "wrong", "ambiguous"])
async def test_witness_recipe_selects_configured_catalog(monkeypatch, state):
    """Keep approval and fresh Source verification on the selected configured profile."""
    retained = captured()
    runtime = _source_runtime(retained.origin, state)
    approval = AsyncMock(return_value=retained)
    verify = AsyncMock()
    read = object()
    pin = AsyncMock(return_value=read)
    monkeypatch.setattr(composition, "_approved_source_witness_capture", approval)
    monkeypatch.setattr(capture, "verify_source_fhir_witness_capture", verify)
    monkeypatch.setattr(capture, "pin_source_fhir_witness_read", pin)
    connection, recipe, approved = object(), object(), object()
    arguments = (connection, recipe, approved, "control_example", runtime)
    if state == "matching":
        assert await composition.require_source_witness_read(*arguments) is read
        assert verify.await_args.kwargs["source_profile"] is runtime.source_profiles[0]
        assert source_origin(verify.await_args.kwargs["reviewed_origin"]) == retained.origin
        assert verify.await_args.kwargs["capture"] is retained
        pin.assert_awaited_once_with(connection, retained)
    else:
        with pytest.raises(composition.RegistrySourceCompositionError, match="profile_unavailable"):
            await composition.require_source_witness_read(*arguments)
        verify.assert_not_awaited()
        pin.assert_not_awaited()
    approval.assert_awaited_once_with(connection, recipe, approved, "control_example", retained_approval=False)
    runtime.source_export.assert_not_called()
