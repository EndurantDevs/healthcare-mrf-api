# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generic family dispatch and original/physical identity contracts, without native authority doubles."""

import re
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_imported_selection_preview as preview
from process import registry_membership_refresh_generic as generic
from process import registry_membership_refresh_sources as sources
from process.network_fhir_membership_source import fhir_physical_source
from process.network_fhir_retained_capture_contract import PRODUCER
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_registry_generic_accepted_capture import semantic_capture
from tests.test_registry_generic_fhir_facts import _recipe
from tests.test_registry_generic_refresh_anchors import _generic_binding


def _bindings(capture):
    return (
        _generic_binding(
            source_id=capture.origin.source_id,
            dataset_schema=capture.origin.schema_name,
            dataset_id=capture.origin.dataset_id,
            edition_id=capture.capture_id,
            source_scope_json={"organization_id": "network-one", "capture_sha256": capture.capture_sha256},
        ),
    )


@pytest.mark.asyncio
async def test_metadata_observation_and_strict_selection_keep_original_approved_capture(monkeypatch):
    original, current = semantic_capture(), semantic_capture("dataset-next", "2026-02-01")
    original_reader = AsyncMock(return_value=original)
    monkeypatch.setattr(generic, "read_original_generic_capture_metadata", original_reader)
    monkeypatch.setattr(generic, "_accepted_capture", AsyncMock(return_value=current))
    monkeypatch.setattr(
        generic, "original_generic_capture_scopes", AsyncMock(return_value={"binding-one": {"original": True}})
    )
    monkeypatch.setattr(generic, "_current_organizations", AsyncMock())
    full = AsyncMock(return_value=SimpleNamespace(capture=original))
    monkeypatch.setattr(generic, "read_original_generic_capture", full)
    current_pin = AsyncMock()
    monkeypatch.setattr(generic, "pin_retained_fhir_read", current_pin)
    bindings, budget = _bindings(original), RegistryPTGGraphReadBudget(4 * 1024**2)
    driver = object()
    observed = await generic.observe_accepted_generic_recipes(driver, bindings, read_budget=budget)
    assert observed[0] == {"binding-one": {"original": True}}
    current_pin.assert_not_awaited()
    actual = await generic.resolve_accepted_generic_recipes(driver, bindings, read_budget=budget)
    assert actual == observed
    current_pin.assert_awaited_once_with(driver, current)
    recipe = actual[1][0]
    assert recipe.binding_coordinates.edition_id == original.capture_id
    assert recipe.source_pin.retained_capture == original
    assert recipe.source_pin.alias_scope == PRODUCER
    assert fhir_physical_source(recipe.source_pin).retained_capture == current
    assert fhir_physical_source(recipe.source_pin).as_of == "2026-02-01"
    vector = sources._result((recipe,)).accepted_sources[0]
    assert vector["capture_generation"] == current.capture_sha256
    assert vector["bindings_sha256"] == current.content_sha256
    assert vector["transform_version"] == "generic-fhir-source-facts-v1"


def test_legacy_original_uses_only_exact_explicit_retained_recipe_date():
    capture = retained()
    recipe = _recipe(pin(capture))
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="semantic_date_unavailable"):
        generic._original_pin(capture, recipe.binding_coordinates, ())
    assert generic._original_pin(capture, recipe.binding_coordinates, (recipe,)) == recipe.source_pin
    conflicting = replace(recipe, source_pin=replace(recipe.source_pin, as_of="2026-02-01"))
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="recipe_ambiguous"):
        generic._original_pin(capture, recipe.binding_coordinates, (recipe, conflicting))


@pytest.mark.asyncio
async def test_generic_preview_binds_logical_capture_after_removed_paging_parameters(monkeypatch):
    from process.network_fhir_membership_source import with_fhir_successor

    original = pin(semantic_capture())
    physical = replace(pin(semantic_capture("dataset-next", "2026-02-01")), as_of="2026-02-01")
    recipe = _recipe(with_fhir_successor(original, physical))
    proof = object()
    monkeypatch.setattr(preview, "_require_recipe_custody", AsyncMock(return_value=proof))
    source_check = AsyncMock()
    monkeypatch.setattr(preview, "require_fhir_source", source_check)
    parameters = await preview._require_recipe_source(object(), recipe)
    assert parameters == (
        physical.dataset_id,
        physical.source_id,
        physical.release_id,
        physical.alias_scope,
        physical.as_of,
        original.retained_capture.capture_sha256,
    )
    assert source_check.await_args.kwargs["retained_read"] is proof
    sql = preview._impact_sql(recipe, '"synthetic_registry"', '"synthetic_selection"', "synthetic_registry")
    assert set(map(int, re.findall(r"\$(\d+)", sql))) == set(range(1, 14))
    assert "capture_sha256', $13" in sql or "capture_sha256',$13" in sql


@pytest.mark.asyncio
async def test_missing_current_capture_never_falls_back_to_old_accepted_head(monkeypatch):
    original = semantic_capture()
    monkeypatch.setattr(generic, "is_original_generic_capture_current", AsyncMock(return_value=False))
    monkeypatch.setattr(generic, "read_current_generic_fhir_head", AsyncMock(return_value=object()))
    missing = AsyncMock(side_effect=RegistryRefreshAnchorUnavailable("current_capture_missing"))
    monkeypatch.setattr(generic, "read_accepted_generic_capture_metadata", missing)
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="current_capture_missing"):
        await generic._accepted_capture(object(), original, RegistryPTGGraphReadBudget(4 * 1024**2))
    legacy = retained()
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="legacy_advance_unavailable"):
        await generic._accepted_capture(object(), legacy, RegistryPTGGraphReadBudget(4 * 1024**2))
    assert missing.await_count == 1
