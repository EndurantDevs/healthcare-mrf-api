# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained publication keeps trusted Source services and authenticated address pins."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import registry_publication_consumer as consumer
from process.network_address_projection import PinnedAddressSource
from process.registry_source_inventory_contract import source_inventory_root
from process.registry_source_inventory_reference import SourceInventoryReference
from process.registry_source_recipe_additions import RegistrySourceRecipeAdditions
from tests.test_registry_publication_consumer_postgres import _ordinary_recipe_execution
from tests.test_registry_source_witness_worker import _runtime, _sources
from tests.test_registry_typed_source_recipe import _recipe


@pytest.fixture
def publication_sources():
    runtime = _runtime()
    recipe = _recipe()
    sources = replace(_sources(runtime), npi_source=PinnedAddressSource("retained_npi", "npi", "b" * 64))
    additions = RegistrySourceRecipeAdditions(
        7, "d" * 64, SourceInventoryReference(source_inventory_root(())), (recipe,)
    )
    return SimpleNamespace(
        sources=sources,
        runtime=runtime,
        phases_by_name={
            "initial_source_recipes": (recipe,),
            "replacement_source_recipes": (recipe,),
            "source_recipe_additions": additions,
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", [None, "replacement_source_recipes", "source_recipe_additions"])
@pytest.mark.parametrize("has_runtime", [False, True])
async def test_retained_publication_keeps_configured_runtime(monkeypatch, publication_sources, phase, has_runtime):
    case = publication_sources
    runtime = case.runtime if has_runtime else None
    changes_by_name = {"source_witness_runtime": runtime}
    if phase is not None:
        changes_by_name[phase] = case.phases_by_name[phase]
    configured = replace(case.sources, **changes_by_name)
    execution = _ordinary_recipe_execution(monkeypatch, head=7, address_sources=configured)
    retained = replace(case.sources, source_witness_runtime=None)
    execution.retained.return_value = retained
    assert await consumer._ordinary_publication(
        execution.connection, execution.request, execution.authorize, execution.config, configured
    ) == {"state": "completed"}
    selected = execution.compose.await_args.kwargs["address_sources"]
    assert selected.base_source is retained.base_source and selected.npi_source is retained.npi_source
    assert selected.source_witness_runtime is runtime and not selected.initial_source_recipes
    assert selected.replacement_source_recipes == configured.replacement_source_recipes
    assert selected.source_recipe_additions is configured.source_recipe_additions
    assert execution.compose.await_args.kwargs["source_manifest"] is execution.source_pin
    assert execution.transactions == [{"isolation": "repeatable_read", "readonly": True}]
    execution.retained.assert_awaited_once_with(
        execution.connection, execution.source_pin, "registry", **({"compact_upgrade": True} if phase else {})
    )
    assert execution.complete.await_args.kwargs.get("source_witness_runtime") is runtime
    case.runtime.source_export.assert_not_called()


@pytest.mark.asyncio
async def test_retained_publication_without_configuration_has_no_runtime(monkeypatch, publication_sources):
    case = publication_sources
    retained = replace(case.sources, source_witness_runtime=None)
    execution = _ordinary_recipe_execution(monkeypatch, head=7, address_sources=retained)
    await consumer._ordinary_publication(
        execution.connection, execution.request, execution.authorize, execution.config, None
    )
    assert execution.compose.await_args.kwargs["address_sources"] is execution.retained.return_value
    assert execution.compose.await_args.kwargs["address_sources"].source_witness_runtime is None
    assert "source_witness_runtime" not in execution.complete.await_args.kwargs
    case.runtime.source_export.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase,head", [("initial_source_recipes", 7), ("replacement_source_recipes", 0), ("source_recipe_additions", 0)]
)
async def test_configured_runtime_preserves_publication_phase_guard(monkeypatch, publication_sources, phase, head):
    case = publication_sources
    configured = replace(case.sources, **{phase: case.phases_by_name[phase]})
    execution = _ordinary_recipe_execution(monkeypatch, head=head, address_sources=configured)
    with pytest.raises(ValueError, match="^registry_publication_source_recipe_phase_invalid$"):
        await consumer._ordinary_publication(
            execution.connection, execution.request, execution.authorize, execution.config, configured
        )
    execution.approval.assert_not_awaited()
    execution.retained.assert_not_awaited()
    execution.compose.assert_not_awaited()
    execution.complete.assert_not_awaited()
    case.runtime.source_export.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["replacement_source_recipes", "source_recipe_additions"])
@pytest.mark.parametrize("pin", ["base_source", "npi_source"])
async def test_runtime_keeps_retained_pin_mismatch_refusal(monkeypatch, publication_sources, phase, pin):
    case = publication_sources
    configured = replace(case.sources, **{phase: case.phases_by_name[phase]})
    execution = _ordinary_recipe_execution(monkeypatch, head=7, address_sources=configured)
    execution.retained.return_value = replace(case.sources, source_witness_runtime=None)
    configured = replace(configured, **{pin: replace(getattr(configured, pin), generation_id="changed")})
    with pytest.raises(ValueError, match="^registry_publication_replacement_source_mismatch$"):
        await consumer._ordinary_publication(
            execution.connection, execution.request, execution.authorize, execution.config, configured
        )
    execution.compose.assert_not_awaited()
    execution.complete.assert_not_awaited()
    case.runtime.source_export.assert_not_called()


@pytest.mark.asyncio
async def test_runtime_cannot_bypass_retained_currentness_failure(monkeypatch, publication_sources):
    case = publication_sources
    execution = _ordinary_recipe_execution(monkeypatch, head=7, address_sources=case.sources)
    failure = ValueError("registry_source_rollback_manifest_changed")
    execution.retained.side_effect = failure
    with pytest.raises(ValueError) as raised:
        await consumer._ordinary_publication(
            execution.connection, execution.request, execution.authorize, execution.config, case.sources
        )
    assert raised.value is failure
    execution.compose.assert_not_awaited()
    execution.complete.assert_not_awaited()
    case.runtime.source_export.assert_not_called()
