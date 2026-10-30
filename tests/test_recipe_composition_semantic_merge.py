# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic orchestration checks; these doubles confer no native Source authority."""

import asyncio
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_source_recipe_composition as composition


@dataclass
class _FHIR:
    retained_capture: object = None
    retained_successor: object = None
    retained_epoch: object = None


@dataclass
class _ACA:
    prepared: object
    retained_successor: object = None
    retained_epoch: object = None


@asynccontextmanager
async def _transaction():
    yield


@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_retained_successor_authenticates_both_capabilities_with_the_same_cache(monkeypatch, failure):
    original, physical, cache = object(), object(), {}
    source = _FHIR(original, _FHIR(physical))
    reads = [object(), object()]
    reader = AsyncMock(side_effect=reads if failure is None else [reads[0], failure()])
    monkeypatch.setattr(composition, "PinnedFHIRMembershipSource", _FHIR)
    monkeypatch.setattr(composition, "_retained_recipe_read", reader)
    recipe, connection = SimpleNamespace(source_pin=source), object()
    if failure is None:
        result = await composition._require_recipe_custody(connection, recipe, retained_fhir_reads=cache)
        assert result == composition.RetainedFHIRSuccessorReads(*reads)
    else:
        with pytest.raises(failure):
            await composition._require_recipe_custody(connection, recipe, retained_fhir_reads=cache)
    assert [call.args for call in reader.await_args_list] == [
        (connection, original, cache),
        (connection, physical, cache),
    ]


async def test_epoch_successor_keeps_separate_original_and_physical_custody(monkeypatch):
    source = _FHIR(retained_epoch=object(), retained_successor=_FHIR(retained_epoch=object()))
    custody = AsyncMock()
    monkeypatch.setattr(composition, "PinnedFHIRMembershipSource", _FHIR)
    monkeypatch.setattr(composition, "_require_fhir_custody", custody)
    connection = object()
    await composition._require_recipe_custody(connection, SimpleNamespace(source_pin=source))
    assert [call.args for call in custody.await_args_list] == [
        (connection, replace(source, retained_successor=None)),
        (connection, source.retained_successor),
    ]


@pytest.mark.parametrize("retained", [None, "successor", "epoch"])
async def test_aca_preparation_cache_is_per_oid_and_cannot_bypass_retained_proof(monkeypatch, retained):
    source = _ACA(SimpleNamespace(ownership=SimpleNamespace(schema_oid=73)))
    if retained == "successor":
        source.retained_successor = object()
    if retained == "epoch":
        source.retained_epoch = object()
    prior, accepted, cache = object(), object(), {}
    cache[73] = prior
    prepare = AsyncMock(return_value=accepted)
    module = ModuleType("process.registry_aca_source_recipe")
    module.prepare_registry_aca_batch_read = prepare
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(composition, "PinnedACAMembershipSource", _ACA)
    connection = object()
    assert (
        await composition._require_recipe_custody(
            connection, SimpleNamespace(source_pin=source), aca_reads_by_oid=cache
        )
        is accepted
    )
    prepare.assert_awaited_once_with(connection, source, verified=prior if retained is None else None)
    assert cache == {73: accepted}


async def test_aca_cached_page_keeps_exact_raw_cursor_selection(monkeypatch):
    source, verified, cursor = object(), object(), object()
    recipe = SimpleNamespace(source_pin=source, binding_coordinates=object())
    connection, approved = object(), object()
    page = SimpleNamespace(next_source_position=object())
    prepared, ordinary = AsyncMock(return_value=page), AsyncMock(return_value=page)
    monkeypatch.setattr(composition, "_read_aca_batch", prepared)
    monkeypatch.setattr(composition, "read_aca_raw_fact_batch", ordinary)
    assert await composition._read_recipe_page(connection, recipe, approved, cursor, "fixture", 7, verified) == (
        page,
        page.next_source_position,
        7,
    )
    prepared.assert_awaited_once_with(
        connection, source, "fixture", approved, recipe.binding_coordinates, True, (None, 7, cursor, True), verified
    )
    ordinary.assert_not_awaited()
    await composition._read_recipe_page(connection, recipe, approved, cursor, "fixture", 7)
    ordinary.assert_awaited_once()


async def test_contribution_loop_shares_caches_and_preserves_provider_only_counters(monkeypatch):
    seen, retained = [], {}

    async def stage(*arguments):
        read_context = arguments[-1]
        seen.append((read_context.aca_reads_by_oid, read_context.retained_fhir_reads))
        return "generation", (2, 3, 7, 4, 3, 1), None, 0

    monkeypatch.setattr(composition, "_stage_source_contribution", stage)
    totals, receipts, generations, reused = await composition._collect_source_contributions(
        object(), [object(), object()], object(), "raw", "fixture", (None, {}, 2), None, retained
    )
    assert seen[0][0] is seen[1][0] and seen[0][1] is seen[1][1] is retained
    assert totals == dict(zip(composition._STAGE_COUNTER_FIELDS, (4, 6, 14, 8, 6, 2), strict=True))
    assert receipts == [] and generations == ["generation", "generation"] and reused == 0


@pytest.mark.parametrize("revision", [1, 2])
async def test_retained_fact_reuse_preserves_versioned_copy_ownership(monkeypatch, revision):
    recipe = SimpleNamespace(source_pin=_FHIR(), binding_coordinates=object())
    artifact = SimpleNamespace(relation='"immutable_owner"."facts"', descriptor={"recipe_sha256": "a" * 64})
    descriptor_by_field = {
        **dict.fromkeys(composition._STAGE_COUNTER_FIELDS, 0),
        "fact_rows": 8,
        "source_generation": "fact-gen",
    }
    descriptor_by_field["provider_only_rows"] = 2
    mapper, copy, extract = AsyncMock(return_value=descriptor_by_field), AsyncMock(), AsyncMock()
    monkeypatch.setattr(composition, "PinnedFHIRMembershipSource", _FHIR)
    monkeypatch.setattr(composition, "recipe_contribution_identity", lambda _recipe: ("a" * 64, "b" * 64))
    monkeypatch.setattr(composition, "map_fhir_facts", mapper)
    monkeypatch.setattr(composition, "reuse_fhir_facts", copy)
    monkeypatch.setattr(composition, "_stage_recipe", extract)
    monkeypatch.setattr(composition, "anchored_source_fact_descriptor", lambda result, _artifact: result)
    monkeypatch.setattr(composition, "fhir_membership_generation", lambda *_args: "membership-gen")
    stage_result = await composition._stage_source_contribution(
        object(),
        recipe,
        object(),
        "raw",
        "fixture",
        ("new_facts", {"b" * 64: artifact}, revision),
        composition.SourceRecipeReadContext(),
    )
    assert stage_result == (
        "fact-gen" if revision == 1 else "membership-gen",
        (0, 0, 0, 0, 0, 2),
        descriptor_by_field,
        8,
    )
    assert mapper.await_args.args[3] == ('pg_temp."new_facts"' if revision == 1 else artifact.relation)
    assert copy.await_count == (1 if revision == 1 else 0)
    extract.assert_not_awaited()


async def test_verified_mapping_retains_filtered_totals_and_never_invents_fact_receipts(monkeypatch):
    connection = SimpleNamespace(transaction=_transaction)
    approved = SimpleNamespace(generation_id="a" * 64)
    cache, offices = {}, object()
    matches = object()
    verified = AsyncMock(return_value=((["filtered-generation"], 1, 3, 4, 2, 1), matches, ("raw-generation",)))
    monkeypatch.setattr(composition, "_require_source_stage_scope", AsyncMock())
    monkeypatch.setattr(composition, "_source_stage_recipes", lambda recipes, _inventory: (recipes, "b" * 64))
    create, finish, facts = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(composition, "_create_source_stage_tables", create)
    monkeypatch.setattr(composition, "_finish_source_stage", finish)
    monkeypatch.setattr(composition, "_stage_verified_retained_recipes", verified)
    monkeypatch.setattr(composition, "retained_source_contributions", facts)
    stage = await composition.stage_registry_source_recipes(
        connection,
        (),
        approved,
        request_id=UUID(int=7),
        control_schema="fixture",
        read_context=composition.SourceRecipeReadContext(
            retained_fhir_reads=cache, verified_office_inputs=offices, source_manifest=object()
        ),
    )
    assert stage.office_matches is matches and stage.raw_source_generations == ("raw-generation",)
    assert stage.source_generations == ("filtered-generation",)
    assert (stage.membership_rows, stage.omitted_rows) == (1, 3)
    assert (stage.raw_source_rows, stage.accepted_raw_source_rows, stage.omitted_raw_source_rows) == (4, 2, 1)
    assert stage.contribution_table_name is None and stage.contribution_receipts == ()
    assert stage.own_fact_rows == stage.reused_fact_rows == stage.provider_only_rows == 0 and stage.inventory is False
    assert create.await_args.args[-1] is None and verified.await_args.args[-2:] == (cache, offices)
    facts.assert_not_awaited()
    assert finish.await_args.args[-1] == 1


async def test_verified_inventory_cannot_claim_unbound_office_policy(monkeypatch):
    scope = AsyncMock()
    monkeypatch.setattr(composition, "_require_source_stage_scope", scope)
    with pytest.raises(composition.RegistrySourceCompositionError, match="verified_inventory_unbound"):
        await composition.stage_registry_source_recipes(
            object(),
            (),
            object(),
            request_id=UUID(int=7),
            control_schema="fixture",
            read_context=composition.SourceRecipeReadContext(verified_office_inputs=object()),
            inventory=True,
        )
    scope.assert_not_awaited()


async def test_verified_replay_counts_physical_successor_without_relabeling_unresolved_rows(monkeypatch):
    """Fold the physical successor while preserving unresolved source accounting."""
    original = SimpleNamespace(
        retained_capture=SimpleNamespace(origin=SimpleNamespace(resource_counts=(("PractitionerRole", 9),)))
    )
    physical = SimpleNamespace(
        retained_capture=SimpleNamespace(origin=SimpleNamespace(resource_counts=(("PractitionerRole", 2),)))
    )
    selected_sources = []

    def physical_source(source):
        selected_sources.append(source)
        return physical

    read = object()
    page, empty = _physical_replay_pages()
    monkeypatch.setattr(composition, "fhir_physical_source", physical_source)
    monkeypatch.setattr(composition, "_require_recipe_custody", AsyncMock(return_value=read))
    monkeypatch.setattr(
        composition,
        "_read_recipe_page",
        AsyncMock(
            side_effect=[
                (page, ("PractitionerRole", "last"), 1000),
                (empty, None, 1000),
            ]
        ),
    )
    totals_by_name = {"membership": 0, "omitted": 0, "raw": 0, "accepted": 0, "omitted_raw": 0, "generations": []}
    stream = composition._verified_retained_pages(
        object(), [SimpleNamespace(source_pin=original)], object(), "fixture", {}, totals_by_name
    )
    assert [observed_page async for observed_page in stream] == [(page, None, read)]
    assert selected_sources == [original]
    assert totals_by_name == {
        "membership": 1,
        "omitted": 1,
        "raw": 2,
        "accepted": 1,
        "omitted_raw": 0,
        "generations": ["physical-generation"],
    }


def _physical_replay_pages():
    """Keep exact physical and terminal accounting fixtures separate from replay assertions."""
    page = SimpleNamespace(
        generation_id="physical-generation",
        complete_raw_facts=True,
        source_rows=2,
        accepted_source_rows=1,
        unresolved_source_rows=1,
        omitted_source_rows=0,
        membership_rows=1,
        omitted_rows=0,
        unresolved_rows=1,
    )
    empty = SimpleNamespace(
        **{
            **vars(page),
            "source_rows": 0,
            "accepted_source_rows": 0,
            "unresolved_source_rows": 0,
            "membership_rows": 0,
            "unresolved_rows": 0,
        }
    )
    return page, empty
