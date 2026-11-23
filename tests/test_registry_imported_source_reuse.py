# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical-edition reuse and committed metadata recovery never own retained cleanup."""

import asyncio
import json
import os
from copy import deepcopy
from dataclasses import replace
from time import perf_counter
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import asyncpg
import pytest

from process import registry_imported_source_preparation as preparation
from process import registry_imported_source_reuse as reuse
from process.network_fhir_source_witness_contract import canonical, digest
from process.network_fhir_typed_capture import COMPONENT, PREFIX
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID
from tests.test_registry_imported_source_preparation import prepare, refresh_receipt
from tests.test_registry_imported_source_preparation import prepared_case as prepared_case
from tests.test_registry_typed_source_recipe import _recipe


def add_source(case, source_id):
    descriptor = deepcopy(case.descriptors[0])
    descriptor["coordinates"]["source_id"] = source_id
    descriptor["source_aliases"] = [source_id]
    refresh_receipt(descriptor)
    case.descriptors.append(descriptor)
    case.origins[source_id] = replace(case.origin, source_id=source_id, source_aliases=(source_id,))
    return descriptor


@pytest.mark.asyncio
async def test_unchanged_source_keeps_exact_original_recipe_without_export(prepared_case, monkeypatch):
    case = prepared_case
    retained = _recipe()
    predecessor = AsyncMock(return_value=(retained,))
    monkeypatch.setattr(preparation, "predecessor_recipes", predecessor)
    result = await prepare(case)
    assert result.recipes == (retained,) and result.recipes[0] is retained
    assert result.created_schemas == ()
    assert result.reused_schemas == (retained.source_pin.capture["schema_name"],)
    assert all(event[0] != "source_open" for event in case.events)
    preparation.capture_typed_source_graph.assert_not_awaited()
    case.connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_unchanged_source_keeps_its_capture_on_a_later_day(prepared_case, monkeypatch):
    retained = _recipe()
    graph = retained.source_pin.capture["graph"]
    graph["origin"]["semantic_projection_as_of"] = None
    assert graph["membership_as_of"] == "2026-10-01"
    monkeypatch.setattr(preparation, "predecessor_recipes", AsyncMock(return_value=(retained,)))
    result = await prepare(prepared_case)
    assert result.recipes[0] is retained and result.created_schemas == ()
    assert not reuse.is_compatible_typed_recipe(retained, retained.source_pin.catalog_descriptor, "2026-11-01")
    preparation.capture_typed_source_graph.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_changed_source_opens_factory_and_creates_capture(prepared_case, monkeypatch):
    case = prepared_case
    changed = add_source(case, "alias-c")
    previous = await prepare(case)
    case.events.clear()
    preparation.capture_typed_source_graph.reset_mock()
    monkeypatch.setattr(preparation, "predecessor_recipes", AsyncMock(return_value=previous.recipes))
    changed["dataset_sha256"] = "f" * 64
    refresh_receipt(changed)
    case.origins["alias-c"] = replace(case.origins["alias-c"], dataset_sha256="f" * 64)
    result = await prepare(case)
    assert result.recipes[0] is previous.recipes[0]
    assert result.recipes[1].source_pin.capture["capture_sha256"] != previous.recipes[1].source_pin.generation_id
    assert len(result.created_schemas) == len(result.reused_schemas) == 1
    assert preparation.capture_typed_source_graph.await_count == 1
    assert [
        event[1]["source_id"] for event in case.events if event[0] == "source_open" and "source_id" in event[1]
    ] == ["alias-c"]


@pytest.mark.asyncio
async def test_practitioner_enrichment_stays_in_catalog_without_graph(prepared_case):
    case = prepared_case
    enrichment = add_source(case, UHC_FLEX_PRACTITIONER_SOURCE_ID)
    for family in enrichment["admission"]["families"].values():
        family["rows"] = 0
    refresh_receipt(enrichment)

    result = await prepare(case)

    assert enrichment in case.descriptors
    assert [recipe.binding_coordinates.source_id for recipe in result.recipes] == [case.origin.source_id]
    preparation.capture_typed_source_graph.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["completed", "refused", "canceled"])
async def test_four_workers_keep_order_and_drain_pending_captures(prepared_case, monkeypatch, outcome):
    """Keep source order and drain each active capture once on failure or cancellation."""
    descriptors = [
        {
            "coordinates": {"source_id": f"source-{index}"},
            "admission": {"publication": {"claimed_resource_count": index}},
        }
        for index in range(8)
    ]
    runtime = replace(prepared_case.runtime, capture_source_exports=tuple(Mock() for _ in range(4)))
    capacity_reached, blocked = asyncio.Event(), asyncio.Event()
    cleanup_started, allow_cleanup = asyncio.Event(), asyncio.Event()
    active_exports = set()
    started_sources = []

    async def capture(_transaction, capture_runtime, descriptor, _observed_at, _previous):
        """Simulate four independent exports, then finish or refuse the leading source."""
        source_id = descriptor["coordinates"]["source_id"]
        started_sources.append(source_id)
        source_export = capture_runtime.source_export
        assert source_export not in active_exports
        active_exports.add(source_export)
        if len(active_exports) == 4:
            capacity_reached.set()
        try:
            await capacity_reached.wait()
            if outcome == "refused" and source_id == "source-7":
                raise ValueError("source refused")
            if outcome != "completed":
                await blocked.wait()
            return source_id, True
        finally:
            if outcome == "canceled":
                cleanup_started.set()
                await allow_cleanup.wait()
                assert asyncio.current_task().cancelling() == 1
            active_exports.remove(source_export)

    monkeypatch.setattr(preparation, "_capture_or_reuse_fhir", capture)
    largest_source_ids = ["source-7", "source-6", "source-5", "source-4"]
    async with asyncio.timeout(10):
        prepare_fhir = preparation._prepare_fhir_recipes(None, runtime, descriptors, "2026-10-01", ())
        if outcome == "refused":
            with pytest.raises(ValueError, match="source refused"):
                await prepare_fhir
        elif outcome == "canceled":
            capture_task = asyncio.create_task(prepare_fhir)
            await capacity_reached.wait()
            capture_task.cancel()
            await cleanup_started.wait()
            capture_task.cancel()
            allow_cleanup.set()
            with pytest.raises(asyncio.CancelledError):
                await capture_task
        else:
            prepared_recipes = await prepare_fhir
            assert [source_id for source_id, _created in prepared_recipes] == [f"source-{index}" for index in range(8)]
    assert (started_sources[:4] if outcome == "completed" else started_sources) == largest_source_ids
    assert active_exports == set()


def test_date_and_policy_invalidate_stable_source_name(monkeypatch):
    recipe = _recipe()
    descriptor = recipe.source_pin.catalog_descriptor
    before = reuse.imported_source_capture_id(descriptor, "2026-10-01")
    assert before == reuse.imported_source_capture_id(deepcopy(descriptor), "2026-10-01")
    assert before != reuse.imported_source_capture_id(descriptor, "2026-10-02")
    monkeypatch.setattr(reuse, "POLICY", "fhir-direct-network-office-next")
    assert not reuse.is_compatible_typed_recipe(recipe, descriptor, "2026-10-01")
    assert before != reuse.imported_source_capture_id(descriptor, "2026-10-01")


def test_source_semantic_date_remains_effective_across_later_requests():
    recipe = _recipe()
    assert reuse.is_compatible_typed_recipe(recipe, recipe.source_pin.catalog_descriptor, "2026-11-01")
    assert recipe.source_pin.capture["graph"]["membership_as_of"] == "2026-10-01"


@pytest.mark.asyncio
async def test_failed_new_source_keeps_completed_and_predecessor_schemas(prepared_case, monkeypatch):
    case = prepared_case
    retained = _recipe()
    add_source(case, "alias-c")
    add_source(case, "alias-e")
    monkeypatch.setattr(preparation, "predecessor_recipes", AsyncMock(return_value=(retained,)))
    case.state["capture_failure"] = ValueError("source refusal")
    with pytest.raises(ValueError, match="source refusal"):
        await prepare(case)
    assert case.schemas == [case.captured[0]["schema_name"]]
    assert retained.source_pin.capture["schema_name"] not in [
        PREFIX + arguments["capture_options"].expected_anchor.capture_id.replace("-", "")
        for kind, arguments in case.events
        if kind == "capture"
    ]
    assert preparation.capture_typed_source_graph.await_count == 2


@pytest.mark.asyncio
async def test_predecessor_inventory_is_pinned_before_preparation(monkeypatch):
    manifest = SimpleNamespace(manifest_sha256="a" * 64)
    resolve = AsyncMock(return_value=manifest)
    retained = AsyncMock(return_value=(_recipe(),))
    monkeypatch.setattr(reuse, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(reuse, "resolve_retained_registry_source_recipes", retained)
    command_by_field = {"expected_serving_generation": 5, "expected_serving_manifest_sha256": "a" * 64}
    assert await reuse.predecessor_recipes(object(), command_by_field, "registry") == retained.return_value
    command_by_field["expected_serving_manifest_sha256"] = "b" * 64
    with pytest.raises(ValueError, match="predecessor_changed"):
        await reuse.predecessor_recipes(object(), command_by_field, "registry")
    assert retained.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata", [None, "unrelated:4:{}", COMPONENT + ":4:{", COMPONENT + ":5:{}"])
async def test_unknown_or_partial_named_artifact_is_never_overwritten(prepared_case, metadata):
    case = prepared_case
    case.connection.fetchrow.return_value = {"metadata": metadata}
    with pytest.raises(ValueError, match="existing_artifact_invalid"):
        await prepare(case)
    preparation.capture_typed_source_graph.assert_not_awaited()
    assert case.schemas == [] and case.events[-1][0] == "rollback"


@pytest.mark.asyncio
async def test_valid_but_incompatible_named_metadata_is_rejected(prepared_case):
    case = prepared_case
    capture = _recipe().source_pin.capture
    capture["graph"]["office_policy"] = "unsupported"
    capture["capture_sha256"] = digest({key: value for key, value in capture.items() if key != "capture_sha256"})
    case.connection.fetchrow.return_value = {"metadata": COMPONENT + ":4:" + canonical(capture)}
    with pytest.raises(ValueError, match="existing_artifact_invalid"):
        await prepare(case)
    preparation.capture_typed_source_graph.assert_not_awaited()


@pytest.mark.asyncio
async def test_repeated_preparation_component_work_and_timings(prepared_case, monkeypatch, record_property):
    case = prepared_case
    for source_id in ("alias-c", "alias-e"):
        add_source(case, source_id)
    started = perf_counter()
    baseline = await prepare(case)
    baseline_seconds = perf_counter() - started
    baseline_exports = sum(event[0] == "source_open" for event in case.events)
    case.events.clear()
    monkeypatch.setattr(preparation, "predecessor_recipes", AsyncMock(return_value=baseline.recipes))
    preparation.capture_typed_source_graph.reset_mock()
    started = perf_counter()
    repeated = await prepare(case)
    repeated_seconds = perf_counter() - started
    evidence_by_field = {
        "scope": "synthetic three-source preparation component; not full Source or publication",
        "sources": 3,
        "uncached_exports": baseline_exports,
        "repeated_exports": sum(event[0] == "source_open" for event in case.events),
        "uncached_seconds": baseline_seconds,
        "repeated_seconds": repeated_seconds,
    }
    record_property("preparation_component", json.dumps(evidence_by_field, sort_keys=True))
    print(json.dumps(evidence_by_field, sort_keys=True))
    assert baseline_exports == 6 and evidence_by_field["repeated_exports"] == 0
    assert all(before is after for before, after in zip(baseline.recipes, repeated.recipes, strict=True))
    assert repeated.created_schemas == () and len(repeated.reused_schemas) == 3
    preparation.capture_typed_source_graph.assert_not_awaited()


@pytest.mark.asyncio
async def test_committed_preparation_recovers_after_new_connection_without_recapture(prepared_case, monkeypatch):
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    case = prepared_case
    dataset_id = str(uuid4())
    case.descriptors[0]["coordinates"].update(dataset_id=dataset_id, edition_id=dataset_id)
    refresh_receipt(case.descriptors[0])
    case.origins[case.origin.source_id] = replace(case.origin, dataset_id=dataset_id)
    original = preparation.capture_typed_source_graph
    created_schemas = []

    async def capture(factory, writer, **arguments):
        document = await original(factory, writer, **arguments)
        schema = document["schema_name"]
        created_schemas.append(schema)
        await writer.execute('CREATE SCHEMA "' + schema + '"')
        metadata = COMPONENT + ":4:" + canonical(document)
        await writer.execute('COMMENT ON SCHEMA "' + schema + "\" IS '" + metadata.replace("'", "''") + "'")
        return document

    monkeypatch.setattr(preparation, "capture_typed_source_graph", capture)
    normalized_dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")
    case.connection = await asyncpg.connect(normalized_dsn)
    try:
        first = await prepare(case)
        await case.connection.close()
        case.connection = await asyncpg.connect(normalized_dsn)
        case.request_id = uuid4()
        case.events.clear()
        recovered = await prepare(case)
        assert recovered.recipes == first.recipes
        assert recovered.created_schemas == () and recovered.reused_schemas == first.created_schemas
        assert len(created_schemas) == 1
        assert sum(event[0] == "source_open" for event in case.events) == 1
        assert await case.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=$1)", created_schemas[0]
        )
    finally:
        for schema in reversed(created_schemas):
            await case.connection.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')
        assert not await case.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=ANY($1::text[]))", created_schemas
        )
        await case.connection.close()
