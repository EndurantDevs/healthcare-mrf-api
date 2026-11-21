# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Active-generation product selection; synthetic inputs establish no native custody."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from math import ceil
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_fhir_catalog_products as products
from process import registry_fhir_catalog_sync as sync
from process.registry_fhir_catalog_retained import _descriptor
from process.registry_native_network_catalog import native_catalog_descriptor
from tests.test_registry_fhir_catalog_products import ClosedInputs, _binding_document
from tests.test_registry_fhir_catalog_retained import recipe_fixture
from tests.test_registry_imported_source_inventory import native_ptg_source


class GenerationInputs(ClosedInputs):
    def __init__(self):
        super().__init__()
        self.original_generation = self.generation
        self.active_descriptor = _descriptor(self.recipe, None, "https://example.test/fhir")
        self.sources.append(self._source(self.active_descriptor))
        self.sources.sort(key=lambda source: source["generation"])
        self.generation = self._source(self.active_descriptor)["generation"]
        self.is_in_transaction = lambda: True
        self.execute = AsyncMock()
        self.fetchval = AsyncMock(return_value=False)
        for source in self.sources:
            source["accepted_at"] = (
                "2099-01-01T00:00:00+00:00"
                if source["generation"] == self.original_generation
                else "2000-01-01T00:00:00+00:00"
            )


async def selected(connection, generations, *, candidate=None):
    return await products.selected_reviewed_fhir_catalog_product_sources(
        connection,
        observation_schema="closed_example",
        snapshot=connection.snapshot if candidate is None else candidate,
        control_schema="registry_example",
        source_generations=generations,
    )


async def output(connection, generations):
    return [
        (kind, rows)
        async for kind, rows in products.iter_reviewed_fhir_catalog_product_rows(
            connection,
            observation_schema="closed_example",
            snapshot=connection.snapshot,
            control_schema="registry_example",
            companies=(),
            groups=(),
            crosswalk_for=lambda _source_id: (),
            source_generations=generations,
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("original", [False, True])
async def test_pinned_generation_selects_same_approval_key_independently_of_freshness(original):
    connection = GenerationInputs()
    descriptor = connection.descriptor if original else connection.active_descriptor
    if original:
        connection.generation = connection.original_generation
    approved_before = deepcopy(connection.selectors)
    assert connection.descriptor["coordinates"] == connection.active_descriptor["coordinates"]
    assert connection.descriptor["capture_sha256"] == connection.active_descriptor["capture_sha256"]
    assert await selected(connection, (connection.generation,)) == {connection.generation: descriptor}
    rows = await output(connection, (connection.generation,))
    bindings = [row for kind, batch in rows if kind == "binding" for row in batch]
    relationships = [row for kind, batch in rows if kind == "relationship" for row in batch]
    assert len(bindings) == 3 and len(relationships) == 22
    assert {row[4] for row in relationships} == {connection.generation}
    assert {json.loads(row[-1])["evidence_sha256"] for row in bindings} == {"e" * 64}
    assert connection.selectors == approved_before
    assert all("registry_fhir_catalog_binding" not in sql for sql, _ in connection.calls)


@pytest.mark.asyncio
async def test_mixed_source_vector_selects_reviewed_products_above_current_source_limit():
    connection = GenerationInputs()
    native_rows = []
    for ordinal in range(999):
        source = native_ptg_source()
        source["source_id"] = source["definitions"][0]["source_key"] = f"source-{ordinal:04d}"
        native_rows.append(connection._source(native_catalog_descriptor(source)))
    connection.sources = sorted(connection.sources + native_rows, key=lambda row: row["generation"])
    candidate = connection.snapshot
    vector = candidate["source_vector_json"]
    vector.sort(key=lambda entry: (entry["source_id"], entry["source_generation"]))
    retained_generations = {connection.original_generation, connection.generation}
    for entry in vector:
        if entry["source_generation"] not in retained_generations:
            entry["status"] = "current"
    selected_generations = tuple(row["generation"] for row in native_rows) + (connection.generation,)
    assert len(vector) == 1001 and len(selected_generations) == 1000
    assert await selected(connection, selected_generations, candidate=candidate) == {
        connection.generation: connection.active_descriptor,
    }
    connection.sources.remove(native_rows[0])
    with pytest.raises(products.FHIRCatalogProductError):
        await selected(connection, selected_generations, candidate=candidate)


@pytest.mark.asyncio
@pytest.mark.parametrize("both", [False, True])
async def test_legacy_or_multiple_active_matches_still_refuse_ambiguity(both):
    connection = GenerationInputs()
    generations = tuple(source["generation"] for source in connection.sources) if both else None
    with pytest.raises(products.FHIRCatalogProductError):
        await selected(connection, generations)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["container", "digest", "element", "duplicate", "unknown"])
async def test_invalid_active_selector_refuses_even_without_approved_products(damage):
    connection = GenerationInputs()
    connection.selectors = []
    generation = connection.generation
    generations = {
        "container": [generation],
        "digest": ("A" * 64,),
        "element": (None,),
        "duplicate": (generation, generation),
        "unknown": ("f" * 64,),
    }[damage]
    with pytest.raises(products.FHIRCatalogProductError):
        await selected(connection, generations)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    ["descriptor", "profile", "source_id", "generation", "accepted_at", "duplicate", "missing", "vector", "shape"],
)
async def test_inactive_rows_cannot_escape_source_validation(damage):
    connection = GenerationInputs()
    candidate = deepcopy(connection.snapshot)
    inactive = next(source for source in connection.sources if source["generation"] == connection.original_generation)
    match damage:
        case "descriptor":
            inactive["descriptor"]["tables"]["resource"]["name"] = "different_resource"
        case "profile" | "source_id" | "generation" | "accepted_at":
            inactive[damage] = {
                "profile": "raw_fhir",
                "source_id": "different-source",
                "generation": "f" * 64,
                "accepted_at": "2026-01-01T00:00:00",
            }[damage]
        case "duplicate":
            connection.sources.append(deepcopy(inactive))
            connection.sources.sort(key=lambda source: source["generation"])
        case "missing":
            connection.sources.remove(inactive)
        case "vector":
            candidate["source_vector_json"] = [
                entry
                for entry in candidate["source_vector_json"]
                if entry["source_generation"] == connection.generation
            ]
        case _:
            candidate["source_vector_json"][0]["unknown"] = True
    with pytest.raises(ValueError):
        await selected(connection, (connection.generation,), candidate=candidate)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["approval_key", "plan_closure", "active_selection"])
async def test_unavailable_active_product_never_falls_back_to_history(missing):
    connection = GenerationInputs()
    generations = (connection.generation,)
    if missing == "approval_key":
        descriptor = _descriptor(recipe_fixture(), None, None)
        connection.sources = [connection._source(connection.descriptor), connection._source(descriptor)]
        connection.sources.sort(key=lambda source: source["generation"])
        connection.generation = connection._source(descriptor)["generation"]
        generations = (connection.generation,)
    elif missing == "plan_closure":
        connection.invalid_closure = True
    else:
        generations = ()
    with pytest.raises(products.FHIRCatalogProductError):
        await output(connection, generations)
    assert not any("'edge',o.document_json" in sql for sql, _ in connection.calls)


@pytest.mark.asyncio
async def test_valid_selector_with_no_approved_products_stays_empty():
    connection = GenerationInputs()
    connection.selectors = []
    assert await selected(connection, (connection.generation,)) == {}
    assert await output(connection, (connection.generation,)) == []
    assert any("'descriptor',descriptor_json" in sql for sql, _ in connection.calls)


@pytest.mark.asyncio
async def test_thousand_reviewed_products_use_batched_writes_and_collision_checks(monkeypatch):
    connection = GenerationInputs()
    connection.selectors = [_binding_document(connection.descriptor, index, "plan-two") for index in range(1, 1001)]
    writer = AsyncMock()
    monkeypatch.setattr(sync, "_approved_maps", AsyncMock(return_value=((), ())))
    monkeypatch.setattr(sync, "write_fhir_catalog_rows", writer)
    result = await sync.project_reviewed_fhir_catalog_products(
        connection,
        SimpleNamespace(schema_name="closed_example"),
        connection.snapshot,
        grant=SimpleNamespace(control_schema="registry_example", crosswalk_for=lambda _source: ()),
        loader_role="loader_example",
        source_generations=(connection.generation,),
    )
    bindings = [call.args[3] for call in writer.await_args_list if call.args[2].endswith("_binding")]
    relationships = [call.args[3] for call in writer.await_args_list if call.args[2].endswith("_relationship")]
    assert result == {"binding": 1000, "relationship": 2000}
    assert len(bindings) == ceil(1000 / products._PAGE)
    assert len(relationships) == ceil(2000 / products._PAGE)
    assert all(0 < len(batch) <= products._PAGE for batch in bindings + relationships)
    assert {row[1] for batch in bindings for row in batch} == set(range(7, 1007))
    assert len({row[2] for batch in bindings for row in batch}) == 1000
    assert len({row[1] for batch in relationships for row in batch}) == 2000
    assert connection.fetchval.await_count == len(bindings)
    assert [len(call.args[1]) for call in connection.fetchval.await_args_list] == [len(batch) for batch in bindings]
    assert {identifier for call in connection.fetchval.await_args_list for identifier in call.args[1]} == {
        UUID(int=index) for index in range(1, 1001)
    }


@pytest.mark.asyncio
async def test_late_product_collision_propagates_and_caller_transaction_rolls_back_partial_output(monkeypatch):
    connection = GenerationInputs()
    count = products._PAGE + 1
    connection.selectors = [
        _binding_document(connection.descriptor, index, "plan-two") for index in range(1, count + 1)
    ]
    occupied = UUID(int=count)
    connection.fetchval.side_effect = lambda _sql, identifiers: occupied in identifiers
    rows_by_kind = {"binding": [(occupied, "ordinary binding")], "relationship": []}
    before = deepcopy(rows_by_kind)
    attempted_kinds = []

    async def write(_connection, _target, table, rows, **_options):
        kind = table.removeprefix("registry_fhir_catalog_")
        assert kind != "binding" or all(row[2] != occupied for row in rows)
        attempted_kinds.append(kind)
        rows_by_kind[kind].extend(rows)

    @asynccontextmanager
    async def transaction():
        try:
            yield
        except BaseException:
            rows_by_kind.clear()
            rows_by_kind.update(deepcopy(before))
            raise

    monkeypatch.setattr(sync, "_approved_maps", AsyncMock(return_value=((), ())))
    monkeypatch.setattr(sync, "write_fhir_catalog_rows", write)
    with pytest.raises(products.FHIRCatalogProductError):
        async with transaction():
            await sync.project_reviewed_fhir_catalog_products(
                connection,
                SimpleNamespace(schema_name="closed_example"),
                connection.snapshot,
                grant=SimpleNamespace(control_schema="registry_example", crosswalk_for=lambda _source: ()),
                loader_role="loader_example",
                source_generations=(connection.generation,),
            )
    assert "binding" in attempted_kinds and "relationship" in attempted_kinds
    assert connection.fetchval.await_count == 2
    assert rows_by_kind == before
