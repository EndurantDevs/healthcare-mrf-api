# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rooted resumes read retained named inputs and refuse work outside a building acquisition."""

import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_rooted_graph_bulk as batches
from process import provider_directory_rooted_graph_source_runtime as runtime
from process.provider_directory_rooted_graph_store_contract import ProviderDirectoryRootedGraphStoreError
from tests.test_named_fhir_source_batches import named_runtime as named_runtime
from tests.test_named_fhir_source_snapshots import named_handoff as named_handoff


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [False, True])
async def test_retained_acquisition_reuses_recorded_input_names(monkeypatch, named_handoff, encoded):
    selected_runtime = SimpleNamespace(candidate_schema="next_candidate", catalog_schema="catalog_schema")
    monkeypatch.setattr(runtime, "current_rooted_graph_runtime", lambda _db=None: selected_runtime)
    original = copy.deepcopy(named_handoff)
    retained = json.dumps(named_handoff, ensure_ascii=False) if encoded else named_handoff
    database = SimpleNamespace(
        scalar=AsyncMock(side_effect=[False, True]),
        all=AsyncMock(return_value=[{"candidate_storage_json": retained, "own_publication": False}]),
    )
    selected = await runtime.rooted_input_relations(database, "retained_acquisition")
    assert selected == original["inputs"]
    assert named_handoff == original
    query = database.scalar.await_args.args[0]
    assert '"snapshot_a"."provider_directory_rooted_graph_acquisition"' in query
    assert database.scalar.await_args.kwargs == {"acquisition_id": "retained_acquisition"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing_publication", "ambiguous_publication", "missing_acquisition"])
async def test_retained_lookup_refuses_missing_or_ambiguous_acquisition(monkeypatch, named_handoff, failure):
    monkeypatch.setattr(
        runtime,
        "current_rooted_graph_runtime",
        lambda _db=None: SimpleNamespace(candidate_schema="next_candidate", catalog_schema="catalog_schema"),
    )
    publications = [{"candidate_storage_json": named_handoff, "own_publication": True}]
    if failure == "missing_publication":
        publications = []
    elif failure == "ambiguous_publication":
        publications *= 2
    database = SimpleNamespace(scalar=AsyncMock(side_effect=[False, False]), all=AsyncMock(return_value=publications))
    with pytest.raises(ValueError, match="rooted_acquisition_changed"):
        await runtime.rooted_input_relations(database, "retained_acquisition")
    assert database.scalar.await_count == (2 if failure == "missing_acquisition" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["result", "initial", "read"])
async def test_unconfigured_rooted_runtime_cannot_enter_data_or_work_paths(monkeypatch, operation):
    monkeypatch.setattr(runtime, "current_rooted_graph_runtime", lambda _db=None: None)
    database = SimpleNamespace(first=AsyncMock(), scalar=AsyncMock(), status=AsyncMock(), all=AsyncMock())
    with pytest.raises((ValueError, RuntimeError)):
        if operation == "result":
            await batches.admit_result_witnesses(database, object(), object(), object())
        elif operation == "initial":
            await batches.copy_initial_root_work(database, "acquisition")
        else:
            await runtime.rooted_input_relations(database, "acquisition")
    for call in (database.first, database.scalar, database.status, database.all):
        call.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["work", "result", "initial"])
async def test_closed_acquisition_refuses_before_frontier_or_accounting(named_runtime, operation):
    database = SimpleNamespace(first=AsyncMock(return_value=None), scalar=AsyncMock(), status=AsyncMock())
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        if operation == "work":
            await batches._named_work_admission(database, "acquisition", "initialize")
        elif operation == "result":
            await batches._lock_live_result(database, SimpleNamespace(acquisition_id="acquisition"))
        else:
            await batches.copy_initial_root_work(database, "acquisition")
    database.scalar.assert_not_awaited()
    database.status.assert_not_awaited()
