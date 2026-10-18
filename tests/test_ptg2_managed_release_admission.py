# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native release orchestration keeps only admitted contributors before reads."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables
from tests.test_plan_pricing_state_scan_policy import _scope
from tests.test_ptg2_plan_release_coverage_edges import _binding, _pagination, _selection


@pytest.fixture(autouse=True)
def managed_scope():
    with _scope():
        yield


def _prepared(monkeypatch, positions):
    bindings = tuple(_binding(index, f"source-{index}", f"snapshot-{index}") for index in range(3))
    tables = tuple((binding.snapshot_id, strict_v3_tables(snapshot_id=binding.snapshot_id)) for binding in bindings)
    selection = replace(_selection(*bindings), _validated_serving_tables=tables)
    admitted_bindings = tuple(bindings[position] for position in positions)
    admission = AsyncMock(return_value=admitted_bindings)
    monkeypatch.setattr(serving, "admitted_plan_release_bindings", admission)
    return SimpleNamespace(selection=selection, bindings=bindings, admitted=admitted_bindings, admission=admission)


def _page(snapshot):
    return {
        "items": [{"npi": 1000000000 + int(snapshot[-1]), "negotiated_rate": 10, "reported_code": "99213"}],
        "pagination": {"total": 1, "total_lower_bound": 1, "total_is_exact": True},
        "query": {"snapshot_id": snapshot, "code": "99213", "order": "asc"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("positions", [(), (1,), (1, 2), (0, 1, 2)])
async def test_native_full_release_admits_before_counts_and_keeps_complete_selection(monkeypatch, positions):
    prepared = _prepared(monkeypatch, positions)
    session = object()
    reads = []

    async def read(actual_session, snapshot, args, pagination, *, serving_tables=None):
        assert actual_session is session and prepared.admission.await_count == 1
        assert serving_tables is prepared.selection.serving_tables_for_snapshot(snapshot)
        assert args["snapshot_id"] == snapshot
        reads.append(snapshot)
        return _page(snapshot)

    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", read)
    response = await serving._search_established_plan_release_index(
        session, {"code": "99213", "include_sources": "true"}, _pagination(limit=10), prepared.selection
    )
    prepared.admission.assert_awaited_once_with(session, prepared.selection)
    expected_snapshots = [binding.snapshot_id for binding in prepared.admitted]
    assert reads == expected_snapshots
    assert response["pagination"]["total"] == len(expected_snapshots)
    assert response["in_network_snapshot_ids"] == expected_snapshots
    assert response["query"]["resolved_snapshot_ids"] == expected_snapshots
    assert prepared.selection.bindings == prepared.bindings
    assert len(prepared.selection.network_tables_by_snapshot()) == 3
    assert response["binding_set_digest"] == prepared.selection.binding_set_digest
    if not positions:
        assert response["items"] == [] and response["query"]["snapshots"] == []
        assert all(binding.source_key not in json.dumps(response) for binding in prepared.bindings)


@pytest.mark.asyncio
async def test_partial_admission_uses_complete_contributor_cache_without_weakening_full_cache(monkeypatch):
    prepared = _prepared(monkeypatch, (1,))
    incomplete = replace(prepared.selection, _validated_serving_tables=prepared.selection._validated_serving_tables[1:])
    read = AsyncMock(return_value=_page("snapshot-1"))
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", read)
    response = await serving._search_established_plan_release_index(object(), {}, _pagination(), incomplete)
    assert response["query"]["resolved_snapshot_ids"] == ["snapshot-1"]
    assert len(incomplete.bindings) == 3 and incomplete.network_tables_by_snapshot() is None
    read.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("positions", [(), (1,), (1, 2)])
async def test_managed_provider_procedures_use_one_session_and_admitted_sources(monkeypatch, positions):
    prepared = _prepared(monkeypatch, positions)
    session = object()
    reads = []

    async def read(actual_session, npi, args, pagination, *, snapshot_id, serving_tables=None):
        assert actual_session is session and npi == 1000000000
        assert prepared.admission.await_count == 1 and args["snapshot_id"] == snapshot_id
        assert serving_tables is prepared.selection.serving_tables_for_snapshot(snapshot_id)
        reads.append(snapshot_id)
        return _page(snapshot_id)

    forbidden = AsyncMock(side_effect=AssertionError("managed reads cannot open independent snapshots"))
    monkeypatch.setattr(serving, "_search_ptg2_provider_procedures_snapshot", read)
    monkeypatch.setattr(serving.sa_db, "reader_session", forbidden)
    monkeypatch.setattr(serving, "_gather_ptg2_network_reads", forbidden)
    response = await serving._search_plan_release_provider_procedures(
        session, 1000000000, {"code": "99213", "include_sources": "true"}, _pagination(limit=10), prepared.selection
    )
    expected_snapshots = [binding.snapshot_id for binding in prepared.admitted]
    assert reads == expected_snapshots and response["in_network_snapshot_ids"] == expected_snapshots
    assert response["pagination"]["total"] == len(expected_snapshots)
    assert prepared.selection.bindings == prepared.bindings
    forbidden.assert_not_awaited()
    if not positions:
        assert response["items"] == [] and response["query"]["snapshots"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("procedure", [False, True])
@pytest.mark.parametrize("failure", [serving.PlanPricingProjectionUnavailable("unavailable"), asyncio.CancelledError()])
async def test_failed_or_cancelled_admission_never_reads_original_contributors(monkeypatch, procedure, failure):
    prepared = _prepared(monkeypatch, (1,))
    prepared.admission.side_effect = failure
    reader = AsyncMock()
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", reader)
    monkeypatch.setattr(serving, "_search_ptg2_provider_procedures_snapshot", reader)
    with pytest.raises(type(failure)) as caught:
        if procedure:
            await serving._search_plan_release_provider_procedures(
                object(), 1000000000, {}, _pagination(), prepared.selection
            )
        else:
            await serving._search_established_plan_release_index(object(), {}, _pagination(), prepared.selection)
    assert caught.value is failure
    reader.assert_not_awaited()


def test_contributor_metadata_does_not_rewrite_immutable_release_or_allowed_amount_contract():
    bindings = (_binding(0, "source-a", "snapshot-a"), _binding(1, "source-b", "snapshot-b"))
    selection = _selection(*bindings)
    response = serving.annotate_plan_release_response({}, selection, contributing_bindings=(bindings[1],))
    assert response["in_network_snapshot_ids"] == response["query"]["resolved_snapshot_ids"] == ["snapshot-b"]
    assert response["binding_set_digest"] == selection.binding_set_digest
    assert selection.bindings == bindings and selection.response_metadata()["in_network_snapshot_ids"] == [
        "snapshot-a",
        "snapshot-b",
    ]
    allowed = serving.annotate_plan_release_response(
        {"pricing_scope": "plan_scoped_allowed_amounts"}, selection, contributing_bindings=()
    )
    assert "in_network_snapshot_ids" not in allowed and "resolved_snapshot_ids" not in allowed
