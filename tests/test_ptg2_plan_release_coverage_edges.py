# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Release-pinned PTG2 routing edges that must remain fail closed."""

from collections import OrderedDict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    PlanReleaseSnapshotBinding,
)
from tests.ptg2_serving_coverage_paydown_support import FakeResult, FakeSession
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables


def _binding(
    ordinal: int,
    source_key: str,
    snapshot_id: str,
    *,
    plan_id: str | None = None,
    market_type: str = "group",
) -> PlanReleaseSnapshotBinding:
    return PlanReleaseSnapshotBinding(
        binding_ordinal=ordinal,
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id or f"plan-{ordinal}",
        plan_market_type=market_type,
        role="in_network",
        required=True,
    )


def _selection(
    *bindings: PlanReleaseSnapshotBinding,
) -> PlanReleaseServingSelection:
    return PlanReleaseServingSelection(
        serving_revision_id="hpserve_01K123456789ABCDEFGHJKMNPQ",
        plan_release_id="hprelease_01K123456789ABCDEFGHJKMNPQ",
        healthporta_plan_id="hpplan_01K123456789ABCDEFGHJKMNPQ",
        plan_version_id="hpversion_01K123456789ABCDEFGHJKMNPQ",
        release_month="2026-07",
        release_status="published",
        binding_set_digest="digest-release-bindings",
        bindings=tuple(bindings),
    )


def _pagination(**overrides):
    pagination_by_field = {
        "page": 1,
        "limit": 2,
        "offset": 0,
        "source": "page",
    }
    pagination_by_field.update(overrides)
    return SimpleNamespace(**pagination_by_field)


@pytest.mark.asyncio
async def test_multi_provider_release_binds_known_network_and_skips_unknown(
    monkeypatch,
):
    binding = _binding(0, "source-a", "snapshot-a", plan_id="release-plan")
    selection = _selection(binding)
    read_calls = []

    async def read_network(source_key, snapshot_id, npi, args, pagination):
        read_calls.append((source_key, snapshot_id, npi, args, pagination.limit))
        return (
            source_key,
            snapshot_id,
            {
                "items": [{"reported_code_system": "CPT", "reported_code": "99213"}],
                "pagination": {"total": 1, "total_is_exact": True},
                "query": {"source_key": source_key},
            },
        )

    monkeypatch.setattr(
        serving,
        "_search_provider_procedures_network",
        read_network,
    )

    response = await serving._search_multi_ptg2_provider_procedures(
        object(),
        1234567890,
        [("source-a", "snapshot-a"), ("source-b", "snapshot-b")],
        {"plan_release_id": selection.plan_release_id},
        _pagination(limit=1),
        release_selection=selection,
    )

    assert len(read_calls) == 1
    assert read_calls[0][3]["plan_id"] == "release-plan"
    assert read_calls[0][3]["snapshot_id"] == "snapshot-a"
    assert read_calls[0][4] == 2
    assert response["plan_release_id"] == selection.plan_release_id
    assert response["query"]["snapshots"] == ["snapshot-a", "snapshot-b"]


@pytest.mark.asyncio
async def test_plan_release_provider_search_handles_empty_single_and_multi(
    monkeypatch,
):
    pagination = _pagination()
    empty_selection = _selection()
    empty_response = await serving._search_plan_release_provider_procedures(
        object(), 1234567890, {"state": "IL"}, pagination, empty_selection
    )
    assert empty_response["result_state"] == "no_match_in_radius"
    assert empty_response["query"]["npi"] == 1234567890

    single = _binding(0, "source-a", "snapshot-a", plan_id="plan-a")
    snapshot_search = AsyncMock(
        return_value={"items": [{"reported_code": "99213"}], "pagination": {"total": 1}}
    )
    monkeypatch.setattr(
        serving,
        "_search_ptg2_provider_procedures_snapshot",
        snapshot_search,
    )
    single_selection = _selection(single)
    single_response = await serving._search_plan_release_provider_procedures(
        object(), 1234567890, {}, pagination, single_selection
    )
    assert single_response["plan_release_id"] == single_selection.plan_release_id
    assert snapshot_search.await_args.kwargs["snapshot_id"] == "snapshot-a"
    assert snapshot_search.await_args.args[2]["source_key"] == "source-a"

    second = _binding(1, "source-b", "snapshot-b", plan_id="plan-b")
    multi_search = AsyncMock(return_value=None)
    monkeypatch.setattr(
        serving,
        "_search_multi_ptg2_provider_procedures",
        multi_search,
    )
    multi_response = await serving._search_plan_release_provider_procedures(
        object(), 1234567890, {}, pagination, _selection(single, second)
    )
    assert multi_response["result_state"] == "no_matching_rates"
    assert multi_search.await_args.kwargs["release_selection"].bindings == (
        single,
        second,
    )


@pytest.mark.asyncio
async def test_provider_procedure_release_resolution_is_exact_and_fail_closed(
    monkeypatch,
):
    binding = _binding(0, "source-a", "snapshot-a")
    selection = _selection(binding)
    resolver = AsyncMock(side_effect=[None, selection])
    release_search = AsyncMock(return_value={"route": "release"})
    monkeypatch.setattr(serving, "resolve_plan_release_serving", resolver)
    monkeypatch.setattr(
        serving,
        "_search_plan_release_provider_procedures",
        release_search,
    )
    pagination = _pagination()

    conflict = await serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_release_id": selection.plan_release_id, "source_key": "source-a"},
        pagination,
    )
    missing = await serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_release_id": selection.plan_release_id},
        pagination,
    )
    resolved = await serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_release_id": selection.plan_release_id},
        pagination,
    )

    assert conflict is None
    assert missing is None
    assert resolved == {"route": "release"}
    assert resolver.await_count == 2
    release_search.assert_awaited_once()


def test_plan_release_empty_helpers_normalize_ambiguous_and_invalid_values():
    selection = _selection(
        _binding(0, "source-a", "snapshot-a", plan_id="plan-a"),
        _binding(
            1,
            "source-b",
            "snapshot-b",
            plan_id="plan-b",
            market_type="individual",
        ),
    )
    pagination = _pagination(limit=0, offset=-4, page=0)
    response = serving._plan_release_response_or_no_match(
        {"items": [], "pagination": {"total": "invalid"}},
        selection,
        {"code": "", "q": "MRI", "lat": 41.0},
        pagination,
        npi=1234567890,
    )

    assert response["result_state"] == "no_match_in_radius"
    assert response["pagination"] == {
        "total": 0,
        "limit": 25,
        "offset": 0,
        "page": 1,
        "has_more": False,
    }
    assert response["query"]["plan_id"] is None
    assert response["query"]["plan_market_type"] is None
    assert response["query"]["source_key"] is None
    assert response["query"]["code"] is None

    matched = serving._plan_release_response_or_no_match(
        {"items": [{"reported_code": "99213"}], "pagination": {"total": 1}},
        selection,
        {},
        _pagination(),
    )
    assert matched["items"] == [{"reported_code": "99213"}]
    assert matched["serving_revision_id"] == selection.serving_revision_id


def _current_tables():
    return strict_v3_tables(
        snapshot_id="snapshot-a",
        shared_snapshot_key=41,
        code_count=2,
        source_count=1,
        coverage_scope_id="scope-a",
        plan_id="plan-a",
        plan_market_type="group",
        source_key="source-a",
        audit_sample={"sample_digest": "audit-digest"},
        source_set={"raw_container_sha256_digest": "source-digest"},
    )


def _current_table_row(**overrides):
    row_by_field = {
        "snapshot_id": "snapshot-a",
        "snapshot_key": "41",
        "storage_generation": serving.PTG2_V3_SHARED_GENERATION,
        "layout_snapshot_key": 41,
        "layout_code_count": "2",
        "layout_source_count": 1,
        "snapshot_coverage_scope_id": "scope-a",
        "layout_coverage_scope_id": "scope-a",
        "attested_coverage_scope_id": "scope-a",
        "snapshot_plan_id": "plan-a",
        "snapshot_plan_market_type": "group",
        "attested_source_key": "source-a",
        "attested_audit_sample_digest": "audit-digest",
        "attested_source_set_digest": "source-digest",
    }
    row_by_field.update(overrides)
    return row_by_field


def test_network_serving_table_currentness_accepts_exact_chain_only():
    tables = _current_tables()
    assert serving._is_network_serving_tables_current(
        tables, _current_table_row()
    )
    assert not serving._is_network_serving_tables_current(
        tables, _current_table_row(snapshot_key="invalid")
    )
    assert not serving._is_network_serving_tables_current(
        tables, _current_table_row(storage_generation="shared_blocks_v4")
    )
    assert not serving._is_network_serving_tables_current(
        tables, _current_table_row(attested_source_set_digest="other")
    )


@pytest.mark.asyncio
async def test_cached_network_table_validation_covers_empty_unknown_and_exact():
    tables = _current_tables()
    empty_session = FakeSession()
    assert await serving._is_cached_network_serving_tables_current(
        empty_session, {}
    )
    assert empty_session.calls == []

    exact_session = FakeSession([FakeResult([_current_table_row()])])
    assert await serving._is_cached_network_serving_tables_current(
        exact_session, {"snapshot-a": tables}
    )

    unknown_session = FakeSession(
        [FakeResult([_current_table_row(snapshot_id="snapshot-b")])]
    )
    assert not await serving._is_cached_network_serving_tables_current(
        unknown_session, {"snapshot-a": tables}
    )

    missing_session = FakeSession([FakeResult()])
    assert not await serving._is_cached_network_serving_tables_current(
        missing_session, {"snapshot-a": tables}
    )


@pytest.mark.asyncio
async def test_network_table_loader_reuses_current_cache_and_deduplicates(
    monkeypatch,
):
    cached = _current_tables()
    cache = OrderedDict({"snapshot-a": cached})
    monkeypatch.setattr(serving, "_PTG2_NETWORK_SERVING_TABLES_CACHE", cache)
    validate_cache = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(
        serving,
        "_is_cached_network_serving_tables_current",
        validate_cache,
    )
    load_tables = AsyncMock(
        side_effect=lambda _session, snapshot_id: strict_v3_tables(
            snapshot_id=snapshot_id
        )
    )
    monkeypatch.setattr(serving, "snapshot_serving_tables", load_tables)
    session = object()

    tables_by_snapshot = await serving._network_tables_by_snapshot_id(
        session,
        [
            ("source-a", "snapshot-a"),
            ("source-a", "snapshot-a"),
            ("source-b", "snapshot-b"),
        ],
    )

    assert tuple(tables_by_snapshot) == ("snapshot-a", "snapshot-b")
    load_tables.assert_awaited_once_with(session, "snapshot-b")
    assert tuple(cache) == ("snapshot-a", "snapshot-b")

    reloaded_tables = await serving._network_tables_by_snapshot_id(
        session,
        [("source-a", "snapshot-a"), ("source-b", "snapshot-b")],
    )
    assert tuple(reloaded_tables) == ("snapshot-a", "snapshot-b")
    assert load_tables.await_count == 3


def _multi_snapshot_read_outcomes():
    return [
        None,
        [],
        [
            ("ignored", "snapshot-ignored", None),
            (
                "",
                "snapshot-a",
                {
                    "items": [{"provider_name": "Alpha"}],
                    "pagination": {"total": "invalid"},
                    "query": {"plan_id": "plan-0"},
                },
            ),
            (
                "source-b",
                "snapshot-b",
                {
                    "items": [{"provider_name": "Beta"}],
                    "pagination": {"total": 2},
                    "query": {"plan_id": "plan-1"},
                },
            ),
        ],
    ]


@pytest.mark.asyncio
async def test_multi_snapshot_release_handles_failed_empty_and_combined_reads(
    monkeypatch,
):
    selection = _selection(
        _binding(0, "", "snapshot-a"),
        _binding(1, "source-b", "snapshot-b"),
    )
    reader = AsyncMock(side_effect=_multi_snapshot_read_outcomes())
    monkeypatch.setattr(serving, "_read_multi_ptg2_snapshots", reader)
    monkeypatch.setattr(
        serving,
        "_sort_ptg2_manifest_provider_items",
        lambda items, _args, **_kwargs: items,
    )
    network_snapshots = [("", "snapshot-a"), ("source-b", "snapshot-b")]
    pagination = _pagination(limit=1)

    failed = await serving._search_multi_ptg2_snapshots(
        object(), network_snapshots, {}, pagination
    )
    empty = await serving._search_multi_ptg2_snapshots(
        object(), network_snapshots, {}, pagination
    )
    combined = await serving._search_multi_ptg2_snapshots(
        object(),
        network_snapshots,
        {"state": "IL"},
        pagination,
        release_selection=selection,
    )

    assert failed is None
    assert empty is None
    assert combined["items"] == [{"provider_name": "Alpha"}]
    assert "network" not in combined["items"][0]
    assert combined["pagination"]["total"] == 2
    assert combined["pagination"]["has_more"] is True
    assert combined["plan_release_id"] == selection.plan_release_id


@pytest.mark.asyncio
async def test_multi_snapshot_release_reader_binds_each_network_or_fails_closed(
    monkeypatch,
):
    binding_a = _binding(0, "source-a", "snapshot-a", plan_id="plan-a")
    binding_b = _binding(1, "source-b", "snapshot-b", plan_id="plan-b")
    tables_by_snapshot_id = {
        "snapshot-a": strict_v3_tables(snapshot_id="snapshot-a"),
        "snapshot-b": strict_v3_tables(snapshot_id="snapshot-b"),
    }
    monkeypatch.setattr(
        serving,
        "_network_tables_by_snapshot_id",
        AsyncMock(return_value=tables_by_snapshot_id),
    )
    snapshot_search = AsyncMock(
        side_effect=lambda _session, snapshot_id, args, _pagination, **_kwargs: {
            "snapshot_id": snapshot_id,
            "plan_id": args["plan_id"],
        }
    )
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", snapshot_search)
    networks = [("source-a", "snapshot-a"), ("source-b", "snapshot-b")]

    complete = await serving._read_multi_ptg2_snapshots(
        object(), networks, {}, _pagination(), _selection(binding_a, binding_b)
    )
    assert [response[2]["plan_id"] for response in complete] == [
        "plan-a",
        "plan-b",
    ]
    assert snapshot_search.await_args.kwargs[
        "serving_tables"
    ] is tables_by_snapshot_id["snapshot-b"]

    snapshot_search.reset_mock()
    incomplete = await serving._read_multi_ptg2_snapshots(
        object(), networks, {}, _pagination(), _selection(binding_a)
    )
    assert incomplete is None
    assert snapshot_search.await_count == 1
