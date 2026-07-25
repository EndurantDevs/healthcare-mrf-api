# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_shared_blocks import PTG2SharedBlockError
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)


_PROVIDER_SET_ID = "01" * 16
_NPI = 1234567890


def _rate_row(provider_set_id=_PROVIDER_SET_ID):
    return {
        "provider_set_global_id_128": provider_set_id,
        "serving_content_hash_128": "02" * 16,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "source_key": 0,
    }


async def _select(
    monkeypatch,
    *,
    tables=None,
    code_rows=None,
    args=None,
    target_count=1,
):
    monkeypatch.setattr(
        serving,
        "_v4_inferred_taxonomy_projection_rule",
        lambda *_args: None,
    )
    return await serving._strict_cost_provider_expansion_selection(
        object(),
        tables
        or SimpleNamespace(
            shared_snapshot_key=None,
            source_key="synthetic-source",
            uses_v4_graph=False,
        ),
        code_rows=code_rows or [{"code_key": 7, "rate_count": 1}],
        args=args or {"plan_id": "synthetic-plan"},
        snapshot_id="synthetic-snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=target_count,
        descending=False,
    )


@pytest.mark.asyncio
async def test_provider_expansion_empty_and_unavailable_paths_fail_closed(monkeypatch):
    """Distinguish an empty sealed code from an unavailable prefix reader."""

    empty_selection = await _select(
        monkeypatch,
        code_rows=[{"code_key": 7, "rate_count": 0}],
    )
    assert empty_selection.exhausted is True
    assert empty_selection.row_data == []

    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=None),
    )
    assert await _select(monkeypatch) is None


@pytest.mark.asyncio
async def test_v4_incremental_expansion_can_report_unavailable(monkeypatch):
    """Propagate an unavailable V4 incremental selection without caching it."""

    incremental = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_select_v4_provider_expansion", incremental)
    selection = await _select(
        monkeypatch,
        tables=SimpleNamespace(
            shared_snapshot_key=None,
            source_key="synthetic-source",
            uses_v4_graph=True,
        ),
    )

    assert selection is None
    incremental.assert_awaited_once()


@pytest.mark.asyncio
async def test_provider_expansion_rejects_a_stalled_prefix(monkeypatch):
    """Reject a dense prefix whose geometric expansion makes no progress."""

    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[_rate_row() for _ in range(64)]),
    )
    monkeypatch.setattr(serving, "_provider_npis_for_sets", AsyncMock(return_value={}))
    monkeypatch.setattr(
        serving,
        "_next_provider_expansion_rate_window",
        lambda current_window, **_kwargs: current_window,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="did not make progress"):
        await _select(
            monkeypatch,
            code_rows=[{"code_key": 7, "rate_count": 65}],
            target_count=2,
        )


def _install_ranked_selection(monkeypatch, *, completion_rows, provider_rows):
    merge_rows = AsyncMock(side_effect=[[_rate_row()], completion_rows])
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge_rows)
    monkeypatch.setattr(
        serving,
        "_provider_npis_for_sets",
        AsyncMock(return_value={_PROVIDER_SET_ID: (_NPI,)}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_selected_npis",
        AsyncMock(return_value={_NPI: (_PROVIDER_SET_ID,)}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={_PROVIDER_SET_ID: 7}),
    )
    monkeypatch.setattr(
        serving,
        "_selected_provider_rows_by_set",
        AsyncMock(return_value=provider_rows),
    )
    return merge_rows


@pytest.mark.asyncio
@pytest.mark.parametrize("unavailable_stage", ["completion", "provider"])
async def test_provider_expansion_propagates_completion_unavailability(
    monkeypatch,
    unavailable_stage,
):
    """Return unavailable when either exact completion dependency is unavailable."""

    completion_rows = None if unavailable_stage == "completion" else [_rate_row()]
    provider_rows = None if unavailable_stage == "provider" else {
        _PROVIDER_SET_ID: [{"npi": _NPI}]
    }
    _install_ranked_selection(
        monkeypatch,
        completion_rows=completion_rows,
        provider_rows=provider_rows,
    )

    assert await _select(monkeypatch) is None


@pytest.mark.asyncio
async def test_provider_expansion_rejects_unknown_completion_set(monkeypatch):
    """Reject reverse membership that cannot resolve to the sealed set dictionary."""

    _install_ranked_selection(
        monkeypatch,
        completion_rows=[_rate_row()],
        provider_rows={_PROVIDER_SET_ID: [{"npi": _NPI}]},
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={}),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="unknown provider set"):
        await _select(monkeypatch)


def _install_v4_filtered_selection_dependencies(monkeypatch, scope_keys):
    monkeypatch.setattr(
        serving,
        "_is_ptg2_provider_filter_requested",
        lambda _args: True,
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(side_effect=[[_rate_row()], [_rate_row()]]),
    )
    monkeypatch.setattr(
        serving,
        "_rank_filtered_provider_expansion_prefix",
        AsyncMock(
            return_value=(
                {("npi", str(_NPI), "CPT", "99213", "FFS", "0"): 0},
                (_NPI,),
                (_PROVIDER_SET_ID,),
            )
        ),
    )
    monkeypatch.setattr(
        serving,
        "_shared_forward_entries_for_code_rows",
        AsyncMock(
            return_value=[SimpleNamespace(provider_set_key=key) for key in scope_keys]
        ),
    )
    reverse_memberships = AsyncMock(return_value={_NPI: (_PROVIDER_SET_ID,)})
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_selected_npis",
        reverse_memberships,
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={_PROVIDER_SET_ID: 7}),
    )
    monkeypatch.setattr(
        serving,
        "_selected_provider_rows_by_set",
        AsyncMock(return_value={_PROVIDER_SET_ID: [{"npi": _NPI}]}),
    )
    return reverse_memberships


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_keys", [(), (7,)])
async def test_v4_filtered_expansion_uses_exact_code_scope(monkeypatch, scope_keys):
    """Require and forward the exact V4 code-scoped provider-set keys."""

    reverse_memberships = _install_v4_filtered_selection_dependencies(
        monkeypatch,
        scope_keys,
    )
    tables = SimpleNamespace(
        shared_snapshot_key=None,
        source_key="synthetic-source",
        uses_v4_graph=True,
    )

    if not scope_keys:
        with pytest.raises(PTG2ManifestArtifactError, match="missing its provider sets"):
            await _select(monkeypatch, tables=tables)
        return

    selection = await _select(monkeypatch, tables=tables)
    assert selection is not None
    assert reverse_memberships.await_args.kwargs["allowed_provider_set_keys"] == frozenset(
        scope_keys
    )


@pytest.mark.asyncio
async def test_optional_procedure_lookup_rolls_back_without_hiding_rates():
    """Keep serving data usable when optional catalog enrichment fails."""

    assert await serving._procedure_details_for_rows(object(), []) == {}
    session = FakeSession([RuntimeError("catalog unavailable")])
    details = await serving._procedure_details_for_rows(
        session,
        [{"reported_code_system": "CPT", "reported_code": "99213"}],
    )

    assert details == {}
    assert session.rollback_count == 1


@pytest.mark.asyncio
async def test_source_provenance_accepts_fallback_key_and_translates_reader_error(
    monkeypatch,
):
    """Use the dense source key fallback and translate shared-block failures."""

    fetch = AsyncMock(return_value={3: {"source_key": 3}})
    monkeypatch.setattr(serving, "fetch_snapshot_source_provenance", fetch)
    tables = strict_v3_tables()

    assert await serving._ptg2_source_provenance_for_rows(
        object(), tables, [{"source_key": "3"}]
    ) == {3: {"source_key": 3}}
    fetch.side_effect = PTG2SharedBlockError("sealed source unavailable")
    with pytest.raises(PTG2ManifestArtifactError, match="sealed source unavailable"):
        await serving._ptg2_source_provenance_for_rows(
            object(), tables, [{"source_artifact_key": 3}]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("source_key", [None, True, "not-an-integer"])
async def test_source_provenance_rejects_missing_or_invalid_dense_key(source_key):
    """Reject serving rows that cannot identify one exact source artifact."""

    with pytest.raises(PTG2ManifestArtifactError, match="source provenance|source key"):
        await serving._ptg2_source_provenance_for_rows(
            object(), strict_v3_tables(), [{"source_artifact_key": source_key}]
        )


def test_item_provenance_requires_dense_artifact_key():
    """Do not expose source metadata without its exact dense artifact key."""

    with pytest.raises(PTG2ManifestArtifactError, match="dense artifact key"):
        serving._item_source_provenance({"source_key": None})


def test_compact_provider_payload_preserves_optional_location_window_fields():
    """Preserve the optional distance-window evidence in public shaping."""

    item = serving._compact_item_from_row(
        {
            "npi": _NPI,
            "address_payload": {"city": "Example City", "state": "IL"},
            "distance_miles": 4.5,
            "zip_match_type": "radius",
            "anchor_zip5": "60001",
            "zip_radius_miles": 10,
        },
        {},
    )

    assert item["distance_miles"] == 4.5
    assert item["zip_match_type"] == "radius"
    assert item["anchor_zip5"] == "60001"
    assert item["zip_radius_miles"] == 10


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("network_snapshots", "expected_route"),
    [
        (["first", "second"], "multi"),
        (["only"], "only"),
        ([], None),
    ],
)
async def test_current_plan_route_never_falls_back_to_another_plan(
    monkeypatch,
    network_snapshots,
    expected_route,
):
    """Select all, one, or no current plan networks without global fallback."""

    snapshots = [
        (f"source-{index}", network_snapshot_id)
        for index, network_snapshot_id in enumerate(network_snapshots)
    ]
    monkeypatch.setattr(
        serving,
        "current_network_snapshots_for_plan",
        AsyncMock(return_value=snapshots),
    )
    monkeypatch.setattr(
        serving,
        "_search_multi_ptg2_snapshots",
        AsyncMock(return_value={"route": "multi"}),
    )
    monkeypatch.setattr(
        serving,
        "_search_one_ptg2_snapshot",
        AsyncMock(
            side_effect=lambda _session, snapshot_id, *_args: {"route": snapshot_id}
        ),
    )

    response = await serving.search_current_ptg2_index(
        object(),
        {"plan_id": "synthetic-plan"},
        SimpleNamespace(limit=10, offset=0),
    )

    assert response == (None if expected_route is None else {"route": expected_route})


@pytest.mark.asyncio
async def test_current_explicit_route_requires_a_resolved_snapshot(monkeypatch):
    """Return no match when an explicit route cannot resolve a current snapshot."""

    monkeypatch.setattr(
        serving,
        "resolve_current_ptg2_snapshot_id",
        AsyncMock(return_value=None),
    )
    assert await serving.search_current_ptg2_index(
        object(),
        {"source_key": "synthetic-source"},
        SimpleNamespace(limit=10, offset=0),
    ) is None


@pytest.mark.asyncio
async def test_reverse_code_query_bounds_selected_keys_and_window():
    """Bind selected code keys and a nonnegative reverse-query window."""

    session = FakeSession([FakeResult([])])
    assert await serving._manifest_reverse_code_rows(
        session,
        strict_v3_tables(),
        requested_plan="synthetic-plan",
        code_value="99213",
        code_system="CPT",
        q_text="",
        code_context=None,
        code_keys=(9, 7, 9),
        limit_rows=-1,
        offset_rows=-2,
    ) == []
    params = session.calls[0][0][1]
    assert params["code_keys"] == [7, 9]
    assert params["code_row_limit"] == 0
    assert params["code_row_offset"] == 0


@pytest.mark.asyncio
async def test_unbounded_provider_membership_deduplicates_valid_npis(monkeypatch):
    """Load every member once when no per-set prefix limit is requested."""

    member_ids = (
        serving._ptg2_npi_member_id(_NPI),
        serving._ptg2_npi_member_id(_NPI),
        "not-an-npi",
    )
    monkeypatch.setattr(
        serving,
        "_provider_npi_member_ids_by_set",
        AsyncMock(return_value={_PROVIDER_SET_ID: member_ids}),
    )

    memberships = await serving._provider_npis_for_sets(
        object(),
        strict_v3_tables(),
        (_PROVIDER_SET_ID,),
        limit_per_set=None,
    )
    assert memberships == {_PROVIDER_SET_ID: (_NPI,)}


@pytest.mark.asyncio
async def test_v4_selected_membership_handles_empty_scope_and_missing_dictionary(
    monkeypatch,
):
    """Return an empty exact scope and reject unresolved V4 set keys."""

    tables = SimpleNamespace(uses_v4_graph=True)
    assert await serving._provider_set_ids_for_selected_npis(
        object(),
        tables,
        (_NPI,),
        allowed_provider_set_keys=frozenset(),
    ) == {_NPI: ()}

    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(return_value={_NPI: (7,)}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value={}),
    )
    with pytest.raises(PTG2ManifestArtifactError, match="missing provider-set"):
        await serving._provider_set_ids_for_selected_npis(
            object(),
            tables,
            (_NPI,),
            allowed_provider_set_keys=frozenset({7}),
        )
