# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Target-first inferred-taxonomy serving for direct V4 layouts."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from tests.test_ptg2_v4_filtered_reverse_serving import (
    _projection_fixture_for,
    _projection_rule,
    _provider_set_id,
    _rate_row,
    _tables,
)


def test_direct_candidate_prefix_deduplicates_shared_memberships() -> None:
    selected, exhausted = serving._v4_direct_candidate_prefix(
        [_rate_row(1), _rate_row(2), _rate_row(3)],
        {1: (1, 2), 2: (1, 3), 3: (4,)},
        target_count=99,
    )

    assert selected == ((0, 1), (0, 2), (1, 3), (2, 4))
    assert exhausted is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("groups_by_set", "candidate_keys_by_group", "error_match"),
    (
        ({}, {7: (1,)}, "set-group projection is incomplete"),
        ({1: (7,)}, {}, "group-NPI projection is incomplete"),
        (
            {1: (7,)},
            {7: (2,)},
            "escaped its sealed candidate scope",
        ),
    ),
)
async def test_direct_candidate_projection_fails_closed(
    monkeypatch,
    groups_by_set,
    candidate_keys_by_group,
    error_match,
) -> None:
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(return_value=groups_by_set),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        AsyncMock(return_value=candidate_keys_by_group),
    )

    with pytest.raises(PTG2ManifestArtifactError, match=error_match):
        await serving._v4_direct_set_candidates(
            object(),
            snapshot_key=17,
            provider_set_keys=(1,),
            candidate_npi_keys=(1,),
        )


@pytest.mark.asyncio
async def test_direct_selector_returns_empty_without_rate_or_graph_reads(
    monkeypatch,
) -> None:
    projection_manifest, candidates = _projection_fixture_for((), {})
    rate_reader = AsyncMock()
    graph_reader = AsyncMock()
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(return_value=candidates),
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        rate_reader,
    )
    monkeypatch.setattr(
        serving,
        "_v4_direct_set_candidates",
        graph_reader,
    )

    selection = await serving._select_v4_taxonomy_expansion(
        object(),
        _tables(projection_manifest),
        code_rows=[{"code_key": 4, "rate_count": 31_916}],
        args={"code_system": "CPT", "code": "70553"},
        snapshot_id="snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=25,
        descending=False,
        projection_manifest=projection_manifest,
        projection_rule=_projection_rule(projection_manifest),
    )

    assert selection.row_data == []
    assert selection.exhausted is True
    rate_reader.assert_not_awaited()
    graph_reader.assert_not_awaited()


@dataclass(frozen=True)
class _DensePrefixEvidence:
    merge_calls: list[tuple[int, int, tuple[int, ...] | None]]
    relation_members: AsyncMock
    relation_intersections: AsyncMock
    dictionary_lookup: AsyncMock


def _patch_dense_prefix_rates(monkeypatch) -> list[
    tuple[int, int, tuple[int, ...] | None]
]:
    prefix_rows = [_rate_row(provider_set_key) for provider_set_key in range(1, 65)]
    merge_calls: list[tuple[int, int, tuple[int, ...] | None]] = []

    async def merge_rows(
        *_args,
        provider_set_keys,
        limit,
        offset,
        **_kwargs,
    ):
        normalized_keys = (
            None
            if provider_set_keys is None
            else tuple(sorted(provider_set_keys))
        )
        merge_calls.append((limit, offset, normalized_keys))
        if normalized_keys is None:
            return prefix_rows[offset : offset + limit]
        assert normalized_keys == (1,)
        return [_rate_row(1)]

    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        merge_rows,
    )
    return merge_calls


def _patch_dense_prefix_providers(monkeypatch, npi_by_key) -> None:
    monkeypatch.setattr(
        serving,
        "_selected_provider_rows_by_set",
        AsyncMock(
            return_value={
                _provider_set_id(1): [
                    {"npi": npi, "provider_name": f"Provider {npi}"}
                    for npi in npi_by_key.values()
                ]
            }
        ),
    )


def _patch_dense_prefix_graph(
    monkeypatch,
    selected_npi_keys: tuple[int, ...],
) -> _DensePrefixEvidence:
    relation_members = AsyncMock(
        side_effect=lambda *_args, relation, owner_keys, **_kwargs: {
            owner_key: ((7,) if owner_key == 1 else ())
            for owner_key in owner_keys
        }
        if relation == "set_groups_direct"
        else (_ for _ in ()).throw(AssertionError(relation))
    )
    relation_intersections = AsyncMock(
        side_effect=lambda *_args, relation, owner_keys, **_kwargs: {
            owner_key: selected_npi_keys for owner_key in owner_keys
        }
        if relation == "group_npis_exact"
        else (_ for _ in ()).throw(AssertionError(relation))
    )
    npi_by_key = {
        npi_key: 1_000_000_000 + npi_key for npi_key in selected_npi_keys
    }
    membership_keys_by_npi = {
        npi: (1,) for npi in npi_by_key.values()
    }
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        relation_members,
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        relation_intersections,
    )
    selected_dictionary_lookup = AsyncMock(return_value=npi_by_key)
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        selected_dictionary_lookup,
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(return_value=membership_keys_by_npi),
    )
    _patch_dense_prefix_providers(monkeypatch, npi_by_key)
    return _DensePrefixEvidence(
        merge_calls=_patch_dense_prefix_rates(monkeypatch),
        relation_members=relation_members,
        relation_intersections=relation_intersections,
        dictionary_lookup=selected_dictionary_lookup,
    )


@pytest.mark.asyncio
async def test_direct_selector_proves_dense_code_page_from_first_prefix(
    monkeypatch,
) -> None:
    """A broad code reads one page, then completes only selected providers."""

    selected_npi_keys = tuple(range(1, 26))
    projection_manifest, candidates = _projection_fixture_for(
        selected_npi_keys,
        {},
    )
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(return_value=candidates),
    )
    evidence = _patch_dense_prefix_graph(monkeypatch, selected_npi_keys)

    selection = await serving._select_v4_taxonomy_expansion(
        object(),
        _tables(projection_manifest),
        code_rows=[{"code_key": 4, "rate_count": 31_916}],
        args={"code_system": "CPT", "code": "70553"},
        snapshot_id="snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=25,
        descending=False,
        projection_manifest=projection_manifest,
        projection_rule=_projection_rule(projection_manifest),
    )

    assert selection.total_lower_bound == 25
    assert selection.exhausted is False
    assert [
        serving_row["_ptg_provider_set_key"]
        for serving_row in selection.row_data
    ] == [1]
    assert evidence.merge_calls == [(64, 0, None), (6_637, 0, (1,))]
    assert evidence.dictionary_lookup.await_args.kwargs["npi_keys"] == (
        selected_npi_keys
    )
    assert evidence.relation_members.await_args.kwargs["owner_keys"] == tuple(
        range(1, 65)
    )
    assert evidence.relation_intersections.await_args.kwargs == {
        "snapshot_key": 17,
        "relation": "group_npis_exact",
        "owner_keys": (7,),
        "allowed_member_keys": selected_npi_keys,
        "schema_name": "mrf",
        "max_members": None,
    }
