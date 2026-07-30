# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated ordered-prefix serving for direct V4 taxonomy selection."""

from __future__ import annotations

import asyncio
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


def test_direct_prefix_falls_back_before_unknown_tail_can_be_outranked() -> None:
    serving_rows = [_rate_row(1), _rate_row(2)]
    prefix_by_set = {
        1: serving._V4DirectSetPrefix((1,), is_complete=False),
        2: serving._V4DirectSetPrefix((2, 3), is_complete=False),
    }

    assert serving._v4_direct_prefix_fallback_keys(
        serving_rows,
        frozenset(),
        {1: (1,), 2: (2, 3)},
        prefix_by_set,
        target_count=3,
    ) == (1,)
    assert (
        serving._v4_direct_prefix_fallback_keys(
            serving_rows,
            frozenset(),
            {1: (1,), 2: (2, 3)},
            prefix_by_set,
            target_count=1,
        )
        == ()
    )


def test_direct_prefix_treats_complete_and_exact_sets_as_exhaustive() -> None:
    serving_rows = [_rate_row(1), _rate_row(2), _rate_row(3)]
    prefix_by_set = {
        2: serving._V4DirectSetPrefix((), is_complete=True),
        3: serving._V4DirectSetPrefix((3,), is_complete=False),
    }

    assert (
        serving._v4_direct_prefix_fallback_keys(
            serving_rows,
            frozenset({1}),
            {1: (), 2: (), 3: (3,)},
            prefix_by_set,
            target_count=1,
        )
        == ()
    )


def test_direct_fallback_keeps_authenticated_prefix_before_exact_tail() -> None:
    prefix_by_set = {
        1: serving._V4DirectSetPrefix((9, 2), is_complete=False),
    }

    assert serving._v4_direct_merge_fallback(
        prefix_by_set,
        {1: (2, 7, 9), 2: (3,)},
    ) == {1: (9, 2, 7), 2: (3,)}
    with pytest.raises(PTG2ManifestArtifactError, match="fallback disagrees"):
        serving._v4_direct_merge_fallback(prefix_by_set, {1: (2, 7)})


@pytest.mark.asyncio
async def test_direct_update_falls_back_only_earlier_sparse_prefix(
    monkeypatch,
) -> None:
    serving_rows = [_rate_row(1), _rate_row(2)]
    prefix_reader = AsyncMock(
        return_value={
            1: serving._V4DirectSetPrefix((1,), is_complete=False),
            2: serving._V4DirectSetPrefix((2, 3), is_complete=False),
        }
    )
    exact_reader = AsyncMock(return_value={1: (1, 4)})
    monkeypatch.setattr(serving, "_v4_direct_ordered_set_prefixes", prefix_reader)
    monkeypatch.setattr(serving, "_v4_direct_set_candidates", exact_reader)
    context = type(
        "DirectContext",
        (),
        {
            "maximum_code_sets": 2,
            "snapshot_key": 17,
            "candidate_npi_keys": (1, 2, 3, 4),
            "request": type("DirectRequest", (), {"target_count": 3})(),
        },
    )()
    candidate_npi_keys_by_set: dict[int, tuple[int, ...]] = {}

    await serving._v4_direct_update_candidates(
        object(),
        _tables(None),
        context,
        serving_rows,
        candidate_npi_keys_by_set,
    )

    assert candidate_npi_keys_by_set == {1: (1, 4), 2: (2, 3)}
    assert exact_reader.await_args.kwargs["provider_set_keys"] == (1,)


@pytest.mark.asyncio
async def test_direct_ordered_prefix_authentication_preserves_group_order(
    monkeypatch,
) -> None:
    ordered_prefix_keys = (9, 2, 7)
    provider_set_id = _provider_set_id(1)
    provider_metadata = serving._ProviderSetGraphMetadata(
        provider_set_key=1,
        provider_count=3,
        prefix_member_count=3,
        prefix_member_digest=serving._v4_npi_prefix_digest(
            ordered_prefix_keys
        ),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(return_value={provider_set_id: provider_metadata}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_ordered_npi_prefix_overrides",
        AsyncMock(return_value={1: ordered_prefix_keys}),
    )

    prefix_by_set = await serving._v4_direct_ordered_set_prefixes(
        object(),
        _tables(None),
        [{**_rate_row(1), "provider_count": 3}],
        {1: provider_set_id},
        (2, 7, 9, 11),
    )

    assert prefix_by_set == {
        1: serving._V4DirectSetPrefix(
            candidate_npi_keys=ordered_prefix_keys,
            is_complete=True,
        )
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metadata_by_id", "prefix_by_key", "error_match"),
    (
        ({}, {}, "dictionary is incomplete"),
        (
            {
                _provider_set_id(1): serving._ProviderSetGraphMetadata(
                    provider_set_key=1,
                    provider_count=1,
                    prefix_member_count=1,
                    prefix_member_digest=serving._v4_npi_prefix_digest((1,)),
                )
            },
            {1: (2,)},
            "failed authentication",
        ),
    ),
)
async def test_direct_ordered_prefix_fails_closed(
    monkeypatch,
    metadata_by_id,
    prefix_by_key,
    error_match,
) -> None:
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(return_value=metadata_by_id),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_ordered_npi_prefix_overrides",
        AsyncMock(return_value=prefix_by_key),
    )

    with pytest.raises(PTG2ManifestArtifactError, match=error_match):
        await serving._v4_direct_ordered_set_prefixes(
            object(),
            _tables(None),
            [_rate_row(1)],
            {1: _provider_set_id(1)},
            (1, 2),
        )


@pytest.mark.asyncio
async def test_direct_prefix_metadata_validates_only_new_growth_sets(
    monkeypatch,
) -> None:
    provider_set_id = _provider_set_id(2)
    ordered_prefix_keys = (2,)
    provider_metadata = serving._ProviderSetGraphMetadata(
        provider_set_key=2,
        provider_count=1,
        prefix_member_count=1,
        prefix_member_digest=serving._v4_npi_prefix_digest(
            ordered_prefix_keys
        ),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(return_value={provider_set_id: provider_metadata}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_ordered_npi_prefix_overrides",
        AsyncMock(return_value={2: ordered_prefix_keys}),
    )
    existing_serving_rate_by_field = {**_rate_row(1), "provider_count": 999}

    prefix_by_set = await serving._v4_direct_ordered_set_prefixes(
        object(),
        _tables(None),
        [existing_serving_rate_by_field, _rate_row(2)],
        {2: provider_set_id},
        (2,),
    )

    assert prefix_by_set[2].candidate_npi_keys == (2,)


@pytest.mark.asyncio
async def test_direct_ordered_prefix_propagates_cancellation(monkeypatch) -> None:
    provider_set_id = _provider_set_id(1)
    provider_metadata = serving._ProviderSetGraphMetadata(
        provider_set_key=1,
        provider_count=1,
        prefix_member_count=1,
        prefix_member_digest=serving._v4_npi_prefix_digest((1,)),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(return_value={provider_set_id: provider_metadata}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_ordered_npi_prefix_overrides",
        AsyncMock(side_effect=asyncio.CancelledError),
    )

    with pytest.raises(asyncio.CancelledError):
        await serving._v4_direct_ordered_set_prefixes(
            object(),
            _tables(None),
            [_rate_row(1)],
            {1: provider_set_id},
            (1,),
        )


@dataclass(frozen=True)
class _DensePrefixEvidence:
    merge_calls: list[tuple[int, int, tuple[int, ...] | None]]
    relation_members: AsyncMock
    relation_intersections: AsyncMock
    dictionary_lookup: AsyncMock
    prefix_lookup: AsyncMock


def _patch_dense_rate_rows(
    monkeypatch,
    provider_count: int,
) -> list[tuple[int, int, tuple[int, ...] | None]]:
    prefix_rows = [_rate_row(provider_set_key) for provider_set_key in range(1, 65)]
    prefix_rows[0]["provider_count"] = provider_count
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


def _dense_metadata_by_key(
    ordered_prefix_keys: tuple[int, ...],
    provider_count: int,
) -> dict[int, serving._ProviderSetGraphMetadata]:
    return {
        provider_set_key: serving._ProviderSetGraphMetadata(
            provider_set_key=provider_set_key,
            provider_count=provider_count if provider_set_key == 1 else 1,
            prefix_member_count=(
                len(ordered_prefix_keys) if provider_set_key == 1 else None
            ),
            prefix_member_digest=(
                serving._v4_npi_prefix_digest(ordered_prefix_keys)
                if provider_set_key == 1
                else None
            ),
        )
        for provider_set_key in range(1, 65)
    }


def _patch_dense_metadata(
    monkeypatch,
    ordered_prefix_keys: tuple[int, ...],
    provider_count: int,
) -> AsyncMock:
    metadata_by_key = _dense_metadata_by_key(
        ordered_prefix_keys,
        provider_count,
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(
            side_effect=lambda _session, _tables, provider_set_ids: {
                provider_set_id: metadata_by_key[int(provider_set_id, 16)]
                for provider_set_id in provider_set_ids
            }
        ),
    )
    prefix_lookup = AsyncMock(return_value={1: ordered_prefix_keys})
    monkeypatch.setattr(
        serving,
        "lookup_v4_ordered_npi_prefix_overrides",
        prefix_lookup,
    )
    return prefix_lookup


def _patch_dense_selected_providers(
    monkeypatch,
    npi_by_key: dict[int, int],
) -> None:
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


def _patch_dense_completion(
    monkeypatch,
    selected_npi_keys: tuple[int, ...],
) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
    relation_members = AsyncMock()
    relation_intersections = AsyncMock()
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
    npi_by_key = {
        npi_key: 1_000_000_000 + npi_key for npi_key in selected_npi_keys
    }
    dictionary_lookup = AsyncMock(return_value=npi_by_key)
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        dictionary_lookup,
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(return_value={npi: (1,) for npi in npi_by_key.values()}),
    )
    _patch_dense_selected_providers(monkeypatch, npi_by_key)
    return relation_members, relation_intersections, dictionary_lookup


def _patch_dense_prefix_path(
    monkeypatch,
    selected_npi_keys: tuple[int, ...],
    ordered_prefix_keys: tuple[int, ...],
    provider_count: int,
) -> _DensePrefixEvidence:
    """Install authenticated prefix collaborators and exact completion fakes."""

    relation_members, relation_intersections, dictionary_lookup = (
        _patch_dense_completion(monkeypatch, selected_npi_keys)
    )
    return _DensePrefixEvidence(
        merge_calls=_patch_dense_rate_rows(monkeypatch, provider_count),
        relation_members=relation_members,
        relation_intersections=relation_intersections,
        dictionary_lookup=dictionary_lookup,
        prefix_lookup=_patch_dense_metadata(
            monkeypatch,
            ordered_prefix_keys,
            provider_count,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("target_count", (1, 25))
async def test_direct_selector_proves_dense_code_page_from_first_prefix(
    monkeypatch,
    target_count,
) -> None:
    """Prove a broad result page from one authenticated ordered override."""

    selected_npi_keys = tuple(range(1, 26))
    ordered_prefix_keys = tuple(range(1, 202))
    projection_manifest, candidates = _projection_fixture_for(
        selected_npi_keys,
        {},
    )
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(return_value=candidates),
    )
    evidence = _patch_dense_prefix_path(
        monkeypatch,
        selected_npi_keys[:target_count],
        ordered_prefix_keys,
        1_000,
    )

    selection = await serving._select_v4_taxonomy_expansion(
        object(),
        _tables(projection_manifest),
        code_rows=[{"code_key": 4, "rate_count": 31_916}],
        args={"code_system": "CPT", "code": "70553"},
        snapshot_id="snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=target_count,
        descending=False,
        projection_manifest=projection_manifest,
        projection_rule=_projection_rule(projection_manifest),
    )

    assert selection.total_lower_bound == target_count
    assert selection.exhausted is False
    assert [
        serving_row["_ptg_provider_set_key"]
        for serving_row in selection.row_data
    ] == [1]
    assert evidence.merge_calls == [(64, 0, None), (6_637, 0, (1,))]
    assert evidence.dictionary_lookup.await_args.kwargs["npi_keys"] == (
        selected_npi_keys[:target_count]
    )
    evidence.relation_members.assert_not_awaited()
    evidence.relation_intersections.assert_not_awaited()
    assert evidence.prefix_lookup.await_args.kwargs == {
        "snapshot_key": 17,
        "provider_set_keys": (1,),
        "schema_name": "mrf",
        "max_members": 201,
    }
