# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Target-first inferred-taxonomy serving for direct V4 layouts."""

from __future__ import annotations

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
async def test_direct_candidate_projection_accepts_sealed_candidate_scope(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(return_value={1: (7, 8), 2: (8,)}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        AsyncMock(return_value={7: (3, 1), 8: (3, 2)}),
    )

    assert await serving._v4_direct_set_candidates(
        object(),
        snapshot_key=17,
        provider_set_keys=(1, 2),
        candidate_npi_keys=(1, 2, 3),
    ) == {1: (1, 2, 3), 2: (2, 3)}


def test_direct_prefix_metadata_rejects_dictionary_key_drift() -> None:
    provider_set_id = _provider_set_id(1)
    metadata_by_id = {
        provider_set_id: serving._ProviderSetGraphMetadata(
            provider_set_key=2,
            provider_count=1,
        )
    }

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="provider-set keys are inconsistent",
    ):
        serving._v4_direct_prefix_metadata(
            {1: provider_set_id},
            metadata_by_id,
        )


def test_direct_prefix_metadata_rejects_rate_cardinality_drift() -> None:
    metadata_by_key = {
        1: serving._ProviderSetGraphMetadata(
            provider_set_key=1,
            provider_count=1,
        )
    }

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="rate provider count is inconsistent",
    ):
        serving._validate_direct_provider_counts(
            [{**_rate_row(1), "provider_count": 2}],
            metadata_by_key,
        )


def test_direct_prefix_metadata_rejects_declared_prefix_drift() -> None:
    metadata_by_key = {
        1: serving._ProviderSetGraphMetadata(
            provider_set_key=1,
            provider_count=1,
            prefix_member_count=1,
            prefix_member_digest=None,
        )
    }

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="prefix count is inconsistent",
    ):
        serving._v4_direct_expected_prefix_counts(metadata_by_key, 201)


@pytest.mark.asyncio
async def test_direct_prefix_metadata_skips_relation_without_overrides(
    monkeypatch,
) -> None:
    provider_set_id = _provider_set_id(1)
    metadata_by_id = {
        provider_set_id: serving._ProviderSetGraphMetadata(
            provider_set_key=1,
            provider_count=1,
        )
    }
    prefix_reader = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(return_value=metadata_by_id),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_ordered_npi_prefix_overrides",
        prefix_reader,
    )

    assert (
        await serving._v4_direct_ordered_set_prefixes(
            object(),
            _tables(None),
            [_rate_row(1)],
            {1: provider_set_id},
            (1,),
        )
        == {}
    )
    prefix_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_prefix_metadata_requires_every_override_owner(
    monkeypatch,
) -> None:
    provider_set_id = _provider_set_id(1)
    metadata_by_id = {
        provider_set_id: serving._ProviderSetGraphMetadata(
            provider_set_key=1,
            provider_count=1,
            prefix_member_count=1,
            prefix_member_digest=serving._v4_npi_prefix_digest((1,)),
        )
    }
    monkeypatch.setattr(
        serving,
        "_provider_set_metadata_for_ids",
        AsyncMock(return_value=metadata_by_id),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_ordered_npi_prefix_overrides",
        AsyncMock(return_value={}),
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="prefix relation is incomplete",
    ):
        await serving._v4_direct_ordered_set_prefixes(
            object(),
            _tables(None),
            [_rate_row(1)],
            {1: provider_set_id},
            (1,),
        )


@pytest.mark.asyncio
async def test_direct_companion_binding_returns_empty_without_hot_reads(
    monkeypatch,
) -> None:
    """A zero-match CMC binding must not spend the 0L binding's graph budget."""

    projection_manifest, candidates = _projection_fixture_for((), {})
    rate_reader = AsyncMock()
    graph_reader = AsyncMock()
    prefix_reader = AsyncMock()
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
    monkeypatch.setattr(
        serving,
        "_v4_direct_ordered_set_prefixes",
        prefix_reader,
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
    prefix_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_selector_rejects_target_above_sealed_prefix_before_io(
    monkeypatch,
) -> None:
    """Apply the common target cap before loading direct candidates or rates."""

    projection_manifest, _candidates = _projection_fixture_for((1,), {})
    candidate_reader = AsyncMock()
    rate_reader = AsyncMock()
    graph_reader = AsyncMock()
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        candidate_reader,
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

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="exceeds the sealed hot-prefix target",
    ):
        await serving._select_v4_taxonomy_expansion(
            object(),
            _tables(projection_manifest),
            code_rows=[{"code_key": 4, "rate_count": 31_916}],
            args={"code_system": "CPT", "code": "70553"},
            snapshot_id="snapshot",
            source_trace_set_hash=None,
            network_names=[],
            target_count=202,
            descending=False,
            projection_manifest=projection_manifest,
            projection_rule=_projection_rule(projection_manifest),
        )

    candidate_reader.assert_not_awaited()
    rate_reader.assert_not_awaited()
    graph_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_prefix_growth_releases_old_rows_and_keeps_candidates(
    monkeypatch,
) -> None:
    first_rows = [_rate_row(1)]
    final_rows = [_rate_row(1), _rate_row(2)]
    final_prefix = serving._V4DirectPrefix(
        serving_rows=final_rows,
        selected_occurrences=((0, 11), (1, 12)),
        is_candidate_prefix_exhausted=False,
        is_source_exhausted=True,
    )
    candidate_scope_ids: list[int] = []

    async def read_window(
        _session,
        _serving_tables,
        _context,
        rate_window,
        candidate_npi_keys_by_set,
    ):
        candidate_scope_ids.append(id(candidate_npi_keys_by_set))
        if rate_window == 64:
            candidate_npi_keys_by_set[1] = (11,)
            return serving._V4DirectPrefix(
                serving_rows=first_rows,
                selected_occurrences=((0, 11),),
                is_candidate_prefix_exhausted=False,
                is_source_exhausted=False,
            )
        assert rate_window == 128
        assert first_rows == []
        assert candidate_npi_keys_by_set == {1: (11,)}
        candidate_npi_keys_by_set[2] = (12,)
        return final_prefix

    monkeypatch.setattr(serving, "_v4_direct_read_window", read_window)
    context = type(
        "DirectContext",
        (),
        {
            "request": type("DirectRequest", (), {"target_count": 2})(),
            "maximum_occurrences": 128,
            "declared_occurrences": 128,
        },
    )()

    prefix = await serving._v4_direct_ranked_prefix(
        object(),
        object(),
        context,
    )

    assert prefix is final_prefix
    assert prefix.serving_rows == final_rows
    assert prefix.selected_occurrences == ((0, 11), (1, 12))
    assert len(set(candidate_scope_ids)) == 1
