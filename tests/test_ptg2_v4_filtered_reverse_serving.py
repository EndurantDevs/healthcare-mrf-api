# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact inferred-taxonomy serving through the V4 scoped reverse graph."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts import ptg2_v4_taxonomy_candidates as taxonomy
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)


def _projection_fixture() -> tuple[
    dict[str, object],
    taxonomy.V4InferredTaxonomyCandidates,
]:
    return _projection_fixture_for(tuple(range(22)), {})


def _projection_fixture_for(
    npi_keys: tuple[int, ...],
    npi_keys_by_pattern: dict[int, tuple[int, ...]],
) -> tuple[
    dict[str, object],
    taxonomy.V4InferredTaxonomyCandidates,
]:
    """Build matching sealed candidate and manifest fixtures for one layout."""

    rule = serving._inferred_provider_taxonomy_rule(
        {"code_system": "CPT", "code": "70553"}
    )
    assert rule is not None
    rule_digest = taxonomy.inferred_provider_taxonomy_rule_digest(rule)
    member_keys = taxonomy.pack_inferred_taxonomy_npi_keys(npi_keys)
    catalog_digest = b"c" * 32
    member_digest = taxonomy.inferred_taxonomy_member_digest(
        rule_digest,
        member_count=len(npi_keys),
        payload=member_keys,
    )
    representation = (
        taxonomy.PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        if npi_keys_by_pattern
        else taxonomy.PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
    )
    pattern_payload = taxonomy.pack_inferred_taxonomy_pattern_npi_keys(
        npi_keys_by_pattern
    )
    pattern_member_count = sum(
        len(pattern_npi_keys)
        for pattern_npi_keys in npi_keys_by_pattern.values()
    )
    pattern_member_digest = taxonomy.inferred_taxonomy_pattern_member_digest(
        rule_digest,
        representation=representation,
        pattern_count=len(npi_keys_by_pattern),
        pattern_member_count=pattern_member_count,
        payload=pattern_payload,
    )
    projection_manifest = taxonomy._candidate_projection_manifest(
        (
            {
                "rule_digest": rule_digest,
                "catalog_contract": (
                    taxonomy.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
                ),
                "catalog_digest": catalog_digest,
                "vector_format": (
                    taxonomy.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
                ),
                "member_count": len(npi_keys),
                "member_digest": member_digest,
                "member_keys": member_keys,
                "representation": representation,
                "pattern_count": len(npi_keys_by_pattern),
                "pattern_member_count": pattern_member_count,
                "pattern_member_bytes": len(pattern_payload),
                "pattern_member_digest": pattern_member_digest,
                "pattern_member_payload": pattern_payload,
            },
        )
    )
    candidates = taxonomy.V4InferredTaxonomyCandidates(
        rule_digest=rule_digest,
        catalog_digest=catalog_digest,
        member_digest=member_digest,
        member_count=len(npi_keys),
        npi_keys=npi_keys,
        representation=representation,
        pattern_count=len(npi_keys_by_pattern),
        pattern_member_count=pattern_member_count,
        pattern_member_bytes=len(pattern_payload),
        pattern_member_digest=pattern_member_digest,
        npi_keys_by_pattern=npi_keys_by_pattern,
    )
    return projection_manifest, candidates


def _tables(projection_manifest: dict[str, object] | None) -> PTG2ServingTables:
    return PTG2ServingTables(
        snapshot_id="ptg2:209901:filtered-reverse",
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        source_key="synthetic-source",
        provider_graph_v4_inferred_taxonomy_candidates=projection_manifest,
    )


def _observe_projection_fixture() -> dict[str, object]:
    rule = serving._inferred_provider_taxonomy_rule(
        {"code_system": "CPT", "code": "70553"}
    )
    assert rule is not None
    rule_digest = taxonomy.inferred_provider_taxonomy_rule_digest(rule)
    member_keys = taxonomy.pack_inferred_taxonomy_npi_keys((0, 1))
    representation = (
        taxonomy.PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
    )
    return taxonomy.shape_v4_inferred_taxonomy_projection_manifest(
        (
            {
                "rule_digest": rule_digest,
                "catalog_contract": (
                    taxonomy.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT
                ),
                "catalog_digest": b"o" * 32,
                "vector_format": (
                    taxonomy.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT
                ),
                "member_count": 2,
                "member_digest": taxonomy.inferred_taxonomy_member_digest(
                    rule_digest,
                    member_count=2,
                    payload=member_keys,
                ),
                "member_keys": member_keys,
                "representation": representation,
                "observe_reason": (
                    taxonomy.PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON
                ),
                "observe_count_lower_bound": 2,
                "pattern_count": 0,
                "pattern_member_count": 0,
                "pattern_member_bytes": 0,
                "pattern_member_digest": (
                    taxonomy.inferred_taxonomy_pattern_member_digest(
                        rule_digest,
                        representation=representation,
                        pattern_count=0,
                        pattern_member_count=0,
                        payload=b"",
                    )
                ),
                "pattern_member_payload": b"",
            },
        ),
        npi_count=2,
        pattern_count=0,
    )


def _provider_set_id(provider_set_key: int) -> str:
    return f"{provider_set_key:032x}"


def _rate_row(provider_set_key: int) -> dict[str, object]:
    return {
        "provider_set_global_id_128": _provider_set_id(provider_set_key),
        "serving_content_hash_128": f"{provider_set_key + 1000:032x}",
        "reported_code_system": "CPT",
        "reported_code": "70553",
        "negotiation_arrangement": "FFS",
        "source_key": 0,
        "provider_count": 1,
        "price_key": provider_set_key,
        "_ptg_provider_set_key": provider_set_key,
    }


@pytest.mark.asyncio
async def test_inferred_taxonomy_v4_uses_exact_scoped_reverse_selection(
    monkeypatch,
) -> None:
    """Resolve 21 matches from 22 candidates and 260 CPT sets exactly."""

    projection_manifest, candidates = _projection_fixture()
    candidate_npis = tuple(1_000_000_001 + index for index in range(22))
    memberships_by_npi = {
        npi: (
            ((index % 14) + 1, ((index + 1) % 14) + 1)
            if index < 19
            else ((index % 14) + 1,)
            if index < 21
            else ()
        )
        for index, npi in enumerate(candidate_npis)
    }
    matching_provider_set_keys = tuple(
        sorted(
            {
                provider_set_key
                for provider_set_keys in memberships_by_npi.values()
                for provider_set_key in provider_set_keys
            }
        )
    )
    assert len(matching_provider_set_keys) == 14
    assert sum(map(len, memberships_by_npi.values())) == 40
    merge_calls: list[tuple[int, ...] | None] = []

    async def merge_rows(*_args, provider_set_keys, **_kwargs):
        normalized_keys = (
            None
            if provider_set_keys is None
            else tuple(sorted(provider_set_keys))
        )
        merge_calls.append(normalized_keys)
        selected_keys = (
            tuple(range(1, 261))
            if normalized_keys is None
            else normalized_keys
        )
        return [_rate_row(provider_set_key) for provider_set_key in selected_keys]

    async def provider_rows(
        *_args,
        npis,
        provider_set_ids_by_npi,
        **_kwargs,
    ):
        return {
            _provider_set_id(provider_set_key): [
                {"npi": npi, "provider_name": f"Provider {npi}"}
                for npi in npis
                if _provider_set_id(provider_set_key)
                in provider_set_ids_by_npi[npi]
            ]
            for provider_set_key in matching_provider_set_keys
        }

    candidate_loader = AsyncMock(return_value=candidates)
    graph_lookup = AsyncMock(return_value=memberships_by_npi)
    taxonomy_scope_calls: list[dict[str, int]] = []

    @contextmanager
    def taxonomy_scope(**limits):
        taxonomy_scope_calls.append(limits)
        yield

    forward_prefix = AsyncMock(
        side_effect=AssertionError("forward provider prefix must not run")
    )
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        candidate_loader,
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        AsyncMock(
            return_value={
                npi_key: candidate_npis[npi_key]
                for npi_key in candidates.npi_keys
            }
        ),
    )
    monkeypatch.setattr(
        serving,
        "_shared_forward_entries_for_code_rows",
        AsyncMock(
            side_effect=AssertionError(
                "direct code scope must use the bounded rate merge"
            )
        ),
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(
        serving,
        "v4_graph_taxonomy_projection_scope",
        taxonomy_scope,
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        merge_rows,
    )
    monkeypatch.setattr(
        serving,
        "_selected_provider_rows_by_set",
        provider_rows,
    )
    monkeypatch.setattr(
        serving,
        "_filtered_provider_npis_for_expansion_set",
        forward_prefix,
    )
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        selection = await serving._strict_cost_provider_expansion_selection(
            object(),
            _tables(projection_manifest),
            code_rows=[{"code_key": 4, "rate_count": 260}],
            args={
                "plan_id": "synthetic-plan",
                "code_system": "CPT",
                "code": "70553",
            },
            snapshot_id="ptg2:209901:filtered-reverse",
            source_trace_set_hash=None,
            network_names=[],
            target_count=26,
            descending=False,
        )
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()

    assert selection.total_lower_bound == 21
    assert selection.exhausted is True
    assert len(selection.row_data) == 14
    assert merge_calls == [None]
    assert candidate_loader.await_args.kwargs["projection_manifest"] == (
        projection_manifest
    )
    assert graph_lookup.await_args.kwargs["allowed_provider_set_keys"] == tuple(
        range(1, 261)
    )
    assert graph_lookup.await_args.kwargs["max_members"] == 65_536
    assert taxonomy_scope_calls == [
        {
            "maximum_members": 131_072,
            "maximum_pages": 256,
            "maximum_bytes": 4_194_304,
            "maximum_batches": 32,
        }
    ]
    assert forward_prefix.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_set_keys", "expected_dimension"),
    (
        ((1,) * 6_701, "code_occurrences"),
        (tuple(range(1, 6_602)), "code_sets"),
    ),
)
async def test_direct_v1_admits_occurrences_and_distinct_sets_separately(
    monkeypatch,
    provider_set_keys,
    expected_dimension,
) -> None:
    """Do not let duplicate occurrences consume the distinct-set budget."""

    projection_manifest, candidates = _projection_fixture()
    candidate_npis = tuple(1_000_000_001 + index for index in range(22))
    graph_lookup = AsyncMock(
        side_effect=AssertionError("over-budget graph work must not start")
    )
    bounded_merge = AsyncMock(
        return_value=[
            _rate_row(provider_set_key)
            for provider_set_key in provider_set_keys
        ]
    )
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(return_value=candidates),
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        AsyncMock(
            return_value={
                npi_key: candidate_npis[npi_key]
                for npi_key in candidates.npi_keys
            }
        ),
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        bounded_merge,
    )
    monkeypatch.setattr(
        serving,
        "_shared_forward_entries_for_code_rows",
        AsyncMock(
            side_effect=AssertionError(
                "direct code scope must use the bounded rate merge"
            )
        ),
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        with pytest.raises(
            serving.PTG2OnlineWorkBudgetExceeded
        ) as exc_info:
            await serving._strict_cost_provider_expansion_selection(
                object(),
                _tables(projection_manifest),
                code_rows=[
                    {"code_key": 4, "rate_count": len(provider_set_keys)}
                ],
                args={
                    "plan_id": "synthetic-plan",
                    "code_system": "CPT",
                    "code": "70553",
                },
                snapshot_id="ptg2:209901:filtered-reverse",
                source_trace_set_hash=None,
                network_names=[],
                target_count=26,
                descending=False,
            )
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()

    assert exc_info.value.dimension == expected_dimension
    assert graph_lookup.await_count == 0
    assert bounded_merge.await_count == 1
    assert bounded_merge.await_args.kwargs["provider_set_keys"] is None
    assert bounded_merge.await_args.kwargs["limit"] == 6_701


def test_pattern_v1_retained_membership_budget_failure_is_typed() -> None:
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        serving._v4_selected_pattern_memberships(
            (1, 2),
            {7: (1, 2)},
            {10: (7,), 11: (7,)},
            {10: _provider_set_id(10), 11: _provider_set_id(11)},
            max_members=3,
        )

    assert exc_info.value.dimension == "retained_memberships"


@pytest.mark.asyncio
async def test_pattern_v1_reference_shape_ranks_without_broad_npi_reverse(
    monkeypatch,
) -> None:
    """Keep 36k candidates factored while probing the 6,448 CPT sets once."""

    candidate_count = 36_224
    provider_set_count = 6_448
    rate_occurrence_count = 6_561
    pattern_count = 45
    candidate_npi_keys = tuple(range(candidate_count))
    npi_keys_by_pattern = {
        pattern_key: tuple(range(pattern_key, candidate_count, pattern_count))
        for pattern_key in range(pattern_count)
    }
    projection_manifest, candidates = _projection_fixture_for(
        candidate_npi_keys,
        npi_keys_by_pattern,
    )
    serving_rows = [
        _rate_row(provider_set_key)
        for provider_set_key in range(1, provider_set_count + 1)
    ]
    serving_rows.extend(
        {
            **_rate_row(provider_set_key),
            "serving_content_hash_128": f"{provider_set_key + 20_000:032x}",
        }
        for provider_set_key in range(
            1,
            rate_occurrence_count - provider_set_count + 1,
        )
    )
    assert len(serving_rows) == rate_occurrence_count
    pattern_keys_by_set = {
        provider_set_key: ((provider_set_key - 1) % pattern_count,)
        for provider_set_key in range(1, provider_set_count + 1)
    }
    selected_value_lookup = AsyncMock(
        side_effect=lambda *_args, npi_keys, **_kwargs: {
            npi_key: 1_000_000_001 + npi_key for npi_key in npi_keys
        }
    )
    merge_rows = AsyncMock(return_value=serving_rows)
    set_pattern_lookup = AsyncMock(return_value=pattern_keys_by_set)
    generic_candidate_reverse = AsyncMock(
        side_effect=AssertionError("broad candidate reverse must not run")
    )
    broad_code_scope = AsyncMock(
        side_effect=AssertionError("forward code scope must not be reread")
    )
    provider_rows = AsyncMock(return_value={})
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(return_value=candidates),
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        selected_value_lookup,
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        merge_rows,
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        set_pattern_lookup,
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        generic_candidate_reverse,
    )
    monkeypatch.setattr(
        serving,
        "_shared_forward_entries_for_code_rows",
        broad_code_scope,
    )
    monkeypatch.setattr(
        serving,
        "_selected_provider_rows_by_set",
        provider_rows,
    )
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        selection = await serving._strict_cost_provider_expansion_selection(
            object(),
            _tables(projection_manifest),
            code_rows=[
                {"code_key": 4, "rate_count": rate_occurrence_count}
            ],
            args={
                "plan_id": "synthetic-plan",
                "code_system": "CPT",
                "code": "70553",
            },
            snapshot_id="ptg2:209901:filtered-reverse",
            source_trace_set_hash=None,
            network_names=[],
            target_count=26,
            descending=False,
        )
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()

    expected_selected_npi_keys = tuple(range(0, 26 * pattern_count, pattern_count))
    assert selection.total_lower_bound == 26
    assert selection.exhausted is False
    assert merge_rows.await_count == 1
    assert merge_rows.await_args.kwargs["provider_set_keys"] is None
    assert merge_rows.await_args.kwargs["limit"] == 6_701
    assert set_pattern_lookup.await_count == 1
    assert set_pattern_lookup.await_args.kwargs["relation"] == "set_patterns"
    assert len(set_pattern_lookup.await_args.kwargs["owner_keys"]) == provider_set_count
    assert set_pattern_lookup.await_args.kwargs["allowed_member_keys"] == tuple(
        range(pattern_count)
    )
    assert selected_value_lookup.await_count == 1
    assert selected_value_lookup.await_args.kwargs["npi_keys"] == (
        expected_selected_npi_keys
    )
    assert len(selected_value_lookup.await_args.kwargs["npi_keys"]) == 26
    assert generic_candidate_reverse.await_count == 0
    assert broad_code_scope.await_count == 0
    assert provider_rows.await_count == 1
    assert len(provider_rows.await_args.kwargs["npis"]) == 26
    assert len(selection.row_data) < len(serving_rows)


@pytest.mark.asyncio
async def test_inferred_taxonomy_v4_missing_candidate_row_fails_closed(
    monkeypatch,
) -> None:
    projection_manifest, _candidates = _projection_fixture()
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(
            side_effect=PTG2ManifestArtifactError(
                "candidate vector is unavailable"
            )
        ),
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="candidate vector is unavailable",
    ):
        await serving._strict_cost_provider_expansion_selection(
            object(),
            _tables(projection_manifest),
            code_rows=[{"code_key": 4, "rate_count": 260}],
            args={"plan_id": "plan", "code_system": "CPT", "code": "70553"},
            snapshot_id="snapshot",
            source_trace_set_hash=None,
            network_names=[],
            target_count=26,
            descending=False,
        )


@pytest.mark.asyncio
async def test_inferred_taxonomy_v4_tampered_candidate_fails_closed(
    monkeypatch,
) -> None:
    projection_manifest, candidates = _projection_fixture()
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(
            return_value=replace(candidates, catalog_digest=b"x" * 32)
        ),
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="changed from their seal",
    ):
        await serving._strict_cost_provider_expansion_selection(
            object(),
            _tables(projection_manifest),
            code_rows=[{"code_key": 4, "rate_count": 260}],
            args={"plan_id": "plan", "code_system": "CPT", "code": "70553"},
            snapshot_id="snapshot",
            source_trace_set_hash=None,
            network_names=[],
            target_count=26,
            descending=False,
        )


def test_inferred_taxonomy_v4_projection_is_optional_for_legacy_snapshot() -> None:
    assert serving._v4_inferred_taxonomy_projection_rule(
        _tables(None),
        {"code_system": "CPT", "code": "70553"},
    ) is None


def test_inferred_taxonomy_v4_explicit_observe_rule_uses_legacy_path(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        taxonomy,
        "PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES",
        1,
    )
    projection_manifest = _observe_projection_fixture()

    assert serving._v4_inferred_taxonomy_projection_rule(
        _tables(projection_manifest),
        {"code_system": "CPT", "code": "70553"},
    ) is None


@pytest.mark.parametrize(
    "explicit_filter",
    (
        {"provider_sex_code": "F"},
        {"taxonomy_codes": "2085R0202X"},
    ),
)
def test_inferred_taxonomy_v4_combined_filter_uses_legacy_path(
    explicit_filter,
) -> None:
    projection_manifest, _candidates = _projection_fixture()
    assert serving._v4_inferred_taxonomy_projection_rule(
        _tables(projection_manifest),
        {
            "code_system": "CPT",
            "code": "70553",
            **explicit_filter,
        },
    ) is None


def test_filtered_reverse_rate_scope_may_be_narrower_after_source_filter() -> None:
    assert serving._v4_filtered_reverse_provider_set_ids(
        [_rate_row(1)],
        (1, 2),
    ) == {1: _provider_set_id(1)}


def test_filtered_reverse_rate_scope_cannot_escape_graph_matches() -> None:
    with pytest.raises(
        PTG2ManifestArtifactError,
        match="escaped its graph matches",
    ):
        serving._v4_filtered_reverse_provider_set_ids(
            [_rate_row(3)],
            (1, 2),
        )
