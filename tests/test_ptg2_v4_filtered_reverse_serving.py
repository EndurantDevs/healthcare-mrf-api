# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact inferred-taxonomy serving through the V4 scoped reverse graph."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts import ptg2_v4_taxonomy_candidates as taxonomy
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from tests.ptg2_v4_provider_prefix_support import sealed_v4_hot_prefix


def _projection_fixture() -> tuple[
    dict[str, object],
    taxonomy.V4InferredTaxonomyCandidates,
]:
    return _projection_fixture_for(tuple(range(22)), {})


def _candidate_projection_fields(
    candidates: taxonomy.V4InferredTaxonomyCandidates,
    member_keys: bytes,
    pattern_payload: bytes,
) -> dict[str, object]:
    return {
        "rule_digest": candidates.rule_digest,
        "catalog_contract": taxonomy.PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT,
        "catalog_digest": candidates.catalog_digest,
        "vector_format": taxonomy.PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": candidates.member_count,
        "member_digest": candidates.member_digest,
        "member_keys": member_keys,
        "representation": candidates.representation,
        "pattern_count": candidates.pattern_count,
        "pattern_member_count": candidates.pattern_member_count,
        "pattern_member_bytes": candidates.pattern_member_bytes,
        "pattern_member_digest": candidates.pattern_member_digest,
        "pattern_member_payload": pattern_payload,
    }


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
        packed_pattern_payload=pattern_payload,
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
    projection_manifest = taxonomy._candidate_projection_manifest(
        (_candidate_projection_fields(candidates, member_keys, pattern_payload),)
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
        provider_graph_v4_hot_prefix=sealed_v4_hot_prefix(),
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
                        packed_pattern_payload=b"",
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


async def _select_provider_expansion(
    projection_manifest,
    *,
    rate_count,
):
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        return await serving._strict_cost_provider_expansion_selection(
            object(),
            _tables(projection_manifest),
            code_rows=[{"code_key": 4, "rate_count": rate_count}],
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


def _exact_reverse_fixture():
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
    matching_keys = tuple(
        sorted(
            {
                provider_set_key
                for provider_set_keys in memberships_by_npi.values()
                for provider_set_key in provider_set_keys
            }
        )
    )
    assert len(matching_keys) == 14
    assert sum(map(len, memberships_by_npi.values())) == 40
    return projection_manifest, candidates, candidate_npis, memberships_by_npi, matching_keys


def _exact_reverse_provider_rows(matching_provider_set_keys):
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

    return provider_rows


def _exact_reverse_candidate_scope(candidate_npis, memberships_by_npi):
    candidate_key_by_npi = {
        npi: npi_key for npi_key, npi in enumerate(candidate_npis)
    }
    return AsyncMock(
        side_effect=lambda *_args, provider_set_keys, **_kwargs: {
            provider_set_key: tuple(
                candidate_key_by_npi[npi]
                for npi, membership_keys in memberships_by_npi.items()
                if provider_set_key in membership_keys
            )
            for provider_set_key in provider_set_keys
        }
    )


def _patch_empty_direct_prefixes(monkeypatch) -> None:
    monkeypatch.setattr(
        serving,
        "_v4_direct_ordered_set_prefixes",
        AsyncMock(return_value={}),
    )


def _patch_exact_reverse_dependencies(
    monkeypatch,
    candidates,
    candidate_npis,
    memberships_by_npi,
    matching_provider_set_keys,
):
    """Install exact reverse collaborators and return their witnesses."""

    merge_calls: list[tuple[int, tuple[int, ...] | None]] = []

    async def merge_rows(*_args, provider_set_keys, limit, **_kwargs):
        normalized_keys = None if provider_set_keys is None else tuple(sorted(provider_set_keys))
        merge_calls.append((limit, normalized_keys))
        selected_keys = tuple(range(1, 261)) if normalized_keys is None else normalized_keys
        return [
            _rate_row(provider_set_key)
            for provider_set_key in selected_keys[:limit]
        ]

    candidate_loader = AsyncMock(return_value=candidates)
    graph_lookup = AsyncMock(return_value=memberships_by_npi)
    candidate_scope = _exact_reverse_candidate_scope(
        candidate_npis,
        memberships_by_npi,
    )
    taxonomy_scope_calls: list[dict[str, int]] = []

    @contextmanager
    def taxonomy_scope(**limits):
        taxonomy_scope_calls.append(limits)
        yield

    forward_prefix = AsyncMock(side_effect=AssertionError("forward provider prefix must not run"))
    monkeypatch.setattr(serving, "load_v4_inferred_taxonomy_candidates", candidate_loader)
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        AsyncMock(
            side_effect=lambda *_args, npi_keys, **_kwargs: {
                npi_key: candidate_npis[npi_key] for npi_key in npi_keys
            }
        ),
    )
    monkeypatch.setattr(serving, "_shared_forward_entries_for_code_rows", AsyncMock(side_effect=AssertionError("direct code scope must use the bounded rate merge")))
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(serving, "_v4_direct_set_candidates", candidate_scope)
    _patch_empty_direct_prefixes(monkeypatch)
    monkeypatch.setattr(serving, "v4_graph_taxonomy_projection_scope", taxonomy_scope)
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge_rows)
    monkeypatch.setattr(
        serving,
        "_selected_provider_rows_by_set",
        _exact_reverse_provider_rows(matching_provider_set_keys),
    )
    monkeypatch.setattr(serving, "_filtered_provider_npis_for_expansion_set", forward_prefix)
    return candidate_loader, graph_lookup, taxonomy_scope_calls, forward_prefix, merge_calls


@pytest.mark.asyncio
async def test_inferred_taxonomy_v4_uses_exact_scoped_reverse_selection(
    monkeypatch,
) -> None:
    """Resolve 21 matches from 22 candidates and 260 CPT sets exactly."""
    fixture = _exact_reverse_fixture()
    projection_manifest, candidates, candidate_npis, memberships_by_npi, matching_keys = fixture
    patched = _patch_exact_reverse_dependencies(
        monkeypatch, candidates, candidate_npis, memberships_by_npi, matching_keys
    )
    candidate_loader, graph_lookup, taxonomy_scope_calls, forward_prefix, merge_calls = patched
    selection = await _select_provider_expansion(projection_manifest, rate_count=260)

    assert selection.total_lower_bound == 21
    assert selection.exhausted is True
    assert len(selection.row_data) == 14
    assert merge_calls == [
        (64, None),
        (128, None),
        (256, None),
        (260, None),
    ]
    assert candidate_loader.await_args.kwargs["projection_manifest"] == (
        projection_manifest
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


def _patch_direct_budget_dependencies(
    monkeypatch,
    candidates,
    provider_set_keys,
) -> tuple[AsyncMock, AsyncMock]:
    graph_lookup = AsyncMock(
        side_effect=AssertionError("over-budget graph work must not start")
    )
    bounded_merge = AsyncMock(
        side_effect=lambda *_args, limit, **_kwargs: [
            _rate_row(provider_set_key)
            for provider_set_key in provider_set_keys[:limit]
        ]
    )
    candidate_scope = AsyncMock(
        side_effect=lambda *_args, provider_set_keys, **_kwargs: {
            provider_set_key: () for provider_set_key in provider_set_keys
        }
    )
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(return_value=candidates),
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        bounded_merge,
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(serving, "_v4_direct_set_candidates", candidate_scope)
    _patch_empty_direct_prefixes(monkeypatch)
    return graph_lookup, bounded_merge


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
    graph_lookup, bounded_merge = _patch_direct_budget_dependencies(
        monkeypatch,
        candidates,
        provider_set_keys,
    )
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await _select_provider_expansion(
            projection_manifest,
            rate_count=len(provider_set_keys),
        )

    assert exc_info.value.dimension == expected_dimension
    assert graph_lookup.await_count == 0
    assert bounded_merge.await_count == 3
    assert bounded_merge.await_args.kwargs["provider_set_keys"] is None
    assert bounded_merge.await_args.kwargs["limit"] == min(
        len(provider_set_keys),
        6_700,
    )
    assert [
        call.kwargs["limit"] for call in bounded_merge.await_args_list
    ] == [
        64,
        1_664,
        min(len(provider_set_keys), 6_700),
    ]


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


@dataclass(frozen=True)
class _PatternReferenceFixture:
    projection_manifest: dict[str, object]
    candidates: taxonomy.V4InferredTaxonomyCandidates
    serving_rows: list[dict[str, object]]
    pattern_keys_by_set: dict[int, tuple[int, ...]]
    expected_selected_npi_keys: tuple[int, ...]
    pattern_count: int
    rate_occurrence_count: int


def _pattern_reference_fixture() -> _PatternReferenceFixture:
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
    return _PatternReferenceFixture(
        projection_manifest=projection_manifest,
        candidates=candidates,
        serving_rows=serving_rows,
        pattern_keys_by_set=pattern_keys_by_set,
        expected_selected_npi_keys=tuple(
            range(0, 26 * pattern_count, pattern_count)
        ),
        pattern_count=pattern_count,
        rate_occurrence_count=rate_occurrence_count,
    )


def _pattern_rate_merge(serving_rows):
    async def merge_rate_rows(
        *_args,
        provider_set_keys,
        limit,
        offset,
        **_kwargs,
    ):
        selected_provider_sets = (
            None
            if provider_set_keys is None
            else frozenset(provider_set_keys)
        )
        eligible_rows = [
            serving_row
            for serving_row in serving_rows
            if selected_provider_sets is None
            or int(serving_row["_ptg_provider_set_key"])
            in selected_provider_sets
        ]
        return eligible_rows[offset : offset + limit]

    return AsyncMock(side_effect=merge_rate_rows)


def _pattern_relation_lookups(pattern_keys_by_set):
    set_pattern_lookup = AsyncMock(
        side_effect=lambda *_args, owner_keys, **_kwargs: {
            provider_set_key: pattern_keys_by_set[provider_set_key]
            for provider_set_key in owner_keys
        }
    )
    pattern_set_lookup = AsyncMock(
        side_effect=lambda *_args, owner_keys, **_kwargs: {
            pattern_key: tuple(
                provider_set_key
                for provider_set_key, set_pattern_keys in pattern_keys_by_set.items()
                if pattern_key in set_pattern_keys
            )
            for pattern_key in owner_keys
        }
    )
    return set_pattern_lookup, pattern_set_lookup


def _patch_pattern_guards(monkeypatch):
    generic_candidate_reverse = AsyncMock(
        side_effect=AssertionError("broad candidate reverse must not run")
    )
    broad_code_scope = AsyncMock(
        side_effect=AssertionError("forward code scope must not be reread")
    )
    provider_rows = AsyncMock(return_value={})
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
    return generic_candidate_reverse, broad_code_scope, provider_rows


def _patch_pattern_reference_dependencies(monkeypatch, fixture):
    selected_value_lookup = AsyncMock(
        side_effect=lambda *_args, npi_keys, **_kwargs: {
            npi_key: 1_000_000_001 + npi_key for npi_key in npi_keys
        }
    )
    merge_rows = _pattern_rate_merge(fixture.serving_rows)
    set_pattern_lookup, pattern_set_lookup = _pattern_relation_lookups(
        fixture.pattern_keys_by_set
    )
    guard_mocks = _patch_pattern_guards(monkeypatch)
    generic_candidate_reverse, broad_code_scope, provider_rows = guard_mocks
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(return_value=fixture.candidates),
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
        "lookup_v4_relation_members",
        pattern_set_lookup,
    )
    return (
        selected_value_lookup,
        merge_rows,
        set_pattern_lookup,
        pattern_set_lookup,
        generic_candidate_reverse,
        broad_code_scope,
        provider_rows,
    )


@pytest.mark.asyncio
async def test_pattern_v1_reference_shape_ranks_without_broad_npi_reverse(
    monkeypatch,
) -> None:
    """Fill the page from the sealed 64-row prefix without a broad reverse."""

    fixture = _pattern_reference_fixture()
    witnesses = _patch_pattern_reference_dependencies(monkeypatch, fixture)
    (
        selected_value_lookup,
        merge_rows,
        set_pattern_lookup,
        pattern_set_lookup,
        generic_candidate_reverse,
        broad_code_scope,
        provider_rows,
    ) = witnesses
    selection = await _select_provider_expansion(
        fixture.projection_manifest,
        rate_count=fixture.rate_occurrence_count,
    )

    assert selection.total_lower_bound == 26
    assert selection.exhausted is False
    assert merge_rows.await_count == 2
    assert merge_rows.await_args_list[0].kwargs["provider_set_keys"] is None
    assert merge_rows.await_args_list[0].kwargs["limit"] == 64
    assert merge_rows.await_args_list[1].kwargs["limit"] == 6_637
    assert len(
        tuple(merge_rows.await_args_list[1].kwargs["provider_set_keys"])
    ) == 144
    assert set_pattern_lookup.await_count == 1
    assert set_pattern_lookup.await_args.kwargs["relation"] == "set_patterns"
    assert len(set_pattern_lookup.await_args.kwargs["owner_keys"]) == 64
    assert set_pattern_lookup.await_args.kwargs["allowed_member_keys"] == tuple(
        range(fixture.pattern_count)
    )
    assert pattern_set_lookup.await_args.kwargs["owner_keys"] == (0,)
    assert selected_value_lookup.await_count == 1
    assert selected_value_lookup.await_args.kwargs["npi_keys"] == (
        fixture.expected_selected_npi_keys
    )
    assert len(selected_value_lookup.await_args.kwargs["npi_keys"]) == 26
    assert generic_candidate_reverse.await_count == 0
    assert broad_code_scope.await_count == 0
    assert provider_rows.await_count == 1
    assert len(provider_rows.await_args.kwargs["npis"]) == 26
    assert len(selection.row_data) < len(fixture.serving_rows)


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


@pytest.mark.asyncio
async def test_inferred_taxonomy_v4_projection_fails_before_graph_read(
    monkeypatch,
) -> None:
    graph_lookup = AsyncMock()
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        graph_lookup,
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await _select_provider_expansion(None, rate_count=260)

    assert exc_info.value.dimension == "inferred_taxonomy_projection"
    graph_lookup.assert_not_awaited()


def test_inferred_taxonomy_v3_keeps_legacy_provider_path() -> None:
    v3_tables = replace(
        _tables(None),
        storage_generation="shared_blocks_v3",
        shared_block_layout="dense_shared_blocks_v3",
    )

    assert serving._v4_inferred_taxonomy_projection_rule(
        v3_tables,
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


def _projection_rule(
    projection_manifest: dict[str, object],
) -> taxonomy.V4InferredTaxonomyProjectionRule:
    resolved = serving._v4_inferred_taxonomy_projection_rule(
        _tables(projection_manifest),
        {"code_system": "CPT", "code": "70553"},
    )
    assert resolved is not None
    return resolved[1]


def test_inferred_taxonomy_v4_rule_loss_fails_closed(monkeypatch) -> None:
    projection_manifest, _candidates = _projection_fixture()
    monkeypatch.setattr(
        serving,
        "_is_inferred_taxonomy_only_provider_filter",
        lambda _args: True,
    )
    monkeypatch.setattr(serving, "_inferred_provider_taxonomy_rule", lambda _args: None)

    with pytest.raises(PTG2ManifestArtifactError, match="lost its rule"):
        serving._v4_inferred_taxonomy_projection_rule(
            _tables(projection_manifest),
            {"code_system": "CPT", "code": "70553"},
        )


@pytest.mark.parametrize(
    ("serving_rows", "error_match"),
    (
        ([{**_rate_row(1), "_ptg_provider_set_key": True}], "invalid provider set"),
        (
            [{**_rate_row(1), "provider_set_global_id_128": None}],
            "invalid provider set",
        ),
        (
            [
                _rate_row(1),
                {
                    **_rate_row(1),
                    "provider_set_global_id_128": _provider_set_id(2),
                },
            ],
            "disagree on provider-set identity",
        ),
    ),
)
def test_filtered_reverse_rate_scope_rejects_invalid_identity(
    serving_rows,
    error_match,
) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match=error_match):
        serving._v4_filtered_reverse_provider_set_ids(serving_rows, None)


def test_inferred_taxonomy_v4_candidate_budgets_are_typed() -> None:
    direct_manifest, direct_candidates = _projection_fixture()
    direct_rule = replace(
        _projection_rule(direct_manifest),
        max_online_inferred_taxonomy_candidates=direct_candidates.member_count - 1,
    )
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as direct_error:
        serving._validate_v4_inferred_taxonomy_candidates(
            direct_candidates,
            direct_rule,
        )
    assert direct_error.value.dimension == "candidate_members"

    pattern_manifest, pattern_candidates = _projection_fixture_for(
        (1, 2),
        {7: (1, 2)},
    )
    pattern_rule = replace(
        _projection_rule(pattern_manifest),
        max_online_candidate_pattern_projection_members=1,
    )
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as pattern_error:
        serving._validate_v4_inferred_taxonomy_candidates(
            pattern_candidates,
            pattern_rule,
        )
    assert pattern_error.value.dimension == "candidate_pattern_projection_members"


def test_inferred_taxonomy_v4_pattern_shape_drift_fails_closed() -> None:
    projection_manifest, candidates = _projection_fixture_for(
        (1, 2),
        {7: (1, 2)},
    )

    with pytest.raises(PTG2ManifestArtifactError, match="pattern projection changed"):
        serving._validate_v4_inferred_taxonomy_candidates(
            replace(candidates, npi_keys_by_pattern={7: (1,)}),
            _projection_rule(projection_manifest),
        )


def test_pattern_candidate_prefix_deduplicates_shared_postings() -> None:
    selected, exhausted = serving._v4_pattern_candidate_prefix(
        [_rate_row(1), _rate_row(2), _rate_row(3)],
        {1: (7, 8), 2: (7,), 3: (9,)},
        {7: (1, 3), 8: (1, 2), 9: (1, 4)},
        target_count=99,
    )

    assert selected == ((0, 1), (0, 2), (0, 3), (2, 4))
    assert exhausted is True


def test_partial_pattern_prefix_caps_speculative_rate_growth() -> None:
    assert serving._next_pattern_rate_window(
        64,
        target_count=11,
        distinct_count=4,
        declared_occurrences=274,
        maximum_occurrences=6_700,
    ) == 80
    assert serving._next_pattern_rate_window(
        64,
        target_count=2,
        distinct_count=0,
        declared_occurrences=100,
        maximum_occurrences=100,
    ) == 100


@pytest.mark.parametrize(
    ("max_members", "npi_keys_by_pattern", "error_match"),
    (
        (-1, {7: (1,)}, "retained-membership cap is invalid"),
        (1, {}, "lost its exact provider-set membership"),
    ),
)
def test_pattern_membership_completion_fails_closed(
    max_members,
    npi_keys_by_pattern,
    error_match,
) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match=error_match):
        serving._v4_selected_pattern_memberships(
            (1,),
            npi_keys_by_pattern,
            {10: (7,)},
            {10: _provider_set_id(10)},
            max_members=max_members,
        )


async def _select_pattern_fixture(
    projection_manifest,
    candidates,
    projection_rule,
    *,
    code_rows=None,
    target_count=1,
    descending=False,
):
    """Run the pattern selector with the shared synthetic request."""
    return await serving._select_v4_pattern_taxonomy_expansion(
        object(),
        _tables(projection_manifest),
        code_rows=(
            [{"code_key": 4, "rate_count": 1}]
            if code_rows is None
            else code_rows
        ),
        args={"code_system": "CPT", "code": "70553"},
        snapshot_id="snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=target_count,
        descending=descending,
        projection_rule=projection_rule,
        candidates=candidates,
    )


def _install_pattern_prefix_fakes(
    monkeypatch,
    serving_rows,
    pattern_keys_by_set,
):
    """Install exact prefix-shaped fakes and return their call records."""

    merge_calls: list[tuple[int, bool, int, tuple[int, ...] | None]] = []
    intersection_calls: list[tuple[int, ...]] = []

    async def merge_rows(
        *_args,
        code_rows,
        provider_set_keys,
        limit,
        offset,
        descending,
        **_kwargs,
    ):
        normalized_provider_set_keys = (
            None
            if provider_set_keys is None
            else tuple(sorted(provider_set_keys))
        )
        merge_calls.append(
            (limit, descending, len(code_rows), normalized_provider_set_keys)
        )
        eligible_rows = (
            serving_rows
            if normalized_provider_set_keys is None
            else [
                serving_row
                for serving_row in serving_rows
                if int(serving_row["_ptg_provider_set_key"])
                in normalized_provider_set_keys
            ]
        )
        return eligible_rows[offset : offset + limit]

    async def intersect_patterns(*_args, owner_keys, **_kwargs):
        normalized_owner_keys = tuple(owner_keys)
        intersection_calls.append(normalized_owner_keys)
        return {
            owner_key: pattern_keys_by_set.get(owner_key, ())
            for owner_key in normalized_owner_keys
        }

    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        merge_rows,
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        intersect_patterns,
    )
    _install_pattern_completion_fakes(monkeypatch, pattern_keys_by_set)
    return merge_calls, intersection_calls


def _install_pattern_completion_fakes(monkeypatch, pattern_keys_by_set):
    """Install selected-pattern completion and enrichment fakes."""

    provider_set_keys_by_pattern: dict[int, list[int]] = {}
    for provider_set_key, pattern_keys in pattern_keys_by_set.items():
        for pattern_key in pattern_keys:
            provider_set_keys_by_pattern.setdefault(pattern_key, []).append(
                provider_set_key
            )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(
            side_effect=lambda *_args, owner_keys, **_kwargs: {
                pattern_key: tuple(
                    provider_set_keys_by_pattern.get(pattern_key, ())
                )
                for pattern_key in owner_keys
            }
        ),
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        AsyncMock(
            side_effect=lambda *_args, npi_keys, **_kwargs: {
                npi_key: 1_000_000_000 + npi_key
                for npi_key in npi_keys
            }
        ),
    )
    monkeypatch.setattr(
        serving,
        "_selected_provider_rows_by_set",
        AsyncMock(return_value={}),
    )


@pytest.mark.asyncio
async def test_pattern_selector_stops_on_authenticated_first_page(
    monkeypatch,
) -> None:
    projection_manifest, candidates = _projection_fixture_for(
        (1, 2, 3),
        {7: (1, 2, 3)},
    )
    serving_rows = [_rate_row(key) for key in range(1, 101)]
    merge_calls, intersection_calls = _install_pattern_prefix_fakes(
        monkeypatch,
        serving_rows,
        {1: (7,)},
    )

    selection = await _select_pattern_fixture(
        projection_manifest,
        candidates,
        _projection_rule(projection_manifest),
        code_rows=[{"code_key": 4, "rate_count": 100}],
        target_count=2,
    )

    assert selection.total_lower_bound == 2
    assert selection.exhausted is False
    assert merge_calls == [
        (64, False, 1, None),
        (6_637, False, 1, (1,)),
    ]
    assert intersection_calls == [tuple(range(1, 65))]


@pytest.mark.asyncio
async def test_pattern_selector_starts_at_authenticated_page_above_64_target(
    monkeypatch,
) -> None:
    npi_keys = tuple(range(1, 101))
    projection_manifest, candidates = _projection_fixture_for(
        npi_keys,
        {7: npi_keys},
    )
    serving_rows = [_rate_row(key) for key in range(1, 101)]
    merge_calls, _intersection_calls = _install_pattern_prefix_fakes(
        monkeypatch,
        serving_rows,
        {1: (7,)},
    )

    selection = await _select_pattern_fixture(
        projection_manifest,
        candidates,
        _projection_rule(projection_manifest),
        code_rows=[{"code_key": 4, "rate_count": 100}],
        target_count=70,
    )

    assert selection.total_lower_bound == 70
    assert merge_calls[0] == (64, False, 1, None)


@pytest.mark.asyncio
async def test_pattern_selector_rejects_unsealed_target_before_reads(
    monkeypatch,
) -> None:
    projection_manifest, candidates = _projection_fixture_for(
        (1,),
        {7: (1,)},
    )
    merge_rows = AsyncMock()
    intersect_patterns = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        merge_rows,
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        intersect_patterns,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="hot-prefix target"):
        await _select_pattern_fixture(
            projection_manifest,
            candidates,
            _projection_rule(projection_manifest),
            code_rows=[{"code_key": 4, "rate_count": 1_000}],
            target_count=202,
        )

    merge_rows.assert_not_awaited()
    intersect_patterns.assert_not_awaited()


@pytest.mark.asyncio
async def test_pattern_selector_grows_until_late_match(monkeypatch) -> None:
    projection_manifest, candidates = _projection_fixture_for(
        (1, 2, 3),
        {7: (1, 2, 3)},
    )
    serving_rows = [_rate_row(key) for key in range(1, 101)]
    merge_calls, intersection_calls = _install_pattern_prefix_fakes(
        monkeypatch,
        serving_rows,
        {70: (7,)},
    )

    selection = await _select_pattern_fixture(
        projection_manifest,
        candidates,
        _projection_rule(projection_manifest),
        code_rows=[{"code_key": 4, "rate_count": 100}],
        target_count=2,
    )

    assert selection.total_lower_bound == 2
    assert selection.exhausted is False
    assert merge_calls == [
        (64, False, 1, None),
        (100, False, 1, None),
    ]
    assert intersection_calls == [tuple(range(1, 65)), tuple(range(65, 101))]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("declared_occurrences", "raises_budget"),
    ((100, False), (101, True)),
)
async def test_pattern_selector_cap_distinguishes_exhausted_source(
    monkeypatch,
    declared_occurrences,
    raises_budget,
) -> None:
    projection_manifest, candidates = _projection_fixture_for(
        (1,),
        {7: (1,)},
    )
    projection_rule = replace(
        _projection_rule(projection_manifest),
        max_online_filtered_reverse_code_occurrences=100,
    )
    serving_rows = [_rate_row(key) for key in range(1, 101)]
    merge_calls, _intersection_calls = _install_pattern_prefix_fakes(
        monkeypatch,
        serving_rows,
        {},
    )

    selection_call = _select_pattern_fixture(
        projection_manifest,
        candidates,
        projection_rule,
        code_rows=[{"code_key": 4, "rate_count": declared_occurrences}],
        target_count=2,
    )
    if raises_budget:
        with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
            await selection_call
        assert exc_info.value.dimension == "code_occurrences"
    else:
        selection = await selection_call
        assert selection.row_data == []
        assert selection.exhausted is True
    assert merge_calls == [
        (64, False, 1, None),
        (100, False, 1, None),
    ]


@pytest.mark.asyncio
async def test_pattern_selector_grows_across_code_variants(monkeypatch) -> None:
    projection_manifest, candidates = _projection_fixture_for(
        (1, 2),
        {7: (1, 2)},
    )
    serving_rows = [_rate_row(key) for key in range(1, 81)]
    merge_calls, _intersection_calls = _install_pattern_prefix_fakes(
        monkeypatch,
        serving_rows,
        {70: (7,)},
    )

    selection = await _select_pattern_fixture(
        projection_manifest,
        candidates,
        _projection_rule(projection_manifest),
        code_rows=[
            {"code_key": 4, "rate_count": 40},
            {"code_key": 5, "rate_count": 40},
        ],
        target_count=2,
    )

    assert selection.total_lower_bound == 2
    assert merge_calls == [
        (64, False, 2, None),
        (80, False, 2, None),
    ]


@pytest.mark.asyncio
async def test_pattern_selector_preserves_descending_prefix(monkeypatch) -> None:
    projection_manifest, candidates = _projection_fixture_for(
        (1, 2),
        {7: (1, 2)},
    )
    serving_rows = [_rate_row(key) for key in range(100, 0, -1)]
    merge_calls, _intersection_calls = _install_pattern_prefix_fakes(
        monkeypatch,
        serving_rows,
        {100: (7,)},
    )

    selection = await _select_pattern_fixture(
        projection_manifest,
        candidates,
        _projection_rule(projection_manifest),
        code_rows=[{"code_key": 4, "rate_count": 100}],
        target_count=2,
        descending=True,
    )

    assert selection.total_lower_bound == 2
    assert merge_calls == [
        (64, True, 1, None),
        (6_637, True, 1, (100,)),
    ]


@pytest.mark.asyncio
async def test_pattern_selector_completes_later_selected_npi_rates(
    monkeypatch,
) -> None:
    selected_npi_keys = tuple(range(1, 26))
    projection_manifest, candidates = _projection_fixture_for(
        selected_npi_keys,
        {7: selected_npi_keys},
    )
    serving_rows = [
        {
            **_rate_row(1),
            "serving_content_hash_128": f"{occurrence:032x}",
            "price_key": occurrence,
        }
        for occurrence in range(1, 65)
    ]
    serving_rows.append(
        {
            **_rate_row(2),
            "serving_content_hash_128": f"{65:032x}",
            "price_key": 65,
        }
    )
    merge_calls, _intersection_calls = _install_pattern_prefix_fakes(
        monkeypatch,
        serving_rows,
        {1: (7,), 2: (7,)},
    )

    selection = await _select_pattern_fixture(
        projection_manifest,
        candidates,
        _projection_rule(projection_manifest),
        code_rows=[{"code_key": 4, "rate_count": 65}],
        target_count=25,
    )

    assert selection.total_lower_bound == 25
    assert any(
        int(completion_row["_ptg_provider_set_key"]) == 2
        and int(completion_row["price_key"]) == 65
        for completion_row in selection.row_data
    )
    assert merge_calls == [
        (64, False, 1, None),
        (6_637, False, 1, (1, 2)),
    ]


@pytest.mark.asyncio
async def test_pattern_selector_rejects_lost_ranked_membership(
    monkeypatch,
) -> None:
    projection_manifest, candidates = _projection_fixture_for(
        (1,),
        {7: (1,)},
    )
    serving_rows = [_rate_row(1), _rate_row(2), *[
        _rate_row(key) for key in range(3, 101)
    ]]
    _install_pattern_prefix_fakes(
        monkeypatch,
        serving_rows,
        {1: (7,), 2: (7,)},
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(return_value={7: (2,)}),
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="completion lost a ranked membership",
    ):
        await _select_pattern_fixture(
            projection_manifest,
            candidates,
            _projection_rule(projection_manifest),
            code_rows=[{"code_key": 4, "rate_count": 100}],
            target_count=1,
        )


@pytest.mark.asyncio
async def test_pattern_selector_charges_completion_after_prefix(
    monkeypatch,
) -> None:
    projection_manifest, candidates = _projection_fixture_for(
        (1,),
        {7: (1,)},
    )
    projection_rule = replace(
        _projection_rule(projection_manifest),
        max_online_filtered_reverse_code_occurrences=64,
    )
    serving_rows = [_rate_row(key) for key in range(1, 101)]
    merge_calls, _intersection_calls = _install_pattern_prefix_fakes(
        monkeypatch,
        serving_rows,
        {1: (7,)},
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await _select_pattern_fixture(
            projection_manifest,
            candidates,
            projection_rule,
            code_rows=[{"code_key": 4, "rate_count": 100}],
            target_count=1,
        )

    assert exc_info.value.dimension == "code_occurrences"
    assert merge_calls == [
        (64, False, 1, None),
        (1, False, 1, (1,)),
    ]


async def _select_direct_fixture(
    projection_manifest,
    projection_rule,
):
    """Run the direct selector with the shared synthetic request."""
    return await serving._select_v4_taxonomy_expansion(
        object(),
        _tables(projection_manifest),
        code_rows=[{"code_key": 4, "rate_count": 1}],
        args={"code_system": "CPT", "code": "70553"},
        snapshot_id="snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=1,
        descending=False,
        projection_manifest=projection_manifest,
        projection_rule=projection_rule,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "error_type", "error_match", "expected_dimension"),
    (
        ("missing_rows", PTG2ManifestArtifactError, "rate rows are unavailable", None),
        ("occurrence_budget", serving.PTG2OnlineWorkBudgetExceeded, None, "code_occurrences"),
        ("set_budget", serving.PTG2OnlineWorkBudgetExceeded, None, "code_sets"),
        ("missing_projection", PTG2ManifestArtifactError, "projection is incomplete", None),
        ("missing_npi", PTG2ManifestArtifactError, "NPI dictionary is incomplete", None),
        ("duplicate_rank", PTG2ManifestArtifactError, "ranking is not unique", None),
        ("missing_enrichment", PTG2ManifestArtifactError, "enrichment is unavailable", None),
    ),
)
async def test_pattern_selector_rejects_broken_sealed_boundaries(
    monkeypatch,
    failure,
    error_type,
    error_match,
    expected_dimension,
) -> None:
    """Reject every corrupted or over-budget pattern selection boundary."""
    projection_manifest, candidates = _projection_fixture_for((1,), {7: (1,)})
    projection_rule = _projection_rule(projection_manifest)
    serving_rows = None if failure == "missing_rows" else [_rate_row(1)]
    pattern_keys_by_set = {} if failure == "missing_projection" else {1: (7,)}
    npi_by_key = {} if failure == "missing_npi" else {1: 1_000_000_001}
    providers_by_set = None if failure == "missing_enrichment" else {}
    if failure == "occurrence_budget":
        projection_rule = replace(
            projection_rule,
            max_online_filtered_reverse_code_occurrences=0,
        )
    if failure == "set_budget":
        projection_rule = replace(
            projection_rule,
            max_online_filtered_reverse_code_sets=0,
        )
    if failure == "duplicate_rank":
        monkeypatch.setattr(
            serving,
            "_v4_pattern_candidate_prefix",
            lambda *_args, **_kwargs: (((0, 1), (0, 1)), False),
        )
    _install_pattern_boundary_fakes(
        monkeypatch,
        serving_rows=serving_rows,
        pattern_keys_by_set=pattern_keys_by_set,
        npi_by_key=npi_by_key,
        providers_by_set=providers_by_set,
    )

    with pytest.raises(error_type, match=error_match) as exc_info:
        await _select_pattern_fixture(
            projection_manifest,
            candidates,
            projection_rule,
        )
    if expected_dimension is not None:
        assert exc_info.value.dimension == expected_dimension


def _install_pattern_boundary_fakes(
    monkeypatch,
    *,
    serving_rows,
    pattern_keys_by_set,
    npi_by_key,
    providers_by_set,
):
    """Install corrupted-boundary fixtures for the pattern selector."""

    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=serving_rows),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        AsyncMock(return_value=pattern_keys_by_set),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(return_value={7: (1,)}),
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        AsyncMock(return_value=npi_by_key),
    )
    monkeypatch.setattr(
        serving,
        "_selected_provider_rows_by_set",
        AsyncMock(return_value=providers_by_set),
    )


@pytest.mark.asyncio
async def test_pattern_selector_returns_exact_empty_selection(
    monkeypatch,
) -> None:
    projection_manifest, candidates = _projection_fixture_for((1,), {7: (1,)})
    serving_rows = [_rate_row(1)]
    pattern_keys_by_set = {1: ()}
    intersection_lookup = AsyncMock(return_value=pattern_keys_by_set)
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=serving_rows),
    )
    monkeypatch.setattr(serving, "lookup_v4_relation_intersections", intersection_lookup)

    selection = await _select_pattern_fixture(
        projection_manifest,
        candidates,
        _projection_rule(projection_manifest),
    )

    assert selection.row_data == []
    assert selection.exhausted is True
    assert intersection_lookup.await_count == 1


@pytest.mark.asyncio
async def test_pattern_selector_rejects_short_authenticated_prefix(
    monkeypatch,
) -> None:
    projection_manifest, candidates = _projection_fixture_for((1,), {7: (1,)})
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[]),
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="rate prefix is incomplete",
    ):
        await _select_pattern_fixture(
            projection_manifest,
            candidates,
            _projection_rule(projection_manifest),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "error_type", "error_match", "expected_dimension"),
    (
        ("missing_npi", PTG2ManifestArtifactError, "NPI dictionary is incomplete", None),
        ("missing_rows", PTG2ManifestArtifactError, "rate rows are unavailable", None),
        ("member_budget", serving.PTG2OnlineWorkBudgetExceeded, None, "retained_memberships"),
        ("graph_error", PTG2SharedBlockError, "different graph error", None),
        ("escaped_scope", PTG2ManifestArtifactError, "lost its exact provider-set membership", None),
        ("missing_enrichment", PTG2ManifestArtifactError, "enrichment is unavailable", None),
    ),
)
async def test_direct_selector_rejects_broken_sealed_boundaries(
    monkeypatch,
    failure,
    error_type,
    error_match,
    expected_dimension,
) -> None:
    """Reject every corrupted or over-budget direct selection boundary."""
    projection_manifest, candidates = _projection_fixture_for((1,), {})
    npi = 1_000_000_001
    npi_by_key = {} if failure == "missing_npi" else {1: npi}
    serving_rows = None if failure == "missing_rows" else [_rate_row(1)]
    provider_set_keys_by_npi = {
        npi: (2,) if failure == "escaped_scope" else (1,)
    }
    graph_error = None
    if failure == "member_budget":
        graph_error = PTG2SharedBlockError(
            "PTG V4 graph selection exceeds max_members"
        )
    if failure == "graph_error":
        graph_error = PTG2SharedBlockError("different graph error")
    graph_lookup = AsyncMock(
        side_effect=graph_error,
        return_value=provider_set_keys_by_npi,
    )
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(return_value=candidates),
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        AsyncMock(return_value=npi_by_key),
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=serving_rows),
    )
    monkeypatch.setattr(
        serving, "_v4_direct_set_candidates", AsyncMock(return_value={1: (1,)})
    )
    _patch_empty_direct_prefixes(monkeypatch)
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(
        serving,
        "_selected_provider_rows_by_set",
        AsyncMock(return_value=None if failure == "missing_enrichment" else {}),
    )

    with pytest.raises(error_type, match=error_match) as exc_info:
        await _select_direct_fixture(
            projection_manifest,
            _projection_rule(projection_manifest),
        )
    if expected_dimension is not None:
        assert exc_info.value.dimension == expected_dimension


@pytest.mark.asyncio
async def test_direct_selector_returns_exact_empty_selection(
    monkeypatch,
) -> None:
    projection_manifest, candidates = _projection_fixture_for((1,), {})
    npi = 1_000_000_001
    graph_lookup = AsyncMock(return_value={npi: ()})
    dictionary_lookup = AsyncMock(return_value={1: npi})
    monkeypatch.setattr(
        serving,
        "load_v4_inferred_taxonomy_candidates",
        AsyncMock(return_value=candidates),
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        dictionary_lookup,
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[_rate_row(1)]),
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(
        serving, "_v4_direct_set_candidates", AsyncMock(return_value={1: ()})
    )
    _patch_empty_direct_prefixes(monkeypatch)

    selection = await _select_direct_fixture(
        projection_manifest,
        _projection_rule(projection_manifest),
    )

    assert selection.row_data == []
    assert selection.exhausted is True
    assert graph_lookup.await_count == 0
    assert dictionary_lookup.await_count == 0
