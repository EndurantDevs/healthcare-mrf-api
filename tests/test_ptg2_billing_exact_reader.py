# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact source/group/set/rate billing-reader tests."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_exact_reader as reader
from api import ptg2_billing_exact_contract as exact_contract
from api import ptg2_billing_geo_contract as geo_contract
from api.ptg2_billing_entity_source_resolution import (
    BillingEntitySourceWitness,
    ResolvedBillingEntitySourceScope,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.ptg2_billing_exact_reader_support import (
    GROUP_A,
    GROUP_B,
    SET_X,
    SET_Y,
    _patch_graph,
    _publication,
    _scope,
    _tables,
)

@pytest.mark.asyncio
async def test_reader_preserves_exact_source_group_set_rate_witnesses(
    monkeypatch,
) -> None:
    forward = _patch_graph(monkeypatch)

    witnesses = await reader.load_exact_billing_rate_occurrence_witnesses(
        object(),
        _tables(),
        source_scope=_scope(),
        code_keys=(10,),
    )

    assert [
        (
            witness.source_key,
            witness.source_record_ordinal,
            witness.provider_group_ref,
            witness.provider_set_key,
            witness.price_key,
        )
        for witness in witnesses
    ] == [
        (0, 0, GROUP_A, 3, 100),
        (1, 0, GROUP_B, 3, 101),
        (1, 0, GROUP_B, 4, 103),
    ]
    assert all(witness.snapshot_key == 17 for witness in witnesses)
    assert all(witness.code_key == 10 for witness in witnesses)
    assert forward.await_args.args[1] == (10,)
    assert forward.await_args.kwargs["provider_set_keys_by_code"] == {
        10: frozenset({3, 4})
    }
    assert forward.await_args.kwargs["source_keys_by_code"] == {10: frozenset({0, 1})}
    assert forward.await_args.kwargs["occurrence_keys"] == (
        (10, 3, 0),
        (10, 3, 1),
        (10, 4, 1),
    )
    assert forward.await_args.kwargs["max_occurrences"] == 32768
    assert (
        forward.await_args.kwargs["retention_budget"].maximum_bytes == 64 * 1024 * 1024
    )
    assert "aa" not in repr(witnesses[0])
    assert "<redacted>" in repr(witnesses[0])


@pytest.mark.asyncio
async def test_reader_requires_v4_but_empty_code_scope_does_not_read(
    monkeypatch,
) -> None:
    graph_read = AsyncMock()
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_manifest_sets_by_group",
        graph_read,
    )
    assert (
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(),
            code_keys=(),
        )
        == ()
    )
    graph_read.assert_not_awaited()
    with pytest.raises(PTG2ManifestArtifactError, match="requires the sealed V4"):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(v4=False),
            source_scope=_scope(),
            code_keys=(10,),
        )


@pytest.mark.asyncio
async def test_reader_rejects_snapshot_mismatch_before_graph_read(monkeypatch) -> None:
    graph_read = AsyncMock()
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_manifest_sets_by_group",
        graph_read,
    )
    with pytest.raises(PTG2ManifestArtifactError, match="another snapshot"):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(snapshot_key=18),
            code_keys=(10,),
        )
    graph_read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("serving_publication", "scope_publication", "error"),
    (
        (None, _publication(), "geometry is unavailable"),
        (
            _publication(),
            _publication(content_digest="5" * 64),
            "geometry does not match",
        ),
        (
            _publication(),
            _publication(
                matched_ein_count=1,
                missing_count=5,
            ),
            "geometry does not match",
        ),
        (
            _publication(),
            _publication(source_ordinal_map_digest="6" * 64),
            "geometry does not match",
        ),
        (
            _publication(),
            _publication(binding_vector_digest="7" * 64),
            "geometry does not match",
        ),
    ),
    ids=(
        "missing",
        "mismatched-digest",
        "permuted-counts",
        "mismatched-source-map-digest",
        "mismatched-binding-vector-digest",
    ),
)
async def test_reader_rejects_missing_mismatched_or_permuted_source_geometry(
    monkeypatch,
    serving_publication,
    scope_publication,
    error,
) -> None:
    graph_read = AsyncMock()
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_manifest_sets_by_group",
        graph_read,
    )

    with pytest.raises(PTG2ManifestArtifactError, match=error):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(source_publication=serving_publication),
            source_scope=_scope(source_publication=scope_publication),
            code_keys=(10,),
        )

    graph_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_returns_no_rates_for_groups_without_sets(monkeypatch) -> None:
    sets = AsyncMock(return_value={GROUP_A: (), GROUP_B: ()})
    set_dictionary = AsyncMock()
    group_dictionary = AsyncMock(return_value={GROUP_A: 7, GROUP_B: 8})
    monkeypatch.setattr(reader.ptg2_serving, "_manifest_sets_by_group", sets)
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_provider_set_keys_for_ids",
        set_dictionary,
    )
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_shared_provider_group_keys_for_ids",
        group_dictionary,
    )

    assert (
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(),
            code_keys=(10,),
        )
        == ()
    )
    assert sets.await_args.kwargs["max_members"] == 8192
    group_dictionary.assert_awaited_once()
    set_dictionary.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_rejects_unknown_group_even_when_it_has_no_sets(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_manifest_sets_by_group",
        AsyncMock(return_value={GROUP_A: (), GROUP_B: ()}),
    )
    monkeypatch.setattr(
        reader.ptg2_serving,
        "_shared_provider_group_keys_for_ids",
        AsyncMock(return_value={GROUP_A: 7}),
    )
    with pytest.raises(PTG2ManifestArtifactError, match="unknown provider group"):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(),
            code_keys=(10,),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sets_by_group", "error"),
    [
        ({GROUP_A: (SET_X,)}, "incomplete"),
        (
            {GROUP_A: (SET_X, SET_X), GROUP_B: (SET_Y,)},
            "duplicates",
        ),
    ],
)
async def test_reader_rejects_invalid_group_to_set_projection(
    monkeypatch,
    sets_by_group,
    error,
) -> None:
    _patch_graph(monkeypatch, sets_by_group=sets_by_group)
    with pytest.raises(PTG2ManifestArtifactError, match=error):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(),
            code_keys=(10,),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("set_keys_by_id", "group_keys_by_id", "error"),
    [
        ({SET_X: 3}, {GROUP_A: 7, GROUP_B: 8}, "unknown provider set"),
        (
            {SET_X: 3, SET_Y: 4},
            {GROUP_A: 7},
            "unknown provider group",
        ),
        (
            {SET_X: 3, SET_Y: 3},
            {GROUP_A: 7, GROUP_B: 8},
            "provider set keys are inconsistent",
        ),
    ],
)
async def test_reader_rejects_invalid_shared_dictionaries(
    monkeypatch,
    set_keys_by_id,
    group_keys_by_id,
    error,
) -> None:
    _patch_graph(
        monkeypatch,
        set_keys_by_id=set_keys_by_id,
        group_keys_by_id=group_keys_by_id,
    )
    with pytest.raises(PTG2ManifestArtifactError, match=error):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(),
            code_keys=(10,),
        )


@pytest.mark.asyncio
async def test_reader_rejects_disagreeing_forward_and_reverse_group_sets(
    monkeypatch,
) -> None:
    _patch_graph(monkeypatch, groups_by_set={3: (7,), 4: (8,)})
    with pytest.raises(PTG2ManifestArtifactError, match="projections are inconsistent"):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(),
            code_keys=(10,),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("occurrences_by_code", "error"),
    [
        ({}, "incomplete"),
        ({10: ((5, 100, 0),)}, "escaped its scope"),
        ({10: ((3, 100, 2),)}, "escaped its scope"),
        ({10: ((4, 100, 0),)}, "escaped its scope"),
        ({10: ((3, 100),)}, "malformed"),
    ],
)
async def test_reader_rejects_invalid_forward_occurrences(
    monkeypatch,
    occurrences_by_code,
    error,
) -> None:
    _patch_graph(monkeypatch, occurrences_by_code=occurrences_by_code)
    with pytest.raises(PTG2ManifestArtifactError, match=error):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(),
            code_keys=(10,),
        )


@pytest.mark.asyncio
async def test_reader_preserves_duplicate_occurrences_and_cross_domain_equal_keys(
    monkeypatch,
) -> None:
    _patch_graph(
        monkeypatch,
        sets_by_group={GROUP_A: (SET_X,), GROUP_B: ()},
        set_keys_by_id={SET_X: 1},
        group_keys_by_id={GROUP_A: 7, GROUP_B: 8},
        groups_by_set={1: (7,)},
        occurrences_by_code={10: ((1, 1, 0), (1, 1, 0))},
    )
    witnesses = await reader.load_exact_billing_rate_occurrence_witnesses(
        object(),
        _tables(),
        source_scope=_scope(),
        code_keys=(10,),
    )
    assert len(witnesses) == 2
    assert all(
        witness.provider_set_key == witness.price_key == 1 for witness in witnesses
    )
    assert [witness.source_record_ordinal for witness in witnesses] == [0, 0]
    assert [witness.occurrence_ordinal for witness in witnesses] == [0, 1]
    assert (
        len(
            {
                (
                    witness.source_key,
                    witness.source_record_ordinal,
                    witness.provider_group_ref,
                    witness.provider_set_key,
                    witness.price_key,
                    witness.occurrence_ordinal,
                )
                for witness in witnesses
            }
        )
        == 2
    )


@pytest.mark.asyncio
async def test_reader_canonicalizes_shared_set_multi_group_duplicates(
    monkeypatch,
) -> None:
    source_scope = ResolvedBillingEntitySourceScope(
        snapshot_key=17,
        publication=_publication(),
        witnesses=(
            BillingEntitySourceWitness(0, 0, GROUP_B),
            BillingEntitySourceWitness(0, 1, GROUP_A),
        ),
    )
    _patch_graph(
        monkeypatch,
        sets_by_group={GROUP_A: (SET_X,), GROUP_B: (SET_X,)},
        set_keys_by_id={SET_X: 3},
        group_keys_by_id={GROUP_A: 7, GROUP_B: 8},
        groups_by_set={3: (7, 8)},
        occurrences_by_code={10: ((3, 100, 0), (3, 100, 0))},
    )

    witnesses = await reader.load_exact_billing_rate_occurrence_witnesses(
        object(),
        _tables(),
        source_scope=source_scope,
        code_keys=(10,),
    )

    assert [
        (
            witness.source_record_ordinal,
            witness.occurrence_ordinal,
            witness.provider_group_ref,
        )
        for witness in witnesses
    ] == [
        (0, 0, GROUP_B),
        (0, 1, GROUP_B),
        (1, 0, GROUP_A),
        (1, 1, GROUP_A),
    ]
    assert geo_contract.validated_rate_witnesses(witnesses) == witnesses


@pytest.mark.asyncio
async def test_reader_bounds_code_and_rate_witness_fanout(monkeypatch) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="code scope exceeds"):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(),
            code_keys=range(65),
        )

    _patch_graph(
        monkeypatch,
        occurrences_by_code={10: ((3, 100, 0), (3, 101, 1))},
    )
    monkeypatch.setattr(reader, "_MAX_RATE_WITNESSES", 1)
    with pytest.raises(PTG2ManifestArtifactError, match="witness scope exceeds"):
        await reader.load_exact_billing_rate_occurrence_witnesses(
            object(),
            _tables(),
            source_scope=_scope(),
            code_keys=(10,),
        )


def test_source_scope_rejects_invalid_or_excessive_groups(monkeypatch) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="invalid witness"):
        reader._source_groups(
            ResolvedBillingEntitySourceScope(
                snapshot_key=17,
                publication=_publication(),
                witnesses=(BillingEntitySourceWitness(0, 0, "not-a-group"),),
            ),
            snapshot_key=17,
            source_count=2,
            source_publication=_publication(),
        )

    monkeypatch.setattr(exact_contract, "MAX_PROVIDER_GROUPS", 1)
    with pytest.raises(PTG2ManifestArtifactError, match="provider-group limit"):
        reader._source_groups(
            _scope(),
            snapshot_key=17,
            source_count=2,
            source_publication=_publication(),
        )
