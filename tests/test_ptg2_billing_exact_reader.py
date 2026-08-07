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
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

GROUP_A = "aa" * 16
GROUP_B = "bb" * 16
SET_X = "11" * 16
SET_Y = "22" * 16


def _tables(*, v4: bool = True) -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4" if v4 else "shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout=(
            "packed_snapshot_maps_v4" if v4 else "dense_shared_blocks_v3"
        ),
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=32,
        provider_shard_span=1024,
    )


def _scope(*, snapshot_key: int = 17) -> ResolvedBillingEntitySourceScope:
    return ResolvedBillingEntitySourceScope(
        snapshot_key=snapshot_key,
        witnesses=(
            BillingEntitySourceWitness(0, 0, GROUP_A),
            BillingEntitySourceWitness(1, 0, GROUP_B),
        ),
    )


def _patch_graph(
    monkeypatch,
    *,
    sets_by_group=None,
    set_keys_by_id=None,
    group_keys_by_id=None,
    groups_by_set=None,
    occurrences_by_code=None,
):
    """Install one complete synthetic graph/read-sidecar projection."""

    graph_responses_by_name = {
        "_manifest_sets_by_group": (
            sets_by_group
            if sets_by_group is not None
            else {GROUP_A: (SET_X,), GROUP_B: (SET_X, SET_Y)}
        ),
        "_provider_set_keys_for_ids": (
            set_keys_by_id if set_keys_by_id is not None else {SET_X: 3, SET_Y: 4}
        ),
        "_shared_provider_group_keys_for_ids": (
            group_keys_by_id
            if group_keys_by_id is not None
            else {GROUP_A: 7, GROUP_B: 8}
        ),
        "_v4_exact_groups_by_set": (
            groups_by_set if groups_by_set is not None else {3: (7, 8), 4: (8,)}
        ),
    }
    for function_name, response_value in graph_responses_by_name.items():
        monkeypatch.setattr(
            reader.ptg2_serving,
            function_name,
            AsyncMock(return_value=response_value),
        )
    forward = AsyncMock(
        return_value=(
            occurrences_by_code
            if occurrences_by_code is not None
            else {
                10: (
                    (3, 100, 0),
                    (3, 101, 1),
                    (4, 103, 1),
                )
            }
        )
    )
    monkeypatch.setattr(
        reader.ptg2_db_sidecars,
        "lookup_forward_occurrences_batch_from_db",
        forward,
    )
    return forward


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
                17,
                (BillingEntitySourceWitness(0, 0, "not-a-group"),),
            ),
            snapshot_key=17,
            source_count=2,
        )

    monkeypatch.setattr(exact_contract, "MAX_PROVIDER_GROUPS", 1)
    with pytest.raises(PTG2ManifestArtifactError, match="provider-group limit"):
        reader._source_groups(
            _scope(),
            snapshot_key=17,
            source_count=2,
        )
