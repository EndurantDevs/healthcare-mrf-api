from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_batch as batch
from api import ptg2_candidate_audit_graph as candidate_graph
from api import ptg2_candidate_audit_reverse as reverse_scope
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def _challenge() -> AuditBatchChallenge:
    return AuditBatchChallenge(
        code_system="CPT",
        code="99213",
        npi=1234567890,
        source_artifact_key=0,
        tuple_digest="a" * 64,
        network_name_digests=("b" * 64,),
        multiplicity=1,
    )


def _persisted_occurrence() -> PersistedAuditOccurrence:
    return PersistedAuditOccurrence(
        occurrence_id=b"p" * 32,
        code_key=8,
        provider_set_key=7,
        price_key=9,
        source_artifact_key=1,
        npi=1111111111,
        atom_ordinal=0,
        atom_key=10,
    )


def _serving_tables() -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=100,
        price_dictionary_block_bytes=2048,
    )


def _code_index() -> CandidateCodeIndex:
    return CandidateCodeIndex(
        by_pair={("CPT", "99213"): ({"code_key": 7},)},
        by_key={7: {"code_key": 7}, 8: {"code_key": 8}},
    )


@pytest.mark.asyncio
async def test_oversized_challenge_graph_preserves_loaded_npi_groups():
    challenge = _challenge()
    graph_lookup = AsyncMock(
        side_effect=[
            {challenge.npi: (4, 5)},
            PTG2ManifestArtifactError(
                "shared PTG graph chunks exceed the read-once byte limit"
            ),
        ]
    )

    with pytest.raises(candidate_graph.ChallengeGraphScopeTooLarge) as exc_info:
        await candidate_graph.provider_set_keys_by_npi(
            graph_lookup,
            object(),
            41,
            "mrf",
            (challenge,),
            (),
        )

    assert exc_info.value.group_keys_by_npi == {challenge.npi: (4, 5)}


@pytest.mark.asyncio
async def test_challenge_graph_does_not_reclassify_other_artifact_errors():
    challenge = _challenge()
    graph_lookup = AsyncMock(
        side_effect=[
            {challenge.npi: (4,)},
            PTG2ManifestArtifactError("corrupt graph payload"),
        ]
    )

    with pytest.raises(PTG2ManifestArtifactError, match="corrupt graph payload"):
        await candidate_graph.provider_set_keys_by_npi(
            graph_lookup,
            object(),
            41,
            "mrf",
            (challenge,),
            (),
        )


@pytest.mark.asyncio
async def test_reverse_candidate_graph_retains_intersecting_memberships():
    graph_lookup = AsyncMock(return_value={5: (2, 7), 6: (9,)})

    observed = await candidate_graph.prove_provider_candidates_by_npi(
        graph_lookup,
        object(),
        41,
        "mrf",
        {1234567890: (5, 6), 1111111111: ()},
        {1234567890: (2, 3), 1111111111: (8,)},
    )

    assert observed == {1234567890: (5,), 1111111111: ()}
    assert graph_lookup.await_args.args[2] == (
        candidate_graph.PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP
    )
    assert tuple(graph_lookup.await_args.args[3]) == (5, 6)


@pytest.mark.asyncio
async def test_reverse_candidate_graph_skips_empty_scope():
    graph_lookup = AsyncMock()

    observed = await candidate_graph.prove_provider_candidates_by_npi(
        graph_lookup,
        object(),
        41,
        "mrf",
        {1234567890: ()},
        {1234567890: (2,)},
    )

    assert observed == {1234567890: ()}
    graph_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_reverse_source_scope_filters_forward_then_proves_graph(
    monkeypatch,
):
    challenge = _challenge()
    persisted = _persisted_occurrence()
    expected_price_index = {(7, 5, 0): (10,), (8, 7, 1): (9,)}
    forward_lookup = AsyncMock(return_value=expected_price_index)
    graph_proof = AsyncMock(
        return_value={challenge.npi: (5,), persisted.npi: (7,)}
    )
    monkeypatch.setattr(
        reverse_scope,
        "lookup_forward_price_index_from_db",
        forward_lookup,
    )
    monkeypatch.setattr(
        reverse_scope,
        "prove_provider_candidates_by_npi",
        graph_proof,
    )
    group_keys_by_npi = {challenge.npi: (4,), persisted.npi: (6,)}

    observed_scope = await reverse_scope.load_reverse_source_candidate_scope(
        object(),
        _serving_tables(),
        (challenge,),
        (persisted,),
        _code_index(),
        group_keys_by_npi,
    )

    assert observed_scope.provider_set_keys_by_npi == {
        challenge.npi: (5,),
        persisted.npi: (7,),
    }
    assert observed_scope.price_keys_by_occurrence is expected_price_index
    assert forward_lookup.await_args.args[1] == {7: (0,), 8: (1,)}
    assert forward_lookup.await_args.kwargs["source_keys_by_code"] == {
        7: (0,),
        8: (1,),
    }
    assert "provider_set_keys_by_code" not in forward_lookup.await_args.kwargs
    assert graph_proof.await_args.args[4] == {
        challenge.npi: {5},
        persisted.npi: {7},
    }
    assert graph_proof.await_args.args[5] is group_keys_by_npi


@pytest.mark.asyncio
async def test_candidate_provider_scope_falls_back_only_for_oversized_graph(
    monkeypatch,
):
    challenge = _challenge()
    expected_price_index = {(7, 5, 0): (10,)}
    broad_scope_lookup = AsyncMock(
        side_effect=candidate_graph.ChallengeGraphScopeTooLarge(
            {challenge.npi: (4,)}
        )
    )
    reverse_scope_loader = AsyncMock(
        return_value=reverse_scope.ReverseCandidateScope(
            provider_set_keys_by_npi={challenge.npi: (5,)},
            price_keys_by_occurrence=expected_price_index,
        )
    )
    monkeypatch.setattr(
        reverse_scope,
        "load_reverse_source_candidate_scope",
        reverse_scope_loader,
    )

    observed_scope = await reverse_scope.load_candidate_provider_scope(
        broad_scope_lookup,
        object(),
        _serving_tables(),
        (challenge,),
        (),
        _code_index(),
    )

    assert observed_scope.provider_set_keys_by_npi == {challenge.npi: (5,)}
    assert observed_scope.price_keys_by_occurrence is expected_price_index
    assert reverse_scope_loader.await_args.args[5] == {challenge.npi: (4,)}


@pytest.mark.asyncio
async def test_candidate_provider_scope_keeps_normal_graph_without_preload():
    challenge = _challenge()
    broad_scope_lookup = AsyncMock(return_value={challenge.npi: (5,)})

    observed_scope = await reverse_scope.load_candidate_provider_scope(
        broad_scope_lookup,
        object(),
        _serving_tables(),
        (challenge,),
        (),
        _code_index(),
    )

    assert observed_scope.provider_set_keys_by_npi == {challenge.npi: (5,)}
    assert observed_scope.price_keys_by_occurrence is None


@pytest.mark.asyncio
async def test_price_load_reuses_preloaded_forward_rows_without_rereading(
    monkeypatch,
):
    challenge = _challenge()
    exact_price_index = {(7, 5, 0): (10,)}
    preloaded_price_index = {**exact_price_index, (7, 9, 0): (99,)}
    forward_lookup = AsyncMock()
    hydration = AsyncMock(
        return_value=SimpleNamespace(
            atom_keys_by_price_key={10: (110,)},
            prices_by_key={10: [{"key": 10}]},
        )
    )
    monkeypatch.setattr(batch, "_candidate_forward_price_keys", forward_lookup)
    monkeypatch.setattr(batch, "_version_three_price_hydration", hydration)

    price_load = await batch._load_candidate_price_data(
        object(),
        _serving_tables(),
        (challenge,),
        {("CPT", "99213"): ({"code_key": 7},)},
        {(challenge.npi, 7): (5,)},
        {7: (5,)},
        preloaded_price_keys_by_occurrence=preloaded_price_index,
    )

    assert price_load.data.price_keys_by_occurrence == exact_price_index
    forward_lookup.assert_not_awaited()
    assert hydration.await_args.args[2] == {10}
