from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_capacity as capacity
from api import ptg2_candidate_audit_graph as candidate_graph
from api import ptg2_candidate_audit_reverse as reverse_scope
from api import ptg2_candidate_audit_v4 as v4_scope
from api import ptg2_db_sidecars as sidecars
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


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


def _code_index() -> CandidateCodeIndex:
    return CandidateCodeIndex(
        by_pair={("CPT", "99213"): ({"code_key": 7},)},
        by_key={7: {"code_key": 7}, 8: {"code_key": 8}},
    )


def test_preloaded_npi_groups_share_budget_with_reverse_indexes():
    challenge = _challenge()
    preloaded_group_bytes = (
        reverse_scope._PRELOADED_NPI_GROUP_MAP_BYTES
        + reverse_scope._PRELOADED_NPI_GROUP_BUCKET_BYTES
        + reverse_scope._PRELOADED_NPI_GROUP_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            preloaded_group_bytes
            + reverse_scope._CODE_SOURCE_NPI_MAP_BYTES
            + reverse_scope._CODE_SOURCE_NPI_KEY_BYTES
            + reverse_scope._CODE_SOURCE_NPI_BUCKET_BYTES
            - 1
        )
    )
    reverse_scope._claim_preloaded_npi_group_retention(
        {challenge.npi: (4,)},
        budget,
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="code/source NPI bucket",
    ):
        reverse_scope._provider_candidates_by_npi(
            (challenge,),
            (),
            _code_index(),
            {},
            budget,
        )

    assert budget.retained_bytes == (
        preloaded_group_bytes
        + reverse_scope._CODE_SOURCE_NPI_MAP_BYTES
        + reverse_scope._CODE_SOURCE_NPI_KEY_BYTES
    )


def test_reverse_projection_rejects_adversarial_npi_provider_capacity():
    challenge = _challenge()
    challenges = tuple(
        replace(challenge, npi=1_000_000_000 + index)
        for index in range(10_000)
    )
    forward_rows_by_occurrence = {
        (7, provider_set_key, 0): (provider_set_key,)
        for provider_set_key in range(100)
    }
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4 * 1024 * 1024)

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="NPI provider membership",
    ):
        reverse_scope._provider_candidates_by_npi(
            challenges,
            (),
            _code_index(),
            forward_rows_by_occurrence,
            budget,
        )

    assert budget.retained_bytes <= budget.maximum_bytes


def test_shared_group_many_npis_fails_before_overflow_membership_retention():
    first_npi = 1_234_567_890
    second_npi = 1_234_567_891
    maximum_bytes = (
        candidate_graph._NPI_PROVIDER_MAP_BYTES
        + 2
        * (
            candidate_graph._NPI_PROVIDER_MAP_ENTRY_BYTES
            + candidate_graph._NPI_PROVIDER_SET_BYTES
        )
        + candidate_graph._NPI_PROVIDER_SET_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=maximum_bytes
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="broad NPI provider membership",
    ):
        candidate_graph._provider_keys_for_challenges(
            (first_npi, second_npi),
            {first_npi: (4,), second_npi: (4,)},
            {4: (7,)},
            budget,
        )

    assert budget.retained_bytes == maximum_bytes


def test_reverse_lookup_stops_consuming_candidates_at_budget_boundary():
    consumed_provider_keys = []

    def candidate_keys():
        for provider_set_key in range(100):
            consumed_provider_keys.append(provider_set_key)
            yield provider_set_key

    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            candidate_graph._PROVIDER_KEY_SET_BYTES
            + candidate_graph._PROVIDER_KEY_SET_MEMBERSHIP_BYTES
        )
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="reverse provider lookup key",
    ):
        candidate_graph._reverse_provider_set_keys(
            {1234567890: candidate_keys()},
            budget,
        )

    assert consumed_provider_keys == [0, 1]


def test_proven_scope_stops_before_materializing_full_candidate_iterable():
    consumed_provider_keys = []

    def candidate_keys():
        for provider_set_key in range(100):
            consumed_provider_keys.append(provider_set_key)
            yield provider_set_key

    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            candidate_graph._PROVEN_PROVIDER_TUPLE_BYTES
            + candidate_graph._PROVEN_PROVIDER_MEMBERSHIP_BYTES
        )
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="proven NPI provider membership",
    ):
        candidate_graph._proven_provider_keys_by_npi(
            {1234567890: candidate_keys()},
            {1234567890: (4,)},
            {provider_set_key: (4,) for provider_set_key in range(100)},
            budget,
        )

    assert consumed_provider_keys == [0, 1]


@pytest.mark.parametrize("provider_count", (0, 5))
def test_proven_scope_exact_budget_covers_result_insertion_peak(provider_count):
    maximum_bytes = (
        candidate_graph._PROVEN_PROVIDER_TUPLE_BYTES
        + provider_count * candidate_graph._PROVEN_PROVIDER_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=maximum_bytes)
    provider_keys = tuple(range(provider_count))

    observed = candidate_graph._proven_provider_keys_by_npi(
        {1234567890: provider_keys},
        {1234567890: (4,)},
        {provider_key: (4,) for provider_key in provider_keys},
        budget,
    )

    assert observed == {1234567890: provider_keys}
    assert budget.retained_bytes == maximum_bytes


@pytest.mark.parametrize("provider_count", (0, 5))
def test_proven_scope_one_byte_under_result_peak_fails_before_retention(
    provider_count,
):
    consumed_provider_keys = []

    def candidate_keys():
        for provider_key in range(provider_count):
            consumed_provider_keys.append(provider_key)
            yield provider_key

    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            candidate_graph._PROVEN_PROVIDER_TUPLE_BYTES
            + provider_count * candidate_graph._PROVEN_PROVIDER_MEMBERSHIP_BYTES
            - 1
        )
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match=(
            "proven NPI provider tuple"
            if provider_count == 0
            else "proven NPI provider membership"
        ),
    ):
        candidate_graph._proven_provider_keys_by_npi(
            {1234567890: candidate_keys()},
            {1234567890: (4,)},
            {provider_key: (4,) for provider_key in range(provider_count)},
            budget,
        )

    assert consumed_provider_keys == list(range(max(0, provider_count)))
    assert budget.retained_bytes == (
        0
        if provider_count == 0
        else candidate_graph._PROVEN_PROVIDER_TUPLE_BYTES
        + (provider_count - 1) * candidate_graph._PROVEN_PROVIDER_MEMBERSHIP_BYTES
    )


def test_reverse_projection_preserves_source_scope_and_persisted_proof():
    first = _challenge()
    second = replace(first, npi=2222222222, source_artifact_key=1)
    persisted = _persisted_occurrence()
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)

    observed = reverse_scope._provider_candidates_by_npi(
        (first, second),
        (persisted,),
        _code_index(),
        {(7, 5, 0): (10,), (7, 6, 1): (11,)},
        budget,
    )

    assert observed == {
        first.npi: {5},
        second.npi: {6},
        persisted.npi: {persisted.provider_set_key},
    }


@pytest.mark.asyncio
async def test_v4_candidate_graph_fails_before_unbounded_projection(
    monkeypatch,
):
    npi = 1_234_567_890
    candidate_keys_by_npi = {npi: {7}}
    candidate_source_bytes = (
        v4_scope._NPI_PROVIDER_MAP_BYTES
        + v4_scope._NPI_PROVIDER_MAP_ENTRY_BYTES
        + v4_scope._NPI_PROVIDER_BUCKET_BYTES
        + v4_scope._NPI_PROVIDER_MEMBERSHIP_BYTES
    )
    allowed_set_bytes = (
        capacity.INTEGER_KEY_SET_BYTES
        + capacity.INTEGER_KEY_SET_MEMBERSHIP_BYTES
    )
    final_fixed_bytes = (
        v4_scope._V4_RESULT_MAP_BYTES
        + v4_scope._V4_RESULT_BUCKET_BYTES
    )
    transient_fixed_bytes = (
        v4_scope._V4_GRAPH_TRANSIENT_MAP_BYTES
        + 5 * v4_scope._V4_GRAPH_TRANSIENT_OWNER_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            candidate_source_bytes
            + allowed_set_bytes
            + final_fixed_bytes
            + transient_fixed_bytes
            + v4_scope._V4_GRAPH_PEAK_MEMBER_BYTES
            - 1
        )
    )
    budget.claim(candidate_source_bytes, category="the candidate source map")
    graph_lookup = AsyncMock()
    monkeypatch.setattr(v4_scope, "_v4_sets_by_npi", graph_lookup)

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="bounded V4 candidate graph projection",
    ):
        await v4_scope.prove_v4_candidate_sets(
            object(),
            object(),
            candidate_keys_by_npi,
            budget,
            schema_name="candidate_schema",
        )

    graph_lookup.assert_not_awaited()
    assert budget.retained_bytes == candidate_source_bytes


@pytest.mark.parametrize("maximum_bytes", (0, -1, True))
def test_decoded_retention_budget_requires_positive_integer(maximum_bytes):
    with pytest.raises(ValueError, match="limit must be positive"):
        CandidateAuditDecodedRetentionBudget(maximum_bytes=maximum_bytes)


def test_decoded_retention_budget_rejects_negative_claim():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1)

    with pytest.raises(ValueError, match="claim is invalid"):
        budget.claim(-1, category="invalid")


def test_decoded_retention_budget_releases_temporary_bytes():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=10)
    budget.claim(10, category="temporary")

    budget.release(10)

    assert budget.retained_bytes == 0
    with pytest.raises(ValueError, match="release is invalid"):
        budget.release(1)


@pytest.mark.asyncio
async def test_forward_index_stops_at_decoded_retention_budget(monkeypatch):
    async def _visit_rows(
        _session,
        _code_keys,
        _options,
        occurrence_consumer,
        **_visit_options,
    ):
        for provider_set_key in range(100):
            occurrence_consumer(7, provider_set_key, provider_set_key, 0)

    monkeypatch.setattr(sidecars, "_visit_forward_batch_keys", _visit_rows)
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024)

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="forward occurrence",
    ):
        await sidecars.lookup_forward_price_index_from_db(
            object(),
            (7,),
            retention_budget=budget,
            shared_snapshot_key=41,
            source_count=2,
            price_dictionary_item_count=100,
            price_dictionary_block_bytes=2048,
        )

    assert budget.retained_bytes <= budget.maximum_bytes
