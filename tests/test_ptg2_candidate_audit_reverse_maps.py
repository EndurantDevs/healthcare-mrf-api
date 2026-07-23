from __future__ import annotations

from dataclasses import replace

import pytest

from api import ptg2_candidate_audit_reverse as reverse_scope
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
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


def _code_index() -> CandidateCodeIndex:
    return CandidateCodeIndex(
        by_pair={("CPT", "99213"): ({"code_key": 7},)},
        by_key={7: {"code_key": 7}},
    )


def test_source_key_map_exact_budget_covers_set_list_and_tuple_peak():
    challenges = tuple(
        replace(_challenge(), source_artifact_key=source_key)
        for source_key in range(5)
    )
    set_retained_bytes = (
        reverse_scope._SOURCE_KEY_SET_MAP_BYTES
        + reverse_scope._SOURCE_KEY_SET_BUCKET_BYTES
        + 5 * reverse_scope._SOURCE_KEY_SET_MEMBERSHIP_BYTES
    )
    result_retained_bytes = (
        reverse_scope._SOURCE_KEY_RESULT_MAP_BYTES
        + reverse_scope._SOURCE_KEY_RESULT_BUCKET_BYTES
        + 5 * reverse_scope._SOURCE_KEY_RESULT_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=set_retained_bytes + result_retained_bytes
    )

    result = reverse_scope._source_keys_by_code_key(
        challenges,
        (),
        _code_index(),
        budget,
    )

    assert result.source_keys_by_code == {7: (0, 1, 2, 3, 4)}
    assert result.retained_bytes == result_retained_bytes
    assert budget.retained_bytes == result_retained_bytes


def test_source_key_map_one_byte_under_stops_before_tuple_conversion():
    challenges = tuple(
        replace(_challenge(), source_artifact_key=source_key)
        for source_key in range(5)
    )
    set_retained_bytes = (
        reverse_scope._SOURCE_KEY_SET_MAP_BYTES
        + reverse_scope._SOURCE_KEY_SET_BUCKET_BYTES
        + 5 * reverse_scope._SOURCE_KEY_SET_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            set_retained_bytes
            + reverse_scope._SOURCE_KEY_RESULT_MAP_BYTES
            + reverse_scope._SOURCE_KEY_RESULT_BUCKET_BYTES
            + 5 * reverse_scope._SOURCE_KEY_RESULT_MEMBERSHIP_BYTES
            - 1
        )
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="ordered source-key bucket",
    ):
        reverse_scope._source_keys_by_code_key(
            challenges,
            (),
            _code_index(),
            budget,
        )

    assert budget.retained_bytes == (
        set_retained_bytes + reverse_scope._SOURCE_KEY_RESULT_MAP_BYTES
    )


def test_source_key_retention_deduplicates_membership():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024)
    source_keys_by_code = {}

    first_claim = reverse_scope._retain_source_key(
        source_keys_by_code,
        7,
        3,
        budget,
    )
    retained_bytes = budget.retained_bytes
    second_claim = reverse_scope._retain_source_key(
        source_keys_by_code,
        7,
        3,
        budget,
    )

    assert first_claim > 0
    assert second_claim == 0
    assert source_keys_by_code == {7: {3}}
    assert budget.retained_bytes == retained_bytes


@pytest.mark.parametrize("npi_count", (1, 5, 19))
def test_code_source_npi_set_exact_and_one_byte_under(npi_count):
    challenges = tuple(
        replace(_challenge(), npi=1_000_000_000 + npi_offset)
        for npi_offset in range(npi_count)
    )
    required_bytes = (
        reverse_scope._CODE_SOURCE_NPI_MAP_BYTES
        + reverse_scope._CODE_SOURCE_NPI_KEY_BYTES
        + reverse_scope._CODE_SOURCE_NPI_BUCKET_BYTES
        + npi_count * reverse_scope._CODE_SOURCE_NPI_MEMBERSHIP_BYTES
    )
    exact_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=required_bytes
    )

    budgeted_npis = reverse_scope._code_source_npis(
        challenges,
        _code_index(),
        exact_budget,
    )

    assert len(budgeted_npis.npis_by_code_source[(7, 0)]) == npi_count
    assert budgeted_npis.retained_bytes == required_bytes
    under_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=required_bytes - 1
    )
    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="code/source NPI membership",
    ):
        reverse_scope._code_source_npis(
            challenges,
            _code_index(),
            under_budget,
        )
    assert under_budget.retained_bytes == (
        reverse_scope._CODE_SOURCE_NPI_MAP_BYTES
        + reverse_scope._CODE_SOURCE_NPI_KEY_BYTES
        + reverse_scope._CODE_SOURCE_NPI_BUCKET_BYTES
        + (npi_count - 1) * reverse_scope._CODE_SOURCE_NPI_MEMBERSHIP_BYTES
    )


def test_code_source_npis_deduplicates_repeated_challenge():
    challenge = _challenge()
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=2048)

    result = reverse_scope._code_source_npis(
        (challenge, challenge),
        _code_index(),
        budget,
    )

    assert result.npis_by_code_source == {(7, 0): {challenge.npi}}
    assert budget.retained_bytes == result.retained_bytes


def _npi_provider_bytes(provider_count: int) -> int:
    """Return the conservative map, bucket, and membership charge."""

    return (
        reverse_scope._NPI_PROVIDER_MAP_BYTES
        + reverse_scope._NPI_PROVIDER_MAP_ENTRY_BYTES
        + reverse_scope._NPI_PROVIDER_BUCKET_BYTES
        + provider_count * reverse_scope._NPI_PROVIDER_MEMBERSHIP_BYTES
    )


def _retained_npi_provider_keys(
    provider_count: int,
    budget: CandidateAuditDecodedRetentionBudget,
) -> dict[int, set[int]]:
    """Build one budgeted NPI-provider bucket for boundary checks."""

    budget.claim(
        reverse_scope._NPI_PROVIDER_MAP_BYTES,
        category="the NPI provider map",
    )
    candidate_keys_by_npi = {}
    reverse_scope._candidate_keys_for_npi(
        candidate_keys_by_npi,
        1234567890,
        budget,
    )
    for provider_key in range(provider_count):
        reverse_scope._retain_npi_provider_membership(
            candidate_keys_by_npi,
            1234567890,
            provider_key,
            budget,
        )
    return candidate_keys_by_npi


@pytest.mark.parametrize("provider_count", (0, 5, 19))
def test_npi_provider_set_exact_budget(provider_count):
    required_bytes = _npi_provider_bytes(provider_count)
    exact_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=required_bytes
    )

    candidate_keys_by_npi = _retained_npi_provider_keys(
        provider_count,
        exact_budget,
    )

    assert candidate_keys_by_npi == {
        1234567890: set(range(provider_count))
    }
    assert exact_budget.retained_bytes == required_bytes


def test_npi_provider_retention_deduplicates_membership():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024)
    candidate_keys_by_npi = {}

    reverse_scope._retain_npi_provider_membership(
        candidate_keys_by_npi,
        1234567890,
        5,
        budget,
    )
    retained_bytes = budget.retained_bytes
    reverse_scope._retain_npi_provider_membership(
        candidate_keys_by_npi,
        1234567890,
        5,
        budget,
    )

    assert candidate_keys_by_npi == {1234567890: {5}}
    assert budget.retained_bytes == retained_bytes


@pytest.mark.parametrize("provider_count", (0, 5, 19))
def test_npi_provider_set_one_byte_under(provider_count):
    under_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=_npi_provider_bytes(provider_count) - 1
    )

    with pytest.raises(CandidateAuditDecodedRetentionError):
        _retained_npi_provider_keys(
            provider_count,
            under_budget,
        )

    assert under_budget.retained_bytes == (
        reverse_scope._NPI_PROVIDER_MAP_BYTES
        + (
            0
            if provider_count == 0
            else reverse_scope._NPI_PROVIDER_MAP_ENTRY_BYTES
            + reverse_scope._NPI_PROVIDER_BUCKET_BYTES
            + (provider_count - 1)
            * reverse_scope._NPI_PROVIDER_MEMBERSHIP_BYTES
        )
    )


def _empty_npi_provider_bytes(npi_count: int) -> int:
    """Return the resize-safe charge for empty NPI provider buckets."""

    return reverse_scope._NPI_PROVIDER_MAP_BYTES + npi_count * (
        reverse_scope._NPI_PROVIDER_MAP_ENTRY_BYTES
        + reverse_scope._NPI_PROVIDER_BUCKET_BYTES
    )


def _retain_empty_npi_provider_buckets(
    npi_count: int,
    budget: CandidateAuditDecodedRetentionBudget,
) -> dict[int, set[int]]:
    """Build empty buckets, including the five-to-six dict resize boundary."""

    budget.claim(
        reverse_scope._NPI_PROVIDER_MAP_BYTES,
        category="the NPI provider map",
    )
    candidate_keys_by_npi = {}
    for npi_offset in range(npi_count):
        reverse_scope._candidate_keys_for_npi(
            candidate_keys_by_npi,
            1_000_000_000 + npi_offset,
            budget,
        )
    return candidate_keys_by_npi


@pytest.mark.parametrize("npi_count", (5, 6, 10, 11, 19, 21, 22))
def test_empty_npi_provider_buckets_exact_budget(npi_count):
    required_bytes = _empty_npi_provider_bytes(npi_count)
    exact_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=required_bytes
    )

    candidate_keys_by_npi = _retain_empty_npi_provider_buckets(
        npi_count,
        exact_budget,
    )

    assert len(candidate_keys_by_npi) == npi_count
    assert all(not keys for keys in candidate_keys_by_npi.values())
    assert exact_budget.retained_bytes == required_bytes


@pytest.mark.parametrize("npi_count", (5, 6, 10, 11, 19, 21, 22))
def test_empty_npi_provider_buckets_one_byte_under(npi_count):
    under_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=_empty_npi_provider_bytes(npi_count) - 1
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="NPI provider map entry and bucket",
    ):
        _retain_empty_npi_provider_buckets(npi_count, under_budget)

    assert under_budget.retained_bytes == (
        reverse_scope._NPI_PROVIDER_MAP_BYTES
        + (npi_count - 1)
        * (
            reverse_scope._NPI_PROVIDER_MAP_ENTRY_BYTES
            + reverse_scope._NPI_PROVIDER_BUCKET_BYTES
        )
    )
