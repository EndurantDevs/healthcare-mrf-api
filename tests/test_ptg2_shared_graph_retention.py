from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_shared_blocks as shared_readers
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)


def _graph_request(*, owner_keys=(1,)):
    return shared_readers._shared_graph_read_request(
        schema_name="mrf",
        snapshot_key=12,
        direction=shared_readers.PTG2_V3_GRAPH_NPI_TO_GROUP,
        owner_keys=owner_keys,
        max_members=None,
    )


def _owner_record(selected_member_count):
    return {
        "owner_key": 1,
        "first_chunk": 0,
        "member_offset": 0,
        "member_count": selected_member_count,
        "selected_member_count": selected_member_count,
    }


def test_direct_graph_records_preserve_aliased_fragments(monkeypatch):
    request = _graph_request(owner_keys=(1, 2))
    payload = shared_readers.SharedBlockPayload(0, 0, 1, b"1234", b"h" * 32)
    validate_payload = Mock(return_value=payload)
    monkeypatch.setattr(shared_readers, "_validated_payload", validate_payload)
    fragment_by_field = {
        "owner_key": 1,
        "member_offset": 0,
        "member_count": 1,
        "selected_member_count": 1,
    }

    locator_by_owner, chunks_by_owner = shared_readers._indexed_direct_graph_records(
        request,
        (fragment_by_field, dict(fragment_by_field)),
    )

    assert locator_by_owner == {1: (0, 1, 1)}
    assert chunks_by_owner == {1: [payload, payload]}
    assert validate_payload.call_count == 2


class _RecordingBudget:
    def __init__(self):
        self.claimed_bytes = []

    def claim(self, byte_count, *, category):
        self.claimed_bytes.append((byte_count, category))

    def release(self, _byte_count):
        return None


def test_graph_owner_normalization_stops_before_second_key_retention():
    consumed_owner_keys = []

    def owner_keys():
        for owner_key in range(100):
            consumed_owner_keys.append(owner_key)
            yield owner_key

    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            shared_readers._GRAPH_OWNER_SET_BYTES
            + shared_readers._GRAPH_OWNER_SET_MEMBERSHIP_BYTES
        )
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="normalized owner key",
    ):
        shared_readers._budgeted_graph_owner_keys(owner_keys(), budget)

    assert consumed_owner_keys == [0, 1]


@pytest.mark.parametrize(
    ("locator_by_owner", "selected_member_count"),
    (
        ({}, 0),
        ({1: (0, 0, 0, 0)}, 0),
        ({1: (0, 0, 5, 5)}, 5),
    ),
)
def test_graph_result_retention_exact_and_one_byte_under(
    locator_by_owner,
    selected_member_count,
):
    request = _graph_request()
    required_bytes = (
        shared_readers._GRAPH_RESULT_OWNER_BYTES
        + selected_member_count * shared_readers._GRAPH_RESULT_MEMBERSHIP_BYTES
    )
    exact_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=required_bytes
    )

    shared_readers._claim_graph_result_retention(
        request,
        locator_by_owner,
        exact_budget,
    )

    assert exact_budget.retained_bytes == required_bytes
    under_budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=required_bytes - 1
    )
    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="graph result owner",
    ):
        shared_readers._claim_graph_result_retention(
            request,
            locator_by_owner,
            under_budget,
        )
    assert under_budget.retained_bytes == 0


def test_graph_result_claims_zero_populated_and_missing_owners_once():
    request = _graph_request(owner_keys=(1, 2, 3))
    budget = _RecordingBudget()

    shared_readers._claim_graph_result_retention(
        request,
        {
            1: (0, 0, 0, 0),
            2: (0, 0, 2, 2),
        },
        budget,
    )

    assert [byte_count for byte_count, _category in budget.claimed_bytes] == [
        shared_readers._GRAPH_RESULT_OWNER_BYTES,
        shared_readers._GRAPH_RESULT_OWNER_BYTES
        + 2 * shared_readers._GRAPH_RESULT_MEMBERSHIP_BYTES,
        shared_readers._GRAPH_RESULT_OWNER_BYTES,
    ]


def test_graph_result_partial_overflow_releases_prior_owner_claim():
    request = _graph_request(owner_keys=(1, 2))
    owner_bytes = shared_readers._GRAPH_RESULT_OWNER_BYTES
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=owner_bytes)

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="graph result owner",
    ):
        shared_readers._claim_graph_result_retention(request, {}, budget)

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_empty_budgeted_graph_request_releases_normalization_claims():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    assert (
        await shared_readers.fetch_shared_graph_members(
            object(),
            schema_name="mrf",
            snapshot_key=12,
            direction=shared_readers.PTG2_V3_GRAPH_NPI_TO_GROUP,
            owner_keys=(),
            retention_budget=budget,
        )
        == {}
    )
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_two_million_npi_groups_stop_before_chunk_fetch_and_decode(
    monkeypatch,
):
    request = _graph_request()
    owner_records = AsyncMock(return_value=(_owner_record(2_000_000),))
    fetch_blocks = AsyncMock()
    decode_members = Mock()
    monkeypatch.setattr(
        shared_readers,
        "_shared_graph_owner_records",
        owner_records,
    )
    monkeypatch.setattr(shared_readers, "fetch_shared_blocks", fetch_blocks)
    monkeypatch.setattr(
        shared_readers,
        "_decoded_scoped_graph_members",
        decode_members,
    )
    budget = CandidateAuditDecodedRetentionBudget()

    with shared_readers.shared_block_read_once_scope(
        max_retained_raw_bytes=64 * 1024 * 1024
    ):
        with pytest.raises(
            CandidateAuditDecodedRetentionError,
            match="graph result owner",
        ):
            await shared_readers._fetch_shared_graph_members_read_once(
                object(),
                request,
                budget,
            )

    owner_records.assert_awaited_once()
    fetch_blocks.assert_not_awaited()
    decode_members.assert_not_called()
    assert budget.retained_bytes <= budget.maximum_bytes


@pytest.mark.asyncio
async def test_direct_graph_budget_rejects_before_payload_prefetch(
    monkeypatch,
):
    request = _graph_request()
    selected_member_count = 10
    owner_records = AsyncMock(
        return_value=(_owner_record(selected_member_count),)
    )
    fetch_blocks = AsyncMock()
    decode_members = Mock()
    monkeypatch.setattr(
        shared_readers,
        "_shared_graph_owner_records",
        owner_records,
    )
    monkeypatch.setattr(shared_readers, "fetch_shared_blocks", fetch_blocks)
    monkeypatch.setattr(
        shared_readers,
        "_decoded_scoped_graph_members",
        decode_members,
    )
    preflight_bytes = (
        shared_readers._GRAPH_PREFLIGHT_BYTES
        + shared_readers._GRAPH_PREFLIGHT_OWNER_BYTES
        + shared_readers._GRAPH_PREFLIGHT_CHUNK_MEMBERSHIP_BYTES
    )
    output_bytes = (
        shared_readers._GRAPH_RESULT_OWNER_BYTES
        + selected_member_count * shared_readers._GRAPH_RESULT_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=preflight_bytes + output_bytes - 1
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="graph result owner",
    ):
        await shared_readers._fetch_shared_graph_members_direct(
            object(),
            request,
            budget,
        )

    owner_records.assert_awaited_once()
    fetch_blocks.assert_not_awaited()
    decode_members.assert_not_called()


@pytest.mark.asyncio
async def test_graph_read_limit_releases_transients_and_unproduced_result(
    monkeypatch,
):
    request = _graph_request()
    owner_records = AsyncMock(return_value=(_owner_record(1),))
    fetch_blocks = AsyncMock(
        side_effect=shared_readers.SharedGraphReadLimitError(
            "shared PTG graph chunks exceed the read-once byte limit"
        )
    )
    decode_members = Mock()
    monkeypatch.setattr(
        shared_readers,
        "_shared_graph_owner_records",
        owner_records,
    )
    monkeypatch.setattr(shared_readers, "fetch_shared_blocks", fetch_blocks)
    monkeypatch.setattr(
        shared_readers,
        "_decoded_scoped_graph_members",
        decode_members,
    )
    budget = CandidateAuditDecodedRetentionBudget()

    with shared_readers.shared_block_read_once_scope(
        max_retained_raw_bytes=64 * 1024
    ):
        with pytest.raises(shared_readers.SharedGraphReadLimitError):
            await shared_readers._fetch_shared_graph_members_read_once(
                object(),
                request,
                budget,
            )

    owner_records.assert_awaited_once()
    fetch_blocks.assert_awaited_once()
    decode_members.assert_not_called()
    assert budget.retained_bytes == 0
