# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import sys
import tracemalloc
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_shared_blocks as shared_readers
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)


class _CancellingBudget(CandidateAuditDecodedRetentionBudget):
    def __init__(self, cancelled_category):
        super().__init__()
        self.cancelled_category = cancelled_category

    def claim(self, byte_count, *, category):
        if category == self.cancelled_category:
            raise asyncio.CancelledError()
        super().claim(byte_count, category=category)


class _GraphDecodeAbort(BaseException):
    pass


def _graph_request():
    return shared_readers._shared_graph_read_request(
        schema_name="mrf",
        snapshot_key=12,
        direction=shared_readers.PTG2_V3_GRAPH_NPI_TO_GROUP,
        owner_keys=(1,),
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


def test_graph_owner_normalization_cancellation_releases_partial_claims():
    budget = CandidateAuditDecodedRetentionBudget()

    def owner_keys():
        yield 1
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        shared_readers._budgeted_graph_owner_keys(owner_keys(), budget)

    assert budget.retained_bytes == 0


def test_graph_owner_normalization_deduplicates_with_budget():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    owner_keys, owner_key_set, retained_bytes = (
        shared_readers._budgeted_graph_owner_keys((1, 1), budget)
    )

    assert owner_keys == (1,)
    assert owner_key_set == {1}
    assert budget.retained_bytes == retained_bytes


@pytest.mark.parametrize(
    "cancelled_category",
    (
        "shared graph request metadata",
        "shared graph owner preflight metadata",
        "a decoded shared graph result owner",
    ),
)
@pytest.mark.asyncio
async def test_graph_claim_boundary_cancellation_releases_every_claim(
    monkeypatch,
    cancelled_category,
):
    monkeypatch.setattr(
        shared_readers,
        "_shared_graph_owner_records",
        AsyncMock(return_value=(_owner_record(1),)),
    )
    budget = _CancellingBudget(cancelled_category)

    with pytest.raises(asyncio.CancelledError):
        await shared_readers.fetch_shared_graph_members(
            object(),
            schema_name="mrf",
            snapshot_key=12,
            direction=shared_readers.PTG2_V3_GRAPH_NPI_TO_GROUP,
            owner_keys=(1,),
            retention_budget=budget,
        )

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_graph_payload_fetch_cancellation_releases_result_and_preflight(
    monkeypatch,
):
    monkeypatch.setattr(
        shared_readers,
        "_shared_graph_owner_records",
        AsyncMock(return_value=(_owner_record(1),)),
    )
    monkeypatch.setattr(
        shared_readers,
        "fetch_shared_blocks",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )
    budget = CandidateAuditDecodedRetentionBudget()

    with pytest.raises(asyncio.CancelledError):
        await shared_readers.fetch_shared_graph_members(
            object(),
            schema_name="mrf",
            snapshot_key=12,
            direction=shared_readers.PTG2_V3_GRAPH_NPI_TO_GROUP,
            owner_keys=(1,),
            retention_budget=budget,
        )

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_graph_decode_base_exception_releases_exact_claims(monkeypatch):
    request = _graph_request()
    members_per_chunk = shared_readers.PTG2_V3_GRAPH_CHUNK_BYTES // 4
    selected_member_count = members_per_chunk + 1

    class _AbortingGraphChunks(dict):
        def get(self, chunk_key, default=None):
            if chunk_key == 1:
                raise _GraphDecodeAbort("stop")
            return super().get(chunk_key, default)

    monkeypatch.setattr(
        shared_readers,
        "_shared_graph_owner_records",
        AsyncMock(return_value=(_owner_record(selected_member_count),)),
    )
    monkeypatch.setattr(
        shared_readers,
        "fetch_shared_blocks",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        shared_readers,
        "_validated_graph_chunks",
        Mock(
            return_value=_AbortingGraphChunks(
                {0: b"\x01\x00\x00\x00" * members_per_chunk}
            )
        ),
    )
    budget = CandidateAuditDecodedRetentionBudget()
    preflight_bytes = (
        shared_readers._GRAPH_PREFLIGHT_BYTES
        + shared_readers._GRAPH_PREFLIGHT_OWNER_BYTES
        + 2 * shared_readers._GRAPH_PREFLIGHT_CHUNK_MEMBERSHIP_BYTES
    )
    result_bytes = (
        shared_readers._GRAPH_RESULT_OWNER_BYTES
        + selected_member_count * shared_readers._GRAPH_RESULT_MEMBERSHIP_BYTES
    )

    with pytest.raises(_GraphDecodeAbort, match="stop"):
        await shared_readers._fetch_shared_graph_members_direct(
            object(),
            request,
            budget,
        )

    assert budget.peak_retained_bytes == preflight_bytes + result_bytes
    assert budget.retained_bytes == 0


@pytest.mark.skipif(
    sys.implementation.name != "cpython" or sys.version_info[:2] != (3, 14),
    reason="retention constants are calibrated against CPython 3.14",
)
def test_direct_graph_decode_peak_fits_cpython_result_claim():
    member_count = 100_000
    first_member = 1 << 62
    request = shared_readers._shared_graph_read_request(
        schema_name="mrf",
        snapshot_key=12,
        direction=shared_readers.PTG2_V3_GRAPH_GROUP_TO_NPI,
        owner_keys=(1,),
        max_members=None,
    )
    raw_members = b"".join(
        (first_member + ordinal).to_bytes(8, "little")
        for ordinal in range(member_count)
    )
    raw_chunks_by_key = {
        chunk_key: raw_members[
            offset : offset + shared_readers.PTG2_V3_GRAPH_CHUNK_BYTES
        ]
        for chunk_key, offset in enumerate(
            range(0, len(raw_members), shared_readers.PTG2_V3_GRAPH_CHUNK_BYTES)
        )
    }
    locator_by_owner = {1: (0, 0, member_count, member_count)}
    claimed_result_bytes = (
        shared_readers._GRAPH_RESULT_OWNER_BYTES
        + member_count * shared_readers._GRAPH_RESULT_MEMBERSHIP_BYTES
    )

    tracemalloc.start()
    try:
        decoded = shared_readers._decoded_scoped_graph_members(
            request,
            locator_by_owner,
            raw_chunks_by_key,
        )
        _current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert decoded[1][0] == first_member
    assert decoded[1][-1] == first_member + member_count - 1
    assert peak_bytes <= claimed_result_bytes
