# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_shared_blocks as shared_readers
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)
from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    fetch_snapshot_source_set_identity,
)


class _AuditAbort(BaseException):
    pass


class _Rows:
    def __init__(self, rows):
        self._rows = tuple(rows)

    def __iter__(self):
        return iter(self._rows)


class _Session:
    def __init__(self, rows):
        self.rows = tuple(rows)

    async def execute(self, _statement, _params=None):
        return _Rows(self.rows)


def test_shared_uvarint_and_mapping_budget_hard_limits():
    with pytest.raises(PTG2SharedBlockError, match="exceeds uint64"):
        shared_readers.read_strict_uvarint(b"\x80" * 10, 0)

    with pytest.raises(ValueError, match="mapping-metadata byte limit"):
        shared_readers.SharedBlockReadOnceScope(
            max_retained_raw_bytes=1,
            max_retained_mapping_bytes=(
                shared_readers._SHARED_MAPPING_RECORD_RETAINED_BYTES - 1
            ),
        )


def test_read_once_scope_rejects_conflicting_decoded_budgets():
    scope = shared_readers.SharedBlockReadOnceScope(max_retained_raw_bytes=1)
    first_budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    scope.bind_decoded_retention_budget(first_budget)
    scope.bind_decoded_retention_budget(first_budget)

    with pytest.raises(PTG2SharedBlockError, match="conflicting decoded budgets"):
        scope.bind_decoded_retention_budget(
            CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
        )


@pytest.mark.asyncio
async def test_read_once_mapping_failure_releases_retained_metadata(monkeypatch):
    scope = shared_readers.SharedBlockReadOnceScope(max_retained_raw_bytes=1)
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    scope.bind_decoded_retention_budget(budget)
    retained_metadata_bytes = 128
    budget.claim(retained_metadata_bytes, category="mapping metadata")
    selection = shared_readers._SharedMappingSelection(
        mapping_records=(),
        observed_block_keys=frozenset(),
        fragment_nos_by_block_key={},
        physical_hashes=frozenset(),
        retained_metadata_bytes=retained_metadata_bytes,
    )
    monkeypatch.setattr(
        scope,
        "_validated_mapping_selection",
        AsyncMock(return_value=selection),
    )
    request = shared_readers._shared_block_read_request(
        schema_name="mrf",
        snapshot_key=1,
        object_kind="test",
        block_keys=(1,),
        fragment_nos=None,
        require_all=True,
    )

    with pytest.raises(PTG2SharedBlockError, match="missing block keys"):
        await scope._fetch_locked(object(), request)

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_source_identity_row_base_exception_releases_claims():
    async def aborting_rows():
        yield {"source_key": 0, "raw_container_sha256": "1" * 64}
        raise _AuditAbort("stop")

    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    with pytest.raises(_AuditAbort, match="stop"):
        await shared_readers._snapshot_source_identity_rows(
            aborting_rows(),
            budget,
        )
    assert budget.retained_bytes == 0

    with pytest.raises(_AuditAbort, match="stop"):
        await shared_readers._snapshot_source_identity_rows(
            aborting_rows(),
            None,
        )


@pytest.mark.asyncio
async def test_source_identity_validation_failure_releases_row_claims():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    with pytest.raises(PTG2SharedBlockError, match="identity metadata is invalid"):
        await fetch_snapshot_source_set_identity(
            _Session([{"source_key": 0, "raw_container_sha256": "bad"}]),
            schema_name="mrf",
            logical_snapshot_id="snapshot",
            expected_source_count=1,
            retention_budget=budget,
        )

    assert budget.retained_bytes == 0
