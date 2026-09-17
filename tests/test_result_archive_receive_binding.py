# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused contract coverage for destination PTG archive receive binding."""

from __future__ import annotations

import pytest

from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_receive_binding as subject


class _TransactionSession:
    def in_transaction(self) -> bool:
        return True


def _source_receipt(**overrides):
    return {
        "snapshot_id": "source-snapshot",
        "snapshot_manifest_sha256": "a" * 64,
        "frozen_binding_sha256": "b" * 64,
        **overrides,
    }


def test_destination_filing_identity_is_bounded_stable_and_destination_owned() -> None:
    first = subject._destination_filing_id(
        schema_name="mrf",
        destination_snapshot_id="destination-snapshot",
        source_key="source-a",
        source_receipt=_source_receipt(),
    )
    replay = subject._destination_filing_id(
        schema_name="mrf",
        destination_snapshot_id="destination-snapshot",
        source_key="source-a",
        source_receipt=_source_receipt(),
    )
    other_destination = subject._destination_filing_id(
        schema_name="mrf",
        destination_snapshot_id="other-snapshot",
        source_key="source-a",
        source_receipt=_source_receipt(),
    )

    assert first == replay
    assert first.startswith("archive-")
    assert len(first.encode("utf-8")) == 64
    assert other_destination != first


@pytest.mark.asyncio
async def test_receive_binding_requires_caller_transaction_before_database_access() -> None:
    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="requires an already-open caller transaction",
    ):
        await subject.receive_frozen_binding_params(
            object(),
            schema_name="mrf",
            staging_schema_name="stage",
            source_snapshot_key=1,
            destination_snapshot_id="destination-snapshot",
            source_key="source-a",
            authenticated_source_archive_metadata={},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("source_snapshot_key", [True, "1", 1.5, -1])
async def test_receive_binding_rejects_non_integer_snapshot_keys_before_database_access(
    source_snapshot_key,
) -> None:
    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="receive binding is invalid",
    ):
        await subject.receive_frozen_binding_params(
            _TransactionSession(),
            schema_name="mrf",
            staging_schema_name="stage",
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id="destination-snapshot",
            source_key="source-a",
            authenticated_source_archive_metadata={},
        )
