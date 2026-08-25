# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed shared-layout contracts exercised by packed finalizers."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_blocks as shared_blocks
from process.ptg_parts.ptg2_shared_blocks import (
    SharedBlockReference,
    SharedLayoutReservation,
    SharedMappingDigestSummary,
    _SharedMappingBinaryCopyDigest,
)


class _ScriptedOneSession:
    def __init__(self, responses) -> None:
        self.responses = iter(responses)

    async def execute(self, *_args, **_kwargs):
        return next(self.responses)

def _reference(**changes) -> SharedBlockReference:
    reference_by_field = {
        "object_kind": "kind",
        "block_key": 1,
        "fragment_no": 0,
        "entry_count": 1,
        "block_hash": b"h" * 32,
        "raw_byte_count": 1,
    }
    reference_by_field.update(changes)
    return SharedBlockReference(**reference_by_field)


def test_mapping_digest_rejects_invalid_state_and_reference_fields() -> None:
    digest = _SharedMappingBinaryCopyDigest()
    digest._state = "unexpected"
    with pytest.raises(AssertionError, match="unexpected shared mapping COPY state"):
        digest._has_consumed_copy_state()

    digest = _SharedMappingBinaryCopyDigest()
    digest._state = "field_length"
    digest._buffer.extend(b"xx")
    assert not digest._has_consumed_copy_field_length(2)

    digest = _SharedMappingBinaryCopyDigest()
    digest.begin_copy("kind")
    with pytest.raises(RuntimeError, match="COPY is active"):
        digest.add_mapping(_reference())
    for reference, message in (
        (_reference(object_kind=""), "object_kind is invalid"),
        (_reference(block_hash=b"short"), "block_hash is invalid"),
        (_reference(block_key=-1), "negative value"),
    ):
        with pytest.raises(RuntimeError, match=message):
            _SharedMappingBinaryCopyDigest().add_mapping(reference)
    with pytest.raises(ValueError, match="digest must contain 32 bytes"):
        shared_blocks._advisory_lock_key(b"short")


@pytest.mark.asyncio
async def test_mapping_summary_rejects_missing_raw_driver() -> None:
    class Session:
        async def connection(self):
            return object()

    with pytest.raises(NotImplementedError, match="raw asyncpg COPY support"):
        await shared_blocks._mapping_copy_from_query(Session())


def _aggregate_row(**changes) -> dict[str, object]:
    aggregate_by_field = {
        "object_kind": "kind",
        "mapping_count": 1,
        "unique_block_count": 1,
        "resolved_mapping_count": 1,
        "entry_count": 1,
        "logical_byte_count": 1,
    }
    aggregate_by_field.update(changes)
    return aggregate_by_field


@pytest.mark.parametrize(
    ("row", "message"),
    (
        (_aggregate_row(object_kind=None), "invalid object_kind"),
        (_aggregate_row(mapping_count=0, resolved_mapping_count=0), "empty object_kind"),
    ),
)
def test_mapping_aggregate_rejects_invalid_groups(row, message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        shared_blocks._validated_mapping_aggregate(row)


@pytest.mark.asyncio
async def test_mapping_aggregates_reject_duplicate_kind() -> None:
    class Session:
        async def execute(self, *_args, **_kwargs):
            return (_aggregate_row(), _aggregate_row())

    with pytest.raises(RuntimeError, match="duplicate object_kind"):
        await shared_blocks._mapping_aggregates_by_kind(
            Session(), schema='"mrf"', snapshot_key=7
        )


@pytest.mark.parametrize("case", ("count", "entries"))
def test_relational_mapping_summary_rejects_copy_drift(case: str) -> None:
    parsed = SharedMappingDigestSummary(
        mapping_digest=b"d" * 32,
        mapping_count=0 if case == "count" else 1,
        unique_block_count=1,
        entry_count=0,
        logical_byte_count=1,
        canonical_byte_count=1,
        object_kinds=("kind",),
    )
    message = "mapping count changed" if case == "count" else "entry_count changed"
    with pytest.raises(RuntimeError, match=message):
        shared_blocks._validated_relational_mapping_summary(
            parsed,
            aggregate_by_kind={"kind": (1, 1, 1, 1)},
            object_kinds=("kind",),
        )


class _Scalar:
    def __init__(self, value) -> None:
        self.value = value

    def scalar(self):
        return self.value


@pytest.mark.asyncio
async def test_shared_layout_abandonment_and_heartbeat_fence_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _ScriptedOneSession((_Scalar(None), _Scalar(None)))
    monkeypatch.setattr(
        shared_blocks, "_is_shared_layout_build_owned", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        shared_blocks, "_is_shared_layout_bound", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(shared_blocks, "_queue_snapshot_blocks_for_gc", AsyncMock())
    monkeypatch.setattr(shared_blocks, "delete_shared_layout_dense_rows", AsyncMock())
    with pytest.raises(RuntimeError, match="ownership changed during abandonment"):
        await shared_blocks.is_shared_layout_build_abandoned(
            session,
            schema_name="mrf",
            snapshot_key=7,
            build_token="build",
        )
    with pytest.raises(RuntimeError, match="heartbeat lost ownership"):
        await shared_blocks.touch_shared_layout_build(
            session,
            schema_name="mrf",
            snapshot_key=7,
            build_token="build",
        )


@pytest.mark.asyncio
async def test_shared_layout_candidate_and_creation_receipts_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation = await shared_blocks._matching_layout_candidate(
        _ScriptedOneSession((_Scalar(7),)),
        schema='"mrf"',
        fingerprint=b"f" * 32,
        build_token="build",
    )
    assert reservation == SharedLayoutReservation(7, False, None)
    with pytest.raises(RuntimeError, match="did not return a key"):
        await shared_blocks._create_shared_layout_reservation(
            _ScriptedOneSession((_Scalar(None),)),
            schema='"mrf"',
            schema_name="mrf",
            fingerprint=b"f" * 32,
            build_token="build",
            storage_shard_id=0,
        )

    expected = SharedLayoutReservation(9, False, None)
    monkeypatch.setattr(shared_blocks, "acquire_ptg2_source_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(shared_blocks, "_matching_shared_layout", AsyncMock(return_value=None))
    monkeypatch.setattr(
        shared_blocks, "_matching_layout_candidate", AsyncMock(return_value=expected)
    )
    create = AsyncMock()
    monkeypatch.setattr(shared_blocks, "_create_shared_layout_reservation", create)
    assert await shared_blocks.reserve_shared_layout(
        object(),
        schema_name="mrf",
        semantic_fingerprint=b"f" * 32,
        build_token="build",
    ) == expected
    create.assert_not_awaited()


def test_unique_block_rows_reject_inconsistent_hash_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_block_by_field = {"block_hash": b"h" * 32, "format_version": 2, "object_kind": "a",
             "codec": "none", "entry_count": 1, "raw_byte_count": 1,
             "stored_byte_count": 1, "payload": b"a"}
    second_block_by_field = {**first_block_by_field, "object_kind": "b"}
    monkeypatch.setattr(shared_blocks, "_block_insert_rows", lambda _blocks: [first_block_by_field, second_block_by_field])
    with pytest.raises(ValueError, match="inconsistent block metadata"):
        shared_blocks._unique_block_rows(())


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("hash", "metadata", "missing"))
async def test_block_insert_validation_rejects_storage_drift(case: str) -> None:
    block_hash = b"h" * 32
    expected_block_by_field = {
        "block_hash": block_hash,
        "format_version": 2,
        "object_kind": "kind",
        "codec": "none",
        "entry_count": 1,
        "raw_byte_count": 1,
        "stored_byte_count": 1,
        "payload": b"x",
    }
    if case == "hash":
        stored = ({**expected_block_by_field, "block_hash": b"x" * 32},)
        message = "unexpected hash"
    elif case == "metadata":
        stored = ({**expected_block_by_field, "entry_count": 2},)
        message = "metadata mismatch"
    else:
        stored = ()
        message = "did not retain every requested hash"
    session = _ScriptedOneSession((None, stored))
    with pytest.raises(RuntimeError, match=message):
        await shared_blocks._insert_and_validate_block_rows(
            session,
            schema='"mrf"',
            block_rows_by_hash={block_hash: expected_block_by_field},
        )


@pytest.mark.asyncio
async def test_v4_binding_requires_complete_map_root() -> None:
    with pytest.raises(RuntimeError, match="requires a complete V4 map root"):
        await shared_blocks._require_v4_layout_for_binding(
            _ScriptedOneSession((_Scalar(None),)),
            schema='"mrf"',
            snapshot_key=7,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("same", "other", "insert", "retry", "lost"))
async def test_v4_binding_retries_only_same_layout(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    monkeypatch.setattr(shared_blocks, "lock_writable_snapshot", AsyncMock())
    monkeypatch.setattr(
        shared_blocks, "_has_inserted_v3_snapshot_binding", AsyncMock(return_value=False)
    )
    if case in {"same", "other"}:
        existing = 7 if case == "same" else 8
        monkeypatch.setattr(
            shared_blocks, "_existing_snapshot_binding_key", AsyncMock(return_value=existing)
        )
    else:
        retry = None if case == "insert" else (7 if case == "retry" else 8)
        monkeypatch.setattr(
            shared_blocks,
            "_existing_snapshot_binding_key",
            AsyncMock(side_effect=(None, retry)),
        )
        monkeypatch.setattr(shared_blocks, "_require_v4_layout_for_binding", AsyncMock())
        monkeypatch.setattr(
            shared_blocks,
            "_has_inserted_v4_snapshot_binding",
            AsyncMock(return_value=case == "insert"),
        )
    if case in {"other", "lost"}:
        with pytest.raises(RuntimeError, match="another layout"):
            await shared_blocks.bind_snapshot_to_shared_layout(
                object(), schema_name="mrf", snapshot_id="snapshot", snapshot_key=7
            )
    else:
        await shared_blocks.bind_snapshot_to_shared_layout(
            object(), schema_name="mrf", snapshot_id="snapshot", snapshot_key=7
        )


def _shared_summary(digest: bytes = b"d" * 32) -> SharedMappingDigestSummary:
    return SharedMappingDigestSummary(
        mapping_digest=digest,
        mapping_count=1,
        unique_block_count=1,
        entry_count=1,
        logical_byte_count=1,
        canonical_byte_count=1,
        object_kinds=("kind",),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("owner", "digest", "update"))
async def test_shared_layout_seal_rejects_ownership_and_digest_drift(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    expected = _shared_summary(b"short" if case == "digest" else b"d" * 32)
    responses = [_Scalar(None if case == "owner" else 7)]
    if case == "update":
        responses.extend((_Scalar(None), _Scalar(None)))
    session = _ScriptedOneSession(responses)
    monkeypatch.setattr(shared_blocks, "acquire_ptg2_source_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(
        shared_blocks,
        "summarize_shared_snapshot_mappings",
        AsyncMock(return_value=expected),
    )
    monkeypatch.setattr(shared_blocks, "acquire_layout_digest_lock", AsyncMock())
    with pytest.raises(RuntimeError, match="lost ownership|digest must contain|expected building"):
        await shared_blocks.seal_shared_layout(
            session,
            schema_name="mrf",
            snapshot_key=7,
            build_token="build",
            expected_summary=expected,
            support_digest=b"s" * 32,
            layout_manifest={},
        )
