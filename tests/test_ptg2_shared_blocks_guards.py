# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
from unittest.mock import Mock

import pytest

from api import ptg2_shared_blocks as shared_readers
from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    PTG2_V3_GRAPH_CHUNK_BYTES,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    decode_shared_block_payload,
    fetch_shared_graph_members,
    fetch_snapshot_source_set_identity,
)
from process.ptg_parts.ptg2_shared_blocks import shared_block_hash


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


def _stored_row(
    *,
    object_kind: str,
    block_key: int,
    fragment_no: int,
    raw_payload: bytes,
    extra: dict | None = None,
):
    stored_by_field = {
        "object_kind": object_kind,
        "block_key": block_key,
        "fragment_no": fragment_no,
        "mapping_entry_count": 1,
        "format_version": 2,
        "codec": "none",
        "raw_byte_count": len(raw_payload),
        "payload": raw_payload,
        "block_hash": shared_block_hash(
            format_version=2,
            object_kind=object_kind,
            codec="none",
            payload=raw_payload,
        ),
    }
    stored_by_field.update(extra or {})
    return stored_by_field


def _provenance_row(**overrides):
    provenance_by_field = {
        "source_key": 0,
        "source_type": "in_network",
        "identity_kind": "raw_container_sha256",
        "identity_sha256": "1" * 64,
        "raw_container_sha256": "2" * 64,
        "logical_json_sha256": "3" * 64,
        "logical_hash_deferred": False,
        "source_trace_set_hash": "4" * 64,
        "source_count": 1,
        "distinct_source_count": 1,
        "minimum_source_key": 0,
        "maximum_source_key": 0,
        "trace_hash_count": 1,
        "resolved_trace_count": 1,
        "source_trace": [{"line_number": 7}],
    }
    provenance_by_field.update(overrides)
    return provenance_by_field


@pytest.mark.asyncio
async def test_source_provenance_empty_and_json_trace_paths():
    assert await shared_readers.fetch_snapshot_source_provenance(
        object(),
        schema_name="mrf",
        logical_snapshot_id="snapshot",
        source_keys=(),
        expected_source_count=1,
    ) == {}

    provenance = await shared_readers.fetch_snapshot_source_provenance(
        _Session([_provenance_row(source_trace='[{"line_number": 7}]')]),
        schema_name="mrf",
        logical_snapshot_id="snapshot",
        source_keys=(0,),
        expected_source_count=1,
    )
    assert provenance[0]["source_trace"] == [{"line_number": 7}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "snapshot_id", "source_keys", "message"),
    (
        ((), "snapshot", (1,), "outside the manifest"),
        ((), "", (0,), "snapshot id is missing"),
        ((_provenance_row(source_count=2),), "snapshot", (0,), "complete and dense"),
        ((_provenance_row(identity_sha256="bad"),), "snapshot", (0,), "identity or trace"),
        ((_provenance_row(source_trace="{"),), "snapshot", (0,), "trace payload is malformed"),
        ((_provenance_row(source_trace={}),), "snapshot", (0,), "trace payload is malformed"),
        ((_provenance_row(), _provenance_row()), "snapshot", (0,), "duplicate source key"),
        ((), "snapshot", (0,), "missing a selected source key"),
    ),
)
async def test_source_provenance_rejects_invalid_rows(
    rows,
    snapshot_id,
    source_keys,
    message,
):
    with pytest.raises(PTG2SharedBlockError, match=message):
        await shared_readers.fetch_snapshot_source_provenance(
            _Session(rows),
            schema_name="mrf",
            logical_snapshot_id=snapshot_id,
            source_keys=source_keys,
            expected_source_count=1,
        )


@pytest.mark.asyncio
async def test_shared_validation_translates_lower_level_payload_errors():
    with pytest.raises(PTG2SharedBlockError, match="zlib"):
        decode_shared_block_payload(
            codec="zlib",
            encoded_payload=b"not-zlib",
            raw_byte_count=1,
        )
    with pytest.raises(PTG2SharedBlockError, match="kind and codec"):
        shared_readers._validated_physical_block(
            {
                "object_kind": "",
                "codec": "none",
                "format_version": 2,
                "payload": b"",
                "raw_byte_count": 0,
                "block_hash": b"x" * 32,
            },
            expected_kind="",
        )
    with pytest.raises(PTG2SharedBlockError, match="identity metadata is invalid"):
        await fetch_snapshot_source_set_identity(
            _Session([{"source_key": 0, "raw_container_sha256": "bad"}]),
            schema_name="mrf",
            logical_snapshot_id="snapshot",
            expected_source_count=1,
        )


def _graph_request(*, owner_keys=(1, 2), max_members=None):
    return shared_readers._shared_graph_read_request(
        schema_name="mrf",
        snapshot_key=12,
        direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
        owner_keys=owner_keys,
        max_members=max_members,
    )


def test_graph_request_guards():
    with pytest.raises(ValueError, match="unsupported"):
        shared_readers._shared_graph_read_request(
            schema_name="mrf",
            snapshot_key=12,
            direction=99,
            owner_keys=(1,),
            max_members=None,
        )
    with pytest.raises(ValueError, match="non-negative"):
        _graph_request(max_members=True)


def test_graph_owner_selection_skips_empty_owner_and_limits_one_owner():
    request = _graph_request()
    selection = shared_readers._validated_graph_owner_selection(
        request,
        (
            {
                "owner_key": 1,
                "first_chunk": 0,
                "member_offset": 0,
                "member_count": 1,
                "selected_member_count": 0,
            },
            {
                "owner_key": 2,
                "first_chunk": 1,
                "member_offset": 0,
                "member_count": 1,
                "selected_member_count": 1,
            },
        ),
        maximum_raw_bytes=PTG2_V3_GRAPH_CHUNK_BYTES,
    )
    assert selection.required_chunk_keys == frozenset({1})

    with pytest.raises(PTG2SharedBlockError, match="owner exceeds"):
        shared_readers._validated_graph_owner_selection(
            request,
            (
                {
                    "owner_key": 1,
                    "first_chunk": 0,
                    "member_offset": PTG2_V3_GRAPH_CHUNK_BYTES - 4,
                    "member_count": 2,
                    "selected_member_count": 2,
                },
            ),
            maximum_raw_bytes=1,
        )


def test_graph_owner_selection_limits_chunks_and_validates_owner():
    request = _graph_request()
    with pytest.raises(PTG2SharedBlockError, match="chunks exceed"):
        shared_readers._validated_graph_owner_selection(
            request,
            (
                {
                    "owner_key": 1,
                    "first_chunk": 0,
                    "member_offset": 0,
                    "member_count": 1,
                    "selected_member_count": 1,
                },
                {
                    "owner_key": 2,
                    "first_chunk": 1,
                    "member_offset": 0,
                    "member_count": 1,
                    "selected_member_count": 1,
                },
            ),
            maximum_raw_bytes=1,
        )
    with pytest.raises(PTG2SharedBlockError, match="invalid or unordered"):
        shared_readers._validated_graph_owner_selection(
            request,
            (
                {
                    "owner_key": 3,
                    "first_chunk": 0,
                    "member_offset": 0,
                    "member_count": 1,
                    "selected_member_count": 1,
                },
            ),
            maximum_raw_bytes=PTG2_V3_GRAPH_CHUNK_BYTES,
        )


def test_scoped_graph_chunk_and_decode_guards(monkeypatch):
    request = _graph_request()
    assert shared_readers._selected_graph_bytes(
        {}, first_chunk=0, member_offset=0, byte_count=0
    ) == b""
    with pytest.raises(PTG2SharedBlockError, match="stream is truncated"):
        shared_readers._selected_graph_bytes(
            {}, first_chunk=0, member_offset=0, byte_count=4
        )
    assert shared_readers._validated_graph_chunks(request, {}) == {}

    invalid_fragment = shared_readers.SharedBlockPayload(
        0,
        1,
        1,
        b"1234",
        b"a" * 32,
    )
    with pytest.raises(PTG2SharedBlockError, match="fragment layout"):
        shared_readers._validated_graph_chunks(request, {0: (invalid_fragment,)})

    monkeypatch.setattr(shared_readers, "claim_shared_block_processing", Mock())
    empty_fragment = shared_readers.SharedBlockPayload(0, 0, 0, b"", b"b" * 32)
    with pytest.raises(PTG2SharedBlockError, match="member framing"):
        shared_readers._validated_graph_chunks(request, {0: (empty_fragment,)})

    raw_chunk = b"\0" * PTG2_V3_GRAPH_CHUNK_BYTES
    aliased = shared_readers.SharedBlockPayload(
        0,
        0,
        PTG2_V3_GRAPH_CHUNK_BYTES // 4,
        raw_chunk,
        b"c" * 32,
    )
    chunks = shared_readers._validated_graph_chunks(
        request,
        {0: (aliased,), 1: (replace(aliased, block_key=1),)},
    )
    assert chunks == {0: raw_chunk, 1: raw_chunk}
    assert shared_readers._decoded_scoped_graph_members(request, {}, {}) == {
        1: (),
        2: (),
    }


def test_direct_graph_locator_and_decode_guards():
    request = _graph_request(owner_keys=(1,))
    with pytest.raises(PTG2SharedBlockError, match="member count is invalid"):
        shared_readers._indexed_direct_graph_records(
            request,
            (
                {
                    "owner_key": 1,
                    "member_offset": 0,
                    "member_count": -1,
                    "selected_member_count": 0,
                },
            ),
        )

    stored_block_record = _stored_row(
        object_kind="graph_npi_groups_v1",
        block_key=0,
        fragment_no=0,
        raw_payload=b"\0" * 4,
        extra={
            "owner_key": 1,
            "member_offset": 0,
            "member_count": 1,
            "selected_member_count": 1,
        },
    )
    with pytest.raises(PTG2SharedBlockError, match="locator changed"):
        shared_readers._indexed_direct_graph_records(
            request,
            (
                stored_block_record,
                {**stored_block_record, "selected_member_count": 0},
            ),
        )

    with pytest.raises(PTG2SharedBlockError, match="stream is truncated"):
        shared_readers._decoded_direct_graph_members(
            request,
            {1: (0, 1, 1)},
            {
                1: [
                    shared_readers.SharedBlockPayload(
                        0,
                        0,
                        1,
                        b"",
                        b"d" * 32,
                    )
                ]
            },
        )


@pytest.mark.asyncio
async def test_graph_empty_and_missing_scope_guards():
    request = _graph_request(owner_keys=(1,))
    with pytest.raises(PTG2SharedBlockError, match="read-once scope is missing"):
        await shared_readers._fetch_shared_graph_members_read_once(object(), request)
    assert await fetch_shared_graph_members(
        object(),
        schema_name="mrf",
        snapshot_key=12,
        direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
        owner_keys=(),
    ) == {}
