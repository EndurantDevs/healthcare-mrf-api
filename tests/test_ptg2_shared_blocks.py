from __future__ import annotations

import os
import struct
import uuid
import zlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import text

from api import ptg2_db_serving_v3, ptg2_db_sidecars
from api import ptg2_shared_blocks as shared_readers
from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    PTG2_V3_GRAPH_CHUNK_BYTES,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    decode_shared_block_payload,
    fetch_shared_blocks,
    fetch_shared_graph_members,
    fetch_snapshot_source_set_metadata,
    fetch_snapshot_source_set_identity,
    shared_block_read_once_scope,
)
from db.connection import db
from process.ptg_parts import ptg2_shared_blocks as shared_blocks_module
from process.ptg_parts import ptg2_serving_binary_v3
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_DENSE_LAYOUT_TABLES,
    SharedBlockReference,
    SharedMappingDigestSummary,
    _SharedMappingBinaryCopyDigest,
    is_shared_layout_build_abandoned,
    lock_shared_layout_for_dense_write,
    SharedBlock,
    bind_snapshot_to_shared_layout,
    delete_shared_layout_dense_rows,
    reserve_shared_layout,
    seal_shared_layout,
    shared_block_hash,
    shared_mapping_digest,
    shared_semantic_fingerprint,
    shared_support_digest,
    summarize_shared_snapshot_mappings,
    touch_shared_layout_build,
)
from process.ptg_parts.ptg2_serving_binary_v3_types import (
    PTG2V3PriceAtomRecord,
)


_PG_BINARY_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def __iter__(self):
        return iter(self._rows)


class _Session:
    def __init__(self, result_rows):
        self.result_rows = list(result_rows)
        self.calls = []

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        return _Rows(self.result_rows)


class _Result(_Rows):
    def __init__(self, rows=(), scalar_value=None):
        super().__init__(rows)
        self.scalar_value = scalar_value

    def scalar(self):
        return self.scalar_value

    def first(self):
        return self._rows[0] if self._rows else None


class _ScriptedSession:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        assert self.results, f"unexpected SQL: {statement}"
        return self.results.pop(0)


class _ReadOnceSession:
    def __init__(self, *, mapping_rows, physical_rows, owner_rows=()):
        self.mapping_rows = list(mapping_rows)
        self.physical_rows = list(physical_rows)
        self.owner_rows = list(owner_rows)
        self.calls = []

    async def execute(self, statement, params=None):
        sql = str(statement)
        params_by_name = dict(params or {})
        self.calls.append((sql, params_by_name))
        if "ptg2_v3_graph_owner" in sql:
            return _Rows(self.owner_rows)
        if "ptg2_v3_snapshot_block" in sql:
            requested_keys = set(params_by_name["block_keys"])
            return _Rows(
                row
                for row in self.mapping_rows
                if int(row["block_key"]) in requested_keys
            )
        if "ptg2_v3_block" in sql:
            requested_hashes = set(params_by_name["block_hashes"])
            return _Rows(
                row
                for row in self.physical_rows
                if bytes(row["block_hash"]) in requested_hashes
            )
        raise AssertionError(f"unexpected SQL: {sql}")


class _MappingCopyDriver:
    def __init__(self, streams_by_kind, *, chunk_sizes=(1, 7, 2, 31, 3, 64)):
        self.streams_by_kind = dict(streams_by_kind)
        self.chunk_sizes = tuple(chunk_sizes)
        self.calls = []

    async def copy_from_query(self, query, *args, output, **copy_options):
        copy_format = copy_options.pop("format")
        assert not copy_options
        self.calls.append((query, args, copy_format))
        stream = self.streams_by_kind[args[1]]
        offset = 0
        chunk_index = 0
        while offset < len(stream):
            chunk_size = self.chunk_sizes[chunk_index % len(self.chunk_sizes)]
            await output(stream[offset : offset + chunk_size])
            offset += chunk_size
            chunk_index += 1
        return f"COPY {len(stream)}"


class _MappingCopyConnection:
    def __init__(self, driver):
        self.driver = driver
        self.raw_connection_calls = 0

    async def get_raw_connection(self):
        self.raw_connection_calls += 1
        return SimpleNamespace(driver_connection=self.driver)


class _MappingCopySession:
    def __init__(self, aggregate_rows, driver):
        self.aggregate_rows = list(aggregate_rows)
        self.connection_object = _MappingCopyConnection(driver)
        self.calls = []
        self.connection_calls = 0

    async def connection(self):
        self.connection_calls += 1
        return self.connection_object

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        return _Rows(self.aggregate_rows)


def _mapping_record(reference: SharedBlockReference) -> bytes:
    object_kind = reference.object_kind.encode("utf-8")
    return b"".join(
        (
            struct.pack(">I", len(object_kind)),
            object_kind,
            struct.pack(">q", reference.block_key),
            struct.pack(">I", reference.fragment_no),
            struct.pack(">Q", reference.entry_count),
            reference.block_hash,
        )
    )


def _binary_copy_stream(
    records: list[bytes],
    *,
    flags: int = 0,
    extension: bytes = b"",
) -> bytes:
    stream = bytearray(b"PGCOPY\n\xff\r\n\0")
    stream.extend(struct.pack(">II", flags, len(extension)))
    stream.extend(extension)
    for record in records:
        stream.extend(struct.pack(">hi", 1, len(record)))
        stream.extend(record)
    stream.extend(struct.pack(">h", -1))
    return bytes(stream)


def _mapping_streams_by_kind(
    references: list[SharedBlockReference],
) -> dict[str, bytes]:
    streams_by_kind = {}
    object_kinds = sorted({reference.object_kind for reference in references})
    for object_kind in object_kinds:
        kind_references = sorted(
            (
                reference
                for reference in references
                if reference.object_kind == object_kind
            ),
            key=lambda reference: (reference.block_key, reference.fragment_no),
        )
        streams_by_kind[object_kind] = _binary_copy_stream(
            [_mapping_record(reference) for reference in kind_references]
        )
    return streams_by_kind


def _mapping_summary(
    references: list[SharedBlockReference],
) -> SharedMappingDigestSummary:
    return SharedMappingDigestSummary(
        mapping_digest=shared_mapping_digest(references),
        mapping_count=len(references),
        unique_block_count=len({reference.block_hash for reference in references}),
        entry_count=sum(reference.entry_count for reference in references),
        logical_byte_count=sum(reference.raw_byte_count for reference in references),
        canonical_byte_count=sum(
            len(_mapping_record(reference)) for reference in references
        ),
        object_kinds=tuple(sorted({reference.object_kind for reference in references})),
    )


def _stored_row(
    *,
    object_kind: str,
    block_key: int,
    fragment_no: int,
    raw_payload: bytes,
    codec: str = "none",
    extra: dict | None = None,
):
    payload = zlib.compress(raw_payload, 1) if codec == "zlib" else raw_payload
    stored_row_map = {
        "object_kind": object_kind,
        "block_key": block_key,
        "fragment_no": fragment_no,
        "mapping_entry_count": 1,
        "format_version": 2,
        "codec": codec,
        "raw_byte_count": len(raw_payload),
        "payload": payload,
        "block_hash": shared_block_hash(
            format_version=2,
            object_kind=object_kind,
            codec=codec,
            payload=payload,
        ),
    }
    stored_row_map.update(extra or {})
    return stored_row_map


def _read_once_rows(
    *,
    object_kind: str,
    raw_payload: bytes,
    coordinates: tuple[tuple[int, int], ...],
    codec: str = "none",
    entry_count: int = 1,
):
    stored = _stored_row(
        object_kind=object_kind,
        block_key=coordinates[0][0],
        fragment_no=coordinates[0][1],
        raw_payload=raw_payload,
        codec=codec,
    )
    mapping_rows = [
        {
            "object_kind": object_kind,
            "block_key": block_key,
            "fragment_no": fragment_no,
            "mapping_entry_count": entry_count,
            "block_hash": stored["block_hash"],
        }
        for block_key, fragment_no in coordinates
    ]
    physical_record_by_name = {
        "block_hash": stored["block_hash"],
        "object_kind": object_kind,
        "format_version": stored["format_version"],
        "codec": codec,
        "block_entry_count": entry_count,
        "raw_byte_count": len(raw_payload),
        "payload": stored["payload"],
    }
    return mapping_rows, [physical_record_by_name]


def _provider_code_payload(code_key):
    encoded_codes, _stats = ptg2_serving_binary_v3.encode_provider_code_set(
        (code_key,)
    )
    payload = bytearray()
    ptg2_serving_binary_v3.append_uvarint(payload, 1)
    ptg2_serving_binary_v3.append_uvarint(payload, 0)
    ptg2_serving_binary_v3.append_uvarint(payload, len(encoded_codes))
    payload.extend(encoded_codes)
    return bytes(payload)


def _partially_aliased_provider_rows():
    object_kind = "provider_set_codes_v3"
    first_payload = _provider_code_payload(1)
    second_payload = _provider_code_payload(2)
    shared_prefix_size = next(
        index
        for index, pair in enumerate(zip(first_payload, second_payload))
        if pair[0] != pair[1]
    )
    raw_parts = (
        first_payload[:shared_prefix_size],
        first_payload[shared_prefix_size:],
        second_payload[shared_prefix_size:],
    )
    stored_parts = [
        _stored_row(
            object_kind=object_kind,
            block_key=0,
            fragment_no=fragment_no,
            raw_payload=raw_payload,
        )
        for fragment_no, raw_payload in enumerate(raw_parts)
    ]
    entry_counts = (1, 0, 0)
    physical_rows = [
        {
            "block_hash": stored_part["block_hash"],
            "object_kind": object_kind,
            "format_version": stored_part["format_version"],
            "codec": stored_part["codec"],
            "block_entry_count": entry_count,
            "raw_byte_count": stored_part["raw_byte_count"],
            "payload": stored_part["payload"],
        }
        for stored_part, entry_count in zip(stored_parts, entry_counts)
    ]
    mapping_rows = [
        {
            "object_kind": object_kind,
            "block_key": block_key,
            "fragment_no": fragment_no,
            "mapping_entry_count": entry_counts[part_index],
            "block_hash": stored_parts[part_index]["block_hash"],
        }
        for block_key, part_indexes in ((0, (0, 1)), (1, (0, 2)))
        for fragment_no, part_index in enumerate(part_indexes)
    ]
    return mapping_rows, physical_rows, raw_parts


def test_semantic_fingerprint_is_order_stable_and_context_sensitive():
    first = shared_semantic_fingerprint(
        {"files": [{"sha256": "a"}], "plan": "one", "arch": "shared_blocks_v3"}
    )
    reordered = shared_semantic_fingerprint(
        {"arch": "shared_blocks_v3", "plan": "one", "files": [{"sha256": "a"}]}
    )
    other_plan = shared_semantic_fingerprint(
        {"files": [{"sha256": "a"}], "plan": "two", "arch": "shared_blocks_v3"}
    )

    assert first == reordered
    assert first != other_plan
    assert len(first) == 32


def test_shared_block_hash_excludes_mapping_key_but_includes_format_and_kind():
    first = SharedBlock("page_v4", 1, 0, 2, "none", 3, b"abc")
    other_mapping = SharedBlock("page_v4", 99, 7, 2, "none", 3, b"abc")
    other_kind = SharedBlock("provider_page_v4", 1, 0, 2, "none", 3, b"abc")

    assert first.block_hash == other_mapping.block_hash
    assert first.block_hash != other_kind.block_hash

    with pytest.raises(ValueError, match="format_version 2"):
        SharedBlock("page_v4", 1, 0, 1, "none", 1, b"a", format_version=1)


def test_mapping_digest_is_order_stable_and_rejects_duplicate_keys():
    first = SharedBlock("page_v4", 1, 0, 1, "none", 1, b"a")
    second = SharedBlock("page_v4", 2, 0, 1, "none", 1, b"b")

    assert shared_mapping_digest([first, second]) == shared_mapping_digest([second, first])
    with pytest.raises(ValueError, match="duplicate"):
        shared_mapping_digest([first, first])


def test_binary_copy_mapping_digest_is_exact_for_unicode_and_arbitrary_chunks():
    references = [
        SharedBlockReference("🧪_kind", 9, 1, 5, b"e" * 32, 17),
        SharedBlockReference("a_kind", 2, 0, 3, b"a" * 32, 7),
        SharedBlockReference("ž_kind", 4, 2, 7, b"d" * 32, 13),
        SharedBlockReference("a_kind", 1, 3, 2, b"a" * 32, 7),
        SharedBlockReference("ž_kind", 4, 1, 6, b"c" * 32, 12),
    ]
    accumulator = _SharedMappingBinaryCopyDigest()
    streams_by_kind = _mapping_streams_by_kind(references)

    for object_kind in sorted(streams_by_kind):
        accumulator.begin_copy(object_kind)
        stream = streams_by_kind[object_kind]
        offset = 0
        chunk_sizes = (1, 13, 2, 29, 3, 5, 47)
        chunk_index = 0
        while offset < len(stream):
            chunk_size = chunk_sizes[chunk_index % len(chunk_sizes)]
            accumulator.feed(stream[offset : offset + chunk_size])
            offset += chunk_size
            chunk_index += 1
        assert accumulator.finish_copy() == sum(
            reference.object_kind == object_kind for reference in references
        )

    parsed = accumulator.finish()

    assert parsed.mapping_digest == shared_mapping_digest(references)
    assert parsed.mapping_count == len(references)
    assert parsed.entry_count == sum(reference.entry_count for reference in references)
    assert parsed.canonical_byte_count == sum(
        len(_mapping_record(reference)) for reference in references
    )


@pytest.mark.parametrize(
    ("stream", "error"),
    [
        (
            b"X" + _binary_copy_stream([])[1:],
            "header signature",
        ),
        (
            _binary_copy_stream([], flags=1),
            "header flags",
        ),
        (
            _binary_copy_stream([], extension=b"x"),
            "header extension",
        ),
        (
            _PG_BINARY_COPY_HEADER + struct.pack(">h", 2),
            "exactly one field",
        ),
        (
            _PG_BINARY_COPY_HEADER
            + struct.pack(">hi", 1, -1)
            + struct.pack(">h", -1),
            "must not be NULL",
        ),
        (
            _PG_BINARY_COPY_HEADER + struct.pack(">hi", 1, 10_000),
            "invalid length",
        ),
        (
            _binary_copy_stream(
                [
                    _mapping_record(
                        SharedBlockReference("a", 1, 0, 1, b"a" * 32, 1)
                    )
                    + b"x"
                ]
            ),
            "does not match object_kind",
        ),
        (
            _binary_copy_stream(
                [_mapping_record(SharedBlockReference("a", -1, 0, 1, b"a" * 32, 1))]
            ),
            "negative mapping value",
        ),
        (
            _binary_copy_stream(
                [
                    _mapping_record(
                        SharedBlockReference("a", 1, 0, 1, b"a" * 32, 1)
                    ),
                    _mapping_record(
                        SharedBlockReference("a", 1, 0, 1, b"a" * 32, 1)
                    ),
                ]
            ),
            "duplicate mapping key",
        ),
        (
            _binary_copy_stream(
                [
                    _mapping_record(
                        SharedBlockReference("a", 2, 0, 1, b"b" * 32, 1)
                    ),
                    _mapping_record(
                        SharedBlockReference("a", 1, 0, 1, b"a" * 32, 1)
                    ),
                ]
            ),
            "out-of-order mapping key",
        ),
        (
            _binary_copy_stream(
                [_mapping_record(SharedBlockReference("a", 1, 0, 1, b"a" * 32, 1))]
            )
            + b"x",
            "after its trailer",
        ),
    ],
)
def test_binary_copy_mapping_digest_rejects_malformed_streams(stream, error):
    accumulator = _SharedMappingBinaryCopyDigest()
    accumulator.begin_copy("a")

    with pytest.raises(RuntimeError, match=error):
        accumulator.feed(stream)


@pytest.mark.parametrize("removed_bytes", [1, 2, 3, 20])
def test_binary_copy_mapping_digest_rejects_truncated_streams(removed_bytes):
    stream = _binary_copy_stream(
        [_mapping_record(SharedBlockReference("a", 1, 0, 1, b"a" * 32, 1))]
    )
    accumulator = _SharedMappingBinaryCopyDigest()
    accumulator.begin_copy("a")
    accumulator.feed(stream[:-removed_bytes])

    with pytest.raises(RuntimeError, match="truncated"):
        accumulator.finish_copy()


@pytest.mark.asyncio
async def test_mapping_summary_uses_supplied_session_and_matches_python_digest():
    """Match the streaming SQL digest to Python using the supplied session."""

    references = [
        SharedBlockReference("🧪_kind", 9, 1, 5, b"e" * 32, 17),
        SharedBlockReference("a_kind", 2, 0, 3, b"b" * 32, 11),
        SharedBlockReference("ž_kind", 4, 2, 7, b"d" * 32, 13),
        SharedBlockReference("a_kind", 1, 3, 2, b"a" * 32, 7),
    ]
    aggregate_rows = []
    for object_kind in {reference.object_kind for reference in references}:
        kind_references = [
            reference
            for reference in references
            if reference.object_kind == object_kind
        ]
        aggregate_rows.append(
            {
                "object_kind": object_kind,
                "mapping_count": len(kind_references),
                "unique_block_count": len(
                    {reference.block_hash for reference in kind_references}
                ),
                "resolved_mapping_count": len(kind_references),
                "entry_count": sum(
                    reference.entry_count for reference in kind_references
                ),
                "logical_byte_count": sum(
                    reference.raw_byte_count for reference in kind_references
                ),
            }
        )
    driver = _MappingCopyDriver(_mapping_streams_by_kind(references))
    session = _MappingCopySession(aggregate_rows, driver)

    summary = await summarize_shared_snapshot_mappings(
        session,
        schema_name="mrf",
        snapshot_key=41,
    )

    assert summary.mapping_digest == shared_mapping_digest(references)
    assert summary.mapping_count == len(references)
    assert summary.unique_block_count == len(
        {reference.block_hash for reference in references}
    )
    assert summary.entry_count == sum(reference.entry_count for reference in references)
    assert summary.logical_byte_count == sum(
        reference.raw_byte_count for reference in references
    )
    assert summary.canonical_byte_count == sum(
        len(_mapping_record(reference)) for reference in references
    )
    assert summary.object_kinds == tuple(
        sorted({reference.object_kind for reference in references})
    )
    assert summary.object_kind_count == 3
    assert session.connection_calls == 1
    assert session.connection_object.raw_connection_calls == 1
    assert session.calls[0][1] == {"snapshot_key": 41}
    assert "LEFT JOIN \"mrf\".ptg2_v3_block" in session.calls[0][0]
    assert [call[1][1] for call in driver.calls] == list(summary.object_kinds)
    assert all(call[1][0] == 41 and call[2] == "binary" for call in driver.calls)


@pytest.mark.asyncio
async def test_mapping_summary_rejects_missing_raw_asyncpg_copy_support():
    session = _MappingCopySession([], SimpleNamespace())

    with pytest.raises(NotImplementedError, match="asyncpg copy_from_query"):
        await summarize_shared_snapshot_mappings(
            session,
            schema_name="mrf",
            snapshot_key=41,
        )


@pytest.mark.asyncio
async def test_mapping_summary_rejects_join_and_copy_count_anomalies():
    """Reject inconsistent aggregate, join, and COPY mapping counts."""

    invalid_unique_session = _MappingCopySession(
        [
            {
                "object_kind": "a",
                "mapping_count": 1,
                "unique_block_count": 2,
                "resolved_mapping_count": 1,
                "entry_count": 1,
                "logical_byte_count": 1,
            }
        ],
        _MappingCopyDriver({}),
    )
    with pytest.raises(RuntimeError, match="unique_block_count exceeds"):
        await summarize_shared_snapshot_mappings(
            invalid_unique_session,
            schema_name="mrf",
            snapshot_key=41,
        )

    unresolved_driver = _MappingCopyDriver({})
    unresolved_session = _MappingCopySession(
        [
            {
                "object_kind": "a",
                "mapping_count": 2,
                "unique_block_count": 1,
                "resolved_mapping_count": 1,
                "entry_count": 2,
                "logical_byte_count": 1,
            }
        ],
        unresolved_driver,
    )
    with pytest.raises(RuntimeError, match="resolve every block_hash"):
        await summarize_shared_snapshot_mappings(
            unresolved_session,
            schema_name="mrf",
            snapshot_key=41,
        )

    reference = SharedBlockReference("a", 1, 0, 1, b"a" * 32, 1)
    changed_driver = _MappingCopyDriver({"a": _binary_copy_stream([_mapping_record(reference)])})
    changed_session = _MappingCopySession(
        [
            {
                "object_kind": "a",
                "mapping_count": 2,
                "unique_block_count": 1,
                "resolved_mapping_count": 2,
                "entry_count": 2,
                "logical_byte_count": 2,
            }
        ],
        changed_driver,
    )
    with pytest.raises(RuntimeError, match="count changed during binary COPY"):
        await summarize_shared_snapshot_mappings(
            changed_session,
            schema_name="mrf",
            snapshot_key=41,
        )


def test_support_digest_is_order_stable_and_context_sensitive():
    first = shared_support_digest({"codes": 2, "graph": {"edges": 9}})
    reordered = shared_support_digest({"graph": {"edges": 9}, "codes": 2})
    changed = shared_support_digest({"codes": 2, "graph": {"edges": 10}})

    assert first == reordered
    assert first != changed
    assert len(first) == 32


@pytest.mark.asyncio
async def test_snapshot_source_set_is_recomputed_from_complete_dense_rows():
    session = _Session(
        [
            {"source_key": 0, "raw_container_sha256": "2" * 64},
            {"source_key": 1, "raw_container_sha256": "1" * 64},
        ]
    )

    metadata = await fetch_snapshot_source_set_metadata(
        session,
        schema_name="mrf",
        logical_snapshot_id="snapshot-1",
        expected_source_count=2,
    )

    assert metadata["source_count"] == 2
    assert len(metadata["raw_container_sha256_digest"]) == 64
    assert session.calls[0][1]["row_limit"] == 3


@pytest.mark.asyncio
async def test_snapshot_source_identity_preserves_dense_ordinal_order():
    first_session = _Session(
        [
            {"source_key": 0, "raw_container_sha256": "1" * 64},
            {"source_key": 1, "raw_container_sha256": "2" * 64},
        ]
    )
    reversed_session = _Session(
        [
            {"source_key": 0, "raw_container_sha256": "2" * 64},
            {"source_key": 1, "raw_container_sha256": "1" * 64},
        ]
    )

    first_set, first_ordered, first_raw_hashes = (
        await fetch_snapshot_source_set_identity(
            first_session,
            schema_name="mrf",
            logical_snapshot_id="snapshot-1",
            expected_source_count=2,
        )
    )
    reversed_set, reversed_ordered, reversed_raw_hashes = (
        await fetch_snapshot_source_set_identity(
            reversed_session,
            schema_name="mrf",
            logical_snapshot_id="snapshot-1",
            expected_source_count=2,
        )
    )

    assert first_set == reversed_set
    assert first_ordered != reversed_ordered
    assert first_raw_hashes == ("1" * 64, "2" * 64)
    assert reversed_raw_hashes == ("2" * 64, "1" * 64)


@pytest.mark.asyncio
async def test_snapshot_source_set_rejects_omitted_or_extra_rows():
    session = _Session(
        [{"source_key": 0, "raw_container_sha256": "1" * 64}]
    )

    with pytest.raises(PTG2SharedBlockError, match="complete and dense"):
        await fetch_snapshot_source_set_metadata(
            session,
            schema_name="mrf",
            logical_snapshot_id="snapshot-1",
            expected_source_count=2,
        )


@pytest.mark.asyncio
async def test_dense_layout_cleanup_explicitly_deletes_every_unconstrained_table():
    session = _ScriptedSession([_Result() for _ in PTG2_V3_DENSE_LAYOUT_TABLES])

    await delete_shared_layout_dense_rows(
        session,
        schema_name="mrf",
        snapshot_key=41,
    )

    assert len(session.calls) == len(PTG2_V3_DENSE_LAYOUT_TABLES)
    for (sql, params), table_name in zip(session.calls, PTG2_V3_DENSE_LAYOUT_TABLES):
        assert f'"mrf"."{table_name}"' in sql
        assert params == {"snapshot_key": 41}


@pytest.mark.asyncio
async def test_failed_build_abandonment_is_token_fenced_and_queues_blocks():
    session = _ScriptedSession(
        [
            _Result(scalar_value=41),
            _Result(scalar_value=None),
            _Result(),
            *[_Result() for _ in PTG2_V3_DENSE_LAYOUT_TABLES],
            _Result(scalar_value=41),
        ]
    )

    abandoned = await is_shared_layout_build_abandoned(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="attempt-41",
    )

    assert abandoned is True
    statements = [sql for sql, _params in session.calls]
    assert "FOR UPDATE" in statements[0]
    assert "ptg2_v3_snapshot_binding" in statements[1]
    assert "ptg2_v3_gc_candidate" in statements[2]
    assert "RETURNING snapshot_key" in statements[-1]
    assert session.calls[-1][1]["build_token"] == "attempt-41"


@pytest.mark.asyncio
async def test_failed_build_abandonment_never_touches_another_attempt_or_binding():
    not_owned = _ScriptedSession([_Result(scalar_value=None)])
    assert not await is_shared_layout_build_abandoned(
        not_owned,
        schema_name="mrf",
        snapshot_key=42,
        build_token="another-attempt",
    )
    assert len(not_owned.calls) == 1

    bound = _ScriptedSession(
        [_Result(scalar_value=42), _Result(scalar_value=1)]
    )
    with pytest.raises(RuntimeError, match="bound shared PTG layout"):
        await is_shared_layout_build_abandoned(
            bound,
            schema_name="mrf",
            snapshot_key=42,
            build_token="attempt-42",
        )
    assert len(bound.calls) == 2


@pytest.mark.asyncio
async def test_dense_write_lock_requires_current_build_token():
    owned = _ScriptedSession([_Result(scalar_value=43)])
    await lock_shared_layout_for_dense_write(
        owned,
        schema_name="mrf",
        snapshot_key=43,
        build_token="attempt-43",
    )
    sql, params = owned.calls[0]
    assert "build_token = :build_token" in sql
    assert params == {
        "snapshot_key": 43,
        "generation": "shared_blocks_v3",
        "build_token": "attempt-43",
    }

    stale = _ScriptedSession([_Result(scalar_value=None)])
    with pytest.raises(RuntimeError, match="lost its build ownership"):
        await lock_shared_layout_for_dense_write(
            stale,
            schema_name="mrf",
            snapshot_key=43,
            build_token="stale-attempt",
        )


@pytest.mark.asyncio
async def test_seal_locks_ownership_then_rechecks_authoritative_summary(monkeypatch):
    expected = SharedBlock(
        "page_v4",
        7,
        0,
        3,
        "none",
        9,
        b"123456789",
    ).reference()
    expected_summary = _mapping_summary([expected])
    summarize = AsyncMock(return_value=expected_summary)
    monkeypatch.setattr(
        shared_blocks_module,
        "summarize_shared_snapshot_mappings",
        summarize,
    )
    session = _ScriptedSession(
        [
            _Result(scalar_value=41),
            _Result(),
            _Result(scalar_value=None),
            _Result(scalar_value=41),
        ]
    )

    sealed = await seal_shared_layout(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="attempt-41",
        expected_summary=expected_summary,
        support_digest=b"s" * 32,
        layout_manifest={"contract": "strict-v3"},
    )

    assert sealed.snapshot_key == 41
    ownership_sql = session.calls[0][0]
    assert "ptg2_v3_snapshot_layout" in ownership_sql
    assert "FOR UPDATE" in ownership_sql
    summarize.assert_awaited_once_with(
        session,
        schema_name="mrf",
        snapshot_key=41,
    )
    update_params = session.calls[-1][1]
    assert update_params["logical_byte_count"] == expected.raw_byte_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "observed_value"),
    [
        ("mapping_digest", b"x" * 32),
        ("mapping_count", 2),
        ("unique_block_count", 0),
        ("entry_count", 2),
        ("logical_byte_count", 2),
        ("canonical_byte_count", 99),
        ("object_kinds", ("other_kind",)),
    ],
)
async def test_seal_rejects_every_authoritative_summary_mismatch(
    monkeypatch,
    field_name,
    observed_value,
):
    expected = SharedBlock("page_v4", 7, 0, 1, "none", 1, b"a").reference()
    expected_summary = _mapping_summary([expected])
    observed_summary = replace(
        expected_summary,
        **{field_name: observed_value},
    )
    summarize = AsyncMock(return_value=observed_summary)
    monkeypatch.setattr(
        shared_blocks_module,
        "summarize_shared_snapshot_mappings",
        summarize,
    )
    session = _ScriptedSession([_Result(scalar_value=41)])

    with pytest.raises(RuntimeError, match=f"summary mismatch for {field_name}"):
        await seal_shared_layout(
            session,
            schema_name="mrf",
            snapshot_key=41,
            build_token="attempt-41",
            expected_summary=expected_summary,
            support_digest=b"s" * 32,
            layout_manifest={},
        )


@pytest.mark.asyncio
async def test_seal_exact_reuse_does_not_queue_still_referenced_blocks_for_gc(
    monkeypatch,
):
    expected = SharedBlock("page_v4", 7, 0, 1, "none", 1, b"a").reference()
    expected_summary = _mapping_summary([expected])
    monkeypatch.setattr(
        shared_blocks_module,
        "summarize_shared_snapshot_mappings",
        AsyncMock(return_value=expected_summary),
    )
    session = _ScriptedSession(
        [
            _Result(scalar_value=41),
            _Result(),
            _Result(scalar_value=17),
            _Result(),
            _Result(),
            *[_Result() for _ in PTG2_V3_DENSE_LAYOUT_TABLES],
            _Result(),
        ]
    )

    sealed = await seal_shared_layout(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="attempt-41",
        expected_summary=expected_summary,
        support_digest=b"s" * 32,
        layout_manifest={"contract": "strict-v3"},
    )

    assert sealed.snapshot_key == 17
    assert sealed.reused is True
    assert not any(
        "ptg2_v3_gc_candidate" in sql
        for sql, _params in session.calls
    )
    assert any(
        "DELETE FROM \"mrf\".ptg2_v3_snapshot_layout" in sql
        for sql, _params in session.calls
    )


def test_decode_shared_block_payload_is_strict():
    raw = b"payload" * 100
    compressed = zlib.compress(raw, 1)

    assert (
        decode_shared_block_payload(
            codec="zlib",
            encoded_payload=compressed,
            raw_byte_count=len(raw),
        )
        == raw
    )
    with pytest.raises(PTG2SharedBlockError, match="length mismatch"):
        decode_shared_block_payload(
            codec="none",
            encoded_payload=b"x",
            raw_byte_count=2,
        )
    with pytest.raises(PTG2SharedBlockError, match="unsupported"):
        decode_shared_block_payload(
            codec="gzip",
            encoded_payload=b"x",
            raw_byte_count=1,
        )
    with pytest.raises(PTG2SharedBlockError, match="byte limit"):
        decode_shared_block_payload(
            codec="zlib",
            encoded_payload=compressed,
            raw_byte_count=len(raw),
            maximum_raw_bytes=len(raw) - 1,
        )
    with pytest.raises(PTG2SharedBlockError, match="framing"):
        decode_shared_block_payload(
            codec="zlib",
            encoded_payload=compressed,
            raw_byte_count=8,
            maximum_raw_bytes=8,
        )


def test_decode_shared_block_payload_rejects_limit_before_zlib_allocation(monkeypatch):
    def unexpected_decompressor():
        raise AssertionError("decompression must not start beyond the raw-byte limit")

    monkeypatch.setattr(zlib, "decompressobj", unexpected_decompressor)

    with pytest.raises(PTG2SharedBlockError, match="byte limit"):
        decode_shared_block_payload(
            codec="zlib",
            encoded_payload=b"not inspected",
            raw_byte_count=1024,
            maximum_raw_bytes=16,
        )


@pytest.mark.parametrize(
    ("operation", "message"),
    [
        (lambda: shared_readers.read_strict_uvarint(b"", 0), "ended inside"),
        (
            lambda: shared_readers.read_strict_uvarint(b"\x80" * 9 + b"\x02", 0),
            "exceeds uint64",
        ),
        (
            lambda: shared_readers.read_strict_uvarint(b"\x80\0", 0),
            "non-canonical",
        ),
        (lambda: shared_readers.dense_source_key_bits(0), "source_count"),
        (
            lambda: shared_readers.decode_dense_source_header(
                b"",
                0,
                format_version=1,
            ),
            "format version",
        ),
        (
            lambda: shared_readers.decode_dense_source_header(
                b"\x01\x01",
                0,
                format_version=1,
            ),
            "missing source_bits",
        ),
        (
            lambda: shared_readers.decode_dense_source_vector(
                b"",
                0,
                entry_count=-1,
                source_count=2,
                source_bits=1,
            ),
            "count is negative",
        ),
        (
            lambda: shared_readers.decode_dense_source_vector(
                b"",
                0,
                entry_count=0,
                source_count=2,
                source_bits=2,
            ),
            "width is invalid",
        ),
        (
            lambda: shared_readers.decode_dense_source_vector(
                b"",
                0,
                entry_count=1,
                source_count=2,
                source_bits=1,
            ),
            "truncated",
        ),
        (
            lambda: shared_readers.decode_dense_source_vector(
                b"\x02",
                0,
                entry_count=1,
                source_count=2,
                source_bits=1,
            ),
            "padding bits",
        ),
        (
            lambda: shared_readers.decode_dense_source_vector(
                b"\x03",
                0,
                entry_count=1,
                source_count=3,
                source_bits=2,
            ),
            "out-of-range key",
        ),
    ],
)
def test_shared_dense_codec_guards_reject_invalid_payloads(operation, message):
    with pytest.raises(PTG2SharedBlockError, match=message):
        operation()


def test_shared_row_mapping_supports_mapping_rows_and_pairs():
    mapped_row = type("MappedRow", (), {"_mapping": {"value": 1}})()

    assert shared_readers._row_mapping(mapped_row) == {"value": 1}
    assert shared_readers._row_mapping((("value", 2),)) == {"value": 2}


def test_shared_block_validators_reject_entry_count_disagreement():
    stored_row = _stored_row(
        object_kind="page_v4",
        block_key=7,
        fragment_no=0,
        raw_payload=b"payload",
    )
    with pytest.raises(PTG2SharedBlockError, match="entry count"):
        shared_readers._validated_physical_block(
            {**stored_row, "block_entry_count": -1},
            expected_kind="page_v4",
        )
    with pytest.raises(PTG2SharedBlockError, match="entry count"):
        shared_readers._validated_payload(
            {
                **stored_row,
                "block_entry_count": 1,
                "mapping_entry_count": 2,
            },
            expected_kind="page_v4",
        )


def test_read_once_scope_rejects_invalid_state_transitions():
    unread_hash = b"x" * 32
    with pytest.raises(ValueError, match="limit must be positive"):
        shared_readers.SharedBlockReadOnceScope(max_retained_raw_bytes=0)
    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        with pytest.raises(PTG2SharedBlockError, match="no physical read"):
            scope.prepare_payload("mrf", unread_hash)
        with pytest.raises(PTG2SharedBlockError, match="logical payload is empty"):
            scope.register_logical_payload("mrf", ())
        with pytest.raises(PTG2SharedBlockError, match="unread fragment"):
            scope.register_logical_payload("mrf", (unread_hash,))
        with pytest.raises(PTG2SharedBlockError, match="logical payload is empty"):
            scope.claim_logical_payload_processing("mrf", ())
        with pytest.raises(PTG2SharedBlockError, match="unprepared fragment"):
            scope.claim_logical_payload_processing("mrf", (unread_hash,))
        with pytest.raises(PTG2SharedBlockError, match="cannot be nested"):
            with shared_block_read_once_scope(
                max_retained_raw_bytes=1024
            ) as nested_scope:
                assert nested_scope is not scope


async def _read_once_fetch(
    session,
    *,
    block_keys=(7,),
    fragment_nos=None,
    require_all=True,
):
    with shared_block_read_once_scope(max_retained_raw_bytes=1024):
        return await fetch_shared_blocks(
            session,
            schema_name="mrf",
            snapshot_key=12,
            object_kind="page_v4",
            block_keys=block_keys,
            fragment_nos=fragment_nos,
            require_all=require_all,
        )


@pytest.mark.asyncio
async def test_shared_fetch_rejects_bad_coordinates_and_incomplete_mappings():
    empty_session = _ReadOnceSession(mapping_rows=(), physical_rows=())
    with pytest.raises(ValueError, match="non-negative"):
        await _read_once_fetch(empty_session, fragment_nos=(-1,))

    assert await _read_once_fetch(
        _ReadOnceSession(mapping_rows=(), physical_rows=()),
        block_keys=(),
    ) == {}
    assert await fetch_shared_blocks(
        _Session([]),
        schema_name="mrf",
        snapshot_key=12,
        object_kind="page_v4",
        block_keys=(),
    ) == {}
    assert await _read_once_fetch(
        _ReadOnceSession(mapping_rows=(), physical_rows=()),
        require_all=False,
    ) == {}

    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"payload",
        coordinates=((7, 0),),
    )
    mapping_rows[0]["fragment_no"] = -1
    with pytest.raises(PTG2SharedBlockError, match="unexpected or unordered"):
        await _read_once_fetch(
            _ReadOnceSession(
                mapping_rows=mapping_rows,
                physical_rows=physical_rows,
            )
        )

    with pytest.raises(PTG2SharedBlockError, match="missing block keys"):
        await _read_once_fetch(
            _ReadOnceSession(mapping_rows=(), physical_rows=())
        )

    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"payload",
        coordinates=((7, 0),),
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )
    assert 7 in await _read_once_fetch(session, fragment_nos=(0,))
    with pytest.raises(PTG2SharedBlockError, match="missing fragments"):
        await _read_once_fetch(
            _ReadOnceSession(
                mapping_rows=mapping_rows,
                physical_rows=physical_rows,
            ),
            fragment_nos=(0, 1),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("duplicate", "unexpected row"),
        ("missing", "missing physical block"),
        ("entry_count", "entry count"),
    ],
)
async def test_read_once_fetch_rejects_invalid_physical_results(failure, message):
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"payload",
        coordinates=((7, 0),),
    )
    if failure == "duplicate":
        physical_rows.append(dict(physical_rows[0]))
    elif failure == "missing":
        physical_rows.clear()
    else:
        mapping_rows[0]["mapping_entry_count"] = 2

    with pytest.raises(PTG2SharedBlockError, match=message):
        await _read_once_fetch(
            _ReadOnceSession(
                mapping_rows=mapping_rows,
                physical_rows=physical_rows,
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "fragment_nos", "message"),
    [
        (
            [
                _stored_row(
                    object_kind="page_v4",
                    block_key=8,
                    fragment_no=0,
                    raw_payload=b"payload",
                )
            ],
            None,
            "unexpected or unordered",
        ),
        ([], None, "missing block keys"),
        (
            [
                _stored_row(
                    object_kind="page_v4",
                    block_key=7,
                    fragment_no=0,
                    raw_payload=b"payload",
                )
            ],
            (0, 1),
            "missing fragments",
        ),
    ],
)
async def test_direct_fetch_rejects_invalid_or_incomplete_results(
    rows,
    fragment_nos,
    message,
):
    with pytest.raises(PTG2SharedBlockError, match=message):
        await fetch_shared_blocks(
            _Session(rows),
            schema_name="mrf",
            snapshot_key=12,
            object_kind="page_v4",
            block_keys=(7,),
            fragment_nos=fragment_nos,
            require_all=True,
        )


def test_shared_read_once_ledger_guards_and_inactive_helpers():
    block_hash = b"x" * 32
    shared_readers.prepare_shared_block_payload(
        schema_name="mrf",
        block_hash=block_hash,
    )
    shared_readers.register_shared_logical_payload(
        schema_name="mrf",
        physical_hashes=(block_hash,),
    )
    shared_readers.claim_shared_logical_payload_processing(
        schema_name="mrf",
        physical_hashes=(block_hash,),
    )

    poisoned = shared_readers.SharedBlockReadOnceScope(max_retained_raw_bytes=1)
    poisoned._poison(RuntimeError("first"))
    poisoned._poison(RuntimeError("second"))
    with pytest.raises(PTG2SharedBlockError, match="poisoned: first"):
        poisoned.assert_read_once()

    inconsistent = shared_readers.SharedBlockReadOnceScope(
        max_retained_raw_bytes=1,
    )
    inconsistent._physical_rows_read = 1
    with pytest.raises(PTG2SharedBlockError, match="does not prove"):
        inconsistent.assert_read_once()


@pytest.mark.asyncio
async def test_source_identity_rejects_unbounded_source_count_before_query():
    with pytest.raises(PTG2SharedBlockError, match="bounded verification limit"):
        await fetch_snapshot_source_set_identity(
            object(),
            schema_name="mrf",
            logical_snapshot_id="snapshot",
            expected_source_count=(
                shared_readers.PTG2_V3_MAX_AUDIT_SOURCE_FILES + 1
            ),
        )
    with pytest.raises(PTG2SharedBlockError, match="snapshot id is missing"):
        await fetch_snapshot_source_set_identity(
            object(),
            schema_name="mrf",
            logical_snapshot_id="",
            expected_source_count=1,
        )


@pytest.mark.asyncio
async def test_fetch_shared_blocks_uses_one_stable_query_without_process_cache():
    stored_rows = [
        _stored_row(object_kind="page_v4", block_key=7, fragment_no=0, raw_payload=b"first"),
        _stored_row(
            object_kind="page_v4",
            block_key=7,
            fragment_no=1,
            raw_payload=b"second",
            codec="zlib",
        ),
    ]
    session = _Session(stored_rows)

    first = await fetch_shared_blocks(
        session,
        schema_name="mrf",
        snapshot_key=12,
        object_kind="page_v4",
        block_keys=[7],
        require_all=True,
    )
    second = await fetch_shared_blocks(
        session,
        schema_name="mrf",
        snapshot_key=12,
        object_kind="page_v4",
        block_keys=[7],
        require_all=True,
    )

    assert [block.payload for block in first[7]] == [b"first", b"second"]
    assert second == first
    assert len(session.calls) == 2
    assert all("ptg2_v3_snapshot_block" in sql and "ptg2_v3_block" in sql for sql, _ in session.calls)


@pytest.mark.asyncio
async def test_read_once_scope_fetches_and_decodes_one_shared_physical_block_once():
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"same immutable payload",
        coordinates=((7, 0), (8, 0)),
        codec="zlib",
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        blocks_by_key = await fetch_shared_blocks(
            session,
            schema_name="mrf",
            snapshot_key=12,
            object_kind="page_v4",
            block_keys=(7, 8),
            require_all=True,
        )
        scope.assert_read_once()
        ledger = scope.ledger

    assert blocks_by_key[7][0].payload == blocks_by_key[8][0].payload
    assert ledger == {
        "logical_block_deliveries": 2,
        "physical_mapping_references": 2,
        "physical_mapping_aliases": 1,
        "unique_physical_blocks": 1,
        "physical_block_reads": 1,
        "physical_block_decodes": 1,
        "physical_payload_preparations": 0,
        "expected_logical_payload_processes": 0,
        "logical_payload_processes": 0,
        "logical_payload_fragment_references": 0,
        "logical_payload_fragment_aliases": 0,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": len(b"same immutable payload"),
    }
    assert len(session.calls) == 2
    assert sum("ptg2_v3_block" in sql for sql, _params in session.calls) == 1


@pytest.mark.asyncio
async def test_forward_alias_is_read_decoded_and_parsed_once_then_fanned_out():
    block_7 = 7 << 31
    block_8 = 8 << 31
    forward_block_bytes = b"\x02\x01\x00\x05\x01\x08"
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="by_code_provider_shard_v1",
        raw_payload=forward_block_bytes,
        coordinates=((block_7, 0), (block_8, 0)),
        codec="zlib",
        entry_count=1,
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        price_keys_by_occurrence = await (
            ptg2_db_sidecars.lookup_forward_price_index_from_db(
                session,
                (7, 8),
                provider_set_keys_by_code={7: (5,), 8: (5,)},
                source_keys_by_code={7: (0,), 8: (0,)},
                shared_snapshot_key=12,
                source_count=1,
                price_dictionary_item_count=128,
                price_dictionary_block_bytes=2048,
            )
        )
        scope.assert_processed_once()
        ledger = scope.ledger

    assert price_keys_by_occurrence == {
        (7, 5, 0): (8,),
        (8, 5, 0): (8,),
    }
    assert ledger == {
        "logical_block_deliveries": 2,
        "physical_mapping_references": 2,
        "physical_mapping_aliases": 1,
        "unique_physical_blocks": 1,
        "physical_block_reads": 1,
        "physical_block_decodes": 1,
        "physical_payload_preparations": 1,
        "expected_logical_payload_processes": 1,
        "logical_payload_processes": 1,
        "logical_payload_fragment_references": 1,
        "logical_payload_fragment_aliases": 0,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": len(forward_block_bytes),
    }


@pytest.mark.asyncio
async def test_price_atom_alias_is_parsed_once_then_rebased_and_fanned_out(
    monkeypatch,
):
    price_atom = PTG2V3PriceAtomRecord("12.34", (None,))
    atom_block_bytes = ptg2_serving_binary_v3.encode_price_atoms((price_atom,))
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="price_atoms_v3",
        raw_payload=atom_block_bytes,
        coordinates=((0, 0), (1, 0)),
        entry_count=1,
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )
    header_spy = Mock(wraps=ptg2_serving_binary_v3._price_atom_header)
    monkeypatch.setattr(
        ptg2_serving_binary_v3,
        "_price_atom_header",
        header_spy,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        atoms_by_key = await ptg2_db_sidecars.lookup_shared_price_atoms_from_db(
            session,
            12,
            atom_keys=(0, 512),
            block_span=512,
            schema_name="mrf",
        )
        scope.assert_processed_once()
        ledger = scope.ledger

    assert atoms_by_key == {0: price_atom, 512: price_atom}
    assert atoms_by_key[0] is atoms_by_key[512]
    header_spy.assert_called_once_with(atom_block_bytes)
    assert ledger == {
        "logical_block_deliveries": 2,
        "physical_mapping_references": 2,
        "physical_mapping_aliases": 1,
        "unique_physical_blocks": 1,
        "physical_block_reads": 1,
        "physical_block_decodes": 1,
        "physical_payload_preparations": 1,
        "expected_logical_payload_processes": 1,
        "logical_payload_processes": 1,
        "logical_payload_fragment_references": 1,
        "logical_payload_fragment_aliases": 0,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": len(atom_block_bytes),
    }


@pytest.mark.asyncio
async def test_partially_aliased_fragments_prepare_once_and_parse_each_logical_payload(
    monkeypatch,
):
    """Distinguish physical preparation from necessary logical tuple parses."""

    mapping_rows, physical_rows, raw_parts = _partially_aliased_provider_rows()
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )
    decoder_spy = Mock(wraps=ptg2_db_serving_v3._decode_provider_code_block)
    monkeypatch.setattr(
        ptg2_db_serving_v3,
        "_decode_provider_code_block",
        decoder_spy,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        code_keys_by_provider = await (
            ptg2_db_sidecars.lookup_provider_code_keys_from_db(
                session,
                12,
                provider_set_keys=(0, 1024),
                schema_name="mrf",
            )
        )
        scope.assert_processed_once()
        ledger = scope.ledger

    assert code_keys_by_provider == {0: (1,), 1024: (2,)}
    assert decoder_spy.call_count == 2
    assert ledger == {
        "logical_block_deliveries": 4,
        "physical_mapping_references": 4,
        "physical_mapping_aliases": 1,
        "unique_physical_blocks": 3,
        "physical_block_reads": 3,
        "physical_block_decodes": 3,
        "physical_payload_preparations": 3,
        "expected_logical_payload_processes": 2,
        "logical_payload_processes": 2,
        "logical_payload_fragment_references": 4,
        "logical_payload_fragment_aliases": 1,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": sum(len(raw_part) for raw_part in raw_parts),
    }


@pytest.mark.asyncio
async def test_read_once_scope_requires_every_registered_logical_payload():
    """Reject missing expected tuples and fabricated replacement claims."""

    mapping_rows, physical_rows, _raw_parts = _partially_aliased_provider_rows()
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        logical_blocks = await ptg2_db_sidecars._shared_logical_blocks_by_key(
            session,
            shared_snapshot_key=12,
            schema_name="mrf",
            artifact_kind="provider_set_codes_v3",
            block_keys=(0, 1),
        )
        scope.claim_logical_payload_processing(
            "mrf",
            logical_blocks[0].physical_hashes,
        )
        with pytest.raises(PTG2SharedBlockError, match="was not registered"):
            scope.claim_logical_payload_processing(
                "mrf",
                (logical_blocks[1].physical_hashes[-1],),
            )
        with pytest.raises(PTG2SharedBlockError, match="ledger is incomplete"):
            scope.assert_processed_once()


@pytest.mark.asyncio
async def test_read_once_scope_rejects_duplicate_physical_processing_claim():
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"process once",
        coordinates=((7, 0),),
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        blocks_by_key = await fetch_shared_blocks(
            session,
            schema_name="mrf",
            snapshot_key=12,
            object_kind="page_v4",
            block_keys=(7,),
            require_all=True,
        )
        block_hash = blocks_by_key[7][0].block_hash
        scope.claim_payload_processing("mrf", block_hash)
        with pytest.raises(PTG2SharedBlockError, match="prepared more than once"):
            scope.claim_payload_processing("mrf", block_hash)


@pytest.mark.asyncio
async def test_read_once_scope_rejects_duplicate_logical_payload_processing():
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"process logical payload once",
        coordinates=((7, 0),),
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        blocks_by_key = await fetch_shared_blocks(
            session,
            schema_name="mrf",
            snapshot_key=12,
            object_kind="page_v4",
            block_keys=(7,),
            require_all=True,
        )
        block_hash = blocks_by_key[7][0].block_hash
        scope.prepare_payload("mrf", block_hash)
        scope.register_logical_payload("mrf", (block_hash,))
        scope.claim_logical_payload_processing("mrf", (block_hash,))
        with pytest.raises(PTG2SharedBlockError, match="processed more than once"):
            scope.claim_logical_payload_processing("mrf", (block_hash,))
        scope.assert_processed_once()


@pytest.mark.asyncio
async def test_read_once_scope_rejects_same_hash_under_later_coordinate():
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"one physical payload",
        coordinates=((7, 0), (8, 0)),
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024):
        await fetch_shared_blocks(
            session,
            schema_name="mrf",
            snapshot_key=12,
            object_kind="page_v4",
            block_keys=(7,),
            require_all=True,
        )
        with pytest.raises(PTG2SharedBlockError, match="requested more than once"):
            await fetch_shared_blocks(
                session,
                schema_name="mrf",
                snapshot_key=12,
                object_kind="page_v4",
                block_keys=(8,),
                require_all=True,
            )

    assert sum("ptg2_v3_block" in sql for sql, _params in session.calls) == 1


@pytest.mark.asyncio
async def test_read_once_scope_rejects_incomplete_processing_ledger():
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"missing processing claim",
        coordinates=((7, 0),),
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        await fetch_shared_blocks(
            session,
            schema_name="mrf",
            snapshot_key=12,
            object_kind="page_v4",
            block_keys=(7,),
            require_all=True,
        )
        with pytest.raises(PTG2SharedBlockError, match="ledger is incomplete"):
            scope.assert_processed_once()


@pytest.mark.asyncio
async def test_read_once_scope_rejects_a_second_logical_block_delivery():
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"one delivery",
        coordinates=((7, 0),),
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024):
        await fetch_shared_blocks(
            session,
            schema_name="mrf",
            snapshot_key=12,
            object_kind="page_v4",
            block_keys=(7,),
            require_all=True,
        )
        with pytest.raises(PTG2SharedBlockError, match="more than once"):
            await fetch_shared_blocks(
                session,
                schema_name="mrf",
                snapshot_key=12,
                object_kind="page_v4",
                block_keys=(7,),
                require_all=True,
            )

    assert len(session.calls) == 3
    assert sum("ptg2_v3_block" in sql for sql, _params in session.calls) == 1


@pytest.mark.asyncio
async def test_read_once_scope_poisoned_corruption_is_never_retried():
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"valid bytes",
        coordinates=((7, 0),),
    )
    physical_rows[0]["payload"] = b"corrupt bytes"
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024):
        with pytest.raises(PTG2SharedBlockError, match="identity"):
            await fetch_shared_blocks(
                session,
                schema_name="mrf",
                snapshot_key=12,
                object_kind="page_v4",
                block_keys=(7,),
                require_all=True,
            )
        with pytest.raises(PTG2SharedBlockError, match="poisoned"):
            await fetch_shared_blocks(
                session,
                schema_name="mrf",
                snapshot_key=12,
                object_kind="page_v4",
                block_keys=(7,),
                require_all=True,
            )
        with pytest.raises(PTG2SharedBlockError, match="poisoned"):
            await fetch_shared_blocks(
                session,
                schema_name="mrf",
                snapshot_key=12,
                object_kind="page_v4",
                block_keys=(),
            )

    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_fetch_shared_blocks_fails_on_corrupt_content_hash():
    row = _stored_row(object_kind="page_v4", block_key=7, fragment_no=0, raw_payload=b"first")
    row["block_hash"] = b"x" * 32

    with pytest.raises(PTG2SharedBlockError, match="identity"):
        await fetch_shared_blocks(
            _Session([row]),
            schema_name="mrf",
            snapshot_key=12,
            object_kind="page_v4",
            block_keys=[7],
        )


@pytest.mark.asyncio
async def test_fetch_shared_blocks_filters_exact_fragments():
    stored_row = _stored_row(
        object_kind="by_code_price_dictionary",
        block_key=0,
        fragment_no=7080,
        raw_payload=b"tail",
    )
    session = _Session([stored_row])

    result = await fetch_shared_blocks(
        session,
        schema_name="mrf",
        snapshot_key=12,
        object_kind="by_code_price_dictionary",
        block_keys=(0,),
        fragment_nos=(7080,),
        require_all=True,
    )

    assert result[0][0].fragment_no == 7080
    sql, params = session.calls[0]
    assert "mapping.fragment_no = ANY" in sql
    assert params["fragment_nos"] == (7080,)


@pytest.mark.asyncio
async def test_graph_members_are_resolved_in_one_query_and_integer_space():
    members = (3, 9, 17)
    encoded = b"xx" + b"".join(member.to_bytes(4, "little") for member in members)
    stored_row = _stored_row(
        object_kind="graph_npi_groups_v1",
        block_key=4,
        fragment_no=0,
        raw_payload=encoded,
        codec="zlib",
        extra={
            "owner_key": 1234567890,
            "first_chunk": 4,
            "member_offset": 2,
            "member_count": len(members),
        },
    )
    session = _Session([stored_row])

    member_map = await fetch_shared_graph_members(
        session,
        schema_name="mrf",
        snapshot_key=12,
        direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
        owner_keys=[1234567890, 9999999999],
    )

    assert member_map == {1234567890: members, 9999999999: ()}
    assert len(session.calls) == 1
    sql, params = session.calls[0]
    assert "ptg2_v3_graph_owner" in sql
    assert "generate_series" in sql
    assert params["member_width"] == 4


@pytest.mark.asyncio
async def test_graph_read_once_scope_loads_one_chunk_shared_by_multiple_owners():
    encoded = (3).to_bytes(4, "little") + (9).to_bytes(4, "little")
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="graph_npi_groups_v1",
        raw_payload=encoded,
        coordinates=((4, 0),),
        codec="zlib",
        entry_count=2,
    )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
        owner_rows=(
            {
                "owner_key": 1111111111,
                "first_chunk": 4,
                "member_offset": 0,
                "member_count": 1,
                "selected_member_count": 1,
            },
            {
                "owner_key": 2222222222,
                "first_chunk": 4,
                "member_offset": 4,
                "member_count": 1,
                "selected_member_count": 1,
            },
        ),
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024) as scope:
        member_map = await fetch_shared_graph_members(
            session,
            schema_name="mrf",
            snapshot_key=12,
            direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
            owner_keys=(1111111111, 2222222222),
        )
        scope.assert_read_once()
        ledger = scope.ledger

    assert member_map == {1111111111: (3,), 2222222222: (9,)}
    assert ledger["unique_physical_blocks"] == 1
    assert ledger["physical_block_reads"] == 1
    assert ledger["physical_block_decodes"] == 1
    assert len(session.calls) == 3


@pytest.mark.asyncio
async def test_graph_read_once_scope_rejects_overlapping_owner_ranges():
    session = _ReadOnceSession(
        mapping_rows=(),
        physical_rows=(),
        owner_rows=(
            {
                "owner_key": 1111111111,
                "first_chunk": 4,
                "member_offset": 0,
                "member_count": 2,
                "selected_member_count": 2,
            },
            {
                "owner_key": 2222222222,
                "first_chunk": 4,
                "member_offset": 4,
                "member_count": 2,
                "selected_member_count": 2,
            },
        ),
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=1024):
        with pytest.raises(PTG2SharedBlockError, match="ranges overlap"):
            await fetch_shared_graph_members(
                session,
                schema_name="mrf",
                snapshot_key=12,
                direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
                owner_keys=(1111111111, 2222222222),
            )


@pytest.mark.asyncio
async def test_graph_read_once_scope_rejects_a_short_nonfinal_chunk():
    raw_chunks_by_key = {
        0: b"a\x00\x00\x00" * ((PTG2_V3_GRAPH_CHUNK_BYTES - 4) // 4),
        1: b"b\x00\x00\x00",
    }
    mapping_rows = []
    physical_rows = []
    for block_key, raw_chunk in raw_chunks_by_key.items():
        stored_row = _stored_row(
            object_kind="graph_npi_groups_v1",
            block_key=block_key,
            fragment_no=0,
            raw_payload=raw_chunk,
        )
        entry_count = len(raw_chunk) // 4
        mapping_rows.append(
            {
                "object_kind": "graph_npi_groups_v1",
                "block_key": block_key,
                "fragment_no": 0,
                "mapping_entry_count": entry_count,
                "block_hash": stored_row["block_hash"],
            }
        )
        physical_rows.append(
            {
                "block_hash": stored_row["block_hash"],
                "object_kind": "graph_npi_groups_v1",
                "format_version": stored_row["format_version"],
                "codec": stored_row["codec"],
                "block_entry_count": entry_count,
                "raw_byte_count": len(raw_chunk),
                "payload": stored_row["payload"],
            }
        )
    session = _ReadOnceSession(
        mapping_rows=mapping_rows,
        physical_rows=physical_rows,
        owner_rows=(
            {
                "owner_key": 1111111111,
                "first_chunk": 0,
                "member_offset": PTG2_V3_GRAPH_CHUNK_BYTES - 8,
                "member_count": 3,
                "selected_member_count": 3,
            },
        ),
    )

    with shared_block_read_once_scope(max_retained_raw_bytes=128 * 1024):
        with pytest.raises(PTG2SharedBlockError, match="member framing"):
            await fetch_shared_graph_members(
                session,
                schema_name="mrf",
                snapshot_key=12,
                direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
                owner_keys=(1111111111,),
            )


@pytest.mark.asyncio
async def test_reservation_reuses_sealed_semantic_layout():
    session = _ScriptedSession(
        [
            _Result(),
            _Result(
                rows=[
                    {
                        "snapshot_key": 17,
                        "state": "sealed",
                        "generation": "shared_blocks_v3",
                        "build_token": "first-run",
                        "layout_manifest": {"rate_count": 11},
                    }
                ]
            ),
            _Result(),
        ]
    )

    reservation = await reserve_shared_layout(
        session,
        schema_name="mrf",
        semantic_fingerprint=b"s" * 32,
        build_token="second-run",
    )

    assert reservation.snapshot_key == 17
    assert reservation.reused is True
    assert reservation.layout_manifest == {"rate_count": 11}
    assert len(session.calls) == 3
    assert "pg_advisory_xact_lock" in session.calls[0][0]
    assert "ptg2_v3_layout_fingerprint" in session.calls[1][0]
    assert "lease_until" in session.calls[2][0]


@pytest.mark.asyncio
async def test_reservation_resumes_only_the_same_build_token():
    """Ensure only the owning build token can resume a building layout."""

    matching = _ScriptedSession(
        [
            _Result(),
            _Result(
                rows=[
                    {
                        "snapshot_key": 23,
                        "state": "building",
                        "generation": "shared_blocks_v3",
                        "build_token": "run-23",
                    }
                ]
            ),
            _Result(scalar_value=23),
            _Result(),
            _Result(),
            *[_Result() for _ in PTG2_V3_DENSE_LAYOUT_TABLES],
            _Result(),
        ]
    )
    reservation = await reserve_shared_layout(
        matching,
        schema_name="mrf",
        semantic_fingerprint=b"t" * 32,
        build_token="run-23",
    )
    assert reservation.snapshot_key == 23
    assert reservation.reused is False
    assert not any("ptg2_v3_gc_candidate" in sql for sql, _params in matching.calls)
    assert not any(
        "DELETE FROM \"mrf\".ptg2_v3_snapshot_block" in sql
        for sql, _params in matching.calls
    )
    assert not any(
        f'\"mrf\".\"{table_name}\"' in sql
        for sql, _params in matching.calls
        for table_name in PTG2_V3_DENSE_LAYOUT_TABLES
    )

    conflicting = _ScriptedSession(
        [
            _Result(),
            _Result(
                rows=[
                    {
                        "snapshot_key": 23,
                        "state": "building",
                        "generation": "shared_blocks_v3",
                        "build_token": "run-23",
                    }
                ]
            ),
        ]
    )
    with pytest.raises(RuntimeError, match="already building"):
        await reserve_shared_layout(
            conflicting,
            schema_name="mrf",
            semantic_fingerprint=b"t" * 32,
            build_token="other-run",
        )


@pytest.mark.asyncio
async def test_snapshot_binding_retry_is_idempotent_only_for_same_layout():
    session = _ScriptedSession([_Result(scalar_value=None), _Result(scalar_value=31)])

    await bind_snapshot_to_shared_layout(
        session,
        schema_name="mrf",
        snapshot_id="snapshot-31",
        snapshot_key=31,
    )

    assert len(session.calls) == 2
    assert "ON CONFLICT (snapshot_id) DO NOTHING" in session.calls[0][0]


@pytest.mark.asyncio
async def test_build_heartbeat_is_owned_and_extends_the_gc_lease():
    session = _ScriptedSession([_Result(scalar_value=41)])

    await touch_shared_layout_build(
        session,
        schema_name="mrf",
        snapshot_key=41,
        build_token="run-41",
    )

    sql, params = session.calls[0]
    assert "heartbeat_at" in sql
    assert "lease_until" in sql
    assert params["snapshot_key"] == 41
    assert params["build_token"] == "run-41"
    assert params["lease_until"] > params["heartbeat_at"]


@pytest.mark.asyncio
async def test_mapping_summary_real_postgres_uses_same_uncommitted_transaction():
    """Read uncommitted mappings through the caller's PostgreSQL transaction."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1 for the isolated PostgreSQL test"
        )

    schema_name = f"ptg2_mapping_digest_{uuid.uuid4().hex[:16]}"
    quoted_schema = f'"{schema_name}"'
    references = [
        SharedBlockReference("🧪_kind", 9, 1, 5, b"e" * 32, 17),
        SharedBlockReference("a_kind", 2, 0, 3, b"a" * 32, 7),
        SharedBlockReference("ž_kind", 4, 2, 7, b"d" * 32, 13),
        SharedBlockReference("a_kind", 1, 3, 2, b"a" * 32, 7),
    ]

    await db.disconnect()
    await db.connect()
    try:
        await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
        await db.execute_ddl(
            f"""
            CREATE TABLE {quoted_schema}.ptg2_v3_block (
                block_hash bytea PRIMARY KEY,
                raw_byte_count bigint NOT NULL CHECK (raw_byte_count >= 0)
            )
            """
        )
        await db.execute_ddl(
            f"""
            CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_block (
                snapshot_key bigint NOT NULL,
                object_kind varchar(64) NOT NULL,
                block_key bigint NOT NULL CHECK (block_key >= 0),
                fragment_no integer NOT NULL CHECK (fragment_no >= 0),
                entry_count bigint NOT NULL CHECK (entry_count >= 0),
                block_hash bytea NOT NULL
                    REFERENCES {quoted_schema}.ptg2_v3_block (block_hash),
                PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
            )
            """
        )
        async with db.transaction() as session:
            await session.execute(
                text(
                    f"""
                    INSERT INTO {quoted_schema}.ptg2_v3_block
                        (block_hash, raw_byte_count)
                    VALUES (:block_hash, :raw_byte_count)
                    """
                ),
                [
                    {
                        "block_hash": block_hash,
                        "raw_byte_count": raw_byte_count,
                    }
                    for block_hash, raw_byte_count in {
                        reference.block_hash: reference.raw_byte_count
                        for reference in references
                    }.items()
                ],
            )
            await session.execute(
                text(
                    f"""
                    INSERT INTO {quoted_schema}.ptg2_v3_snapshot_block
                        (snapshot_key, object_kind, block_key, fragment_no,
                         entry_count, block_hash)
                    VALUES
                        (:snapshot_key, :object_kind, :block_key, :fragment_no,
                         :entry_count, :block_hash)
                    """
                ),
                [
                    {
                        "snapshot_key": 41,
                        "object_kind": reference.object_kind,
                        "block_key": reference.block_key,
                        "fragment_no": reference.fragment_no,
                        "entry_count": reference.entry_count,
                        "block_hash": reference.block_hash,
                    }
                    for reference in references
                ],
            )

            summary = await summarize_shared_snapshot_mappings(
                session,
                schema_name=schema_name,
                snapshot_key=41,
            )

        assert summary.mapping_digest == shared_mapping_digest(references)
        assert summary.mapping_count == len(references)
        assert summary.unique_block_count == len(
            {reference.block_hash for reference in references}
        )
        assert summary.entry_count == sum(
            reference.entry_count for reference in references
        )
        assert summary.logical_byte_count == sum(
            reference.raw_byte_count for reference in references
        )
        assert summary.object_kinds == tuple(
            sorted({reference.object_kind for reference in references})
        )
    finally:
        await db.execute_ddl(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await db.disconnect()
