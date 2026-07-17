from __future__ import annotations

import zlib

import pytest

from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    decode_shared_block_payload,
    fetch_shared_blocks,
    fetch_shared_graph_members,
    fetch_snapshot_source_set_metadata,
)
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_DENSE_LAYOUT_TABLES,
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
    touch_shared_layout_build,
)


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
async def test_seal_validates_mapping_without_rejoining_immutable_blocks():
    expected = SharedBlock(
        "page_v4",
        7,
        0,
        3,
        "none",
        9,
        b"123456789",
    ).reference()
    session = _ScriptedSession(
        [
            _Result(scalar_value=41),
            _Result(
                rows=[
                    {
                        "object_kind": expected.object_kind,
                        "block_key": expected.block_key,
                        "fragment_no": expected.fragment_no,
                        "entry_count": expected.entry_count,
                        "block_hash": expected.block_hash,
                    }
                ]
            ),
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
        expected_blocks=(expected,),
        support_digest=b"s" * 32,
        layout_manifest={"contract": "strict-v3"},
    )

    assert sealed.snapshot_key == 41
    mapping_sql = session.calls[1][0]
    assert "ptg2_v3_snapshot_block" in mapping_sql
    assert "ptg2_v3_block block" not in mapping_sql
    assert "JOIN" not in mapping_sql
    update_params = session.calls[-1][1]
    assert update_params["logical_byte_count"] == expected.raw_byte_count


@pytest.mark.asyncio
async def test_seal_still_rejects_mapping_hash_mismatch_without_block_join():
    expected = SharedBlock("page_v4", 7, 0, 1, "none", 1, b"a").reference()
    session = _ScriptedSession(
        [
            _Result(scalar_value=41),
            _Result(
                rows=[
                    {
                        "object_kind": expected.object_kind,
                        "block_key": expected.block_key,
                        "fragment_no": expected.fragment_no,
                        "entry_count": expected.entry_count,
                        "block_hash": b"x" * 32,
                    }
                ]
            ),
        ]
    )

    with pytest.raises(RuntimeError, match="mapping mismatch"):
        await seal_shared_layout(
            session,
            schema_name="mrf",
            snapshot_key=41,
            build_token="attempt-41",
            expected_blocks=(expected,),
            support_digest=b"s" * 32,
            layout_manifest={},
        )


def test_decode_shared_block_payload_is_strict():
    raw = b"payload" * 100
    compressed = zlib.compress(raw, 1)

    assert (
        decode_shared_block_payload(
            codec="zlib",
            payload=compressed,
            raw_byte_count=len(raw),
        )
        == raw
    )
    with pytest.raises(PTG2SharedBlockError, match="length mismatch"):
        decode_shared_block_payload(
            codec="none",
            payload=b"x",
            raw_byte_count=2,
        )
    with pytest.raises(PTG2SharedBlockError, match="unsupported"):
        decode_shared_block_payload(
            codec="gzip",
            payload=b"x",
            raw_byte_count=1,
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
