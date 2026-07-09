# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import struct
import zlib

import pytest

import api.ptg2_db_sidecars as db_sidecars
from api.ptg2_db_sidecars import (
    PTG2DbArtifactReader,
    lookup_global_sidecar_members_many_from_db,
    lookup_serving_by_code_sidecar_from_db,
    lookup_serving_by_provider_set_patterns_from_db,
)
from process.ptg_parts.ptg2_artifact_blobs import ptg2_db_artifact_uri
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    write_global_membership_sidecar,
    write_serving_by_code_sidecar,
    write_serving_by_provider_set_sidecar,
)


@pytest.fixture(autouse=True)
def clear_sidecar_caches():
    db_sidecars._CHUNK_CACHE.clear()
    db_sidecars._CHUNK_CACHE_BYTES = 0
    db_sidecars._BINARY_DICTIONARY_CACHE.clear()
    db_sidecars._BINARY_DICTIONARY_CACHE_STATE["byte_count"] = 0


class FakeResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = list(rows or [])
        self._scalar = scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)


class FakeChunkSession:
    def __init__(self, raw_payload: bytes, *, chunk_bytes: int, compression: str = "zlib"):
        self.chunk_bytes = chunk_bytes
        self.compression = compression
        self.chunks = [
            raw_payload[offset : offset + chunk_bytes]
            for offset in range(0, len(raw_payload), chunk_bytes)
        ]
        self.calls = []

    async def execute_chunk_query(self, _statement, params=None):
        query_parameters_dict = dict(params or {})
        self.calls.append(query_parameters_dict)
        if "chunk_nos" in query_parameters_dict:
            chunk_rows = []
            for chunk_no in query_parameters_dict["chunk_nos"]:
                chunk = self.chunks[int(chunk_no)]
                stored_payload = zlib.compress(chunk) if self.compression == "zlib" else chunk
                chunk_rows.append(
                    {
                        "chunk_no": int(chunk_no),
                        "compression": self.compression,
                        "payload": stored_payload,
                        "raw_byte_count": len(chunk),
                    }
                )
            return FakeResult(rows=chunk_rows)
        if "chunk_no" not in query_parameters_dict:
            return FakeResult(scalar=sum(len(chunk) for chunk in self.chunks))
        chunk = self.chunks[int(query_parameters_dict["chunk_no"])]
        stored_payload = zlib.compress(chunk) if self.compression == "zlib" else chunk
        return FakeResult(
            rows=[
                {
                    "compression": self.compression,
                    "payload": stored_payload,
                    "raw_byte_count": len(chunk),
                }
            ]
        )

    execute = execute_chunk_query


def _db_entry(sidecar: dict, raw_payload: bytes, *, artifact_id: str, chunk_bytes: int) -> dict:
    entry = dict(sidecar)
    entry.update(
        {
            "path": "/missing/local/cache/file.bin",
            "storage_uri": ptg2_db_artifact_uri(artifact_id),
            "sha256": hashlib.sha256(raw_payload).hexdigest(),
            "byte_count": len(raw_payload),
            "chunk_bytes": chunk_bytes,
        }
    )
    return entry


@pytest.mark.asyncio
async def test_db_artifact_reader_batches_multi_chunk_range_reads():
    raw_payload = b"abcdefghijklmnopqrstuvwxyz"
    entry = _db_entry({}, raw_payload, artifact_id="batched-artifact", chunk_bytes=5)
    session = FakeChunkSession(raw_payload, chunk_bytes=5, compression="zlib")
    reader = PTG2DbArtifactReader(session, entry, schema_name="mrf")

    payload = await reader.read_at(3, 17)

    assert payload == raw_payload[3:20]
    batch_calls = [call for call in session.calls if call.get("chunk_nos")]
    assert len(batch_calls) == 1
    assert batch_calls[0]["chunk_nos"] == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_db_membership_sidecar_reads_compressed_chunks_without_local_file(tmp_path):
    owner = bytes.fromhex("00000000000000000000000000000011")
    member_a = bytes.fromhex("00000000000000000000000000000021")
    member_b = bytes.fromhex("00000000000000000000000000000022")
    manifest = write_global_membership_sidecar(tmp_path, "provider_forward", {owner: [member_b, member_a]})
    sidecar = manifest["sidecars"][0]
    raw_payload = (tmp_path / sidecar["path"]).read_bytes()
    chunk_bytes = 13
    entry = _db_entry(sidecar, raw_payload, artifact_id="membership-artifact", chunk_bytes=chunk_bytes)
    session = FakeChunkSession(raw_payload, chunk_bytes=chunk_bytes, compression="zlib")

    members = await lookup_global_sidecar_members_many_from_db(
        session,
        entry,
        [owner.hex(), "00000000000000000000000000009999"],
        schema_name="mrf",
    )

    assert members[owner] == (member_a, member_b)
    assert members[bytes.fromhex("00000000000000000000000000009999")] == ()
    assert session.calls


@pytest.mark.asyncio
async def test_db_membership_sidecar_prefetches_small_index_for_many_owners(tmp_path, monkeypatch):
    owner_a = bytes.fromhex("00000000000000000000000000000011")
    owner_b = bytes.fromhex("00000000000000000000000000000012")
    missing_owner = bytes.fromhex("00000000000000000000000000009999")
    member_a = bytes.fromhex("00000000000000000000000000000021")
    member_b = bytes.fromhex("00000000000000000000000000000022")
    manifest = write_global_membership_sidecar(
        tmp_path,
        "provider_forward",
        {
            owner_a: [member_a],
            owner_b: [member_b],
        },
    )
    sidecar = manifest["sidecars"][0]
    raw_payload = (tmp_path / sidecar["path"]).read_bytes()
    entry = _db_entry(sidecar, raw_payload, artifact_id="membership-index-prefetch-artifact", chunk_bytes=31)
    session = FakeChunkSession(raw_payload, chunk_bytes=31, compression="zlib")

    async def fail_sparse_lookup(*_args, **_kwargs):
        raise AssertionError("small DB membership indexes should be prefetched once")

    monkeypatch.setattr(db_sidecars, "_INDEX_READ_MAX_BYTES", 1024 * 1024)
    monkeypatch.setattr(db_sidecars, "_index_lookup_from_db", fail_sparse_lookup)

    members = await lookup_global_sidecar_members_many_from_db(
        session,
        entry,
        [owner_a.hex(), owner_b.hex(), missing_owner.hex()],
        schema_name="mrf",
    )

    assert members[owner_a] == (member_a,)
    assert members[owner_b] == (member_b,)
    assert members[missing_owner] == ()
    assert any(call.get("chunk_nos") for call in session.calls)


@pytest.mark.asyncio
async def test_db_membership_sidecar_batches_sparse_index_lookup_for_many_owners(tmp_path, monkeypatch):
    owner_a = bytes.fromhex("00000000000000000000000000000011")
    owner_b = bytes.fromhex("00000000000000000000000000000012")
    owner_c = bytes.fromhex("00000000000000000000000000000013")
    missing_owner = bytes.fromhex("00000000000000000000000000009999")
    member_a = bytes.fromhex("00000000000000000000000000000021")
    member_b = bytes.fromhex("00000000000000000000000000000022")
    member_c = bytes.fromhex("00000000000000000000000000000023")
    manifest = write_global_membership_sidecar(
        tmp_path,
        "provider_forward",
        {
            owner_a: [member_a],
            owner_b: [member_b],
            owner_c: [member_c],
        },
    )
    sidecar = manifest["sidecars"][0]
    raw_payload = (tmp_path / sidecar["path"]).read_bytes()
    entry = _db_entry(sidecar, raw_payload, artifact_id="membership-sparse-batch-artifact", chunk_bytes=31)
    session = FakeChunkSession(raw_payload, chunk_bytes=31, compression="zlib")

    async def fail_single_owner_sparse_lookup(*_args, **_kwargs):
        raise AssertionError("many-owner sparse index lookup should be batched")

    monkeypatch.setattr(db_sidecars, "_INDEX_READ_MAX_BYTES", 8)
    monkeypatch.setattr(db_sidecars, "_index_lookup_from_db", fail_single_owner_sparse_lookup)

    members = await lookup_global_sidecar_members_many_from_db(
        session,
        entry,
        [owner_a.hex(), owner_b.hex(), owner_c.hex(), missing_owner.hex()],
        schema_name="mrf",
    )

    assert members[owner_a] == (member_a,)
    assert members[owner_b] == (member_b,)
    assert members[owner_c] == (member_c,)
    assert members[missing_owner] == ()
    assert any(call.get("chunk_nos") for call in session.calls)


@pytest.mark.asyncio
async def test_db_dense_membership_sidecar_uses_sparse_range_reads(monkeypatch):
    owner_a = bytes.fromhex("00000000000000000000000000000011")
    owner_b = bytes.fromhex("00000000000000000000000000000012")
    member_a = bytes.fromhex("00000000000000000000000000000021")
    member_b = bytes.fromhex("00000000000000000000000000000022")
    member_c = bytes.fromhex("00000000000000000000000000000023")
    payload = bytearray()
    payload.extend(struct.pack("<8sIQQ", PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC, 1, 2, 3))
    payload.extend(struct.pack("<16sQI", owner_a, 0, 2))
    payload.extend(struct.pack("<16sQI", owner_b, 2, 1))
    payload.extend(member_a)
    payload.extend(member_b)
    payload.extend(member_c)
    payload.extend(struct.pack("<III", 0, 2, 1))
    raw_payload = bytes(payload)
    entry = _db_entry(
        {
            "record_format": PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
            "owner_count": 2,
            "member_count": 3,
            "member_global_count": 3,
        },
        raw_payload,
        artifact_id="dense-membership-artifact",
        chunk_bytes=11,
    )
    session = FakeChunkSession(raw_payload, chunk_bytes=11, compression="zlib")
    monkeypatch.setattr(db_sidecars, "_INDEX_READ_MAX_BYTES", 8)

    members = await lookup_global_sidecar_members_many_from_db(
        session,
        entry,
        [owner_a.hex()],
        schema_name="mrf",
    )

    assert members[owner_a] == (member_a, member_c)
    assert any(call.get("chunk_no") is not None for call in session.calls)


@pytest.mark.asyncio
async def test_db_serving_by_code_sidecar_reads_only_matching_rows(tmp_path):
    price_a = bytes.fromhex("00000000000000000000000000000031")
    price_b = bytes.fromhex("00000000000000000000000000000032")
    sidecar = write_serving_by_code_sidecar(
        tmp_path / "serving_by_code.ptg2sbc",
        [
            (7, 3, 10, price_a),
            (7, 5, 20, price_b),
            (9, 1, 30, price_a),
        ],
    )
    raw_payload = (tmp_path / "serving_by_code.ptg2sbc").read_bytes()
    chunk_bytes = 17
    entry = _db_entry(sidecar, raw_payload, artifact_id="serving-by-code-artifact", chunk_bytes=chunk_bytes)
    session = FakeChunkSession(raw_payload, chunk_bytes=chunk_bytes, compression="zlib")

    rows = await lookup_serving_by_code_sidecar_from_db(
        session,
        entry,
        7,
        schema_name="mrf",
        provider_set_keys=[5],
    )

    assert [(row.code_key, row.provider_set_key, row.provider_count, row.price_set_global_id_128) for row in rows] == [
        (7, 5, 20, price_b.hex())
    ]


@pytest.mark.asyncio
async def test_db_serving_by_provider_set_patterns_support_reverse_lookup(tmp_path):
    price_a = bytes.fromhex("00000000000000000000000000000041")
    price_b = bytes.fromhex("00000000000000000000000000000042")
    sidecar = write_serving_by_provider_set_sidecar(
        tmp_path / "serving_by_provider_set.ptg2sbp",
        [
            (5, 7, 10, price_a),
            (5, 9, 20, price_b),
            (6, 7, 30, price_a),
        ],
    )
    raw_payload = (tmp_path / "serving_by_provider_set.ptg2sbp").read_bytes()
    chunk_bytes = 19
    entry = _db_entry(sidecar, raw_payload, artifact_id="serving-by-provider-artifact", chunk_bytes=chunk_bytes)
    session = FakeChunkSession(raw_payload, chunk_bytes=chunk_bytes, compression="none")

    patterns = await lookup_serving_by_provider_set_patterns_from_db(
        session,
        entry,
        5,
        schema_name="mrf",
        code_keys=[9],
    )

    assert len(patterns) == 1
    assert patterns[0].code_keys == (9,)
    assert patterns[0].entries == ((20, price_b.hex()),)
