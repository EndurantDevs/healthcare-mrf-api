# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import zlib

import pytest

from api.ptg2_db_sidecars import (
    lookup_global_sidecar_members_many_from_db,
    lookup_serving_by_code_sidecar_from_db,
    lookup_serving_by_provider_set_patterns_from_db,
)
from process.ptg_parts.ptg2_artifact_blobs import ptg2_db_artifact_uri
from process.ptg_parts.ptg2_manifest_artifacts import (
    write_global_membership_sidecar,
    write_serving_by_code_sidecar,
    write_serving_by_provider_set_sidecar,
)


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

    async def execute(self, _statement, params=None):
        params = dict(params or {})
        self.calls.append(params)
        if "chunk_no" not in params:
            return FakeResult(scalar=sum(len(chunk) for chunk in self.chunks))
        chunk = self.chunks[int(params["chunk_no"])]
        payload = zlib.compress(chunk) if self.compression == "zlib" else chunk
        return FakeResult(
            rows=[
                {
                    "compression": self.compression,
                    "payload": payload,
                    "raw_byte_count": len(chunk),
                }
            ]
        )


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
