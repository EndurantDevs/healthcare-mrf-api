# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path
from typing import Any, AsyncIterator, Iterable

import pytest

from process.ptg_parts import ptg2_manifest_artifacts as artifacts


_PRICE_A = "11" * 16
_PRICE_B = "22" * 16
_PRICE_C = "33" * 16


async def _async_rows(rows: Iterable[Iterable[Any]]) -> AsyncIterator[Iterable[Any]]:
    for row in rows:
        yield row


@pytest.fixture(autouse=True)
def _close_sidecar_mmaps():
    yield
    with artifacts._SIDE_CAR_MMAP_LOCK:
        cached_mmaps = tuple(artifacts._SIDE_CAR_MMAP_CACHE.values())
        artifacts._SIDE_CAR_MMAP_CACHE.clear()
    for file_handle, mapped, _size, _mtime in cached_mmaps:
        mapped.close()
        file_handle.close()


@pytest.mark.asyncio
async def test_async_serving_by_code_roundtrip_filters_and_reuses(tmp_path: Path):
    path = tmp_path / "by-code.ptg2sbc"
    sidecar_rows = (
        (1, 2, 5, bytes.fromhex(_PRICE_A)),
        (1, 10, 6, _PRICE_B),
        (1, 20, 7, memoryview(bytes.fromhex(_PRICE_A))),
        (4, 3, 8, "33333333-3333-3333-3333-333333333333"),
    )
    entry = await artifacts.write_serving_by_code_sidecar_async(
        path, _async_rows(sidecar_rows), name="forward"
    )

    assert entry["name"] == "forward"
    assert entry["row_count"] == 4
    assert entry["code_count"] == 2
    assert entry["price_set_count"] == 3
    assert entry["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    decoded = artifacts.lookup_serving_by_code_sidecar(path, 1, metadata=entry)
    assert [
        (sidecar_row.provider_set_key, sidecar_row.provider_count)
        for sidecar_row in decoded
    ] == [
        (2, 5),
        (10, 6),
        (20, 7),
    ]
    assert [sidecar_row.price_set_global_id_128 for sidecar_row in decoded] == [
        _PRICE_A,
        _PRICE_B,
        _PRICE_A,
    ]
    assert artifacts.lookup_serving_by_code_sidecar(
        path, 1, provider_set_keys=[2, 20], metadata=entry
    ) == (decoded[0], decoded[2])
    assert artifacts.lookup_serving_by_code_sidecar(
        path, 1, provider_set_keys=[2], metadata=entry
    ) == (decoded[0],)
    assert not artifacts.lookup_serving_by_code_sidecar(
        path, 1, provider_set_keys=[], metadata=entry
    )
    assert not artifacts.lookup_serving_by_code_sidecar(path, 0, metadata=entry)
    assert not artifacts.lookup_serving_by_code_sidecar(path, 3, metadata=entry)
    assert not artifacts.lookup_serving_by_code_sidecar(path, 9, metadata=entry)

    reused = await artifacts.write_serving_by_code_sidecar_async(
        path, _async_rows(()), name="forward", expected_row_count=4
    )
    assert reused["sha256"] == entry["sha256"]
    assert reused["row_count"] == 4


@pytest.mark.asyncio
async def test_async_serving_by_provider_set_roundtrip_patterns_and_reuses(tmp_path: Path):
    path = tmp_path / "by-provider.ptg2sbp"
    sidecar_rows = (
        (2, 1, 5, _PRICE_A),
        (2, 1, 7, _PRICE_B),
        (2, 3, 5, _PRICE_A),
        (2, 3, 7, _PRICE_B),
        (2, 4, 8, _PRICE_C),
        (9, 2, 4, _PRICE_A),
    )
    entry = await artifacts.write_serving_by_provider_set_sidecar_async(
        path, _async_rows(sidecar_rows), name="reverse"
    )

    assert entry["row_count"] == 6
    assert entry["provider_set_count"] == 2
    assert entry["code_count"] == 4
    assert entry["pattern_count"] == 3
    decoded = artifacts.lookup_serving_by_provider_set_sidecar(path, 2, metadata=entry)
    assert [
        (sidecar_row.code_key, sidecar_row.provider_count)
        for sidecar_row in decoded
    ] == [
        (1, 5),
        (1, 7),
        (3, 5),
        (3, 7),
        (4, 8),
    ]
    assert artifacts.lookup_serving_by_provider_set_sidecar(
        path, 2, code_keys=[3], metadata=entry
    ) == decoded[2:4]
    assert not artifacts.lookup_serving_by_provider_set_sidecar(path, 1, metadata=entry)
    assert not artifacts.lookup_serving_by_provider_set_sidecar(path, 5, metadata=entry)
    assert not artifacts.lookup_serving_by_provider_set_sidecar(path, 20, metadata=entry)
    patterns = artifacts.lookup_serving_by_provider_set_patterns(path, 2, metadata=entry)
    assert [(pattern.code_keys, pattern.entries) for pattern in patterns] == [
        ((1, 3), ((5, _PRICE_A), (7, _PRICE_B))),
        ((4,), ((8, _PRICE_C),)),
    ]
    assert artifacts.lookup_serving_by_provider_set_patterns(
        path, 2, code_keys=[3], metadata=entry
    ) == (artifacts.PTG2ServingProviderSetPattern((3,), patterns[0].entries),)
    assert not artifacts.lookup_serving_by_provider_set_patterns(
        path, 2, code_keys=[], metadata=entry
    )
    assert not artifacts.lookup_serving_by_provider_set_patterns(path, 7, metadata=entry)

    reused = await artifacts.write_serving_by_provider_set_sidecar_async(
        path, _async_rows(()), name="reverse", expected_row_count=6
    )
    assert reused["sha256"] == entry["sha256"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("writer", "rows", "message"),
    (
        (artifacts.write_serving_by_code_sidecar_async, ((1, 2, 3),), "four columns"),
        (
            artifacts.write_serving_by_code_sidecar_async,
            ((1, 4, 1, _PRICE_A), (1, 3, 1, _PRICE_A)),
            "ordered by provider_set_key",
        ),
        (artifacts.write_serving_by_code_sidecar_async, ((1, 2, 3, "xyz"),), "hex or uuid"),
        (
            artifacts.write_serving_by_provider_set_sidecar_async,
            ((2, 1, 3),),
            "four columns",
        ),
        (
            artifacts.write_serving_by_provider_set_sidecar_async,
            ((2, 1, 3, b"short"),),
            "16 bytes",
        ),
    ),
)
async def test_async_serving_writers_reject_malformed_rows(
    tmp_path: Path, writer, rows, message: str
):
    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        await writer(tmp_path / "invalid.sidecar", _async_rows(rows))


def _raw_sidecar(magic: bytes, header: Any, body: bytes = b"") -> bytes:
    encoded = json.dumps(header, separators=(",", ":")).encode()
    return magic + struct.pack("<I", len(encoded)) + encoded + body


@pytest.mark.parametrize("metadata", ({}, {"byte_count": -1}, {"byte_count": "1"}))
def test_serving_lookup_requires_bounded_metadata(tmp_path: Path, metadata: dict[str, Any]):
    path = tmp_path / "sidecar"
    path.write_bytes(b"anything")
    with pytest.raises(artifacts.PTG2ManifestArtifactError, match="byte count"):
        artifacts.lookup_serving_by_code_sidecar(path, 1, metadata=metadata)


@pytest.mark.parametrize(
    "payload",
    (
        b"short",
        artifacts.PTG2_SERVING_BY_CODE_MAGIC + struct.pack("<I", 50) + b"{}",
        _raw_sidecar(artifacts.PTG2_SERVING_BY_CODE_MAGIC, []),
        _raw_sidecar(artifacts.PTG2_SERVING_BY_CODE_MAGIC, {"format": "wrong"}),
        _raw_sidecar(
            artifacts.PTG2_SERVING_BY_CODE_MAGIC,
            {"format": artifacts.PTG2_SERVING_BY_CODE_FORMAT, "price_set_count": 1, "code_count": 1},
        ),
    ),
)
def test_serving_lookup_rejects_malformed_headers_and_indexes(tmp_path: Path, payload: bytes):
    path = tmp_path / "malformed"
    path.write_bytes(payload)
    with pytest.raises((artifacts.PTG2ManifestArtifactError, json.JSONDecodeError, struct.error)):
        artifacts.lookup_serving_by_code_sidecar(path, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", (b"\x01", b"\x80", b"\x80" * 10))
async def test_serving_lookup_rejects_invalid_price_keys_and_uvarints(
    tmp_path: Path, replacement: bytes
):
    path = tmp_path / f"corrupt-{len(replacement)}"
    await artifacts.write_serving_by_code_sidecar_async(
        path, _async_rows(((1, 2, 3, _PRICE_A),))
    )
    payload = path.read_bytes()
    path.write_bytes(payload[:-1] + replacement)
    with pytest.raises(artifacts.PTG2ManifestArtifactError):
        artifacts.lookup_serving_by_code_sidecar(path, 1)
