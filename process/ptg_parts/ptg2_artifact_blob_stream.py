# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded streaming helpers for PostgreSQL-owned PTG2 artifacts."""

from __future__ import annotations

import hashlib
import zlib
from pathlib import Path
from typing import Any

from sqlalchemy import text


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return dict(row)
    return dict(row or {})


async def stream_artifact_chunks(
    session,
    qualified_chunks: str,
    artifact_id: str,
    tmp_path: Path,
):
    """Stream ordered artifact chunks into ``tmp_path`` and hash raw bytes."""

    digest = hashlib.sha256()
    total_raw_bytes = 0
    chunk_count = 0
    with tmp_path.open("wb") as out:
        chunk_stream = await session.stream(
            text(
                f"SELECT chunk_no, compression, payload, raw_byte_count "
                f"FROM {qualified_chunks} WHERE artifact_id = :artifact_id "
                "ORDER BY chunk_no"
            ),
            {"artifact_id": artifact_id},
        )
        async for chunk_row in chunk_stream:
            chunk_record_map = _row_mapping(chunk_row)
            compression = str(chunk_record_map.get("compression") or "none")
            stored_chunk_bytes = bytes(chunk_record_map.get("payload") or b"")
            raw_chunk = (
                zlib.decompress(stored_chunk_bytes)
                if compression == "zlib"
                else stored_chunk_bytes
            )
            expected_raw = chunk_record_map.get("raw_byte_count")
            if expected_raw is not None and len(raw_chunk) != int(expected_raw):
                raise ValueError(
                    "artifact chunk raw byte_count mismatch for "
                    f"{artifact_id}:{chunk_count}"
                )
            out.write(raw_chunk)
            digest.update(raw_chunk)
            total_raw_bytes += len(raw_chunk)
            chunk_count += 1
    return digest, total_raw_bytes, chunk_count
