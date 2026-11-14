# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Saved copies of opted-in Bulk Data outputs, written compressed at download speed and parsed afterwards.

A payer's signed output links can expire long before a large export is parsed. A saved copy holds the validated
bytes (same length and strong ETag), so parsing and byte-offset resume no longer depend on the link.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
from compression import zstd
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator

SPOOL_SOURCES_ENV = "HLTHPRT_PROVIDER_DIRECTORY_BULK_SPOOL_SOURCES"
SPOOL_ROOT_ENV = "HLTHPRT_PROVIDER_DIRECTORY_BULK_SPOOL_ROOT"
READ_BYTES = 4 << 20
# Each frame is fsynced and recorded, so a broken download resumes from its last whole frame.
FRAME_BYTES = 256 << 20
_RANGE_RE = re.compile(r"bytes=(\d+)-")


class SpoolLengthError(ValueError):
    """The saved bytes do not add up to the validated length; the copy is discarded and downloaded again."""


@dataclass(frozen=True)
class SpooledOutput:
    path: Path
    etag: str
    content_length: int


def is_enabled(source_record: dict[str, Any]) -> bool:
    """Copies are made only for the sources the setting names, and only when a copy root is configured."""
    source_ids = {name.strip() for name in os.getenv(SPOOL_SOURCES_ENV, "").split(",") if name.strip()}
    return bool(os.getenv(SPOOL_ROOT_ENV)) and source_record.get("source_id") in source_ids


def spooled_output(checkpoint_id: str, output_index: int, validator: Any) -> SpooledOutput:
    """Name the copy by export, output and validated content, so no other file can stand in for it."""
    if not re.fullmatch(r"[0-9a-f]{16,128}", checkpoint_id):
        raise ValueError("bulk_export_spool_checkpoint_invalid")
    name = f"{int(output_index)}-{validator.etag_hash[:16]}.ndjson.zst"
    path = Path(os.environ[SPOOL_ROOT_ENV], checkpoint_id, name)
    return SpooledOutput(path, validator.etag, validator.content_length_bytes)


def served_offset(spooled: SpooledOutput, headers: dict[str, Any]) -> int | None:
    """The offset a whole-file or open-ended ranged GET of this validated output asks for; None for any other."""
    headers_by_name = {str(name).lower(): str(value) for name, value in headers.items()}
    if headers_by_name.get("if-match") != spooled.etag:
        return None
    if "range" not in headers_by_name:
        return 0
    match = _RANGE_RE.fullmatch(headers_by_name["range"])
    return int(match.group(1)) if match else None


class SpoolWriter:
    """Append raw bytes as zstd frames; only fsynced, recorded frames count toward the resume offset."""

    def __init__(self, spooled: SpooledOutput) -> None:
        self.spooled = spooled
        self.part_path = spooled.path.with_name(spooled.path.name + ".part")
        self.mark_path = spooled.path.with_name(spooled.path.name + ".mark")
        spooled.path.parent.mkdir(parents=True, exist_ok=True)
        mark_by_name = {"raw": 0, "stored": 0}
        if self.part_path.exists() and self.mark_path.exists():
            mark_by_name = json.loads(self.mark_path.read_text())
        self.handle = open(self.part_path, "r+b" if mark_by_name["stored"] else "wb")
        self.handle.truncate(mark_by_name["stored"])
        self.handle.seek(mark_by_name["stored"])
        self.raw_bytes = self.durable_bytes = mark_by_name["raw"]
        self.compressor = zstd.ZstdCompressor(level=3)
        # Writes run in worker threads; a cancelled caller must not close or retry under a write still in flight.
        self.lock = threading.Lock()

    def write(self, chunk: bytes) -> None:
        """Compress one chunk, closing a durable frame once a frame's worth of raw bytes has arrived."""
        with self.lock:
            self.handle.write(self.compressor.compress(chunk))
            self.raw_bytes += len(chunk)
            if self.raw_bytes - self.durable_bytes >= FRAME_BYTES:
                self._close_frame()

    def finish(self) -> None:
        """Publish the copy under its final name once it holds exactly the validated length."""
        with self.lock:
            if self.raw_bytes != self.spooled.content_length:
                raise SpoolLengthError("bulk_export_spool_length_mismatch")
            self._close_frame()
            self.handle.close()
            os.replace(self.part_path, self.spooled.path)
            self.mark_path.unlink(missing_ok=True)

    def close(self) -> None:
        """Release the file once any write in flight has ended; the part and its mark stay for the next attempt."""
        with self.lock:
            self.handle.close()

    def discard(self) -> None:
        """Drop a copy whose bytes cannot be trusted, so the next attempt starts over."""
        with self.lock:
            self.handle.close()
            self.part_path.unlink(missing_ok=True)
            self.mark_path.unlink(missing_ok=True)

    def _close_frame(self) -> None:
        self.handle.write(self.compressor.flush(zstd.ZstdCompressor.FLUSH_FRAME))
        self.handle.flush()
        os.fsync(self.handle.fileno())
        self.durable_bytes = self.raw_bytes
        pending_mark_path = self.mark_path.with_name(self.mark_path.name + ".new")
        pending_mark_path.write_text(json.dumps({"raw": self.raw_bytes, "stored": self.handle.tell()}))
        os.replace(pending_mark_path, self.mark_path)


class SpooledResponse:
    """The response a GET of the validated output would give, read from its saved copy."""

    def __init__(self, spooled: SpooledOutput, offset: int) -> None:
        self.status = 206 if offset else 200
        self.headers = {
            "content-encoding": "identity",
            "etag": spooled.etag,
            "content-length": str(spooled.content_length - offset),
        }
        if offset:
            self.headers["content-range"] = f"bytes {offset}-{spooled.content_length - 1}/{spooled.content_length}"
        self.content = self
        self.spooled = spooled
        self.offset = offset

    async def iter_chunked(self, size: int) -> AsyncIterator[bytes]:
        """Yield the raw bytes from the requested offset, decompressing off the event loop."""
        handle = await asyncio.to_thread(zstd.open, self.spooled.path, "rb")
        try:
            await asyncio.to_thread(handle.seek, self.offset)
            while chunk := await asyncio.to_thread(handle.read, size):
                yield chunk
        finally:
            handle.close()
