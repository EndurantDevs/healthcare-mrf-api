# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained raw-artifact storage helpers for PTG2 imports."""

from __future__ import annotations

import datetime
import fcntl
import hashlib
import json
import os
import re
import tempfile
import threading
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from process.ptg_parts.canonical import semantic_hash
from process.ptg_parts.domain import PTG2_ARTIFACT_RAW, PTG2HeadMetadata


_PROCESS_LOCKS_GUARD = threading.Lock()
_PROCESS_LOCKS: dict[str, threading.Lock] = {}


def _process_lock(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _PROCESS_LOCKS_GUARD:
        return _PROCESS_LOCKS.setdefault(key, threading.Lock())


class _PTG2FileLock:
    """Serialize store mutations across threads, processes, and worker pods."""

    def __init__(self, path: Path):
        self.path = path
        self._thread_lock = _process_lock(path)
        self._fd: int | None = None

    def acquire(self) -> None:
        """Acquire the process and filesystem artifact lock."""
        self._thread_lock.acquire()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with suppress(OSError):
                self.path.parent.chmod(0o777)
            self._fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o666)
            with suppress(OSError):
                os.chmod(self.path, 0o666)
            fcntl.flock(self._fd, fcntl.LOCK_EX)
        except BaseException:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None
            self._thread_lock.release()
            raise

    def release(self) -> None:
        """Release the filesystem and process artifact lock."""
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None
            self._thread_lock.release()

    def __enter__(self) -> "_PTG2FileLock":
        self.acquire()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


def resolve_ptg2_artifact_dir() -> Path:
    """Return the configured artifact directory, creating it when needed."""
    configured = os.getenv("HLTHPRT_PTG2_ARTIFACT_DIR")
    root = Path(configured) if configured else Path(tempfile.gettempdir()) / "healthporta-ptg2-artifacts"
    root.mkdir(parents=True, exist_ok=True)
    return root


def content_addressed_path(root: str | Path, digest: str, kind: str = PTG2_ARTIFACT_RAW, suffix: str = "") -> Path:
    """Build the sharded path for an artifact digest and optional suffix."""
    digest_text = str(digest)
    clean_suffix = suffix if suffix.startswith(".") or not suffix else f".{suffix}"
    return Path(root) / kind / digest_text[:2] / digest_text[2:4] / f"{digest_text}{clean_suffix}"


def ptg2_temp_parent() -> Path:
    """Return the PTG2 artifact store's writable temporary directory."""
    return PTG2ArtifactStore().tmp_dir


def _safe_url_suffix(url: str) -> str:
    path = urlsplit(url).path
    suffixes = "".join(Path(path).suffixes[-2:])
    return suffixes[:24]


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    """Return a file's SHA-256 digest and byte count."""
    digest = hashlib.sha256()
    total = 0
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(chunk_size), b""):
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def _hash_existing_file_into(path: str | Path, digest, chunk_size: int = 1024 * 1024) -> int:
    total = 0
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(chunk_size), b""):
            digest.update(chunk)
            total += len(chunk)
    return total


def _range_sidecar_path(partial_path: Path) -> Path:
    return partial_path.with_name(f"{partial_path.name}.ranges.json")


def _load_completed_ranges(sidecar_path: Path, *, total_bytes: int, etag: str | None) -> set[tuple[int, int]]:
    if not sidecar_path.exists():
        return set()
    try:
        payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    if int(payload.get("total_bytes") or 0) != total_bytes:
        return set()
    if (payload.get("etag") or None) != (etag or None):
        return set()
    completed_ranges = set()
    for item in payload.get("completed") or []:
        try:
            start = int(item[0])
            end = int(item[1])
        except Exception:
            continue
        if 0 <= start <= end < total_bytes:
            completed_ranges.add((start, end))
    return completed_ranges


def _write_completed_ranges(
    sidecar_path: Path,
    *,
    total_bytes: int,
    etag: str | None,
    completed: set[tuple[int, int]],
) -> None:
    payload = {
        "total_bytes": total_bytes,
        "etag": etag,
        "completed": [[start, end] for start, end in sorted(completed)],
        "updated_at": _utcnow().isoformat(),
    }
    tmp_path = sidecar_path.with_suffix(sidecar_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    os.replace(tmp_path, sidecar_path)


class PTG2ArtifactStore:
    def __init__(self, root: str | Path | None = None):
        self.root = Path(root) if root else resolve_ptg2_artifact_dir()
        self.root.mkdir(parents=True, exist_ok=True)
        self.tmp_dir = self.root / "tmp"
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.root / "manifest.jsonl"
        self.leases_dir = self.root / "leases"
        self.locks_dir = self.root / ".locks"

    def artifact_path(self, digest: str, kind: str = PTG2_ARTIFACT_RAW, suffix: str = "") -> Path:
        """Return this store's content-addressed path for an artifact."""
        return content_addressed_path(self.root, digest, kind=kind, suffix=suffix)

    def partial_path(self, canonical_url: str, suffix: str = "") -> Path:
        """Return and create the parent for a resumable URL-specific partial path."""
        digest = semantic_hash(canonical_url, domain="ptg2_partial_raw")
        clean_suffix = suffix if suffix.startswith(".") or not suffix else f".{suffix}"
        path = self.root / "partial-retained" / f"{digest}{clean_suffix}.partial"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def storage_uri(self, path: str | Path) -> str:
        """Return an absolute file URI for a local artifact path."""
        return Path(path).resolve().as_uri()

    def path_from_uri(self, uri: str) -> Path:
        """Convert a local file URI or path string into a Path."""
        if uri.startswith("file://"):
            return Path(unquote(urlsplit(uri).path))
        return Path(uri)

    @contextmanager
    def retention_lock(self):
        """Hold the store-wide lease, manifest, and collection lock."""

        with _PTG2FileLock(self.root / ".retention.lock"):
            yield

    def named_lock(self, namespace: str, key: str) -> _PTG2FileLock:
        """Return a per-artifact lock without creating another artifact copy."""

        safe_namespace = re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(namespace))[:64] or "artifact"
        safe_key = re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(key))[:160] or "default"
        return _PTG2FileLock(self.locks_dir / safe_namespace / f"{safe_key}.lock")

    def _manifest_entries(self) -> list[dict[str, Any]]:
        if not self.manifest_path.exists():
            return []
        entries: list[dict[str, Any]] = []
        try:
            with open(self.manifest_path, "r", encoding="utf-8") as fp:
                for line in fp:
                    try:
                        payload = json.loads(line)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if isinstance(payload, dict):
                        entries.append(payload)
        except FileNotFoundError:
            return []
        return entries

    def find_candidates(self, canonical_url: str) -> list[dict[str, Any]]:
        """Return retained raw-artifact manifest entries for a canonical URL."""
        latest_by_artifact: dict[tuple[str, str], dict[str, Any]] = {}
        for payload in self._manifest_entries():
            if (
                payload.get("canonical_url") != canonical_url
                or payload.get("artifact_kind") != PTG2_ARTIFACT_RAW
            ):
                continue
            uri = str(payload.get("raw_storage_uri") or payload.get("storage_uri") or "")
            digest = str(payload.get("raw_sha256") or "")
            latest_by_artifact[(uri, digest)] = payload
        return [
            payload
            for payload in latest_by_artifact.values()
            if payload.get("status") in (None, "", "available")
        ]

    def find_logical_candidates(self, raw_sha256: str) -> list[dict[str, Any]]:
        """Return retained logical JSON records for one raw container digest."""

        return [
            payload
            for payload in self._manifest_entries()
            if payload.get("artifact_kind") == "logical_json"
            and payload.get("raw_sha256") == raw_sha256
            and payload.get("status") in (None, "", "available")
        ]

    def record_manifest(self, payload: dict[str, Any]) -> None:
        """Append a timestamped artifact record to this store's manifest."""
        payload = dict(payload)
        payload.setdefault("recorded_at", _utcnow().isoformat())
        encoded = (json.dumps(payload, sort_keys=True, default=str) + "\n").encode("utf-8")
        with self.retention_lock():
            fd = os.open(
                self.manifest_path,
                os.O_RDWR | os.O_CREAT | os.O_APPEND,
                0o666,
            )
            try:
                end_offset = os.lseek(fd, 0, os.SEEK_END)
                if end_offset > 0 and os.pread(fd, 1, end_offset - 1) != b"\n":
                    # Isolate a crash-torn tail so this complete record remains
                    # independently readable and recoverable.
                    os.write(fd, b"\n")
                view = memoryview(encoded)
                while view:
                    written = os.write(fd, view)
                    if written <= 0:
                        raise OSError("short write while appending PTG artifact manifest")
                    view = view[written:]
                os.fsync(fd)
            finally:
                os.close(fd)


def _is_strong_etag(etag: str | None) -> bool:
    if not etag:
        return False
    normalized = etag.strip()
    return re.fullmatch(r'"[\x21\x23-\x7e\x80-\xff]*"', normalized) is not None


def _valid_raw_sha256(candidate: dict[str, Any]) -> str | None:
    raw_sha256 = candidate.get("raw_sha256")
    if not isinstance(raw_sha256, str) or re.fullmatch(r"[0-9a-fA-F]{64}", raw_sha256) is None:
        return None
    return raw_sha256.lower()


def choose_reusable_raw_artifact(
    candidates: list[dict[str, Any]],
    head: PTG2HeadMetadata | None,
    store: PTG2ArtifactStore | None = None,
    reuse_policy: str = "metadata_or_hash",
) -> tuple[dict[str, Any] | None, str | None]:
    """Choose a retained raw artifact that satisfies reuse policy."""
    candidates = [
        candidate
        for candidate in candidates
        if candidate.get("status") in (None, "", "available")
        and _valid_raw_sha256(candidate) is not None
    ]
    if not candidates:
        return None, None

    def has_raw_file(candidate: dict[str, Any]) -> bool:
        """Return whether a candidate's retained raw file exists."""
        if store is None:
            return True
        raw_uri = candidate.get("raw_storage_uri") or candidate.get("storage_uri")
        if not raw_uri:
            return False
        return store.path_from_uri(raw_uri).exists()

    if head is not None:
        for candidate in reversed(candidates):
            has_same_length = (
                head.content_length is not None
                and candidate.get("content_length") is not None
                and int(candidate["content_length"]) == int(head.content_length)
            )
            if (
                has_same_length
                and _is_strong_etag(head.etag)
                and candidate.get("etag") == head.etag
                and has_raw_file(candidate)
            ):
                return candidate, "strong_etag_length"
        if reuse_policy in {"metadata", "metadata_or_hash"}:
            for candidate in reversed(candidates):
                has_same_length = (
                    head.content_length is not None
                    and candidate.get("content_length") is not None
                    and int(candidate["content_length"]) == int(head.content_length)
                )
                same_modified = bool(head.last_modified and candidate.get("last_modified") == head.last_modified)
                if has_same_length and same_modified and has_raw_file(candidate):
                    return candidate, "length_last_modified"
    if store is not None and reuse_policy in {"hash", "metadata_or_hash"}:
        for candidate in reversed(candidates):
            raw_uri = candidate.get("raw_storage_uri") or candidate.get("storage_uri")
            expected = _valid_raw_sha256(candidate)
            if not raw_uri or not expected:
                continue
            raw_path = store.path_from_uri(raw_uri)
            if not raw_path.exists():
                continue
            actual, _ = sha256_file(raw_path)
            if actual == expected:
                return candidate, "verified_local_sha256"
    return None, None
