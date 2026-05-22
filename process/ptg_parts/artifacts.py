# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained raw-artifact storage helpers for PTG2 imports."""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from process.ptg_parts.canonical import semantic_hash
from process.ptg_parts.domain import PTG2_ARTIFACT_RAW, PTG2HeadMetadata


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


def resolve_ptg2_artifact_dir() -> Path:
    configured = os.getenv("HLTHPRT_PTG2_ARTIFACT_DIR")
    root = Path(configured) if configured else Path(tempfile.gettempdir()) / "healthporta-ptg2-artifacts"
    root.mkdir(parents=True, exist_ok=True)
    return root


def content_addressed_path(root: str | Path, digest: str, kind: str = PTG2_ARTIFACT_RAW, suffix: str = "") -> Path:
    digest_text = str(digest)
    clean_suffix = suffix if suffix.startswith(".") or not suffix else f".{suffix}"
    return Path(root) / kind / digest_text[:2] / digest_text[2:4] / f"{digest_text}{clean_suffix}"


def ptg2_temp_parent() -> Path:
    return PTG2ArtifactStore().tmp_dir


def _safe_url_suffix(url: str) -> str:
    path = urlsplit(url).path
    suffixes = "".join(Path(path).suffixes[-2:])
    return suffixes[:24]


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
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
    completed = set()
    for item in payload.get("completed") or []:
        try:
            start = int(item[0])
            end = int(item[1])
        except Exception:
            continue
        if 0 <= start <= end < total_bytes:
            completed.add((start, end))
    return completed


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

    def artifact_path(self, digest: str, kind: str = PTG2_ARTIFACT_RAW, suffix: str = "") -> Path:
        return content_addressed_path(self.root, digest, kind=kind, suffix=suffix)

    def partial_path(self, canonical_url: str, suffix: str = "") -> Path:
        digest = semantic_hash(canonical_url, domain="ptg2_partial_raw")
        clean_suffix = suffix if suffix.startswith(".") or not suffix else f".{suffix}"
        path = self.root / "partial-retained" / f"{digest}{clean_suffix}.partial"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def storage_uri(self, path: str | Path) -> str:
        return Path(path).resolve().as_uri()

    def path_from_uri(self, uri: str) -> Path:
        if uri.startswith("file://"):
            return Path(unquote(urlsplit(uri).path))
        return Path(uri)

    def find_candidates(self, canonical_url: str) -> list[dict[str, Any]]:
        if not self.manifest_path.exists():
            return []
        candidates: list[dict[str, Any]] = []
        with open(self.manifest_path, "r", encoding="utf-8") as fp:
            for line in fp:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("canonical_url") == canonical_url and payload.get("artifact_kind") == PTG2_ARTIFACT_RAW:
                    candidates.append(payload)
        return candidates

    def record_manifest(self, payload: dict[str, Any]) -> None:
        payload = dict(payload)
        payload.setdefault("recorded_at", _utcnow().isoformat())
        with open(self.manifest_path, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def _is_strong_etag(etag: str | None) -> bool:
    if not etag:
        return False
    return not etag.strip().lower().startswith("w/")


def choose_reusable_raw_artifact(
    candidates: list[dict[str, Any]],
    head: PTG2HeadMetadata | None,
    store: PTG2ArtifactStore | None = None,
    reuse_policy: str = "metadata_or_hash",
) -> tuple[dict[str, Any] | None, str | None]:
    if not candidates:
        return None, None
    if head is not None:
        for candidate in reversed(candidates):
            same_length = (
                head.content_length is not None
                and candidate.get("content_length") is not None
                and int(candidate["content_length"]) == int(head.content_length)
            )
            if same_length and _is_strong_etag(head.etag) and candidate.get("etag") == head.etag:
                return candidate, "strong_etag_length"
        if reuse_policy in {"metadata", "metadata_or_hash"}:
            for candidate in reversed(candidates):
                same_length = (
                    head.content_length is not None
                    and candidate.get("content_length") is not None
                    and int(candidate["content_length"]) == int(head.content_length)
                )
                same_modified = bool(head.last_modified and candidate.get("last_modified") == head.last_modified)
                if same_length and same_modified:
                    return candidate, "length_last_modified"
    if store is not None and reuse_policy in {"hash", "metadata_or_hash"}:
        for candidate in reversed(candidates):
            raw_uri = candidate.get("raw_storage_uri") or candidate.get("storage_uri")
            expected = candidate.get("raw_sha256") or candidate.get("sha256")
            if not raw_uri or not expected:
                continue
            raw_path = store.path_from_uri(raw_uri)
            if not raw_path.exists():
                continue
            actual, _ = sha256_file(raw_path)
            if actual == expected:
                return candidate, "verified_local_sha256"
    return None, None
