# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable request reuse and restart state for one immutable VSP capture."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.vsp_directory_contract import ADAPTER_ID, SOURCE_ID, canonical_json, request_key, require
from process.vsp_directory_http import strict_json


def _directory(path):
    path.mkdir(parents=True, exist_ok=True, mode=0o2770)
    if path.stat().st_mode & 0o7777 != 0o2770:
        path.chmod(0o2770)
    return path


def _atomic(path, raw, *, immutable=False):
    """Make complete fsynced bytes visible, without replacing immutable evidence."""
    _directory(path.parent)
    fd, temporary = tempfile.mkstemp(prefix=".vsp-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
            os.fchmod(output.fileno(), 0o640)
        if immutable:
            try:
                os.link(temporary, path)
            except FileExistsError:
                require(path.read_bytes() == raw, "vsp_retained_evidence_changed")
        else:
            os.replace(temporary, path)
        parent_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        Path(temporary).unlink(missing_ok=True)


def configured_root():
    """Reuse the directory artifact volume; temporary storage cannot hold an import."""
    configured = os.environ.get("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", "")
    require(bool(configured), "vsp_artifact_root_required")
    root = Path(configured).resolve()
    require(
        root.is_absolute() and root != Path("/") and not root.is_relative_to(Path(tempfile.gettempdir()).resolve()),
        "vsp_artifact_root_invalid",
    )
    return _directory(root / "vsp-directory")


class VSPCaptureStore:
    """A source-wide acquisition lock, immutable responses and small atomic checkpoints."""

    def __init__(self, root, root_run_id, *, require_retained=False):
        require(type(root_run_id) is str and 0 < len(root_run_id) <= 256, "vsp_capture_identity_invalid")
        self.root = _directory(Path(root))
        self.capture_id = hashlib.sha256(canonical_json([SOURCE_ID, ADAPTER_ID, root_run_id]).encode()).hexdigest()
        self.path = self.root / self.capture_id
        self._lock = PTG2ArtifactStore(self.root).named_lock("vsp-directory", "source-acquisition")
        identity_by_field = {
            "adapter_id": ADAPTER_ID,
            "source_id": SOURCE_ID,
            "root_run_id": root_run_id,
            "capture_id": self.capture_id,
        }
        identity = self.path / "identity.json"
        expected = canonical_json(identity_by_field).encode()
        if require_retained:
            require(identity.is_file() and identity.read_bytes() == expected, "vsp_retained_capture_required")
        else:
            _atomic(identity, expected, immutable=True)

    def acquire(self):
        """Refuse duplicate source workers instead of multiplying origin request load."""
        require(self._lock.try_acquire(blocking=False) is not None, "vsp_source_capture_already_active")

    def release(self):
        """Release only this process's source-wide acquisition lock."""
        self._lock.release()

    def checkpoint(self, name, value):
        """Persist bounded caller-owned progress before another origin request."""
        require(name in {"discovery", "enrichment", "complete", "binding"}, "vsp_checkpoint_invalid")
        _atomic(self.path / (name + ".json"), canonical_json(value).encode(), immutable=name == "complete")

    def load_checkpoint(self, name):
        """Load a checkpoint belonging to the exact capture identity."""
        require(name in {"discovery", "enrichment", "complete", "binding"}, "vsp_checkpoint_invalid")
        path = self.path / (name + ".json")
        return strict_json(path.read_bytes()) if path.exists() else None

    async def pause(self, boundary, cooldowns):
        """Keep source-wide Retry-After and route cooldown state across restarts."""
        candidate = datetime.fromisoformat(boundary)
        require(candidate.tzinfo is not None, "vsp_cooldown_invalid")
        path = self.root / "http-pause.json"
        if path.exists():
            previous = strict_json(path.read_bytes())
            candidate = max(candidate, datetime.fromisoformat(previous["not_before"]))
        _atomic(path, canonical_json({"not_before": candidate.isoformat(), "cooldowns": cooldowns}).encode())

    def pause_state(self):
        """Return a future origin delay; elapsed waits do not trigger a refetch."""
        path = self.root / "http-pause.json"
        if not path.exists():
            return None
        value = strict_json(path.read_bytes())
        boundary = datetime.fromisoformat(value["not_before"])
        require(boundary.tzinfo is not None, "vsp_cooldown_invalid")
        return value if boundary > datetime.now(UTC) else None

    def retain(self, raw, provenance):
        """Retain source bytes before recording a reusable request result."""
        digest = hashlib.sha256(raw).hexdigest()
        require(digest == provenance["sha256"] and len(raw) == provenance["byte_count"], "vsp_response_digest_mismatch")
        key = request_key(provenance["method"], provenance["url"], provenance.get("payload"))
        require(key == provenance["request_key"], "vsp_request_identity_mismatch")
        _atomic(self.path / "raw" / digest[:2] / (digest + ".json"), raw, immutable=True)
        metadata = canonical_json(provenance).encode()
        receipt_id = hashlib.sha256(metadata).hexdigest()
        _atomic(self.path / "receipts" / (receipt_id + ".json"), metadata, immutable=True)
        # Fresh census rechecks keep separate receipts and never rewrite first observations.
        cache = self.path / "requests" / (key + ".json")
        if not cache.exists():
            _atomic(cache, metadata, immutable=True)

    def lookup(self, method, url, payload=None):
        """Verify retained bytes and exact query identity before avoiding an origin call."""
        key = request_key(method, url, payload)
        path = self.path / "requests" / (key + ".json")
        if not path.exists():
            return None
        metadata = strict_json(path.read_bytes())
        require(
            metadata["request_key"] == key
            and metadata["method"] == method
            and metadata["url"] == url
            and metadata.get("payload") == payload,
            "vsp_request_identity_mismatch",
        )
        digest = metadata["sha256"]
        require(
            type(digest) is str and len(digest) == 64 and all(x in "0123456789abcdef" for x in digest),
            "vsp_response_digest_mismatch",
        )
        raw = (self.path / "raw" / digest[:2] / (digest + ".json")).read_bytes()
        require(
            len(raw) == metadata["byte_count"] and hashlib.sha256(raw).hexdigest() == digest,
            "vsp_response_digest_mismatch",
        )
        return strict_json(raw), metadata

    def requests(self):
        """Stream this capture's exact retained request provenance for final accounting."""
        directory = self.path / "receipts"
        for path in sorted(directory.glob("*.json")):
            yield strict_json(path.read_bytes())
