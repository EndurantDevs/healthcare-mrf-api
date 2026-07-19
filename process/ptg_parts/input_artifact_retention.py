# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lease and collect retained PTG raw and expanded logical input artifacts."""

from __future__ import annotations

import argparse
import asyncio
import contextvars
import datetime
import hashlib
import json
import logging
import os
import socket
import tempfile
import threading
import uuid
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Iterator, TypeVar

from process.ptg_parts.artifacts import PTG2ArtifactStore, sha256_file


logger = logging.getLogger(__name__)


PTG2_ARTIFACT_LEASE_TTL_SECONDS_ENV = "HLTHPRT_PTG2_ARTIFACT_LEASE_TTL_SECONDS"
PTG2_ARTIFACT_LEASE_HEARTBEAT_SECONDS_ENV = "HLTHPRT_PTG2_ARTIFACT_LEASE_HEARTBEAT_SECONDS"
PTG2_INPUT_ARTIFACT_RETENTION_HOURS_ENV = "HLTHPRT_PTG2_INPUT_ARTIFACT_RETENTION_HOURS"
PTG2_INPUT_ARTIFACT_MIN_AGE_HOURS_ENV = "HLTHPRT_PTG2_INPUT_ARTIFACT_MIN_AGE_HOURS"
PTG2_INPUT_ARTIFACT_TARGET_BYTES_ENV = "HLTHPRT_PTG2_INPUT_ARTIFACT_TARGET_BYTES"
PTG2_INPUT_ARTIFACT_MAX_DELETE_BYTES_ENV = "HLTHPRT_PTG2_INPUT_ARTIFACT_MAX_DELETE_BYTES"
PTG2_INPUT_ARTIFACT_MAX_DELETE_FILES_ENV = "HLTHPRT_PTG2_INPUT_ARTIFACT_MAX_DELETE_FILES"

DEFAULT_LEASE_TTL_SECONDS = 6 * 60 * 60
DEFAULT_LEASE_HEARTBEAT_SECONDS = 5 * 60
DEFAULT_RETENTION_HOURS = 24.0
DEFAULT_MIN_AGE_HOURS = 1.0
DEFAULT_TARGET_BYTES = 300 * 1024**3
DEFAULT_MAX_DELETE_BYTES = 100 * 1024**3
DEFAULT_MAX_DELETE_FILES = 2000
LEASE_SCHEMA_VERSION = 1
UNLEASED_SCHEMA_VERSION = 1
NAMED_LOCK_STRIPES = 256
MANAGED_ARTIFACT_KINDS = frozenset({"raw", "logical", "partial-retained"})

_CURRENT_LEASE_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "ptg2_artifact_lease_id",
    default=None,
)
_LEASE_REGISTRY_LOCK = threading.Lock()
_LEASE_REGISTRY: dict[str, "PTG2ArtifactLease"] = {}
_T = TypeVar("_T")


class PTG2ArtifactLeaseLostError(RuntimeError):
    """Raised when an import can no longer prove ownership of its artifact lease."""


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _timestamp(value: datetime.datetime) -> str:
    normalized = value.astimezone(datetime.UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime.datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.UTC)
    return parsed.astimezone(datetime.UTC)


def _make_shared_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with suppress(OSError):
        path.chmod(0o777)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    _make_shared_directory(path.parent)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, sort_keys=True, separators=(",", ":"))
            fp.write("\n")
            fp.flush()
            os.fsync(fp.fileno())
        with suppress(OSError):
            temporary_path.chmod(0o666)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


def _lease_marker_path(store: PTG2ArtifactStore, lease_id: str) -> Path:
    return store.leases_dir / f"{lease_id}.json"


def _unleased_dir(store: PTG2ArtifactStore) -> Path:
    return store.root / ".retention" / "unleased"


def _unleased_marker_path(store: PTG2ArtifactStore, relative_path: str) -> Path:
    digest = hashlib.sha256(relative_path.encode("utf-8")).hexdigest()
    return _unleased_dir(store) / f"{digest}.json"


def _unleased_payload(relative_path: str, now: datetime.datetime) -> dict[str, Any]:
    return {
        "schema_version": UNLEASED_SCHEMA_VERSION,
        "relative_path": relative_path,
        "unleased_since": _timestamp(now),
    }


def _normalized_managed_relative_path(value: Any, *, prefix: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError("artifact reference is not a string")
    candidate = value.rstrip("/") if prefix else value
    path = Path(candidate)
    if (
        not candidate
        or path.is_absolute()
        or path.as_posix() != candidate
        or not path.parts
        or path.parts[0] not in MANAGED_ARTIFACT_KINDS
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError(f"invalid managed artifact reference: {value!r}")
    return candidate


def _payload_reference_sets(payload: dict[str, Any]) -> tuple[set[str], set[str]]:
    path_values = payload.get("paths", [])
    prefix_values = payload.get("prefixes", [])
    if not isinstance(path_values, list) or not isinstance(prefix_values, list):
        raise ValueError("artifact lease references must be lists")
    return (
        {_normalized_managed_relative_path(value) for value in path_values},
        {
            _normalized_managed_relative_path(value, prefix=True)
            for value in prefix_values
        },
    )


def _read_unleased_since_locked(
    store: PTG2ArtifactStore,
    relative_path: str,
) -> datetime.datetime | None:
    marker_path = _unleased_marker_path(store, relative_path)
    payload = _read_json(marker_path)
    if payload is None:
        return None
    if (
        payload.get("schema_version") != UNLEASED_SCHEMA_VERSION
        or payload.get("relative_path") != relative_path
    ):
        raise RuntimeError(f"Invalid PTG artifact retention marker: {marker_path}")
    parsed = _parse_timestamp(payload.get("unleased_since"))
    if parsed is None:
        raise RuntimeError(f"Invalid PTG artifact retention timestamp: {marker_path}")
    return parsed


def _is_newly_marked_unleased_locked(
    store: PTG2ArtifactStore,
    relative_path: str,
    *,
    now: datetime.datetime,
) -> bool:
    marker_path = _unleased_marker_path(store, relative_path)
    if marker_path.exists():
        # The first observed unleased time starts the grace period. Repeated GC
        # runs must not extend it indefinitely.
        _read_unleased_since_locked(store, relative_path)
        return False
    _atomic_write_json(marker_path, _unleased_payload(relative_path, now))
    return True


_mark_unleased_locked = _is_newly_marked_unleased_locked


def _clear_unleased_locked(store: PTG2ArtifactStore, relative_path: str) -> None:
    _unleased_marker_path(store, relative_path).unlink(missing_ok=True)


def current_artifact_lease_id() -> str | None:
    """Return the artifact lease ID bound to the current context, if any."""

    return _CURRENT_LEASE_ID.get()


@contextmanager
def bind_artifact_lease(lease_id: str | None) -> Iterator[None]:
    """Bind a lease for this context, then restore the previous binding."""

    token = _CURRENT_LEASE_ID.set(str(lease_id) if lease_id else None)
    try:
        yield
    finally:
        _CURRENT_LEASE_ID.reset(token)


def _lease_referenced_paths_locked(
    store: PTG2ArtifactStore,
    payload: dict[str, Any],
) -> set[str]:
    referenced, prefixes = _payload_reference_sets(payload)
    for prefix in sorted(prefixes):
        prefix_path = store.root / prefix
        if not prefix_path.exists() or not prefix_path.is_dir():
            continue
        for path in sorted(prefix_path.rglob("*")):
            if path.is_file() and not path.is_symlink():
                referenced.add(path.relative_to(store.root).as_posix())
    return referenced


def _mark_released_paths_unleased_locked(
    store: PTG2ArtifactStore,
    payload: dict[str, Any],
    *,
    active_exact_paths: set[str],
    active_prefixes: set[str],
    now: datetime.datetime,
) -> set[str]:
    newly_unleased_paths: set[str] = set()
    for relative_path in sorted(_lease_referenced_paths_locked(store, payload)):
        if _is_protected(relative_path, active_exact_paths, active_prefixes):
            continue
        path = store.root / relative_path
        if path.is_file() and not path.is_symlink():
            if _mark_unleased_locked(store, relative_path, now=now):
                newly_unleased_paths.add(relative_path)
    return newly_unleased_paths


class PTG2ArtifactLease:
    """One crash-expiring import reference set stored as an atomic marker."""

    def __init__(
        self,
        *,
        store: PTG2ArtifactStore | None = None,
        owner: str,
        ttl_seconds: int | None = None,
        heartbeat_seconds: int | None = None,
        lease_id: str | None = None,
    ):
        self.store = store or PTG2ArtifactStore()
        self.owner = str(owner)
        self.ttl_seconds = max(
            int(ttl_seconds if ttl_seconds is not None else _env_int(
                PTG2_ARTIFACT_LEASE_TTL_SECONDS_ENV,
                DEFAULT_LEASE_TTL_SECONDS,
            )),
            60,
        )
        configured_heartbeat = (
            heartbeat_seconds
            if heartbeat_seconds is not None
            else _env_int(
                PTG2_ARTIFACT_LEASE_HEARTBEAT_SECONDS_ENV,
                DEFAULT_LEASE_HEARTBEAT_SECONDS,
            )
        )
        self.heartbeat_seconds = max(min(int(configured_heartbeat), self.ttl_seconds // 2), 0)
        self.lease_id = lease_id or uuid.uuid4().hex
        self.marker_path = _lease_marker_path(self.store, self.lease_id)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._released = False

    def _new_payload(self, now: datetime.datetime) -> dict[str, Any]:
        return {
            "schema_version": LEASE_SCHEMA_VERSION,
            "lease_id": self.lease_id,
            "owner": self.owner,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "created_at": _timestamp(now),
            "heartbeat_at": _timestamp(now),
            "expires_at": _timestamp(now + datetime.timedelta(seconds=self.ttl_seconds)),
            "ttl_seconds": self.ttl_seconds,
            "paths": [],
            "prefixes": [],
        }

    def start(self) -> "PTG2ArtifactLease":
        """Create and register the durable marker, starting renewal if configured."""

        if self._started and not self._released:
            return self
        now = _utcnow()
        with self.store.retention_lock():
            _make_shared_directory(self.store.leases_dir)
            if self.marker_path.exists():
                raise RuntimeError(f"PTG artifact lease already exists: {self.lease_id}")
            _atomic_write_json(self.marker_path, self._new_payload(now))
        self._started = True
        self._released = False
        with _LEASE_REGISTRY_LOCK:
            _LEASE_REGISTRY[self.lease_id] = self
        if self.heartbeat_seconds > 0:
            self._thread = threading.Thread(
                target=self._heartbeat_loop,
                name=f"ptg2-artifact-lease-{self.lease_id[:12]}",
                daemon=True,
            )
            self._thread.start()
        return self

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_seconds):
            try:
                self.heartbeat()
            except Exception:
                # A later artifact access fails closed if the marker was actually lost.
                continue

    def heartbeat(self) -> None:
        """Extend the lease expiry, raising if its durable marker was lost."""

        if self._released:
            return
        now = _utcnow()
        with self.store.retention_lock():
            # release() marks the lease before waiting for this same lock. A
            # heartbeat that was already queued must not turn normal release
            # into an apparent lease-loss failure.
            if self._released:
                return
            payload = _read_json(self.marker_path)
            if payload is None or payload.get("lease_id") != self.lease_id:
                raise PTG2ArtifactLeaseLostError(
                    f"PTG artifact lease marker was lost: {self.lease_id}"
                )
            payload["heartbeat_at"] = _timestamp(now)
            payload["expires_at"] = _timestamp(
                now + datetime.timedelta(seconds=self.ttl_seconds)
            )
            _atomic_write_json(self.marker_path, payload)

    @property
    def is_released(self) -> bool:
        """Return whether this lease has been released locally."""

        return self._released

    released = is_released

    def release(self) -> None:
        """Stop renewal, remove the marker, and mark unprotected artifacts unleased."""

        if self._released:
            return
        self._released = True
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=5)
        with self.store.retention_lock():
            lease_payload = _read_json(self.marker_path)
            self.marker_path.unlink(missing_ok=True)
            if (
                lease_payload is not None
                and lease_payload.get("lease_id") == self.lease_id
            ):
                now = _utcnow()
                try:
                    exact_paths, prefixes, _active_ids, _stale = _active_lease_references_locked(
                        self.store,
                        now=now,
                    )
                except RuntimeError:
                    logger.exception(
                        "PTG artifact lease released, but unleased metadata was not updated"
                    )
                else:
                    _mark_released_paths_unleased_locked(
                        self.store,
                        lease_payload,
                        active_exact_paths=exact_paths,
                        active_prefixes=prefixes,
                        now=now,
                    )
        with _LEASE_REGISTRY_LOCK:
            _LEASE_REGISTRY.pop(self.lease_id, None)


@contextmanager
def artifact_lease_context(
    *,
    owner: str,
    store: PTG2ArtifactStore | None = None,
    ttl_seconds: int | None = None,
    heartbeat_seconds: int | None = None,
) -> Iterator[PTG2ArtifactLease]:
    """Start and bind a lease for the context, releasing it on exit."""

    lease = PTG2ArtifactLease(
        store=store,
        owner=owner,
        ttl_seconds=ttl_seconds,
        heartbeat_seconds=heartbeat_seconds,
    ).start()
    try:
        with bind_artifact_lease(lease.lease_id):
            yield lease
    finally:
        lease.release()


def has_released_current_artifact_lease() -> bool:
    """Release the registered current lease and report whether one was found."""

    lease_id = current_artifact_lease_id()
    if not lease_id:
        return False
    with _LEASE_REGISTRY_LOCK:
        lease = _LEASE_REGISTRY.get(lease_id)
    if lease is None:
        return False
    lease.release()
    return True


release_current_artifact_lease = has_released_current_artifact_lease


async def guard_artifact_lease(
    lease: PTG2ArtifactLease,
    operation: Awaitable[_T],
) -> _T:
    """Cancel an async import promptly if its durable lease cannot be renewed."""

    operation_task = asyncio.ensure_future(operation)
    if lease.heartbeat_seconds <= 0:
        return await operation_task

    async def monitor() -> None:
        """Renew the lease until canceled, surfacing heartbeat failures."""

        interval = max(min(lease.heartbeat_seconds, 60), 1)
        while True:
            await asyncio.sleep(interval)
            if lease.released:
                continue
            await asyncio.to_thread(lease.heartbeat)

    monitor_task = asyncio.create_task(
        monitor(),
        name=f"ptg2-artifact-lease-guard-{lease.lease_id[:12]}",
    )
    try:
        done, _pending = await asyncio.wait(
            {operation_task, monitor_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if operation_task in done:
            return await operation_task
        try:
            await monitor_task
        except PTG2ArtifactLeaseLostError:
            raise
        except Exception as exc:
            raise PTG2ArtifactLeaseLostError(
                f"PTG artifact lease heartbeat failed: {lease.lease_id}"
            ) from exc
        raise PTG2ArtifactLeaseLostError(
            f"PTG artifact lease monitor stopped unexpectedly: {lease.lease_id}"
        )
    finally:
        if not operation_task.done():
            operation_task.cancel()
        if not monitor_task.done():
            monitor_task.cancel()
        await asyncio.gather(operation_task, monitor_task, return_exceptions=True)


def _relative_artifact_path(store: PTG2ArtifactStore, path: str | Path) -> str:
    root = store.root.resolve()
    candidate = Path(path).resolve(strict=False)
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"Artifact path is outside the retained store: {candidate}") from exc
    if not relative.parts or relative.parts[0] not in MANAGED_ARTIFACT_KINDS:
        raise ValueError(f"Artifact path is not a managed retained-input path: {candidate}")
    return relative.as_posix()


def _updated_lease_payload_locked(
    store: PTG2ArtifactStore,
    lease_id: str,
    *,
    relative_path: str,
    prefix: bool,
) -> tuple[Path, dict[str, Any]]:
    marker_path = _lease_marker_path(store, lease_id)
    payload = _read_json(marker_path)
    if payload is None or payload.get("lease_id") != lease_id:
        raise PTG2ArtifactLeaseLostError(f"PTG artifact lease marker was lost: {lease_id}")
    field = "prefixes" if prefix else "paths"
    values = {str(value) for value in payload.get(field, []) if value}
    values.add(relative_path)
    payload[field] = sorted(values)
    now = _utcnow()
    ttl_seconds = max(int(payload.get("ttl_seconds") or DEFAULT_LEASE_TTL_SECONDS), 60)
    payload["heartbeat_at"] = _timestamp(now)
    payload["expires_at"] = _timestamp(now + datetime.timedelta(seconds=ttl_seconds))
    return marker_path, payload


def has_protected_existing_artifact(
    store: PTG2ArtifactStore,
    path: str | Path,
) -> bool:
    """Atomically add an existing file to the current import reference set."""

    artifact_path = Path(path)
    relative_path = _relative_artifact_path(store, artifact_path)
    lease_id = current_artifact_lease_id()
    with store.retention_lock():
        if not artifact_path.exists() or not artifact_path.is_file() or artifact_path.is_symlink():
            return False
        if lease_id:
            marker_path, payload = _updated_lease_payload_locked(
                store,
                lease_id,
                relative_path=relative_path,
                prefix=False,
            )
            _atomic_write_json(marker_path, payload)
            _clear_unleased_locked(store, relative_path)
    return True


protect_existing_artifact = has_protected_existing_artifact


def protect_artifact_path(store: PTG2ArtifactStore, path: str | Path) -> None:
    """Protect a managed path before a downloader creates or resumes it."""

    relative_path = _relative_artifact_path(store, path)
    lease_id = current_artifact_lease_id()
    if not lease_id:
        return
    with store.retention_lock():
        marker_path, payload = _updated_lease_payload_locked(
            store,
            lease_id,
            relative_path=relative_path,
            prefix=False,
        )
        _atomic_write_json(marker_path, payload)
        _clear_unleased_locked(store, relative_path)


def protect_artifact_prefix(store: PTG2ArtifactStore, path: str | Path) -> None:
    """Protect a logical namespace before a long expansion starts writing."""

    relative_path = _relative_artifact_path(store, path)
    lease_id = current_artifact_lease_id()
    if not lease_id:
        return
    with store.retention_lock():
        marker_path, payload = _updated_lease_payload_locked(
            store,
            lease_id,
            relative_path=relative_path,
            prefix=True,
        )
        _atomic_write_json(marker_path, payload)
        prefix_path = store.root / relative_path
        if prefix_path.exists():
            for artifact_path in prefix_path.rglob("*"):
                if artifact_path.is_file() and not artifact_path.is_symlink():
                    _clear_unleased_locked(
                        store,
                        artifact_path.relative_to(store.root).as_posix(),
                    )


def publish_artifact_file(
    store: PTG2ArtifactStore,
    temporary_path: str | Path,
    final_path: str | Path,
    *,
    expected_sha256: str | None = None,
) -> Path:
    """Publish one file and register its lease reference in the same store lock."""

    temporary = Path(temporary_path)
    final = Path(final_path)
    relative_path = _relative_artifact_path(store, final)
    lease_id = current_artifact_lease_id()
    with store.retention_lock():
        marker: tuple[Path, dict[str, Any]] | None = None
        if lease_id:
            marker = _updated_lease_payload_locked(
                store,
                lease_id,
                relative_path=relative_path,
                prefix=False,
            )
        final.parent.mkdir(parents=True, exist_ok=True)
        if final.exists():
            if final.is_symlink() or not final.is_file():
                raise RuntimeError(f"Retained artifact target is not a regular file: {final}")
            if temporary != final:
                if expected_sha256 is None:
                    temporary.unlink(missing_ok=True)
                else:
                    existing_sha256, _existing_size = sha256_file(final)
                    if existing_sha256 == expected_sha256:
                        temporary.unlink(missing_ok=True)
                    else:
                        staged_sha256, _staged_size = sha256_file(temporary)
                        if staged_sha256 != expected_sha256:
                            raise RuntimeError(
                                "Retained artifact staging checksum does not match its identity: "
                                f"{temporary}"
                            )
                        os.replace(temporary, final)
        else:
            os.replace(temporary, final)
        if marker is not None:
            _atomic_write_json(*marker)
            _clear_unleased_locked(store, relative_path)
        else:
            _mark_unleased_locked(store, relative_path, now=_utcnow())
    return final


@asynccontextmanager
async def async_named_artifact_lock(
    store: PTG2ArtifactStore,
    namespace: str,
    key: str,
):
    """Acquire one bounded lock stripe without blocking an event loop."""

    digest = hashlib.sha256(f"{namespace}\0{key}".encode("utf-8")).digest()
    stripe = int.from_bytes(digest[:2], "big") % NAMED_LOCK_STRIPES
    lock = store.named_lock("retained-input", f"stripe-{stripe:03d}")
    acquire_task = asyncio.create_task(asyncio.to_thread(lock.acquire))
    try:
        await asyncio.shield(acquire_task)
    except BaseException:
        await acquire_task
        lock.release()
        raise
    try:
        yield
    finally:
        lock.release()


@dataclass(frozen=True)
class _StoredArtifact:
    path: Path
    relative_path: str
    size: int
    modified_at: float
    protected: bool
    unleased_since: datetime.datetime | None


@dataclass(frozen=True)
class _StaleLease:
    path: Path
    payload: dict[str, Any]


@dataclass(frozen=True)
class PTG2InputArtifactGCResult:
    executed: bool
    root: Path
    active_lease_ids: tuple[str, ...]
    stale_lease_files: tuple[Path, ...]
    protected_files: tuple[Path, ...]
    newly_unleased_files: tuple[Path, ...]
    eligible_files: tuple[Path, ...]
    selected_files: tuple[Path, ...]
    deleted_files: tuple[Path, ...]
    total_bytes_before: int
    total_bytes_after: int
    selected_bytes: int
    deleted_bytes: int
    target_bytes: int | None
    manifest_entries_before: int
    manifest_entries_after: int
    manifest_invalid_lines: int

    @property
    def over_target_bytes(self) -> int:
        """Return bytes remaining above the configured target after this cycle."""

        if self.target_bytes is None:
            return 0
        return max(self.total_bytes_after - self.target_bytes, 0)


def _active_lease_references_locked(
    store: PTG2ArtifactStore,
    *,
    now: datetime.datetime,
) -> tuple[set[str], set[str], tuple[str, ...], tuple[_StaleLease, ...]]:
    exact_paths: set[str] = set()
    prefixes: set[str] = set()
    active_ids: list[str] = []
    stale_leases: list[_StaleLease] = []
    if not store.leases_dir.exists():
        return exact_paths, prefixes, (), ()
    for marker_path in sorted(store.leases_dir.glob("*.json")):
        lease_payload = _read_json(marker_path)
        expected_id = marker_path.stem
        if (
            lease_payload is None
            or lease_payload.get("schema_version") != LEASE_SCHEMA_VERSION
            or lease_payload.get("lease_id") != expected_id
        ):
            raise RuntimeError(
                f"Invalid PTG artifact lease marker; cleanup fails closed: {marker_path}"
            )
        expires_at = _parse_timestamp(lease_payload.get("expires_at"))
        if expires_at is None:
            raise RuntimeError(
                f"Invalid PTG artifact lease expiration; cleanup fails closed: {marker_path}"
            )
        try:
            referenced_paths, referenced_prefixes = _payload_reference_sets(lease_payload)
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid PTG artifact lease references; cleanup fails closed: {marker_path}"
            ) from exc
        if expires_at <= now:
            stale_leases.append(
                _StaleLease(path=marker_path, payload=lease_payload)
            )
            continue
        active_ids.append(expected_id)
        exact_paths.update(referenced_paths)
        prefixes.update(referenced_prefixes)
    return exact_paths, prefixes, tuple(active_ids), tuple(stale_leases)


def _is_protected(relative_path: str, exact_paths: set[str], prefixes: set[str]) -> bool:
    if relative_path in exact_paths:
        return True
    return any(
        relative_path == prefix or relative_path.startswith(f"{prefix}/")
        for prefix in prefixes
    )


def _stale_unleased_candidates_locked(
    store: PTG2ArtifactStore,
    stale_leases: tuple[_StaleLease, ...],
    *,
    active_exact_paths: set[str],
    active_prefixes: set[str],
) -> dict[str, datetime.datetime]:
    candidates_by_path: dict[str, datetime.datetime] = {}
    for stale_lease in stale_leases:
        expires_at = _parse_timestamp(stale_lease.payload.get("expires_at"))
        if expires_at is None:
            raise RuntimeError(f"Invalid PTG artifact lease expiration: {stale_lease.path}")
        for relative_path in _lease_referenced_paths_locked(store, stale_lease.payload):
            if _is_protected(relative_path, active_exact_paths, active_prefixes):
                continue
            artifact_path = store.root / relative_path
            if not artifact_path.is_file() or artifact_path.is_symlink():
                continue
            previous = candidates_by_path.get(relative_path)
            if previous is None or expires_at > previous:
                candidates_by_path[relative_path] = expires_at
    return candidates_by_path


def _stored_artifacts_locked(
    store: PTG2ArtifactStore,
    *,
    exact_paths: set[str],
    prefixes: set[str],
    persist_unleased: bool,
    observation_time: datetime.datetime,
    unleased_since_overrides: dict[str, datetime.datetime] | None = None,
) -> tuple[list[_StoredArtifact], list[Path]]:
    stored_artifacts: list[_StoredArtifact] = []
    newly_unleased_paths: list[Path] = []
    for kind in sorted(MANAGED_ARTIFACT_KINDS):
        kind_root = store.root / kind
        if not kind_root.exists():
            continue
        for path in sorted(kind_root.rglob("*")):
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                stat = path.stat()
            except OSError:
                continue
            relative_path = path.relative_to(store.root).as_posix()
            is_protected = _is_protected(relative_path, exact_paths, prefixes)
            unleased_since = _read_unleased_since_locked(store, relative_path)
            if not is_protected and unleased_since is None:
                unleased_since = (unleased_since_overrides or {}).get(relative_path)
                if unleased_since is None:
                    # Unknown/legacy files get a durable first-observed time.
                    # Their old mtime never short-circuits the grace period.
                    unleased_since = observation_time
                newly_unleased_paths.append(path)
                if persist_unleased:
                    _mark_unleased_locked(store, relative_path, now=unleased_since)
            stored_artifacts.append(
                _StoredArtifact(
                    path=path,
                    relative_path=relative_path,
                    size=stat.st_size,
                    modified_at=stat.st_mtime,
                    protected=is_protected,
                    unleased_since=unleased_since,
                )
            )
    return stored_artifacts, newly_unleased_paths


def _select_artifacts(
    stored: list[_StoredArtifact],
    *,
    now_timestamp: float,
    retention_seconds: float,
    min_age_seconds: float,
    target_bytes: int | None,
    max_delete_bytes: int | None,
    max_delete_files: int | None,
) -> tuple[list[_StoredArtifact], list[_StoredArtifact]]:
    """Return age-eligible artifacts and a bounded, retention-first selection."""

    eligible_artifacts: list[_StoredArtifact] = []
    retention_due_paths: set[str] = set()
    for artifact in stored:
        if artifact.protected or artifact.unleased_since is None:
            continue
        unleased_age = now_timestamp - artifact.unleased_since.timestamp()
        file_age = now_timestamp - artifact.modified_at
        if unleased_age >= min_age_seconds and file_age >= min_age_seconds:
            eligible_artifacts.append(artifact)
            if unleased_age >= retention_seconds:
                retention_due_paths.add(artifact.relative_path)
    eligible_artifacts.sort(
        key=lambda artifact: (
            artifact.unleased_since.timestamp()
            if artifact.unleased_since
            else now_timestamp,
            artifact.modified_at,
            artifact.relative_path,
        )
    )
    selected_artifacts: list[_StoredArtifact] = []
    selected_paths: set[str] = set()
    selected_bytes = 0

    def can_select(artifact: _StoredArtifact) -> bool:
        """Return whether an item fits deletion caps, allowing one oversized first item."""

        if (
            max_delete_files is not None
            and len(selected_artifacts) >= max_delete_files
        ):
            return False
        if (
            max_delete_bytes is None
            or selected_bytes + artifact.size <= max_delete_bytes
        ):
            return True
        return not selected_artifacts

    def add(artifact: _StoredArtifact, current_bytes: int) -> int:
        """Record an item for deletion and return the updated selected byte count."""

        selected_artifacts.append(artifact)
        selected_paths.add(artifact.relative_path)
        return current_bytes + artifact.size

    for artifact in eligible_artifacts:
        if artifact.relative_path in retention_due_paths and can_select(artifact):
            selected_bytes = add(artifact, selected_bytes)

    total_bytes = sum(artifact.size for artifact in stored)
    projected_bytes = total_bytes - selected_bytes
    if target_bytes is not None and projected_bytes > target_bytes:
        for artifact in eligible_artifacts:
            if artifact.relative_path in selected_paths:
                continue
            if not can_select(artifact):
                continue
            selected_bytes = add(artifact, selected_bytes)
            projected_bytes -= artifact.size
            if projected_bytes <= target_bytes:
                break
    return eligible_artifacts, selected_artifacts


def _manifest_record_key(payload: dict[str, Any]) -> tuple[str, ...]:
    kind = str(payload.get("artifact_kind") or "unknown")
    uri = str(
        payload.get("raw_storage_uri")
        or payload.get("logical_storage_uri")
        or payload.get("storage_uri")
        or ""
    )
    content_digest = str(payload.get("raw_sha256") or payload.get("partial_sha256") or "")
    if kind == "partial_raw":
        # A resumable path is overwritten as a download advances. Only its
        # latest observation is useful regardless of intermediate hashes.
        content_digest = ""
    return (
        kind,
        str(payload.get("canonical_url") or ""),
        content_digest,
        str(payload.get("logical_sha256") or ""),
        uri,
    )


def _is_record_pointing_to_missing_file(
    store: PTG2ArtifactStore,
    payload: dict[str, Any],
) -> bool:
    uri = (
        payload.get("raw_storage_uri")
        or payload.get("logical_storage_uri")
        or payload.get("storage_uri")
    )
    if not isinstance(uri, str) or not uri:
        return False
    try:
        path = store.path_from_uri(uri).resolve(strict=False)
        path.relative_to(store.root.resolve())
    except (OSError, ValueError):
        return False
    return not path.exists()


_record_points_to_missing_managed_file = _is_record_pointing_to_missing_file


def _validate_manifest_locked(store: PTG2ArtifactStore) -> int:
    if not store.manifest_path.exists():
        return 0
    valid_entries = 0
    try:
        manifest = store.manifest_path.open("r", encoding="utf-8")
    except FileNotFoundError:
        return 0
    with manifest:
        for line_number, line in enumerate(manifest, start=1):
            try:
                payload = json.loads(line)
            except (ValueError, TypeError) as exc:
                raise RuntimeError(
                    "Invalid PTG artifact manifest; cleanup fails closed: "
                    f"{store.manifest_path}:{line_number}"
                ) from exc
            if not isinstance(payload, dict):
                raise RuntimeError(
                    "Invalid PTG artifact manifest record; cleanup fails closed: "
                    f"{store.manifest_path}:{line_number}"
                )
            valid_entries += 1
    return valid_entries


def _compact_manifest_locked(store: PTG2ArtifactStore) -> tuple[int, int, int]:
    if not store.manifest_path.exists():
        return 0, 0, 0
    latest_by_key: dict[tuple[str, ...], tuple[int, dict[str, Any]]] = {}
    valid_entries = 0
    invalid_lines = 0
    try:
        manifest = store.manifest_path.open("r", encoding="utf-8")
    except FileNotFoundError:
        return 0, 0, 0
    with manifest:
        for index, line in enumerate(manifest):
            try:
                manifest_record = json.loads(line)
            except (ValueError, TypeError):
                invalid_lines += 1
                continue
            if not isinstance(manifest_record, dict):
                invalid_lines += 1
                continue
            valid_entries += 1
            latest_by_key[_manifest_record_key(manifest_record)] = (
                index,
                manifest_record,
            )
    compacted_records = [
        manifest_record
        for _index, manifest_record in sorted(
            latest_by_key.values(),
            key=lambda record: record[0],
        )
        if not _record_points_to_missing_managed_file(store, manifest_record)
    ]
    if invalid_lines:
        raise RuntimeError(
            "Invalid PTG artifact manifest; cleanup fails closed before compaction: "
            f"{store.manifest_path}"
        )
    fd, temporary_name = tempfile.mkstemp(
        prefix=".manifest.jsonl.",
        suffix=".tmp",
        dir=store.root,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            for manifest_record in compacted_records:
                fp.write(
                    json.dumps(manifest_record, sort_keys=True, default=str) + "\n"
                )
            fp.flush()
            os.fsync(fp.fileno())
        with suppress(OSError):
            temporary_path.chmod(0o666)
        os.replace(temporary_path, store.manifest_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return valid_entries, len(compacted_records), invalid_lines


def _prune_empty_artifact_directories(store: PTG2ArtifactStore) -> None:
    for kind in sorted(MANAGED_ARTIFACT_KINDS):
        root = store.root / kind
        if not root.exists():
            continue
        for directory in sorted(
            (path for path in root.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            try:
                directory.rmdir()
            except OSError:
                continue


def _prune_atomic_metadata_temps_locked(store: PTG2ArtifactStore) -> None:
    candidates = list(store.root.glob(".manifest.jsonl.*.tmp"))
    candidates.extend(store.leases_dir.glob(".*.json.*.tmp"))
    candidates.extend(_unleased_dir(store).glob(".*.json.*.tmp"))
    for path in candidates:
        try:
            if path.is_file() or path.is_symlink():
                path.unlink()
        except FileNotFoundError:
            continue


def _prune_unleased_metadata_locked(store: PTG2ArtifactStore) -> None:
    directory = _unleased_dir(store)
    if not directory.exists():
        return
    for marker_path in sorted(directory.glob("*.json")):
        payload = _read_json(marker_path)
        try:
            relative_path = _normalized_managed_relative_path(
                payload.get("relative_path") if payload is not None else None
            )
        except ValueError:
            relative_path = ""
        if (
            payload is None
            or payload.get("schema_version") != UNLEASED_SCHEMA_VERSION
            or not relative_path
            or _parse_timestamp(payload.get("unleased_since")) is None
            or _unleased_marker_path(store, relative_path) != marker_path
        ):
            raise RuntimeError(
                f"Invalid PTG artifact retention marker; cleanup fails closed: {marker_path}"
            )
        artifact_path = store.root / relative_path
        if not artifact_path.exists():
            marker_path.unlink(missing_ok=True)


def collect_ptg2_input_artifacts(
    *,
    root: str | Path | None = None,
    execute: bool = False,
    now: datetime.datetime | None = None,
    retention_hours: float | None = None,
    min_age_hours: float | None = None,
    target_bytes: int | None = DEFAULT_TARGET_BYTES,
    max_delete_bytes: int | None = DEFAULT_MAX_DELETE_BYTES,
    max_delete_files: int | None = DEFAULT_MAX_DELETE_FILES,
) -> PTG2InputArtifactGCResult:
    """Plan or execute one race-safe, bounded retained-input collection cycle."""

    store = PTG2ArtifactStore(root)
    current_time = now or _utcnow()
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=datetime.UTC)
    current_time = current_time.astimezone(datetime.UTC)
    retention_value = (
        retention_hours
        if retention_hours is not None
        else _env_float(PTG2_INPUT_ARTIFACT_RETENTION_HOURS_ENV, DEFAULT_RETENTION_HOURS)
    )
    min_age_value = (
        min_age_hours
        if min_age_hours is not None
        else _env_float(PTG2_INPUT_ARTIFACT_MIN_AGE_HOURS_ENV, DEFAULT_MIN_AGE_HOURS)
    )
    if target_bytes == DEFAULT_TARGET_BYTES and os.getenv(PTG2_INPUT_ARTIFACT_TARGET_BYTES_ENV):
        target_bytes = _env_int(PTG2_INPUT_ARTIFACT_TARGET_BYTES_ENV, DEFAULT_TARGET_BYTES)
    if max_delete_bytes == DEFAULT_MAX_DELETE_BYTES and os.getenv(PTG2_INPUT_ARTIFACT_MAX_DELETE_BYTES_ENV):
        max_delete_bytes = _env_int(
            PTG2_INPUT_ARTIFACT_MAX_DELETE_BYTES_ENV,
            DEFAULT_MAX_DELETE_BYTES,
        )
    if max_delete_files == DEFAULT_MAX_DELETE_FILES and os.getenv(PTG2_INPUT_ARTIFACT_MAX_DELETE_FILES_ENV):
        max_delete_files = _env_int(
            PTG2_INPUT_ARTIFACT_MAX_DELETE_FILES_ENV,
            DEFAULT_MAX_DELETE_FILES,
        )
    retention_seconds = max(float(retention_value), 0.0) * 3600
    min_age_seconds = max(float(min_age_value), 0.0) * 3600
    deleted_files: list[Path] = []
    deleted_bytes = 0
    manifest_before = 0
    manifest_after = 0
    manifest_invalid = 0
    newly_unleased_paths: set[Path] = set()

    with store.retention_lock():
        manifest_before = _validate_manifest_locked(store)
        manifest_after = manifest_before
        exact_paths, prefixes, active_ids, stale_leases = _active_lease_references_locked(
            store,
            now=current_time,
        )
        stale_unleased = _stale_unleased_candidates_locked(
            store,
            stale_leases,
            active_exact_paths=exact_paths,
            active_prefixes=prefixes,
        )
        if execute:
            _prune_atomic_metadata_temps_locked(store)
            for stale_lease in stale_leases:
                stale_lease.path.unlink(missing_ok=True)
            for relative_path, unleased_since in sorted(stale_unleased.items()):
                if _mark_unleased_locked(store, relative_path, now=unleased_since):
                    newly_unleased_paths.add(store.root / relative_path)
        stored_artifacts, discovered_unleased = _stored_artifacts_locked(
            store,
            exact_paths=exact_paths,
            prefixes=prefixes,
            persist_unleased=execute,
            observation_time=current_time,
            unleased_since_overrides=stale_unleased,
        )
        newly_unleased_paths.update(discovered_unleased)
        eligible_artifacts, selected_artifacts = _select_artifacts(
            stored_artifacts,
            now_timestamp=current_time.timestamp(),
            retention_seconds=retention_seconds,
            min_age_seconds=min_age_seconds,
            target_bytes=target_bytes,
            max_delete_bytes=max_delete_bytes,
            max_delete_files=max_delete_files,
        )
        # Unknown files first discovered by this collector always receive one
        # complete cycle. Expired leases are different: their explicit expiry
        # is already a durable, heartbeat-backed unleased boundary.
        stale_paths = {store.root / relative_path for relative_path in stale_unleased}
        unknown_discovered_paths = set(discovered_unleased) - stale_paths
        selected_artifacts = [
            artifact
            for artifact in selected_artifacts
            if artifact.path not in unknown_discovered_paths
        ]
        if execute:
            for artifact in stored_artifacts:
                if artifact.protected:
                    _clear_unleased_locked(store, artifact.relative_path)
            for artifact in selected_artifacts:
                try:
                    artifact.path.unlink()
                except FileNotFoundError:
                    continue
                _clear_unleased_locked(store, artifact.relative_path)
                deleted_files.append(artifact.path)
                deleted_bytes += artifact.size
            _prune_empty_artifact_directories(store)
            _prune_unleased_metadata_locked(store)
            manifest_before, manifest_after, manifest_invalid = _compact_manifest_locked(store)

    total_before = sum(artifact.size for artifact in stored_artifacts)
    total_after = total_before - deleted_bytes
    return PTG2InputArtifactGCResult(
        executed=execute,
        root=store.root,
        active_lease_ids=active_ids,
        stale_lease_files=tuple(stale_lease.path for stale_lease in stale_leases),
        protected_files=tuple(
            artifact.path for artifact in stored_artifacts if artifact.protected
        ),
        newly_unleased_files=tuple(sorted(newly_unleased_paths)),
        eligible_files=tuple(
            artifact.path for artifact in eligible_artifacts
        ),
        selected_files=tuple(
            artifact.path for artifact in selected_artifacts
        ),
        deleted_files=tuple(deleted_files),
        total_bytes_before=total_before,
        total_bytes_after=total_after,
        selected_bytes=sum(artifact.size for artifact in selected_artifacts),
        deleted_bytes=deleted_bytes,
        target_bytes=target_bytes,
        manifest_entries_before=manifest_before,
        manifest_entries_after=manifest_after,
        manifest_invalid_lines=manifest_invalid,
    )


def _optional_nonnegative(value: int) -> int | None:
    return None if value < 0 else value


def _print_retention_summary(
    retention_summary: PTG2InputArtifactGCResult,
) -> None:
    """Print stable key-value metrics for one garbage-collection cycle."""

    print(f"artifact_root={retention_summary.root}")
    print(f"cleanup_executed={str(retention_summary.executed).lower()}")
    print(f"active_leases={len(retention_summary.active_lease_ids)}")
    print(f"stale_leases={len(retention_summary.stale_lease_files)}")
    print(f"protected_files={len(retention_summary.protected_files)}")
    print(f"newly_unleased_files={len(retention_summary.newly_unleased_files)}")
    print(f"eligible_files={len(retention_summary.eligible_files)}")
    print(f"selected_files={len(retention_summary.selected_files)}")
    print(f"selected_bytes={retention_summary.selected_bytes}")
    print(f"deleted_files={len(retention_summary.deleted_files)}")
    print(f"deleted_bytes={retention_summary.deleted_bytes}")
    print(f"total_bytes_before={retention_summary.total_bytes_before}")
    print(f"total_bytes_after={retention_summary.total_bytes_after}")
    print(f"over_target_bytes={retention_summary.over_target_bytes}")
    print(
        f"manifest_entries_before={retention_summary.manifest_entries_before}"
    )
    print(f"manifest_entries_after={retention_summary.manifest_entries_after}")
    print(f"manifest_invalid_lines={retention_summary.manifest_invalid_lines}")


def _run_cli() -> None:
    """Run retained-input garbage collection and print cycle statistics."""

    parser = argparse.ArgumentParser(
        description=(
            "Collect unleased PTG raw, logical, and resumable partial input artifacts "
            "and compact their manifest."
        )
    )
    parser.add_argument("--root", default=None, help="Artifact root; defaults to HLTHPRT_PTG2_ARTIFACT_DIR.")
    parser.add_argument("--execute", action="store_true", help="Delete selected files; default is dry-run.")
    parser.add_argument(
        "--retention-hours",
        type=float,
        default=_env_float(PTG2_INPUT_ARTIFACT_RETENTION_HOURS_ENV, DEFAULT_RETENTION_HOURS),
    )
    parser.add_argument(
        "--min-age-hours",
        type=float,
        default=_env_float(PTG2_INPUT_ARTIFACT_MIN_AGE_HOURS_ENV, DEFAULT_MIN_AGE_HOURS),
    )
    parser.add_argument(
        "--target-bytes",
        type=int,
        default=_env_int(PTG2_INPUT_ARTIFACT_TARGET_BYTES_ENV, DEFAULT_TARGET_BYTES),
        help="Raw+logical target; use -1 to disable capacity pressure.",
    )
    parser.add_argument(
        "--max-delete-bytes",
        type=int,
        default=_env_int(PTG2_INPUT_ARTIFACT_MAX_DELETE_BYTES_ENV, DEFAULT_MAX_DELETE_BYTES),
        help="Per-cycle deletion bound; use -1 for no byte bound.",
    )
    parser.add_argument(
        "--max-delete-files",
        type=int,
        default=_env_int(PTG2_INPUT_ARTIFACT_MAX_DELETE_FILES_ENV, DEFAULT_MAX_DELETE_FILES),
        help="Per-cycle deletion bound; use -1 for no file bound.",
    )
    args = parser.parse_args()
    retention_summary = collect_ptg2_input_artifacts(
        root=args.root,
        execute=args.execute,
        retention_hours=args.retention_hours,
        min_age_hours=args.min_age_hours,
        target_bytes=_optional_nonnegative(args.target_bytes),
        max_delete_bytes=_optional_nonnegative(args.max_delete_bytes),
        max_delete_files=_optional_nonnegative(args.max_delete_files),
    )
    _print_retention_summary(retention_summary)


main = _run_cli


if __name__ == "__main__":
    _run_cli()
