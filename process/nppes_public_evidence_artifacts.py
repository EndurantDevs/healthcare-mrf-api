# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable content-addressed storage helpers for NPPES acquisition."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import stat
import tempfile

from process.nppes_public_evidence_archive import archive_error
from process.ptg_parts.artifacts import PTG2ArtifactStore, sha256_file


_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_INDEX_CONTRACT = "healthporta.nppes-retained-url-index.v1"
_RETAINED_SUFFIXES = frozenset({".html", ".zip"})


@dataclass(frozen=True, slots=True, repr=False)
class RetainedHttpArtifact:
    """One exact retained response body and bounded HTTP observation."""

    path: Path
    source_url: str
    final_url: str
    sha256: str
    byte_count: int
    etag: str | None
    last_modified: str | None
    acquired_at: str


async def _invoke_cancel(callback) -> None:
    if callback is None:
        return
    result = callback()
    if inspect.isawaitable(result):
        await result


def resolve_nppes_artifact_root() -> Path:
    """Resolve the required durable artifact root and reject temporary roots."""

    try:
        configured = os.getenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_ARTIFACT_ROOT", "")
        if not configured or "\x00" in configured:
            raise archive_error()
        root = Path(configured)
        if not root.is_absolute() or root == Path(root.anchor):
            raise archive_error()
        temporary_root = Path(tempfile.gettempdir()).resolve()
        resolved_parent = root.parent.resolve()
        if resolved_parent == temporary_root or temporary_root in resolved_parent.parents:
            raise archive_error()
        if root.exists() and root.is_symlink():
            raise archive_error()
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        resolved = root.resolve()
        root_stat = resolved.stat()
        if (
            resolved != root
            or not resolved.is_dir()
            or root_stat.st_uid != os.geteuid()
            or root_stat.st_mode & 0o022
        ):
            raise archive_error()
    except Exception:
        normalized_error = archive_error()
    else:
        return resolved
    raise normalized_error


def nppes_artifact_store() -> PTG2ArtifactStore:
    """Return the source-specific durable artifact store."""

    return PTG2ArtifactStore(resolve_nppes_artifact_root())


def canonical_cms_url(value: object) -> str:
    """Require the one canonical HTTPS CMS NPPES URL shape."""

    from urllib.parse import urlsplit

    try:
        if type(value) is not str:
            raise archive_error()
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "download.cms.gov"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port not in {None, 443}
            or not parsed.path.startswith("/nppes/")
            or parsed.query
            or parsed.fragment
        ):
            raise archive_error()
        canonical = f"https://download.cms.gov{parsed.path}"
        if value != canonical:
            raise archive_error()
    except Exception:
        normalized_error = archive_error()
    else:
        return canonical
    raise normalized_error


def index_path(store: PTG2ArtifactStore, source_url: str) -> Path:
    """Return the bounded latest-observation index path for one source URL."""

    root = _validated_store_root(store)
    digest = hashlib.sha256(source_url.encode("ascii")).hexdigest()
    return root / "nppes-url-index" / f"{digest}.json"


def read_index_payload(path: Path) -> dict[str, object] | None:
    """Read one bounded URL-index object without following a symlink."""

    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024:
        raise archive_error()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise archive_error() from None
    if type(payload) is not dict:
        raise archive_error()
    return payload


def retained_path(
    store: PTG2ArtifactStore,
    digest: str,
    suffix: str,
) -> Path:
    """Return the permanent content-addressed path for verified source bytes."""

    root = _validated_store_root(store)
    if _SHA256_RE.fullmatch(digest) is None or suffix not in _RETAINED_SUFFIXES:
        raise archive_error()
    return (
        root
        / "nppes-evidence-retained"
        / digest[:2]
        / digest[2:4]
        / f"{digest}{suffix}"
    )


def retain_verified_inode(
    store: PTG2ArtifactStore,
    source_path: Path,
    digest: str,
    suffix: str,
) -> Path:
    """Hard-link verified bytes outside generic expiring artifact classes."""

    try:
        retained = _retain_verified_inode(store, source_path, digest, suffix)
    except Exception:
        normalized_error = archive_error()
    else:
        return retained
    raise normalized_error


def _remove_created_inode(final_path: Path, linked_stat: os.stat_result) -> None:
    with suppress(OSError):
        current_stat = final_path.stat(follow_symlinks=False)
        if (
            current_stat.st_dev == linked_stat.st_dev
            and current_stat.st_ino == linked_stat.st_ino
        ):
            final_path.unlink()


def _fsync_directory(path: Path) -> None:
    directory = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _retain_verified_inode(
    store: PTG2ArtifactStore,
    source_path: Path,
    digest: str,
    suffix: str,
) -> Path:
    final_path = retained_path(store, digest, suffix)
    final_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    source_stat = source_path.stat(follow_symlinks=False)
    if (
        final_path.parent.is_symlink()
        or source_path.is_symlink()
        or not stat.S_ISREG(source_stat.st_mode)
    ):
        raise archive_error()
    final_path.parent.chmod(0o700)
    try:
        os.link(source_path, final_path, follow_symlinks=False)
    except FileExistsError:
        if final_path.is_symlink() or not final_path.is_file():
            raise archive_error() from None
        retained_digest, retained_bytes = sha256_file(final_path)
        if retained_digest != digest or retained_bytes <= 0:
            raise archive_error() from None
        return final_path
    linked_stat = final_path.stat(follow_symlinks=False)
    try:
        if (
            source_stat.st_dev != linked_stat.st_dev
            or source_stat.st_ino != linked_stat.st_ino
            or source_stat.st_size != linked_stat.st_size
        ):
            raise archive_error()
        retained_digest, retained_bytes = sha256_file(final_path)
        if retained_digest != digest or retained_bytes != source_stat.st_size:
            raise archive_error()
    except Exception:
        _remove_created_inode(final_path, linked_stat)
        raise
    _fsync_directory(final_path.parent)
    return final_path


def _validated_index_identity(
    index_values_by_name: dict[str, object],
    source_url: str,
    suffix: str,
    max_bytes: int,
) -> tuple[str, int]:
    digest = index_values_by_name.get("sha256")
    byte_count = index_values_by_name.get("byte_count")
    if (
        index_values_by_name.get("contract") != _INDEX_CONTRACT
        or index_values_by_name.get("source_url") != source_url
        or index_values_by_name.get("suffix") != suffix
        or type(digest) is not str
        or _SHA256_RE.fullmatch(digest) is None
        or type(byte_count) is not int
        or not 1 <= byte_count <= max_bytes
    ):
        raise archive_error()
    return digest, byte_count


async def _resolved_retained_cache_path(
    store: PTG2ArtifactStore,
    digest: str,
    byte_count: int,
    suffix: str,
) -> Path | None:
    expected_path = retained_path(store, digest, suffix)
    if not expected_path.exists():
        managed_path = store.artifact_path(digest, kind="raw")
        if managed_path.is_file() and not managed_path.is_symlink():
            managed_digest, managed_bytes = await asyncio.to_thread(
                sha256_file,
                managed_path,
            )
            if managed_digest == digest and managed_bytes == byte_count:
                expected_path = await asyncio.to_thread(
                    retain_verified_inode,
                    store,
                    managed_path,
                    digest,
                    suffix,
                )
    if expected_path.is_symlink():
        raise archive_error()
    if not expected_path.is_file():
        return None
    return expected_path


def _validated_http_observation(
    index_values_by_name: dict[str, object],
    source_url: str,
) -> tuple[str, str | None, str | None, str]:
    etag = index_values_by_name.get("etag")
    last_modified = index_values_by_name.get("last_modified")
    final_url = index_values_by_name.get("final_url")
    acquired_at = index_values_by_name.get("acquired_at")
    if type(final_url) is not str or type(acquired_at) is not str:
        raise archive_error()
    etag = _validated_metadata_value(etag)
    last_modified = _validated_metadata_value(last_modified)
    try:
        datetime.strptime(acquired_at, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise archive_error() from None
    if canonical_cms_url(final_url) != source_url:
        raise archive_error()
    return final_url, etag, last_modified, acquired_at


def _validated_metadata_value(value: object) -> str | None:
    if value is None:
        return None
    if type(value) is not str:
        raise archive_error()
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        raise archive_error() from None
    if len(encoded) > 4096 or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise archive_error()
    return value


def _validated_store_root(store: object) -> Path:
    if type(store) is not PTG2ArtifactStore:
        raise archive_error()
    root = store.root
    if root.is_symlink() or not root.is_dir():
        raise archive_error()
    root_stat = root.stat()
    if root_stat.st_uid != os.geteuid() or root_stat.st_mode & 0o022:
        raise archive_error()
    return root


async def verified_cached_artifact(
    store: PTG2ArtifactStore,
    source_url: str,
    suffix: str,
    max_bytes: int,
    cancel_check=None,
) -> RetainedHttpArtifact | None:
    """Rehash and return one exact durable URL-index target when present."""

    await _invoke_cancel(cancel_check)
    index_values_by_name = read_index_payload(index_path(store, source_url))
    if index_values_by_name is None:
        return None
    digest, byte_count = _validated_index_identity(
        index_values_by_name,
        source_url,
        suffix,
        max_bytes,
    )
    expected_path = await _resolved_retained_cache_path(
        store,
        digest,
        byte_count,
        suffix,
    )
    if expected_path is None:
        return None
    actual_digest, actual_bytes = await asyncio.to_thread(sha256_file, expected_path)
    await _invoke_cancel(cancel_check)
    if actual_digest != digest or actual_bytes != byte_count:
        return None
    final_url, etag, last_modified, acquired_at = _validated_http_observation(
        index_values_by_name,
        source_url,
    )
    return RetainedHttpArtifact(
        path=expected_path,
        source_url=source_url,
        final_url=final_url,
        sha256=digest,
        byte_count=byte_count,
        etag=etag,
        last_modified=last_modified,
        acquired_at=acquired_at,
    )


def atomic_write_index(path: Path, payload: dict[str, object]) -> None:
    """Atomically replace one bounded URL-observation index."""

    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.is_symlink():
        raise archive_error()
    path.parent.chmod(0o700)
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("ascii")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = (
    "RetainedHttpArtifact",
    "atomic_write_index",
    "canonical_cms_url",
    "index_path",
    "nppes_artifact_store",
    "resolve_nppes_artifact_root",
    "retain_verified_inode",
    "retained_path",
    "verified_cached_artifact",
)
