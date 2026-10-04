# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain exact CMS Doctors distribution bytes before parsing them."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path


def _artifact_root() -> Path:
    configured = os.getenv("HLTHPRT_CMS_DOCTORS_ARTIFACT_ROOT", "").strip()
    if not configured:
        shared_root = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", "").strip()
        configured = os.path.join(shared_root, "cms-doctors") if shared_root else ""
    if not configured or not os.path.isabs(configured):
        raise RuntimeError("cms_doctors_artifact_root_required")
    root = Path(os.path.realpath(configured))
    temporary_roots = (
        "/tmp",
        "/private/tmp",
        "/var/tmp",
        "/private/var/tmp",
        "/var/folders",
        "/private/var/folders",
        "/dev/shm",
        "/run",
    )
    if root == Path(root.anchor) or any(
        str(root) == temporary or str(root).startswith(temporary + "/") for temporary in temporary_roots
    ):
        raise RuntimeError("cms_doctors_artifact_root_not_durable")
    return root


def validate_doctors_artifact_root() -> None:
    """Fail before downloading when the required artifact root is not writable."""
    root = _artifact_root()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(dir=root, prefix=".cms-doctors-probe-") as probe:
        probe.write(b"1")
        probe.flush()


def retain_doctors_artifact(source_path: str, source_url: str) -> dict:
    """Install a content-addressed source artifact on an explicitly durable root."""
    root = _artifact_root()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    suffix = ".zip" if str(source_path).lower().endswith(".zip") else ".csv"
    digest = hashlib.sha256()
    size = 0
    temporary_path = None
    try:
        with (
            open(source_path, "rb") as source_file,
            tempfile.NamedTemporaryFile(dir=root, prefix=".cms-doctors-", delete=False) as destination,
        ):
            temporary_path = Path(destination.name)
            while chunk := source_file.read(8 * 1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
                destination.write(chunk)
            destination.flush()
            os.fsync(destination.fileno())
        if size == 0:
            raise RuntimeError("cms_doctors_source_empty")
        artifact = root / f"{digest.hexdigest()}{suffix}"
        try:
            os.link(temporary_path, artifact)
        except FileExistsError:
            with artifact.open("rb") as retained:
                if hashlib.file_digest(retained, "sha256").hexdigest() != digest.hexdigest():
                    raise RuntimeError("cms_doctors_artifact_content_conflict")
        directory_fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return {
            "source_url": source_url,
            "content_sha256": digest.hexdigest(),
            "content_bytes": size,
            "file_name": artifact.name,
        }
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def verify_doctors_artifact(receipt: dict) -> Path:
    """Reject publication if the retained source bytes no longer match their receipt."""
    if not isinstance(receipt, dict):
        raise RuntimeError("cms_doctors_artifact_receipt_invalid")
    digest = receipt.get("content_sha256")
    file_name = receipt.get("file_name")
    size = receipt.get("content_bytes")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(ch not in "0123456789abcdef" for ch in digest)
        or not isinstance(file_name, str)
        or file_name not in {f"{digest}.csv", f"{digest}.zip"}
        or type(size) is not int
        or size <= 0
    ):
        raise RuntimeError("cms_doctors_artifact_receipt_invalid")
    path = _artifact_root() / file_name
    try:
        with path.open("rb") as retained:
            if (
                os.fstat(retained.fileno()).st_size != size
                or hashlib.file_digest(retained, "sha256").hexdigest() != digest
            ):
                raise RuntimeError("cms_doctors_artifact_changed")
    except FileNotFoundError as error:
        raise RuntimeError("cms_doctors_artifact_missing") from error
    return path
