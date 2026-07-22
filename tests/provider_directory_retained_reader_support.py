# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Filesystem fixtures for retained-reader unit and PostgreSQL proofs."""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

from process.provider_directory_retained_blob_producer import (
    install_retained_artifact_blob,
)
from process.provider_directory_retained_blob_store import (
    retained_artifact_blob_components,
)


def write_retained_artifact_blob(
    artifact_root: Path,
    artifact_bytes: bytes,
) -> tuple[str, Path]:
    """Write exact test bytes through the canonical shared path helper."""

    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = artifact_root / f".producer-{uuid.uuid4().hex}.partial"
    source_path.write_bytes(artifact_bytes)
    try:
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    finally:
        source_path.unlink(missing_ok=True)
    blob_path = artifact_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    return artifact_sha256, blob_path
