# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Filesystem fixtures for retained-reader unit and PostgreSQL proofs."""

from __future__ import annotations

import hashlib
from pathlib import Path

from process.provider_directory_retained_blob_store import (
    retained_artifact_blob_components,
)


def write_retained_artifact_blob(
    artifact_root: Path,
    artifact_bytes: bytes,
) -> tuple[str, Path]:
    """Write exact test bytes through the canonical shared path helper."""

    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    blob_path = artifact_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    blob_path.parent.mkdir(parents=True)
    blob_path.write_bytes(artifact_bytes)
    return artifact_sha256, blob_path
