# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared filesystem-lock construction for retained UHC artifacts."""

from pathlib import Path

from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.uhc_retained_types import _validate_sha256


def retained_source_lock(
    output_directory: str | Path,
    artifact_sha256: str,
):
    """Return the raw-content lock shared by every range-layout version."""

    _validate_sha256(artifact_sha256)
    store = PTG2ArtifactStore(Path(output_directory).resolve())
    return store.named_lock(
        "uhc-retained-source",
        artifact_sha256,
    )
