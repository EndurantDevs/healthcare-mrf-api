# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Descriptor-checked retained storage operations for source artifacts."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

from process.formulary_fhir.source_artifact_contract import VerifiedSourceArtifact
from process.provider_directory_retained_blob_producer import (
    install_retained_artifact_blob,
)
from process.provider_directory_retained_blob_store import (
    open_retained_artifact_blob,
)


def verify_retained_source_artifact(
    artifact_sha256: str,
    artifact_byte_count: int,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Read and rehash one retained blob through its descriptor-bound reader."""

    with open_retained_artifact_blob(
        artifact_sha256,
        artifact_byte_count,
    ) as retained_reader:
        while retained_reader.read(1024 * 1024):
            if cancel_check is not None:
                cancel_check()


def install_and_verify_source_artifact(
    source_path: Path,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> None:
    """Install one blob and prove the retained bytes before ledger binding."""

    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=artifact_byte_count,
    )
    verify_retained_source_artifact(
        artifact_sha256,
        artifact_byte_count,
    )


@contextmanager
def open_verified_source_artifact(
    artifact: VerifiedSourceArtifact,
) -> Iterator[Any]:
    """Open one verified artifact for a complete sequential parse."""

    if type(artifact) is not VerifiedSourceArtifact:
        raise ValueError("FHIR formulary verified source artifact is invalid")
    with open_retained_artifact_blob(
        artifact.artifact_sha256,
        artifact.artifact_byte_count,
    ) as retained_reader:
        yield retained_reader


__all__ = (
    "install_and_verify_source_artifact",
    "open_verified_source_artifact",
    "verify_retained_source_artifact",
)
