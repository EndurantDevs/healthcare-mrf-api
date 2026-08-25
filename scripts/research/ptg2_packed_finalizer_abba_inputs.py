"""Fail-closed admission for externally supplied packed-finalizer inputs."""

from __future__ import annotations

from pathlib import Path

from scripts.research.ptg2_packed_finalizer_abba_contract import (
    BenchmarkArtifacts,
)


def load_representative_artifacts(
    manifest_path: Path,
    source_receipt_path: Path,
) -> BenchmarkArtifacts:
    """Reject self-attested inputs until a trusted compiler contract exists."""

    raise ValueError(
        "representative ABBA inputs require an externally authenticated "
        "compiler receipt contract"
    )


__all__ = ("load_representative_artifacts",)
