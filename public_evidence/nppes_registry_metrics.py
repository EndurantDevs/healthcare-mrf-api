# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Value-safe control metrics for one NPPES registry replay manifest."""

from __future__ import annotations

from typing import Mapping

from public_evidence.nppes_registry_replay_contract import (
    validate_nppes_registry_manifest,
)


def nppes_manifest_metrics(manifest: object) -> Mapping[str, object]:
    """Return counts and opaque release identities without source row values."""

    fixed = validate_nppes_registry_manifest(manifest)
    return {
        "archive_name": fixed.identity.archive_name,
        "artifact_sha256": fixed.identity.artifact_sha256,
        "artifact_byte_count": fixed.identity.artifact_byte_count,
        "snapshot_at": fixed.identity.snapshot_at,
        "source_release_ref": fixed.release.source_release_ref,
        "source_record_count": fixed.source_record_count,
        "projected_record_count": fixed.projected_record_count,
        "excluded_record_count": fixed.excluded_record_count,
        "exclusion_counts": {
            reason: record_count for reason, record_count in fixed.exclusion_counts
        },
        "evidence_root_sha256": fixed.evidence_root_sha256,
        "manifest_sha256": fixed.manifest_sha256,
    }
