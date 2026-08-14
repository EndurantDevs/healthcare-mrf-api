# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical metadata binding for one private UHC drug spool."""

from __future__ import annotations

from typing import Any

from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence


SPOOL_EVIDENCE_FIELDS = frozenset(
    {
        "artifact_set_sha256",
        "duplicate_count",
        "file_count",
        "max_last_updated_at",
        "medication_membership_count",
        "plan_count",
        "raw_plan_entry_count",
        "raw_record_count",
        "source_file_set_sha256",
        "source_id",
        "superseded_count",
    }
)
PARTIAL_SPOOL_EVIDENCE_FIELDS = SPOOL_EVIDENCE_FIELDS | {
    "excluded_file_count",
    "expected_file_count",
}
SPOOL_ARTIFACT_PROOF_FIELDS = frozenset(
    {
        "artifact_sha256",
        "catalog_modified_at",
        "family",
        "file_name",
        "source_file_id",
    }
)


def artifact_proof_rows(
    artifact_set: VerifiedSourceArtifactSet,
) -> tuple[dict[str, Any], ...]:
    """Return the exact artifact fields referenced by membership provenance."""

    if type(artifact_set) is not VerifiedSourceArtifactSet:
        raise ValueError("UHC drug artifact set is invalid")
    return tuple(
        {
            "artifact_sha256": artifact.artifact_sha256,
            "catalog_modified_at": artifact.identity.catalog_modified_at,
            "family": artifact.identity.family,
            "file_name": artifact.identity.file_name,
            "source_file_id": artifact.identity.source_file_id,
        }
        for artifact in artifact_set.artifacts
    )


def spool_evidence_payload(
    evidence: UHCDrugSpoolEvidence,
) -> dict[str, Any]:
    """Serialize every audit counter independently retained in the spool."""

    if type(evidence) is not UHCDrugSpoolEvidence:
        raise ValueError("UHC drug spool evidence is invalid")
    payload = {
        "artifact_set_sha256": evidence.artifact_set_sha256,
        "duplicate_count": evidence.duplicate_count,
        "file_count": evidence.file_count,
        "max_last_updated_at": evidence.max_last_updated_at.isoformat(),
        "medication_membership_count": evidence.medication_membership_count,
        "plan_count": evidence.plan_count,
        "raw_plan_entry_count": evidence.raw_plan_entry_count,
        "raw_record_count": evidence.raw_record_count,
        "source_file_set_sha256": evidence.source_file_set_sha256,
        "source_id": evidence.source_id,
        "superseded_count": evidence.superseded_count,
    }
    if not evidence.is_coverage_complete:
        payload.update(
            {
                "excluded_file_count": evidence.excluded_file_count,
                "expected_file_count": evidence.expected_file_count,
            }
        )
    return payload


__all__ = (
    "SPOOL_ARTIFACT_PROOF_FIELDS",
    "SPOOL_EVIDENCE_FIELDS",
    "PARTIAL_SPOOL_EVIDENCE_FIELDS",
    "artifact_proof_rows",
    "spool_evidence_payload",
)
