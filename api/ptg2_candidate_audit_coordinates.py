# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate sealed candidate audit coordinates through shared serving indexes."""

from __future__ import annotations

from typing import Mapping, Sequence

from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_candidate_audit_projection import CandidatePriceData
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def validate_persisted_audit_graph_scope(
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    code_index: CandidateCodeIndex,
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
) -> None:
    """Require every sealed code/provider/NPI coordinate in the same indexes."""

    for occurrence in persisted_audit_occurrences:
        if occurrence.code_key not in code_index.by_key:
            raise PTG2ManifestArtifactError(
                "PTG2 candidate persisted audit code is missing from the sealed layout"
            )
        if occurrence.provider_set_key not in provider_set_keys_by_npi.get(
            occurrence.npi,
            (),
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 candidate persisted audit provider is missing from its NPI graph"
            )


def validate_persisted_audit_price_scope(
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence],
    price_data: CandidatePriceData,
) -> None:
    """Require exact forward and membership coordinates for every sealed row."""

    for occurrence in persisted_audit_occurrences:
        occurrence_key = (
            occurrence.code_key,
            occurrence.provider_set_key,
            occurrence.source_artifact_key,
        )
        if occurrence.price_key not in price_data.price_keys_by_occurrence.get(
            occurrence_key,
            (),
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 candidate persisted audit price is missing from its forward occurrence"
            )
        atom_keys = price_data.atom_keys_by_price_key.get(occurrence.price_key)
        if (
            atom_keys is None
            or occurrence.atom_ordinal >= len(atom_keys)
            or atom_keys[occurrence.atom_ordinal] != occurrence.atom_key
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 candidate persisted audit atom disagrees with its price membership"
            )


__all__ = [
    "validate_persisted_audit_graph_scope",
    "validate_persisted_audit_price_scope",
]
