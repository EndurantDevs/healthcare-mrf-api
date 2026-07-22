# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compact Profile evidence carried by each exact physical FHIR winner."""

INLINE_PROFILE_EVIDENCE_CONTRACT_ID = (
    "healthporta.provider-directory.inline-profile-evidence.v1"
)

INLINE_PROFILE_EVIDENCE_COLUMNS = (
    "active",
    "effective_start",
    "effective_end",
    "observed_at",
    "profile_evidence_json",
)


__all__ = [
    "INLINE_PROFILE_EVIDENCE_COLUMNS",
    "INLINE_PROFILE_EVIDENCE_CONTRACT_ID",
]
