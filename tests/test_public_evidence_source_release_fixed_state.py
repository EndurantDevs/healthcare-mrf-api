# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact fixed-state rejection for publication-disabled source releases."""

from __future__ import annotations

import pytest

from public_evidence import source_release_contract as release
from tests.public_evidence_source_release_support import release_input

FIXED_STRINGS = (
    ("contract", release.PUBLIC_EVIDENCE_SOURCE_RELEASE_CONTRACT),
    ("foundation_scope", release.PUBLIC_EVIDENCE_FOUNDATION_SCOPE),
    ("lifecycle_state", "verified_disabled"),
    ("serving_authority", "none"),
    ("current_pointer_authority", "none"),
)
REQUIRED_TRUE_STATE = tuple(
    "artifact_bytes_verified public_access_verified "
    "processing_retention_rights_verified semantic_limits_verified "
    "completeness_attestation_verified".split()
)
REQUIRED_FALSE_STATE = tuple(
    "legal_ownership_claimed exact_rate_site_claimed whole_source_complete "
    "redistribution_enabled export_enabled publication_enabled replacement_enabled "
    "deletion_enabled retirement_enabled supersession_enabled".split()
)


class EquivalentString(str):
    pass


class EqualityCompatible:
    def __eq__(self, _other: object) -> bool:
        return True

    def __ne__(self, _other: object) -> bool:
        return False


def _tampered_descriptor(field_name: str, value: object) -> object:
    descriptor = release.build_public_evidence_source_release(release_input())
    object.__setattr__(descriptor, field_name, value)
    return descriptor


@pytest.mark.parametrize(("field_name", "expected"), FIXED_STRINGS)
def test_revalidation_requires_exact_builtin_fixed_strings(
    field_name: str,
    expected: str,
) -> None:
    replacements = (
        "wrong-fixed-state",
        EquivalentString(expected),
        EqualityCompatible(),
    )
    for replacement in replacements:
        descriptor = _tampered_descriptor(field_name, replacement)
        with pytest.raises(release.PublicEvidenceSourceReleaseError):
            release.validate_public_evidence_source_release(descriptor)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    tuple((field_name, 1) for field_name in REQUIRED_TRUE_STATE)
    + tuple((field_name, 0) for field_name in REQUIRED_FALSE_STATE),
)
def test_revalidation_requires_exact_builtin_boolean_state(
    field_name: str,
    replacement: int,
) -> None:
    descriptor = _tampered_descriptor(field_name, replacement)
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(descriptor)


def test_fixed_state_rejection_precedes_canonical_reconstruction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor = _tampered_descriptor("publication_enabled", 0)
    monkeypatch.setattr(
        release,
        "_normalized_release",
        lambda _raw: pytest.fail("canonical reconstruction was reached"),
    )
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(descriptor)
