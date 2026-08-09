# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adversarial JSON boundaries for reviewed subset activation manifests."""

from __future__ import annotations

import pytest

from process import provider_directory_fhir_subset_activation as activation


def _duplicate_top_level_status() -> str:
    return (
        '{"schema_version":1,"importer":"provider-directory-fhir",'
        '"operation":"reviewed-subset-source-state-sync",'
        '"desired_candidate_status":"pending_two_matching_reviewed_'
        'subset_acquisitions","desired_candidate_status":'
        '"verified_two_matching_reviewed_subset_acquisitions",'
        '"evidence":null}'
    )


def _duplicate_evidence_digest() -> str:
    return (
        '{"schema_version":1,"importer":"provider-directory-fhir",'
        '"operation":"reviewed-subset-source-state-sync",'
        '"desired_candidate_status":"verified_two_matching_reviewed_'
        'subset_acquisitions","evidence":{"source_contract_sha256":"'
        + "1" * 64
        + '","source_contract_sha256":"'
        + "2" * 64
        + '","cutoff":"2026-08-09T00:00:00.000000Z",'
        '"verification_source_scope_sha256":"'
        + "3" * 64
        + '","completion_proof_sha256":"'
        + "4" * 64
        + '"}}'
    )


@pytest.mark.parametrize(
    "raw_manifest",
    (_duplicate_top_level_status(), _duplicate_evidence_digest()),
)
def test_manifest_rejects_duplicate_members(tmp_path, raw_manifest):
    """Reject duplicate authorization fields at every JSON object depth."""

    manifest_path = tmp_path / "activation.json"
    manifest_path.write_text(raw_manifest, encoding="utf-8")

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        activation.reviewed_subset_activation_manifest(manifest_path)

    assert error.value.code == "manifest"
