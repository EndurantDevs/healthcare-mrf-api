# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original header fields select one profile; invalid combinations never fall back."""

import pytest

from process import network_fhir_source_witness_contract as profiles
from process.network_fhir_source_witness_publication import publication_profile


def _header(**changes):
    return {
        "hash_contract": None,
        "hash_contract_type": None,
        "proof_absent": True,
        "uhc_proof_absent": True,
        "completion_proof_required_version": None,
        "completion_absent": True,
        "candidate_storage_json": None,
        "admission_absent": True,
        "proof_sha256": None,
        "semantic_as_of_absent": True,
        "semantic_as_of": None,
    } | changes


def _admitted(*, uhc=False, completion=False, **changes):
    return (
        _header(
            hash_contract="transport_neutral_v2",
            hash_contract_type="string",
            proof_absent=uhc,
            uhc_proof_absent=not uhc,
            admission_absent=False,
            content_proof_admission_version=1,
            content_proof_admission_kind="uhc_canonical" if uhc else "generic",
            publication_metadata_sha256="a" * 64,
            proof_sha256="b" * 64,
            completion_proof_required_version=3 if completion else None,
            completion_absent=not completion,
            completion_proof_sha256="c" * 64 if completion else None,
        )
        | changes
    )


@pytest.mark.parametrize(
    "header,expected",
    [
        (_header(), profiles.OBSERVED_LEGACY_PROFILE),
        (_header(hash_contract="transport_bound_v1", hash_contract_type="string"), profiles.OBSERVED_LEGACY_PROFILE),
        (_header(proof_absent=False, proof_sha256="b" * 64), profiles.OBSERVED_PROOF_PROFILE),
        (
            _header(
                hash_contract="semantic_content_v3",
                hash_contract_type="string",
                semantic_as_of_absent=False,
                semantic_as_of="2026-01-01",
            ),
            profiles.OBSERVED_SEMANTIC_PROFILE,
        ),
        (_admitted(hash_contract=None, hash_contract_type=None), profiles.ADMITTED_LEGACY_PROFILE),
        (_admitted(hash_contract="transport_bound_v1"), profiles.ADMITTED_LEGACY_PROFILE),
        (_admitted(), profiles.ADMITTED_TRANSPORT_PROFILE),
        (_admitted(uhc=True), profiles.ADMITTED_UHC_PROFILE),
        (_admitted(completion=True), profiles.ADMITTED_TRANSPORT_PROFILE),
        (_header(hash_contract="semantic_content_v4", hash_contract_type="string"), profiles.SEMANTIC_PROFILE),
    ],
)
def test_original_header_routes_all_supported_profiles_without_rewriting(header, expected):
    original_header_by_field = dict(header)
    assert publication_profile(header) == expected
    assert header == original_header_by_field


@pytest.mark.parametrize(
    "header,changes",
    [
        (_header(), {"hash_contract": "unknown", "hash_contract_type": "string"}),
        (_header(), {"hash_contract_type": "string"}),
        (_header(), {"hash_contract": "transport_bound_v1"}),
        (_header(), {"hash_contract": 3, "hash_contract_type": "string"}),
        (_header(), {"proof_absent": False, "uhc_proof_absent": False}),
        (_header(), {"proof_absent": False, "proof_sha256": None}),
        (_header(), {"proof_sha256": "b" * 64}),
        (_header(), {"uhc_proof_absent": False}),
        (_header(), {"candidate_storage_json": {}}),
        (_header(), {"completion_absent": False}),
        (_header(), {"semantic_as_of_absent": False, "semantic_as_of": "2026-01-01"}),
        (
            _header(hash_contract="semantic_content_v3", hash_contract_type="string"),
            {"semantic_as_of_absent": False, "semantic_as_of": 2026},
        ),
        (
            _header(
                hash_contract="semantic_content_v3",
                hash_contract_type="string",
                semantic_as_of_absent=False,
                semantic_as_of="2026-01-01",
            ),
            {"proof_absent": False, "proof_sha256": "b" * 64},
        ),
        (_admitted(), {"content_proof_admission_version": True}),
        (_admitted(), {"publication_metadata_sha256": "invalid"}),
        (_admitted(), {"proof_sha256": None}),
        (_admitted(), {"content_proof_admission_kind": "unknown"}),
        (_admitted(), {"content_proof_admission_kind": "uhc_canonical"}),
        (_admitted(), {"proof_absent": True}),
        (_admitted(), {"admission_absent": True}),
        (_admitted(uhc=True), {"proof_absent": False}),
        (_admitted(uhc=True), {"uhc_proof_absent": True}),
        (_admitted(uhc=True), {"content_proof_admission_kind": "generic"}),
        (_admitted(uhc=True), {"hash_contract": "transport_bound_v1"}),
        (_admitted(completion=True), {"completion_absent": True}),
        (_admitted(completion=True), {"completion_proof_sha256": None}),
        (_admitted(completion=True), {"completion_proof_sha256": "g" * 64}),
        (_admitted(completion=True), {"hash_contract": "transport_bound_v1"}),
        (_admitted(completion=True), {"hash_contract": "semantic_content_v4"}),
        (_admitted(completion=True), {"content_proof_admission_kind": "uhc_canonical"}),
        (_admitted(completion=True), {"admission_absent": True}),
    ],
)
def test_invalid_header_refuses_instead_of_falling_back(header, changes):
    with pytest.raises(profiles.SourceWitnessContractError):
        publication_profile(header | changes)


@pytest.mark.parametrize("version", [True, False, "3", 2, 4, 3.0])
def test_completion_requires_the_exact_integer_version(version):
    with pytest.raises(profiles.SourceWitnessContractError):
        publication_profile(_admitted(completion=True, completion_proof_required_version=version))


def test_completion_requires_a_present_digest():
    header = _admitted(completion=True)
    del header["completion_proof_sha256"]
    with pytest.raises(profiles.SourceWitnessContractError):
        publication_profile(header)
