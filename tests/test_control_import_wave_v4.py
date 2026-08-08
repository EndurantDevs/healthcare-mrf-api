"""Closed-contract V4 admission projection and recovery proof tests."""

from __future__ import annotations

import copy

import pytest

from api import control_import_wave_attestation as attestation

from api.control_import_wave_attestation import (
    ATTESTATION_VERSION,
    LEGACY_ATTESTATION_VERSION,
    ROLLBACK_ATTESTATION_VERSION,
    SUPERSESSION_ATTESTATION_VERSION,
)
from api.control_import_waves import (
    sign_cohort_attestation,
    validate_import_wave_payload,
)
from tests.test_control_import_waves import _KEY, _payload


def test_v3_supersession_proof_is_signed_and_successor_bound():
    wave_payload = _payload(schema_version=SUPERSESSION_ATTESTATION_VERSION)
    validated = validate_import_wave_payload(
        wave_payload,
        attestation_key=_KEY,
    )

    assert validated["supersession"] == (
        wave_payload["cohort_attestation"]["supersession"]
    )
    assert validated["supersession"]["successor_wave_id"] == "wave-unit"

    tampered = copy.deepcopy(wave_payload)
    tampered["cohort_attestation"]["supersession"][
        "successor_wave_id"
    ] = "other"
    unsigned_attestation_map = {
        key: attestation_field_value
        for key, attestation_field_value in tampered[
            "cohort_attestation"
        ].items()
        if key != "signature"
    }
    tampered["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
    with pytest.raises(ValueError, match="another successor"):
        validate_import_wave_payload(tampered, attestation_key=_KEY)


@pytest.mark.parametrize(
    "schema_version",
    (
        LEGACY_ATTESTATION_VERSION,
        ATTESTATION_VERSION,
        SUPERSESSION_ATTESTATION_VERSION,
    ),
)
def test_pre_v4_projection_does_not_gain_rollback_field(schema_version):
    validated = validate_import_wave_payload(
        _payload(schema_version=schema_version),
        attestation_key=_KEY,
    )

    assert "admission_rollback_supersession" not in validated


def test_v4_accepts_both_signed_successor_bound_recovery_proofs():
    wave_payload = _payload(schema_version=ROLLBACK_ATTESTATION_VERSION)
    validated = validate_import_wave_payload(
        wave_payload,
        attestation_key=_KEY,
    )

    assert validated["supersession"] == (
        wave_payload["cohort_attestation"]["supersession"]
    )
    assert validated["admission_rollback_supersession"] == (
        wave_payload["cohort_attestation"][
            "admission_rollback_supersession"
        ]
    )
    assert validated["admission_rollback_supersession"][
        "successor_wave_id"
    ] == "wave-unit"


@pytest.mark.parametrize(
    "missing_field",
    ("supersession", "admission_rollback_supersession"),
)
def test_v4_rejects_an_incomplete_recovery_pair(missing_field):
    incomplete = _payload(schema_version=ROLLBACK_ATTESTATION_VERSION)
    incomplete["cohort_attestation"].pop(missing_field)

    with pytest.raises(ValueError, match="fields are not exact"):
        validate_import_wave_payload(incomplete, attestation_key=_KEY)


def test_v4_rejects_rollback_bound_to_another_successor():
    tampered = copy.deepcopy(
        _payload(schema_version=ROLLBACK_ATTESTATION_VERSION)
    )
    tampered["cohort_attestation"]["admission_rollback_supersession"][
        "successor_wave_id"
    ] = "other"
    unsigned_attestation_map = {
        key: attestation_field_value
        for key, attestation_field_value in tampered[
            "cohort_attestation"
        ].items()
        if key != "signature"
    }
    tampered["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )

    with pytest.raises(ValueError, match="another successor"):
        validate_import_wave_payload(tampered, attestation_key=_KEY)


@pytest.mark.parametrize(
    "unsigned_attestation",
    ({}, {"schema_version": "unsupported-version"}),
)
def test_attestation_signing_rejects_missing_or_unknown_versions(
    unsigned_attestation,
):
    with pytest.raises(ValueError, match="schema_version is unsupported"):
        sign_cohort_attestation(unsigned_attestation, key=_KEY)


def test_snapshot_rejects_an_unknown_attestation_version():
    with pytest.raises(ValueError, match="schema_version is unsupported"):
        attestation._validate_snapshot(
            {},
            schema_version="unsupported-version",
        )
