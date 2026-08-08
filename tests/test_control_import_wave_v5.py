"""Closed-contract V5 materialized-preclaim recovery tests."""

from __future__ import annotations

import copy

import pytest

from api import control_import_wave_attestation as attestation
from api.control_import_wave_attestation import (
    ATTESTATION_VERSION,
    LEGACY_ATTESTATION_VERSION,
    MATERIALIZED_PRECLAIM_ATTESTATION_VERSION,
    ROLLBACK_ATTESTATION_VERSION,
    SUPERSESSION_ATTESTATION_VERSION,
)
from api.control_import_waves import (
    sign_cohort_attestation,
    validate_import_wave_payload,
)
from tests.test_control_import_waves import _KEY, _payload


def _resign(payload: dict) -> None:
    unsigned_attestation_map = {
        field_name: field_value
        for field_name, field_value in payload["cohort_attestation"].items()
        if field_name != "signature"
    }
    payload["cohort_attestation"]["signature"] = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )


def test_v5_accepts_only_the_signed_successor_bound_materialized_proof():
    payload = _payload(
        schema_version=MATERIALIZED_PRECLAIM_ATTESTATION_VERSION
    )

    validated = validate_import_wave_payload(payload, attestation_key=_KEY)

    proof = payload["cohort_attestation"][
        "materialized_preclaim_supersession"
    ]
    assert validated["materialized_preclaim_supersession"] == proof
    assert proof["successor_wave_id"] == "wave-unit"
    assert "admission_rollback_supersession" not in validated
    assert validated["supersession"] is None


@pytest.mark.parametrize(
    "schema_version",
    (
        LEGACY_ATTESTATION_VERSION,
        ATTESTATION_VERSION,
        SUPERSESSION_ATTESTATION_VERSION,
        ROLLBACK_ATTESTATION_VERSION,
    ),
)
def test_pre_v5_projection_does_not_gain_materialized_proof(schema_version):
    validated = validate_import_wave_payload(
        _payload(schema_version=schema_version),
        attestation_key=_KEY,
    )

    assert "materialized_preclaim_supersession" not in validated


@pytest.mark.parametrize(
    "mutation",
    (
        lambda attestation_map: attestation_map.pop(
            "materialized_preclaim_supersession"
        ),
        lambda attestation_map: attestation_map.__setitem__(
            "supersession",
            {},
        ),
        lambda attestation_map: attestation_map.__setitem__(
            "admission_rollback_supersession",
            {},
        ),
    ),
)
def test_v5_rejects_missing_or_legacy_recovery_fields(mutation):
    payload = _payload(
        schema_version=MATERIALIZED_PRECLAIM_ATTESTATION_VERSION
    )
    mutation(payload["cohort_attestation"])

    with pytest.raises(ValueError, match="fields are not exact"):
        validate_import_wave_payload(payload, attestation_key=_KEY)


def test_v5_rejects_a_resigned_proof_bound_to_another_successor():
    payload = copy.deepcopy(
        _payload(schema_version=MATERIALIZED_PRECLAIM_ATTESTATION_VERSION)
    )
    payload["cohort_attestation"]["materialized_preclaim_supersession"][
        "successor_wave_id"
    ] = "other-wave"
    _resign(payload)

    with pytest.raises(ValueError, match="another successor"):
        validate_import_wave_payload(payload, attestation_key=_KEY)


def test_v5_uses_its_own_hmac_domain():
    payload = _payload(
        schema_version=MATERIALIZED_PRECLAIM_ATTESTATION_VERSION
    )
    unsigned_attestation_map = {
        field_name: field_value
        for field_name, field_value in payload["cohort_attestation"].items()
        if field_name != "signature"
    }
    v5_signature = sign_cohort_attestation(unsigned_attestation_map, key=_KEY)
    unsigned_attestation_map["schema_version"] = ROLLBACK_ATTESTATION_VERSION

    assert v5_signature != attestation.sign_cohort_attestation(
        unsigned_attestation_map,
        key=_KEY,
    )
