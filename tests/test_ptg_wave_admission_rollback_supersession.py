"""Closed-contract tests for one absent-admission retirement proof."""

from __future__ import annotations

from copy import deepcopy

import pytest

from api.ptg_wave_kubernetes import _job_name
from process._ptg_wave_redis_encoding import (
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    wave_queue_name,
)
from process.ptg_wave_admission_rollback_supersession import (
    DATABASE_FIELDS,
    PTGWaveAdmissionRollbackConflict,
    build_admission_rollback_supersession_proof,
    validate_admission_rollback_predecessor,
    validate_admission_rollback_supersession_proof,
)
from process.ptg_wave_state import canonical_json, sha256_digest


def _predecessor() -> dict[str, object]:
    request_digest = "a" * 64
    wave_digest = sha256_digest(
        (
            PTG_SMALL_WAVE_PROTOCOL_IDENTITY
            + "\0"
            + request_digest
        ).encode("utf-8")
    )
    return {
        "wave_id": "predecessor-wave",
        "idempotency_key": "predecessor-wave",
        "request_digest": request_digest,
        "wave_digest": wave_digest,
        "release_queue": wave_queue_name(wave_digest),
        "intent_count": 17,
    }


def _proof() -> dict[str, object]:
    predecessor = _predecessor()
    return build_admission_rollback_supersession_proof(
        predecessor,
        "successor-wave",
        database={name: 0 for name in DATABASE_FIELDS},
        kubernetes={
            "job_name": _job_name(str(predecessor["wave_digest"])),
            "job_present": False,
            "pod_count": 0,
        },
        redis={
            "queue_name": predecessor["release_queue"],
            "queued_entry_count": 0,
            "ready_slot_count": 0,
            "release_present": False,
            "health_check_present": False,
        },
    )


def _resign(proof: dict[str, object]) -> None:
    unsigned_proof_map = {
        name: proof_field_value
        for name, proof_field_value in proof.items()
        if name != "proof_digest"
    }
    proof["proof_digest"] = sha256_digest(
        canonical_json(unsigned_proof_map)
    )


def test_proof_is_exact_digest_bound_and_successor_bound():
    proof = _proof()

    assert validate_admission_rollback_supersession_proof(
        proof,
        predecessor=_predecessor(),
        predecessor_wave_id="predecessor-wave",
        successor_wave_id="successor-wave",
    ) == proof

    for expected_successor in ("another-successor", "predecessor-wave"):
        with pytest.raises(PTGWaveAdmissionRollbackConflict):
            validate_admission_rollback_supersession_proof(
                proof,
                predecessor=_predecessor(),
                successor_wave_id=expected_successor,
            )

    tampered = deepcopy(proof)
    tampered["proof_digest"] = "0" * 64
    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="proof digest is invalid",
    ):
        validate_admission_rollback_supersession_proof(tampered)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("idempotency_key", "other-request"),
        ("request_digest", "b" * 64),
        ("wave_digest", "c" * 64),
        ("release_queue", "arq:PTGSmall:wave:" + "d" * 64),
        ("intent_count", True),
        ("intent_count", 0),
        ("intent_count", 4097),
    ),
)
def test_predecessor_derivations_are_closed(field, value):
    predecessor = _predecessor()
    predecessor[field] = value

    with pytest.raises(PTGWaveAdmissionRollbackConflict):
        validate_admission_rollback_predecessor(predecessor)


@pytest.mark.parametrize("field", sorted(DATABASE_FIELDS))
def test_every_nonzero_database_artifact_blocks_retirement(field):
    proof = _proof()
    proof["database"][field] = 1
    _resign(proof)

    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="database proof is not empty",
    ):
        validate_admission_rollback_supersession_proof(proof)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    (
        ("kubernetes", "job_name", "unrelated-job"),
        ("kubernetes", "job_present", True),
        ("kubernetes", "pod_count", 1),
        ("redis", "queue_name", "arq:PTGSmall:wave:" + "e" * 64),
        ("redis", "queued_entry_count", 1),
        ("redis", "ready_slot_count", 1),
        ("redis", "release_present", True),
        ("redis", "health_check_present", True),
    ),
)
def test_every_external_artifact_blocks_retirement(section, field, value):
    proof = _proof()
    proof[section][field] = value
    _resign(proof)

    with pytest.raises(PTGWaveAdmissionRollbackConflict):
        validate_admission_rollback_supersession_proof(proof)


def test_proof_rejects_extra_fields_and_noncanonical_scalars():
    proof = _proof()
    proof["extra"] = None
    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="fields are not exact",
    ):
        validate_admission_rollback_supersession_proof(proof)

    proof = _proof()
    proof["database"]["wave_id_count"] = False
    _resign(proof)
    with pytest.raises(PTGWaveAdmissionRollbackConflict):
        validate_admission_rollback_supersession_proof(proof)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("schema_version", "unsupported-version"),
        ("recovery_basis", "unsupported-basis"),
    ),
)
def test_proof_rejects_an_unsupported_version_or_basis(field, value):
    proof = _proof()
    proof[field] = value

    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="version or basis is unsupported",
    ):
        validate_admission_rollback_supersession_proof(proof)


def test_proof_rejects_every_mismatched_expected_identity():
    proof = _proof()
    other_predecessor = _predecessor()
    other_predecessor["wave_id"] = "other-predecessor"
    other_predecessor["idempotency_key"] = "other-predecessor"

    with pytest.raises(PTGWaveAdmissionRollbackConflict, match="predecessor"):
        validate_admission_rollback_supersession_proof(
            proof,
            predecessor=other_predecessor,
        )
    with pytest.raises(PTGWaveAdmissionRollbackConflict, match="predecessor"):
        validate_admission_rollback_supersession_proof(
            proof,
            predecessor_wave_id="other-predecessor",
        )
    with pytest.raises(PTGWaveAdmissionRollbackConflict, match="successor"):
        validate_admission_rollback_supersession_proof(
            proof,
            successor_wave_id="other-successor",
        )


@pytest.mark.parametrize(
    ("section", "field"),
    (
        ("kubernetes", "job_present"),
        ("redis", "release_present"),
        ("redis", "health_check_present"),
    ),
)
def test_proof_rejects_nonboolean_absence_flags(section, field):
    proof = _proof()
    proof[section][field] = 0
    _resign(proof)

    with pytest.raises(PTGWaveAdmissionRollbackConflict, match="boolean"):
        validate_admission_rollback_supersession_proof(proof)


def test_proof_rejects_a_nonhex_digest():
    proof = _proof()
    proof["proof_digest"] = "G" * 64

    with pytest.raises(PTGWaveAdmissionRollbackConflict, match="SHA-256"):
        validate_admission_rollback_supersession_proof(proof)
