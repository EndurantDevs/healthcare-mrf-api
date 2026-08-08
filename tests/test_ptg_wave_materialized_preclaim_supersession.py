"""Pure proof tests for a durable Job that failed before worker claims."""

from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest

from api.ptg_wave_kubernetes import build_ptg_wave_job
from process import ptg_wave_materialized_preclaim_supersession as materialized
from process._ptg_wave_redis_encoding import (
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
)
from process.ptg_wave_materialized_preclaim_supersession import (
    PTGWaveMaterializedPreclaimObservation,
    attest_materialized_preclaim_supersession,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
    validate_materialized_preclaim_supersession_proof,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from tests.test_ptg_wave_preclaim_supersession import (
    _BARRIER_FACTORY,
    _IMAGE,
    _JOBS_DIGEST,
    _MANIFEST_DIGEST,
    _RUNTIME_IMAGE,
    _actual_job,
    _empty_redis_attestation,
    _intents_and_runs,
)


_REQUEST_DIGEST = "6" * 64
_WAVE_DIGEST = sha256_digest(
    (PTG_SMALL_WAVE_PROTOCOL_IDENTITY + "\0" + _REQUEST_DIGEST).encode()
)


def _materialized_manifest(intent_count: int) -> tuple[dict, bytes, dict]:
    manifest = build_ptg_wave_job(
        wave_digest=_WAVE_DIGEST,
        manifest_digest=_MANIFEST_DIGEST,
        jobs_digest=_JOBS_DIGEST,
        job_count=intent_count,
        image=_IMAGE,
        runtime_image_identity=_RUNTIME_IMAGE,
        barrier_factory=_BARRIER_FACTORY,
    )
    manifest_bytes = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    annotations = manifest["metadata"]["annotations"]
    return manifest, manifest_bytes, annotations


def _materialized_receipt(annotations: dict) -> dict:
    return {
        "wave_digest": _WAVE_DIGEST,
        "job_uid": "synthetic-job-uid",
        "manifest_identity": annotations[
            "healthporta.com/ptg-wave-manifest-identity"
        ],
        "config_identity": annotations[
            "healthporta.com/ptg-wave-config-identity"
        ],
        "pinned_image_reference": _IMAGE,
        "pinned_image_digest": "d" * 64,
        "runtime_image_identity": _RUNTIME_IMAGE,
    }


def _prior_recovery_rows() -> tuple[dict, SimpleNamespace, SimpleNamespace]:
    logical_evidence_map = {"proof_digest": "1" * 64}
    rollback_evidence_map = {"proof_digest": "2" * 64}
    cohort_map = {
        "schema_version": "healthporta.ptg-import-wave-attestation.v4",
        "wave_id": "materialized-wave",
        "supersession": logical_evidence_map,
        "admission_rollback_supersession": rollback_evidence_map,
    }
    logical_recovery = SimpleNamespace(
        predecessor_wave_id="logical-predecessor",
        successor_wave_id="materialized-wave",
        recovery_basis="logical_preclaim_failure",
        recovery_evidence=logical_evidence_map,
        recovery_evidence_sha256="1" * 64,
    )
    rollback_recovery = SimpleNamespace(
        predecessor_wave_id="rollback-predecessor",
        successor_wave_id="materialized-wave",
        recovery_basis="admission_rollback_absent",
        recovery_evidence=rollback_evidence_map,
        recovery_evidence_sha256="2" * 64,
    )
    return cohort_map, logical_recovery, rollback_recovery


def _materialized_wave(intent_count: int = 13) -> SimpleNamespace:
    """Build one exact synthetic V10 durable boundary."""

    manifest, manifest_bytes, annotations = _materialized_manifest(
        intent_count
    )
    receipt_map = _materialized_receipt(annotations)
    cohort_map, logical_recovery, rollback_recovery = _prior_recovery_rows()
    return SimpleNamespace(
        wave_id="materialized-wave",
        idempotency_key="materialized-wave",
        request_digest=_REQUEST_DIGEST,
        cohort_attestation_digest="7" * 64,
        cohort_attestation=cohort_map,
        wave_digest=_WAVE_DIGEST,
        manifest_digest=_MANIFEST_DIGEST,
        jobs_digest=_JOBS_DIGEST,
        intent_count=intent_count,
        worker_limit=12,
        queue="arq:PTGSmall",
        worker_class="process.PTGSmall",
        resource_class="small",
        release_queue=annotations["healthporta.com/ptg-wave-queue"],
        kubernetes_config_identity=annotations[
            "healthporta.com/ptg-wave-config-identity"
        ],
        kubernetes_manifest_identity=annotations[
            "healthporta.com/ptg-wave-manifest-identity"
        ],
        pinned_image_reference=_IMAGE,
        pinned_image_digest="d" * 64,
        runtime_image_identity=_RUNTIME_IMAGE,
        kubernetes_manifest=manifest,
        kubernetes_manifest_bytes=manifest_bytes,
        kubernetes_manifest_sha256=sha256_digest(manifest_bytes),
        state="slots_waiting",
        uncertainty_resume_state=None,
        k8s_post_ticket="post:synthetic",
        k8s_post_started_at=object(),
        kubernetes_job_uid="synthetic-job-uid",
        kubernetes_job_receipt=receipt_map,
        kubernetes_job_receipt_digest=sha256_digest(
            canonical_json(receipt_map)
        ),
        logical_recovery=logical_recovery,
        rollback_recovery=rollback_recovery,
    )


def _observation(
    wave: SimpleNamespace,
    *,
    claims: tuple = (),
    outcomes: tuple = (),
    worker_events: tuple = (),
    actual_job: dict | None = None,
    redis_attestation: dict | None = None,
) -> PTGWaveMaterializedPreclaimObservation:
    intents, runs = _intents_and_runs(wave)
    return PTGWaveMaterializedPreclaimObservation(
        predecessor_wave=wave,
        intents=intents,
        runs=runs,
        claims=claims,
        outcomes=outcomes,
        worker_start_event_ordinals=worker_events,
        logical_supersession=wave.logical_recovery,
        admission_rollback=wave.rollback_recovery,
        actual_job=(
            _actual_job(wave.kubernetes_manifest)
            if actual_job is None
            else actual_job
        ),
        redis_unclaimed_attestation=(
            _empty_redis_attestation(wave)
            if redis_attestation is None
            else redis_attestation
        ),
    )


def _attest(wave: SimpleNamespace | None = None, **changes) -> dict:
    materialized_wave = _materialized_wave() if wave is None else wave
    return attest_materialized_preclaim_supersession(
        _observation(materialized_wave, **changes),
        "successor-wave",
    )


def test_materialized_proof_binds_durable_job_without_historical_pods():
    proof = _attest()

    assert proof["recovery_basis"] == "materialized_preclaim_failure"
    assert proof["database"] == {
        "state": "slots_waiting",
        "pristine_run_count": 13,
        "claim_count": 0,
        "outcome_count": 0,
        "worker_start_event_count": 0,
    }
    assert proof["kubernetes"]["failed"] == 12
    assert proof["kubernetes"]["job_receipt_digest"] == (
        proof["predecessor"]["kubernetes_job_receipt_digest"]
    )
    assert "pods" not in proof["kubernetes"]
    assert "resourceVersion" not in repr(proof)
    assert proof["predecessor"]["pinned_image_digest"] == "d" * 64


def test_materialized_proof_validator_requires_exact_binding_and_digest():
    proof = _attest()
    assert validate_materialized_preclaim_supersession_proof(
        proof,
        predecessor_wave_id="materialized-wave",
        successor_wave_id="successor-wave",
    ) == proof

    tampered = copy.deepcopy(proof)
    tampered["database"]["claim_count"] = 1
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="pristine"):
        validate_materialized_preclaim_supersession_proof(tampered)

    tampered = copy.deepcopy(proof)
    tampered["proof_digest"] = "0" * 64
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="digest"):
        validate_materialized_preclaim_supersession_proof(tampered)


@pytest.mark.parametrize(
    ("field_name", "field_value", "message"),
    (
        ("state", "released", "slots_waiting"),
        ("uncertainty_resume_state", "slots_waiting", "slots_waiting"),
        ("kubernetes_job_receipt", {"wrong": True}, "receipt is corrupt"),
        ("kubernetes_ready_attestation_digest", "a" * 64, "progress"),
        ("redis_release_ticket", "release", "progress"),
        ("outcomes_digest", "b" * 64, "progress"),
    ),
)
def test_materialized_proof_rejects_boundary_or_progress_drift(
    field_name,
    field_value,
    message,
):
    wave = _materialized_wave()
    setattr(wave, field_name, field_value)

    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match=message):
        _attest(wave)


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"claims": (object(),)}, "zero claims"),
        ({"outcomes": (object(),)}, "zero outcomes"),
        ({"worker_events": (0,)}, "zero worker start"),
    ),
)
def test_materialized_proof_rejects_any_database_work(changes, message):
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match=message):
        _attest(**changes)


def test_materialized_proof_rejects_job_redis_or_prior_recovery_drift():
    wave = _materialized_wave()
    active_job = _actual_job(wave.kubernetes_manifest)
    active_job["status"]["active"] = 1
    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        _attest(wave, actual_job=active_job)

    redis_attestation = _empty_redis_attestation(wave)
    redis_attestation["release_present"] = True
    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        _attest(wave, redis_attestation=redis_attestation)

    wave.logical_recovery.recovery_evidence_sha256 = "3" * 64
    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="prior recovery",
    ):
        _attest(wave)


def test_materialized_proof_hides_unexpected_external_error_details(monkeypatch):
    def raise_unexpected_error(*_args):
        raise RuntimeError("private Kubernetes endpoint detail")

    monkeypatch.setattr(
        materialized,
        "_attest_terminal_preclaim_job",
        raise_unexpected_error,
    )

    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="materialized preclaim external evidence is invalid",
    ) as exc_info:
        _attest()
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert "private Kubernetes" not in str(exc_info.value)


def test_materialized_proof_rejects_coerced_scalar_types():
    proof = _attest()
    proof["kubernetes"]["failed"] = 12.0
    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        validate_materialized_preclaim_supersession_proof(proof)

    proof = _attest()
    proof["redis"]["release_present"] = 0
    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        validate_materialized_preclaim_supersession_proof(proof)

    proof = _attest()
    proof["predecessor"]["runtime_image_identity"] = "sha256:" + "z" * 64
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="SHA-256"):
        validate_materialized_preclaim_supersession_proof(proof)
