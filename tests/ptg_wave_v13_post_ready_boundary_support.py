"""Neutral fixtures for V13 post-ready abandonment boundary tests."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

from process.ptg_wave_controller import PTGWaveBundle, restore_wave_manifest
from process.ptg_wave_redis import (
    attest_ptg_small_wave_unclaimed_failure_redis,
    register_ptg_small_wave_slot,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v13_post_ready_abandonment import (
    PTGWaveV13PostReadyObservation,
    V13_QUARANTINE_REASON,
    abandonment_receipt_payload,
)
from tests.ptg_wave_redis_test_support import FakeRedis
from tests.ptg_wave_v12_pristine_abandonment_support import boundary
from tests.test_ptg_wave_kubernetes_failure_attestation import _actual_job
from tests.test_ptg_wave_v13_post_ready_abandonment import (
    _failed_job,
    _redigest,
    _retained_failed_pods,
)


def worker_status(pod):
    """Return the retained worker status from one neutral Pod fixture."""

    return pod["status"]["containerStatuses"][0]


def termination_status(pod):
    """Return the retained worker termination from one neutral Pod fixture."""

    return worker_status(pod)["state"]["terminated"]


def mutate_wave(observation, **changes):
    """Apply one durable-wave drift to a fresh observation."""

    for name, value in changes.items():
        setattr(observation.predecessor_wave, name, value)
    return observation


def mutate_job_receipt(observation):
    """Corrupt the durable Job receipt while preserving the observation shape."""

    observation.predecessor_wave.kubernetes_job_receipt = {}
    return observation


def mutate_proof_job_receipt(proof):
    """Rehash a Job receipt with an invalid admission-bound identity."""

    receipt = proof["kubernetes"]["job_receipt"]
    receipt["job_uid"] = ""
    proof["kubernetes"]["job_receipt_digest"] = sha256_digest(
        canonical_json(receipt)
    )


def mutate_failure(proof, **changes):
    """Apply and rehash one failure-attestation mutation."""

    proof["kubernetes"]["failure"].update(changes)
    _redigest(proof)


def mutate_retained_slot(proof, **changes):
    """Apply and rehash one retained-Pod mutation."""

    proof["kubernetes"]["failure"]["retained_failed_slots"][0].update(changes)
    _redigest(proof)


def mutate_retained_termination(proof, **changes):
    """Apply and rehash one retained termination mutation."""

    proof["kubernetes"]["failure"]["retained_failed_slots"][0][
        "termination"
    ].update(changes)
    _redigest(proof)


async def observation_boundary():
    """Build one exact DB, Kubernetes, and Redis observation boundary."""

    wave, intents, runs, admission = boundary()
    actual_job = _actual_job(wave.kubernetes_manifest)
    wave.kubernetes_job_uid = actual_job["metadata"]["uid"]
    wave.kubernetes_job_receipt = {
        **wave.kubernetes_job_receipt,
        "job_uid": wave.kubernetes_job_uid,
    }
    wave.kubernetes_job_receipt_digest = sha256_digest(
        canonical_json(wave.kubernetes_job_receipt)
    )
    redis = FakeRedis()
    manifest = restore_wave_manifest(PTGWaveBundle(wave=wave, intents=intents))
    for slot in range(12):
        await register_ptg_small_wave_slot(
            redis,
            manifest.reference,
            slot=slot,
            pod_uid=f"pod-uid-{slot}",
        )
    redis_attestation = await attest_ptg_small_wave_unclaimed_failure_redis(
        redis,
        manifest,
    )
    return (
        PTGWaveV13PostReadyObservation(
            predecessor_wave=wave,
            intents=intents,
            runs=runs,
            claims=(),
            outcomes=(),
            worker_start_event_ordinals=(),
            logical_supersession=None,
            admission_rollback=None,
            actual_job=_failed_job(wave.kubernetes_manifest),
            actual_pods=_retained_failed_pods(wave.kubernetes_manifest),
            redis_unclaimed_attestation=redis_attestation.as_mapping(),
        ),
        admission,
        redis,
    )


class RuntimeResult:
    """Minimal scalar/row result for locked-observation tests."""

    def __init__(self, *, scalar=None, rows=()):
        self.scalar = scalar
        self.rows = rows

    def scalar_one_or_none(self):
        return self.scalar

    def scalars(self):
        return self

    def all(self):
        return self.rows


class RuntimeSession:
    """Ordered fake session preserving the runtime's SQL call sequence."""

    def __init__(self, results):
        self.results = list(results)

    @property
    def remaining(self):
        return len(self.results)

    async def execute(self, *_args, **_kwargs):
        return self.results.pop(0)


def runtime_session(observation):
    """Return the ten-result session used by one complete locked observation."""

    return RuntimeSession(
        [
            RuntimeResult(),
            RuntimeResult(scalar=observation.predecessor_wave),
            RuntimeResult(scalar=None),
            RuntimeResult(rows=observation.intents),
            RuntimeResult(rows=observation.runs),
            RuntimeResult(rows=observation.claims),
            RuntimeResult(rows=observation.outcomes),
            RuntimeResult(rows=()),
            RuntimeResult(scalar=observation.logical_supersession),
            RuntimeResult(scalar=observation.admission_rollback),
        ]
    )


def stored_v13_quarantine(proof, request, signer):
    """Return one exact persisted V13 quarantine and signed receipt."""

    receipt = signer.sign_receipt(
        schema="healthporta.ptg-wave-abandonment-receipt.v2",
        key_id=request["key_id"],
        issued_at=dt.datetime(2026, 8, 17, 0, 7, tzinfo=dt.UTC),
        receipt_payload=abandonment_receipt_payload(proof),
    )
    unsigned_proof_by_field = {
        field_name: field_value
        for field_name, field_value in proof.items()
        if field_name != "proof_digest"
    }
    return SimpleNamespace(
        predecessor_wave_id=request["operation_id"],
        reason=V13_QUARANTINE_REASON,
        recovery_basis=V13_QUARANTINE_REASON,
        cutover_id=request["cutover_id"],
        recovery_evidence=proof,
        recovery_evidence_sha256=proof["proof_digest"],
        recovery_evidence_canonical=canonical_json(unsigned_proof_by_field),
        receipt_key_id=request["key_id"],
        abandonment_receipt=receipt,
        abandonment_receipt_payload_digest=receipt["payload_digest"],
        abandonment_receipt_issued_at=dt.datetime(
            2026,
            8,
            17,
            0,
            7,
            tzinfo=dt.UTC,
        ),
    )


def route_request(*, body=None, args=None):
    """Return one neutral authenticated-control request shape."""

    return SimpleNamespace(
        json={} if body is None else body,
        args={} if args is None else args,
        app=SimpleNamespace(
            ctx=SimpleNamespace(
                ptg_wave_redis=object(),
                ptg_wave_receipt_keyring=object(),
            )
        ),
    )
