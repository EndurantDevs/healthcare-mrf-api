"""Regression coverage for the isolated post-ready V13 abandonment family."""

import copy
import datetime as dt
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_import_wave_abandonment as abandonment
from api import control_import_wave_v13_abandonment as v13_abandonment
from api import control_wave_routes as routes
from process.ptg_wave_controller import PTGWaveBundle, restore_wave_manifest
from process.ptg_wave_receipt_contract import ordinary_cutover_id
from process.ptg_wave_redis import (
    attest_ptg_small_wave_unclaimed_failure_redis,
    register_ptg_small_wave_slot,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v13_post_ready_abandonment import (
    PTGWaveV13PostReadyObservation,
    V13_ABANDONMENT_PROOF_SCHEMA,
    V13_ABANDONMENT_REQUEST_SCHEMA,
    V13_QUARANTINE_REASON,
    attest_v13_post_ready_abandonment,
    abandonment_receipt_payload,
    validate_v13_abandonment_proof,
)
from tests.ptg_wave_redis_test_support import FakeRedis
from tests.ptg_wave_v12_pristine_abandonment_support import (
    Session,
    Transaction,
    boundary,
    keyring,
)
from tests.test_ptg_wave_kubernetes_failure_attestation import (
    _actual_job,
    _failed_pods,
)


def test_v13_has_a_distinct_post_ready_contract():
    """V13 must not silently reuse the strict V12 proof family."""

    assert V13_QUARANTINE_REASON == "v13_post_ready_unreleased_failure_cutover"
    assert V13_ABANDONMENT_REQUEST_SCHEMA == (
        "healthporta.ptg-wave.v13-post-ready-unreleased-failure-"
        "abandonment-request.v1"
    )
    assert V13_ABANDONMENT_PROOF_SCHEMA == (
        "healthporta.ptg-wave.v13-post-ready-unreleased-failure-"
        "abandonment-proof.v1"
    )


@pytest.mark.asyncio
async def test_v13_proof_binds_retained_pods_and_unreleased_redis_slots():
    """The full proof ties the retained failure subset to Redis identities."""

    proof, admission = await _proof()

    assert proof["schema_version"] == V13_ABANDONMENT_PROOF_SCHEMA
    assert proof["database"]["state"] == "slots_waiting"
    assert set(proof["kubernetes"]) == {
        "job_receipt",
        "job_receipt_digest",
        "ready_attestation",
        "ready_attestation_digest",
        "failure",
    }
    assert proof["kubernetes"]["ready_attestation"] is None
    assert proof["kubernetes"]["ready_attestation_digest"] is None
    assert len(proof["kubernetes"]["failure"]["retained_failed_slots"]) == 2
    assert len(proof["redis"]["ready_slots"]) == 12
    assert proof["redis"]["release_present"] is False
    assert proof["redis"]["queued_ordinals"] == []
    assert validate_v13_abandonment_proof(
        proof,
        operation_id=admission["wave_id"],
        cutover_id=ordinary_cutover_id(admission["wave_id"]),
        admission=admission,
    ) == proof


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        lambda proof: proof["kubernetes"]["failure"]["retained_failed_slots"][0].update(
            pod_uid="replaced-pod"
        ),
        lambda proof: proof["redis"]["ready_slots"][0].update(
            pod_uid="replaced-pod"
        ),
        lambda proof: proof["redis"].update(release_present=True),
        lambda proof: proof["redis"]["queued_ordinals"].append(0),
    ],
)
async def test_v13_rejects_tampered_post_ready_evidence(mutate):
    """No failure, release, or ready-slot detail may be weakened to a count."""

    proof, _admission = await _proof()
    tampered = copy.deepcopy(proof)
    mutate(tampered)

    with pytest.raises(Exception, match="V13|fresh V13"):
        validate_v13_abandonment_proof(tampered)


@pytest.mark.asyncio
async def test_v13_rejects_rehashed_boolean_for_a_zero_job_status():
    """A JSON boolean must not stand in for an observed numeric zero."""

    proof, _admission = await _proof()
    tampered = copy.deepcopy(proof)
    tampered["kubernetes"]["failure"]["job_ready"] = False
    _redigest(tampered)

    with pytest.raises(Exception, match="failure attestation"):
        validate_v13_abandonment_proof(tampered)


@pytest.mark.asyncio
async def test_v13_first_and_replay_route_path_never_calls_mutation_hooks(
    monkeypatch,
):
    """Only the immutable quarantine insert is allowed on a first/replay call."""

    proof, admission = await _proof()
    request = _request(admission)
    signer = keyring(monkeypatch)
    first_session = Session()
    observer = AsyncMock(return_value=proof)
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: Transaction(first_session),
    )
    monkeypatch.setattr(abandonment, "acquire_ptg_admission_lock", AsyncMock())
    monkeypatch.setattr(
        v13_abandonment,
        "attest_locked_v13_abandonment",
        observer,
    )
    redis = _NoMutationRedis()

    receipt, created = await abandonment.abandon_materialized_preclaim_wave(
        admission["wave_id"],
        request,
        redis=redis,
        receipt_keyring=signer,
        receipt_issued_at=dt.datetime(2026, 8, 17, 0, 7, tzinfo=dt.UTC),
    )

    assert created is True
    assert receipt["payload"] == abandonment_receipt_payload(proof)
    assert len(first_session.added) == 1
    stored = first_session.added[0]
    assert stored.reason == V13_QUARANTINE_REASON
    assert stored.recovery_evidence == proof
    observer.assert_awaited_once()

    replay_session = Session(stored)
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: Transaction(replay_session),
    )
    replay, replay_created = await abandonment.abandon_materialized_preclaim_wave(
        admission["wave_id"],
        copy.deepcopy(request),
        redis=redis,
        receipt_keyring=signer,
    )

    assert replay_created is False
    assert json.loads(json.dumps(replay)) == json.loads(json.dumps(receipt))
    assert replay_session.added == []
    observer.assert_awaited_once()


@pytest.mark.asyncio
async def test_v13_route_accepts_the_existing_cutover_post_shape(monkeypatch):
    """V13 stays on the ordinary cutover POST route with no new endpoint."""

    proof, admission = await _proof()
    request_body = _request(admission)
    receipt_by_field = {
        "schema": "healthporta.ptg-wave-abandonment-receipt.v2",
        "key_id": request_body["key_id"],
        "issued_at": "2026-08-17T00:07:00.000000Z",
        "payload": abandonment_receipt_payload(proof),
        "payload_digest": "1" * 64,
        "signature": "2" * 512,
    }
    service = AsyncMock(
        side_effect=((receipt_by_field, True), (receipt_by_field, False))
    )
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(routes, "abandon_materialized_preclaim_wave", service)
    request = SimpleNamespace(
        json=request_body,
        app=SimpleNamespace(
            ctx=SimpleNamespace(
                ptg_wave_redis=_NoMutationRedis(),
                ptg_wave_receipt_keyring=object(),
            )
        ),
    )

    first = await routes.control_abandon_materialized_preclaim_wave(
        request,
        admission["wave_id"],
    )
    replay = await routes.control_abandon_materialized_preclaim_wave(
        request,
        admission["wave_id"],
    )

    assert first.status == 201
    assert replay.status == 200
    assert service.await_count == 2


@pytest.mark.asyncio
async def test_v13_receipt_get_replays_the_persisted_envelope_without_observation(
    monkeypatch,
):
    """Ambiguous POST recovery is a read-only receipt lookup."""

    proof, admission = await _proof()
    request = _request(admission)
    signer = keyring(monkeypatch)
    receipt_by_field = signer.sign_receipt(
        schema="healthporta.ptg-wave-abandonment-receipt.v2",
        key_id=request["key_id"],
        issued_at=dt.datetime(2026, 8, 17, 0, 7, tzinfo=dt.UTC),
        receipt_payload=abandonment_receipt_payload(proof),
    )
    stored = SimpleNamespace(
        predecessor_wave_id=admission["wave_id"],
        reason=V13_QUARANTINE_REASON,
        cutover_id=request["cutover_id"],
        recovery_basis=V13_QUARANTINE_REASON,
        recovery_evidence=proof,
        recovery_evidence_canonical=canonical_json(
            {
                name: field_value
                for name, field_value in proof.items()
                if name != "proof_digest"
            }
        ),
        recovery_evidence_sha256=proof["proof_digest"],
        receipt_key_id=receipt_by_field["key_id"],
        abandonment_receipt=receipt_by_field,
        abandonment_receipt_payload_digest=receipt_by_field["payload_digest"],
        abandonment_receipt_issued_at=dt.datetime(
            2026, 8, 17, 0, 7, tzinfo=dt.UTC
        ),
    )
    class _Result:
        def scalar_one_or_none(self):
            return stored

    monkeypatch.setattr(
        abandonment.db,
        "execute",
        AsyncMock(return_value=_Result()),
    )
    replay = await abandonment.get_v13_post_ready_abandonment(
        admission["wave_id"],
        receipt_keyring=signer,
    )
    assert replay == receipt_by_field


@pytest.mark.asyncio
async def test_v13_receipt_get_route_returns_the_same_envelope(monkeypatch):
    """The dedicated authenticated GET exposes only the stored receipt."""

    _proof_value, admission = await _proof()
    receipt_by_field = {"schema": "healthporta.ptg-wave-abandonment-receipt.v2"}
    service = AsyncMock(return_value=receipt_by_field)
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(routes, "get_v13_post_ready_abandonment", service)
    request = SimpleNamespace(
        args={},
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_receipt_keyring=object())),
    )

    result = await routes.control_get_v13_abandonment(
        request,
        admission["wave_id"],
    )

    assert result.status == 200
    assert json.loads(result.body) == receipt_by_field
    service.assert_awaited_once_with(
        admission["wave_id"],
        receipt_keyring=request.app.ctx.ptg_wave_receipt_keyring,
    )


class _NoMutationRedis:
    """Fail if the cutover service itself attempts any Redis operation."""

    def __getattr__(self, name):
        raise AssertionError(f"unexpected Redis mutation hook: {name}")


def _request(admission: dict) -> dict:
    operation_id = admission["wave_id"]
    return {
        "schema": V13_ABANDONMENT_REQUEST_SCHEMA,
        "key_id": admission["receipt_key_id"],
        "operation_id": operation_id,
        "cutover_id": ordinary_cutover_id(operation_id),
        "admission": admission,
    }


def _redigest(proof: dict) -> None:
    """Rehash a deliberately malformed pure-proof candidate."""

    failure = proof["kubernetes"]["failure"]
    failure["attestation_digest"] = sha256_digest(
        canonical_json(
            {
                name: value
                for name, value in failure.items()
                if name != "attestation_digest"
            }
        )
    )
    unsigned_proof_by_field = {
        name: value for name, value in proof.items() if name != "proof_digest"
    }
    proof["proof_digest"] = sha256_digest(
        V13_ABANDONMENT_PROOF_SCHEMA.encode("ascii")
        + b"\0"
        + canonical_json(unsigned_proof_by_field)
    )


async def _proof():
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
    proof = attest_v13_post_ready_abandonment(
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
        cutover_id=ordinary_cutover_id(wave.wave_id),
        admission=admission,
    )
    return proof, admission


def _failed_job(manifest: dict) -> dict:
    job = _actual_job(manifest)
    job["status"] = {
        "conditions": [
            {
                "lastProbeTime": "2026-08-17T00:06:11Z",
                "lastTransitionTime": "2026-08-17T00:06:11Z",
                "message": "Job has reached the specified backoff limit",
                "reason": "BackoffLimitExceeded",
                "status": "True",
                "type": "FailureTarget",
            },
            {
                "lastProbeTime": "2026-08-17T00:06:13Z",
                "lastTransitionTime": "2026-08-17T00:06:13Z",
                "message": "Job has reached the specified backoff limit",
                "reason": "BackoffLimitExceeded",
                "status": "True",
                "type": "Failed",
            },
        ],
        "failed": 12,
        "ready": 0,
        "startTime": "2026-08-17T00:06:01Z",
        "terminating": 0,
        "uncountedTerminatedPods": {},
    }
    return job


def _retained_failed_pods(manifest: dict) -> list[dict]:
    pods = _failed_pods(manifest)
    for slot, pod in enumerate(pods):
        worker = pod["status"]["containerStatuses"][0]
        container_id = f"containerd://retained-{slot}"
        worker.clear()
        worker.update(
            {
                "allocatedResources": {},
                "containerID": container_id,
                "image": "sha256:" + "e" * 64,
                "imageID": "registry.example/worker@sha256:" + "d" * 64,
                "lastState": {},
                "name": "ptg-wave-worker",
                "ready": False,
                "resources": {},
                "restartCount": 0,
                "started": False,
                "state": {
                    "terminated": {
                        "containerID": container_id,
                        "exitCode": 1,
                        "finishedAt": "2026-08-17T00:06:09Z",
                        "reason": "Error",
                        "startedAt": (
                            "2026-08-17T00:06:03Z"
                            if slot == 0
                            else "2026-08-17T00:06:02Z"
                        ),
                    }
                },
                "user": {},
                "volumeMounts": [],
            }
        )
    return [pods[0], pods[2]]
