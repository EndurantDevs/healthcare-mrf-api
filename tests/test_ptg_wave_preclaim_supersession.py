"""Focused pure-contract tests for logical pre-claim supersession."""

from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest

from api.ptg_wave_kubernetes import build_ptg_wave_job
from process.ptg_wave_preclaim_supersession import (
    PTGWavePreclaimSupersessionConflict,
    attest_logical_preclaim_supersession,
    validate_logical_preclaim_supersession_proof,
)
from process.ptg_wave_state import canonical_json, sha256_digest


_WAVE_DIGEST = "a" * 64
_MANIFEST_DIGEST = "b" * 64
_JOBS_DIGEST = "c" * 64
_IMAGE = "registry.example/worker@sha256:" + "d" * 64
_RUNTIME_IMAGE = "sha256:" + "e" * 64
_BARRIER_FACTORY = "process.ptg_wave_redis_adapter.create_ptg_wave_redis_barrier"


class _IntSubclass(int):
    pass


class _StringSubclass(str):
    pass


def _manifest(intent_count: int = 13) -> dict:
    return build_ptg_wave_job(
        wave_digest=_WAVE_DIGEST,
        manifest_digest=_MANIFEST_DIGEST,
        jobs_digest=_JOBS_DIGEST,
        job_count=intent_count,
        image=_IMAGE,
        runtime_image_identity=_RUNTIME_IMAGE,
        barrier_factory=_BARRIER_FACTORY,
    )


def _actual_job(manifest: dict) -> dict:
    actual = copy.deepcopy(manifest)
    actual["metadata"].update(
        {
            "uid": "synthetic-job-uid",
            "resourceVersion": "100",
            "creationTimestamp": "2000-01-01T00:00:00Z",
        }
    )
    actual["status"] = {
        "failed": 12,
        "conditions": [
            {
                "type": "Failed",
                "status": "True",
                "lastTransitionTime": "2000-01-01T00:01:00Z",
            }
        ],
    }
    return actual


def _wave(intent_count: int = 13) -> SimpleNamespace:
    manifest = _manifest(intent_count)
    manifest_bytes = json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    annotations = manifest["metadata"]["annotations"]
    return SimpleNamespace(
        wave_id="predecessor-wave",
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
        state="uncertain",
        uncertainty_resume_state="slots_waiting",
        k8s_post_ticket="post:synthetic",
        k8s_post_started_at=object(),
    )


def _intents_and_runs(wave: SimpleNamespace) -> tuple[list, list]:
    intents = []
    runs = []
    for ordinal in range(wave.intent_count):
        intent = SimpleNamespace(
            wave_id=wave.wave_id,
            ordinal=ordinal,
            run_id=f"run-{ordinal}",
            job_id=f"job-{ordinal}",
            source_file_import_id=f"source-{ordinal}",
        )
        intents.append(intent)
        runs.append(
            SimpleNamespace(
                run_id=intent.run_id,
                importer="ptg",
                source_file_import_id=intent.source_file_import_id,
                import_id=intent.source_file_import_id,
                status="queued",
                phase_detail="wave admitted; controller materialization pending",
                started_at=None,
                finished_at=None,
                snapshot_id=None,
                error=None,
                progress={
                    "unit": "run",
                    "total": 1,
                    "done": 0,
                    "pct": 0,
                    "message": "wave admitted; controller materialization pending",
                },
                metrics={
                    "wave_id": wave.wave_id,
                    "queue": wave.release_queue,
                    "base_queue": "arq:PTGSmall",
                    "worker_class": "process.PTGSmall",
                    "resource_class": "small",
                    "worker_limit": 12,
                    "job_id": intent.job_id,
                    "ordinal": ordinal,
                    "wave_digest": wave.wave_digest,
                },
            )
        )
    return intents, runs


def _empty_redis_attestation(wave: SimpleNamespace) -> dict:
    evidence = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "target_key_count": 4 + (4 * wave.intent_count),
        "ready_slots": [],
        "ready_slots_digest": sha256_digest(canonical_json([])),
        "release_present": False,
        "release_digest": None,
        "release_receipt": None,
        "queued_ordinals": [],
        "job_ordinals": [],
        "result_ordinals": [],
        "retry_ordinals": [],
        "in_progress_ordinals": [],
        "health_check_present": False,
    }
    return {
        **evidence,
        "attestation_digest": sha256_digest(canonical_json(evidence)),
    }


def _attest(
    wave: SimpleNamespace | None = None,
    actual_job: dict | None = None,
    redis: dict | None = None,
    claims: list | None = None,
    outcomes: list | None = None,
):
    wave = _wave() if wave is None else wave
    intents, runs = _intents_and_runs(wave)
    return attest_logical_preclaim_supersession(
        wave,
        intents,
        runs,
        [] if claims is None else claims,
        [] if outcomes is None else outcomes,
        [],
        _actual_job(wave.kubernetes_manifest) if actual_job is None else actual_job,
        _empty_redis_attestation(wave) if redis is None else redis,
        "successor-wave",
    )


def test_logical_preclaim_witness_binds_all_intent_runs_not_twelve_workers():
    witness = _attest()
    mapping = witness.as_mapping()

    assert mapping["recovery_basis"] == "logical_preclaim_failure"
    assert mapping["predecessor"]["intent_count"] == 13
    assert mapping["kubernetes"]["failed"] == 12
    assert mapping["redis"]["ready_slot_count"] == 0
    assert mapping["database"] == {
        "pristine_run_count": 13,
        "claim_count": 0,
        "outcome_count": 0,
        "worker_start_event_count": 0,
    }
    assert len(mapping["proof_digest"]) == 64
    assert "pods" not in mapping["kubernetes"]
    assert "resourceVersion" not in repr(mapping)
    assert "Timestamp" not in repr(mapping)


def test_witness_is_stable_when_volatile_job_observation_fields_change():
    wave = _wave()
    actual = _actual_job(wave.kubernetes_manifest)
    first = _attest(wave, actual)
    actual["metadata"]["resourceVersion"] = "101"
    actual["metadata"]["creationTimestamp"] = "2001-01-01T00:00:00Z"
    actual["status"]["conditions"][0]["lastTransitionTime"] = "2001-01-01T00:01:00Z"

    assert _attest(wave, actual).proof_digest == first.proof_digest


def test_canonical_proof_validator_requires_exact_binding_and_digest():
    proof = _attest().as_mapping()
    assert validate_logical_preclaim_supersession_proof(
        proof,
        predecessor_wave_id="predecessor-wave",
        successor_wave_id="successor-wave",
    ) == proof

    tampered = copy.deepcopy(proof)
    tampered["database"]["worker_start_event_count"] = 1
    with pytest.raises(
        PTGWavePreclaimSupersessionConflict,
        match="database proof",
    ):
        validate_logical_preclaim_supersession_proof(tampered)

    tampered = copy.deepcopy(proof)
    tampered["proof_digest"] = "0" * 64
    with pytest.raises(
        PTGWavePreclaimSupersessionConflict,
        match="proof digest",
    ):
        validate_logical_preclaim_supersession_proof(tampered)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("predecessor", "intent_count", True),
        ("database", "pristine_run_count", _IntSubclass(13)),
        ("database", "claim_count", 0.0),
        ("kubernetes", "completions", _IntSubclass(12)),
        ("kubernetes", "failed", 12.0),
        ("kubernetes", "failed_condition", 1),
        ("kubernetes", "complete_condition", 0),
        ("redis", "ready_slot_count", True),
        ("redis", "queued_ordinal_count", _IntSubclass(0)),
        ("redis", "result_ordinal_count", 0.0),
        ("redis", "release_present", 0),
        ("redis", "health_check_present", 1),
        ("kubernetes", "job_uid", _StringSubclass("synthetic-job-uid")),
    ],
)
def test_canonical_proof_validator_rejects_coerced_scalar_types(
    section, field, value
):
    proof = _attest().as_mapping()
    proof[section][field] = value

    with pytest.raises(PTGWavePreclaimSupersessionConflict):
        validate_logical_preclaim_supersession_proof(proof)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda wave, actual, redis: actual["status"].__setitem__("ready", 1),
            "ready must be zero",
        ),
        (
            lambda wave, actual, redis: actual["status"].__setitem__("failed", 12.0),
            "failed count",
        ),
        (
            lambda wave, actual, redis: actual["status"].__setitem__(
                "failed", _IntSubclass(12)
            ),
            "failed count",
        ),
        (
            lambda wave, actual, redis: actual["status"].__setitem__(
                "conditions", [{"type": "Complete", "status": "True"}]
            ),
            "true Failed",
        ),
        (
            lambda wave, actual, redis: redis.__setitem__("ready_slots", [{"slot": 0}]),
            "empty pre-release",
        ),
        (
            lambda wave, actual, redis: redis.__setitem__("job_count", 13.0),
            "job_count",
        ),
        (
            lambda wave, actual, redis: redis.__setitem__(
                "target_key_count", _IntSubclass(56)
            ),
            "target_key_count",
        ),
        (
            lambda wave, actual, redis: redis.__setitem__("release_present", 0),
            "release_present",
        ),
        (
            lambda wave, actual, redis: setattr(
                wave, "kubernetes_job_receipt", {"unexpected": True}
            ),
            "receipt or lifecycle marker",
        ),
    ],
)
def test_witness_blocks_any_non_preclaim_evidence(mutate, message):
    wave = _wave()
    actual = _actual_job(wave.kubernetes_manifest)
    redis = _empty_redis_attestation(wave)
    mutate(wave, actual, redis)

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match=message):
        _attest(wave, actual, redis)


def test_witness_blocks_claims_outcomes_or_nonpristine_run():
    wave = _wave()
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="no claims"):
        _attest(wave, claims=[object()])
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="no outcomes"):
        _attest(wave, outcomes=[object()])

    intents, runs = _intents_and_runs(wave)
    runs[0].started_at = object()
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="not pristine"):
        attest_logical_preclaim_supersession(
            wave,
            intents,
            runs,
            [],
            [],
            [],
            _actual_job(wave.kubernetes_manifest),
            _empty_redis_attestation(wave),
            "successor-wave",
        )


def test_witness_blocks_worker_start_event_marker():
    wave = _wave()
    intents, runs = _intents_and_runs(wave)
    with pytest.raises(
        PTGWavePreclaimSupersessionConflict,
        match="no worker start events",
    ):
        attest_logical_preclaim_supersession(
            wave,
            intents,
            runs,
            [],
            [],
            [0],
            _actual_job(wave.kubernetes_manifest),
            _empty_redis_attestation(wave),
            "successor-wave",
        )
