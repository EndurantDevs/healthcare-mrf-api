"""Pure fail-closed recovery contracts for the exact PTG worker wave."""

from __future__ import annotations

import types

import pytest

from process.ptg_wave_failure import (
    PTGWaveFailureConflict,
    _expected_redis_ready_slots,
    _expected_redis_release_mapping,
    _outcomes_digest,
    read_only_recovery_plan,
    verify_unclaimed_dead_letter_terminal_eligibility,
)
from process.ptg_wave_outcomes import linkage_mapping_digest, sign_linkage_ack
from process.ptg_wave_state import canonical_json, sha256_digest


_PIN = "1" * 64
_RUNTIME = "sha256:" + "2" * 64
_CONFIG = "3" * 64
_MANIFEST = "4" * 64
_WAVE = "5" * 64
_JOBS = "6" * 64
_RELEASE = "7" * 64
_FAILURE_SCHEMA = "healthporta.ptg-wave.unclaimed-failure.v1"
_KEY = "unit-test-linkage-key"
_PROTOCOL = "healthporta.ptg-small.exact-wave.v1"
_SERIALIZER = "arq-0.28.process-msgpack.v1"


def _wave(**overrides):
    wave_field_map = {
        "wave_id": "wave-unit", "wave_digest": _WAVE, "intent_count": 2,
        "state": "awaiting_linkage", "kubernetes_manifest_identity": _MANIFEST,
        "kubernetes_manifest": {"metadata": {"name": "ptg-wave-unit"}},
        "pinned_image_reference": f"registry/unit@sha256:{_PIN}",
        "pinned_image_digest": _PIN, "runtime_image_identity": _RUNTIME,
        "kubernetes_config_identity": _CONFIG, "kubernetes_job_uid": None,
        "kubernetes_job_receipt_digest": None,
        "kubernetes_ready_attestation": None,
        "release_queue": f"arq:PTGSmall:wave:{_WAVE}", "jobs_digest": _JOBS,
        "manifest_digest": "a" * 64,
        "protocol_identity": _PROTOCOL, "serializer_identity": _SERIALIZER,
        "redis_release_attestation": None, "redis_release_attestation_digest": None,
        "k8s_post_ticket": "post-ticket", "redis_release_ticket": None,
        "redis_cleanup_ticket": None, "redis_cleanup_evidence_digest": None,
        "kubernetes_delete_ticket": None, "kubernetes_delete_evidence_digest": None,
        "outcomes_digest": "8" * 64, "failure_receipt": None,
        "failure_receipt_digest": None, "linkage_ack": None, "linkage_ack_digest": None,
    }
    wave_field_map.update(overrides)
    return types.SimpleNamespace(**wave_field_map)


def _ready_attestation():
    return {
        "slots": [
            {
                "slot": slot, "pod_uid": f"pod-{slot}",
                "runtime_image_identity": _RUNTIME,
            }
            for slot in range(12)
        ]
    }


def _records():
    intents = [
        types.SimpleNamespace(
            ordinal=ordinal, run_id=f"run-{ordinal}", job_id=f"job-{ordinal}",
            source_file_import_id=f"source-{ordinal}", content_version="v1",
        )
        for ordinal in range(2)
    ]
    outcomes = [
        types.SimpleNamespace(
            ordinal=item.ordinal, run_id=item.run_id, job_id=item.job_id,
            source_file_import_id=item.source_file_import_id,
            content_version=item.content_version, status="dead_letter",
            snapshot_id=None, import_id=None,
        )
        for item in intents
    ]
    return intents, outcomes


def _failure_receipt(wave, *, reason="kubernetes_post_absent"):
    if reason == "kubernetes_post_absent":
        operation = "kubernetes_post"
        ticket = wave.k8s_post_ticket
        failure_evidence_map = {
            "wave_digest": wave.wave_digest,
            "manifest_identity": wave.kubernetes_manifest_identity,
            "job_name": "ptg-wave-unit", "job_absent": True,
            "pod_count": 0, "pods_absent": True,
        }
        origin_state = "slots_waiting"
    elif reason == "redis_release_absent":
        operation = "redis_release"
        ticket = wave.redis_release_ticket
        failure_evidence_map = {
            "wave_digest": wave.wave_digest, "release_queue": wave.release_queue,
            "jobs_digest": wave.jobs_digest, "job_count": wave.intent_count,
            "queue_empty": True, "payload_keys_empty": True, "retry_empty": True,
            "in_progress_empty": True, "health_check_empty": True,
        }
        origin_state = "redis_releasing"
    else:
        operation = "worker_start"
        ticket = None
        failed_slots = [
            {
                "slot": slot["slot"], "pod_uid": slot["pod_uid"],
                "phase": "Failed", "runtime_image_identity": _RUNTIME,
            }
            for slot in wave.kubernetes_ready_attestation["slots"]
        ]
        failure_evidence_map = {
            "schema_version": "healthporta.ptg-wave.kubernetes-preclaim-failure.v1",
            "wave_digest": wave.wave_digest, "queue": wave.release_queue,
            "manifest_digest": wave.manifest_digest, "jobs_digest": wave.jobs_digest,
            "job_count": wave.intent_count, "config_identity": _CONFIG,
            "manifest_identity": _MANIFEST, "image_identity": wave.pinned_image_reference,
            "runtime_image_identity": _RUNTIME, "job_name": "ptg-wave-unit",
            "job_uid": wave.kubernetes_job_uid, "backoff_limit": 0,
            "job_active": 0, "job_failed": 12, "job_succeeded": 0,
            "job_failure_condition": {"type": "Failed", "status": "True"},
            "failed_slots": failed_slots,
        }
        failure_evidence_map["attestation_digest"] = sha256_digest(
            canonical_json(failure_evidence_map)
        )
        origin_state = "released"
    return {
        "schema_version": _FAILURE_SCHEMA, "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest, "origin_state": origin_state, "reason": reason,
        "operation": operation,
        "operation_ticket": ticket,
        "evidence": failure_evidence_map,
        "evidence_digest": sha256_digest(canonical_json(failure_evidence_map)),
        "unclaimed_ordinals_digest": sha256_digest(canonical_json({
            "schema_version": 1, "wave_id": wave.wave_id, "ordinals": [0, 1],
        })),
    }


def _link_wave(wave, outcomes):
    records = [{
        "ordinal": outcome.ordinal, "run_id": outcome.run_id,
        "job_id": outcome.job_id,
        "source_file_import_id": outcome.source_file_import_id,
        "content_version": outcome.content_version, "status": outcome.status,
        "snapshot_id": outcome.snapshot_id, "import_id": outcome.import_id,
    } for outcome in outcomes]
    wave.outcomes_digest = _outcomes_digest(records)
    unsigned_ack_map = {
        "schema_version": "healthporta.ptg-wave-linkage-ack.v1", "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest, "intent_count": wave.intent_count,
        "mapping_digest": linkage_mapping_digest(outcomes),
        "outcomes_digest": wave.outcomes_digest,
    }
    linkage_ack_map = {
        **unsigned_ack_map,
        "signature": sign_linkage_ack(unsigned_ack_map, key=_KEY),
    }
    wave.linkage_ack = linkage_ack_map
    wave.linkage_ack_digest = sha256_digest(canonical_json(linkage_ack_map))


def _absent_terminal_receipt(wave):
    redis_receipt_map = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
        "wave_id": wave.wave_digest, "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest, "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count, "target_key_count": 4 + (4 * wave.intent_count),
        "ready_slots": [], "ready_slots_digest": sha256_digest(canonical_json([])),
        "release_present": False, "release_digest": None, "release_receipt": None,
        "queued_ordinals": [], "job_ordinals": [], "result_ordinals": [],
        "retry_ordinals": [], "in_progress_ordinals": [], "health_check_present": False,
    }
    redis_receipt_map["attestation_digest"] = sha256_digest(
        canonical_json(redis_receipt_map)
    )
    return {
        "kubernetes": dict(wave.failure_receipt["evidence"]),
        "redis": redis_receipt_map,
    }


def test_unreceipted_external_tickets_have_only_get_recovery_actions():
    wave = _wave(
        state="cleaning", redis_cleanup_ticket="cleanup-ticket",
        kubernetes_delete_ticket="delete-ticket",
    )
    plan = read_only_recovery_plan(wave)
    assert plan is not None
    assert plan.operation == "kubernetes_delete"
    assert plan.required_observation == "get_exact_job_and_labeled_pods_absence"
    assert plan.mutation_permitted is False

    wave.kubernetes_delete_evidence_digest = "b" * 64
    plan = read_only_recovery_plan(wave)
    assert plan is not None
    assert plan.operation == "redis_cleanup"
    assert plan.mutation_permitted is False

    wave.redis_cleanup_evidence_digest = "c" * 64
    wave.redis_release_ticket = "release-ticket"
    plan = read_only_recovery_plan(wave)
    assert plan is not None
    assert plan.operation == "redis_release"
    assert plan.required_observation == "get_exact_release_receipt"


def test_get_proven_absent_post_can_dead_letter_all_unclaimed_outcomes():
    intents, outcomes = _records()
    wave = _wave()
    failure = _failure_receipt(wave)
    wave.failure_receipt = failure
    wave.failure_receipt_digest = sha256_digest(canonical_json(failure))
    _link_wave(wave, outcomes)

    evidence = verify_unclaimed_dead_letter_terminal_eligibility(
        wave, intents, [], outcomes, _absent_terminal_receipt(wave), key=_KEY,
    )
    assert evidence["failure_receipt_digest"] == wave.failure_receipt_digest
    assert evidence["kubernetes"]["job_absent"] is True


def test_preclaim_backofflimit_zero_failure_never_becomes_a_success():
    intents, outcomes = _records()
    wave = _wave(
        kubernetes_job_uid="job-unit", kubernetes_job_receipt_digest="d" * 64,
        kubernetes_ready_attestation=_ready_attestation(),
        redis_release_attestation={"release_digest": _RELEASE},
        redis_release_attestation_digest="e" * 64,
    )
    failure = _failure_receipt(wave, reason="pre_claim_failure")
    wave.failure_receipt = failure
    wave.failure_receipt_digest = sha256_digest(canonical_json(failure))
    _link_wave(wave, outcomes)
    terminal_receipt_map = {
        "kubernetes": dict(failure["evidence"]),
        "redis": _released_terminal_redis_receipt(wave),
    }
    evidence = verify_unclaimed_dead_letter_terminal_eligibility(
        wave, intents, [], outcomes, terminal_receipt_map, key=_KEY,
    )
    assert evidence["redis_pre_cleanup"]["job_ordinals"] == [0, 1]

    terminal_receipt_map["redis"]["job_ordinals"] = [0]
    terminal_receipt_map["redis"]["attestation_digest"] = sha256_digest(canonical_json({
        key: receipt_field_value
        for key, receipt_field_value in terminal_receipt_map["redis"].items()
        if key != "attestation_digest"
    }))
    with pytest.raises(PTGWaveFailureConflict, match="released Redis failure receipt differs"):
        verify_unclaimed_dead_letter_terminal_eligibility(
            wave, intents, [], outcomes, terminal_receipt_map, key=_KEY,
        )

    outcomes[0].status = "succeeded"
    with pytest.raises(PTGWaveFailureConflict, match="dead letter"):
        verify_unclaimed_dead_letter_terminal_eligibility(
            wave, intents, [], outcomes, terminal_receipt_map, key=_KEY,
        )


@pytest.mark.parametrize("ready_count", [0, 5])
def test_unreleased_slots_waiting_preclaim_accepts_only_ready_kubernetes_subset(ready_count):
    intents, outcomes = _records()
    wave = _wave(
        kubernetes_job_uid="job-unit", kubernetes_job_receipt_digest="d" * 64,
        kubernetes_ready_attestation=_ready_attestation(),
    )
    failure = _failure_receipt(wave, reason="pre_claim_failure")
    failure["origin_state"] = "slots_waiting"
    wave.failure_receipt = failure
    wave.failure_receipt_digest = sha256_digest(canonical_json(failure))
    _link_wave(wave, outcomes)
    terminal_receipt_map = _absent_terminal_receipt(wave)
    ready_slots = _expected_redis_ready_slots(wave)[:ready_count]
    terminal_receipt_map["redis"]["ready_slots"] = ready_slots
    terminal_receipt_map["redis"]["ready_slots_digest"] = sha256_digest(
        canonical_json(ready_slots)
    )
    terminal_receipt_map["redis"]["attestation_digest"] = sha256_digest(canonical_json({
        key: receipt_field_value
        for key, receipt_field_value in terminal_receipt_map["redis"].items()
        if key != "attestation_digest"
    }))

    evidence = verify_unclaimed_dead_letter_terminal_eligibility(
        wave, intents, [], outcomes, terminal_receipt_map, key=_KEY,
    )
    assert evidence["redis_pre_cleanup"]["ready_slots"] == ready_slots

    if ready_slots:
        terminal_receipt_map["redis"]["ready_slots"] = list(
            reversed(ready_slots)
        )
    else:
        foreign_pod_map = dict(_expected_redis_ready_slots(wave)[0])
        foreign_pod_map["pod_uid"] = "foreign-pod"
        terminal_receipt_map["redis"]["ready_slots"] = [foreign_pod_map]
    terminal_receipt_map["redis"]["ready_slots_digest"] = sha256_digest(
        canonical_json(terminal_receipt_map["redis"]["ready_slots"])
    )
    terminal_receipt_map["redis"]["attestation_digest"] = sha256_digest(canonical_json({
        key: receipt_field_value
        for key, receipt_field_value in terminal_receipt_map["redis"].items()
        if key != "attestation_digest"
    }))
    with pytest.raises(PTGWaveFailureConflict, match="canonical Kubernetes subset"):
        verify_unclaimed_dead_letter_terminal_eligibility(
            wave, intents, [], outcomes, terminal_receipt_map, key=_KEY,
        )


def test_partial_ready_membership_is_rejected_after_release():
    intents, outcomes = _records()
    wave = _wave(
        kubernetes_job_uid="job-unit", kubernetes_job_receipt_digest="d" * 64,
        kubernetes_ready_attestation=_ready_attestation(),
        redis_release_attestation={"release_digest": _RELEASE},
        redis_release_attestation_digest="e" * 64,
    )
    failure = _failure_receipt(wave, reason="pre_claim_failure")
    wave.failure_receipt = failure
    wave.failure_receipt_digest = sha256_digest(canonical_json(failure))
    _link_wave(wave, outcomes)
    terminal_receipt_map = {
        "kubernetes": dict(failure["evidence"]),
        "redis": _released_terminal_redis_receipt(wave),
    }
    ready_slots = _expected_redis_ready_slots(wave)[:5]
    terminal_receipt_map["redis"]["ready_slots"] = ready_slots
    terminal_receipt_map["redis"]["ready_slots_digest"] = sha256_digest(
        canonical_json(ready_slots)
    )
    terminal_receipt_map["redis"]["attestation_digest"] = sha256_digest(canonical_json({
        key: receipt_field_value
        for key, receipt_field_value in terminal_receipt_map["redis"].items()
        if key != "attestation_digest"
    }))

    with pytest.raises(PTGWaveFailureConflict, match="differs from Kubernetes"):
        verify_unclaimed_dead_letter_terminal_eligibility(
            wave, intents, [], outcomes, terminal_receipt_map, key=_KEY,
        )


def test_unclaimed_failure_rejects_a_worker_claim_or_active_redis_work():
    intents, outcomes = _records()
    wave = _wave()
    failure = _failure_receipt(wave)
    wave.failure_receipt = failure
    wave.failure_receipt_digest = sha256_digest(canonical_json(failure))
    _link_wave(wave, outcomes)
    terminal_receipt_map = _absent_terminal_receipt(wave)
    with pytest.raises(PTGWaveFailureConflict, match="zero worker claims"):
        verify_unclaimed_dead_letter_terminal_eligibility(
            wave, intents, [types.SimpleNamespace(ordinal=0)], outcomes,
            terminal_receipt_map,
            key=_KEY,
        )
    terminal_receipt_map["redis"]["job_ordinals"] = [0]
    terminal_receipt_map["redis"]["attestation_digest"] = sha256_digest(canonical_json({
        key: receipt_field_value
        for key, receipt_field_value in terminal_receipt_map["redis"].items()
        if key != "attestation_digest"
    }))
    with pytest.raises(PTGWaveFailureConflict, match="unreleased Redis failure receipt is not absent"):
        verify_unclaimed_dead_letter_terminal_eligibility(
            wave, intents, [], outcomes, terminal_receipt_map, key=_KEY,
        )


def _released_terminal_redis_receipt(wave):
    ready_slots = _expected_redis_ready_slots(wave)
    release_receipt = _expected_redis_release_mapping(wave, ready_slots)
    release_digest = sha256_digest(canonical_json(release_receipt))
    wave.redis_release_attestation = {"release_digest": release_digest}
    redis_receipt_map = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
        "wave_id": wave.wave_digest, "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest, "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count, "target_key_count": 4 + (4 * wave.intent_count),
        "ready_slots": ready_slots,
        "ready_slots_digest": sha256_digest(canonical_json(ready_slots)),
        "release_present": True, "release_digest": release_digest,
        "release_receipt": release_receipt, "queued_ordinals": [0, 1],
        "job_ordinals": [0, 1], "result_ordinals": [], "retry_ordinals": [],
        "in_progress_ordinals": [], "health_check_present": True,
    }
    redis_receipt_map["attestation_digest"] = sha256_digest(
        canonical_json(redis_receipt_map)
    )
    return redis_receipt_map
