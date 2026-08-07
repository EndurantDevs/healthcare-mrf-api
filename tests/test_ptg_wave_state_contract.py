"""Pure contract tests for durable exact PTG wave state and terminal proof."""

from __future__ import annotations

import types
import inspect

import pytest

from process.ptg_wave_outcomes import (
    PTGWaveOutcomeConflict,
    linkage_mapping_digest,
    sign_linkage_ack,
    verify_terminal_eligibility,
)
from process.ptg_wave_claims import _text, claim_wave_job_start
from process.ptg_wave_state import (
    PTGWaveStateConflict,
    _validate_materialization,
    _validate_ready_receipt,
    assert_transition,
    canonical_json,
    operation_ticket_owner,
    sha256_digest,
)


_PIN = "1" * 64
_RUNTIME = "sha256:" + "2" * 64
_CONFIG = "3" * 64
_MANIFEST = "4" * 64
_WAVE = "5" * 64
_JOBS = "6" * 64
_RELEASE = "7" * 64


def _wave(**overrides):
    slots = [
        {"slot": slot, "pod_uid": f"pod-{slot}", "runtime_image_identity": _RUNTIME}
        for slot in range(12)
    ]
    wave_field_map = {
        "wave_id": "wave-unit", "wave_digest": _WAVE, "intent_count": 2,
        "kubernetes_manifest_sha256": "a" * 64, "kubernetes_manifest_identity": _MANIFEST,
        "pinned_image_reference": f"registry/unit@sha256:{_PIN}",
        "pinned_image_digest": _PIN, "runtime_image_identity": _RUNTIME,
        "kubernetes_config_identity": _CONFIG, "kubernetes_job_uid": "job-unit",
        "kubernetes_manifest": {"metadata": {"name": "ptg-wave-unit"}},
        "kubernetes_ready_attestation": {"slots": slots}, "release_queue": f"arq:PTGSmall:wave:{_WAVE}",
        "jobs_digest": _JOBS, "manifest_digest": "c" * 64,
        "redis_release_attestation": {"release_digest": _RELEASE},
        "outcomes_digest": None, "linkage_ack": None, "linkage_ack_digest": None,
    }
    wave_field_map.update(overrides)
    return types.SimpleNamespace(**wave_field_map)


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
            content_version=item.content_version, status="succeeded", snapshot_id=f"snapshot-{item.ordinal}",
            import_id=item.source_file_import_id,
        )
        for item in intents
    ]
    claims = [
        types.SimpleNamespace(
            wave_id="wave-unit", ordinal=item.ordinal, run_id=item.run_id, job_id=item.job_id,
            kubernetes_job_uid="job-unit", manifest_identity=_MANIFEST,
            pinned_image_reference=f"registry/unit@sha256:{_PIN}",
            pinned_image_digest=_PIN, runtime_image_identity=_RUNTIME, config_identity=_CONFIG,
            slot=item.ordinal, pod_uid=f"pod-{item.ordinal}", claim_status="started",
            failure_code=None, claim_attempt_token=f"{item.ordinal + 1:032x}",
        )
        for item in intents
    ]
    return intents, claims, outcomes


def test_transition_path_disallows_skips_and_uncertainty_resume_drift():
    state_names = [
        "admitted", "materializing", "slots_waiting", "redis_releasing", "released",
        "executing", "awaiting_linkage", "terminalizing", "cleaning", "succeeded",
    ]
    for current, target in zip(state_names, state_names[1:]):
        assert_transition(current, target)
    with pytest.raises(PTGWaveStateConflict, match="invalid"):
        assert_transition("admitted", "slots_waiting")
    assert_transition("slots_waiting", "uncertain")
    with pytest.raises(PTGWaveStateConflict, match="only resume"):
        assert_transition("uncertain", "released", resume_state="slots_waiting")
    assert_transition("uncertain", "slots_waiting", resume_state="slots_waiting")
    # Atomic Redis publication wakes workers before the release receipt can be
    # committed, so a first claim may race the controller's DB transition.
    assert_transition("redis_releasing", "executing")


def test_materialization_requires_a_digest_pinned_reference_and_exact_bytes():
    manifest_map = {"apiVersion": "batch/v1", "kind": "Job"}
    manifest_bytes = canonical_json(manifest_map)
    saved, digest = _validate_materialization(
        manifest_map,
        manifest_bytes,
        f"registry/unit@sha256:{_PIN}",
        _PIN,
        _RUNTIME,
        _CONFIG,
        _MANIFEST,
    )
    assert saved == manifest_map
    assert digest == sha256_digest(manifest_bytes)
    with pytest.raises(PTGWaveStateConflict, match="pinned"):
        _validate_materialization(
            manifest_map,
            manifest_bytes,
            "registry/unit:stable",
            _PIN,
            _RUNTIME,
            _CONFIG,
            _MANIFEST,
        )
    with pytest.raises(PTGWaveStateConflict, match="differ"):
        _validate_materialization({"kind": "Other"}, manifest_bytes, f"registry/unit@sha256:{_PIN}", _PIN, _RUNTIME, _CONFIG, _MANIFEST)


def test_ready_receipt_binds_all_runtime_slots():
    wave = _wave()
    ready_receipt_map = {
        "wave_digest": _WAVE, "job_uid": "job-unit", "manifest_identity": _MANIFEST,
        "config_identity": _CONFIG, "pinned_image_reference": f"registry/unit@sha256:{_PIN}",
        "pinned_image_digest": _PIN, "runtime_image_identity": _RUNTIME,
        "slots": wave.kubernetes_ready_attestation["slots"],
    }
    assert _validate_ready_receipt(wave, ready_receipt_map) == ready_receipt_map
    ready_receipt_map["slots"] = ready_receipt_map["slots"][:-1]
    with pytest.raises(PTGWaveStateConflict, match="exactly 12"):
        _validate_ready_receipt(wave, ready_receipt_map)


def test_worker_claim_cannot_accept_a_caller_supplied_kubernetes_job_uid():
    assert "kubernetes_job_uid" not in inspect.signature(claim_wave_job_start).parameters
    with pytest.raises(Exception, match="non-empty bounded"):
        _text(" pod ", "pod_uid", 128)


def test_operation_ticket_has_one_mutating_owner_on_replay():
    ticket = "controller-ticket-1"
    assert operation_ticket_owner(None, ticket) is True
    assert operation_ticket_owner(ticket, ticket) is False
    assert operation_ticket_owner(ticket, "later-controller-ticket") is False


def _terminal_outcome_records(outcomes):
    return [
        {
            "ordinal": outcome_entry.ordinal,
            "run_id": outcome_entry.run_id,
            "job_id": outcome_entry.job_id,
            "source_file_import_id": outcome_entry.source_file_import_id,
            "content_version": outcome_entry.content_version,
            "status": outcome_entry.status,
            "snapshot_id": outcome_entry.snapshot_id,
            "import_id": outcome_entry.import_id,
        }
        for outcome_entry in outcomes
    ]


def _terminal_outcomes_digest(outcomes):
    return sha256_digest(canonical_json({
        "domain": "healthporta.ptg-wave.outcomes.v1",
        "records": _terminal_outcome_records(outcomes),
    }))


def _install_linkage_ack(wave, outcomes, key):
    unsigned_ack_map = {
        "schema_version": "healthporta.ptg-wave-linkage-ack.v1",
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "intent_count": wave.intent_count,
        "mapping_digest": linkage_mapping_digest(outcomes),
        "outcomes_digest": wave.outcomes_digest,
    }
    ack_map = {
        **unsigned_ack_map,
        "signature": sign_linkage_ack(unsigned_ack_map, key=key),
    }
    wave.linkage_ack = ack_map
    wave.linkage_ack_digest = sha256_digest(canonical_json(ack_map))


def _terminal_receipt_map(wave):
    kubernetes_receipt_map = {
        "schema_version": 1, "wave_digest": wave.wave_digest,
        "queue": wave.release_queue, "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest, "job_count": wave.intent_count,
        "config_identity": _CONFIG, "manifest_identity": _MANIFEST,
        "image_identity": wave.pinned_image_reference,
        "runtime_image_identity": _RUNTIME, "job_name": "ptg-wave-unit",
        "job_uid": wave.kubernetes_job_uid, "completed_slots": list(range(12)),
        "slots": [
            {
                "slot": slot_entry["slot"],
                "pod_uid": slot_entry["pod_uid"],
                "phase": "Succeeded",
            }
            for slot_entry in wave.kubernetes_ready_attestation["slots"]
        ],
    }
    kubernetes_receipt_map["attestation_digest"] = sha256_digest(
        canonical_json(kubernetes_receipt_map)
    )
    redis_receipt_map = {
        "schema_version": 1, "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue, "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest, "job_count": wave.intent_count,
        "image_identity": wave.pinned_image_reference, "release_digest": _RELEASE,
        "target_key_count": 4 + 4 * wave.intent_count,
        "queue_entry_count": 0, "job_payload_count": 0, "result_count": 2,
        "retry_count": 0, "in_progress_count": 0, "health_check_count": 1,
        "result_presence_digest": "b" * 64,
    }
    redis_receipt_map["attestation_digest"] = sha256_digest(
        canonical_json(redis_receipt_map)
    )
    return {
        "kubernetes": kubernetes_receipt_map,
        "redis": redis_receipt_map,
    }


def test_terminal_verifier_requires_exact_terminal_evidence():
    """Reject terminal proof without all durable exact witnesses."""

    intents, claims, outcomes = _records()
    wave = _wave(outcomes_digest=_terminal_outcomes_digest(outcomes))
    key = b"unit-linkage-key"
    _install_linkage_ack(wave, outcomes, key)
    terminal_receipt_map = _terminal_receipt_map(wave)
    evidence = verify_terminal_eligibility(
        wave,
        intents,
        claims,
        outcomes,
        terminal_receipt_map,
        key=key,
    )
    assert evidence["linkage_ack_digest"] == wave.linkage_ack_digest
    assert evidence["rejected_ordinals"] == []
    assert evidence["rejected_ordinals_digest"] == sha256_digest(canonical_json({
        "schema_version": 1, "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest, "rejected_ordinals": [],
    }))
    outcomes[1].snapshot_id = None
    with pytest.raises(PTGWaveOutcomeConflict):
        verify_terminal_eligibility(
            wave,
            intents,
            claims,
            outcomes,
            terminal_receipt_map,
            key=key,
        )


def test_terminal_verifier_accepts_a_rejected_claim_only_for_its_failed_outcome():
    """Accept only the matching rejected-claim terminal outcome."""

    intents, claims, outcomes = _records()
    claims[1].claim_status = "rejected"
    claims[1].failure_code = "ptg_exact_wave_claim_rejected"
    outcomes[1].status = "failed"
    outcomes[1].snapshot_id = None
    outcomes[1].import_id = None
    wave = _wave(outcomes_digest=_terminal_outcomes_digest(outcomes))
    key = b"unit-linkage-key"
    _install_linkage_ack(wave, outcomes, key)
    terminal_receipt_map = _terminal_receipt_map(wave)
    evidence = verify_terminal_eligibility(
        wave,
        intents,
        claims,
        outcomes,
        terminal_receipt_map,
        key=key,
    )
    assert evidence["rejected_ordinals"] == [1]

    outcomes[1].status = "canceled"
    with pytest.raises(PTGWaveOutcomeConflict, match="claim disposition"):
        verify_terminal_eligibility(
            wave,
            intents,
            claims,
            outcomes,
            terminal_receipt_map,
            key=key,
        )
