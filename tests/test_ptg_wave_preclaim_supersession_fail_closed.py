"""Fail-closed edge coverage for logical pre-claim supersession evidence."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import ptg_wave_preclaim_supersession as supersession
from process.ptg_wave_preclaim_supersession import (
    PTGWavePreclaimObservation,
    PTGWavePreclaimSupersessionConflict,
    validate_logical_preclaim_supersession_proof,
)
from process.ptg_wave_state import sha256_digest
from tests.test_ptg_wave_preclaim_supersession import (
    _actual_job,
    _attest,
    _empty_redis_attestation,
    _intents_and_runs,
    _wave,
)


def _observation() -> PTGWavePreclaimObservation:
    wave = _wave()
    intents, runs = _intents_and_runs(wave)
    return PTGWavePreclaimObservation(
        predecessor_wave=wave,
        intents=intents,
        runs=runs,
        claims=[],
        outcomes=[],
        worker_start_event_ordinals=[],
        actual_job=_actual_job(wave.kubernetes_manifest),
        redis_unclaimed_attestation=_empty_redis_attestation(wave),
    )


@pytest.mark.parametrize(
    ("path", "value", "message"),
    (
        (("extra",), True, "fields are not exact"),
        (("schema_version",), "unsupported", "unsupported"),
        (("schema_version",), 1, "must be text"),
        (("successor_wave_id",), "predecessor-wave", "must differ"),
        (("predecessor", "intent_count"), 0, "intent count is invalid"),
        (("kubernetes", "failed"), 11, "Kubernetes proof is not exact"),
        (("redis", "queued_ordinal_count"), 1, "Redis proof is not empty"),
        (("predecessor",), {}, "fields are not exact"),
        (("predecessor", "wave_digest"), "g" * 64, "lowercase SHA-256"),
        (("predecessor", "wave_id"), "", "wave ID is invalid"),
    ),
)
def test_proof_contract_rejects_every_malformed_layer(path, value, message):
    proof_map = _attest().as_mapping()
    target_map = proof_map
    for field_name in path[:-1]:
        target_map = target_map[field_name]
    target_map[path[-1]] = value

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match=message):
        validate_logical_preclaim_supersession_proof(proof_map)


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    (
        ("state", "slots_waiting", "uncertain"),
        ("k8s_post_started_at", None, "start receipt"),
        ("worker_limit", 11, "twelve workers"),
    ),
)
def test_predecessor_boundary_rejects_inexact_state(field_name, value, message):
    wave = _wave()
    setattr(wave, field_name, value)

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match=message):
        supersession._require_predecessor_preclaim_boundary(wave)


def _drop_intent(intents, _runs) -> None:
    intents.pop()


def _duplicate_ordinal(intents, _runs) -> None:
    intents[-1].ordinal = 0


def _move_intent_to_another_wave(intents, _runs) -> None:
    intents[0].wave_id = "other-wave"


def _remove_intent_run_id(intents, _runs) -> None:
    intents[0].run_id = ""


def _remove_run_identity(_intents, runs) -> None:
    runs[0].run_id = None


def _change_run_set(_intents, runs) -> None:
    runs[0].run_id = "other-run"


def _make_ordinal_negative(intents, _runs) -> None:
    intents[0].ordinal = -1


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (_drop_intent, "every admitted intent"),
        (_duplicate_ordinal, "contiguous ordinals"),
        (_move_intent_to_another_wave, "another wave"),
        (_remove_intent_run_id, "intent identities"),
        (_remove_run_identity, "ImportRun identities"),
        (_change_run_set, "exactly match"),
        (_make_ordinal_negative, "ordinal is invalid"),
    ),
)
def test_intent_and_run_partition_rejects_inexact_membership(mutate, message):
    wave = _wave()
    intents, runs = _intents_and_runs(wave)
    mutate(intents, runs)

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match=message):
        supersession._require_exact_intents_and_pristine_runs(wave, intents, runs)


def _remove_manifest(wave, _actual_job_map) -> None:
    wave.kubernetes_manifest = None


def _remove_manifest_bytes(wave, _actual_job_map) -> None:
    wave.kubernetes_manifest_bytes = None


def _corrupt_manifest_digest(wave, _actual_job_map) -> None:
    wave.kubernetes_manifest_sha256 = "0" * 64


def _make_manifest_non_json(wave, _actual_job_map) -> None:
    wave.kubernetes_manifest_bytes = b"{"
    wave.kubernetes_manifest_sha256 = sha256_digest(b"{")


def _make_manifest_mapping_differ(wave, _actual_job_map) -> None:
    wave.kubernetes_manifest_bytes = b"{}"
    wave.kubernetes_manifest_sha256 = sha256_digest(b"{}")


def _change_actual_job_spec(_wave_value, actual_job_map) -> None:
    actual_job_map["spec"]["parallelism"] = 11


def _change_durable_wave_identity(wave, _actual_job_map) -> None:
    wave.wave_digest = "f" * 64


def _remove_job_status(_wave_value, actual_job_map) -> None:
    actual_job_map.pop("status")


def _add_completed_indexes(_wave_value, actual_job_map) -> None:
    actual_job_map["status"]["completedIndexes"] = "0"


def _remove_job_conditions(_wave_value, actual_job_map) -> None:
    actual_job_map["status"]["conditions"] = None


def _make_condition_non_mapping(_wave_value, actual_job_map) -> None:
    actual_job_map["status"]["conditions"] = [1]


def _make_condition_status_invalid(_wave_value, actual_job_map) -> None:
    actual_job_map["status"]["conditions"][0]["status"] = "Yes"


def _duplicate_condition_type(_wave_value, actual_job_map) -> None:
    actual_job_map["status"]["conditions"].append(
        {"type": "Failed", "status": "False"}
    )


def _add_true_complete_condition(_wave_value, actual_job_map) -> None:
    actual_job_map["status"]["conditions"].append(
        {"type": "Complete", "status": "True"}
    )


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (_remove_manifest, "lacks its exact desired"),
        (_remove_manifest_bytes, "manifest bytes"),
        (_corrupt_manifest_digest, "bytes are corrupt"),
        (_make_manifest_non_json, "not JSON"),
        (_make_manifest_mapping_differ, "differ from its mapping"),
        (_change_actual_job_spec, "does not exactly attest"),
        (_change_durable_wave_identity, "durable predecessor identity"),
        (_remove_job_status, "terminal status is missing"),
        (_add_completed_indexes, "must not report completed"),
        (_remove_job_conditions, "conditions are missing"),
        (_make_condition_non_mapping, "condition is invalid"),
        (_make_condition_status_invalid, "condition is invalid"),
        (_duplicate_condition_type, "condition is invalid"),
        (_add_true_complete_condition, "true Complete"),
    ),
)
def test_kubernetes_preclaim_evidence_rejects_every_inexact_layer(mutate, message):
    wave = _wave()
    actual_job_map = _actual_job(wave.kubernetes_manifest)
    mutate(wave, actual_job_map)

    with pytest.raises(PTGWavePreclaimSupersessionConflict, match=message):
        supersession._attest_terminal_preclaim_job(wave, actual_job_map)


def test_redis_evidence_requires_exact_fields_and_digest():
    wave = _wave()
    redis_attestation_map = _empty_redis_attestation(wave)
    redis_attestation_map.pop("release_digest")
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="fields"):
        supersession._attest_empty_unclaimed_redis(wave, redis_attestation_map)

    redis_attestation_map = _empty_redis_attestation(wave)
    redis_attestation_map["attestation_digest"] = "0" * 64
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="digest"):
        supersession._attest_empty_unclaimed_redis(wave, redis_attestation_map)


def test_observation_primitives_reject_inexact_sequence_identity_and_count():
    observation = _observation()
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="must differ"):
        supersession.attest_logical_preclaim_supersession(
            observation,
            "predecessor-wave",
        )
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="sequence"):
        supersession.attest_logical_preclaim_supersession(
            replace(observation, intents=None),
            "successor-wave",
        )
    with pytest.raises(PTGWavePreclaimSupersessionConflict, match="integer"):
        supersession._int_attr(SimpleNamespace(intent_count=True), "intent_count")
