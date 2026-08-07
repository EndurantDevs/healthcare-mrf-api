# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned Kubernetes exact-wave contract edges."""

from __future__ import annotations

from tests.test_ptg_wave_kubernetes_contract_edges import (
    _ShortIterationList,
    _actual_job,
    _contract,
    _failed_job,
    _failed_pods,
    _initial_attestation,
    _manifest,
    _terminal_initial_attestation,
    _terminal_job,
    _terminal_pods,
    copy,
    failure,
    kubernetes,
    live,
    pytest,
    replace,
    terminal,
)


@pytest.mark.parametrize(
    ("status", "message"),
    [
        ({"active": 0, "succeeded": 0}, "failed count is missing"),
        (
            {
                "active": 0,
                "failed": 12,
                "succeeded": 0,
                "conditions": [{"type": "", "status": "True"}],
            },
            "type is invalid",
        ),
        (
            {
                "active": 0,
                "failed": 12,
                "succeeded": 0,
                "conditions": [{"type": "Failed", "status": "invalid"}],
            },
            "status is invalid",
        ),
        (
            {
                "active": 0,
                "failed": 12,
                "succeeded": 0,
                "conditions": [
                    {"type": "Failed", "status": "True"},
                    {"type": "Failed", "status": "False"},
                ],
            },
            "repeat a type",
        ),
        (
            {
                "active": 0,
                "failed": 12,
                "succeeded": 0,
                "conditions": [{"type": "Failed", "status": "False"}],
            },
            "true Failed",
        ),
    ],
)
def test_preclaim_failure_status_requires_one_exact_failed_condition(status, message):
    with pytest.raises(kubernetes.PTGWaveContractError, match=message):
        failure._attest_failed_job_status({"status": status})

def test_preclaim_failure_rejects_retry_duplicate_members_and_short_iteration():
    manifest, contract = _contract()

    retried = copy.deepcopy(manifest)
    retried["spec"]["backoffLimit"] = 1
    with pytest.raises(kubernetes.PTGWaveContractError, match="backoffLimit zero"):
        failure._require_no_retry_manifest(retried)

    duplicate = _failed_pods(manifest)
    duplicate[1]["metadata"]["uid"] = duplicate[0]["metadata"]["uid"]
    with pytest.raises(kubernetes.PTGWaveContractError, match="unique indexes and UIDs"):
        failure._attest_failed_pods(
            contract,
            duplicate,
            job_name=manifest["metadata"]["name"],
            job_uid=_actual_job(manifest)["metadata"]["uid"],
        )

    with pytest.raises(kubernetes.PTGWaveContractError, match="cover indexes"):
        failure._attest_failed_pods(
            contract,
            _ShortIterationList(_failed_pods(manifest)),
            job_name=manifest["metadata"]["name"],
            job_uid=_actual_job(manifest)["metadata"]["uid"],
        )

@pytest.mark.parametrize(
    ("worker_status", "message"),
    [
        (
            {"ready": False, "restartCount": 0, "state": {"terminated": {}}},
            "non-zero integer",
        ),
        (
            {
                "ready": False,
                "restartCount": 0,
                "state": {"terminated": {"exitCode": 1}, "waiting": {}},
            },
            "must be terminated",
        ),
    ],
)
def test_preclaim_failure_worker_status_requires_only_a_nonzero_termination(
    worker_status,
    message,
):
    with pytest.raises(kubernetes.PTGWaveContractError, match=message):
        failure._require_failed_worker_status(worker_status)

def test_terminal_evidence_mapping_is_canonical_and_digest_is_exposed():
    manifest, contract = _contract()
    initial = _initial_attestation(manifest)
    attestation = terminal._terminal_attestation(
        contract,
        initial,
        tuple(range(12)),
        dict(initial.pod_uid_by_slot),
    )

    evidence = attestation.evidence_mapping()
    assert evidence["completed_slots"] == list(range(12))
    assert evidence["slots"] == [
        {"slot": slot, "pod_uid": f"pod-uid-{slot}", "phase": "Succeeded"}
        for slot in range(12)
    ]
    assert attestation.as_mapping()["attestation_digest"] == attestation.attestation_digest

def test_terminal_initial_membership_rejects_missing_or_repeated_pods():
    manifest, contract = _contract()
    initial = _initial_attestation(manifest)

    missing = replace(initial, pod_uid_by_slot={0: "pod-uid-0"})
    with pytest.raises(kubernetes.PTGWaveContractError, match="exactly twelve"):
        terminal._require_initial_attestation(contract, missing)

    repeated = replace(
        initial,
        pod_uid_by_slot={slot: "same-pod" for slot in range(12)},
    )
    with pytest.raises(kubernetes.PTGWaveContractError, match="must be unique"):
        terminal._require_initial_attestation(contract, repeated)

@pytest.mark.parametrize(
    ("completed_indexes", "message"),
    [
        ("01", "non-canonical"),
        ("12", "out-of-range"),
        ("1-0", "range is invalid"),
        ("0-1-2", "shape is invalid"),
    ],
)
def test_terminal_completed_indexes_are_canonical_ordered_and_nonoverlapping(
    completed_indexes,
    message,
):
    with pytest.raises(kubernetes.PTGWaveContractError, match=message):
        terminal._completed_slots(completed_indexes)

@pytest.mark.parametrize(
    "status",
    [
        {"active": True},
        {"active": -1},
        {"active": "0"},
    ],
)
def test_terminal_job_counts_reject_boolean_negative_and_text_values(status):
    with pytest.raises(kubernetes.PTGWaveContractError, match="count is invalid"):
        terminal._job_status_count(status, "active")

def test_terminal_pods_reject_incomplete_iteration_even_when_length_is_twelve():
    manifest, contract = _contract()
    terminal_job = _terminal_job(manifest)
    with pytest.raises(kubernetes.PTGWaveContractError, match="cover indexes"):
        terminal._attest_terminal_pods(
            contract,
            _ShortIterationList(_terminal_pods(manifest)),
            job_name=manifest["metadata"]["name"],
            job_uid=terminal_job["metadata"]["uid"],
        )

def test_terminal_public_attestation_rejects_replaced_initial_membership():
    manifest = _manifest()
    initial = _terminal_initial_attestation(manifest)
    replaced = replace(
        initial,
        pod_uid_by_slot={slot: f"replaced-{slot}" for slot in range(12)},
    )

    with pytest.raises(kubernetes.PTGWaveContractError, match="membership differs"):
        terminal.attest_terminal_ptg_wave_kubernetes_objects(
            manifest,
            replaced,
            _terminal_job(manifest),
            _terminal_pods(manifest),
        )

def test_preclaim_failure_public_attestation_rejects_changed_initial_membership():
    manifest = _manifest()
    initial = _initial_attestation(manifest)
    replaced = replace(
        initial,
        pod_uid_by_slot={slot: f"replaced-{slot}" for slot in range(12)},
    )

    with pytest.raises(kubernetes.PTGWaveContractError, match="membership differs"):
        failure.attest_preclaim_failure_kubernetes(
            manifest,
            replaced,
            _failed_job(manifest),
            _failed_pods(manifest),
        )

def test_wave_manifest_rejects_missing_worker_container():
    manifest = _manifest()
    manifest["spec"]["template"]["spec"]["containers"] = []
    with pytest.raises(kubernetes.PTGWaveContractError, match="one worker container"):
        kubernetes.validate_ptg_wave_job_manifest(manifest)

def test_failed_job_status_count_uses_explicit_default():
    assert failure._job_status_count({}, "active", default=0) == 0

def test_live_attestation_requires_exactly_twelve_pods():
    manifest = _manifest()
    with pytest.raises(kubernetes.PTGWaveContractError, match="exactly twelve"):
        live.attest_ptg_wave_kubernetes_objects(
            manifest,
            _actual_job(manifest),
            [],
        )
