

"""Fail-closed edge contracts for the exact twelve-slot Kubernetes wave."""


from __future__ import annotations


import copy


from dataclasses import replace


import pytest


from api import ptg_wave_kubernetes as kubernetes


from api import ptg_wave_kubernetes_attestation as live


from api import ptg_wave_kubernetes_failure_attestation as failure


from api import ptg_wave_kubernetes_terminal_attestation as terminal


from api.ptg_wave_kubernetes_receipt_attestation import (
    attest_ptg_wave_slot_receipts,
)


from tests.test_ptg_wave_kubernetes import _receipts


from tests.test_ptg_wave_kubernetes_failure_attestation import (
    _actual_job,
    _failed_job,
    _failed_pods,
    _initial_attestation,
    _initial_pods,
    _manifest,
)


from tests.test_ptg_wave_kubernetes_terminal import (
    _initial_attestation as _terminal_initial_attestation,
    _terminal_job,
    _terminal_pods,
)


class _ShortIterationList(list):
    """Looks complete to a length check but omits the final member when read."""

    def __iter__(self):
        return iter(list.__getitem__(self, slice(0, 11)))


def _contract():
    manifest = _manifest()
    return manifest, kubernetes.validate_ptg_wave_job_manifest(manifest)


def _contract_values(contract):
    return {
        "wave_digest": contract.wave_digest,
        "queue": contract.queue,
        "manifest_digest": contract.manifest_digest,
        "jobs_digest": contract.jobs_digest,
        "job_count": contract.job_count,
        "config_identity": contract.config_identity,
        "manifest_identity": contract.manifest_identity,
        "runtime_image_identity": contract.runtime_image_identity,
    }


@pytest.mark.parametrize(
    ("operation", "message"),
    [
        (lambda: kubernetes.queue_for_wave("A" * 64), "lowercase"),
        (lambda: kubernetes._job_count_from_text("not-a-count"), "canonical decimal"),
        (lambda: kubernetes._job_count_from_text("01"), "canonical decimal"),
        (lambda: kubernetes._environment_by_name({}), "env must be a list"),
        (
            lambda: kubernetes._environment_by_name(
                [{"name": "X"}, {"name": "X"}]
            ),
            "duplicate worker env",
        ),
        (lambda: kubernetes._env_value({}, "X"), "missing worker env"),
        (lambda: kubernetes._env_field_path({}, "X"), "missing worker env"),
        (
            lambda: kubernetes._require_env_value(
                {"X": {"value": "wrong"}},
                "X",
                "expected",
            ),
            "does not match",
        ),
        (lambda: kubernetes._require_image("registry.example/worker:latest"), "pinned"),
        (lambda: kubernetes._require_digest("digest", "G" * 64), "lowercase"),
        (lambda: kubernetes._require_factory("not-a-dotted-factory"), "dotted"),
    ],
)
def test_scalar_contract_edges_fail_closed(operation, message):
    with pytest.raises(kubernetes.PTGWaveContractError, match=message):
        operation()

@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda manifest: manifest.__setitem__("apiVersion", "v1"),
            "batch/v1 Job",
        ),
        (
            lambda manifest: manifest["metadata"].__setitem__("name", "other-job"),
            "canonical wave digest",
        ),
        (
            lambda manifest: manifest["metadata"]["annotations"].__setitem__(
                "healthporta.com/ptg-wave-queue",
                "arq:PTGSmall",
            ),
            "full wave digest",
        ),
        (
            lambda manifest: manifest["metadata"]["annotations"].__setitem__(
                "healthporta.com/ptg-wave-worker-class",
                "process.FHIR",
            ),
            "PTGSmall",
        ),
        (
            lambda manifest: manifest["metadata"]["labels"].__setitem__(
                "healthporta.com/ptg-wave",
                "false",
            ),
            "PTG-wave-specific",
        ),
        (
            lambda manifest: manifest["spec"]["template"]["spec"].__setitem__(
                "restartPolicy",
                "OnFailure",
            ),
            "must not restart",
        ),
        (
            lambda manifest: manifest["spec"]["template"]["spec"].__setitem__(
                "containers",
                [],
            ),
            "exactly one",
        ),
        (
            lambda manifest: manifest["spec"]["template"]["metadata"].__setitem__(
                "labels",
                {},
            ),
            "must exactly match",
        ),
    ],
)
def test_manifest_validation_rejects_metadata_and_execution_edges(mutate, message):
    manifest = _manifest()
    mutate(manifest)

    with pytest.raises(kubernetes.PTGWaveContractError, match=message):
        kubernetes.validate_ptg_wave_job_manifest(manifest)

def test_container_and_downward_api_contracts_reject_tampering():
    manifest, contract = _contract()
    values = _contract_values(contract)
    container = manifest["spec"]["template"]["spec"]["containers"][0]

    unbarriered = copy.deepcopy(container)
    unbarriered["command"] = ["python", "-m", "process.other_worker"]
    with pytest.raises(kubernetes.PTGWaveContractError, match="release barrier"):
        kubernetes._validate_wave_container(unbarriered, values)

    environment = kubernetes._environment_by_name(copy.deepcopy(container["env"]))
    environment["HLTHPRT_PTG_WAVE_SLOT_INDEX"]["valueFrom"]["fieldRef"][
        "fieldPath"
    ] = "metadata.uid"
    with pytest.raises(kubernetes.PTGWaveContractError, match="slot index"):
        kubernetes._validate_wave_environment(environment, values, contract.image)

    environment = kubernetes._environment_by_name(copy.deepcopy(container["env"]))
    environment["HLTHPRT_PTG_WAVE_POD_UID"]["valueFrom"]["fieldRef"][
        "fieldPath"
    ] = "metadata.name"
    with pytest.raises(kubernetes.PTGWaveContractError, match="pod UID"):
        kubernetes._validate_wave_environment(environment, values, contract.image)

def test_live_job_attestation_rejects_invalid_job_and_option_edges():
    manifest, contract = _contract()

    with pytest.raises(kubernetes.PTGWaveContractError, match="batch/v1 Job"):
        live._attest_actual_job(contract, manifest, {})

    wrong_name = _actual_job(manifest)
    wrong_name["metadata"]["name"] = "other-job"
    with pytest.raises(kubernetes.PTGWaveContractError, match="canonical wave"):
        live._attest_actual_job(contract, manifest, wrong_name)

    for options, message in (
        ({"activeDeadlineSeconds": 1}, "unsupported execution options"),
        ({"suspend": True}, "must not be suspended"),
    ):
        actual = _actual_job(manifest)
        actual["spec"].update(options)
        with pytest.raises(kubernetes.PTGWaveContractError, match=message):
            live._attest_actual_job(contract, manifest, actual)

def test_live_template_attestation_rejects_changed_worker_config_identity():
    manifest, contract = _contract()
    actual_spec = copy.deepcopy(manifest["spec"])
    actual_spec["template"]["spec"]["containers"][0]["resources"] = {
        "limits": {"memory": "13Gi"}
    }

    with pytest.raises(kubernetes.PTGWaveContractError, match="worker config"):
        live._require_actual_template(contract, actual_spec, manifest["spec"])

@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda pod: pod.__setitem__("kind", "ConfigMap"),
            "v1 Pod",
        ),
        (
            lambda pod: pod["spec"]["containers"][0].__setitem__(
                "image",
                "registry.example/worker@sha256:" + "0" * 64,
            ),
            "desired image",
        ),
        (
            lambda pod: pod["spec"].__setitem__("containers", []),
            "one worker container",
        ),
        (
            lambda pod: pod["spec"]["containers"][0].__setitem__(
                "name",
                "other",
            ),
            "unexpected worker container",
        ),
        (
            lambda pod: pod["status"].__setitem__("containerStatuses", []),
            "one worker container status",
        ),
        (
            lambda pod: pod["status"]["containerStatuses"][0].__setitem__(
                "name",
                "other",
            ),
            "unexpected container status",
        ),
    ],
)
def test_live_pod_identity_rejects_object_and_container_edges(mutate, message):
    manifest, contract = _contract()
    job = _actual_job(manifest)
    pod = _initial_pods(manifest)[0]
    mutate(pod)

    with pytest.raises(kubernetes.PTGWaveContractError, match=message):
        live._attest_pod_identity(
            contract,
            pod,
            job_name=job["metadata"]["name"],
            job_uid=job["metadata"]["uid"],
        )

@pytest.mark.parametrize(
    ("annotations", "message"),
    [
        ({}, "trimmed string"),
        (
            {"batch.kubernetes.io/job-completion-index": "01"},
            "not canonical",
        ),
        (
            {"batch.kubernetes.io/job-completion-index": "12"},
            "outside zero through eleven",
        ),
    ],
)
def test_live_pod_completion_index_is_canonical_and_bounded(annotations, message):
    with pytest.raises(kubernetes.PTGWaveContractError, match=message):
        live._pod_completion_index({"annotations": annotations})

@pytest.mark.parametrize(
    ("image_id", "message"),
    [
        ("", "non-empty trimmed string"),
        ("containerd://registry.example/worker:latest", "sha256 image"),
        ("containerd://sha256:" + "G" * 64, "sha256 image"),
    ],
)
def test_live_image_id_requires_a_sha256_identity(image_id, message):
    with pytest.raises(kubernetes.PTGWaveContractError, match=message):
        live._normalize_actual_image_id(image_id)

def test_live_attestation_rejects_duplicate_uid_and_incomplete_iteration():
    manifest = _manifest()
    pods = _initial_pods(manifest)
    pods[1]["metadata"]["uid"] = pods[0]["metadata"]["uid"]
    with pytest.raises(kubernetes.PTGWaveContractError, match="unique pod UID"):
        live.attest_ptg_wave_kubernetes_objects(
            manifest,
            _actual_job(manifest),
            pods,
        )

    with pytest.raises(kubernetes.PTGWaveContractError, match="cover indexes"):
        live.attest_ptg_wave_kubernetes_objects(
            manifest,
            _actual_job(manifest),
            _ShortIterationList(_initial_pods(manifest)),
        )

def test_callback_receipts_reject_count_slot_coverage_and_identity_drift():
    manifest = _manifest()
    receipts = _receipts(manifest)

    with pytest.raises(kubernetes.PTGWaveContractError, match="exactly twelve"):
        attest_ptg_wave_slot_receipts(manifest, receipts[:-1])

    invalid_slot = copy.deepcopy(receipts)
    invalid_slot[0]["slot_index"] = True
    with pytest.raises(kubernetes.PTGWaveContractError, match="invalid Indexed"):
        attest_ptg_wave_slot_receipts(manifest, invalid_slot)

    duplicate_slot = copy.deepcopy(receipts)
    duplicate_slot[1]["slot_index"] = 0
    duplicate_slot[1]["pod_uid"] = "other-pod"
    with pytest.raises(kubernetes.PTGWaveContractError, match="repeat"):
        attest_ptg_wave_slot_receipts(manifest, duplicate_slot)

    with pytest.raises(kubernetes.PTGWaveContractError, match="cover slots"):
        attest_ptg_wave_slot_receipts(manifest, _ShortIterationList(receipts))

    drifted = copy.deepcopy(receipts)
    drifted[0]["queue"] = "arq:PTGSmall"
    with pytest.raises(kubernetes.PTGWaveContractError, match="does not match"):
        attest_ptg_wave_slot_receipts(manifest, drifted)

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
