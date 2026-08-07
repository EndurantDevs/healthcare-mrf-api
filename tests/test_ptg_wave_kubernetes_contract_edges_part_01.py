# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned Kubernetes exact-wave contract edges."""

from __future__ import annotations

from tests.test_ptg_wave_kubernetes_contract_edges import (
    _ShortIterationList,
    _actual_job,
    _contract,
    _contract_values,
    _initial_pods,
    _manifest,
    _receipts,
    attest_ptg_wave_slot_receipts,
    copy,
    kubernetes,
    live,
    pytest,
)


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
