# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic contract coverage for pre-claim Kubernetes failure evidence."""

from __future__ import annotations

import copy

import pytest

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTG_WAVE_SLOT_COUNT,
    build_ptg_wave_job,
    validate_ptg_wave_job_manifest,
)
from api.ptg_wave_kubernetes_attestation import (
    attest_ptg_wave_kubernetes_objects,
)
from api.ptg_wave_kubernetes_failure_attestation import (
    attest_preclaim_failure_ptg_wave_kubernetes_objects,
)


_WAVE = "a" * 64
_REDIS_MANIFEST = "b" * 64
_JOBS = "c" * 64
_IMAGE = "registry.example/worker@sha256:" + "d" * 64
_RUNTIME_IMAGE_IDENTITY = "sha256:" + "e" * 64
_FACTORY = "process.ptg_wave_release_redis.barrier_from_environment"


def _manifest() -> dict:
    return build_ptg_wave_job(
        wave_digest=_WAVE,
        manifest_digest=_REDIS_MANIFEST,
        jobs_digest=_JOBS,
        job_count=24,
        image=_IMAGE,
        runtime_image_identity=_RUNTIME_IMAGE_IDENTITY,
        barrier_factory=_FACTORY,
    )


def _actual_job(manifest: dict) -> dict:
    actual_job = copy.deepcopy(manifest)
    actual_job["metadata"]["uid"] = "job-uid-synthetic"
    return actual_job


def _initial_pods(manifest: dict) -> list[dict]:
    contract = validate_ptg_wave_job_manifest(manifest)
    job = _actual_job(manifest)
    owner_reference_map = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "name": job["metadata"]["name"],
        "uid": job["metadata"]["uid"],
        "controller": True,
        "blockOwnerDeletion": True,
    }
    return [
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": f"{job['metadata']['name']}-{slot}",
                "uid": f"pod-uid-{slot}",
                "labels": {**manifest["metadata"]["labels"]},
                "annotations": {
                    **manifest["metadata"]["annotations"],
                    "batch.kubernetes.io/job-completion-index": str(slot),
                },
                "ownerReferences": [owner_reference_map],
            },
            "spec": {
                "containers": [{"name": "ptg-wave-worker", "image": contract.image}],
            },
            "status": {
                "phase": "Running",
                "containerStatuses": [{
                    "name": "ptg-wave-worker",
                    "ready": True,
                    "image": contract.image,
                    "imageID": f"containerd://{contract.runtime_image_identity}",
                }],
            },
        }
        for slot in range(PTG_WAVE_SLOT_COUNT)
    ]


def _initial_attestation(manifest: dict):
    return attest_ptg_wave_kubernetes_objects(
        manifest,
        _actual_job(manifest),
        _initial_pods(manifest),
    )


def _failed_job(manifest: dict) -> dict:
    job = _actual_job(manifest)
    job["status"] = {
        "active": 0,
        "failed": PTG_WAVE_SLOT_COUNT,
        "succeeded": 0,
        "conditions": [{"type": "Failed", "status": "True"}],
    }
    return job


def _failed_pods(manifest: dict) -> list[dict]:
    pods = _initial_pods(manifest)
    for pod in pods:
        status = pod["status"]
        status["phase"] = "Failed"
        worker_status = status["containerStatuses"][0]
        worker_status.update({
            "ready": False,
            "restartCount": 0,
            "state": {"terminated": {"exitCode": 137}},
        })
    return pods


def _attest(manifest: dict | None = None):
    manifest = _manifest() if manifest is None else manifest
    return attest_preclaim_failure_ptg_wave_kubernetes_objects(
        manifest,
        _initial_attestation(manifest),
        _failed_job(manifest),
        _failed_pods(manifest),
    )


def test_preclaim_failure_attestation_binds_every_initial_pod_without_success():
    attestation = _attest()

    assert attestation.job_active == 0
    assert attestation.job_failed == PTG_WAVE_SLOT_COUNT
    assert attestation.job_succeeded == 0
    assert attestation.failed_pod_uid_by_slot == {
        slot: f"pod-uid-{slot}" for slot in range(PTG_WAVE_SLOT_COUNT)
    }
    mapping = attestation.as_mapping()
    assert mapping["backoff_limit"] == 0
    assert mapping["failed_slots"] == [
        {
            "slot": slot,
            "pod_uid": f"pod-uid-{slot}",
            "phase": "Failed",
            "runtime_image_identity": _RUNTIME_IMAGE_IDENTITY,
        }
        for slot in range(PTG_WAVE_SLOT_COUNT)
    ]
    assert len(mapping["attestation_digest"]) == 64


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda job: job["status"].__setitem__("active", 1),
            "active",
        ),
        (
            lambda job: job["status"].__setitem__("failed", 0),
            "exactly twelve failed slots",
        ),
        (
            lambda job: job["status"].__setitem__("succeeded", 1),
            "must not report success",
        ),
        (
            lambda job: job["status"].__setitem__("completedIndexes", "0"),
            "must not report completed indexes",
        ),
        (
            lambda job: job["status"].__setitem__("failed", True),
            "count is invalid",
        ),
        (
            lambda job: job["status"].__setitem__("conditions", []),
            "conditions are missing",
        ),
        (
            lambda job: job["status"]["conditions"].append(
                {"type": "Complete", "status": "True"}
            ),
            "must not have a true Complete",
        ),
    ],
)
def test_preclaim_failure_rejects_active_success_or_malformed_job_status(
    mutate,
    message,
):
    manifest = _manifest()
    job = _failed_job(manifest)
    mutate(job)

    with pytest.raises(PTGWaveContractError, match=message):
        attest_preclaim_failure_ptg_wave_kubernetes_objects(
            manifest,
            _initial_attestation(manifest),
            job,
            _failed_pods(manifest),
        )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda pods: pods.pop(),
            "exactly twelve failed wave Pods",
        ),
        (
            lambda pods: pods.append(copy.deepcopy(pods[0])),
            "exactly twelve failed wave Pods",
        ),
        (
            lambda pods: pods[4]["metadata"].__setitem__("uid", "replaced-pod"),
            "slot-to-Pod UID membership",
        ),
        (
            lambda pods: pods[4]["status"].__setitem__("phase", "Succeeded"),
            "must be Failed",
        ),
        (
            lambda pods: pods[4]["status"]["containerStatuses"][0].__setitem__(
                "imageID", "containerd://sha256:" + "f" * 64
            ),
            "imageIDs",
        ),
        (
            lambda pods: pods[4]["status"]["containerStatuses"][0].__setitem__(
                "ready", True
            ),
            "must not be Ready",
        ),
        (
            lambda pods: pods[4]["status"]["containerStatuses"][0].__setitem__(
                "state", {"terminated": {"exitCode": 0}}
            ),
            "non-zero integer",
        ),
        (
            lambda pods: pods[4]["status"]["containerStatuses"][0].__setitem__(
                "restartCount", 1
            ),
            "restartCount",
        ),
    ],
)
def test_preclaim_failure_rejects_changed_missing_extra_or_success_pods(
    mutate,
    message,
):
    manifest = _manifest()
    pods = _failed_pods(manifest)
    mutate(pods)

    with pytest.raises(PTGWaveContractError, match=message):
        attest_preclaim_failure_ptg_wave_kubernetes_objects(
            manifest,
            _initial_attestation(manifest),
            _failed_job(manifest),
            pods,
        )


def test_preclaim_failure_rejects_a_replaced_job_or_retried_manifest():
    manifest = _manifest()
    job = _failed_job(manifest)
    job["metadata"]["uid"] = "replacement-job"

    with pytest.raises(PTGWaveContractError, match="Job identity"):
        attest_preclaim_failure_ptg_wave_kubernetes_objects(
            manifest,
            _initial_attestation(manifest),
            job,
            _failed_pods(manifest),
        )

    retried_manifest = _manifest()
    retried_manifest["spec"]["backoffLimit"] = 1
    with pytest.raises(PTGWaveContractError, match="cannot retry"):
        _attest(retried_manifest)
