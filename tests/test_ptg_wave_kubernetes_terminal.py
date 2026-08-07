# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
from dataclasses import replace

import pytest

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTG_WAVE_SLOT_COUNT,
    build_ptg_wave_job,
    validate_ptg_wave_job_manifest,
)
from api.ptg_wave_kubernetes_attestation import (
    attest_existing_ptg_wave_job,
    attest_ptg_wave_kubernetes_objects,
)
from api.ptg_wave_kubernetes_terminal_attestation import (
    attest_terminal_ptg_wave_kubernetes_objects,
)


_WAVE = "a" * 64
_REDIS_MANIFEST = "b" * 64
_JOBS = "d" * 64
_IMAGE = "registry.example/engine@sha256:" + "c" * 64
_RUNTIME_IMAGE_IDENTITY = "sha256:" + "f" * 64
_FACTORY = "process.ptg_wave_release_redis.barrier_from_environment"


def _manifest() -> dict:
    return build_ptg_wave_job(
        wave_digest=_WAVE,
        manifest_digest=_REDIS_MANIFEST,
        jobs_digest=_JOBS,
        job_count=3586,
        image=_IMAGE,
        runtime_image_identity=_RUNTIME_IMAGE_IDENTITY,
        barrier_factory=_FACTORY,
    )


def _actual_job(manifest: dict) -> dict:
    actual_job = copy.deepcopy(manifest)
    actual_job["metadata"]["uid"] = "job-uid-123"
    return actual_job


def _ready_pods(manifest: dict) -> list[dict]:
    contract = validate_ptg_wave_job_manifest(manifest)
    actual_job = _actual_job(manifest)
    owner_reference_by_field = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "name": actual_job["metadata"]["name"],
        "uid": actual_job["metadata"]["uid"],
        "controller": True,
        "blockOwnerDeletion": True,
    }
    return [
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": f"{actual_job['metadata']['name']}-{slot}",
                "uid": f"pod-uid-{slot}",
                "labels": {**manifest["metadata"]["labels"]},
                "annotations": {
                    **manifest["metadata"]["annotations"],
                    "batch.kubernetes.io/job-completion-index": str(slot),
                },
                "ownerReferences": [owner_reference_by_field],
            },
            "spec": {
                "containers": [
                    {
                        "name": "ptg-wave-worker",
                        "image": contract.image,
                    }
                ]
            },
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {
                        "name": "ptg-wave-worker",
                        "ready": True,
                        "image": contract.image,
                        "imageID": f"containerd://{contract.runtime_image_identity}",
                    }
                ],
            },
        }
        for slot in range(PTG_WAVE_SLOT_COUNT)
    ]


def _terminal_job(manifest: dict) -> dict:
    actual_job = _actual_job(manifest)
    actual_job["status"] = {
        "active": 0,
        "failed": 0,
        "succeeded": PTG_WAVE_SLOT_COUNT,
        "completedIndexes": "0-11",
    }
    return actual_job


def _terminal_pods(manifest: dict) -> list[dict]:
    terminal_pods = _ready_pods(manifest)
    for terminal_pod in terminal_pods:
        terminal_pod["status"]["phase"] = "Succeeded"
        terminal_pod["status"]["containerStatuses"][0]["ready"] = False
    return terminal_pods


def _initial_attestation(manifest: dict):
    return attest_ptg_wave_kubernetes_objects(
        manifest,
        _actual_job(manifest),
        _ready_pods(manifest),
    )


def test_existing_job_is_attested_before_any_pods_or_status_exist():
    manifest = _manifest()
    existing_job = attest_existing_ptg_wave_job(
        manifest,
        _actual_job(manifest),
    )

    assert existing_job.job_uid == "job-uid-123"
    assert existing_job.job_name == manifest["metadata"]["name"]
    assert existing_job.image_identity == _IMAGE
    assert existing_job.runtime_image_identity == _RUNTIME_IMAGE_IDENTITY


@pytest.mark.parametrize(
    "mutation",
    [
        lambda actual_job: actual_job["metadata"].__setitem__("uid", ""),
        lambda actual_job: actual_job["spec"].__setitem__("parallelism", 11),
        lambda actual_job: actual_job["metadata"]["annotations"].__setitem__(
            "healthporta.com/ptg-wave-jobs-digest",
            "e" * 64,
        ),
        lambda actual_job: actual_job["spec"].__setitem__(
            "ttlSecondsAfterFinished",
            60,
        ),
        lambda actual_job: actual_job["spec"]["template"]["spec"].__setitem__(
            "restartPolicy",
            "OnFailure",
        ),
        lambda actual_job: next(
            environment_entry
            for environment_entry in actual_job["spec"]["template"]["spec"][
                "containers"
            ][0]["env"]
            if environment_entry["name"] == "HLTHPRT_ACTIVE_WORKER_QUEUE"
        ).__setitem__("value", "arq:PTGSmall"),
    ],
)
def test_existing_job_attestation_rejects_non_exact_objects(mutation):
    manifest = _manifest()
    actual_job = _actual_job(manifest)
    mutation(actual_job)

    with pytest.raises(PTGWaveContractError):
        attest_existing_ptg_wave_job(manifest, actual_job)


def test_terminal_attestation_closes_exact_initial_membership():
    manifest = _manifest()
    initial_attestation = _initial_attestation(manifest)
    terminal_attestation = attest_terminal_ptg_wave_kubernetes_objects(
        manifest,
        initial_attestation,
        _terminal_job(manifest),
        _terminal_pods(manifest),
    )

    assert terminal_attestation.job_uid == initial_attestation.job_uid
    assert terminal_attestation.completed_slots == tuple(range(12))
    assert terminal_attestation.pod_uid_by_slot == initial_attestation.pod_uid_by_slot
    assert terminal_attestation.runtime_image_identity == _RUNTIME_IMAGE_IDENTITY


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("active", 1),
        ("failed", 1),
        ("succeeded", 11),
        ("completedIndexes", "0-10"),
        ("completedIndexes", "0-10,10"),
    ],
)
def test_terminal_attestation_rejects_non_terminal_job_status(field, bad_value):
    manifest = _manifest()
    terminal_job = _terminal_job(manifest)
    terminal_job["status"][field] = bad_value

    with pytest.raises(PTGWaveContractError, match="terminal Job"):
        attest_terminal_ptg_wave_kubernetes_objects(
            manifest,
            _initial_attestation(manifest),
            terminal_job,
            _terminal_pods(manifest),
        )


def test_terminal_attestation_rejects_a_replaced_job_uid():
    manifest = _manifest()
    terminal_job = _terminal_job(manifest)
    terminal_job["metadata"]["uid"] = "replacement-job-uid"

    with pytest.raises(PTGWaveContractError, match="Job identity"):
        attest_terminal_ptg_wave_kubernetes_objects(
            manifest,
            _initial_attestation(manifest),
            terminal_job,
            _terminal_pods(manifest),
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda terminal_pods: terminal_pods[11]["status"].__setitem__(
                "phase",
                "Running",
            ),
            "Succeeded",
        ),
        (
            lambda terminal_pods: terminal_pods[11]["metadata"].__setitem__(
                "uid",
                "replacement-pod-uid",
            ),
            "slot-to-pod UID",
        ),
        (
            lambda terminal_pods: terminal_pods[11]["metadata"][
                "ownerReferences"
            ][0].__setitem__("uid", "wrong-owner-uid"),
            "owner",
        ),
        (
            lambda terminal_pods: terminal_pods[11]["status"][
                "containerStatuses"
            ][0].__setitem__("imageID", "containerd://sha256:" + "e" * 64),
            "imageIDs",
        ),
        (
            lambda terminal_pods: terminal_pods.pop(),
            "exactly twelve",
        ),
        (
            lambda terminal_pods: terminal_pods[11]["metadata"][
                "annotations"
            ].__setitem__("batch.kubernetes.io/job-completion-index", "10"),
            "unique indexes",
        ),
    ],
)
def test_terminal_attestation_rejects_changed_persisted_pod_evidence(
    mutation,
    message,
):
    manifest = _manifest()
    terminal_pods = _terminal_pods(manifest)
    mutation(terminal_pods)

    with pytest.raises(PTGWaveContractError, match=message):
        attest_terminal_ptg_wave_kubernetes_objects(
            manifest,
            _initial_attestation(manifest),
            _terminal_job(manifest),
            terminal_pods,
        )


def test_terminal_attestation_revalidates_persisted_initial_image_identity():
    manifest = _manifest()
    initial_attestation = replace(
        _initial_attestation(manifest),
        runtime_image_identity="sha256:" + "e" * 64,
    )

    with pytest.raises(PTGWaveContractError, match="initial attestation"):
        attest_terminal_ptg_wave_kubernetes_objects(
            manifest,
            initial_attestation,
            _terminal_job(manifest),
            _terminal_pods(manifest),
        )
