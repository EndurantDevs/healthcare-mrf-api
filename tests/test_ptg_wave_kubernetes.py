# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import copy
from dataclasses import replace
from unittest.mock import patch

import pytest

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTG_WAVE_SLOT_COUNT,
    build_ptg_wave_job,
    queue_for_wave,
    validate_ptg_wave_job_manifest,
)
from api.ptg_wave_kubernetes_attestation import attest_ptg_wave_kubernetes_objects
from api.ptg_wave_kubernetes_receipt_attestation import (
    attest_ptg_wave_slot_receipts,
)
from process import ptg_wave_worker
from process.ptg_wave_barrier import PTGWaveWorkerIdentity, run_after_wave_release
from process.ptg_wave_worker import run_wave_worker


_WAVE = "a" * 64
_REDIS_MANIFEST = "b" * 64
_JOBS = "d" * 64
_JOB_COUNT = 3586
_RUNTIME_IMAGE_IDENTITY = "sha256:" + "f" * 64
_FACTORY = "process.ptg_wave_release_redis.barrier_from_environment"


def _manifest() -> dict:
    return build_ptg_wave_job(
        wave_digest=_WAVE,
        manifest_digest=_REDIS_MANIFEST,
        jobs_digest=_JOBS,
        job_count=_JOB_COUNT,
        image="registry.example/engine@sha256:" + "c" * 64,
        runtime_image_identity=_RUNTIME_IMAGE_IDENTITY,
        barrier_factory=_FACTORY,
    )


def _receipts(manifest: dict) -> list[dict]:
    contract = validate_ptg_wave_job_manifest(manifest)
    return [
        {
            "slot_index": index,
            "pod_uid": f"pod-{index}",
            "wave_digest": contract.wave_digest,
            "queue": contract.queue,
            "worker_class": "process.PTGSmall",
            "manifest_digest": contract.manifest_digest,
            "jobs_digest": contract.jobs_digest,
            "job_count": contract.job_count,
            "config_identity": contract.config_identity,
            "manifest_identity": contract.manifest_identity,
            "image_identity": contract.image,
            "runtime_image_identity": contract.runtime_image_identity,
        }
        for index in range(PTG_WAVE_SLOT_COUNT)
    ]


def _identity(slot_index: int = 3) -> PTGWaveWorkerIdentity:
    contract = validate_ptg_wave_job_manifest(_manifest())
    return PTGWaveWorkerIdentity(
        wave_digest=contract.wave_digest,
        queue=contract.queue,
        worker_class="process.PTGSmall",
        slot_index=slot_index,
        pod_uid=f"pod-{slot_index}",
        manifest_digest=contract.manifest_digest,
        jobs_digest=contract.jobs_digest,
        job_count=contract.job_count,
        config_identity=contract.config_identity,
        manifest_identity=contract.manifest_identity,
        image_identity=contract.image,
        runtime_image_identity=contract.runtime_image_identity,
    )


def _actual_job(manifest: dict) -> dict:
    actual_job = copy.deepcopy(manifest)
    actual_job["metadata"]["uid"] = "job-uid-123"
    return actual_job


def _actual_pods(manifest: dict) -> list[dict]:
    contract = validate_ptg_wave_job_manifest(manifest)
    job = _actual_job(manifest)
    owner_reference_by_field = {
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
                "containerStatuses": [
                    {
                        "name": "ptg-wave-worker",
                        "ready": True,
                        "image": contract.image,
                        "imageID": f"containerd://{contract.runtime_image_identity}",
                    }
                ]
            },
        }
        for slot in range(PTG_WAVE_SLOT_COUNT)
    ]


def test_builds_exact_indexed_ptg_wave_job_with_release_only_entrypoint(monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT", "import-worker")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", '{"process.PTGSmall":{"limits":{"memory":"12Gi"}}}')
    manifest = _manifest()
    contract = validate_ptg_wave_job_manifest(manifest)

    assert contract.wave_digest == _WAVE
    assert contract.queue == queue_for_wave(_WAVE)
    assert contract.manifest_digest == _REDIS_MANIFEST
    assert contract.jobs_digest == _JOBS
    assert contract.job_count == _JOB_COUNT
    assert contract.image.endswith("sha256:" + "c" * 64)
    assert contract.runtime_image_identity == _RUNTIME_IMAGE_IDENTITY
    assert contract.image.rsplit("@", 1)[1] != contract.runtime_image_identity
    assert manifest["metadata"]["name"] == f"hpw-ptg-wave-{_WAVE[:40]}"
    assert manifest["spec"]["completionMode"] == "Indexed"
    assert manifest["spec"]["completions"] == manifest["spec"]["parallelism"] == 12
    assert manifest["spec"]["backoffLimit"] == 0
    assert "ttlSecondsAfterFinished" not in manifest["spec"]
    pod_spec = manifest["spec"]["template"]["spec"]
    assert pod_spec["serviceAccountName"] == "import-worker"
    container = pod_spec["containers"][0]
    assert container["command"] == ["/opt/venv/bin/python", "-m", "process.ptg_wave_worker"]
    assert container["resources"] == {"limits": {"memory": "12Gi"}}
    environment_by_name = {
        environment_entry["name"]: environment_entry
        for environment_entry in container["env"]
    }
    assert environment_by_name["HLTHPRT_PTG_WAVE_SLOT_INDEX"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.annotations['batch.kubernetes.io/job-completion-index']"
    assert environment_by_name["HLTHPRT_PTG_WAVE_POD_UID"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.uid"
    assert environment_by_name["HLTHPRT_PTG_WAVE_REDIS_MANIFEST_DIGEST"]["value"] == _REDIS_MANIFEST
    assert environment_by_name["HLTHPRT_PTG_WAVE_JOBS_DIGEST"]["value"] == _JOBS
    assert environment_by_name["HLTHPRT_PTG_WAVE_JOB_COUNT"]["value"] == str(_JOB_COUNT)
    assert environment_by_name["HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY"][
        "value"
    ] == _RUNTIME_IMAGE_IDENTITY
    assert "fhir" not in str(manifest).lower()


@pytest.mark.parametrize("job_count", [0, 4097, True, "1"])
def test_job_count_must_be_an_integer_from_one_through_4096(job_count):
    with pytest.raises(PTGWaveContractError, match="job_count"):
        build_ptg_wave_job(
            wave_digest=_WAVE,
            manifest_digest=_REDIS_MANIFEST,
            jobs_digest=_JOBS,
            job_count=job_count,
            image="registry.example/engine@sha256:" + "c" * 64,
            runtime_image_identity=_RUNTIME_IMAGE_IDENTITY,
            barrier_factory=_FACTORY,
        )


@pytest.mark.parametrize(
    "runtime_image_identity",
    [
        "f" * 64,
        "registry.example/engine@sha256:" + "f" * 64,
        "sha256:" + "F" * 64,
    ],
)
def test_runtime_image_identity_must_be_a_canonical_sha256(runtime_image_identity):
    with pytest.raises(PTGWaveContractError, match="runtime_image_identity"):
        build_ptg_wave_job(
            wave_digest=_WAVE,
            manifest_digest=_REDIS_MANIFEST,
            jobs_digest=_JOBS,
            job_count=_JOB_COUNT,
            image="registry.example/engine@sha256:" + "c" * 64,
            runtime_image_identity=runtime_image_identity,
            barrier_factory=_FACTORY,
        )


def test_config_identity_binds_public_worker_resource_and_reference_shapes():
    with patch.dict(
        "os.environ",
        {"HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON": '{"process.PTGSmall":{"limits":{"memory":"12Gi"}}}'},
    ):
        first_contract = validate_ptg_wave_job_manifest(_manifest())
    with patch.dict(
        "os.environ",
        {"HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON": '{"process.PTGSmall":{"limits":{"memory":"13Gi"}}}'},
    ):
        second_contract = validate_ptg_wave_job_manifest(_manifest())

    assert first_contract.config_identity != second_contract.config_identity
    assert first_contract.manifest_identity != second_contract.manifest_identity


@pytest.mark.parametrize(
    "mutate",
    [
        lambda manifest: manifest["spec"].__setitem__("parallelism", 11),
        lambda manifest: manifest["spec"].__setitem__("completionMode", "NonIndexed"),
        lambda manifest: manifest["spec"].__setitem__("backoffLimit", 1),
        lambda manifest: manifest["spec"]["template"]["spec"]["containers"][0].__setitem__("command", ["worker"]),
    ],
)
def test_manifest_validation_rejects_non_exact_or_unbarriered_jobs(mutate):
    manifest = _manifest()
    mutate(manifest)
    with pytest.raises(PTGWaveContractError):
        validate_ptg_wave_job_manifest(manifest)


def test_manifest_identity_rejects_a_tampered_pinned_image():
    manifest = _manifest()
    tampered_image = "registry.example/engine@sha256:" + "e" * 64
    container = manifest["spec"]["template"]["spec"]["containers"][0]
    container["image"] = tampered_image
    next(item for item in container["env"] if item["name"] == "HLTHPRT_PTG_WAVE_IMAGE_IDENTITY")["value"] = tampered_image
    with pytest.raises(PTGWaveContractError, match="manifest identity"):
        validate_ptg_wave_job_manifest(manifest)


def test_manifest_identity_rejects_a_tampered_runtime_image_identity():
    manifest = _manifest()
    tampered_runtime_identity = "sha256:" + "e" * 64
    manifest["metadata"]["annotations"][
        "healthporta.com/ptg-wave-runtime-image-identity"
    ] = tampered_runtime_identity
    container = manifest["spec"]["template"]["spec"]["containers"][0]
    next(
        environment_entry
        for environment_entry in container["env"]
        if environment_entry["name"]
        == "HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY"
    )["value"] = tampered_runtime_identity

    with pytest.raises(PTGWaveContractError, match="manifest identity"):
        validate_ptg_wave_job_manifest(manifest)


def test_callback_attestation_requires_exact_slots_but_does_not_claim_image_status():
    manifest = _manifest()
    attestation = attest_ptg_wave_slot_receipts(manifest, _receipts(manifest))

    assert attestation.pod_uid_by_slot == {index: f"pod-{index}" for index in range(12)}
    assert not hasattr(attestation, "image_id")

    duplicate_pod = _receipts(manifest)
    duplicate_pod[1]["pod_uid"] = duplicate_pod[0]["pod_uid"]
    with pytest.raises(PTGWaveContractError, match="unique pod UID"):
        attest_ptg_wave_slot_receipts(manifest, duplicate_pod)


def test_kubernetes_attestation_binds_exact_job_pods_ready_state_and_actual_image():
    manifest = _manifest()
    attestation = attest_ptg_wave_kubernetes_objects(
        manifest,
        _actual_job(manifest),
        _actual_pods(manifest),
    )

    assert attestation.job_name == manifest["metadata"]["name"]
    assert attestation.job_uid == "job-uid-123"
    assert attestation.runtime_image_identity == _RUNTIME_IMAGE_IDENTITY
    assert set(attestation.pod_uid_by_slot) == set(range(12))


def test_runtime_image_attestation_rejects_the_distinct_pull_manifest_digest():
    manifest = _manifest()
    pods = _actual_pods(manifest)
    pods[11]["status"]["containerStatuses"][0]["imageID"] = (
        "containerd://sha256:" + "c" * 64
    )

    with pytest.raises(PTGWaveContractError, match="runtime image identity"):
        attest_ptg_wave_kubernetes_objects(
            manifest,
            _actual_job(manifest),
            pods,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda pods: pods[11]["metadata"]["annotations"].__setitem__("batch.kubernetes.io/job-completion-index", "10"), "completion index"),
        (lambda pods: pods[11]["metadata"]["ownerReferences"][0].__setitem__("uid", "wrong-job"), "owner"),
        (lambda pods: pods[11]["status"]["containerStatuses"][0].__setitem__("ready", False), "not Ready"),
        (
            lambda pods: pods[11]["status"]["containerStatuses"][0].__setitem__(
                "imageID",
                "containerd://sha256:" + "e" * 64,
            ),
            "imageIDs",
        ),
    ],
)
def test_kubernetes_attestation_rejects_non_exact_live_pod_evidence(mutation, message):
    manifest = _manifest()
    pods = _actual_pods(manifest)
    mutation(pods)

    with pytest.raises(PTGWaveContractError, match=message):
        attest_ptg_wave_kubernetes_objects(manifest, _actual_job(manifest), pods)


class _Barrier:
    def __init__(self, events: list[str], identity: PTGWaveWorkerIdentity):
        self.events = events
        self.identity = identity

    async def register_ready(self, identity):
        assert identity == self.identity
        self.events.append("ready")
        return {}

    async def wait_for_release(self, identity):
        assert identity == self.identity
        self.events.append("release")
        return {
            "released": True,
            "wave_digest": identity.wave_digest,
            "queue": identity.queue,
            "worker_class": identity.worker_class,
            "manifest_digest": identity.manifest_digest,
            "jobs_digest": identity.jobs_digest,
            "job_count": identity.job_count,
            "config_identity": identity.config_identity,
            "manifest_identity": identity.manifest_identity,
            "image_identity": identity.image_identity,
            "runtime_image_identity": identity.runtime_image_identity,
        }


class _WrongRuntimeImageBarrier(_Barrier):
    async def wait_for_release(self, identity):
        release_receipt = await super().wait_for_release(identity)
        release_receipt["runtime_image_identity"] = "sha256:" + "e" * 64
        return release_receipt


def test_worker_factory_is_not_constructed_until_after_exact_release_receipt():
    identity = _identity()
    events: list[str] = []

    def start_worker():
        events.append("worker")
        return "ran"

    result = asyncio.run(run_after_wave_release(identity, _Barrier(events, identity), start_worker))

    assert result == "ran"
    assert events == ["ready", "release", "worker"]


def test_runtime_image_mismatch_in_release_prevents_worker_construction():
    identity = _identity()
    events: list[str] = []

    with pytest.raises(PTGWaveContractError, match="runtime_image_identity"):
        asyncio.run(
            run_after_wave_release(
                identity,
                _WrongRuntimeImageBarrier(events, identity),
                lambda: events.append("worker"),
            )
        )

    assert events == ["ready", "release"]


def test_worker_identity_rejects_shared_queue_before_registering():
    identity = _identity(0)
    with pytest.raises(PTGWaveContractError, match="queue"):
        replace(identity, queue="arq:PTGSmall").validate()


def test_wave_worker_defers_arq_construction_until_barrier_release():
    wave_worker_identity = _identity()
    wave_environment_by_name = {
        "HLTHPRT_PTG_WAVE_DIGEST": wave_worker_identity.wave_digest,
        "HLTHPRT_ACTIVE_WORKER_QUEUE": wave_worker_identity.queue,
        "HLTHPRT_ACTIVE_WORKER_CLASS": wave_worker_identity.worker_class,
        "HLTHPRT_PTG_WAVE_SLOT_INDEX": str(wave_worker_identity.slot_index),
        "HLTHPRT_PTG_WAVE_POD_UID": wave_worker_identity.pod_uid,
        "HLTHPRT_PTG_WAVE_REDIS_MANIFEST_DIGEST": wave_worker_identity.manifest_digest,
        "HLTHPRT_PTG_WAVE_JOBS_DIGEST": wave_worker_identity.jobs_digest,
        "HLTHPRT_PTG_WAVE_JOB_COUNT": str(wave_worker_identity.job_count),
        "HLTHPRT_PTG_WAVE_CONFIG_IDENTITY": wave_worker_identity.config_identity,
        "HLTHPRT_PTG_WAVE_MANIFEST_IDENTITY": wave_worker_identity.manifest_identity,
        "HLTHPRT_PTG_WAVE_IMAGE_IDENTITY": wave_worker_identity.image_identity,
        "HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY": (
            wave_worker_identity.runtime_image_identity
        ),
    }
    events: list[str] = []

    def worker_runner(actual_wave_worker_identity):
        assert actual_wave_worker_identity == wave_worker_identity
        events.append("worker")
        return "ran"

    with patch.dict("os.environ", wave_environment_by_name):
        run_result = asyncio.run(
            run_wave_worker(
                barrier_factory=lambda actual_wave_worker_identity: _Barrier(
                    events,
                    actual_wave_worker_identity,
                ),
                worker_runner=worker_runner,
            )
        )

    assert run_result == "ran"
    assert events == ["ready", "release", "worker"]


def test_released_slot_drains_its_dedicated_queue_with_one_concurrent_job():
    wave_worker_identity = _identity()
    worker_calls: list[tuple[type, dict]] = []

    class BaseSettings:
        pass

    class AsyncWorker:
        async def async_run(self):
            return "ran"

    def create_fake_worker(worker_settings, **worker_options):
        worker_calls.append((worker_settings, worker_options))
        return AsyncWorker()

    with patch.dict("os.environ", {"HLTHPRT_PTG_WAVE_WORKER_SETTINGS": "process.PTGSmall"}):
        with patch.object(ptg_wave_worker, "import_string", return_value=BaseSettings):
            with patch.object(ptg_wave_worker, "create_worker", side_effect=create_fake_worker):
                assert asyncio.run(ptg_wave_worker._drain_wave_queue(wave_worker_identity)) == "ran"

    worker_settings, worker_options = worker_calls[0]
    assert worker_settings.queue_name == wave_worker_identity.queue
    assert worker_settings.max_jobs == 1
    assert worker_settings.queue_read_limit == 12
    assert worker_options == {
        "burst": True,
        "max_jobs": 1,
        "queue_read_limit": 12,
    }
