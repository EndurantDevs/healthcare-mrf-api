"""Unit contracts for the exact-wave Kubernetes I/O boundary."""

from __future__ import annotations

import copy
from unittest.mock import Mock

import pytest

from api import ptg_wave_kubernetes_client as client
from api.ptg_wave_kubernetes import PTGWaveContractError, build_ptg_wave_job


_WAVE = "a" * 64


def _manifest() -> dict:
    return build_ptg_wave_job(
        wave_digest=_WAVE,
        manifest_digest="b" * 64,
        jobs_digest="c" * 64,
        job_count=12,
        image="registry.example/engine@sha256:" + "d" * 64,
        runtime_image_identity="sha256:" + "e" * 64,
        barrier_factory="process.ptg_wave_redis_adapter.create_ptg_wave_redis_barrier",
    )


@pytest.fixture
def kubernetes(monkeypatch):
    request = Mock()
    monkeypatch.setattr(client, "_kubernetes_request", request)
    monkeypatch.setattr(client, "_kubernetes_namespace", Mock(return_value="mrf workers"))
    return request


def test_post_get_and_delete_use_exact_validated_paths(kubernetes):
    manifest = _manifest()
    kubernetes.return_value = {"kind": "Job", "metadata": {"uid": "job-uid"}}

    assert client.post_wave_job(manifest)["kind"] == "Job"
    assert kubernetes.call_args.args == (
        "POST",
        "/apis/batch/v1/namespaces/mrf%20workers/jobs",
        manifest,
    )

    kubernetes.reset_mock()
    assert client.get_wave_job(_WAVE)["metadata"]["uid"] == "job-uid"
    method, path = kubernetes.call_args.args
    assert method == "GET"
    assert path.endswith("/jobs/" + manifest["metadata"]["name"])

    kubernetes.reset_mock()
    assert client.delete_wave_job(_WAVE, "job uid")["kind"] == "Job"
    method, path, body = kubernetes.call_args.args
    assert method == "DELETE"
    assert path.endswith("/jobs/" + manifest["metadata"]["name"])
    assert body == {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "preconditions": {"uid": "job uid"},
        "propagationPolicy": "Foreground",
    }


def test_get_translates_only_not_found(kubernetes):
    kubernetes.side_effect = client.KubernetesApiError(404, "missing")
    assert client.get_wave_job(_WAVE) is None

    kubernetes.side_effect = client.KubernetesApiError(403, "forbidden")
    with pytest.raises(client.KubernetesApiError, match="forbidden"):
        client.get_wave_job(_WAVE)


def test_wave_pod_list_binds_the_complete_label_selector(kubernetes):
    kubernetes.return_value = {
        "apiVersion": "v1",
        "kind": "PodList",
        "items": [{"metadata": {"uid": "pod-1"}}],
    }
    assert client.list_wave_pods(_WAVE) == [
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"uid": "pod-1"},
        },
    ]
    method, path = kubernetes.call_args.args
    assert method == "GET"
    assert path.startswith("/api/v1/namespaces/mrf%20workers/pods?")
    assert "labelSelector=" in path
    assert "healthporta.com%2Fptg-wave-digest-hash%3Dffe054fe7ae0cb6d" in path

    for invalid in (
        {},
        {"apiVersion": "v1", "kind": "PodList", "items": {}},
        {"apiVersion": "v1", "kind": "PodList", "items": [None]},
        {"apiVersion": "batch/v1", "kind": "PodList", "items": []},
        {"apiVersion": "v1", "kind": "List", "items": []},
        {
            "apiVersion": "v1",
            "kind": "PodList",
            "items": [{"apiVersion": "v2", "kind": "Pod"}],
        },
        {
            "apiVersion": "v1",
            "kind": "PodList",
            "items": [{"apiVersion": "v1", "kind": "Job"}],
        },
    ):
        kubernetes.return_value = invalid
        with pytest.raises(PTGWaveContractError, match="v1 Pod|object items array"):
            client.list_wave_pods(_WAVE)


def test_generic_job_list_includes_only_ptg_family_workers(kubernetes):
    def job(worker_class=None, *, metadata=True):
        annotations = (
            {} if worker_class is None
            else {"healthporta.com/worker-class": worker_class}
        )
        return (
            {"metadata": {"annotations": annotations}}
            if metadata else {"metadata": None}
        )

    included = job("process.PTGSmall")
    included_with_space = job("  process.PTGNormal  ")
    kubernetes.return_value = {
        "items": [
            included,
            included_with_space,
            job("process.FHIR"),
            job("other.PTG"),
            job(),
            job(metadata=False),
        ],
    }
    assert client.list_generic_ptg_jobs() == [included, included_with_space]
    method, path = kubernetes.call_args.args
    assert method == "GET"
    assert "managed-by%3Dhealthporta-worker-launcher" in path
    assert "engine%3Dmrf" in path

    for invalid in ({}, {"items": "jobs"}, {"items": [None]}):
        kubernetes.return_value = invalid
        with pytest.raises(PTGWaveContractError, match="object items array"):
            client.list_generic_ptg_jobs()


def test_absence_observation_is_get_only(monkeypatch):
    get_job = Mock(return_value=None)
    list_pods = Mock(return_value=[])
    monkeypatch.setattr(client, "get_wave_job", get_job)
    monkeypatch.setattr(client, "list_wave_pods", list_pods)
    assert client.wave_absence_observation(_WAVE) == {
        "job_absent": True,
        "pod_count": 0,
        "pods_absent": True,
    }

    get_job.return_value = {"metadata": {"uid": "job"}}
    list_pods.return_value = [{"metadata": {"uid": "pod"}}]
    assert client.wave_absence_observation(_WAVE) == {
        "job_absent": False,
        "pod_count": 1,
        "pods_absent": False,
    }


def test_boundary_rejects_invalid_objects_and_text(kubernetes, monkeypatch):
    kubernetes.return_value = []
    with pytest.raises(PTGWaveContractError, match="response must be an object"):
        client.get_wave_job(_WAVE)

    with pytest.raises(PTGWaveContractError):
        client.post_wave_job({})

    with pytest.raises(PTGWaveContractError, match="UID"):
        client.delete_wave_job(_WAVE, "")

    monkeypatch.setattr(client, "_kubernetes_namespace", Mock(return_value=""))
    with pytest.raises(PTGWaveContractError, match="namespace"):
        client.list_wave_pods(_WAVE)

    monkeypatch.setattr(client, "_kubernetes_namespace", Mock(return_value="mrf workers"))
    assert client._job_path("name/with space").endswith("/name%2Fwith%20space")
    with pytest.raises(PTGWaveContractError, match="Job name"):
        client._job_path("")


def test_post_copies_the_caller_manifest(kubernetes):
    manifest = _manifest()
    expected = copy.deepcopy(manifest)
    kubernetes.return_value = {"metadata": {"uid": "job"}}
    client.post_wave_job(manifest)
    assert manifest == expected
    assert kubernetes.call_args.args[2] is not manifest
