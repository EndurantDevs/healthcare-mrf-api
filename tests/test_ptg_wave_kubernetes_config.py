"""Public-safe worker configuration and template round-trip contracts."""

from __future__ import annotations

import copy
from unittest.mock import Mock

import pytest

from api import ptg_wave_kubernetes_config as config


def _install_worker_sources(
    monkeypatch,
    *,
    volumes=(),
    secret_refs=(),
    environment_sources=(),
    resources=None,
):
    worker_spec = object()
    monkeypatch.setattr(
        config.control_workers,
        "_BY_WORKER_CLASS",
        {config.PTG_WAVE_WORKER_CLASS: worker_spec},
    )
    monkeypatch.setattr(
        config.control_workers,
        "_worker_job_pvc_volumes",
        Mock(return_value=list(volumes)),
    )
    monkeypatch.setattr(
        config.control_workers,
        "_worker_job_secret_volumes",
        Mock(return_value=[]),
    )
    monkeypatch.setattr(
        config.control_workers,
        "_worker_job_secret_env",
        Mock(return_value=list(secret_refs)),
    )
    monkeypatch.setattr(
        config.control_workers,
        "_worker_job_env_from",
        Mock(return_value=list(environment_sources)),
    )
    monkeypatch.setattr(config.control_workers, "_worker_python", Mock(return_value="python3"))
    monkeypatch.setattr(config.control_workers, "_repo_root", Mock(return_value="/srv/app"))
    monkeypatch.setattr(
        config.control_workers,
        "_worker_job_resources",
        Mock(return_value={} if resources is None else resources),
    )
    monkeypatch.setattr(
        config.control_workers,
        "_worker_job_container_security_context",
        Mock(return_value={"allowPrivilegeEscalation": False}),
    )
    monkeypatch.setattr(
        config.control_workers,
        "_worker_job_pod_security_context",
        Mock(return_value={"runAsNonRoot": True}),
    )
    monkeypatch.setattr(
        config.control_workers,
        "_csv",
        lambda value: [item.strip() for item in value.split(",") if item.strip()],
    )


def _contract_values():
    return {
        "image": "registry.example/engine@sha256:" + "1" * 64,
        "queue": "arq:PTGSmall:wave:" + "2" * 64,
        "wave_digest": "2" * 64,
        "manifest_digest": "3" * 64,
        "jobs_digest": "4" * 64,
        "job_count": "25",
        "config_identity": "5" * 64,
        "manifest_identity": "6" * 64,
        "runtime_image_identity": "sha256:" + "7" * 64,
        "barrier_factory": "process.ptg_wave_redis_adapter.create_ptg_wave_redis_barrier",
    }


def test_full_worker_config_round_trips_through_rendered_template(monkeypatch):
    volume_by_field = {
        "volume": {"name": "cache", "emptyDir": {}},
        "volumeMount": {"name": "cache", "mountPath": "/cache"},
    }
    secret_ref_by_field = {
        "name": "DATABASE_URL",
        "valueFrom": {
            "secretKeyRef": {"name": "database", "key": "url", "optional": False},
        },
    }
    source_by_field = {"prefix": "APP_", "configMapRef": {"name": "worker", "optional": True}}
    _install_worker_sources(
        monkeypatch,
        volumes=[volume_by_field],
        secret_refs=[secret_ref_by_field],
        environment_sources=[source_by_field],
        resources={"limits": {"memory": "12Gi"}},
    )
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_POLICY", "Always")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT", "ptg-wave")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_SECRET", "pull-one,pull-two")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "node-unit")

    worker_config = config.build_worker_config()
    pod_spec = config.render_wave_pod_spec(worker_config, _contract_values())
    container = pod_spec["containers"][0]

    assert pod_spec["volumes"] == [volume_by_field["volume"]]
    assert pod_spec["serviceAccountName"] == "ptg-wave"
    assert pod_spec["imagePullSecrets"] == [
        {"name": "pull-one"},
        {"name": "pull-two"},
    ]
    assert container["envFrom"] == [source_by_field]
    assert container["resources"] == {"limits": {"memory": "12Gi"}}
    assert container["volumeMounts"] == [volume_by_field["volumeMount"]]
    assert secret_ref_by_field in container["env"]
    assert config.worker_config_identity_from_template({"spec": pod_spec}) == worker_config["identity"]


def test_minimal_worker_config_omits_optional_pod_and_container_fields(monkeypatch):
    _install_worker_sources(monkeypatch)
    worker_config = config.build_worker_config()
    pod_spec = config.render_wave_pod_spec(worker_config, _contract_values())
    container = pod_spec["containers"][0]
    assert not {"volumes", "serviceAccountName", "imagePullSecrets"} & set(pod_spec)
    assert not {"envFrom", "resources", "volumeMounts"} & set(container)
    assert config.worker_config_identity_from_template({"spec": pod_spec}) == worker_config["identity"]


@pytest.mark.parametrize(
    ("reference", "message"),
    [
        ({"name": "X", "value": "secret"}, "secret values"),
        ({"name": "X", "valueFrom": {}}, "reference-only"),
        ({"name": "X", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}}, "reference-only"),
        ({"name": "X", "valueFrom": {"secretKeyRef": []}}, "must be an object"),
        ({"name": "X", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k", "extra": True}}}, "shape is invalid"),
        ({"name": "X", "valueFrom": {"secretKeyRef": {"name": "", "key": "k"}}}, "secret name"),
        ({"name": "X", "valueFrom": {"secretKeyRef": {"name": "s", "key": ""}}}, "secret key"),
        ({"name": "X", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k", "optional": "no"}}}, "optional must be boolean"),
    ],
)
def test_secret_environment_is_reference_only(reference, message):
    with pytest.raises(config.PTGWaveContractError, match=message):
        config._require_reference_only_environment([reference])


@pytest.mark.parametrize(
    ("reference", "message"),
    [
        ({"name": "X", "value": "literal"}, "secret values"),
        ({"name": "X", "valueFrom": {"fieldRef": {"fieldPath": " metadata.uid"}}}, "fieldPath"),
        ({"name": "X", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid", "extra": True}}}, "shape is invalid"),
        ({"name": "X", "valueFrom": {"configMapKeyRef": {"name": "x"}}}, "reference shape"),
    ],
)
def test_rendered_environment_allows_only_secret_or_downward_references(reference, message):
    with pytest.raises(config.PTGWaveContractError, match=message):
        config._require_safe_environment_refs([reference])


def test_rendered_environment_accepts_secret_and_downward_references():
    config._require_safe_environment_refs([
        {"name": "S", "valueFrom": {"secretKeyRef": {"name": "secret", "key": "key"}}},
        {"name": "P", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.uid"}}},
    ])


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ({}, "shape is invalid"),
        ({"configMapRef": {"name": "one"}, "secretRef": {"name": "two"}}, "shape is invalid"),
        ({"secretRef": {"name": "one"}, "extra": True}, "shape is invalid"),
        ({"secretRef": []}, "must be an object"),
        ({"secretRef": {"name": "one", "key": "bad"}}, "shape is invalid"),
        ({"secretRef": {"name": ""}}, "envFrom name"),
        ({"secretRef": {"name": "one", "optional": "no"}}, "optional must be boolean"),
    ],
)
def test_env_from_sources_are_named_references_only(source, message):
    with pytest.raises(config.PTGWaveContractError, match=message):
        config._require_reference_only_sources([source])


def test_template_verifier_rejects_malformed_public_shapes(monkeypatch):
    _install_worker_sources(monkeypatch)
    pod_spec = config.render_wave_pod_spec(config.build_worker_config(), _contract_values())

    invalid_templates = []
    for containers in (None, [], [{}, {}]):
        invalid = copy.deepcopy(pod_spec)
        invalid["containers"] = containers
        invalid_templates.append(({"spec": invalid}, "exactly one"))

    invalid = copy.deepcopy(pod_spec)
    invalid["containers"][0]["env"] = {}
    invalid_templates.append(({"spec": invalid}, "env must be a list"))

    invalid = copy.deepcopy(pod_spec)
    invalid["containers"][0]["envFrom"] = {}
    invalid_templates.append(({"spec": invalid}, "envFrom must be a list"))

    invalid = copy.deepcopy(pod_spec)
    invalid["imagePullSecrets"] = {}
    invalid_templates.append(({"spec": invalid}, "imagePullSecrets must be a list"))

    invalid = copy.deepcopy(pod_spec)
    invalid["imagePullSecrets"] = [None]
    invalid_templates.append(({"spec": invalid}, "image pull secret must be an object"))

    invalid_templates.append(({"spec": []}, "must be an object"))

    for template, message in invalid_templates:
        with pytest.raises(config.PTGWaveContractError, match=message):
            config.worker_config_identity_from_template(template)
