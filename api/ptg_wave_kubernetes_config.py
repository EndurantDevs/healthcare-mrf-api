# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic, public-safe worker configuration for PTG wave Jobs."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Mapping, Sequence

from api import control_workers


PTG_WAVE_WORKER_CLASS = "process.PTGSmall"
DOWNWARD_SLOT_FIELD = (
    "metadata.annotations['batch.kubernetes.io/job-completion-index']"
)


class PTGWaveContractError(ValueError):
    """Raised when a manifest or observation is not an exact PTG wave."""


def build_worker_config() -> dict[str, Any]:
    """Capture the deployed PTGSmall worker shape without reading secret data."""

    worker_spec = control_workers._BY_WORKER_CLASS[PTG_WAVE_WORKER_CLASS]
    volume_specs = [
        *control_workers._worker_job_pvc_volumes(),
        *control_workers._worker_job_secret_volumes(worker_spec),
    ]
    secret_environment_refs = control_workers._worker_job_secret_env(
        PTG_WAVE_WORKER_CLASS
    )
    _require_reference_only_environment(secret_environment_refs)
    environment_sources = control_workers._worker_job_env_from()
    _require_reference_only_sources(environment_sources)
    config_by_field = {
        "schema": "healthporta.ptg-wave-worker-config.v1",
        "worker_class": PTG_WAVE_WORKER_CLASS,
        "command": [
            control_workers._worker_python(),
            "-m",
            "process.ptg_wave_worker",
        ],
        "working_dir": str(control_workers._repo_root()),
        "image_pull_policy": os.getenv(
            "HLTHPRT_WORKER_JOB_IMAGE_PULL_POLICY",
            "IfNotPresent",
        ),
        "resources": control_workers._worker_job_resources(worker_spec),
        "container_security": (
            control_workers._worker_job_container_security_context()
        ),
        "pod_security": control_workers._worker_job_pod_security_context(
            has_group_read_volume=bool(volume_specs),
        ),
        "volume_mounts": [
            volume_spec["volumeMount"] for volume_spec in volume_specs
        ],
        "volumes": [volume_spec["volume"] for volume_spec in volume_specs],
        "environment_sources": environment_sources,
        "environment_refs": [
            *_wave_downward_environment(),
            *secret_environment_refs,
        ],
        "service_account": os.getenv(
            "HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT",
            "",
        ).strip(),
        "image_pull_secrets": control_workers._csv(
            os.getenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_SECRET", "").strip()
        ),
    }
    return {
        **config_by_field,
        "secret_environment_refs": secret_environment_refs,
        "identity": _canonical_digest(config_by_field),
    }


def render_wave_pod_spec(
    worker_config: Mapping[str, Any],
    contract_values_by_name: Mapping[str, str],
) -> dict[str, Any]:
    """Render the pod portion of the immutable PTG wave contract."""

    container_by_field = _wave_container(
        worker_config,
        contract_values_by_name,
    )
    pod_spec_by_field: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "securityContext": worker_config["pod_security"],
        "containers": [container_by_field],
    }
    if worker_config["volumes"]:
        pod_spec_by_field["volumes"] = worker_config["volumes"]
    if worker_config["service_account"]:
        pod_spec_by_field["serviceAccountName"] = worker_config["service_account"]
    if worker_config["image_pull_secrets"]:
        pod_spec_by_field["imagePullSecrets"] = [
            {"name": secret_name}
            for secret_name in worker_config["image_pull_secrets"]
        ]
    return pod_spec_by_field


def _wave_container(
    worker_config: Mapping[str, Any],
    contract_values_by_name: Mapping[str, str],
) -> dict[str, Any]:
    container_by_field: dict[str, Any] = {
        "name": "ptg-wave-worker",
        "image": contract_values_by_name["image"],
        "imagePullPolicy": worker_config["image_pull_policy"],
        "workingDir": worker_config["working_dir"],
        "command": worker_config["command"],
        "env": _wave_environment(contract_values_by_name),
        "securityContext": worker_config["container_security"],
    }
    container_by_field["env"].extend(
        worker_config["secret_environment_refs"]
    )
    if worker_config["environment_sources"]:
        container_by_field["envFrom"] = worker_config["environment_sources"]
    if worker_config["resources"]:
        container_by_field["resources"] = worker_config["resources"]
    if worker_config["volume_mounts"]:
        container_by_field["volumeMounts"] = worker_config["volume_mounts"]
    return container_by_field


def _wave_environment(
    contract_values_by_name: Mapping[str, str],
) -> list[dict[str, Any]]:
    scalar_environment_entries = [
        {"name": "HLTHPRT_WORKER_LAUNCHER", "value": "process"},
        {
            "name": "HLTHPRT_IMPORT_NODE_ID",
            "value": os.getenv("HLTHPRT_IMPORT_NODE_ID", ""),
        },
        {
            "name": "HLTHPRT_ACTIVE_WORKER_CLASS",
            "value": PTG_WAVE_WORKER_CLASS,
        },
        {
            "name": "HLTHPRT_ACTIVE_WORKER_QUEUE",
            "value": contract_values_by_name["queue"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_DIGEST",
            "value": contract_values_by_name["wave_digest"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_REDIS_MANIFEST_DIGEST",
            "value": contract_values_by_name["manifest_digest"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_JOBS_DIGEST",
            "value": contract_values_by_name["jobs_digest"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_JOB_COUNT",
            "value": contract_values_by_name["job_count"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_CONFIG_IDENTITY",
            "value": contract_values_by_name["config_identity"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_MANIFEST_IDENTITY",
            "value": contract_values_by_name["manifest_identity"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_IMAGE_IDENTITY",
            "value": contract_values_by_name["image"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY",
            "value": contract_values_by_name["runtime_image_identity"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_BARRIER_FACTORY",
            "value": contract_values_by_name["barrier_factory"],
        },
        {
            "name": "HLTHPRT_PTG_WAVE_WORKER_SETTINGS",
            "value": PTG_WAVE_WORKER_CLASS,
        },
    ]
    return [*scalar_environment_entries, *_wave_downward_environment()]


def _wave_downward_environment() -> list[dict[str, Any]]:
    return [
        {
            "name": "HLTHPRT_PTG_WAVE_SLOT_INDEX",
            "valueFrom": {"fieldRef": {"fieldPath": DOWNWARD_SLOT_FIELD}},
        },
        {
            "name": "HLTHPRT_PTG_WAVE_POD_UID",
            "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}},
        },
    ]


def _require_reference_only_environment(
    environment_refs: Sequence[Mapping[str, Any]],
) -> None:
    for environment_ref in environment_refs:
        if "value" in environment_ref:
            raise PTGWaveContractError(
                "worker config identity cannot include secret values"
            )
        value_from = _mapping(
            environment_ref.get("valueFrom"),
            "secret environment valueFrom",
        )
        if set(value_from) != {"secretKeyRef"}:
            raise PTGWaveContractError(
                "worker secret environment must be reference-only"
            )
        _require_named_key_reference(value_from["secretKeyRef"])


def _require_safe_environment_refs(
    environment_refs: Sequence[Mapping[str, Any]],
) -> None:
    for environment_ref in environment_refs:
        if "value" in environment_ref:
            raise PTGWaveContractError(
                "worker config identity cannot include secret values"
            )
        value_from = _mapping(
            environment_ref.get("valueFrom"),
            "worker environment valueFrom",
        )
        if set(value_from) == {"secretKeyRef"}:
            _require_named_key_reference(value_from["secretKeyRef"])
        elif set(value_from) == {"fieldRef"}:
            field_ref = _mapping(value_from["fieldRef"], "worker fieldRef")
            if set(field_ref) - {"apiVersion", "fieldPath"}:
                raise PTGWaveContractError(
                    "worker environment fieldRef shape is invalid"
                )
            _require_nonempty_text(field_ref.get("fieldPath"), "fieldPath")
        else:
            raise PTGWaveContractError(
                "worker environment reference shape is invalid"
            )


def _require_named_key_reference(reference: Any) -> None:
    reference_by_field = _mapping(reference, "worker secretKeyRef")
    if set(reference_by_field) - {"name", "key", "optional"}:
        raise PTGWaveContractError(
            "worker secret environment reference shape is invalid"
        )
    _require_nonempty_text(reference_by_field.get("name"), "secret name")
    _require_nonempty_text(reference_by_field.get("key"), "secret key")
    if "optional" in reference_by_field and not isinstance(
        reference_by_field["optional"],
        bool,
    ):
        raise PTGWaveContractError(
            "worker secret environment optional must be boolean"
        )


def _require_reference_only_sources(
    environment_sources: Sequence[Mapping[str, Any]],
) -> None:
    for environment_source in environment_sources:
        source_kinds = set(environment_source) & {"configMapRef", "secretRef"}
        if len(source_kinds) != 1 or set(environment_source) - {
            "prefix",
            *source_kinds,
        }:
            raise PTGWaveContractError("worker envFrom reference shape is invalid")
        source_ref = _mapping(
            environment_source[next(iter(source_kinds))],
            "worker envFrom reference",
        )
        if set(source_ref) - {"name", "optional"}:
            raise PTGWaveContractError("worker envFrom reference shape is invalid")
        _require_nonempty_text(source_ref.get("name"), "envFrom name")
        if "optional" in source_ref and not isinstance(source_ref["optional"], bool):
            raise PTGWaveContractError("worker envFrom optional must be boolean")


def worker_config_identity_from_template(template: Mapping[str, Any]) -> str:
    """Recompute the public worker-config digest from a rendered Job template."""

    pod_spec_by_field = _mapping(template.get("spec"), "spec.template.spec")
    containers = pod_spec_by_field.get("containers")
    if not isinstance(containers, list) or len(containers) != 1:
        raise PTGWaveContractError(
            "wave job must have exactly one worker container"
        )
    container_by_field = _mapping(
        containers[0],
        "spec.template.spec.containers[0]",
    )
    environment_entries = container_by_field.get("env")
    if not isinstance(environment_entries, list):
        raise PTGWaveContractError("worker env must be a list")
    environment_refs = [
        environment_entry
        for environment_entry in environment_entries
        if isinstance(environment_entry, Mapping) and "valueFrom" in environment_entry
    ]
    _require_safe_environment_refs(environment_refs)
    environment_sources = container_by_field.get("envFrom", [])
    if not isinstance(environment_sources, list):
        raise PTGWaveContractError("worker envFrom must be a list")
    _require_reference_only_sources(environment_sources)
    config_by_field = _template_config_shape(
        pod_spec_by_field,
        container_by_field,
        environment_refs,
        environment_sources,
    )
    return _canonical_digest(config_by_field)


def _template_config_shape(
    pod_spec_by_field: Mapping[str, Any],
    container_by_field: Mapping[str, Any],
    environment_refs: Sequence[Mapping[str, Any]],
    environment_sources: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    pull_secret_entries = pod_spec_by_field.get("imagePullSecrets", [])
    if not isinstance(pull_secret_entries, list):
        raise PTGWaveContractError("imagePullSecrets must be a list")
    return {
        "schema": "healthporta.ptg-wave-worker-config.v1",
        "worker_class": PTG_WAVE_WORKER_CLASS,
        "command": container_by_field.get("command"),
        "working_dir": container_by_field.get("workingDir"),
        "image_pull_policy": container_by_field.get("imagePullPolicy"),
        "resources": container_by_field.get("resources", {}),
        "container_security": container_by_field.get("securityContext"),
        "pod_security": pod_spec_by_field.get("securityContext"),
        "volume_mounts": container_by_field.get("volumeMounts", []),
        "volumes": pod_spec_by_field.get("volumes", []),
        "environment_sources": environment_sources,
        "environment_refs": environment_refs,
        "service_account": pod_spec_by_field.get("serviceAccountName", ""),
        "image_pull_secrets": [
            _mapping(pull_secret, "image pull secret").get("name")
            for pull_secret in pull_secret_entries
        ],
    }


def _mapping(object_value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(object_value, Mapping):
        raise PTGWaveContractError(f"{name} must be an object")
    return object_value


def _canonical_digest(object_value: Any) -> str:
    encoded = json.dumps(
        object_value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_nonempty_text(text_value: Any, name: str) -> str:
    if (
        not isinstance(text_value, str)
        or not text_value.strip()
        or text_value != text_value.strip()
    ):
        raise PTGWaveContractError(f"{name} must be a non-empty trimmed string")
    return text_value
