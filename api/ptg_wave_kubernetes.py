# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed Kubernetes contract for an exact twelve-slot PTG import wave.

This module deliberately does not create Jobs or talk to Redis.  The durable
wave controller owns those side effects; it supplies the immutable wave inputs
and records the returned manifest and slot receipts.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Mapping

from api.ptg_wave_kubernetes_config import (
    DOWNWARD_SLOT_FIELD as _DOWNWARD_SLOT_FIELD,
    PTGWaveContractError,
    PTG_WAVE_WORKER_CLASS,
    build_worker_config,
    _canonical_digest,
    _mapping,
    _require_reference_only_environment,
    render_wave_pod_spec,
    worker_config_identity_from_template as _worker_config_identity_from_template,
)


PTG_WAVE_SLOT_COUNT = 12
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_FACTORY_RE = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+$")
_IMAGE_RE = re.compile(r"^\S+@sha256:[0-9a-f]{64}$")
_RUNTIME_IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class PTGWaveJobContract:
    wave_digest: str
    queue: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    config_identity: str
    manifest_identity: str
    image: str
    runtime_image_identity: str


def queue_for_wave(wave_digest: str) -> str:
    """Return the only queue permitted for a validated PTG wave digest."""

    _require_digest("wave_digest", wave_digest)
    return f"arq:PTGSmall:wave:{wave_digest}"


def build_ptg_wave_job(
    *,
    wave_digest: str,
    manifest_digest: str,
    jobs_digest: str,
    job_count: int,
    image: str,
    runtime_image_identity: str,
    barrier_factory: str,
) -> dict[str, Any]:
    """Build one Indexed Job whose twelve slots cannot poll before release.

    ``barrier_factory`` is a trusted, deployed callable selected by the
    controller integration.  This pure builder does not provide a fallback:
    an absent or invalid factory is rejected before a Job can be created.
    """

    worker_config_by_field = build_worker_config()
    contract_values_by_name = _validated_contract_values(
        wave_digest=wave_digest,
        manifest_digest=manifest_digest,
        jobs_digest=jobs_digest,
        job_count=job_count,
        image=image,
        runtime_image_identity=runtime_image_identity,
        barrier_factory=barrier_factory,
        worker_config_by_field=worker_config_by_field,
    )
    return _render_wave_job(contract_values_by_name, worker_config_by_field)


def _validated_contract_values(
    *,
    wave_digest: str,
    manifest_digest: str,
    jobs_digest: str,
    job_count: int,
    image: str,
    runtime_image_identity: str,
    barrier_factory: str,
    worker_config_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    _require_digest("wave_digest", wave_digest)
    _require_digest("manifest_digest", manifest_digest)
    _require_digest("jobs_digest", jobs_digest)
    immutable_values_by_name = {
        "wave_digest": wave_digest,
        "queue": queue_for_wave(wave_digest),
        "manifest_digest": manifest_digest,
        "jobs_digest": jobs_digest,
        "job_count": _require_job_count(job_count),
        "image": _require_image(image),
        "runtime_image_identity": _require_runtime_image_identity(
            runtime_image_identity
        ),
        "config_identity": worker_config_by_field["identity"],
        "barrier_factory": _require_factory(barrier_factory),
    }
    return {
        **immutable_values_by_name,
        "manifest_identity": _manifest_identity(**immutable_values_by_name),
    }


def _render_wave_job(
    contract_values_by_name: Mapping[str, Any],
    worker_config_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    wave_digest = contract_values_by_name["wave_digest"]
    labels_by_name = _wave_labels(wave_digest)
    annotations_by_name = _wave_annotations(**contract_values_by_name)
    pod_contract_values_by_name = {
        contract_name: str(contract_value)
        for contract_name, contract_value in contract_values_by_name.items()
    }

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": _job_name(wave_digest),
            "labels": labels_by_name,
            "annotations": annotations_by_name,
        },
        "spec": {
            "backoffLimit": 0,
            "completionMode": "Indexed",
            "completions": PTG_WAVE_SLOT_COUNT,
            "parallelism": PTG_WAVE_SLOT_COUNT,
            "template": {
                "metadata": {
                    "labels": labels_by_name,
                    "annotations": annotations_by_name,
                },
                "spec": render_wave_pod_spec(
                    worker_config_by_field,
                    pod_contract_values_by_name,
                ),
            },
        },
    }


def _wave_annotations(**contract_values: str | int) -> dict[str, str]:
    return {
        "healthporta.com/ptg-wave-digest": str(contract_values["wave_digest"]),
        "healthporta.com/ptg-wave-queue": str(contract_values["queue"]),
        "healthporta.com/ptg-wave-redis-manifest-digest": str(
            contract_values["manifest_digest"]
        ),
        "healthporta.com/ptg-wave-jobs-digest": str(contract_values["jobs_digest"]),
        "healthporta.com/ptg-wave-job-count": str(contract_values["job_count"]),
        "healthporta.com/ptg-wave-worker-class": PTG_WAVE_WORKER_CLASS,
        "healthporta.com/ptg-wave-runtime-image-identity": str(
            contract_values["runtime_image_identity"]
        ),
        "healthporta.com/ptg-wave-config-identity": str(
            contract_values["config_identity"]
        ),
        "healthporta.com/ptg-wave-manifest-identity": str(
            contract_values["manifest_identity"]
        ),
    }


def validate_ptg_wave_job_manifest(manifest: Mapping[str, Any]) -> PTGWaveJobContract:
    """Validate the immutable PTG-only Job shape and return its contract."""

    if manifest.get("apiVersion") != "batch/v1" or manifest.get("kind") != "Job":
        raise PTGWaveContractError("expected a batch/v1 Job")
    contract_values_by_name = _validate_wave_metadata(manifest)
    pinned_image, barrier_factory = _validate_wave_job_spec(
        manifest,
        contract_values_by_name,
    )
    immutable_values_by_name = {
        name: value
        for name, value in contract_values_by_name.items()
        if name != "manifest_identity"
    }
    expected_identity = _manifest_identity(
        image=pinned_image,
        barrier_factory=barrier_factory,
        **immutable_values_by_name,
    )
    if contract_values_by_name["manifest_identity"] != expected_identity:
        raise PTGWaveContractError("manifest identity does not bind the exact wave job")
    return PTGWaveJobContract(image=pinned_image, **contract_values_by_name)


def _validate_wave_metadata(manifest: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _mapping(manifest.get("metadata"), "metadata")
    annotations_by_name = _mapping(metadata.get("annotations"), "metadata.annotations")
    wave_digest = _annotation(annotations_by_name, "healthporta.com/ptg-wave-digest")
    _require_digest("healthporta.com/ptg-wave-digest", wave_digest)
    if metadata.get("name") != _job_name(wave_digest):
        raise PTGWaveContractError("job name does not bind the canonical wave digest")
    queue = _annotation(annotations_by_name, "healthporta.com/ptg-wave-queue")
    if queue != queue_for_wave(wave_digest):
        raise PTGWaveContractError("wave queue does not bind the full wave digest")
    if _annotation(annotations_by_name, "healthporta.com/ptg-wave-worker-class") != PTG_WAVE_WORKER_CLASS:
        raise PTGWaveContractError("wave worker class must be process.PTGSmall")
    contract_values_by_name = {
        "wave_digest": wave_digest,
        "queue": queue,
        "manifest_digest": _annotation(
            annotations_by_name,
            "healthporta.com/ptg-wave-redis-manifest-digest",
        ),
        "jobs_digest": _annotation(
            annotations_by_name,
            "healthporta.com/ptg-wave-jobs-digest",
        ),
        "job_count": _job_count_from_text(
            _annotation(annotations_by_name, "healthporta.com/ptg-wave-job-count")
        ),
        "runtime_image_identity": _annotation(
            annotations_by_name,
            "healthporta.com/ptg-wave-runtime-image-identity",
        ),
        "config_identity": _annotation(annotations_by_name, "healthporta.com/ptg-wave-config-identity"),
        "manifest_identity": _annotation(annotations_by_name, "healthporta.com/ptg-wave-manifest-identity"),
    }
    _require_digest("healthporta.com/ptg-wave-redis-manifest-digest", contract_values_by_name["manifest_digest"])
    _require_digest("healthporta.com/ptg-wave-jobs-digest", contract_values_by_name["jobs_digest"])
    _require_runtime_image_identity(contract_values_by_name["runtime_image_identity"])
    _require_digest("healthporta.com/ptg-wave-config-identity", contract_values_by_name["config_identity"])
    _require_digest("healthporta.com/ptg-wave-manifest-identity", contract_values_by_name["manifest_identity"])
    if _mapping(metadata.get("labels"), "metadata.labels") != _wave_labels(wave_digest):
        raise PTGWaveContractError("job labels are not PTG-wave-specific")
    return contract_values_by_name


def _validate_wave_job_spec(
    manifest: Mapping[str, Any],
    contract_values_by_name: Mapping[str, Any],
) -> tuple[str, str]:
    spec_by_field = _mapping(manifest.get("spec"), "spec")
    if spec_by_field.get("completionMode") != "Indexed" or spec_by_field.get("backoffLimit") != 0:
        raise PTGWaveContractError("wave job must be Indexed and cannot retry")
    if any(spec_by_field.get(name) != PTG_WAVE_SLOT_COUNT for name in ("completions", "parallelism")):
        raise PTGWaveContractError("wave job must have exactly twelve slots")
    template = _mapping(spec_by_field.get("template"), "spec.template")
    _validate_wave_template_metadata(template, manifest.get("metadata"))
    if _worker_config_identity_from_template(template) != contract_values_by_name["config_identity"]:
        raise PTGWaveContractError("config identity does not bind the rendered worker config")
    pod_spec_by_field = _mapping(template.get("spec"), "spec.template.spec")
    if pod_spec_by_field.get("restartPolicy") != "Never":
        raise PTGWaveContractError("wave workers must not restart in place")
    containers = pod_spec_by_field.get("containers")
    if not isinstance(containers, list) or len(containers) != 1:
        raise PTGWaveContractError("wave job must have exactly one worker container")
    return _validate_wave_container(containers[0], contract_values_by_name)


def _validate_wave_template_metadata(template: Mapping[str, Any], job_metadata: Any) -> None:
    template_metadata = _mapping(template.get("metadata"), "spec.template.metadata")
    job_metadata_by_field = _mapping(job_metadata, "metadata")
    for name in ("labels", "annotations"):
        if template_metadata.get(name) != job_metadata_by_field.get(name):
            raise PTGWaveContractError(f"pod {name} must exactly match the wave job")


def _validate_wave_container(
    container_value: Any,
    contract_values_by_name: Mapping[str, Any],
) -> tuple[str, str]:
    container_by_field = _mapping(container_value, "spec.template.spec.containers[0]")
    command = container_by_field.get("command")
    if container_by_field.get("name") != "ptg-wave-worker" or not _is_wave_command(command):
        raise PTGWaveContractError("wave worker must enter through the release barrier")
    pinned_image = _require_image(container_by_field.get("image"))
    environment_by_name = _environment_by_name(container_by_field.get("env"))
    _validate_wave_environment(environment_by_name, contract_values_by_name, pinned_image)
    return pinned_image, _require_factory(_env_value(environment_by_name, "HLTHPRT_PTG_WAVE_BARRIER_FACTORY"))


def _is_wave_command(command: Any) -> bool:
    return isinstance(command, list) and len(command) == 3 and command[1:] == ["-m", "process.ptg_wave_worker"]


def _validate_wave_environment(
    environment_by_name: Mapping[str, Mapping[str, Any]],
    contract_values_by_name: Mapping[str, Any],
    pinned_image: str,
) -> None:
    expected_by_name = {
        "HLTHPRT_WORKER_LAUNCHER": "process",
        "HLTHPRT_ACTIVE_WORKER_CLASS": PTG_WAVE_WORKER_CLASS,
        "HLTHPRT_ACTIVE_WORKER_QUEUE": contract_values_by_name["queue"],
        "HLTHPRT_PTG_WAVE_DIGEST": contract_values_by_name["wave_digest"],
        "HLTHPRT_PTG_WAVE_REDIS_MANIFEST_DIGEST": contract_values_by_name[
            "manifest_digest"
        ],
        "HLTHPRT_PTG_WAVE_JOBS_DIGEST": contract_values_by_name["jobs_digest"],
        "HLTHPRT_PTG_WAVE_JOB_COUNT": str(contract_values_by_name["job_count"]),
        "HLTHPRT_PTG_WAVE_CONFIG_IDENTITY": contract_values_by_name["config_identity"],
        "HLTHPRT_PTG_WAVE_MANIFEST_IDENTITY": contract_values_by_name["manifest_identity"],
        "HLTHPRT_PTG_WAVE_IMAGE_IDENTITY": pinned_image,
        "HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY": contract_values_by_name[
            "runtime_image_identity"
        ],
        "HLTHPRT_PTG_WAVE_WORKER_SETTINGS": PTG_WAVE_WORKER_CLASS,
    }
    for name, expected_value in expected_by_name.items():
        _require_env_value(environment_by_name, name, expected_value)
    _validate_extra_environment_refs(environment_by_name, set(expected_by_name))
    if _env_field_path(environment_by_name, "HLTHPRT_PTG_WAVE_SLOT_INDEX") != _DOWNWARD_SLOT_FIELD:
        raise PTGWaveContractError("slot index must use Indexed Job downward API")
    if _env_field_path(environment_by_name, "HLTHPRT_PTG_WAVE_POD_UID") != "metadata.uid":
        raise PTGWaveContractError("pod UID must use the downward API")


def _validate_extra_environment_refs(
    environment_by_name: Mapping[str, Mapping[str, Any]],
    scalar_names: set[str],
) -> None:
    fixed_names = scalar_names | {
        "HLTHPRT_WORKER_LAUNCHER",
        "HLTHPRT_IMPORT_NODE_ID",
        "HLTHPRT_PTG_WAVE_BARRIER_FACTORY",
        "HLTHPRT_PTG_WAVE_WORKER_SETTINGS",
        "HLTHPRT_PTG_WAVE_SLOT_INDEX",
        "HLTHPRT_PTG_WAVE_POD_UID",
    }
    extra_refs = [
        environment_ref
        for name, environment_ref in environment_by_name.items()
        if name not in fixed_names
    ]
    _require_reference_only_environment(extra_refs)


def _wave_labels(wave_digest: str) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": "healthporta-ptg-wave-worker",
        "app.kubernetes.io/managed-by": "healthporta-ptg-wave-controller",
        "healthporta.com/engine": "mrf-ptg-wave",
        "healthporta.com/ptg-wave": "true",
        "healthporta.com/ptg-wave-digest-hash": hashlib.sha256(
            wave_digest.encode()
        ).hexdigest()[:16],
    }


def _job_name(wave_digest: str) -> str:
    return f"hpw-ptg-wave-{wave_digest[:40]}"


def _manifest_identity(**values: Any) -> str:
    return _canonical_digest(
        {
            "schema": "healthporta.ptg-wave-kubernetes-manifest.v1",
            **values,
        }
    )


def _require_job_count(value: Any) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= 4096
    ):
        raise PTGWaveContractError("job_count must be from 1 through 4096")
    return value


def _job_count_from_text(value: str) -> int:
    if not value.isdecimal():
        raise PTGWaveContractError("job_count annotation must be canonical decimal")
    parsed = _require_job_count(int(value))
    if value != str(parsed):
        raise PTGWaveContractError("job_count annotation must be canonical decimal")
    return parsed


def _annotation(annotations: Mapping[str, Any], name: str) -> str:
    return _require_text(name, annotations.get(name))


def _environment_by_name(value: Any) -> dict[str, Mapping[str, Any]]:
    if not isinstance(value, list):
        raise PTGWaveContractError("worker env must be a list")
    environment_by_name: dict[str, Mapping[str, Any]] = {}
    for item in value:
        entry = _mapping(item, "worker env entry")
        name = _require_text("worker env name", entry.get("name"))
        if name in environment_by_name:
            raise PTGWaveContractError(f"duplicate worker env {name}")
        environment_by_name[name] = entry
    return environment_by_name


def _env_value(env: Mapping[str, Mapping[str, Any]], name: str) -> str:
    entry = env.get(name)
    if entry is None:
        raise PTGWaveContractError(f"missing worker env {name}")
    return _require_text(name, entry.get("value"))


def _env_field_path(env: Mapping[str, Mapping[str, Any]], name: str) -> str:
    entry = env.get(name)
    if entry is None:
        raise PTGWaveContractError(f"missing worker env {name}")
    value_from = _mapping(entry.get("valueFrom"), f"{name}.valueFrom")
    field_ref = _mapping(value_from.get("fieldRef"), f"{name}.fieldRef")
    return _require_text(f"{name}.fieldPath", field_ref.get("fieldPath"))


def _require_env_value(env: Mapping[str, Mapping[str, Any]], name: str, expected: str) -> None:
    if _env_value(env, name) != expected:
        raise PTGWaveContractError(f"worker env {name} does not match the wave contract")


def _require_text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise PTGWaveContractError(f"{name} must be a non-empty trimmed string")
    return value


def _require_image(value: Any) -> str:
    image = _require_text("worker image", value)
    if not _IMAGE_RE.fullmatch(image):
        raise PTGWaveContractError("worker image must be pinned by a sha256 digest")
    return image


def _require_runtime_image_identity(value: Any) -> str:
    runtime_image_identity = _require_text("runtime_image_identity", value)
    if not _RUNTIME_IMAGE_RE.fullmatch(runtime_image_identity):
        raise PTGWaveContractError(
            "runtime_image_identity must be a canonical sha256 digest"
        )
    return runtime_image_identity


def _require_digest(name: str, value: Any) -> None:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise PTGWaveContractError(f"{name} must be a lowercase 64-hex digest")


def _require_factory(value: Any) -> str:
    value = _require_text("barrier_factory", value)
    if not _FACTORY_RE.fullmatch(value):
        raise PTGWaveContractError("barrier_factory must be an importable dotted callable")
    return value
