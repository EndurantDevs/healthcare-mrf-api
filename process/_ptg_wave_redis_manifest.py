# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable-input normalization and digest construction for exact PTG waves."""

from __future__ import annotations

import copy
import math
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime, time as datetime_time
from pathlib import Path
from typing import Any

from arq.jobs import deserialize_job as arq_deserialize_job
from arq.jobs import serialize_job as arq_serialize_job

from process._ptg_wave_redis_models import (
    PTG_SMALL_WAVE_FUNCTION,
    PTG_SMALL_WAVE_MAX_JOB_COUNT,
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    PTG_SMALL_WAVE_SERIALIZER_IDENTITY,
    WAVE_SCHEMA_VERSION,
    PTGSmallWaveAttestationError,
    PTGSmallWaveJob,
    PTGSmallWaveManifest,
    PTGSmallWaveRuntimeIdentity,
    PTGSmallWaveValidationError,
    canonical_json_bytes,
    require_job_id,
    require_digest,
    require_pinned_image_identity,
    require_protocol_identity,
    require_runtime_image_identity,
    require_wave_id,
    runtime_identity_digest,
    sha256_hex,
    wave_queue_name,
)
from process.serialization import deserialize_job, serialize_job


def build_ptg_small_wave_manifest(
    task_payloads: Sequence[Mapping[str, Any]],
    *,
    execution_digest: str,
    job_ids: Sequence[str],
    enqueue_time_ms: int,
    runtime_identity: PTGSmallWaveRuntimeIdentity | None = None,
    protocol_identity: str = PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    serializer_identity: str = PTG_SMALL_WAVE_SERIALIZER_IDENTITY,
) -> PTGSmallWaveManifest:
    """Build one immutable manifest from explicit durable DB identities."""

    wave_id, ordered_job_ids, protocol_id, serializer_id = _normalize_build_inputs(
        task_payloads,
        execution_digest,
        job_ids,
        enqueue_time_ms,
        protocol_identity,
        serializer_identity,
    )
    jobs = _serialize_manifest_jobs(
        task_payloads,
        ordered_job_ids,
        enqueue_time_ms,
    )
    queue_name = wave_queue_name(wave_id)
    jobs_digest = _calculate_jobs_digest(jobs, protocol_id, serializer_id)
    manifest_digest = _calculate_manifest_digest(
        wave_id=wave_id,
        queue_name=queue_name,
        enqueue_time_ms=enqueue_time_ms,
        job_count=len(jobs),
        jobs_digest=jobs_digest,
        protocol_identity=protocol_id,
        serializer_identity=serializer_id,
    )
    runtime_fields = _runtime_identity_fields(runtime_identity)
    return PTGSmallWaveManifest(
        wave_id=wave_id,
        queue_name=queue_name,
        enqueue_time_ms=enqueue_time_ms,
        protocol_identity=protocol_id,
        serializer_identity=serializer_id,
        jobs=jobs,
        jobs_digest=jobs_digest,
        manifest_digest=manifest_digest,
        config_identity=runtime_fields[0],
        kubernetes_manifest_identity=runtime_fields[1],
        image_identity=runtime_fields[2],
        runtime_image_identity=runtime_fields[3],
        runtime_identity_digest=runtime_fields[4],
    )


def bind_ptg_small_wave_runtime_identity(
    manifest: PTGSmallWaveManifest,
    runtime_identity: PTGSmallWaveRuntimeIdentity,
) -> PTGSmallWaveManifest:
    """Bind post-render identities without changing jobs or manifest digest."""

    validate_ptg_small_wave_manifest(manifest, require_runtime_identity=False)
    runtime_fields = _runtime_identity_fields(runtime_identity)
    if manifest.runtime_identity_digest is not None:
        if (
            manifest.config_identity,
            manifest.kubernetes_manifest_identity,
            manifest.image_identity,
            manifest.runtime_image_identity,
            manifest.runtime_identity_digest,
        ) == runtime_fields:
            return manifest
        raise PTGSmallWaveValidationError(
            "wave manifest runtime identities are already bound"
        )
    bound_manifest = replace(
        manifest,
        config_identity=runtime_fields[0],
        kubernetes_manifest_identity=runtime_fields[1],
        image_identity=runtime_fields[2],
        runtime_image_identity=runtime_fields[3],
        runtime_identity_digest=runtime_fields[4],
    )
    validate_ptg_small_wave_manifest(bound_manifest)
    return bound_manifest


def _runtime_identity_fields(
    runtime_identity: PTGSmallWaveRuntimeIdentity | None,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    if runtime_identity is None:
        return None, None, None, None, None
    if not isinstance(runtime_identity, PTGSmallWaveRuntimeIdentity):
        raise PTGSmallWaveValidationError(
            "runtime_identity must be a PTGSmallWaveRuntimeIdentity"
        )
    config_identity = require_digest(
        "config_identity",
        runtime_identity.config_identity,
    )
    manifest_identity = require_digest(
        "kubernetes_manifest_identity",
        runtime_identity.kubernetes_manifest_identity,
    )
    image_identity = require_pinned_image_identity(runtime_identity.image_identity)
    runtime_image = require_runtime_image_identity(
        runtime_identity.runtime_image_identity
    )
    return (
        config_identity,
        manifest_identity,
        image_identity,
        runtime_image,
        runtime_identity_digest(
            config_identity,
            manifest_identity,
            image_identity,
            runtime_image,
        ),
    )


def _normalize_build_inputs(
    task_payloads: Sequence[Mapping[str, Any]],
    execution_digest: str,
    job_ids: Sequence[str],
    enqueue_time_ms: int,
    protocol_identity: str,
    serializer_identity: str,
) -> tuple[str, tuple[str, ...], str, str]:
    if not isinstance(enqueue_time_ms, int) or isinstance(enqueue_time_ms, bool):
        raise PTGSmallWaveValidationError("enqueue_time_ms must be an integer")
    if enqueue_time_ms < 0:
        raise PTGSmallWaveValidationError("enqueue_time_ms must not be negative")
    if not 1 <= len(task_payloads) <= PTG_SMALL_WAVE_MAX_JOB_COUNT:
        raise PTGSmallWaveValidationError(
            f"task payload count must be from 1 through {PTG_SMALL_WAVE_MAX_JOB_COUNT}"
        )
    if len(job_ids) != len(task_payloads):
        raise PTGSmallWaveValidationError(
            "job_ids must exactly match task payload count"
        )
    ordered_job_ids = tuple(require_job_id(job_id) for job_id in job_ids)
    if len(set(ordered_job_ids)) != len(ordered_job_ids):
        raise PTGSmallWaveValidationError("job_ids must be unique")
    return (
        require_wave_id(execution_digest),
        ordered_job_ids,
        require_protocol_identity("protocol_identity", protocol_identity),
        require_protocol_identity("serializer_identity", serializer_identity),
    )


def _serialize_manifest_jobs(
    task_payloads: Sequence[Mapping[str, Any]],
    ordered_job_ids: tuple[str, ...],
    enqueue_time_ms: int,
) -> tuple[PTGSmallWaveJob, ...]:
    jobs: list[PTGSmallWaveJob] = []
    for ordinal, control_payload in enumerate(task_payloads):
        normalized_payload = _canonicalize_control_payload(
            control_payload,
            ordinal=ordinal,
        )
        serialized_job = arq_serialize_job(
            PTG_SMALL_WAVE_FUNCTION,
            (normalized_payload,),
            {},
            None,
            enqueue_time_ms,
            serializer=serialize_job,
        )
        jobs.append(
            PTGSmallWaveJob(
                ordinal=ordinal,
                job_id=ordered_job_ids[ordinal],
                score_ms=enqueue_time_ms,
                serialized_job=serialized_job,
                serialized_job_digest=sha256_hex(serialized_job),
            )
        )
    return tuple(jobs)


def _calculate_jobs_digest(
    jobs: tuple[PTGSmallWaveJob, ...],
    protocol_identity: str,
    serializer_identity: str,
) -> str:
    digest_bytes = canonical_json_bytes(
        {
            "schema_version": WAVE_SCHEMA_VERSION,
            "protocol_identity": protocol_identity,
            "serializer_identity": serializer_identity,
            "jobs": [job.as_manifest_mapping() for job in jobs],
        }
    )
    return sha256_hex(digest_bytes)


def _calculate_manifest_digest(
    *,
    wave_id: str,
    queue_name: str,
    enqueue_time_ms: int,
    job_count: int,
    jobs_digest: str,
    protocol_identity: str,
    serializer_identity: str,
) -> str:
    digest_bytes = canonical_json_bytes(
        {
            "schema_version": WAVE_SCHEMA_VERSION,
            "wave_id": wave_id,
            "queue_name": queue_name,
            "enqueue_time_ms": enqueue_time_ms,
            "job_count": job_count,
            "jobs_digest": jobs_digest,
            "protocol_identity": protocol_identity,
            "serializer_identity": serializer_identity,
        }
    )
    return sha256_hex(digest_bytes)


def _canonicalize_control_payload(
    control_payload: Mapping[str, Any],
    *,
    ordinal: int,
) -> dict[str, Any]:
    if not isinstance(control_payload, Mapping):
        raise PTGSmallWaveValidationError(
            f"job ordinal {ordinal} payload must be a mapping"
        )
    return _canonicalize_value(
        control_payload,
        location=f"job ordinal {ordinal} payload",
    )


def _canonicalize_value(candidate: Any, *, location: str) -> Any:
    if isinstance(candidate, Mapping):
        if not all(isinstance(key, str) for key in candidate):
            raise PTGSmallWaveValidationError(
                f"{location} mapping keys must be strings"
            )
        return {
            key: _canonicalize_value(candidate[key], location=f"{location}.{key}")
            for key in sorted(candidate)
        }
    if isinstance(candidate, list):
        return [
            _canonicalize_value(member, location=f"{location}[{index}]")
            for index, member in enumerate(candidate)
        ]
    if isinstance(candidate, tuple):
        return tuple(
            _canonicalize_value(member, location=f"{location}[{index}]")
            for index, member in enumerate(candidate)
        )
    if isinstance(candidate, (set, frozenset)):
        raise PTGSmallWaveValidationError(
            f"{location} must not contain an unordered set"
        )
    if isinstance(candidate, float) and not math.isfinite(candidate):
        raise PTGSmallWaveValidationError(
            f"{location} must not contain a non-finite float"
        )
    supported_scalars = (
        str,
        int,
        float,
        bool,
        bytes,
        type(None),
        datetime,
        date,
        datetime_time,
        Path,
    )
    if isinstance(candidate, supported_scalars):
        return copy.deepcopy(candidate)
    raise PTGSmallWaveValidationError(
        f"{location} contains unsupported value type {type(candidate).__name__}"
    )


def validate_ptg_small_wave_manifest(
    manifest: PTGSmallWaveManifest,
    *,
    require_runtime_identity: bool = True,
) -> None:
    """Fail closed unless every immutable manifest field is internally bound."""

    if not isinstance(manifest, PTGSmallWaveManifest):
        raise PTGSmallWaveValidationError(
            "manifest must be a PTGSmallWaveManifest"
        )
    _validate_manifest_identity(manifest)
    _validate_manifest_runtime_identity(manifest, require_runtime_identity)
    _validate_manifest_jobs(manifest)
    expected_jobs_digest = _calculate_jobs_digest(
        manifest.jobs,
        manifest.protocol_identity,
        manifest.serializer_identity,
    )
    if manifest.jobs_digest != expected_jobs_digest:
        raise PTGSmallWaveValidationError(
            "manifest jobs digest does not bind its ordered jobs"
        )
    expected_manifest_digest = _calculate_manifest_digest(
        wave_id=manifest.wave_id,
        queue_name=manifest.queue_name,
        enqueue_time_ms=manifest.enqueue_time_ms,
        job_count=len(manifest.jobs),
        jobs_digest=manifest.jobs_digest,
        protocol_identity=manifest.protocol_identity,
        serializer_identity=manifest.serializer_identity,
    )
    if manifest.manifest_digest != expected_manifest_digest:
        raise PTGSmallWaveValidationError(
            "manifest digest does not bind the complete wave"
        )


def _validate_manifest_runtime_identity(
    manifest: PTGSmallWaveManifest,
    is_required: bool,
) -> None:
    runtime_fields = (
        manifest.config_identity,
        manifest.kubernetes_manifest_identity,
        manifest.image_identity,
        manifest.runtime_image_identity,
        manifest.runtime_identity_digest,
    )
    if runtime_fields == (None, None, None, None, None):
        if is_required:
            raise PTGSmallWaveValidationError(
                "wave manifest runtime identities are not bound"
            )
        return
    if not all(isinstance(field, str) for field in runtime_fields):
        raise PTGSmallWaveValidationError(
            "wave manifest runtime identities are only partially bound"
        )
    expected_digest = runtime_identity_digest(
        require_digest("config_identity", manifest.config_identity),
        require_digest(
            "kubernetes_manifest_identity",
            manifest.kubernetes_manifest_identity,
        ),
        require_pinned_image_identity(manifest.image_identity),
        require_runtime_image_identity(manifest.runtime_image_identity),
    )
    if manifest.runtime_identity_digest != expected_digest:
        raise PTGSmallWaveValidationError(
            "runtime identity digest does not bind controller identities"
        )


def _validate_manifest_identity(manifest: PTGSmallWaveManifest) -> None:
    require_wave_id(manifest.wave_id)
    if manifest.queue_name != wave_queue_name(manifest.wave_id):
        raise PTGSmallWaveValidationError(
            "manifest queue name does not match its wave ID"
        )
    require_protocol_identity("protocol_identity", manifest.protocol_identity)
    require_protocol_identity("serializer_identity", manifest.serializer_identity)
    if (
        not isinstance(manifest.enqueue_time_ms, int)
        or isinstance(manifest.enqueue_time_ms, bool)
        or manifest.enqueue_time_ms < 0
    ):
        raise PTGSmallWaveValidationError("manifest enqueue_time_ms must be non-negative")


def _validate_manifest_jobs(manifest: PTGSmallWaveManifest) -> None:
    if not isinstance(manifest.jobs, tuple):
        raise PTGSmallWaveValidationError("manifest jobs must be an immutable tuple")
    if not 1 <= len(manifest.jobs) <= PTG_SMALL_WAVE_MAX_JOB_COUNT:
        raise PTGSmallWaveValidationError(
            "manifest job count must be from 1 through 4096"
        )
    if any(not isinstance(job, PTGSmallWaveJob) for job in manifest.jobs):
        raise PTGSmallWaveValidationError("manifest jobs must be PTGSmallWaveJob records")
    if any(
        not isinstance(job.ordinal, int)
        or isinstance(job.ordinal, bool)
        or job.ordinal != expected_ordinal
        for expected_ordinal, job in enumerate(manifest.jobs)
    ):
        raise PTGSmallWaveValidationError("manifest job ordinals must be ordered from zero")
    ordered_job_ids = tuple(require_job_id(job.job_id) for job in manifest.jobs)
    if len(set(ordered_job_ids)) != len(ordered_job_ids):
        raise PTGSmallWaveValidationError("manifest job IDs must be unique")
    if any(
        not isinstance(job.score_ms, int)
        or isinstance(job.score_ms, bool)
        or job.score_ms != manifest.enqueue_time_ms
        for job in manifest.jobs
    ):
        raise PTGSmallWaveValidationError("manifest job scores must equal enqueue_time_ms")
    for job in manifest.jobs:
        if not isinstance(job.serialized_job, bytes):
            raise PTGSmallWaveValidationError("manifest serialized jobs must retain exact bytes")
        require_digest("serialized_job_digest", job.serialized_job_digest)
        if job.serialized_job_digest != sha256_hex(job.serialized_job):
            raise PTGSmallWaveValidationError(
                "manifest serialized-job digest does not match its bytes"
            )
        _assert_arq_job_definition(job, job.serialized_job)


def _assert_arq_job_definition(
    job: PTGSmallWaveJob,
    serialized_job: bytes,
) -> None:
    try:
        actual_job = arq_deserialize_job(
            serialized_job,
            deserializer=deserialize_job,
        )
        expected_job = arq_deserialize_job(
            job.serialized_job,
            deserializer=deserialize_job,
        )
    except Exception as exc:
        raise PTGSmallWaveAttestationError(
            f"ARQ job definition cannot be deserialized for ordinal {job.ordinal}"
        ) from exc
    if (
        actual_job != expected_job
        or actual_job.function != PTG_SMALL_WAVE_FUNCTION
        or actual_job.kwargs != {}
        or actual_job.job_try is not None
        or len(actual_job.args) != 1
        or not isinstance(actual_job.args[0], Mapping)
        or int(actual_job.enqueue_time.timestamp() * 1000) != job.score_ms
    ):
        raise PTGSmallWaveAttestationError(
            f"ARQ job definition is invalid for ordinal {job.ordinal}"
        )


def attest_arq_job_bytes(job: PTGSmallWaveJob, serialized_job: bytes) -> None:
    """Verify stored bytes are the exact manifest-bound public ARQ job."""

    if serialized_job != job.serialized_job:
        raise PTGSmallWaveAttestationError(
            f"ARQ job payload is missing or tampered for ordinal {job.ordinal}"
        )
    _assert_arq_job_definition(job, serialized_job)
