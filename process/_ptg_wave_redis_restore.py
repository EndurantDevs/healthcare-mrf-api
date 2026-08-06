# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validation-only restoration of already-admitted PTG ARQ job bytes."""

from __future__ import annotations

from collections.abc import Sequence

from process._ptg_wave_redis_manifest import validate_ptg_small_wave_manifest
from process._ptg_wave_redis_models import (
    PTGSmallWaveJob,
    PTGSmallWaveManifest,
    PTGSmallWaveValidationError,
    require_digest,
    require_protocol_identity,
    require_wave_id,
    wave_queue_name,
)


def restore_ptg_small_wave_manifest(
    jobs: Sequence[PTGSmallWaveJob],
    *,
    execution_digest: str,
    jobs_digest: str,
    manifest_digest: str,
    protocol_identity: str,
    serializer_identity: str,
) -> PTGSmallWaveManifest:
    """Restore an unbound manifest without copying or reserializing job bytes."""

    if (
        not isinstance(jobs, Sequence)
        or isinstance(jobs, (str, bytes, bytearray))
        or not jobs
    ):
        raise PTGSmallWaveValidationError(
            "restored jobs must be a non-empty ordered sequence"
        )
    restored_jobs = tuple(jobs)
    if not isinstance(restored_jobs[0], PTGSmallWaveJob):
        raise PTGSmallWaveValidationError(
            "restored jobs must contain only PTGSmallWaveJob records"
        )
    wave_id = require_wave_id(execution_digest)
    restored_manifest = PTGSmallWaveManifest(
        wave_id=wave_id,
        queue_name=wave_queue_name(wave_id),
        enqueue_time_ms=restored_jobs[0].score_ms,
        protocol_identity=require_protocol_identity(
            "protocol_identity",
            protocol_identity,
        ),
        serializer_identity=require_protocol_identity(
            "serializer_identity",
            serializer_identity,
        ),
        jobs=restored_jobs,
        jobs_digest=require_digest("jobs_digest", jobs_digest),
        manifest_digest=require_digest("manifest_digest", manifest_digest),
        config_identity=None,
        kubernetes_manifest_identity=None,
        image_identity=None,
        runtime_image_identity=None,
        runtime_identity_digest=None,
    )
    validate_ptg_small_wave_manifest(
        restored_manifest,
        require_runtime_identity=False,
    )
    return restored_manifest
