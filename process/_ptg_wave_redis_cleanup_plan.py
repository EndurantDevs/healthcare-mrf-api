"""Exact Redis cleanup boundaries shared by terminal wave paths."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from arq.constants import (
    health_check_key_suffix,
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)

from process._ptg_wave_redis_manifest import validate_ptg_small_wave_manifest
from process._ptg_wave_redis_models import (
    PTGSmallWaveCleanupPlan,
    PTGSmallWaveManifest,
    canonical_json_bytes,
    sha256_hex,
)


def plan_ptg_small_wave_terminal_cleanup(
    manifest: PTGSmallWaveManifest,
) -> PTGSmallWaveCleanupPlan:
    """Build the exact Redis deletion boundary owned by one manifest."""

    validate_ptg_small_wave_manifest(manifest)
    return PTGSmallWaveCleanupPlan(
        wave_id=manifest.wave_id,
        manifest_digest=manifest.manifest_digest,
        queue_name=manifest.queue_name,
        ready_key=manifest.ready_key,
        release_key=manifest.release_key,
        health_check_key=manifest.queue_name + health_check_key_suffix,
        job_keys=tuple(job_key_prefix + job.job_id for job in manifest.jobs),
        result_keys=tuple(result_key_prefix + job.job_id for job in manifest.jobs),
        retry_keys=tuple(retry_key_prefix + job.job_id for job in manifest.jobs),
        in_progress_keys=tuple(
            in_progress_key_prefix + job.job_id for job in manifest.jobs
        ),
    )


def canonical_mapping_digest(mapping: Mapping[str, Any]) -> str:
    """Return the canonical SHA-256 digest for one evidence mapping."""

    return sha256_hex(canonical_json_bytes(mapping))
