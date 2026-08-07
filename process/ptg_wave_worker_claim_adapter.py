"""Pure exact-wave worker claim identity construction."""

from __future__ import annotations

import os
from typing import Any

from process.ptg_wave_barrier import PTGWaveWorkerIdentity


def exact_wave_claim_values(
    ctx: Any,
    params_by_name: dict[str, Any],
    *,
    run_id: str,
    claim_attempt_token: str,
) -> dict[str, Any] | None:
    """Build one exact worker identity without making a durable change."""

    wave_claim_field_map = {
        "wave_id": params_by_name.get("_wave_id"),
        "wave_digest": params_by_name.get("_wave_digest"),
        "job_id": params_by_name.get("_wave_job_id"),
    }
    has_wave_payload = any(
        environment_value is not None
        for environment_value in wave_claim_field_map.values()
    )
    has_wave_environment = bool(
        os.getenv("HLTHPRT_PTG_WAVE_DIGEST", "").strip()
    )
    if not has_wave_payload and not has_wave_environment:
        return None
    if not all(
        isinstance(environment_value, str)
        and environment_value
        and environment_value == environment_value.strip()
        for environment_value in wave_claim_field_map.values()
    ):
        raise RuntimeError("PTG exact-wave payload identity is incomplete")
    if not run_id:
        raise RuntimeError("PTG exact-wave task requires a run_id")
    context_job_id = _context_job_id(ctx)
    if context_job_id != wave_claim_field_map["job_id"]:
        raise RuntimeError("PTG exact-wave ARQ job identity does not match")
    identity = PTGWaveWorkerIdentity.from_environment()
    if identity.wave_digest != wave_claim_field_map["wave_digest"]:
        raise RuntimeError("PTG exact-wave execution digest does not match")
    expected_queue = str(params_by_name.get("_expected_queue") or "").strip()
    expected_worker_class = str(
        params_by_name.get("_expected_worker_class") or ""
    ).strip()
    if identity.queue != expected_queue or identity.worker_class != expected_worker_class:
        raise RuntimeError("PTG exact-wave worker lane does not match")
    return {
        "wave_id": wave_claim_field_map["wave_id"],
        "run_id": run_id,
        "job_id": wave_claim_field_map["job_id"],
        "slot": identity.slot_index,
        "pod_uid": identity.pod_uid,
        "pinned_image_reference": identity.image_identity,
        "pinned_image_digest": identity.image_identity.rsplit("@sha256:", 1)[1],
        "runtime_image_identity": identity.runtime_image_identity,
        "config_identity": identity.config_identity,
        "manifest_identity": identity.manifest_identity,
        "claim_attempt_token": claim_attempt_token,
    }


def _context_job_id(ctx: Any) -> str:
    if not isinstance(ctx, dict):
        return ""
    return str(ctx.get("job_id") or "").strip()
