# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Controller-bound fixed-size worker references and slot identities."""

from __future__ import annotations

from process._ptg_wave_redis_models import (
    PTG_SMALL_WAVE_SLOTS,
    PTGSmallWaveReference,
    PTGSmallWaveSlotIdentity,
    PTGSmallWaveValidationError,
    require_digest,
    require_identity,
    require_job_count,
    require_pinned_image_identity,
    require_protocol_identity,
    require_runtime_image_identity,
    require_wave_id,
    runtime_identity_digest,
    wave_queue_name,
)


def validate_ptg_small_wave_reference(reference: PTGSmallWaveReference) -> None:
    """Fail closed unless a worker reference has exact fixed-size fields."""

    if not isinstance(reference, PTGSmallWaveReference):
        raise PTGSmallWaveValidationError(
            "reference must be a PTGSmallWaveReference"
        )
    require_wave_id(reference.wave_id)
    if reference.queue_name != wave_queue_name(reference.wave_id):
        raise PTGSmallWaveValidationError(
            "reference queue name does not match its wave ID"
        )
    require_digest("manifest_digest", reference.manifest_digest)
    require_digest("jobs_digest", reference.jobs_digest)
    require_job_count(reference.job_count)
    require_protocol_identity("protocol_identity", reference.protocol_identity)
    require_protocol_identity("serializer_identity", reference.serializer_identity)
    require_digest("config_identity", reference.config_identity)
    require_digest(
        "kubernetes_manifest_identity",
        reference.kubernetes_manifest_identity,
    )
    require_pinned_image_identity(reference.image_identity)
    require_runtime_image_identity(reference.runtime_image_identity)
    expected_runtime_digest = runtime_identity_digest(
        reference.config_identity,
        reference.kubernetes_manifest_identity,
        reference.image_identity,
        reference.runtime_image_identity,
    )
    if reference.runtime_identity_digest != expected_runtime_digest:
        raise PTGSmallWaveValidationError(
            "reference runtime identity digest does not bind controller identities"
        )


def create_ptg_small_wave_slot_identity(
    reference: PTGSmallWaveReference,
    *,
    slot: int,
    pod_uid: str,
) -> PTGSmallWaveSlotIdentity:
    """Create one exact worker registration bound to a validated reference."""

    if (
        not isinstance(slot, int)
        or isinstance(slot, bool)
        or slot not in PTG_SMALL_WAVE_SLOTS
    ):
        raise PTGSmallWaveValidationError(
            "slot must be an integer from 0 through 11"
        )
    return PTGSmallWaveSlotIdentity(
        slot=slot,
        pod_uid=require_identity("pod_uid", pod_uid),
        config_identity=reference.config_identity,
        kubernetes_manifest_identity=reference.kubernetes_manifest_identity,
        image_identity=reference.image_identity,
        runtime_image_identity=reference.runtime_image_identity,
        runtime_identity_digest=reference.runtime_identity_digest,
        wave_id=reference.wave_id,
        manifest_digest=reference.manifest_digest,
        queue_name=reference.queue_name,
        jobs_digest=reference.jobs_digest,
        job_count=reference.job_count,
        protocol_identity=reference.protocol_identity,
        serializer_identity=reference.serializer_identity,
    )
