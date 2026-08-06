# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable data shapes for exact PTG Redis waves."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from process._ptg_wave_redis_encoding import (
    PTG_SMALL_WAVE_FUNCTION,
    PTG_SMALL_WAVE_MAX_JOB_COUNT,
    PTG_SMALL_WAVE_PROTOCOL_IDENTITY,
    PTG_SMALL_WAVE_QUEUE_PREFIX,
    PTG_SMALL_WAVE_SERIALIZER_IDENTITY,
    PTG_SMALL_WAVE_SLOT_COUNT,
    PTG_SMALL_WAVE_SLOTS,
    PTG_SMALL_WAVE_WORKER_CLASS,
    WAVE_SCHEMA_VERSION,
    PTGSmallWaveAttestationError,
    PTGSmallWaveBarrierTimeout,
    PTGSmallWaveCleanupActiveError,
    PTGSmallWaveConflictError,
    PTGSmallWaveError,
    PTGSmallWaveValidationError,
    as_optional_bytes,
    as_text,
    canonical_json_bytes,
    decode_job_count,
    encode_job_count,
    require_digest,
    require_identity,
    require_job_count,
    require_job_id,
    require_pinned_image_identity,
    require_protocol_identity,
    require_runtime_image_identity,
    require_wave_id,
    runtime_identity_digest,
    sha256_hex,
    wave_queue_name,
    wave_ready_key,
    wave_release_channel,
    wave_release_key,
)
from process._ptg_wave_redis_unclaimed_models import (
    PTGSmallWaveUnclaimedFailureRedisAttestation,
    PTGSmallWaveUnclaimedFailureRedisCleanupReceipt,
    PTGSmallWaveUnclaimedFailureRedisPostCleanupAttestation,
    PTGWaveUnclaimedRedisAttestation,
    PTGWaveUnclaimedRedisCleanupAttestation,
    PTGWaveUnclaimedRedisCleanupReceipt,
)


@dataclass(frozen=True)
class PTGSmallWaveJob:
    """One immutable ARQ job, addressed by its wave-local ordinal."""

    ordinal: int
    job_id: str
    score_ms: int
    serialized_job: bytes
    serialized_job_digest: str

    def as_manifest_mapping(self) -> dict[str, Any]:
        """Return the canonical digest input for this ordered job."""

        return {
            "ordinal": self.ordinal,
            "job_id": self.job_id,
            "score_ms": self.score_ms,
            "serialized_job_digest": self.serialized_job_digest,
        }


@dataclass(frozen=True)
class PTGSmallWaveRuntimeIdentity:
    """Controller-verified identities known after Kubernetes rendering."""

    config_identity: str
    kubernetes_manifest_identity: str
    image_identity: str
    runtime_image_identity: str


@dataclass(frozen=True)
class PTGSmallWaveManifest:
    """The immutable ordered definition that owns one dedicated queue."""

    wave_id: str
    queue_name: str
    enqueue_time_ms: int
    protocol_identity: str
    serializer_identity: str
    jobs: tuple[PTGSmallWaveJob, ...]
    jobs_digest: str
    manifest_digest: str
    config_identity: str | None
    kubernetes_manifest_identity: str | None
    image_identity: str | None
    runtime_image_identity: str | None
    runtime_identity_digest: str | None

    @property
    def ready_key(self) -> str:
        """Return this wave's exact worker-registration hash key."""

        return wave_ready_key(self.wave_id)

    @property
    def release_key(self) -> str:
        """Return this wave's exact durable release key."""

        return wave_release_key(self.wave_id)

    @property
    def release_channel(self) -> str:
        """Return this wave's exact transient release channel."""

        return wave_release_channel(self.wave_id)

    @property
    def job_ids(self) -> tuple[str, ...]:
        """Return the durable controller job IDs in manifest order."""

        return tuple(job.job_id for job in self.jobs)

    @property
    def reference(self) -> "PTGSmallWaveReference":
        """Return fixed-size worker state after controller identity binding."""

        (
            config_identity,
            manifest_identity,
            image_identity,
            runtime_image,
            identity_digest,
        ) = _bound_runtime_identities(self)
        return PTGSmallWaveReference(
            wave_id=self.wave_id,
            queue_name=self.queue_name,
            manifest_digest=self.manifest_digest,
            jobs_digest=self.jobs_digest,
            job_count=len(self.jobs),
            protocol_identity=self.protocol_identity,
            serializer_identity=self.serializer_identity,
            config_identity=config_identity,
            kubernetes_manifest_identity=manifest_identity,
            image_identity=image_identity,
            runtime_image_identity=runtime_image,
            runtime_identity_digest=identity_digest,
        )


@dataclass(frozen=True)
class PTGSmallWaveReference:
    """O(1) worker state with controller-bound runtime identities."""

    wave_id: str
    queue_name: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    protocol_identity: str
    serializer_identity: str
    config_identity: str
    kubernetes_manifest_identity: str
    image_identity: str
    runtime_image_identity: str
    runtime_identity_digest: str

    @property
    def ready_key(self) -> str:
        """Return this wave's exact worker-registration hash key."""

        return wave_ready_key(self.wave_id)

    @property
    def release_key(self) -> str:
        """Return this wave's exact durable release key."""

        return wave_release_key(self.wave_id)

    @property
    def release_channel(self) -> str:
        """Return this wave's exact transient release channel."""

        return wave_release_channel(self.wave_id)


@dataclass(frozen=True)
class PTGSmallWaveSlotIdentity:
    """One explicit worker identity that is allowed to enter the barrier."""

    slot: int
    pod_uid: str
    config_identity: str
    kubernetes_manifest_identity: str
    image_identity: str
    runtime_image_identity: str
    runtime_identity_digest: str
    wave_id: str
    manifest_digest: str
    queue_name: str
    jobs_digest: str
    job_count: int
    protocol_identity: str
    serializer_identity: str

    def as_mapping(self) -> dict[str, Any]:
        """Return the canonical fixed-size slot identity mapping."""

        return {
            "config_identity": self.config_identity,
            "kubernetes_manifest_identity": self.kubernetes_manifest_identity,
            "image_identity": self.image_identity,
            "runtime_image_identity": self.runtime_image_identity,
            "runtime_identity_digest": self.runtime_identity_digest,
            "manifest_digest": self.manifest_digest,
            "pod_uid": self.pod_uid,
            "queue_name": self.queue_name,
            "slot": self.slot,
            "wave_id": self.wave_id,
            "jobs_digest": self.jobs_digest,
            "job_count": encode_job_count(self.job_count),
            "protocol_identity": self.protocol_identity,
            "serializer_identity": self.serializer_identity,
        }


@dataclass(frozen=True)
class PTGSmallWaveReceipt:
    """The exact release record, including controller-bound identities."""

    wave_id: str
    queue_name: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    protocol_identity: str
    serializer_identity: str
    config_identity: str
    kubernetes_manifest_identity: str
    image_identity: str
    runtime_image_identity: str
    runtime_identity_digest: str
    ready_slots: tuple[PTGSmallWaveSlotIdentity, ...]
    ready_slots_digest: str
    release_digest: str
    release_payload: bytes

    def as_mapping(self) -> dict[str, Any]:
        """Return the canonical release mapping without its derived digest."""

        return {
            "schema_version": WAVE_SCHEMA_VERSION,
            "wave_id": self.wave_id,
            "queue_name": self.queue_name,
            "manifest_digest": self.manifest_digest,
            "jobs_digest": self.jobs_digest,
            "job_count": encode_job_count(self.job_count),
            "protocol_identity": self.protocol_identity,
            "serializer_identity": self.serializer_identity,
            "config_identity": self.config_identity,
            "kubernetes_manifest_identity": self.kubernetes_manifest_identity,
            "image_identity": self.image_identity,
            "runtime_image_identity": self.runtime_image_identity,
            "runtime_identity_digest": self.runtime_identity_digest,
            "ready_slots": [slot.as_mapping() for slot in self.ready_slots],
            "ready_slots_digest": self.ready_slots_digest,
        }


@dataclass(frozen=True)
class PTGSmallWaveBarrierReceipt:
    """Fixed-size worker result returned after release validation."""

    wave_id: str
    queue_name: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    protocol_identity: str
    serializer_identity: str
    config_identity: str
    kubernetes_manifest_identity: str
    image_identity: str
    runtime_image_identity: str
    runtime_identity_digest: str
    ready_slots_digest: str
    release_digest: str


@dataclass(frozen=True)
class PTGSmallWaveReadiness:
    """Read-only fixed-size observation of the twelve-slot barrier."""

    reference: PTGSmallWaveReference
    registered_slots: tuple[PTGSmallWaveSlotIdentity, ...]
    missing_slots: tuple[int, ...]
    config_identity: str | None
    kubernetes_manifest_identity: str | None
    image_identity: str | None
    runtime_image_identity: str | None
    ready: bool
    released: bool
    release_digest: str | None


@dataclass(frozen=True)
class PTGSmallWaveCleanupPlan:
    """Exact controller-side Redis keys owned by one supplied manifest."""

    wave_id: str
    manifest_digest: str
    queue_name: str
    ready_key: str
    release_key: str
    health_check_key: str
    job_keys: tuple[str, ...]
    result_keys: tuple[str, ...]
    retry_keys: tuple[str, ...]
    in_progress_keys: tuple[str, ...]

    @property
    def target_keys(self) -> tuple[str, ...]:
        """Return only the Redis keys owned by this exact manifest."""

        return (
            self.queue_name,
            self.ready_key,
            self.release_key,
            self.health_check_key,
            *self.job_keys,
            *self.result_keys,
            *self.retry_keys,
            *self.in_progress_keys,
        )


@dataclass(frozen=True)
class PTGSmallWaveCleanupReceipt:
    """Result of one exact terminal cleanup transaction."""

    wave_id: str
    manifest_digest: str
    target_key_count: int
    deleted_key_count: int
    pre_cleanup_attestation_digest: str
    pre_cleanup_attestation: "PTGSmallWavePreCleanupAttestation"

    def as_mapping(self) -> dict[str, Any]:
        """Return the exact cleanup transaction and its bound pre-clean witness."""

        return {
            "schema_version": WAVE_SCHEMA_VERSION,
            "wave_id": self.wave_id,
            "manifest_digest": self.manifest_digest,
            "target_key_count": self.target_key_count,
            "deleted_key_count": self.deleted_key_count,
            "pre_cleanup_attestation_digest": self.pre_cleanup_attestation_digest,
            "pre_cleanup": self.pre_cleanup_attestation.as_mapping(),
        }


@dataclass(frozen=True)
class PTGSmallWavePreCleanupAttestation:
    """Read-only proof that only terminal, cleanup-safe Redis keys remain."""

    wave_id: str
    queue_name: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    image_identity: str
    release_digest: str
    target_key_count: int
    queue_entry_count: int
    job_payload_count: int
    result_count: int
    retry_count: int
    in_progress_count: int
    health_check_count: int
    result_presence_digest: str
    attestation_digest: str

    def evidence_mapping(self) -> dict[str, Any]:
        """Return the canonical digest input without its derived digest."""

        return {
            "schema_version": WAVE_SCHEMA_VERSION,
            "wave_id": self.wave_id,
            "queue_name": self.queue_name,
            "manifest_digest": self.manifest_digest,
            "jobs_digest": self.jobs_digest,
            "job_count": self.job_count,
            "image_identity": self.image_identity,
            "release_digest": self.release_digest,
            "target_key_count": self.target_key_count,
            "queue_entry_count": self.queue_entry_count,
            "job_payload_count": self.job_payload_count,
            "result_count": self.result_count,
            "retry_count": self.retry_count,
            "in_progress_count": self.in_progress_count,
            "health_check_count": self.health_check_count,
            "result_presence_digest": self.result_presence_digest,
        }

    def as_mapping(self) -> dict[str, Any]:
        """Return canonical evidence plus its separately derived digest."""

        return {
            **self.evidence_mapping(),
            "attestation_digest": self.attestation_digest,
        }


@dataclass(frozen=True)
class PTGSmallWavePostCleanupAttestation:
    """GET-only proof that every exact cleanup target is absent."""

    wave_id: str
    manifest_digest: str
    target_key_count: int
    absent_target_count: int
    attestation_digest: str

    def evidence_mapping(self) -> dict[str, Any]:
        """Return the canonical digest input without its derived digest."""

        return {
            "schema_version": WAVE_SCHEMA_VERSION,
            "wave_id": self.wave_id,
            "manifest_digest": self.manifest_digest,
            "target_key_count": self.target_key_count,
            "absent_target_count": self.absent_target_count,
        }

    def as_mapping(self) -> dict[str, Any]:
        """Return canonical evidence plus its separately derived digest."""

        return {
            **self.evidence_mapping(),
            "attestation_digest": self.attestation_digest,
        }


def _bound_runtime_identities(
    manifest: PTGSmallWaveManifest,
) -> tuple[str, str, str, str, str]:
    runtime_fields = (
        manifest.config_identity,
        manifest.kubernetes_manifest_identity,
        manifest.image_identity,
        manifest.runtime_image_identity,
        manifest.runtime_identity_digest,
    )
    if not all(isinstance(field, str) for field in runtime_fields):
        raise PTGSmallWaveValidationError(
            "wave manifest runtime identities must be bound before worker use"
        )
    return cast(tuple[str, str, str, str, str], runtime_fields)
