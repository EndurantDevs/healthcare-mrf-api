"""Immutable Redis evidence for exact waves with zero durable claims."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from process._ptg_wave_redis_models import (
        PTGSmallWaveReceipt,
        PTGSmallWaveSlotIdentity,
    )


@dataclass(frozen=True)
class PTGWaveUnclaimedRedisAttestation:
    """Immutable Redis evidence for an exact wave with zero DB claims."""

    wave_id: str
    queue_name: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    target_key_count: int
    ready_slots: tuple[PTGSmallWaveSlotIdentity, ...]
    ready_slots_digest: str
    release_present: bool
    release_digest: str | None
    release_receipt: PTGSmallWaveReceipt | None
    queued_ordinals: tuple[int, ...]
    job_ordinals: tuple[int, ...]
    result_ordinals: tuple[int, ...]
    retry_ordinals: tuple[int, ...]
    in_progress_ordinals: tuple[int, ...]
    health_check_present: bool
    attestation_digest: str

    def evidence_mapping(self) -> dict[str, Any]:
        """Return all canonical state used to derive the attestation digest."""

        return {
            "schema_version": "healthporta.ptg-wave.redis-unclaimed-failure.v1",
            "wave_id": self.wave_id,
            "queue_name": self.queue_name,
            "manifest_digest": self.manifest_digest,
            "jobs_digest": self.jobs_digest,
            "job_count": self.job_count,
            "target_key_count": self.target_key_count,
            "ready_slots": [slot.as_mapping() for slot in self.ready_slots],
            "ready_slots_digest": self.ready_slots_digest,
            "release_present": self.release_present,
            "release_digest": self.release_digest,
            "release_receipt": (
                self.release_receipt.as_mapping()
                if self.release_receipt is not None
                else None
            ),
            "queued_ordinals": list(self.queued_ordinals),
            "job_ordinals": list(self.job_ordinals),
            "result_ordinals": list(self.result_ordinals),
            "retry_ordinals": list(self.retry_ordinals),
            "in_progress_ordinals": list(self.in_progress_ordinals),
            "health_check_present": self.health_check_present,
        }

    def as_mapping(self) -> dict[str, Any]:
        """Return canonical evidence and its separately derived digest."""

        return {
            **self.evidence_mapping(),
            "attestation_digest": self.attestation_digest,
        }


@dataclass(frozen=True)
class PTGWaveUnclaimedRedisCleanupReceipt:
    """One one-shot all-unclaimed cleanup bound to prior full evidence."""

    wave_id: str
    manifest_digest: str
    target_key_count: int
    deleted_key_count: int
    expected_attestation_digest: str
    attestation: PTGWaveUnclaimedRedisAttestation

    def as_mapping(self) -> dict[str, Any]:
        """Return the complete cleanup receipt suitable for durable storage."""

        return {
            "schema_version": "healthporta.ptg-wave.redis-unclaimed-cleanup.v1",
            "wave_id": self.wave_id,
            "manifest_digest": self.manifest_digest,
            "target_key_count": self.target_key_count,
            "deleted_key_count": self.deleted_key_count,
            "expected_attestation_digest": self.expected_attestation_digest,
            "attestation": self.attestation.as_mapping(),
        }


@dataclass(frozen=True)
class PTGWaveUnclaimedRedisCleanupAttestation:
    """GET-only absence proof tied to its all-unclaimed pre-clean witness."""

    wave_id: str
    manifest_digest: str
    target_key_count: int
    absent_target_count: int
    expected_attestation_digest: str
    attestation_digest: str

    def evidence_mapping(self) -> dict[str, Any]:
        """Return all canonical state used to derive the attestation digest."""

        return {
            "schema_version": "healthporta.ptg-wave.redis-unclaimed-post-cleanup.v1",
            "wave_id": self.wave_id,
            "manifest_digest": self.manifest_digest,
            "target_key_count": self.target_key_count,
            "absent_target_count": self.absent_target_count,
            "expected_attestation_digest": self.expected_attestation_digest,
        }

    def as_mapping(self) -> dict[str, Any]:
        """Return canonical evidence plus its separately derived digest."""

        return {
            **self.evidence_mapping(),
            "attestation_digest": self.attestation_digest,
        }


PTGSmallWaveUnclaimedFailureRedisAttestation = PTGWaveUnclaimedRedisAttestation
PTGSmallWaveUnclaimedFailureRedisCleanupReceipt = PTGWaveUnclaimedRedisCleanupReceipt
PTGSmallWaveUnclaimedFailureRedisPostCleanupAttestation = (
    PTGWaveUnclaimedRedisCleanupAttestation
)
