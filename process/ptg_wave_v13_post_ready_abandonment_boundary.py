"""Durable predecessor-boundary checks for V13 abandonment evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from api.ptg_wave_kubernetes import PTG_WAVE_SLOT_COUNT
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_preclaim_supersession import (
    _require_persisted_manifest_integrity,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v13_post_ready_abandonment_contract import _JOB_RECEIPT_FIELDS


def _require_post_ready_boundary(wave: Any) -> None:
    """Require the durable, unreleased predecessor boundary before observation."""

    _require_unreleased_wave_boundary(wave)
    _require_persisted_manifest_integrity(wave, wave.kubernetes_manifest)
    _validate_stored_job_receipt(wave)
    _require_no_later_lifecycle_evidence(wave)


def _require_unreleased_wave_boundary(wave: Any) -> None:
    if (
        getattr(wave, "state", None) != "slots_waiting"
        or getattr(wave, "uncertainty_resume_state", None) is not None
        or getattr(wave, "worker_limit", None) != PTG_WAVE_SLOT_COUNT
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 predecessor is not the exact unreleased slots_waiting wave"
        )
    required_fields = (
        "k8s_post_ticket",
        "k8s_post_started_at",
        "kubernetes_job_uid",
        "kubernetes_job_receipt",
        "kubernetes_job_receipt_digest",
        "kubernetes_manifest",
        "kubernetes_manifest_bytes",
        "kubernetes_manifest_sha256",
        "kubernetes_manifest_identity",
        "kubernetes_config_identity",
        "pinned_image_reference",
        "pinned_image_digest",
        "runtime_image_identity",
    )
    if any(getattr(wave, name, None) is None for name in required_fields):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 predecessor lacks its durable Job creation receipt"
        )
    if (
        getattr(wave, "kubernetes_ready_attestation", None) is not None
        or getattr(wave, "kubernetes_ready_attestation_digest", None) is not None
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 predecessor must not invent a Kubernetes readiness receipt"
        )


def _validate_stored_job_receipt(wave: Any) -> None:
    receipt = getattr(wave, "kubernetes_job_receipt", None)
    expected_receipt_by_field = {
        "wave_digest": wave.wave_digest,
        "job_uid": wave.kubernetes_job_uid,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "config_identity": wave.kubernetes_config_identity,
        "pinned_image_reference": wave.pinned_image_reference,
        "pinned_image_digest": wave.pinned_image_digest,
        "runtime_image_identity": wave.runtime_image_identity,
    }
    if (
        not isinstance(receipt, Mapping)
        or set(receipt) != _JOB_RECEIPT_FIELDS
        or dict(receipt) != expected_receipt_by_field
        or wave.kubernetes_job_receipt_digest
        != sha256_digest(canonical_json(expected_receipt_by_field))
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 predecessor Job receipt is corrupt"
        )


def _require_no_later_lifecycle_evidence(wave: Any) -> None:
    absent_fields = (
        "redis_release_ticket",
        "redis_release_started_at",
        "redis_release_attestation",
        "redis_release_attestation_digest",
        "failure_receipt",
        "failure_receipt_digest",
        "outcomes_digest",
        "linkage_ack",
        "linkage_ack_digest",
        "linkage_receipt",
        "linkage_receipt_payload_digest",
        "linkage_receipt_issued_at",
        "terminal_evidence_digest",
        "terminal_summary",
        "redis_cleanup_ticket",
        "redis_cleanup_started_at",
        "redis_cleanup_evidence",
        "redis_cleanup_evidence_digest",
        "kubernetes_delete_ticket",
        "kubernetes_delete_started_at",
        "kubernetes_delete_evidence",
        "kubernetes_delete_evidence_digest",
        "cleanup_evidence_digest",
        "cleanup_summary",
        "resolved_at",
    )
    if any(getattr(wave, name, None) is not None for name in absent_fields):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V13 predecessor contains release or later lifecycle evidence"
        )
