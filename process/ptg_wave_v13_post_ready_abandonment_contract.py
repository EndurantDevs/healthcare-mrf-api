"""Closed V13 proof schemas and canonical digest primitives."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_quarantine_basis import (
    V13_POST_READY_UNRELEASED_FAILURE_CUTOVER_BASIS,
)
from process.ptg_wave_state import canonical_json, sha256_digest


V13_ABANDONMENT_REQUEST_SCHEMA = (
    "healthporta.ptg-wave.v13-post-ready-unreleased-failure-"
    "abandonment-request.v1"
)
V13_ABANDONMENT_PROOF_SCHEMA = (
    "healthporta.ptg-wave.v13-post-ready-unreleased-failure-"
    "abandonment-proof.v1"
)
V13_QUARANTINE_REASON = V13_POST_READY_UNRELEASED_FAILURE_CUTOVER_BASIS
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z")

_PROOF_FIELDS = frozenset(
    {
        "schema_version",
        "recovery_basis",
        "operation_id",
        "cutover_id",
        "admission",
        "database",
        "kubernetes",
        "redis",
        "proof_digest",
    }
)
_DATABASE_FIELDS = frozenset(
    {
        "state",
        "intent_count",
        "run_count",
        "pristine_run_count",
        "unassigned_run_count",
        "claim_count",
        "outcome_count",
        "worker_start_event_count",
        "member_rows_digest",
        "intent_rows_digest",
        "run_rows_digest",
    }
)
_KUBERNETES_FIELDS = frozenset(
    {
        "job_receipt",
        "job_receipt_digest",
        "ready_attestation",
        "ready_attestation_digest",
        "failure",
    }
)
_JOB_RECEIPT_FIELDS = frozenset(
    {
        "wave_digest",
        "job_uid",
        "manifest_identity",
        "config_identity",
        "pinned_image_reference",
        "pinned_image_digest",
        "runtime_image_identity",
    }
)
_FAILURE_FIELDS = frozenset(
    {
        "schema_version",
        "wave_digest",
        "queue",
        "manifest_digest",
        "jobs_digest",
        "job_count",
        "config_identity",
        "manifest_identity",
        "image_identity",
        "runtime_image_identity",
        "job_name",
        "job_uid",
        "backoff_limit",
        "job_active",
        "job_failed",
        "job_succeeded",
        "job_ready",
        "job_terminating",
        "completed_indexes",
        "failed_indexes",
        "completion_time",
        "start_time",
        "uncounted_terminated_pods",
        "job_conditions",
        "retained_failed_slots",
        "attestation_digest",
    }
)
_RETAINED_SLOT_FIELDS = frozenset(
    {"slot", "pod_uid", "phase", "runtime_image_identity", "termination"}
)
_TERMINATION_FIELDS = frozenset(
    {"container_id", "reason", "exit_code", "started_at", "finished_at"}
)
_CONDITION_FIELDS = frozenset(
    {"type", "status", "reason", "message", "last_probe_time", "last_transition_time"}
)
_REDIS_FIELDS = frozenset(
    {
        "schema_version",
        "wave_id",
        "queue_name",
        "manifest_digest",
        "jobs_digest",
        "job_count",
        "target_key_count",
        "ready_slots",
        "ready_slots_digest",
        "release_present",
        "release_digest",
        "release_receipt",
        "queued_ordinals",
        "job_ordinals",
        "result_ordinals",
        "retry_ordinals",
        "in_progress_ordinals",
        "health_check_present",
        "attestation_digest",
    }
)
_REDIS_SLOT_FIELDS = frozenset(
    {
        "config_identity",
        "kubernetes_manifest_identity",
        "image_identity",
        "runtime_image_identity",
        "runtime_identity_digest",
        "manifest_digest",
        "pod_uid",
        "queue_name",
        "slot",
        "wave_id",
        "jobs_digest",
        "job_count",
        "protocol_identity",
        "serializer_identity",
    }
)
ABANDONMENT_PAYLOAD_FIELDS = frozenset(
    {
        "operation_id",
        "cutover_id",
        "wave_id",
        "wave_digest",
        "state",
        "quarantine_reason",
        "recovery_schema",
        "recovery_evidence_sha256",
        "admission",
        "database",
        "kubernetes",
        "redis",
    }
)


def _proof_digest(unsigned_proof_by_field: Mapping[str, Any]) -> str:
    return sha256_digest(
        V13_ABANDONMENT_PROOF_SCHEMA.encode("ascii")
        + b"\0"
        + canonical_json(dict(unsigned_proof_by_field))
    )


def _require_digest(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise PTGWaveMaterializedPreclaimConflict(f"{name} is invalid")
    return value
