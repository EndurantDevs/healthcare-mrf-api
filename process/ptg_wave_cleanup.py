"""Durable exact-wave terminal and cleanup receipts, without external I/O."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from db.models import (
    PTGImportWaveClaim,
    PTGImportWaveIntent,
    PTGImportWaveOutcome,
    db,
)
from process.ptg_wave_terminal_state import derive_terminal_state as _derive_terminal_state
from process.ptg_wave_failure import (
    _verify_failure_redis,
    is_claimed_prestart_failure_receipt,
    verify_claimed_prestart_dead_letter_terminal_eligibility,
    verify_unclaimed_dead_letter_terminal_eligibility,
)
from process.ptg_wave_outcomes import verify_terminal_eligibility
from process.ptg_wave_state import (
    PTGWaveStateConflict,
    _locked_wave,
    _now,
    _transition,
    _ticket,
    canonical_json,
    operation_ticket_owner,
    sha256_digest,
)


async def begin_terminalizing(wave_id: str) -> None:
    """Fence terminal proof collection after all stable outcomes are linked."""

    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if wave.outcomes_digest is None or wave.linkage_ack_digest is None:
            raise PTGWaveStateConflict("terminalization requires persisted outcomes and linkage acknowledgement")
        await _transition(session, wave, "terminalizing")


async def persist_terminal_evidence(wave_id: str, terminal_receipt: object) -> str:
    """Verify a locked all-N snapshot and persist its reduced terminal proof.

    The controller supplies only external observations.  It cannot supply the
    summary that releases capacity: that summary is derived here from the
    locked wave, intents, claims, immutable outcomes, and linkage receipt.
    """

    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if wave.state != "terminalizing":
            raise PTGWaveStateConflict("terminal evidence is not expected for this wave")
        if wave.outcomes_digest is None or wave.linkage_ack_digest is None:
            raise PTGWaveStateConflict("terminal evidence lacks stable linkage prerequisites")
        intents = (await session.execute(
            select(PTGImportWaveIntent)
            .where(PTGImportWaveIntent.wave_id == wave_id)
            .order_by(PTGImportWaveIntent.ordinal)
            .with_for_update()
        )).scalars().all()
        claims = (await session.execute(
            select(PTGImportWaveClaim)
            .where(PTGImportWaveClaim.wave_id == wave_id)
            .order_by(PTGImportWaveClaim.ordinal)
            .with_for_update()
        )).scalars().all()
        outcomes = (await session.execute(
            select(PTGImportWaveOutcome)
            .where(PTGImportWaveOutcome.wave_id == wave_id)
            .order_by(PTGImportWaveOutcome.ordinal)
            .with_for_update()
        )).scalars().all()
        if is_claimed_prestart_failure_receipt(wave.failure_receipt):
            verifier = verify_claimed_prestart_dead_letter_terminal_eligibility
        elif wave.failure_receipt_digest is not None:
            verifier = verify_unclaimed_dead_letter_terminal_eligibility
        else:
            verifier = verify_terminal_eligibility
        evidence = verifier(wave, intents, claims, outcomes, terminal_receipt)
        digest = sha256_digest(canonical_json(evidence))
        await _transition(session, wave, "cleaning", values={
            "terminal_evidence_digest": digest,
            "terminal_summary": evidence,
        })
    return digest


async def mark_redis_cleanup_started(wave_id: str, *, operation_ticket: str) -> dict[str, Any]:
    """Commit the Redis marker before an adapter may delete exact keys."""

    operation_ticket = _ticket(operation_ticket)
    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if not operation_ticket_owner(wave.redis_cleanup_ticket, operation_ticket):
            return {"owner": False}
        if wave.state != "cleaning" or wave.terminal_evidence_digest is None:
            raise PTGWaveStateConflict("Redis cleanup is not expected for this wave")
        if wave.redis_cleanup_started_at is not None:
            raise PTGWaveStateConflict("Redis cleanup was started; reconcile it with GET only")
        wave.redis_cleanup_ticket = operation_ticket
        wave.redis_cleanup_started_at = _now()
        await session.flush()
        return {
            "owner": True, "operation_ticket": operation_ticket,
            "wave_digest": wave.wave_digest, "release_queue": wave.release_queue,
            "jobs_digest": wave.jobs_digest, "job_count": wave.intent_count,
            "release_digest": (wave.redis_release_attestation or {}).get("release_digest"),
        }


async def record_redis_cleanup_absent(wave_id: str, evidence: object) -> str:
    """Persist the full canonical exact-key GET absence attestation."""

    if not isinstance(evidence, dict):
        raise PTGWaveStateConflict("Redis cleanup evidence must be an object")
    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if wave.state != "cleaning" or wave.redis_cleanup_started_at is None:
            raise PTGWaveStateConflict("Redis cleanup absence receipt is not expected")
        _validate_redis_cleanup_evidence(wave, evidence)
        digest = sha256_digest(canonical_json(evidence))
        if wave.redis_cleanup_evidence_digest is not None:
            if wave.redis_cleanup_evidence_digest != digest:
                raise PTGWaveStateConflict("Redis cleanup evidence conflicts with the first receipt")
            return digest
        wave.redis_cleanup_evidence = evidence
        wave.redis_cleanup_evidence_digest = digest
        await session.flush()
        return digest


async def mark_kubernetes_delete_started(wave_id: str, *, operation_ticket: str) -> dict[str, Any]:
    """Commit the Kubernetes DELETE marker before the adapter may issue DELETE."""

    operation_ticket = _ticket(operation_ticket)
    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if not operation_ticket_owner(wave.kubernetes_delete_ticket, operation_ticket):
            return {"owner": False}
        early_failure_stop = (
            wave.state == "terminalizing"
            and isinstance(wave.failure_receipt, dict)
            and wave.failure_receipt.get("reason") == "redis_release_absent"
            and wave.linkage_ack_digest is not None
        )
        if not early_failure_stop and (
            wave.state != "cleaning" or wave.redis_cleanup_evidence_digest is None
        ):
            raise PTGWaveStateConflict("Kubernetes deletion requires persisted Redis absence evidence")
        if wave.kubernetes_delete_started_at is not None:
            raise PTGWaveStateConflict("Kubernetes deletion was started; reconcile it with GET only")
        is_job_absent = wave.kubernetes_job_uid is None
        if is_job_absent and (
            wave.k8s_post_started_at is None
            or wave.kubernetes_job_receipt_digest is not None
        ):
            raise PTGWaveStateConflict(
                "Kubernetes absence without a created Job must follow the persisted POST ticket"
            )
        wave.kubernetes_delete_ticket = operation_ticket
        wave.kubernetes_delete_started_at = _now()
        await session.flush()
        return {
            "owner": True, "operation_ticket": operation_ticket,
            "wave_digest": wave.wave_digest, "job_uid": wave.kubernetes_job_uid,
            "manifest_identity": wave.kubernetes_manifest_identity,
            # A controller receiving False must make no DELETE: the exact
            # Job was already GET-proven absent after the committed POST ticket.
            "delete_permitted": not is_job_absent,
        }


async def record_kubernetes_delete_absent(wave_id: str, evidence: object) -> str:
    """Persist GET-only absence evidence for the same UID-targeted Job deletion."""

    if not isinstance(evidence, dict):
        raise PTGWaveStateConflict("Kubernetes deletion evidence must be an object")
    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        early_failure_stop = (
            wave.state == "terminalizing"
            and isinstance(wave.failure_receipt, dict)
            and wave.failure_receipt.get("reason") == "redis_release_absent"
        )
        if (wave.state != "cleaning" and not early_failure_stop) or wave.kubernetes_delete_started_at is None:
            raise PTGWaveStateConflict("Kubernetes deletion absence receipt is not expected")
        _validate_kubernetes_absence_evidence(wave, evidence)
        digest = sha256_digest(canonical_json(evidence))
        if wave.kubernetes_delete_evidence_digest is not None:
            if wave.kubernetes_delete_evidence_digest != digest:
                raise PTGWaveStateConflict("Kubernetes deletion evidence conflicts with the first receipt")
            return digest
        wave.kubernetes_delete_evidence = evidence
        wave.kubernetes_delete_evidence_digest = digest
        await session.flush()
        return digest


async def persist_cleanup_and_terminal(
    wave_id: str,
) -> str:
    """Derive the final state and release capacity from locked exact evidence."""

    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if wave.state != "cleaning" or wave.terminal_evidence_digest is None:
            raise PTGWaveStateConflict("cleanup cannot release capacity before terminal evidence")
        if any(terminal_summary_value is None for terminal_summary_value in (
            wave.redis_cleanup_started_at, wave.redis_cleanup_evidence_digest,
            wave.kubernetes_delete_started_at, wave.kubernetes_delete_evidence_digest,
        )):
            raise PTGWaveStateConflict("cleanup markers and exact absence receipts are required")
        if (
            sha256_digest(canonical_json(wave.redis_cleanup_evidence))
            != wave.redis_cleanup_evidence_digest
            or sha256_digest(canonical_json(wave.kubernetes_delete_evidence))
            != wave.kubernetes_delete_evidence_digest
        ):
            raise PTGWaveStateConflict("persisted cleanup receipt digest is corrupt")
        _validate_redis_cleanup_evidence(wave, wave.redis_cleanup_evidence)
        _validate_kubernetes_absence_evidence(wave, wave.kubernetes_delete_evidence)
        outcomes = (await session.execute(
            select(PTGImportWaveOutcome)
            .where(PTGImportWaveOutcome.wave_id == wave_id)
            .order_by(PTGImportWaveOutcome.ordinal)
            .with_for_update()
        )).scalars().all()
        terminal_state = _derive_terminal_state(wave, outcomes)
        cleanup_evidence_map = {
            "schema_version": 1,
            "wave_id": wave.wave_id,
            "wave_digest": wave.wave_digest,
            "terminal_state": terminal_state,
            "terminal_evidence_digest": wave.terminal_evidence_digest,
            "redis_post_cleanup": wave.redis_cleanup_evidence,
            "kubernetes_absence": wave.kubernetes_delete_evidence,
        }
        digest = sha256_digest(canonical_json(cleanup_evidence_map))
        await _transition(session, wave, terminal_state, values={
            "cleanup_evidence_digest": digest,
            "cleanup_summary": cleanup_evidence_map,
            "resolved_at": _now(),
        })
    return digest


def _validate_redis_cleanup_evidence(wave: Any, evidence: object) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        raise PTGWaveStateConflict("Redis cleanup evidence must be an object")
    expected_fields = {
        "schema_version", "operation_ticket", "mode", "pre_cleanup",
        "operation_receipt", "post_cleanup",
    }
    if (
        set(evidence) != expected_fields
        or evidence["schema_version"] != "healthporta.ptg-wave.redis-cleanup.v1"
        or evidence["operation_ticket"] != wave.redis_cleanup_ticket
        or evidence["mode"] not in {"executed", "get_only_reconciled"}
    ):
        raise PTGWaveStateConflict("Redis cleanup evidence does not bind its one-shot operation")
    terminal = wave.terminal_summary if isinstance(wave.terminal_summary, dict) else {}
    persisted_pre = terminal.get("redis_pre_cleanup")
    pre = evidence["pre_cleanup"]
    if not isinstance(pre, dict) or pre != persisted_pre:
        raise PTGWaveStateConflict("Redis cleanup evidence lost its terminal pre-cleanup attestation")
    if getattr(wave, "failure_receipt_digest", None) is not None:
        _verify_failure_redis(wave, wave.failure_receipt, pre)
    else:
        _validate_redis_pre_cleanup_evidence(wave, pre)
    post = evidence["post_cleanup"]
    if getattr(wave, "failure_receipt_digest", None) is not None:
        _validate_unclaimed_redis_post_cleanup(wave, post, pre)
    else:
        _validate_redis_post_cleanup_evidence(wave, post)
    operation = evidence["operation_receipt"]
    if evidence["mode"] == "get_only_reconciled":
        if operation is not None:
            raise PTGWaveStateConflict("GET-only Redis cleanup recovery cannot invent an EXEC receipt")
    else:
        if getattr(wave, "failure_receipt_digest", None) is not None:
            _validate_unclaimed_redis_cleanup_operation(wave, operation, pre)
        else:
            _validate_redis_cleanup_operation(wave, operation, pre)
    return evidence


def _validate_redis_post_cleanup_evidence(wave: Any, evidence: object) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        raise PTGWaveStateConflict("Redis post-cleanup evidence must be an object")
    expected_fields = {
        "schema_version", "wave_id", "manifest_digest", "target_key_count",
        "absent_target_count", "attestation_digest",
    }
    expected_target_count = 4 + (4 * wave.intent_count)
    unsigned_evidence_map = {
        name: field_value
        for name, field_value in evidence.items()
        if name != "attestation_digest"
    }
    if (
        set(evidence) != expected_fields
        or evidence["schema_version"] != 1
        or evidence["wave_id"] != wave.wave_digest
        or evidence["manifest_digest"] != wave.manifest_digest
        or evidence["target_key_count"] != expected_target_count
        or evidence["absent_target_count"] != expected_target_count
        or evidence["attestation_digest"]
        != sha256_digest(canonical_json(unsigned_evidence_map))
    ):
        raise PTGWaveStateConflict("Redis cleanup evidence does not prove every exact target absent")
    return evidence


def _validate_redis_pre_cleanup_evidence(wave: Any, evidence: object) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        raise PTGWaveStateConflict("Redis pre-cleanup evidence must be an object")
    expected_fields = {
        "schema_version", "wave_id", "queue_name", "manifest_digest", "jobs_digest",
        "job_count", "image_identity", "release_digest", "target_key_count",
        "queue_entry_count", "job_payload_count", "result_count", "retry_count",
        "in_progress_count", "health_check_count", "result_presence_digest",
        "attestation_digest",
    }
    unsigned_evidence_map = {
        name: field_value
        for name, field_value in evidence.items()
        if name != "attestation_digest"
    }
    release = wave.redis_release_attestation or {}
    if (
        set(evidence) != expected_fields
        or evidence["schema_version"] != 1
        or evidence["wave_id"] != wave.wave_digest
        or evidence["queue_name"] != wave.release_queue
        or evidence["manifest_digest"] != wave.manifest_digest
        or evidence["jobs_digest"] != wave.jobs_digest
        or evidence["job_count"] != wave.intent_count
        or evidence["image_identity"] != wave.pinned_image_reference
        or evidence["release_digest"] != release.get("release_digest")
        or evidence["target_key_count"] != 4 + (4 * wave.intent_count)
        or any(evidence[name] != 0 for name in (
            "queue_entry_count", "job_payload_count", "retry_count", "in_progress_count",
        ))
        or not isinstance(evidence["result_count"], int)
        or isinstance(evidence["result_count"], bool)
        or not 0 <= evidence["result_count"] <= wave.intent_count
        or evidence["health_check_count"] not in {0, 1}
        or evidence["attestation_digest"]
        != sha256_digest(canonical_json(unsigned_evidence_map))
    ):
        raise PTGWaveStateConflict("Redis pre-cleanup evidence does not prove exact-wave idleness")
    _digest_like(evidence["result_presence_digest"], "Redis result-presence digest")
    return evidence


def _validate_redis_cleanup_operation(wave: Any, operation: object, pre: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(operation, dict):
        raise PTGWaveStateConflict("Redis cleanup execution receipt must be an object")
    expected_fields = {
        "schema_version", "wave_id", "manifest_digest", "target_key_count",
        "deleted_key_count", "pre_cleanup_attestation_digest", "pre_cleanup",
    }
    deleted = operation.get("deleted_key_count")
    if (
        set(operation) != expected_fields
        or operation["schema_version"] != 1
        or operation["wave_id"] != wave.wave_digest
        or operation["manifest_digest"] != wave.manifest_digest
        or operation["target_key_count"] != 4 + (4 * wave.intent_count)
        or not isinstance(deleted, int) or isinstance(deleted, bool)
        or not 0 <= deleted <= operation["target_key_count"]
        or operation["pre_cleanup_attestation_digest"] != pre["attestation_digest"]
        or operation["pre_cleanup"] != pre
    ):
        raise PTGWaveStateConflict("Redis cleanup execution receipt is not exact")
    return operation


def _validate_unclaimed_redis_cleanup_operation(
    wave: Any,
    operation: object,
    pre: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(operation, dict):
        raise PTGWaveStateConflict("unclaimed Redis cleanup execution receipt must be an object")
    expected_fields = {
        "schema_version", "wave_id", "manifest_digest", "target_key_count",
        "deleted_key_count", "expected_attestation_digest", "attestation",
    }
    deleted = operation.get("deleted_key_count")
    if (
        set(operation) != expected_fields
        or operation["schema_version"] != "healthporta.ptg-wave.redis-unclaimed-cleanup.v1"
        or operation["wave_id"] != wave.wave_digest
        or operation["manifest_digest"] != wave.manifest_digest
        or operation["target_key_count"] != 4 + (4 * wave.intent_count)
        or not isinstance(deleted, int) or isinstance(deleted, bool)
        or not 0 <= deleted <= operation["target_key_count"]
        or operation["expected_attestation_digest"] != pre["attestation_digest"]
        or operation["attestation"] != pre
    ):
        raise PTGWaveStateConflict("unclaimed Redis cleanup execution receipt is not exact")
    return operation


def _validate_unclaimed_redis_post_cleanup(
    wave: Any,
    post: object,
    pre: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(post, dict):
        raise PTGWaveStateConflict("unclaimed Redis post-cleanup evidence must be an object")
    expected_fields = {
        "schema_version", "wave_id", "manifest_digest", "target_key_count",
        "absent_target_count", "expected_attestation_digest", "attestation_digest",
    }
    unsigned_evidence_map = {
        name: field_value
        for name, field_value in post.items()
        if name != "attestation_digest"
    }
    target_count = 4 + (4 * wave.intent_count)
    if (
        set(post) != expected_fields
        or post["schema_version"] != "healthporta.ptg-wave.redis-unclaimed-post-cleanup.v1"
        or post["wave_id"] != wave.wave_digest
        or post["manifest_digest"] != wave.manifest_digest
        or post["target_key_count"] != target_count
        or post["absent_target_count"] != target_count
        or post["expected_attestation_digest"] != pre["attestation_digest"]
        or post["attestation_digest"]
        != sha256_digest(canonical_json(unsigned_evidence_map))
    ):
        raise PTGWaveStateConflict("unclaimed Redis cleanup did not prove every exact target absent")
    return post


def _digest_like(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise PTGWaveStateConflict(f"{name} must be a SHA-256 digest")
    return value


def _validate_kubernetes_absence_evidence(wave: Any, evidence: object) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        raise PTGWaveStateConflict("Kubernetes deletion evidence must be an object")
    expected_fields = {
        "schema_version", "operation_ticket", "wave_digest", "job_name", "job_uid",
        "manifest_identity", "delete_permitted", "job_absent", "pod_count",
        "pods_absent", "observation_digest",
    }
    metadata = wave.kubernetes_manifest.get("metadata") if isinstance(wave.kubernetes_manifest, dict) else None
    unsigned_evidence_map = {
        name: field_value
        for name, field_value in evidence.items()
        if name != "observation_digest"
    }
    if (
        set(evidence) != expected_fields
        or evidence["schema_version"] != "healthporta.ptg-wave.kubernetes-absence.v1"
        or evidence["operation_ticket"] != wave.kubernetes_delete_ticket
        or evidence["wave_digest"] != wave.wave_digest
        or evidence["job_name"] != (metadata.get("name") if isinstance(metadata, dict) else None)
        or evidence["job_uid"] != wave.kubernetes_job_uid
        or evidence["manifest_identity"] != wave.kubernetes_manifest_identity
        or evidence["delete_permitted"] is not (wave.kubernetes_job_uid is not None)
        or evidence["job_absent"] is not True
        or evidence["pod_count"] != 0
        or evidence["pods_absent"] is not True
        or evidence["observation_digest"]
        != sha256_digest(canonical_json(unsigned_evidence_map))
    ):
        raise PTGWaveStateConflict(
            "Kubernetes deletion evidence does not prove the exact Job and Pods absent"
        )
    return evidence


__all__ = [
    "begin_terminalizing", "mark_kubernetes_delete_started", "mark_redis_cleanup_started",
    "persist_cleanup_and_terminal", "persist_terminal_evidence", "record_kubernetes_delete_absent",
    "record_redis_cleanup_absent",
]
