"""Database-only state transitions for an exact PTG worker wave.

The controller calls these methods around external operations.  In particular,
the methods that mark a POST or Redis EXEC *commit before* the caller may
perform that operation.  They intentionally do not import Kubernetes, Redis,
or application lifecycle code.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from typing import Any

from sqlalchemy import select, update

from db.models import PTGImportWave, PTGImportWaveClaim, db
from process.ptg_parts.ptg_wave_admission_fence import PTG_WAVE_TERMINAL_STATES
from process.ptg_wave_receipt_projection import wave_receipt_mapping
from process.ptg_wave_release_validation import validate_release_receipt


_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_RUNTIME_IMAGE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PINNED_IMAGE_REFERENCE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:([0-9a-f]{64})$"
)
_OPERATION_TICKET = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_IDENTITY_STATES = frozenset({
    "materializing", "slots_waiting", "redis_releasing", "released",
    "executing", "awaiting_linkage", "terminalizing", "cleaning",
})
_NEXT_STATES = {
    "admitted": frozenset({"materializing"}),
    "materializing": frozenset({"slots_waiting", "uncertain"}),
    # A validated all-unclaimed failure may enter linkage wait from a state
    # before normal execution.  The only caller is the failure module, which
    # first persists a closed failure receipt and refuses any worker claim.
    "slots_waiting": frozenset({"redis_releasing", "awaiting_linkage", "uncertain"}),
    # Publishing the Redis release is atomic, but it wakes workers before the
    # controller can persist the resulting receipt.  A correctly attested
    # worker may therefore claim during this narrow state and advance the wave
    # directly to executing when the receipt is recorded.
    "redis_releasing": frozenset({"released", "executing", "awaiting_linkage", "uncertain"}),
    "released": frozenset({"executing", "awaiting_linkage", "uncertain"}),
    "executing": frozenset({"awaiting_linkage", "uncertain"}),
    "awaiting_linkage": frozenset({"terminalizing", "uncertain"}),
    "terminalizing": frozenset({"cleaning", "uncertain"}),
    "cleaning": frozenset(set(PTG_WAVE_TERMINAL_STATES) | {"uncertain"}),
    "uncertain": _IDENTITY_STATES,
}


class PTGWaveStateConflict(RuntimeError):
    """The persisted wave cannot safely take the requested transition."""


def canonical_json(value: Any) -> bytes:
    """Serialize one wave receipt into deterministic canonical JSON bytes."""

    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PTGWaveStateConflict("wave receipt must be canonical JSON") from exc


def sha256_digest(value: bytes) -> str:
    """Return the lowercase SHA-256 digest for canonical receipt bytes."""

    return hashlib.sha256(value).hexdigest()


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or not _HEX_64.fullmatch(value):
        raise PTGWaveStateConflict(f"{name} must be a SHA-256 digest")
    return value


def _runtime_image_identity(value: object) -> str:
    if not isinstance(value, str) or not _RUNTIME_IMAGE.fullmatch(value):
        raise PTGWaveStateConflict("runtime image identity must be an exact container sha256 identity")
    return value


def _ticket(value: object) -> str:
    if not isinstance(value, str) or not _OPERATION_TICKET.fullmatch(value):
        raise PTGWaveStateConflict("external-operation ticket must be a bounded canonical identifier")
    return value


def is_operation_ticket_owner(
    existing_ticket: str | None,
    candidate_ticket: object,
) -> bool:
    """Return true only for the first locked caller of a closed external operation."""

    _ticket(candidate_ticket)
    return existing_ticket is None


operation_ticket_owner = is_operation_ticket_owner


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def assert_transition(current: str, target: str, *, resume_state: str | None = None) -> None:
    """Reject skipped or ambiguous transitions before any database mutation."""

    if target not in _NEXT_STATES.get(current, frozenset()):
        raise PTGWaveStateConflict(f"invalid exact-wave transition {current!r} -> {target!r}")
    if current == "uncertain" and target != resume_state:
        raise PTGWaveStateConflict("uncertain wave may only resume its persisted state")
    if target == "uncertain" and current not in _IDENTITY_STATES:
        raise PTGWaveStateConflict("admitted wave cannot enter an external-operation uncertainty")


async def _locked_wave(session: Any, wave_id: str) -> PTGImportWave:
    result = await session.execute(
        select(PTGImportWave).where(PTGImportWave.wave_id == wave_id).with_for_update()
    )
    wave = result.scalar_one_or_none()
    if wave is None:
        raise PTGWaveStateConflict("exact wave is not admitted")
    return wave


async def _transition(
    session: Any,
    wave: PTGImportWave,
    target: str,
    *,
    values: dict[str, Any] | None = None,
) -> None:
    assert_transition(wave.state, target, resume_state=wave.uncertainty_resume_state)
    transition_field_map = dict(values or {})
    transition_field_map.update({
        "state": target,
        "state_version": wave.state_version + 1,
        "uncertainty_resume_state": wave.state if target == "uncertain" else None,
    })
    result = await session.execute(
        update(PTGImportWave)
        .where(
            PTGImportWave.wave_id == wave.wave_id,
            PTGImportWave.state == wave.state,
            PTGImportWave.state_version == wave.state_version,
        )
        .values(**transition_field_map)
    )
    if result.rowcount != 1:
        raise PTGWaveStateConflict("exact-wave state changed concurrently")


def _validate_materialization(
    manifest: object,
    manifest_bytes: bytes,
    image_reference: str,
    image_digest: str,
    runtime_image_identity: str,
    config_identity: str,
    manifest_identity: str,
) -> tuple[dict[str, Any], str]:
    if not isinstance(manifest, dict) or not manifest:
        raise PTGWaveStateConflict("desired Kubernetes manifest must be a non-empty object")
    if not isinstance(manifest_bytes, bytes) or not manifest_bytes:
        raise PTGWaveStateConflict("desired Kubernetes manifest bytes are required")
    try:
        decoded = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PTGWaveStateConflict("desired Kubernetes manifest bytes must be JSON") from exc
    if decoded != manifest:
        raise PTGWaveStateConflict("desired Kubernetes manifest JSON and bytes differ")
    _digest(image_digest, "pinned image digest")
    _runtime_image_identity(runtime_image_identity)
    _digest(config_identity, "Kubernetes config identity")
    reference_match = _PINNED_IMAGE_REFERENCE.fullmatch(image_reference) if isinstance(image_reference, str) else None
    if reference_match is None or reference_match.group(1) != image_digest:
        raise PTGWaveStateConflict("worker image identity is not pinned to its digest")
    _digest(manifest_identity, "Kubernetes manifest identity")
    return dict(manifest), sha256_digest(manifest_bytes)


async def persist_materialization(
    wave_id: str,
    *,
    manifest: object,
    manifest_bytes: bytes,
    image_reference: str,
    image_digest: str,
    runtime_image_identity: str,
    config_identity: str,
    manifest_identity: str,
) -> str:
    """Persist the exact desired Job before any Kubernetes POST is permitted."""

    manifest_json, manifest_digest = _validate_materialization(
        manifest, manifest_bytes, image_reference, image_digest, runtime_image_identity,
        config_identity, manifest_identity,
    )
    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        await _transition(session, wave, "materializing", values={
            "kubernetes_manifest": manifest_json,
            "kubernetes_manifest_bytes": manifest_bytes,
            "kubernetes_manifest_sha256": manifest_digest,
            "kubernetes_manifest_identity": manifest_identity,
            "pinned_image_reference": image_reference,
            "pinned_image_digest": image_digest,
            "runtime_image_identity": runtime_image_identity,
            "kubernetes_config_identity": config_identity,
        })
    return manifest_digest


def _require_materialization(wave: PTGImportWave) -> None:
    values = (
        wave.kubernetes_manifest, wave.kubernetes_manifest_bytes,
        wave.kubernetes_manifest_sha256, wave.kubernetes_manifest_identity,
        wave.pinned_image_reference, wave.pinned_image_digest, wave.runtime_image_identity,
        wave.kubernetes_config_identity,
    )
    if any(value is None for value in values):
        raise PTGWaveStateConflict("desired Kubernetes manifest has not been persisted")
    if sha256_digest(bytes(wave.kubernetes_manifest_bytes)) != wave.kubernetes_manifest_sha256:
        raise PTGWaveStateConflict("persisted Kubernetes manifest bytes are corrupt")
    _validate_materialization(
        wave.kubernetes_manifest, bytes(wave.kubernetes_manifest_bytes),
        wave.pinned_image_reference, wave.pinned_image_digest, wave.runtime_image_identity,
        wave.kubernetes_config_identity, wave.kubernetes_manifest_identity,
    )


async def mark_kubernetes_post_started(wave_id: str, *, operation_ticket: str) -> dict[str, Any]:
    """Commit a POST-start receipt, returning only a saved desired Job record."""

    operation_ticket = _ticket(operation_ticket)
    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if not operation_ticket_owner(wave.k8s_post_ticket, operation_ticket):
            return {"owner": False}
        _require_materialization(wave)
        await _transition(session, wave, "slots_waiting", values={
            "k8s_post_ticket": operation_ticket, "k8s_post_started_at": _now(),
        })
        return {
            "owner": True, "wave_digest": wave.wave_digest,
            "manifest": wave.kubernetes_manifest,
            "manifest_bytes": bytes(wave.kubernetes_manifest_bytes),
            "manifest_sha256": wave.kubernetes_manifest_sha256,
            "manifest_identity": wave.kubernetes_manifest_identity,
            "pinned_image_reference": wave.pinned_image_reference,
            "pinned_image_digest": wave.pinned_image_digest,
            "runtime_image_identity": wave.runtime_image_identity,
            "config_identity": wave.kubernetes_config_identity,
        }


def _validate_job_receipt(wave: PTGImportWave, receipt: object) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        raise PTGWaveStateConflict("Kubernetes Job receipt must be an object")
    expected_receipt_fields = {
        "wave_digest", "job_uid", "manifest_identity", "config_identity",
        "pinned_image_reference", "pinned_image_digest", "runtime_image_identity",
    }
    if set(receipt) != expected_receipt_fields:
        raise PTGWaveStateConflict("Kubernetes Job receipt fields are not exact")
    expected_receipt_field_map = {
        "wave_digest": wave.wave_digest, "manifest_identity": wave.kubernetes_manifest_identity,
        "config_identity": wave.kubernetes_config_identity,
        "pinned_image_reference": wave.pinned_image_reference,
        "pinned_image_digest": wave.pinned_image_digest,
        "runtime_image_identity": wave.runtime_image_identity,
    }
    if any(
        receipt[name] != expected_value
        for name, expected_value in expected_receipt_field_map.items()
    ):
        raise PTGWaveStateConflict("Kubernetes Job receipt does not bind the desired manifest")
    if not isinstance(receipt["job_uid"], str) or not receipt["job_uid"].strip():
        raise PTGWaveStateConflict("Kubernetes Job receipt has no UID")
    return receipt


async def record_kubernetes_job_created(wave_id: str, receipt: object) -> str:
    """Persist the POST/409 GET Job receipt before the controller waits for Pods."""

    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if wave.state != "slots_waiting" or wave.k8s_post_started_at is None:
            raise PTGWaveStateConflict("Kubernetes Job creation is not expected for this wave")
        receipt = _validate_job_receipt(wave, receipt)
        digest = sha256_digest(canonical_json(receipt))
        if wave.kubernetes_job_receipt_digest is not None:
            if wave.kubernetes_job_receipt_digest != digest:
                raise PTGWaveStateConflict("Kubernetes Job receipt conflicts with the first receipt")
            return digest
        wave.kubernetes_job_uid = receipt["job_uid"]
        wave.kubernetes_job_receipt = receipt
        wave.kubernetes_job_receipt_digest = digest
        await session.flush()
        return digest


def _validate_ready_receipt(wave: PTGImportWave, receipt: object) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        raise PTGWaveStateConflict("Kubernetes readiness receipt must be an object")
    expected_receipt_fields = {
        "wave_digest", "job_uid", "manifest_identity", "config_identity", "pinned_image_reference",
        "pinned_image_digest", "runtime_image_identity", "slots",
    }
    if set(receipt) != expected_receipt_fields:
        raise PTGWaveStateConflict("Kubernetes readiness receipt fields are not exact")
    for field, expected_value in {
        "wave_digest": wave.wave_digest,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "config_identity": wave.kubernetes_config_identity,
        "pinned_image_reference": wave.pinned_image_reference,
        "pinned_image_digest": wave.pinned_image_digest,
    }.items():
        if receipt[field] != expected_value:
            raise PTGWaveStateConflict(f"Kubernetes readiness {field} does not match the wave")
    if not isinstance(receipt["job_uid"], str) or not receipt["job_uid"].strip():
        raise PTGWaveStateConflict("Kubernetes readiness receipt has no Job UID")
    slots = receipt["slots"]
    if not isinstance(slots, list) or len(slots) != 12:
        raise PTGWaveStateConflict("Kubernetes readiness receipt must attest exactly 12 slots")
    seen_slot_ids: set[int] = set()
    for slot in slots:
        if not isinstance(slot, dict) or set(slot) != {"slot", "pod_uid", "runtime_image_identity"}:
            raise PTGWaveStateConflict("Kubernetes slot receipt fields are not exact")
        if (
            not isinstance(slot["slot"], int)
            or slot["slot"] in seen_slot_ids
            or not 0 <= slot["slot"] < 12
        ):
            raise PTGWaveStateConflict("Kubernetes readiness slots must be unique indexes 0 through 11")
        if not isinstance(slot["pod_uid"], str) or not slot["pod_uid"].strip():
            raise PTGWaveStateConflict("Kubernetes readiness slot has no Pod UID")
        if slot["runtime_image_identity"] != receipt["runtime_image_identity"]:
            raise PTGWaveStateConflict("Kubernetes readiness slot image differs from the pin")
        seen_slot_ids.add(slot["slot"])
    if seen_slot_ids != set(range(12)):
        raise PTGWaveStateConflict("Kubernetes readiness slots must cover indexes 0 through 11")
    return receipt


async def record_kubernetes_ready(wave_id: str, receipt: object) -> str:
    """Record the exact Job and initial twelve-pod readiness attestation."""

    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if wave.state != "slots_waiting" or wave.k8s_post_started_at is None:
            raise PTGWaveStateConflict("Kubernetes readiness is not expected for this wave")
        receipt = _validate_ready_receipt(wave, receipt)
        digest = sha256_digest(canonical_json(receipt))
        if wave.kubernetes_ready_attestation_digest is not None:
            if wave.kubernetes_ready_attestation_digest != digest:
                raise PTGWaveStateConflict("Kubernetes readiness receipt conflicts with the first receipt")
            return digest
        _runtime_image_identity(receipt["runtime_image_identity"])
        if wave.runtime_image_identity != receipt["runtime_image_identity"]:
            raise PTGWaveStateConflict("Kubernetes runtime image differs from the desired manifest")
        if wave.kubernetes_job_uid is None or wave.kubernetes_job_receipt_digest is None:
            raise PTGWaveStateConflict("Kubernetes Job creation receipt must precede readiness")
        if wave.kubernetes_job_uid != receipt["job_uid"]:
            raise PTGWaveStateConflict("Kubernetes readiness Job UID differs from the created Job")
        wave.kubernetes_ready_attestation = receipt
        wave.kubernetes_ready_attestation_digest = digest
        await session.flush()
        return digest


async def has_started_redis_release(
    wave_id: str,
    *,
    operation_ticket: str,
) -> bool:
    """Commit the Redis EXEC-start marker before an adapter may execute it."""

    operation_ticket = _ticket(operation_ticket)
    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if not is_operation_ticket_owner(wave.redis_release_ticket, operation_ticket):
            return False
        if wave.kubernetes_ready_attestation_digest is None:
            raise PTGWaveStateConflict("Redis release requires the persisted 12-slot readiness receipt")
        await _transition(session, wave, "redis_releasing", values={
            "redis_release_ticket": operation_ticket, "redis_release_started_at": _now(),
        })
    return True


mark_redis_release_started = has_started_redis_release


def _validate_release_receipt(wave: PTGImportWave, receipt: object) -> dict[str, Any]:
    """Validate the exact 12-slot Redis release against persisted readiness."""

    return validate_release_receipt(
        wave,
        receipt,
        conflict_type=PTGWaveStateConflict,
        canonical_json=canonical_json,
        sha256_digest=sha256_digest,
        digest_validator=_digest,
    )


async def record_redis_release(wave_id: str, receipt: object) -> str:
    """Persist the exact Redis release receipt after GET-only reconciliation."""

    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if wave.state != "redis_releasing" or wave.redis_release_started_at is None:
            raise PTGWaveStateConflict("Redis release receipt is not expected for this wave")
        receipt = _validate_release_receipt(wave, receipt)
        digest = sha256_digest(canonical_json(receipt))
        if wave.redis_release_attestation_digest is not None:
            if wave.redis_release_attestation_digest != digest:
                raise PTGWaveStateConflict("Redis release receipt conflicts with the first receipt")
            return digest
        has_claim = (await session.execute(
            select(PTGImportWaveClaim.ordinal)
            .where(PTGImportWaveClaim.wave_id == wave_id)
            .limit(1)
        )).scalar_one_or_none() is not None
        await _transition(session, wave, "executing" if has_claim else "released", values={
            "redis_release_attestation": receipt,
            "redis_release_attestation_digest": digest,
        })
        return digest


async def mark_uncertain(wave_id: str, *, expected_state: str) -> None:
    """Fence an ambiguous external result; only a GET-only reconciler may resume it."""

    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if wave.state != expected_state:
            raise PTGWaveStateConflict("wave state changed before uncertainty fence")
        await _transition(session, wave, "uncertain")


async def resolve_uncertainty(wave_id: str, *, reconciled_state: str) -> None:
    """Return an uncertain wave only to its persisted pre-ambiguity state."""

    async with db.transaction() as session:
        wave = await _locked_wave(session, wave_id)
        if wave.state != "uncertain":
            raise PTGWaveStateConflict("wave is not uncertain")
        await _transition(session, wave, reconciled_state)


async def get_wave_receipts(wave_id: str) -> dict[str, Any] | None:
    """GET-only controller helper; it has no external or database side effect."""

    wave_query_result = await db.execute(
        select(PTGImportWave).where(PTGImportWave.wave_id == wave_id)
    )
    wave = wave_query_result.scalar_one_or_none()
    if wave is None:
        return None
    return wave_receipt_mapping(wave)


__all__ = [
    "PTGWaveStateConflict", "assert_transition", "canonical_json", "get_wave_receipts",
    "mark_kubernetes_post_started", "mark_redis_release_started", "mark_uncertain",
    "persist_materialization", "record_kubernetes_job_created", "record_kubernetes_ready",
    "operation_ticket_owner", "record_redis_release",
    "resolve_uncertainty", "sha256_digest",
]
