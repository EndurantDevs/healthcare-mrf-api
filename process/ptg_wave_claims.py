"""Fail-closed worker-start claims for exact PTG waves.

The dynamic wave worker must call this before invoking the PTG function.  A
claim is intentionally one-shot: replayed ARQ delivery fails before any price
work is started instead of silently running a second copy.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select, update

from db.models import ImportRun, PTGImportWave, PTGImportWaveClaim, PTGImportWaveIntent, db
from process.ptg_wave_state import PTGWaveStateConflict


_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_RUNTIME_IMAGE = re.compile(r"^sha256:[0-9a-f]{64}$")
_REJECTION_FAILURE_CODE = "ptg_exact_wave_claim_rejected"
_REJECTABLE_RUN_STATES = frozenset({"queued", "starting", "running"})


class PTGWaveClaimConflict(PTGWaveStateConflict):
    """A worker context did not match its single admitted execution claim."""


@dataclass(frozen=True)
class PTGWaveClaimResolution:
    """Durable result of reconciling a failed worker-start claim attempt."""

    status: str
    ordinal: int
    claim_status: str
    same_attempt: bool


@dataclass(frozen=True)
class PTGWaveClaimInput:
    """Caller-supplied worker identity before fail-closed normalization."""

    wave_id: object
    run_id: object
    job_id: object
    slot: object
    pod_uid: object
    pinned_image_reference: object
    pinned_image_digest: object
    runtime_image_identity: object
    config_identity: object
    manifest_identity: object
    claim_attempt_token: object


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or not _HEX_64.fullmatch(value):
        raise PTGWaveClaimConflict(f"{name} must be a SHA-256 digest")
    return value


def _text(value: object, name: str, limit: int) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > limit:
        raise PTGWaveClaimConflict(f"{name} must be a non-empty bounded string")
    return value


def _slot(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < 12:
        raise PTGWaveClaimConflict("worker slot must be an index from 0 through 11")
    return value


def _runtime_identity(value: object) -> str:
    if not isinstance(value, str) or not _RUNTIME_IMAGE.fullmatch(value):
        raise PTGWaveClaimConflict("runtime image identity must be a container sha256 identity")
    return value


def _failure_code(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value):
        raise PTGWaveClaimConflict("claim rejection failure code is invalid")
    return value


def _attempt_token(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise PTGWaveClaimConflict("claim attempt token is invalid")
    return value


def _claim_values(
    claim_input: PTGWaveClaimInput,
) -> dict[str, Any]:
    """Normalize only caller-supplied fields before any durable mutation."""

    return {
        "wave_id": _text(claim_input.wave_id, "wave_id", 64),
        "run_id": _text(claim_input.run_id, "run_id", 64),
        "job_id": _text(claim_input.job_id, "job_id", 96),
        "slot": _slot(claim_input.slot),
        "pod_uid": _text(claim_input.pod_uid, "pod_uid", 128),
        "pinned_image_reference": _text(
            claim_input.pinned_image_reference, "pinned image reference", 512,
        ),
        "pinned_image_digest": _digest(
            claim_input.pinned_image_digest,
            "pinned image digest",
        ),
        "runtime_image_identity": _runtime_identity(
            claim_input.runtime_image_identity
        ),
        "config_identity": _digest(claim_input.config_identity, "config identity"),
        "manifest_identity": _digest(
            claim_input.manifest_identity,
            "manifest identity",
        ),
        "claim_attempt_token": _attempt_token(claim_input.claim_attempt_token),
    }


def _coerce_claim_input(
    claim_input: PTGWaveClaimInput | None,
    claim_fields: dict[str, object],
) -> PTGWaveClaimInput:
    if claim_input is not None:
        if claim_fields:
            raise PTGWaveClaimConflict(
                "claim input cannot be combined with individual claim fields"
            )
        return claim_input
    try:
        return PTGWaveClaimInput(**claim_fields)
    except TypeError as exc:
        raise PTGWaveClaimConflict("claim identity fields are not exact") from exc


def _exact_ready_slot(wave: Any, *, slot: int, pod_uid: str, runtime_image_identity: str) -> None:
    """Require this worker to be one of the original twelve attested Pods."""

    receipt = wave.kubernetes_ready_attestation
    if (
        not isinstance(receipt, dict)
        or not isinstance(wave.kubernetes_job_uid, str)
        or not wave.kubernetes_job_uid
        or receipt.get("job_uid") != wave.kubernetes_job_uid
    ):
        raise PTGWaveClaimConflict("worker wave has no persisted Kubernetes readiness receipt")
    slots = receipt.get("slots")
    if not isinstance(slots, list) or len(slots) != 12:
        raise PTGWaveClaimConflict("worker wave lacks an exact 12-slot readiness receipt")
    by_slot = {
        item.get("slot"): item
        for item in slots
        if isinstance(item, dict) and isinstance(item.get("slot"), int)
    }
    if set(by_slot) != set(range(12)):
        raise PTGWaveClaimConflict("worker wave lacks an exact 12-slot readiness receipt")
    expected_slot = by_slot[slot]
    if (
        expected_slot.get("pod_uid") != pod_uid
        or expected_slot.get("runtime_image_identity") != runtime_image_identity
    ):
        raise PTGWaveClaimConflict("worker Pod UID was not part of the initial 12-slot receipt")


async def _locked_claim_identity(
    session: Any,
    normalized_claim_fields: dict[str, Any],
    *,
    allow_states: frozenset[str],
) -> tuple[Any, Any]:
    """Revalidate every durable execution pin before touching a claim or run."""

    wave_query_result = await session.execute(
        select(PTGImportWave)
        .where(PTGImportWave.wave_id == normalized_claim_fields["wave_id"])
        .with_for_update()
    )
    wave = wave_query_result.scalar_one_or_none()
    if wave is None:
        raise PTGWaveClaimConflict("worker wave is not admitted")
    if wave.state not in allow_states:
        raise PTGWaveClaimConflict("worker claim is not allowed in the persisted wave state")
    if (
        wave.kubernetes_manifest_identity != normalized_claim_fields["manifest_identity"]
        or wave.pinned_image_reference != normalized_claim_fields["pinned_image_reference"]
        or wave.pinned_image_digest != normalized_claim_fields["pinned_image_digest"]
        or wave.runtime_image_identity != normalized_claim_fields["runtime_image_identity"]
        or wave.kubernetes_config_identity != normalized_claim_fields["config_identity"]
    ):
        raise PTGWaveClaimConflict("worker identity differs from the persisted execution pin")
    _exact_ready_slot(
        wave,
        slot=normalized_claim_fields["slot"],
        pod_uid=normalized_claim_fields["pod_uid"],
        runtime_image_identity=normalized_claim_fields["runtime_image_identity"],
    )
    intent_result = await session.execute(
        select(PTGImportWaveIntent)
        .where(
            PTGImportWaveIntent.wave_id == normalized_claim_fields["wave_id"],
            PTGImportWaveIntent.run_id == normalized_claim_fields["run_id"],
            PTGImportWaveIntent.job_id == normalized_claim_fields["job_id"],
        )
        .with_for_update()
    )
    intent = intent_result.scalar_one_or_none()
    if intent is None:
        raise PTGWaveClaimConflict("worker job/run pair is not an admitted wave intent")
    return wave, intent


async def _advance_released_wave_for_rejection(session: Any, wave: Any) -> None:
    """Make a durable rejection observable by the terminal outcome snapshot."""

    if wave.state != "released":
        return
    prior_version = wave.state_version
    result = await session.execute(
        update(PTGImportWave)
        .where(
            PTGImportWave.wave_id == wave.wave_id,
            PTGImportWave.state == "released",
            PTGImportWave.state_version == prior_version,
        )
        .values(state="executing", state_version=prior_version + 1)
        .execution_options(synchronize_session="fetch")
    )
    if result.rowcount != 1:
        raise PTGWaveClaimConflict("wave state changed before claim rejection")
    if wave.state != "executing" or wave.state_version != prior_version + 1:
        raise PTGWaveClaimConflict("wave state synchronization failed after claim rejection")


def _has_matching_claim(
    claim: Any,
    normalized_claim_fields: dict[str, Any],
    *,
    wave: Any,
    ordinal: int,
) -> bool:
    return (
        claim.wave_id == normalized_claim_fields["wave_id"]
        and claim.ordinal == ordinal
        and claim.run_id == normalized_claim_fields["run_id"]
        and claim.job_id == normalized_claim_fields["job_id"]
        and claim.slot == normalized_claim_fields["slot"]
        and claim.pod_uid == normalized_claim_fields["pod_uid"]
        and claim.kubernetes_job_uid == wave.kubernetes_job_uid
        and claim.pinned_image_reference == normalized_claim_fields["pinned_image_reference"]
        and claim.pinned_image_digest == normalized_claim_fields["pinned_image_digest"]
        and claim.runtime_image_identity == normalized_claim_fields["runtime_image_identity"]
        and claim.config_identity == normalized_claim_fields["config_identity"]
        and claim.manifest_identity == normalized_claim_fields["manifest_identity"]
    )


async def _advance_wave_for_start_claim(
    session: Any,
    normalized_claim_fields: dict[str, Any],
) -> None:
    wave_query_result = await session.execute(
        select(PTGImportWave)
        .where(PTGImportWave.wave_id == normalized_claim_fields["wave_id"])
        .with_for_update()
    )
    wave = wave_query_result.scalar_one_or_none()
    if wave is None:
        raise PTGWaveClaimConflict("worker wave is not admitted")
    if wave.state == "released":
        transition_result = await session.execute(
            update(PTGImportWave)
            .where(
                PTGImportWave.wave_id == normalized_claim_fields["wave_id"],
                PTGImportWave.state == "released",
                PTGImportWave.state_version == wave.state_version,
            )
            .values(state="executing", state_version=wave.state_version + 1)
        )
        if transition_result.rowcount != 1:
            raise PTGWaveClaimConflict("wave state changed before worker claim")
    elif wave.state == "redis_releasing":
        # Publication wakes workers before the release receipt is durable.
        if wave.redis_release_ticket is None or wave.redis_release_started_at is None:
            raise PTGWaveClaimConflict(
                "worker claim requires a persisted Redis release operation"
            )
    elif wave.state != "executing":
        raise PTGWaveClaimConflict(
            "worker claim is not allowed in the persisted wave state"
        )


async def _require_unclaimed_intent_claim(
    session: Any,
    normalized_claim_fields: dict[str, Any],
    *,
    ordinal: int,
) -> None:
    duplicate_query_result = await session.execute(
        select(PTGImportWaveClaim.ordinal)
        .where(
            PTGImportWaveClaim.wave_id == normalized_claim_fields["wave_id"],
            PTGImportWaveClaim.ordinal == ordinal,
        )
        .with_for_update()
    )
    if duplicate_query_result.scalar_one_or_none() is not None:
        raise PTGWaveClaimConflict(
            "wave intent was already claimed; duplicate worker delivery is refused"
        )


async def claim_wave_job_start(
    claim_input: PTGWaveClaimInput | None = None,
    **claim_fields: object,
) -> None:
    """Atomically bind one ARQ job to its attested slot before PTG execution."""

    normalized_claim_fields = _claim_values(
        _coerce_claim_input(claim_input, claim_fields)
    )
    async with db.transaction() as session:
        await _advance_wave_for_start_claim(session, normalized_claim_fields)
        # Keep the transition above inside the wave lock.  It admits the first
        # worker immediately after Redis release, while the helper below
        # rechecks the exact same identity before inserting its immutable row.
        wave, intent = await _locked_claim_identity(
            session, normalized_claim_fields,
            allow_states=frozenset({"redis_releasing", "released", "executing"}),
        )
        await _require_unclaimed_intent_claim(
            session,
            normalized_claim_fields,
            ordinal=intent.ordinal,
        )
        session.add(PTGImportWaveClaim(
            **normalized_claim_fields,
            ordinal=intent.ordinal, kubernetes_job_uid=wave.kubernetes_job_uid,
            claim_status="started", failure_code=None,
            claimed_at=dt.datetime.now(dt.UTC).replace(tzinfo=None),
        ))
        await session.flush()


async def _existing_claim_resolution(
    session: Any,
    wave: Any,
    intent: Any,
    normalized_claim_fields: dict[str, Any],
) -> PTGWaveClaimResolution | None:
    existing_claim = (await session.execute(
        select(PTGImportWaveClaim)
        .where(
            PTGImportWaveClaim.wave_id == normalized_claim_fields["wave_id"],
            PTGImportWaveClaim.ordinal == intent.ordinal,
        )
        .with_for_update()
    )).scalar_one_or_none()
    if existing_claim is None:
        return None
    if not _has_matching_claim(
        existing_claim,
        normalized_claim_fields,
        wave=wave,
        ordinal=intent.ordinal,
    ):
        raise PTGWaveClaimConflict(
            "existing worker claim differs from this exact identity"
        )
    if existing_claim.claim_status not in {"started", "rejected"}:
        raise PTGWaveClaimConflict("existing worker claim has an invalid status")
    return PTGWaveClaimResolution(
        status=(
            "claimed" if existing_claim.claim_status == "started" else "rejected"
        ),
        ordinal=intent.ordinal,
        claim_status=existing_claim.claim_status,
        same_attempt=(
            existing_claim.claim_attempt_token
            == normalized_claim_fields["claim_attempt_token"]
        ),
    )


async def _persist_rejected_claim_and_run(
    session: Any,
    wave: Any,
    intent: Any,
    normalized_claim_fields: dict[str, Any],
    *,
    failure_code: str,
) -> PTGWaveClaimResolution:
    run = (await session.execute(
        select(ImportRun)
        .where(ImportRun.run_id == normalized_claim_fields["run_id"])
        .with_for_update()
    )).scalar_one_or_none()
    if run is None:
        raise PTGWaveClaimConflict("admitted wave intent lacks its ImportRun")
    if run.status not in _REJECTABLE_RUN_STATES:
        raise PTGWaveClaimConflict(
            "claim rejection cannot rewrite a non-launchable ImportRun"
        )
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    session.add(PTGImportWaveClaim(
        **normalized_claim_fields,
        ordinal=intent.ordinal,
        kubernetes_job_uid=wave.kubernetes_job_uid,
        claim_status="rejected",
        failure_code=failure_code,
        claimed_at=now,
    ))
    run.status = "failed"
    run.phase_detail = "PTG exact-wave worker start rejected"
    run.finished_at = now
    run.heartbeat_at = now
    run.error = {"code": failure_code, "retryable": False}
    run.progress = {
        "unit": "run", "total": 1, "done": 0, "pct": 0, "message": "failed",
    }
    await session.flush()
    return PTGWaveClaimResolution(
        status="rejected",
        ordinal=intent.ordinal,
        claim_status="rejected",
        same_attempt=True,
    )


async def reconcile_wave_claim_exception(
    claim_input: PTGWaveClaimInput | None = None,
    *,
    failure_code: str = _REJECTION_FAILURE_CODE,
    **claim_fields: object,
) -> PTGWaveClaimResolution:
    """Resolve a valid-but-failed claim attempt without retrying worker work.

    Inputs are validated and rebound to the locked wave, intent, runtime pin,
    and original twelve Pod receipt before a rejected row is possible.  A
    duplicate committed start is reported as ``claimed``; callers must still
    refuse the replay rather than execute the import a second time.
    """

    normalized_claim_fields = _claim_values(
        _coerce_claim_input(claim_input, claim_fields)
    )
    failure_code = _failure_code(failure_code)
    async with db.transaction() as session:
        wave, intent = await _locked_claim_identity(
            session, normalized_claim_fields,
            allow_states=frozenset({"redis_releasing", "released", "executing"}),
        )
        await _advance_released_wave_for_rejection(session, wave)
        existing_resolution = await _existing_claim_resolution(
            session,
            wave,
            intent,
            normalized_claim_fields,
        )
        if existing_resolution is not None:
            return existing_resolution
        return await _persist_rejected_claim_and_run(
            session,
            wave,
            intent,
            normalized_claim_fields,
            failure_code=failure_code,
        )


__all__ = [
    "PTGWaveClaimConflict", "PTGWaveClaimInput", "PTGWaveClaimResolution",
    "claim_wave_job_start",
    "reconcile_wave_claim_exception",
]
