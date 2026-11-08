# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed SOURCE POST and one retained-state observation on uncertain delivery.

The caller owns no page transaction during HTTP. An observation is not an ACK,
rollback proof or permission to resend the attempted cursor. The runner may
continue only from independently verified forward progress under the same lease.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import ClassVar

from sqlalchemy import select

from db.models.custom_import import CustomImportBuildStream
from process.custom_import import admission_authorization as admission
from process.custom_import import admission_worker as worker
from process.custom_import import source_authorization as authorization
from process.custom_import.build_source import _page_session, _prepare_statement
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost

_RECEIPT_FIELDS = frozenset(
    {
        "execution_id",
        "build_id",
        "fence",
        "stream_slot",
        "capture_bundle_id",
        "before",
        "after",
        "phase",
        "rows_processed",
        "stream_complete",
    }
)


@dataclass(frozen=True, slots=True)
class SourceBatchReceipt:
    execution_id: int
    build_id: int
    fence: int
    stream_slot: int
    capture_bundle_id: int
    before: authorization.SourceCursor
    after: authorization.SourceCursor
    phase: str
    rows_processed: int
    stream_complete: bool


@dataclass(frozen=True, slots=True)
class RetainedSourceProgress:
    execution_id: int
    build_id: int
    fence: int
    stream_slot: int
    capture_bundle_id: int
    cursor: authorization.SourceCursor
    phase: str
    stream_complete: bool


class SourceReconciliationRequired(worker.AdmissionTransportError):
    """Original attempt plus at most one observation; never implicit retry."""

    def __init__(self, pins, progress):
        super().__init__("custom_import_source_reconciliation_required")
        self.pins = pins
        self.progress = progress


def _position(cursor):
    return cursor.next_part_ordinal, cursor.next_part_row_ordinal


def validate_receipt(document, pins, *, capture_bundle_id, was_complete):
    """Validate the full committed cursor, including a real EOF/freeze transition."""

    if type(document) is not dict or document.keys() != _RECEIPT_FIELDS:
        raise worker._unavailable()
    for name in ("execution_id", "build_id", "fence", "stream_slot", "capture_bundle_id"):
        admission._positive_id(document[name])
        expected = capture_bundle_id if name == "capture_bundle_id" else getattr(pins, name)
        if document[name] != expected:
            raise worker._unavailable()
    before, after = authorization.cursor(document["before"]), authorization.cursor(document["after"])
    count, phase, complete = document["rows_processed"], document["phase"], document["stream_complete"]
    if (
        before != pins.expected_cursor
        or type(count) is not int
        or not 0 <= count <= 100_000
        or type(phase) is not str
        or phase not in {"source", "admission"}
        or type(complete) is not bool
        or type(was_complete) is not bool
        or phase == "admission"
        and not complete
        or after.next_source_ordinal - before.next_source_ordinal != count
        or _position(after) < _position(before)
        or after.next_pack_ordinal < before.next_pack_ordinal
    ):
        raise worker._unavailable()
    if count:
        if was_complete or _position(after) <= _position(before) or after.next_pack_ordinal <= before.next_pack_ordinal:
            raise worker._unavailable()
    elif (
        was_complete
        and phase != "admission"
        or after.next_pack_ordinal != before.next_pack_ordinal
        or not complete
        and not (after.next_part_ordinal > before.next_part_ordinal and after.next_part_row_ordinal == 0)
        or _position(after) != _position(before)
        and after.next_part_row_ordinal != 0
    ):
        raise worker._unavailable()
    return SourceBatchReceipt(**{**document, "before": before, "after": after})


def _receipt(raw, pins, *, capture_bundle_id, was_complete):
    try:
        document = json.loads(
            raw.decode("ascii"),
            object_pairs_hook=admission._unique_object,
            parse_constant=admission._reject_number,
            parse_float=admission._reject_number,
            parse_int=admission._integer,
        )
        return validate_receipt(document, pins, capture_bundle_id=capture_bundle_id, was_complete=was_complete)
    except UnicodeError, ValueError, RecursionError:
        failure = worker._unavailable()
    raise failure


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceBatchTransport(worker.AdmissionBatchTransport):
    """Same bounded TLS client; only the fixed SOURCE path and purpose differ."""

    _source: ClassVar[bool] = True

    async def send(self, request, pins, *, capture_bundle_id, stream_complete):
        """Attempt one request against the original retained cursor and completion."""

        if (
            type(pins) is not authorization.SourceBatchPins
            or pins.execution_id != request.execution_id
            or pins.fence != request.fence
            or self.bind_request(request).authorization_expires_at != request.authorization_expires_at
            or type(stream_complete) is not bool
        ):
            raise worker._unavailable()
        admission._positive_id(capture_bundle_id)
        document = asdict(pins)
        authorization.batch_pins(document)
        body = json.dumps(document, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode(
            "ascii"
        )
        return _receipt(
            await self._exchange(request, body),
            pins,
            capture_bundle_id=capture_bundle_id,
            was_complete=stream_complete,
        )


async def _retained_progress(session_factory, request, build_id, stream_slot):
    """Complete the existing locked page before exposing its metadata to HTTP."""

    admission._positive_id(stream_slot)
    if stream_slot > (1 << 15) - 1:
        raise worker._unavailable()
    async with _page_session(session_factory, request, build_id) as (session, build):
        if build.phase not in worker._POST_ADMISSION_PHASES | {"source", "admission"}:
            raise worker._unavailable()
        await _prepare_statement(session)
        retained = (
            await session.scalars(
                select(CustomImportBuildStream)
                .where(
                    CustomImportBuildStream.build_id == build_id,
                    CustomImportBuildStream.stream_slot == stream_slot,
                )
                .with_for_update()
            )
        ).one_or_none()
        if retained is None or build.phase != "source" and retained.replay_verified_at is None:
            raise worker._unavailable()
        cursor = authorization.cursor({name: getattr(retained, name) for name in authorization.CURSOR_FIELDS})
        admission._positive_id(build.capture_bundle_id)
        return RetainedSourceProgress(
            request.execution_id,
            build_id,
            request.fence,
            stream_slot,
            build.capture_bundle_id,
            cursor,
            build.phase,
            retained.replay_verified_at is not None,
        )


def has_forward_progress(before, after):
    """A retained observation may advance any coordinate, but never regress one."""

    matches = (
        type(after) is RetainedSourceProgress
        and all(
            getattr(before, name) == getattr(after, name)
            for name in ("execution_id", "build_id", "fence", "stream_slot", "capture_bundle_id")
        )
        and _position(after.cursor) >= _position(before.cursor)
        and after.cursor.next_source_ordinal >= before.cursor.next_source_ordinal
        and after.cursor.next_pack_ordinal >= before.cursor.next_pack_ordinal
        and (not before.stream_complete or after.stream_complete)
        and before.phase == "source"
        and (
            after.cursor != before.cursor
            or not before.stream_complete
            and after.stream_complete
            or before.phase == "source"
            and after.phase != "source"
        )
    )
    if not matches:
        return False
    if after.cursor.next_source_ordinal > before.cursor.next_source_ordinal:
        return (
            not before.stream_complete
            and _position(after.cursor) > _position(before.cursor)
            and after.cursor.next_pack_ordinal > before.cursor.next_pack_ordinal
        )
    return after.cursor.next_pack_ordinal == before.cursor.next_pack_ordinal and (
        after.cursor == before.cursor
        or not before.stream_complete
        and after.cursor.next_part_ordinal > before.cursor.next_part_ordinal
        and after.cursor.next_part_row_ordinal == 0
    )


async def source_next_batch(session_factory, request, build_id, stream_slot, transport):
    """No local locks across POST and no blind replay after any uncertain result."""

    request = transport.bind_request(request)
    before = await _retained_progress(session_factory, request, build_id, stream_slot)
    if before.phase != "source":
        return before
    pins = authorization.SourceBatchPins(build_id, request.execution_id, request.fence, stream_slot, before.cursor)
    try:
        return await transport.send(
            request,
            pins,
            capture_bundle_id=before.capture_bundle_id,
            stream_complete=before.stream_complete,
        )
    except worker.AdmissionTransportError:
        retained = None
    try:
        observation = await _retained_progress(session_factory, request, build_id, stream_slot)
        if has_forward_progress(before, observation):
            retained = observation
    except CancellationRequested, LeaseAuthorityLost:
        raise
    except Exception:
        retained = None
    raise SourceReconciliationRequired(pins, retained)
