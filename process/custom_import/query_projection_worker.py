# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed projection POST with one read-only observation after uncertain delivery."""

from __future__ import annotations

import hmac
import json
from dataclasses import asdict, dataclass
from typing import ClassVar

from sqlalchemy import select, text

from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import admission_authorization as admission
from process.custom_import import admission_worker as worker
from process.custom_import import query_projection_authorization as authorization
from process.custom_import.build_graph import _read_authority
from process.custom_import.query_projection_binding import _definition_binding, _manifest_shape
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from process.custom_import.source_batch import _read_session

_CURSOR_NAMES = (
    "phase",
    "after_entity_binding_id",
    "after_selected_value",
    "after_child_key_sha256",
    "index_count",
    "proof_count",
)
_PROGRESS_FIELDS = frozenset((*_CURSOR_NAMES, "root_count", "child_count"))
_IDENTITY_NAMES = ("execution_id", "build_id", "fence", "generation_id", "capture_bundle_id")
_RECEIPT_FIELDS = frozenset((*_IDENTITY_NAMES, "before", "after"))
_PHASES = ("root", "child", "indexes", "verify_root", "verify_child", "digest_root", "digest_child", "complete")


@dataclass(frozen=True, slots=True)
class RetainedProjectionProgress:
    """A detached retained observation, never a substitute for a committed receipt."""

    execution_id: int
    build_id: int
    fence: int
    generation_id: int
    capture_bundle_id: int
    progress: object


class ProjectionReconciliationRequired(worker.AdmissionTransportError):
    """Preserve original attempted pins without authorizing an implicit resend."""

    def __init__(self, pins, progress):
        super().__init__("custom_import_projection_reconciliation_required")
        self.pins = pins
        self.progress = progress


def _pins(build_id, execution_id, fence, progress):
    return authorization.batch_pins(
        {
            "build_id": build_id,
            "execution_id": execution_id,
            "fence": fence,
            **{f"expected_{name}": getattr(progress, name) for name in _CURSOR_NAMES},
        }
    )


def _progress(document):
    from process.custom_import.query_projection_build import ProjectionProgress

    if type(document) is not dict or document.keys() != _PROGRESS_FIELDS:
        raise worker._unavailable()
    for name in ("root_count", "child_count"):
        admission._positive_id(document[name], minimum=0)
    progress = ProjectionProgress(**document)
    _pins(1, 1, 1, progress)
    return progress


def _position(progress):
    selected = -(2**63) if progress.after_selected_value is None else progress.after_selected_value
    return progress.after_entity_binding_id, selected, progress.after_child_key_sha256 or ""


def has_forward_progress(before, after):
    """Permit only monotonic counts and a forward retained phase or context."""

    if any(
        getattr(after, name) < getattr(before, name)
        for name in ("root_count", "child_count", "index_count", "proof_count")
    ):
        return False
    if _PHASES.index(after.phase) != _PHASES.index(before.phase):
        return _PHASES.index(after.phase) > _PHASES.index(before.phase)
    if before.phase in {"verify_root", "verify_child"}:
        return (before.root_count, before.child_count, before.index_count) == (
            after.root_count,
            after.child_count,
            after.index_count,
        ) and _position(after) > _position(before)
    if before.phase in {"root", "child"}:
        other_count = "child_count" if before.phase == "root" else "root_count"
        return getattr(before, other_count) == getattr(after, other_count) and _position(after) > _position(before)
    if before.phase in {"digest_root", "digest_child"}:
        return after.proof_count > before.proof_count and (
            before.root_count,
            before.child_count,
            before.index_count,
        ) == (after.root_count, after.child_count, after.index_count)
    return (
        before.phase == "indexes"
        and after.index_count > before.index_count
        and (before.root_count, before.child_count, _position(before))
        == (after.root_count, after.child_count, _position(after))
    )


def validate_receipt(document, pins, *, generation_id, capture_bundle_id, before=None):
    """Bind a committed receipt to the exact observed attempt and complete cursor."""

    from process.custom_import.query_projection_build import ProjectionBatchReceipt

    if type(document) is not dict or document.keys() != _RECEIPT_FIELDS:
        raise worker._unavailable()
    for name in _IDENTITY_NAMES:
        admission._positive_id(document[name])
        expected = (
            generation_id
            if name == "generation_id"
            else capture_bundle_id
            if name == "capture_bundle_id"
            else getattr(pins, name)
        )
        if document[name] != expected:
            raise worker._unavailable()
    previous, after = _progress(document["before"]), _progress(document["after"])
    if (
        _pins(pins.build_id, pins.execution_id, pins.fence, previous) != pins
        or before is not None
        and previous != before
    ):
        raise worker._unavailable()
    if not (previous.phase == "complete" and previous == after) and not has_forward_progress(previous, after):
        raise worker._unavailable()
    phase_step = _PHASES.index(after.phase) - _PHASES.index(previous.phase)
    if phase_step > 1 or after.index_count - previous.index_count > 1:
        raise worker._unavailable()
    return ProjectionBatchReceipt(**{**document, "before": previous, "after": after})


def _receipt(raw, pins, **expected):
    try:
        document = json.loads(
            raw.decode("ascii"),
            object_pairs_hook=admission._unique_object,
            parse_constant=admission._reject_number,
            parse_float=admission._reject_number,
            parse_int=admission._integer,
        )
        return validate_receipt(document, pins, **expected)
    except UnicodeError, ValueError, TypeError, KeyError, RecursionError:
        failure = worker._unavailable()
    raise failure


@dataclass(frozen=True, slots=True, kw_only=True)
class QueryProjectionBatchTransport(worker.AdmissionBatchTransport):
    """Reuse the bounded TLS client with only the fixed projection purpose."""

    _projection: ClassVar[bool] = True

    async def send(self, request, pins, *, generation_id, capture_bundle_id, before):
        """Send once and bind the committed receipt to the caller's detached observation."""

        if (
            type(pins) is not authorization.ProjectionBatchPins
            or pins.execution_id != request.execution_id
            or pins.fence != request.fence
            or self.bind_request(request).authorization_expires_at != request.authorization_expires_at
        ):
            raise worker._unavailable()
        document = asdict(pins)
        authorization.batch_pins(document)
        body = json.dumps(document, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode(
            "ascii"
        )
        return _receipt(
            await self._exchange(request, body),
            pins,
            generation_id=generation_id,
            capture_bundle_id=capture_bundle_id,
            before=before,
        )


def _observe(session, request, build_id):
    build, _deadline = _read_authority(session, request, build_id)
    if build.phase not in {"output", "verifying", "verified"}:
        raise worker._unavailable()
    family, manifest = CustomImportSnapshotFamily, CustomImportQueryProjection
    parent_records = session.execute(
        select(family, manifest)
        .outerjoin(manifest, manifest.family_id == family.family_id)
        .where(
            family.execution_id == request.execution_id,
            family.producing_fence == request.fence,
            family.generation_id == build.generation_id,
        )
    ).one_or_none()
    if parent_records is None:
        raise worker._unavailable()
    parent, retained = parent_records
    if any(
        getattr(parent, name) != getattr(build, name)
        for name in (
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "capture_bundle_id",
            "generation_id",
        )
    ) or not hmac.compare_digest(bytes(parent.producing_token_sha256), bytes(build.producing_token_sha256)):
        raise worker._unavailable()
    progress = _observed_manifest(parent, retained, request.definition, build.phase)
    return RetainedProjectionProgress(
        request.execution_id, build_id, request.fence, build.generation_id, build.capture_bundle_id, progress
    )


def _observed_manifest(parent, retained, definition, build_phase):
    from process.custom_import.query_projection_build import ProjectionProgress

    if retained is None:
        if build_phase != "output":
            raise worker._unavailable()
        return ProjectionProgress("root", 0, None, None, 0, 0, 0)
    if any(
        getattr(retained, name) != getattr(parent, name)
        for name in (
            "generation_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "producing_fence",
        )
    ) or not hmac.compare_digest(bytes(retained.producing_token_sha256), bytes(parent.producing_token_sha256)):
        raise worker._unavailable()
    _manifest_shape(retained, require_complete=False)
    _definition_binding(retained, definition)
    return _progress(asdict(ProjectionProgress.from_manifest(retained)))


async def _retained_progress(session_factory, request, build_id):
    """Read only retained control rows; never initialize tables or invoke owner writers."""

    async with _read_session(session_factory) as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        return await session.run_sync(lambda sync: _observe(sync, request, build_id))


async def projection_next_batch(session_factory, request, build_id, transport):
    """Release local reads before POST, then reconcile uncertain delivery at most once."""

    request = transport.bind_request(request)
    before = await _retained_progress(session_factory, request, build_id)
    if before.progress.phase == "complete":
        from process.custom_import.query_projection_build import ProjectionBatchReceipt

        return ProjectionBatchReceipt(
            **{name: getattr(before, name) for name in _IDENTITY_NAMES}, before=before.progress, after=before.progress
        )
    pins = _pins(build_id, request.execution_id, request.fence, before.progress)
    try:
        return await transport.send(
            request,
            pins,
            generation_id=before.generation_id,
            capture_bundle_id=before.capture_bundle_id,
            before=before.progress,
        )
    except worker.AdmissionTransportError:
        retained = None
    try:
        observation = await _retained_progress(session_factory, request, build_id)
        if all(
            getattr(before, name) == getattr(observation, name) for name in _IDENTITY_NAMES
        ) and has_forward_progress(before.progress, observation.progress):
            retained = observation
    except CancellationRequested, LeaseAuthorityLost:
        raise
    except Exception:
        retained = None
    raise ProjectionReconciliationRequired(pins, retained)
