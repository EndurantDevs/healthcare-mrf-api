"""Control routes and lifecycle hooks for exact PTGSmall waves."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, NotFound, SanicException

from api.control_auth import require_control_auth
from api.control_import_waves import (
    ImportWaveConflict,
    MAX_ATTESTATION_CANONICAL_BYTES,
    admit_import_wave,
    get_import_wave,
)
from process.ptg_parts.ptg_wave_admission_fence import PTGWaveCapacityConflict
from process.ptg_wave_controller import (
    start_ptg_wave_controller,
    stop_ptg_wave_controller,
)
from process.ptg_wave_outcomes import (
    PTGWaveOutcomeConflict,
    get_wave_outcomes_page,
    record_linkage_ack,
)
from process.ptg_wave_state import get_wave_receipts
from process.ptg_wave_preclaim_supersession import (
    PTGWavePreclaimSupersessionConflict,
)
from process.ptg_wave_preclaim_supersession_runtime import (
    get_logical_preclaim_supersession_candidate,
)
from process.ptg_wave_admission_rollback_supersession import (
    PTGWaveAdmissionRollbackConflict,
    validate_admission_rollback_predecessor,
    validate_admission_rollback_successor,
)
from process.ptg_wave_admission_rollback_supersession_runtime import (
    get_admission_rollback_supersession_candidate,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_materialized_preclaim_supersession_runtime import (
    get_materialized_preclaim_supersession_candidate,
)


async def control_start_ptg_wave_controller(app, _loop):
    """Start the fail-closed reconciler only when explicitly enabled."""

    await start_ptg_wave_controller(app)


async def control_stop_ptg_wave_controller(app, _loop):
    """Stop the exact-wave reconciler before the control server exits."""

    await stop_ptg_wave_controller(app)


async def control_admit_import_wave(request):
    """Record a signed exact-wave admission without publishing it."""

    require_control_auth(request)
    raw_content_length = request.headers.get("content-length")
    try:
        declared_content_length = (
            int(raw_content_length) if raw_content_length is not None else None
        )
    except (TypeError, ValueError) as exc:
        raise BadRequest("Content-Length is invalid") from exc
    if (
        declared_content_length is not None
        and declared_content_length > MAX_ATTESTATION_CANONICAL_BYTES
    ) or len(request.body or b"") > MAX_ATTESTATION_CANONICAL_BYTES:
        raise SanicException(
            "import wave request exceeds its byte limit",
            status_code=413,
        )
    try:
        wave, created = await admit_import_wave(
            request.json,
            redis=getattr(
                getattr(getattr(request, "app", None), "ctx", None),
                "ptg_wave_redis",
                None,
            ),
        )
    except (
        ImportWaveConflict,
        PTGWaveCapacityConflict,
        PTGWavePreclaimSupersessionConflict,
        PTGWaveAdmissionRollbackConflict,
        PTGWaveMaterializedPreclaimConflict,
    ) as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(wave, status=201 if created else 200, default=str)


async def control_get_import_wave(request, wave_id: str):
    """Return one durable wave without advancing reconciliation."""

    require_control_auth(request)
    try:
        wave = await get_import_wave(wave_id)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if wave is None:
        raise NotFound("import wave not found")
    return response.json(wave, default=str)


async def control_get_import_wave_outcomes(request, wave_id: str):
    """Return one immutable all-N outcome page for GET-only recovery."""

    require_control_auth(request)
    raw_after = request.args.get("after_ordinal")
    raw_limit = request.args.get("limit")
    try:
        after_ordinal = int(raw_after) if raw_after is not None else None
        limit = int(raw_limit) if raw_limit is not None else 200
        payload = await get_wave_outcomes_page(
            wave_id,
            after_ordinal=after_ordinal,
            limit=limit,
        )
    except (TypeError, ValueError, PTGWaveOutcomeConflict) as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(payload, default=str)


async def control_record_import_wave_linkage(request, wave_id: str):
    """Persist one signed all-N source-linkage acknowledgement."""

    require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    if set(payload) != {"linkage_ack"}:
        raise BadRequest("request must contain only linkage_ack")
    try:
        digest = await record_linkage_ack(wave_id, payload["linkage_ack"])
    except PTGWaveOutcomeConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    return response.json({"wave_id": wave_id, "linkage_ack_digest": digest})


async def control_get_import_wave_proof(request, wave_id: str):
    """Return durable controller receipts without reconciling the wave."""

    require_control_auth(request)
    proof = await get_wave_receipts(wave_id)
    if proof is None:
        raise NotFound("import wave not found")
    return response.json(proof, default=str)


async def control_get_logical_preclaim_supersession(
    request,
    wave_id: str,
):
    """Observe one exact GET-only candidate for a fresh successor admission."""

    require_control_auth(request)
    successor_wave_id = request.args.get("successor_wave_id")
    if not isinstance(successor_wave_id, str) or not successor_wave_id:
        raise BadRequest("successor_wave_id is required")
    try:
        proof = await get_logical_preclaim_supersession_candidate(
            wave_id,
            successor_wave_id,
            redis=getattr(request.app.ctx, "ptg_wave_redis", None),
        )
    except PTGWavePreclaimSupersessionConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    return response.json(proof, default=str)


async def control_get_admission_rollback_supersession(
    request,
    wave_id: str,
):
    """Observe one GET-only absence candidate for a fresh successor."""

    require_control_auth(request)
    expected_fields = {
        "successor_wave_id",
        "idempotency_key",
        "request_digest",
        "wave_digest",
        "release_queue",
        "intent_count",
    }
    if set(request.args) != expected_fields:
        raise BadRequest("admission rollback query fields are not exact")
    try:
        query_by_field = {
            field: _single_query_argument(request.args, field)
            for field in expected_fields
        }
        intent_count_text = query_by_field["intent_count"]
        if (
            type(intent_count_text) is not str
            or not intent_count_text.isascii()
            or not intent_count_text.isdecimal()
            or (len(intent_count_text) > 1 and intent_count_text[0] == "0")
        ):
            raise ValueError("intent_count must be canonical decimal text")
        descriptor = validate_admission_rollback_predecessor({
            "wave_id": wave_id,
            "idempotency_key": query_by_field["idempotency_key"],
            "request_digest": query_by_field["request_digest"],
            "wave_digest": query_by_field["wave_digest"],
            "release_queue": query_by_field["release_queue"],
            "intent_count": int(intent_count_text),
        })
        successor_wave_id = validate_admission_rollback_successor(
            descriptor["wave_id"],
            query_by_field["successor_wave_id"],
        )
    except (TypeError, ValueError, PTGWaveAdmissionRollbackConflict) as exc:
        raise BadRequest(str(exc)) from exc
    try:
        proof = await get_admission_rollback_supersession_candidate(
            descriptor,
            successor_wave_id,
            redis=getattr(request.app.ctx, "ptg_wave_redis", None),
        )
    except PTGWaveAdmissionRollbackConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    return response.json(proof, default=str)


async def control_get_materialized_preclaim_supersession(
    request,
    wave_id: str,
):
    """Observe a successor-bound failed-Job candidate without mutation."""

    require_control_auth(request)
    if set(request.args) != {"successor_wave_id"}:
        raise BadRequest("materialized preclaim query fields are not exact")
    try:
        successor_wave_id = validate_admission_rollback_successor(
            wave_id,
            _single_query_argument(request.args, "successor_wave_id"),
        )
    except (TypeError, ValueError, PTGWaveAdmissionRollbackConflict) as exc:
        raise BadRequest(str(exc)) from exc
    try:
        proof = await get_materialized_preclaim_supersession_candidate(
            wave_id,
            successor_wave_id,
            redis=getattr(request.app.ctx, "ptg_wave_redis", None),
        )
    except PTGWaveMaterializedPreclaimConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    return response.json(proof, default=str)


def _single_query_argument(arguments, field: str):
    """Return one query value while rejecting repeated keys."""

    getlist = getattr(arguments, "getlist", None)
    if callable(getlist):
        values = getlist(field)
        if len(values) != 1:
            raise ValueError(f"{field} must occur exactly once")
        return values[0]
    return arguments.get(field)


def register_control_wave_routes(blueprint):
    """Register exact-wave endpoints and controller lifecycle hooks."""

    blueprint.listener("after_server_start")(control_start_ptg_wave_controller)
    blueprint.listener("before_server_stop")(control_stop_ptg_wave_controller)
    blueprint.post("/import-waves")(control_admit_import_wave)
    blueprint.get("/import-waves/<wave_id>")(control_get_import_wave)
    blueprint.get("/import-waves/<wave_id>/outcomes")(
        control_get_import_wave_outcomes
    )
    blueprint.post("/import-waves/<wave_id>/linkage-ack")(
        control_record_import_wave_linkage
    )
    blueprint.get("/import-waves/<wave_id>/proof")(
        control_get_import_wave_proof
    )
    blueprint.get(
        "/import-waves/<wave_id>/logical-preclaim-supersession"
    )(control_get_logical_preclaim_supersession)
    blueprint.get(
        "/import-waves/<wave_id>/admission-rollback-supersession"
    )(control_get_admission_rollback_supersession)
    blueprint.get(
        "/import-waves/<wave_id>/materialized-preclaim-supersession"
    )(control_get_materialized_preclaim_supersession)
    return blueprint


__all__ = ["register_control_wave_routes"]
