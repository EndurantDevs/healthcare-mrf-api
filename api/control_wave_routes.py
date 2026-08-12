"""Control routes and lifecycle hooks for exact PTGSmall waves."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, NotFound, SanicException

from api.control_auth import require_control_auth
from api.control_import_waves import (
    ImportWaveConflict,
    MAX_ATTESTATION_CANONICAL_BYTES,
    RECEIPT_ATTESTATION_VERSION,
    admit_import_wave,
    get_import_wave,
)
from api.control_import_wave_abandonment import (
    abandon_materialized_preclaim_wave,
    get_materialized_preclaim_abandonment,
)
from api.control_wave_linkage_route import control_record_import_wave_linkage
from process.ptg_parts.ptg_wave_admission_fence import PTGWaveCapacityConflict
from process.ptg_wave_controller import (
    start_ptg_wave_controller,
    stop_ptg_wave_controller,
)
from process.ptg_wave_outcomes import (
    PTGWaveOutcomeConflict,
    get_wave_outcomes_page,
)
from process.ptg_wave_ordinary_terminal_receipt import (
    ORDINARY_TERMINAL_REQUEST_SCHEMA,
    PTGWaveOrdinaryTerminalConflict,
    PTGWaveOrdinaryTerminalRetryable,
    issue_ordinary_terminal_receipt,
)
from process.ptg_wave_state import get_wave_receipts
from process.ptg_wave_receipt_authority import (
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
)
from process.ptg_wave_receipt_key_coverage import (
    assert_nonterminal_receipt_key_coverage,
)
from process.ptg_wave_receipt_process_authority import (
    load_process_receipt_keyring,
    require_process_receipt_keyring,
)
from process.ptg_wave_receipt_contract import (
    ABANDONMENT_REQUEST_SCHEMA,
    PTGWaveReceiptContractError,
)
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


async def control_initialize_ptg_wave_receipt_authority(app, _loop):
    """Pin and validate process receipt authority before accepting traffic."""

    receipt_keyring = load_process_receipt_keyring()
    if receipt_keyring is not None:
        await assert_nonterminal_receipt_key_coverage(keyring=receipt_keyring)
    app.ctx.ptg_wave_receipt_keyring = receipt_keyring


async def control_start_ptg_wave_controller(app, _loop):
    """Start controller or signer Redis resources after authority is pinned."""

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
        raw_attestation = (
            request.json.get("cohort_attestation")
            if isinstance(request.json, dict)
            else None
        )
        receipt_keyring = (
            _require_request_receipt_keyring(request)
            if isinstance(raw_attestation, dict)
            and raw_attestation.get("schema_version")
            == RECEIPT_ATTESTATION_VERSION
            else _request_receipt_keyring(request)
        )
        wave, created = await admit_import_wave(
            request.json,
            redis=getattr(
                getattr(getattr(request, "app", None), "ctx", None),
                "ptg_wave_redis",
                None,
            ),
            receipt_keyring=receipt_keyring,
        )
    except (
        ImportWaveConflict,
        PTGWaveCapacityConflict,
        PTGWavePreclaimSupersessionConflict,
        PTGWaveAdmissionRollbackConflict,
        PTGWaveMaterializedPreclaimConflict,
    ) as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except PTGWaveReceiptAuthorityError as exc:
        raise SanicException(str(exc), status_code=503) from exc
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


async def control_get_receipt_key_epochs(request):
    """Expose bounded public epochs for audit, never dynamic trust."""

    require_control_auth(request)
    try:
        keyring = _require_request_receipt_keyring(request)
        payload = keyring.public_epochs_mapping()
    except PTGWaveReceiptAuthorityError as exc:
        raise SanicException(str(exc), status_code=503) from exc
    return response.json(payload)


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


async def control_abandon_materialized_preclaim_wave(
    request,
    wave_id: str,
):
    """Quarantine one pristine materialized wave for ordinary admission."""

    require_control_auth(request)
    request_payload = request.json if isinstance(request.json, dict) else {}
    legacy_fields = {"cutover_id"}
    v12_fields = {
        "schema",
        "key_id",
        "operation_id",
        "cutover_id",
        "admission",
    }
    payload_fields = frozenset(request_payload)
    if payload_fields not in {
        frozenset(legacy_fields),
        frozenset(v12_fields),
    }:
        raise BadRequest(
            "request must contain only cutover_id or the exact V12 fields"
        )
    if payload_fields == frozenset(v12_fields) and (
        request_payload.get("schema") != ABANDONMENT_REQUEST_SCHEMA
    ):
        raise BadRequest("V12 abandonment request schema is unsupported")
    try:
        abandonment_result, created = await abandon_materialized_preclaim_wave(
            wave_id,
            request_payload
            if payload_fields == frozenset(v12_fields)
            else request_payload["cutover_id"],
            redis=getattr(request.app.ctx, "ptg_wave_redis", None),
            receipt_keyring=(
                _require_request_receipt_keyring(request)
                if payload_fields == frozenset(v12_fields)
                else _request_receipt_keyring(request)
            ),
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise SanicException(str(exc), status_code=503) from exc
    except (
        PTGWaveMaterializedPreclaimConflict,
        PTGWaveReceiptContractError,
    ) as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(abandonment_result, status=201 if created else 200, default=str)


async def control_get_materialized_preclaim_abandonment(
    request,
    wave_id: str,
):
    """Return one persisted legacy abandonment proof without mutation."""

    require_control_auth(request)
    if request.args:
        raise BadRequest("materialized abandonment query fields are not exact")
    try:
        proof = await get_materialized_preclaim_abandonment(wave_id)
    except PTGWaveMaterializedPreclaimConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if proof is None:
        raise NotFound("materialized abandonment proof not found")
    return response.json(proof, default=str)


async def control_issue_ordinary_terminal_receipt(
    request,
    wave_id: str,
):
    """Sign one later ordinary member result without an all-member gate."""

    require_control_auth(request)
    request_payload = request.json if isinstance(request.json, dict) else {}
    expected_fields = {
        "schema",
        "key_id",
        "operation_id",
        "member_ordinal",
        "source_file_import_id",
        "run_id",
    }
    if set(request_payload) != expected_fields:
        raise BadRequest(
            "request must contain only the exact ordinary terminal fields"
        )
    if request_payload.get("schema") != ORDINARY_TERMINAL_REQUEST_SCHEMA:
        raise BadRequest("ordinary terminal receipt request schema is unsupported")
    try:
        receipt, created = await issue_ordinary_terminal_receipt(
            wave_id,
            request_payload,
            receipt_keyring=_require_request_receipt_keyring(request),
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise SanicException(str(exc), status_code=503) from exc
    except PTGWaveOrdinaryTerminalRetryable as exc:
        raise SanicException(
            str(exc),
            status_code=503,
            headers={"Retry-After": "1"},
        ) from exc
    except PTGWaveOrdinaryTerminalConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    return response.json(
        receipt,
        status=201 if created else 200,
        default=str,
    )


def _single_query_argument(arguments, field: str):
    """Return one query value while rejecting repeated keys."""

    getlist = getattr(arguments, "getlist", None)
    if callable(getlist):
        values = getlist(field)
        if len(values) != 1:
            raise ValueError(f"{field} must occur exactly once")
        return values[0]
    return arguments.get(field)


def _request_receipt_keyring(request):
    """Return the immutable authority loaded for this server process."""

    return getattr(
        getattr(getattr(request, "app", None), "ctx", None),
        "ptg_wave_receipt_keyring",
        None,
    )


def _require_request_receipt_keyring(request):
    """Require the immutable signer pinned for this process."""

    return require_process_receipt_keyring(
        _request_receipt_keyring(request)
    )


def register_control_wave_routes(blueprint):
    """Register exact-wave endpoints and controller lifecycle hooks."""

    blueprint.listener("before_server_start")(
        control_initialize_ptg_wave_receipt_authority
    )
    blueprint.listener("after_server_start")(control_start_ptg_wave_controller)
    blueprint.listener("before_server_stop")(control_stop_ptg_wave_controller)
    blueprint.post("/import-waves")(control_admit_import_wave)
    blueprint.get("/import-waves/<wave_id>")(control_get_import_wave)
    blueprint.get("/import-wave-receipt-key-epochs")(
        control_get_receipt_key_epochs
    )
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
    blueprint.post(
        "/import-waves/<wave_id>/materialized-preclaim-abandonment"
    )(control_abandon_materialized_preclaim_wave)
    blueprint.get(
        "/import-waves/<wave_id>/materialized-preclaim-abandonment"
    )(control_get_materialized_preclaim_abandonment)
    blueprint.post(
        "/import-waves/<wave_id>/ordinary-terminal-receipts"
    )(control_issue_ordinary_terminal_receipt)
    return blueprint


__all__ = ["register_control_wave_routes"]
