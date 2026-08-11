"""Control route for one immutable PTG wave linkage acknowledgement."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.control_auth import require_control_auth
from process.ptg_wave_outcomes import PTGWaveOutcomeConflict, record_linkage_ack
from process.ptg_wave_receipt_authority import (
    PTGWaveReceiptAuthorityError,
)
from process.ptg_wave_receipt_process_authority import (
    require_process_receipt_keyring,
)


def _request_receipt_keyring(request):
    """Return the immutable receipt authority pinned for this process."""

    return getattr(
        getattr(getattr(request, "app", None), "ctx", None),
        "ptg_wave_receipt_keyring",
        None,
    )


async def control_record_import_wave_linkage(request, wave_id: str):
    """Persist one signed all-N source-linkage acknowledgement."""

    require_control_auth(request)
    request_by_field = request.json if isinstance(request.json, dict) else {}
    legacy_fields = {"linkage_ack"}
    receipt_fields = {"linkage_ack", "cutover_id", "key_id"}
    request_fields = frozenset(request_by_field)
    if request_fields not in {frozenset(legacy_fields), frozenset(receipt_fields)}:
        raise BadRequest(
            "request must contain only linkage_ack or the exact V12 linkage fields"
        )
    try:
        linkage_result = await record_linkage_ack(
            wave_id,
            request_by_field["linkage_ack"],
            cutover_id=request_by_field.get("cutover_id"),
            receipt_key_id=request_by_field.get("key_id"),
            receipt_keyring=(
                require_process_receipt_keyring(_request_receipt_keyring(request))
                if request_fields == frozenset(receipt_fields)
                else _request_receipt_keyring(request)
            ),
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise SanicException(str(exc), status_code=503) from exc
    except PTGWaveOutcomeConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    if isinstance(linkage_result, dict):
        return response.json(linkage_result)
    return response.json(
        {"wave_id": wave_id, "linkage_ack_digest": linkage_result}
    )


__all__ = ["control_record_import_wave_linkage"]
