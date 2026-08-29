# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control-boundary adapters for private frozen PTG rate-file sets."""

from __future__ import annotations

from typing import Any

from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_PROTECTED_FIELDS,
    protected_frozen_field_presence,
)
from process.ptg_frozen_control import normalize_protected_rate_params
from process.ptg_singleton_direct_control import protected_singleton_direct_presence


def ptg_import_file_payload(request_payload: dict[str, Any]) -> dict[str, Any]:
    """Map the scalar route while refusing the private multipart envelope."""

    supplied_params = request_payload.get("params")
    if any(
        field_name in request_payload
        for field_name in FROZEN_RATE_FILE_PROTECTED_FIELDS
    ) or (
        isinstance(supplied_params, dict)
        and any(
            field_name in supplied_params
            for field_name in FROZEN_RATE_FILE_PROTECTED_FIELDS
        )
    ):
        raise ValueError(
            "multipart frozen rate files use the internal import engine payload"
        )
    params_by_name = dict(request_payload.get("params") or {})
    for key in (
        "source_key",
        "source_file_id",
        "source_file_import_id",
        "in_network_url",
        "content_version",
        "import_month",
        "plan_ids",
        "plan_market_types",
        "max_files",
        "test_mode",
    ):
        if key in request_payload and key not in params_by_name:
            params_by_name[key] = request_payload[key]
    return {
        "run_id": request_payload.get("run_id"),
        "importer": "ptg",
        "params": params_by_name,
        "idempotency_key": request_payload.get("idempotency_key"),
        "triggered_by": (
            request_payload.get("triggered_by") or "source_file_import"
        ),
        "schedule_id": request_payload.get("schedule_id"),
        "subscription_id": request_payload.get("subscription_id"),
        "source_file_import_id": (
            request_payload.get("source_file_import_id")
            or params_by_name.get("source_file_import_id")
        ),
    }


def validated_control_import_payload(
    request_payload: dict[str, Any],
) -> dict[str, Any]:
    """Validate one private PTG source envelope before run persistence."""

    if str(request_payload.get("importer") or "") != "ptg":
        return request_payload
    raw_params = request_payload.get("params")
    params_by_name = dict(raw_params) if isinstance(raw_params, dict) else {}
    supplied_fields = protected_frozen_field_presence(params_by_name)
    if not supplied_fields and not protected_singleton_direct_presence(
        params_by_name
    ):
        return request_payload
    params_by_name = normalize_protected_rate_params(params_by_name)
    nested_source_file_import_id = params_by_name["source_file_import_id"]
    outer_ids = (
        request_payload.get("source_file_import_id"),
        request_payload.get("import_id"),
    )
    if any(
        not isinstance(raw_id, str)
        or raw_id.strip() != nested_source_file_import_id
        for raw_id in outer_ids
    ):
        raise ValueError(
            "protected outer and nested source_file_import_id and import_id "
            "must all match"
        )
    return {
        **request_payload,
        "source_file_import_id": nested_source_file_import_id,
        "import_id": nested_source_file_import_id,
        "params": params_by_name,
    }


__all__ = [
    "ptg_import_file_payload",
    "validated_control_import_payload",
]
