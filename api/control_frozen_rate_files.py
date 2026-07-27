# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control-boundary adapters for private frozen PTG rate-file sets."""

from __future__ import annotations

from typing import Any

from process.ptg_parts.frozen_rate_files import (
    normalize_frozen_rate_file_set,
)


def ptg_import_file_payload(request_payload: dict[str, Any]) -> dict[str, Any]:
    """Map the scalar route while refusing the private multipart envelope."""

    supplied_params = request_payload.get("params")
    if (
        "frozen_rate_files" in request_payload
        or "frozen_rate_file_set_sha256" in request_payload
        or (
            isinstance(supplied_params, dict)
            and (
                "frozen_rate_files" in supplied_params
                or "frozen_rate_file_set_sha256" in supplied_params
            )
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
    """Validate the private PTG multipart envelope before run persistence."""

    if str(request_payload.get("importer") or "") != "ptg":
        return request_payload
    raw_params = request_payload.get("params")
    params_by_name = dict(raw_params) if isinstance(raw_params, dict) else {}
    has_files = "frozen_rate_files" in params_by_name
    has_digest = "frozen_rate_file_set_sha256" in params_by_name
    if not has_files and not has_digest:
        return request_payload
    if not has_files or not has_digest:
        raise ValueError(
            "frozen_rate_files and frozen_rate_file_set_sha256 are required together"
        )
    normalized_files, set_digest = normalize_frozen_rate_file_set(
        params_by_name["frozen_rate_files"],
        params_by_name["frozen_rate_file_set_sha256"],
    )
    params_by_name["frozen_rate_files"] = normalized_files
    params_by_name["frozen_rate_file_set_sha256"] = set_digest
    return {**request_payload, "params": params_by_name}


__all__ = [
    "ptg_import_file_payload",
    "validated_control_import_payload",
]
