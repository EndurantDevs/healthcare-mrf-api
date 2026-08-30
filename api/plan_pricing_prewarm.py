# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prewarm API Layer from immutable supply-side provider-set density."""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import aiohttp
from api.plan_pricing_projection_contract import (
    HEX_DIGEST,
    LEGACY_PROJECTION_CONTRACT,
    PROJECTION_CONTRACT,
    canonical_json,
)
from api.plan_pricing_prewarm_selection import (
    MAX_PREWARM_SHAPES,
    PrewarmShape,
    is_broad_em_shape as _is_broad_em_shape,
    select_shapes as _select_shapes,
)
from api.plan_pricing_prewarm_http import (
    PREWARM_API_BASE_URL_ENV,
    PREWARM_API_TOKEN_ENV,
    PREWARM_PATH,
    PrewarmHttpConfig,
    prewarm_http_config,
)
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    normalize_plan_release_id,
    resolve_plan_release_serving,
)
from db.connection import db


PREWARM_CONTRACT = "plan_pricing_prewarm_v1"
PREWARM_CONCURRENCY = 8
_SERVING_REVISION_ID = re.compile(r"^hpserve_[0-9A-HJKMNP-TV-Z]{26}$")
_SERVING_REVISION_PUBLISHED_AT = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:"
    r"[0-9]{2}\.[0-9]{6}Z$"
)
@dataclass(frozen=True)
class _PrewarmResult:
    error: dict[str, str] | None = None
    cache_key_digest: str | None = None
    payload_bytes: int = 0


def _validate_identifiers(
    plan_release_id: Any,
    serving_revision_id: Any,
    projection_id: Any,
) -> tuple[str, str, str]:
    release_id = str(plan_release_id or "")
    revision_id = str(serving_revision_id or "")
    candidate_id = str(projection_id or "")
    if normalize_plan_release_id(release_id) != release_id:
        raise ValueError("plan-pricing prewarm plan_release_id is invalid")
    if not _SERVING_REVISION_ID.fullmatch(revision_id):
        raise ValueError("plan-pricing prewarm serving_revision_id is invalid")
    if not HEX_DIGEST.fullmatch(candidate_id):
        raise ValueError("plan-pricing prewarm projection_id is invalid")
    return release_id, revision_id, candidate_id


async def _exact_ready_selection(
    session: Any,
    *,
    plan_release_id: str,
    serving_revision_id: str,
    projection_id: str,
) -> PlanReleaseServingSelection:
    selection = await resolve_plan_release_serving(
        session,
        plan_release_id,
        projection_only=True,
    )
    if (
        selection is None
        or selection.plan_release_id != plan_release_id
        or selection.serving_revision_id != serving_revision_id
        or selection.pricing_projection_id != projection_id
        or not _SERVING_REVISION_PUBLISHED_AT.fullmatch(
            str(
                getattr(selection, "serving_revision_published_at", None)
                or ""
            )
        )
    ):
        raise ValueError(
            "plan-pricing prewarm requires the exact current ready projection"
        )
    return selection


def _shape_error(shape: PrewarmShape, error: str) -> dict[str, str]:
    return {
        "code_system": shape.code_system,
        "code": shape.code,
        "zip5": shape.geo_cell,
        "error": error,
    }


def _failed_result(shape: PrewarmShape, error: str) -> _PrewarmResult:
    return _PrewarmResult(error=_shape_error(shape, error))


def _shared_cache_result(
    selection: PlanReleaseServingSelection,
    shape: PrewarmShape,
    response_payload_by_field: Any,
) -> _PrewarmResult:
    if not isinstance(response_payload_by_field, Mapping) or not isinstance(
        response_payload_by_field.get("data"), Mapping
    ):
        return _failed_result(shape, "invalid_response")
    response_data_by_field = response_payload_by_field["data"]
    if (
        response_data_by_field.get("plan_release_id")
        != selection.plan_release_id
        or response_data_by_field.get("serving_revision_id")
        != selection.serving_revision_id
    ):
        return _failed_result(shape, "release_identity_mismatch")
    if response_data_by_field.get("stored_shared") is not True:
        return _failed_result(shape, "shared_cache_not_stored")
    cache_key_digest = response_data_by_field.get("cache_key_digest")
    payload_bytes = response_data_by_field.get("payload_bytes")
    if (
        not isinstance(cache_key_digest, str)
        or not HEX_DIGEST.fullmatch(cache_key_digest)
        or not isinstance(payload_bytes, int)
        or isinstance(payload_bytes, bool)
        or payload_bytes <= 0
    ):
        return _failed_result(shape, "invalid_cache_receipt")
    return _PrewarmResult(
        cache_key_digest=cache_key_digest,
        payload_bytes=payload_bytes,
    )


async def _http_response_result(
    response: Any,
    selection: PlanReleaseServingSelection,
    shape: PrewarmShape,
) -> _PrewarmResult:
    if response.status == 409:
        try:
            conflict_by_field = await response.json(content_type=None)
        except (TypeError, ValueError, UnicodeDecodeError):
            conflict_by_field = {}
        conflict_error = (
            "prewarm_capacity_exceeded"
            if isinstance(conflict_by_field, Mapping)
            and conflict_by_field.get("detail")
            == "plan_pricing_prewarm_capacity_exceeded"
            else "release_identity_mismatch"
        )
        return _failed_result(shape, conflict_error)
    if response.status != 200:
        return _failed_result(shape, f"http_status_{response.status}")
    try:
        response_payload_by_field = await response.json(content_type=None)
    except (TypeError, ValueError, UnicodeDecodeError):
        return _failed_result(shape, "invalid_json")
    return _shared_cache_result(selection, shape, response_payload_by_field)


async def _prewarm_one(
    http_session: Any,
    semaphore: asyncio.Semaphore,
    config: PrewarmHttpConfig,
    selection: PlanReleaseServingSelection,
    shape: PrewarmShape,
) -> _PrewarmResult:
    query_by_field = {
        "healthporta_plan_id": selection.healthporta_plan_id,
        "code_system": shape.code_system,
        "code": shape.code,
        "zip5": shape.geo_cell,
        "zip_radius_miles": 25,
        "limit": 3,
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
    }
    try:
        async with semaphore:
            async with http_session.get(
                f"{config.base_url}{PREWARM_PATH}",
                params=query_by_field,
                headers=config.headers,
                ssl=config.verify_tls,
                allow_redirects=False,
            ) as response:
                return await _http_response_result(
                    response, selection, shape
                )
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
        return _failed_result(shape, "transport_error")


def _selected_shape_digest(shapes: Sequence[PrewarmShape]) -> str:
    shape_rows = [
        [shape.code_system, shape.code, shape.geo_cell, shape.provider_count]
        for shape in shapes
    ]
    return hashlib.sha256(
        canonical_json(shape_rows).encode("utf-8")
    ).hexdigest()


def _cache_key_set_digest(stored_results: Sequence[_PrewarmResult]) -> str:
    cache_key_digests = sorted(
        stored_result.cache_key_digest
        for stored_result in stored_results
        if stored_result.cache_key_digest is not None
    )
    return hashlib.sha256(
        canonical_json(cache_key_digests).encode("utf-8")
    ).hexdigest()


def _ordered_error_rows(
    errors: Sequence[Mapping[str, str]],
) -> list[dict[str, str]]:
    return sorted(
        (dict(error_by_field) for error_by_field in errors),
        key=lambda error_by_field: (
            error_by_field.get("code_system", ""),
            error_by_field.get("code", ""),
            error_by_field.get("zip5", ""),
            error_by_field.get("error", ""),
        ),
    )


def _receipt(
    selection: PlanReleaseServingSelection,
    shapes: Sequence[PrewarmShape],
    excluded_e_and_m_count: int,
    errors: Sequence[Mapping[str, str]],
    stored_results: Sequence[_PrewarmResult],
) -> dict[str, Any]:
    ordered_errors = _ordered_error_rows(errors)
    selected_count = len(shapes)
    failed_count = sum(
        1 for error_by_field in ordered_errors if error_by_field.get("code_system")
    )
    warmed_count = len(stored_results)
    receipt_by_field: dict[str, Any] = {
        "contract": PREWARM_CONTRACT,
        "status": "partial" if ordered_errors else "complete",
        "ranking_basis": "provider_set_member_density",
        "ranking_semantics": "supply_not_enrollee_or_request_demand",
        "per_release_shape_cap": MAX_PREWARM_SHAPES,
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
        "projection_id": selection.pricing_projection_id,
        "selected_shape_count": selected_count,
        "attempted_shape_count": selected_count,
        "warmed_shape_count": warmed_count,
        "stored_shared_count": warmed_count,
        "stored_payload_bytes": sum(
            stored_result.payload_bytes for stored_result in stored_results
        ),
        "cache_key_set_digest": _cache_key_set_digest(stored_results),
        "failed_shape_count": failed_count,
        "excluded_e_and_m_count": excluded_e_and_m_count,
        "error_count": len(ordered_errors),
        "selected_shape_digest": _selected_shape_digest(shapes),
        "errors": ordered_errors,
    }
    receipt_by_field["receipt_digest"] = hashlib.sha256(
        canonical_json(receipt_by_field).encode("utf-8")
    ).hexdigest()
    receipt_by_field["terminal_progress"] = {
        "unit": "shapes",
        "done": selected_count,
        "total": selected_count,
        "pct": 100,
        "message": receipt_by_field["status"],
        "phase": "plan-pricing prewarm completed",
    }
    return receipt_by_field


def _append_global_error(
    receipt_by_field: Mapping[str, Any],
    error: str,
) -> dict[str, Any]:
    amended_receipt_by_field = {
        key: value
        for key, value in receipt_by_field.items()
        if key not in {"receipt_digest", "terminal_progress"}
    }
    amended_receipt_by_field["status"] = "partial"
    amended_receipt_by_field["errors"] = _ordered_error_rows(
        [*amended_receipt_by_field["errors"], {"error": error}]
    )
    amended_receipt_by_field["error_count"] = len(
        amended_receipt_by_field["errors"]
    )
    amended_receipt_by_field["receipt_digest"] = hashlib.sha256(
        canonical_json(amended_receipt_by_field).encode("utf-8")
    ).hexdigest()
    amended_receipt_by_field["terminal_progress"] = {
        **receipt_by_field["terminal_progress"],
        "message": "partial",
    }
    return amended_receipt_by_field


async def _prewarm_shapes(
    http_session: Any,
    config: PrewarmHttpConfig,
    selection: PlanReleaseServingSelection,
    shapes: Sequence[PrewarmShape],
) -> dict[str, Any]:
    requested_shapes = tuple(shape for shape in shapes if not _is_broad_em_shape(shape))
    excluded_e_and_m_count = len(shapes) - len(requested_shapes)
    semaphore = asyncio.Semaphore(PREWARM_CONCURRENCY)
    results = await asyncio.gather(
        *(
            _prewarm_one(
                http_session,
                semaphore,
                config,
                selection,
                shape,
            )
            for shape in requested_shapes
        )
    )
    return _receipt(
        selection,
        requested_shapes,
        excluded_e_and_m_count,
        tuple(result.error for result in results if result.error is not None),
        tuple(result for result in results if result.error is None),
    )


async def prewarm_plan_pricing(
    *,
    plan_release_id: str,
    serving_revision_id: str,
    projection_id: str,
) -> dict[str, Any]:
    """Warm the exact current release and return a replay-stable receipt."""

    release_id, revision_id, candidate_id = _validate_identifiers(
        plan_release_id,
        serving_revision_id,
        projection_id,
    )
    config = prewarm_http_config()
    async with db.transaction() as session:
        selection = await _exact_ready_selection(
            session,
            plan_release_id=release_id,
            serving_revision_id=revision_id,
            projection_id=candidate_id,
        )
        shapes = await _select_shapes(
            session,
            candidate_id,
            selection.pricing_projection_contract
            or LEGACY_PROJECTION_CONTRACT,
        )
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as http_session:
        receipt_by_field = await _prewarm_shapes(
            http_session,
            config,
            selection,
            shapes,
        )
    try:
        async with db.transaction() as session:
            await _exact_ready_selection(
                session,
                plan_release_id=release_id,
                serving_revision_id=revision_id,
                projection_id=candidate_id,
            )
    except ValueError:
        return _append_global_error(
            receipt_by_field,
            "release_identity_changed",
        )
    return receipt_by_field
