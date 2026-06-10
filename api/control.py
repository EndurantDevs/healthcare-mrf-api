# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import hmac

from sanic import Blueprint, response
from sanic.exceptions import BadRequest, Forbidden, NotFound, SanicException

from api.control_imports import (
    create_import_run,
    finalize_import_run,
    get_import_run,
    importer_registry,
    list_import_runs,
    node_health,
    parse_ptg_toc_preview,
    request_cancel,
    retry_import_run,
)
from api.control_workers import ensure_worker, worker_registry

blueprint = Blueprint("control", url_prefix="/control/v1")


@blueprint.exception(SanicException)
async def control_error(request, exc: SanicException):
    return response.json(_error_payload(request, exc), status=getattr(exc, "status_code", 500))


def _require_control_auth(request) -> None:
    expected = str(os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    if not expected:
        raise Forbidden("control API token is required")
    headers = getattr(request, "headers", {}) or {}
    auth_header = str(headers.get("Authorization", ""))
    bearer = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
    explicit = str(headers.get("X-HealthPorta-Control-Token", "")).strip()
    if not (
        hmac.compare_digest(bearer, expected)
        or hmac.compare_digest(explicit, expected)
    ):
        raise Forbidden("control API token is invalid")


@blueprint.get("/importers")
async def control_importers(request):
    _require_control_auth(request)
    return response.json({"items": importer_registry(), "next_cursor": None})


@blueprint.get("/health/node")
async def control_node_health(request):
    _require_control_auth(request)
    return response.json(node_health(), default=str)


@blueprint.get("/workers")
async def control_workers(request):
    _require_control_auth(request)
    return response.json({"items": worker_registry(), "next_cursor": None}, default=str)


@blueprint.post("/workers/ensure")
async def control_ensure_worker(request):
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    return response.json(ensure_worker(payload), status=202, default=str)


@blueprint.post("/ptg/parse-toc-preview")
async def control_ptg_parse_toc_preview(request):
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        preview = parse_ptg_toc_preview(payload)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(preview, default=str)


@blueprint.post("/ptg/import-file")
async def control_ptg_import_file(request):
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    run_payload = _ptg_import_file_payload(payload)
    try:
        run, created = await create_import_run(run_payload)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(run, status=201 if created else 409, default=str)


@blueprint.post("/imports")
async def control_create_import(request):
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        run, created = await create_import_run(payload)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    status = 201 if created else 409
    return response.json(run, status=status, default=str)


@blueprint.get("/imports")
async def control_list_imports(request):
    _require_control_auth(request)
    args = request.args
    runs = await list_import_runs(
        status=args.get("status"),
        importer=args.get("importer"),
        limit=int(args.get("limit") or 50),
    )
    return response.json({"items": runs, "next_cursor": None}, default=str)


@blueprint.get("/imports/<run_id>")
async def control_get_import(request, run_id: str):
    _require_control_auth(request)
    run = await get_import_run(run_id)
    if not run:
        raise NotFound("import run not found")
    return response.json(run, default=str)


@blueprint.post("/imports/<run_id>/cancel")
async def control_cancel_import(request, run_id: str):
    _require_control_auth(request)
    try:
        run = await request_cancel(run_id)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if not run:
        raise NotFound("import run not found")
    return response.json(run, status=202, default=str)


@blueprint.post("/imports/<run_id>/retry")
async def control_retry_import(request, run_id: str):
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    result = await retry_import_run(run_id, payload)
    if result is None:
        raise NotFound("import run not found")
    run, created = result
    return response.json(run, status=201 if created else 409, default=str)


@blueprint.post("/imports/<run_id>/finalize")
async def control_finalize_import(request, run_id: str):
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        run = await finalize_import_run(run_id, payload)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if run is None:
        raise NotFound("import run not found")
    return response.json(run, status=202, default=str)


def _ptg_import_file_payload(payload: dict) -> dict:
    params = dict(payload.get("params") or {})
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
        if key in payload and key not in params:
            params[key] = payload[key]
    return {
        "run_id": payload.get("run_id"),
        "importer": "ptg",
        "params": params,
        "idempotency_key": payload.get("idempotency_key"),
        "triggered_by": payload.get("triggered_by") or "source_file_import",
        "schedule_id": payload.get("schedule_id"),
        "subscription_id": payload.get("subscription_id"),
        "source_file_import_id": payload.get("source_file_import_id") or params.get("source_file_import_id"),
    }


def _error_payload(request, exc: SanicException) -> dict:
    status_code = int(getattr(exc, "status_code", 500) or 500)
    return {
        "error": {
            "code": _error_code(status_code),
            "message": str(exc),
            "detail": {},
            "request_id": _request_id(request),
        }
    }


def _error_code(status_code: int) -> str:
    return {
        400: "invalid_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        409: "conflict",
    }.get(status_code, "internal")


def _request_id(request) -> str:
    header = str(request.headers.get("X-Request-ID", "") or request.headers.get("X-Request-Id", "")).strip()
    if header:
        return header
    return str(getattr(request, "id", "") or "")
