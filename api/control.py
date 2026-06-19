# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os
import hmac

from sanic import Blueprint, response
from sanic.exceptions import BadRequest, Forbidden, NotFound, SanicException

from api.control_imports import (
    create_import_run,
    ensure_import_run_table,
    finalize_import_run,
    get_import_run,
    importer_registry,
    list_import_runs_page,
    node_health,
    parse_ptg_toc_preview,
    request_cancel,
    retry_import_run,
)
from api.control_workers import ensure_worker, worker_registry
from process.ptg_parts.source_snapshot_control import (
    SourceSnapshotConflict,
    build_ptg2_source_snapshot_remove_plan,
    promote_ptg2_source_snapshot,
    remove_ptg2_source_snapshot,
)

blueprint = Blueprint("control", url_prefix="/control/v1")


@blueprint.listener("before_server_start")
async def control_ensure_import_run_table(_app, _loop):
    # Safety net only: mrf.import_run is created by the alembic migration
    # 20260610120000_add_import_run_table; this idempotent ensure keeps fresh
    # nodes working before migrations have run, without per-request DDL.
    await ensure_import_run_table()


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
    return response.json(await node_health(), default=str)


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
        preview = await asyncio.to_thread(parse_ptg_toc_preview, payload)
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


@blueprint.post("/ptg/source-snapshots/promote")
async def control_ptg_source_snapshot_promote(request):
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        result = await promote_ptg2_source_snapshot(
            source_key=str(payload.get("source_key") or ""),
            snapshot_id=str(payload.get("snapshot_id") or ""),
            expected_current_snapshot_id=(
                str(payload.get("expected_current_snapshot_id"))
                if payload.get("expected_current_snapshot_id") is not None
                else None
            ),
        )
    except SourceSnapshotConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(result, default=str)


@blueprint.post("/ptg/source-snapshots/remove-plan")
async def control_ptg_source_snapshot_remove_plan(request):
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        plan = await build_ptg2_source_snapshot_remove_plan(
            snapshot_id=str(payload.get("snapshot_id") or ""),
            source_key=str(payload.get("source_key") or "") or None,
        )
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(plan, default=str)


@blueprint.post("/ptg/source-snapshots/remove")
async def control_ptg_source_snapshot_remove(request):
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        result = await remove_ptg2_source_snapshot(
            snapshot_id=str(payload.get("snapshot_id") or ""),
            source_key=str(payload.get("source_key") or "") or None,
        )
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(result, default=str)


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
    try:
        page = await list_import_runs_page(
            status=args.get("status"),
            importer=args.get("importer"),
            limit=int(args.get("limit") or 50),
            cursor=args.get("cursor"),
        )
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(page, default=str)


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
