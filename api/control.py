# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio

from sanic import Blueprint, response
from sanic.exceptions import BadRequest, NotFound, SanicException

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
from api.control_auth import require_control_auth as _require_control_auth
from api.provider_directory_sources import provider_directory_source_catalog
from api.mrf_discovery_catalog import (
    DEFAULT_FILE_PAGE_SIZE,
    DEFAULT_SOURCE_PAGE_SIZE,
    MAX_FILE_PAGE_SIZE,
    MAX_SOURCE_PAGE_SIZE,
    list_discovery_source_files_page,
    list_discovery_sources_page,
    page_limit,
)
from process.ptg_parts.ptg2_candidate_attestation import (
    record_candidate_audit_attestation,
)
from process.ptg_parts.source_snapshot_control import (
    SourceSnapshotConflict,
    build_ptg2_source_snapshot_remove_plan,
    promote_ptg2_source_snapshot,
    remove_ptg2_source_snapshot,
    retire_ptg2_source_snapshot,
)

blueprint = Blueprint("control", url_prefix="/control/v1")


@blueprint.listener("before_server_start")
async def control_ensure_import_run_table(_app, _loop):
    """Ensure control-plane import state exists before the server accepts requests."""

    # Safety net only: mrf.import_run is created by the alembic migration
    # 20260610120000_add_import_run_table; this idempotent ensure keeps fresh
    # nodes working before migrations have run, without per-request DDL.
    await ensure_import_run_table()


@blueprint.exception(SanicException)
async def control_error(request, exc: SanicException):
    """Render control-plane exceptions with the stable JSON error contract."""

    return response.json(_error_payload(request, exc), status=getattr(exc, "status_code", 500))


@blueprint.get("/importers")
async def control_importers(request):
    """List importers available to authenticated control-plane clients."""

    _require_control_auth(request)
    return response.json({"items": importer_registry(), "next_cursor": None})


@blueprint.get("/provider-directory/sources")
async def control_provider_directory_sources(request):
    """List reviewed FHIR sources from the deployed acquisition contract."""
    _require_control_auth(request)
    return response.json(provider_directory_source_catalog())


@blueprint.get("/health/node")
async def control_node_health(request):
    """Return node, database, queue, and worker health for operators."""

    _require_control_auth(request)
    return response.json(await node_health(), default=str)


@blueprint.get("/workers")
async def control_workers(request):
    """List configured control-plane worker definitions and states."""

    _require_control_auth(request)
    return response.json({"items": worker_registry(), "next_cursor": None}, default=str)


@blueprint.post("/workers/ensure")
async def control_ensure_worker(request):
    """Ensure the requested importer worker is running."""

    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    return response.json(ensure_worker(payload), status=202, default=str)


@blueprint.get("/mrf/discovery/sources")
async def control_mrf_discovery_sources(request):
    """Return a cursor-paginated view of stored discovery sources."""

    _require_control_auth(request)
    try:
        limit = page_limit(
            request.args.get("limit"),
            default=DEFAULT_SOURCE_PAGE_SIZE,
            maximum=MAX_SOURCE_PAGE_SIZE,
        )
        payload = await list_discovery_sources_page(
            cursor=request.args.get("cursor"),
            limit=limit,
            query=request.args.get("q"),
            discovery_run_id=request.args.get("run_id"),
        )
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(payload, default=str)


@blueprint.get("/mrf/discovery/sources/<source_id:str>/files")
async def control_mrf_discovery_source_files(request, source_id: str):
    """Return a cursor-paginated view of files stored for one source."""

    _require_control_auth(request)
    try:
        limit = page_limit(
            request.args.get("limit"),
            default=DEFAULT_FILE_PAGE_SIZE,
            maximum=MAX_FILE_PAGE_SIZE,
        )
        payload = await list_discovery_source_files_page(
            source_id,
            cursor=request.args.get("cursor"),
            limit=limit,
        )
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(payload, default=str)


@blueprint.post("/ptg/parse-toc-preview")
async def control_ptg_parse_toc_preview(request):
    """Parse a PTG table of contents without dispatching an import."""

    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        preview = await asyncio.to_thread(parse_ptg_toc_preview, payload)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(preview, default=str)


@blueprint.post("/ptg/import-file")
async def control_ptg_import_file(request):
    """Create one source-file-scoped PTG import run."""

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
    """Promote a validated source snapshot and optionally refresh addresses."""

    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    source_key = str(payload.get("source_key") or "")
    snapshot_id = str(payload.get("snapshot_id") or "")
    try:
        result = await promote_ptg2_source_snapshot(
            source_key=source_key,
            snapshot_id=snapshot_id,
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
    if _ptg_source_snapshot_refresh_requested(payload):
        try:
            refresh_payload = _ptg_source_snapshot_refresh_payload(
                payload,
                source_key=source_key,
                snapshot_id=snapshot_id,
            )
            refresh_run, created = await create_import_run(refresh_payload)
        except ValueError as exc:
            raise BadRequest(str(exc)) from exc
        result = dict(result)
        result["address_refresh"] = {"run": refresh_run, "created": created}
    return response.json(result, default=str)


@blueprint.post("/ptg/source-snapshots/attest")
async def control_ptg_source_snapshot_attest(request):
    """Bind one passing release audit report to an exact validated candidate."""

    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    report = payload.get("report")
    if not isinstance(report, dict):
        raise BadRequest("report must be an object")
    try:
        result = await record_candidate_audit_attestation(
            snapshot_id=str(payload.get("snapshot_id") or ""),
            source_key=str(payload.get("source_key") or ""),
            plan_id=str(payload.get("plan_id") or ""),
            plan_market_type=str(payload.get("plan_market_type") or ""),
            report=report,
        )
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(result, default=str)


@blueprint.post("/ptg/source-snapshots/remove-plan")
async def control_ptg_source_snapshot_remove_plan(request):
    """Preview the resources affected by removing one source snapshot."""

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
    """Remove a non-current source snapshot and its owned resources."""

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


@blueprint.post("/ptg/source-snapshots/retire")
async def control_ptg_source_snapshot_retire(request):
    """Retire a source-scoped PTG2 snapshot after validating control-plane auth."""
    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        result = await retire_ptg2_source_snapshot(
            snapshot_id=str(payload.get("snapshot_id") or ""),
            source_key=str(payload.get("source_key") or "") or None,
        )
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(result, default=str)


@blueprint.post("/imports")
async def control_create_import(request):
    """Create an authenticated import run from a control payload."""

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
    """Return one filtered cursor page of import runs."""

    _require_control_auth(request)
    args = request.args
    try:
        page = await list_import_runs_page(
            status=args.get("status"),
            importer=args.get("importer"),
            retry_of_run_id=args.get("retry_of_run_id"),
            limit=int(args.get("limit") or 50),
            cursor=args.get("cursor"),
        )
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(page, default=str)


@blueprint.get("/imports/<run_id>")
async def control_get_import(request, run_id: str):
    """Return one import run or a not-found control response."""

    _require_control_auth(request)
    run = await get_import_run(run_id)
    if not run:
        raise NotFound("import run not found")
    return response.json(run, default=str)


@blueprint.post("/imports/<run_id>/cancel")
async def control_cancel_import(request, run_id: str):
    """Request cooperative cancellation for an active import run."""

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
    """Create a retry run derived from a prior import run."""

    _require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        result = await retry_import_run(run_id, payload)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if result is None:
        raise NotFound("import run not found")
    run, created = result
    return response.json(run, status=201 if created else 409, default=str)


@blueprint.post("/imports/<run_id>/finalize")
async def control_finalize_import(request, run_id: str):
    """Dispatch the importer-specific finalization step for a run."""

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


def _request_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _ptg_source_snapshot_refresh_requested(payload: dict) -> bool:
    refresh = payload.get("address_refresh") if isinstance(payload.get("address_refresh"), dict) else {}
    return any(
        _request_bool(value)
        for value in (
            payload.get("refresh_addresses"),
            payload.get("enqueue_address_refresh"),
            payload.get("run_address_refresh"),
            refresh.get("enabled"),
        )
    )


def _ptg_source_snapshot_refresh_payload(payload: dict, *, source_key: str, snapshot_id: str) -> dict:
    raw_refresh = payload.get("address_refresh")
    if raw_refresh is not None and not isinstance(raw_refresh, dict):
        raise ValueError("address_refresh must be an object")
    refresh = raw_refresh or {}
    params = dict(refresh.get("params") or {})
    for key in (
        "test_mode",
        "limit_per_source",
        "publish",
        "skip_publish",
        "refresh_mode",
    ):
        if key in refresh and key not in params:
            params[key] = refresh[key]
        elif key in payload and key not in params:
            params[key] = payload[key]
    params.setdefault("refresh_mode", "full")
    params["trigger_source_key"] = source_key
    params["trigger_snapshot_id"] = snapshot_id
    return {
        "run_id": refresh.get("run_id") or payload.get("refresh_run_id"),
        "importer": "entity-address-unified",
        "params": params,
        "idempotency_key": (
            refresh.get("idempotency_key")
            or payload.get("refresh_idempotency_key")
            or f"entity-address-unified:{source_key}:{snapshot_id}"
        ),
        "triggered_by": refresh.get("triggered_by") or payload.get("triggered_by") or "ptg_source_snapshot_promote",
        "schedule_id": refresh.get("schedule_id") or payload.get("schedule_id"),
        "subscription_id": refresh.get("subscription_id") or payload.get("subscription_id"),
        "import_id": refresh.get("import_id") or payload.get("refresh_import_id"),
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
