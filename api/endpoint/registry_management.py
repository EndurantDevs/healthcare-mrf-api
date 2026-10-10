# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated management of durable drafts, separate from serving reads."""

import json
from urllib.parse import parse_qs
from uuid import UUID

import asyncpg
from sanic import Blueprint, response
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_control_auth
from process.company_network_link_store import (
    CompanyLinkBatchCommand,
    CompanyLinkBatchTarget,
    apply_company_network_link_batch,
    read_group_company_links,
)
from process.network_source_binding_store import (
    NetworkSourceBindingBatchCommand,
    apply_network_source_binding_batch,
    list_network_source_bindings,
)
from process.registry_manual_undo import (
    RegistryManualUndoCommand,
    prepare_registry_manual_undo,
    read_registry_manual_history,
)
from process.registry_published_plan_binding_refs import published_plan_binding_requests
from process.registry_record_store import (
    RegistryActor,
    RegistryAddressUnavailable,
    RegistryRecordCommand,
    RegistryRecordConflict,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
)
from process.registry_source_site_catalog import (
    RegistrySourceSiteCatalogUnavailable,
    read_registry_source_sites,
)
from process.registry_targets import verify_registry_targets

blueprint = Blueprint("registry_management", url_prefix="/registry/manage", version=1)
_HEADERS = {"Cache-Control": "private, no-store"}


def _unique_object(pairs):
    document_by_key = {}
    for key, value in pairs:
        if key in document_by_key:
            raise ValueError("registry_duplicate_field")
        document_by_key[key] = value
    return document_by_key


def _body(request, maximum=None):
    if maximum is None:
        kind = request.match_info.get("kind")
        maximum = 8 * 1024 * 1024 if kind in {"membership", "company_links"} else 131072 if kind == "company" else 65536
    if request.args or len(request.body) > maximum:
        raise ValueError("registry_request_too_large")
    document = json.loads(request.body, object_pairs_hook=_unique_object)
    if type(document) is not dict:
        raise ValueError("registry_object_required")
    return document


def _uuid(value):
    if type(value) is not str:
        raise ValueError("registry_uuid_invalid")
    parsed = UUID(value)
    if not parsed.int or str(parsed) != value:
        raise ValueError("registry_uuid_invalid")
    return parsed


def _identity(kind, value):
    if kind in {"network", "membership"}:
        if type(value) is not int or not 1 <= value <= 2147483647:
            raise ValueError("registry_network_invalid")
        return value
    if kind not in {"group", "company", "company_links", "provider", "location", "site_binding", "network_binding"}:
        raise ValueError("registry_kind_invalid")
    return _uuid(value)


def _path_identity(kind, value):
    if kind in {"network", "membership"}:
        if not value.isascii() or not value.isdecimal() or str(int(value)) != value:
            raise ValueError("registry_network_invalid")
        value = int(value)
    return _identity(kind, value)


def _list_query(request):
    query_by_name = parse_qs(request.query_string, keep_blank_values=True, strict_parsing=True)
    if (
        request.body
        or set(query_by_name) - {"limit", "offset", "record_ids"}
        or any(len(values) != 1 for values in query_by_name.values())
    ):
        raise ValueError("registry_page_invalid")
    return {key: values[0] for key, values in query_by_name.items()}


def _record_selectors(query_by_name, kind):
    if "record_ids" not in query_by_name:
        return None
    values = query_by_name["record_ids"].split(",")
    if not 1 <= len(values) <= 100:
        raise ValueError("registry_selector_invalid")
    return tuple(_path_identity(kind, value) for value in values)


def _actor(actor_document):
    if type(actor_document) is not dict or set(actor_document) - {"impersonator_id"} != {
        "kind",
        "user_id",
        "client_id",
    }:
        raise ValueError("registry_actor_invalid")
    return RegistryActor(
        actor_document["kind"],
        _uuid(actor_document["user_id"]),
        actor_document["client_id"],
        _uuid(actor_document["impersonator_id"]) if actor_document.get("impersonator_id") is not None else None,
    )


def _command(document, kind):
    required_fields = {"record_id", "operation", "expected_revision", "fields", "reason", "idempotency_key", "actor"}
    if set(document) - {"allocation_key"} != required_fields:
        raise ValueError("registry_command_invalid")
    actor = _actor(document["actor"])
    record_id = (
        None
        if kind == "network" and document["operation"] == "create" and document["record_id"] is None
        else _identity(kind, document["record_id"])
    )
    command = RegistryRecordCommand(
        kind,
        record_id,
        document["operation"],
        document["expected_revision"],
        document["fields"],
        document["reason"],
        document["idempotency_key"],
        _uuid(document["allocation_key"]) if document.get("allocation_key") is not None else None,
    )
    return command, actor


def _batch_target(document):
    if (
        type(document) is not dict
        or set(document) != {"company_id", "expected_revision", "network_ids", "group_id", "network_assertions"}
        or type(document["network_ids"]) is not list
    ):
        raise ValueError("registry_company_link_batch_target_invalid")
    return CompanyLinkBatchTarget(
        company_id=_identity("company", document["company_id"]),
        expected_revision=document["expected_revision"],
        network_ids=tuple(document["network_ids"]),
        group_id=_uuid(document["group_id"]) if document["group_id"] is not None else None,
        network_assertions=document["network_assertions"],
    )


def _batch_command(document):
    if (
        set(document) != {"targets", "reason", "idempotency_key", "selection_group_id", "actor"}
        or type(document["targets"]) is not list
        or not 1 <= len(document["targets"]) <= 100
    ):
        raise ValueError("registry_company_link_batch_command_invalid")
    return CompanyLinkBatchCommand(
        targets=tuple(_batch_target(target) for target in document["targets"]),
        reason=document["reason"],
        idempotency_key=document["idempotency_key"],
        selection_group_id=_uuid(document["selection_group_id"])
        if document["selection_group_id"] is not None
        else None,
    ), _actor(document["actor"])


def _error(status):
    error_codes_by_status = {
        400: "registry_request_invalid",
        404: "registry_record_not_found",
        409: "registry_revision_conflict",
        503: "registry_unavailable",
    }
    return response.json({"error": {"code": error_codes_by_status[status]}}, status=status, headers=_HEADERS)


@blueprint.post("/targets/verify")
async def verify_targets(request):
    """Verify bounded exact identities for the app's independent grant policy."""
    require_control_auth(request)
    try:
        document = _body(request)
        if set(document) - {"boundary"} != {"targets"}:
            raise ValueError("registry_targets_invalid")
        async with request.ctx.sa_session.begin():
            targets = await verify_registry_targets(
                request.ctx.sa_session, document["targets"], boundary=document.get("boundary", "management")
            )
        return response.json({"targets": targets}, headers=_HEADERS)
    except ValueError:
        return _error(400)
    except SQLAlchemyError:
        return _error(503)


@blueprint.post("/source-sites")
async def read_source_sites(request):
    """Return offices for one exact provider from an explicitly pinned generation."""
    require_control_auth(request)
    try:
        document = _body(request)
        if not {"source_generation", "provider_system", "provider_id"} <= set(document) or set(document) - {
            "source_generation",
            "provider_system",
            "provider_id",
            "limit",
            "offset",
        }:
            raise ValueError("registry_source_site_query_invalid")
        async with request.ctx.sa_session.begin():
            await request.ctx.sa_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            connection = await request.ctx.sa_session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            catalog = await read_registry_source_sites(
                driver,
                generation_id=document["source_generation"],
                provider_system=document["provider_system"],
                provider_id=document["provider_id"],
                limit=document.get("limit", 50),
                offset=document.get("offset", 0),
            )
        return response.json(catalog, headers=_HEADERS)
    except ValueError, TypeError:
        return _error(400)
    except SQLAlchemyError, asyncpg.PostgresError, RegistrySourceSiteCatalogUnavailable:
        return _error(503)


@blueprint.get("/<kind>")
async def list_records(request, kind):
    """Return a bounded draft page to the trusted authorization layer."""
    require_control_auth(request)
    try:
        query_by_name = _list_query(request)
        async with request.ctx.sa_session.begin():
            records = await list_registry_records(
                request.ctx.sa_session,
                kind,
                limit=int(query_by_name.get("limit", "50")),
                offset=int(query_by_name.get("offset", "0")),
                record_ids=_record_selectors(query_by_name, kind),
            )
        return response.json({"records": records}, headers=_HEADERS)
    except ValueError:
        return _error(400)
    except SQLAlchemyError:
        return _error(503)


@blueprint.get("/<kind>/<record_id>")
async def read_record(request, kind, record_id):
    """Management reads may show pending corrections; directory reads may not."""
    require_control_auth(request)
    try:
        if request.args:
            raise ValueError("registry_detail_invalid")
        identity = _path_identity(kind, record_id)
        async with request.ctx.sa_session.begin():
            record = await get_registry_record(request.ctx.sa_session, kind, identity)
        return _error(404) if record is None else response.json({"record": record}, headers=_HEADERS)
    except ValueError:
        return _error(400)
    except SQLAlchemyError:
        return _error(503)


@blueprint.post("/<kind>")
async def write_record(request, kind):
    """Retain one authorized correction and its actor without publishing it."""
    require_control_auth(request)
    try:
        command, actor = _command(_body(request), kind)
        async with request.ctx.sa_session.begin():
            if kind == "site_binding":
                await request.ctx.sa_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            result = await apply_registry_record_command(request.ctx.sa_session, command, actor)
        return response.json(result, headers=_HEADERS)
    except RegistryRecordConflict:
        return _error(409)
    except ValueError, TypeError:
        return _error(400)
    except SQLAlchemyError, RegistryAddressUnavailable:
        return _error(503)


@blueprint.get("/<kind>/<record_id>/history", ignore_body=False)
async def read_record_history(request, kind, record_id):
    """Return bounded full manual revisions to the authorization layer."""
    require_control_auth(request)
    try:
        query = _list_query(request)
        if "record_ids" in query:
            raise ValueError("registry_manual_history_request_invalid")
        identity = _path_identity(kind, record_id)
        async with request.ctx.sa_session.begin():
            await request.ctx.sa_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            result = await read_registry_manual_history(
                request.ctx.sa_session,
                kind,
                identity,
                limit=int(query.get("limit", "50")),
                offset=int(query.get("offset", "0")),
            )
        return _error(404) if result is None else response.json(result, headers=_HEADERS)
    except ValueError, TypeError:
        return _error(400)
    except SQLAlchemyError, RegistryAddressUnavailable:
        return _error(503)


@blueprint.post("/<kind>/<record_id>/undo")
async def undo_record(request, kind, record_id):
    """Save historical editable content as an ordinary unapproved correction."""
    require_control_auth(request)
    try:
        document = _body(request, maximum=65536)
        if set(document) != {"expected_revision", "target_revision", "reason", "idempotency_key", "actor"}:
            raise ValueError("registry_manual_undo_request_invalid")
        command = RegistryManualUndoCommand(
            kind,
            _path_identity(kind, record_id),
            document["expected_revision"],
            document["target_revision"],
            document["reason"],
            document["idempotency_key"],
        )
        actor = _actor(document["actor"])
        async with request.ctx.sa_session.begin():
            await request.ctx.sa_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            prepared = await prepare_registry_manual_undo(request.ctx.sa_session, command, actor)
            receipt = await apply_registry_record_command(request.ctx.sa_session, prepared.command, actor)
        provenance = prepared.provenance
        return response.json(
            {
                "receipt": receipt,
                "undo": {
                    "target_revision": provenance.target_revision,
                    "target_custom_revision": provenance.target_custom_revision,
                    "target_request_sha256": provenance.target_request_sha256,
                    "target_archived": provenance.target_archived,
                    "reason": prepared.command.reason,
                },
            },
            headers=_HEADERS,
        )
    except RegistryRecordConflict:
        return _error(409)
    except ValueError, TypeError:
        return _error(400)
    except SQLAlchemyError, RegistryAddressUnavailable:
        return _error(503)


@blueprint.post("/company_links/batch")
async def write_company_link_batch(request):
    """Retain one explicit selected-company batch through a native set-based save."""
    require_control_auth(request)
    try:
        command, actor = _batch_command(_body(request, maximum=8 * 1024 * 1024))
        async with request.ctx.sa_session.begin():
            await request.ctx.sa_session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            connection = await request.ctx.sa_session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            result = await apply_company_network_link_batch(driver, command, actor)
        return response.json(result, headers=_HEADERS)
    except RegistryRecordConflict:
        return _error(409)
    except ValueError, TypeError:
        return _error(400)
    except SQLAlchemyError, asyncpg.PostgresError, RegistryAddressUnavailable:
        return _error(503)


@blueprint.post("/company_links/group_snapshot")
async def read_company_link_group_snapshot(request):
    """Read one group page narrowed by the authorization layer's exact companies."""
    require_control_auth(request)
    try:
        document = _body(request, maximum=256 * 1024)
        if set(document) != {"group_id", "company_ids", "limit", "offset"}:
            raise ValueError("registry_company_link_group_request_invalid")
        companies = document["company_ids"]
        if companies is not None and (type(companies) is not list or len(companies) > 5000):
            raise ValueError("registry_company_link_group_request_invalid")
        companies = None if companies is None else tuple(_identity("company", company_id) for company_id in companies)
        async with request.ctx.sa_session.begin():
            connection = await request.ctx.sa_session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            group_links = await read_group_company_links(
                driver,
                _identity("group", document["group_id"]),
                company_ids=companies,
                limit=document["limit"],
                offset=document["offset"],
            )
        if group_links is None:
            return _error(404)
        if len(json.dumps(group_links, ensure_ascii=False).encode()) > 8 * 1024 * 1024:
            raise RegistryAddressUnavailable("registry_company_link_group_response_limit")
        return response.json(group_links, headers=_HEADERS)
    except ValueError, TypeError:
        return _error(400)
    except SQLAlchemyError, asyncpg.PostgresError, RegistryAddressUnavailable:
        return _error(503)


@blueprint.post("/source_bindings/batch")
async def write_source_binding_batch(request):
    """Persist a whole reviewed source set without advancing approved search."""
    require_control_auth(request)
    try:
        document = _body(request, maximum=8 * 1024 * 1024)
        if set(document) != {"rows", "reason", "idempotency_key", "actor"} or type(document["rows"]) is not list:
            raise ValueError("registry_source_binding_request_invalid")
        command = NetworkSourceBindingBatchCommand(
            json.dumps(document["rows"], ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(),
            document["reason"],
            document["idempotency_key"],
        )
        actor = _actor(document["actor"])
        published_requests = published_plan_binding_requests(command.input_bytes)
        service = getattr(request.app.ctx, "registry_ptg_scope_engine", None)
        async with request.ctx.sa_session.begin():
            isolation = "REPEATABLE READ" if published_requests else "READ COMMITTED"
            await request.ctx.sa_session.execute(text("SET TRANSACTION ISOLATION LEVEL " + isolation))
            connection = await request.ctx.sa_session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            receipt = await apply_network_source_binding_batch(
                driver, command, actor, published_plan_store=getattr(service, "store", None)
            )
        return response.json(receipt, headers=_HEADERS)
    except RegistryRecordConflict:
        return _error(409)
    except ValueError, TypeError:
        return _error(400)
    except SQLAlchemyError, asyncpg.PostgresError, RegistryAddressUnavailable:
        return _error(503)


@blueprint.post("/source_bindings/query")
async def read_source_binding_page(request):
    """Return exact network and optional source-key draft evidence for review."""
    require_control_auth(request)
    try:
        document = _body(request)
        if set(document) != {"network_id", "source_system", "binding_key", "limit", "offset"}:
            raise ValueError("registry_source_binding_query_invalid")
        async with request.ctx.sa_session.begin():
            connection = await request.ctx.sa_session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            records = await list_network_source_bindings(driver, **document)
        return response.json({"records": records}, headers=_HEADERS)
    except ValueError, TypeError:
        return _error(400)
    except SQLAlchemyError, asyncpg.PostgresError, RegistryAddressUnavailable:
        return _error(503)
