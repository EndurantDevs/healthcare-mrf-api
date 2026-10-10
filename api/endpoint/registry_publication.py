# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Trusted previews and queued requests never assume the protected publisher role."""

import json
import re
from urllib.parse import parse_qs

import asyncpg
from sanic import Blueprint, response
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_control_auth
from api.endpoint.registry_management import _error, _unique_object, _uuid
from process.registry_approval_preview import preview_registry_approval
from process.registry_approval_store import (
    RegistryApprovalCommand,
    RegistryApprovalConflict,
    _validated_command,
)
from process.registry_approved_company_read import MAX_APPROVED_COMPANY_PAGE_SIZE, read_registry_approved_companies
from process.registry_imported_selection_preview import (
    RegistryImportedSelectionUnavailable,
)
from process.registry_publication_queue import (
    RegistryReactivationCommand,
    _reactivation_actor,
    _reactivation_document,
    enqueue_registry_publication,
    enqueue_registry_reactivation,
    enqueue_registry_source_rollback_preview,
    enqueue_registry_source_rollback_publication,
    read_registry_publication_request,
    validated_registry_source_rollback_preview_command,
    validated_registry_source_rollback_publication_command,
)
from process.registry_record_store import (
    RegistryActor,
    RegistryRecordConflict,
    _validated_actor,
)
from process.registry_source_observation_store import _namespace

blueprint = Blueprint("registry_publication", url_prefix="/registry/publication", version=1)
_HEADERS = {"Cache-Control": "private, no-store"}


@blueprint.middleware("response")
async def publication_response_headers(_request, response_message):
    """Keep authenticated receipts and pre-database denials out of shared caches."""
    response_message.headers.update(_HEADERS)


def _command(request):
    if getattr(request, "query_string", "") or request.args or not request.body or len(request.body) > 65536:
        raise ValueError("registry_publication_request_invalid")
    document = json.loads(request.body, object_pairs_hook=_unique_object)
    required_fields = {
        "expected_draft_revision",
        "expected_approved_revision",
        "expected_serving_generation",
        "selection",
        "reason",
        "idempotency_key",
        "actor",
        "session_token_sha256",
    }
    if type(document) is not dict or set(document) != required_fields:
        raise ValueError("registry_publication_request_invalid")
    selected = document["selection"]
    if type(selected) is not list or not 1 <= len(selected) <= 100:
        raise ValueError("registry_publication_selection_invalid")
    actor_document = document["actor"]
    if type(actor_document) is not dict or set(actor_document) - {"impersonator_id"} != {
        "kind",
        "user_id",
        "client_id",
    }:
        raise ValueError("registry_publication_actor_invalid")
    actor = RegistryActor(
        actor_document["kind"],
        _uuid(actor_document["user_id"]),
        actor_document["client_id"],
        _uuid(actor_document["impersonator_id"]) if actor_document.get("impersonator_id") is not None else None,
    )
    _validated_actor(actor)
    command = RegistryApprovalCommand(
        document["expected_draft_revision"],
        document["expected_approved_revision"],
        tuple(selected),
        document["reason"],
        document["idempotency_key"],
    )
    _validated_command(command)
    generation = document["expected_serving_generation"]
    if type(generation) is not int or not 0 <= generation < 2**63:
        raise ValueError("registry_publication_head_invalid")
    digest = document["session_token_sha256"]
    if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("registry_session_hash_invalid")
    return command, actor, generation, digest


async def _driver(session):
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    connection = await session.connection()
    return (await connection.get_raw_connection()).driver_connection


def _retained_operation_command(request, operation):
    if request.body is None or not request.body or len(request.body) > 4096 or request.query_string or request.args:
        raise ValueError("registry_publication_request_invalid")
    document_by_name = json.loads(request.body, object_pairs_hook=_unique_object)
    if (
        type(document_by_name) is not dict
        or set(document_by_name)
        != {
            "operation",
            "target_generation",
            "expected_serving_generation",
            "expected_approved_revision",
            "reason",
            "idempotency_key",
            "actor",
            "session_token_sha256",
        }
        or document_by_name["operation"] != operation
    ):
        raise ValueError("registry_reactivation_command_invalid")
    actor_by_name = document_by_name["actor"]
    if type(actor_by_name) is not dict or set(actor_by_name) != {
        "kind",
        "user_id",
        "client_id",
    }:
        raise ValueError("registry_reactivation_actor_invalid")
    actor = RegistryActor(
        actor_by_name["kind"],
        _uuid(actor_by_name["user_id"]),
        actor_by_name["client_id"],
    )
    _reactivation_actor(actor)
    command = RegistryReactivationCommand(
        document_by_name["target_generation"],
        document_by_name["expected_serving_generation"],
        document_by_name["expected_approved_revision"],
        document_by_name["reason"],
        document_by_name["idempotency_key"],
    )
    _reactivation_document(command)
    session_hash = document_by_name["session_token_sha256"]
    if type(session_hash) is not str or re.fullmatch(r"[0-9a-f]{64}", session_hash) is None:
        raise ValueError("registry_session_hash_invalid")
    return command, actor, session_hash


def _reactivation_command(request):
    return _retained_operation_command(request, "reactivate_generation")


def _source_rollback_publication_command(request):
    if not request.body or len(request.body) > 4096 or request.query_string or request.args:
        raise ValueError("registry_publication_request_invalid")
    document_by_name = json.loads(request.body, object_pairs_hook=_unique_object)
    if type(document_by_name) is not dict or set(document_by_name) != {
        "operation",
        "preview_request_id",
        "candidate_sha256",
        "expected_serving_generation",
        "expected_approved_revision",
        "reason",
        "idempotency_key",
        "actor",
        "session_token_sha256",
    }:
        raise ValueError("registry_source_rollback_publication_command_invalid")
    command_by_name = validated_registry_source_rollback_publication_command(
        {
            field_name: field_value
            for field_name, field_value in document_by_name.items()
            if field_name not in {"actor", "session_token_sha256"}
        }
    )
    actor_by_name = document_by_name["actor"]
    if type(actor_by_name) is not dict or set(actor_by_name) != {
        "kind",
        "user_id",
        "client_id",
    }:
        raise ValueError("registry_reactivation_actor_invalid")
    actor = RegistryActor(
        actor_by_name["kind"],
        _uuid(actor_by_name["user_id"]),
        actor_by_name["client_id"],
    )
    _reactivation_actor(actor)
    session_hash = document_by_name["session_token_sha256"]
    if type(session_hash) is not str or re.fullmatch(r"[0-9a-f]{64}", session_hash) is None:
        raise ValueError("registry_session_hash_invalid")
    return command_by_name, actor, session_hash


@blueprint.post("/source-rollback/publish")
async def queue_source_rollback_publication(request):
    """Queue explicit reviewed publication; HTTP keeps its ordinary API privileges."""
    require_control_auth(request)
    try:
        command_by_name, actor, session_hash = _source_rollback_publication_command(request)
        async with request.ctx.sa_session.begin():
            receipt = await enqueue_registry_source_rollback_publication(
                await _driver(request.ctx.sa_session),
                command_by_name,
                actor,
                session_token_sha256=session_hash,
            )
        return response.json(receipt, headers=_HEADERS)
    except RegistryRecordConflict:
        return _error(409)
    except ValueError, TypeError, UnicodeError, RecursionError:
        return _error(400)
    except asyncpg.PostgresError, SQLAlchemyError:
        return _error(503)


@blueprint.post("/source-rollback/preview")
async def queue_source_rollback_preview(request):
    """Queue a reviewed retained-source preview; do not switch the serving head."""
    require_control_auth(request)
    try:
        command, actor, session_hash = _retained_operation_command(request, "prepare_source_rollback")
        command_by_name = validated_registry_source_rollback_preview_command(
            {
                "operation": "prepare_source_rollback",
                **command.__dict__,
            }
        )
        async with request.ctx.sa_session.begin():
            receipt = await enqueue_registry_source_rollback_preview(
                await _driver(request.ctx.sa_session),
                command_by_name,
                actor,
                session_token_sha256=session_hash,
            )
        return response.json(receipt, headers=_HEADERS)
    except RegistryRecordConflict:
        return _error(409)
    except ValueError, TypeError, UnicodeError, RecursionError:
        return _error(400)
    except asyncpg.PostgresError, SQLAlchemyError:
        return _error(503)


@blueprint.post("/reactivate")
async def queue_reactivation(request):
    """Queue a distinct administrator command without entering the publisher role."""
    require_control_auth(request)
    try:
        command, actor, session_hash = _reactivation_command(request)
        async with request.ctx.sa_session.begin():
            receipt = await enqueue_registry_reactivation(
                await _driver(request.ctx.sa_session),
                command,
                actor,
                session_token_sha256=session_hash,
            )
        return response.json(receipt, headers=_HEADERS)
    except RegistryRecordConflict:
        return _error(409)
    except ValueError, TypeError, UnicodeError, RecursionError:
        return _error(400)
    except asyncpg.PostgresError, SQLAlchemyError:
        return _error(503)


@blueprint.get("/state", ignore_body=False)
async def publication_state(request):
    """Read current draft, approved and serving heads through trusted control auth."""
    require_control_auth(request)
    try:
        if request.body or getattr(request, "query_string", "") or request.args:
            raise ValueError("registry_publication_request_invalid")
        async with request.ctx.sa_session.begin():
            driver = await _driver(request.ctx.sa_session)
            row = await driver.fetchrow(
                f"SELECT draft_revision,approved_revision,coalesce(generation_id,0) AS serving_generation FROM {_namespace(None)}.registry_revision_control CROSS JOIN {_namespace(None)}.network_serving_control WHERE registry_revision_control.id=1 AND network_serving_control.id=1"
            )
        return _error(503) if row is None else response.json(dict(row), headers=_HEADERS)
    except ValueError:
        return _error(400)
    except asyncpg.PostgresError, SQLAlchemyError:
        return _error(503)


@blueprint.post("/<operation>")
async def request_publication(request, operation):
    """Preview or queue one selection; HTTP never enters the publisher role."""
    require_control_auth(request)
    try:
        if operation not in {"preview", "queue"}:
            raise ValueError("registry_publication_operation_invalid")
        command, actor, expected_head, session_hash = _command(request)
        async with request.ctx.sa_session.begin():
            if operation == "preview":
                await request.ctx.sa_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            driver = await _driver(request.ctx.sa_session)
            if operation == "queue":
                publication_receipt = await enqueue_registry_publication(
                    driver,
                    command,
                    actor,
                    session_token_sha256=session_hash,
                    expected_serving_generation=expected_head,
                )
            else:
                current_head = await driver.fetchval(
                    f"SELECT coalesce(generation_id,0) FROM {_namespace(None)}.network_serving_control WHERE id=1"
                )
                if current_head is None or current_head != expected_head:
                    raise RegistryApprovalConflict("registry_queue_revision_conflict")
                publication_receipt = await preview_registry_approval(driver, command, actor)
        return response.json(publication_receipt, headers=_HEADERS)
    except RegistryRecordConflict:
        return _error(409)
    except ValueError, TypeError:
        return _error(400)
    except RegistryImportedSelectionUnavailable:
        return _error(503)
    except asyncpg.PostgresError, SQLAlchemyError:
        return _error(503)


@blueprint.get("/requests/<request_id>", ignore_body=False)
async def publication_request(request, request_id):
    """Read a controller status receipt for the authenticated application layer."""
    require_control_auth(request)
    try:
        if request.body or getattr(request, "query_string", "") or request.args:
            raise ValueError("registry_publication_request_invalid")
        identity = _uuid(request_id)
        async with request.ctx.sa_session.begin():
            result = await read_registry_publication_request(await _driver(request.ctx.sa_session), identity)
        return _error(404) if result is None else response.json(result, headers=_HEADERS)
    except ValueError:
        return _error(400)
    except asyncpg.PostgresError, SQLAlchemyError:
        return _error(503)


@blueprint.get("/approved-companies", ignore_body=False)
async def approved_companies(request):
    """Read the current retained company map through existing service authority."""
    require_control_auth(request)
    try:
        query_by_field = parse_qs(request.query_string, keep_blank_values=True, strict_parsing=True)
        if (
            request.body
            or set(query_by_field) - {"limit", "offset", "approved_revision"}
            or any(len(query_values) != 1 for query_values in query_by_field.values())
        ):
            raise ValueError("registry_page_invalid")
        if any(not re.fullmatch(r"0|[1-9][0-9]{0,15}", query_values[0]) for query_values in query_by_field.values()):
            raise ValueError("registry_page_invalid")
        page_limit = int(query_by_field.get("limit", [str(MAX_APPROVED_COMPANY_PAGE_SIZE)])[0])
        page_offset = int(query_by_field.get("offset", ["0"])[0])
        if (
            not 1 <= page_limit <= MAX_APPROVED_COMPANY_PAGE_SIZE
            or not 0 <= page_offset <= 1000000
            or page_offset > 0
            and "approved_revision" not in query_by_field
        ):
            raise ValueError("registry_page_invalid")
        if "approved_revision" in query_by_field and int(query_by_field["approved_revision"][0]) > 2**53 - 1:
            raise ValueError("registry_page_invalid")
        async with request.ctx.sa_session.begin():
            await request.ctx.sa_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            company_page_by_field = await read_registry_approved_companies(
                await _driver(request.ctx.sa_session),
                limit=page_limit,
                offset=page_offset,
                approved_revision=int(query_by_field["approved_revision"][0])
                if "approved_revision" in query_by_field
                else None,
            )
        return response.json(company_page_by_field, headers=_HEADERS)
    except RegistryApprovalConflict:
        return _error(409)
    except ValueError, TypeError:
        return _error(400)
    except asyncpg.PostgresError, SQLAlchemyError, RuntimeError:
        return _error(503)
