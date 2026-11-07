# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""A dedicated, read-only projection of exact retained execution evidence."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import hmac
import json
import os
import re

from sanic import Blueprint, response
from sqlalchemy import select, text

from db.models.custom_import import CustomImportExecution
from process.custom_import.operator import (
    ExecutionEvidenceStatus,
    OperatorObjectNotFound,
    inspect_execution_evidence,
)

blueprint = Blueprint("control_execution_evidence", url_prefix="/control/v1")

_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_REQUEST_FIELDS = frozenset({"dataset_id", "definition_revision_id", "idempotency_key", "candidate_generation_id"})
_NO_STORE = {"Cache-Control": "no-store"}
_MAX_BODY_BYTES = 512
_MAX_BIGINT = 2**63 - 1


class _InvalidRequest(ValueError):
    pass


class _Unavailable(RuntimeError):
    pass


def _reply(payload: dict, status: int = 200):
    return response.json(payload, status=status, headers=_NO_STORE)


def _error(code: str, status: int):
    return _reply({"error": code}, status)


def _is_authorized(headers) -> bool:
    expected = (os.getenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN") or "").strip()
    control = (os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    expected_bytes = expected.encode("utf-8", "surrogatepass")
    control_bytes = control.encode("utf-8", "surrogatepass")
    if not expected_bytes or control_bytes and hmac.compare_digest(expected_bytes, control_bytes):
        return False
    authorization = str((headers or {}).get("Authorization", ""))
    if not authorization.startswith("Bearer "):
        return False
    supplied = authorization.removeprefix("Bearer ").strip().encode("utf-8", "surrogatepass")
    return hmac.compare_digest(supplied, expected_bytes)


def _object(pairs):
    request_dict = dict(pairs)
    if len(request_dict) != len(pairs):
        raise _InvalidRequest("duplicate field")
    return request_dict


def _positive_id(value: object) -> int:
    if type(value) is not int or not 0 < value <= _MAX_BIGINT:
        raise _InvalidRequest("invalid identifier")
    return value


def _request_pins(request) -> tuple[int, int, str, int | None]:
    body = getattr(request, "body", None)
    if type(body) is not bytes or not 1 <= len(body) <= _MAX_BODY_BYTES or getattr(request, "query_string", ""):
        raise _InvalidRequest("invalid body")
    try:
        document = json.loads(body, object_pairs_hook=_object)
    except (ValueError, UnicodeError) as exc:
        raise _InvalidRequest("invalid body") from exc
    if type(document) is not dict or document.keys() != _REQUEST_FIELDS:
        raise _InvalidRequest("invalid fields")
    dataset_id = _positive_id(document["dataset_id"])
    definition_revision_id = _positive_id(document["definition_revision_id"])
    idempotency_key = document["idempotency_key"]
    if type(idempotency_key) is not str or _IDEMPOTENCY_KEY.fullmatch(idempotency_key) is None:
        raise _InvalidRequest("invalid key")
    candidate_generation_id = document["candidate_generation_id"]
    if candidate_generation_id is not None:
        candidate_generation_id = _positive_id(candidate_generation_id)
    return dataset_id, definition_revision_id, idempotency_key, candidate_generation_id


async def _execution_id(session, dataset_id: int, definition_revision_id: int, idempotency_key: str) -> int:
    rows = (
        await session.execute(
            select(CustomImportExecution.execution_id, CustomImportExecution.dataset_id)
            .where(
                CustomImportExecution.definition_revision_id == definition_revision_id,
                CustomImportExecution.idempotency_key == idempotency_key,
            )
            .limit(2)
            .execution_options(autoflush=False)
        )
    ).all()
    if len(rows) != 1:
        if not rows:
            raise OperatorObjectNotFound("execution was not found")
        raise _Unavailable("execution locator is ambiguous")
    if rows[0].dataset_id != dataset_id:
        raise OperatorObjectNotFound("execution was not found")
    return _positive_id(rows[0].execution_id)


def _timestamp(value: dt.datetime | None) -> str:
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise _Unavailable("terminal timestamp is invalid")
    return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def _seal_projection(seal) -> dict:
    return {
        "sealing_fence": seal.sealing_fence,
        "materialization_sha256": seal.materialization_sha256,
        "effective_output_sha256": seal.effective_output_sha256,
        "sealed_at": _timestamp(seal.sealed_at),
    }


def _no_change_projection(no_change) -> dict | None:
    if no_change is None:
        return None
    return {
        "base_generation_id": no_change.base_generation_id,
        "base_pointer_version": no_change.base_pointer_version,
        "effective_output_sha256": no_change.effective_output_sha256,
        "receipt_sha256": no_change.receipt_sha256,
        "sealed_at": _timestamp(no_change.sealed_at),
    }


def _generation_projection(evidence: ExecutionEvidenceStatus, candidate_generation_id: int | None) -> dict | None:
    """Classify a pinned or uniquely recovered terminal candidate."""

    execution = evidence.execution
    generation = evidence.generation
    if candidate_generation_id is None:
        if execution.state not in {"failed", "canceled"} or execution.failure_class is None:
            raise _Unavailable("no-generation terminal evidence is unavailable")
        if generation is not None and (
            execution.state != "canceled"
            or generation.publication_state not in {"unsealed", "sealed_unpublished"}
            or generation.no_change is not None
            or evidence.current is not None
            and evidence.current.generation_id == generation.generation_id
        ):
            raise _Unavailable("canceled generation evidence is unavailable")
        return None
    if (
        execution.state not in {"completed", "no_change"}
        or execution.failure_class is not None
        or evidence.source_binding_revision_id is None
        or evidence.source_binding_sha256 is None
        or execution.capture_bundle_id is None
        or evidence.capture_manifest_sha256 is None
        or generation is None
        or generation.generation_id != candidate_generation_id
        or generation.seal is None
    ):
        raise _Unavailable("sealed generation evidence is unavailable")
    if execution.state == "no_change":
        if generation.publication_state != "no_change" or generation.no_change is None:
            raise _Unavailable("no-change evidence is unavailable")
    elif generation.publication_state not in {"current", "superseded", "sealed_unpublished"} or generation.no_change:
        raise _Unavailable("completed generation evidence is unavailable")
    current_pointer_dict = None
    if generation.publication_state == "current":
        if evidence.current is None or evidence.current.generation_id != candidate_generation_id:
            raise _Unavailable("current pointer evidence is unavailable")
        current_pointer_dict = {
            "generation_id": candidate_generation_id,
            "pointer_version": evidence.current.pointer_version,
        }
    return {
        "generation_id": candidate_generation_id,
        "source_bundle_sha256": generation.source_bundle_sha256,
        "publication_state": generation.publication_state,
        "seal": _seal_projection(generation.seal),
        "current_pointer": current_pointer_dict,
        "no_change": _no_change_projection(generation.no_change),
    }


def _projection(
    evidence: ExecutionEvidenceStatus,
    *,
    dataset_id: int,
    definition_revision_id: int,
    execution_id: int,
    candidate_generation_id: int | None,
) -> dict:
    """Serialize only independently checked retained identities and digests."""

    execution = evidence.execution
    if (
        execution.dataset_id != dataset_id
        or execution.definition_revision_id != definition_revision_id
        or execution.execution_id != execution_id
    ):
        raise _Unavailable("execution identity differs")
    return {
        "execution": {
            "execution_id": execution.execution_id,
            "dataset_id": execution.dataset_id,
            "definition_revision_id": execution.definition_revision_id,
            "schema_revision_id": execution.schema_revision_id,
            "state": execution.state,
            "failure_class": execution.failure_class,
            "finished_at": _timestamp(execution.finished_at),
        },
        "definition": {"sha256": evidence.definition_sha256},
        "schema": {"sha256": evidence.schema_sha256},
        "source_binding": (
            {"revision_id": evidence.source_binding_revision_id, "sha256": evidence.source_binding_sha256}
            if evidence.source_binding_revision_id is not None
            else None
        ),
        "capture": (
            {"bundle_id": execution.capture_bundle_id, "manifest_sha256": evidence.capture_manifest_sha256}
            if execution.capture_bundle_id is not None
            else None
        ),
        "generation": _generation_projection(evidence, candidate_generation_id),
    }


async def serve_execution_evidence(request, session):
    """Resolve trusted request pins before inspecting any worker-selected candidate."""

    if not _is_authorized(getattr(request, "headers", None)):
        return _error("forbidden", 403)
    try:
        dataset_id, definition_revision_id, idempotency_key, candidate_generation_id = _request_pins(request)
    except _InvalidRequest:
        return _error("invalid_request", 400)
    if session is None or session.in_transaction():
        return _error("evidence_unavailable", 503)
    try:
        async with asyncio.timeout(5), session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            execution_id = await _execution_id(session, dataset_id, definition_revision_id, idempotency_key)
            evidence = await inspect_execution_evidence(
                session,
                dataset_id=dataset_id,
                execution_id=execution_id,
                candidate_generation_id=candidate_generation_id,
            )
            if candidate_generation_id is None and evidence.execution.state in {"completed", "no_change"}:
                if evidence.generation is None:
                    raise _Unavailable("terminal generation evidence is unavailable")
                candidate_generation_id = evidence.generation.generation_id
            response_dict = _projection(
                evidence,
                dataset_id=dataset_id,
                definition_revision_id=definition_revision_id,
                execution_id=execution_id,
                candidate_generation_id=candidate_generation_id,
            )
    except OperatorObjectNotFound:
        return _error("not_found", 404)
    except Exception:
        return _error("evidence_unavailable", 503)
    return _reply(response_dict)


@blueprint.post("/custom-import/execution-evidence")
async def execution_evidence(request):
    """Read exact retained evidence through the dedicated operator audience."""

    return await serve_execution_evidence(request, getattr(request.ctx, "sa_session", None))


async def serve_owner_handoff(request, factory, *, operation):
    """Fixed private owner operations; signed scope is required before mutation."""
    from process.custom_import import family_shard_handoff as handoff
    from process.custom_import.family_shard_owner_cli import profile

    if not _is_authorized(getattr(request, "headers", None)):
        return _error("forbidden", 403)
    try:
        if getattr(request, "query_string", ""):
            raise ValueError()
        request_by_field = handoff.document(request.body)
        profile_document, key, _limits = profile()
        if operation == "prepare":
            return await _prepare_owner_handoff(factory, request_by_field, profile_document)
        return await _signed_owner_handoff(factory, request_by_field, profile_document, key, operation)
    except Exception:
        # Neither the request nor exception text is echoed (the body contains a bearer).
        return _error("owner_handoff_unavailable", 503)


def _owner_factory():
    from db.connection import db

    return db.session_factory


@blueprint.post("/custom-import/owner-handoff-prepare")
async def owner_handoff_prepare(request):
    """Prepare one native original-request descriptor through private authentication."""
    return await serve_owner_handoff(request, _owner_factory(), operation="prepare")


@blueprint.post("/custom-import/owner-handoff")
async def owner_handoff(request):
    """Accept one signed original request into durable owner custody."""
    return await serve_owner_handoff(request, _owner_factory(), operation="launch")


@blueprint.post("/custom-import/owner-handoff-observe")
async def owner_handoff_observe(request):
    """Observe exact owner custody and Job identity after an uncertain reply."""
    return await serve_owner_handoff(request, _owner_factory(), operation="observe")


@blueprint.post("/custom-import/owner-handoff-stop")
async def owner_handoff_stop(request):
    """Stop the exact owner Job only after native cancellation or finality."""
    return await serve_owner_handoff(request, _owner_factory(), operation="stop")


@blueprint.post("/custom-import/owner-handoff-cleanup")
async def owner_handoff_cleanup(request):
    """Release protected files only after native finality and exact Pod absence."""
    return await serve_owner_handoff(request, _owner_factory(), operation="cleanup")


@blueprint.post("/custom-import/owner-attempt")
async def owner_attempt(request):
    """Operate one explicitly signed recovery child of the original owner."""
    from api.custom_import_owner_attempt import serve

    return await serve(request, _owner_factory())


async def _prepare_owner_handoff(factory, request_by_field, profile_document):
    """Resolve the native sealed request without a cursor or worker result hint."""
    from process.custom_import import family_shard_handoff as handoff
    from process.custom_import.family_shard_production import original_request

    if type(request_by_field) is not dict or request_by_field.keys() != {"locator", "bearer"}:
        raise ValueError()
    locator = request_by_field["locator"]
    if type(locator) is not dict or locator.keys() != {
        "dataset_id",
        "definition_revision_id",
        "source_binding_revision_id",
        "controller_execution_id",
    }:
        raise ValueError()
    for name in ("dataset_id", "definition_revision_id", "source_binding_revision_id"):
        _positive_id(locator[name])
    if (
        type(locator["controller_execution_id"]) is not str
        or _IDEMPOTENCY_KEY.fullmatch(locator["controller_execution_id"]) is None
    ):
        raise ValueError()
    original = await original_request(
        factory, locator, handoff.checked_bearer(request_by_field["bearer"].encode("ascii"))
    )
    return _reply(
        {
            "native": original.native,
            "owner_profile_sha256": hashlib.sha256(handoff.canonical(profile_document)).hexdigest(),
            "owner_image": profile_document["image"],
        }
    )


async def _signed_owner_handoff(factory, request_by_field, profile_document, key, operation):
    """Dispatch only the closed signed owner lifecycle operations."""
    import time

    from api import custom_import_owner_retirement as retired
    from api import custom_import_owner_worker as owner
    from process.custom_import import family_shard_handoff as handoff
    from process.custom_import.family_shard_production import retained_state

    fields = (
        {"signed", "bearer"}
        if operation == "launch"
        else {"signed", "receipt"}
        if operation in {"stop", "cleanup"}
        else {"signed"}
    )
    if type(request_by_field) is not dict or request_by_field.keys() != fields:
        raise ValueError()
    signed = request_by_field["signed"]
    bearer = handoff.checked_bearer(request_by_field["bearer"].encode("ascii")) if operation == "launch" else None
    handoff.verify(signed, verifying_key=key, bearer=bearer, now=time.time() if operation == "launch" else None)
    owner.require_profile(signed, profile_document)
    if operation == "launch":
        owner_receipt = await owner.launch(signed, bearer, factory)
        return _reply(owner_receipt | owner.custody_receipt(signed))
    state = (
        await retained_state(factory, signed["descriptor"])
        if operation == "observe"
        else await retired.terminal_state(factory, signed, profile_document)
    )
    if operation == "observe":
        owner_receipt = await owner.observed(signed, profile_document)
        if owner_receipt is not None:
            handoff.is_launch_file_created(
                signed, "job.json", {"job_uid": owner_receipt["job_uid"], "handoff_id": handoff.identity(signed)}
            )
        return _reply((owner_receipt or {"status": "absent"}) | owner.custody_receipt(signed) | {"engine_state": state})
    if state not in {"canceling", "canceled", "failed", "completed", "no_change", "retired"}:
        raise ValueError()
    receipt = request_by_field["receipt"]
    if type(receipt) is not dict or receipt.get("handoff_id") != handoff.identity(signed):
        raise ValueError()
    absent = await owner.is_stopped(
        signed, profile_document, expected_uid=receipt.get("job_uid"), require_terminal=operation == "cleanup"
    )
    is_cleaned = False
    if operation == "cleanup" and absent:
        if state == "canceling":
            raise ValueError()
        if await retired.ensure_files_cleaned(factory, signed, profile_document):
            owner.cleanup(signed, receipt)
            is_cleaned = True
    return _reply(
        {
            "handoff_id": handoff.identity(signed),
            "pods_absent": absent,
            "cleaned": is_cleaned,
        }
    )


@blueprint.post("/custom-import/retained-capture-copy")
async def retained_capture_copy(request):
    """Delegate a closed maintenance operation through the existing private authority."""
    from api.custom_import_retained_copy import serve

    return await serve(request, _owner_factory())
