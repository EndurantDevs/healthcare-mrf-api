# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""A dedicated, read-only projection of exact retained execution evidence."""

from __future__ import annotations

import asyncio
import datetime as dt
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
    """Classify a pinned terminal candidate without selecting a current fallback."""

    execution = evidence.execution
    generation = evidence.generation
    if candidate_generation_id is None:
        if execution.state not in {"failed", "canceled"} or generation is not None or execution.failure_class is None:
            raise _Unavailable("no-generation terminal evidence is unavailable")
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
