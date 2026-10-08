# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Purpose-separated admission handler using the existing application pool.

No request supplies a definition, source configuration, SQL, lease lifetime, or
new deadline. Retained preloads reconstruct inputs; the admission operation's
locked permit check, live lease checks, and pre-commit expiry grant authority.
An unavailable response can mean an uncertain COMMIT, not a proven rollback.
"""

from __future__ import annotations

import asyncio
import hmac
import os
from datetime import datetime, timedelta, timezone

from sanic import Blueprint, response
from sanic.exceptions import BadRequest, PayloadTooLarge
from sqlalchemy import and_, func, select, text

from db.connection import db
from db.models.custom_import import CustomImportBuildAttempt, CustomImportExecution
from process.custom_import import admission_sql
from process.custom_import.admission_authorization import (
    CONTEXT_HEADER,
    KEY_ID_HEADER,
    SIGNATURE_HEADER,
    AdmissionAuthorizationError,
    AdmissionKeyring,
    VerifiedAdmission,
    _origin,
    load_keyring,
    verify_request,
)
from process.custom_import.build_source import SourceBuildRequest
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from process.custom_import.snowflake_source_binding import load_snowflake_source_binding

_HEADERS = ("Content-Type", "Authorization", CONTEXT_HEADER, KEY_ID_HEADER, SIGNATURE_HEADER)
_HEADER_NAMES = frozenset(name.lower() for name in _HEADERS)
KEYRING_FILE_ENV = "HLTHPRT_CUSTOM_IMPORT_ADMISSION_KEYRING_FILE"
ORIGIN_ENV = "HLTHPRT_CUSTOM_IMPORT_ADMISSION_ORIGIN"
MAX_BODY_BYTES = 512
MAX_BATCH_RESPONSE_SECONDS = 300
blueprint = Blueprint("custom_import_admission", url_prefix="/control/v1")


class AdmissionUnavailable(RuntimeError):
    """No admission outcome can safely be reported by this handler."""


def _unavailable():
    return AdmissionUnavailable("custom_import_admission_unavailable")


def _contract_headers(headers, *, source=False):
    """Use the server's duplicate-preserving Header, never a collapsed mapping."""

    if not callable(getattr(headers, "getall", None)):
        raise AdmissionAuthorizationError("custom_import_admission_authorization_invalid")
    expected_headers = tuple(name.replace("-Admission-", "-Source-") for name in _HEADERS) if source else _HEADERS
    expected_names = frozenset(name.lower() for name in expected_headers)
    pairs = list(headers.items())
    for name, _value in pairs:
        if (
            not isinstance(name, str)
            or not name.isascii()
            or (name.lower().startswith("x-custom-import-") and name.lower() not in expected_names)
        ):
            raise AdmissionAuthorizationError("custom_import_admission_authorization_invalid")
    selected_headers = []
    for name in expected_headers:
        values = headers.getall(name, [])
        matched_values = [value for candidate, value in pairs if candidate.lower() == name.lower()]
        if len(values) != 1 or matched_values != list(values):
            raise AdmissionAuthorizationError("custom_import_admission_authorization_invalid")
        selected_headers.append((name, values[0]))
    return selected_headers


def _retained_build_query(pins):
    """Select a build only through its complete retained execution identity."""

    return (
        select(CustomImportBuildAttempt, CustomImportExecution)
        .join(
            CustomImportExecution,
            and_(
                CustomImportExecution.execution_id == CustomImportBuildAttempt.execution_id,
                CustomImportExecution.dataset_id == CustomImportBuildAttempt.dataset_id,
                CustomImportExecution.definition_revision_id == CustomImportBuildAttempt.definition_revision_id,
                CustomImportExecution.schema_revision_id == CustomImportBuildAttempt.schema_revision_id,
            ),
        )
        .where(
            CustomImportBuildAttempt.build_id == pins.build_id,
            CustomImportExecution.execution_id == pins.execution_id,
        )
    )


def _source_request(build, loaded, verified):
    """Reconstruct batch bounds only from the verified retained configuration."""

    permit, pins = verified.permit, verified.pins
    policy = loaded.binding.processing_policy.build
    return SourceBuildRequest(
        dataset_id=permit.dataset_id,
        definition_revision_id=permit.definition_revision_id,
        schema_revision_id=permit.schema_revision_id,
        execution_id=pins.execution_id,
        lease_token=verified.token.value,
        fence=pins.fence,
        definition=loaded.definition,
        expected_base_generation_id=build.base_generation_id,
        expected_pointer_version=build.base_pointer_version,
        complete_scope=build.complete_scope,
        page_row_limit=build.page_row_limit,
        page_byte_limit=build.page_byte_limit,
        statement_timeout_ms=build.statement_timeout_ms,
        build_deadline_at=build.build_deadline_at,
        lease_seconds=policy.lease_seconds,
        authorization_expires_at=permit.expires_at,
    )


async def _retained_request(session_factory, verified: VerifiedAdmission) -> SourceBuildRequest:
    """Read only immutable request material; close this session before admission."""

    if "authorization_expires_at" not in SourceBuildRequest.__dataclass_fields__:
        raise _unavailable()
    permit, pins = verified.permit, verified.pins
    async with asyncio.timeout(5), session_factory() as session:
        async with session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(select(func.set_config("statement_timeout", "5000", True)))
            retained = (await session.execute(_retained_build_query(pins))).one_or_none()
            if retained is None:
                raise _unavailable()
            build, execution = retained
            if (
                execution.dataset_id != permit.dataset_id
                or execution.definition_revision_id != permit.definition_revision_id
                or execution.schema_revision_id != permit.schema_revision_id
                or execution.idempotency_key != permit.idempotency_key
                or execution.source_binding_revision_id != permit.source_binding_revision_id
            ):
                raise _unavailable()
            loaded = await load_snowflake_source_binding(
                session,
                definition_revision_id=permit.definition_revision_id,
                source_binding_revision_id=permit.source_binding_revision_id,
            )
            if (
                loaded.dataset_id != permit.dataset_id
                or loaded.definition_revision_id != permit.definition_revision_id
                or loaded.schema_revision_id != permit.schema_revision_id
                or loaded.source_binding_revision_id != permit.source_binding_revision_id
                or not hmac.compare_digest(loaded.source_binding_sha256, bytes.fromhex(permit.source_binding_sha256))
                or loaded.binding.processing_policy is None
            ):
                raise _unavailable()
            policy = loaded.binding.processing_policy.build
            if any(
                getattr(build, name) != getattr(policy, name)
                for name in (
                    "page_row_limit",
                    "page_byte_limit",
                    "statement_timeout_ms",
                )
            ):
                raise _unavailable()
            return _source_request(build, loaded, verified)


def _receipt(verified, admission_result):
    """Encode only the committed result for the exact attempted cursor."""

    if admission_result.phase not in {"admission", "graph", "rejected"}:
        raise _unavailable()
    for name, maximum in (
        ("after_occurrence_id", 2**63 - 1),
        ("rows_processed", 100_000),
        ("candidate_error_count", 2**63 - 1),
    ):
        counter = getattr(admission_result, name)
        if type(counter) is not int or not 0 <= counter <= maximum:
            raise _unavailable()
    if admission_result.after_occurrence_id < verified.pins.expected_after_occurrence_id:
        raise _unavailable()
    if (
        admission_result.rows_processed > 0
        and admission_result.after_occurrence_id <= verified.pins.expected_after_occurrence_id
        or admission_result.rows_processed == 0
        and (
            admission_result.phase not in {"graph", "rejected"}
            or admission_result.after_occurrence_id != verified.pins.expected_after_occurrence_id
        )
    ):
        raise _unavailable()
    return {
        "execution_id": verified.pins.execution_id,
        "build_id": verified.pins.build_id,
        "fence": verified.pins.fence,
        "phase": admission_result.phase,
        "after_occurrence_id": admission_result.after_occurrence_id,
        "rows_processed": admission_result.rows_processed,
        "candidate_error_count": admission_result.candidate_error_count,
    }


def _reply(payload, status=200):
    return response.json(payload, status=status, headers={"Cache-Control": "no-store"})


def extend_batch_response_deadline(request, *, expires_at, trusted_now):
    """Let verified batch work finish without changing other requests' deadlines."""

    if getattr(request, "transport", None) is None:
        return
    protocol = request.protocol
    if getattr(protocol, "response_timeout", None) is not None:
        remaining = min(MAX_BATCH_RESPONSE_SECONDS, (expires_at - trusted_now).total_seconds())
        _set_response_timeout(protocol, max(request.app.config.RESPONSE_TIMEOUT, remaining))


def _set_response_timeout(protocol, seconds):
    """Rearm Sanic when a shorter deadline replaces a scheduled long interval."""

    previous, protocol.response_timeout = protocol.response_timeout, seconds
    if seconds < previous:
        if protocol._callback_check_timeouts is not None:
            protocol._callback_check_timeouts.cancel()
        protocol.check_timeouts()


def register_batch_response_deadline(app):
    """Reset the connection's timeout before each reused HTTP request."""

    @app.signal("http.lifecycle.handle")
    async def reset(request):
        """Do not let a previous verified batch extend a keep-alive request."""
        if getattr(request, "transport", None) is not None:
            protocol = request.protocol
            if getattr(protocol, "response_timeout", None) not in (None, request.app.config.RESPONSE_TIMEOUT):
                _set_response_timeout(protocol, request.app.config.RESPONSE_TIMEOUT)


@blueprint.listener("before_server_start")
async def initialize_admission_authority(app, _loop):
    """Pin dedicated authority once; missing configuration keeps admission off."""

    keyring_path, origin = os.environ.get(KEYRING_FILE_ENV), os.environ.get(ORIGIN_ENV)
    app.ctx.custom_import_admission_authority = None
    if keyring_path is None and origin is None:
        return
    try:
        if not keyring_path or not origin:
            raise _unavailable()
        approved_origin = _origin(origin)
        with open(keyring_path, "rb") as keyring_file:
            keyring = load_keyring(keyring_file.read(4_097))
        app.ctx.custom_import_admission_authority = (keyring, approved_origin)
    except Exception:
        raise _unavailable() from None


async def _receive_admission_body(request):
    """Bound accumulation as well as the native HTTP stream before reading."""

    request.stream.request_max_size = MAX_BODY_BYTES
    chunks, byte_count = [], 0
    async for chunk in request.stream:
        byte_count += len(chunk)
        if byte_count > MAX_BODY_BYTES:
            raise PayloadTooLarge("admission body exceeds limit")
        chunks.append(chunk)
    request.body = b"".join(chunks)


@blueprint.post("/custom-import/admission-batch", stream=True, strict_slashes=True)
async def admission_batch(request):
    """Keep body and origin authority independent of caller-controlled headers."""

    authority = getattr(request.app.ctx, "custom_import_admission_authority", None)
    if authority is None:
        return _reply({"error": "admission_unavailable"}, 503)
    try:
        _contract_headers(request.headers)
        await _receive_admission_body(request)
    except PayloadTooLarge:
        return _reply({"error": "admission_body_too_large"}, 413)
    except BadRequest, AdmissionAuthorizationError:
        return _reply({"error": "admission_forbidden"}, 403)
    except Exception:
        return _reply({"error": "admission_unavailable"}, 503)
    keyring, expected_origin = authority
    return await serve_admission_batch(
        request,
        db.session_factory,
        keyring=keyring,
        expected_origin=expected_origin,
        trusted_now=datetime.now(timezone.utc),
    )


async def serve_admission_batch(
    request,
    session_factory,
    *,
    keyring: AdmissionKeyring,
    expected_origin: str,
    trusted_now: datetime,
):
    """Serve one batch; unavailable outcomes require retained-state reconciliation."""

    try:
        verified = verify_request(
            headers=_contract_headers(request.headers),
            body=request.body,
            method=request.method,
            path=request.path,
            query_string=request.query_string,
            trusted_now=trusted_now,
            expected_origin=expected_origin,
            keyring=keyring,
        )
        extend_batch_response_deadline(
            request,
            expires_at=min(verified.permit.expires_at, trusted_now + timedelta(seconds=5)),
            trusted_now=trusted_now,
        )
        retained = await _retained_request(session_factory, verified)
        extend_batch_response_deadline(
            request,
            expires_at=min(
                verified.permit.expires_at,
                retained.build_deadline_at,
                trusted_now + timedelta(seconds=retained.lease_seconds),
            ),
            trusted_now=trusted_now,
        )
        admission_result = await admission_sql.admit_source_batch(
            session_factory,
            retained,
            verified.pins.build_id,
            verified.pins.expected_after_occurrence_id,
            admission_permit=verified.permit,
        )
        return _reply(_receipt(verified, admission_result))
    except AdmissionAuthorizationError:
        return _reply({"error": "admission_forbidden"}, 403)
    except LeaseAuthorityLost, CancellationRequested:
        return _reply({"error": "admission_authority_lost"}, 409)
    except admission_sql.AdmissionError as error:
        if error.args == ("custom_import_build_progress_conflict",):
            return _reply({"error": "admission_progress_conflict"}, 409)
        return _reply({"error": "admission_unavailable"}, 503)
    except Exception:
        return _reply({"error": "admission_unavailable"}, 503)
