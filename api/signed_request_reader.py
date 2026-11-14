# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Post-authentication Reader ownership for signed extension transports."""

from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from uuid import UUID

from db.connection import current_session, db, has_reader_session
from process.custom_import.failure_diagnostics import _static_attribute
from process.custom_import.read_contracts import CustomImportReadUnavailableError


def is_owned_reader(session):
    """Require actual task/factory ownership, beyond a session-info marker."""
    return (
        session is not None
        and has_reader_session(db)
        and current_session() is session
        and session.info.get("api_reader_verified") is True
    )


def _request_uuid(value):
    if type(value) is UUID:
        return str(value)
    if type(value) is str and len(value) == 36:
        try:
            parsed = UUID(value)
        except ValueError:
            return None
        if str(parsed) == value.lower():
            return str(parsed)
    return None


def _remember_reader_failure(failure, request, session, database, *, boundary, verified_request_id=None):
    """Copy safe evidence before cleanup, without retaining the request or session."""
    try:
        if boundary not in {"signed_read", "claims_pin"}:
            return
        fields_by_name = {"boundary": boundary}
        headers = getattr(request, "headers", {}) or {}
        verified_id = _request_uuid(verified_request_id)
        forwarded_id = _request_uuid(headers.get("X-Request-ID") or headers.get("x-request-id"))
        core_id = _request_uuid(getattr(request, "id", None))
        fields_by_name.update(
            request_id=verified_id or forwarded_id or core_id,
            request_id_source="verified_transport"
            if verified_id
            else "forwarded"
            if forwarded_id
            else "core"
            if core_id
            else None,
            forwarded_request_id=forwarded_id,
            core_request_id=core_id,
        )
        failure._api_reader_diagnostic = fields_by_name
    except Exception:
        # Diagnostics cannot replace the original failure or cancellation.
        return


@contextmanager
def _bind_request_reader(request, session, *, verified_request_id=None):
    """Keep existing endpoint helpers on the same authenticated Reader."""
    missing = object()
    previous_context = getattr(request, "ctx", missing)
    context = SimpleNamespace() if previous_context is missing else previous_context
    previous_by_name = {name: getattr(context, name, missing) for name in ("sa_session", "session")}
    if previous_context is missing:
        request.ctx = context
    context.sa_session = context.session = session
    try:
        yield
    except BaseException as failure:
        _remember_reader_failure(
            failure, request, session, db, boundary="signed_read", verified_request_id=verified_request_id
        )
        raise
    finally:
        for name, previous in previous_by_name.items():
            if previous is missing:
                delattr(context, name)
            else:
                setattr(context, name, previous)
        if previous_context is missing:
            delattr(request, "ctx")


@asynccontextmanager
async def signed_request_reader(request, ordinary_session, *, plan_workspace=False, verified_request_id=None):
    """Preserve a cold caller and own all signed payload reads through encoding."""
    context_session = getattr(getattr(request, "ctx", None), "sa_session", ordinary_session)
    if (
        ordinary_session is None
        or context_session is not ordinary_session
        or ordinary_session.in_transaction()
        and not is_owned_reader(ordinary_session)
    ):
        raise CustomImportReadUnavailableError("signed read requires an unused caller session")
    try:
        async with db.reader_session(independent=True, prepare_plan_workspace=plan_workspace) as session:
            if not is_owned_reader(session):
                raise CustomImportReadUnavailableError("signed read Reader ownership is unavailable")
            with _bind_request_reader(request, session, verified_request_id=verified_request_id):
                if request.path.startswith("/api/v1/extensions/custom-import/providers"):
                    from api.reference_family_reads import pin_claims_reader

                    await pin_claims_reader(session, api_read=True)
                yield session
    except BaseException as failure:
        if type(_static_attribute(failure, "_api_reader_diagnostic")) is not dict:
            _remember_reader_failure(
                failure, request, None, db, boundary="signed_read", verified_request_id=verified_request_id
            )
        raise
