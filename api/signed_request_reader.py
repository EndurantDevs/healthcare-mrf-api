# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Post-authentication Reader ownership for signed extension transports."""

from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace

from db.connection import current_session, db, has_reader_session
from process.custom_import.read_contracts import CustomImportReadUnavailableError


def is_owned_reader(session):
    """Require actual task/factory ownership, beyond a session-info marker."""
    return (
        session is not None
        and has_reader_session(db)
        and current_session() is session
        and session.info.get("api_reader_verified") is True
    )


@contextmanager
def _bind_request_reader(request, session):
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
    finally:
        for name, previous in previous_by_name.items():
            if previous is missing:
                delattr(context, name)
            else:
                setattr(context, name, previous)
        if previous_context is missing:
            delattr(request, "ctx")


@asynccontextmanager
async def signed_request_reader(request, ordinary_session, *, plan_workspace=False):
    """Preserve a cold caller and own all signed payload reads through encoding."""
    context_session = getattr(getattr(request, "ctx", None), "sa_session", ordinary_session)
    if (
        ordinary_session is None
        or context_session is not ordinary_session
        or ordinary_session.in_transaction()
        and not is_owned_reader(ordinary_session)
    ):
        raise CustomImportReadUnavailableError("signed read requires an unused caller session")
    async with db.reader_session(independent=True, prepare_plan_workspace=plan_workspace) as session:
        if not is_owned_reader(session):
            raise CustomImportReadUnavailableError("signed read Reader ownership is unavailable")
        with _bind_request_reader(request, session):
            if request.path.startswith("/api/v1/extensions/custom-import/providers"):
                from api.reference_family_reads import pin_claims_reader

                await pin_claims_reader(session)
            yield session
