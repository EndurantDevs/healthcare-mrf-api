# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit exact current release reads through the internal HTTP listener."""

import re

from sanic import response
from sanic.exceptions import Forbidden, InvalidUsage, ServiceUnavailable

from db.connection import current_session, db, has_reader_session

PLAN_RELEASE_READ_BOUNDARY_HEADER = "X-Plan-Release-Read-Boundary"
PLAN_RELEASE_REVISION_SELECTOR = "plan_release_serving_revision_id"
PLAN_RELEASE_DIGEST_SELECTOR = "plan_release_binding_set_digest"
PLAN_RELEASE_ADMISSION_CHANGED = "plan_release_read_admission_changed"


def _admission_identity(args):
    """The private selectors are one complete, unambiguous tuple."""
    patterns = (
        ("plan_release_id", r"hprelease_[0-9A-HJKMNP-TV-Z]{26}"),
        (PLAN_RELEASE_REVISION_SELECTOR, r"hpserve_[0-9A-HJKMNP-TV-Z]{26}"),
        (PLAN_RELEASE_DIGEST_SELECTOR, r"[0-9a-f]{64}"),
    )
    values = []
    for name, pattern in patterns:
        candidates = args.getlist(name, [])
        if len(candidates) != 1 or re.fullmatch(pattern, candidates[0]) is None:
            raise InvalidUsage("invalid internal plan release admission")
        values.append(candidates[0])
    return tuple(values)


async def require_internal_plan_release_read(request):
    """Compare the routed tuple before serving in the same request Reader."""
    args = request.get_args(keep_blank_values=True)
    has_admission = PLAN_RELEASE_REVISION_SELECTOR in args or PLAN_RELEASE_DIGEST_SELECTOR in args
    if "plan_release_id" not in args and not has_admission:
        return None
    if request.headers.getall(PLAN_RELEASE_READ_BOUNDARY_HEADER, []) != ["internal"]:
        raise Forbidden("raw plan release selectors require the internal read listener")
    if has_admission:
        _admission_identity(args)
    session = getattr(request.ctx, "sa_session", None)
    if not _is_owned_reader(session):
        from sqlalchemy.exc import SQLAlchemyError

        try:
            session = await db.bind_request_reader(request)
        except RuntimeError, SQLAlchemyError:
            raise ServiceUnavailable("plan release admission requires the request Reader") from None
    return await admit_bound_plan_release_read(session, args)


def _is_owned_reader(session):
    return (
        session is not None
        and has_reader_session(db)
        and current_session() is session
        and session.info.get("api_reader_verified") is True
    )


async def admit_bound_plan_release_read(session, args):
    """Compare a selector tuple already authenticated by its internal transport."""
    if PLAN_RELEASE_REVISION_SELECTOR not in args and PLAN_RELEASE_DIGEST_SELECTOR not in args:
        return None
    release_id, revision_id, binding_digest = _admission_identity(args)
    if (
        session is None
        or not has_reader_session(db)
        or current_session() is not session
        or session.info.get("api_reader_verified") is not True
    ):
        raise ServiceUnavailable("plan release admission requires the request Reader")
    from api.plan_release_serving_resolution import resolve_plan_release_guard_selection

    selection = await resolve_plan_release_guard_selection(session, release_id)
    if (
        selection is None
        or selection.serving_revision_id != revision_id
        or selection.binding_set_digest != binding_digest
    ):
        return response.json(
            {"status": 409, "code": PLAN_RELEASE_ADMISSION_CHANGED, "message": PLAN_RELEASE_ADMISSION_CHANGED},
            status=409,
            headers={"Cache-Control": "private, no-store"},
        )
    return None
