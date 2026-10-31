# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep request session cleanup in the task that acquired its context tokens."""

from contextlib import asynccontextmanager

from sanic import Sanic

from db.connection import db


@asynccontextmanager
async def _request_session_cleanup(request):
    failure = None
    try:
        yield
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            await db._close_request_session(request, success=False)
        except BaseException as cleanup_failure:
            if failure is None:
                raise
            failure.add_note("Request session cleanup failed: " + type(cleanup_failure).__name__)


class RequestLifecycleSanic(Sanic):
    """Retain response middleware's commit decision and roll back orphaned sessions."""

    async def handle_request(self, request):
        """Close any session left behind by a failed or cancelled request."""
        async with _request_session_cleanup(request):
            await super().handle_request(request)

    async def handle_exception(self, request, exception, run_middleware=True):
        """Cover exception handling entered directly by either transport."""
        async with _request_session_cleanup(request):
            await super().handle_exception(request, exception, run_middleware)
