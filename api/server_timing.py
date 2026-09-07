"""Application handling time through response middleware, before network writes."""

import time

from sanic.http.constants import Stage


SERVER_DURATION_HEADER = "X-HealthPorta-Server-Duration-Ns"


def register_server_timing(app):
    """Measure buffered JSON responses with the native Sanic lifecycle signals."""

    @app.signal("http.lifecycle.handle")
    async def begin(request):
        """Start before routing and request middleware on this request only."""
        request.ctx._healthporta_server_started_ns = time.perf_counter_ns()

    @app.signal("http.lifecycle.response")
    async def finish(request, response):
        """Include serialization and session cleanup; exclude transmission."""
        started = request.ctx.__dict__.pop("_healthporta_server_started_ns", None)
        if getattr(response.stream, "stage", None) is not Stage.HANDLER:
            return
        response.headers.popall(SERVER_DURATION_HEADER, None)
        content_type = response.headers.get("content-type", response.content_type) or ""
        if (
            type(started) is not int
            or not isinstance(response.body, bytes)
            or not response.body
            or content_type.partition(";")[0].strip().lower() != "application/json"
        ):
            return
        duration = time.perf_counter_ns() - started
        if duration >= 0:
            response.headers[SERVER_DURATION_HEADER] = str(duration)
