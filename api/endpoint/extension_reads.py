# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated generic extension-read routes."""

from __future__ import annotations

from typing import Any

from sanic import Blueprint

from api.custom_import_read_http import serve_custom_import_search

blueprint = Blueprint("custom_import", url_prefix="/extensions/custom-import", version=1)


def _session(request: Any) -> Any:
    return getattr(getattr(request, "ctx", None), "sa_session", None)


@blueprint.post("/search", name="custom_import.search")
async def search(request: Any):
    """Forward one extension search through the closed signed boundary."""

    return await serve_custom_import_search(request, _session(request))
