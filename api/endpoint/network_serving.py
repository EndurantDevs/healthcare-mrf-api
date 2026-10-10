# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated identity reads for current and explicitly retained network serving."""

from urllib.parse import parse_qs

import asyncpg
from sanic import Blueprint, response
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_control_auth
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest

blueprint = Blueprint("network_serving", url_prefix="/registry/serving", version=1)
_HEADERS = {"Cache-Control": "private, no-store"}


def _generation(request):
    if request.body or len(request.query_string) > 128:
        raise ValueError("network_serving_request_invalid")
    query_by_name = parse_qs(request.query_string, keep_blank_values=True, strict_parsing=True, max_num_fields=1)
    if set(query_by_name) - {"network_generation"}:
        raise ValueError("network_serving_request_invalid")
    if not query_by_name:
        return None
    generation = query_by_name["network_generation"][0]
    if not generation.isascii() or not generation.isdecimal() or len(generation) > 19 or generation.startswith("0"):
        raise ValueError("network_serving_generation_invalid")
    generation = int(generation)
    if not 1 <= generation <= 9223372036854775807:
        raise ValueError("network_serving_generation_invalid")
    return generation


def _error(status):
    code = "network_serving_request_invalid" if status == 400 else "network_serving_unavailable"
    return response.json({"error": {"code": code}}, status=status, headers=_HEADERS)


@blueprint.get("/manifest", ignore_body=False)
async def serving_manifest(request):
    """Return only one verified immutable identity through a read-only native pin."""
    require_control_auth(request)
    try:
        generation_id = _generation(request)
    except ValueError:
        return _error(400)
    try:
        session = request.ctx.sa_session
        async with session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            connection = await session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            manifest = await resolve_network_serving_manifest(driver, generation_id=generation_id)
        identity_by_name = {
            field: getattr(manifest, field)
            for field in (
                "generation_id",
                "schema_revision",
                "source_generations",
                "approved_custom_revision",
                "manifest_sha256",
            )
        }
        return response.json(
            identity_by_name,
            headers=_HEADERS | {"X-Network-Generation": str(manifest.generation_id)},
        )
    except NetworkServingReadUnavailable, asyncpg.PostgresError, SQLAlchemyError:
        return _error(503)
