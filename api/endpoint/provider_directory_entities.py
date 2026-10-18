# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private upstream routes for generation-pinned provider-directory entities."""

import re
from dataclasses import replace

from sanic import Blueprint, response
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_control_auth
from api.provider_directory_cms_entities import read_cms_entities
from api.provider_directory_entities_contract import DirectoryReadError, parse_directory_read
from api.provider_directory_medical_groups import read_medical_groups

blueprint = Blueprint("provider_directory_entities", url_prefix="/provider-directory/entities", version=1)
_ERRORS = {
    400: ("provider_directory_query_invalid", "Provider directory query is invalid."),
    404: ("provider_directory_entity_not_found", "Provider directory entity not found."),
    409: ("provider_directory_generation_stale", "Provider directory pagination must restart."),
    503: ("provider_directory_serving_unavailable", "Provider directory serving is temporarily unavailable."),
}
_HEADERS = {"Cache-Control": "private, no-store"}


@blueprint.middleware("response")
async def private_response(_request, result):
    """Keep source and authorization failures out of shared caches."""
    result.headers.update(_HEADERS)


async def _read(request, kind, entity_id, shape):
    response_headers_by_name = dict(_HEADERS)
    try:
        query = parse_directory_read(kind, entity_id, shape, request.query_string)
        if query.network_ids is not None:
            require_control_auth(request)
            digest = request.headers.get("X-Network-Access-Scope")
            if digest is not None and re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise DirectoryReadError(400)
            query = replace(query, access_scope_sha256=digest)
            if query.source_id == "cms-doctors":
                raise DirectoryReadError(503)
            payload = await read_cms_entities(request.ctx.sa_session, query, response_headers=response_headers_by_name)
            return response.json(payload, headers=response_headers_by_name)
        reader = read_medical_groups if query.source_id == "cms-doctors" else read_cms_entities
        payload = await reader(request.ctx.sa_session, query)
        return response.json(payload, headers=_HEADERS)
    except DirectoryReadError as error:
        status = error.status
    except SQLAlchemyError:
        status = 503
    code, message = _ERRORS[status]
    return response.json({"error": {"code": code, "message": message}}, status=status, headers=response_headers_by_name)


@blueprint.get("/<kind>")
async def list_entities(request, kind):
    """Return one bounded page in the requested source generation."""
    return await _read(request, kind, None, "entities")


@blueprint.get("/<kind>/<entity_id>")
async def entity_detail(request, kind, entity_id):
    """Return one exact source-bound identity or the gateway not-found contract."""
    return await _read(request, kind, entity_id, "entity")


@blueprint.get("/<kind>/<entity_id>/relationships")
async def entity_relationships(request, kind, entity_id):
    """Return explicit source assertions without inferring target identities."""
    return await _read(request, kind, entity_id, "relationships")
