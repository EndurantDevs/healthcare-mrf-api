# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private upstream routes for generation-pinned provider-directory entities."""

from sanic import Blueprint, response
from sqlalchemy.exc import SQLAlchemyError

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


async def _read(request, kind, entity_id, shape):
    try:
        query = parse_directory_read(kind, entity_id, shape, request.query_string)
        reader = read_medical_groups if query.source_id == "cms-doctors" else read_cms_entities
        payload = await reader(request.ctx.sa_session, query)
        return response.json(payload, headers=_HEADERS)
    except DirectoryReadError as error:
        status = error.status
    except SQLAlchemyError:
        status = 503
    code, message = _ERRORS[status]
    return response.json({"error": {"code": code, "message": message}}, status=status, headers=_HEADERS)


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
