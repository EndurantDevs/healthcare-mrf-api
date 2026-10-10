# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated provider identities and exact offices in pinned network serving."""

import re
from urllib.parse import parse_qs

import asyncpg
from sanic import Blueprint, response
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_control_auth
from api.network_address_scope import network_address_read_scope
from api.network_provider_read import (
    NetworkOfficeSelection,
    NetworkProviderNotFound,
    NetworkProviderReadError,
    NetworkProviderReadUnavailable,
    office_selection,
    read_network_provider_detail,
    read_network_provider_page,
)
from api.network_provider_read import (
    _selector as validate_provider_identity,
)
from process.network_serving_read import (
    NetworkServingReadUnavailable,
    parse_canonical_network_ids,
    resolve_network_serving_manifest,
)

blueprint = Blueprint("network_providers", url_prefix="/registry/serving", version=1)
_HEADERS = {"Cache-Control": "private, no-store"}


@blueprint.middleware("response")
async def private_response(_request, result):
    """Keep authentication failures and all provider results private."""
    result.headers.update(_HEADERS)


def _decimal(value, minimum, maximum):
    if type(value) is not str or re.fullmatch(r"0|[1-9][0-9]{0,18}", value) is None:
        raise ValueError("network_provider_request_invalid")
    number = int(value)
    if not minimum <= number <= maximum:
        raise ValueError("network_provider_request_invalid")
    return number


def _parameters(request, *, is_detail):
    """Validate the complete unique selector boundary before database access."""
    if request.body or len(request.query_string.encode("utf-8")) > 2048:
        raise ValueError("network_provider_request_invalid")
    allowed_names = {"network_ids", "network_generation", "location_id", "lat", "long", "radius_miles"}
    if not is_detail:
        allowed_names.update(("limit", "offset"))
    query_by_name = parse_qs(
        request.query_string, keep_blank_values=True, strict_parsing=True, max_num_fields=len(allowed_names)
    )
    if (
        set(query_by_name) - allowed_names
        or "network_ids" not in query_by_name
        or any(len(query_values) != 1 for query_values in query_by_name.values())
    ):
        raise ValueError("network_provider_request_invalid")
    access_scope_sha256 = request.headers.get("X-Network-Access-Scope")
    if access_scope_sha256 is not None and re.fullmatch(r"[0-9a-f]{64}", access_scope_sha256) is None:
        raise ValueError("network_provider_request_invalid")
    generation_id = (
        _decimal(query_by_name["network_generation"][0], 1, 9223372036854775807)
        if "network_generation" in query_by_name
        else None
    )
    office_filters_by_name = {
        field: float(query_by_name[field][0]) for field in ("lat", "long", "radius_miles") if field in query_by_name
    }
    if "location_id" in query_by_name:
        office_filters_by_name["location_id"] = query_by_name["location_id"][0]
    office_selection(**office_filters_by_name)
    return {
        "network_ids": parse_canonical_network_ids(query_by_name["network_ids"][0]),
        "generation_id": generation_id,
        "limit": _decimal(query_by_name.get("limit", ["50"])[0], 1, 100),
        "offset": _decimal(query_by_name.get("offset", ["0"])[0], 0, 1_000_000),
        "access_scope_sha256": access_scope_sha256,
        "office_filters": office_filters_by_name,
    }


def _error(status, generation_id=None):
    code_by_status = {
        400: "network_provider_request_invalid",
        404: "network_provider_not_found",
        503: "network_provider_unavailable",
    }
    headers = _HEADERS if generation_id is None else _HEADERS | {"X-Network-Generation": str(generation_id)}
    return response.json({"error": {"code": code_by_status[status]}}, status=status, headers=headers)


async def _provider_response(request, *, provider_system=None, provider_id=None):
    """Pin and read one complete result in a request-owned read-only transaction."""
    require_control_auth(request)
    is_detail = provider_system is not None
    try:
        parameters_by_name = _parameters(request, is_detail=is_detail)
        if is_detail:
            validate_provider_identity(provider_system, provider_id)
    except ValueError:
        return _error(400)
    try:
        session = request.ctx.sa_session
        async with session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            connection = await session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            manifest = await resolve_network_serving_manifest(driver, generation_id=parameters_by_name["generation_id"])
            with network_address_read_scope(
                manifest,
                parameters_by_name["network_ids"],
                access_scope_sha256=parameters_by_name["access_scope_sha256"],
            ) as scope:
                if is_detail:
                    provider_document = await read_network_provider_detail(
                        driver,
                        scope,
                        provider_system=provider_system,
                        provider_id=provider_id,
                        office_filters=NetworkOfficeSelection(**parameters_by_name["office_filters"]),
                    )
                else:
                    provider_document = await read_network_provider_page(
                        driver,
                        scope,
                        limit=parameters_by_name["limit"],
                        offset=parameters_by_name["offset"],
                        office_filters=NetworkOfficeSelection(**parameters_by_name["office_filters"]),
                    )
        return response.json(
            provider_document, headers=_HEADERS | {"X-Network-Generation": str(manifest.generation_id)}
        )
    except NetworkProviderReadError:
        return _error(400)
    except NetworkProviderNotFound:
        return _error(404, manifest.generation_id)
    except NetworkProviderReadUnavailable, NetworkServingReadUnavailable, asyncpg.PostgresError, SQLAlchemyError:
        return _error(503)


@blueprint.get("/providers", ignore_body=False)
async def network_provider_page(request):
    """Return a bounded page of exact identities and their selected offices."""
    return await _provider_response(request)


@blueprint.get("/providers/<provider_system>/<provider_id>", ignore_body=False)
async def network_provider_detail(request, provider_system, provider_id):
    """Return one explicit identity only when it has a selected office."""
    return await _provider_response(request, provider_system=provider_system, provider_id=provider_id)
