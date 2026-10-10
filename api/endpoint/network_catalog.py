# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected approved network catalog reads in request-owned snapshots."""

import hashlib
import json
import re
import unicodedata

import asyncpg
from sanic import Blueprint, response
from sanic.exceptions import Forbidden
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_control_auth
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_network_catalog_read import (
    MAX_EXCLUSIONS,
    MAX_RESPONSE_BYTES,
    RegistryNetworkCatalogError,
    RegistryNetworkCatalogLegacySelector,
    RegistryNetworkCatalogQuery,
    read_registry_network_catalog,
    read_registry_network_catalog_detail,
    resolve_registry_network_catalog_legacy,
)

blueprint = Blueprint("network_catalog", url_prefix="/registry/serving", version=1)
_HEADERS = {"Cache-Control": "private, no-store"}
_SOURCE_FIELDS = {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"}
_CATALOG_PATHS = {
    "/api/v1/registry/serving/catalog",
    "/api/v1/registry/serving/catalog/detail",
    "/api/v1/registry/serving/catalog/legacy",
}


def register_network_catalog_private_responses(app):
    """Cover framework refusals before a blueprint route has been selected."""
    app.register_middleware(private_catalog_response, "response")


async def private_catalog_response(request, http_response):
    """Apply catalog cache policy to the three exact paths, including method errors."""
    if request.path in _CATALOG_PATHS:
        http_response.headers.update(_HEADERS)


@blueprint.middleware("response")
async def private_response(_request, http_response):
    """Keep every catalog response private, including framework errors."""
    http_response.headers.update(_HEADERS)


def _error(status):
    code = {
        400: "network_catalog_request_invalid",
        403: "network_catalog_forbidden",
        404: "network_catalog_not_found",
        503: "network_catalog_unavailable",
    }[status]
    return response.json({"error": {"code": code}}, status=status, headers=_HEADERS)


def _decimal(value, minimum):
    if type(value) is not str or re.fullmatch(r"0|[1-9][0-9]{0,18}", value) is None:
        raise ValueError("invalid decimal")
    number = int(value)
    if not minimum <= number <= 9223372036854775807:
        raise ValueError("invalid decimal")
    return number


def _integer(value, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError("invalid integer")
    return value


def _object(value, *, allowed, required=()):
    if type(value) is not dict or set(value) - allowed or not set(required) <= set(value):
        raise ValueError("invalid object")
    return value


def _unique_pairs(pairs):
    document_by_field = {}
    for key, value in pairs:
        if key in document_by_field:
            raise ValueError("duplicate field")
        document_by_field[key] = value
    return document_by_field


def _invalid_constant(_value):
    raise ValueError("invalid constant")


def _source(value):
    return RegistryNetworkSourceCoordinates(**_object(value, allowed=_SOURCE_FIELDS, required=_SOURCE_FIELDS))


def _policy_parameters(document_by_field):
    """Validate the server-supplied policy with the gateway client identity format."""
    client_id = document_by_field["client_id"]
    if (
        type(client_id) is not str
        or not client_id
        or len(client_id) > 64
        or client_id.strip() != client_id
        or any(unicodedata.category(char) == "Cc" for char in client_id)
    ):
        raise ValueError("invalid client")
    client_id.encode("utf-8")
    _decimal(document_by_field["policy_revision"], 0)
    excluded_ids = document_by_field["excluded_network_ids"]
    if type(excluded_ids) is not list or len(excluded_ids) > MAX_EXCLUSIONS:
        raise ValueError("invalid exclusions")
    previous = 0
    for excluded_id in excluded_ids:
        _integer(excluded_id, 2147483647)
        if excluded_id <= previous:
            raise ValueError("invalid exclusions")
        previous = excluded_id
    policy_by_field = {key: document_by_field[key] for key in ("client_id", "policy_revision", "excluded_network_ids")}
    scope = hashlib.sha256(
        json.dumps(policy_by_field, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return policy_by_field, tuple(excluded_ids), scope


def _query_selector(query_by_field, kind):
    """Keep canonical and legacy selectors distinct and source coordinates complete."""
    common_fields = {"network_generation"}
    if kind == "list":
        _object(query_by_field, allowed=common_fields | {"limit", "offset", "search", "archived", "source"})
    elif kind == "detail":
        _object(query_by_field, allowed=common_fields | {"network_id"}, required={"network_id"})
    else:
        _object(
            query_by_field,
            allowed=common_fields | {"namespace", "value", "source_scope", "source"},
            required={"namespace", "value", "source_scope", "source"},
        )
    generation = _decimal(query_by_field["network_generation"], 1) if "network_generation" in query_by_field else None
    if kind == "list":
        if "search" in query_by_field and type(query_by_field["search"]) is not str:
            raise ValueError("invalid search")
        selector = RegistryNetworkCatalogQuery(
            limit=query_by_field.get("limit", 25),
            offset=query_by_field.get("offset", 0),
            generation_id=generation,
            search=query_by_field.get("search"),
            archived=query_by_field.get("archived", False),
            source=_source(query_by_field["source"]) if "source" in query_by_field else None,
        )
    elif kind == "detail":
        selector = _integer(query_by_field["network_id"], 2147483647)
    else:
        if type(query_by_field["source_scope"]) is not dict:
            raise ValueError("invalid source scope")
        selector = RegistryNetworkCatalogLegacySelector(
            _source(query_by_field["source"]),
            query_by_field["namespace"],
            query_by_field["value"],
            json.dumps(
                query_by_field["source_scope"], allow_nan=False, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8"),
        )
    return selector, generation


def _parameters(request, kind):
    """Decode a closed UTF-8 policy envelope before obtaining a transaction."""
    if request.query_string or type(request.body) is not bytes or len(request.body) > MAX_RESPONSE_BYTES:
        raise ValueError("invalid body")
    document_by_field = json.loads(
        request.body.decode("utf-8"), object_pairs_hook=_unique_pairs, parse_constant=_invalid_constant
    )
    fields = {"client_id", "policy_revision", "excluded_network_ids", "query"}
    _object(document_by_field, allowed=fields, required=fields)
    policy_by_field, excluded_ids, scope = _policy_parameters(document_by_field)
    selector, generation = _query_selector(document_by_field["query"], kind)
    return policy_by_field, excluded_ids, selector, generation, scope


async def _catalog_response(request, kind):
    """Authorize first, then pin approved metadata in one read-only snapshot."""
    try:
        require_control_auth(request)
    except Forbidden:
        return _error(403)
    try:
        policy_by_field, excluded, selector, generation, scope = _parameters(request, kind)
    except ValueError, TypeError, UnicodeError, RecursionError:
        return _error(400)
    try:
        session = request.ctx.sa_session
        async with session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            connection = await session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            if kind == "list":
                page = await read_registry_network_catalog(driver, selector, excluded_network_ids=excluded)
            elif kind == "detail":
                page = await read_registry_network_catalog_detail(
                    driver, selector, excluded_network_ids=excluded, generation_id=generation
                )
            else:
                page = await resolve_registry_network_catalog_legacy(
                    driver, selector, excluded_network_ids=excluded, generation_id=generation
                )
        _decimal(page["generation"], 1)
        http_response = response.json(
            {"client_id": policy_by_field["client_id"], "policy_revision": policy_by_field["policy_revision"]} | page,
            headers=_HEADERS | {"X-Network-Generation": page["generation"], "X-Network-Access-Scope": scope},
        )
        if len(http_response.body) > MAX_RESPONSE_BYTES:
            return _error(503)
        return http_response
    except RegistryNetworkCatalogError as error:
        if str(error) == "registry_network_catalog_request_invalid":
            return _error(400)
        if str(error) == "registry_network_catalog_detail_denied":
            return _error(404)
        return _error(503)
    except asyncpg.PostgresError, asyncpg.InterfaceError, SQLAlchemyError, OSError, KeyError, TypeError, ValueError:
        return _error(503)


@blueprint.post("/catalog")
async def network_catalog_page(request):
    """Return a bounded page of authorized approved network metadata."""
    return await _catalog_response(request, "list")


@blueprint.post("/catalog/detail")
async def network_catalog_detail(request):
    """Return one explicit canonical network only when visible."""
    return await _catalog_response(request, "detail")


@blueprint.post("/catalog/legacy")
async def network_catalog_legacy(request):
    """Resolve one exact source-scoped legacy identity without canonical guessing."""
    return await _catalog_response(request, "legacy")
