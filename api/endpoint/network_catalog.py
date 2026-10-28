# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected approved network catalog reads in request-owned snapshots."""

import hashlib
import json
import re
import unicodedata

import asyncpg
from sanic import Blueprint, response
from sanic.exceptions import Forbidden, SanicException
from sanic.response import HTTPResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_registry_auth
from api.network_catalog_fhir_query import FHIRCatalogSelection, parse_fhir_catalog_query
from api.provider_collection_network_contract import parse_managed_collection_query
from api.provider_collection_network_read import ProviderCollectionServiceRefusal, read_managed_provider_collection
from api.provider_detail_network_contract import parse_managed_detail_query
from api.provider_detail_network_read import MAX_DETAIL_BYTES, ManagedProviderDetailAbsent, read_managed_provider_detail
from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_directory_managed_read import parse_managed_directory_query, read_managed_directory
from api.provider_profile_network_contract import parse_managed_profile_query
from api.provider_profile_network_read import read_managed_provider_profile
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_catalog_policy import POLICY_V2, RegistryOrganizationVisibility
from process.registry_fhir_catalog_read import read_fhir_catalog
from process.registry_network_catalog_read import (
    MAX_EXCLUSIONS,
    MAX_RESPONSE_BYTES,
    RegistryNetworkAutocompleteQuery,
    RegistryNetworkCatalogError,
    RegistryNetworkCatalogLegacyBatchSelector,
    RegistryNetworkCatalogLegacySelector,
    RegistryNetworkCatalogQuery,
    read_registry_network_autocomplete,
    read_registry_network_catalog,
    read_registry_network_catalog_detail,
    resolve_registry_network_catalog_legacy,
)
from process.registry_provider_plan_read import RegistryProviderPlansQuery, read_registry_provider_plans
from process.registry_serving_organization_read import (
    RegistryOrganizationCatalogQuery,
    read_registry_serving_organizations,
)

blueprint = Blueprint("network_catalog", url_prefix="/registry/serving", version=1)
_HEADERS = {"Cache-Control": "private, no-store"}
_SOURCE_FIELDS = {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"}
_MANAGED_PROVIDER_KINDS = {"provider-profile", "provider-detail", "provider-collection"}
_CATALOG_PATHS = {
    "/api/v1/registry/serving/catalog",
    "/api/v1/registry/serving/catalog/detail",
    "/api/v1/registry/serving/catalog/legacy",
    "/api/v1/registry/serving/organizations",
    "/api/v1/registry/serving/organizations/detail",
    "/api/v1/registry/serving/providers/plans",
    "/api/v1/registry/serving/providers/profile",
    "/api/v1/registry/serving/providers/detail",
    "/api/v1/registry/serving/providers/collection",
    "/api/v1/registry/serving/directory/entities",
}


def register_network_catalog_private_responses(app):
    """Cover framework refusals before a blueprint route has been selected."""
    app.register_middleware(private_catalog_response, "response")


async def private_catalog_response(request, http_response):
    """Keep protected catalog paths and autocomplete POST refusals private."""
    path = request.path.removesuffix("/")
    if path in _CATALOG_PATHS or (request.method == "POST" and path == "/api/v1/plan/network/autocomplete"):
        http_response.headers.update(_HEADERS)


@blueprint.middleware("response")
async def private_response(_request, http_response):
    """Keep every catalog response private, including framework errors."""
    http_response.headers.update(_HEADERS)


def _error(status, scope=None):
    code = {
        400: "network_catalog_request_invalid",
        403: "network_catalog_forbidden",
        404: "network_catalog_not_found",
        409: "network_catalog_source_changed",
        503: "network_catalog_unavailable",
    }[status]
    return response.json(
        {"error": {"code": code}},
        status=status,
        headers=_HEADERS | ({"X-Network-Access-Scope": scope} if scope else {}),
    )


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
    if "contract_version" in document_by_field:
        return _v2_policy_parameters(document_by_field, excluded_ids)
    scope = hashlib.sha256(
        json.dumps(policy_by_field, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return policy_by_field, tuple(excluded_ids), scope


def _v2_policy_parameters(document_by_field, excluded_ids):
    if (
        document_by_field["contract_version"] != POLICY_V2
        or document_by_field["organization_visibility_default"] != "system_wide"
    ):
        raise ValueError("invalid policy version")
    for name in ("excluded_company_ids", "excluded_group_ids"):
        if type(document_by_field[name]) is not list:
            raise ValueError("invalid exclusions")
    RegistryOrganizationVisibility(
        tuple(document_by_field["excluded_company_ids"]), tuple(document_by_field["excluded_group_ids"])
    )
    policy_by_field = {key: entry for key, entry in document_by_field.items() if key != "query"}
    normalized_by_field = {key: entry for key, entry in policy_by_field.items() if key != "access_scope_sha256"}
    normalized_by_field["policy_revision"] = _decimal(policy_by_field["policy_revision"], 0)
    scope = hashlib.sha256(
        json.dumps(normalized_by_field, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    if document_by_field["access_scope_sha256"] != scope:
        raise ValueError("invalid policy digest")
    return policy_by_field, tuple(excluded_ids), scope


def _organization_selector(query_by_field, kind):
    common_fields = {"network_generation"}
    required_fields = {"record_kind"} | ({"record_id"} if kind == "organization-detail" else set())
    allowed_fields = required_fields | common_fields | ({"limit", "offset"} if kind == "organization-list" else set())
    _object(query_by_field, allowed=allowed_fields, required=required_fields)
    if type(query_by_field["record_kind"]) is not str or query_by_field["record_kind"] not in {"company", "group"}:
        raise ValueError("invalid organization kind")
    generation = _decimal(query_by_field["network_generation"], 1) if "network_generation" in query_by_field else None
    selector_by_field = {"record_kind": query_by_field["record_kind"]}
    if kind == "organization-detail":
        from process.registry_network_catalog_read import _uuid

        selector_by_field["record_ids"] = (_uuid(query_by_field["record_id"]),)
        selector_by_field.update(limit=1, offset=0)
    else:
        selector_by_field.update(
            limit=_integer(query_by_field.get("limit", 50), 100),
            offset=_decimal(str(query_by_field.get("offset", 0)), 0),
        )
        if type(query_by_field.get("offset", 0)) is not int or selector_by_field["offset"] > 1000000:
            raise ValueError("invalid organization offset")
    return selector_by_field, generation


def _autocomplete_selector(query_by_field):
    """Keep discovery inputs closed without fabricating selected network IDs."""
    _object(
        query_by_field,
        allowed={"query", "limit", "zip_code", "network_generation"},
        required={"query", "limit"},
    )
    if "zip_code" in query_by_field and type(query_by_field["zip_code"]) is not str:
        raise ValueError("invalid ZIP")
    generation = _decimal(query_by_field["network_generation"], 1) if "network_generation" in query_by_field else None
    return RegistryNetworkAutocompleteQuery(
        query=query_by_field["query"],
        limit=query_by_field["limit"],
        zip_code=query_by_field.get("zip_code"),
        generation_id=generation,
    ), generation


def _provider_plans_selector(query_by_field):
    """Keep complete office-qualified reads independent of public pagination."""
    _object(query_by_field, allowed={"npi", "year", "network_generation"}, required={"npi"})
    if "year" in query_by_field and type(query_by_field["year"]) is not int:
        raise ValueError("invalid year")
    generation = _decimal(query_by_field["network_generation"], 1) if "network_generation" in query_by_field else None
    return RegistryProviderPlansQuery(query_by_field["npi"], query_by_field.get("year"), generation), generation


def _provider_selector(query_by_field, kind):
    """Preserve each provider read's closed query contract."""
    if kind == "provider-plans":
        return _provider_plans_selector(query_by_field)
    if kind == "provider-profile":
        return parse_managed_profile_query(query_by_field), None
    if kind == "provider-detail":
        return parse_managed_detail_query(query_by_field), None
    if kind == "provider-collection":
        return parse_managed_collection_query(query_by_field), None
    raise ValueError("invalid provider read")


def _validate_catalog_query_fields(query_by_field, kind):
    """Require each catalog selector's closed fields before decoding its values."""
    common_fields = {"network_generation"}
    if kind == "list":
        _object(
            query_by_field, allowed=common_fields | {"limit", "offset", "search", "archived", "source", "benefit_code"}
        )
    elif kind == "detail":
        _object(query_by_field, allowed=common_fields | {"network_id"}, required={"network_id"})
    elif "values" in query_by_field:
        _object(query_by_field, allowed=common_fields | {"namespace", "values"}, required={"namespace", "values"})
        if type(query_by_field["values"]) is not list:
            raise ValueError("invalid legacy batch")
    else:
        _object(
            query_by_field,
            allowed=common_fields | {"namespace", "value", "source_scope", "source"},
            required={"namespace", "value", "source_scope", "source"},
        )


def _query_selector(query_by_field, kind):
    """Keep canonical and legacy selectors distinct and source coordinates complete."""
    if type(query_by_field) is dict and "view" in query_by_field:
        return parse_fhir_catalog_query(query_by_field, kind)
    if kind == "directory":
        return parse_managed_directory_query(query_by_field), None
    if kind == "autocomplete":
        return _autocomplete_selector(query_by_field)
    if kind.startswith("provider-"):
        return _provider_selector(query_by_field, kind)
    if kind.startswith("organization"):
        return _organization_selector(query_by_field, kind)
    _validate_catalog_query_fields(query_by_field, kind)
    generation = _decimal(query_by_field["network_generation"], 1) if "network_generation" in query_by_field else None
    if kind == "list":
        if "search" in query_by_field and type(query_by_field["search"]) is not str:
            raise ValueError("invalid search")
        if "benefit_code" in query_by_field and type(query_by_field["benefit_code"]) is not str:
            raise ValueError("invalid declared benefit type")
        selector = RegistryNetworkCatalogQuery(
            limit=query_by_field.get("limit", 25),
            offset=query_by_field.get("offset", 0),
            generation_id=generation,
            search=query_by_field.get("search"),
            archived=query_by_field.get("archived", False),
            benefit_code=query_by_field.get("benefit_code"),
            source=_source(query_by_field["source"]) if "source" in query_by_field else None,
        )
    elif kind == "detail":
        selector = _integer(query_by_field["network_id"], 2147483647)
    elif "values" in query_by_field:
        selector = RegistryNetworkCatalogLegacyBatchSelector(
            query_by_field["namespace"], tuple(query_by_field["values"])
        )
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
    if type(document_by_field) is dict and "contract_version" in document_by_field:
        fields |= {
            "contract_version",
            "organization_visibility_default",
            "excluded_company_ids",
            "excluded_group_ids",
            "access_scope_sha256",
        }
    elif kind.startswith("organization") or kind in {
        "autocomplete",
        "provider-plans",
        "provider-profile",
        "provider-detail",
        "provider-collection",
        "directory",
    }:
        raise ValueError("full policy version required")
    _object(document_by_field, allowed=fields, required=fields)
    policy_by_field, excluded_ids, scope = _policy_parameters(document_by_field)
    selector, generation = _query_selector(document_by_field["query"], kind)
    if isinstance(selector, FHIRCatalogSelection) and policy_by_field.get("contract_version") != POLICY_V2:
        raise ValueError("full policy version required")
    return policy_by_field, excluded_ids, selector, generation, scope


async def _read_catalog_page(driver, kind, selector, generation, excluded, options):
    if isinstance(selector, FHIRCatalogSelection):
        return await read_fhir_catalog(
            driver, selector.query, excluded_network_ids=excluded, network_id=selector.network_id, **options
        )
    if kind == "autocomplete":
        return await read_registry_network_autocomplete(driver, selector, excluded_network_ids=excluded)
    if kind == "provider-plans":
        return await read_registry_provider_plans(driver, selector, excluded_network_ids=excluded)
    if kind.startswith("organization"):
        return await read_registry_serving_organizations(
            driver, RegistryOrganizationCatalogQuery(**selector, generation_id=generation), **options
        )
    if kind == "list":
        return await read_registry_network_catalog(driver, selector, excluded_network_ids=excluded, **options)
    reader = read_registry_network_catalog_detail if kind == "detail" else resolve_registry_network_catalog_legacy
    return await reader(driver, selector, excluded_network_ids=excluded, generation_id=generation, **options)


async def _read_managed_provider_page(request, kind, selector, excluded, scope, options):
    """Keep provider composition on its request-owned Reader snapshot."""
    if kind == "provider-profile":
        return await read_managed_provider_profile(selector, excluded_network_ids=excluded, access_scope_sha256=scope)
    reader = read_managed_provider_detail if kind == "provider-detail" else read_managed_provider_collection
    return await reader(request, selector, excluded_network_ids=excluded, access_scope_sha256=scope, **options)


def _detail_absence_response(error, kind, visibility, policy_by_field, scope):
    """Emit only native absence retained from the already-authorized read scope."""
    if kind != "provider-detail" or visibility is None or error.proof["access_scope_sha256"] != scope:
        return _error(503, scope)
    return response.json(
        {"client_id": policy_by_field["client_id"], "policy_revision": policy_by_field["policy_revision"]}
        | error.proof
        | {"error": {"code": "provider_detail_not_found"}},
        status=404,
        headers=_HEADERS
        | {
            "X-Network-Generation": error.proof["generation"],
            "X-Network-Access-Scope": scope,
            "X-Network-Manifest-SHA256": error.proof["manifest_sha256"],
        },
    )


async def _read_catalog_snapshot(request, kind, selector, generation, excluded, options):
    """Read the selected catalog within one request-owned, read-only snapshot."""
    session = request.ctx.sa_session
    async with session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text("SET LOCAL lock_timeout='1s'"))
        connection = await session.connection()
        driver = (await connection.get_raw_connection()).driver_connection
        return await _read_catalog_page(driver, kind, selector, generation, excluded, options)


def _catalog_response_headers(selector, page, scope):
    headers = _HEADERS | {"X-Network-Access-Scope": scope}
    if page["generation"] is None:
        if not isinstance(selector, FHIRCatalogSelection) or page["approved_custom_revision"] is not None:
            raise ValueError("invalid unattached metadata catalog response")
    else:
        _decimal(page["generation"], 1)
        headers["X-Network-Generation"] = page["generation"]
    return headers


async def _catalog_response(request, kind):
    """Authorize first, then pin approved metadata in one read-only snapshot."""
    try:
        require_registry_auth(request)
    except Forbidden:
        return _error(403)
    try:
        policy_by_field, excluded, selector, generation, scope = _parameters(request, kind)
    except DirectoryReadError as error:
        return _error(error.status)
    except ValueError, TypeError, UnicodeError, RecursionError:
        return _error(400)
    try:
        visibility = _catalog_visibility(policy_by_field)
        options = {} if visibility is None else {"organization_policy": visibility}
        if isinstance(selector, FHIRCatalogSelection):
            options["access_scope_sha256"] = scope
        if kind == "directory":
            return await read_managed_directory(
                request, selector, policy=policy_by_field, exclusions=excluded, access_scope_sha256=scope
            )
        if kind in _MANAGED_PROVIDER_KINDS:
            if kind == "provider-collection" and selector.kind == "billing-search":
                options["client_id"] = policy_by_field["client_id"]
            page = await _read_managed_provider_page(request, kind, selector, excluded, scope, options)
            if isinstance(page, HTTPResponse):
                page.headers.update(_HEADERS | {"X-Network-Access-Scope": scope})
                return page if len(page.body) <= MAX_RESPONSE_BYTES else _error(503, scope)
        else:
            page = await _read_catalog_snapshot(request, kind, selector, generation, excluded, options)
        if kind == "organization-detail" and (page["total"] != 1 or len(page["items"]) != 1):
            return _error(404, scope)
        http_response = response.json(
            {"client_id": policy_by_field["client_id"], "policy_revision": policy_by_field["policy_revision"]} | page,
            headers=_catalog_response_headers(selector, page, scope),
        )
        maximum = MAX_DETAIL_BYTES if kind in {"provider-detail", "provider-collection"} else MAX_RESPONSE_BYTES
        error_scope = scope if visibility is not None else None
        return http_response if len(http_response.body) <= maximum else _error(503, error_scope)
    except ProviderCollectionServiceRefusal as error:
        if visibility is not None and kind == "provider-collection" and selector.kind == "by-service":
            return response.json(error.document, status=422, headers=_HEADERS | {"X-Network-Access-Scope": scope})
        return _error(503, scope if visibility is not None else None)
    except ManagedProviderDetailAbsent as error:
        return _detail_absence_response(error, kind, visibility, policy_by_field, scope)
    except RegistryNetworkCatalogError as error:
        return _catalog_reader_error(
            error, kind, visibility, scope, enriched=isinstance(selector, FHIRCatalogSelection)
        )
    except DirectoryReadError as error:
        return _directory_error(error, selector, scope) if kind == "directory" else _error(error.status, scope)
    except SanicException as error:
        if error.status_code == 409 and visibility is not None and kind in _MANAGED_PROVIDER_KINDS:
            return _error(409, scope)
        return _error(error.status_code if error.status_code in {400, 403, 404, 503} else 503, scope)
    except asyncpg.PostgresError, asyncpg.InterfaceError, SQLAlchemyError, OSError, KeyError, TypeError, ValueError:
        return _error(503, scope if visibility is not None else None)


def _catalog_reader_error(error, kind, visibility, scope, *, enriched=False):
    code = str(error)
    if code == "registry_network_catalog_source_changed" and (
        enriched
        or kind == "directory"
        or (visibility is not None and (kind == "legacy" or kind in _MANAGED_PROVIDER_KINDS))
    ):
        return _error(409, scope)
    status = {
        "registry_network_catalog_request_invalid": 400,
        "registry_network_catalog_detail_denied": 404,
    }.get(code, 503)
    return _error(status, scope if visibility is not None else None)


def _catalog_visibility(policy):
    if "contract_version" not in policy:
        return None
    return RegistryOrganizationVisibility(tuple(policy["excluded_company_ids"]), tuple(policy["excluded_group_ids"]))


def _directory_error(error, query, scope):
    if error.status == 404 and query.shape != "entities":
        return response.json(
            {"error": {"code": "provider_directory_entity_not_found"}},
            status=404,
            headers=_HEADERS | {"X-Network-Access-Scope": scope},
        )
    return _error(error.status, scope)


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


@blueprint.post("/organizations")
async def organization_catalog_page(request):
    """Return default-visible organizations from the pinned serving approval."""
    return await _catalog_response(request, "organization-list")


@blueprint.post("/organizations/detail")
async def organization_catalog_detail(request):
    """Return one exact independently authorized organization."""
    return await _catalog_response(request, "organization-detail")


@blueprint.post("/providers/plans")
async def provider_plans(request):
    """Read source-qualified plans at authorized exact provider offices."""
    return await _catalog_response(request, "provider-plans")


@blueprint.post("/providers/profile")
async def provider_profile(request):
    """Compose source-qualified profiles under the verified current policy."""
    return await _catalog_response(request, "provider-profile")


@blueprint.post("/providers/detail")
async def provider_detail(request):
    """Compose native detail and exact memberships under one verified policy."""
    return await _catalog_response(request, "provider-detail")


@blueprint.post("/providers/collection")
async def provider_collection(request):
    """Read a provider collection under one verified policy and serving snapshot."""
    return await _catalog_response(request, "provider-collection")


@blueprint.post("/directory/entities")
async def directory_entities(request):
    """Read original Source entities under the authenticated complete default policy."""
    return await _catalog_response(request, "directory")
