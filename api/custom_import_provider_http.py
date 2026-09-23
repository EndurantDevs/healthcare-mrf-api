# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed, pinned composition of imported fields with native provider search."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import text

from api import custom_import_read_http as transport
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    MAX_FILTER_TERMS,
    MAX_NPI_PAGE_SIZE,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
)
from process.custom_import.read_core import (
    CustomImportReadService,
    NpiEntityRelationQuery,
    ReadFilter,
    ReadOrderTerm,
    _bounded_read_window,
    _validate_npi_page,
)
from process.custom_import.read_identity import verify_published_generation

CUSTOM_IMPORT_PROVIDERS_PATH = "/api/v1/extensions/custom-import/providers"

_PROVIDER_QUERY_FIELDS = frozenset(
    "q name_like first_name last_name organization_name npi phone address_key address_site_key "
    "zip_code postal_code city state classification specialization section display_name codes primary_only "
    "entity_type_code provider_sex_code plan_network has_insurance plan_release_id procedure_codes "
    "procedure_code_system medication_codes medication_code_system year page page_size limit start offset "
    "order_by include_total include_sources include_evidence debug show view".split()
)


@dataclass(frozen=True, slots=True)
class _ParsedProviderRequest:
    target: transport._TransportTarget
    native_args: RequestParameters
    context: tuple[ReadFilter, ...]
    filters: tuple[ReadFilter, ...]
    order_terms: tuple[ReadOrderTerm, ...] | None
    require_match: bool


def _provider_relation_query(
    parsed: _ParsedProviderRequest,
    *,
    require_exact_context: bool = False,
) -> NpiEntityRelationQuery:
    """Bind one provider body to the read-core relation input."""

    return NpiEntityRelationQuery(
        context_filters=parsed.context,
        filters=parsed.filters,
        order_terms=parsed.order_terms,
        require_match=parsed.require_match,
        require_exact_context=require_exact_context,
    )


def _parse_native_query(document: object) -> RequestParameters:
    """Keep native parsing while closing the signed provider-page input shape."""

    if type(document) is not dict or not set(document) <= _PROVIDER_QUERY_FIELDS:
        raise CustomImportReadRequestError("native provider query is invalid")
    values_by_name: dict[str, list[str]] = {}
    for name, value in document.items():
        values = value if name == "name_like" and type(value) is list else [value]
        if not 1 <= len(values) <= 16 or any(type(item) is not str or len(item) > 2048 for item in values):
            raise CustomImportReadRequestError("native provider query is invalid")
        values_by_name[name] = list(values)
    args = RequestParameters(values_by_name)
    if str(args.get("view", "")).strip().lower() == "sitemap":
        raise CustomImportReadRequestError("provider-page response is required")
    if "include_total" in document and args.get("include_total").strip().lower() not in {"1", "true", "yes", "on"}:
        raise CustomImportReadRequestError("exact provider totals are required")
    args["include_total"] = ["true"]
    return args


def _parse_provider_request(body: bytes, *, native_query_parser=_parse_native_query) -> _ParsedProviderRequest:
    """Separate context selectors from whether imported membership is required."""

    document = transport._strict_json(body)
    if (
        type(document) is not dict
        or set(document) != {"target", "native_query", "context", "filters", "order", "require_match"}
        or transport._canonical_json_bytes(document) != body
    ):
        raise transport._fail()
    if type(document["require_match"]) is not bool:
        raise CustomImportReadRequestError("imported membership mode is invalid")
    order = None if document["order"] is None else transport._parse_order_documents(document["order"])
    if order is None and not document["require_match"]:
        raise CustomImportReadRequestError("order-only queries require imported ordering")
    context = transport._parse_filter_documents(document["context"])
    filters = transport._parse_filter_documents(document["filters"])
    if len(context) + len(filters) > MAX_FILTER_TERMS:
        raise CustomImportReadRequestError("filter count exceeds the read-core limit")
    if any(predicate.operator != "eq" or predicate.value is None for predicate in context):
        raise CustomImportReadRequestError("context selectors are invalid")
    if any(predicate.operator not in {"eq", "gt", "lt"} or predicate.value is None for predicate in filters):
        raise CustomImportReadRequestError("metric filters are invalid")
    return _ParsedProviderRequest(
        target=transport._parse_target(document["target"]),
        native_args=native_query_parser(document["native_query"]),
        context=context,
        filters=filters,
        order_terms=order,
        require_match=document["require_match"],
    )


async def serve_custom_import_providers(request: Any, session: Any):
    """Run one authorized provider page in a single bounded read snapshot."""

    if (
        getattr(request, "method", None) != "POST"
        or getattr(request, "path", None) != CUSTOM_IMPORT_PROVIDERS_PATH
        or getattr(request, "query_string", "")
    ):
        return transport._error(404)
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= transport._MAX_BODY_BYTES:
            raise transport._fail()
        parsed = _parse_provider_request(body)
        verified = transport._verify_provider_transport(
            headers=request.headers,
            body=body,
            request=parsed,
            trusted_now=transport._trusted_now(),
            keyring=transport._keyring(),
            path=CUSTOM_IMPORT_PROVIDERS_PATH,
        )
        if session is None or session.in_transaction():
            raise CustomImportReadUnavailableError("fresh provider read session is required")
        async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000), session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            async with _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS):
                provider_payload = await _read_provider_payload(request, session, parsed, verified)
                encoded = transport._canonical_json_bytes(provider_payload)
                if len(encoded) > transport._MAX_RESPONSE_BYTES:
                    raise CustomImportReadUnavailableError("provider response is unavailable")
                return transport._response(encoded, 200)
    except Exception as failure:
        transport._log_failure(failure)
        status = 400 if isinstance(failure, InvalidUsage) else transport._failure_status(failure)
        return transport._error(status)


async def _read_provider_payload(request, session, parsed, verified):
    """Compose and hydrate one page inside the caller's read snapshot."""

    pinned_target = await transport._resolve_pinned_target(session, parsed.target)
    service = CustomImportReadService(authorizer=transport._TransportAuthorizer(verified, pinned_target))
    authorization = ExtensionReadAuthorization(verified.credential)
    query = _provider_relation_query(parsed)
    prepared = await service.prepare_npi_entity_relation(
        session,
        authorization=authorization,
        target=pinned_target,
        query=query,
    )
    context = ProviderImportQuery(prepared, compile_npi_entity_relation(prepared.statement), query.require_match)
    reply = await _provider_page(request, native_args=parsed.native_args, import_context=context)
    if reply.status != 200 or len(reply.body) > transport._MAX_RESPONSE_BYTES:
        raise CustomImportReadUnavailableError("provider response is unavailable")
    provider_payload = _provider_payload(reply.body)
    await _hydrate_provider_rows(
        session, service, authorization, pinned_target, prepared, parsed, query, provider_payload["rows"]
    )
    await verify_published_generation(session, pinned_target)
    return provider_payload


async def _hydrate_provider_rows(
    session, service, authorization, pinned_target, prepared, parsed, query, provider_rows
):
    """Hydrate each selected family once, including multiple addresses per NPI."""

    imported_items = await service.hydrate_npi_page(
        session,
        authorization=authorization,
        pinned_target=pinned_target,
        prepared=prepared,
        entity_values=tuple(dict.fromkeys(str(provider["npi"]) for provider in provider_rows)),
        query=query,
    )
    for provider in provider_rows:
        imported_item = imported_items.get(str(provider["npi"]))
        if imported_item is None and query.require_match:
            raise CustomImportReadUnavailableError("provider import match is unavailable")
        provider["custom_import"] = (
            None
            if imported_item is None
            else {
                "target": transport._target_document(parsed.target),
                **transport._search_item_payload(imported_item),
            }
        )


def _provider_payload(body: bytes) -> dict[str, Any]:
    """Reject malformed native pages before collecting bounded hydration keys."""

    payload = json.loads(body)
    if (
        type(payload) is not dict
        or type(payload.get("rows")) is not list
        or len(payload["rows"]) > MAX_NPI_PAGE_SIZE
        or any(
            type(provider) is not dict or type(provider.get("npi")) not in {int, str} for provider in payload["rows"]
        )
    ):
        raise CustomImportReadUnavailableError("provider response is unavailable")
    try:
        _validate_npi_page(tuple(str(provider["npi"]) for provider in payload["rows"]))
    except CustomImportReadRequestError:
        raise CustomImportReadUnavailableError("provider response is unavailable") from None
    return payload


async def _provider_page(request: Any, **kwargs: Any):
    """Load the native handler only after signed transport validation."""

    from api.endpoint.npi import list_providers

    return await list_providers(request, **kwargs)
