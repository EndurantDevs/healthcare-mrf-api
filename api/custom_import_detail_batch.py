# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed full-family hydration for an already selected native provider page."""

from __future__ import annotations

import asyncio

from sqlalchemy import text

from api import custom_import_read_http as transport
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    MAX_FULL_FAMILY_PAGE_SIZE,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
)
from process.custom_import.read_core import (
    CustomImportReadService,
    NpiEntityRelationQuery,
    _bounded_read_window,
    _validate_npi_page,
)
from process.custom_import.read_identity import verify_published_generation

CUSTOM_IMPORT_DETAIL_BATCH_PATH = "/api/v1/extensions/custom-import/detail/batch"


def _parse_batch_request(body):
    """Reuse detail selectors and admit only bounded unique exact NPI identities."""

    document = transport._strict_json(body)
    if type(document) is not dict or transport._canonical_json_bytes(document) != body:
        raise transport._fail()
    entities = document.pop("entities", None)
    if (
        "entity" in document
        or type(entities) is not dict
        or set(entities) != {"adapter_id", "values"}
        or entities["adapter_id"] != "npi"
        or type(entities["values"]) is not list
        or not 1 <= len(entities["values"]) <= MAX_FULL_FAMILY_PAGE_SIZE
    ):
        raise transport._fail()
    values = tuple(entities["values"])
    _validate_npi_page(values)
    document["entity"] = {"adapter_id": "npi", "value": values[0]}
    return transport._parse_detail_request(transport._canonical_json_bytes(document)), values


async def serve_custom_import_detail_batch(request, session):
    """Authorize one attachment before hydrating exact IDs without native search."""

    if (
        getattr(request, "method", None) != "POST"
        or getattr(request, "path", None) != CUSTOM_IMPORT_DETAIL_BATCH_PATH
        or getattr(request, "query_string", "")
    ):
        return transport._error(404)
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= transport._MAX_BODY_BYTES:
            raise transport._fail()
        parsed, npi_values = _parse_batch_request(body)
        verified = transport._verify_transport(
            headers=request.headers,
            body=body,
            request=parsed,
            trusted_now=transport._trusted_now(),
            keyring=transport._keyring(),
            path=CUSTOM_IMPORT_DETAIL_BATCH_PATH,
        )
        if session is None or session.in_transaction():
            raise CustomImportReadUnavailableError("fresh detail batch session is required")
        async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000), session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            async with _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS):
                encoded = await _read_batch_payload(session, parsed, npi_values, verified)
                return transport._response(encoded, 200)
    except Exception as failure:
        transport._log_failure(failure)
        return transport._error(transport._failure_status(failure))


async def _read_batch_payload(session, parsed, npi_values, verified):
    """Reuse the authorized page hydrator and retain one null for every absence."""

    pinned = await transport._resolve_pinned_target(session, parsed.target)
    service = CustomImportReadService(authorizer=transport._TransportAuthorizer(verified, pinned))
    authorization = ExtensionReadAuthorization(verified.credential)
    query = NpiEntityRelationQuery(
        context_filters=parsed.context,
        require_match=False,
        grouped_entity_selection=parsed.grouped_entity_selection,
        family_entitlement=parsed.family_entitlement if parsed.grouped_entity_selection is not None else None,
        grouped_child_query=parsed.grouped_child_query,
    )
    prepared = await service.prepare_npi_entity_relation(
        session, authorization=authorization, target=pinned, query=query
    )
    imported = await service.hydrate_npi_page(
        session,
        authorization=authorization,
        pinned_target=pinned,
        prepared=prepared,
        entity_values=npi_values,
        query=query,
        full_family=True,
    )
    if type(imported) is not dict or not set(imported) <= set(npi_values):
        raise CustomImportReadUnavailableError("detail batch identities are unavailable")
    provider_items = []
    for provider_npi in npi_values:
        imported_family = imported.get(provider_npi)
        family = None if imported_family is None else transport._detail_payload(imported_family, parsed.target)
        if len(transport._canonical_json_bytes(family)) > transport._MAX_RESPONSE_BYTES:
            raise CustomImportReadUnavailableError("detail batch family exceeds its bound")
        provider_items.append({"npi": provider_npi, "custom_import": family})
    encoded = transport._canonical_json_bytes(
        {"target": transport._target_document(parsed.target), "items": provider_items}
    )
    if len(encoded) > (len(npi_values) + 1) * transport._MAX_RESPONSE_BYTES:
        raise CustomImportReadUnavailableError("detail batch response exceeds its bound")
    await verify_published_generation(session, pinned)
    return encoded
