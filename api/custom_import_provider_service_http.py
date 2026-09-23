# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed provider-service pages composed with one pinned imported relation."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import text

from api import custom_import_read_http as transport
from api.custom_import_provider_http import _hydrate_provider_rows, _parse_provider_request, _provider_relation_query
from api.custom_import_provider_service_sql import ProviderServiceImportQuery
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    MAX_NPI_PAGE_SIZE,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
)
from process.custom_import.read_core import CustomImportReadService, _bounded_read_window, _validate_npi_page
from process.custom_import.read_identity import verify_published_generation

CUSTOM_IMPORT_PROVIDER_SERVICE_PATH = "/api/v1/extensions/custom-import/providers/by-service"
_NATIVE_FIELDS = frozenset(
    "code code_system year state city zip5 zip_radius_miles specialty classification taxonomy_codes "
    "provider_sex_code q min_claims min_total_cost page offset limit order order_by "
    "include_legacy_fields include_sources include_evidence include_details include_debug".split()
)


def _parse_service_native_query(document: object) -> RequestParameters:
    """Admit only the ordinary claims lane, with code selectors bound in the signed body."""

    if (
        type(document) is not dict
        or not {"code", "code_system"} <= set(document) <= _NATIVE_FIELDS
        or any(type(value) is not str or len(value) > 2048 or "\x00" in value for value in document.values())
        or not document["code"].strip()
        or not document["code_system"].strip()
    ):
        raise CustomImportReadRequestError("native provider-service query is invalid")
    return RequestParameters({name: [value] for name, value in document.items()})


async def serve_custom_import_provider_service(request: Any, session: Any):
    """Authorize and serve one exact claims page in one read-only snapshot."""

    if (
        getattr(request, "method", None) != "POST"
        or getattr(request, "path", None) != CUSTOM_IMPORT_PROVIDER_SERVICE_PATH
        or getattr(request, "query_string", "")
    ):
        return transport._error(404)
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= transport._MAX_BODY_BYTES:
            raise transport._fail()
        parsed = _parse_provider_request(body, native_query_parser=_parse_service_native_query)
        verified = transport._verify_provider_transport(
            headers=request.headers,
            body=body,
            request=parsed,
            trusted_now=transport._trusted_now(),
            keyring=transport._keyring(),
            path=CUSTOM_IMPORT_PROVIDER_SERVICE_PATH,
        )
        if session is None or session.in_transaction():
            raise CustomImportReadUnavailableError("fresh provider-service read session is required")
        async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000), session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            async with _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS):
                service_payload = await _read_service_payload(request, session, parsed, verified)
                encoded = transport._canonical_json_bytes(service_payload)
                if len(encoded) > transport._MAX_RESPONSE_BYTES:
                    raise CustomImportReadUnavailableError("provider-service response is unavailable")
                return transport._response(encoded, 200)
    except Exception as failure:
        transport._log_failure(failure)
        status = 400 if isinstance(failure, InvalidUsage) else transport._failure_status(failure)
        return transport._error(status)


async def _read_service_payload(request, session, parsed, verified):
    pinned_target = await transport._resolve_pinned_target(session, parsed.target)
    service = CustomImportReadService(authorizer=transport._TransportAuthorizer(verified, pinned_target))
    authorization = ExtensionReadAuthorization(verified.credential)
    query = _provider_relation_query(parsed, require_exact_context=True)
    prepared = await service.prepare_npi_entity_relation(
        session,
        authorization=authorization,
        target=pinned_target,
        query=query,
    )
    reply = await _service_page(
        request,
        native_args=parsed.native_args,
        import_context=ProviderServiceImportQuery(prepared, query.require_match),
    )
    if reply.status != 200 or len(reply.body) > transport._MAX_RESPONSE_BYTES:
        raise CustomImportReadUnavailableError("provider-service response is unavailable")
    payload = _service_payload(reply.body)
    await _hydrate_provider_rows(
        session, service, authorization, pinned_target, prepared, parsed, query, payload["items"]
    )
    await verify_published_generation(session, pinned_target)
    return payload


def _service_payload(body: bytes) -> dict[str, Any]:
    """Reject malformed native pages before collecting bounded hydration keys."""

    payload = json.loads(body)
    if (
        type(payload) is not dict
        or type(payload.get("items")) is not list
        or len(payload["items"]) > MAX_NPI_PAGE_SIZE
        or type(payload.get("pagination")) is not dict
        or type(payload.get("query")) is not dict
        or any(type(item) is not dict or type(item.get("npi")) not in {int, str} for item in payload["items"])
    ):
        raise CustomImportReadUnavailableError("provider-service response is unavailable")
    try:
        for item in payload["items"]:
            item["npi"] = str(item["npi"])
        _validate_npi_page(tuple(item["npi"] for item in payload["items"]))
    except CustomImportReadRequestError:
        raise CustomImportReadUnavailableError("provider-service response is unavailable") from None
    return payload


async def _service_page(request: Any, **kwargs: Any):
    """Load the native handler only after signed transport validation."""

    from api.endpoint.pricing import list_providers_by_procedure

    return await list_providers_by_procedure(request, **kwargs)


__all__ = ("CUSTOM_IMPORT_PROVIDER_SERVICE_PATH", "serve_custom_import_provider_service")
