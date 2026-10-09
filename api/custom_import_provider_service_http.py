# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed provider-service pages composed with one pinned imported relation."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import text

from api import custom_import_read_http as transport
from api.custom_import_provider_http import (
    _hydrate_provider_rows,
    _parse_provider_request,
    _provider_relation_query,
    _provider_response_limit,
)
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
    "include_legacy_fields include_sources include_evidence include_details include_debug "
    "plan_release_id npi lat long radius radius_miles mode include_providers include_code_details "
    "include_allowed_amounts include_unverified_addresses pos place_of_service modifier modifiers "
    "billing_code_modifier rate negotiated_rate rate_tolerance negotiated_rate_tolerance".split()
)


def _parse_service_native_query(document: object) -> RequestParameters:
    """Admit only the ordinary claims lane, with code selectors bound in the signed body."""

    if (
        type(document) is not dict
        or not {"code", "code_system"} <= set(document) <= _NATIVE_FIELDS
        or any(
            type(entry_id) is not str or len(entry_id) > 2048 or "\x00" in entry_id for entry_id in document.values()
        )
        or not document["code"].strip()
        or not document["code_system"].strip()
    ):
        raise CustomImportReadRequestError("native provider-service query is invalid")
    return RequestParameters({name: [entry_id] for name, entry_id in document.items()})


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
            await _prepare_service_snapshot(session, parsed.native_args)
            async with _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS):
                service_payload = await _read_service_payload(request, session, parsed, verified)
                encoded = transport._canonical_json_bytes(service_payload)
                if len(encoded) > _provider_response_limit(parsed, len(service_payload["items"])):
                    raise CustomImportReadUnavailableError("provider-service response is unavailable")
                return transport._response(encoded, 200)
    except Exception as failure:
        transport._log_failure(failure)
        status = 400 if isinstance(failure, InvalidUsage) else transport._failure_status(failure)
        return transport._error(status)


async def _prepare_service_snapshot(session, native_args):
    """Allocate plan-only TEMP storage before entering the shared read-only snapshot."""

    if native_args.get("plan_release_id"):
        from api.custom_import_plan_sql import prepare_plan_query_tables

        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await prepare_plan_query_tables(session)
        await session.execute(text("SET TRANSACTION READ ONLY"))
    else:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))


async def _read_service_payload(request, session, parsed, verified):
    from api.endpoint.pricing import _parse_int

    pinned_target = await transport._resolve_pinned_target(session, parsed.target)
    service = CustomImportReadService(authorizer=transport._TransportAuthorizer(verified, pinned_target))
    authorization = ExtensionReadAuthorization(verified.credential)
    query = _provider_relation_query(
        parsed,
        require_exact_context=parsed.grouped_entity_selection is None,
        native_npi=_parse_int(parsed.native_args.get("npi") or None, "npi", minimum=1),
    )
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
    response_by_field = _service_payload(reply.body, plan_release_id=parsed.native_args.get("plan_release_id"))
    await _hydrate_provider_rows(
        session, service, authorization, pinned_target, prepared, parsed, query, response_by_field["items"]
    )
    await verify_published_generation(session, pinned_target)
    return response_by_field


def _service_payload(body: bytes, *, plan_release_id: str | None = None) -> dict[str, Any]:
    """Reject malformed native pages before collecting bounded hydration keys."""

    response_by_field = json.loads(body)
    if (
        type(response_by_field) is not dict
        or type(response_by_field.get("items")) is not list
        or len(response_by_field["items"]) > MAX_NPI_PAGE_SIZE
        or type(response_by_field.get("pagination")) is not dict
        or type(response_by_field.get("query")) is not dict
        or any(
            type(provider_item) is not dict or type(provider_item.get("npi")) not in {int, str}
            for provider_item in response_by_field["items"]
        )
    ):
        raise CustomImportReadUnavailableError("provider-service response is unavailable")
    try:
        for provider_item in response_by_field["items"]:
            provider_item["npi"] = str(provider_item["npi"])
        npis = tuple(provider_item["npi"] for provider_item in response_by_field["items"])
        if plan_release_id:
            identities = response_by_field.get("custom_import_native_entry_ids")
            if (
                response_by_field.get("plan_release_id") != plan_release_id
                or response_by_field["query"].get("plan_release_id") != plan_release_id
                or response_by_field.get("pricing_scope") != "plan_scoped_ptg"
                or type(identities) is not list
                or len(identities) != len(npis)
                or any(
                    type(entry_id) is not str or re.fullmatch(r"[0-9a-f]{64}", entry_id) is None
                    for entry_id in identities
                )
                or len(set(identities)) != len(identities)
            ):
                raise CustomImportReadRequestError("provider-service native entry identity is invalid")
            _validate_npi_page(tuple(dict.fromkeys(npis)))
        else:
            if "custom_import_native_entry_ids" in response_by_field:
                raise CustomImportReadRequestError("provider-service native entry identity is invalid")
            _validate_npi_page(npis)
    except CustomImportReadRequestError:
        raise CustomImportReadUnavailableError("provider-service response is unavailable") from None
    return response_by_field


async def _service_page(request: Any, **kwargs: Any):
    """Load the native handler only after signed transport validation."""

    from api.endpoint.pricing import list_providers_by_procedure

    return await list_providers_by_procedure(request, **kwargs)


__all__ = ("CUSTOM_IMPORT_PROVIDER_SERVICE_PATH", "serve_custom_import_provider_service")
