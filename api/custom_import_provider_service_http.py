# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed provider-service pages composed with one pinned imported relation."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters

from api import custom_import_provider_collection as managed_collection
from api import custom_import_read_http as transport
from api.custom_import_provider_http import (
    _hydrate_provider_rows,
    _parse_provider_request,
    _provider_relation_query,
    _provider_response_limit,
    _ProviderScopePreparer,
)
from api.custom_import_provider_network import provider_network_read_scope, provider_network_response
from api.custom_import_provider_service_sql import ProviderServiceImportQuery
from api.provider_collection_network_read import ProviderCollectionServiceRefusal
from api.provider_service_legacy_selection import has_legacy_service_selectors, legacy_service_selection
from api.signed_request_reader import signed_request_reader
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


_MANAGED_SELECTOR_FIELDS = frozenset(
    "plan_id plan_external_id plan_id_type plan_market_type market_type source_key snapshot_id".split()
)


def _parse_service_native_query(document: object, *, allow_legacy_selectors=False) -> RequestParameters:
    """Close signed service options; legacy routing needs the full managed policy."""

    if (
        type(document) is not dict
        or not {"code", "code_system"}
        <= set(document)
        <= (_NATIVE_FIELDS | _MANAGED_SELECTOR_FIELDS if allow_legacy_selectors else _NATIVE_FIELDS)
        or any(
            type(entry_id) is not str or len(entry_id) > 2048 or "\x00" in entry_id for entry_id in document.values()
        )
        or not document["code"].strip()
        or not document["code_system"].strip()
    ):
        raise CustomImportReadRequestError("native provider-service query is invalid")
    return RequestParameters({name: [entry_id] for name, entry_id in document.items()})


async def serve_custom_import_provider_service(request: Any, session: Any):
    """Authorize and serve one exact service page in one read-only snapshot."""

    if (
        getattr(request, "method", None) != "POST"
        or getattr(request, "path", None) != CUSTOM_IMPORT_PROVIDER_SERVICE_PATH
        or getattr(request, "query_string", "")
    ):
        return transport._error(404)
    is_managed_mode = False
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= managed_collection.MAX_MANAGED_BODY_BYTES:
            raise transport._fail()
        parsed = _parse_provider_request(
            body,
            native_query_parser=lambda document: _parse_service_native_query(document, allow_legacy_selectors=True),
            allow_network_binding=True,
            allow_managed_collection=True,
            collection_kind="by-service",
        )
        is_managed_mode = parsed.managed_collection is not None
        if not is_managed_mode and any(name in parsed.native_args for name in _MANAGED_SELECTOR_FIELDS):
            raise CustomImportReadRequestError("native provider-service query is invalid")
        if not is_managed_mode and len(body) > transport._MAX_BODY_BYTES:
            raise transport._fail()
        if is_managed_mode:
            from api.endpoint.pagination import _parse_non_negative_int

            limit = _parse_non_negative_int(parsed.native_args.get("limit"), "limit")
            if limit is not None and limit > 50:
                raise CustomImportReadRequestError("provider-service page exceeds its bound")
        verified = transport._verify_provider_transport(
            headers=request.headers,
            body=body,
            request=parsed,
            trusted_now=transport._trusted_now(),
            keyring=transport._keyring(),
            path=CUSTOM_IMPORT_PROVIDER_SERVICE_PATH,
            managed_collection=is_managed_mode,
        )
        if is_managed_mode:
            return await _serve_managed_service_page(request, parsed, verified)
        return await _serve_legacy_service_page(request, session, parsed, verified)
    except ProviderCollectionServiceRefusal as failure:
        if not is_managed_mode:
            return transport._error(503)
        reply = transport._response(transport._canonical_json_bytes(failure.document), 422)
        reply.headers["X-Network-Access-Scope"] = parsed.managed_collection.scope_sha256
        return reply
    except Exception as failure:
        transport._log_failure(failure)
        if is_managed_mode:
            return managed_collection.managed_collection_error(failure)
        status = 400 if isinstance(failure, InvalidUsage) else transport._failure_status(failure)
        return transport._error(status)


async def _serve_legacy_service_page(request, session, parsed, verified):
    async with (
        asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000),
        signed_request_reader(
            request, session, plan_workspace=bool(parsed.native_args.get("plan_release_id"))
        ) as session,
    ):
        await _prepare_service_snapshot(session, parsed.native_args)
        async with (
            _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS),
            provider_network_read_scope(session, parsed.network_binding) as manifest,
        ):
            service_payload = await _read_service_payload(request, session, parsed, verified)
            encoded = transport._canonical_json_bytes(service_payload)
            if len(encoded) > _provider_response_limit(parsed, len(service_payload["items"])):
                raise CustomImportReadUnavailableError("provider-service response is unavailable")
            return provider_network_response(encoded, manifest)


async def _serve_managed_service_page(request, parsed, verified):
    temporary_setup = None
    if parsed.native_args.get("plan_release_id") or has_legacy_service_selectors(parsed.native_args):
        from api.custom_import_plan_sql import prepare_plan_query_tables

        temporary_setup = prepare_plan_query_tables
    async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000):
        async with managed_collection.managed_import_collection_scope(
            request, parsed.managed_collection, temporary_setup=temporary_setup
        ) as (reader, manifest):
            payload = await _read_service_payload(request, reader, parsed, verified)
            return managed_collection.managed_collection_response(
                payload,
                parsed.managed_collection,
                manifest,
                maximum_bytes=_provider_response_limit(parsed, len(payload["items"])),
            )


async def _prepare_service_snapshot(session, native_args):
    """Require fixed empty workspace preparation before the genuine RO snapshot."""
    if native_args.get("plan_release_id") and session.info.get("native_plan_workspace") is not True:
        raise CustomImportReadUnavailableError("plan Reader workspace is unavailable")


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

    scope_preparer = _ProviderScopePreparer(session, service, authorization, pinned_target, query, prepared)
    async with legacy_service_selection(session, parsed.native_args) as native_args:
        reply = await _service_page(
            request,
            native_args=native_args,
            import_context=ProviderServiceImportQuery(prepared, query.require_match, scope_preparer.prepare),
        )
        plan_release_id = native_args.get("plan_release_id")
    if reply.status == 422 and parsed.managed_collection is not None:
        raise ProviderCollectionServiceRefusal(reply.body)
    if reply.status != 200 and parsed.managed_collection is not None:
        from sanic.exceptions import SanicException

        status = reply.status if reply.status in {400, 403, 404, 409, 503} else 503
        raise SanicException("Provider data is unavailable.", status_code=status)
    maximum_native_bytes = _provider_response_limit(parsed, parsed.maximum_family_rows)
    if reply.status != 200 or len(reply.body) > maximum_native_bytes:
        raise CustomImportReadUnavailableError("provider-service response is unavailable")
    response_by_field = _service_payload(reply.body, plan_release_id=plan_release_id)
    if len(reply.body) > _provider_response_limit(parsed, len(response_by_field["items"])):
        raise CustomImportReadUnavailableError("provider-service response is unavailable")
    # The public default omits this duplicate legacy alias while retaining all prices.
    if str(parsed.native_args.get("include_legacy_fields") or "").lower() != "true":
        for provider_item in response_by_field["items"]:
            if "prices" in provider_item and provider_item.get("tic_prices") == provider_item["prices"]:
                provider_item.pop("tic_prices", None)
    await _hydrate_provider_rows(
        session,
        service,
        authorization,
        pinned_target,
        scope_preparer.prepared,
        parsed,
        query,
        response_by_field["items"],
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

    native_args = kwargs.get("native_args")
    if native_args is None:
        return await list_providers_by_procedure(request, **kwargs)
    from api.network_address_scope import current_network_address_scope

    if current_network_address_scope() is None or not has_legacy_service_selectors(native_args):
        return await list_providers_by_procedure(request, **kwargs)
    async with legacy_service_selection(request.ctx.sa_session, native_args) as normalized:
        return await list_providers_by_procedure(request, **(kwargs | {"native_args": normalized}))


__all__ = ("CUSTOM_IMPORT_PROVIDER_SERVICE_PATH", "serve_custom_import_provider_service")
