# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose configured imported fields with authenticated exact billing search."""

from __future__ import annotations

import asyncio
import hmac
from dataclasses import dataclass

import orjson
from sanic.request.parameters import RequestParameters

from api import billing_search_http as billing_http
from api import custom_import_read_http as transport
from api.billing_search_endpoint_access import (
    authorize_billing_search_endpoint,
    validate_billing_search_endpoint_access_state,
)
from api.billing_search_request import parse_billing_search_request
from api.billing_search_response import shape_billing_search_response
from api.billing_search_transport_contract import BILLING_SEARCH_TRANSPORT_PATH
from api.custom_import_billing_query import _new_billing_import_query
from api.custom_import_provider_http import (
    _hydrate_provider_rows,
    _parse_provider_request,
    _provider_relation_query,
    _provider_response_limit,
)
from api.ptg2_billing_search_service import search_exact_billing_provider_page
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    CustomImportReadAuthorizationError,
    CustomImportReadError,
    CustomImportReadRequestError,
    ExtensionReadAuthorization,
)
from process.custom_import.read_core import CustomImportReadService, _bounded_read_window
from process.custom_import.read_identity import verify_published_generation

CUSTOM_IMPORT_BILLING_SEARCH_PATH = "/api/v1/extensions/custom-import/providers/billing-search"
_BILLING_PAGE_SIZE = 100


@dataclass(frozen=True, slots=True)
class _ParsedBillingRequest:
    provider: object
    billing_transport_context_sha256: str

    @property
    def target(self):
        """Expose the exact requested target to the shared transport verifier."""

        return self.provider.target


def _parse_billing_native_query(document):
    if type(document) is not dict or any(
        type(name) is not str or type(value) is not str or len(value) > 2048 or "\x00" in value
        for name, value in document.items()
    ):
        raise transport._fail()
    parameters = RequestParameters({name: [value] for name, value in document.items()})
    try:
        request = parse_billing_search_request(parameters)
    except Exception:
        raise transport._fail() from None
    if request.query_pairs != tuple(sorted(document.items())):
        raise transport._fail()
    return parameters


def _parse_billing_request(body):
    document = transport._strict_json(body)
    if type(document) is not dict or transport._canonical_json_bytes(document) != body:
        raise transport._fail()
    digest = document.get("billing_transport_context_sha256")
    if (
        type(digest) is not str
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or "include_filter" not in document
    ):
        raise transport._fail()
    provider_by_field = {name: value for name, value in document.items() if name != "billing_transport_context_sha256"}
    provider = _parse_provider_request(
        transport._canonical_json_bytes(provider_by_field),
        native_query_parser=_parse_billing_native_query,
        maximum_family_rows=_BILLING_PAGE_SIZE,
    )
    return _ParsedBillingRequest(provider, digest)


async def _read_billing_payload(session, parsed, verified, access, cursor_keyring, trusted_now):
    provider = parsed.provider
    pinned_target = await transport._resolve_pinned_target(session, parsed.target)
    service = CustomImportReadService(authorizer=transport._TransportAuthorizer(verified, pinned_target))
    authorization = ExtensionReadAuthorization(verified.credential)
    query = _provider_relation_query(
        provider,
        require_exact_context=provider.grouped_entity_selection is None,
        native_npi=access.request.provider_npi,
    )
    prepared = await service.prepare_npi_entity_relation(
        session,
        authorization=authorization,
        target=pinned_target,
        query=query,
    )
    access, access_state = validate_billing_search_endpoint_access_state(access, trusted_now=trusted_now)
    import_context = _new_billing_import_query(prepared, query.require_match, pinned_target, access_state)
    search_result = await search_exact_billing_provider_page(
        session,
        access=access,
        cursor_keyring=cursor_keyring,
        trusted_now=trusted_now,
        import_context=import_context,
    )
    billing_payload = shape_billing_search_response(
        access,
        search_result,
        cursor_keyring=cursor_keyring,
        trusted_now=trusted_now,
        import_scope=import_context.import_scope,
    )
    # Native field, numeric, witness and text budgets are validated before imports.
    native_body = orjson.dumps(billing_payload)
    if len(native_body) > billing_http._MAX_SUCCESS_BODY_BYTES:
        raise transport.CustomImportReadUnavailableError("billing response exceeds its native bound")
    await _hydrate_provider_rows(
        session,
        service,
        authorization,
        pinned_target,
        prepared,
        provider,
        query,
        billing_payload["items"],
    )
    await verify_published_generation(session, pinned_target)
    encoded = orjson.dumps(billing_payload)
    if len(encoded) > _provider_response_limit(
        provider, len(billing_payload["items"]), maximum_rows=_BILLING_PAGE_SIZE
    ):
        raise transport.CustomImportReadUnavailableError("billing import response exceeds its bound")
    return encoded


async def serve_custom_import_billing_search(request, session):
    """Require both signed authorities before one pinned read-only composition."""

    if (
        getattr(request, "method", None) != "POST"
        or getattr(request, "path", None) != CUSTOM_IMPORT_BILLING_SEARCH_PATH
        or getattr(request, "query_string", "")
    ):
        return billing_http._error_response(404)
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= transport._MAX_BODY_BYTES:
            raise transport._fail()
        parsed = _parse_billing_request(body)
        trusted_now = transport._trusted_now()
        verified = transport._verify_provider_transport(
            headers=request.headers,
            body=body,
            request=parsed,
            trusted_now=trusted_now,
            keyring=transport._keyring(),
            path=CUSTOM_IMPORT_BILLING_SEARCH_PATH,
        )
        # This is the subordinate canonical GET descriptor, never the outer POST.
        access = authorize_billing_search_endpoint(
            parsed.provider.native_args,
            request.headers,
            method="GET",
            path=BILLING_SEARCH_TRANSPORT_PATH,
            trusted_now=trusted_now,
            keyring=billing_http._transport_keyring(),
        )
        if not hmac.compare_digest(
            parsed.billing_transport_context_sha256,
            access.verified_transport.transport_context_sha256,
        ):
            raise transport._fail()
        cursor_keyring = billing_http._cursor_keyring()
        if session is None or session.in_transaction():
            raise transport.CustomImportReadUnavailableError("fresh billing read session is required")
        async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000), session.begin():
            await session.execute(billing_http._READ_TRANSACTION_SQL)
            async with _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS):
                encoded = await _read_billing_payload(session, parsed, verified, access, cursor_keyring, trusted_now)
    except (CustomImportReadError, transport.CustomImportReadTransportError) as failure:
        transport._log_failure(failure)
        if isinstance(failure, CustomImportReadRequestError):
            return transport._error(400)
        status = (
            404
            if isinstance(failure, (transport.CustomImportReadTransportError, CustomImportReadAuthorizationError))
            else 503
        )
        return billing_http._error_response(status)
    except Exception as failure:
        return billing_http._failure_response(failure)
    return transport._response(encoded, 200)


__all__ = ("CUSTOM_IMPORT_BILLING_SEARCH_PATH", "serve_custom_import_billing_search")
