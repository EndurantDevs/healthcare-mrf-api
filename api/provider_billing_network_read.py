# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate canonical billing authority inside signed managed collections."""

import hmac

import orjson

from api import billing_search_http as billing_http
from api.billing_search_endpoint_access import BillingSearchEndpointAccessError, authorize_billing_search_endpoint
from api.billing_search_response import shape_billing_search_response
from api.billing_search_transport_contract import BILLING_SEARCH_TRANSPORT_PATH, _framed_sha256
from api.custom_import_billing_http import _parse_billing_native_query
from api.network_address_scope import current_network_address_scope
from api.ptg2_billing_search_contract import serving_unavailable
from api.ptg2_billing_search_service import search_exact_billing_provider_page

BILLING_SEARCH_NATIVE_FIELDS = frozenset(
    {
        "billing_entity_ref",
        "plan_release_id",
        "code",
        "code_system",
        "limit",
        "cursor",
        "include_evidence",
        "lat",
        "long",
        "radius_miles",
        "zip5",
        "npi",
        "modifiers",
        "place_of_service",
        "billing_transport_context_sha256",
    }
)


def require_managed_billing_client(access, client_id):
    """Bind the complete policy client to the independently signed billing tenant."""
    expected = _framed_sha256(b"HEALTHPORTA_BILLING_SEARCH_TENANT_SCOPE_V1\x00", orjson.dumps({"client_id": client_id}))
    if not hmac.compare_digest(expected, access.authorization_context.tenant_scope_sha256):
        raise BillingSearchEndpointAccessError("billing_search_endpoint_access_invalid")


def authorize_managed_billing_search(request, *, native_args):
    """Validate the nested real billing signature without opening a Reader."""
    document_by_field = {name: native_args.get(name) for name in native_args}
    if set(document_by_field) - BILLING_SEARCH_NATIVE_FIELDS:
        raise serving_unavailable()
    context_digest = document_by_field.pop("billing_transport_context_sha256", None)
    if type(context_digest) is not str or len(context_digest) != 64 or set(context_digest) - set("0123456789abcdef"):
        raise serving_unavailable()
    parameters = _parse_billing_native_query(document_by_field)
    trusted_now = billing_http._trusted_now()
    # The subordinate canonical GET descriptor never changes the actual request.
    access = authorize_billing_search_endpoint(
        parameters,
        request.headers,
        method="GET",
        path=BILLING_SEARCH_TRANSPORT_PATH,
        trusted_now=trusted_now,
        keyring=billing_http._transport_keyring(),
    )
    if not hmac.compare_digest(context_digest, access.verified_transport.transport_context_sha256):
        raise serving_unavailable()
    return access, trusted_now


async def read_managed_billing_search(request, *, session, native_args):
    """Keep both real signatures, exact billing selector and original response checks."""
    try:
        access, trusted_now = authorize_managed_billing_search(request, native_args=native_args)
        if current_network_address_scope() is None or not session.in_transaction():
            raise serving_unavailable()
        keyring = billing_http._cursor_keyring()
        service_result = await search_exact_billing_provider_page(
            session, access=access, cursor_keyring=keyring, trusted_now=trusted_now
        )
        document = shape_billing_search_response(
            access, service_result, cursor_keyring=keyring, trusted_now=trusted_now
        )
        encoded = orjson.dumps(document)
        if len(encoded) > billing_http._MAX_SUCCESS_BODY_BYTES:
            raise serving_unavailable()
        return billing_http._json_bytes_response(encoded, status=200)
    except Exception as failure:
        return billing_http._failure_response(failure)
