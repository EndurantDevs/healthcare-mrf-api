# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
from collections.abc import Iterator, Mapping
import hashlib
import hmac
import json

import pytest
from sanic.compat import Header

from api import billing_search_post_gateway_transport as gateway_transport
from api.billing_search_post_endpoint_access import (
    BillingSearchPostEndpointAccessError,
    authorize_billing_search_post_endpoint,
    validate_billing_search_post_endpoint_access,
)
from api.billing_search_post_transport import (
    BILLING_SEARCH_POST_MEDIA_TYPE,
    BILLING_SEARCH_POST_METHOD,
    BILLING_SEARCH_POST_PATH,
    parse_billing_search_post_transport,
)
from api.billing_search_transport_keys import BillingSearchTransportKeyring

PLAN_ID = "hpplan_" + "0" * 26
PLAN_RELEASE_ID = "hprelease_" + "0" * 26
KEY_ID = "synthetic-a"
KEY = bytes(range(32))
NOW = "2026-08-07T10:00:10Z"
SYNTHETIC_EIN = "12-" + "3" * 7
REPLACEMENT_SYNTHETIC_EIN = "98-" + "7" * 7


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _body(
    *,
    include_evidence: bool = False,
    tax_identity_value: str = SYNTHETIC_EIN,
) -> bytes:
    payload: dict[str, object] = {
        "billing_identity": {
            "tax_identity": {
                "type": "ein",
                "value": tax_identity_value,
            }
        },
        "geo": {"radius_miles": 0, "zip5": "00000"},
        "healthporta_plan_id": PLAN_ID,
        "procedure": {
            "code": "00000",
            "code_system": "CPT",
            "modifiers": [],
            "place_of_service": [],
        },
    }
    if include_evidence:
        payload["include_evidence"] = True
    return json.dumps(
        payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _context(body: bytes, **updates: object) -> dict[str, object]:
    parsed = parse_billing_search_post_transport(
        body,
        method=BILLING_SEARCH_POST_METHOD,
        path=BILLING_SEARCH_POST_PATH,
        media_type=BILLING_SEARCH_POST_MEDIA_TYPE,
    )
    context_by_field: dict[str, object] = {
        "audience": gateway_transport.BILLING_SEARCH_TRANSPORT_AUDIENCE,
        "audit_scope_sha256": _sha("audit"),
        "capabilities": ["pricing:billing-search"],
        "contract": gateway_transport.BILLING_SEARCH_POST_TRANSPORT_CONTRACT,
        "expires_at": "2026-08-07T10:01:00Z",
        "issued_at": "2026-08-07T10:00:00Z",
        "issuer": gateway_transport.BILLING_SEARCH_TRANSPORT_ISSUER,
        "media_type": BILLING_SEARCH_POST_MEDIA_TYPE,
        "metering_receipt_sha256": "0" * 64,
        "metering_request_id": "00000000-0000-4000-8000-000000000000",
        "method": BILLING_SEARCH_POST_METHOD,
        "path": BILLING_SEARCH_POST_PATH,
        "plan_entitlement_sha256": (
            gateway_transport.billing_search_plan_entitlement_sha256(PLAN_RELEASE_ID)
        ),
        "plan_release_id": PLAN_RELEASE_ID,
        "principal_scope_sha256": _sha("principal"),
        "quota_scope_sha256": _sha("quota"),
        "request_shape_sha256": parsed.request_shape_sha256,
        "tenant_scope_sha256": _sha("tenant"),
    }
    context_by_field.update(updates)
    context_by_field["metering_receipt_sha256"] = (
        gateway_transport._metering_receipt_sha256(context_by_field)
    )
    return context_by_field


def _headers(body: bytes, **context_updates: object) -> dict[str, str]:
    context = _context(body, **context_updates)
    context_bytes = gateway_transport._canonical_json_bytes(context)
    signature = hmac.new(
        KEY,
        gateway_transport._signature_message(KEY_ID, context_bytes, body),
        hashlib.sha256,
    ).digest()
    return {
        gateway_transport.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER: (
            base64.urlsafe_b64encode(context_bytes).rstrip(b"=").decode("ascii")
        ),
        gateway_transport.BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER: KEY_ID,
        gateway_transport.BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER: (
            base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")
        ),
    }


def _keyring() -> BillingSearchTransportKeyring:
    return BillingSearchTransportKeyring(
        active_key_id=KEY_ID,
        keys_by_id={KEY_ID: KEY},
    )


def _authorize(
    body: bytes,
    headers: Mapping[str, object],
):
    return authorize_billing_search_post_endpoint(
        body,
        headers,
        method=BILLING_SEARCH_POST_METHOD,
        path=BILLING_SEARCH_POST_PATH,
        media_type=BILLING_SEARCH_POST_MEDIA_TYPE,
        trusted_now=NOW,
        keyring=_keyring(),
    )


class _MultiHeaders(Mapping[str, str]):
    def __init__(self, pairs: list[tuple[str, str]]) -> None:
        self._pairs = pairs

    def __getitem__(self, key: str) -> str:
        values = self.getlist(key)
        if not values:
            raise KeyError(key)
        return values[0]

    def __iter__(self) -> Iterator[str]:
        return iter(dict(self._pairs))

    def __len__(self) -> int:
        return len(dict(self._pairs))

    def items(self, multi: bool = False):
        return list(self._pairs) if multi else list(dict(self._pairs).items())

    def getlist(self, key: str) -> list[str]:
        return [value for name, value in self._pairs if name.lower() == key.lower()]


def test_endpoint_access_verifies_transport_shape_and_authority() -> None:
    body = _body()
    access = _authorize(body, _headers(body))

    assert access.plan_release_id == PLAN_RELEASE_ID
    assert access.request.healthporta_plan_id == PLAN_ID
    assert access.request.selector_kind == "tax_identity"
    assert repr(access) == "<redacted-billing-search-post-endpoint-access>"
    assert SYNTHETIC_EIN not in repr(access)
    assert (
        validate_billing_search_post_endpoint_access(
            access,
            trusted_now=NOW,
        )
        is access
    )


def test_endpoint_access_rejects_valid_request_object_substitution() -> None:
    body = _body()
    access = _authorize(body, _headers(body))
    replacement_body = _body(tax_identity_value=REPLACEMENT_SYNTHETIC_EIN)
    replacement = _authorize(
        replacement_body,
        _headers(replacement_body),
    )
    object.__setattr__(
        access,
        "_BillingSearchPostEndpointAccess__request",
        replacement.request,
    )

    with pytest.raises(BillingSearchPostEndpointAccessError):
        validate_billing_search_post_endpoint_access(
            access,
            trusted_now=NOW,
        )


def test_endpoint_access_revalidation_uses_the_current_trusted_time() -> None:
    body = _body()
    access = _authorize(body, _headers(body))

    with pytest.raises(BillingSearchPostEndpointAccessError):
        validate_billing_search_post_endpoint_access(
            access,
            trusted_now="2026-08-07T10:01:00Z",
        )


def test_endpoint_access_accepts_the_real_sanic_header_mapping() -> None:
    body = _body()

    access = _authorize(body, Header(_headers(body)))

    assert access.plan_release_id == PLAN_RELEASE_ID


@pytest.mark.parametrize("mutation", ["duplicate", "extra"])
def test_endpoint_access_rejects_ambiguous_or_extra_internal_headers(
    mutation: str,
) -> None:
    body = _body()
    headers = _headers(body)
    pairs = list(headers.items())
    if mutation == "duplicate":
        pairs.append((next(iter(headers)), next(iter(headers.values()))))
    else:
        pairs.append(("X-HealthPorta-Billing-Search-Unexpected", "synthetic"))

    with pytest.raises(
        BillingSearchPostEndpointAccessError,
        match="^billing_search_post_endpoint_access_invalid$",
    ):
        _authorize(body, _MultiHeaders(pairs))


def test_endpoint_access_rejects_signed_request_shape_substitution() -> None:
    body = _body()
    headers = _headers(body, request_shape_sha256=_sha("wrong-shape"))

    with pytest.raises(BillingSearchPostEndpointAccessError):
        _authorize(body, headers)


def test_endpoint_access_requires_provenance_capability_for_evidence() -> None:
    body = _body(include_evidence=True)
    headers = _headers(body)

    with pytest.raises(BillingSearchPostEndpointAccessError) as captured:
        _authorize(body, headers)

    assert SYNTHETIC_EIN not in str(captured.value)
    assert captured.value.__cause__ is None


def test_endpoint_access_accepts_stronger_provenance_capability() -> None:
    body = _body(include_evidence=True)
    headers = _headers(
        body,
        capabilities=[
            "pricing:billing-search",
            "pricing:billing-search:provenance",
        ],
    )

    access = _authorize(body, headers)

    assert access.request.include_evidence is True
