# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated HTTP-boundary tests for billing-search GET requests."""

from __future__ import annotations

import base64
from collections.abc import Mapping
import copy
import hashlib
import hmac
import json
import pickle

import pytest
from sanic.request import RequestParameters
from sanic.response import raw

from api import billing_search_endpoint_access as endpoint
from api import billing_search_gateway_transport as gateway_transport
from api import billing_search_transport_contract as transport_contract
from api import billing_search_transport_keys as transport_keys
from api.billing_search_request import parse_billing_search_request

BILLING_ENTITY_REF = (
    "be1_AAECAwQFBgcICQoLDA0ODxIr3ljg-uNk13KslT9vSXm4lGO1maZsqjUk0Jf9HUBm"
)
PLAN_RELEASE_ID = "hprelease_01K123456789ABCDEFGHJKMNPQ"
KEY_ID = "synthetic-get"
KEY = bytes(range(32))
TRUSTED_NOW = "2031-01-02T03:04:05Z"
ISSUED_AT = "2031-01-02T03:03:55Z"
EXPIRES_AT = "2031-01-02T03:04:55Z"
REQUEST_ID = "123e4567-e89b-42d3-a456-426614174000"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _query_pairs(**overrides: str) -> tuple[tuple[str, str], ...]:
    values_by_name = {
        "billing_entity_ref": BILLING_ENTITY_REF,
        "code": "00000",
        "code_system": "CPT",
        "limit": "25",
        "plan_release_id": PLAN_RELEASE_ID,
        "zip5": "00000",
    }
    values_by_name.update(overrides)
    return transport_contract.normalize_billing_search_query_pairs(
        tuple(values_by_name.items())
    )


def _parameters(**overrides: str) -> dict[str, str]:
    return dict(_query_pairs(**overrides))


def _context(
    query_pairs: tuple[tuple[str, str], ...] | None = None,
    *,
    capabilities: list[str] | None = None,
) -> dict[str, object]:
    exact_query_pairs = query_pairs or _query_pairs()
    plan_release_id = dict(exact_query_pairs)["plan_release_id"]
    plan_entitlement_sha256 = transport_contract.billing_search_plan_entitlement_sha256(
        plan_release_id
    )
    query_sha256 = transport_contract.billing_search_query_sha256(exact_query_pairs)
    context_by_field: dict[str, object] = {
        "audience": transport_contract.BILLING_SEARCH_TRANSPORT_AUDIENCE,
        "audit_scope_sha256": _sha("synthetic-audit-scope"),
        "capabilities": capabilities or ["pricing:billing-search"],
        "contract": transport_contract.BILLING_SEARCH_TRANSPORT_CONTRACT,
        "expires_at": EXPIRES_AT,
        "issued_at": ISSUED_AT,
        "issuer": transport_contract.BILLING_SEARCH_TRANSPORT_ISSUER,
        "metering_receipt_sha256": "0" * 64,
        "metering_request_id": REQUEST_ID,
        "method": "GET",
        "path": transport_contract.BILLING_SEARCH_TRANSPORT_PATH,
        "plan_entitlement_sha256": plan_entitlement_sha256,
        "plan_release_id": plan_release_id,
        "principal_scope_sha256": _sha("synthetic-principal-scope"),
        "query_sha256": query_sha256,
        "quota_scope_sha256": _sha("synthetic-quota-scope"),
        "tenant_scope_sha256": _sha("synthetic-tenant-scope"),
    }
    context_by_field["metering_receipt_sha256"] = (
        transport_contract.billing_search_metering_receipt_sha256(
            method=context_by_field["method"],
            path=context_by_field["path"],
            plan_entitlement_sha256=context_by_field["plan_entitlement_sha256"],
            query_sha256=context_by_field["query_sha256"],
            quota_scope_sha256=context_by_field["quota_scope_sha256"],
            request_id=context_by_field["metering_request_id"],
        )
    )
    return context_by_field


def _keyring() -> transport_keys.BillingSearchTransportKeyring:
    return transport_keys.BillingSearchTransportKeyring(
        active_key_id=KEY_ID,
        keys_by_id={KEY_ID: KEY},
    )


def _signed_headers(
    query_pairs: tuple[tuple[str, str], ...] | None = None,
    *,
    capabilities: list[str] | None = None,
) -> dict[str, str]:
    context_by_field = _context(query_pairs, capabilities=capabilities)
    context_bytes = transport_contract._canonical_json_bytes(context_by_field)
    signature = hmac.new(
        KEY,
        gateway_transport._signature_message(KEY_ID, context_bytes),
        hashlib.sha256,
    ).digest()
    return {
        transport_contract.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER: _base64url(
            context_bytes
        ),
        transport_contract.BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER: KEY_ID,
        transport_contract.BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER: _base64url(
            signature
        ),
    }


def _authorize(
    parameters: Mapping[str, object] | None = None,
    headers: Mapping[str, object] | None = None,
    **overrides: object,
) -> endpoint.BillingSearchEndpointAccess:
    arguments_by_name = {
        "method": "GET",
        "path": transport_contract.BILLING_SEARCH_TRANSPORT_PATH,
        "trusted_now": TRUSTED_NOW,
        "keyring": _keyring(),
    }
    arguments_by_name.update(overrides)
    return endpoint.authorize_billing_search_endpoint(
        _parameters() if parameters is None else parameters,
        _signed_headers() if headers is None else headers,
        **arguments_by_name,
    )


def _keyring_environment() -> dict[str, str]:
    document_by_field = {
        "active_key_id": KEY_ID,
        "contract": transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_CONTRACT,
        "keys": [{"key_base64url": _base64url(KEY), "key_id": KEY_ID}],
    }
    return {
        transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: json.dumps(
            document_by_field,
            separators=(",", ":"),
            sort_keys=True,
        )
    }


def _endpoint_traceback_locals(error: BaseException) -> list[dict[str, object]]:
    library_locals = []
    traceback = error.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_globals.get("__name__") == (
            "api.billing_search_endpoint_access"
        ):
            library_locals.append(traceback.tb_frame.f_locals)
        traceback = traceback.tb_next
    return library_locals


class _ListOnlyHeaders(dict[str, str]):
    def items(self, multi: bool = False):
        del multi
        return []

    def getall(self, _header_name: str) -> list[str]:
        raise KeyError

    def getlist(self, header_name: str) -> list[str]:
        return [self[header_name]]


class _ItemsNotCallable:
    items = None


class _FallbackItemsFailure:
    def items(self, **options: object):
        if options:
            raise TypeError
        raise RuntimeError("synthetic-items-failure")


@pytest.mark.parametrize("mapping", [_ItemsNotCallable(), _FallbackItemsFailure()])
def test_endpoint_mapping_enumeration_failures_are_closed(mapping) -> None:
    with pytest.raises(
        endpoint.BillingSearchEndpointAccessError,
        match="^billing_search_endpoint_access_invalid$",
    ) as captured:
        endpoint._mapping_items(mapping)

    assert captured.value.__cause__ is None


def test_endpoint_authenticates_exact_get_and_seals_redacted_state() -> None:
    access = _authorize()

    assert access.request.query_pairs == _query_pairs()
    assert access.request.plan_release_id == PLAN_RELEASE_ID
    assert access.authorization_context.plan_entitlement_sha256 == (
        transport_contract.billing_search_plan_entitlement_sha256(PLAN_RELEASE_ID)
    )
    assert access.verified_transport.query_sha256 == (
        transport_contract.billing_search_query_sha256(_query_pairs())
    )
    assert repr(access) == "<redacted-billing-search-endpoint-access>"
    assert BILLING_ENTITY_REF not in repr(access)
    assert copy.copy(access) is access
    assert copy.deepcopy(access) is access
    assert (
        endpoint.validate_billing_search_endpoint_access(
            access,
            trusted_now=TRUSTED_NOW,
        )
        is access
    )

    with pytest.raises(endpoint.BillingSearchEndpointAccessError):
        endpoint.BillingSearchEndpointAccess()
    with pytest.raises(endpoint.BillingSearchEndpointAccessError):
        pickle.dumps(access)
    with pytest.raises(TypeError):
        access.synthetic = "blocked"
    with pytest.raises(TypeError):
        del access.synthetic


def test_endpoint_revalidation_uses_fresh_time_and_rejects_expiry() -> None:
    access = _authorize()

    assert (
        endpoint.validate_billing_search_endpoint_access(
            access,
            trusted_now="2031-01-02T03:04:54Z",
        )
        is access
    )
    with pytest.raises(endpoint.BillingSearchEndpointAccessError):
        endpoint.validate_billing_search_endpoint_access(
            access,
            trusted_now=EXPIRES_AT,
        )
    assert not hasattr(access, "trusted_now")


@pytest.mark.parametrize(
    "request_overrides",
    [
        {"method": "POST"},
        {"path": "/api/v1/pricing/providers/by-service"},
        {"trusted_now": "2031-01-02T03:04:05+00:00"},
    ],
)
def test_endpoint_accepts_only_canonical_get_coordinates(request_overrides) -> None:
    with pytest.raises(
        endpoint.BillingSearchEndpointAccessError,
        match="^billing_search_endpoint_access_invalid$",
    ):
        _authorize(**request_overrides)


@pytest.mark.parametrize(
    "parameters",
    [
        _parameters(code="00001"),
        _parameters(plan_release_id="hprelease_01K123456789ABCDEFGHJKMNP0"),
        _parameters(cursor="bsc1_k1_" + "A" * 64),
    ],
)
def test_endpoint_rejects_query_release_or_cursor_not_attested_by_gateway(
    parameters,
) -> None:
    with pytest.raises(endpoint.BillingSearchEndpointAccessError):
        _authorize(parameters=parameters)


def test_endpoint_requires_provenance_capability_for_evidence() -> None:
    query_pairs = _query_pairs(include_evidence="true")
    parameters_by_name = dict(query_pairs)
    base_headers = _signed_headers(query_pairs)
    provenance_headers = _signed_headers(
        query_pairs,
        capabilities=[
            "pricing:billing-search",
            "pricing:billing-search:provenance",
        ],
    )

    with pytest.raises(endpoint.BillingSearchEndpointAccessError):
        _authorize(parameters=parameters_by_name, headers=base_headers)
    assert (
        _authorize(
            parameters=parameters_by_name,
            headers=provenance_headers,
        ).request.include_evidence
        is True
    )


def test_endpoint_accepts_sanic_query_and_header_multimaps() -> None:
    query_parameters = RequestParameters(
        {name: [value] for name, value in _query_pairs()}
    )
    request_headers = raw(b"").headers
    for header_name, header_value in _signed_headers().items():
        request_headers[header_name] = header_value

    access = _authorize(parameters=query_parameters, headers=request_headers)

    assert access.request.query_pairs == _query_pairs()
    request_headers.add(
        transport_contract.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER,
        _signed_headers()[transport_contract.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER],
    )
    with pytest.raises(endpoint.BillingSearchEndpointAccessError):
        _authorize(headers=request_headers)


def test_endpoint_requires_closed_unique_signed_headers() -> None:
    valid_headers = _signed_headers()
    assert (
        _authorize(
            headers={**valid_headers, "Authorization": "Bearer synthetic"}
        ).request.plan_release_id
        == PLAN_RELEASE_ID
    )
    assert (
        _authorize(headers=_ListOnlyHeaders(valid_headers)).request.plan_release_id
        == PLAN_RELEASE_ID
    )

    duplicate_case_by_name = {
        **valid_headers,
        transport_contract.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER.lower(): (
            valid_headers[transport_contract.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER]
        ),
    }
    headers_without_signature_by_name = {
        header_name: header_value
        for header_name, header_value in valid_headers.items()
        if header_name != transport_contract.BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER
    }
    unknown_signed_by_name = {
        **valid_headers,
        "X-HealthPorta-Billing-Search-Unknown": "synthetic",
    }
    noncanonical_value_by_name = {
        **valid_headers,
        transport_contract.BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER: " synthetic-get",
    }
    nonstring_name_by_name = {**valid_headers, 1: "synthetic"}
    for invalid_headers in (
        duplicate_case_by_name,
        headers_without_signature_by_name,
        unknown_signed_by_name,
        noncanonical_value_by_name,
        nonstring_name_by_name,
        {},
        object(),
    ):
        with pytest.raises(endpoint.BillingSearchEndpointAccessError):
            _authorize(headers=invalid_headers)


def test_endpoint_loads_injected_and_environment_keyrings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment_by_name = _keyring_environment()
    access = _authorize(keyring=None, environment_map=environment_by_name)
    assert access.request.plan_release_id == PLAN_RELEASE_ID

    monkeypatch.setenv(
        transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV,
        environment_by_name[transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV],
    )
    assert _authorize(keyring=None).request.plan_release_id == PLAN_RELEASE_ID

    with pytest.raises(endpoint.BillingSearchEndpointAccessError):
        _authorize(environment_map=environment_by_name)


def test_endpoint_keyring_failure_drops_environment_document() -> None:
    sensitive_marker = "synthetic-endpoint-key-document-marker"
    environment_by_name = {
        transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: sensitive_marker
    }
    with pytest.raises(endpoint.BillingSearchEndpointAccessError) as captured:
        _authorize(keyring=None, environment_map=environment_by_name)

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert sensitive_marker not in repr(captured.value)
    library_locals = _endpoint_traceback_locals(captured.value)
    assert library_locals
    assert all(
        sensitive_marker not in repr(local_values) for local_values in library_locals
    )


@pytest.mark.parametrize(
    "private_name,replacement_factory",
    [
        ("_BillingSearchEndpointAccess__state_hmac", lambda: b"x" * 32),
        ("_BillingSearchEndpointAccess__verified_transport", object),
        (
            "_BillingSearchEndpointAccess__request",
            lambda: parse_billing_search_request(_parameters(code="00001")),
        ),
    ],
)
def test_endpoint_detects_sealed_access_state_tampering(
    private_name,
    replacement_factory,
) -> None:
    access = _authorize()
    object.__setattr__(access, private_name, replacement_factory())

    with pytest.raises(endpoint.BillingSearchEndpointAccessError):
        endpoint.validate_billing_search_endpoint_access(
            access,
            trusted_now=TRUSTED_NOW,
        )


class _FailureHeaders(dict):
    def __init__(self, values_by_name: Mapping[str, str], failure_surface: str) -> None:
        super().__init__(values_by_name)
        self._failure_surface = failure_surface

    def items(self, *args, **kwargs):
        if self._failure_surface == "items":
            raise RuntimeError("synthetic-header-failure-marker")
        return super().items(*args, **kwargs)

    def getall(self, _name: str):
        if self._failure_surface == "getall":
            raise RuntimeError("synthetic-header-failure-marker")
        raise KeyError


@pytest.mark.parametrize("failure_surface", ["items", "getall"])
def test_endpoint_sanitizes_header_accessor_failures(failure_surface) -> None:
    with pytest.raises(endpoint.BillingSearchEndpointAccessError) as captured:
        _authorize(headers=_FailureHeaders(_signed_headers(), failure_surface))

    assert (
        repr(captured.value)
        == "BillingSearchEndpointAccessError('billing_search_endpoint_access_invalid')"
    )
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert "synthetic-header-failure-marker" not in repr(captured.value)


def test_endpoint_rejects_wrong_types_and_invalid_signature_value_free() -> None:
    invalid_signature_by_name = {
        **_signed_headers(),
        transport_contract.BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER: "A" * 43,
    }
    failure_calls = (
        lambda: endpoint.validate_billing_search_endpoint_access(
            object(),
            trusted_now=TRUSTED_NOW,
        ),
        lambda: _authorize(parameters=object()),
        lambda: _authorize(headers=invalid_signature_by_name),
    )
    for invoke in failure_calls:
        with pytest.raises(
            endpoint.BillingSearchEndpointAccessError,
            match="^billing_search_endpoint_access_invalid$",
        ) as captured:
            invoke()
        assert captured.value.__cause__ is None
        assert captured.value.__context__ is None
        assert BILLING_ENTITY_REF not in repr(captured.value)
