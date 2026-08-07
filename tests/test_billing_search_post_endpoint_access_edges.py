# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
from collections.abc import Iterator, Mapping
import copy
import hashlib
import hmac
import json
import pickle

import pytest

from api import billing_search_post_endpoint_access as endpoint_access
from api import billing_search_post_gateway_transport as gateway_transport
from api import billing_search_post_transport as post_transport
from api import billing_search_transport_keys as transport_keys

PLAN_ID = "hpplan_" + "0" * 26
PLAN_RELEASE_ID = "hprelease_" + "0" * 26
KEY_ID = "synthetic-edge"
KEY = bytes(range(32))
NOW = "2026-08-07T10:00:10Z"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _body() -> bytes:
    return json.dumps(
        {
            "billing_identity": {
                "tax_identity": {"type": "ein", "value": "12-3333333"}
            },
            "geo": {"radius_miles": 0, "zip5": "00000"},
            "healthporta_plan_id": PLAN_ID,
            "procedure": {
                "code": "00000",
                "code_system": "CPT",
                "modifiers": [],
                "place_of_service": [],
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _request_shape(body: bytes) -> str:
    return post_transport.parse_billing_search_post_transport(
        body,
        method=post_transport.BILLING_SEARCH_POST_METHOD,
        path=post_transport.BILLING_SEARCH_POST_PATH,
        media_type=post_transport.BILLING_SEARCH_POST_MEDIA_TYPE,
    ).request_shape_sha256


def _context(body: bytes, **updates: object) -> dict[str, object]:
    context_by_field: dict[str, object] = {
        "audience": gateway_transport.BILLING_SEARCH_TRANSPORT_AUDIENCE,
        "audit_scope_sha256": _sha("audit-edge"),
        "capabilities": ["pricing:billing-search"],
        "contract": gateway_transport.BILLING_SEARCH_POST_TRANSPORT_CONTRACT,
        "expires_at": "2026-08-07T10:01:00Z",
        "issued_at": "2026-08-07T10:00:00Z",
        "issuer": gateway_transport.BILLING_SEARCH_TRANSPORT_ISSUER,
        "media_type": post_transport.BILLING_SEARCH_POST_MEDIA_TYPE,
        "metering_receipt_sha256": "0" * 64,
        "metering_request_id": "00000000-0000-4000-8000-000000000000",
        "method": post_transport.BILLING_SEARCH_POST_METHOD,
        "path": post_transport.BILLING_SEARCH_POST_PATH,
        "plan_entitlement_sha256": (
            gateway_transport.billing_search_plan_entitlement_sha256(PLAN_RELEASE_ID)
        ),
        "plan_release_id": PLAN_RELEASE_ID,
        "principal_scope_sha256": _sha("principal-edge"),
        "quota_scope_sha256": _sha("quota-edge"),
        "request_shape_sha256": _request_shape(body),
        "tenant_scope_sha256": _sha("tenant-edge"),
    }
    context_by_field.update(updates)
    context_by_field["metering_receipt_sha256"] = (
        gateway_transport._metering_receipt_sha256(context_by_field)
    )
    return context_by_field


def _signed_headers(
    body: bytes,
    context_by_field: dict[str, object] | None = None,
) -> dict[str, str]:
    context_bytes = gateway_transport._canonical_json_bytes(
        _context(body) if context_by_field is None else context_by_field
    )
    signature = hmac.new(
        KEY,
        gateway_transport._signature_message(KEY_ID, context_bytes, body),
        hashlib.sha256,
    ).digest()
    return {
        gateway_transport.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER: _base64url(
            context_bytes
        ),
        gateway_transport.BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER: KEY_ID,
        gateway_transport.BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER: _base64url(
            signature
        ),
    }


def _keyring() -> transport_keys.BillingSearchTransportKeyring:
    return transport_keys.BillingSearchTransportKeyring(
        active_key_id=KEY_ID,
        keys_by_id={KEY_ID: KEY},
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


def _authorize(
    body: bytes,
    headers: Mapping[str, object],
) -> endpoint_access.BillingSearchPostEndpointAccess:
    return endpoint_access.authorize_billing_search_post_endpoint(
        body,
        headers,
        method=post_transport.BILLING_SEARCH_POST_METHOD,
        path=post_transport.BILLING_SEARCH_POST_PATH,
        media_type=post_transport.BILLING_SEARCH_POST_MEDIA_TYPE,
        trusted_now=NOW,
        keyring=_keyring(),
    )


def _billing_search_library_traceback_locals(
    error: BaseException,
) -> list[dict[str, object]]:
    library_locals = []
    current_error: BaseException | None = error
    while current_error is not None:
        traceback = current_error.__traceback__
        while traceback is not None:
            module_name = traceback.tb_frame.f_globals.get("__name__", "")
            if module_name in {
                "api.billing_search_post_endpoint_access",
                "api.billing_search_transport_keys",
            }:
                library_locals.append(traceback.tb_frame.f_locals)
            traceback = traceback.tb_next
        current_error = current_error.__context__
    return library_locals


class _ItemsNotCallable:
    items = None


class _FallbackItemsFailure:
    def items(self, **options: object) -> object:
        if options:
            raise TypeError
        raise RuntimeError


class _PrimaryItemsFailure:
    def items(self, **_options: object) -> object:
        raise RuntimeError


class _AccessorFallback:
    def getall(self, _name: str) -> list[str]:
        raise KeyError

    def getlist(self, _name: str) -> list[str]:
        return ["synthetic-value"]


class _AccessorFailure:
    def getall(self, _name: str) -> list[str]:
        raise RuntimeError


class _AccessorOnlyHeaders(Mapping[str, str]):
    def __init__(self, values_by_name: Mapping[str, str]) -> None:
        self._values_by_name = dict(values_by_name)

    def __getitem__(self, name: str) -> str:
        return self._values_by_name[name]

    def __iter__(self) -> Iterator[str]:
        return iter(())

    def __len__(self) -> int:
        return 0

    def items(self, multi: bool = False):
        del multi
        return []

    def getall(self, _name: str) -> list[str]:
        raise KeyError

    def getlist(self, name: str) -> list[str]:
        return [self._values_by_name[name]]


@pytest.mark.parametrize(
    "mapping",
    [_ItemsNotCallable(), _FallbackItemsFailure(), _PrimaryItemsFailure()],
)
def test_endpoint_mapping_enumeration_failures_are_closed(mapping: object) -> None:
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access._mapping_items(mapping)


def test_endpoint_header_accessor_fallback_and_failure_are_closed() -> None:
    assert endpoint_access._accessor_values(
        _AccessorFallback(),
        "synthetic-header",
    ) == ["synthetic-value"]
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access._accessor_values(
            _AccessorFailure(),
            "synthetic-header",
        )


def test_endpoint_header_collection_rejects_ambiguous_shapes() -> None:
    body = _body()
    headers = _signed_headers(body)
    assert endpoint_access._closed_header_values(
        _AccessorOnlyHeaders(headers)
    ) == tuple(headers.values())

    invalid_headers_by_name = dict(headers)
    invalid_headers_by_name[
        gateway_transport.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER
    ] = " noncanonical"
    for candidate in (object(), {1: "synthetic"}, {}, invalid_headers_by_name):
        with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
            endpoint_access._closed_header_values(candidate)


def test_endpoint_keyring_loading_covers_both_environment_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = _keyring_environment()
    loaded = endpoint_access._request_keyring(None, environment)
    assert loaded.active_key_id == KEY_ID

    monkeypatch.setenv(
        transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV,
        environment[transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV],
    )
    assert endpoint_access._request_keyring(None, None).active_key_id == KEY_ID

    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access._request_keyring(_keyring(), environment)


@pytest.mark.parametrize("use_process_environment", [False, True])
def test_endpoint_keyring_failure_does_not_retain_environment_document(
    monkeypatch: pytest.MonkeyPatch,
    use_process_environment: bool,
) -> None:
    sensitive_marker = "synthetic-endpoint-key-document-marker"
    environment_by_name = {
        transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: sensitive_marker
    }
    endpoint_options_by_name = {"environment_map": environment_by_name}
    if use_process_environment:
        monkeypatch.setenv(
            transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV,
            sensitive_marker,
        )
        endpoint_options_by_name = {}
    body = _body()
    with pytest.raises(
        endpoint_access.BillingSearchPostEndpointAccessError
    ) as captured:
        endpoint_access.authorize_billing_search_post_endpoint(
            body,
            _signed_headers(body),
            method=post_transport.BILLING_SEARCH_POST_METHOD,
            path=post_transport.BILLING_SEARCH_POST_PATH,
            media_type=post_transport.BILLING_SEARCH_POST_MEDIA_TYPE,
            trusted_now=NOW,
            **endpoint_options_by_name,
        )

    library_locals = _billing_search_library_traceback_locals(captured.value)
    assert library_locals
    assert all(
        sensitive_marker not in repr(local_values) for local_values in library_locals
    )


def test_endpoint_header_failure_does_not_retain_environment_document() -> None:
    sensitive_marker = "synthetic-header-failure-key-document-marker"
    environment_by_name = {
        transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: sensitive_marker
    }
    with pytest.raises(
        endpoint_access.BillingSearchPostEndpointAccessError
    ) as captured:
        endpoint_access.authorize_billing_search_post_endpoint(
            _body(),
            {},
            method=post_transport.BILLING_SEARCH_POST_METHOD,
            path=post_transport.BILLING_SEARCH_POST_PATH,
            media_type=post_transport.BILLING_SEARCH_POST_MEDIA_TYPE,
            trusted_now=NOW,
            environment_map=environment_by_name,
        )

    library_locals = _billing_search_library_traceback_locals(captured.value)
    assert library_locals
    assert all(
        sensitive_marker not in repr(local_values) for local_values in library_locals
    )


def test_endpoint_rejecting_two_key_sources_does_not_retain_environment() -> None:
    sensitive_marker = "synthetic-dual-source-key-document-marker"
    environment_by_name = {
        transport_keys.BILLING_SEARCH_TRANSPORT_KEYRING_ENV: sensitive_marker
    }
    with pytest.raises(
        endpoint_access.BillingSearchPostEndpointAccessError
    ) as captured:
        endpoint_access._request_keyring(_keyring(), environment_by_name)

    library_locals = _billing_search_library_traceback_locals(captured.value)
    assert library_locals
    assert all(
        sensitive_marker not in repr(local_values) for local_values in library_locals
    )


def test_endpoint_access_object_protocol_is_immutable_and_redacted() -> None:
    body = _body()
    access = _authorize(body, _signed_headers(body))

    assert access.authorization_context.capabilities == ("pricing:billing-search",)
    assert copy.copy(access) is access
    assert copy.deepcopy(access) is access
    with pytest.raises(TypeError) as set_error:
        access.synthetic = "blocked"
    with pytest.raises(TypeError) as delete_error:
        del access.synthetic
    with pytest.raises(
        endpoint_access.BillingSearchPostEndpointAccessError
    ) as pickle_error:
        pickle.dumps(access)
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access.BillingSearchPostEndpointAccess()
    protocol_errors = (set_error.value, delete_error.value, pickle_error.value)
    protocol_frames = []
    for protocol_error in protocol_errors:
        traceback = protocol_error.__traceback__
        while traceback is not None:
            if traceback.tb_frame.f_globals.get("__name__") == (
                "api.billing_search_post_endpoint_access"
            ):
                protocol_frames.append(traceback.tb_frame.f_locals)
            traceback = traceback.tb_next
    assert protocol_frames
    assert all("self" not in local_values for local_values in protocol_frames)
    assert all(
        "12-3333333" not in repr(local_values) for local_values in protocol_frames
    )


def test_endpoint_access_revalidation_rejects_type_and_shape_mismatch() -> None:
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access.validate_billing_search_post_endpoint_access(
            object(),
            trusted_now=NOW,
        )

    body = _body()
    context_by_field = _context(
        body,
        request_shape_sha256=_sha("different-shape"),
    )
    headers = _signed_headers(body, context_by_field)
    verified = gateway_transport.verify_billing_search_post_transport(
        headers[gateway_transport.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER],
        headers[gateway_transport.BILLING_SEARCH_TRANSPORT_KEY_ID_HEADER],
        headers[gateway_transport.BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER],
        body_bytes=body,
        keyring=_keyring(),
        trusted_now=NOW,
    )
    request = post_transport.parse_billing_search_post_transport(
        body,
        method=post_transport.BILLING_SEARCH_POST_METHOD,
        path=post_transport.BILLING_SEARCH_POST_PATH,
        media_type=post_transport.BILLING_SEARCH_POST_MEDIA_TYPE,
    )
    access = endpoint_access._new_access(request, verified)
    assert (
        endpoint_access._validated_access_or_none(
            access,
            trusted_now=NOW,
        )
        is None
    )


def test_endpoint_translates_parser_and_authentication_failures() -> None:
    valid_body = _body()
    malformed_body = b"{"
    context_by_field = _context(
        valid_body,
        request_shape_sha256=_sha("malformed-body"),
    )
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        _authorize(
            malformed_body,
            _signed_headers(malformed_body, context_by_field),
        )

    headers = _signed_headers(valid_body)
    signature_name = gateway_transport.BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER
    headers[signature_name] = headers[signature_name][:-1] + "A"
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        _authorize(valid_body, headers)
