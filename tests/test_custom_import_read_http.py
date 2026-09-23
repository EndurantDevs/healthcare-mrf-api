# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from types import SimpleNamespace

import orjson
import pytest
from sanic import Blueprint, Sanic, response
from sanic.compat import Header

from api import custom_import_read_http as http
from api.endpoint import extension_reads
from process.custom_import.read_contracts import ExtensionReadAuthorization, PinnedReadTarget
from process.custom_import.read_core import (
    ReadChild,
    ReadFieldValue,
    RootDetail,
    SearchItem,
    SearchPage,
    WinnerLocator,
)

_KEY = bytes(range(32))
_CURSOR_KEY = bytes(reversed(range(32)))
_KEY_ID = "test-1"
_NOW = "2031-01-02T03:04:05Z"
_TARGET = {
    "dataset_key": "synthetic_dataset",
    "generation_id": 101,
    "definition_revision_id": 21,
    "schema_revision_id": 31,
    "profile_id": "synthetic_profile",
}
_BODY = (
    b'{"cursor":"synthetic-cursor-a","filters":[{"field_id":"region","operator":"eq",'
    b'"value":"north"},{"field_id":"status","operator":"eq","value":"active"}],'
    b'"page_size":50,"target":'
    b'{"dataset_key":"synthetic_dataset","definition_revision_id":21,"generation_id":101,'
    b'"profile_id":"synthetic_profile","schema_revision_id":31}}'
)
_DETAIL_BODY = (
    b'{"entity":{"adapter_id":"synthetic","value":"binding-synthetic"},"family_entitlement":"full_family","target":'
    b'{"dataset_key":"synthetic_dataset","definition_revision_id":21,"generation_id":101,'
    b'"profile_id":"synthetic_profile","schema_revision_id":31}}'
)
_PROVIDER_VECTOR_BODY = (
    b'{"context":[{"field_id":"region","operator":"eq","value":"north"}],"filters":'
    b'[{"field_id":"status","operator":"eq","value":"active"}],"native_query":'
    b'{"include_total":"true","limit":"2","page":"3","q":"synthetic"},"order":'
    b'[{"direction":"desc","field_id":"score"}],"require_match":true,"target":'
    b'{"dataset_key":"synthetic_dataset","definition_revision_id":21,"generation_id":101,'
    b'"profile_id":"synthetic_profile","schema_revision_id":31}}'
)


def _encoded(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _keyring_document() -> str:
    return json.dumps(
        {
            "active_key_id": _KEY_ID,
            "contract": http.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_CONTRACT,
            "keys": [{"key_id": _KEY_ID, "key_base64url": _encoded(_KEY)}],
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _headers(
    *,
    body: bytes = _BODY,
    scope: str = "a" * 64,
    request_id: str = "123e4567-e89b-42d3-a456-426614174000",
    path: str = http.CUSTOM_IMPORT_READ_PATH,
    contract: str = http.CUSTOM_IMPORT_READ_TRANSPORT_CONTRACT,
    body_sha256=http.custom_import_read_body_sha256,
    signature_message=http.custom_import_read_signature_message,
) -> dict[str, str]:
    context = json.dumps(
        {
            "audience": http.CUSTOM_IMPORT_READ_AUDIENCE,
            "authorization_scope_sha256": scope,
            "body_sha256": body_sha256(body),
            "capability": http.CUSTOM_IMPORT_READ_CAPABILITY,
            "contract": contract,
            "expires_at": "2031-01-02T03:05:05Z",
            "issued_at": "2031-01-02T03:04:05Z",
            "issuer": http.CUSTOM_IMPORT_READ_ISSUER,
            "method": "POST",
            "path": path,
            "request_id": request_id,
            "target": _TARGET,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    signature = hmac.new(
        _KEY,
        signature_message(_KEY_ID, context),
        hashlib.sha256,
    ).digest()
    return {
        http.CUSTOM_IMPORT_READ_CONTEXT_HEADER: _encoded(context),
        http.CUSTOM_IMPORT_READ_KEY_ID_HEADER: _KEY_ID,
        http.CUSTOM_IMPORT_READ_SIGNATURE_HEADER: _encoded(signature),
    }


def _resigned_headers(
    *,
    body: bytes = _BODY,
    path: str = http.CUSTOM_IMPORT_READ_PATH,
    contract: str = http.CUSTOM_IMPORT_READ_TRANSPORT_CONTRACT,
    body_sha256=http.custom_import_read_body_sha256,
    signature_message=http.custom_import_read_signature_message,
    **changes,
) -> dict[str, str]:
    headers = _headers(
        body=body,
        path=path,
        contract=contract,
        body_sha256=body_sha256,
        signature_message=signature_message,
    )
    encoded = headers[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER]
    context = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
    for name, value in changes.items():
        if value is None:
            context.pop(name, None)
        else:
            context[name] = value
    canonical = http._canonical_json_bytes(context)
    headers[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER] = _encoded(canonical)
    headers[http.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = _encoded(
        hmac.new(_KEY, signature_message(_KEY_ID, canonical), hashlib.sha256).digest()
    )
    return headers


def _provider_headers(*, body: bytes = _BODY, path: str) -> dict[str, str]:
    """Create a provider-only v2 permit from the shared synthetic keyring."""

    return _headers(
        body=body,
        path=path,
        contract=http.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
        body_sha256=http.custom_import_provider_body_sha256,
        signature_message=http.custom_import_provider_signature_message,
    )


def _resigned_provider_headers(*, body: bytes = _BODY, path: str, **changes) -> dict[str, str]:
    """Re-sign one provider-only v2 permit after changing its context."""

    return _resigned_headers(
        body=body,
        path=path,
        contract=http.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
        body_sha256=http.custom_import_provider_body_sha256,
        signature_message=http.custom_import_provider_signature_message,
        **changes,
    )


@dataclass
class _Request:
    body: bytes
    headers: dict[str, str]
    method: str = "POST"
    path: str = http.CUSTOM_IMPORT_READ_PATH


class _Service:
    def __init__(self, *, authorizer, cursor_secret=None, **_kwargs) -> None:
        self.authorizer = authorizer
        self.cursor_secret = cursor_secret

    async def search(self, _session, *, authorization, request):
        assert type(authorization) is ExtensionReadAuthorization
        assert self.authorizer.authorize(authorization, target=request.target).value == "a" * 64
        assert self.cursor_secret is not None and len(self.cursor_secret) == 32
        target = request.target
        return SearchPage(
            target=target,
            total=1,
            items=(
                SearchItem(
                    winner=WinnerLocator(1, 2, 3, b"w" * 32),
                    root_fields=(
                        ReadFieldValue("name", "string", "value", "Synthetic"),
                        ReadFieldValue("amount", "decimal", "value", __import__("decimal").Decimal("1.20")),
                        ReadFieldValue("empty", "string", "null", None),
                    ),
                    context_child_revision_id=None,
                    context_fields=(
                        ReadFieldValue("effective", "date", "value", date(2031, 1, 2)),
                        ReadFieldValue("observed", "timestamp", "value", datetime(2031, 1, 2, tzinfo=timezone.utc)),
                    ),
                ),
            ),
            next_cursor="cir1.synthetic",
            expires_at=1_234,
            query_fingerprint="b" * 64,
            authorization_scope_sha256="a" * 64,
        )

    async def root_detail_for_entity(self, _session, *, authorization, request):
        assert type(authorization) is ExtensionReadAuthorization
        assert self.authorizer.authorize(authorization, target=request.target).value == "a" * 64
        assert request.entity.adapter_id == "synthetic"
        assert request.entity.value == "binding-synthetic"
        assert request.family_entitlement == "full_family"
        detail_target = request.target
        return RootDetail(
            target=detail_target,
            winner=WinnerLocator(1, 2, 3, b"w" * 32),
            root_fields=(ReadFieldValue("name", "string", "value", "Synthetic"),),
            children=(
                ReadChild(
                    "rates",
                    1,
                    (ReadFieldValue("amount", "decimal", "value", __import__("decimal").Decimal("1.20")),),
                ),
                ReadChild(
                    "rates",
                    2,
                    (ReadFieldValue("amount", "decimal", "null", None),),
                ),
                ReadChild(
                    "contacts",
                    3,
                    (ReadFieldValue("city", "string", "value", "Synthetic City"),),
                ),
            ),
            authorization_scope_sha256="a" * 64,
        )


def _install_keyring(monkeypatch) -> None:
    http._keyring_for_document.cache_clear()
    http._cursor_secret_for_document.cache_clear()
    monkeypatch.setenv(http.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_ENV, _keyring_document())
    monkeypatch.setenv(http.CUSTOM_IMPORT_READ_CURSOR_SECRET_ENV, _encoded(_CURSOR_KEY))
    monkeypatch.setattr(http, "_trusted_now", lambda: _NOW)


async def _resolved_target(_session, target):
    assert target.dataset_key == "synthetic_dataset"
    return PinnedReadTarget(
        dataset_id=11,
        generation_id=target.generation_id,
        definition_revision_id=target.definition_revision_id,
        schema_revision_id=target.schema_revision_id,
        profile_id=target.profile_id,
    )


def test_fixed_cross_service_signing_vector() -> None:
    headers = _headers()
    assert (
        http.custom_import_read_body_sha256(_BODY) == "92134c5517296b759b9ff515a7990992aebaeb0bb7a27f5d7cb33ebc4e1292cc"
    )
    assert headers[http.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] == "0lPhXUpjLydSp9KP9PfUWW4pKH6urmluooCuO8LdWtQ"


@pytest.mark.parametrize(
    ("path", "signature"),
    (
        ("/api/v1/extensions/custom-import/providers", "MLw8nzQsW5TPQyDLljpQdUalI8i0U3ZBnOrI7iHY7_o"),
        ("/api/v1/extensions/custom-import/providers/geo", "R4lB57L1YbobB2nqunUnZ8-JvsFre5XxVA2LiLYlbA4"),
        ("/api/v1/extensions/custom-import/providers/by-service", "eZOOZTD-XitZGg5lE2xK4rOfcl1xU1kbBa5pXfG-ZEw"),
    ),
)
def test_fixed_provider_v2_cross_service_signing_vectors(path, signature) -> None:
    """Provider routes share v2 framing and bind their distinct paths."""

    headers = _provider_headers(body=_PROVIDER_VECTOR_BODY, path=path)

    assert http.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT == "healthporta.custom-import-extension-read-transport.v2"
    assert http.custom_import_provider_body_sha256(_PROVIDER_VECTOR_BODY) == (
        "27cbb313f4bcd26a4a5c2eccd7c227e2bfca55b167692512707afdd2305217d5"
    )
    assert headers[http.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] == signature


def test_transport_accepts_runtime_multidict_header_keys() -> None:
    request = http._parse_search_request(_BODY)
    verified = http._verify_transport(
        headers=Header({key.lower(): value for key, value in _headers().items()}),
        body=_BODY,
        request=request,
        trusted_now=_NOW,
        keyring=http._load_keyring(_keyring_document()),
    )

    assert verified.target.dataset_key == "synthetic_dataset"


def test_canonical_unicode_filter_values_round_trip() -> None:
    request_by_field = {
        "cursor": None,
        "filters": [{"field_id": "city", "operator": "eq", "value": "Montréal 東京 🙂 \u007f"}],
        "page_size": 50,
        "target": _TARGET,
    }
    body = http._canonical_json_bytes(request_by_field)

    assert body.isascii()
    assert b"Montr\\u00e9al \\u6771\\u4eac \\ud83d\\ude42 \\u007f" in body
    assert http._parse_search_request(body).filters[0].value == request_by_field["filters"][0]["value"]


def test_optional_order_terms_round_trip_with_fixed_null_ordering() -> None:
    request_by_field = json.loads(_BODY)
    request_by_field["order"] = [
        {"field_id": "metric", "direction": "desc"},
        {"field_id": "display_name", "direction": "asc"},
    ]
    parsed = http._parse_search_request(http._canonical_json_bytes(request_by_field))

    assert parsed.order_terms is not None
    assert [(term.field_id, term.direction, term.nulls) for term in parsed.order_terms] == [
        ("metric", "desc", "last"),
        ("display_name", "asc", "last"),
    ]
    assert parsed.bind(11).order_terms == parsed.order_terms
    assert http._parse_search_request(_BODY).order_terms is None


def test_signed_order_direction_cannot_be_changed_without_resigning() -> None:
    request_by_field = json.loads(_BODY)
    request_by_field["order"] = [{"field_id": "metric", "direction": "asc"}]
    body = http._canonical_json_bytes(request_by_field)
    request_by_field["order"][0]["direction"] = "desc"
    tampered_body = http._canonical_json_bytes(request_by_field)

    with pytest.raises(http.CustomImportReadTransportError):
        http._verify_transport(
            headers=_headers(body=body),
            body=tampered_body,
            request=http._parse_search_request(tampered_body),
            trusted_now=_NOW,
            keyring=http._load_keyring(_keyring_document()),
        )


def test_detail_full_family_entitlement_cannot_be_changed_without_resigning() -> None:
    document = json.loads(_DETAIL_BODY)
    body = http._canonical_json_bytes(document)
    document["family_entitlement"] = "root_fields"
    tampered_body = http._canonical_json_bytes(document)

    with pytest.raises(http.CustomImportReadTransportError):
        http._verify_transport(
            headers=_headers(body=body, path=http.CUSTOM_IMPORT_DETAIL_PATH),
            body=tampered_body,
            request=http._parse_detail_request(body),
            trusted_now=_NOW,
            keyring=http._load_keyring(_keyring_document()),
            path=http.CUSTOM_IMPORT_DETAIL_PATH,
        )


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(lambda: http.custom_import_read_body_sha256(b""), id="empty-body"),
        pytest.param(
            lambda: http.custom_import_read_body_sha256(b"x" * (http._MAX_BODY_BYTES + 1)),
            id="large-body",
        ),
        pytest.param(lambda: http.custom_import_read_signature_message("Bad", b"x"), id="key-id"),
        pytest.param(lambda: http.custom_import_read_signature_message("test-1", b""), id="context"),
        pytest.param(lambda: http._base64url_decode(None), id="base64-type"),
        pytest.param(lambda: http._base64url_decode("A"), id="base64-content"),
        pytest.param(lambda: http._base64url_decode("AB"), id="base64-noncanonical"),
        pytest.param(lambda: http._strict_json(b'{"a":1,"a":2}'), id="duplicate-json"),
        pytest.param(lambda: http._strict_json(b'{"a":1.5}'), id="float-json"),
        pytest.param(lambda: http._strict_json(b"\xff"), id="non-ascii-json"),
        pytest.param(lambda: http._canonical_utc("2031-13-02T03:04:05Z"), id="utc-value"),
        pytest.param(lambda: http._canonical_utc(None), id="utc-shape"),
        pytest.param(lambda: http._canonical_sha256("0" * 64), id="zero-digest"),
        pytest.param(lambda: http._load_keyring(_keyring_document()).key_for("missing"), id="missing-key"),
    ],
)
def test_transport_primitives_fail_closed(operation) -> None:
    with pytest.raises(http.CustomImportReadTransportError):
        operation()


@pytest.mark.parametrize(
    "document",
    [
        None,
        "",
        "é",
        "{}",
        '{"active_key_id":"test-1","contract":"wrong","keys":[]}',
        '{"active_key_id":"Bad","contract":"healthporta.custom-import-extension-read-transport-keyring.v1","keys":[]}',
        json.dumps(
            {
                "active_key_id": "test-1",
                "contract": http.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_CONTRACT,
                "keys": [{"key_id": "test-1"}],
            }
        ),
        json.dumps(
            {
                "active_key_id": "test-1",
                "contract": http.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_CONTRACT,
                "keys": [{"key_id": "Bad", "key_base64url": _encoded(_KEY)}],
            }
        ),
        json.dumps(
            {
                "active_key_id": "test-1",
                "contract": http.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_CONTRACT,
                "keys": [{"key_id": "test-1", "key_base64url": _encoded(b"short")}],
            }
        ),
        json.dumps(
            {
                "active_key_id": "missing",
                "contract": http.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_CONTRACT,
                "keys": [{"key_id": "test-1", "key_base64url": _encoded(_KEY)}],
            }
        ),
    ],
)
def test_keyring_rejects_malformed_closed_documents(document) -> None:
    with pytest.raises(http.CustomImportReadTransportError):
        http._load_keyring(document)


def test_closed_headers_reject_malformed_runtime_shapes() -> None:
    class BrokenItems(dict):
        def items(self, *_args, **_kwargs):
            raise RuntimeError("synthetic mapping failure")

    class BrokenAccessor(dict):
        def getall(self, _name):
            raise RuntimeError("synthetic accessor failure")

    class MissingAccessor(dict):
        def getall(self, _name):
            raise KeyError

    class AccessorOnly(dict):
        def __init__(self, values):
            super().__init__()
            self.values = values

        def getall(self, name):
            return [self.values[name]]

    valid = _headers()
    cases = [
        [],
        BrokenItems(valid),
        {1: "value"},
        {**valid, "X-HealthPorta-Extension-Read-Unknown": "value"},
        BrokenAccessor(valid),
        MissingAccessor(valid),
        {},
        {**valid, http.CUSTOM_IMPORT_READ_KEY_ID_HEADER: " test-1"},
    ]
    for headers in cases:
        with pytest.raises(http.CustomImportReadTransportError):
            http._closed_headers(headers)
    assert http._closed_headers(AccessorOnly(valid)) == (
        valid[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER],
        valid[http.CUSTOM_IMPORT_READ_KEY_ID_HEADER],
        valid[http.CUSTOM_IMPORT_READ_SIGNATURE_HEADER],
    )


@pytest.mark.parametrize(
    "document",
    [
        [],
        {"cursor": None, "filters": [], "page_size": 50},
        {"cursor": None, "filters": [], "page_size": 50, "target": {"dataset_key": "synthetic_dataset"}},
        {"cursor": None, "filters": [], "page_size": 50, "target": {**_TARGET, "dataset_key": "Bad"}},
        {"cursor": None, "filters": [], "page_size": 50, "target": {**_TARGET, "generation_id": 0}},
        {"cursor": None, "filters": {}, "page_size": 50, "target": _TARGET},
        {"cursor": None, "filters": ["bad"], "page_size": 50, "target": _TARGET},
        {"cursor": None, "filters": [], "order": [], "page_size": 50, "target": _TARGET},
        {
            "cursor": None,
            "filters": [],
            "order": [{"field_id": "metric", "direction": "up"}],
            "page_size": 50,
            "target": _TARGET,
        },
        {
            "cursor": None,
            "filters": [],
            "order": [{"field_id": "metric", "direction": []}],
            "page_size": 50,
            "target": _TARGET,
        },
        {
            "cursor": None,
            "filters": [],
            "order": [{"field_id": "metric", "direction": "asc", "nulls": "last"}],
            "page_size": 50,
            "target": _TARGET,
        },
        {
            "cursor": None,
            "filters": [{"field_id": "name", "operator": "eq"}],
            "page_size": 50,
            "target": _TARGET,
        },
        {
            "cursor": None,
            "filters": [{"field_id": "Bad", "operator": "eq", "value": "x"}],
            "page_size": 50,
            "target": _TARGET,
        },
    ],
)
def test_search_parser_rejects_malformed_closed_documents(document) -> None:
    body = http._canonical_json_bytes(document)
    with pytest.raises(http.CustomImportReadTransportError):
        http._parse_search_request(body)


def test_search_parser_requires_canonical_json() -> None:
    with pytest.raises(http.CustomImportReadTransportError):
        http._parse_search_request(json.dumps(json.loads(_BODY)).encode("ascii"))


@pytest.mark.parametrize(
    "document",
    (
        {"target": _TARGET},
        {"entity": "synthetic", "target": _TARGET},
        {"entity": {"adapter_id": "synthetic", "value": "synthetic"}, "family_entitlement": False, "target": _TARGET},
        {
            "entity": {"adapter_id": "synthetic", "value": "synthetic"},
            "family_entitlement": "root_fields",
            "target": _TARGET,
        },
        {"entity": {"adapter_id": "synthetic", "value": ""}, "target": _TARGET},
        {"entity": {"adapter_id": "wrong-adapter", "value": "synthetic"}, "target": _TARGET},
        {"entity": {"adapter_id": "synthetic", "value": "x" * 513}, "target": _TARGET},
        {"entity": {"adapter_id": "synthetic", "value": "synthetic"}, "target": _TARGET, "cursor": None},
    ),
)
def test_detail_parser_rejects_malformed_closed_documents(document) -> None:
    with pytest.raises(http.CustomImportReadTransportError):
        http._parse_detail_request(http._canonical_json_bytes(document))


def test_detail_parser_binds_generic_entity_without_a_vendor_policy() -> None:
    parsed = http._parse_detail_request(_DETAIL_BODY)
    request = parsed.bind(11)

    assert request.target == PinnedReadTarget(11, 101, 21, 31, "synthetic_profile")
    assert (request.entity.adapter_id, request.entity.value) == ("synthetic", "binding-synthetic")
    assert request.family_entitlement == "full_family"


@pytest.mark.parametrize(
    "changes",
    [
        {"contract": None},
        {"expires_at": "2031-01-02T03:04:05Z"},
        {"issued_at": "2031-01-02T03:05:06Z"},
        {"request_id": "not-a-uuid"},
        {"issuer": "another-gateway"},
    ],
)
def test_verified_transport_rejects_resigned_context_mismatches(changes) -> None:
    with pytest.raises(http.CustomImportReadTransportError):
        http._verify_transport(
            headers=_resigned_headers(**changes),
            body=_BODY,
            request=http._parse_search_request(_BODY),
            trusted_now=_NOW,
            keyring=http._load_keyring(_keyring_document()),
        )


def test_verified_transport_rejects_noncanonical_signed_context() -> None:
    headers = _headers()
    raw = base64.urlsafe_b64decode(
        headers[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER]
        + "=" * (-len(headers[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER]) % 4)
    )
    noncanonical = json.dumps(json.loads(raw), sort_keys=True).encode("ascii")
    headers[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER] = _encoded(noncanonical)
    headers[http.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = _encoded(
        hmac.new(_KEY, http.custom_import_read_signature_message(_KEY_ID, noncanonical), hashlib.sha256).digest()
    )
    with pytest.raises(http.CustomImportReadTransportError):
        http._verify_transport(
            headers=headers,
            body=_BODY,
            request=http._parse_search_request(_BODY),
            trusted_now=_NOW,
            keyring=http._load_keyring(_keyring_document()),
        )


@pytest.mark.parametrize("signature", ["A" * 129, _encoded(b"short")])
def test_verified_transport_rejects_header_bounds(signature) -> None:
    headers = _headers()
    headers[http.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = signature
    with pytest.raises(http.CustomImportReadTransportError):
        http._verify_transport(
            headers=headers,
            body=_BODY,
            request=http._parse_search_request(_BODY),
            trusted_now=_NOW,
            keyring=http._load_keyring(_keyring_document()),
        )


def test_transport_authorizer_rejects_wrong_artifacts() -> None:
    request = http._parse_search_request(_BODY)
    verified = http._verify_transport(
        headers=_headers(),
        body=_BODY,
        request=request,
        trusted_now=_NOW,
        keyring=http._load_keyring(_keyring_document()),
    )
    target = request.bind(11).target
    authorizer = http._TransportAuthorizer(verified, target)

    assert authorizer.authorize(object(), target=target) is None
    assert authorizer.authorize(ExtensionReadAuthorization("wrong"), target=target) is None
    assert authorizer.authorize(ExtensionReadAuthorization(verified.credential), target=request.bind(12).target) is None


def test_cursor_and_error_helpers_fail_closed() -> None:
    with pytest.raises(http.CustomImportReadTransportError):
        http._cursor_secret_for_document(_encoded(b"short"))
    assert http._failure_status(http.CustomImportReadRequestError("synthetic")) == 400
    assert http._failure_status(RuntimeError("synthetic")) == 503
    assert http._UTC.fullmatch(http._trusted_now()) is not None


@pytest.mark.asyncio
async def test_signed_search_authorizes_before_service_and_returns_typed_payload(monkeypatch) -> None:
    _install_keyring(monkeypatch)
    monkeypatch.setattr(http, "CustomImportReadService", _Service)
    monkeypatch.setattr(http, "_resolve_pinned_target", _resolved_target)

    http_response = await http.serve_custom_import_search(_Request(_BODY, _headers()), object())

    assert http_response.status == 200
    assert http_response.headers["Cache-Control"] == "private, no-store"
    assert orjson.loads(http_response.body) == {
        "target": _TARGET,
        "total": 1,
        "items": [
            {
                "root_fields": [
                    {"field_id": "name", "field_type": "string", "state": "value", "value": "Synthetic"},
                    {"field_id": "amount", "field_type": "decimal", "state": "value", "value": "1.20"},
                    {"field_id": "empty", "field_type": "string", "state": "null", "value": None},
                ],
                "context_fields": [
                    {"field_id": "effective", "field_type": "date", "state": "value", "value": "2031-01-02"},
                    {
                        "field_id": "observed",
                        "field_type": "timestamp",
                        "state": "value",
                        "value": "2031-01-02T00:00:00Z",
                    },
                ],
            }
        ],
        "next_cursor": "cir1.synthetic",
        "expires_at": 1_234,
    }


@pytest.mark.asyncio
async def test_signed_detail_returns_one_full_family_with_multiple_collections(monkeypatch) -> None:
    _install_keyring(monkeypatch)
    http._cursor_secret_for_document.cache_clear()
    monkeypatch.delenv(http.CUSTOM_IMPORT_READ_CURSOR_SECRET_ENV)
    monkeypatch.setattr(http, "CustomImportReadService", _Service)
    monkeypatch.setattr(http, "_resolve_pinned_target", _resolved_target)
    request = _Request(
        _DETAIL_BODY,
        _headers(body=_DETAIL_BODY, path=http.CUSTOM_IMPORT_DETAIL_PATH),
        path=http.CUSTOM_IMPORT_DETAIL_PATH,
    )

    http_response = await http.serve_custom_import_detail(request, object())

    assert http_response.status == 200
    assert orjson.loads(http_response.body) == {
        "target": _TARGET,
        "root_fields": [{"field_id": "name", "field_type": "string", "state": "value", "value": "Synthetic"}],
        "children": [
            {
                "collection": "rates",
                "fields": [{"field_id": "amount", "field_type": "decimal", "state": "value", "value": "1.20"}],
            },
            {
                "collection": "rates",
                "fields": [{"field_id": "amount", "field_type": "decimal", "state": "null", "value": None}],
            },
            {
                "collection": "contacts",
                "fields": [{"field_id": "city", "field_type": "string", "state": "value", "value": "Synthetic City"}],
            },
        ],
    }


@pytest.mark.asyncio
async def test_registered_route_forwards_the_bound_session(monkeypatch) -> None:
    session = object()
    observed_calls = []

    async def serve(request, candidate_session):
        observed_calls.append((request.method, request.path, candidate_session))
        return response.json({"ok": True})

    monkeypatch.setattr(extension_reads, "serve_custom_import_search", serve)
    app = Sanic(f"custom-import-read-route-{uuid.uuid4().hex}")

    @app.middleware("request")
    async def bind(request):
        request.ctx.sa_session = session

    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    _request, result = await app.asgi_client.post(http.CUSTOM_IMPORT_READ_PATH, data=_BODY)

    assert result.status_code == 200
    assert result.json == {"ok": True}
    assert observed_calls == [("POST", http.CUSTOM_IMPORT_READ_PATH, session)]


@pytest.mark.asyncio
async def test_detail_route_forwards_the_bound_session(monkeypatch) -> None:
    session = object()
    observed_calls = []

    async def serve(request, candidate_session):
        observed_calls.append((request.method, request.path, candidate_session))
        return response.json({"ok": True})

    monkeypatch.setattr(extension_reads, "serve_custom_import_detail", serve)
    app = Sanic(f"custom-import-detail-route-{uuid.uuid4().hex}")

    @app.middleware("request")
    async def bind(request):
        request.ctx.sa_session = session

    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    _request, result = await app.asgi_client.post(http.CUSTOM_IMPORT_DETAIL_PATH, data=_DETAIL_BODY)

    assert result.status_code == 200
    assert result.json == {"ok": True}
    assert observed_calls == [("POST", http.CUSTOM_IMPORT_DETAIL_PATH, session)]


@pytest.mark.asyncio
async def test_invalid_signature_stops_before_service_or_sql(monkeypatch) -> None:
    _install_keyring(monkeypatch)

    class ForbiddenService:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("invalid permit reached service construction")

    monkeypatch.setattr(http, "CustomImportReadService", ForbiddenService)
    headers = _headers()
    headers[http.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = "A" * 43

    class NoSqlSession:
        async def execute(self, _statement):
            raise AssertionError("invalid permit reached SQL")

    result = await http.serve_custom_import_search(_Request(_BODY, headers), NoSqlSession())

    assert result.status == 404
    assert orjson.loads(result.body) == {"error": {"code": "resource_not_found", "message": "Resource not found."}}


@pytest.mark.asyncio
async def test_missing_detail_capability_stops_before_service_or_sql(monkeypatch) -> None:
    _install_keyring(monkeypatch)

    class ForbiddenService:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("missing capability reached service construction")

    monkeypatch.setattr(http, "CustomImportReadService", ForbiddenService)

    class NoSqlSession:
        async def execute(self, _statement):
            raise AssertionError("missing capability reached SQL")

    result = await http.serve_custom_import_detail(
        _Request(
            _DETAIL_BODY,
            _resigned_headers(
                body=_DETAIL_BODY,
                path=http.CUSTOM_IMPORT_DETAIL_PATH,
                capability="other:capability",
            ),
            path=http.CUSTOM_IMPORT_DETAIL_PATH,
        ),
        NoSqlSession(),
    )

    assert result.status == 404
    assert result.headers["Cache-Control"] == "private, no-store"
    assert orjson.loads(result.body) == {"error": {"code": "resource_not_found", "message": "Resource not found."}}


@pytest.mark.asyncio
@pytest.mark.parametrize("family_entitlement", (None, False, "root_fields"))
async def test_detail_requires_full_family_entitlement_before_service_or_sql(monkeypatch, family_entitlement) -> None:
    _install_keyring(monkeypatch)

    class ForbiddenService:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("detail entitlement reached service construction")

    class NoSqlSession:
        calls = 0

        async def execute(self, _statement):
            self.calls += 1
            raise AssertionError("detail entitlement reached SQL")

    request_body_map = {"entity": {"adapter_id": "synthetic", "value": "binding-synthetic"}, "target": _TARGET}
    if family_entitlement is not None:
        request_body_map["family_entitlement"] = family_entitlement
    body = http._canonical_json_bytes(request_body_map)
    session = NoSqlSession()
    monkeypatch.setattr(http, "CustomImportReadService", ForbiddenService)

    result = await http.serve_custom_import_detail(
        _Request(body, _headers(body=body, path=http.CUSTOM_IMPORT_DETAIL_PATH), path=http.CUSTOM_IMPORT_DETAIL_PATH),
        session,
    )

    assert result.status == 404
    assert session.calls == 0


@pytest.mark.asyncio
async def test_missing_keyring_stops_before_service_or_sql(monkeypatch) -> None:
    http._keyring_for_document.cache_clear()

    class ForbiddenService:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("missing keyring reached service construction")

    monkeypatch.delenv(http.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_ENV, raising=False)
    monkeypatch.setattr(http, "CustomImportReadService", ForbiddenService)

    class NoSqlSession:
        async def execute(self, _statement):
            raise AssertionError("missing keyring reached SQL")

    result = await http.serve_custom_import_search(_Request(_BODY, _headers()), NoSqlSession())

    assert result.status == 404


@pytest.mark.asyncio
async def test_missing_cursor_secret_stops_before_sql(monkeypatch) -> None:
    _install_keyring(monkeypatch)
    http._cursor_secret_for_document.cache_clear()
    monkeypatch.delenv(http.CUSTOM_IMPORT_READ_CURSOR_SECRET_ENV)

    class NoSqlSession:
        async def execute(self, _statement):
            raise AssertionError("missing cursor secret reached SQL")

    result = await http.serve_custom_import_search(_Request(_BODY, _headers()), NoSqlSession())

    assert result.status == 404


def test_cursor_secret_is_independent_of_transport_key_rotation(monkeypatch) -> None:
    _install_keyring(monkeypatch)
    before = http._cursor_secret()
    monkeypatch.setenv(
        http.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_ENV,
        _keyring_document().replace('"active_key_id":"test-1"', '"active_key_id":"test-2"'),
    )

    assert http._cursor_secret() == before


@pytest.mark.asyncio
async def test_malformed_page_stops_before_sql(monkeypatch) -> None:
    _install_keyring(monkeypatch)
    body = _BODY.replace(b'"page_size":50', b'"page_size":0')

    class NoSqlSession:
        async def execute(self, _statement):
            raise AssertionError("malformed request reached SQL")

    result = await http.serve_custom_import_search(_Request(body, _headers(body=body)), NoSqlSession())

    assert result.status == 404


@pytest.mark.asyncio
async def test_malformed_operator_stops_before_sql(monkeypatch) -> None:
    _install_keyring(monkeypatch)
    body = (
        b'{"cursor":null,"filters":[{"field_id":"name","operator":[]}],"page_size":2,"target":'
        b'{"dataset_key":"synthetic_dataset","definition_revision_id":21,"generation_id":101,'
        b'"profile_id":"synthetic_profile","schema_revision_id":31}}'
    )

    class NoSqlSession:
        async def execute(self, _statement):
            raise AssertionError("malformed operator reached SQL")

    result = await http.serve_custom_import_search(_Request(body, _headers(body=body)), NoSqlSession())

    assert result.status == 404


@pytest.mark.asyncio
async def test_missing_session_returns_bounded_no_store_error(monkeypatch) -> None:
    _install_keyring(monkeypatch)

    result = await http.serve_custom_import_search(_Request(_BODY, _headers()), None)

    assert result.status == 503
    assert result.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
async def test_stalled_dataset_resolution_uses_the_shared_read_timeout(monkeypatch) -> None:
    _install_keyring(monkeypatch)
    monkeypatch.setattr(http, "DEFAULT_READ_TIMEOUT_MS", 1)

    class StalledSession:
        async def execute(self, _statement):
            await asyncio.Event().wait()

    result = await http.serve_custom_import_search(_Request(_BODY, _headers()), StalledSession())

    assert result.status == 503
    assert result.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
async def test_wrong_path_stops_before_keyring_and_service(monkeypatch) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("wrong path crossed the route gate")

    monkeypatch.setattr(http, "_keyring", forbidden)
    monkeypatch.setattr(http, "CustomImportReadService", forbidden)

    result = await http.serve_custom_import_search(
        _Request(_BODY, _headers(), path="/api/v1/extensions/custom-import/detail"), object()
    )

    assert result.status == 404


@pytest.mark.asyncio
async def test_wrong_method_and_invalid_body_use_bounded_errors(monkeypatch) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("invalid request crossed the route gate")

    monkeypatch.setattr(http, "_keyring", forbidden)
    wrong_method = await http.serve_custom_import_search(_Request(_BODY, _headers(), method="GET"), object())
    invalid_body = await http.serve_custom_import_search(_Request(b"", _headers()), object())

    assert wrong_method.status == 404
    assert invalid_body.status == 404


@pytest.mark.asyncio
async def test_oversized_response_fails_closed(monkeypatch) -> None:
    _install_keyring(monkeypatch)
    monkeypatch.setattr(http, "CustomImportReadService", _Service)
    monkeypatch.setattr(http, "_resolve_pinned_target", _resolved_target)
    monkeypatch.setattr(http, "_MAX_RESPONSE_BYTES", 1)

    result = await http.serve_custom_import_search(_Request(_BODY, _headers()), object())

    assert result.status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_type", "expected_status", "expected_error"),
    (
        (
            http.CustomImportReadEntityAbsentError,
            404,
            {
                "code": "custom_import_entity_absent",
                "message": "Requested custom import entity was not found.",
            },
        ),
        (
            http.CustomImportReadUnavailableError,
            503,
            {
                "code": "custom_import_read_unavailable",
                "message": "Custom import read is temporarily unavailable.",
            },
        ),
        (
            http.CustomImportReadAuthorizationError,
            404,
            {"code": "resource_not_found", "message": "Resource not found."},
        ),
    ),
)
async def test_detail_root_maps_absence_distinct_from_unavailable(
    monkeypatch, failure_type, expected_status, expected_error
) -> None:
    _install_keyring(monkeypatch)

    class DetailFailureService(_Service):
        async def root_detail_for_entity(self, _session, *, authorization, request):
            assert self.authorizer.authorize(authorization, target=request.target).value == "a" * 64
            raise failure_type("synthetic detail failure")

    monkeypatch.setattr(http, "CustomImportReadService", DetailFailureService)
    monkeypatch.setattr(http, "_resolve_pinned_target", _resolved_target)
    result = await http.serve_custom_import_detail(
        _Request(
            _DETAIL_BODY,
            _headers(body=_DETAIL_BODY, path=http.CUSTOM_IMPORT_DETAIL_PATH),
            path=http.CUSTOM_IMPORT_DETAIL_PATH,
        ),
        object(),
    )

    assert result.status == expected_status
    assert result.headers["Cache-Control"] == "private, no-store"
    assert orjson.loads(result.body) == {"error": expected_error}


@pytest.mark.asyncio
async def test_detail_absence_before_transport_authorization_fails_closed(monkeypatch) -> None:
    _install_keyring(monkeypatch)

    def absent(*_args, **_kwargs):
        raise http.CustomImportReadEntityAbsentError("synthetic detail failure")

    monkeypatch.setattr(http, "_verify_transport", absent)
    result = await http.serve_custom_import_detail(
        _Request(
            _DETAIL_BODY,
            _headers(body=_DETAIL_BODY, path=http.CUSTOM_IMPORT_DETAIL_PATH),
            path=http.CUSTOM_IMPORT_DETAIL_PATH,
        ),
        object(),
    )

    assert result.status == 503
    assert result.headers["Cache-Control"] == "private, no-store"
    assert orjson.loads(result.body) == {
        "error": {"code": "custom_import_read_unavailable", "message": "Custom import read is temporarily unavailable."}
    }


@pytest.mark.asyncio
async def test_endpoint_forwards_request_session(monkeypatch) -> None:
    session = object()
    request = SimpleNamespace(ctx=SimpleNamespace(sa_session=session))
    observed_calls = []

    async def serve(candidate_request, candidate_session):
        observed_calls.append((candidate_request, candidate_session))
        return "synthetic-response"

    monkeypatch.setattr(extension_reads, "serve_custom_import_search", serve)

    assert await extension_reads.search(request) == "synthetic-response"
    assert observed_calls == [(request, session)]


def test_duplicate_transport_header_is_rejected() -> None:
    class DuplicateHeaders(dict):
        def getall(self, _name):
            return [self[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER]] * 2

    request = http._parse_search_request(_BODY)
    with pytest.raises(http.CustomImportReadTransportError):
        http._verify_transport(
            headers=DuplicateHeaders(_headers()),
            body=_BODY,
            request=request,
            trusted_now=_NOW,
            keyring=http._load_keyring(_keyring_document()),
        )


@pytest.mark.asyncio
async def test_dataset_key_resolution_uses_one_exact_unique_lookup() -> None:
    statements = []

    class Result:
        def scalar_one_or_none(self):
            return 11

    class Session:
        async def execute(self, statement):
            statements.append(statement)
            return Result()

    target = http._parse_target(_TARGET)
    pinned = await http._resolve_pinned_target(Session(), target)

    assert pinned == PinnedReadTarget(11, 101, 21, 31, "synthetic_profile")
    assert len(statements) == 1
    compiled = statements[0].compile()
    assert compiled.params == {"dataset_key_1": "synthetic_dataset"}
    assert "custom_import_dataset.dataset_key" in str(statements[0])


@pytest.mark.asyncio
async def test_nonunique_dataset_resolution_fails_closed() -> None:
    class Result:
        def scalar_one_or_none(self):
            raise RuntimeError("unexpected duplicate")

    class Session:
        async def execute(self, _statement):
            return Result()

    with pytest.raises(http.CustomImportReadUnavailableError):
        await http._resolve_pinned_target(Session(), http._parse_target(_TARGET))


@pytest.mark.asyncio
async def test_missing_dataset_returns_closed_not_found(monkeypatch) -> None:
    _install_keyring(monkeypatch)

    class Result:
        def scalar_one_or_none(self):
            return None

    class Session:
        async def execute(self, _statement):
            return Result()

    result = await http.serve_custom_import_search(_Request(_BODY, _headers()), Session())

    assert result.status == 404
    assert result.headers["Cache-Control"] == "private, no-store"


def test_transport_target_mismatch_is_rejected() -> None:
    request = http._parse_search_request(_BODY)
    keyring = http._load_keyring(_keyring_document())
    context_headers = _headers()
    raw_context = base64.urlsafe_b64decode(
        context_headers[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER]
        + "=" * (-len(context_headers[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER]) % 4)
    )
    decoded = json.loads(raw_context)
    decoded["target"]["generation_id"] = 102
    context = json.dumps(decoded, separators=(",", ":"), sort_keys=True).encode("ascii")
    context_headers[http.CUSTOM_IMPORT_READ_CONTEXT_HEADER] = _encoded(context)
    context_headers[http.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = _encoded(
        hmac.new(_KEY, http.custom_import_read_signature_message(_KEY_ID, context), hashlib.sha256).digest()
    )

    with pytest.raises(http.CustomImportReadTransportError):
        http._verify_transport(
            headers=context_headers,
            body=_BODY,
            request=request,
            trusted_now=_NOW,
            keyring=keyring,
        )


def test_detail_transport_requires_its_exact_signed_path_and_target() -> None:
    request = http._parse_detail_request(_DETAIL_BODY)
    headers = _headers(body=_DETAIL_BODY, path=http.CUSTOM_IMPORT_DETAIL_PATH)
    keyring = http._load_keyring(_keyring_document())

    assert (
        http._verify_transport(
            headers=headers,
            body=_DETAIL_BODY,
            request=request,
            trusted_now=_NOW,
            keyring=keyring,
            path=http.CUSTOM_IMPORT_DETAIL_PATH,
        ).target
        == request.target
    )
    with pytest.raises(http.CustomImportReadTransportError):
        http._verify_transport(
            headers=headers,
            body=_DETAIL_BODY,
            request=request,
            trusted_now=_NOW,
            keyring=keyring,
        )


def test_transport_requires_canonical_uuid4_request_id() -> None:
    request = http._parse_search_request(_BODY)
    with pytest.raises(http.CustomImportReadTransportError):
        http._verify_transport(
            headers=_headers(request_id="123e4567-e89b-12d3-a456-426614174000"),
            body=_BODY,
            request=request,
            trusted_now=_NOW,
            keyring=http._load_keyring(_keyring_document()),
        )
