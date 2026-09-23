# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed HTTP boundary for generic custom-import extension reads."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import re
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import orjson
from sanic import response
from sqlalchemy import select

from db.models.custom_import import CustomImportDataset
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    CustomImportReadAuthorizationError,
    CustomImportReadCursorError,
    CustomImportReadEntityAbsentError,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
    ExtensionReadScope,
    PinnedReadTarget,
)
from process.custom_import.read_core import (
    CustomImportReadService,
    EntityLocator,
    ReadFilter,
    ReadOrderTerm,
    RootDetailRequest,
    SearchRequest,
)

if TYPE_CHECKING:
    from api.custom_import_provider_http import _ParsedProviderRequest

CUSTOM_IMPORT_READ_TRANSPORT_CONTRACT = "healthporta.custom-import-extension-read-transport.v1"
CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT = "healthporta.custom-import-extension-read-transport.v2"
CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_CONTRACT = "healthporta.custom-import-extension-read-transport-keyring.v1"
CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_ENV = "HLTHPRT_CUSTOM_IMPORT_EXTENSION_READ_TRANSPORT_KEYRING_JSON"
CUSTOM_IMPORT_READ_CURSOR_SECRET_ENV = "HLTHPRT_CUSTOM_IMPORT_READ_CURSOR_SECRET_BASE64URL"
CUSTOM_IMPORT_READ_CONTEXT_HEADER = "X-HealthPorta-Extension-Read-Context"
CUSTOM_IMPORT_READ_KEY_ID_HEADER = "X-HealthPorta-Extension-Read-Key-Id"
CUSTOM_IMPORT_READ_SIGNATURE_HEADER = "X-HealthPorta-Extension-Read-Signature"
CUSTOM_IMPORT_READ_PATH = "/api/v1/extensions/custom-import/search"
CUSTOM_IMPORT_DETAIL_PATH = "/api/v1/extensions/custom-import/detail"
CUSTOM_IMPORT_READ_ISSUER = "healthporta-extension-gateway"
CUSTOM_IMPORT_READ_AUDIENCE = "healthcare-mrf-api"
CUSTOM_IMPORT_READ_CAPABILITY = "custom-import:extension-read"
CUSTOM_IMPORT_READ_MAX_TTL_SECONDS = 60

_CACHE_CONTROL = "private, no-store"
_MAX_BODY_BYTES = 16 * 1024
_MAX_RESPONSE_BYTES = 256 * 1024
_MAX_CONTEXT_BYTES = 2048
_MAX_CONTEXT_CHARACTERS = 3072
_MAX_KEYRING_BYTES = 4096
_MAX_KEYS = 4
_KEY_BYTES = 32
_SIGNATURE_BYTES = 32
_BODY_HASH_DOMAIN = b"HEALTHPORTA_CUSTOM_IMPORT_EXTENSION_READ_BODY_V1\x00"
_SIGNATURE_DOMAIN = b"HEALTHPORTA_CUSTOM_IMPORT_EXTENSION_READ_TRANSPORT_V1\x00"
_PROVIDER_BODY_HASH_DOMAIN = b"HEALTHPORTA_CUSTOM_IMPORT_EXTENSION_READ_BODY_V2\x00"
_PROVIDER_SIGNATURE_DOMAIN = b"HEALTHPORTA_CUSTOM_IMPORT_EXTENSION_READ_TRANSPORT_V2\x00"
_CURSOR_DOMAIN = b"HEALTHPORTA_CUSTOM_IMPORT_EXTENSION_READ_CURSOR_V1\x00"
_HEADER_PREFIX = "x-healthporta-extension-read-"
_HEADER_NAMES = (
    CUSTOM_IMPORT_READ_CONTEXT_HEADER,
    CUSTOM_IMPORT_READ_KEY_ID_HEADER,
    CUSTOM_IMPORT_READ_SIGNATURE_HEADER,
)
_CONTEXT_FIELDS = frozenset(
    {
        "audience",
        "authorization_scope_sha256",
        "body_sha256",
        "capability",
        "contract",
        "expires_at",
        "issued_at",
        "issuer",
        "method",
        "path",
        "request_id",
        "target",
    }
)
_ALLOWED_HEADER_NAMES = frozenset(value.lower() for value in _HEADER_NAMES)
_KEY_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,31}", flags=re.ASCII)
_BASE64URL = re.compile(r"[A-Za-z0-9_-]+", flags=re.ASCII)
_SHA256 = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_UTC = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", flags=re.ASCII)
_DATASET_KEY = re.compile(r"[a-z][a-z0-9_]{0,62}", flags=re.ASCII)
_ERRORS = {
    400: ("custom_import_read_request_invalid", "Invalid custom import read request."),
    404: ("resource_not_found", "Resource not found."),
    503: ("custom_import_read_unavailable", "Custom import read is temporarily unavailable."),
}
_ENTITY_ABSENT_ERROR = ("custom_import_entity_absent", "Requested custom import entity was not found.")
logger = logging.getLogger(__name__)


class CustomImportReadTransportError(RuntimeError):
    """Value-free failure at the signed extension-read boundary."""


def _fail() -> CustomImportReadTransportError:
    return CustomImportReadTransportError("custom_import_read_transport_invalid")


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _framed_sha256(domain: bytes, value: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)
    return digest.hexdigest()


def custom_import_read_body_sha256(body: bytes) -> str:
    """Return the framed digest that a gateway signs for one canonical body."""

    if type(body) is not bytes or not 1 <= len(body) <= _MAX_BODY_BYTES:
        raise _fail()
    return _framed_sha256(_BODY_HASH_DOMAIN, body)


def custom_import_provider_body_sha256(body: bytes) -> str:
    """Return the provider-v2 framed digest for one canonical body."""

    if type(body) is not bytes or not 1 <= len(body) <= _MAX_BODY_BYTES:
        raise _fail()
    return _framed_sha256(_PROVIDER_BODY_HASH_DOMAIN, body)


def custom_import_read_signature_message(key_id: str, context: bytes) -> bytes:
    """Return the exact HMAC input used by the fixed cross-service contract."""

    if _KEY_ID.fullmatch(key_id) is None or not 1 <= len(context) <= _MAX_CONTEXT_BYTES:
        raise _fail()
    encoded_key_id = key_id.encode("ascii")
    return b"".join(
        (
            _SIGNATURE_DOMAIN,
            len(encoded_key_id).to_bytes(2, "big"),
            encoded_key_id,
            len(context).to_bytes(8, "big"),
            context,
        )
    )


def custom_import_provider_signature_message(key_id: str, context: bytes) -> bytes:
    """Return the exact provider-v2 HMAC input for a signed context."""

    if _KEY_ID.fullmatch(key_id) is None or not 1 <= len(context) <= _MAX_CONTEXT_BYTES:
        raise _fail()
    encoded_key_id = key_id.encode("ascii")
    return b"".join(
        (
            _PROVIDER_SIGNATURE_DOMAIN,
            len(encoded_key_id).to_bytes(2, "big"),
            encoded_key_id,
            len(context).to_bytes(8, "big"),
            context,
        )
    )


@dataclass(frozen=True, slots=True)
class _TransportContract:
    """Keep one transport's fixed framing values together."""

    name: str
    body_sha256: Callable[[bytes], str]
    signature_message: Callable[[str, bytes], bytes]
    credential_domain: bytes


_GENERIC_TRANSPORT = _TransportContract(
    CUSTOM_IMPORT_READ_TRANSPORT_CONTRACT,
    custom_import_read_body_sha256,
    custom_import_read_signature_message,
    _SIGNATURE_DOMAIN,
)
_PROVIDER_TRANSPORT = _TransportContract(
    CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
    custom_import_provider_body_sha256,
    custom_import_provider_signature_message,
    _PROVIDER_SIGNATURE_DOMAIN,
)


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _base64url_decode(value: object) -> bytes:
    if type(value) is not str or _BASE64URL.fullmatch(value) is None:
        raise _fail()
    try:
        decoded = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except binascii.Error, ValueError:
        raise _fail() from None
    if not decoded or not hmac.compare_digest(_base64url_encode(decoded), value):
        raise _fail()
    return decoded


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    members_by_name: dict[str, object] = {}
    for name, member in pairs:
        if name in members_by_name:
            raise ValueError
        members_by_name[name] = member
    return members_by_name


def _reject_json_number(_value: str) -> None:
    raise ValueError


def _strict_json(raw: bytes) -> object:
    try:
        return json.loads(
            raw.decode("ascii"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_number,
            parse_float=_reject_json_number,
        )
    except UnicodeDecodeError, json.JSONDecodeError, ValueError:
        raise _fail() from None


def _canonical_utc(value: object) -> tuple[str, datetime]:
    if type(value) is not str or _UTC.fullmatch(value) is None:
        raise _fail()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        raise _fail() from None
    return value, parsed


def _canonical_sha256(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None or value == "0" * 64:
        raise _fail()
    return value


@dataclass(frozen=True, slots=True)
class _Keyring:
    active_key_id: str
    keys: tuple[tuple[str, bytes], ...]

    def key_for(self, key_id: str) -> bytes:
        """Return the exact configured transport key or fail closed."""

        for candidate, key in self.keys:
            if hmac.compare_digest(candidate, key_id):
                return key
        raise _fail()


def _load_keyring(document: object) -> _Keyring:
    if type(document) is not str:
        raise _fail()
    try:
        encoded = document.encode("ascii")
    except UnicodeEncodeError:
        raise _fail() from None
    if not 1 <= len(encoded) <= _MAX_KEYRING_BYTES:
        raise _fail()
    parsed = _strict_json(encoded)
    if (
        type(parsed) is not dict
        or frozenset(parsed) != {"active_key_id", "contract", "keys"}
        or parsed.get("contract") != CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_CONTRACT
        or type(parsed.get("keys")) is not list
        or not 1 <= len(parsed["keys"]) <= _MAX_KEYS
        or _KEY_ID.fullmatch(parsed.get("active_key_id", "")) is None
    ):
        raise _fail()
    keys: list[tuple[str, bytes]] = []
    for key_entry in parsed["keys"]:
        if type(key_entry) is not dict or frozenset(key_entry) != {"key_id", "key_base64url"}:
            raise _fail()
        key_id = key_entry.get("key_id")
        if type(key_id) is not str or _KEY_ID.fullmatch(key_id) is None:
            raise _fail()
        key = _base64url_decode(key_entry.get("key_base64url"))
        if len(key) != _KEY_BYTES:
            raise _fail()
        keys.append((key_id, key))
    normalized_keys = tuple(sorted(keys))
    if (
        len({key_id for key_id, _key in normalized_keys}) != len(normalized_keys)
        or len({key for _key_id, key in normalized_keys}) != len(normalized_keys)
        or parsed["active_key_id"] not in {key_id for key_id, _key in normalized_keys}
    ):
        raise _fail()
    return _Keyring(active_key_id=parsed["active_key_id"], keys=normalized_keys)


@lru_cache(maxsize=1)
def _keyring_for_document(document: str | None) -> _Keyring:
    return _load_keyring(document)


def _keyring() -> _Keyring:
    return _keyring_for_document(os.environ.get(CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_ENV))


def _closed_headers(headers: Mapping[str, Any]) -> tuple[str, str, str]:
    if not isinstance(headers, Mapping):
        raise _fail()
    try:
        header_pairs = list(headers.items(multi=True))
    except TypeError:
        header_pairs = list(headers.items())
    except Exception:
        raise _fail() from None
    for name, _header_value in header_pairs:
        if not isinstance(name, str):
            raise _fail()
        if name.lower().startswith(_HEADER_PREFIX) and name.lower() not in _ALLOWED_HEADER_NAMES:
            raise _fail()
    header_values: list[str] = []
    for expected in _HEADER_NAMES:
        matched_values = [
            header_value
            for name, header_value in header_pairs
            if isinstance(name, str) and name.lower() == expected.lower()
        ]
        accessor = getattr(headers, "getall", None)
        if not callable(accessor):
            accessor = getattr(headers, "getlist", None)
        if callable(accessor):
            try:
                accessor_values = list(accessor(expected))
            except KeyError, TypeError:
                accessor_values = []
            except Exception:
                raise _fail() from None
            if len(accessor_values) != 1 or (matched_values and matched_values != accessor_values):
                raise _fail()
            if not matched_values:
                matched_values = accessor_values
        if len(matched_values) != 1:
            raise _fail()
        header_value = matched_values[0]
        if (
            not isinstance(header_value, str)
            or not header_value
            or header_value != header_value.strip()
            or not header_value.isascii()
            or not header_value.isprintable()
        ):
            raise _fail()
        header_values.append(header_value)
    return header_values[0], header_values[1], header_values[2]


@dataclass(frozen=True, slots=True)
class _TransportTarget:
    dataset_key: str
    generation_id: int
    definition_revision_id: int
    schema_revision_id: int
    profile_id: str


@dataclass(frozen=True, slots=True)
class _ParsedSearchRequest:
    target: _TransportTarget
    filters: tuple[ReadFilter, ...]
    order_terms: tuple[ReadOrderTerm, ...] | None
    page_size: int
    cursor: str | None

    def bind(self, dataset_id: int) -> SearchRequest:
        """Bind the verified external request to one internal dataset ID."""

        return SearchRequest(
            target=PinnedReadTarget(
                dataset_id=dataset_id,
                generation_id=self.target.generation_id,
                definition_revision_id=self.target.definition_revision_id,
                schema_revision_id=self.target.schema_revision_id,
                profile_id=self.target.profile_id,
            ),
            filters=self.filters,
            order_terms=self.order_terms,
            page_size=self.page_size,
            cursor=self.cursor,
        )


@dataclass(frozen=True, slots=True)
class _ParsedDetailRequest:
    target: _TransportTarget
    entity: EntityLocator
    family_entitlement: str

    def bind(self, dataset_id: int) -> RootDetailRequest:
        """Bind the verified external request to one internal dataset ID."""

        return RootDetailRequest(
            target=PinnedReadTarget(
                dataset_id=dataset_id,
                generation_id=self.target.generation_id,
                definition_revision_id=self.target.definition_revision_id,
                schema_revision_id=self.target.schema_revision_id,
                profile_id=self.target.profile_id,
            ),
            entity=self.entity,
            family_entitlement=self.family_entitlement,
        )


def _parse_target(value: object) -> _TransportTarget:
    if type(value) is not dict or frozenset(value) != {
        "dataset_key",
        "generation_id",
        "definition_revision_id",
        "schema_revision_id",
        "profile_id",
    }:
        raise _fail()
    try:
        target = _TransportTarget(**value)
    except TypeError:
        raise _fail() from None
    if type(target.dataset_key) is not str or _DATASET_KEY.fullmatch(target.dataset_key) is None:
        raise _fail()
    try:
        PinnedReadTarget(
            dataset_id=1,
            generation_id=target.generation_id,
            definition_revision_id=target.definition_revision_id,
            schema_revision_id=target.schema_revision_id,
            profile_id=target.profile_id,
        )
    except CustomImportReadRequestError:
        raise _fail() from None
    return target


def _target_document(target: _TransportTarget) -> dict[str, object]:
    return {
        "dataset_key": target.dataset_key,
        "generation_id": target.generation_id,
        "definition_revision_id": target.definition_revision_id,
        "schema_revision_id": target.schema_revision_id,
        "profile_id": target.profile_id,
    }


def _parse_search_request(body: bytes) -> _ParsedSearchRequest:
    """Parse one canonical, closed, signed search request body."""

    document = _strict_json(body)
    base_fields = frozenset({"cursor", "filters", "page_size", "target"})
    if type(document) is not dict or frozenset(document) not in {base_fields, base_fields | {"order"}}:
        raise _fail()
    if _canonical_json_bytes(document) != body:
        raise _fail()
    transport_target = _parse_target(document.get("target"))
    read_filters = _parse_filter_documents(document.get("filters"))
    order_terms = _parse_order_documents(document.get("order")) if "order" in document else None
    try:
        parsed_request = _ParsedSearchRequest(
            target=transport_target,
            filters=read_filters,
            order_terms=order_terms,
            page_size=document.get("page_size"),
            cursor=document.get("cursor"),
        )
        parsed_request.bind(1)
        return parsed_request
    except CustomImportReadRequestError, TypeError:
        raise _fail() from None


def _parse_detail_request(body: bytes) -> _ParsedDetailRequest:
    """Parse one canonical, closed, signed entity detail request body."""

    document = _strict_json(body)
    if type(document) is not dict or frozenset(document) != {"entity", "family_entitlement", "target"}:
        raise _fail()
    if _canonical_json_bytes(document) != body:
        raise _fail()
    try:
        parsed_request = _ParsedDetailRequest(
            target=_parse_target(document.get("target")),
            entity=_parse_entity(document.get("entity")),
            family_entitlement=document.get("family_entitlement"),
        )
        parsed_request.bind(1)
        return parsed_request
    except CustomImportReadRequestError, TypeError:
        raise _fail() from None


def _parse_entity(value: object) -> EntityLocator:
    if type(value) is not dict or frozenset(value) != {"adapter_id", "value"}:
        raise _fail()
    try:
        return EntityLocator(adapter_id=value.get("adapter_id"), value=value.get("value"))
    except CustomImportReadRequestError:
        raise _fail() from None


def _parse_filter_documents(filter_documents: object) -> tuple[ReadFilter, ...]:
    """Parse the bounded structured filter list."""

    if type(filter_documents) is not list or len(filter_documents) > 3:
        raise _fail()
    read_filters: list[ReadFilter] = []
    for filter_document in filter_documents:
        if type(filter_document) is not dict:
            raise _fail()
        operator = filter_document.get("operator")
        if type(operator) is not str:
            raise _fail()
        expected_fields = {"field_id", "operator"}
        if operator not in {"is_null", "is_missing"}:
            expected_fields.add("value")
        if frozenset(filter_document) != expected_fields:
            raise _fail()
        try:
            read_filters.append(
                ReadFilter(
                    field_id=filter_document.get("field_id"),
                    operator=operator,
                    value=filter_document.get("value"),
                )
            )
        except CustomImportReadRequestError:
            raise _fail() from None
    return tuple(read_filters)


def _parse_order_documents(order_documents: object) -> tuple[ReadOrderTerm, ...]:
    """Parse request-selected order terms with the fixed null policy."""

    if type(order_documents) is not list or not 1 <= len(order_documents) <= 3:
        raise _fail()
    order_terms: list[ReadOrderTerm] = []
    for order_document in order_documents:
        if type(order_document) is not dict or frozenset(order_document) != {"field_id", "direction"}:
            raise _fail()
        try:
            order_terms.append(
                ReadOrderTerm(
                    field_id=order_document.get("field_id"),
                    direction=order_document.get("direction"),
                    nulls="last",
                )
            )
        except CustomImportReadRequestError:
            raise _fail() from None
    return tuple(order_terms)


@dataclass(frozen=True, slots=True)
class _VerifiedTransport:
    credential: str
    scope: str
    target: _TransportTarget


def _verified_context(
    headers: Mapping[str, Any],
    keyring: _Keyring,
    *,
    transport_contract: _TransportContract = _GENERIC_TRANSPORT,
) -> tuple[bytes, dict[str, object]]:
    context_header, key_id, signature_header = _closed_headers(headers)
    if (
        len(context_header) > _MAX_CONTEXT_CHARACTERS
        or len(signature_header) > 128
        or _KEY_ID.fullmatch(key_id) is None
    ):
        raise _fail()
    context = _base64url_decode(context_header)
    signature = _base64url_decode(signature_header)
    if len(context) > _MAX_CONTEXT_BYTES or len(signature) != _SIGNATURE_BYTES:
        raise _fail()
    expected_signature = hmac.new(
        keyring.key_for(key_id),
        transport_contract.signature_message(key_id, context),
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(signature, expected_signature):
        raise _fail()
    context_fields = _strict_json(context)
    if (
        type(context_fields) is not dict
        or _canonical_json_bytes(context_fields) != context
        or frozenset(context_fields) != _CONTEXT_FIELDS
    ):
        raise _fail()
    return context, context_fields


def _verify_transport(
    *,
    headers: Mapping[str, Any],
    body: bytes,
    request: _ParsedSearchRequest | _ParsedDetailRequest | _ParsedProviderRequest,
    trusted_now: str,
    keyring: _Keyring,
    path: str = CUSTOM_IMPORT_READ_PATH,
    transport_contract: _TransportContract = _GENERIC_TRANSPORT,
) -> _VerifiedTransport:
    """Verify one signed transport permit against the exact request."""

    context, context_fields = _verified_context(headers, keyring, transport_contract=transport_contract)
    issued_at, issued = _canonical_utc(context_fields.get("issued_at"))
    expires_at, expires = _canonical_utc(context_fields.get("expires_at"))
    _now, now = _canonical_utc(trusted_now)
    if (
        not 0 < (expires - issued).total_seconds() <= CUSTOM_IMPORT_READ_MAX_TTL_SECONDS
        or now < issued
        or now >= expires
    ):
        raise _fail()
    try:
        request_id = uuid.UUID(context_fields.get("request_id"))
    except AttributeError, TypeError, ValueError:
        raise _fail() from None
    if request_id.version != 4 or str(request_id) != context_fields["request_id"]:
        raise _fail()
    expected_by_field = {
        "audience": CUSTOM_IMPORT_READ_AUDIENCE,
        "body_sha256": transport_contract.body_sha256(body),
        "capability": CUSTOM_IMPORT_READ_CAPABILITY,
        "contract": transport_contract.name,
        "issuer": CUSTOM_IMPORT_READ_ISSUER,
        "method": "POST",
        "path": path,
    }
    for name, expected_value in expected_by_field.items():
        actual_value = context_fields.get(name)
        if type(actual_value) is not str or not hmac.compare_digest(actual_value, expected_value):
            raise _fail()
    scope = _canonical_sha256(context_fields.get("authorization_scope_sha256"))
    transport_target = _parse_target(context_fields.get("target"))
    if transport_target != request.target or _target_document(transport_target) != context_fields["target"]:
        raise _fail()
    credential = _framed_sha256(transport_contract.credential_domain, context)
    return _VerifiedTransport(credential=credential, scope=scope, target=transport_target)


def _verify_provider_transport(
    *,
    headers: Mapping[str, Any],
    body: bytes,
    request: _ParsedProviderRequest,
    trusted_now: str,
    keyring: _Keyring,
    path: str,
) -> _VerifiedTransport:
    """Verify the provider-only v2 transport without changing generic reads."""

    return _verify_transport(
        headers=headers,
        body=body,
        request=request,
        trusted_now=trusted_now,
        keyring=keyring,
        path=path,
        transport_contract=_PROVIDER_TRANSPORT,
    )


class _TransportAuthorizer:
    """Constrain one already-verified HTTP permit to its exact target."""

    def __init__(self, transport: _VerifiedTransport, target: PinnedReadTarget) -> None:
        self._transport = transport
        self._target = target

    def authorize(
        self,
        authorization: ExtensionReadAuthorization,
        *,
        target: PinnedReadTarget,
    ) -> ExtensionReadScope | None:
        """Return the signed scope only for the exact verified target."""

        if (
            type(authorization) is not ExtensionReadAuthorization
            or target != self._target
            or not hmac.compare_digest(authorization.credential, self._transport.credential)
        ):
            return None
        return ExtensionReadScope(self._transport.scope)


async def _resolve_pinned_target(session: Any, target: _TransportTarget) -> PinnedReadTarget:
    """Resolve the stable external dataset key only after permit verification."""

    try:
        result = await session.execute(
            select(CustomImportDataset.dataset_id).where(CustomImportDataset.dataset_key == target.dataset_key)
        )
        dataset_id = result.scalar_one_or_none()
    except Exception:
        raise CustomImportReadUnavailableError("pinned dataset is unavailable") from None
    if dataset_id is None:
        raise CustomImportReadAuthorizationError("extension read is not authorized")
    try:
        return PinnedReadTarget(
            dataset_id=dataset_id,
            generation_id=target.generation_id,
            definition_revision_id=target.definition_revision_id,
            schema_revision_id=target.schema_revision_id,
            profile_id=target.profile_id,
        )
    except Exception:
        raise CustomImportReadUnavailableError("pinned dataset is unavailable") from None


@lru_cache(maxsize=1)
def _cursor_secret_for_document(document: str | None) -> bytes:
    secret = _base64url_decode(document)
    if len(secret) != _KEY_BYTES:
        raise _fail()
    return hmac.new(
        secret,
        _CURSOR_DOMAIN,
        hashlib.sha256,
    ).digest()


def _cursor_secret() -> bytes:
    return _cursor_secret_for_document(os.environ.get(CUSTOM_IMPORT_READ_CURSOR_SECRET_ENV))


def _trusted_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _field_value(value: Any) -> dict[str, object]:
    encoded: object = value.value
    if value.state == "value":
        if value.field_type == "decimal":
            encoded = format(encoded, "f")
        elif value.field_type == "date":
            encoded = encoded.isoformat()
        elif value.field_type == "timestamp":
            encoded = encoded.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "field_id": value.field_id,
        "field_type": value.field_type,
        "state": value.state,
        "value": encoded,
    }


def _page_payload(page: Any, target: _TransportTarget) -> dict[str, object]:
    return {
        "target": _target_document(target),
        "total": page.total,
        "items": [_search_item_payload(item) for item in page.items],
        "next_cursor": page.next_cursor,
        "expires_at": page.expires_at,
    }


def _search_item_payload(item: Any) -> dict[str, object]:
    return {
        "root_fields": [_field_value(value) for value in item.root_fields],
        "context_fields": [_field_value(value) for value in item.context_fields],
    }


def _detail_payload(detail: Any, target: _TransportTarget) -> dict[str, object]:
    return {
        "target": _target_document(target),
        "root_fields": [_field_value(value) for value in detail.root_fields],
        "children": [
            {
                "collection": child.collection,
                "fields": [_field_value(value) for value in child.fields],
            }
            for child in detail.children
        ],
    }


def _response(body: bytes, status: int):
    return response.raw(
        body,
        status=status,
        headers={"Cache-Control": _CACHE_CONTROL},
        content_type="application/json",
    )


def _error(status: int, *, detail: tuple[str, str] | None = None):
    code, message = _ERRORS[status] if detail is None else detail
    return _response(orjson.dumps({"error": {"code": code, "message": message}}), status)


def _failure_status(failure: Exception) -> int:
    if isinstance(failure, (CustomImportReadTransportError, CustomImportReadAuthorizationError)):
        return 404
    if isinstance(failure, (CustomImportReadRequestError, CustomImportReadCursorError)):
        return 400
    return 503


def _log_failure(failure: BaseException) -> None:
    logger.warning(
        "Custom import extension read failed",
        extra={"custom_import_read_failure_class": type(failure).__name__},
    )


async def serve_custom_import_search(request: Any, session: Any):
    """Serve one signed, pinned custom-import search without fallback modes."""

    if getattr(request, "method", None) != "POST" or getattr(request, "path", None) != CUSTOM_IMPORT_READ_PATH:
        return _error(404)
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= _MAX_BODY_BYTES:
            raise _fail()
        parsed_request = _parse_search_request(body)
        keyring = _keyring()
        verified = _verify_transport(
            headers=request.headers,
            body=body,
            request=parsed_request,
            trusted_now=_trusted_now(),
            keyring=keyring,
        )
        cursor_secret = _cursor_secret()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + DEFAULT_READ_TIMEOUT_MS / 1_000
        async with asyncio.timeout_at(deadline):
            pinned_target = await _resolve_pinned_target(session, parsed_request.target)
        remaining_timeout_ms = int((deadline - loop.time()) * 1_000)
        if remaining_timeout_ms < 1:
            raise CustomImportReadUnavailableError("custom import read timed out")
        search_request = parsed_request.bind(pinned_target.dataset_id)
        service = CustomImportReadService(
            authorizer=_TransportAuthorizer(verified, pinned_target),
            cursor_secret=cursor_secret,
            statement_timeout_ms=remaining_timeout_ms,
        )
        page = await service.search(
            session,
            authorization=ExtensionReadAuthorization(verified.credential),
            request=search_request,
        )
        encoded = orjson.dumps(_page_payload(page, parsed_request.target))
        if len(encoded) > _MAX_RESPONSE_BYTES:
            raise CustomImportReadUnavailableError("custom import response exceeds the bound")
    except Exception as failure:
        _log_failure(failure)
        return _error(_failure_status(failure))
    return _response(encoded, 200)


async def serve_custom_import_detail(request: Any, session: Any):
    """Serve one signed, pinned entity detail without cursor fallback modes."""

    if getattr(request, "method", None) != "POST" or getattr(request, "path", None) != CUSTOM_IMPORT_DETAIL_PATH:
        return _error(404)
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= _MAX_BODY_BYTES:
            raise _fail()
        parsed_request = _parse_detail_request(body)
        keyring = _keyring()
        verified = _verify_transport(
            headers=request.headers,
            body=body,
            request=parsed_request,
            trusted_now=_trusted_now(),
            keyring=keyring,
            path=CUSTOM_IMPORT_DETAIL_PATH,
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + DEFAULT_READ_TIMEOUT_MS / 1_000
        async with asyncio.timeout_at(deadline):
            pinned_target = await _resolve_pinned_target(session, parsed_request.target)
        remaining_timeout_ms = int((deadline - loop.time()) * 1_000)
        if remaining_timeout_ms < 1:
            raise CustomImportReadUnavailableError("custom import read timed out")
        detail_request = parsed_request.bind(pinned_target.dataset_id)
        service = CustomImportReadService(
            authorizer=_TransportAuthorizer(verified, pinned_target),
            statement_timeout_ms=remaining_timeout_ms,
        )
        detail = await service.root_detail_for_entity(
            session,
            authorization=ExtensionReadAuthorization(verified.credential),
            request=detail_request,
        )
        encoded = orjson.dumps(_detail_payload(detail, parsed_request.target))
        if len(encoded) > _MAX_RESPONSE_BYTES:
            raise CustomImportReadUnavailableError("custom import response exceeds the bound")
    except CustomImportReadEntityAbsentError:
        return _error(404, detail=_ENTITY_ABSENT_ERROR)
    except Exception as failure:
        _log_failure(failure)
        return _error(_failure_status(failure))
    return _response(encoded, 200)


__all__ = (
    "CUSTOM_IMPORT_READ_AUDIENCE",
    "CUSTOM_IMPORT_READ_CAPABILITY",
    "CUSTOM_IMPORT_READ_CONTEXT_HEADER",
    "CUSTOM_IMPORT_READ_CURSOR_SECRET_ENV",
    "CUSTOM_IMPORT_DETAIL_PATH",
    "CUSTOM_IMPORT_READ_ISSUER",
    "CUSTOM_IMPORT_READ_KEY_ID_HEADER",
    "CUSTOM_IMPORT_READ_PATH",
    "CUSTOM_IMPORT_READ_SIGNATURE_HEADER",
    "CUSTOM_IMPORT_READ_TRANSPORT_CONTRACT",
    "CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_CONTRACT",
    "CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_ENV",
    "CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT",
    "custom_import_provider_body_sha256",
    "custom_import_provider_signature_message",
    "custom_import_read_body_sha256",
    "custom_import_read_signature_message",
    "serve_custom_import_detail",
    "serve_custom_import_search",
)
