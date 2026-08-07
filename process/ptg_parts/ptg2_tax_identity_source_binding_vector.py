# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical digest for durable source-binding payloads."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from typing import Any

PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT = (
    "ptg2_tax_identity_source_binding_vector_v1"
)

_ERROR = "ptg2_tax_identity_source_binding_vector_invalid"
_DOMAIN = b"PTG2TAXSOURCEBINDINGS\x01"
_MAX_BIGINT = 2**63 - 1
_POLICY_ID = re.compile(r"ptg-tin-hmac-sha256-v1:[a-z0-9](?:[a-z0-9._-]{0,31})\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_RECORD_BYTES = 65
_RECORD_FORMAT = "ptg2_provider_group_tax_identity_v1"
_NUMERIC_FIELDS = (
    ("format_version", 1),
    ("record_bytes", _RECORD_BYTES),
    ("artifact_byte_count", None),
    ("provider_group_count", None),
    ("matched_ein_count", None),
    ("missing_count", None),
    ("malformed_count", None),
    ("unsupported_type_count", None),
)


class TaxIdentitySourceBindingVectorError(ValueError):
    """A value-free binding-vector validation failure."""


def _fail() -> TaxIdentitySourceBindingVectorError:
    return TaxIdentitySourceBindingVectorError(_ERROR)


def _strict_int(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= _MAX_BIGINT
    ):
        raise _fail()
    return value


def _strict_digest(value: object) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise _fail()
    digest = bytes(value)
    if len(digest) != 32:
        raise _fail()
    return digest


def _strict_sha256(value: object) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_policy(value: object) -> str:
    if (
        not isinstance(value, str)
        or _POLICY_ID.fullmatch(value) is None
        or len(value.encode("ascii")) > 55
    ):
        raise _fail()
    return value


def _digest_ascii(digest: Any, value: object) -> None:
    if not isinstance(value, str):
        raise _fail()
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        raise _fail() from None
    digest.update(len(encoded).to_bytes(4, "big"))
    digest.update(encoded)


def tax_identity_source_binding_vector_digest(
    binding_values_by_source: Iterable[Mapping[str, Any]],
) -> bytes:
    """Hash every durable binding payload field in dense source-key order."""

    if isinstance(binding_values_by_source, (str, bytes, bytearray, Mapping)):
        raise _fail()
    try:
        bindings = tuple(binding_values_by_source)
    except Exception:
        raise _fail() from None
    digest = hashlib.sha256()
    digest.update(_DOMAIN)
    _digest_ascii(digest, PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT)
    digest.update(len(bindings).to_bytes(4, "big"))
    for expected_source_key, binding_by_field in enumerate(bindings):
        if not isinstance(binding_by_field, Mapping):
            raise _fail()
        source_key = _strict_int(binding_by_field.get("source_key"))
        source_type = binding_by_field.get("source_type")
        identity_kind = binding_by_field.get("identity_kind")
        record_format = binding_by_field.get("record_format")
        if (
            source_key != expected_source_key
            or source_type != "in_network"
            or identity_kind
            not in {"logical_json_sha256_v1", "raw_container_sha256_v1"}
            or record_format != _RECORD_FORMAT
        ):
            raise _fail()
        digest.update(source_key.to_bytes(4, "big"))
        _digest_ascii(digest, source_type)
        _digest_ascii(digest, identity_kind)
        digest.update(
            bytes.fromhex(_strict_sha256(binding_by_field.get("identity_sha256")))
        )
        _digest_ascii(digest, _strict_policy(binding_by_field.get("token_policy_id")))
        digest.update(
            _strict_digest(binding_by_field.get("token_policy_descriptor_sha256"))
        )
        _digest_ascii(digest, record_format)
        for field_name, expected_value in _NUMERIC_FIELDS:
            field_value = _strict_int(binding_by_field.get(field_name))
            if expected_value is not None and field_value != expected_value:
                raise _fail()
            digest.update(field_value.to_bytes(8, "big"))
        digest.update(_strict_digest(binding_by_field.get("artifact_sha256")))
    return digest.digest()


__all__ = [
    "PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT",
    "TaxIdentitySourceBindingVectorError",
    "tax_identity_source_binding_vector_digest",
]
