# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lossless JSON validation shared by the native projection spool parser."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import json
from typing import Any

from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)


@dataclass(frozen=True)
class _ExactJsonNumber:
    token: str


def invalid_native_spool() -> ProviderDirectoryProjectionError:
    """Return the stable error used for every malformed spool shape."""

    return ProviderDirectoryProjectionError(
        "provider_directory_projection_native_spool_invalid"
    )


def _reject_constant(_constant: str) -> None:
    raise invalid_native_spool()


def _exact_number(number_token: str) -> _ExactJsonNumber:
    return _ExactJsonNumber(number_token)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    object_map: dict[str, Any] = {}
    for field_name, field_content in pairs:
        if field_name in object_map:
            raise invalid_native_spool()
        object_map[field_name] = field_content
    return object_map


def _decoded_mapping(encoded_json: bytes, **number_hooks: Any) -> dict[str, Any]:
    try:
        decoded_map = json.loads(
            encoded_json.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
            **number_hooks,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise invalid_native_spool() from error
    if not isinstance(decoded_map, dict):
        raise invalid_native_spool()
    return decoded_map


def decoded_json_object(encoded_json: bytes) -> dict[str, Any]:
    """Decode a strict JSON object with ordinary summary-number semantics."""

    return _decoded_mapping(encoded_json)


def decoded_fhir_object(encoded_json: bytes) -> dict[str, Any]:
    """Decode FHIR decimals losslessly for semantic transformation."""

    return _decoded_mapping(encoded_json, parse_float=Decimal)


def exactly_decoded_object(encoded_json: bytes) -> dict[str, Any]:
    """Decode every number as its exact lexical token."""

    return _decoded_mapping(
        encoded_json,
        parse_int=_exact_number,
        parse_float=_exact_number,
    )


def canonical_json(json_content: Any) -> bytes:
    """Encode ordinary JSON with Rust-compatible compact separators."""

    try:
        return json.dumps(
            json_content,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise invalid_native_spool() from error


def canonical_exact_json(
    json_content: Any,
    *,
    should_sort_keys: bool = True,
) -> bytes:
    """Encode JSON while retaining each exact numeric token."""

    if isinstance(json_content, _ExactJsonNumber):
        return json_content.token.encode("ascii")
    if json_content is None or type(json_content) is bool or isinstance(json_content, str):
        return canonical_json(json_content)
    if isinstance(json_content, list):
        encoded_entries = (
            canonical_exact_json(entry, should_sort_keys=should_sort_keys)
            for entry in json_content
        )
        return b"[" + b",".join(encoded_entries) + b"]"
    if isinstance(json_content, dict):
        field_names = sorted(json_content) if should_sort_keys else json_content
        encoded_fields = (
            canonical_json(field_name)
            + b":"
            + canonical_exact_json(
                json_content[field_name],
                should_sort_keys=should_sort_keys,
            )
            for field_name in field_names
        )
        return b"{" + b",".join(encoded_fields) + b"}"
    raise invalid_native_spool()


def native_record_payload_bytes(frame_payload: bytes) -> bytes:
    """Extract the final canonical payload value from an exact record frame."""

    payload_marker = b',"payload_json":'
    if not frame_payload.endswith(b"}") or frame_payload.count(payload_marker) != 1:
        raise invalid_native_spool()
    payload_json_bytes = frame_payload.rsplit(payload_marker, 1)[1][:-1]
    if not payload_json_bytes:
        raise invalid_native_spool()
    return payload_json_bytes


def canonical_native_record_bytes(
    native_record_map: dict[str, Any],
    payload_json_bytes: bytes,
    record_fields: tuple[str, ...],
) -> bytes:
    """Rebuild the exact native record shell around retained payload bytes."""

    encoded_fields = [
        canonical_json(field_name)
        + b":"
        + canonical_json(native_record_map[field_name])
        for field_name in record_fields[:-1]
    ]
    encoded_fields.append(canonical_json("payload_json") + b":" + payload_json_bytes)
    return b"{" + b",".join(encoded_fields) + b"}"


__all__ = (
    "canonical_exact_json",
    "canonical_json",
    "canonical_native_record_bytes",
    "decoded_fhir_object",
    "decoded_json_object",
    "exactly_decoded_object",
    "invalid_native_spool",
    "native_record_payload_bytes",
)
