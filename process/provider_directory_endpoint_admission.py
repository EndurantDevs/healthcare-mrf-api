# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict bounded admission for Provider Directory endpoint identity data."""

from __future__ import annotations

import hashlib
import json
import math
import urllib.parse
from typing import Any


_ENDPOINT_BASE_MAX_LENGTH = 2048
_ENDPOINT_DESCRIPTOR_MAX_DEPTH = 8
_ENDPOINT_DESCRIPTOR_MAX_NODES = 4096
_ENDPOINT_DESCRIPTOR_MAX_KEY_LENGTH = 256
_ENDPOINT_DESCRIPTOR_MAX_STRING_BYTES = 16 * 1024
_CREDENTIAL_DESCRIPTOR_MAX_BYTES = 16 * 1024
_ENDPOINT_SIGNATURE_MAX_BYTES = 64 * 1024


class ProviderDirectoryEndpointIdentityError(ValueError):
    """Report an endpoint record that does not reproduce its stored identity."""


def _json_error(field_name: str) -> ProviderDirectoryEndpointIdentityError:
    return ProviderDirectoryEndpointIdentityError(
        f"{field_name} must contain bounded JSON data"
    )


class _BoundedJsonSnapshot:
    """Capture exact built-in JSON data without invoking input callbacks."""

    __slots__ = (
        "_ancestor_ids",
        "_field_name",
        "_max_bytes",
        "_nodes_seen",
        "_text_bytes_seen",
    )

    def __init__(self, field_name: str, max_bytes: int) -> None:
        self._ancestor_ids: set[int] = set()
        self._field_name = field_name
        self._max_bytes = max_bytes
        self._nodes_seen = 0
        self._text_bytes_seen = 0

    def capture(self, json_value: object, depth: int = 0) -> Any:
        """Return one bounded recursive snapshot of exact JSON built-ins."""

        if depth > _ENDPOINT_DESCRIPTOR_MAX_DEPTH:
            raise _json_error(self._field_name)
        self._nodes_seen += 1
        if self._nodes_seen > _ENDPOINT_DESCRIPTOR_MAX_NODES:
            raise _json_error(self._field_name)
        json_type = type(json_value)
        if json_type is dict:
            return self._capture_dict(json_value, depth)
        if json_type is list:
            return self._capture_list(json_value, depth)
        return self._capture_scalar(json_value)

    def _capture_dict(self, json_map: dict[object, object], depth: int) -> dict[str, Any]:
        if len(json_map) > _ENDPOINT_DESCRIPTOR_MAX_NODES:
            raise _json_error(self._field_name)
        container_id = id(json_map)
        self._reject_recursive_container(container_id)
        self._ancestor_ids.add(container_id)
        try:
            captured_pairs = tuple(dict.items(json_map))
            snapshot_by_key: dict[str, Any] = {}
            for json_key, nested_json in captured_pairs:
                if type(json_key) is not str:
                    raise _json_error(self._field_name)
                self._charge_text(json_key, is_key=True)
                snapshot_by_key[json_key] = self.capture(nested_json, depth + 1)
            return snapshot_by_key
        except (MemoryError, RuntimeError):
            raise _json_error(self._field_name) from None
        finally:
            self._ancestor_ids.remove(container_id)

    def _capture_list(self, json_list: list[object], depth: int) -> list[Any]:
        if len(json_list) > _ENDPOINT_DESCRIPTOR_MAX_NODES:
            raise _json_error(self._field_name)
        container_id = id(json_list)
        self._reject_recursive_container(container_id)
        self._ancestor_ids.add(container_id)
        try:
            captured_entries = tuple(json_list)
            return [self.capture(entry, depth + 1) for entry in captured_entries]
        except (MemoryError, RuntimeError):
            raise _json_error(self._field_name) from None
        finally:
            self._ancestor_ids.remove(container_id)

    def _capture_scalar(self, scalar_value: object) -> object:
        scalar_type = type(scalar_value)
        if scalar_type is str:
            self._charge_text(scalar_value)
            return scalar_value
        if scalar_type is int:
            if scalar_value.bit_length() > 256:
                raise _json_error(self._field_name)
            return scalar_value
        if scalar_type is float:
            if not math.isfinite(scalar_value):
                raise _json_error(self._field_name)
            return scalar_value
        if scalar_type is bool or scalar_value is None:
            return scalar_value
        raise _json_error(self._field_name)

    def _charge_text(self, text_value: str, *, is_key: bool = False) -> None:
        if is_key and len(text_value) > _ENDPOINT_DESCRIPTOR_MAX_KEY_LENGTH:
            raise _json_error(self._field_name)
        if len(text_value) > _ENDPOINT_DESCRIPTOR_MAX_STRING_BYTES:
            raise _json_error(self._field_name)
        try:
            encoded_length = len(text_value.encode("utf-8"))
        except UnicodeError:
            raise _json_error(self._field_name) from None
        self._text_bytes_seen += encoded_length
        if self._text_bytes_seen > self._max_bytes:
            raise _json_error(self._field_name)

    def _reject_recursive_container(self, container_id: int) -> None:
        if container_id in self._ancestor_ids:
            raise _json_error(self._field_name)


def _snapshot_json_value(
    json_value: object,
    field_name: str,
    *,
    max_bytes: int,
) -> Any:
    return _BoundedJsonSnapshot(field_name, max_bytes).capture(json_value)


def _render_json_snapshot(
    json_value: object,
    field_name: str,
    *,
    max_bytes: int,
) -> str:
    try:
        rendered_json = json.dumps(
            json_value,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if len(rendered_json.encode("utf-8")) > max_bytes:
            raise _json_error(field_name)
        return rendered_json
    except (MemoryError, OverflowError, TypeError, UnicodeError, ValueError):
        raise _json_error(field_name) from None


def _canonical_json(json_value: object, field_name: str) -> str:
    json_snapshot = _snapshot_json_value(
        json_value,
        field_name,
        max_bytes=_ENDPOINT_SIGNATURE_MAX_BYTES,
    )
    return _render_json_snapshot(
        json_snapshot,
        field_name,
        max_bytes=_ENDPOINT_SIGNATURE_MAX_BYTES,
    )


def _json_object(
    json_value: object,
    field_name: str,
    *,
    max_bytes: int,
) -> tuple[dict[str, Any], str]:
    if type(json_value) is not dict:
        raise ProviderDirectoryEndpointIdentityError(
            f"{field_name} must be a JSON object"
        )
    json_snapshot = _snapshot_json_value(json_value, field_name, max_bytes=max_bytes)
    rendered_json = _render_json_snapshot(
        json_snapshot,
        field_name,
        max_bytes=max_bytes,
    )
    return json_snapshot, rendered_json


def _stored_json_object(
    stored_json: object,
    field_name: str,
    *,
    max_bytes: int,
) -> tuple[dict[str, Any], str]:
    if type(stored_json) is not str:
        raise ProviderDirectoryEndpointIdentityError(
            f"stored {field_name} is invalid"
        )
    try:
        if len(stored_json.encode("utf-8")) > max_bytes:
            raise ValueError
        decoded_json = json.loads(stored_json)
    except (MemoryError, RecursionError, TypeError, UnicodeError, ValueError):
        raise ProviderDirectoryEndpointIdentityError(
            f"stored {field_name} is invalid"
        ) from None
    decoded_object, rendered_json = _json_object(
        decoded_json,
        field_name,
        max_bytes=max_bytes,
    )
    if rendered_json != stored_json:
        raise ProviderDirectoryEndpointIdentityError(
            f"stored {field_name} is invalid"
        )
    return decoded_object, rendered_json


def _identity_hash(json_value: object, field_name: str) -> str:
    canonical_json = _canonical_json(json_value, field_name)
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _validate_canonical_api_base(api_base_value: object) -> str:
    if not _has_valid_api_base_text(api_base_value):
        raise ProviderDirectoryEndpointIdentityError("canonical API base is invalid")
    try:
        parsed_api_base = urllib.parse.urlsplit(api_base_value)
        parsed_hostname = parsed_api_base.hostname
        parsed_port = parsed_api_base.port
        parsed_username = parsed_api_base.username
        parsed_password = parsed_api_base.password
    except (UnicodeError, ValueError):
        raise ProviderDirectoryEndpointIdentityError(
            "canonical API base is invalid"
        ) from None
    if not _has_valid_parsed_api_base(
        parsed_api_base,
        parsed_hostname,
        parsed_port,
        parsed_username,
        parsed_password,
    ):
        raise ProviderDirectoryEndpointIdentityError("canonical API base is invalid")
    expected_api_base = urllib.parse.urlunsplit(
        (
            parsed_api_base.scheme.lower(),
            parsed_api_base.netloc.lower(),
            parsed_api_base.path.rstrip("/"),
            "",
            "",
        )
    )
    if api_base_value != expected_api_base:
        raise ProviderDirectoryEndpointIdentityError(
            "canonical API base is not canonical"
        )
    return api_base_value


def _has_valid_api_base_text(api_base_value: object) -> bool:
    return (
        type(api_base_value) is str
        and bool(api_base_value)
        and api_base_value == api_base_value.strip()
        and len(api_base_value) <= _ENDPOINT_BASE_MAX_LENGTH
        and all(
            character.isprintable() and not character.isspace()
            for character in api_base_value
        )
    )


def _has_valid_parsed_api_base(
    parsed_api_base: urllib.parse.SplitResult,
    parsed_hostname: str | None,
    parsed_port: int | None,
    parsed_username: str | None,
    parsed_password: str | None,
) -> bool:
    return (
        parsed_api_base.scheme.lower() in {"http", "https"}
        and bool(parsed_api_base.netloc)
        and parsed_hostname is not None
        and parsed_username is None
        and parsed_password is None
        and (parsed_port is None or 1 <= parsed_port <= 65535)
        and not parsed_api_base.netloc.endswith(":")
        and "\\" not in parsed_api_base.netloc
        and all(
            character.isprintable() and not character.isspace()
            for character in parsed_hostname
        )
    )


def _admit_provider_directory_endpoint_components(
    *,
    canonical_api_base: object,
    credential_descriptor_json: object,
    endpoint_signature_json: object,
) -> dict[str, object]:
    """Normalize one strict endpoint identity record from callback-free inputs."""

    normalized_api_base = _validate_canonical_api_base(canonical_api_base)
    credential_descriptor, credential_json = _json_object(
        credential_descriptor_json,
        "credential descriptor",
        max_bytes=_CREDENTIAL_DESCRIPTOR_MAX_BYTES,
    )
    endpoint_signature, signature_json = _json_object(
        endpoint_signature_json,
        "endpoint signature",
        max_bytes=_ENDPOINT_SIGNATURE_MAX_BYTES,
    )
    credential_descriptor_hash = _identity_hash(
        credential_descriptor,
        "credential descriptor",
    )
    endpoint_signature_hash = _identity_hash(
        endpoint_signature,
        "endpoint signature",
    )
    endpoint_id = _identity_hash(
        {
            "canonical_api_base": normalized_api_base,
            "credential_descriptor": credential_descriptor,
            "endpoint_signature": endpoint_signature,
        },
        "endpoint identity",
    )
    return {
        "endpoint_id": endpoint_id,
        "canonical_api_base": normalized_api_base,
        "credential_descriptor_hash": credential_descriptor_hash,
        "endpoint_signature_hash": endpoint_signature_hash,
        "credential_descriptor_json": credential_descriptor,
        "endpoint_signature_json": endpoint_signature,
        "credential_descriptor_canonical_json": credential_json,
        "endpoint_signature_canonical_json": signature_json,
    }
