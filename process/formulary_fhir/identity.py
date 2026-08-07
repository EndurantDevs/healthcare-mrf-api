# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical endpoint and route-safe identities for FHIR formularies."""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import ipaddress
import json
import math
import re
import urllib.parse
from typing import Any


FHIR_PUBLIC_ID_PREFIX = "fhir_"
FHIR_PUBLIC_ID_BITS = 130
FHIR_PUBLIC_ID_CHARS = FHIR_PUBLIC_ID_BITS // 5
FHIR_ID_PATTERN = re.compile(r"[A-Za-z0-9\-.]{1,64}\Z")
FHIR_BASE_PATH_PATTERN = re.compile(r"[A-Za-z0-9._~!$&'()*+,;=:@/-]*\Z")
DNS_HOST_PATTERN = re.compile(
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z"
)
FHIR_INSTANT_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})\Z"
)
ALTERNATIVE_REFERENCE_PATTERN = re.compile(
    r"MedicationKnowledge/([A-Za-z0-9\-.]{1,64})\Z"
)
CORRECTION_PREFIX_PATTERN = re.compile(r"[A-Za-z0-9.-]{1,16}\Z")
CODING_FIELDS = frozenset({"system", "version", "code", "display", "userSelected"})


def strict_fhir_text(
    raw_text: object,
    field_name: str,
    *,
    maximum_length: int,
    is_required: bool = False,
) -> str | None:
    """Return one exact FHIR string primitive without coercion."""

    if raw_text is None and not is_required:
        return None
    if (
        type(raw_text) is not str
        or not raw_text
        or len(raw_text) > maximum_length
        or raw_text != raw_text.strip()
        or any(not character.isprintable() for character in raw_text)
    ):
        raise ValueError(f"FHIR {field_name} primitive is invalid")
    return raw_text


def parse_fhir_instant(raw_instant: object, *, field_name: str) -> dt.datetime:
    """Parse one exact timezone-bearing FHIR instant as UTC."""

    instant_text = strict_fhir_text(
        raw_instant,
        field_name,
        maximum_length=40,
        is_required=True,
    )
    assert instant_text is not None
    if not FHIR_INSTANT_PATTERN.fullmatch(instant_text):
        raise ValueError(f"FHIR {field_name} instant is invalid")
    try:
        parsed_instant = dt.datetime.fromisoformat(
            instant_text.replace("Z", "+00:00")
        )
    except ValueError:
        raise ValueError(f"FHIR {field_name} instant is invalid") from None
    if parsed_instant.tzinfo is None:
        raise ValueError(f"FHIR {field_name} instant is invalid")
    return parsed_instant.astimezone(dt.UTC)


def validate_fhir_json_node(json_node: Any, *, depth: int = 0) -> None:
    """Require JSON-native primitives under a bounded nesting depth."""

    if depth > 32:
        raise ValueError("FHIR JSON nesting exceeds the parser bound")
    if json_node is None or type(json_node) in {str, bool, int}:
        return
    if type(json_node) is float:
        if math.isfinite(json_node):
            return
        raise ValueError("FHIR JSON number is invalid")
    if type(json_node) is list:
        for nested_node in json_node:
            validate_fhir_json_node(nested_node, depth=depth + 1)
        return
    if type(json_node) is dict and all(type(key) is str for key in json_node):
        for nested_node in json_node.values():
            validate_fhir_json_node(nested_node, depth=depth + 1)
        return
    raise ValueError("FHIR JSON primitive types are invalid")


def fhir_json_snapshot(json_object: dict[str, Any]) -> dict[str, Any]:
    """Return a canonical deep copy after strict primitive validation."""

    validate_fhir_json_node(json_object)
    canonical_json = json.dumps(
        json_object,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return json.loads(canonical_json)


def fhir_content_hash(normalized_fields: dict[str, Any]) -> str:
    """Hash a strict normalized JSON object deterministically."""

    validate_fhir_json_node(normalized_fields)
    canonical_json = json.dumps(
        normalized_fields,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def optional_fhir_instant(
    raw_instant: object,
    *,
    field_name: str,
) -> dt.datetime | None:
    """Parse an optional exact timezone-bearing FHIR instant."""

    if raw_instant is None:
        return None
    return parse_fhir_instant(raw_instant, field_name=field_name)


def strict_fhir_resource(resource: object, expected_type: str) -> dict[str, Any]:
    """Require one JSON-native resource object of the exact expected type."""

    if type(resource) is not dict or resource.get("resourceType") != expected_type:
        raise ValueError(f"FHIR resource must be {expected_type}")
    validate_fhir_json_node(resource)
    return resource


def fhir_resource_metadata(
    resource: dict[str, Any],
) -> tuple[str | None, dt.datetime]:
    """Return strict version and last-updated primitives."""

    metadata = resource.get("meta")
    if type(metadata) is not dict:
        raise ValueError("FHIR resource meta object is required")
    version_id = strict_fhir_text(
        metadata.get("versionId"),
        "meta.versionId",
        maximum_length=256,
    )
    last_updated = parse_fhir_instant(
        metadata.get("lastUpdated"),
        field_name="meta.lastUpdated",
    )
    return version_id, last_updated


def resource_last_updated(resource: object) -> dt.datetime:
    """Return the strict current resource version timestamp."""

    if type(resource) is not dict:
        raise ValueError("FHIR resource object is invalid")
    return fhir_resource_metadata(resource)[1]


def preferred_coding_display(
    codings: tuple[Any, ...],
    *,
    preferred_system: str,
) -> str | None:
    """Prefer a display from one exact coding system, then any display."""

    preferred_display = next(
        (
            coding.display
            for coding in codings
            if coding.system == preferred_system and coding.display
        ),
        None,
    )
    return preferred_display or next(
        (coding.display for coding in codings if coding.display),
        None,
    )


def canonical_fhir_base(base_url: object) -> str:
    """Require one already-canonical HTTPS FHIR base without credentials."""

    if (
        type(base_url) is not str
        or not base_url
        or len(base_url) > 2_048
        or base_url != base_url.strip()
        or any(not character.isprintable() for character in base_url)
    ):
        raise ValueError("FHIR base is invalid")
    try:
        parsed_base = urllib.parse.urlsplit(base_url)
        parsed_port = parsed_base.port
    except (UnicodeError, ValueError):
        raise ValueError("FHIR base is invalid") from None
    hostname = parsed_base.hostname
    path_segments = parsed_base.path.split("/")[1:] if parsed_base.path else []
    try:
        ipaddress.ip_address(hostname or "")
        is_ip_literal = True
    except ValueError:
        is_ip_literal = False
    is_canonical = bool(
        base_url.startswith("https://")
        and parsed_base.scheme == "https"
        and hostname
        and "." in hostname
        and not is_ip_literal
        and DNS_HOST_PATTERN.fullmatch(hostname)
        and parsed_base.netloc == hostname
        and parsed_port is None
        and parsed_base.username is None
        and parsed_base.password is None
        and not parsed_base.query
        and not parsed_base.fragment
        and not parsed_base.path.endswith("/")
        and "\\" not in parsed_base.path
        and "%" not in parsed_base.path
        and FHIR_BASE_PATH_PATTERN.fullmatch(parsed_base.path)
        and all(segment not in {"", ".", ".."} for segment in path_segments)
    )
    if not is_canonical:
        raise ValueError("FHIR base is not a canonical HTTPS endpoint")
    return base_url


def validated_fhir_id(logical_id: object, *, label: str = "FHIR logical id") -> str:
    """Return one exact FHIR id primitive without coercion."""

    if type(logical_id) is not str or not FHIR_ID_PATTERN.fullmatch(logical_id):
        raise ValueError(f"{label} is invalid")
    return logical_id


def canonical_list_identity(base_url: object, list_id: object) -> str:
    """Return the unhashed canonical identity for one upstream List."""

    canonical_base = canonical_fhir_base(base_url)
    clean_list_id = validated_fhir_id(list_id, label="coverage plan List id")
    return f"{canonical_base}/List/{clean_list_id}"


def public_formulary_id(base_url: object, list_id: object) -> str:
    """Return ``fhir_`` plus the leading 130 SHA-256 bits in base32."""

    identity = canonical_list_identity(base_url, list_id).encode("utf-8")
    encoded_digest = base64.b32encode(hashlib.sha256(identity).digest()).decode(
        "ascii"
    )
    return FHIR_PUBLIC_ID_PREFIX + encoded_digest.lower()[:FHIR_PUBLIC_ID_CHARS]
