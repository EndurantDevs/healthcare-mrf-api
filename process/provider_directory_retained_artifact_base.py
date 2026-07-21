# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-neutral contract for retained Provider Directory artifacts."""

from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any


RETAINED_ARTIFACT_CONTRACT_ID = (
    "healthporta-provider-directory-retained-artifact-campaign-v1"
)
RETAINED_LAYOUT_MANIFEST_ID = (
    "healthporta-provider-directory-retained-layout-manifest-v1"
)
FIXED_CATALOG = "fixed_catalog"
ORDERED_STREAMS = "ordered_streams"
PROVIDER_FILE = "provider_file"
BULK_NDJSON = "bulk_ndjson"
FHIR_BUNDLE_PAGE = "fhir_bundle_page"
FHIR_BUNDLE_BLOCK = "fhir_bundle_block"
ARTIFACT_KINDS = frozenset(
    {PROVIDER_FILE, BULK_NDJSON, FHIR_BUNDLE_PAGE, FHIR_BUNDLE_BLOCK}
)
PAYLOAD = "payload"
TERMINAL_ZERO = "terminal_zero"
RANGED_STRONG_VALIDATOR = "ranged_strong_validator"
ATOMIC_CATALOG_OBJECT = "atomic_catalog_object"
PRODUCER_VERIFIED = "producer_verified"
STRONG_ETAG = "strong_etag"
CATALOG_OBJECT = "catalog_object"
PRODUCER_PROOF = "producer_proof"
_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
_SAFE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}")
_KEY_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,31}")
_CIPHER_PREFIX = "pdart3:"
_HMAC_PREFIX = "pdhmac3:"
_CIPHER_DOMAIN = b"healthporta:provider-directory:retained-artifact:v3"
_HMAC_DOMAIN = b"healthporta:provider-directory:artifact-identity:v3\x00"
MAX_CANONICAL_JSON_BYTES = 8 * 1024 * 1024
MAX_CANONICAL_JSON_DEPTH = 32
MAX_CANONICAL_JSON_NODES = 100_000


class RetainedArtifactError(RuntimeError):
    """Fail closed while preserving one capability-free operator code."""

    def __init__(self, code: str):
        if not _SAFE_ID_PATTERN.fullmatch(code):
            code = "retained_artifact_failed"
        self.code = code
        super().__init__(code)


class RetainedCampaignIncomplete(RetainedArtifactError):
    """A consumer attempted to claim an incomplete artifact campaign."""


class RetainedCampaignMismatch(RetainedArtifactError):
    """An immutable producer, campaign, or claim identity changed."""


def _bounded_text_bytes(text: str, maximum_bytes: int) -> int:
    if len(text) > maximum_bytes:
        raise RetainedArtifactError("retained_contract_value_invalid")
    encoded_size = len(text.encode("utf-8"))
    if encoded_size > maximum_bytes:
        raise RetainedArtifactError("retained_contract_value_invalid")
    return encoded_size


def _json_scalar_byte_count(current_value: Any, maximum_bytes: int) -> int | None:
    if current_value is None or type(current_value) is bool:
        return 0
    if type(current_value) is str:
        return _bounded_text_bytes(current_value, maximum_bytes)
    if type(current_value) is int:
        if not -(2**63) <= current_value <= 2**63 - 1:
            raise RetainedArtifactError("retained_contract_value_invalid")
        return 0
    if type(current_value) is float:
        if not math.isfinite(current_value):
            raise RetainedArtifactError("retained_contract_value_invalid")
        return 0
    return None


def _json_container_entries(
    current_value: Any,
    current_depth: int,
    container_ids: set[int],
    maximum_bytes: int,
) -> tuple[list[tuple[Any, int]], int]:
    if type(current_value) not in {dict, list}:
        raise RetainedArtifactError("retained_contract_value_invalid")
    container_identity = id(current_value)
    if container_identity in container_ids:
        raise RetainedArtifactError("retained_contract_value_invalid")
    container_ids.add(container_identity)
    if type(current_value) is list:
        return [(nested_value, current_depth + 1) for nested_value in current_value], 0
    nested_entries: list[tuple[Any, int]] = []
    key_byte_count = 0
    for key, nested_value in current_value.items():
        if type(key) is not str:
            raise RetainedArtifactError("retained_contract_value_invalid")
        key_byte_count += _bounded_text_bytes(key, maximum_bytes)
        nested_entries.append((nested_value, current_depth + 1))
    return nested_entries, key_byte_count


def _validate_json_tree(
    json_document: Any,
    *,
    maximum_bytes: int,
    maximum_depth: int,
    maximum_nodes: int,
) -> None:
    pending_values = [(json_document, 0)]
    container_ids: set[int] = set()
    node_count = 0
    text_byte_count = 0
    while pending_values:
        current_value, current_depth = pending_values.pop()
        node_count += 1
        if node_count > maximum_nodes or current_depth > maximum_depth:
            raise RetainedArtifactError("retained_contract_value_invalid")
        scalar_byte_count = _json_scalar_byte_count(current_value, maximum_bytes)
        if scalar_byte_count is None:
            nested_entries, key_byte_count = _json_container_entries(
                current_value,
                current_depth,
                container_ids,
                maximum_bytes,
            )
            pending_values.extend(nested_entries)
            text_byte_count += key_byte_count
        else:
            text_byte_count += scalar_byte_count
        if text_byte_count > maximum_bytes:
            raise RetainedArtifactError("retained_contract_value_invalid")


def canonical_json(
    value: Any,
    *,
    maximum_bytes: int = MAX_CANONICAL_JSON_BYTES,
    maximum_depth: int = MAX_CANONICAL_JSON_DEPTH,
    maximum_nodes: int = MAX_CANONICAL_JSON_NODES,
) -> bytes:
    """Encode one bounded stable hash input."""

    try:
        _validate_json_tree(
            value,
            maximum_bytes=maximum_bytes,
            maximum_depth=maximum_depth,
            maximum_nodes=maximum_nodes,
        )
        encoded_value = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, UnicodeError, ValueError, RecursionError) as error:
        raise RetainedArtifactError("retained_contract_value_invalid") from error
    if len(encoded_value) > maximum_bytes:
        raise RetainedArtifactError("retained_contract_value_invalid")
    return encoded_value


def sha256_json(
    value: Any,
    *,
    maximum_bytes: int = MAX_CANONICAL_JSON_BYTES,
    maximum_depth: int = MAX_CANONICAL_JSON_DEPTH,
    maximum_nodes: int = MAX_CANONICAL_JSON_NODES,
) -> str:
    """Hash one canonical JSON value."""

    return hashlib.sha256(
        canonical_json(
            value,
            maximum_bytes=maximum_bytes,
            maximum_depth=maximum_depth,
            maximum_nodes=maximum_nodes,
        )
    ).hexdigest()


def require_digest(value: Any, field: str) -> str:
    """Return a lowercase SHA-256 digest or raise a safe field error."""

    normalized = value if isinstance(value, str) else ""
    if not _DIGEST_PATTERN.fullmatch(normalized):
        raise RetainedArtifactError(f"{field}_invalid")
    return normalized


def require_safe_id(value: Any, field: str, *, maximum: int = 128) -> str:
    """Return one bounded printable identifier or raise a safe field error."""

    normalized = value if isinstance(value, str) else ""
    if len(normalized) > maximum or not _SAFE_ID_PATTERN.fullmatch(normalized):
        raise RetainedArtifactError(f"{field}_invalid")
    return normalized


def require_nonnegative_int(value: Any, field: str) -> int:
    """Return one signed-bigint-compatible nonnegative integer."""

    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > 2**63 - 1
    ):
        raise RetainedArtifactError(f"{field}_invalid")
    return value


def require_positive_int(value: Any, field: str) -> int:
    """Return one strictly positive signed-bigint-compatible integer."""

    result = require_nonnegative_int(value, field)
    if result == 0:
        raise RetainedArtifactError(f"{field}_invalid")
    return result
