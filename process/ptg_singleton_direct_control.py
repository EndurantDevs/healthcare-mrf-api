# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated non-frozen singleton-direct PTG input contract."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

from process.ptg_parts.frozen_rate_binding import (
    INVALID_PRICE_EXCLUSION_POLICY_FIELD,
    protected_frozen_tuple_presence,
)
from process.ptg_parts.ptg2_invalid_price_exclusion import (
    validate_invalid_price_exclusion_policy,
)
from process.ptg_singleton_direct_resource import PTG_SMALL_RESOURCE_CONTRACT
from process.ptg_singleton_direct_errors import (
    SingletonDirectValidationError,
    singleton_direct_failure_payload,
)


DIRECT_RATE_FILE_INTENT_CONTRACT = "ptg_singleton_direct_file_intent_v1"
DIRECT_RATE_FILE_INTENT_FIELD = "direct_rate_file_intent"
DIRECT_RATE_FILE_INTENT_SHA256_FIELD = "direct_rate_file_intent_sha256"
DIRECT_RATE_FILE_INTENT_DIGEST_DOMAIN = "ptg-singleton-direct-file-intent-v1"
DIRECT_RATE_FILE_PROTECTED_FIELDS = (
    DIRECT_RATE_FILE_INTENT_FIELD,
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    "allowed_url",
    "in_network_url",
    "max_files",
)
_DIRECT_RATE_FILE_MARKER_FIELDS = (
    DIRECT_RATE_FILE_INTENT_FIELD, DIRECT_RATE_FILE_INTENT_SHA256_FIELD
)
DIRECT_RATE_FILE_PUBLIC_MARKER = "direct_rate_file_intent_protected"

_DIRECT_INTENT_FIELDS = frozenset(
    {
        "contract",
        "source_file_import_id",
        "source_file_id",
        "content_version",
        "source_type",
        "canonical_url",
        "source_key",
        "content_file_count",
    }
)
_DIRECT_WAVE_PARAM_BASE_FIELDS = frozenset(
    {
        "version",
        "importer",
        "operation_id",
        "source_file_import_id",
        "import_id",
        "source_file_id",
        "content_version",
        "import_month",
        "node_id",
        "use_stored_catalog",
        DIRECT_RATE_FILE_INTENT_FIELD,
        DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
        "ptg_resource",
        "source_key",
        "plan_ids",
        "plan_market_types",
        "max_files",
    }
)
_SELECTOR_FIELD_BY_SOURCE_TYPE = dict(
    allowed_amounts="allowed_url", in_network="in_network_url"
)
_DIRECT_SELECTOR_FIELDS = frozenset(_SELECTOR_FIELD_BY_SOURCE_TYPE.values())
_COMPETING_SELECTOR_FIELDS = frozenset(
    {
        "toc_url",
        "toc_urls",
        "toc_list",
        "file_url_contains",
        "provider_ref_url",
    }
)
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


def protected_singleton_direct_presence(
    params_by_name: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return supplied direct marker fields in declaration order."""

    return tuple(
        field_name
        for field_name in _DIRECT_RATE_FILE_MARKER_FIELDS
        if field_name in params_by_name
    )


def normalize_protected_singleton_direct_params(
    params_by_name: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the closed direct tuple and return canonical parameters."""

    normalized_params_by_name = dict(params_by_name)
    supplied_fields = protected_singleton_direct_presence(
        normalized_params_by_name
    )
    if not supplied_fields:
        return normalized_params_by_name
    _require_direct_marker_tuple(normalized_params_by_name, supplied_fields)
    direct_intent, direct_digest = _validated_direct_intent(
        normalized_params_by_name
    )
    _require_direct_outer_matches(normalized_params_by_name, direct_intent)
    _require_singleton_selection(normalized_params_by_name)
    if INVALID_PRICE_EXCLUSION_POLICY_FIELD in normalized_params_by_name:
        if direct_intent["source_type"] != "in_network":
            raise SingletonDirectValidationError(
                "invalid price exclusion requires an in-network source"
            )
        normalized_params_by_name[INVALID_PRICE_EXCLUSION_POLICY_FIELD] = (
            validated_singleton_invalid_price_exclusion(
                normalized_params_by_name[INVALID_PRICE_EXCLUSION_POLICY_FIELD]
            )
        )
    normalized_params_by_name[DIRECT_RATE_FILE_INTENT_FIELD] = direct_intent
    normalized_params_by_name[DIRECT_RATE_FILE_INTENT_SHA256_FIELD] = (
        direct_digest
    )
    selector_field = _direct_selector_field(direct_intent["source_type"])
    normalized_params_by_name[selector_field] = direct_intent[
        "canonical_url"
    ]
    normalized_params_by_name["max_files"] = 1
    return normalized_params_by_name


def validated_singleton_invalid_price_exclusion(
    raw_policy: Any,
) -> dict[str, Any]:
    """Require one canonical policy source for one protected direct file."""

    try:
        policy_by_name = validate_invalid_price_exclusion_policy(raw_policy)
    except ValueError as exc:
        raise SingletonDirectValidationError(
            "singleton direct invalid price exclusion policy is invalid"
        ) from exc
    if policy_by_name["source_count"] != 1:
        raise SingletonDirectValidationError(
            "singleton direct invalid price exclusion must bind one source"
        )
    return policy_by_name


def _require_direct_marker_tuple(
    params_by_name: Mapping[str, Any],
    supplied_fields: Sequence[str],
) -> None:
    """Require versioned, exclusive direct markers and selectors."""

    if len(supplied_fields) != len(_DIRECT_RATE_FILE_MARKER_FIELDS):
        raise SingletonDirectValidationError(
            "protected singleton direct fields are required together"
        )
    if (
        type(params_by_name.get("version")) is not int
        or params_by_name.get("version") != 2
    ):
        raise SingletonDirectValidationError(
            "protected singleton direct version must be 2"
        )
    if (
        sum(
            field_name in params_by_name
            for field_name in _DIRECT_SELECTOR_FIELDS
        )
        != 1
        or "max_files" not in params_by_name
    ):
        raise SingletonDirectValidationError(
            "protected singleton direct requires exactly one role selector"
        )
    if protected_frozen_tuple_presence(params_by_name):
        raise SingletonDirectValidationError(
            "singleton direct and frozen multipart inputs are exclusive"
        )
    if _COMPETING_SELECTOR_FIELDS & set(params_by_name):
        raise SingletonDirectValidationError(
            "singleton direct input has a competing source selector"
        )


def _validated_direct_intent(
    params_by_name: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    """Return one normalized intent and its verified digest."""

    direct_intent = _normalized_direct_intent(
        params_by_name[DIRECT_RATE_FILE_INTENT_FIELD]
    )
    direct_digest = params_by_name[DIRECT_RATE_FILE_INTENT_SHA256_FIELD]
    if (
        not isinstance(direct_digest, str)
        or _HEX_64.fullmatch(direct_digest) is None
        or direct_digest != singleton_direct_intent_sha256(direct_intent)
    ):
        raise SingletonDirectValidationError(
            "singleton direct intent digest is invalid"
        )
    return direct_intent, direct_digest


def _require_direct_outer_matches(
    params_by_name: Mapping[str, Any],
    direct_intent: Mapping[str, Any],
) -> None:
    """Require every outer selector to match the signed nested intent."""

    expected_values_by_field = {
        "source_file_import_id": direct_intent["source_file_import_id"],
        "import_id": direct_intent["source_file_import_id"],
        "source_file_id": direct_intent["source_file_id"],
        "content_version": direct_intent["content_version"],
        "source_key": direct_intent["source_key"],
    }
    selector_field = _direct_selector_field(direct_intent["source_type"])
    expected_values_by_field[selector_field] = direct_intent[
        "canonical_url"
    ]
    if any(
        params_by_name.get(field_name) != expected_value
        for field_name, expected_value in expected_values_by_field.items()
    ):
        raise SingletonDirectValidationError(
            "singleton direct intent conflicts with outer parameters"
        )


def _require_singleton_selection(params_by_name: Mapping[str, Any]) -> None:
    """Require exactly one stored file on the canonical small-file path."""

    if (
        params_by_name.get("use_stored_catalog") is not True
        or params_by_name.get("max_files") != 1
        or isinstance(params_by_name.get("max_files"), bool)
    ):
        raise SingletonDirectValidationError(
            "singleton direct input must select exactly one stored file"
        )


def require_exact_wave_singleton_direct_params(
    params_by_name: Mapping[str, Any],
    *,
    wave_id: str,
) -> None:
    """Require the exact v2 outer contract before admission."""

    if not protected_singleton_direct_presence(params_by_name):
        return
    direct_intent = params_by_name.get(DIRECT_RATE_FILE_INTENT_FIELD)
    source_type = (
        direct_intent.get("source_type")
        if isinstance(direct_intent, Mapping)
        else None
    )
    selector_field = _direct_selector_field(source_type)
    expected_fields = set(_DIRECT_WAVE_PARAM_BASE_FIELDS) | {selector_field}
    if INVALID_PRICE_EXCLUSION_POLICY_FIELD in params_by_name:
        expected_fields.add(INVALID_PRICE_EXCLUSION_POLICY_FIELD)
    if set(params_by_name) != expected_fields:
        raise SingletonDirectValidationError(
            "singleton direct wave parameter fields are not exact"
        )
    if (
        params_by_name.get("importer") != "ptg"
        or params_by_name.get("operation_id") != wave_id
        or params_by_name.get("ptg_resource")
        != PTG_SMALL_RESOURCE_CONTRACT
    ):
        raise SingletonDirectValidationError(
            "singleton direct wave identity or resource is invalid"
        )
    _required_text(
        params_by_name.get("import_month"),
        field_name="import_month",
        max_bytes=16,
    )
    _required_text(
        params_by_name.get("node_id"),
        field_name="node_id",
        max_bytes=64,
    )
    plan_ids = params_by_name.get("plan_ids")
    if (
        not isinstance(plan_ids, list)
        or not plan_ids
        or any(
            not isinstance(plan_id, str)
            or not plan_id
            or plan_id != plan_id.strip()
            for plan_id in plan_ids
        )
        or plan_ids != sorted(set(plan_ids))
        or params_by_name.get("plan_market_types") != ["group"]
    ):
        raise SingletonDirectValidationError(
            "singleton direct wave plan scope is invalid"
        )


def singleton_direct_intent_sha256(
    direct_intent: Mapping[str, Any],
) -> str:
    """Hash one canonical intent in the cross-service digest domain."""

    body = json.dumps(
        dict(direct_intent),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(
        f"{DIRECT_RATE_FILE_INTENT_DIGEST_DOMAIN}:{body}".encode("utf-8")
    ).hexdigest()


def singleton_direct_source_key(source_file_id: str) -> str:
    """Reproduce the orchestrator's stable opaque PTG source key."""

    normalized = _required_text(
        source_file_id,
        field_name="source_file_id",
        max_bytes=64,
    )
    digest = hashlib.sha256(
        (
            "ptg-singleton-direct-source-key-v1:"
            + normalized
        ).encode("utf-8")
    ).hexdigest()
    return f"ptg_{digest[:24]}"


def _normalized_direct_intent(raw_intent: Any) -> dict[str, Any]:
    if not isinstance(raw_intent, Mapping) or set(raw_intent) != set(
        _DIRECT_INTENT_FIELDS
    ):
        raise SingletonDirectValidationError(
            "singleton direct intent fields are not exact"
        )
    if (
        raw_intent.get("contract") != DIRECT_RATE_FILE_INTENT_CONTRACT
        or raw_intent.get("source_type")
        not in _SELECTOR_FIELD_BY_SOURCE_TYPE
        or raw_intent.get("content_file_count") != 1
        or isinstance(raw_intent.get("content_file_count"), bool)
    ):
        raise SingletonDirectValidationError(
            "singleton direct intent contract is invalid"
        )
    normalized_intent_map = {
        "contract": DIRECT_RATE_FILE_INTENT_CONTRACT,
        "source_file_import_id": _required_text(
            raw_intent.get("source_file_import_id"),
            field_name="source_file_import_id",
            max_bytes=64,
        ),
        "source_file_id": _required_text(
            raw_intent.get("source_file_id"),
            field_name="source_file_id",
            max_bytes=64,
        ),
        "content_version": _required_text(
            raw_intent.get("content_version"),
            field_name="content_version",
            max_bytes=128,
        ),
        "source_type": raw_intent["source_type"],
        "canonical_url": _canonical_https_url(
            raw_intent.get("canonical_url")
        ),
        "source_key": _required_text(
            raw_intent.get("source_key"),
            field_name="source_key",
            max_bytes=96,
        ),
        "content_file_count": 1,
    }
    if normalized_intent_map["source_key"] != singleton_direct_source_key(
        normalized_intent_map["source_file_id"]
    ):
        raise SingletonDirectValidationError(
            "singleton direct source_key is invalid"
        )
    return normalized_intent_map


def _direct_selector_field(source_type: Any) -> str:
    selector_field = _SELECTOR_FIELD_BY_SOURCE_TYPE.get(source_type)
    if selector_field is None:
        raise SingletonDirectValidationError(
            "singleton direct source_type is invalid"
        )
    return selector_field


def _required_text(
    value: Any,
    *,
    field_name: str,
    max_bytes: int,
) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > max_bytes
        or any(ord(character) < 32 for character in value)
    ):
        raise SingletonDirectValidationError(
            f"singleton direct {field_name} is invalid"
        )
    return value


def _canonical_https_url(raw_url: Any) -> str:
    url = _required_text(
        raw_url,
        field_name="canonical_url",
        max_bytes=4096,
    )
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise SingletonDirectValidationError(
            "singleton direct URL must be canonical query-free HTTPS"
        ) from exc
    canonical_netloc = parsed.netloc.casefold()
    if canonical_netloc.endswith(":443"):
        canonical_netloc = canonical_netloc[:-4]
    canonical = urlunsplit(
        (parsed.scheme.casefold(), canonical_netloc, parsed.path or "/", "", "")
    )
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.query
        or parsed.fragment
        or parsed.path in {"", "/"}
        or canonical != url
    ):
        raise SingletonDirectValidationError(
            "singleton direct URL must be canonical query-free HTTPS"
        )
    return url


__all__ = [
    "DIRECT_RATE_FILE_INTENT_CONTRACT",
    "DIRECT_RATE_FILE_INTENT_FIELD",
    "DIRECT_RATE_FILE_INTENT_SHA256_FIELD",
    "DIRECT_RATE_FILE_PROTECTED_FIELDS",
    "DIRECT_RATE_FILE_PUBLIC_MARKER",
    "PTG_SMALL_RESOURCE_CONTRACT",
    "SingletonDirectValidationError",
    "normalize_protected_singleton_direct_params",
    "protected_singleton_direct_presence",
    "require_exact_wave_singleton_direct_params",
    "singleton_direct_failure_payload",
    "validated_singleton_invalid_price_exclusion",
]
