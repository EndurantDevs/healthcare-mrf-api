"""Public/event projection for private frozen multipart evidence."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_FIELD,
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    DIRECT_RATE_FILE_PUBLIC_MARKER,
)


_FROZEN_INPUT_MARKER_KEYS = frozenset(
    {
        "frozen_rate_file_set_contract",
        "frozen_rate_files",
        "frozen_rate_file_set_sha256",
        "frozen_rate_file_count",
        "frozen_rate_file_proof",
        "frozen_rate_file_proof_sha256",
        "frozen_rate_file_set_protected",
    }
)
_FROZEN_MARKER_KEYS = _FROZEN_INPUT_MARKER_KEYS | frozenset(
    {
        DIRECT_RATE_FILE_INTENT_FIELD,
        DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
        DIRECT_RATE_FILE_PUBLIC_MARKER,
        "invalid_price_exclusion_policy",
        "allowed_url",
        "in_network_url",
    }
)
_PRIVATE_SCALAR_KEYS = frozenset(
    {
        "canonical_url",
        "etag",
        "last_modified",
        "raw_sha256",
        "logical_sha256",
        "engine_source_identity_hash",
        "engine_source_file_version_id",
        "frozen_rate_file_set_sha256",
        "frozen_rate_file_proof_sha256",
        "allowed_url",
        "in_network_url",
        "source_file_id",
        "source_key",
    }
)
_PRIVATE_EVIDENCE_KEYS = frozenset(
    {
        "frozen_rate_file_set_contract",
        "frozen_rate_files",
        "frozen_rate_file_set_sha256",
        "frozen_rate_file_proof",
        "frozen_rate_file_proof_sha256",
        "invalid_price_exclusion_policy",
        "source_file_versions",
        "successful_files",
        "skipped_files",
        "failed_files",
        DIRECT_RATE_FILE_INTENT_FIELD,
        "allowed_url",
        "in_network_url",
        "source_file_id",
        "source_key",
    }
)


def has_frozen_private_evidence(fragment: Any) -> bool:
    """Return whether a nested payload carries the protected namespace."""

    if isinstance(fragment, Mapping):
        if _FROZEN_MARKER_KEYS & set(fragment):
            return True
        return any(
            has_frozen_private_evidence(value)
            for value in fragment.values()
        )
    if isinstance(fragment, (list, tuple)):
        return any(has_frozen_private_evidence(value) for value in fragment)
    return False


def frozen_private_scalar_values(fragment: Any) -> frozenset[str]:
    """Collect exact private scalar values for recursive free-text redaction."""

    private_values: set[str] = set()
    _collect_private_values(fragment, private_values)
    return frozenset(private_values)


def _collect_private_values(fragment: Any, private_values: set[str]) -> None:
    if isinstance(fragment, Mapping):
        for field_name, field_value in fragment.items():
            if (
                field_name in _PRIVATE_SCALAR_KEYS
                and isinstance(field_value, str)
                and field_value
            ):
                private_values.add(field_value)
            _collect_private_values(field_value, private_values)
    elif isinstance(fragment, (list, tuple)):
        for nested_fragment in fragment:
            _collect_private_values(nested_fragment, private_values)


def redact_frozen_public_values(
    response_fragment: Any,
    private_values: frozenset[str],
    *,
    strip_evidence: bool = False,
) -> Any:
    """Remove evidence objects and redact their values from public payloads."""

    if not private_values and not strip_evidence:
        return response_fragment
    if isinstance(response_fragment, Mapping):
        return {
            key: redact_frozen_public_values(
                nested_fragment,
                private_values,
                strip_evidence=strip_evidence,
            )
            for key, nested_fragment in response_fragment.items()
            if not (
                (strip_evidence or private_values)
                and key in _PRIVATE_EVIDENCE_KEYS
            )
        }
    if isinstance(response_fragment, list):
        return [
            redact_frozen_public_values(
                nested_fragment,
                private_values,
                strip_evidence=strip_evidence,
            )
            for nested_fragment in response_fragment
        ]
    if isinstance(response_fragment, tuple):
        return tuple(
            redact_frozen_public_values(
                nested_fragment,
                private_values,
                strip_evidence=strip_evidence,
            )
            for nested_fragment in response_fragment
        )
    if isinstance(response_fragment, str) and any(
        private_value in response_fragment
        for private_value in private_values
    ):
        return "[protected frozen source]"
    return response_fragment


def project_frozen_status_event(
    status_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Project a create or terminal event without private source evidence."""

    payload = dict(status_payload)
    if not has_frozen_private_evidence(payload):
        return payload
    direct_digest = _direct_intent_digest(payload)
    has_frozen_evidence = _has_frozen_input_marker(payload)
    private_values = frozen_private_scalar_values(payload)
    projected = redact_frozen_public_values(
        payload,
        private_values,
        strip_evidence=True,
    )
    file_count = _frozen_file_count(payload)
    if has_frozen_evidence:
        _set_public_marker(projected.get("params"), file_count)
        _set_public_marker(projected.get("metrics"), file_count)
    _set_direct_public_marker(projected.get("params"), direct_digest)
    _set_direct_public_marker(projected.get("metrics"), direct_digest)
    return projected


def _has_frozen_input_marker(fragment: Any) -> bool:
    if isinstance(fragment, Mapping):
        return bool(_FROZEN_INPUT_MARKER_KEYS & set(fragment)) or any(
            _has_frozen_input_marker(value) for value in fragment.values()
        )
    if isinstance(fragment, (list, tuple)):
        return any(_has_frozen_input_marker(value) for value in fragment)
    return False


def _direct_intent_digest(fragment: Any) -> str | None:
    if isinstance(fragment, Mapping):
        raw_digest = fragment.get(DIRECT_RATE_FILE_INTENT_SHA256_FIELD)
        if (
            isinstance(raw_digest, str)
            and len(raw_digest) == 64
            and all(character in "0123456789abcdef" for character in raw_digest)
        ):
            return raw_digest
        for value in fragment.values():
            nested_digest = _direct_intent_digest(value)
            if nested_digest is not None:
                return nested_digest
    elif isinstance(fragment, (list, tuple)):
        for value in fragment:
            nested_digest = _direct_intent_digest(value)
            if nested_digest is not None:
                return nested_digest
    return None


def _frozen_file_count(fragment: Any) -> int | None:
    if isinstance(fragment, Mapping):
        raw_count = fragment.get("frozen_rate_file_count")
        if type(raw_count) is int and raw_count > 0:
            return raw_count
        for value in fragment.values():
            nested_count = _frozen_file_count(value)
            if nested_count is not None:
                return nested_count
    elif isinstance(fragment, (list, tuple)):
        for value in fragment:
            nested_count = _frozen_file_count(value)
            if nested_count is not None:
                return nested_count
    return None


def _set_public_marker(fragment: Any, file_count: int | None) -> None:
    if not isinstance(fragment, dict):
        return
    fragment["frozen_rate_file_set_protected"] = True
    if file_count is not None:
        fragment["frozen_rate_file_count"] = file_count


def _set_direct_public_marker(
    fragment: Any,
    intent_digest: str | None,
) -> None:
    if not isinstance(fragment, dict) or intent_digest is None:
        return
    fragment[DIRECT_RATE_FILE_PUBLIC_MARKER] = True
    fragment[DIRECT_RATE_FILE_INTENT_SHA256_FIELD] = intent_digest


__all__ = [
    "frozen_private_scalar_values",
    "has_frozen_private_evidence",
    "project_frozen_status_event",
    "redact_frozen_public_values",
]
