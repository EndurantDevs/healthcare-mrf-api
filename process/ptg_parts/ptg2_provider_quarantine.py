"""Bounded evidence for malformed provider identifiers retained by strict V3."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any


PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT = (
    "ptg2_provider_identifier_quarantine_v1"
)
_HASH_DOMAIN = b"PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V1\0"
_MAX_DISTINCT_VALUES = 1024
_MIN_I64 = -(2**63)
_MAX_I64 = 2**63 - 1
_EVIDENCE_FIELDS = frozenset(
    {"contract", "occurrence_count", "distinct_value_count", "sha256"}
)


def _digest(counts: Mapping[int, int]) -> str:
    digest = hashlib.sha256(_HASH_DOMAIN)
    for value, count in sorted(counts.items()):
        digest.update(str(value).encode("ascii"))
        digest.update(b"\0")
        digest.update(int(count).to_bytes(8, "big"))
    return digest.hexdigest()


def provider_identifier_quarantine_payload(
    counts: Mapping[int, int] | Counter[int],
) -> dict[str, Any]:
    """Return the canonical bounded quarantine payload for PostgreSQL JSONB."""

    normalized_counts_by_identifier: dict[int, int] = {}
    for raw_value, raw_count in counts.items():
        if type(raw_value) is not int or not (_MIN_I64 <= raw_value <= _MAX_I64):
            raise ValueError("quarantined provider identifier is not an int64")
        if raw_value == 0:
            raise ValueError("TIN-only NPI marker cannot appear in provider identifier quarantine")
        if 1_000_000_000 <= raw_value <= 9_999_999_999:
            raise ValueError("valid NPI cannot appear in provider identifier quarantine")
        if type(raw_count) is not int or raw_count <= 0 or raw_count >= 2**64:
            raise ValueError("quarantined provider identifier count is invalid")
        normalized_counts_by_identifier[raw_value] = raw_count
    if len(normalized_counts_by_identifier) > _MAX_DISTINCT_VALUES:
        raise ValueError("provider identifier quarantine exceeds 1024 distinct values")
    occurrence_count = sum(normalized_counts_by_identifier.values())
    if occurrence_count >= 2**64:
        raise ValueError("provider identifier quarantine occurrence count overflows uint64")
    return {
        "contract": PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT,
        "occurrence_count": occurrence_count,
        "distinct_value_count": len(normalized_counts_by_identifier),
        "entries": [
            {"value": str(identifier), "occurrence_count": count}
            for identifier, count in sorted(normalized_counts_by_identifier.items())
        ],
        "sha256": _digest(normalized_counts_by_identifier),
    }


def validate_provider_identifier_quarantine(
    quarantine_payload: Any,
) -> dict[str, Any]:
    """Validate and canonicalize one scanner or persisted quarantine payload."""

    if not isinstance(quarantine_payload, Mapping):
        raise ValueError("provider identifier quarantine must be an object")
    if set(quarantine_payload) != {
        "contract",
        "occurrence_count",
        "distinct_value_count",
        "entries",
        "sha256",
    }:
        raise ValueError("provider identifier quarantine fields are incompatible")
    if (
        quarantine_payload.get("contract")
        != PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT
    ):
        raise ValueError("provider identifier quarantine contract is incompatible")
    entries = quarantine_payload.get("entries")
    if not isinstance(entries, list):
        raise ValueError("provider identifier quarantine entries must be an array")
    counts: Counter[int] = Counter()
    previous_value: int | None = None
    for entry in entries:
        if not isinstance(entry, Mapping) or set(entry) != {
            "value",
            "occurrence_count",
        }:
            raise ValueError("provider identifier quarantine entry is incompatible")
        value_text = entry.get("value")
        count = entry.get("occurrence_count")
        if not isinstance(value_text, str):
            raise ValueError("quarantined provider identifier value must be text")
        try:
            identifier = int(value_text)
        except ValueError as exc:
            raise ValueError("quarantined provider identifier value is invalid") from exc
        if str(identifier) != value_text:
            raise ValueError("quarantined provider identifier value is not canonical")
        if previous_value is not None and identifier <= previous_value:
            raise ValueError("quarantined provider identifier values are not ordered")
        previous_value = identifier
        counts[identifier] = count
    canonical = provider_identifier_quarantine_payload(counts)
    if dict(quarantine_payload) != canonical:
        raise ValueError("provider identifier quarantine digest or counts do not match")
    return canonical


def provider_identifier_quarantine_evidence(
    quarantine_payload: Any,
) -> dict[str, Any]:
    """Return bounded report evidence without raw malformed identifiers."""

    canonical = validate_provider_identifier_quarantine(quarantine_payload)
    return {
        field_name: canonical[field_name]
        for field_name in sorted(_EVIDENCE_FIELDS)
    }


def validate_provider_identifier_quarantine_evidence(
    raw_evidence: Any,
) -> dict[str, Any]:
    """Validate redacted quarantine counts and digest for candidate binding."""

    if not isinstance(raw_evidence, Mapping) or set(raw_evidence) != _EVIDENCE_FIELDS:
        raise ValueError("provider identifier quarantine evidence is incompatible")
    evidence_by_field = dict(raw_evidence)
    occurrence_count = evidence_by_field.get("occurrence_count")
    distinct_count = evidence_by_field.get("distinct_value_count")
    digest = evidence_by_field.get("sha256")
    if (
        evidence_by_field.get("contract")
        != PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT
        or type(occurrence_count) is not int
        or occurrence_count < 0
        or occurrence_count >= 2**64
        or type(distinct_count) is not int
        or distinct_count < 0
        or distinct_count > _MAX_DISTINCT_VALUES
        or distinct_count > occurrence_count
        or (distinct_count == 0) != (occurrence_count == 0)
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ValueError("provider identifier quarantine evidence is invalid")
    return evidence_by_field


def combine_provider_identifier_quarantines(
    payloads: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Combine exact per-source quarantine payloads into one snapshot payload."""

    counts: Counter[int] = Counter()
    for payload in payloads:
        canonical = validate_provider_identifier_quarantine(payload)
        for entry in canonical["entries"]:
            counts[int(entry["value"])] += int(entry["occurrence_count"])
    return provider_identifier_quarantine_payload(counts)


__all__ = [
    "PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT",
    "combine_provider_identifier_quarantines",
    "provider_identifier_quarantine_evidence",
    "provider_identifier_quarantine_payload",
    "validate_provider_identifier_quarantine",
    "validate_provider_identifier_quarantine_evidence",
]
