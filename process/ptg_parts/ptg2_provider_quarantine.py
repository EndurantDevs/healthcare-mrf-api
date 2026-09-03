"""Bounded evidence for quarantined provider identifiers and definitions."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any


PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT = (
    "ptg2_provider_identifier_quarantine_v1"
)
PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT_V2 = (
    "ptg2_provider_identifier_quarantine_v2"
)
_HASH_DOMAIN = b"PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V1\0"
_HASH_DOMAIN_V2 = b"PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2\0"
_MAX_DISTINCT_VALUES = 1024
_MAX_PROVIDER_GROUP_CONFLICTS = 1024
_MAX_PROVIDER_GROUP_CONFLICTING_DEFINITIONS = 4096
_MIN_I64 = -(2**63)
_MAX_I64 = 2**63 - 1
_EVIDENCE_FIELDS_V1 = frozenset(
    {"contract", "occurrence_count", "distinct_value_count", "sha256"}
)
_EVIDENCE_FIELDS_V2 = _EVIDENCE_FIELDS_V1 | frozenset(
    {
        "provider_group_conflict_count",
        "provider_group_conflicting_definition_count",
    }
)


def _digest(counts: Mapping[int, int]) -> str:
    digest = hashlib.sha256(_HASH_DOMAIN)
    for value, count in sorted(counts.items()):
        digest.update(str(value).encode("ascii"))
        digest.update(b"\0")
        digest.update(int(count).to_bytes(8, "big"))
    return digest.hexdigest()


def _sha256_text(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"provider group conflict {field_name} is invalid")
    return value


def _provider_group_conflicts(
    conflicts: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    canonical: list[dict[str, Any]] = []
    definition_count = 0
    for conflict in conflicts:
        if not isinstance(conflict, Mapping) or set(conflict) != {
            "provider_group_id_sha256",
            "definition_sha256",
        }:
            raise ValueError("provider group conflict fields are incompatible")
        provider_group_id_sha256 = _sha256_text(
            conflict.get("provider_group_id_sha256"), "identifier digest"
        )
        raw_definitions = conflict.get("definition_sha256")
        if not isinstance(raw_definitions, (list, tuple)):
            raise ValueError("provider group conflict definitions must be an array")
        definition_sha256 = [
            _sha256_text(value, "definition digest") for value in raw_definitions
        ]
        if len(definition_sha256) < 2 or definition_sha256 != sorted(
            set(definition_sha256)
        ):
            raise ValueError(
                "provider group conflict definitions are not canonical"
            )
        definition_count += len(definition_sha256)
        canonical.append(
            {
                "provider_group_id_sha256": provider_group_id_sha256,
                "definition_sha256": definition_sha256,
            }
        )
    canonical.sort(
        key=lambda conflict: (
            conflict["provider_group_id_sha256"],
            conflict["definition_sha256"],
        )
    )
    if len(canonical) > _MAX_PROVIDER_GROUP_CONFLICTS:
        raise ValueError("provider group conflicts exceed 1024 identifiers")
    if definition_count > _MAX_PROVIDER_GROUP_CONFLICTING_DEFINITIONS:
        raise ValueError("provider group conflicts exceed 4096 definitions")
    return canonical


def _digest_v2(
    entries: list[dict[str, Any]],
    conflicts: list[dict[str, Any]],
) -> str:
    payload = {
        "entries": entries,
        "provider_group_definition_conflicts": conflicts,
    }
    return hashlib.sha256(
        _HASH_DOMAIN_V2
        + json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def provider_identifier_quarantine_payload(
    counts: Mapping[int, int] | Counter[int],
    *,
    provider_group_definition_conflicts: Iterable[Mapping[str, Any]] = (),
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
    entries = [
        {"value": str(identifier), "occurrence_count": count}
        for identifier, count in sorted(normalized_counts_by_identifier.items())
    ]
    conflicts = _provider_group_conflicts(provider_group_definition_conflicts)
    if not conflicts:
        return {
            "contract": PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT,
            "occurrence_count": occurrence_count,
            "distinct_value_count": len(normalized_counts_by_identifier),
            "entries": entries,
            "sha256": _digest(normalized_counts_by_identifier),
        }
    return {
        "contract": PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT_V2,
        "occurrence_count": occurrence_count,
        "distinct_value_count": len(normalized_counts_by_identifier),
        "entries": entries,
        "provider_group_conflict_count": len(conflicts),
        "provider_group_conflicting_definition_count": sum(
            len(conflict["definition_sha256"]) for conflict in conflicts
        ),
        "provider_group_definition_conflicts": conflicts,
        "sha256": _digest_v2(entries, conflicts),
    }


def validate_provider_identifier_quarantine(
    quarantine_payload: Any,
) -> dict[str, Any]:
    """Validate and canonicalize one scanner or persisted quarantine payload."""

    if not isinstance(quarantine_payload, Mapping):
        raise ValueError("provider identifier quarantine must be an object")
    contract = quarantine_payload.get("contract")
    v1_fields = {
        "contract",
        "occurrence_count",
        "distinct_value_count",
        "entries",
        "sha256",
    }
    v2_fields = v1_fields | {
        "provider_group_conflict_count",
        "provider_group_conflicting_definition_count",
        "provider_group_definition_conflicts",
    }
    if contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT:
        expected_fields = v1_fields
    elif contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT_V2:
        expected_fields = v2_fields
    else:
        raise ValueError("provider identifier quarantine contract is incompatible")
    if set(quarantine_payload) != expected_fields:
        raise ValueError("provider identifier quarantine fields are incompatible")
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
    raw_conflicts = (
        quarantine_payload.get("provider_group_definition_conflicts", ())
        if contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT_V2
        else ()
    )
    canonical = provider_identifier_quarantine_payload(
        counts,
        provider_group_definition_conflicts=raw_conflicts,
    )
    if dict(quarantine_payload) != canonical:
        raise ValueError("provider identifier quarantine digest or counts do not match")
    return canonical


def provider_identifier_quarantine_evidence(
    quarantine_payload: Any,
) -> dict[str, Any]:
    """Return bounded report evidence without raw malformed identifiers."""

    canonical = validate_provider_identifier_quarantine(quarantine_payload)
    evidence_fields = (
        _EVIDENCE_FIELDS_V2
        if canonical["contract"] == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT_V2
        else _EVIDENCE_FIELDS_V1
    )
    return {
        field_name: canonical[field_name]
        for field_name in sorted(evidence_fields)
    }


def validate_provider_identifier_quarantine_evidence(
    raw_evidence: Any,
) -> dict[str, Any]:
    """Validate redacted quarantine counts and digest for candidate binding."""

    if not isinstance(raw_evidence, Mapping):
        raise ValueError("provider identifier quarantine evidence is incompatible")
    evidence_by_field = dict(raw_evidence)
    contract = evidence_by_field.get("contract")
    evidence_fields = frozenset(evidence_by_field)
    if evidence_fields not in {_EVIDENCE_FIELDS_V1, _EVIDENCE_FIELDS_V2}:
        raise ValueError("provider identifier quarantine evidence is incompatible")
    contract_is_valid = (
        evidence_fields == _EVIDENCE_FIELDS_V1
        and contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT
    ) or (
        evidence_fields == _EVIDENCE_FIELDS_V2
        and contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT_V2
    )
    occurrence_count = evidence_by_field.get("occurrence_count")
    distinct_count = evidence_by_field.get("distinct_value_count")
    digest = evidence_by_field.get("sha256")
    if (
        not contract_is_valid
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
    if contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT_V2:
        conflict_count = evidence_by_field.get("provider_group_conflict_count")
        definition_count = evidence_by_field.get(
            "provider_group_conflicting_definition_count"
        )
        if (
            type(conflict_count) is not int
            or not 1 <= conflict_count <= _MAX_PROVIDER_GROUP_CONFLICTS
            or type(definition_count) is not int
            or not 2 * conflict_count
            <= definition_count
            <= _MAX_PROVIDER_GROUP_CONFLICTING_DEFINITIONS
        ):
            raise ValueError("provider identifier quarantine evidence is invalid")
    return evidence_by_field


def combine_provider_identifier_quarantines(
    payloads: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Combine exact per-source quarantine payloads into one snapshot payload."""

    counts: Counter[int] = Counter()
    conflicts: list[dict[str, Any]] = []
    for payload in payloads:
        canonical = validate_provider_identifier_quarantine(payload)
        for entry in canonical["entries"]:
            counts[int(entry["value"])] += int(entry["occurrence_count"])
        conflicts.extend(canonical.get("provider_group_definition_conflicts", ()))
    return provider_identifier_quarantine_payload(
        counts,
        provider_group_definition_conflicts=conflicts,
    )


__all__ = [
    "PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT",
    "PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT_V2",
    "combine_provider_identifier_quarantines",
    "provider_identifier_quarantine_evidence",
    "provider_identifier_quarantine_payload",
    "validate_provider_identifier_quarantine",
    "validate_provider_identifier_quarantine_evidence",
]
