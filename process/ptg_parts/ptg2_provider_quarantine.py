"""Bounded evidence for malformed provider identifiers retained by strict V3."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any


PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT = (
    "ptg2_provider_identifier_quarantine_v1"
)
PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT = (
    "ptg2_provider_identifier_quarantine_v2"
)
_HASH_DOMAIN = b"PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V1\0"
_V2_HASH_DOMAIN = b"PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2\0"
_TEXT_HASH_DOMAIN = b"PTG2_PROVIDER_IDENTIFIER_TEXT_V1\0"
_MAX_DISTINCT_VALUES = 1024
_MAX_PROVIDER_GROUP_CONFLICTS = 1024
_MAX_PROVIDER_GROUP_CONFLICTING_DEFINITIONS = 4096
_MAX_TEXT_BYTES = 128
_MIN_I64 = -(2**63)
_MAX_I64 = 2**63 - 1
_EVIDENCE_FIELDS = frozenset(
    {"contract", "occurrence_count", "distinct_value_count", "sha256"}
)
_V2_EVIDENCE_FIELDS = _EVIDENCE_FIELDS | frozenset(
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


def _text_identity(value: str) -> tuple[str, int]:
    encoded = value.encode("utf-8")
    if len(encoded) > _MAX_TEXT_BYTES:
        raise ValueError("text provider identifier quarantine value exceeds 128 bytes")
    return hashlib.sha256(_TEXT_HASH_DOMAIN + encoded).hexdigest(), len(encoded)


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
    definition_digests_by_identifier: dict[str, set[str]] = {}
    input_definition_count = 0
    for conflict_index, conflict in enumerate(conflicts):
        if conflict_index >= _MAX_PROVIDER_GROUP_CONFLICTING_DEFINITIONS:
            raise ValueError("provider group conflicts exceed 4096 records")
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
        if (
            len(raw_definitions)
            > _MAX_PROVIDER_GROUP_CONFLICTING_DEFINITIONS - input_definition_count
        ):
            raise ValueError("provider group conflicts exceed 4096 definitions")
        definition_digests = [
            _sha256_text(definition_digest, "definition digest")
            for definition_digest in raw_definitions
        ]
        if len(definition_digests) < 2 or definition_digests != sorted(
            set(definition_digests)
        ):
            raise ValueError("provider group conflict definitions are not canonical")
        input_definition_count += len(definition_digests)
        definition_digests_by_identifier.setdefault(
            provider_group_id_sha256, set()
        ).update(definition_digests)
    if len(definition_digests_by_identifier) > _MAX_PROVIDER_GROUP_CONFLICTS:
        raise ValueError("provider group conflicts exceed 1024 identifiers")
    canonical_conflicts = [
        {
            "provider_group_id_sha256": identifier_digest,
            "definition_sha256": sorted(definition_digests),
        }
        for identifier_digest, definition_digests in sorted(
            definition_digests_by_identifier.items()
        )
    ]
    definition_count = sum(
        len(conflict["definition_sha256"])
        for conflict in canonical_conflicts
    )
    if definition_count > _MAX_PROVIDER_GROUP_CONFLICTING_DEFINITIONS:
        raise ValueError("provider group conflicts exceed 4096 definitions")
    return canonical_conflicts


def _validated_integer_counts(counts: Mapping[int, int]) -> dict[int, int]:
    validated_counts_by_identifier: dict[int, int] = {}
    for identifier, count in counts.items():
        if type(identifier) is not int or not (
            _MIN_I64 <= identifier <= _MAX_I64
        ):
            raise ValueError("quarantined provider identifier is not an int64")
        if identifier == 0:
            raise ValueError(
                "TIN-only NPI marker cannot appear in provider identifier quarantine"
            )
        if 1_000_000_000 <= identifier <= 9_999_999_999:
            raise ValueError("valid NPI cannot appear in provider identifier quarantine")
        if type(count) is not int or count <= 0 or count >= 2**64:
            raise ValueError("quarantined provider identifier count is invalid")
        validated_counts_by_identifier[identifier] = count
    return validated_counts_by_identifier


def _v2_text_entry(
    value_sha256: str,
    byte_length: int,
    count: int,
    digest: Any,
) -> dict[str, Any]:
    if (
        not isinstance(value_sha256, str)
        or len(value_sha256) != 64
        or any(character not in "0123456789abcdef" for character in value_sha256)
        or type(byte_length) is not int
        or not 0 <= byte_length <= _MAX_TEXT_BYTES
    ):
        raise ValueError("quarantined text provider identifier identity is invalid")
    if type(count) is not int or count <= 0 or count >= 2**64:
        raise ValueError("quarantined provider identifier count is invalid")
    digest.update(b"string\0")
    digest.update(value_sha256.encode("ascii"))
    digest.update(b"\0")
    digest.update(int(byte_length).to_bytes(8, "big"))
    digest.update(int(count).to_bytes(8, "big"))
    return {
        "kind": "string",
        "value_sha256": value_sha256,
        "byte_length": byte_length,
        "occurrence_count": count,
    }


def _update_provider_group_conflict_digest(
    digest: Any,
    conflicts: Iterable[Mapping[str, Any]],
) -> None:
    for conflict in conflicts:
        digest.update(b"provider_group_definition_conflict\0")
        digest.update(conflict["provider_group_id_sha256"].encode("ascii"))
        digest.update(b"\0")
        definitions = conflict["definition_sha256"]
        digest.update(len(definitions).to_bytes(8, "big"))
        for definition_sha256 in definitions:
            digest.update(definition_sha256.encode("ascii"))
            digest.update(b"\0")


def _payload_v2(
    integer_counts: Mapping[int, int],
    text_counts: Mapping[tuple[str, int], int],
    conflicts: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return canonical V2 evidence for malformed IDs and group conflicts."""

    integer_counts = _validated_integer_counts(integer_counts)
    if len(integer_counts) + len(text_counts) > _MAX_DISTINCT_VALUES:
        raise ValueError("provider identifier quarantine exceeds 1024 distinct values")
    canonical_conflicts = _provider_group_conflicts(conflicts)
    digest = hashlib.sha256(_V2_HASH_DOMAIN)
    entries: list[dict[str, Any]] = []
    occurrence_count = 0
    for identifier, count in sorted(integer_counts.items()):
        occurrence_count += count
        if occurrence_count >= 2**64:
            raise ValueError(
                "provider identifier quarantine occurrence count overflows uint64"
            )
        digest.update(b"integer\0")
        digest.update(str(identifier).encode("ascii"))
        digest.update(b"\0")
        digest.update(int(count).to_bytes(8, "big"))
        entries.append(
            {
                "kind": "integer",
                "value": str(identifier),
                "occurrence_count": count,
            }
        )
    for (value_sha256, byte_length), count in sorted(text_counts.items()):
        occurrence_count += count
        if occurrence_count >= 2**64:
            raise ValueError(
                "provider identifier quarantine occurrence count overflows uint64"
            )
        entries.append(
            _v2_text_entry(value_sha256, byte_length, count, digest)
        )
    _update_provider_group_conflict_digest(digest, canonical_conflicts)
    return {
        "contract": PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT,
        "occurrence_count": occurrence_count,
        "distinct_value_count": len(entries),
        "entries": entries,
        "provider_group_conflict_count": len(canonical_conflicts),
        "provider_group_conflicting_definition_count": sum(
            len(conflict["definition_sha256"])
            for conflict in canonical_conflicts
        ),
        "provider_group_definition_conflicts": canonical_conflicts,
        "sha256": digest.hexdigest(),
    }


def provider_identifier_quarantine_payload(
    counts: Mapping[int, int] | Counter[int],
    *,
    text_counts: Mapping[str, int] | Counter[str] | None = None,
    provider_group_definition_conflicts: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Return the canonical bounded quarantine payload for PostgreSQL JSONB."""

    normalized_counts_by_identifier = _validated_integer_counts(counts)
    normalized_text_counts: Counter[tuple[str, int]] = Counter()
    for raw_value, raw_count in (text_counts or {}).items():
        if type(raw_value) is not str:
            raise ValueError("quarantined text provider identifier is not text")
        if type(raw_count) is not int or raw_count <= 0 or raw_count >= 2**64:
            raise ValueError("quarantined provider identifier count is invalid")
        normalized_text_counts[_text_identity(raw_value)] += raw_count
    if (
        len(normalized_counts_by_identifier) + len(normalized_text_counts)
        > _MAX_DISTINCT_VALUES
    ):
        raise ValueError("provider identifier quarantine exceeds 1024 distinct values")
    occurrence_count = sum(normalized_counts_by_identifier.values())
    if occurrence_count >= 2**64:
        raise ValueError("provider identifier quarantine occurrence count overflows uint64")
    conflicts = _provider_group_conflicts(provider_group_definition_conflicts)
    if normalized_text_counts or conflicts:
        return _payload_v2(
            normalized_counts_by_identifier,
            normalized_text_counts,
            conflicts,
        )
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


def _validated_integer_entry(
    entry: Mapping[str, Any],
    contract: str,
    previous_identifier: int | None,
    *,
    has_seen_text: bool,
) -> tuple[int, Any]:
    expected_fields = {"value", "occurrence_count"}
    if contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT:
        expected_fields.add("kind")
    if set(entry) != expected_fields or has_seen_text:
        raise ValueError("provider identifier quarantine entry is incompatible")
    value_text = entry.get("value")
    if not isinstance(value_text, str):
        raise ValueError("quarantined provider identifier value must be text")
    try:
        identifier = int(value_text)
    except ValueError as exc:
        raise ValueError("quarantined provider identifier value is invalid") from exc
    if str(identifier) != value_text:
        raise ValueError("quarantined provider identifier value is not canonical")
    if previous_identifier is not None and identifier <= previous_identifier:
        raise ValueError("quarantined provider identifier values are not ordered")
    return identifier, entry.get("occurrence_count")


def _validated_text_entry(
    entry: Mapping[str, Any],
    previous_identity: tuple[str, int] | None,
) -> tuple[tuple[str, int], Any]:
    if entry.get("kind") != "string" or set(entry) != {
            "kind",
            "value_sha256",
            "byte_length",
            "occurrence_count",
    }:
        raise ValueError("provider identifier quarantine entry is incompatible")
    identity = (entry.get("value_sha256"), entry.get("byte_length"))
    if (
        not isinstance(identity[0], str)
        or len(identity[0]) != 64
        or any(character not in "0123456789abcdef" for character in identity[0])
        or type(identity[1]) is not int
        or not 0 <= identity[1] <= _MAX_TEXT_BYTES
    ):
        raise ValueError("quarantined text provider identifier identity is invalid")
    if previous_identity is not None and identity <= previous_identity:
        raise ValueError("quarantined provider identifier values are not ordered")
    return identity, entry.get("occurrence_count")


def _quarantine_contract_and_expected_fields(
    quarantine_payload: Mapping[str, Any],
) -> tuple[str, set[str]]:
    expected_fields = {
        "contract",
        "occurrence_count",
        "distinct_value_count",
        "entries",
        "sha256",
    }
    contract = quarantine_payload.get("contract")
    if contract not in {
        PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT,
        PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT,
    }:
        raise ValueError("provider identifier quarantine contract is incompatible")
    if contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT:
        expected_fields |= {
            "provider_group_conflict_count",
            "provider_group_conflicting_definition_count",
            "provider_group_definition_conflicts",
        }
    return contract, expected_fields


def validate_provider_identifier_quarantine(
    quarantine_payload: Any,
) -> dict[str, Any]:
    """Validate and canonicalize one scanner or persisted quarantine payload."""

    if not isinstance(quarantine_payload, Mapping):
        raise ValueError("provider identifier quarantine must be an object")
    contract, expected_fields = _quarantine_contract_and_expected_fields(
        quarantine_payload
    )
    if set(quarantine_payload) != expected_fields:
        raise ValueError("provider identifier quarantine fields are incompatible")
    entries = quarantine_payload.get("entries")
    if not isinstance(entries, list):
        raise ValueError("provider identifier quarantine entries must be an array")
    counts: Counter[int] = Counter()
    text_counts: Counter[tuple[str, int]] = Counter()
    previous_identifier: int | None = None
    previous_text_identity: tuple[str, int] | None = None
    has_seen_text = False
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise ValueError("provider identifier quarantine entry is incompatible")
        entry_kind = (
            entry.get("kind")
            if contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT
            else "integer"
        )
        if entry_kind == "integer":
            previous_identifier, count = _validated_integer_entry(
                entry,
                contract,
                previous_identifier,
                has_seen_text=has_seen_text,
            )
            counts[previous_identifier] = count
            continue
        previous_text_identity, count = _validated_text_entry(
            entry, previous_text_identity
        )
        has_seen_text = True
        text_counts[previous_text_identity] = count
    canonical = (
        provider_identifier_quarantine_payload(counts)
        if contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT
        else _payload_v2(
            counts,
            text_counts,
            quarantine_payload.get("provider_group_definition_conflicts", ()),
        )
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
        _V2_EVIDENCE_FIELDS
        if canonical["contract"]
        == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT
        else _EVIDENCE_FIELDS
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
    if evidence_fields not in {_EVIDENCE_FIELDS, _V2_EVIDENCE_FIELDS}:
        raise ValueError("provider identifier quarantine evidence is incompatible")
    is_contract_valid = (
        evidence_fields == _EVIDENCE_FIELDS
        and contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT
    ) or (
        evidence_fields == _V2_EVIDENCE_FIELDS
        and contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT
    )
    occurrence_count = evidence_by_field.get("occurrence_count")
    distinct_count = evidence_by_field.get("distinct_value_count")
    digest = evidence_by_field.get("sha256")
    if (
        not is_contract_valid
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
    if contract == PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT:
        conflict_count = evidence_by_field.get("provider_group_conflict_count")
        definition_count = evidence_by_field.get(
            "provider_group_conflicting_definition_count"
        )
        if (
            type(conflict_count) is not int
            or not 0 <= conflict_count <= _MAX_PROVIDER_GROUP_CONFLICTS
            or type(definition_count) is not int
            or definition_count > _MAX_PROVIDER_GROUP_CONFLICTING_DEFINITIONS
            or (
                (conflict_count == 0 and definition_count != 0)
                or (
                    conflict_count > 0
                    and definition_count < 2 * conflict_count
                )
            )
        ):
            raise ValueError("provider identifier quarantine evidence is invalid")
    return evidence_by_field


def combine_provider_identifier_quarantines(
    payloads: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Combine exact per-source quarantine payloads into one snapshot payload."""

    counts: Counter[int] = Counter()
    text_counts: Counter[tuple[str, int]] = Counter()
    definition_digests_by_identifier: dict[str, set[str]] = {}
    conflicting_definition_count = 0
    for source_payload in payloads:
        canonical = validate_provider_identifier_quarantine(source_payload)
        for entry in canonical["entries"]:
            if entry.get("kind", "integer") == "integer":
                counts[int(entry["value"])] += int(entry["occurrence_count"])
            else:
                text_counts[(entry["value_sha256"], entry["byte_length"])] += int(
                    entry["occurrence_count"]
                )
        for conflict in canonical.get("provider_group_definition_conflicts", ()):
            identifier_digest = conflict["provider_group_id_sha256"]
            definitions = definition_digests_by_identifier.get(identifier_digest)
            if definitions is None:
                if len(definition_digests_by_identifier) >= _MAX_PROVIDER_GROUP_CONFLICTS:
                    raise ValueError("provider group conflicts exceed 1024 identifiers")
                definitions = set()
                definition_digests_by_identifier[identifier_digest] = definitions
            new_definitions = set(conflict["definition_sha256"]).difference(definitions)
            if (
                len(new_definitions)
                > _MAX_PROVIDER_GROUP_CONFLICTING_DEFINITIONS
                - conflicting_definition_count
            ):
                raise ValueError("provider group conflicts exceed 4096 definitions")
            definitions.update(new_definitions)
            conflicting_definition_count += len(new_definitions)
    conflicts = [
        {
            "provider_group_id_sha256": identifier_digest,
            "definition_sha256": sorted(definition_digests),
        }
        for identifier_digest, definition_digests in sorted(
            definition_digests_by_identifier.items()
        )
    ]
    return (
        _payload_v2(counts, text_counts, conflicts)
        if text_counts or conflicts
        else provider_identifier_quarantine_payload(counts)
    )


__all__ = [
    "PTG2_PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT",
    "PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V2_CONTRACT",
    "combine_provider_identifier_quarantines",
    "provider_identifier_quarantine_evidence",
    "provider_identifier_quarantine_payload",
    "validate_provider_identifier_quarantine",
    "validate_provider_identifier_quarantine_evidence",
]
