# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact-source evidence for explicitly authorized malformed-price exclusions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any


PTG2_INVALID_PRICE_EXCLUSION_CONTRACT = "ptg2_invalid_price_exclusion_v1"
PTG2_INVALID_PRICE_EXCLUSION_SOURCE_CONTRACT = "ptg2_invalid_price_exclusion_source_v1"
PTG2_INVALID_PRICE_EXCLUSION_REASON = "invalid_iso_calendar_date"
INVALID_PRICE_EXCLUSION_POLICY_FIELD = "invalid_price_exclusion_policy"
_MAX_SOURCES = 128
_MAX_ENTRIES = 1024
PTG2_INVALID_PRICE_EXCLUSION_SOURCE_MAX_JSON_BYTES = 65_536
_ENTRY_FIELDS = frozenset(
    {
        "object_ordinal",
        "rate_ordinal",
        "price_ordinal",
        "invalid_value_sha256",
    }
)
_SOURCE_FIELDS = frozenset(
    {
        "raw_source_sha256",
        "emptied_rate_count",
        "entries",
        "sha256",
    }
)
_POLICY_FIELDS = frozenset(
    {
        "contract",
        "reason",
        "excluded_price_count",
        "emptied_rate_count",
        "source_count",
        "sources",
        "sha256",
    }
)
_EVIDENCE_FIELDS = _POLICY_FIELDS - {"sources"}
_SOURCE_EVIDENCE_FIELDS = frozenset(
    {
        "contract",
        "reason",
        "excluded_price_count",
        "emptied_rate_count",
        "sha256",
    }
)


def _sha256(value: Any, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"invalid price exclusion {field_name} is invalid")
    return value


def _count(value: Any, field_name: str) -> int:
    if type(value) is not int or value < 0 or value >= 2**64:
        raise ValueError(f"invalid price exclusion {field_name} is invalid")
    return value


def invalid_price_value_sha256(value: str) -> str:
    """Hash one normalized invalid value without retaining it in evidence."""

    if not isinstance(value, str) or not value:
        raise ValueError("invalid price exclusion value is invalid")
    return hashlib.sha256(b"PTG2_INVALID_PRICE_EXCLUSION_VALUE_V1\0" + value.encode("utf-8")).hexdigest()


def _entry(raw_entry: Any) -> dict[str, Any]:
    if not isinstance(raw_entry, Mapping) or set(raw_entry) != _ENTRY_FIELDS:
        raise ValueError("invalid price exclusion entry fields are incompatible")
    return {
        "object_ordinal": _count(raw_entry.get("object_ordinal"), "object_ordinal"),
        "rate_ordinal": _count(raw_entry.get("rate_ordinal"), "rate_ordinal"),
        "price_ordinal": _count(raw_entry.get("price_ordinal"), "price_ordinal"),
        "invalid_value_sha256": _sha256(raw_entry.get("invalid_value_sha256"), "invalid_value_sha256"),
    }


def _source_digest(entries: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256(b"PTG2_INVALID_PRICE_EXCLUSION_SOURCE_V1\0")
    for entry in entries:
        for field_name in ("object_ordinal", "rate_ordinal", "price_ordinal"):
            digest.update(int(entry[field_name]).to_bytes(8, "big"))
        digest.update(bytes.fromhex(entry["invalid_value_sha256"]))
    return digest.hexdigest()


def invalid_price_exclusion_source(
    *,
    raw_source_sha256: str,
    entries: Iterable[Mapping[str, Any]],
    emptied_rate_count: int,
) -> dict[str, Any]:
    """Build exact bounded source evidence from discovered raw coordinates."""

    canonical_entries = sorted(
        (_entry(entry) for entry in entries),
        key=lambda entry: (
            entry["object_ordinal"],
            entry["rate_ordinal"],
            entry["price_ordinal"],
        ),
    )
    if not canonical_entries or len(canonical_entries) > _MAX_ENTRIES:
        raise ValueError("invalid price exclusion entry count is invalid")
    coordinates = [
        (
            entry["object_ordinal"],
            entry["rate_ordinal"],
            entry["price_ordinal"],
        )
        for entry in canonical_entries
    ]
    if len(coordinates) != len(set(coordinates)):
        raise ValueError("invalid price exclusion coordinates are ambiguous")
    emptied_count = _count(emptied_rate_count, "emptied_rate_count")
    if emptied_count > len(canonical_entries):
        raise ValueError("invalid price exclusion emptied-rate count is invalid")
    return {
        "raw_source_sha256": _sha256(raw_source_sha256, "raw_source_sha256"),
        "emptied_rate_count": emptied_count,
        "entries": canonical_entries,
        "sha256": _source_digest(canonical_entries),
    }


def _source(raw_source: Any) -> dict[str, Any]:
    if not isinstance(raw_source, Mapping) or set(raw_source) != _SOURCE_FIELDS:
        raise ValueError("invalid price exclusion source fields are incompatible")
    entries = raw_source.get("entries")
    if not isinstance(entries, list):
        raise ValueError("invalid price exclusion source entries are invalid")
    canonical = invalid_price_exclusion_source(
        raw_source_sha256=raw_source.get("raw_source_sha256"),
        entries=entries,
        emptied_rate_count=raw_source.get("emptied_rate_count"),
    )
    if dict(raw_source) != canonical:
        raise ValueError("invalid price exclusion source digest does not match")
    return canonical


def _policy_digest(source_rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256(b"PTG2_INVALID_PRICE_EXCLUSION_V1\0")
    for source_by_field in source_rows:
        digest.update(bytes.fromhex(source_by_field["raw_source_sha256"]))
        digest.update(len(source_by_field["entries"]).to_bytes(8, "big"))
        digest.update(int(source_by_field["emptied_rate_count"]).to_bytes(8, "big"))
        digest.update(bytes.fromhex(source_by_field["sha256"]))
    return digest.hexdigest()


def _source_expectation(source_by_name: Mapping[str, Any]) -> dict[str, Any]:
    expectation_by_name = {
        "contract": PTG2_INVALID_PRICE_EXCLUSION_SOURCE_CONTRACT,
        "reason": PTG2_INVALID_PRICE_EXCLUSION_REASON,
        "excluded_price_count": len(source_by_name["entries"]),
        **source_by_name,
    }
    encoded = json.dumps(
        expectation_by_name,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > PTG2_INVALID_PRICE_EXCLUSION_SOURCE_MAX_JSON_BYTES:
        raise ValueError("invalid price exclusion source exceeds scanner transport limit")
    return expectation_by_name


def invalid_price_exclusion_policy(
    source_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build the bounded canonical policy from exact per-source evidence."""

    canonical_sources = sorted(
        (_source(source_by_field) for source_by_field in source_rows),
        key=lambda source_by_field: source_by_field["raw_source_sha256"],
    )
    if not canonical_sources or len(canonical_sources) > _MAX_SOURCES:
        raise ValueError("invalid price exclusion source count is invalid")
    source_hashes = [source_by_field["raw_source_sha256"] for source_by_field in canonical_sources]
    if len(source_hashes) != len(set(source_hashes)):
        raise ValueError("invalid price exclusion source identities are ambiguous")
    for source_by_name in canonical_sources:
        _source_expectation(source_by_name)
    excluded_price_count = sum(len(source_by_field["entries"]) for source_by_field in canonical_sources)
    emptied_rate_count = sum(source_by_field["emptied_rate_count"] for source_by_field in canonical_sources)
    return {
        "contract": PTG2_INVALID_PRICE_EXCLUSION_CONTRACT,
        "reason": PTG2_INVALID_PRICE_EXCLUSION_REASON,
        "excluded_price_count": excluded_price_count,
        "emptied_rate_count": emptied_rate_count,
        "source_count": len(canonical_sources),
        "sources": canonical_sources,
        "sha256": _policy_digest(canonical_sources),
    }


def validate_invalid_price_exclusion_policy(raw_policy: Any) -> dict[str, Any]:
    """Validate one private policy and return its canonical representation."""

    if not isinstance(raw_policy, Mapping) or set(raw_policy) != _POLICY_FIELDS:
        raise ValueError("invalid price exclusion policy fields are incompatible")
    if (
        raw_policy.get("contract") != PTG2_INVALID_PRICE_EXCLUSION_CONTRACT
        or raw_policy.get("reason") != PTG2_INVALID_PRICE_EXCLUSION_REASON
        or not isinstance(raw_policy.get("sources"), list)
    ):
        raise ValueError("invalid price exclusion policy contract is incompatible")
    canonical = invalid_price_exclusion_policy(raw_policy["sources"])
    if dict(raw_policy) != canonical:
        raise ValueError("invalid price exclusion policy digest or counts do not match")
    return canonical


def validated_candidate_invalid_price_exclusion_policy(
    raw_policy: Any,
    frozen_binding: Mapping[str, Any] | None,
    raw_source_sha256: Iterable[str],
) -> dict[str, Any] | None:
    """Bind one run policy to its frozen or singleton physical sources."""

    binding_policy = (
        frozen_binding.get(INVALID_PRICE_EXCLUSION_POLICY_FIELD)
        if frozen_binding is not None
        else None
    )
    if frozen_binding is not None and raw_policy != binding_policy:
        raise ValueError(
            "candidate invalid price exclusion binding changed"
        )
    if raw_policy is None:
        return None
    policy = validate_invalid_price_exclusion_policy(raw_policy)
    if frozen_binding is None:
        physical_source_values = tuple(
            _sha256(raw_source, "raw_source_sha256")
            for raw_source in raw_source_sha256
        )
        policy_sources = {
            source_by_field["raw_source_sha256"]
            for source_by_field in policy["sources"]
        }
        if (
            len(physical_source_values) != 1
            or policy_sources != set(physical_source_values)
        ):
            raise ValueError(
                "candidate singleton invalid price exclusion source changed"
            )
    return policy


def invalid_price_exclusion_source_expectation(
    raw_policy: Any,
    raw_source_sha256: str,
) -> dict[str, Any] | None:
    """Return the exact private scanner expectation for one frozen source."""

    canonical = validate_invalid_price_exclusion_policy(raw_policy)
    source_sha256 = _sha256(raw_source_sha256, "raw_source_sha256")
    source_by_field = next(
        (
            candidate_source_by_field
            for candidate_source_by_field in canonical["sources"]
            if candidate_source_by_field["raw_source_sha256"] == source_sha256
        ),
        None,
    )
    if source_by_field is None:
        return None
    return _source_expectation(source_by_field)


def invalid_price_exclusion_source_evidence(
    raw_expectation: Any,
) -> dict[str, Any]:
    """Return redacted evidence expected from one scanner process."""

    if not isinstance(raw_expectation, Mapping):
        raise ValueError("invalid price exclusion source expectation is invalid")
    policy = invalid_price_exclusion_policy(
        [{field_name: raw_expectation.get(field_name) for field_name in _SOURCE_FIELDS}]
    )
    expectation = invalid_price_exclusion_source_expectation(
        policy,
        str(raw_expectation.get("raw_source_sha256") or ""),
    )
    if expectation != dict(raw_expectation):
        raise ValueError("invalid price exclusion source expectation does not match")
    return {
        "contract": PTG2_INVALID_PRICE_EXCLUSION_SOURCE_CONTRACT,
        "reason": PTG2_INVALID_PRICE_EXCLUSION_REASON,
        "excluded_price_count": expectation["excluded_price_count"],
        "emptied_rate_count": expectation["emptied_rate_count"],
        "sha256": expectation["sha256"],
    }


def validate_invalid_price_exclusion_source_evidence(
    raw_evidence: Any,
) -> dict[str, Any]:
    """Validate the bounded source evidence emitted by the Rust scanner."""

    if not isinstance(raw_evidence, Mapping) or set(raw_evidence) != (_SOURCE_EVIDENCE_FIELDS):
        raise ValueError("invalid price exclusion source evidence is incompatible")
    evidence_by_name = dict(raw_evidence)
    if (
        evidence_by_name.get("contract") != PTG2_INVALID_PRICE_EXCLUSION_SOURCE_CONTRACT
        or evidence_by_name.get("reason") != PTG2_INVALID_PRICE_EXCLUSION_REASON
    ):
        raise ValueError("invalid price exclusion source evidence is incompatible")
    excluded_count = _count(evidence_by_name.get("excluded_price_count"), "excluded_price_count")
    emptied_count = _count(evidence_by_name.get("emptied_rate_count"), "emptied_rate_count")
    if excluded_count == 0 or emptied_count > excluded_count:
        raise ValueError("invalid price exclusion source evidence counts are invalid")
    evidence_by_name["sha256"] = _sha256(evidence_by_name.get("sha256"), "source sha256")
    return evidence_by_name


def invalid_price_exclusion_evidence(raw_policy: Any) -> dict[str, Any]:
    """Return report-safe policy evidence without private source identities."""

    canonical = validate_invalid_price_exclusion_policy(raw_policy)
    return {field_name: canonical[field_name] for field_name in sorted(_EVIDENCE_FIELDS)}


def validate_invalid_price_exclusion_evidence(raw_evidence: Any) -> dict[str, Any]:
    """Validate report-safe aggregate evidence without private source identities."""

    if not isinstance(raw_evidence, Mapping) or set(raw_evidence) != _EVIDENCE_FIELDS:
        raise ValueError("invalid price exclusion evidence is incompatible")
    evidence_by_name = dict(raw_evidence)
    if (
        evidence_by_name.get("contract") != PTG2_INVALID_PRICE_EXCLUSION_CONTRACT
        or evidence_by_name.get("reason") != PTG2_INVALID_PRICE_EXCLUSION_REASON
    ):
        raise ValueError("invalid price exclusion evidence is incompatible")
    excluded_count = _count(evidence_by_name.get("excluded_price_count"), "excluded_price_count")
    emptied_count = _count(evidence_by_name.get("emptied_rate_count"), "emptied_rate_count")
    source_count = _count(evidence_by_name.get("source_count"), "source_count")
    if excluded_count == 0 or source_count == 0 or emptied_count > excluded_count:
        raise ValueError("invalid price exclusion evidence counts are invalid")
    evidence_by_name["sha256"] = _sha256(evidence_by_name.get("sha256"), "policy sha256")
    return evidence_by_name


def validate_candidate_invalid_price_exclusion_evidence(
    raw_policy: Any,
    snapshot_evidence: Any,
    layout_evidence: Any,
    raw_source_sha256: Iterable[str],
) -> dict[str, Any] | None:
    """Bind sealed public evidence to the exact private source policy."""

    if raw_policy is None:
        if snapshot_evidence is not None or layout_evidence is not None:
            raise ValueError("candidate invalid price exclusion has no exact policy")
        return None
    policy = validate_invalid_price_exclusion_policy(raw_policy)
    candidate_sources = {_sha256(raw_source, "raw_source_sha256") for raw_source in raw_source_sha256}
    policy_sources = {source_by_field["raw_source_sha256"] for source_by_field in policy["sources"]}
    if not policy_sources <= candidate_sources:
        raise ValueError("candidate invalid price exclusion contains an unbound source")
    expected = invalid_price_exclusion_evidence(policy)
    try:
        snapshot = validate_invalid_price_exclusion_evidence(snapshot_evidence)
        layout = validate_invalid_price_exclusion_evidence(layout_evidence)
    except ValueError as exc:
        raise ValueError("candidate invalid price exclusion evidence is incompatible") from exc
    if snapshot != expected or layout != expected:
        raise ValueError("candidate invalid price exclusion changed after layout sealing")
    return expected


__all__ = [
    "INVALID_PRICE_EXCLUSION_POLICY_FIELD",
    "PTG2_INVALID_PRICE_EXCLUSION_CONTRACT",
    "PTG2_INVALID_PRICE_EXCLUSION_REASON",
    "PTG2_INVALID_PRICE_EXCLUSION_SOURCE_MAX_JSON_BYTES",
    "PTG2_INVALID_PRICE_EXCLUSION_SOURCE_CONTRACT",
    "invalid_price_exclusion_evidence",
    "invalid_price_exclusion_policy",
    "invalid_price_exclusion_source",
    "invalid_price_exclusion_source_evidence",
    "invalid_price_exclusion_source_expectation",
    "invalid_price_value_sha256",
    "validate_invalid_price_exclusion_policy",
    "validated_candidate_invalid_price_exclusion_policy",
    "validate_candidate_invalid_price_exclusion_evidence",
    "validate_invalid_price_exclusion_evidence",
    "validate_invalid_price_exclusion_source_evidence",
]
