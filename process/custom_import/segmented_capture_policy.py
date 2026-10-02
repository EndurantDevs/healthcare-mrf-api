# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable declared limits for segmented capture, without admission authority."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import canonical_json

SEGMENTED_CAPTURE_POLICY_CONTRACT = "custom-import/segmented-capture-policy/v1"
_MAX_BIGINT = (1 << 63) - 1
# Metadata seal safety ceiling; actual admitted part counts are chosen elsewhere.
_MAX_PARTS = 131_072
_PART_LIMIT_MAXIMUMS = {
    "maximum_compressed_bytes": 64 * 1024 * 1024,
    "maximum_decoded_bytes": 256 * 1024 * 1024,
    "maximum_record_bytes": 1024 * 1024,
    "maximum_records": 1_000_000,
    "maximum_fields_per_record": 1_024,
    "read_chunk_bytes": 64 * 1024 * 1024,
}
_BUDGET_KEYS = frozenset(
    {
        "maximum_parts",
        "maximum_compressed_bytes",
        "maximum_decoded_bytes",
        "maximum_arrow_bytes",
        "maximum_records",
        "maximum_manifest_bytes",
    }
)
_SCALAR_MAXIMUMS = {
    "maximum_part_arrow_bytes": 256 * 1024 * 1024,
    "maximum_part_manifest_bytes": 2 * 1024 * 1024,
    "maximum_dataset_retained_bytes": _MAX_BIGINT,
    "acquisition_deadline_seconds": 86_400,
}
_POLICY_KEYS = frozenset({"contract", "part_limits", "stream_budget", "bundle_budget", *_SCALAR_MAXIMUMS})


class SegmentedCapturePolicyError(ValueError):
    """A declared segmented policy is incomplete, incoherent or exceeds a ceiling."""


def _positive_integer(value: object, label: str, maximum: int = _MAX_BIGINT) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise SegmentedCapturePolicyError(f"{label} must be a positive integer at most {maximum}")
    return value


def _exact_mapping(value: object, keys: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise SegmentedCapturePolicyError(f"{label} must contain exactly the required fields")
    return dict(value)


def _part_limits(value: object) -> CaptureLimits:
    document = _exact_mapping(value, frozenset(_PART_LIMIT_MAXIMUMS), "part_limits")
    limits_by_name = {
        key: _positive_integer(document[key], f"part_limits.{key}", maximum)
        for key, maximum in _PART_LIMIT_MAXIMUMS.items()
    }
    try:
        return CaptureLimits(**limits_by_name)
    except ValueError as exc:
        raise SegmentedCapturePolicyError("part_limits are incoherent") from exc


def _budget(value: object, label: str) -> Mapping[str, int]:
    document = _exact_mapping(value, _BUDGET_KEYS, label)
    return MappingProxyType(
        {
            key: _positive_integer(
                document[key], f"{label}.{key}", _MAX_PARTS if key == "maximum_parts" else _MAX_BIGINT
            )
            for key in _BUDGET_KEYS
        }
    )


@dataclass(frozen=True)
class SegmentedCapturePolicy:
    """A sealed declaration of limits; callers must independently authorize admission."""

    part_limits: CaptureLimits
    stream_budget: Mapping[str, int]
    bundle_budget: Mapping[str, int]
    maximum_part_arrow_bytes: int
    maximum_part_manifest_bytes: int
    maximum_dataset_retained_bytes: int
    acquisition_deadline_seconds: int
    canonical: str = field(init=False, repr=False)
    digest: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.part_limits, CaptureLimits):
            raise SegmentedCapturePolicyError("part_limits must use CaptureLimits")
        part_limits = _part_limits({key: getattr(self.part_limits, key) for key in _PART_LIMIT_MAXIMUMS})
        object.__setattr__(self, "part_limits", part_limits)
        object.__setattr__(self, "stream_budget", _budget(self.stream_budget, "stream_budget"))
        object.__setattr__(self, "bundle_budget", _budget(self.bundle_budget, "bundle_budget"))
        for name, maximum in _SCALAR_MAXIMUMS.items():
            _positive_integer(getattr(self, name), name, maximum)
        self._validate_budgets()
        canonical = canonical_json(self.to_mapping())
        object.__setattr__(self, "canonical", canonical)
        object.__setattr__(
            self,
            "digest",
            hashlib.sha256(
                SEGMENTED_CAPTURE_POLICY_CONTRACT.encode("ascii") + b":" + canonical.encode("utf-8")
            ).hexdigest(),
        )

    def _validate_budgets(self) -> None:
        part_minimum_by_budget = {
            "maximum_parts": 1,
            "maximum_compressed_bytes": self.part_limits.maximum_compressed_bytes,
            "maximum_decoded_bytes": self.part_limits.maximum_decoded_bytes,
            "maximum_arrow_bytes": self.maximum_part_arrow_bytes,
            "maximum_records": self.part_limits.maximum_records,
            "maximum_manifest_bytes": self.maximum_part_manifest_bytes,
        }
        for key, minimum in part_minimum_by_budget.items():
            if self.stream_budget[key] < minimum:
                raise SegmentedCapturePolicyError(f"stream_budget.{key} does not cover one maximum part")
            if self.bundle_budget[key] < self.stream_budget[key]:
                raise SegmentedCapturePolicyError(f"bundle_budget.{key} does not cover stream_budget")
        retained_bytes = self.bundle_budget["maximum_compressed_bytes"] + self.bundle_budget["maximum_manifest_bytes"]
        if retained_bytes > _MAX_BIGINT or self.maximum_dataset_retained_bytes < retained_bytes:
            raise SegmentedCapturePolicyError(
                "maximum_dataset_retained_bytes does not cover the bundle without overflow"
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SegmentedCapturePolicy:
        """Parse a closed, fully explicit declaration without choosing admission values."""

        document = _exact_mapping(value, _POLICY_KEYS, "segmented capture policy")
        if not isinstance(document["contract"], str) or document["contract"] != SEGMENTED_CAPTURE_POLICY_CONTRACT:
            raise SegmentedCapturePolicyError("segmented capture policy contract is unsupported")
        return cls(
            part_limits=_part_limits(document["part_limits"]),
            stream_budget=document["stream_budget"],
            bundle_budget=document["bundle_budget"],
            **{name: document[name] for name in _SCALAR_MAXIMUMS},
        )

    def to_mapping(self) -> dict[str, Any]:
        """Return an independent mutable wire document for this frozen declaration."""

        return {
            "contract": SEGMENTED_CAPTURE_POLICY_CONTRACT,
            "part_limits": {key: getattr(self.part_limits, key) for key in _PART_LIMIT_MAXIMUMS},
            "stream_budget": dict(self.stream_budget),
            "bundle_budget": dict(self.bundle_budget),
            **{name: getattr(self, name) for name in _SCALAR_MAXIMUMS},
        }
