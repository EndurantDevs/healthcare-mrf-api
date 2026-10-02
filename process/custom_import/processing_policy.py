# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed processing limits retained inside an immutable source binding."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from process.custom_import.segmented_capture_policy import (
    SegmentedCapturePolicy,
    SegmentedCapturePolicyError,
    _exact_mapping,
    _positive_integer,
)

_BUILD_MAXIMUMS = {
    "page_row_limit": 256,
    "page_byte_limit": 268_435_456,
    "statement_timeout_ms": 2_147_483_647,
    "lease_seconds": 3_600,
    "build_deadline_seconds": 86_400,
}


@dataclass(frozen=True)
class BuildPolicy:
    """Explicit build limits; an execution must separately establish authority."""

    page_row_limit: int
    page_byte_limit: int
    statement_timeout_ms: int
    lease_seconds: int
    build_deadline_seconds: int

    def __post_init__(self) -> None:
        for name, maximum in _BUILD_MAXIMUMS.items():
            _positive_integer(getattr(self, name), f"build.{name}", maximum)

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> BuildPolicy:
        """Parse all build fields without supplying defaults or ignoring extras."""

        document = _exact_mapping(mapping, frozenset(_BUILD_MAXIMUMS), "build policy")
        return cls(**document)

    def to_mapping(self) -> dict[str, int]:
        """Return an independent wire mapping of the immutable limits."""

        return asdict(self)


@dataclass(frozen=True)
class ProcessingPolicy:
    """One complete operator declaration, with no separate digest or admission."""

    capture: SegmentedCapturePolicy
    driver_timeout_seconds: int
    build: BuildPolicy

    def __post_init__(self) -> None:
        if not isinstance(self.capture, SegmentedCapturePolicy) or not isinstance(self.build, BuildPolicy):
            raise SegmentedCapturePolicyError("processing policy requires capture and build declarations")
        object.__setattr__(self, "capture", SegmentedCapturePolicy.from_mapping(self.capture.to_mapping()))
        object.__setattr__(self, "build", BuildPolicy.from_mapping(self.build.to_mapping()))
        _positive_integer(self.driver_timeout_seconds, "driver_timeout_seconds", 120)

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> ProcessingPolicy:
        """Parse a closed declaration using the existing capture-policy checks."""

        document = _exact_mapping(
            mapping, frozenset({"capture", "driver_timeout_seconds", "build"}), "processing policy"
        )
        return cls(
            capture=SegmentedCapturePolicy.from_mapping(document["capture"]),
            driver_timeout_seconds=document["driver_timeout_seconds"],
            build=BuildPolicy.from_mapping(document["build"]),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Return fresh nested mappings; the binding owns canonicalization and hashing."""

        return {
            "capture": self.capture.to_mapping(),
            "driver_timeout_seconds": self.driver_timeout_seconds,
            "build": self.build.to_mapping(),
        }
