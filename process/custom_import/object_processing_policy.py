# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed object-source processing limits retained in an immutable binding."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from types import MappingProxyType
from typing import Any

from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import canonical_json
from process.custom_import.prepared_source_codec import prepared_source_row_byte_limit
from process.custom_import.processing_policy import BuildPolicy
from process.custom_import.segmented_capture_policy import _exact_mapping, _positive_integer

OBJECT_PROCESSING_POLICY_CONTRACT = "custom-import/object-processing-policy/v1"
_MAX_BIGINT = (1 << 63) - 1
_CAPTURE_MAXIMUMS = asdict(CaptureLimits())
_PREPARED_MAXIMUMS = {
    "part_byte_limit": 64 * 1024 * 1024,
    "part_row_limit": 1_000_000,
    "maximum_parts": 131_072,
    "maximum_total_bytes": _MAX_BIGINT,
}
_POLICY_KEYS = frozenset(
    {"contract", "capture_limits", "acquisition_timeout_seconds", "maximum_dataset_retained_bytes", "prepared", "build"}
)
_ERROR = "object processing policy is invalid"


class ObjectProcessingPolicyError(ValueError):
    """An object policy is incomplete, incoherent or exceeds a safety ceiling."""


def _capture_limits(value: object) -> CaptureLimits:
    document = _exact_mapping(value, frozenset(_CAPTURE_MAXIMUMS), "capture_limits")
    return CaptureLimits(
        **{
            name: _positive_integer(document[name], f"capture_limits.{name}", maximum)
            for name, maximum in _CAPTURE_MAXIMUMS.items()
        }
    )


def _prepared_limits(value: object, limits: CaptureLimits) -> Mapping[str, int]:
    document = _exact_mapping(value, frozenset(_PREPARED_MAXIMUMS), "prepared")
    checked_by_name = {
        name: _positive_integer(document[name], f"prepared.{name}", maximum)
        for name, maximum in _PREPARED_MAXIMUMS.items()
    }
    part_byte_limit = checked_by_name["part_byte_limit"]
    if checked_by_name["maximum_total_bytes"] < part_byte_limit or part_byte_limit < prepared_source_row_byte_limit(
        limits
    ):
        raise ObjectProcessingPolicyError(_ERROR)
    return MappingProxyType(checked_by_name)


@dataclass(frozen=True)
class ObjectProcessingPolicy:
    """One explicit declaration; admission and authority remain separate checks."""

    capture_limits: CaptureLimits
    acquisition_timeout_seconds: int
    maximum_dataset_retained_bytes: int
    prepared: Mapping[str, int]
    build: BuildPolicy
    canonical: str = field(init=False, repr=False)
    digest: str = field(init=False)

    def __post_init__(self) -> None:
        try:
            if not isinstance(self.capture_limits, CaptureLimits) or not isinstance(self.build, BuildPolicy):
                raise ObjectProcessingPolicyError(_ERROR)
            object.__setattr__(self, "capture_limits", _capture_limits(asdict(self.capture_limits)))
            object.__setattr__(self, "prepared", _prepared_limits(self.prepared, self.capture_limits))
            object.__setattr__(self, "build", BuildPolicy.from_mapping(self.build.to_mapping()))
            _positive_integer(self.acquisition_timeout_seconds, "acquisition_timeout_seconds", 3_600)
            retained = self.maximum_dataset_retained_bytes
            if isinstance(retained, bool) or not isinstance(retained, int) or not 0 <= retained <= _MAX_BIGINT:
                raise ObjectProcessingPolicyError(_ERROR)
        except ValueError:
            raise ObjectProcessingPolicyError(_ERROR) from None
        canonical = canonical_json(self.to_mapping())
        object.__setattr__(self, "canonical", canonical)
        object.__setattr__(
            self,
            "digest",
            hashlib.sha256(
                OBJECT_PROCESSING_POLICY_CONTRACT.encode("ascii") + b":" + canonical.encode("utf-8")
            ).hexdigest(),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ObjectProcessingPolicy:
        """Require every declared field without defaults or ignored extensions."""

        try:
            document = _exact_mapping(value, _POLICY_KEYS, "object processing policy")
            if type(document["contract"]) is not str or document["contract"] != OBJECT_PROCESSING_POLICY_CONTRACT:
                raise ObjectProcessingPolicyError(_ERROR)
            return cls(
                capture_limits=_capture_limits(document["capture_limits"]),
                acquisition_timeout_seconds=document["acquisition_timeout_seconds"],
                maximum_dataset_retained_bytes=document["maximum_dataset_retained_bytes"],
                prepared=document["prepared"],
                build=BuildPolicy.from_mapping(document["build"]),
            )
        except ValueError:
            raise ObjectProcessingPolicyError(_ERROR) from None

    def to_mapping(self) -> dict[str, Any]:
        """Return independent nested mappings for the canonical wire declaration."""

        return {
            "contract": OBJECT_PROCESSING_POLICY_CONTRACT,
            "capture_limits": asdict(self.capture_limits),
            "acquisition_timeout_seconds": self.acquisition_timeout_seconds,
            "maximum_dataset_retained_bytes": self.maximum_dataset_retained_bytes,
            "prepared": dict(self.prepared),
            "build": self.build.to_mapping(),
        }
