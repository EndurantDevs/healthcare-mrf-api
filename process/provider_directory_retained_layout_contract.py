# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Artifact observations and immutable retained layout contracts."""

from __future__ import annotations

import datetime as dt
import copy
import weakref
from dataclasses import dataclass, field
from typing import Any

from process.provider_directory_retained_artifact_base import (
    ATOMIC_CATALOG_OBJECT,
    CATALOG_OBJECT,
    PRODUCER_PROOF,
    PRODUCER_VERIFIED,
    RANGED_STRONG_VALIDATOR,
    RETAINED_LAYOUT_MANIFEST_ID,
    STRONG_ETAG,
    RetainedArtifactError,
    require_digest,
    require_nonnegative_int,
    require_positive_int,
    require_safe_id,
    sha256_json,
)
from process.provider_directory_retained_campaign_contract import is_strong_etag


MAX_PRODUCED_ARTIFACT_RANGES = 8192


class _PrivateValidatorSnapshot:
    __slots__ = ()

    def __repr__(self) -> str:
        return "<private-validator-redacted>"

    __str__ = __repr__

    def __copy__(self) -> "_PrivateValidatorSnapshot":
        return self

    def __deepcopy__(self, _memo: dict[int, Any]) -> "_PrivateValidatorSnapshot":
        return self


class _LivePrivateValidator:
    __slots__ = ("__owner", "__value")

    def __init__(self, owner: "ArtifactObservation", value: str):
        object.__setattr__(self, "_LivePrivateValidator__owner", weakref.ref(owner))
        object.__setattr__(self, "_LivePrivateValidator__value", value)

    def _open_for_owner(self, owner: "ArtifactObservation") -> str:
        if self.__owner() is not owner or not is_strong_etag(self.__value):
            raise RetainedArtifactError("strong_validator_required")
        return self.__value

    def __repr__(self) -> str:
        return "<private-validator>"

    __str__ = __repr__

    def __copy__(self) -> _PrivateValidatorSnapshot:
        return _PrivateValidatorSnapshot()

    def __deepcopy__(self, _memo: dict[int, Any]) -> _PrivateValidatorSnapshot:
        return _PrivateValidatorSnapshot()

    def __reduce_ex__(self, _protocol: int):
        return (_PrivateValidatorSnapshot, ())


def _open_private_validator(observation: "ArtifactObservation") -> str:
    validator = observation.validator
    if type(validator) is not _LivePrivateValidator:
        raise RetainedArtifactError("private_validator_unavailable")
    return validator._open_for_owner(observation)


@dataclass(frozen=True)
class ArtifactObservation:
    """Exact length and immutable transport identity for one payload item."""

    mode: str
    byte_count: int
    validator_kind: str
    validator: _LivePrivateValidator | _PrivateValidatorSnapshot | str | None = field(
        repr=False,
        compare=False,
    )
    immutable_identity_sha256: str
    request_interval_ms: int = 0
    retry_not_before: dt.datetime | None = None

    def __post_init__(self) -> None:
        if type(self.validator) is str:
            object.__setattr__(
                self,
                "validator",
                _LivePrivateValidator(self, self.validator),
            )
        elif isinstance(self.validator, _LivePrivateValidator):
            object.__setattr__(self, "validator", _PrivateValidatorSnapshot())

    def _inert_copy(self) -> "ArtifactObservation":
        validator = None if self.validator is None else _PrivateValidatorSnapshot()
        return ArtifactObservation(
            mode=self.mode,
            byte_count=self.byte_count,
            validator_kind=self.validator_kind,
            validator=validator,
            immutable_identity_sha256=self.immutable_identity_sha256,
            request_interval_ms=self.request_interval_ms,
            retry_not_before=self.retry_not_before,
        )

    def __copy__(self) -> "ArtifactObservation":
        return self._inert_copy()

    def __deepcopy__(self, memo: dict[int, Any]) -> "ArtifactObservation":
        copied_observation = self._inert_copy()
        memo[id(self)] = copied_observation
        return copied_observation

    def validate(self) -> "ArtifactObservation":
        """Validate the immutable acquisition mode, length, and validator."""

        require_positive_int(self.byte_count, "observed_byte_count")
        require_digest(self.immutable_identity_sha256, "immutable_identity_sha256")
        require_nonnegative_int(self.request_interval_ms, "request_interval_ms")
        if self.request_interval_ms > 24 * 60 * 60 * 1000:
            raise RetainedArtifactError("request_interval_invalid")
        if self.retry_not_before is not None and (
            not isinstance(self.retry_not_before, dt.datetime)
            or self.retry_not_before.tzinfo is None
            or self.retry_not_before.utcoffset() is None
        ):
            raise RetainedArtifactError("retry_not_before_invalid")
        if self.mode == RANGED_STRONG_VALIDATOR:
            if self.validator_kind != STRONG_ETAG:
                raise RetainedArtifactError("strong_validator_required")
            _open_private_validator(self)
        elif self.mode == ATOMIC_CATALOG_OBJECT:
            if self.validator_kind != CATALOG_OBJECT or self.validator is not None:
                raise RetainedArtifactError("catalog_object_proof_invalid")
        elif self.mode == PRODUCER_VERIFIED:
            if self.validator_kind != PRODUCER_PROOF or self.validator is not None:
                raise RetainedArtifactError("producer_proof_invalid")
        else:
            raise RetainedArtifactError("acquisition_mode_invalid")
        return self


@dataclass(frozen=True)
class LeaseIdentity:
    """Fencing identity returned by a generic campaign/item lease."""

    owner: str
    epoch: int

    def validate(self) -> "LeaseIdentity":
        """Validate the owner and positive fencing epoch."""

        require_safe_id(self.owner, "lease_owner")
        require_positive_int(self.epoch, "lease_epoch")
        return self


@dataclass(frozen=True)
class ArtifactLayoutRange:
    range_ordinal: int
    raw_byte_start: int
    raw_byte_end: int
    raw_byte_count: int
    raw_sha256: str
    record_start: int
    record_end: int
    record_count: int
    canonical_sha256: str
    canonical_byte_count: int


def _validated_range_payload(
    layout_range: ArtifactLayoutRange,
    expected_ordinal: int,
) -> tuple[Any, ...]:
    if type(layout_range) is not ArtifactLayoutRange:
        raise RetainedArtifactError("artifact_layout_range_invalid")
    range_values = (
        layout_range.range_ordinal,
        layout_range.raw_byte_start,
        layout_range.raw_byte_end,
        layout_range.raw_byte_count,
        layout_range.raw_sha256,
        layout_range.record_start,
        layout_range.record_end,
        layout_range.record_count,
        layout_range.canonical_sha256,
        layout_range.canonical_byte_count,
    )
    range_ordinal, raw_start, raw_end, raw_count = range_values[:4]
    record_start, record_end, record_count = range_values[5:8]
    require_nonnegative_int(range_ordinal, "range_ordinal")
    require_nonnegative_int(raw_start, "raw_byte_start")
    require_positive_int(raw_end, "raw_byte_end")
    require_positive_int(raw_count, "raw_byte_count")
    require_digest(range_values[4], "raw_sha256")
    require_nonnegative_int(record_start, "record_start")
    require_nonnegative_int(record_end, "record_end")
    require_nonnegative_int(record_count, "record_count")
    require_digest(range_values[8], "canonical_sha256")
    require_positive_int(range_values[9], "canonical_byte_count")
    if (
        range_ordinal != expected_ordinal
        or raw_end <= raw_start
        or raw_count != raw_end - raw_start
        or record_end < record_start
        or record_count != record_end - record_start
    ):
        raise RetainedArtifactError("artifact_layout_range_invalid")
    return range_values


def _range_payload_mapping(range_payload: tuple[Any, ...]) -> dict[str, Any]:
    return dict(
        zip(
            (
                "range_ordinal",
                "raw_byte_start",
                "raw_byte_end",
                "raw_byte_count",
                "raw_sha256",
                "record_start",
                "record_end",
                "record_count",
                "canonical_sha256",
                "canonical_byte_count",
            ),
            range_payload,
        )
    )


@dataclass(frozen=True)
class ProducedArtifact:
    """One producer-verified physical object ready for generic registration."""

    artifact_sha256: str
    artifact_kind: str
    artifact_byte_count: int
    artifact_record_count: int
    artifact_path: str
    layout_contract_id: str
    layout_contract_version: int
    range_set_sha256: str
    canonical_byte_count: int
    manifest_sha256: str
    manifest_byte_count: int
    manifest_path: str
    producer_build_id: str
    ranges: tuple[ArtifactLayoutRange, ...]
    _range_payloads: tuple[tuple[Any, ...], ...] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if (
            type(self.ranges) is not tuple
            or not self.ranges
            or len(self.ranges) > MAX_PRODUCED_ARTIFACT_RANGES
        ):
            raise RetainedArtifactError("artifact_ranges_invalid")
        object.__setattr__(
            self,
            "_range_payloads",
            tuple(
                _validated_range_payload(layout_range, range_ordinal)
                for range_ordinal, layout_range in enumerate(self.ranges)
            ),
        )


def expected_range_set_digest(produced: ProducedArtifact) -> str:
    """Recompute the generic range-set identity from every ordered proof."""

    return sha256_json(
        {
            "artifact_sha256": produced.artifact_sha256,
            "artifact_byte_count": produced.artifact_byte_count,
            "artifact_record_count": produced.artifact_record_count,
            "layout_contract_id": produced.layout_contract_id,
            "layout_contract_version": produced.layout_contract_version,
            "ranges": [
                _range_payload_mapping(range_payload)
                for range_payload in produced._range_payloads
            ],
        }
    )


def produced_manifest_payload(produced: ProducedArtifact) -> dict[str, Any]:
    """Return the exact semantic manifest bound during generic admission."""

    return {
        "manifest_contract_id": RETAINED_LAYOUT_MANIFEST_ID,
        "artifact": {
            "sha256": produced.artifact_sha256,
            "kind": produced.artifact_kind,
            "byte_count": produced.artifact_byte_count,
            "record_count": produced.artifact_record_count,
        },
        "layout": {
            "contract_id": produced.layout_contract_id,
            "contract_version": produced.layout_contract_version,
            "range_count": len(produced._range_payloads),
            "range_set_sha256": produced.range_set_sha256,
            "canonical_byte_count": produced.canonical_byte_count,
            "producer_build_id": produced.producer_build_id,
        },
        "ranges": [
            _range_payload_mapping(range_payload)
            for range_payload in produced._range_payloads
        ],
    }


def produced_layout_digest(produced: ProducedArtifact) -> str:
    """Identify one immutable layout independently from the physical object."""

    return sha256_json(
        {
            "artifact_sha256": produced.artifact_sha256,
            "artifact_record_count": produced.artifact_record_count,
            "layout_contract_id": produced.layout_contract_id,
            "layout_contract_version": produced.layout_contract_version,
            "layout_range_count": len(produced._range_payloads),
            "range_set_sha256": produced.range_set_sha256,
            "canonical_byte_count": produced.canonical_byte_count,
            "manifest_sha256": produced.manifest_sha256,
            "manifest_byte_count": produced.manifest_byte_count,
            "producer_build_id": produced.producer_build_id,
        }
    )
