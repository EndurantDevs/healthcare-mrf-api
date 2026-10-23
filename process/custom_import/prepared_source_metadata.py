# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure canonical metadata for bounded prepared replay parts and summaries."""

from __future__ import annotations

import hashlib
import re
import struct
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import NamedTuple

from process.custom_import.definition import canonical_json
from process.custom_import.object_processing_policy import ObjectProcessingPolicy
from process.custom_import.prepared_source_codec import PREPARED_SOURCE_CONTRACT

PREPARED_PART_CONTRACT = "custom-import/prepared-source-part/v1"
_PART_SET_DOMAIN = b"custom-import/prepared-source-part-set/v1\0"
_MAX_BIGINT = (1 << 63) - 1
_MAX_PARTS = 131_072
_MAX_PART_BYTES = 64 * 1024 * 1024
_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_EMPTY_SHA256 = hashlib.sha256(b"").digest()
_IDENTITY_IDS = (
    "execution_id",
    "capture_bundle_id",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "source_binding_revision_id",
)
_IDENTITY_HASHES = (
    "source_binding_sha256",
    "request_identity_sha256",
    "raw_manifest_sha256",
    "processing_policy_sha256",
)
_ERROR = "prepared_source_metadata_invalid"


class PreparedSourceMetadataError(ValueError):
    """Prepared metadata has invalid scalar bounds, framing or identity."""


def _integer(value: object, minimum: int = 0, maximum: int = _MAX_BIGINT) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise PreparedSourceMetadataError(_ERROR)


def _sha256(value: object) -> None:
    if type(value) is not bytes or len(value) != 32:
        raise PreparedSourceMetadataError(_ERROR)


@dataclass(frozen=True)
class PreparedPartMetadata:
    """One typed part manifest, independent of payload reads and current leases."""

    stream_slot: int
    part_ordinal: int
    first_source_ordinal: int
    record_count: int
    byte_count: int
    payload_sha256: bytes
    canonical: str = field(init=False, repr=False)
    digest: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _integer(self.stream_slot, 1, 9)
        _integer(self.part_ordinal, 1, _MAX_PARTS)
        _integer(self.first_source_ordinal, 1)
        _integer(self.record_count, 0, 1_000_000)
        _integer(self.byte_count, 0, _MAX_PART_BYTES)
        _sha256(self.payload_sha256)
        if self.record_count == 0:
            if (self.part_ordinal, self.first_source_ordinal, self.byte_count, self.payload_sha256) != (
                1,
                1,
                0,
                _EMPTY_SHA256,
            ):
                raise PreparedSourceMetadataError(_ERROR)
        elif self.byte_count == 0 or self.first_source_ordinal + self.record_count - 1 > _MAX_BIGINT:
            raise PreparedSourceMetadataError(_ERROR)
        canonical = canonical_json(self.to_mapping())
        object.__setattr__(self, "canonical", canonical)
        object.__setattr__(self, "digest", hashlib.sha256(canonical.encode("utf-8")).digest())

    def to_mapping(self) -> dict:
        """Rebuild the exact closed manifest from validated scalar metadata."""

        return {
            "contract": PREPARED_PART_CONTRACT,
            "stream_slot": self.stream_slot,
            "part_ordinal": self.part_ordinal,
            "first_source_ordinal": self.first_source_ordinal,
            "record_count": self.record_count,
            "byte_count": self.byte_count,
            "payload_sha256": self.payload_sha256.hex(),
        }

    def verify_manifest(self, canonical: str, digest: bytes) -> None:
        """Compare retained text and digest to the typed reconstruction, without parsing."""

        _sha256(digest)
        if type(canonical) is not str or canonical != self.canonical or digest != self.digest:
            raise PreparedSourceMetadataError(_ERROR)


@dataclass(frozen=True)
class PreparedStreamSummary:
    """Bounded scalar totals; use the part fold to establish their provenance."""

    stream_slot: int
    part_count: int
    record_count: int
    byte_count: int
    manifest_byte_count: int
    part_set_sha256: bytes

    def __post_init__(self) -> None:
        _integer(self.stream_slot, 1, 9)
        _integer(self.part_count, 1, _MAX_PARTS)
        _integer(self.record_count)
        _integer(self.byte_count)
        _integer(self.manifest_byte_count, 2 * self.part_count, _MAX_MANIFEST_BYTES * self.part_count)
        _sha256(self.part_set_sha256)
        if self.record_count == 0:
            if self.part_count != 1 or self.byte_count != 0:
                raise PreparedSourceMetadataError(_ERROR)
        elif self.byte_count == 0 or self.part_count > self.record_count:
            raise PreparedSourceMetadataError(_ERROR)

    def to_mapping(self) -> dict:
        """Return the exact stream-summary wire fields with hexadecimal hashes."""

        return {
            "stream_slot": self.stream_slot,
            "part_count": self.part_count,
            "record_count": self.record_count,
            "byte_count": self.byte_count,
            "manifest_byte_count": self.manifest_byte_count,
            "part_set_sha256": self.part_set_sha256.hex(),
        }


def _policy_limits(policy: ObjectProcessingPolicy) -> None:
    if type(policy) is not ObjectProcessingPolicy:
        raise PreparedSourceMetadataError(_ERROR)


def _summary_limits(part_count: int, record_count: int, byte_count: int, policy: ObjectProcessingPolicy) -> None:
    _integer(part_count, 1, policy.prepared["maximum_parts"])
    _integer(record_count, 0, policy.capture_limits.maximum_records)
    _integer(byte_count, 0, policy.prepared["maximum_total_bytes"])
    if (
        record_count > part_count * policy.prepared["part_row_limit"]
        or byte_count > part_count * policy.prepared["part_byte_limit"]
    ):
        raise PreparedSourceMetadataError(_ERROR)


class PreparedStreamFold:
    """Accumulate ordered metadata across pages without retaining prior parts."""

    def __init__(self, stream_slot: int, policy: ObjectProcessingPolicy) -> None:
        _integer(stream_slot, 1, 9)
        _policy_limits(policy)
        self._stream_slot = stream_slot
        self._policy = policy
        self._part_count = self._record_count = self._byte_count = self._manifest_byte_count = 0
        self._part_set = hashlib.sha256(_PART_SET_DOMAIN)

    def add(self, part: PreparedPartMetadata) -> None:
        """Validate one next part before advancing its counts and framed digest."""

        if type(part) is not PreparedPartMetadata or (
            part.stream_slot != self._stream_slot
            or part.part_ordinal != self._part_count + 1
            or part.first_source_ordinal != self._record_count + 1
            or (self._part_count > 0 and self._record_count == 0)
        ):
            raise PreparedSourceMetadataError(_ERROR)
        _integer(part.record_count, 0, self._policy.prepared["part_row_limit"])
        _integer(part.byte_count, 0, self._policy.prepared["part_byte_limit"])
        part_count = self._part_count + 1
        record_count = self._record_count + part.record_count
        byte_count = self._byte_count + part.byte_count
        encoded_manifest = part.canonical.encode("utf-8")
        _integer(len(encoded_manifest), 2, _MAX_MANIFEST_BYTES)
        manifest_byte_count = self._manifest_byte_count + len(encoded_manifest)
        _integer(manifest_byte_count)
        _summary_limits(part_count, record_count, byte_count, self._policy)
        self._part_set.update(struct.pack(">Q", len(encoded_manifest)))
        self._part_set.update(encoded_manifest)
        self._part_count, self._record_count = part_count, record_count
        self._byte_count, self._manifest_byte_count = byte_count, manifest_byte_count

    def finish(self) -> PreparedStreamSummary:
        """Return the validated fold; the caller proves complete original EOF."""

        return PreparedStreamSummary(
            self._stream_slot,
            self._part_count,
            self._record_count,
            self._byte_count,
            self._manifest_byte_count,
            self._part_set.digest(),
        )


def summarize_prepared_stream(
    parts: Iterable[PreparedPartMetadata], *, stream_slot: int, policy: ObjectProcessingPolicy
) -> PreparedStreamSummary:
    """Fold an iterable using the same bounded incremental validation and digest."""

    fold = PreparedStreamFold(stream_slot, policy)
    for part in parts:
        fold.add(part)
    return fold.finish()


@dataclass(frozen=True)
class PreparedSourceIdentity:
    """Retained source identity pins; current fence and token are deliberately absent."""

    execution_id: int
    capture_bundle_id: int
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    source_binding_revision_id: int
    source_binding_sha256: bytes
    request_identity_sha256: bytes
    raw_manifest_sha256: bytes
    processing_policy_sha256: bytes

    def __post_init__(self) -> None:
        for name in _IDENTITY_IDS:
            _integer(getattr(self, name), 1)
        for name in _IDENTITY_HASHES:
            _sha256(getattr(self, name))

    def to_mapping(self) -> dict:
        """Keep binding/policy and per-run content pins as distinct exact fields."""

        return {
            **{name: getattr(self, name) for name in _IDENTITY_IDS},
            **{name: getattr(self, name).hex() for name in _IDENTITY_HASHES},
        }


class PreparedSourceSummary(NamedTuple):
    canonical: str
    digest: bytes
    streams: tuple[PreparedStreamSummary, ...]


def prepared_source_summary(
    identity: PreparedSourceIdentity,
    stream_summaries: Iterable[PreparedStreamSummary],
    *,
    stream_count: int,
    policy: ObjectProcessingPolicy,
) -> PreparedSourceSummary:
    """Build a bounded complete summary; original-byte and retention checks stay separate."""

    _integer(stream_count, 1, 9)
    _policy_limits(policy)
    if type(identity) is not PreparedSourceIdentity or identity.processing_policy_sha256 != bytes.fromhex(
        policy.digest
    ):
        raise PreparedSourceMetadataError(_ERROR)
    summaries = []
    part_count = record_count = byte_count = manifest_byte_count = 0
    for stream in stream_summaries:
        if (
            type(stream) is not PreparedStreamSummary
            or stream.stream_slot != len(summaries) + 1
            or len(summaries) >= stream_count
        ):
            raise PreparedSourceMetadataError(_ERROR)
        part_count += stream.part_count
        record_count += stream.record_count
        byte_count += stream.byte_count
        manifest_byte_count += stream.manifest_byte_count
        _integer(manifest_byte_count)
        _summary_limits(part_count, record_count, byte_count, policy)
        summaries.append(stream)
    if len(summaries) != stream_count:
        raise PreparedSourceMetadataError(_ERROR)
    canonical = canonical_json(
        {
            "contract": PREPARED_SOURCE_CONTRACT,
            **identity.to_mapping(),
            "streams": [stream.to_mapping() for stream in summaries],
        }
    )
    encoded = canonical.encode("utf-8")
    _integer(len(encoded), 2, _MAX_MANIFEST_BYTES)
    digest = hashlib.sha256(PREPARED_SOURCE_CONTRACT.encode("ascii") + b":" + encoded).digest()
    return PreparedSourceSummary(canonical, digest, tuple(summaries))


def parse_sealed_summary(
    canonical_summary, prepared_sha256, identity, progress, policy
) -> tuple[PreparedStreamSummary, ...]:
    """Verify sealed identity and counters before decoding fixed-width part-set hashes."""

    if type(canonical_summary) is not str or not 2 <= len(canonical_summary) <= _MAX_MANIFEST_BYTES:
        raise PreparedSourceMetadataError(_ERROR)
    prefix = canonical_json({"contract": PREPARED_SOURCE_CONTRACT, **identity.to_mapping(), "streams": []})[:-2]
    patterns = []
    for stream in progress:
        fields_by_name = {
            name: getattr(stream, name)
            for name in ("stream_slot", "part_count", "record_count", "byte_count", "manifest_byte_count")
        }
        fields_by_name["part_set_sha256"] = ""
        patterns.append(
            re.escape(canonical_json(fields_by_name)).replace(
                re.escape('"part_set_sha256":""'), '"part_set_sha256":"([0-9a-f]{64})"'
            )
        )
    # A fixed identity/counter grammar rejects allocation abuse before decoding any field.
    matched = re.fullmatch(re.escape(prefix) + ",".join(patterns) + re.escape("]}"), canonical_summary)
    if matched is None:
        raise PreparedSourceMetadataError(_ERROR)
    streams = tuple(
        PreparedStreamSummary(
            stream.stream_slot,
            stream.part_count,
            stream.record_count,
            stream.byte_count,
            stream.manifest_byte_count,
            bytes.fromhex(digest),
        )
        for stream, digest in zip(progress, matched.groups(), strict=True)
    )
    summary = prepared_source_summary(identity, streams, stream_count=len(streams), policy=policy)
    if (canonical_summary, prepared_sha256) != (summary.canonical, summary.digest):
        raise PreparedSourceMetadataError(_ERROR)
    return streams
