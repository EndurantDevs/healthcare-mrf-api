# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict v2 logical-range manifests for native-retained UHC JSON files."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from process.uhc_retained_types import (
    RawRangeProof,
    RetainedRawArtifactProof,
    UHCRetainedAdmissionError,
    _reject_duplicate_keys,
    _validate_artifact_identity,
    _validate_contract_version,
    _validate_range_count,
    _validate_sha256,
    _verify_artifact,
)


RANGE_CONTRACT_ID = "healthporta.uhc.retained-json-ranges.v2"
RANGE_CONTRACT_VERSION = 2
RANGE_CANONICALIZATION_ID = "json-object-remove-crlf-append-lf.v1"
_MAX_DATABASE_INTEGER = 2**63 - 1
_MAX_RANGE_MANIFEST_BYTES = 1024 * 1024
_RANGE_ENTRY_FIELDS = {
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
}
_MANIFEST_FIELDS = {
    "contract_id",
    "contract_version",
    "canonicalization_id",
    "producer_build_id",
    "raw_artifact",
    "range_count",
    "ranges",
    "range_set_sha256",
}


@dataclass(frozen=True)
class RangeManifestExpectation:
    """Exact file identities and contract fields required during verification."""

    raw_path: Path
    manifest_path: Path
    artifact_sha256: str
    artifact_bytes: int
    manifest_sha256: str
    manifest_bytes: int
    range_count: int
    producer_build_id: str
    verify_raw_bytes: bool = True


def retained_raw_path(output_root: Path, artifact_sha256: str) -> Path:
    """Return the one canonical retained path for exact raw bytes."""

    _validate_sha256(artifact_sha256)
    return output_root / f"raw-{artifact_sha256}.json"


def range_manifest_path(
    output_root: Path,
    artifact_sha256: str,
    range_count: int,
    *,
    contract_version: int = RANGE_CONTRACT_VERSION,
) -> Path:
    """Return the versioned manifest path for one logical range layout."""

    _validate_sha256(artifact_sha256)
    _validate_range_count(range_count)
    _validate_contract_version(contract_version)
    return output_root / (
        f"raw-{artifact_sha256}-ranges-{range_count}-"
        f"v{contract_version}.manifest.json"
    )


def _manifest_file_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        metadata.st_mode,
        metadata.st_nlink,
    )


def _read_descriptor_bytes(descriptor: int, byte_count: int) -> bytes:
    encoded_parts = []
    remaining_bytes = byte_count
    while remaining_bytes:
        encoded_part = os.read(descriptor, min(64 * 1024, remaining_bytes))
        if not encoded_part:
            raise UHCRetainedAdmissionError(
                "retained range manifest changed while reading"
            )
        encoded_parts.append(encoded_part)
        remaining_bytes -= len(encoded_part)
    return b"".join(encoded_parts)


def _read_manifest_descriptor(descriptor: int) -> bytes:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_mode & 0o022
    ):
        raise UHCRetainedAdmissionError("retained range manifest is invalid")
    if before.st_size <= 0 or before.st_size > _MAX_RANGE_MANIFEST_BYTES:
        raise UHCRetainedAdmissionError("retained range manifest exceeds byte limit")
    encoded_manifest = _read_descriptor_bytes(descriptor, before.st_size)
    if _manifest_file_identity(os.fstat(descriptor)) != _manifest_file_identity(before):
        raise UHCRetainedAdmissionError(
            "retained range manifest changed while reading"
        )
    return encoded_manifest


def _read_manifest_bytes(manifest_path: Path) -> bytes:
    """Read one stable regular manifest under a strict memory cap."""

    descriptor = None
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(manifest_path, flags)
        return _read_manifest_descriptor(descriptor)
    except OSError as error:
        raise UHCRetainedAdmissionError(
            "retained range manifest is invalid"
        ) from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _required_string(mapping: dict[str, object], field: str) -> str:
    field_value = mapping.get(field)
    if not isinstance(field_value, str) or not field_value:
        raise UHCRetainedAdmissionError(
            f"retained range manifest {field.replace('_', ' ')} is invalid"
        )
    return field_value


def _required_integer(
    mapping: dict[str, object],
    field: str,
    *,
    minimum: int = 0,
    maximum: int = _MAX_DATABASE_INTEGER,
) -> int:
    field_value = mapping.get(field)
    if (
        type(field_value) is not int
        or field_value < minimum
        or field_value > maximum
    ):
        raise UHCRetainedAdmissionError(
            f"retained range manifest {field.replace('_', ' ')} is invalid"
        )
    return field_value


def range_set_digest(
    artifact_sha256: str,
    artifact_byte_count: int,
    record_count: int,
    ranges: tuple[RawRangeProof, ...],
) -> str:
    """Hash every versioned raw-range and canonical proof in ordinal order."""

    digest = hashlib.sha256()
    digest.update(RANGE_CONTRACT_ID.encode("ascii") + b"\0")
    digest.update(RANGE_CONTRACT_VERSION.to_bytes(8, "big"))
    digest.update(RANGE_CANONICALIZATION_ID.encode("ascii") + b"\0")
    digest.update(bytes.fromhex(artifact_sha256))
    digest.update(artifact_byte_count.to_bytes(8, "big"))
    digest.update(record_count.to_bytes(8, "big"))
    digest.update(len(ranges).to_bytes(8, "big"))
    for raw_range in ranges:
        for field_value in (
            raw_range.range_ordinal,
            raw_range.raw_byte_start,
            raw_range.raw_byte_end,
            raw_range.raw_byte_count,
            raw_range.record_start,
            raw_range.record_end,
            raw_range.record_count,
            raw_range.canonical_byte_count,
        ):
            digest.update(field_value.to_bytes(8, "big"))
        digest.update(bytes.fromhex(raw_range.raw_sha256))
        digest.update(bytes.fromhex(raw_range.canonical_sha256))
    return digest.hexdigest()


def _decode_range_manifest_entry(
    range_mapping: object,
    *,
    raw_path: Path,
    artifact_sha256: str,
    range_count: int,
    range_ordinal: int,
    expected_record_start: int,
    previous_byte_end: int,
) -> RawRangeProof:
    """Decode one exact manifest entry and enforce contiguous bounds."""

    if not isinstance(range_mapping, dict):
        raise UHCRetainedAdmissionError("retained range manifest entry is invalid")
    if set(range_mapping) != _RANGE_ENTRY_FIELDS:
        raise UHCRetainedAdmissionError("retained range manifest entry shape is invalid")
    raw_range = RawRangeProof(
        artifact_sha256=artifact_sha256,
        contract_version=RANGE_CONTRACT_VERSION,
        range_count=range_count,
        range_ordinal=_required_integer(range_mapping, "range_ordinal"),
        raw_byte_start=_required_integer(range_mapping, "raw_byte_start"),
        raw_byte_end=_required_integer(
            range_mapping,
            "raw_byte_end",
            minimum=1,
        ),
        raw_sha256=_required_string(range_mapping, "raw_sha256"),
        raw_byte_count=_required_integer(
            range_mapping,
            "raw_byte_count",
            minimum=1,
        ),
        record_start=_required_integer(range_mapping, "record_start"),
        record_end=_required_integer(range_mapping, "record_end", minimum=1),
        record_count=_required_integer(range_mapping, "record_count", minimum=1),
        canonical_sha256=_required_string(range_mapping, "canonical_sha256"),
        canonical_byte_count=_required_integer(
            range_mapping,
            "canonical_byte_count",
            minimum=1,
        ),
        path=str(raw_path),
    )
    _validate_sha256(raw_range.raw_sha256)
    _validate_sha256(raw_range.canonical_sha256)
    has_invalid_bounds = (
        raw_range.range_ordinal != range_ordinal
        or raw_range.raw_byte_start < previous_byte_end
        or raw_range.raw_byte_end <= raw_range.raw_byte_start
        or raw_range.raw_byte_count
        != raw_range.raw_byte_end - raw_range.raw_byte_start
        or raw_range.record_start != expected_record_start
        or raw_range.record_end - raw_range.record_start != raw_range.record_count
    )
    if has_invalid_bounds:
        raise UHCRetainedAdmissionError("retained raw range proof is not contiguous")
    return raw_range


def _ranges_from_manifest(
    range_mappings: list[object],
    *,
    raw_path: Path,
    artifact_sha256: str,
    artifact_byte_count: int,
    record_count: int,
    range_count: int,
) -> tuple[RawRangeProof, ...]:
    if len(range_mappings) != range_count:
        raise UHCRetainedAdmissionError("retained range manifest count is invalid")
    ranges = []
    expected_record_start = 0
    previous_byte_end = 0
    for range_ordinal, range_mapping in enumerate(range_mappings):
        raw_range = _decode_range_manifest_entry(
            range_mapping,
            raw_path=raw_path,
            artifact_sha256=artifact_sha256,
            range_count=range_count,
            range_ordinal=range_ordinal,
            expected_record_start=expected_record_start,
            previous_byte_end=previous_byte_end,
        )
        if raw_range.raw_byte_end > artifact_byte_count:
            raise UHCRetainedAdmissionError("retained raw range exceeds the artifact")
        ranges.append(raw_range)
        expected_record_start = raw_range.record_end
        previous_byte_end = raw_range.raw_byte_end
    if expected_record_start != record_count:
        raise UHCRetainedAdmissionError("retained range record coverage is incomplete")
    return tuple(ranges)


def _strict_manifest(encoded_manifest: bytes) -> dict[str, object]:
    try:
        manifest = json.loads(
            encoded_manifest,
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (UnicodeDecodeError, ValueError) as error:
        raise UHCRetainedAdmissionError("retained range manifest is invalid") from error
    if not isinstance(manifest, dict):
        raise UHCRetainedAdmissionError("retained range manifest shape is invalid")
    return manifest


def _validate_build_id(build_id: str, *, label: str) -> str:
    """Require bounded printable ASCII provenance safe for logs and storage."""

    if (
        not isinstance(build_id, str)
        or not build_id
        or len(build_id) > 256
        or not build_id.isascii()
        or not build_id.isprintable()
    ):
        raise UHCRetainedAdmissionError(f"retained range {label} is invalid")
    return build_id


def _load_verified_manifest_document(
    expectation: RangeManifestExpectation,
) -> dict[str, object]:
    _validate_artifact_identity(
        expectation.artifact_sha256,
        expectation.artifact_bytes,
    )
    _validate_artifact_identity(
        expectation.manifest_sha256,
        expectation.manifest_bytes,
    )
    _validate_range_count(expectation.range_count)
    _validate_build_id(expectation.producer_build_id, label="producer build ID")
    output_root = expectation.raw_path.parent.resolve()
    canonical_raw_path = retained_raw_path(
        output_root,
        expectation.artifact_sha256,
    ).resolve()
    canonical_manifest_path = range_manifest_path(
        output_root,
        expectation.artifact_sha256,
        expectation.range_count,
    ).resolve()
    if output_root / expectation.raw_path.name != canonical_raw_path or (
        output_root / expectation.manifest_path.name != canonical_manifest_path
    ):
        raise UHCRetainedAdmissionError("retained range artifact path is not canonical")
    if expectation.verify_raw_bytes:
        _verify_artifact(
            expectation.raw_path,
            expectation.artifact_sha256,
            expectation.artifact_bytes,
        )
    encoded_manifest = _read_manifest_bytes(expectation.manifest_path)
    if len(encoded_manifest) != expectation.manifest_bytes or (
        hashlib.sha256(encoded_manifest).hexdigest()
        != expectation.manifest_sha256
    ):
        raise UHCRetainedAdmissionError("retained range manifest proof does not match")
    return _strict_manifest(encoded_manifest)


def _decode_manifest_collections(
    manifest: dict[str, object],
    expectation: RangeManifestExpectation,
) -> tuple[int, list[object]]:
    if set(manifest) != _MANIFEST_FIELDS:
        raise UHCRetainedAdmissionError("retained range manifest shape is invalid")
    if (
        manifest["contract_id"] != RANGE_CONTRACT_ID
        or manifest["contract_version"] != RANGE_CONTRACT_VERSION
        or manifest["canonicalization_id"] != RANGE_CANONICALIZATION_ID
        or manifest["producer_build_id"] != expectation.producer_build_id
        or manifest["range_count"] != expectation.range_count
    ):
        raise UHCRetainedAdmissionError("retained range manifest contract is invalid")
    raw_mapping = manifest["raw_artifact"]
    range_mappings = manifest["ranges"]
    if not isinstance(raw_mapping, dict) or not isinstance(range_mappings, list):
        raise UHCRetainedAdmissionError("retained range manifest shape is invalid")
    record_count = _required_integer(raw_mapping, "record_count", minimum=1)
    expected_raw_mapping = {
        "file_name": expectation.raw_path.name,
        "sha256": expectation.artifact_sha256,
        "byte_count": expectation.artifact_bytes,
        "record_count": record_count,
    }
    if raw_mapping != expected_raw_mapping:
        raise UHCRetainedAdmissionError("retained range raw identity is invalid")
    return record_count, range_mappings


def _build_retained_artifact_proof(
    expectation: RangeManifestExpectation,
    *,
    record_count: int,
    retained_ranges: tuple[RawRangeProof, ...],
    range_set_sha256: str,
) -> RetainedRawArtifactProof:
    raw_artifact = RetainedRawArtifactProof(
        path=str(expectation.raw_path),
        sha256=expectation.artifact_sha256,
        byte_count=expectation.artifact_bytes,
        record_count=record_count,
        contract_version=RANGE_CONTRACT_VERSION,
        range_count=expectation.range_count,
        producer_build_id=expectation.producer_build_id,
        range_set_sha256=range_set_sha256,
        canonical_byte_count=sum(
            raw_range.canonical_byte_count for raw_range in retained_ranges
        ),
        manifest_path=str(expectation.manifest_path),
        manifest_sha256=expectation.manifest_sha256,
        manifest_byte_count=expectation.manifest_bytes,
    )
    if raw_artifact.canonical_byte_count > _MAX_DATABASE_INTEGER:
        raise UHCRetainedAdmissionError("retained canonical byte count is invalid")
    return raw_artifact


def _load_verified_range_manifest(
    expectation: RangeManifestExpectation,
) -> tuple[RetainedRawArtifactProof, tuple[RawRangeProof, ...]]:
    """Verify exact files and decode one complete native range proof."""

    manifest = _load_verified_manifest_document(expectation)
    record_count, range_mappings = _decode_manifest_collections(
        manifest,
        expectation,
    )
    retained_ranges = _ranges_from_manifest(
        range_mappings,
        raw_path=expectation.raw_path,
        artifact_sha256=expectation.artifact_sha256,
        artifact_byte_count=expectation.artifact_bytes,
        record_count=record_count,
        range_count=expectation.range_count,
    )
    range_set_sha256 = range_set_digest(
        expectation.artifact_sha256,
        expectation.artifact_bytes,
        record_count,
        retained_ranges,
    )
    if manifest["range_set_sha256"] != range_set_sha256:
        raise UHCRetainedAdmissionError("retained range-set identity is invalid")
    return (
        _build_retained_artifact_proof(
            expectation,
            record_count=record_count,
            retained_ranges=retained_ranges,
            range_set_sha256=range_set_sha256,
        ),
        retained_ranges,
    )


def load_verified_range_manifest(**expectation_fields: object) -> tuple[RetainedRawArtifactProof, tuple[RawRangeProof, ...]]:
    """Verify exact files using the stable keyword-only manifest contract."""

    if not set(expectation_fields).issubset({
        "raw_path", "manifest_path", "expected_artifact_sha256",
        "expected_artifact_bytes", "expected_manifest_sha256",
        "expected_manifest_bytes", "expected_range_count", "producer_build_id",
        "verify_raw_bytes",
    }):
        raise TypeError("unexpected retained range manifest argument")
    try:
        expectation = RangeManifestExpectation(
            raw_path=expectation_fields["raw_path"],
            manifest_path=expectation_fields["manifest_path"],
            artifact_sha256=expectation_fields["expected_artifact_sha256"],
            artifact_bytes=expectation_fields["expected_artifact_bytes"],
            manifest_sha256=expectation_fields["expected_manifest_sha256"],
            manifest_bytes=expectation_fields["expected_manifest_bytes"],
            range_count=expectation_fields["expected_range_count"],
            producer_build_id=expectation_fields["producer_build_id"],
            verify_raw_bytes=expectation_fields.get("verify_raw_bytes", True),
        )
    except KeyError as error:
        raise TypeError("missing retained range manifest argument") from error
    return _load_verified_range_manifest(expectation)
