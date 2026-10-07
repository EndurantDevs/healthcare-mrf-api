# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Caller-owned durable registration for sealed custom-import captures."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportSourceStream,
)
from process.custom_import._source_text import _SourceTextValidationError, validate_snapshot_token
from process.custom_import.capture import CaptureManifest, SealedCapture
from process.custom_import.definition import (
    CONTRACT_VERSION,
    MAX_CHILD_COLLECTIONS,
    CustomImportDefinition,
    DefinitionError,
)
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CandidateRunnerError, CandidateRunRequest

__all__ = (
    "CaptureBundleConflict",
    "CaptureBundleRegistration",
    "CapturePayloadUnavailable",
    "CaptureReceipt",
    "CaptureStoreError",
    "CaptureStoreTransactionRequired",
    "ReplayableParquetCapture",
    "SegmentedParquetPart",
    "load_replayable_parquet_bundle",
    "open_replayable_parquet_parts",
    "open_segmented_parquet_parts",
    "open_segmented_cursor_part",
    "verify_segmented_stream_metadata",
    "register_capture_bundle",
    "register_replayable_parquet_bundle",
)


_CAPTURE_STORE_CONTRACT = "custom-import/capture-store/v1"
_MAX_BIGINT = 2**63 - 1
_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_PARQUET_PART_PAYLOAD_CONTRACT = "custom-import/parquet-parts/v1"
_PARQUET_PART_SET_DOMAIN = b"custom-import/parquet-parts/v1\x00"
_MAX_PARQUET_PART_BYTES = 64 * 1024 * 1024
_MAX_PARQUET_PARTS_PER_CAPTURE = 4_096
_MAX_PARQUET_PARTS_PER_BUNDLE = 8_192
_MAX_PARQUET_BYTES_PER_BUNDLE = 128 * 1024 * 1024
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class CaptureStoreError(ValueError):
    """A capture receipt or persisted capture bundle is not safe to retain."""


class CaptureStoreTransactionRequired(CaptureStoreError):
    """Registration needs one active caller-owned transaction."""


class CaptureBundleConflict(CaptureStoreError):
    """An existing source snapshot is not an exact replay of this bundle."""


class CapturePayloadUnavailable(CaptureStoreError):
    """An exact capture bundle has no complete durable payload to replay."""


@dataclass(frozen=True)
class CaptureReceipt:
    """One connector-sealed capture for one declared source stream.

    ``canonical_manifest`` and ``manifest_sha256`` are retained as a paired,
    connector-owned seal.  The generic store validates the canonical JSON and
    preserves the connector's digest without assuming its connector-specific
    hash domain.
    """

    stream_id: str
    source_snapshot_token: str
    byte_count: int
    content_sha256: str
    canonical_manifest: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.stream_id, str) or not _IDENTIFIER.fullmatch(self.stream_id):
            raise CaptureStoreError("capture stream_id must be lower_snake_case")
        object.__setattr__(self, "source_snapshot_token", _snapshot_token(self.source_snapshot_token))
        if (
            isinstance(self.byte_count, bool)
            or not isinstance(self.byte_count, int)
            or not 0 <= self.byte_count <= _MAX_BIGINT
        ):
            raise CaptureStoreError("capture byte_count must be a non-negative bigint")
        object.__setattr__(self, "content_sha256", _sha256(self.content_sha256, "capture content digest"))
        object.__setattr__(self, "canonical_manifest", _canonical_manifest(self.canonical_manifest))
        object.__setattr__(self, "manifest_sha256", _sha256(self.manifest_sha256, "capture manifest digest"))


@dataclass(frozen=True)
class ReplayableParquetCapture:
    """One sealed receipt paired with its immutable ordered Parquet parts."""

    receipt: CaptureReceipt
    parts: tuple[bytes, ...] = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, CaptureReceipt):
            raise CaptureStoreError("replayable Parquet capture requires a capture receipt")
        parts = _validated_parquet_parts(self.parts)
        part_bytes = sum(len(part) for part in parts)
        if not 1 <= self.receipt.byte_count <= _MAX_PARQUET_PART_BYTES:
            raise CaptureStoreError("replayable Parquet receipt byte count exceeds the durable payload limit")
        if part_bytes != self.receipt.byte_count:
            raise CaptureStoreError("replayable Parquet parts do not match the receipt byte count")
        object.__setattr__(self, "parts", parts)


@dataclass(frozen=True)
class CaptureBundleRegistration:
    """The composite IDs for one newly retained or exactly replayed bundle."""

    capture_bundle_id: int
    stream_slots: tuple[int, ...]
    created: bool


@dataclass(frozen=True)
class SegmentedParquetPart:
    """One verified retained part and connector-asserted replay accounting."""

    receipt: CaptureReceipt
    ordinal: int
    capture: SealedCapture = field(repr=False)
    record_count: int
    arrow_byte_count: int


@dataclass(frozen=True)
class _VerifiedParquetPart:
    receipt: CaptureReceipt
    ordinal: int
    payload: bytes = field(repr=False)
    capture_manifest: CaptureManifest | None = None
    record_count: int | None = None
    arrow_byte_count: int | None = None


@dataclass(frozen=True)
class _CaptureIdentity:
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int


@dataclass(frozen=True)
class _PreparedCaptureBundle:
    identity: _CaptureIdentity
    streams: tuple[tuple[str, int], ...]
    receipts_by_stream: Mapping[str, CaptureReceipt]
    snapshot_token: str
    snapshot_token_sha256: bytes
    canonical_manifest: str
    manifest_sha256: bytes

    @property
    def stream_slots(self) -> tuple[int, ...]:
        """Return definition-owned stream slots in canonical order."""

        return tuple(stream_slot for _, stream_slot in self.streams)


def _positive_id(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= _MAX_BIGINT:
        raise CaptureStoreError(f"{label} must be a positive bigint")
    return value


def _snapshot_token(value: object) -> str:
    try:
        return validate_snapshot_token(value)
    except _SourceTextValidationError as exc:
        raise CaptureStoreError("capture snapshot token is invalid") from exc


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise CaptureStoreError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _build_unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    values_by_key: dict[str, Any] = {}
    for key, value in pairs:
        if key in values_by_key:
            raise CaptureStoreError("capture manifest has a duplicate object key")
        values_by_key[key] = value
    return values_by_key


def _reject_json_constant(value: str) -> None:
    raise CaptureStoreError(f"capture manifest cannot contain {value}")


def _canonical_manifest(value: object) -> str:
    if not isinstance(value, str):
        raise CaptureStoreError("capture manifest must be text")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CaptureStoreError("capture manifest must be valid UTF-8") from exc
    if not encoded or len(encoded) > _MAX_MANIFEST_BYTES:
        raise CaptureStoreError("capture manifest exceeds the byte limit")
    try:
        document = json.loads(value, object_pairs_hook=_build_unique_json_object, parse_constant=_reject_json_constant)
        canonical = json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (RecursionError, TypeError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        if isinstance(exc, CaptureStoreError):
            raise
        raise CaptureStoreError("capture manifest is not canonical JSON") from exc
    if not isinstance(document, dict) or canonical != value:
        raise CaptureStoreError("capture manifest is not canonical JSON")
    return canonical


def _require_transaction(session: object) -> None:
    in_transaction = getattr(session, "in_transaction", None)
    if not callable(in_transaction) or not in_transaction():
        raise CaptureStoreTransactionRequired("capture registration requires an active caller transaction")


def _require_clean_session(session: object) -> None:
    for attribute in ("new", "dirty", "deleted"):
        if getattr(session, attribute, ()):
            raise CaptureStoreError("capture registration requires a clean session before locking its dataset")


def _identity(
    *,
    dataset_id: object,
    definition_revision_id: object,
    schema_revision_id: object,
) -> _CaptureIdentity:
    return _CaptureIdentity(
        dataset_id=_positive_id(dataset_id, "dataset_id"),
        definition_revision_id=_positive_id(definition_revision_id, "definition_revision_id"),
        schema_revision_id=_positive_id(schema_revision_id, "schema_revision_id"),
    )


def _validate_receipts(receipt_tuple: object) -> tuple[CaptureReceipt, ...]:
    if not isinstance(receipt_tuple, tuple) or not receipt_tuple:
        raise CaptureStoreError("capture receipts must be a non-empty tuple")
    if len(receipt_tuple) > MAX_CHILD_COLLECTIONS + 1:
        raise CaptureStoreError("capture receipts exceed the v1 stream limit")
    if not all(isinstance(receipt, CaptureReceipt) for receipt in receipt_tuple):
        raise CaptureStoreError("capture receipts must use the declared receipt type")
    if sum(len(receipt.canonical_manifest.encode("utf-8")) for receipt in receipt_tuple) > _MAX_MANIFEST_BYTES:
        raise CaptureStoreError("capture receipt manifests exceed the aggregate byte limit")
    try:
        receipts = tuple(
            CaptureReceipt(
                stream_id=receipt.stream_id,
                source_snapshot_token=receipt.source_snapshot_token,
                byte_count=receipt.byte_count,
                content_sha256=receipt.content_sha256,
                canonical_manifest=receipt.canonical_manifest,
                manifest_sha256=receipt.manifest_sha256,
            )
            for receipt in receipt_tuple
        )
    except (AttributeError, CaptureStoreError) as exc:
        if isinstance(exc, CaptureStoreError):
            raise
        raise CaptureStoreError("capture receipts must use the declared receipt type") from exc
    if len({receipt.stream_id for receipt in receipts}) != len(receipts):
        raise CaptureStoreError("capture receipt stream ids must be unique")
    if len({receipt.source_snapshot_token for receipt in receipts}) != 1:
        raise CaptureStoreError("capture receipts must share one exact snapshot token")
    return receipts


def _validated_parquet_parts(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, tuple):
        raise CaptureStoreError("replayable Parquet parts must be a tuple")
    if not 1 <= len(value) <= _MAX_PARQUET_PARTS_PER_CAPTURE:
        raise CaptureStoreError("replayable Parquet parts must contain from 1 through 4096 entries")
    if not all(isinstance(part, bytes) and 1 <= len(part) <= _MAX_PARQUET_PART_BYTES for part in value):
        raise CaptureStoreError("replayable Parquet parts must be bounded non-empty bytes")
    return value


def _payload_set_sha256(parts: tuple[bytes, ...]) -> bytes:
    """Hash ordered part identity without concatenating retained payload bytes."""

    digest = hashlib.sha256(_PARQUET_PART_SET_DOMAIN)
    for ordinal, part in enumerate(parts, start=1):
        _add_payload_part_digest(digest, ordinal, len(part), hashlib.sha256(part).digest())
    return digest.digest()


def _add_payload_part_digest(digest: Any, ordinal: int, byte_count: int, payload_sha256: bytes) -> None:
    """Preserve the ordered payload-set framing for eager and incremental reads."""

    digest.update(ordinal.to_bytes(4, byteorder="big", signed=False))
    digest.update(byte_count.to_bytes(8, byteorder="big", signed=False))
    digest.update(payload_sha256)


def _validate_replayable_parquet_captures(value: object) -> tuple[ReplayableParquetCapture, ...]:
    if not isinstance(value, tuple) or not value:
        raise CaptureStoreError("replayable Parquet captures must be a non-empty tuple")
    if not all(isinstance(capture, ReplayableParquetCapture) for capture in value):
        raise CaptureStoreError("replayable Parquet captures must use the declared capture type")
    captures = tuple(
        ReplayableParquetCapture(
            receipt=_validate_receipts((capture.receipt,))[0],
            parts=capture.parts,
        )
        for capture in value
    )
    _validate_receipts(tuple(capture.receipt for capture in captures))
    if (
        sum(len(capture.parts) for capture in captures) > _MAX_PARQUET_PARTS_PER_BUNDLE
        or sum(sum(len(part) for part in capture.parts) for capture in captures) > _MAX_PARQUET_BYTES_PER_BUNDLE
    ):
        raise CaptureStoreError("replayable Parquet captures exceed the aggregate durable payload limit")
    return captures


async def _lock_dataset(session: AsyncSession, identity: _CaptureIdentity) -> None:
    dataset = (
        await session.execute(
            select(CustomImportDataset)
            .where(CustomImportDataset.dataset_id == identity.dataset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if dataset is None:
        raise CaptureStoreError("capture dataset does not exist")


async def _validated_streams(
    session: AsyncSession,
    identity: _CaptureIdentity,
) -> tuple[tuple[str, int], ...]:
    definition_model = await session.get(CustomImportDefinitionRevision, identity.definition_revision_id)
    if (
        definition_model is None
        or definition_model.dataset_id != identity.dataset_id
        or definition_model.schema_revision_id != identity.schema_revision_id
    ):
        raise CaptureStoreError("capture identity does not match a persisted definition")
    try:
        definition = CustomImportDefinition.from_json(definition_model.canonical_definition)
        registry = await load_registry(
            session,
            CandidateRunRequest(
                dataset_id=identity.dataset_id,
                definition_revision_id=identity.definition_revision_id,
                schema_revision_id=identity.schema_revision_id,
                execution_id=1,
                lease_token=b"capture-registration",
                definition=definition,
                roots=(),
                children_by_collection={},
            ),
        )
    except (CandidateRunnerError, DefinitionError, TypeError, ValueError) as exc:
        raise CaptureStoreError("persisted definition graph is invalid") from exc
    return tuple(sorted(registry.stream_slots.items(), key=lambda entry: entry[1]))


async def _validated_replayable_parquet_streams(
    session: AsyncSession,
    identity: _CaptureIdentity,
    streams: tuple[tuple[str, int], ...],
) -> None:
    result = await session.execute(
        select(CustomImportSourceStream).where(
            CustomImportSourceStream.dataset_id == identity.dataset_id,
            CustomImportSourceStream.definition_revision_id == identity.definition_revision_id,
            CustomImportSourceStream.schema_revision_id == identity.schema_revision_id,
        )
    )
    source_by_slot = {source.stream_slot: source for source in result.scalars().all()}
    if len(source_by_slot) != len(streams) or set(source_by_slot) != {stream_slot for _, stream_slot in streams}:
        raise CaptureStoreError("persisted source streams do not exactly match the durable replay identity")
    for stream_id, stream_slot in streams:
        source = source_by_slot[stream_slot]
        if source.stream_id != stream_id or source.decoder != "parquet" or source.compression != "none":
            raise CaptureStoreError("durable replay requires persisted Parquet streams without compression")


def _index_receipts_by_stream(
    streams: tuple[tuple[str, int], ...],
    receipts: tuple[CaptureReceipt, ...],
) -> dict[str, CaptureReceipt]:
    receipts_by_stream = {receipt.stream_id: receipt for receipt in receipts}
    if set(receipts_by_stream) != {stream_id for stream_id, _ in streams}:
        raise CaptureStoreError("capture receipts do not exactly cover persisted definition source streams")
    return receipts_by_stream


def _index_replayable_captures_by_stream(
    streams: tuple[tuple[str, int], ...],
    captures: tuple[ReplayableParquetCapture, ...],
) -> dict[str, ReplayableParquetCapture]:
    captures_by_stream = {capture.receipt.stream_id: capture for capture in captures}
    if set(captures_by_stream) != {stream_id for stream_id, _ in streams}:
        raise CaptureStoreError("replayable Parquet captures do not exactly cover persisted definition source streams")
    return captures_by_stream


def _prepare_capture_bundle(
    identity: _CaptureIdentity,
    streams: tuple[tuple[str, int], ...],
    receipts_by_stream: Mapping[str, CaptureReceipt],
) -> _PreparedCaptureBundle:
    snapshot_token = next(iter(receipts_by_stream.values())).source_snapshot_token
    bundle_values_by_key = {
        "contract": _CAPTURE_STORE_CONTRACT,
        "dataset_id": identity.dataset_id,
        "definition_revision_id": identity.definition_revision_id,
        "schema_revision_id": identity.schema_revision_id,
        "snapshot_token": snapshot_token,
        "streams": [
            {
                "byte_count": receipt.byte_count,
                "canonical_manifest": json.loads(receipt.canonical_manifest),
                "content_sha256": receipt.content_sha256,
                "manifest_sha256": receipt.manifest_sha256,
                "stream_id": stream_id,
                "stream_slot": stream_slot,
            }
            for stream_id, stream_slot in streams
            for receipt in (receipts_by_stream[stream_id],)
        ],
    }
    canonical = json.dumps(
        bundle_values_by_key,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    encoded = canonical.encode("utf-8")
    if len(encoded) > _MAX_MANIFEST_BYTES:
        raise CaptureStoreError("capture bundle manifest exceeds the byte limit")
    digest = hashlib.sha256(
        f"{CONTRACT_VERSION}\x00{_CAPTURE_STORE_CONTRACT}\x00bundle\x00".encode("ascii") + encoded
    ).digest()
    return _PreparedCaptureBundle(
        identity=identity,
        streams=streams,
        receipts_by_stream=receipts_by_stream,
        snapshot_token=snapshot_token,
        snapshot_token_sha256=hashlib.sha256(snapshot_token.encode("utf-8")).digest(),
        canonical_manifest=canonical,
        manifest_sha256=digest,
    )


def _is_stored_digest_equal(value: object, expected: bytes) -> bool:
    return isinstance(value, (bytes, bytearray, memoryview)) and bytes(value) == expected


def _is_matching_bundle(
    bundle: CustomImportCaptureBundle,
    prepared: _PreparedCaptureBundle,
) -> bool:
    identity = prepared.identity
    return (
        bundle.dataset_id == identity.dataset_id
        and bundle.definition_revision_id == identity.definition_revision_id
        and bundle.schema_revision_id == identity.schema_revision_id
        and bundle.snapshot_token == prepared.snapshot_token
        and _is_stored_digest_equal(bundle.snapshot_token_sha256, prepared.snapshot_token_sha256)
        and bundle.canonical_manifest == prepared.canonical_manifest
        and _is_stored_digest_equal(bundle.manifest_sha256, prepared.manifest_sha256)
        and bundle.stream_count == len(prepared.streams)
    )


async def _is_matching_capture_rows(
    session: AsyncSession,
    bundle: CustomImportCaptureBundle,
    prepared: _PreparedCaptureBundle,
) -> bool:
    result = await session.execute(
        select(CustomImportCapture)
        .where(CustomImportCapture.capture_bundle_id == bundle.capture_bundle_id)
        .with_for_update()
    )
    captures = tuple(result.scalars().all())
    captures_by_slot = {capture.stream_slot: capture for capture in captures}
    if len(captures_by_slot) != len(captures) or len(captures) != len(prepared.streams):
        return False
    for stream_id, stream_slot in prepared.streams:
        receipt = prepared.receipts_by_stream[stream_id]
        capture = captures_by_slot.get(stream_slot)
        if capture is None or (
            capture.dataset_id != prepared.identity.dataset_id
            or capture.definition_revision_id != prepared.identity.definition_revision_id
            or capture.schema_revision_id != prepared.identity.schema_revision_id
            or capture.byte_count != receipt.byte_count
            or capture.canonical_manifest != receipt.canonical_manifest
            or not _is_stored_digest_equal(capture.content_sha256, bytes.fromhex(receipt.content_sha256))
            or not _is_stored_digest_equal(capture.manifest_sha256, bytes.fromhex(receipt.manifest_sha256))
        ):
            return False
    return True


def _stored_bytes(value: object) -> bytes | None:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        return None
    return bytes(value)


def _durable_payload_metadata(capture: CustomImportCapture) -> tuple[int, bytes] | None:
    values = (capture.payload_contract, capture.payload_part_count, capture.payload_set_sha256)
    if all(value is None for value in values):
        return None
    part_count = capture.payload_part_count
    payload_set_sha256 = _stored_bytes(capture.payload_set_sha256)
    if (
        capture.payload_contract != _PARQUET_PART_PAYLOAD_CONTRACT
        or isinstance(part_count, bool)
        or not isinstance(part_count, int)
        or not 1 <= part_count <= _MAX_PARQUET_PARTS_PER_CAPTURE
        or payload_set_sha256 is None
        or len(payload_set_sha256) != 32
        or isinstance(capture.byte_count, bool)
        or not isinstance(capture.byte_count, int)
        or not 1 <= capture.byte_count <= _MAX_PARQUET_PART_BYTES
    ):
        raise CaptureBundleConflict("durable capture payload metadata is invalid")
    return part_count, payload_set_sha256


def _new_capture_bundle(prepared: _PreparedCaptureBundle) -> CustomImportCaptureBundle:
    identity = prepared.identity
    return CustomImportCaptureBundle(
        dataset_id=identity.dataset_id,
        definition_revision_id=identity.definition_revision_id,
        schema_revision_id=identity.schema_revision_id,
        snapshot_token=prepared.snapshot_token,
        snapshot_token_sha256=prepared.snapshot_token_sha256,
        canonical_manifest=prepared.canonical_manifest,
        manifest_sha256=prepared.manifest_sha256,
        stream_count=len(prepared.streams),
    )


async def _is_matching_replayable_parquet_rows(
    session: AsyncSession,
    bundle: CustomImportCaptureBundle,
    prepared: _PreparedCaptureBundle,
    captures_by_stream: Mapping[str, ReplayableParquetCapture],
) -> bool | None:
    """Return whether locked stored rows exactly match a replayable Parquet bundle."""

    capture_query = await session.execute(
        select(CustomImportCapture)
        .where(CustomImportCapture.capture_bundle_id == bundle.capture_bundle_id)
        .with_for_update()
    )
    stored_captures = tuple(capture_query.scalars().all())
    persisted_by_slot = {capture.stream_slot: capture for capture in stored_captures}
    if len(persisted_by_slot) != len(stored_captures) or len(stored_captures) != len(prepared.streams):
        return False
    return await _is_matching_stored_replayable_payload(
        session,
        bundle,
        prepared,
        captures_by_stream,
        persisted_by_slot,
    )


async def _is_matching_stored_replayable_payload(
    session: AsyncSession,
    bundle: CustomImportCaptureBundle,
    prepared: _PreparedCaptureBundle,
    captures_by_stream: Mapping[str, ReplayableParquetCapture],
    persisted_by_slot: Mapping[int, CustomImportCapture],
) -> bool | None:
    """Return whether stored durable payload metadata and parts exactly match."""

    metadata_by_slot: dict[int, tuple[int, bytes] | None] = {}
    for stream_id, stream_slot in prepared.streams:
        capture = persisted_by_slot.get(stream_slot)
        expected = captures_by_stream[stream_id]
        if capture is None or (
            capture.dataset_id != prepared.identity.dataset_id
            or capture.definition_revision_id != prepared.identity.definition_revision_id
            or capture.schema_revision_id != prepared.identity.schema_revision_id
            or capture.byte_count != expected.receipt.byte_count
            or capture.canonical_manifest != expected.receipt.canonical_manifest
            or not _is_stored_digest_equal(capture.content_sha256, bytes.fromhex(expected.receipt.content_sha256))
            or not _is_stored_digest_equal(capture.manifest_sha256, bytes.fromhex(expected.receipt.manifest_sha256))
        ):
            return False
        metadata_by_slot[stream_slot] = _durable_payload_metadata(capture)
    if all(metadata is None for metadata in metadata_by_slot.values()):
        return None
    if any(metadata is None for metadata in metadata_by_slot.values()):
        return False

    parts_by_slot = await _locked_parquet_parts_by_slot(session, bundle)
    if set(parts_by_slot) != set(metadata_by_slot):
        return False
    for stream_id, stream_slot in prepared.streams:
        expected = captures_by_stream[stream_id]
        metadata = metadata_by_slot[stream_slot]
        assert metadata is not None
        part_count, payload_set_sha256 = metadata
        persisted_parts = parts_by_slot[stream_slot]
        if part_count != len(expected.parts) or payload_set_sha256 != _payload_set_sha256(expected.parts):
            return False
        if len(persisted_parts) != len(expected.parts):
            return False
        for ordinal, (part, expected_payload) in enumerate(zip(persisted_parts, expected.parts, strict=True), start=1):
            stored_payload = _stored_bytes(part.payload)
            if (
                part.part_ordinal != ordinal
                or part.byte_count != len(expected_payload)
                or stored_payload != expected_payload
                or not _is_stored_digest_equal(part.payload_sha256, hashlib.sha256(expected_payload).digest())
                or stored_payload is None
                or hashlib.sha256(stored_payload).digest() != hashlib.sha256(expected_payload).digest()
            ):
                return False
    return True


async def _locked_parquet_parts_by_slot(
    session: AsyncSession,
    bundle: CustomImportCaptureBundle,
) -> dict[int, list[CustomImportCaptureParquetPart]]:
    part_query = await session.execute(
        select(CustomImportCaptureParquetPart)
        .where(CustomImportCaptureParquetPart.capture_bundle_id == bundle.capture_bundle_id)
        .order_by(CustomImportCaptureParquetPart.stream_slot, CustomImportCaptureParquetPart.part_ordinal)
        .with_for_update()
    )
    parts_by_slot: dict[int, list[CustomImportCaptureParquetPart]] = {}
    for part in part_query.scalars().all():
        parts_by_slot.setdefault(part.stream_slot, []).append(part)
    return parts_by_slot


async def _snapshot_bundles(
    session: AsyncSession,
    prepared: _PreparedCaptureBundle,
) -> tuple[CustomImportCaptureBundle, ...]:
    identity = prepared.identity
    result = await session.execute(
        select(CustomImportCaptureBundle)
        .where(
            CustomImportCaptureBundle.dataset_id == identity.dataset_id,
            CustomImportCaptureBundle.definition_revision_id == identity.definition_revision_id,
            CustomImportCaptureBundle.schema_revision_id == identity.schema_revision_id,
            CustomImportCaptureBundle.snapshot_token_sha256 == prepared.snapshot_token_sha256,
            CustomImportCaptureBundle.snapshot_token == prepared.snapshot_token,
            CustomImportCaptureBundle.capture_state == "sealed",
        )
        .with_for_update()
    )
    return tuple(result.scalars().all())


async def _find_existing_registration(
    session: AsyncSession,
    prepared: _PreparedCaptureBundle,
) -> CaptureBundleRegistration | None:
    matching_bundles = await _snapshot_bundles(session, prepared)
    if not matching_bundles:
        return None
    exact_bundles = tuple(bundle for bundle in matching_bundles if _is_matching_bundle(bundle, prepared))
    if (
        len(matching_bundles) != 1
        or len(exact_bundles) != 1
        or not await _is_matching_capture_rows(session, exact_bundles[0], prepared)
    ):
        raise CaptureBundleConflict("source snapshot is already bound to a different or partial capture bundle")
    return CaptureBundleRegistration(
        capture_bundle_id=exact_bundles[0].capture_bundle_id,
        stream_slots=prepared.stream_slots,
        created=False,
    )


async def _find_existing_replayable_parquet_registration(
    session: AsyncSession,
    prepared: _PreparedCaptureBundle,
    captures_by_stream: Mapping[str, ReplayableParquetCapture],
) -> CaptureBundleRegistration | None:
    matching_bundles = await _snapshot_bundles(session, prepared)
    if not matching_bundles:
        return None
    exact_bundles = tuple(bundle for bundle in matching_bundles if _is_matching_bundle(bundle, prepared))
    if len(matching_bundles) != 1 or len(exact_bundles) != 1:
        raise CaptureBundleConflict("source snapshot is already bound to a different or partial capture bundle")
    matching_payload = await _is_matching_replayable_parquet_rows(
        session,
        exact_bundles[0],
        prepared,
        captures_by_stream,
    )
    if matching_payload is None:
        raise CapturePayloadUnavailable("exact capture bundle has no durable payload")
    if not matching_payload:
        raise CaptureBundleConflict("source snapshot is already bound to a different or partial durable capture bundle")
    return CaptureBundleRegistration(
        capture_bundle_id=exact_bundles[0].capture_bundle_id,
        stream_slots=prepared.stream_slots,
        created=False,
    )


async def _insert_capture_bundle(
    session: AsyncSession,
    prepared: _PreparedCaptureBundle,
) -> CaptureBundleRegistration:
    identity = prepared.identity
    bundle = _new_capture_bundle(prepared)
    session.add(bundle)
    await session.flush()
    if not isinstance(bundle.capture_bundle_id, int) or bundle.capture_bundle_id <= 0:
        raise CaptureStoreError("capture bundle insert did not return an identifier")
    session.add_all(
        CustomImportCapture(
            capture_bundle_id=bundle.capture_bundle_id,
            dataset_id=identity.dataset_id,
            definition_revision_id=identity.definition_revision_id,
            schema_revision_id=identity.schema_revision_id,
            stream_slot=stream_slot,
            content_sha256=bytes.fromhex(receipt.content_sha256),
            byte_count=receipt.byte_count,
            canonical_manifest=receipt.canonical_manifest,
            manifest_sha256=bytes.fromhex(receipt.manifest_sha256),
        )
        for stream_id, stream_slot in prepared.streams
        for receipt in (prepared.receipts_by_stream[stream_id],)
    )
    await session.flush()
    return CaptureBundleRegistration(
        capture_bundle_id=bundle.capture_bundle_id,
        stream_slots=prepared.stream_slots,
        created=True,
    )


async def _insert_replayable_parquet_bundle(
    session: AsyncSession,
    prepared: _PreparedCaptureBundle,
    captures_by_stream: Mapping[str, ReplayableParquetCapture],
) -> CaptureBundleRegistration:
    """Insert one prepared durable capture bundle and its ordered Parquet parts."""

    identity = prepared.identity
    bundle = _new_capture_bundle(prepared)
    session.add(bundle)
    await session.flush()
    if not isinstance(bundle.capture_bundle_id, int) or bundle.capture_bundle_id <= 0:
        raise CaptureStoreError("capture bundle insert did not return an identifier")

    for stream_id, stream_slot in prepared.streams:
        replayable = captures_by_stream[stream_id]
        receipt = replayable.receipt
        session.add(
            CustomImportCapture(
                capture_bundle_id=bundle.capture_bundle_id,
                dataset_id=identity.dataset_id,
                definition_revision_id=identity.definition_revision_id,
                schema_revision_id=identity.schema_revision_id,
                stream_slot=stream_slot,
                content_sha256=bytes.fromhex(receipt.content_sha256),
                byte_count=receipt.byte_count,
                canonical_manifest=receipt.canonical_manifest,
                manifest_sha256=bytes.fromhex(receipt.manifest_sha256),
                payload_contract=_PARQUET_PART_PAYLOAD_CONTRACT,
                payload_part_count=len(replayable.parts),
                payload_set_sha256=_payload_set_sha256(replayable.parts),
            )
        )
    await session.flush()

    session.add_all(
        CustomImportCaptureParquetPart(
            capture_bundle_id=bundle.capture_bundle_id,
            stream_slot=stream_slot,
            part_ordinal=ordinal,
            byte_count=len(part_payload),
            payload=part_payload,
            payload_sha256=hashlib.sha256(part_payload).digest(),
        )
        for stream_id, stream_slot in prepared.streams
        for ordinal, part_payload in enumerate(captures_by_stream[stream_id].parts, start=1)
    )
    await session.flush()
    return CaptureBundleRegistration(
        capture_bundle_id=bundle.capture_bundle_id,
        stream_slots=prepared.stream_slots,
        created=True,
    )


async def register_capture_bundle(
    session: AsyncSession,
    *,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    receipts: tuple[CaptureReceipt, ...],
) -> CaptureBundleRegistration:
    """Persist one complete sealed source snapshot or return its exact replay.

    The caller owns the surrounding transaction.  Registration locks the
    dataset before reading or writing captures so equivalent submissions cannot
    create competing bundle identities.  This is a trusted internal boundary:
    connector code must verify payload and connector-domain manifest seals
    before constructing the caller-owned receipts.
    """

    _require_transaction(session)
    _require_clean_session(session)
    identity = _identity(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    )
    validated_receipts = _validate_receipts(receipts)
    await _lock_dataset(session, identity)
    streams = await _validated_streams(session, identity)
    receipts_by_stream = _index_receipts_by_stream(streams, validated_receipts)
    prepared = _prepare_capture_bundle(
        identity,
        streams,
        receipts_by_stream,
    )
    existing = await _find_existing_registration(session, prepared)
    return existing if existing is not None else await _insert_capture_bundle(session, prepared)


async def register_replayable_parquet_bundle(
    session: AsyncSession,
    *,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    captures: tuple[ReplayableParquetCapture, ...],
) -> CaptureBundleRegistration:
    """Persist a complete bounded Parquet payload bundle in the caller transaction."""

    _require_transaction(session)
    _require_clean_session(session)
    identity = _identity(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    )
    replayable_captures = _validate_replayable_parquet_captures(captures)
    await _lock_dataset(session, identity)
    streams = await _validated_streams(session, identity)
    await _validated_replayable_parquet_streams(session, identity, streams)
    receipts_by_stream = _index_receipts_by_stream(
        streams,
        tuple(capture.receipt for capture in replayable_captures),
    )
    captures_by_stream = _index_replayable_captures_by_stream(streams, replayable_captures)
    prepared = _prepare_capture_bundle(identity, streams, receipts_by_stream)
    existing = await _find_existing_replayable_parquet_registration(session, prepared, captures_by_stream)
    return (
        existing
        if existing is not None
        else await _insert_replayable_parquet_bundle(session, prepared, captures_by_stream)
    )


async def _load_replayable_bundle_model(
    session: AsyncSession,
    capture_bundle_id: int,
    identity: _CaptureIdentity,
) -> CustomImportCaptureBundle:
    bundle = (
        await session.execute(
            select(CustomImportCaptureBundle).where(
                CustomImportCaptureBundle.capture_bundle_id == capture_bundle_id,
                CustomImportCaptureBundle.dataset_id == identity.dataset_id,
                CustomImportCaptureBundle.definition_revision_id == identity.definition_revision_id,
                CustomImportCaptureBundle.schema_revision_id == identity.schema_revision_id,
                CustomImportCaptureBundle.capture_state == "sealed",
            )
        )
    ).scalar_one_or_none()
    if bundle is None or bundle.capture_state != "sealed":
        raise CaptureBundleConflict("capture bundle does not match the durable replay identity")
    return bundle


async def _load_replayable_captures_by_slot(
    session: AsyncSession,
    capture_bundle_id: int,
    identity: _CaptureIdentity,
    streams: tuple[tuple[str, int], ...],
) -> dict[int, CustomImportCapture]:
    capture_rows = tuple(
        (
            await session.execute(
                select(CustomImportCapture)
                .where(
                    CustomImportCapture.capture_bundle_id == capture_bundle_id,
                    CustomImportCapture.capture_state == "sealed",
                )
                .order_by(CustomImportCapture.stream_slot)
            )
        )
        .scalars()
        .all()
    )
    captures_by_slot = {capture.stream_slot: capture for capture in capture_rows}
    expected_slots = {stream_slot for _, stream_slot in streams}
    if len(captures_by_slot) != len(capture_rows) or set(captures_by_slot) != expected_slots:
        raise CaptureBundleConflict("durable capture bundle has missing or extra stream captures")
    if any(
        capture.dataset_id != identity.dataset_id
        or capture.definition_revision_id != identity.definition_revision_id
        or capture.schema_revision_id != identity.schema_revision_id
        for capture in capture_rows
    ):
        raise CaptureBundleConflict("durable capture bundle stream identity has drifted")
    return captures_by_slot


def _complete_payload_metadata_by_slot(
    captures_by_slot: Mapping[int, CustomImportCapture],
) -> dict[int, tuple[int, bytes] | None]:
    payload_metadata_by_slot = {
        stream_slot: _durable_payload_metadata(capture) for stream_slot, capture in captures_by_slot.items()
    }
    if all(metadata is None for metadata in payload_metadata_by_slot.values()):
        raise CapturePayloadUnavailable("capture bundle has no durable payload")
    if any(metadata is None for metadata in payload_metadata_by_slot.values()):
        raise CaptureBundleConflict("durable capture bundle has mixed payload availability")
    return payload_metadata_by_slot


async def _iter_replayable_parquet_parts(
    part_rows: Any,
    capture_bundle_id: int,
    receipts_by_slot: Mapping[int, CaptureReceipt],
    metadata_by_slot: Mapping[int, tuple[int, bytes]],
    accounting_by_slot: Mapping[int, Any] | None = None,
) -> AsyncIterator[_VerifiedParquetPart]:
    """Validate each part locally; validate complete coverage only at exhaustion."""

    counts = dict.fromkeys(receipts_by_slot, 0)
    byte_counts = dict.fromkeys(receipts_by_slot, 0)
    digests_by_slot = {slot: hashlib.sha256(_PARQUET_PART_SET_DOMAIN) for slot in receipts_by_slot}
    previous_slot = 0
    async for part_row in part_rows:
        stored_bundle_id, slot, ordinal, byte_count, stored_payload, stored_digest = part_row[:6]
        if (
            isinstance(stored_bundle_id, bool)
            or not isinstance(stored_bundle_id, int)
            or stored_bundle_id != capture_bundle_id
            or isinstance(slot, bool)
            or not isinstance(slot, int)
            or slot not in receipts_by_slot
        ):
            raise CaptureBundleConflict("durable capture bundle has missing or extra payload parts")
        part_payload = _stored_bytes(stored_payload)
        payload_digest = _stored_bytes(stored_digest)
        if (
            slot < previous_slot
            or isinstance(ordinal, bool)
            or not isinstance(ordinal, int)
            or ordinal != counts[slot] + 1
            or ordinal > metadata_by_slot[slot][0]
            or part_payload is None
            or not 1 <= len(part_payload) <= _MAX_PARQUET_PART_BYTES
            or isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count != len(part_payload)
            or payload_digest is None
            or len(payload_digest) != 32
            or hashlib.sha256(part_payload).digest() != payload_digest
        ):
            raise CaptureBundleConflict("durable capture payload part has drifted")
        previous_slot = slot
        counts[slot] += 1
        byte_counts[slot] += byte_count
        if byte_counts[slot] > receipts_by_slot[slot].byte_count:
            raise CaptureBundleConflict("durable capture bundle payload is invalid")
        _add_payload_part_digest(digests_by_slot[slot], ordinal, byte_count, payload_digest)
        manifest = record_count = arrow_byte_count = None
        if accounting_by_slot is not None:
            manifest, record_count, arrow_byte_count = accounting_by_slot[slot].add(
                receipts_by_slot[slot], ordinal, part_payload, part_row[6:]
            )
        yield _VerifiedParquetPart(
            receipts_by_slot[slot], ordinal, part_payload, manifest, record_count, arrow_byte_count
        )

    _verify_complete_part_totals(
        receipts_by_slot, metadata_by_slot, counts, byte_counts, digests_by_slot, accounting_by_slot
    )


def _verify_complete_part_totals(
    receipts_by_slot, metadata_by_slot, counts, byte_counts, digests_by_slot, accounting_by_slot
):
    for slot, receipt in receipts_by_slot.items():
        expected_count, expected_digest = metadata_by_slot[slot]
        if counts[slot] != expected_count:
            raise CaptureBundleConflict("durable capture bundle has an incomplete part count")
        if byte_counts[slot] != receipt.byte_count:
            raise CaptureBundleConflict("durable capture bundle payload is invalid")
        if digests_by_slot[slot].digest() != expected_digest:
            raise CaptureBundleConflict("durable capture payload set digest has drifted")
        if accounting_by_slot is not None:
            accounting_by_slot[slot].finish()


async def _load_parquet_part_metadata(
    session: AsyncSession,
    identity: _CaptureIdentity,
    capture_bundle_id: int,
) -> tuple[dict[int, CaptureReceipt], dict[int, tuple[int, bytes]], Mapping[int, Any] | None]:
    """Freeze and validate complete immutable headers before part iteration."""

    streams = await _validated_streams(session, identity)
    await _validated_replayable_parquet_streams(session, identity, streams)
    bundle = await _load_replayable_bundle_model(session, capture_bundle_id, identity)
    captures_by_slot = await _load_replayable_captures_by_slot(session, capture_bundle_id, identity, streams)
    if bundle.payload_contract == "custom-import/parquet-parts/v2":
        from process.custom_import.capture_pending import _load_segmented_replay_metadata

        try:
            return _load_segmented_replay_metadata(bundle, streams, captures_by_slot)
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            raise CaptureBundleConflict("segmented capture sealed metadata is invalid") from exc
    stored_metadata = _complete_payload_metadata_by_slot(captures_by_slot)
    try:
        receipts = _validate_receipts(
            tuple(
                CaptureReceipt(
                    stream_id=stream_id,
                    source_snapshot_token=bundle.snapshot_token,
                    byte_count=captures_by_slot[slot].byte_count,
                    content_sha256=bytes(captures_by_slot[slot].content_sha256).hex(),
                    canonical_manifest=captures_by_slot[slot].canonical_manifest,
                    manifest_sha256=bytes(captures_by_slot[slot].manifest_sha256).hex(),
                )
                for stream_id, slot in streams
            )
        )
    except (AttributeError, TypeError, ValueError, CaptureStoreError) as exc:
        raise CaptureBundleConflict("durable capture bundle payload is invalid") from exc
    # Freeze immutable values before yielding; never retain mutable ORM rows
    # as authority for a later part or its completion checks.
    receipts_by_slot = {slot: receipt for (_, slot), receipt in zip(streams, receipts, strict=True)}
    metadata_by_slot = {slot: (metadata[0], bytes(metadata[1])) for slot, metadata in stored_metadata.items()}
    if (
        sum(metadata[0] for metadata in metadata_by_slot.values()) > _MAX_PARQUET_PARTS_PER_BUNDLE
        or sum(receipt.byte_count for receipt in receipts) > _MAX_PARQUET_BYTES_PER_BUNDLE
    ):
        raise CaptureStoreError("replayable Parquet captures exceed the aggregate durable payload limit")
    prepared = _prepare_capture_bundle(identity, streams, {receipt.stream_id: receipt for receipt in receipts})
    if not _is_matching_bundle(bundle, prepared):
        raise CaptureBundleConflict("durable capture bundle identity has drifted")
    return receipts_by_slot, metadata_by_slot, None


@asynccontextmanager
async def _open_verified_parquet_parts(
    session: AsyncSession,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
) -> AsyncIterator[AsyncIterator[_VerifiedParquetPart]]:
    """Borrow a session and yield locally validated ordered durable parts.

    The consumer must exhaust the iterator to establish complete store-level
    payload integrity. A yielded receipt is retained connector metadata, not
    proof that its complete capture or bundle has been verified. This function
    does not perform connector-specific acquisition validation or publication.

    The context owns and closes its iterator and SQL result on exhaustion,
    early exit, error, or cancellation. It never closes, commits, or rolls back
    the caller's session. Do not use or commit that session concurrently; after
    driver cancellation, transaction recovery remains the caller's responsibility.
    """

    identity = _identity(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    )
    capture_bundle_id = _positive_id(capture_bundle_id, "capture_bundle_id")
    with session.no_autoflush:
        receipts_by_slot, metadata_by_slot, accounting_by_slot = await _load_parquet_part_metadata(
            session, identity, capture_bundle_id
        )
        part_rows = await session.stream(_part_statement(capture_bundle_id, accounting_by_slot))
    parts = _iter_replayable_parquet_parts(
        part_rows, capture_bundle_id, receipts_by_slot, metadata_by_slot, accounting_by_slot
    )
    try:
        yield parts
    finally:
        try:
            await parts.aclose()
        finally:
            await part_rows.close()


def _part_statement(capture_bundle_id, accounting_by_slot):
    columns = [
        CustomImportCaptureParquetPart.capture_bundle_id,
        CustomImportCaptureParquetPart.stream_slot,
        CustomImportCaptureParquetPart.part_ordinal,
        CustomImportCaptureParquetPart.byte_count,
        CustomImportCaptureParquetPart.payload,
        CustomImportCaptureParquetPart.payload_sha256,
    ]
    if accounting_by_slot is not None:
        columns.extend(
            (
                CustomImportCaptureParquetPart.canonical_capture_manifest,
                CustomImportCaptureParquetPart.capture_manifest_sha256,
                CustomImportCaptureParquetPart.decoded_byte_count,
                CustomImportCaptureParquetPart.arrow_byte_count,
                CustomImportCaptureParquetPart.record_count,
            )
        )
    return (
        select(*columns)
        .where(CustomImportCaptureParquetPart.capture_bundle_id == capture_bundle_id)
        .order_by(CustomImportCaptureParquetPart.stream_slot, CustomImportCaptureParquetPart.part_ordinal)
        .execution_options(yield_per=1)
    )


@asynccontextmanager
async def open_replayable_parquet_parts(
    session: AsyncSession,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
) -> AsyncIterator[AsyncIterator[tuple[CaptureReceipt, int, bytes]]]:
    """Yield validated ordered parts; actual iterator EOF establishes completeness.

    The context closes its iterator/result on every exit and borrows the caller's
    transaction. It does not decode source records or establish publication.
    """

    async with _open_verified_parquet_parts(
        session,
        capture_bundle_id=capture_bundle_id,
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    ) as verified:
        parts = _legacy_part_tuples(verified)
        try:
            yield parts
        finally:
            await parts.aclose()


async def _legacy_part_tuples(verified):
    async for part in verified:
        yield part.receipt, part.ordinal, part.payload


@asynccontextmanager
async def open_segmented_parquet_parts(
    session: AsyncSession,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
) -> AsyncIterator[AsyncIterator[SegmentedParquetPart]]:
    """Yield sealed v2 parts with complete retained manifests and asserted metrics.

    Complete store integrity requires iterator EOF. Actual decoded records and
    whole-part Arrow accounting remain the replay consumer's responsibility.
    """

    async with _open_verified_parquet_parts(
        session,
        capture_bundle_id=capture_bundle_id,
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    ) as verified:
        parts = _segmented_parts(verified)
        try:
            yield parts
        finally:
            await parts.aclose()


async def _segmented_parts(verified):
    async for part in verified:
        if part.capture_manifest is None or part.record_count is None or part.arrow_byte_count is None:
            raise CapturePayloadUnavailable("segmented replay requires a sealed v2 capture")
        yield SegmentedParquetPart(
            part.receipt,
            part.ordinal,
            SealedCapture(part.payload, part.capture_manifest),
            part.record_count,
            part.arrow_byte_count,
        )


@asynccontextmanager
async def open_segmented_cursor_part(
    session: AsyncSession,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    stream_slot: int,
    part_ordinal: int,
) -> AsyncIterator[SegmentedParquetPart]:
    """Read only the durable cursor's part; closing this reader is not stream EOF.

    Complete stream metadata is checked separately after actual final-part
    decode/close. The existing full-bundle reader and generic v1 behavior stay
    unchanged. This borrows, and never commits, the caller's transaction.
    """

    identity = _identity(
        dataset_id=dataset_id, definition_revision_id=definition_revision_id, schema_revision_id=schema_revision_id
    )
    capture_bundle_id = _positive_id(capture_bundle_id, "capture_bundle_id")
    stream_slot = _positive_id(stream_slot, "stream_slot")
    part_ordinal = _positive_id(part_ordinal, "part_ordinal")
    with session.no_autoflush:
        receipts, metadata, accounting = await _load_parquet_part_metadata(session, identity, capture_bundle_id)
        if accounting is None or stream_slot not in receipts or not 1 <= part_ordinal <= metadata[stream_slot][0]:
            raise CapturePayloadUnavailable("cursor replay requires an existing sealed v2 part")
        statement = (
            _part_statement(capture_bundle_id, accounting)
            .where(
                CustomImportCaptureParquetPart.stream_slot == stream_slot,
                CustomImportCaptureParquetPart.part_ordinal == part_ordinal,
            )
            .limit(2)
        )
        part_rows = await session.stream(statement)
    primary = None
    try:
        part = None
        async for part_row in part_rows:
            if part is not None:
                raise CaptureBundleConflict("durable cursor has duplicate parts")
            part = _validated_cursor_part(
                receipts[stream_slot],
                part_row,
                accounting[stream_slot].policy,
                (capture_bundle_id, stream_slot, part_ordinal),
            )
        if part is None:
            raise CapturePayloadUnavailable("durable cursor payload part is missing")
        yield part
    except BaseException as exc:
        primary = exc
    await _close_cursor_result(part_rows, primary)


def _add_segmented_metadata(accounting, receipt, ordinal, byte_count, payload_digest, metadata):
    """Verify the seal without rereading previously decoded immutable payloads."""

    from process.custom_import.capture_pending import _ACCOUNTING_FIELDS, _count

    canonical, stored_digest, decoded, arrow, record_count = metadata
    canonical = _canonical_manifest(canonical)
    manifest_bytes = canonical.encode("utf-8")
    manifest_digest = hashlib.sha256(manifest_bytes).digest()
    policy = accounting.policy
    if len(manifest_bytes) > policy.maximum_part_manifest_bytes or not _is_stored_digest_equal(
        stored_digest, manifest_digest
    ):
        raise CaptureBundleConflict("segmented capture per-part manifest digest has drifted")
    try:
        manifest = CaptureManifest(**json.loads(canonical))
    except TypeError as exc:
        raise CaptureBundleConflict("segmented capture per-part manifest shape has drifted") from exc
    if (
        manifest.stream_id != receipt.stream_id
        or manifest.source_snapshot_token != receipt.source_snapshot_token
        or manifest.format != "parquet"
        or manifest.compression != "none"
        or type(manifest.compressed_bytes) is not int
        or type(manifest.decoded_bytes) is not int
        or manifest.compressed_bytes != byte_count
        or manifest.decoded_bytes != decoded
        or decoded != byte_count
        or manifest.compressed_sha256 != payload_digest.hex()
        or manifest.decoded_sha256 != payload_digest.hex()
    ):
        raise CaptureBundleConflict("segmented capture per-part manifest identity has drifted")
    for name in ("stream_sha256", "compressed_sha256", "decoded_sha256", "capture_sha256"):
        _sha256(getattr(manifest, name), name)
    _count(decoded, "decoded_byte_count", policy.part_limits.maximum_decoded_bytes)
    _count(arrow, "arrow_byte_count", policy.maximum_part_arrow_bytes)
    _count(record_count, "record_count", policy.part_limits.maximum_records)
    if not 1 <= byte_count <= policy.part_limits.maximum_compressed_bytes:
        raise CaptureBundleConflict("segmented capture per-part bytes exceed its policy")
    _add_payload_part_digest(accounting.digest, ordinal, len(manifest_bytes), manifest_digest)
    for increment in (decoded, arrow, record_count):
        accounting.digest.update(increment.to_bytes(8, "big"))
    for name, increment in zip(
        _ACCOUNTING_FIELDS, (1, byte_count, decoded, arrow, record_count, len(manifest_bytes)), strict=True
    ):
        accounting.totals_by_field[name] += increment
        if accounting.totals_by_field[name] > accounting.expected_by_field[name]:
            raise CaptureBundleConflict("segmented capture part accounting exceeds its sealed receipt")


async def _close_cursor_result(part_rows, primary=None):
    """Drain cursor-result cleanup even if its caller is canceled again."""

    task = asyncio.create_task(part_rows.close())
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if primary is None:
                primary = exc
            else:
                primary._custom_import_retry_blocked = True
        except BaseException:
            break
    try:
        task.result()
    except BaseException as exc:
        if primary is None:
            primary = exc
        elif primary is not exc:
            primary._custom_import_retry_blocked = True
            primary.add_note(f"cursor reader cleanup also failed: {type(exc).__name__}")
            primary.__cause__ = exc
    if primary is not None:
        raise primary


async def verify_segmented_stream_metadata(
    session: AsyncSession, *, stream_slot: int, deadline: float, **identity_values
) -> None:
    """Verify ordered full-stream seals at SQL EOF, without fetching any payload.

    This is not a substitute for actual per-part decoding. The SOURCE consumer
    may use it only after its durable prefix and final part were decoded and
    closed successfully. No early-close or caller EOF claim grants completion.
    """

    bundle_id = _positive_id(identity_values.pop("capture_bundle_id"), "capture_bundle_id")
    identity = _identity(**identity_values)
    stream_slot = _positive_id(stream_slot, "stream_slot")
    with session.no_autoflush:
        receipts, metadata, accounting = await _load_parquet_part_metadata(session, identity, bundle_id)
        if accounting is None or stream_slot not in receipts:
            raise CapturePayloadUnavailable("stream verification requires a sealed v2 capture")
        statement = _segment_metadata_statement(bundle_id, accounting, stream_slot)
        part_rows = await session.stream(statement)
    count, byte_total = 0, 0
    digest = hashlib.sha256(_PARQUET_PART_SET_DOMAIN)
    primary = None
    try:
        async for part_row in part_rows:
            if time.monotonic() >= deadline:
                raise CaptureStoreError("segmented metadata verification deadline elapsed")
            stored_bundle, slot, ordinal, byte_count, payload_digest = part_row[:5]
            payload_digest = _stored_bytes(payload_digest)
            if (
                (stored_bundle, slot, ordinal) != (bundle_id, stream_slot, count + 1)
                or count >= metadata[stream_slot][0]
                or type(byte_count) is not int
                or payload_digest is None
                or len(payload_digest) != 32
            ):
                raise CaptureBundleConflict("segmented stream ordered metadata coverage has drifted")
            _add_segmented_metadata(accounting[slot], receipts[slot], ordinal, byte_count, payload_digest, part_row[5:])
            _add_payload_part_digest(digest, ordinal, byte_count, payload_digest)
            count += 1
            byte_total += byte_count
        if (count, byte_total, digest.digest()) != (
            metadata[stream_slot][0],
            receipts[stream_slot].byte_count,
            metadata[stream_slot][1],
        ):
            raise CaptureBundleConflict("segmented stream complete metadata coverage has drifted")
        accounting[stream_slot].finish()
    except BaseException as exc:
        primary = exc
    await _close_cursor_result(part_rows, primary)


def _validated_cursor_part(receipt, part_row, policy, expected_identity):
    """Validate one complete encoded part before handing it to the decoder."""
    from process.custom_import.capture_pending import _validated_part_accounting

    bundle_id, slot, ordinal, byte_count, part_payload, digest = part_row[:6]
    part_payload, digest = _stored_bytes(part_payload), _stored_bytes(digest)
    if (
        (bundle_id, slot, ordinal) != expected_identity
        or type(byte_count) is not int
        or part_payload is None
        or byte_count != len(part_payload)
        or digest is None
        or hashlib.sha256(part_payload).digest() != digest
    ):
        raise CaptureBundleConflict("durable cursor payload part has drifted")
    manifest, _, _, arrow, record_count = _validated_part_accounting(receipt, part_payload, part_row[6:], policy)
    return SegmentedParquetPart(receipt, ordinal, SealedCapture(part_payload, manifest), record_count, arrow)


def _segment_metadata_statement(bundle_id, accounting, stream_slot):
    """Select seals/accounting only; never prior immutable payload bytes."""
    statement = _part_statement(bundle_id, accounting).where(CustomImportCaptureParquetPart.stream_slot == stream_slot)
    return statement.with_only_columns(*(column for column in statement.selected_columns if column.name != "payload"))


async def load_replayable_parquet_bundle(
    session: AsyncSession,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
) -> tuple[ReplayableParquetCapture, ...]:
    """Load an exact complete durable bundle through the incremental verifier."""

    receipts_by_stream: dict[str, CaptureReceipt] = {}
    parts_by_stream: dict[str, list[bytes]] = {}
    async with _open_verified_parquet_parts(
        session,
        capture_bundle_id=capture_bundle_id,
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    ) as parts:
        async for part in parts:
            if part.capture_manifest is not None:
                raise CaptureStoreError("segmented captures require incremental replay")
            receipt = part.receipt
            receipts_by_stream[receipt.stream_id] = receipt
            parts_by_stream.setdefault(receipt.stream_id, []).append(part.payload)
    return _validate_replayable_parquet_captures(
        tuple(
            ReplayableParquetCapture(receipt=receipt, parts=tuple(parts_by_stream[stream_id]))
            for stream_id, receipt in receipts_by_stream.items()
        )
    )
