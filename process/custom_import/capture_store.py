# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Caller-owned durable registration for sealed custom-import captures."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
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
    "load_replayable_parquet_bundle",
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
        digest.update(ordinal.to_bytes(4, byteorder="big", signed=False))
        digest.update(len(part).to_bytes(8, byteorder="big", signed=False))
        digest.update(hashlib.sha256(part).digest())
    return digest.digest()


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


async def _is_matching_replayable_parquet_rows(
    session: AsyncSession,
    bundle: CustomImportCaptureBundle,
    prepared: _PreparedCaptureBundle,
    captures_by_stream: Mapping[str, ReplayableParquetCapture],
) -> bool | None:
    result = await session.execute(
        select(CustomImportCapture)
        .where(CustomImportCapture.capture_bundle_id == bundle.capture_bundle_id)
        .with_for_update()
    )
    captures = tuple(result.scalars().all())
    persisted_by_slot = {capture.stream_slot: capture for capture in captures}
    if len(persisted_by_slot) != len(captures) or len(captures) != len(prepared.streams):
        return False
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

    part_result = await session.execute(
        select(CustomImportCaptureParquetPart)
        .where(CustomImportCaptureParquetPart.capture_bundle_id == bundle.capture_bundle_id)
        .order_by(CustomImportCaptureParquetPart.stream_slot, CustomImportCaptureParquetPart.part_ordinal)
        .with_for_update()
    )
    parts_by_slot: dict[int, list[CustomImportCaptureParquetPart]] = {}
    for part in part_result.scalars().all():
        parts_by_slot.setdefault(part.stream_slot, []).append(part)
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
            payload = _stored_bytes(part.payload)
            if (
                part.part_ordinal != ordinal
                or part.byte_count != len(expected_payload)
                or payload != expected_payload
                or not _is_stored_digest_equal(part.payload_sha256, hashlib.sha256(expected_payload).digest())
                or payload is None
                or hashlib.sha256(payload).digest() != hashlib.sha256(expected_payload).digest()
            ):
                return False
    return True


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
    bundle = CustomImportCaptureBundle(
        dataset_id=identity.dataset_id,
        definition_revision_id=identity.definition_revision_id,
        schema_revision_id=identity.schema_revision_id,
        snapshot_token=prepared.snapshot_token,
        snapshot_token_sha256=prepared.snapshot_token_sha256,
        canonical_manifest=prepared.canonical_manifest,
        manifest_sha256=prepared.manifest_sha256,
        stream_count=len(prepared.streams),
    )
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
    identity = prepared.identity
    bundle = CustomImportCaptureBundle(
        dataset_id=identity.dataset_id,
        definition_revision_id=identity.definition_revision_id,
        schema_revision_id=identity.schema_revision_id,
        snapshot_token=prepared.snapshot_token,
        snapshot_token_sha256=prepared.snapshot_token_sha256,
        canonical_manifest=prepared.canonical_manifest,
        manifest_sha256=prepared.manifest_sha256,
        stream_count=len(prepared.streams),
    )
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
            byte_count=len(payload),
            payload=payload,
            payload_sha256=hashlib.sha256(payload).digest(),
        )
        for stream_id, stream_slot in prepared.streams
        for ordinal, payload in enumerate(captures_by_stream[stream_id].parts, start=1)
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


async def load_replayable_parquet_bundle(
    session: AsyncSession,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
) -> tuple[ReplayableParquetCapture, ...]:
    """Load one exact durable bundle as in-memory parts, never a filesystem locator."""

    identity = _identity(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    )
    capture_bundle_id = _positive_id(capture_bundle_id, "capture_bundle_id")
    streams = await _validated_streams(session, identity)
    await _validated_replayable_parquet_streams(session, identity, streams)
    bundle = (
        await session.execute(
            select(CustomImportCaptureBundle).where(
                CustomImportCaptureBundle.capture_bundle_id == capture_bundle_id,
                CustomImportCaptureBundle.dataset_id == identity.dataset_id,
                CustomImportCaptureBundle.definition_revision_id == identity.definition_revision_id,
                CustomImportCaptureBundle.schema_revision_id == identity.schema_revision_id,
            )
        )
    ).scalar_one_or_none()
    if bundle is None:
        raise CaptureBundleConflict("capture bundle does not match the durable replay identity")
    capture_rows = tuple(
        (
            await session.execute(
                select(CustomImportCapture)
                .where(CustomImportCapture.capture_bundle_id == capture_bundle_id)
                .order_by(CustomImportCapture.stream_slot)
            )
        )
        .scalars()
        .all()
    )
    capture_by_slot = {capture.stream_slot: capture for capture in capture_rows}
    expected_slots = {stream_slot for _, stream_slot in streams}
    if len(capture_by_slot) != len(capture_rows) or set(capture_by_slot) != expected_slots:
        raise CaptureBundleConflict("durable capture bundle has missing or extra stream captures")
    if any(
        capture.dataset_id != identity.dataset_id
        or capture.definition_revision_id != identity.definition_revision_id
        or capture.schema_revision_id != identity.schema_revision_id
        for capture in capture_rows
    ):
        raise CaptureBundleConflict("durable capture bundle stream identity has drifted")
    payload_metadata_by_slot = {
        stream_slot: _durable_payload_metadata(capture) for stream_slot, capture in capture_by_slot.items()
    }
    if all(metadata is None for metadata in payload_metadata_by_slot.values()):
        raise CapturePayloadUnavailable("capture bundle has no durable payload")
    if any(metadata is None for metadata in payload_metadata_by_slot.values()):
        raise CaptureBundleConflict("durable capture bundle has mixed payload availability")

    part_rows = tuple(
        (
            await session.execute(
                select(CustomImportCaptureParquetPart)
                .where(CustomImportCaptureParquetPart.capture_bundle_id == capture_bundle_id)
                .order_by(CustomImportCaptureParquetPart.stream_slot, CustomImportCaptureParquetPart.part_ordinal)
            )
        )
        .scalars()
        .all()
    )
    parts_by_slot: dict[int, list[CustomImportCaptureParquetPart]] = {}
    for part in part_rows:
        parts_by_slot.setdefault(part.stream_slot, []).append(part)
    if set(parts_by_slot) != expected_slots:
        raise CaptureBundleConflict("durable capture bundle has missing or extra payload parts")

    replayable_captures: list[ReplayableParquetCapture] = []
    receipts_by_stream: dict[str, CaptureReceipt] = {}
    try:
        for stream_id, stream_slot in streams:
            capture = capture_by_slot[stream_slot]
            metadata = payload_metadata_by_slot[stream_slot]
            assert metadata is not None
            part_count, expected_set_sha256 = metadata
            persisted_parts = parts_by_slot[stream_slot]
            if len(persisted_parts) != part_count:
                raise CaptureBundleConflict("durable capture bundle has an incomplete part count")
            payloads: list[bytes] = []
            for ordinal, part in enumerate(persisted_parts, start=1):
                payload = _stored_bytes(part.payload)
                digest = _stored_bytes(part.payload_sha256)
                if (
                    part.part_ordinal != ordinal
                    or payload is None
                    or part.byte_count != len(payload)
                    or digest is None
                    or len(digest) != 32
                    or hashlib.sha256(payload).digest() != digest
                ):
                    raise CaptureBundleConflict("durable capture payload part has drifted")
                payloads.append(payload)
            parts = tuple(payloads)
            if _payload_set_sha256(parts) != expected_set_sha256:
                raise CaptureBundleConflict("durable capture payload set digest has drifted")
            receipt = CaptureReceipt(
                stream_id=stream_id,
                source_snapshot_token=bundle.snapshot_token,
                byte_count=capture.byte_count,
                content_sha256=bytes(capture.content_sha256).hex(),
                canonical_manifest=capture.canonical_manifest,
                manifest_sha256=bytes(capture.manifest_sha256).hex(),
            )
            replayable = ReplayableParquetCapture(receipt=receipt, parts=parts)
            receipts_by_stream[stream_id] = receipt
            replayable_captures.append(replayable)
    except (AttributeError, TypeError, ValueError, CaptureStoreError) as exc:
        if isinstance(exc, CaptureBundleConflict):
            raise
        raise CaptureBundleConflict("durable capture bundle payload is invalid") from exc

    prepared = _prepare_capture_bundle(identity, streams, receipts_by_stream)
    if not _is_matching_bundle(bundle, prepared):
        raise CaptureBundleConflict("durable capture bundle identity has drifted")
    return _validate_replayable_parquet_captures(tuple(replayable_captures))
