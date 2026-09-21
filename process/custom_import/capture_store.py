# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Caller-owned durable registration for sealed custom-import captures."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportDataset,
    CustomImportDefinitionRevision,
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
    "CaptureReceipt",
    "CaptureStoreError",
    "CaptureStoreTransactionRequired",
    "register_capture_bundle",
)


_CAPTURE_STORE_CONTRACT = "custom-import/capture-store/v1"
_MAX_BIGINT = 2**63 - 1
_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class CaptureStoreError(ValueError):
    """A capture receipt or persisted capture bundle is not safe to retain."""


class CaptureStoreTransactionRequired(CaptureStoreError):
    """Registration needs one active caller-owned transaction."""


class CaptureBundleConflict(CaptureStoreError):
    """An existing source snapshot is not an exact replay of this bundle."""


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


def _index_receipts_by_stream(
    streams: tuple[tuple[str, int], ...],
    receipts: tuple[CaptureReceipt, ...],
) -> dict[str, CaptureReceipt]:
    receipts_by_stream = {receipt.stream_id: receipt for receipt in receipts}
    if set(receipts_by_stream) != {stream_id for stream_id, _ in streams}:
        raise CaptureStoreError("capture receipts do not exactly cover persisted definition source streams")
    return receipts_by_stream


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
