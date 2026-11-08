# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Caller-owned retention and verified replay of original source objects."""

from __future__ import annotations

import json
from dataclasses import asdict

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import CustomImportCaptureParquetPart, CustomImportDefinitionRevision
from db.models.custom_import_raw_capture import (
    MAX_RAW_OBJECT_BYTES,
    RAW_OBJECT_CONTRACT,
    CustomImportCaptureRawObject,
)
from process.custom_import import capture_retention, capture_store
from process.custom_import.capture import CaptureError, CaptureManifest, SealedCapture, verify_capture
from process.custom_import.capture_authority import require_raw_capture_authority
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.capture_store import (
    CaptureBundleConflict,
    CaptureBundleRegistration,
    CapturePayloadUnavailable,
    CaptureReceipt,
    CaptureStoreError,
)
from process.custom_import.definition import MAX_CHILD_COLLECTIONS, CustomImportDefinition, canonical_json

__all__ = ("register_raw_capture_bundle", "load_replayable_raw_bundle")

_DEFAULT_LIMITS = CaptureLimits()
_MAX_BUNDLE_BYTES = 128 * 1024 * 1024
_RAW = CustomImportCaptureRawObject
_COPY_COLUMNS = (
    "capture_bundle_id",
    "stream_slot",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "payload_contract",
    "byte_count",
    "payload_sha256",
    "payload",
)
_METADATA_COLUMNS = tuple(getattr(_RAW, name) for name in _COPY_COLUMNS[:-1])


def _checked_limits(limits):
    if not isinstance(limits, CaptureLimits):
        raise CaptureStoreError("raw capture limits are invalid")
    checked = CaptureLimits(**asdict(limits))
    if any(value > getattr(_DEFAULT_LIMITS, name) for name, value in asdict(checked).items()):
        raise CaptureStoreError("raw capture limits may only tighten defaults")
    return checked


def _check_manifest_sizes(manifests, limits):
    for manifest in manifests:
        for size, maximum in (
            (manifest.compressed_bytes, min(MAX_RAW_OBJECT_BYTES, limits.maximum_compressed_bytes)),
            (manifest.decoded_bytes, limits.maximum_decoded_bytes),
        ):
            if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= maximum:
                raise CaptureStoreError("raw capture exceeds its object byte limit")
    if (
        sum(manifest.compressed_bytes for manifest in manifests)
        > min(_MAX_BUNDLE_BYTES, limits.maximum_compressed_bytes)
        or sum(manifest.decoded_bytes for manifest in manifests) > limits.maximum_decoded_bytes
    ):
        raise CaptureStoreError("raw capture bundle exceeds its aggregate byte limit")


def _receipt(manifest):
    return CaptureReceipt(
        stream_id=manifest.stream_id,
        source_snapshot_token=manifest.source_snapshot_token,
        byte_count=manifest.compressed_bytes,
        content_sha256=manifest.compressed_sha256,
        canonical_manifest=canonical_json({"contract": RAW_OBJECT_CONTRACT, "capture": asdict(manifest)}),
        manifest_sha256=manifest.capture_sha256,
    )


def _checked_captures(captures, limits):
    if not isinstance(captures, tuple) or not 1 <= len(captures) <= MAX_CHILD_COLLECTIONS + 1:
        raise CaptureStoreError("raw captures require a bounded nonempty tuple")
    if any(
        not isinstance(capture, SealedCapture)
        or not isinstance(capture.manifest, CaptureManifest)
        or not isinstance(capture.payload, bytes)
        or len(capture.payload) != capture.manifest.compressed_bytes
        for capture in captures
    ):
        raise CaptureStoreError("raw captures require exact sealed original bytes")
    _check_manifest_sizes(tuple(capture.manifest for capture in captures), limits)
    return capture_store._validate_receipts(tuple(_receipt(capture.manifest) for capture in captures))


async def _definition(session, identity):
    model = await session.get(CustomImportDefinitionRevision, identity.definition_revision_id)
    if model is None or (model.dataset_id, model.schema_revision_id) != (
        identity.dataset_id,
        identity.schema_revision_id,
    ):
        raise CaptureBundleConflict("raw capture identity does not match a persisted definition")
    try:
        return CustomImportDefinition.from_json(model.canonical_definition)
    except (TypeError, ValueError) as exc:
        raise CaptureBundleConflict("raw capture definition is invalid") from exc


def _verify_captures(captures, definition, limits):
    by_stream = {capture.manifest.stream_id: capture for capture in captures}
    if len(by_stream) != len(captures) or set(by_stream) != {stream.stream_id for stream in definition.source_streams}:
        raise CaptureBundleConflict("raw captures do not exactly cover the persisted source streams")
    try:
        for stream in definition.source_streams:
            verify_capture(by_stream[stream.stream_id], stream, limits=limits)
    except CaptureError as exc:
        raise CaptureBundleConflict("raw capture does not match its sealed source manifest") from exc
    return by_stream


async def register_raw_capture_bundle(
    session: AsyncSession,
    *,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    captures: tuple[SealedCapture, ...],
    maximum_dataset_retained_bytes: int,
    limits: CaptureLimits = _DEFAULT_LIMITS,
) -> CaptureBundleRegistration:
    """Register, COPY and read back originals in one caller-owned transaction.

    An existing metadata-only bundle cannot be backfilled. The caller must roll
    back the transaction on any failure and bind the execution before committing.
    """

    capture_store._require_transaction(session)
    capture_store._require_clean_session(session)
    identity = capture_store._identity(
        dataset_id=dataset_id, definition_revision_id=definition_revision_id, schema_revision_id=schema_revision_id
    )
    if (
        isinstance(maximum_dataset_retained_bytes, bool)
        or not isinstance(maximum_dataset_retained_bytes, int)
        or not 0 <= maximum_dataset_retained_bytes <= capture_store._MAX_BIGINT
    ):
        raise CaptureStoreError("maximum_dataset_retained_bytes must be a bounded non-negative integer")
    limits = _checked_limits(limits)
    receipts = _checked_captures(captures, limits)
    definition = await _definition(session, identity)
    captures_by_stream = _verify_captures(captures, definition, limits)
    registration = await capture_store.register_capture_bundle(session, **asdict(identity), receipts=receipts)
    if registration.created:
        await require_raw_capture_authority(session)
        await capture_retention.validate_dataset_retained_bytes(
            session,
            dataset_id=dataset_id,
            new_byte_count=sum(len(capture.payload) for capture in captures),
            maximum_retained_bytes=maximum_dataset_retained_bytes,
        )
        await _copy_raw_objects(session, registration, identity, captures_by_stream)
    retained = await load_replayable_raw_bundle(
        session, capture_bundle_id=registration.capture_bundle_id, **asdict(identity), limits=limits
    )
    if {capture.manifest.stream_id: capture for capture in retained} != captures_by_stream:
        raise CaptureBundleConflict("raw capture readback differs from the submitted originals")
    return registration


async def _copy_raw_objects(session, registration, identity, captures_by_stream):
    streams = await capture_store._validated_streams(session, identity)
    if tuple(slot for _, slot in streams) != registration.stream_slots:
        raise CaptureBundleConflict("raw capture registration stream slots have drifted")
    copy_records = tuple(
        (
            registration.capture_bundle_id,
            slot,
            identity.dataset_id,
            identity.definition_revision_id,
            identity.schema_revision_id,
            RAW_OBJECT_CONTRACT,
            len(capture.payload),
            bytes.fromhex(capture.manifest.compressed_sha256),
            capture.payload,
        )
        for stream_id, slot in streams
        for capture in (captures_by_stream[stream_id],)
    )
    connection = await session.connection()
    schema_translate_map = connection.sync_connection.get_execution_options().get("schema_translate_map")
    schema_name = _RAW.__table__.schema
    if schema_translate_map is not None and (schema_name is not None or None in schema_translate_map):
        schema_name = schema_translate_map.get(schema_name, schema_name)
        if not schema_name:
            # SQLAlchemy renders an explicit None translation using its dialect default.
            schema_name = connection.sync_connection.dialect.default_schema_name
            if not schema_name:
                raise CaptureStoreError("translated raw capture default schema is unavailable")
    raw = await connection.get_raw_connection()
    await raw.driver_connection.copy_records_to_table(
        _RAW.__tablename__, schema_name=schema_name, columns=_COPY_COLUMNS, records=copy_records
    )


def _retained_manifests(bundle, capture_headers_by_slot, streams, identity, limits):
    if bundle.payload_contract is not None or any(
        any(
            getattr(capture_header, name) is not None
            for name in (
                "payload_contract",
                "payload_part_count",
                "payload_set_sha256",
                "manifest_set_sha256",
                "eof_at",
            )
        )
        for capture_header in capture_headers_by_slot.values()
    ):
        raise CaptureBundleConflict("raw capture bundle mixes payload contracts")
    try:
        manifests_by_slot = {}
        receipts = []
        for stream_id, slot in streams:
            capture_header = capture_headers_by_slot[slot]
            receipt = CaptureReceipt(
                stream_id=stream_id,
                source_snapshot_token=bundle.snapshot_token,
                byte_count=capture_header.byte_count,
                content_sha256=capture_store._stored_bytes(capture_header.content_sha256).hex(),
                canonical_manifest=capture_header.canonical_manifest,
                manifest_sha256=capture_store._stored_bytes(capture_header.manifest_sha256).hex(),
            )
            document = json.loads(receipt.canonical_manifest)
            if set(document) != {"contract", "capture"} or document["contract"] != RAW_OBJECT_CONTRACT:
                raise CaptureBundleConflict("raw capture manifest contract is invalid")
            manifest = CaptureManifest(**document["capture"])
            if _receipt(manifest) != receipt:
                raise CaptureBundleConflict("raw capture receipt and original manifest disagree")
            manifests_by_slot[slot] = manifest
            receipts.append(receipt)
        validated = capture_store._validate_receipts(tuple(receipts))
        prepared = capture_store._prepare_capture_bundle(
            identity, streams, {receipt.stream_id: receipt for receipt in validated}
        )
        if not capture_store._is_matching_bundle(bundle, prepared):
            raise CaptureBundleConflict("raw capture bundle manifest has drifted")
        _check_manifest_sizes(tuple(manifests_by_slot.values()), limits)
        return manifests_by_slot
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, CaptureStoreError):
            raise
        raise CaptureBundleConflict("raw capture retained manifest is invalid") from exc


async def _raw_metadata(session, bundle_id, identity, manifests_by_slot):
    has_parquet = select(CustomImportCaptureParquetPart.stream_slot).where(
        CustomImportCaptureParquetPart.capture_bundle_id == bundle_id
    )
    raw_metadata_rows = tuple(
        (
            await session.execute(
                select(*_METADATA_COLUMNS, func.octet_length(_RAW.payload), has_parquet.exists())
                .where(_RAW.capture_bundle_id == bundle_id)
                .order_by(_RAW.stream_slot)
                .limit(MAX_CHILD_COLLECTIONS + 2)
            )
        ).all()
    )
    if not raw_metadata_rows:
        raise CapturePayloadUnavailable("capture bundle has no retained original payload")
    expected_metadata_rows = tuple(
        (
            bundle_id,
            slot,
            identity.dataset_id,
            identity.definition_revision_id,
            identity.schema_revision_id,
            RAW_OBJECT_CONTRACT,
            manifest.compressed_bytes,
            bytes.fromhex(manifest.compressed_sha256),
        )
        for slot, manifest in manifests_by_slot.items()
    )
    if len(raw_metadata_rows) != len(expected_metadata_rows) or any(
        tuple(raw_metadata[:8]) != expected_metadata or raw_metadata[8] != expected_metadata[6] or raw_metadata[9]
        for raw_metadata, expected_metadata in zip(raw_metadata_rows, expected_metadata_rows, strict=True)
    ):
        raise CaptureBundleConflict("raw payload metadata is incomplete, mixed or belongs to another owner")
    return expected_metadata_rows


async def load_replayable_raw_bundle(
    session: AsyncSession,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    limits: CaptureLimits = _DEFAULT_LIMITS,
) -> tuple[SealedCapture, ...]:
    """Preflight the entire retained bundle before fetching any original bytes."""

    identity = capture_store._identity(
        dataset_id=dataset_id, definition_revision_id=definition_revision_id, schema_revision_id=schema_revision_id
    )
    bundle_id = capture_store._positive_id(capture_bundle_id, "capture_bundle_id")
    limits = _checked_limits(limits)
    with session.no_autoflush:
        streams = await capture_store._validated_streams(session, identity)
        definition = await _definition(session, identity)
        bundle = await capture_store._load_replayable_bundle_model(session, bundle_id, identity)
        capture_headers_by_slot = await capture_store._load_replayable_captures_by_slot(
            session, bundle_id, identity, streams
        )
        manifests_by_slot = _retained_manifests(bundle, capture_headers_by_slot, streams, identity, limits)
        metadata_rows = await _raw_metadata(session, bundle_id, identity, manifests_by_slot)
        # Freeze every header in the predicate: changed rows cannot enlarge the
        # preflighted payload read or silently replace its content identity.
        header_predicates = [
            and_(*(column == header_value for column, header_value in zip(_METADATA_COLUMNS, header_row, strict=True)))
            for header_row in metadata_rows
        ]
        statement = (
            select(*_METADATA_COLUMNS, _RAW.payload)
            .where(
                or_(*header_predicates),
                func.octet_length(_RAW.payload) == _RAW.byte_count,
            )
            .order_by(_RAW.stream_slot)
        )
        payload_rows = tuple((await session.execute(statement)).all())
    if len(payload_rows) != len(metadata_rows) or any(
        tuple(payload_row[:8]) != expected_metadata
        for payload_row, expected_metadata in zip(payload_rows, metadata_rows, strict=True)
    ):
        raise CaptureBundleConflict("raw payload rows changed after metadata validation")
    captures = tuple(
        SealedCapture(payload=payload_row[8], manifest=manifests_by_slot[payload_row[1]])
        for payload_row in payload_rows
    )
    _verify_captures(captures, definition, limits)
    return captures
