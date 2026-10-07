# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fenced, caller-owned incremental retention of one segmented capture."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportDefinitionRevision,
    CustomImportLease,
    CustomImportSourceBindingRevision,
)
from process.custom_import import execution as lifecycle
from process.custom_import.capture import CaptureError, CaptureManifest, SealedCapture, verify_capture
from process.custom_import.capture_store import (
    CaptureBundleConflict,
    CaptureBundleRegistration,
    CaptureReceipt,
    CaptureStoreError,
    _add_payload_part_digest,
    _canonical_manifest,
    _identity,
    _is_stored_digest_equal,
    _positive_id,
    _require_clean_session,
    _require_transaction,
    _sha256,
    _snapshot_token,
    _validated_replayable_parquet_streams,
    _validated_streams,
)
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy

SEGMENTED_PAYLOAD_CONTRACT = "custom-import/parquet-parts/v2"
_MANIFEST_SET_DOMAIN = b"custom-import/parquet-manifest-accounting/v1\0"
_ACCOUNTING_FIELDS = (
    "part_count",
    "byte_count",
    "decoded_byte_count",
    "arrow_byte_count",
    "record_count",
    "manifest_byte_count",
)
_BUDGET_FIELDS = (
    "maximum_parts",
    "maximum_compressed_bytes",
    "maximum_decoded_bytes",
    "maximum_arrow_bytes",
    "maximum_records",
    "maximum_manifest_bytes",
)


def _digest(value: object, label: str) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)) or len(value) != 32:
        raise CaptureStoreError(f"{label} must contain exactly 32 bytes")
    return bytes(value)


def _count(value: object, label: str, maximum: int = (1 << 63) - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise CaptureStoreError(f"{label} must be a bounded non-negative integer")
    return value


@dataclass(frozen=True)
class PendingCaptureRequest:
    """Immutable exact attempt identity; the policy itself grants no admission."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    execution_id: int
    fence: int
    token: bytes = field(repr=False)
    request_identity_sha256: bytes
    source_request_sha256: bytes
    statement_sha256: bytes
    source_snapshot_token: str
    policy: SegmentedCapturePolicy
    source_binding_revision_id: int | None = None
    source_binding_sha256: bytes | None = None

    def __post_init__(self) -> None:
        for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id", "fence"):
            _positive_id(getattr(self, name), name)
        lifecycle.lease_token_sha256(self.token)
        if not isinstance(self.token, (bytes, bytearray, memoryview)):
            raise CaptureStoreError("pending capture token must be bytes-like")
        object.__setattr__(self, "token", bytes(self.token))
        for name in ("request_identity_sha256", "source_request_sha256", "statement_sha256"):
            object.__setattr__(self, name, _digest(getattr(self, name), name))
        object.__setattr__(self, "source_snapshot_token", _snapshot_token(self.source_snapshot_token))
        if not isinstance(self.policy, SegmentedCapturePolicy):
            raise CaptureStoreError("pending capture requires an explicit segmented policy")
        object.__setattr__(self, "policy", SegmentedCapturePolicy.from_mapping(self.policy.to_mapping()))
        if (self.source_binding_revision_id is None) != (self.source_binding_sha256 is None):
            raise CaptureStoreError("source binding identifier and digest must be paired")
        if self.source_binding_revision_id is not None:
            _positive_id(self.source_binding_revision_id, "source_binding_revision_id")
            object.__setattr__(
                self, "source_binding_sha256", _digest(self.source_binding_sha256, "source_binding_sha256")
            )

    @property
    def identity(self):
        """Return the exact retained definition and schema owner identifiers."""

        return _identity(
            dataset_id=self.dataset_id,
            definition_revision_id=self.definition_revision_id,
            schema_revision_id=self.schema_revision_id,
        )


def _request(session: AsyncSession, request: PendingCaptureRequest) -> PendingCaptureRequest:
    _require_transaction(session)
    _require_clean_session(session)
    if not isinstance(request, PendingCaptureRequest):
        raise CaptureStoreError("pending capture requires an exact request")
    return replace(request)


async def _lock_execution(session: AsyncSession, request: PendingCaptureRequest):
    context = await lifecycle._lock_current_capture_binding(
        session,
        execution_id=request.execution_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        fence=request.fence,
        token_sha256=lifecycle.lease_token_sha256(request.token),
    )
    if context is None:
        raise CaptureBundleConflict("pending capture requires a current running lease")
    execution, _, _ = context
    if not _is_stored_digest_equal(execution.request_identity_sha256, request.request_identity_sha256):
        raise CaptureBundleConflict("pending capture execution request has drifted")
    if execution.source_binding_revision_id != request.source_binding_revision_id:
        raise CaptureBundleConflict("pending capture source binding has drifted")
    if request.source_binding_revision_id is not None:
        binding = await session.get(CustomImportSourceBindingRevision, request.source_binding_revision_id)
        if binding is None or (
            (binding.dataset_id, binding.definition_revision_id, binding.schema_revision_id)
            != (request.dataset_id, request.definition_revision_id, request.schema_revision_id)
            or not _is_stored_digest_equal(binding.binding_sha256, request.source_binding_sha256)
        ):
            raise CaptureBundleConflict("pending capture source binding has drifted")
    return execution


def _validate_bundle(bundle: CustomImportCaptureBundle, request: PendingCaptureRequest) -> None:
    expected_by_field = {
        "dataset_id": request.dataset_id,
        "definition_revision_id": request.definition_revision_id,
        "schema_revision_id": request.schema_revision_id,
        "payload_contract": SEGMENTED_PAYLOAD_CONTRACT,
        "producing_execution_id": request.execution_id,
        "producing_fence": request.fence,
        "source_binding_revision_id": request.source_binding_revision_id,
        "snapshot_token": request.source_snapshot_token,
        "canonical_policy": request.policy.canonical,
    }
    digests_by_field = {
        "producing_token_sha256": lifecycle.lease_token_sha256(request.token),
        "request_identity_sha256": request.request_identity_sha256,
        "source_request_sha256": request.source_request_sha256,
        "statement_sha256": request.statement_sha256,
        "policy_sha256": bytes.fromhex(request.policy.digest),
        "snapshot_token_sha256": hashlib.sha256(request.source_snapshot_token.encode("utf-8")).digest(),
    }
    if request.source_binding_sha256 is not None:
        digests_by_field["source_binding_sha256"] = request.source_binding_sha256
    elif bundle.source_binding_sha256 is not None:
        raise CaptureBundleConflict("pending capture identity has drifted")
    if any(getattr(bundle, key) != value for key, value in expected_by_field.items()) or any(
        not _is_stored_digest_equal(getattr(bundle, key), value) for key, value in digests_by_field.items()
    ):
        raise CaptureBundleConflict("pending capture identity has drifted")


async def _fresh_remaining_seconds(session: AsyncSession, request: PendingCaptureRequest, bundle=None) -> float:
    expires_at = (
        await session.execute(
            select(CustomImportLease.expires_at).where(CustomImportLease.execution_id == request.execution_id)
        )
    ).scalar_one()
    now = await lifecycle._database_now(session)
    deadline = min(expires_at, bundle.acquisition_deadline_at) if bundle is not None else expires_at
    remaining = (deadline - now).total_seconds()
    if remaining <= 0:
        raise CaptureBundleConflict("pending capture authority has expired")
    return remaining


async def _lock_bundle(session: AsyncSession, request: PendingCaptureRequest, capture_bundle_id: int):
    execution = await _lock_execution(session, request)
    bundle = (
        await session.execute(
            select(CustomImportCaptureBundle)
            .where(CustomImportCaptureBundle.capture_bundle_id == _positive_id(capture_bundle_id, "capture_bundle_id"))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if bundle is None:
        raise CaptureBundleConflict("pending capture bundle does not exist")
    _validate_bundle(bundle, request)
    if execution.capture_bundle_id not in (None, bundle.capture_bundle_id):
        raise CaptureBundleConflict("execution is bound to a different capture")
    await _fresh_remaining_seconds(session, request, bundle)
    return bundle, execution


async def _locked_streams(session: AsyncSession, bundle: CustomImportCaptureBundle):
    return tuple(
        (
            await session.execute(
                select(CustomImportCapture)
                .where(CustomImportCapture.capture_bundle_id == bundle.capture_bundle_id)
                .order_by(CustomImportCapture.stream_slot)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )


async def begin_pending_parquet_bundle(
    session: AsyncSession,
    *,
    request: PendingCaptureRequest,
) -> CaptureBundleRegistration:
    """Begin once per exact producer/fence; never reuse another attempt's bytes."""

    request = _request(session, request)
    execution = await _lock_execution(session, request)
    streams = await _validated_streams(session, request.identity)
    await _validated_replayable_parquet_streams(session, request.identity, streams)
    existing = (
        await session.execute(
            select(CustomImportCaptureBundle)
            .where(
                CustomImportCaptureBundle.producing_execution_id == request.execution_id,
                CustomImportCaptureBundle.producing_fence == request.fence,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if existing is not None:
        _validate_bundle(existing, request)
        await _fresh_remaining_seconds(session, request, existing)
        stored = await _locked_streams(session, existing)
        if tuple(capture.stream_slot for capture in stored) != tuple(slot for _, slot in streams):
            raise CaptureBundleConflict("pending capture stream coverage has drifted")
        if execution.capture_bundle_id not in (None, existing.capture_bundle_id):
            raise CaptureBundleConflict("execution is bound to a different capture")
        if existing.capture_state == "sealed" and execution.capture_bundle_id != existing.capture_bundle_id:
            raise CaptureBundleConflict("sealed capture is not bound to its producer")
        await _fresh_remaining_seconds(session, request, existing)
        return CaptureBundleRegistration(existing.capture_bundle_id, tuple(slot for _, slot in streams), False)
    if execution.capture_bundle_id is not None:
        raise CaptureBundleConflict("execution already has a sealed capture")
    await _fresh_remaining_seconds(session, request)
    return await _insert_pending_bundle(session, request, streams)


async def _insert_pending_bundle(session, request, streams):
    bundle_id = (
        await session.execute(
            insert(CustomImportCaptureBundle)
            .values(
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                schema_revision_id=request.schema_revision_id,
                snapshot_token=request.source_snapshot_token,
                snapshot_token_sha256=hashlib.sha256(request.source_snapshot_token.encode("utf-8")).digest(),
                stream_count=len(streams),
                payload_contract=SEGMENTED_PAYLOAD_CONTRACT,
                capture_state="pending",
                producing_execution_id=request.execution_id,
                producing_fence=request.fence,
                producing_token_sha256=lifecycle.lease_token_sha256(request.token),
                request_identity_sha256=request.request_identity_sha256,
                source_binding_revision_id=request.source_binding_revision_id,
                source_binding_sha256=request.source_binding_sha256,
                source_request_sha256=request.source_request_sha256,
                statement_sha256=request.statement_sha256,
                canonical_policy=request.policy.canonical,
                policy_sha256=bytes.fromhex(request.policy.digest),
                canonical_manifest=None,
                manifest_sha256=None,
                sealed_at=None,
            )
            .returning(CustomImportCaptureBundle.capture_bundle_id)
        )
    ).scalar_one()
    await session.execute(
        insert(CustomImportCapture),
        [
            dict(
                capture_bundle_id=bundle_id,
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                schema_revision_id=request.schema_revision_id,
                stream_slot=slot,
                payload_contract=SEGMENTED_PAYLOAD_CONTRACT,
                capture_state="pending",
                sealed_at=None,
            )
            for _, slot in streams
        ],
    )
    return CaptureBundleRegistration(bundle_id, tuple(slot for _, slot in streams), True)


async def _source_stream(session: AsyncSession, request: PendingCaptureRequest, stream_id: str):
    revision = await session.get(CustomImportDefinitionRevision, request.definition_revision_id)
    if revision is None or (revision.dataset_id, revision.schema_revision_id) != (
        request.dataset_id,
        request.schema_revision_id,
    ):
        raise CaptureBundleConflict("pending capture definition has drifted")
    definition = CustomImportDefinition.from_json(revision.canonical_definition)
    for stream in definition.source_streams:
        if stream.stream_id == stream_id and stream.format == "parquet" and stream.compression == "none":
            return stream
    raise CaptureStoreError("pending capture part has no declared Parquet stream")


def _part_values(capture: SealedCapture, ordinal: int, record_count: int, arrow_byte_count: int, policy):
    _positive_id(ordinal, "part_ordinal")
    if ordinal > policy.stream_budget["maximum_parts"]:
        raise CaptureStoreError("part ordinal exceeds the declared stream budget")
    _count(record_count, "record_count", policy.part_limits.maximum_records)
    _count(arrow_byte_count, "arrow_byte_count", policy.maximum_part_arrow_bytes)
    canonical = _canonical_manifest(canonical_json(asdict(capture.manifest)))
    if len(canonical.encode("utf-8")) > policy.maximum_part_manifest_bytes:
        raise CaptureStoreError("part manifest exceeds the declared part budget")
    return dict(
        part_ordinal=ordinal,
        byte_count=len(capture.payload),
        payload=capture.payload,
        payload_sha256=hashlib.sha256(capture.payload).digest(),
        canonical_capture_manifest=canonical,
        capture_manifest_sha256=hashlib.sha256(canonical.encode("utf-8")).digest(),
        decoded_byte_count=capture.manifest.decoded_bytes,
        arrow_byte_count=arrow_byte_count,
        record_count=record_count,
    )


def _check_budget(header, values_by_field, budget) -> None:
    for field_name, budget_name in zip(_ACCOUNTING_FIELDS, _BUDGET_FIELDS, strict=True):
        if getattr(header, "committed_" + field_name) + values_by_field[field_name] > budget[budget_name]:
            raise CaptureStoreError("pending capture exceeds its declared accounting budget")


async def _pending_stream(session, bundle, request, stream_id: str):
    streams = await _validated_streams(session, request.identity)
    slot_by_stream = dict(streams)
    if stream_id not in slot_by_stream:
        raise CaptureStoreError("pending capture stream is not declared")
    capture = await session.get(
        CustomImportCapture,
        (bundle.capture_bundle_id, slot_by_stream[stream_id]),
        with_for_update=True,
        populate_existing=True,
    )
    if bundle.capture_state != "pending" or capture is None or capture.capture_state != "pending":
        raise CaptureBundleConflict("pending capture stream is closed")
    await _fresh_remaining_seconds(session, request, bundle)
    return capture


async def append_pending_parquet_part(
    session: AsyncSession,
    *,
    request: PendingCaptureRequest,
    capture_bundle_id: int,
    capture: SealedCapture,
    ordinal: int,
    record_count: int,
    arrow_byte_count: int,
) -> int:
    """Return one new insertion, or zero for an exact live uncharged retry."""

    request = _request(session, request)
    values_by_field = await _verified_part_values(session, request, capture, ordinal, record_count, arrow_byte_count)
    bundle, _ = await _lock_bundle(session, request, capture_bundle_id)
    stored = await _pending_stream(session, bundle, request, capture.manifest.stream_id)
    if stored.eof_at is not None:
        raise CaptureBundleConflict("pending capture stream has reached EOF")
    existing = await session.get(
        CustomImportCaptureParquetPart, (bundle.capture_bundle_id, stored.stream_slot, ordinal)
    )
    if existing is not None:
        if any(getattr(existing, key) != expected for key, expected in values_by_field.items()):
            raise CaptureBundleConflict("pending capture part retry has drifted")
        await _fresh_remaining_seconds(session, request, bundle)
        return 0
    if ordinal != stored.committed_part_count + 1:
        raise CaptureBundleConflict("pending capture part ordinal is not contiguous")
    increments_by_field = dict(
        zip(
            _ACCOUNTING_FIELDS,
            (
                1,
                len(capture.payload),
                capture.manifest.decoded_bytes,
                arrow_byte_count,
                record_count,
                len(values_by_field["canonical_capture_manifest"].encode("utf-8")),
            ),
            strict=True,
        )
    )
    _check_budget(stored, increments_by_field, request.policy.stream_budget)
    _check_budget(bundle, increments_by_field, request.policy.bundle_budget)
    await _fresh_remaining_seconds(session, request, bundle)
    await session.execute(
        insert(CustomImportCaptureParquetPart).values(
            capture_bundle_id=bundle.capture_bundle_id,
            stream_slot=stored.stream_slot,
            **values_by_field,
        )
    )
    return 1


async def _verified_part_values(session, request, capture, ordinal, record_count, arrow_byte_count):
    if not isinstance(capture, SealedCapture):
        raise CaptureStoreError("pending capture part requires SealedCapture")
    if not isinstance(capture.manifest, CaptureManifest):
        raise CaptureStoreError("pending capture part requires CaptureManifest")
    stream = await _source_stream(session, request, capture.manifest.stream_id)
    try:
        verify_capture(capture, stream, limits=request.policy.part_limits)
    except CaptureError as exc:
        raise CaptureStoreError("pending capture part is invalid") from exc
    if capture.manifest.source_snapshot_token != request.source_snapshot_token:
        raise CaptureBundleConflict("pending capture part snapshot has drifted")
    return _part_values(capture, ordinal, record_count, arrow_byte_count, request.policy)


async def mark_pending_parquet_eof(
    session: AsyncSession,
    *,
    request: PendingCaptureRequest,
    capture_bundle_id: int,
    stream_id: str,
    part_count: int,
    record_count: int,
) -> int:
    """Return one retained EOF transition, or zero for an exact pending repeat."""

    request = _request(session, request)
    _count(part_count, "part_count", request.policy.stream_budget["maximum_parts"])
    _count(record_count, "record_count", request.policy.stream_budget["maximum_records"])
    bundle, _ = await _lock_bundle(session, request, capture_bundle_id)
    stream = await _pending_stream(session, bundle, request, stream_id)
    if part_count < 1 or (part_count, record_count) != (stream.committed_part_count, stream.committed_record_count):
        raise CaptureBundleConflict("pending capture EOF accounting has drifted")
    if stream.eof_at is not None:
        return 0
    await _fresh_remaining_seconds(session, request, bundle)
    await session.execute(
        update(CustomImportCapture)
        .where(
            CustomImportCapture.capture_bundle_id == bundle.capture_bundle_id,
            CustomImportCapture.stream_slot == stream.stream_slot,
        )
        .values(eof_at=func.clock_timestamp())
    )
    return 1


def _accounting(header) -> dict[str, int]:
    return {name: _count(getattr(header, "committed_" + name), name) for name in _ACCOUNTING_FIELDS}


def _receipt_document(receipt: CaptureReceipt, bundle, policy: SegmentedCapturePolicy) -> dict:
    canonical = _canonical_manifest(receipt.canonical_manifest)
    if hashlib.sha256(canonical.encode("utf-8")).hexdigest() != receipt.manifest_sha256:
        raise CaptureBundleConflict("segmented capture manifest digest has drifted")
    document = json.loads(canonical)
    if document.get("contract_version") != SEGMENTED_PAYLOAD_CONTRACT or document.get("policy_sha256") != policy.digest:
        raise CaptureBundleConflict("segmented capture receipt contract or policy has drifted")
    if receipt.source_snapshot_token != bundle.snapshot_token:
        raise CaptureBundleConflict("segmented capture receipt snapshot has drifted")
    optional_by_field = {
        "source_request_sha256": bytes(bundle.source_request_sha256).hex(),
        "statement_sha256": bytes(bundle.statement_sha256).hex(),
        "source_binding_sha256": bytes(bundle.source_binding_sha256).hex() if bundle.source_binding_sha256 else None,
        "source_snapshot_token": bundle.snapshot_token,
        "stream_id": receipt.stream_id,
    }
    required_source_fields = {"source_request_sha256", "statement_sha256"}
    if bundle.source_binding_sha256 is not None:
        required_source_fields.add("source_binding_sha256")
    if not required_source_fields <= document.keys():
        raise CaptureBundleConflict("segmented capture receipt source identity is incomplete")
    if any(name in document and document[name] != expected for name, expected in optional_by_field.items()):
        raise CaptureBundleConflict("segmented capture receipt source identity has drifted")
    for name in _ACCOUNTING_FIELDS:
        _count(document.get(name), name)
    for name in ("payload_set_sha256", "manifest_set_sha256"):
        _sha256(document.get(name), name)
    if (
        document["part_count"] < 1
        or receipt.byte_count != document["byte_count"]
        or receipt.content_sha256 != document["payload_set_sha256"]
    ):
        raise CaptureBundleConflict("segmented capture receipt payload identity has drifted")
    return document


def _validate_stream_receipt(receipt: CaptureReceipt, stream, bundle, policy) -> dict:
    if not isinstance(receipt, CaptureReceipt):
        raise CaptureStoreError("segmented capture receipt requires CaptureReceipt")
    document = _receipt_document(receipt, bundle, policy)
    if any(document[name] != value for name, value in _accounting(stream).items()):
        raise CaptureBundleConflict("segmented capture receipt accounting has drifted")
    for name, limit in zip(_ACCOUNTING_FIELDS, _BUDGET_FIELDS, strict=True):
        if document[name] > policy.stream_budget[limit]:
            raise CaptureStoreError("segmented capture receipt exceeds the declared stream budget")
    return document


async def _arm_seal_timeout(session, request, bundle) -> None:
    remaining = await _fresh_remaining_seconds(session, request, bundle)
    timeout_ms = int(remaining * 500)
    if timeout_ms < 1:
        raise CaptureBundleConflict("pending capture has insufficient seal authority")
    await session.execute(select(func.set_config("statement_timeout", f"{timeout_ms}ms", True)))


async def _seal_stream(session, request, bundle, stream, receipt) -> None:
    document = _validate_stream_receipt(receipt, stream, bundle, request.policy)
    if stream.capture_state == "sealed":
        if stream.canonical_manifest != receipt.canonical_manifest or not _is_stored_digest_equal(
            stream.manifest_sha256, bytes.fromhex(receipt.manifest_sha256)
        ):
            raise CaptureBundleConflict("segmented capture sealed receipt retry has drifted")
        return
    if stream.capture_state != "pending" or stream.eof_at is None:
        raise CaptureBundleConflict("segmented capture seal requires retained EOF")
    await _arm_seal_timeout(session, request, bundle)
    await session.execute(
        update(CustomImportCapture)
        .where(
            CustomImportCapture.capture_bundle_id == bundle.capture_bundle_id,
            CustomImportCapture.stream_slot == stream.stream_slot,
        )
        .values(
            capture_state="sealed",
            canonical_manifest=receipt.canonical_manifest,
            manifest_sha256=bytes.fromhex(receipt.manifest_sha256),
            byte_count=receipt.byte_count,
            content_sha256=bytes.fromhex(receipt.content_sha256),
            payload_part_count=document["part_count"],
            payload_set_sha256=bytes.fromhex(document["payload_set_sha256"]),
            manifest_set_sha256=bytes.fromhex(document["manifest_set_sha256"]),
            sealed_at=func.clock_timestamp(),
        )
    )


def _bundle_manifest(bundle, streams, receipts_by_stream, policy) -> str:
    accounting_by_field = _accounting(bundle)
    for field_name, limit_name in zip(_ACCOUNTING_FIELDS, _BUDGET_FIELDS, strict=True):
        if accounting_by_field[field_name] > policy.bundle_budget[limit_name]:
            raise CaptureStoreError("segmented capture exceeds the declared bundle budget")
    binding_by_field = (
        {"source_binding_sha256": bytes(bundle.source_binding_sha256).hex()}
        if bundle.source_binding_sha256 is not None
        else {}
    )
    return _canonical_manifest(
        canonical_json(
            {
                "contract_version": SEGMENTED_PAYLOAD_CONTRACT,
                "policy_sha256": policy.digest,
                "source_request_sha256": bytes(bundle.source_request_sha256).hex(),
                "statement_sha256": bytes(bundle.statement_sha256).hex(),
                "stream_count": len(streams),
                "streams": [
                    dict(stream_slot=slot, manifest_sha256=receipts_by_stream[stream_id].manifest_sha256)
                    for stream_id, slot in streams
                ],
                **accounting_by_field,
                **binding_by_field,
            }
        )
    )


async def seal_pending_parquet_bundle(
    session: AsyncSession,
    *,
    request: PendingCaptureRequest,
    capture_bundle_id: int,
    receipts: tuple[CaptureReceipt, ...],
) -> CaptureBundleRegistration:
    """Seal every EOF stream and bind its producer atomically in this transaction.

    The database recomputes metadata digests and accounting under the armed
    statement timeout. Source decoding and complete replay remain caller work.
    Any failure requires the caller to roll back this transaction.
    """

    request = _request(session, request)
    if (
        not isinstance(receipts, tuple)
        or not receipts
        or not all(isinstance(receipt, CaptureReceipt) for receipt in receipts)
    ):
        raise CaptureStoreError("segmented seal requires a non-empty tuple of CaptureReceipts")
    receipts_by_stream = {receipt.stream_id: receipt for receipt in receipts}
    bundle, execution = await _lock_bundle(session, request, capture_bundle_id)
    streams = await _validated_streams(session, request.identity)
    stored = await _locked_streams(session, bundle)
    if (
        len(receipts_by_stream) != len(receipts)
        or set(receipts_by_stream) != {name for name, _ in streams}
        or tuple(capture.stream_slot for capture in stored) != tuple(slot for _, slot in streams)
    ):
        raise CaptureBundleConflict("segmented capture seal stream coverage has drifted")
    for (stream_id, _), capture in zip(streams, stored, strict=True):
        await _seal_stream(session, request, bundle, capture, receipts_by_stream[stream_id])
    canonical = _bundle_manifest(bundle, streams, receipts_by_stream, request.policy)
    await _seal_bundle(session, request, bundle, execution, canonical)
    await _fresh_remaining_seconds(session, request, bundle)
    return CaptureBundleRegistration(bundle.capture_bundle_id, tuple(slot for _, slot in streams), False)


async def _seal_bundle(session, request, bundle, execution, canonical):
    if bundle.capture_state == "sealed":
        if (
            bundle.canonical_manifest != canonical
            or not _is_stored_digest_equal(bundle.manifest_sha256, hashlib.sha256(canonical.encode("utf-8")).digest())
            or execution.capture_bundle_id != bundle.capture_bundle_id
        ):
            raise CaptureBundleConflict("segmented capture sealed bundle retry has drifted")
    elif bundle.capture_state == "pending":
        await _arm_seal_timeout(session, request, bundle)
        await session.execute(
            update(CustomImportCaptureBundle)
            .where(
                CustomImportCaptureBundle.capture_bundle_id == bundle.capture_bundle_id,
            )
            .values(
                capture_state="sealed",
                canonical_manifest=canonical,
                manifest_sha256=hashlib.sha256(canonical.encode("utf-8")).digest(),
                sealed_at=func.clock_timestamp(),
            )
        )
        bound = await lifecycle.bind_execution_capture_bundle(
            session,
            execution_id=request.execution_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            capture_bundle_id=bundle.capture_bundle_id,
            fence=request.fence,
            token=request.token,
        )
        if bound is None:
            raise CaptureBundleConflict("segmented capture lost authority before binding")
    else:
        raise CaptureBundleConflict("segmented capture bundle state is invalid")


@dataclass
class _ReplayAccounting:
    """One stream's bounded metadata state; no retained payload ledger."""

    policy: SegmentedCapturePolicy
    expected_by_field: Mapping[str, int]
    manifest_set_sha256: bytes
    source_snapshot_token: str
    totals_by_field: dict[str, int] = field(default_factory=lambda: dict.fromkeys(_ACCOUNTING_FIELDS, 0))
    digest: Any = field(default_factory=lambda: hashlib.sha256(_MANIFEST_SET_DOMAIN), repr=False)

    def add(self, receipt, ordinal, payload, metadata) -> tuple[CaptureManifest, int, int]:
        """Validate one part's asserted metadata and update bounded stream totals."""

        manifest, canonical, decoded, arrow, records = _validated_part_accounting(
            receipt, payload, metadata, self.policy
        )
        manifest_bytes = len(canonical.encode("utf-8"))
        manifest_digest = hashlib.sha256(canonical.encode("utf-8")).digest()
        _add_payload_part_digest(self.digest, ordinal, manifest_bytes, manifest_digest)
        for value in (decoded, arrow, records):
            self.digest.update(value.to_bytes(8, "big"))
        for name, increment in zip(
            _ACCOUNTING_FIELDS, (1, len(payload), decoded, arrow, records, manifest_bytes), strict=True
        ):
            self.totals_by_field[name] += increment
            if self.totals_by_field[name] > self.expected_by_field[name]:
                raise CaptureBundleConflict("segmented capture part accounting exceeds its sealed receipt")
        return manifest, records, arrow

    def finish(self) -> None:
        """Require exact metadata coverage only at actual stored iterator EOF."""

        if self.totals_by_field != dict(self.expected_by_field) or self.digest.digest() != self.manifest_set_sha256:
            raise CaptureBundleConflict("segmented capture complete manifest accounting has drifted")


def _validated_part_accounting(receipt, part_payload, metadata, policy):
    result = _validated_part_manifest(
        receipt, len(part_payload), hashlib.sha256(part_payload).hexdigest(), metadata, policy
    )
    if not 1 <= len(part_payload) <= policy.part_limits.maximum_compressed_bytes:
        raise CaptureBundleConflict("segmented capture per-part part_payload exceeds its policy")
    return result


def _validated_part_manifest(receipt, byte_count, payload_sha256_hex, metadata, policy):
    """Apply identical manifest checks to payload and payload-free part reads."""

    if len(metadata) != 5:
        raise CaptureBundleConflict("segmented capture per-part metadata is incomplete")
    canonical, stored_digest, decoded, arrow, record_count = metadata
    canonical = _canonical_manifest(canonical)
    manifest_bytes = canonical.encode("utf-8")
    if len(manifest_bytes) > policy.maximum_part_manifest_bytes or not _is_stored_digest_equal(
        stored_digest, hashlib.sha256(manifest_bytes).digest()
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
        or manifest.compressed_sha256 != payload_sha256_hex
        or manifest.decoded_bytes != decoded
        or decoded != byte_count
        or manifest.decoded_sha256 != payload_sha256_hex
    ):
        raise CaptureBundleConflict("segmented capture per-part manifest identity has drifted")
    for name in ("stream_sha256", "compressed_sha256", "decoded_sha256", "capture_sha256"):
        _sha256(getattr(manifest, name), name)
    _count(decoded, "decoded_byte_count", policy.part_limits.maximum_decoded_bytes)
    _count(arrow, "arrow_byte_count", policy.maximum_part_arrow_bytes)
    _count(record_count, "record_count", policy.part_limits.maximum_records)
    return manifest, canonical, decoded, arrow, record_count


def _segmented_stream_metadata(bundle, streams, captures_by_slot, policy):
    receipts_by_slot = {}
    metadata_by_slot = {}
    accounting_by_slot = {}
    for stream_id, slot in streams:
        capture = captures_by_slot[slot]
        if (
            capture.capture_state != "sealed"
            or capture.eof_at is None
            or capture.payload_contract != SEGMENTED_PAYLOAD_CONTRACT
        ):
            raise CaptureBundleConflict("segmented capture stream is not sealed")
        receipt = CaptureReceipt(
            stream_id=stream_id,
            source_snapshot_token=bundle.snapshot_token,
            byte_count=capture.byte_count,
            content_sha256=_digest(capture.content_sha256, "content_sha256").hex(),
            canonical_manifest=capture.canonical_manifest,
            manifest_sha256=_digest(capture.manifest_sha256, "manifest_sha256").hex(),
        )
        document = _validate_stream_receipt(receipt, capture, bundle, policy)
        payload_digest = bytes.fromhex(document["payload_set_sha256"])
        manifest_digest = bytes.fromhex(document["manifest_set_sha256"])
        if (
            capture.payload_part_count != document["part_count"]
            or not _is_stored_digest_equal(capture.payload_set_sha256, payload_digest)
            or not _is_stored_digest_equal(capture.manifest_set_sha256, manifest_digest)
        ):
            raise CaptureBundleConflict("segmented capture stream final metadata has drifted")
        receipts_by_slot[slot] = receipt
        metadata_by_slot[slot] = (document["part_count"], payload_digest)
        accounting_by_slot[slot] = _ReplayAccounting(
            policy, _accounting(capture), manifest_digest, bundle.snapshot_token
        )
    return receipts_by_slot, metadata_by_slot, accounting_by_slot


def _load_segmented_replay_metadata(bundle, streams, captures_by_slot):
    """Freeze the sealed policy and header accounting before the shared reader."""

    canonical = _canonical_manifest(bundle.canonical_manifest)
    policy = SegmentedCapturePolicy.from_mapping(json.loads(bundle.canonical_policy))
    if (
        policy.canonical != bundle.canonical_policy
        or not _is_stored_digest_equal(bundle.policy_sha256, bytes.fromhex(policy.digest))
        or not _is_stored_digest_equal(bundle.manifest_sha256, hashlib.sha256(canonical.encode("utf-8")).digest())
        or not _is_stored_digest_equal(
            bundle.snapshot_token_sha256, hashlib.sha256(bundle.snapshot_token.encode("utf-8")).digest()
        )
        or bundle.stream_count != len(streams)
    ):
        raise CaptureBundleConflict("segmented capture sealed bundle identity has drifted")
    receipts_by_slot, metadata_by_slot, accounting_by_slot = _segmented_stream_metadata(
        bundle, streams, captures_by_slot, policy
    )
    receipts_by_stream = {receipt.stream_id: receipt for receipt in receipts_by_slot.values()}
    expected_by_field = json.loads(_bundle_manifest(bundle, streams, receipts_by_stream, policy))
    actual_by_field = json.loads(canonical)
    _validate_bundle_receipt_shape(actual_by_field)
    if any(actual_by_field.get(key) != value for key, value in expected_by_field.items()) or any(
        sum(state.expected_by_field[name] for state in accounting_by_slot.values()) != expected_by_field[name]
        for name in _ACCOUNTING_FIELDS
    ):
        raise CaptureBundleConflict("segmented capture sealed bundle accounting has drifted")
    return receipts_by_slot, metadata_by_slot, accounting_by_slot


def _validate_bundle_receipt_shape(document_by_field) -> None:
    for name in (*_ACCOUNTING_FIELDS, "stream_count"):
        _count(document_by_field.get(name), name)
    stream_entries = document_by_field.get("streams")
    if (
        not isinstance(stream_entries, list)
        or not stream_entries
        or len(stream_entries) != document_by_field["stream_count"]
    ):
        raise CaptureBundleConflict("segmented capture bundle stream receipt coverage is invalid")
    previous_slot = 0
    for entry in stream_entries:
        if not isinstance(entry, dict) or set(entry) != {"stream_slot", "manifest_sha256"}:
            raise CaptureBundleConflict("segmented capture bundle stream receipt entry is invalid")
        slot = _positive_id(entry["stream_slot"], "stream_slot")
        _sha256(entry["manifest_sha256"], "stream manifest digest")
        if slot <= previous_slot:
            raise CaptureBundleConflict("segmented capture bundle streams are not strictly sorted")
        previous_slot = slot
