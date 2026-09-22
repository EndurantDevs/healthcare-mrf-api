# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fenced finality and pointer publication for ``custom-import/v1``.

All state-changing paths lock dataset, execution, lease, then generation or
finality rows.  The database migration uses that same dataset row as the
append/seal serialization parent.  Callers must provide a clean transaction:
otherwise ORM autoflush could acquire a lower-order lock before the dataset.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportChildCollection,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportCurrentGeneration,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportEntityBinding,
    CustomImportExecution,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportField,
    CustomImportFieldAlias,
    CustomImportFieldSlot,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportNoChangeSeal,
    CustomImportPack,
    CustomImportPublicationEvent,
    CustomImportRejection,
    CustomImportRootRecord,
    CustomImportRootRevision,
    CustomImportRootScalar,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportSourceStream,
    CustomImportWinner,
)
from process.custom_import.definition import canonical_json, canonical_sha256
from process.custom_import.execution import (
    MAX_BIGINT,
    MAX_LEASE_SECONDS,
    lease_token_sha256,
    require_separate_publication_transaction,
)

PublicationKind = Literal["activated", "rolled_back", "no_change"]
_GENERATION_SEAL_CONTRACT = "custom-import-generation-seal/v1"
_NO_CHANGE_SEAL_CONTRACT = "custom-import-no-change-seal/v1"
FINALITY_EVENT_CONTRACT = "custom-import-finality/v1"
_MATERIALIZATION_DOMAIN = "generation-materialization/v1"
_EFFECTIVE_OUTPUT_DOMAIN = "generation-effective-output/v1"
_SOURCE_BUNDLE_DOMAIN = "source-bundle/v1"
_NO_CHANGE_RECEIPT_DOMAIN = "no-change-receipt/v1"
# Finality holds the dataset serialization lock while it snapshots the
# immutable graph.  Extend only the exact current lease before that bounded
# critical section, rather than depending on a heartbeat that is blocked by
# the same lock.  The lifecycle module's hard lease ceiling bounds the hold.
_SEAL_LEASE_WINDOW_SECONDS = MAX_LEASE_SECONDS
_MATERIALIZATION_STREAM_CHUNK_SIZE = 256
_FINALITY_SCAN_WINDOW_KEY = "custom_import_finality_scan_window"


class PublicationConflict(RuntimeError):
    """The requested finality or pointer transition is not authoritative."""


@dataclass(frozen=True)
class _FinalityScanWindow:
    """One bounded local view of the database-authoritative seal deadline."""

    expires_at: dt.datetime
    monotonic_deadline: float


@dataclass(frozen=True)
class PublicationReceipt:
    publication_event_id: int
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    execution_id: int
    event_kind: PublicationKind
    from_generation_id: int | None
    to_generation_id: int
    expected_pointer_version: int
    committed_pointer_version: int
    event_sha256: str
    replayed: bool = False


@dataclass(frozen=True)
class GenerationSealReceipt:
    generation_id: int
    dataset_id: int
    execution_id: int
    materialization_sha256: str
    effective_output_sha256: str
    root_count: int
    family_count: int
    generation_family_count: int
    family_child_count: int
    winner_count: int
    profile_count: int
    root_scalar_count: int
    child_scalar_count: int
    replayed: bool = False


@dataclass(frozen=True)
class _PublicationEventDetails:
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    execution_id: int
    event_kind: PublicationKind
    from_generation_id: int | None
    to_generation_id: int
    expected_pointer_version: int
    committed_pointer_version: int


@dataclass(frozen=True)
class _GenerationPublicationRequest:
    event_kind: Literal["activated", "rolled_back"]
    dataset_id: int
    target_generation_id: int
    expected_generation_id: int | None
    expected_pointer_version: int
    committed_pointer_version: int


@dataclass(frozen=True)
class _GenerationSealRequest:
    dataset_id: int
    generation_id: int
    lease_fence: int
    token_sha256: bytes


@dataclass(frozen=True)
class _NoChangeRequest:
    dataset_id: int
    execution_id: int
    expected_generation_id: int
    expected_pointer_version: int
    candidate_generation_id: int
    lease_fence: int
    token_sha256: bytes


@dataclass(frozen=True)
class _Materialization:
    source_bundle_sha256: bytes
    materialization_sha256: bytes
    effective_output_sha256: bytes
    root_count: int
    family_count: int
    generation_family_count: int
    family_child_count: int
    winner_count: int
    profile_count: int
    root_scalar_count: int
    child_scalar_count: int


def _positive_integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 < value <= MAX_BIGINT:
        raise PublicationConflict(f"{label} must be a positive integer")
    return value


def _pointer_version(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= MAX_BIGINT:
        raise PublicationConflict("expected pointer version must be a non-negative integer")
    return value


def _increment_pointer_version(value: int) -> int:
    if value >= MAX_BIGINT:
        raise PublicationConflict("pointer version cannot advance beyond PostgreSQL BIGINT")
    return value + 1


def _sha256(value: object, label: str) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)) or len(value) != 32:
        raise PublicationConflict(f"{label} must contain 32 bytes")
    return bytes(value)


def _require_transaction(session: AsyncSession) -> None:
    if not session.in_transaction():
        raise PublicationConflict("publication requires a caller-owned transaction")


def _require_clean_session(session: AsyncSession) -> None:
    for attribute in ("new", "dirty", "deleted"):
        if getattr(session, attribute, ()):
            raise PublicationConflict("publication requires a clean session before it acquires the dataset lock")


async def _begin_finality_operation(session: AsyncSession) -> None:
    _require_transaction(session)
    _require_clean_session(session)
    await require_separate_publication_transaction(session)
    isolation = await session.scalar(select(func.current_setting("transaction_isolation")))
    if isolation != "read committed":
        raise PublicationConflict("finality requires PostgreSQL READ COMMITTED isolation")


def _event_document(event_details: _PublicationEventDetails) -> tuple[str, bytes]:
    event_fields_by_name = {
        "contract": "custom-import-publication-event/v1",
        "dataset_id": event_details.dataset_id,
        "definition_revision_id": event_details.definition_revision_id,
        "event_kind": event_details.event_kind,
        "execution_id": event_details.execution_id,
        "from_generation_id": event_details.from_generation_id,
        "schema_revision_id": event_details.schema_revision_id,
        "to_generation_id": event_details.to_generation_id,
        "expected_pointer_version": event_details.expected_pointer_version,
        "committed_pointer_version": event_details.committed_pointer_version,
    }
    canonical = canonical_json(event_fields_by_name)
    return canonical, bytes.fromhex(canonical_sha256(event_fields_by_name, domain="event"))


def _event_details_from_event(event: CustomImportPublicationEvent) -> _PublicationEventDetails:
    return _PublicationEventDetails(
        dataset_id=event.dataset_id,
        definition_revision_id=event.definition_revision_id,
        schema_revision_id=event.schema_revision_id,
        execution_id=event.execution_id,
        event_kind=event.event_kind,
        from_generation_id=event.from_generation_id,
        to_generation_id=event.to_generation_id,
        expected_pointer_version=event.expected_pointer_version,
        committed_pointer_version=event.committed_pointer_version,
    )


def verify_publication_event_material(event: CustomImportPublicationEvent) -> None:
    """Require an immutable publication event to retain its canonical receipt."""

    try:
        canonical, digest = _event_document(_event_details_from_event(event))
        event_digest = bytes(event.event_sha256)
    except AttributeError, TypeError, ValueError:
        raise PublicationConflict("persisted publication receipt is not canonical") from None
    if event.canonical_event != canonical or not hmac.compare_digest(event_digest, digest):
        raise PublicationConflict("persisted publication receipt is not canonical")


_verify_event_material = verify_publication_event_material


def _receipt(event: CustomImportPublicationEvent, *, replayed: bool) -> PublicationReceipt:
    return PublicationReceipt(
        publication_event_id=event.publication_event_id,
        dataset_id=event.dataset_id,
        definition_revision_id=event.definition_revision_id,
        schema_revision_id=event.schema_revision_id,
        execution_id=event.execution_id,
        event_kind=event.event_kind,
        from_generation_id=event.from_generation_id,
        to_generation_id=event.to_generation_id,
        expected_pointer_version=event.expected_pointer_version,
        committed_pointer_version=event.committed_pointer_version,
        event_sha256=bytes(event.event_sha256).hex(),
        replayed=replayed,
    )


async def _locked_dataset(session: AsyncSession, dataset_id: int) -> CustomImportDataset:
    dataset = (
        await session.execute(
            select(CustomImportDataset)
            .where(CustomImportDataset.dataset_id == dataset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if dataset is None:
        raise PublicationConflict("dataset does not exist")
    return dataset


async def _generation_execution_id(session: AsyncSession, *, dataset_id: int, generation_id: int) -> int:
    """Read immutable ownership without acquiring a generation lock too early."""

    execution_id = (
        await session.execute(
            select(CustomImportGeneration.execution_id).where(
                CustomImportGeneration.dataset_id == dataset_id,
                CustomImportGeneration.generation_id == generation_id,
            )
        )
    ).scalar_one_or_none()
    if execution_id is None:
        raise PublicationConflict("target generation does not belong to the dataset")
    return execution_id


async def _generation_snapshot(
    session: AsyncSession,
    *,
    dataset_id: int,
    generation_id: int,
) -> CustomImportGeneration:
    """Read immutable candidate identity after the dataset serialization lock."""

    generation = (
        await session.execute(
            select(CustomImportGeneration)
            .where(
                CustomImportGeneration.dataset_id == dataset_id,
                CustomImportGeneration.generation_id == generation_id,
            )
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if generation is None:
        raise PublicationConflict("target generation does not belong to the dataset")
    return generation


async def _locked_execution(session: AsyncSession, *, execution_id: int, dataset_id: int) -> CustomImportExecution:
    execution = (
        await session.execute(
            select(CustomImportExecution)
            .where(
                CustomImportExecution.execution_id == execution_id,
                CustomImportExecution.dataset_id == dataset_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if execution is None:
        raise PublicationConflict("execution does not belong to the dataset")
    return execution


async def _locked_lease(session: AsyncSession, execution_id: int) -> CustomImportLease:
    lease = (
        await session.execute(
            select(CustomImportLease)
            .where(CustomImportLease.execution_id == execution_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if lease is None:
        raise PublicationConflict("execution lease does not exist")
    return lease


async def _locked_generation(
    session: AsyncSession,
    *,
    dataset_id: int,
    generation_id: int,
) -> CustomImportGeneration:
    generation = (
        await session.execute(
            select(CustomImportGeneration)
            .where(
                CustomImportGeneration.dataset_id == dataset_id,
                CustomImportGeneration.generation_id == generation_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if generation is None:
        raise PublicationConflict("target generation does not belong to the dataset")
    return generation


async def _locked_generation_seal(
    session: AsyncSession,
    generation_id: int,
) -> CustomImportGenerationSeal | None:
    return (
        await session.execute(
            select(CustomImportGenerationSeal)
            .where(CustomImportGenerationSeal.generation_id == generation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _locked_no_change_seal(session: AsyncSession, execution_id: int) -> CustomImportNoChangeSeal | None:
    return (
        await session.execute(
            select(CustomImportNoChangeSeal)
            .where(CustomImportNoChangeSeal.execution_id == execution_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _locked_pointer(session: AsyncSession, dataset_id: int) -> CustomImportCurrentGeneration | None:
    return (
        await session.execute(
            select(CustomImportCurrentGeneration)
            .where(CustomImportCurrentGeneration.dataset_id == dataset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _database_now(session: AsyncSession) -> dt.datetime:
    now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    if not isinstance(now, dt.datetime) or now.tzinfo is None:
        raise PublicationConflict("database clock did not return an aware timestamp")
    return now


@asynccontextmanager
async def _finality_scan_window(
    session: AsyncSession,
    *,
    now: dt.datetime,
    expires_at: dt.datetime,
):
    """Bound streamed seal hashing to the exact lease renewed under the locks."""

    remaining_seconds = (expires_at - now).total_seconds()
    if remaining_seconds <= 0:
        raise PublicationConflict("generation sealing lease expired before materialization")
    previous = session.info.get(_FINALITY_SCAN_WINDOW_KEY)
    previous_statement_timeout = await session.scalar(select(func.current_setting("statement_timeout")))
    if not isinstance(previous_statement_timeout, str):
        raise PublicationConflict("database did not return the current statement timeout")
    session.info[_FINALITY_SCAN_WINDOW_KEY] = _FinalityScanWindow(
        expires_at=expires_at,
        monotonic_deadline=time.monotonic() + remaining_seconds,
    )
    has_operation_failed = False
    try:
        yield
    except BaseException:
        has_operation_failed = True
        raise
    finally:
        if previous is None:
            session.info.pop(_FINALITY_SCAN_WINDOW_KEY, None)
        else:
            session.info[_FINALITY_SCAN_WINDOW_KEY] = previous
        try:
            # ``set_config(..., true)`` is transaction-local.  Restore the
            # caller's own budget before final flushes or later work in the
            # caller-owned transaction; a failed SQL statement instead makes
            # the transaction abort, whose rollback resets local settings.
            await session.execute(select(func.set_config("statement_timeout", previous_statement_timeout, True)))
        except Exception:
            if not has_operation_failed:
                raise


def _current_finality_scan_window(session: AsyncSession) -> _FinalityScanWindow | None:
    """Return a well-typed active sealing budget, if graph hashing owns one."""

    scan_window = session.info.get(_FINALITY_SCAN_WINDOW_KEY)
    if scan_window is not None and not isinstance(scan_window, _FinalityScanWindow):
        raise PublicationConflict("finality materialization scan window is invalid")
    return scan_window


async def _prepare_bounded_materialization_statement(session: AsyncSession) -> None:
    """Set the remaining database statement budget before opening a stream."""

    scan_window = _current_finality_scan_window(session)
    if scan_window is None:
        return
    remaining_milliseconds = int((scan_window.monotonic_deadline - time.monotonic()) * 1_000)
    if remaining_milliseconds <= 0:
        raise PublicationConflict("generation sealing exceeded its lease-bounded materialization window")
    await session.execute(select(func.set_config("statement_timeout", str(max(1, remaining_milliseconds)), True)))


def _require_materialization_budget(session: AsyncSession) -> None:
    """Stop a client-side stream at the same deadline as its SQL statements."""

    scan_window = _current_finality_scan_window(session)
    if scan_window is not None and time.monotonic() >= scan_window.monotonic_deadline:
        raise PublicationConflict("generation sealing exceeded its lease-bounded materialization window")


async def _renew_finality_lease(
    session: AsyncSession,
    execution: CustomImportExecution,
    lease: CustomImportLease,
    *,
    fence: int,
    token_sha256: bytes,
    conflict_message: str,
) -> dt.datetime:
    """Extend the exact live lease before a bounded finality graph scan.

    Dataset, execution, and lease are already locked in that order.  The
    guarded update prevents a stale holder from gaining a seal window and
    gives the scan a documented maximum lease horizon.  Other executions'
    heartbeats renew only their execution and lease, without this dataset
    serialization parent.
    """

    now = await _database_now(session)
    if execution.state != "running" or not _has_live_lease_authority(
        lease,
        fence=fence,
        token_sha256=token_sha256,
        now=now,
    ):
        raise PublicationConflict(conflict_message)
    expires_at = now + dt.timedelta(seconds=_SEAL_LEASE_WINDOW_SECONDS)
    renewed = await session.execute(
        update(CustomImportLease)
        .where(
            CustomImportLease.execution_id == execution.execution_id,
            CustomImportLease.fence == fence,
            CustomImportLease.token_sha256 == token_sha256,
            CustomImportLease.expires_at > now,
        )
        .values(heartbeat_at=now, expires_at=expires_at, updated_at=now)
        .returning(CustomImportLease.expires_at)
    )
    if renewed.scalar_one_or_none() is None:
        raise PublicationConflict(conflict_message)
    # Keep the already locked ORM row aligned with the conditional update so
    # the post-scan authority validation observes the new seal window.
    lease.heartbeat_at = now
    lease.expires_at = expires_at
    return now


def _has_live_lease_authority(
    lease: CustomImportLease,
    *,
    fence: int,
    token_sha256: bytes,
    now: dt.datetime,
) -> bool:
    return (
        lease.fence == fence
        and lease.token_sha256 is not None
        and hmac.compare_digest(bytes(lease.token_sha256), token_sha256)
        and lease.expires_at is not None
        and lease.expires_at > now
    )


def _require_expected_pointer(
    pointer: CustomImportCurrentGeneration | None,
    *,
    expected_generation_id: int | None,
    expected_pointer_version: int,
) -> None:
    if pointer is None:
        if expected_generation_id is not None or expected_pointer_version != 0:
            raise PublicationConflict("current generation does not match the expected empty pointer")
        return
    if pointer.generation_id != expected_generation_id or pointer.pointer_version != expected_pointer_version:
        raise PublicationConflict("current generation compare-and-swap failed")


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            raise PublicationConflict("materialization contains a naive timestamp")
        return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, dt.date):
        return value.isoformat()
    raise PublicationConflict("materialization contains an unsupported scalar value")


def _model_document(model: Any, *, omit: frozenset[str] = frozenset({"created_at", "sealed_at"})) -> dict[str, Any]:
    return {
        column.name: _json_value(getattr(model, column.name))
        for column in model.__table__.columns
        if column.name not in omit
    }


_MATERIALIZATION_IDENTITY_COLUMNS = frozenset(
    {
        "base_dataset_id",
        "base_generation_id",
        "capture_bundle_id",
        "child_revision_id",
        "context_child_revision_id",
        "dataset_id",
        "definition_revision_id",
        "entity_binding_id",
        "execution_id",
        "family_revision_id",
        "generation_id",
        "pack_id",
        "producing_execution_id",
        "producing_fence",
        "producing_token_sha256",
        "root_record_id",
        "root_revision_id",
        "schema_revision_id",
    }
)
_MATERIALIZATION_VOLATILE_COLUMNS = frozenset(
    {
        "changed_at",
        "created_at",
        "finished_at",
        "candidate_sha256",
        "heartbeat_at",
        "sealed_at",
        "sealing_fence",
        "sealing_token_sha256",
        "snapshot_token",
        "snapshot_token_sha256",
        "started_at",
        "updated_at",
    }
)


def _materialization_document(model: Any) -> dict[str, Any]:
    """Encode retained semantic content without database allocation artifacts."""

    return _model_document(
        model,
        omit=_MATERIALIZATION_IDENTITY_COLUMNS | _MATERIALIZATION_VOLATILE_COLUMNS,
    )


def _effective_output_revision_document(model: Any) -> dict[str, Any]:
    """Encode served revision content without source-position provenance."""

    return _model_document(
        model,
        omit=(_MATERIALIZATION_IDENTITY_COLUMNS | _MATERIALIZATION_VOLATILE_COLUMNS | frozenset({"source_ordinal"})),
    )


def _new_digest(domain: str) -> hashlib._Hash:
    digest = hashlib.sha256()
    digest.update(b"custom-import/v1\x00")
    digest.update(domain.encode("ascii"))
    digest.update(b"\x00")
    return digest


def _add_digest_record(digest: hashlib._Hash, section: str, document: dict[str, Any]) -> None:
    serialized = json.dumps(
        document, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    digest.update(section.encode("ascii"))
    digest.update(b"\x00")
    digest.update(len(serialized).to_bytes(8, "big"))
    digest.update(serialized)


async def _capture_source_bundle_digest(
    session: AsyncSession,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
) -> bytes:
    """Hash an exactly complete capture bundle for one immutable execution identity."""

    bundle = await _capture_bundle_for_identity(
        session,
        capture_bundle_id=capture_bundle_id,
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    )
    source_stream_slots = await _definition_source_stream_slots(
        session,
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    )
    digest = _new_digest(_SOURCE_BUNDLE_DOMAIN)
    _add_digest_record(
        digest,
        "bundle",
        {"manifest_sha256": _json_value(bundle.manifest_sha256), "stream_count": bundle.stream_count},
    )
    capture_stream_slots = await _add_capture_bundle_material(
        session,
        digest,
        capture_bundle_id=capture_bundle_id,
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
    )
    _require_exact_capture_coverage(bundle, source_stream_slots, capture_stream_slots)
    return digest.digest()


async def _capture_bundle_for_identity(
    session: AsyncSession,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
) -> CustomImportCaptureBundle:
    """Load the bundle only when it has the execution's immutable identity."""

    bundle = (
        await session.execute(
            select(CustomImportCaptureBundle).where(
                CustomImportCaptureBundle.capture_bundle_id == capture_bundle_id,
                CustomImportCaptureBundle.dataset_id == dataset_id,
                CustomImportCaptureBundle.definition_revision_id == definition_revision_id,
                CustomImportCaptureBundle.schema_revision_id == schema_revision_id,
            )
        )
    ).scalar_one_or_none()
    if bundle is None:
        raise PublicationConflict("capture bundle does not match immutable execution identity")
    return bundle


async def _definition_source_stream_slots(
    session: AsyncSession,
    *,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
) -> list[int]:
    """Return the definition's canonical source-stream slots."""

    source_stream_slots: list[int] = []
    async for stream_slot in _stream_materialization_scalars(
        session,
        select(CustomImportSourceStream.stream_slot)
        .where(
            CustomImportSourceStream.definition_revision_id == definition_revision_id,
            CustomImportSourceStream.dataset_id == dataset_id,
            CustomImportSourceStream.schema_revision_id == schema_revision_id,
        )
        .order_by(CustomImportSourceStream.stream_slot),
    ):
        source_stream_slots.append(stream_slot)
    return source_stream_slots


async def _add_capture_bundle_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    *,
    capture_bundle_id: int,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
) -> list[int]:
    """Add captures in stream order and return the covered slots."""

    capture_stream_slots: list[int] = []
    async for capture in _stream_materialization_scalars(
        session,
        select(CustomImportCapture)
        .where(
            CustomImportCapture.capture_bundle_id == capture_bundle_id,
            CustomImportCapture.dataset_id == dataset_id,
            CustomImportCapture.definition_revision_id == definition_revision_id,
            CustomImportCapture.schema_revision_id == schema_revision_id,
        )
        .order_by(CustomImportCapture.stream_slot),
    ):
        capture_stream_slots.append(capture.stream_slot)
        _add_digest_record(
            digest,
            "capture",
            {
                "byte_count": capture.byte_count,
                "content_sha256": _json_value(capture.content_sha256),
                "manifest_sha256": _json_value(capture.manifest_sha256),
                "stream_slot": capture.stream_slot,
            },
        )
    return capture_stream_slots


def _require_exact_capture_coverage(
    bundle: CustomImportCaptureBundle,
    source_stream_slots: list[int],
    capture_stream_slots: list[int],
) -> None:
    """Reject bundles whose recorded captures differ from definition streams."""

    if (
        len(source_stream_slots) != bundle.stream_count
        or len(capture_stream_slots) != bundle.stream_count
        or tuple(capture_stream_slots) != tuple(source_stream_slots)
    ):
        raise PublicationConflict("capture bundle does not exactly cover the definition streams")


async def _materialization(session: AsyncSession, generation: CustomImportGeneration) -> _Materialization:
    """Recompute a canonical digest from precisely the rows serving a generation."""

    source_bundle_sha256 = await _validated_source_bundle_digest(session, generation)
    digest = _new_digest(_MATERIALIZATION_DOMAIN)
    await _add_generation_identity_material(session, digest, generation)
    await _add_capture_material(session, digest, generation)
    profile_count = await _add_definition_shape_material(session, digest, generation)

    await _add_execution_material(session, digest, generation)

    family_count, family_child_count = await _add_family_material(session, digest, generation)
    root_scalar_count = await _add_root_scalar_material(session, digest, generation)
    child_scalar_count = await _add_child_scalar_material(session, digest, generation)
    winner_count = await _add_winner_material(session, digest, generation)
    effective_output_sha256 = await _effective_output_materialization(session, generation)
    return _Materialization(
        source_bundle_sha256=source_bundle_sha256,
        materialization_sha256=digest.digest(),
        effective_output_sha256=effective_output_sha256,
        root_count=family_count,
        family_count=family_count,
        generation_family_count=family_count,
        family_child_count=family_child_count,
        winner_count=winner_count,
        profile_count=profile_count,
        root_scalar_count=root_scalar_count,
        child_scalar_count=child_scalar_count,
    )


async def _effective_output_materialization(
    session: AsyncSession,
    generation: CustomImportGeneration,
) -> bytes:
    """Hash served output independently of capture and execution evidence.

    The complete materialization receipt deliberately binds source captures,
    packs, rejections, and the producer.  No-change needs a narrower,
    explicitly versioned equivalence contract for the result actually served:
    definition/schema semantics plus selected families, projections, and
    winners.  It never accepts a caller-provided digest.
    """

    definition = await session.get(CustomImportDefinitionRevision, generation.definition_revision_id)
    schema_revision = await session.get(CustomImportSchemaRevision, generation.schema_revision_id)
    if (
        definition is None
        or schema_revision is None
        or definition.dataset_id != generation.dataset_id
        or schema_revision.dataset_id != generation.dataset_id
    ):
        raise PublicationConflict("generation definition or schema identity is missing")
    digest = _new_digest(_EFFECTIVE_OUTPUT_DOMAIN)
    _add_digest_record(digest, "definition", _materialization_document(definition))
    _add_digest_record(digest, "schema", _materialization_document(schema_revision))
    await _add_definition_shape_material(session, digest, generation)
    await _add_family_material(session, digest, generation, effective_output=True)
    await _add_root_scalar_material(session, digest, generation)
    await _add_child_scalar_material(session, digest, generation)
    await _add_winner_material(session, digest, generation)
    return digest.digest()


async def _validated_source_bundle_digest(
    session: AsyncSession,
    generation: CustomImportGeneration,
) -> bytes:
    """Return the retained source digest only when it matches the candidate."""

    source_bundle_sha256 = await _capture_source_bundle_digest(
        session,
        capture_bundle_id=generation.capture_bundle_id,
        dataset_id=generation.dataset_id,
        definition_revision_id=generation.definition_revision_id,
        schema_revision_id=generation.schema_revision_id,
    )
    if not hmac.compare_digest(bytes(generation.source_bundle_sha256), source_bundle_sha256):
        raise PublicationConflict("generation source bundle digest does not match retained captures")
    return source_bundle_sha256


async def _add_generation_identity_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    generation: CustomImportGeneration,
) -> None:
    """Add the generation's immutable definition, schema, and capture identities."""

    definition = await session.get(CustomImportDefinitionRevision, generation.definition_revision_id)
    schema_revision = await session.get(CustomImportSchemaRevision, generation.schema_revision_id)
    capture_bundle = await session.get(CustomImportCaptureBundle, generation.capture_bundle_id)
    if (
        definition is None
        or schema_revision is None
        or capture_bundle is None
        or definition.dataset_id != generation.dataset_id
        or schema_revision.dataset_id != generation.dataset_id
    ):
        raise PublicationConflict("generation definition or schema identity is missing")
    _add_digest_record(digest, "generation", _materialization_document(generation))
    _add_digest_record(digest, "definition", _materialization_document(definition))
    _add_digest_record(digest, "schema", _materialization_document(schema_revision))
    _add_digest_record(digest, "capture_bundle", _materialization_document(capture_bundle))


async def _stream_materialization_scalars(session: AsyncSession, statement: Any):
    """Yield one ordered ORM relation without retaining its whole graph in memory."""

    await _prepare_bounded_materialization_statement(session)
    rows = await session.stream_scalars(statement.execution_options(yield_per=_MATERIALIZATION_STREAM_CHUNK_SIZE))
    try:
        async for row in rows:
            _require_materialization_budget(session)
            yield row
            # ``yield`` resumes only after the caller has digested this row.
            # Check again so a small result set with expensive canonical JSON
            # cannot run past the lease window between chunk boundaries.
            _require_materialization_budget(session)
    finally:
        await rows.close()


async def _stream_materialization_records(session: AsyncSession, statement: Any):
    """Yield ordered joined rows with the same bounded cursor discipline."""

    await _prepare_bounded_materialization_statement(session)
    rows = await session.stream(statement.execution_options(yield_per=_MATERIALIZATION_STREAM_CHUNK_SIZE))
    try:
        async for row in rows:
            _require_materialization_budget(session)
            yield row
            _require_materialization_budget(session)
    finally:
        await rows.close()


def _generation_attempt_authority(generation: CustomImportGeneration) -> tuple[int, bytes]:
    """Return the immutable candidate's exact output-attempt fence/token."""

    if generation.producing_fence is None or generation.producing_token_sha256 is None:
        raise PublicationConflict("generation has no producing attempt authority")
    return generation.producing_fence, bytes(generation.producing_token_sha256)


def _pack_attempt_conditions(generation: CustomImportGeneration):
    fence, token_sha256 = _generation_attempt_authority(generation)
    return (
        CustomImportPack.execution_id == generation.execution_id,
        CustomImportPack.producing_fence == fence,
        CustomImportPack.producing_token_sha256 == token_sha256,
    )


def _family_attempt_conditions(generation: CustomImportGeneration):
    fence, token_sha256 = _generation_attempt_authority(generation)
    return (
        CustomImportFamilyRevision.producing_execution_id == generation.execution_id,
        CustomImportFamilyRevision.producing_fence == fence,
        CustomImportFamilyRevision.producing_token_sha256 == token_sha256,
    )


async def _add_capture_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    generation: CustomImportGeneration,
) -> None:
    """Add every retained stream capture in deterministic stream order."""

    captured_streams = (
        select(CustomImportCapture)
        .where(
            CustomImportCapture.capture_bundle_id == generation.capture_bundle_id,
            CustomImportCapture.dataset_id == generation.dataset_id,
            CustomImportCapture.definition_revision_id == generation.definition_revision_id,
            CustomImportCapture.schema_revision_id == generation.schema_revision_id,
        )
        .order_by(CustomImportCapture.stream_slot)
    )
    async for capture in _stream_materialization_scalars(session, captured_streams):
        _add_digest_record(digest, "capture", _materialization_document(capture))


async def _add_schema_shape_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    generation: CustomImportGeneration,
) -> None:
    """Add field slots, fields, and child collections for the selected schema."""

    await _add_materialization_rows(
        session,
        digest,
        "field_slot",
        select(CustomImportFieldSlot)
        .join(
            CustomImportField,
            and_(
                CustomImportField.dataset_id == CustomImportFieldSlot.dataset_id,
                CustomImportField.field_slot == CustomImportFieldSlot.field_slot,
            ),
        )
        .where(
            CustomImportField.dataset_id == generation.dataset_id,
            CustomImportField.schema_revision_id == generation.schema_revision_id,
        )
        .distinct()
        .order_by(CustomImportFieldSlot.field_slot),
    )
    await _add_materialization_rows(
        session,
        digest,
        "field",
        select(CustomImportField)
        .where(
            CustomImportField.dataset_id == generation.dataset_id,
            CustomImportField.schema_revision_id == generation.schema_revision_id,
        )
        .order_by(CustomImportField.collection_slot, CustomImportField.field_slot),
    )
    await _add_materialization_rows(
        session,
        digest,
        "child_collection",
        select(CustomImportChildCollection)
        .where(
            CustomImportChildCollection.dataset_id == generation.dataset_id,
            CustomImportChildCollection.schema_revision_id == generation.schema_revision_id,
        )
        .order_by(CustomImportChildCollection.collection_slot),
    )


async def _add_definition_shape_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    generation: CustomImportGeneration,
) -> int:
    """Add definition-owned shape rows and return the profile count."""

    await _add_schema_shape_material(session, digest, generation)
    has_generation_definition = CustomImportSourceStream.definition_revision_id == generation.definition_revision_id
    await _add_materialization_rows(
        session,
        digest,
        "source_stream",
        select(CustomImportSourceStream)
        .where(has_generation_definition)
        .order_by(CustomImportSourceStream.stream_slot),
    )
    await _add_materialization_rows(
        session,
        digest,
        "field_alias",
        select(CustomImportFieldAlias)
        .where(CustomImportFieldAlias.definition_revision_id == generation.definition_revision_id)
        .order_by(CustomImportFieldAlias.stream_slot, CustomImportFieldAlias.alias_name),
    )
    selection_profile_count = await _add_materialization_rows(
        session,
        digest,
        "selection_profile",
        select(CustomImportSelectionProfile)
        .where(CustomImportSelectionProfile.definition_revision_id == generation.definition_revision_id)
        .order_by(CustomImportSelectionProfile.profile_slot),
    )
    return selection_profile_count


async def _add_materialization_rows(
    session: AsyncSession,
    digest: hashlib._Hash,
    section: str,
    statement: Any,
) -> int:
    """Hash an ordered relation incrementally and return its exact count."""

    record_count = 0
    async for materialization_record in _stream_materialization_scalars(session, statement):
        _add_digest_record(digest, section, _materialization_document(materialization_record))
        record_count += 1
    return record_count


async def _add_execution_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    generation: CustomImportGeneration,
) -> None:
    """Add packs and rejection evidence bound to the producing execution."""

    await _add_materialization_rows(
        session,
        digest,
        "pack",
        select(CustomImportPack)
        .where(
            CustomImportPack.execution_id == generation.execution_id,
            CustomImportPack.dataset_id == generation.dataset_id,
            CustomImportPack.definition_revision_id == generation.definition_revision_id,
            CustomImportPack.schema_revision_id == generation.schema_revision_id,
            CustomImportPack.capture_bundle_id == generation.capture_bundle_id,
            *_pack_attempt_conditions(generation),
        )
        .order_by(CustomImportPack.stream_slot, CustomImportPack.pack_ordinal),
    )
    await _add_materialization_rows(
        session,
        digest,
        "rejection",
        select(CustomImportRejection)
        .where(
            CustomImportRejection.execution_id == generation.execution_id,
            CustomImportRejection.dataset_id == generation.dataset_id,
            CustomImportRejection.definition_revision_id == generation.definition_revision_id,
            CustomImportRejection.schema_revision_id == generation.schema_revision_id,
            CustomImportRejection.producing_fence == generation.producing_fence,
            CustomImportRejection.producing_token_sha256 == generation.producing_token_sha256,
        )
        .order_by(CustomImportRejection.rejection_ordinal),
    )


def _join_generation_family_revision(statement: Any) -> Any:
    """Join a generation-family link to its immutable family revision."""

    return statement.join(
        CustomImportFamilyRevision,
        and_(
            CustomImportFamilyRevision.family_revision_id == CustomImportGenerationFamily.family_revision_id,
            CustomImportFamilyRevision.dataset_id == CustomImportGenerationFamily.dataset_id,
        ),
    )


def _join_family_root_revision(statement: Any) -> Any:
    """Join a family revision to its root revision."""

    return statement.join(
        CustomImportRootRevision,
        and_(
            CustomImportRootRevision.root_revision_id == CustomImportFamilyRevision.root_revision_id,
            CustomImportRootRevision.dataset_id == CustomImportFamilyRevision.dataset_id,
        ),
    )


def _join_root_revision_pack(statement: Any, root_pack: Any) -> Any:
    """Join a root revision to the fenced pack that produced it."""

    return statement.join(
        root_pack,
        and_(
            root_pack.pack_id == CustomImportRootRevision.pack_id,
            root_pack.dataset_id == CustomImportRootRevision.dataset_id,
            root_pack.definition_revision_id == CustomImportRootRevision.definition_revision_id,
            root_pack.schema_revision_id == CustomImportRootRevision.schema_revision_id,
        ),
    )


def _join_generation_family_root_record(statement: Any) -> Any:
    """Join a selected family link to its interned root identity."""

    return statement.join(
        CustomImportRootRecord,
        and_(
            CustomImportRootRecord.root_record_id == CustomImportGenerationFamily.root_record_id,
            CustomImportRootRecord.dataset_id == CustomImportGenerationFamily.dataset_id,
        ),
    )


def _join_family_root_record(statement: Any) -> Any:
    """Join a family revision to its interned root identity."""

    return statement.join(
        CustomImportRootRecord,
        and_(
            CustomImportRootRecord.root_record_id == CustomImportFamilyRevision.root_record_id,
            CustomImportRootRecord.dataset_id == CustomImportFamilyRevision.dataset_id,
        ),
    )


def _join_family_entity_binding(statement: Any) -> Any:
    """Join a family revision to its immutable entity binding."""

    return statement.join(
        CustomImportEntityBinding,
        and_(
            CustomImportEntityBinding.entity_binding_id == CustomImportFamilyRevision.entity_binding_id,
            CustomImportEntityBinding.dataset_id == CustomImportFamilyRevision.dataset_id,
        ),
    )


def _join_family_child(statement: Any) -> Any:
    """Join a family revision to its retained child link."""

    return statement.join(
        CustomImportFamilyChild,
        and_(
            CustomImportFamilyChild.family_revision_id == CustomImportGenerationFamily.family_revision_id,
            CustomImportFamilyChild.dataset_id == CustomImportGenerationFamily.dataset_id,
        ),
    )


def _join_family_child_revision(statement: Any) -> Any:
    """Join a family child link to its immutable child revision."""

    return statement.join(
        CustomImportChildRevision,
        and_(
            CustomImportChildRevision.child_revision_id == CustomImportFamilyChild.child_revision_id,
            CustomImportChildRevision.dataset_id == CustomImportFamilyChild.dataset_id,
        ),
    )


def _join_child_revision_pack(statement: Any, child_pack: Any) -> Any:
    """Join a child revision to the fenced pack that produced it."""

    return statement.join(
        child_pack,
        and_(
            child_pack.pack_id == CustomImportChildRevision.pack_id,
            child_pack.dataset_id == CustomImportChildRevision.dataset_id,
            child_pack.definition_revision_id == CustomImportChildRevision.definition_revision_id,
            child_pack.schema_revision_id == CustomImportChildRevision.schema_revision_id,
        ),
    )


def _where_generation_family_attempt(statement: Any, generation: CustomImportGeneration) -> Any:
    """Keep a materialization relation within its selected producing attempt."""

    return statement.where(
        CustomImportGenerationFamily.generation_id == generation.generation_id,
        CustomImportGenerationFamily.dataset_id == generation.dataset_id,
        *_family_attempt_conditions(generation),
    )


def _where_pack_attempt(statement: Any, pack: Any, generation: CustomImportGeneration) -> Any:
    """Restrict one pack relation to the selected producing attempt."""

    return statement.where(
        pack.execution_id == generation.execution_id,
        pack.producing_fence == generation.producing_fence,
        pack.producing_token_sha256 == generation.producing_token_sha256,
    )


def _root_identity_order() -> tuple[Any, ...]:
    """Return canonical root identity tie breakers shared by graph queries."""

    return (
        CustomImportRootRecord.key_contract_sha256,
        CustomImportRootRecord.logical_key_sha256,
        CustomImportRootRecord.canonical_logical_key,
    )


def _family_material_order() -> tuple[Any, ...]:
    """Return the total order for selected family material."""

    return (
        *_root_identity_order(),
        CustomImportFamilyRevision.family_sha256,
        CustomImportFamilyRevision.child_count,
        CustomImportRootRevision.source_ordinal,
        CustomImportRootRevision.payload_sha256,
        CustomImportRootRevision.canonical_payload,
        CustomImportEntityBinding.adapter_id,
        CustomImportEntityBinding.value_sha256,
        CustomImportEntityBinding.canonical_value,
    )


def _family_child_material_order() -> tuple[Any, ...]:
    """Return the total order for selected family children."""

    return (
        *_root_identity_order(),
        CustomImportFamilyRevision.family_sha256,
        CustomImportFamilyRevision.child_count,
        CustomImportFamilyChild.collection_slot,
        CustomImportChildRevision.child_key_sha256,
        CustomImportChildRevision.canonical_child_key,
        CustomImportChildRevision.payload_sha256,
        CustomImportChildRevision.canonical_payload,
        CustomImportChildRevision.canonical_parent_key,
        CustomImportChildRevision.parent_key_sha256,
        CustomImportChildRevision.source_ordinal,
    )


def _family_material_statement(generation: CustomImportGeneration):
    """Build the selected-root-family relation in canonical digest order."""

    statement = select(
        CustomImportGenerationFamily,
        CustomImportFamilyRevision,
        CustomImportRootRevision,
        CustomImportRootRecord,
        CustomImportEntityBinding,
    ).select_from(CustomImportGenerationFamily)
    statement = _join_generation_family_revision(statement)
    statement = _join_family_root_revision(statement)
    statement = _join_root_revision_pack(statement, CustomImportPack)
    statement = _join_generation_family_root_record(statement)
    statement = _join_family_entity_binding(statement)
    statement = _where_generation_family_attempt(statement, generation)
    statement = _where_pack_attempt(statement, CustomImportPack, generation)
    return statement.order_by(*_family_material_order())


def _family_child_material_statement(generation: CustomImportGeneration):
    """Build selected family children in canonical collection and key order."""

    root_pack = aliased(CustomImportPack)
    child_pack = aliased(CustomImportPack)
    statement = select(
        CustomImportGenerationFamily,
        CustomImportFamilyChild,
        CustomImportChildRevision,
        CustomImportRootRecord,
    ).select_from(CustomImportGenerationFamily)
    statement = _join_generation_family_revision(statement)
    statement = _join_family_child(statement)
    statement = _join_family_root_revision(statement)
    statement = _join_root_revision_pack(statement, root_pack)
    statement = _join_family_child_revision(statement)
    statement = _join_child_revision_pack(statement, child_pack)
    statement = _join_generation_family_root_record(statement)
    statement = _where_generation_family_attempt(statement, generation)
    statement = _where_pack_attempt(statement, root_pack, generation)
    statement = _where_pack_attempt(statement, child_pack, generation)
    return statement.order_by(*_family_child_material_order())


async def _add_family_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    generation: CustomImportGeneration,
    *,
    effective_output: bool = False,
) -> tuple[int, int]:
    """Hash families and children while enforcing exact child-key membership."""

    expected_child_counts_by_family: dict[int, int] = {}
    family_count = 0
    async for family_material_row in _stream_materialization_records(session, _family_material_statement(generation)):
        _add_family_revision_material(
            digest,
            family_material_row,
            expected_child_counts_by_family,
            effective_output=effective_output,
        )
        family_count += 1

    child_counts_by_family: dict[int, int] = {}
    seen_child_logical_keys: set[tuple[int, int, bytes]] = set()
    family_child_count = 0
    async for family_child_material_row in _stream_materialization_records(
        session, _family_child_material_statement(generation)
    ):
        _validate_and_add_family_child(
            digest,
            family_child_material_row,
            expected_child_counts_by_family,
            child_counts_by_family,
            seen_child_logical_keys,
            effective_output=effective_output,
        )
        family_child_count += 1
    if any(
        child_counts_by_family.get(family_revision_id, 0) != child_count
        for family_revision_id, child_count in expected_child_counts_by_family.items()
    ):
        raise PublicationConflict("family child count does not match exact membership")
    return family_count, family_child_count


def _add_family_revision_material(
    digest: hashlib._Hash,
    record: tuple[Any, ...],
    expected_child_counts_by_family: dict[int, int],
    *,
    effective_output: bool,
) -> None:
    """Hash one root family and retain only its declared child-count check."""

    _generation_family, family, root_revision, root_record, entity_binding = record
    expected_child_counts_by_family[family.family_revision_id] = family.child_count
    _add_digest_record(
        digest,
        "generation_family",
        {
            "family_sha256": _json_value(family.family_sha256),
            "root_key_sha256": _json_value(root_record.logical_key_sha256),
        },
    )
    _add_digest_record(digest, "root_record", _materialization_document(root_record))
    _add_digest_record(digest, "family_revision", _materialization_document(family))
    revision_document = (
        _effective_output_revision_document(root_revision)
        if effective_output
        else _materialization_document(root_revision)
    )
    _add_digest_record(digest, "root_revision", revision_document)
    _add_digest_record(digest, "entity_binding", _materialization_document(entity_binding))


def _validate_and_add_family_child(
    digest: hashlib._Hash,
    family_child_material_row: tuple[Any, ...],
    expected_child_counts_by_family: dict[int, int],
    child_counts_by_family: dict[int, int],
    seen_child_logical_keys: set[tuple[int, int, bytes]],
    *,
    effective_output: bool,
) -> None:
    """Hash one child after checking its selected-root identity and uniqueness."""

    generation_family, family_child, child_revision, root_record = family_child_material_row
    if child_revision.canonical_parent_key != root_record.canonical_logical_key or not hmac.compare_digest(
        bytes(child_revision.parent_key_sha256), bytes(root_record.logical_key_sha256)
    ):
        raise PublicationConflict("child parent identity does not match its selected root record")
    child_logical_key = (
        family_child.family_revision_id,
        family_child.collection_slot,
        bytes(child_revision.child_key_sha256),
    )
    if child_logical_key in seen_child_logical_keys:
        raise PublicationConflict("family has duplicate logical child key")
    seen_child_logical_keys.add(child_logical_key)
    family_revision_id = generation_family.family_revision_id
    if family_revision_id not in expected_child_counts_by_family:
        raise PublicationConflict("family child does not belong to the selected generation")
    child_counts_by_family[family_revision_id] = child_counts_by_family.get(family_revision_id, 0) + 1
    _add_digest_record(
        digest,
        "family_child",
        {
            "child_key_sha256": _json_value(child_revision.child_key_sha256),
            "collection_slot": family_child.collection_slot,
            "root_key_sha256": _json_value(root_record.logical_key_sha256),
        },
    )
    revision_document = (
        _effective_output_revision_document(child_revision)
        if effective_output
        else _materialization_document(child_revision)
    )
    _add_digest_record(digest, "child_revision", revision_document)


def _join_root_scalar(statement: Any) -> Any:
    """Join the selected family root revision to one scalar projection."""

    return statement.join(
        CustomImportRootScalar,
        and_(
            CustomImportRootScalar.root_revision_id == CustomImportFamilyRevision.root_revision_id,
            CustomImportRootScalar.dataset_id == CustomImportFamilyRevision.dataset_id,
        ),
    )


def _root_scalar_material_order() -> tuple[Any, ...]:
    """Return the total order for root scalar projections."""

    return (
        *_root_identity_order(),
        CustomImportRootRevision.source_ordinal,
        CustomImportRootRevision.payload_sha256,
        CustomImportRootRevision.canonical_payload,
        CustomImportRootScalar.field_collection_slot,
        CustomImportRootScalar.field_slot,
        CustomImportRootScalar.projection_slot,
        CustomImportRootScalar.field_type,
        CustomImportRootScalar.value_state,
        CustomImportRootScalar.string_value,
        CustomImportRootScalar.integer_value,
        CustomImportRootScalar.decimal_value,
        CustomImportRootScalar.boolean_value,
        CustomImportRootScalar.date_value,
        CustomImportRootScalar.timestamp_value,
    )


def _root_scalar_material_statement(generation: CustomImportGeneration):
    """Build root scalar projections in canonical root and field order."""

    root_pack = aliased(CustomImportPack)
    statement = select(
        CustomImportGenerationFamily,
        CustomImportFamilyRevision,
        CustomImportRootScalar,
        CustomImportRootRecord,
    ).select_from(CustomImportGenerationFamily)
    statement = _join_generation_family_revision(statement)
    statement = _join_family_root_revision(statement)
    statement = _join_root_revision_pack(statement, root_pack)
    statement = _join_root_scalar(statement)
    statement = _join_generation_family_root_record(statement)
    statement = _where_generation_family_attempt(statement, generation)
    statement = _where_pack_attempt(statement, root_pack, generation)
    return statement.order_by(*_root_scalar_material_order())


async def _add_root_scalar_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    generation: CustomImportGeneration,
) -> int:
    """Hash root scalar projections and return their exact retained count."""

    root_scalar_count = 0
    async for _generation_family, _family, scalar, root_record in _stream_materialization_records(
        session, _root_scalar_material_statement(generation)
    ):
        _add_digest_record(
            digest,
            "root_scalar",
            {
                "root_key_sha256": _json_value(root_record.logical_key_sha256),
                "scalar": _materialization_document(scalar),
            },
        )
        root_scalar_count += 1
    return root_scalar_count


def _join_child_scalar(statement: Any) -> Any:
    """Join a selected family child to one scalar projection."""

    return statement.join(
        CustomImportChildScalar,
        and_(
            CustomImportChildScalar.child_revision_id == CustomImportFamilyChild.child_revision_id,
            CustomImportChildScalar.dataset_id == CustomImportFamilyChild.dataset_id,
        ),
    )


def _child_scalar_material_order() -> tuple[Any, ...]:
    """Return the total order for child scalar projections."""

    return (
        *_root_identity_order(),
        CustomImportFamilyRevision.family_sha256,
        CustomImportFamilyChild.collection_slot,
        CustomImportChildRevision.child_key_sha256,
        CustomImportChildRevision.canonical_child_key,
        CustomImportChildRevision.payload_sha256,
        CustomImportChildRevision.canonical_payload,
        CustomImportChildRevision.canonical_parent_key,
        CustomImportChildRevision.parent_key_sha256,
        CustomImportChildRevision.source_ordinal,
        CustomImportChildScalar.field_collection_slot,
        CustomImportChildScalar.field_slot,
        CustomImportChildScalar.projection_slot,
        CustomImportChildScalar.field_type,
        CustomImportChildScalar.value_state,
        CustomImportChildScalar.string_value,
        CustomImportChildScalar.integer_value,
        CustomImportChildScalar.decimal_value,
        CustomImportChildScalar.boolean_value,
        CustomImportChildScalar.date_value,
        CustomImportChildScalar.timestamp_value,
    )


def _child_scalar_material_statement(generation: CustomImportGeneration):
    """Build child scalar projections in canonical root, collection, key, and field order."""

    root_pack = aliased(CustomImportPack)
    child_pack = aliased(CustomImportPack)
    statement = select(
        CustomImportGenerationFamily,
        CustomImportFamilyChild,
        CustomImportChildScalar,
        CustomImportChildRevision,
        CustomImportRootRecord,
    ).select_from(CustomImportGenerationFamily)
    statement = _join_generation_family_revision(statement)
    statement = _join_family_child(statement)
    statement = _join_family_root_revision(statement)
    statement = _join_root_revision_pack(statement, root_pack)
    statement = _join_family_child_revision(statement)
    statement = _join_child_revision_pack(statement, child_pack)
    statement = _join_child_scalar(statement)
    statement = _join_generation_family_root_record(statement)
    statement = _where_generation_family_attempt(statement, generation)
    statement = _where_pack_attempt(statement, root_pack, generation)
    statement = _where_pack_attempt(statement, child_pack, generation)
    return statement.order_by(*_child_scalar_material_order())


async def _add_child_scalar_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    generation: CustomImportGeneration,
) -> int:
    """Hash child scalar projections and return their exact retained count."""

    child_scalar_count = 0
    async for _generation_family, _family_child, scalar, child_revision, root_record in _stream_materialization_records(
        session, _child_scalar_material_statement(generation)
    ):
        _add_digest_record(
            digest,
            "child_scalar",
            {
                "child_key_sha256": _json_value(child_revision.child_key_sha256),
                "root_key_sha256": _json_value(root_record.logical_key_sha256),
                "scalar": _materialization_document(scalar),
            },
        )
        child_scalar_count += 1
    return child_scalar_count


def _join_winner_generation_family(statement: Any) -> Any:
    """Join a winner to its selected generation family."""

    return statement.join(
        CustomImportGenerationFamily,
        and_(
            CustomImportGenerationFamily.generation_id == CustomImportWinner.generation_id,
            CustomImportGenerationFamily.dataset_id == CustomImportWinner.dataset_id,
            CustomImportGenerationFamily.family_revision_id == CustomImportWinner.family_revision_id,
        ),
    )


def _join_winner_entity_binding(statement: Any) -> Any:
    """Join a winner to its immutable entity binding."""

    return statement.join(
        CustomImportEntityBinding,
        and_(
            CustomImportEntityBinding.entity_binding_id == CustomImportWinner.entity_binding_id,
            CustomImportEntityBinding.dataset_id == CustomImportWinner.dataset_id,
        ),
    )


def _join_winner_selection_profile(statement: Any) -> Any:
    """Join a winner to the selection profile that defines its context."""

    return statement.join(
        CustomImportSelectionProfile,
        and_(
            CustomImportSelectionProfile.definition_revision_id == CustomImportWinner.definition_revision_id,
            CustomImportSelectionProfile.dataset_id == CustomImportWinner.dataset_id,
            CustomImportSelectionProfile.schema_revision_id == CustomImportWinner.schema_revision_id,
            CustomImportSelectionProfile.profile_slot == CustomImportWinner.profile_slot,
        ),
    )


def _join_winner_family_revision(statement: Any) -> Any:
    """Join a winner to the family whose selection it records."""

    return statement.join(
        CustomImportFamilyRevision,
        and_(
            CustomImportFamilyRevision.family_revision_id == CustomImportWinner.family_revision_id,
            CustomImportFamilyRevision.dataset_id == CustomImportWinner.dataset_id,
        ),
    )


def _outerjoin_winner_context_child(statement: Any, context_pack: Any) -> Any:
    """Join an optional winner context child and the pack that produced it."""

    return statement.outerjoin(
        CustomImportChildRevision,
        and_(
            CustomImportChildRevision.child_revision_id == CustomImportWinner.context_child_revision_id,
            CustomImportChildRevision.dataset_id == CustomImportWinner.dataset_id,
        ),
    ).outerjoin(
        context_pack,
        and_(
            context_pack.pack_id == CustomImportChildRevision.pack_id,
            context_pack.dataset_id == CustomImportChildRevision.dataset_id,
            context_pack.definition_revision_id == CustomImportChildRevision.definition_revision_id,
            context_pack.schema_revision_id == CustomImportChildRevision.schema_revision_id,
        ),
    )


def _where_winner_generation_attempt(statement: Any, generation: CustomImportGeneration) -> Any:
    """Keep winner material within the generation and family producing attempt."""

    return statement.where(
        CustomImportWinner.generation_id == generation.generation_id,
        CustomImportWinner.dataset_id == generation.dataset_id,
        *_family_attempt_conditions(generation),
    )


def _where_winner_context_attempt(statement: Any, context_pack: Any, generation: CustomImportGeneration) -> Any:
    """Require an optional winner context child to share the producing attempt."""

    return statement.where(
        or_(
            CustomImportWinner.context_child_revision_id.is_(None),
            and_(
                context_pack.execution_id == generation.execution_id,
                context_pack.producing_fence == generation.producing_fence,
                context_pack.producing_token_sha256 == generation.producing_token_sha256,
            ),
        )
    )


def _winner_material_order() -> tuple[Any, ...]:
    """Return the total semantic order for winner selections."""

    return (
        CustomImportWinner.profile_slot,
        CustomImportEntityBinding.adapter_id,
        CustomImportEntityBinding.value_sha256,
        CustomImportEntityBinding.canonical_value,
        *_root_identity_order(),
        CustomImportChildRevision.child_key_sha256,
        CustomImportChildRevision.canonical_child_key,
        CustomImportChildRevision.payload_sha256,
        CustomImportChildRevision.canonical_payload,
        CustomImportFamilyRevision.family_sha256,
        CustomImportWinner.context_collection_slot,
        CustomImportWinner.context_key_sha256,
    )


def _winner_material_statement(generation: CustomImportGeneration):
    """Build winner rows in selection-profile and entity-context digest order."""

    root_pack = aliased(CustomImportPack)
    context_pack = aliased(CustomImportPack)
    statement = select(
        CustomImportWinner,
        CustomImportEntityBinding,
        CustomImportFamilyRevision,
        CustomImportRootRecord,
        CustomImportChildRevision,
        CustomImportSelectionProfile,
    ).select_from(CustomImportWinner)
    statement = _join_winner_generation_family(statement)
    statement = _join_winner_entity_binding(statement)
    statement = _join_winner_selection_profile(statement)
    statement = _join_winner_family_revision(statement)
    statement = _join_family_root_revision(statement)
    statement = _join_root_revision_pack(statement, root_pack)
    statement = _join_family_root_record(statement)
    statement = _outerjoin_winner_context_child(statement, context_pack)
    statement = _where_winner_generation_attempt(statement, generation)
    statement = _where_pack_attempt(statement, root_pack, generation)
    statement = _where_winner_context_attempt(statement, context_pack, generation)
    return statement.order_by(*_winner_material_order())


async def _add_winner_material(
    session: AsyncSession,
    digest: hashlib._Hash,
    generation: CustomImportGeneration,
) -> int:
    """Hash winner selections and their entity binding evidence."""

    winner_count = 0
    async for winner, entity_binding, family, root_record, context_child, profile in _stream_materialization_records(
        session, _winner_material_statement(generation)
    ):
        expected_context_collection_slot = profile.context_collection_slot or 0
        if winner.context_collection_slot != expected_context_collection_slot:
            raise PublicationConflict("winner context collection does not match its selection profile")
        winner_fields_by_name = _materialization_document(winner)
        winner_fields_by_name.update(
            {
                "context_child_key_sha256": (
                    None if context_child is None else _json_value(context_child.child_key_sha256)
                ),
                "family_sha256": _json_value(family.family_sha256),
                "root_key_sha256": _json_value(root_record.logical_key_sha256),
            }
        )
        _add_digest_record(digest, "winner", winner_fields_by_name)
        _add_digest_record(digest, "winner_binding", _materialization_document(entity_binding))
        winner_count += 1
    return winner_count


def _seal_receipt(seal: CustomImportGenerationSeal, *, replayed: bool) -> GenerationSealReceipt:
    return GenerationSealReceipt(
        generation_id=seal.generation_id,
        dataset_id=seal.dataset_id,
        execution_id=seal.execution_id,
        materialization_sha256=bytes(seal.materialization_sha256).hex(),
        effective_output_sha256=bytes(seal.effective_output_sha256).hex(),
        root_count=seal.root_count,
        family_count=seal.family_count,
        generation_family_count=seal.generation_family_count,
        family_child_count=seal.family_child_count,
        winner_count=seal.winner_count,
        profile_count=seal.profile_count,
        root_scalar_count=seal.root_scalar_count,
        child_scalar_count=seal.child_scalar_count,
        replayed=replayed,
    )


def _validate_generation_seal_identity(
    seal: CustomImportGenerationSeal,
    generation: CustomImportGeneration,
) -> None:
    """Validate immutable seal ownership without rescanning its frozen graph."""

    sealed_identity_fields = (
        (seal.dataset_id, generation.dataset_id),
        (seal.definition_revision_id, generation.definition_revision_id),
        (seal.schema_revision_id, generation.schema_revision_id),
        (seal.execution_id, generation.execution_id),
        (seal.capture_bundle_id, generation.capture_bundle_id),
    )
    if (
        seal.seal_contract != _GENERATION_SEAL_CONTRACT
        or any(left != right for left, right in sealed_identity_fields)
        or not all(
            isinstance(digest_value, (bytes, bytearray, memoryview)) and len(digest_value) == 32
            for digest_value in (
                seal.sealing_token_sha256,
                seal.materialization_sha256,
                seal.effective_output_sha256,
            )
        )
    ):
        raise PublicationConflict("generation seal does not match immutable materialization")
    if (
        generation.producing_fence is None
        or generation.producing_token_sha256 is None
        or seal.sealing_fence != generation.producing_fence
        or not hmac.compare_digest(bytes(seal.sealing_token_sha256), bytes(generation.producing_token_sha256))
    ):
        raise PublicationConflict("generation seal authority differs from producing authority")


def _validate_generation_seal(
    seal: CustomImportGenerationSeal,
    generation: CustomImportGeneration,
    materialization: _Materialization,
) -> None:
    _validate_generation_seal_identity(seal, generation)
    sealed_materialization_fields = (
        (seal.root_count, materialization.root_count),
        (seal.family_count, materialization.family_count),
        (seal.generation_family_count, materialization.generation_family_count),
        (seal.family_child_count, materialization.family_child_count),
        (seal.winner_count, materialization.winner_count),
        (seal.profile_count, materialization.profile_count),
        (seal.root_scalar_count, materialization.root_scalar_count),
        (seal.child_scalar_count, materialization.child_scalar_count),
    )
    if any(left != right for left, right in sealed_materialization_fields):
        raise PublicationConflict("generation seal does not match immutable materialization")
    if not hmac.compare_digest(bytes(seal.materialization_sha256), materialization.materialization_sha256):
        raise PublicationConflict("generation seal materialization digest differs")
    if not hmac.compare_digest(bytes(seal.effective_output_sha256), materialization.effective_output_sha256):
        raise PublicationConflict("generation seal effective output digest differs")


def _generation_seal_replay(
    seal: CustomImportGenerationSeal,
    generation: CustomImportGeneration,
    request: _GenerationSealRequest,
) -> GenerationSealReceipt:
    _validate_generation_seal_identity(seal, generation)
    if seal.sealing_fence != request.lease_fence or not hmac.compare_digest(
        bytes(seal.sealing_token_sha256), request.token_sha256
    ):
        raise PublicationConflict("generation seal replay authority differs")
    return _seal_receipt(seal, replayed=True)


async def _validated_generation_seal(
    session: AsyncSession,
    generation: CustomImportGeneration,
) -> CustomImportGenerationSeal:
    seal = await _locked_generation_seal(session, generation.generation_id)
    if seal is None:
        raise PublicationConflict("target generation is not sealed")
    _validate_generation_seal_identity(seal, generation)
    return seal


def _generation_seal_request(
    *,
    dataset_id: int,
    generation_id: int,
    lease_fence: int,
    lease_token: str | bytes | bytearray | memoryview,
) -> _GenerationSealRequest:
    try:
        token_sha256 = lease_token_sha256(lease_token)
    except ValueError as exc:
        raise PublicationConflict(str(exc)) from exc
    return _GenerationSealRequest(
        dataset_id=_positive_integer(dataset_id, "dataset id"),
        generation_id=_positive_integer(generation_id, "generation id"),
        lease_fence=_positive_integer(lease_fence, "lease fence"),
        token_sha256=token_sha256,
    )


async def _replayed_generation_seal(
    session: AsyncSession,
    generation: CustomImportGeneration,
    request: _GenerationSealRequest,
) -> GenerationSealReceipt | None:
    """Return an exact sealed replay only when its receipt still validates."""

    existing_seal = await _locked_generation_seal(session, generation.generation_id)
    if existing_seal is None:
        return None
    return _generation_seal_replay(
        existing_seal,
        generation,
        request,
    )


async def _lock_generation_sealing_authority(
    session: AsyncSession,
    request: _GenerationSealRequest,
    generation_snapshot: CustomImportGeneration,
) -> tuple[CustomImportExecution, CustomImportLease, CustomImportGeneration]:
    """Acquire execution, lease, and candidate locks after the dataset lock."""

    execution = await _locked_execution(
        session,
        execution_id=generation_snapshot.execution_id,
        dataset_id=request.dataset_id,
    )
    lease = await _locked_lease(session, execution.execution_id)
    generation = await _locked_generation(
        session,
        dataset_id=request.dataset_id,
        generation_id=request.generation_id,
    )
    return execution, lease, generation


def _validate_generation_sealing_authority(
    execution: CustomImportExecution,
    lease: CustomImportLease,
    generation: CustomImportGeneration,
    materialization: _Materialization,
    request: _GenerationSealRequest,
    now: dt.datetime,
) -> None:
    """Require the current holder and generation counts before issuing a seal."""

    if execution.state != "running" or not _has_live_lease_authority(
        lease,
        fence=request.lease_fence,
        token_sha256=request.token_sha256,
        now=now,
    ):
        raise PublicationConflict("generation sealing requires a current running lease")
    if (
        generation.execution_id != execution.execution_id
        or generation.producing_fence != request.lease_fence
        or generation.producing_token_sha256 is None
        or not hmac.compare_digest(bytes(generation.producing_token_sha256), request.token_sha256)
    ):
        raise PublicationConflict("generation producing authority is stale")
    if generation.root_count != materialization.root_count or generation.family_count != materialization.family_count:
        raise PublicationConflict("generation root or family count does not match exact membership")


def _new_generation_seal(
    generation: CustomImportGeneration,
    materialization: _Materialization,
    request: _GenerationSealRequest,
) -> CustomImportGenerationSeal:
    """Build the immutable finality receipt from one verified materialization."""

    return CustomImportGenerationSeal(
        generation_id=generation.generation_id,
        dataset_id=generation.dataset_id,
        definition_revision_id=generation.definition_revision_id,
        schema_revision_id=generation.schema_revision_id,
        execution_id=generation.execution_id,
        capture_bundle_id=generation.capture_bundle_id,
        seal_contract=_GENERATION_SEAL_CONTRACT,
        sealing_fence=request.lease_fence,
        sealing_token_sha256=request.token_sha256,
        root_count=materialization.root_count,
        family_count=materialization.family_count,
        generation_family_count=materialization.generation_family_count,
        family_child_count=materialization.family_child_count,
        winner_count=materialization.winner_count,
        profile_count=materialization.profile_count,
        root_scalar_count=materialization.root_scalar_count,
        child_scalar_count=materialization.child_scalar_count,
        materialization_sha256=materialization.materialization_sha256,
        effective_output_sha256=materialization.effective_output_sha256,
    )


async def _complete_generation_sealing_execution(
    session: AsyncSession,
    execution: CustomImportExecution,
    request: _GenerationSealRequest,
    now: dt.datetime,
) -> None:
    """Complete the producer and expire its exact fenced lease together."""

    execution_update = await session.execute(
        update(CustomImportExecution)
        .where(
            CustomImportExecution.execution_id == execution.execution_id,
            CustomImportExecution.state == "running",
        )
        .values(state="completed", finished_at=now, updated_at=now)
    )
    lease_update = await session.execute(
        update(CustomImportLease)
        .where(
            CustomImportLease.execution_id == execution.execution_id,
            CustomImportLease.fence == request.lease_fence,
            CustomImportLease.token_sha256 == request.token_sha256,
            CustomImportLease.expires_at > now,
        )
        .values(heartbeat_at=now, expires_at=now, updated_at=now)
    )
    if execution_update.rowcount != 1 or lease_update.rowcount != 1:
        raise PublicationConflict("generation producer changed while sealing")


async def _seal_live_generation(
    session: AsyncSession,
    execution: CustomImportExecution,
    lease: CustomImportLease,
    generation: CustomImportGeneration,
    request: _GenerationSealRequest,
) -> GenerationSealReceipt:
    """Persist a new seal and terminalize its verified live producer."""

    renewed_at = await _renew_finality_lease(
        session,
        execution,
        lease,
        fence=request.lease_fence,
        token_sha256=request.token_sha256,
        conflict_message="generation sealing requires a current running lease",
    )
    async with _finality_scan_window(session, now=renewed_at, expires_at=lease.expires_at):
        materialization = await _materialization(session, generation)
        _require_materialization_budget(session)
    now = await _database_now(session)
    _validate_generation_sealing_authority(execution, lease, generation, materialization, request, now)
    generation_seal = _new_generation_seal(generation, materialization, request)
    session.add(generation_seal)
    await session.flush()
    await _complete_generation_sealing_execution(session, execution, request, now)
    return _seal_receipt(generation_seal, replayed=False)


async def seal_generation(
    session: AsyncSession,
    *,
    dataset_id: int,
    generation_id: int,
    lease_fence: int,
    lease_token: str | bytes | bytearray | memoryview,
) -> GenerationSealReceipt:
    """Seal one live-fence candidate and complete its execution atomically."""

    await _begin_finality_operation(session)
    request = _generation_seal_request(
        dataset_id=dataset_id,
        generation_id=generation_id,
        lease_fence=lease_fence,
        lease_token=lease_token,
    )
    await _locked_dataset(session, request.dataset_id)
    generation_snapshot = await _generation_snapshot(
        session,
        dataset_id=request.dataset_id,
        generation_id=request.generation_id,
    )
    replay = await _replayed_generation_seal(session, generation_snapshot, request)
    if replay is not None:
        return replay
    execution, lease, generation = await _lock_generation_sealing_authority(
        session,
        request,
        generation_snapshot,
    )
    replay = await _replayed_generation_seal(session, generation, request)
    if replay is not None:
        return replay
    return await _seal_live_generation(session, execution, lease, generation, request)


def _generation_publication_request(
    *,
    event_kind: Literal["activated", "rolled_back"],
    dataset_id: int,
    target_generation_id: int,
    expected_generation_id: int | None,
    expected_pointer_version: int,
) -> _GenerationPublicationRequest:
    pointer_version = _pointer_version(expected_pointer_version)
    return _GenerationPublicationRequest(
        event_kind=event_kind,
        dataset_id=_positive_integer(dataset_id, "dataset id"),
        target_generation_id=_positive_integer(target_generation_id, "target generation id"),
        expected_generation_id=(
            None
            if expected_generation_id is None
            else _positive_integer(expected_generation_id, "expected generation id")
        ),
        expected_pointer_version=pointer_version,
        committed_pointer_version=_increment_pointer_version(pointer_version),
    )


async def _exact_event(
    session: AsyncSession,
    *,
    dataset_id: int,
    execution_id: int,
    event_kind: PublicationKind,
    from_generation_id: int | None,
    to_generation_id: int,
    expected_pointer_version: int,
    committed_pointer_version: int,
) -> CustomImportPublicationEvent | None:
    conditions = [
        CustomImportPublicationEvent.dataset_id == dataset_id,
        CustomImportPublicationEvent.execution_id == execution_id,
        CustomImportPublicationEvent.event_kind == event_kind,
        CustomImportPublicationEvent.to_generation_id == to_generation_id,
        CustomImportPublicationEvent.expected_pointer_version == expected_pointer_version,
        CustomImportPublicationEvent.committed_pointer_version == committed_pointer_version,
    ]
    conditions.append(
        CustomImportPublicationEvent.from_generation_id.is_(None)
        if from_generation_id is None
        else CustomImportPublicationEvent.from_generation_id == from_generation_id
    )
    return (await session.execute(select(CustomImportPublicationEvent).where(*conditions))).scalar_one_or_none()


def _validate_target_generation(
    request: _GenerationPublicationRequest,
    target_generation: CustomImportGeneration,
) -> None:
    if request.event_kind == "rolled_back" and request.expected_generation_id is None:
        raise PublicationConflict("rollback requires an existing current generation")
    if request.event_kind == "activated" and target_generation.base_generation_id != request.expected_generation_id:
        raise PublicationConflict("activation target base generation does not match the expected generation")


def _validate_generation_producer(
    producer: CustomImportExecution,
    generation: CustomImportGeneration,
    seal: CustomImportGenerationSeal,
) -> None:
    if producer.state != "completed":
        raise PublicationConflict("target generation producer is not completed")
    if (
        producer.execution_id != generation.execution_id
        or producer.definition_revision_id != generation.definition_revision_id
        or producer.schema_revision_id != generation.schema_revision_id
        or producer.capture_bundle_id != generation.capture_bundle_id
        or seal.execution_id != producer.execution_id
    ):
        raise PublicationConflict("target generation producer identity is inconsistent")


async def _advance_generation_pointer(
    session: AsyncSession,
    pointer: CustomImportCurrentGeneration | None,
    request: _GenerationPublicationRequest,
    target_generation: CustomImportGeneration,
) -> None:
    _require_expected_pointer(
        pointer,
        expected_generation_id=request.expected_generation_id,
        expected_pointer_version=request.expected_pointer_version,
    )
    if request.expected_generation_id == request.target_generation_id:
        raise PublicationConflict("publication target is already current")
    if pointer is None:
        session.add(
            CustomImportCurrentGeneration(
                dataset_id=request.dataset_id,
                definition_revision_id=target_generation.definition_revision_id,
                schema_revision_id=target_generation.schema_revision_id,
                generation_id=request.target_generation_id,
                pointer_version=request.committed_pointer_version,
            )
        )
        return
    pointer_update = await session.execute(
        update(CustomImportCurrentGeneration)
        .where(
            CustomImportCurrentGeneration.dataset_id == request.dataset_id,
            CustomImportCurrentGeneration.generation_id == request.expected_generation_id,
            CustomImportCurrentGeneration.pointer_version == request.expected_pointer_version,
        )
        .values(
            definition_revision_id=target_generation.definition_revision_id,
            schema_revision_id=target_generation.schema_revision_id,
            generation_id=request.target_generation_id,
            pointer_version=request.committed_pointer_version,
            changed_at=func.clock_timestamp(),
        )
    )
    if pointer_update.rowcount != 1:
        raise PublicationConflict("current generation changed during publication")


def _new_publication_event(details: _PublicationEventDetails) -> CustomImportPublicationEvent:
    canonical, digest = _event_document(details)
    return CustomImportPublicationEvent(
        dataset_id=details.dataset_id,
        definition_revision_id=details.definition_revision_id,
        schema_revision_id=details.schema_revision_id,
        execution_id=details.execution_id,
        event_kind=details.event_kind,
        from_generation_id=details.from_generation_id,
        to_generation_id=details.to_generation_id,
        expected_pointer_version=details.expected_pointer_version,
        committed_pointer_version=details.committed_pointer_version,
        finality_contract=FINALITY_EVENT_CONTRACT,
        canonical_event=canonical,
        event_sha256=digest,
    )


def _generation_publication_event(
    request: _GenerationPublicationRequest,
    generation: CustomImportGeneration,
) -> CustomImportPublicationEvent:
    """Build the immutable event describing one generation-pointer transition."""

    return _new_publication_event(
        _PublicationEventDetails(
            dataset_id=request.dataset_id,
            definition_revision_id=generation.definition_revision_id,
            schema_revision_id=generation.schema_revision_id,
            execution_id=generation.execution_id,
            event_kind=request.event_kind,
            from_generation_id=request.expected_generation_id,
            to_generation_id=request.target_generation_id,
            expected_pointer_version=request.expected_pointer_version,
            committed_pointer_version=request.committed_pointer_version,
        )
    )


async def _publish_generation(
    session: AsyncSession,
    *,
    event_kind: Literal["activated", "rolled_back"],
    dataset_id: int,
    target_generation_id: int,
    expected_generation_id: int | None,
    expected_pointer_version: int,
) -> PublicationReceipt:
    """Move a pointer once or return its exact immutable transition replay."""

    await _begin_finality_operation(session)
    request = _generation_publication_request(
        event_kind=event_kind,
        dataset_id=dataset_id,
        target_generation_id=target_generation_id,
        expected_generation_id=expected_generation_id,
        expected_pointer_version=expected_pointer_version,
    )
    await _locked_dataset(session, request.dataset_id)
    execution_id = await _generation_execution_id(
        session,
        dataset_id=request.dataset_id,
        generation_id=request.target_generation_id,
    )
    producer = await _locked_execution(session, execution_id=execution_id, dataset_id=request.dataset_id)
    target_generation = await _locked_generation(
        session,
        dataset_id=request.dataset_id,
        generation_id=request.target_generation_id,
    )
    seal = await _validated_generation_seal(session, target_generation)
    replay = await _exact_event(
        session,
        dataset_id=request.dataset_id,
        execution_id=target_generation.execution_id,
        event_kind=request.event_kind,
        from_generation_id=request.expected_generation_id,
        to_generation_id=request.target_generation_id,
        expected_pointer_version=request.expected_pointer_version,
        committed_pointer_version=request.committed_pointer_version,
    )
    if replay is not None:
        _verify_event_material(replay)
        return _receipt(replay, replayed=True)
    _validate_target_generation(request, target_generation)
    _validate_generation_producer(producer, target_generation, seal)
    pointer = await _locked_pointer(session, request.dataset_id)
    await _advance_generation_pointer(session, pointer, request, target_generation)
    event = _generation_publication_event(request, target_generation)
    session.add(event)
    await session.flush()
    return _receipt(event, replayed=False)


async def activate_generation(
    session: AsyncSession,
    *,
    dataset_id: int,
    target_generation_id: int,
    expected_generation_id: int | None,
    expected_pointer_version: int,
) -> PublicationReceipt:
    """Activate exactly one sealed completed generation."""

    return await _publish_generation(
        session,
        event_kind="activated",
        dataset_id=dataset_id,
        target_generation_id=target_generation_id,
        expected_generation_id=expected_generation_id,
        expected_pointer_version=expected_pointer_version,
    )


async def rollback_generation(
    session: AsyncSession,
    *,
    dataset_id: int,
    target_generation_id: int,
    expected_generation_id: int,
    expected_pointer_version: int,
) -> PublicationReceipt:
    """Move the pointer to exactly one sealed retained generation."""

    return await _publish_generation(
        session,
        event_kind="rolled_back",
        dataset_id=dataset_id,
        target_generation_id=target_generation_id,
        expected_generation_id=expected_generation_id,
        expected_pointer_version=expected_pointer_version,
    )


def _no_change_request(
    *,
    dataset_id: int,
    execution_id: int,
    expected_generation_id: int,
    expected_pointer_version: int,
    candidate_generation_id: int,
    lease_fence: int,
    lease_token: str | bytes | bytearray | memoryview,
) -> _NoChangeRequest:
    try:
        token_sha256 = lease_token_sha256(lease_token)
    except ValueError as exc:
        raise PublicationConflict(str(exc)) from exc
    return _NoChangeRequest(
        dataset_id=_positive_integer(dataset_id, "dataset id"),
        execution_id=_positive_integer(execution_id, "execution id"),
        expected_generation_id=_positive_integer(expected_generation_id, "expected generation id"),
        expected_pointer_version=_pointer_version(expected_pointer_version),
        candidate_generation_id=_positive_integer(candidate_generation_id, "candidate generation id"),
        lease_fence=_positive_integer(lease_fence, "lease fence"),
        token_sha256=token_sha256,
    )


def _no_change_receipt_document(
    request: _NoChangeRequest,
    execution: CustomImportExecution,
    *,
    base_generation: CustomImportGeneration,
    candidate_generation: CustomImportGeneration,
    base_seal: CustomImportGenerationSeal,
    candidate_seal: CustomImportGenerationSeal,
) -> tuple[str, bytes]:
    receipt_fields_by_name = {
        "base_generation_id": base_generation.generation_id,
        "base_effective_output_sha256": bytes(base_seal.effective_output_sha256).hex(),
        "base_pointer_version": request.expected_pointer_version,
        "base_source_bundle_sha256": bytes(base_generation.source_bundle_sha256).hex(),
        "candidate_generation_id": candidate_generation.generation_id,
        "candidate_source_bundle_sha256": bytes(candidate_generation.source_bundle_sha256).hex(),
        "capture_bundle_id": execution.capture_bundle_id,
        "contract": _NO_CHANGE_SEAL_CONTRACT,
        "dataset_id": request.dataset_id,
        "definition_revision_id": execution.definition_revision_id,
        "effective_output_sha256": bytes(candidate_seal.effective_output_sha256).hex(),
        "execution_id": execution.execution_id,
        "schema_revision_id": execution.schema_revision_id,
        "sealing_fence": request.lease_fence,
        "sealing_token_sha256": request.token_sha256.hex(),
    }
    canonical = json.dumps(
        receipt_fields_by_name,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = _new_digest(_NO_CHANGE_RECEIPT_DOMAIN)
    digest.update(canonical.encode("utf-8"))
    return canonical, digest.digest()


def _validate_no_change_seal(
    seal: CustomImportNoChangeSeal,
    request: _NoChangeRequest,
    execution: CustomImportExecution,
    base_generation: CustomImportGeneration,
    candidate_generation: CustomImportGeneration,
    base_seal: CustomImportGenerationSeal,
    candidate_seal: CustomImportGenerationSeal,
) -> None:
    if (
        seal.seal_contract != _NO_CHANGE_SEAL_CONTRACT
        or seal.dataset_id != request.dataset_id
        or seal.execution_id != request.execution_id
        or seal.base_generation_id != request.expected_generation_id
        or seal.candidate_generation_id != request.candidate_generation_id
        or seal.base_pointer_version != request.expected_pointer_version
        or seal.definition_revision_id != execution.definition_revision_id
        or seal.schema_revision_id != execution.schema_revision_id
        or seal.capture_bundle_id != execution.capture_bundle_id
        or seal.sealing_fence != request.lease_fence
        or not hmac.compare_digest(bytes(seal.sealing_token_sha256), request.token_sha256)
        or not hmac.compare_digest(bytes(seal.base_source_bundle_sha256), bytes(base_generation.source_bundle_sha256))
        or not hmac.compare_digest(
            bytes(seal.candidate_source_bundle_sha256), bytes(candidate_generation.source_bundle_sha256)
        )
        or not hmac.compare_digest(
            bytes(base_seal.effective_output_sha256), bytes(candidate_seal.effective_output_sha256)
        )
        or not hmac.compare_digest(bytes(seal.effective_output_sha256), bytes(candidate_seal.effective_output_sha256))
    ):
        raise PublicationConflict("no-change replay does not match its immutable receipt")
    canonical, receipt_sha256 = _no_change_receipt_document(
        request,
        execution,
        base_generation=base_generation,
        candidate_generation=candidate_generation,
        base_seal=base_seal,
        candidate_seal=candidate_seal,
    )
    if seal.canonical_receipt != canonical or not hmac.compare_digest(bytes(seal.receipt_sha256), receipt_sha256):
        raise PublicationConflict("no-change receipt is not canonical")


def _validate_no_change_event(
    event: CustomImportPublicationEvent,
    seal: CustomImportNoChangeSeal,
) -> None:
    """Bind an immutable no-change event to its immutable finality receipt."""

    if (
        event.event_kind != "no_change"
        or event.finality_contract != FINALITY_EVENT_CONTRACT
        or event.dataset_id != seal.dataset_id
        or event.execution_id != seal.execution_id
        or event.definition_revision_id != seal.definition_revision_id
        or event.schema_revision_id != seal.schema_revision_id
        or event.from_generation_id != seal.base_generation_id
        or event.to_generation_id != seal.base_generation_id
        or event.expected_pointer_version != seal.base_pointer_version
        or event.committed_pointer_version != seal.base_pointer_version
    ):
        raise PublicationConflict("no-change publication event does not match its immutable receipt")
    _verify_event_material(event)


async def _no_change_event(
    session: AsyncSession,
    request: _NoChangeRequest,
) -> CustomImportPublicationEvent | None:
    return (
        await session.execute(
            select(CustomImportPublicationEvent).where(
                CustomImportPublicationEvent.execution_id == request.execution_id,
                CustomImportPublicationEvent.dataset_id == request.dataset_id,
                CustomImportPublicationEvent.event_kind == "no_change",
            )
        )
    ).scalar_one_or_none()


async def _finalize_no_change_execution(
    session: AsyncSession,
    request: _NoChangeRequest,
    now: dt.datetime,
) -> None:
    execution_result = await session.execute(
        update(CustomImportExecution)
        .where(
            CustomImportExecution.execution_id == request.execution_id,
            CustomImportExecution.state == "running",
        )
        .values(state="no_change", finished_at=now, updated_at=now)
    )
    lease_result = await session.execute(
        update(CustomImportLease)
        .where(
            CustomImportLease.execution_id == request.execution_id,
            CustomImportLease.fence == request.lease_fence,
            CustomImportLease.token_sha256 == request.token_sha256,
            CustomImportLease.expires_at > now,
        )
        .values(heartbeat_at=now, expires_at=now, updated_at=now)
    )
    if execution_result.rowcount != 1 or lease_result.rowcount != 1:
        raise PublicationConflict("execution changed while recording no change")


async def _replayed_no_change_receipt(
    session: AsyncSession,
    request: _NoChangeRequest,
    execution: CustomImportExecution,
) -> PublicationReceipt | None:
    """Return an exact no-change replay only when its seal and event agree."""

    existing_seal = await _locked_no_change_seal(session, request.execution_id)
    if existing_seal is None:
        return None
    base_generation = await _locked_generation(
        session,
        dataset_id=request.dataset_id,
        generation_id=request.expected_generation_id,
    )
    candidate_generation = await _locked_generation(
        session,
        dataset_id=request.dataset_id,
        generation_id=request.candidate_generation_id,
    )
    base_seal = await _validated_generation_seal(session, base_generation)
    candidate_seal = await _validated_generation_seal(session, candidate_generation)
    _validate_no_change_seal(
        existing_seal,
        request,
        execution,
        base_generation,
        candidate_generation,
        base_seal,
        candidate_seal,
    )
    event = await _no_change_event(session, request)
    if event is None:
        raise PublicationConflict("no-change receipt has no publication event")
    _validate_no_change_event(event, existing_seal)
    return _receipt(event, replayed=True)


async def _locked_no_change_candidate(
    session: AsyncSession,
    request: _NoChangeRequest,
    execution: CustomImportExecution,
) -> tuple[
    CustomImportLease,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportGeneration,
]:
    """Lock a current base plus the running execution's candidate output."""

    lease = await _locked_lease(session, request.execution_id)
    pointer = await _locked_pointer(session, request.dataset_id)
    _require_expected_pointer(
        pointer,
        expected_generation_id=request.expected_generation_id,
        expected_pointer_version=request.expected_pointer_version,
    )
    if pointer is None:
        raise PublicationConflict("no-change requires a current generation")
    base_generation = await _locked_generation(
        session,
        dataset_id=request.dataset_id,
        generation_id=request.expected_generation_id,
    )
    base_seal = await _validated_generation_seal(session, base_generation)
    candidate_generation = await _locked_generation(
        session,
        dataset_id=request.dataset_id,
        generation_id=request.candidate_generation_id,
    )
    _require_no_change_candidate_execution(execution, candidate_generation)
    return lease, base_generation, base_seal, candidate_generation


def _require_no_change_candidate_execution(
    execution: CustomImportExecution,
    candidate_generation: CustomImportGeneration,
) -> None:
    """Require a running execution to own the candidate it is about to seal."""

    if execution.state != "running":
        raise PublicationConflict("only a running execution can finish with no change")
    if (
        execution.execution_id != candidate_generation.execution_id
        or execution.definition_revision_id != candidate_generation.definition_revision_id
        or execution.schema_revision_id != candidate_generation.schema_revision_id
        or execution.capture_bundle_id is None
        or execution.capture_bundle_id != candidate_generation.capture_bundle_id
    ):
        raise PublicationConflict("no-change candidate does not belong to the running execution")


async def _seal_no_change_candidate(
    session: AsyncSession,
    request: _NoChangeRequest,
    execution: CustomImportExecution,
    lease: CustomImportLease,
    candidate_generation: CustomImportGeneration,
) -> CustomImportGenerationSeal:
    """Freeze a no-change candidate without prematurely terminalizing its execution."""

    existing_seal = await _locked_generation_seal(session, candidate_generation.generation_id)
    if existing_seal is not None:
        _validate_generation_seal_identity(existing_seal, candidate_generation)
        if existing_seal.sealing_fence != request.lease_fence or not hmac.compare_digest(
            bytes(existing_seal.sealing_token_sha256), request.token_sha256
        ):
            raise PublicationConflict("no-change candidate seal authority differs")
        return existing_seal
    materialization = await _materialization(session, candidate_generation)
    now = await _database_now(session)
    seal_request = _GenerationSealRequest(
        dataset_id=request.dataset_id,
        generation_id=candidate_generation.generation_id,
        lease_fence=request.lease_fence,
        token_sha256=request.token_sha256,
    )
    _validate_generation_sealing_authority(
        execution,
        lease,
        candidate_generation,
        materialization,
        seal_request,
        now,
    )
    candidate_seal = _new_generation_seal(candidate_generation, materialization, seal_request)
    session.add(candidate_seal)
    await session.flush()
    return candidate_seal


def _new_no_change_seal(
    request: _NoChangeRequest,
    execution: CustomImportExecution,
    base_generation: CustomImportGeneration,
    candidate_generation: CustomImportGeneration,
    base_seal: CustomImportGenerationSeal,
    candidate_seal: CustomImportGenerationSeal,
) -> CustomImportNoChangeSeal:
    """Build an immutable proof over two sealed materialization receipts."""

    canonical_receipt, receipt_sha256 = _no_change_receipt_document(
        request,
        execution,
        base_generation=base_generation,
        candidate_generation=candidate_generation,
        base_seal=base_seal,
        candidate_seal=candidate_seal,
    )
    return CustomImportNoChangeSeal(
        execution_id=execution.execution_id,
        dataset_id=execution.dataset_id,
        definition_revision_id=execution.definition_revision_id,
        schema_revision_id=execution.schema_revision_id,
        capture_bundle_id=execution.capture_bundle_id,
        base_generation_id=base_generation.generation_id,
        candidate_generation_id=candidate_generation.generation_id,
        base_pointer_version=request.expected_pointer_version,
        seal_contract=_NO_CHANGE_SEAL_CONTRACT,
        base_source_bundle_sha256=base_generation.source_bundle_sha256,
        candidate_source_bundle_sha256=candidate_generation.source_bundle_sha256,
        effective_output_sha256=candidate_seal.effective_output_sha256,
        sealing_fence=request.lease_fence,
        sealing_token_sha256=request.token_sha256,
        canonical_receipt=canonical_receipt,
        receipt_sha256=receipt_sha256,
    )


def _no_change_publication_event(
    request: _NoChangeRequest,
    base_generation: CustomImportGeneration,
) -> CustomImportPublicationEvent:
    """Build the immutable event that records a terminal no-change outcome."""

    return _new_publication_event(
        _PublicationEventDetails(
            dataset_id=request.dataset_id,
            definition_revision_id=base_generation.definition_revision_id,
            schema_revision_id=base_generation.schema_revision_id,
            execution_id=request.execution_id,
            event_kind="no_change",
            from_generation_id=base_generation.generation_id,
            to_generation_id=base_generation.generation_id,
            expected_pointer_version=request.expected_pointer_version,
            committed_pointer_version=request.expected_pointer_version,
        )
    )


async def _record_new_no_change(
    session: AsyncSession,
    request: _NoChangeRequest,
    execution: CustomImportExecution,
) -> PublicationReceipt:
    """Seal and terminalize a newly proven no-change execution."""

    lease, base_generation, base_seal, candidate_generation = await _locked_no_change_candidate(
        session, request, execution
    )
    renewed_at = await _renew_finality_lease(
        session,
        execution,
        lease,
        fence=request.lease_fence,
        token_sha256=request.token_sha256,
        conflict_message="execution lease is lost or expired",
    )
    async with _finality_scan_window(session, now=renewed_at, expires_at=lease.expires_at):
        candidate_seal = await _seal_no_change_candidate(session, request, execution, lease, candidate_generation)
        _require_materialization_budget(session)
    if not hmac.compare_digest(bytes(base_seal.effective_output_sha256), bytes(candidate_seal.effective_output_sha256)):
        raise PublicationConflict("no-change candidate effective output differs from the current generation")
    now = await _database_now(session)
    if not _has_live_lease_authority(
        lease,
        fence=request.lease_fence,
        token_sha256=request.token_sha256,
        now=now,
    ):
        raise PublicationConflict("execution lease is lost or expired")
    no_change_seal = _new_no_change_seal(
        request,
        execution,
        base_generation,
        candidate_generation,
        base_seal,
        candidate_seal,
    )
    session.add(no_change_seal)
    await session.flush()
    await _finalize_no_change_execution(session, request, now)
    event = _no_change_publication_event(request, base_generation)
    session.add(event)
    await session.flush()
    return _receipt(event, replayed=False)


async def record_no_change(
    session: AsyncSession,
    *,
    dataset_id: int,
    execution_id: int,
    expected_generation_id: int,
    expected_pointer_version: int,
    candidate_generation_id: int,
    lease_fence: int,
    lease_token: str | bytes | bytearray | memoryview,
) -> PublicationReceipt:
    """Atomically seal an owned candidate and prove it equals the current output."""

    await _begin_finality_operation(session)
    request = _no_change_request(
        dataset_id=dataset_id,
        execution_id=execution_id,
        expected_generation_id=expected_generation_id,
        expected_pointer_version=expected_pointer_version,
        candidate_generation_id=candidate_generation_id,
        lease_fence=lease_fence,
        lease_token=lease_token,
    )
    await _locked_dataset(session, request.dataset_id)
    execution = await _locked_execution(
        session,
        execution_id=request.execution_id,
        dataset_id=request.dataset_id,
    )
    replay = await _replayed_no_change_receipt(session, request, execution)
    if replay is not None:
        return replay
    return await _record_new_no_change(session, request, execution)


__all__ = (
    "FINALITY_EVENT_CONTRACT",
    "GenerationSealReceipt",
    "PublicationConflict",
    "PublicationKind",
    "PublicationReceipt",
    "activate_generation",
    "record_no_change",
    "rollback_generation",
    "seal_generation",
    "verify_publication_event_material",
)
