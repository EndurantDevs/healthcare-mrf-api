# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic publication transitions for immutable custom-import generations.

The caller owns the surrounding :class:`~sqlalchemy.ext.asyncio.AsyncSession`
transaction.  These operations never commit independently: the pointer and its
immutable receipt therefore either become visible together or not at all.
One publication transaction is scoped to one dataset; callers that batch
datasets must use separate transactions or acquire them in a deterministic
order.
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCurrentGeneration,
    CustomImportDataset,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportLease,
    CustomImportPublicationEvent,
)
from process.custom_import.definition import canonical_json, canonical_sha256
from process.custom_import.execution import (
    lease_token_sha256,
    require_separate_publication_transaction,
)

PublicationKind = Literal["activated", "rolled_back", "no_change"]


class PublicationConflict(RuntimeError):
    """The requested publication does not match the locked current state."""


@dataclass(frozen=True)
class PublicationReceipt:
    """Stable result of one committed publication transition or exact replay."""

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


def _positive_integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise PublicationConflict(f"{label} must be a positive integer")
    return value


def _pointer_version(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise PublicationConflict("expected pointer version must be a non-negative integer")
    return value


def _require_transaction(session: AsyncSession) -> None:
    if not session.in_transaction():
        raise PublicationConflict("publication requires a caller-owned transaction")


def _event_document(
    *,
    dataset_id: int,
    definition_revision_id: int,
    schema_revision_id: int,
    execution_id: int,
    event_kind: PublicationKind,
    from_generation_id: int | None,
    to_generation_id: int,
    expected_pointer_version: int,
    committed_pointer_version: int,
) -> tuple[str, bytes]:
    document = {
        "contract": "custom-import-publication-event/v1",
        "dataset_id": dataset_id,
        "definition_revision_id": definition_revision_id,
        "event_kind": event_kind,
        "execution_id": execution_id,
        "from_generation_id": from_generation_id,
        "schema_revision_id": schema_revision_id,
        "to_generation_id": to_generation_id,
        "expected_pointer_version": expected_pointer_version,
        "committed_pointer_version": committed_pointer_version,
    }
    serialized = canonical_json(document)
    return serialized, bytes.fromhex(canonical_sha256(document, domain="event"))


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


async def _generation(
    session: AsyncSession,
    *,
    dataset_id: int,
    generation_id: int,
) -> CustomImportGeneration:
    generation = (
        await session.execute(
            select(CustomImportGeneration).where(
                CustomImportGeneration.generation_id == generation_id,
                CustomImportGeneration.dataset_id == dataset_id,
            )
        )
    ).scalar_one_or_none()
    if generation is None:
        raise PublicationConflict("target generation does not belong to the dataset")
    return generation


async def _locked_execution(
    session: AsyncSession,
    *,
    execution_id: int,
    dataset_id: int,
) -> CustomImportExecution:
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


async def _locked_pointer(
    session: AsyncSession,
    dataset_id: int,
) -> CustomImportCurrentGeneration | None:
    return (
        await session.execute(
            select(CustomImportCurrentGeneration)
            .where(CustomImportCurrentGeneration.dataset_id == dataset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


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


async def _exact_event(
    session: AsyncSession,
    *,
    dataset_id: int,
    event_kind: PublicationKind,
    from_generation_id: int | None,
    to_generation_id: int,
    expected_pointer_version: int,
    committed_pointer_version: int,
) -> CustomImportPublicationEvent | None:
    return (
        await session.execute(
            select(CustomImportPublicationEvent).where(
                CustomImportPublicationEvent.dataset_id == dataset_id,
                CustomImportPublicationEvent.event_kind == event_kind,
                CustomImportPublicationEvent.from_generation_id.is_(None)
                if from_generation_id is None
                else CustomImportPublicationEvent.from_generation_id == from_generation_id,
                CustomImportPublicationEvent.to_generation_id == to_generation_id,
                CustomImportPublicationEvent.expected_pointer_version == expected_pointer_version,
                CustomImportPublicationEvent.committed_pointer_version == committed_pointer_version,
            )
        )
    ).scalar_one_or_none()


def _verify_event_material(event: CustomImportPublicationEvent) -> None:
    canonical_event, event_sha256 = _event_document(
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
    if event.canonical_event != canonical_event or not hmac.compare_digest(bytes(event.event_sha256), event_sha256):
        raise PublicationConflict("persisted publication receipt is not canonical")


async def _publish_generation(
    session: AsyncSession,
    *,
    event_kind: Literal["activated", "rolled_back"],
    dataset_id: int,
    target_generation_id: int,
    expected_generation_id: int | None,
    expected_pointer_version: int,
) -> PublicationReceipt:
    _require_transaction(session)
    await require_separate_publication_transaction(session)
    dataset_id = _positive_integer(dataset_id, "dataset id")
    target_generation_id = _positive_integer(target_generation_id, "target generation id")
    if expected_generation_id is not None:
        expected_generation_id = _positive_integer(expected_generation_id, "expected generation id")
    expected_pointer_version = _pointer_version(expected_pointer_version)
    if event_kind == "rolled_back" and expected_generation_id is None:
        raise PublicationConflict("rollback requires an existing current generation")

    await _locked_dataset(session, dataset_id)
    target = await _generation(
        session,
        dataset_id=dataset_id,
        generation_id=target_generation_id,
    )
    if event_kind == "activated" and target.base_generation_id != expected_generation_id:
        raise PublicationConflict("activation target base generation does not match the expected generation")
    producer = await _locked_execution(
        session,
        execution_id=target.execution_id,
        dataset_id=dataset_id,
    )
    if producer.state != "completed":
        raise PublicationConflict("target generation producer is not completed")
    if (
        producer.definition_revision_id != target.definition_revision_id
        or producer.schema_revision_id != target.schema_revision_id
        or producer.capture_bundle_id != target.capture_bundle_id
    ):
        raise PublicationConflict("target generation producer identity is inconsistent")

    pointer = await _locked_pointer(session, dataset_id)
    committed_pointer_version = expected_pointer_version + 1
    if (
        pointer is not None
        and pointer.generation_id == target_generation_id
        and pointer.pointer_version == committed_pointer_version
    ):
        replay = await _exact_event(
            session,
            dataset_id=dataset_id,
            event_kind=event_kind,
            from_generation_id=expected_generation_id,
            to_generation_id=target_generation_id,
            expected_pointer_version=expected_pointer_version,
            committed_pointer_version=committed_pointer_version,
        )
        if replay is not None:
            _verify_event_material(replay)
            return _receipt(replay, replayed=True)

    _require_expected_pointer(
        pointer,
        expected_generation_id=expected_generation_id,
        expected_pointer_version=expected_pointer_version,
    )
    if expected_generation_id == target_generation_id:
        raise PublicationConflict("publication target is already current")

    if pointer is None:
        pointer = CustomImportCurrentGeneration(
            dataset_id=dataset_id,
            definition_revision_id=target.definition_revision_id,
            schema_revision_id=target.schema_revision_id,
            generation_id=target_generation_id,
            pointer_version=committed_pointer_version,
        )
        session.add(pointer)
    else:
        pointer_result = await session.execute(
            update(CustomImportCurrentGeneration)
            .where(
                CustomImportCurrentGeneration.dataset_id == dataset_id,
                CustomImportCurrentGeneration.generation_id == expected_generation_id,
                CustomImportCurrentGeneration.pointer_version == expected_pointer_version,
            )
            .values(
                definition_revision_id=target.definition_revision_id,
                schema_revision_id=target.schema_revision_id,
                generation_id=target_generation_id,
                pointer_version=committed_pointer_version,
                changed_at=func.clock_timestamp(),
            )
        )
        if pointer_result.rowcount != 1:
            raise PublicationConflict("current generation changed during publication")

    canonical_event, event_sha256 = _event_document(
        dataset_id=dataset_id,
        definition_revision_id=target.definition_revision_id,
        schema_revision_id=target.schema_revision_id,
        execution_id=target.execution_id,
        event_kind=event_kind,
        from_generation_id=expected_generation_id,
        to_generation_id=target_generation_id,
        expected_pointer_version=expected_pointer_version,
        committed_pointer_version=committed_pointer_version,
    )
    event = CustomImportPublicationEvent(
        dataset_id=dataset_id,
        definition_revision_id=target.definition_revision_id,
        schema_revision_id=target.schema_revision_id,
        execution_id=target.execution_id,
        event_kind=event_kind,
        from_generation_id=expected_generation_id,
        to_generation_id=target_generation_id,
        expected_pointer_version=expected_pointer_version,
        committed_pointer_version=committed_pointer_version,
        canonical_event=canonical_event,
        event_sha256=event_sha256,
    )
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
    """Activate a completed generation by one exact pointer CAS."""

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
    """Point a dataset back to one exact retained completed generation."""

    return await _publish_generation(
        session,
        event_kind="rolled_back",
        dataset_id=dataset_id,
        target_generation_id=target_generation_id,
        expected_generation_id=expected_generation_id,
        expected_pointer_version=expected_pointer_version,
    )


async def record_no_change(
    session: AsyncSession,
    *,
    dataset_id: int,
    execution_id: int,
    expected_generation_id: int,
    expected_pointer_version: int,
    effective_generation_sha256: bytes | bytearray | memoryview,
    lease_fence: int,
    lease_token: str | bytes | bytearray | memoryview,
) -> PublicationReceipt:
    """Atomically terminate an unchanged import without creating a generation."""

    _require_transaction(session)
    await require_separate_publication_transaction(session)
    dataset_id = _positive_integer(dataset_id, "dataset id")
    execution_id = _positive_integer(execution_id, "execution id")
    expected_generation_id = _positive_integer(expected_generation_id, "expected generation id")
    expected_pointer_version = _pointer_version(expected_pointer_version)
    lease_fence = _positive_integer(lease_fence, "lease fence")
    if (
        not isinstance(effective_generation_sha256, (bytes, bytearray, memoryview))
        or len(effective_generation_sha256) != 32
    ):
        raise PublicationConflict("effective generation digest must contain 32 bytes")
    effective_generation_sha256 = bytes(effective_generation_sha256)
    try:
        token_sha256 = lease_token_sha256(lease_token)
    except ValueError as exc:
        raise PublicationConflict(str(exc)) from exc

    await _locked_dataset(session, dataset_id)
    execution = await _locked_execution(
        session,
        execution_id=execution_id,
        dataset_id=dataset_id,
    )
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

    pointer = await _locked_pointer(session, dataset_id)
    _require_expected_pointer(
        pointer,
        expected_generation_id=expected_generation_id,
        expected_pointer_version=expected_pointer_version,
    )
    assert pointer is not None

    if execution.state == "no_change":
        replay = (
            await session.execute(
                select(CustomImportPublicationEvent).where(
                    CustomImportPublicationEvent.execution_id == execution_id,
                    CustomImportPublicationEvent.event_kind == "no_change",
                    CustomImportPublicationEvent.dataset_id == dataset_id,
                    CustomImportPublicationEvent.from_generation_id == expected_generation_id,
                    CustomImportPublicationEvent.to_generation_id == expected_generation_id,
                    CustomImportPublicationEvent.expected_pointer_version == expected_pointer_version,
                    CustomImportPublicationEvent.committed_pointer_version == expected_pointer_version,
                )
            )
        ).scalar_one_or_none()
        if replay is None:
            raise PublicationConflict("no-change replay does not match its immutable receipt")
        _verify_event_material(replay)
        replay_generation = await _generation(
            session,
            dataset_id=dataset_id,
            generation_id=expected_generation_id,
        )
        if not hmac.compare_digest(bytes(replay_generation.generation_sha256), effective_generation_sha256):
            raise PublicationConflict("no-change replay content digest differs")
        if (
            lease.fence != lease_fence
            or lease.token_sha256 is None
            or not hmac.compare_digest(bytes(lease.token_sha256), token_sha256)
        ):
            raise PublicationConflict("no-change replay lease authority differs")
        return _receipt(replay, replayed=True)
    if execution.state != "running":
        raise PublicationConflict("only a running execution can finish with no change")
    current = await _generation(
        session,
        dataset_id=dataset_id,
        generation_id=expected_generation_id,
    )
    if (
        execution.definition_revision_id != current.definition_revision_id
        or execution.schema_revision_id != current.schema_revision_id
    ):
        raise PublicationConflict("no-change execution identity differs from the current generation")
    if not hmac.compare_digest(bytes(current.generation_sha256), effective_generation_sha256):
        raise PublicationConflict("effective content differs from the current generation")

    now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    if (
        lease.fence != lease_fence
        or lease.token_sha256 is None
        or not hmac.compare_digest(bytes(lease.token_sha256), token_sha256)
        or lease.expires_at is None
        or lease.expires_at <= now
    ):
        raise PublicationConflict("execution lease is lost or expired")

    execution_result = await session.execute(
        update(CustomImportExecution)
        .where(
            CustomImportExecution.execution_id == execution_id,
            CustomImportExecution.state == "running",
        )
        .values(
            state="no_change",
            terminal_reason=None,
            finished_at=now,
            updated_at=now,
        )
    )
    if execution_result.rowcount != 1:
        raise PublicationConflict("execution changed while recording no change")
    lease_result = await session.execute(
        update(CustomImportLease)
        .where(
            CustomImportLease.execution_id == execution_id,
            CustomImportLease.fence == lease_fence,
            CustomImportLease.token_sha256 == token_sha256,
            CustomImportLease.expires_at > now,
        )
        .values(heartbeat_at=now, expires_at=now, updated_at=now)
    )
    if lease_result.rowcount != 1:
        raise PublicationConflict("execution lease changed while recording no change")

    canonical_event, event_sha256 = _event_document(
        dataset_id=dataset_id,
        definition_revision_id=current.definition_revision_id,
        schema_revision_id=current.schema_revision_id,
        execution_id=execution_id,
        event_kind="no_change",
        from_generation_id=expected_generation_id,
        to_generation_id=expected_generation_id,
        expected_pointer_version=expected_pointer_version,
        committed_pointer_version=expected_pointer_version,
    )
    event = CustomImportPublicationEvent(
        dataset_id=dataset_id,
        definition_revision_id=current.definition_revision_id,
        schema_revision_id=current.schema_revision_id,
        execution_id=execution_id,
        event_kind="no_change",
        from_generation_id=expected_generation_id,
        to_generation_id=expected_generation_id,
        expected_pointer_version=expected_pointer_version,
        committed_pointer_version=expected_pointer_version,
        canonical_event=canonical_event,
        event_sha256=event_sha256,
    )
    session.add(event)
    await session.flush()
    return _receipt(event, replayed=False)


__all__ = (
    "PublicationConflict",
    "PublicationKind",
    "PublicationReceipt",
    "activate_generation",
    "record_no_change",
    "rollback_generation",
)
