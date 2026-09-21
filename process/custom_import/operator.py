# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Caller-owned status reads for durable custom-import executions and generations."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import and_, exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportNoChangeSeal,
    CustomImportPublicationEvent,
)

MAX_BIGINT = 9_223_372_036_854_775_807
_EXECUTION_STATES = frozenset({"queued", "running", "canceling", "canceled", "failed", "completed", "no_change"})
_MECHANISMS = frozenset({"local", "queued", "external"})
_GENERATION_SEAL_CONTRACT = "custom-import-generation-seal/v1"
_NO_CHANGE_SEAL_CONTRACT = "custom-import-no-change-seal/v1"
_FINALITY_CONTRACT = "custom-import-finality/v1"

PublicationState = Literal["unsealed", "sealed_unpublished", "current", "superseded", "no_change"]

_GENERATION_STATUS_COLUMNS = (
    CustomImportGeneration.generation_id,
    CustomImportGeneration.dataset_id,
    CustomImportGeneration.definition_revision_id,
    CustomImportGeneration.schema_revision_id,
    CustomImportGeneration.execution_id,
    CustomImportGeneration.capture_bundle_id,
    CustomImportGeneration.base_generation_id,
    CustomImportGeneration.root_count,
    CustomImportGeneration.family_count,
    CustomImportGeneration.created_at,
    CustomImportGenerationSeal.seal_contract,
    CustomImportGenerationSeal.sealing_fence,
    CustomImportGenerationSeal.root_count.label("sealed_root_count"),
    CustomImportGenerationSeal.family_count.label("sealed_family_count"),
    CustomImportGenerationSeal.generation_family_count,
    CustomImportGenerationSeal.family_child_count,
    CustomImportGenerationSeal.winner_count,
    CustomImportGenerationSeal.profile_count,
    CustomImportGenerationSeal.root_scalar_count,
    CustomImportGenerationSeal.child_scalar_count,
    CustomImportGenerationSeal.materialization_sha256,
    CustomImportGenerationSeal.effective_output_sha256.label("sealed_effective_output_sha256"),
    CustomImportGenerationSeal.sealed_at,
    CustomImportCurrentGeneration.generation_id.label("current_generation_id"),
    CustomImportCurrentGeneration.definition_revision_id.label("current_definition_revision_id"),
    CustomImportCurrentGeneration.schema_revision_id.label("current_schema_revision_id"),
    CustomImportCurrentGeneration.pointer_version,
    CustomImportCurrentGeneration.changed_at.label("pointer_changed_at"),
    CustomImportNoChangeSeal.seal_contract.label("no_change_contract"),
    CustomImportNoChangeSeal.base_generation_id.label("no_change_base_generation_id"),
    CustomImportNoChangeSeal.base_pointer_version.label("no_change_base_pointer_version"),
    CustomImportNoChangeSeal.effective_output_sha256.label("no_change_effective_output_sha256"),
    CustomImportNoChangeSeal.receipt_sha256.label("no_change_receipt_sha256"),
    CustomImportNoChangeSeal.sealed_at.label("no_change_sealed_at"),
)


class OperatorInspectionError(RuntimeError):
    """Base class for value-free operator inspection failures."""


class OperatorTransactionRequired(OperatorInspectionError):
    """The caller did not supply an active transaction."""


class OperatorObjectNotFound(OperatorInspectionError):
    """The exact dataset-scoped execution or generation does not exist."""


class OperatorInvariantError(OperatorInspectionError):
    """Persisted state cannot produce a trustworthy operator snapshot."""


@dataclass(frozen=True, slots=True)
class LeaseStatus:
    """Safe lease timing and fence evidence without token material."""

    fence: int
    heartbeat_at: dt.datetime | None
    expires_at: dt.datetime | None


@dataclass(frozen=True, slots=True)
class ExecutionStatus:
    """One exact execution state without idempotency or terminal-reason payloads."""

    execution_id: int
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    capture_bundle_id: int | None
    mechanism: str
    state: str
    started_at: dt.datetime | None
    finished_at: dt.datetime | None
    created_at: dt.datetime
    updated_at: dt.datetime
    lease: LeaseStatus | None


@dataclass(frozen=True, slots=True)
class GenerationSealStatus:
    """Immutable generation finality without the sealing-token digest."""

    sealing_fence: int
    root_count: int
    family_count: int
    generation_family_count: int
    family_child_count: int
    winner_count: int
    profile_count: int
    root_scalar_count: int
    child_scalar_count: int
    materialization_sha256: str
    effective_output_sha256: str
    sealed_at: dt.datetime


@dataclass(frozen=True, slots=True)
class CurrentGenerationStatus:
    """The exact dataset pointer observed in the same statement."""

    generation_id: int
    definition_revision_id: int
    schema_revision_id: int
    pointer_version: int
    changed_at: dt.datetime


@dataclass(frozen=True, slots=True)
class NoChangeStatus:
    """Immutable evidence that this candidate retained an existing generation."""

    base_generation_id: int
    base_pointer_version: int
    effective_output_sha256: str
    receipt_sha256: str
    sealed_at: dt.datetime


@dataclass(frozen=True, slots=True)
class GenerationStatus:
    """One exact generation plus seal, pointer, and publication evidence.

    Retained pre-finality generations can be current or superseded with no
    seal; ``unsealed`` means no seal and no publication evidence was observed.
    """

    generation_id: int
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    execution_id: int
    capture_bundle_id: int
    base_generation_id: int | None
    root_count: int
    family_count: int
    created_at: dt.datetime
    publication_state: PublicationState
    ever_published: bool
    seal: GenerationSealStatus | None
    current: CurrentGenerationStatus | None
    no_change: NoChangeStatus | None


def _positive_id(value: object) -> int:
    if type(value) is not int or not 0 < value <= MAX_BIGINT:
        raise OperatorInspectionError("custom import operator identifier is invalid")
    return value


def _require_transaction(session: AsyncSession) -> None:
    if not session.in_transaction():
        raise OperatorTransactionRequired("custom import operator transaction is required")


def _digest(value: object) -> str:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    raw = bytes(value)
    if len(raw) != 32:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return raw.hex()


def _lease_status(row) -> LeaseStatus | None:
    fence = row["lease_fence"]
    if fence is None:
        return None
    if type(fence) is not int or fence < 0:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    heartbeat_at = row["lease_heartbeat_at"]
    expires_at = row["lease_expires_at"]
    if (fence == 0 and expires_at is not None) or (fence > 0 and expires_at is None):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return LeaseStatus(fence=fence, heartbeat_at=heartbeat_at, expires_at=expires_at)


async def inspect_execution(
    session: AsyncSession,
    *,
    dataset_id: int,
    execution_id: int,
) -> ExecutionStatus:
    """Read one dataset-scoped execution in the caller's active transaction."""

    dataset_id = _positive_id(dataset_id)
    execution_id = _positive_id(execution_id)
    _require_transaction(session)
    statement = (
        select(
            CustomImportExecution.execution_id,
            CustomImportExecution.dataset_id,
            CustomImportExecution.definition_revision_id,
            CustomImportExecution.schema_revision_id,
            CustomImportExecution.capture_bundle_id,
            CustomImportExecution.mechanism,
            CustomImportExecution.state,
            CustomImportExecution.started_at,
            CustomImportExecution.finished_at,
            CustomImportExecution.created_at,
            CustomImportExecution.updated_at,
            CustomImportLease.fence.label("lease_fence"),
            CustomImportLease.heartbeat_at.label("lease_heartbeat_at"),
            CustomImportLease.expires_at.label("lease_expires_at"),
        )
        .select_from(CustomImportExecution)
        .outerjoin(CustomImportLease, CustomImportLease.execution_id == CustomImportExecution.execution_id)
        .where(
            CustomImportExecution.dataset_id == dataset_id,
            CustomImportExecution.execution_id == execution_id,
        )
        .execution_options(autoflush=False)
    )
    execution_snapshot = (await session.execute(statement)).mappings().one_or_none()
    if execution_snapshot is None:
        raise OperatorObjectNotFound("custom import execution was not found")
    state = execution_snapshot["state"]
    mechanism = execution_snapshot["mechanism"]
    if state not in _EXECUTION_STATES or mechanism not in _MECHANISMS:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return ExecutionStatus(
        execution_id=execution_snapshot["execution_id"],
        dataset_id=execution_snapshot["dataset_id"],
        definition_revision_id=execution_snapshot["definition_revision_id"],
        schema_revision_id=execution_snapshot["schema_revision_id"],
        capture_bundle_id=execution_snapshot["capture_bundle_id"],
        mechanism=mechanism,
        state=state,
        started_at=execution_snapshot["started_at"],
        finished_at=execution_snapshot["finished_at"],
        created_at=execution_snapshot["created_at"],
        updated_at=execution_snapshot["updated_at"],
        lease=_lease_status(execution_snapshot),
    )


def _seal_status(row) -> GenerationSealStatus | None:
    contract = row["seal_contract"]
    if contract is None:
        return None
    if contract != _GENERATION_SEAL_CONTRACT:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return GenerationSealStatus(
        sealing_fence=row["sealing_fence"],
        root_count=row["sealed_root_count"],
        family_count=row["sealed_family_count"],
        generation_family_count=row["generation_family_count"],
        family_child_count=row["family_child_count"],
        winner_count=row["winner_count"],
        profile_count=row["profile_count"],
        root_scalar_count=row["root_scalar_count"],
        child_scalar_count=row["child_scalar_count"],
        materialization_sha256=_digest(row["materialization_sha256"]),
        effective_output_sha256=_digest(row["sealed_effective_output_sha256"]),
        sealed_at=row["sealed_at"],
    )


def _current_status(row) -> CurrentGenerationStatus | None:
    generation_id = row["current_generation_id"]
    if generation_id is None:
        return None
    values = (
        generation_id,
        row["current_definition_revision_id"],
        row["current_schema_revision_id"],
        row["pointer_version"],
    )
    if any(type(value) is not int or value <= 0 for value in values):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return CurrentGenerationStatus(
        generation_id=generation_id,
        definition_revision_id=row["current_definition_revision_id"],
        schema_revision_id=row["current_schema_revision_id"],
        pointer_version=row["pointer_version"],
        changed_at=row["pointer_changed_at"],
    )


def _no_change_status(row) -> NoChangeStatus | None:
    contract = row["no_change_contract"]
    if contract is None:
        return None
    if contract != _NO_CHANGE_SEAL_CONTRACT:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return NoChangeStatus(
        base_generation_id=row["no_change_base_generation_id"],
        base_pointer_version=row["no_change_base_pointer_version"],
        effective_output_sha256=_digest(row["no_change_effective_output_sha256"]),
        receipt_sha256=_digest(row["no_change_receipt_sha256"]),
        sealed_at=row["no_change_sealed_at"],
    )


def _publication_state(
    *,
    generation_id: int,
    current: CurrentGenerationStatus | None,
    seal: GenerationSealStatus | None,
    no_change: NoChangeStatus | None,
    ever_published: bool,
    no_change_event_exists: bool,
) -> PublicationState:
    if (no_change is not None) != no_change_event_exists:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    if no_change is not None:
        if seal is None or ever_published or current is not None and current.generation_id == generation_id:
            raise OperatorInvariantError("custom import operator evidence is invalid")
        return "no_change"
    if current is not None and current.generation_id == generation_id:
        return "current"
    if ever_published:
        return "superseded"
    return "sealed_unpublished" if seal is not None else "unsealed"


def _ever_published_expression():
    return (
        exists()
        .where(
            CustomImportPublicationEvent.dataset_id == CustomImportGeneration.dataset_id,
            CustomImportPublicationEvent.execution_id == CustomImportGeneration.execution_id,
            CustomImportPublicationEvent.to_generation_id == CustomImportGeneration.generation_id,
            CustomImportPublicationEvent.event_kind.in_(("activated", "rolled_back")),
        )
        .label("ever_published")
    )


def _no_change_event_expression():
    return (
        exists()
        .where(
            CustomImportPublicationEvent.dataset_id == CustomImportGeneration.dataset_id,
            CustomImportPublicationEvent.definition_revision_id == CustomImportGeneration.definition_revision_id,
            CustomImportPublicationEvent.schema_revision_id == CustomImportGeneration.schema_revision_id,
            CustomImportPublicationEvent.execution_id == CustomImportGeneration.execution_id,
            CustomImportPublicationEvent.event_kind == "no_change",
            CustomImportPublicationEvent.from_generation_id == CustomImportNoChangeSeal.base_generation_id,
            CustomImportPublicationEvent.to_generation_id == CustomImportNoChangeSeal.base_generation_id,
            CustomImportPublicationEvent.expected_pointer_version == CustomImportNoChangeSeal.base_pointer_version,
            CustomImportPublicationEvent.committed_pointer_version == CustomImportNoChangeSeal.base_pointer_version,
            CustomImportPublicationEvent.finality_contract == _FINALITY_CONTRACT,
        )
        .label("no_change_event_exists")
    )


def _generation_statement(dataset_id: int, generation_id: int):
    return (
        select(
            *_GENERATION_STATUS_COLUMNS,
            _ever_published_expression(),
            _no_change_event_expression(),
        )
        .select_from(CustomImportGeneration)
        .outerjoin(
            CustomImportGenerationSeal,
            CustomImportGenerationSeal.generation_id == CustomImportGeneration.generation_id,
        )
        .outerjoin(
            CustomImportCurrentGeneration,
            CustomImportCurrentGeneration.dataset_id == CustomImportGeneration.dataset_id,
        )
        .outerjoin(
            CustomImportNoChangeSeal,
            and_(
                CustomImportNoChangeSeal.execution_id == CustomImportGeneration.execution_id,
                CustomImportNoChangeSeal.candidate_generation_id == CustomImportGeneration.generation_id,
            ),
        )
        .where(
            CustomImportGeneration.dataset_id == dataset_id,
            CustomImportGeneration.generation_id == generation_id,
        )
        .execution_options(autoflush=False)
    )


async def inspect_generation(
    session: AsyncSession,
    *,
    dataset_id: int,
    generation_id: int,
) -> GenerationStatus:
    """Read one generation and its publication evidence in one statement."""

    dataset_id = _positive_id(dataset_id)
    generation_id = _positive_id(generation_id)
    _require_transaction(session)
    generation_snapshot = (
        (await session.execute(_generation_statement(dataset_id, generation_id))).mappings().one_or_none()
    )
    if generation_snapshot is None:
        raise OperatorObjectNotFound("custom import generation was not found")
    seal = _seal_status(generation_snapshot)
    current = _current_status(generation_snapshot)
    no_change = _no_change_status(generation_snapshot)
    is_ever_published = generation_snapshot["ever_published"]
    has_no_change_event = generation_snapshot["no_change_event_exists"]
    if type(is_ever_published) is not bool or type(has_no_change_event) is not bool:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    publication_state = _publication_state(
        generation_id=generation_snapshot["generation_id"],
        current=current,
        seal=seal,
        no_change=no_change,
        ever_published=is_ever_published,
        no_change_event_exists=has_no_change_event,
    )
    return GenerationStatus(
        generation_id=generation_snapshot["generation_id"],
        dataset_id=generation_snapshot["dataset_id"],
        definition_revision_id=generation_snapshot["definition_revision_id"],
        schema_revision_id=generation_snapshot["schema_revision_id"],
        execution_id=generation_snapshot["execution_id"],
        capture_bundle_id=generation_snapshot["capture_bundle_id"],
        base_generation_id=generation_snapshot["base_generation_id"],
        root_count=generation_snapshot["root_count"],
        family_count=generation_snapshot["family_count"],
        created_at=generation_snapshot["created_at"],
        publication_state=publication_state,
        ever_published=is_ever_published,
        seal=seal,
        current=current,
        no_change=no_change,
    )


__all__ = (
    "CurrentGenerationStatus",
    "ExecutionStatus",
    "GenerationSealStatus",
    "GenerationStatus",
    "LeaseStatus",
    "NoChangeStatus",
    "OperatorInspectionError",
    "OperatorInvariantError",
    "OperatorObjectNotFound",
    "OperatorTransactionRequired",
    "inspect_execution",
    "inspect_generation",
)
