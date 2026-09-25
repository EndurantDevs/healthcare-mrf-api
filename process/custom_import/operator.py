# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Caller-owned status reads for durable custom-import executions and generations."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import and_, exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportCurrentGeneration,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportNoChangeSeal,
    CustomImportPublicationEvent,
    CustomImportSchemaRevision,
    CustomImportSourceBindingRevision,
)

MAX_BIGINT = 9_223_372_036_854_775_807
_EXECUTION_STATES = frozenset({"queued", "running", "canceling", "canceled", "failed", "completed", "no_change"})
_MECHANISMS = frozenset({"local", "queued", "external"})
_GENERATION_SEAL_CONTRACT = "custom-import-generation-seal/v1"
_NO_CHANGE_SEAL_CONTRACT = "custom-import-no-change-seal/v1"
_FINALITY_CONTRACT = "custom-import-finality/v1"

PublicationState = Literal["unsealed", "sealed_unpublished", "current", "superseded", "no_change"]

_SEAL_STATUS_COLUMNS = (
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
)
_CURRENT_STATUS_COLUMNS = (
    CustomImportCurrentGeneration.generation_id.label("current_generation_id"),
    CustomImportCurrentGeneration.definition_revision_id.label("current_definition_revision_id"),
    CustomImportCurrentGeneration.schema_revision_id.label("current_schema_revision_id"),
    CustomImportCurrentGeneration.pointer_version,
    CustomImportCurrentGeneration.changed_at.label("pointer_changed_at"),
)
_NO_CHANGE_STATUS_COLUMNS = (
    CustomImportNoChangeSeal.seal_contract.label("no_change_contract"),
    CustomImportNoChangeSeal.base_generation_id.label("no_change_base_generation_id"),
    CustomImportNoChangeSeal.base_pointer_version.label("no_change_base_pointer_version"),
    CustomImportNoChangeSeal.effective_output_sha256.label("no_change_effective_output_sha256"),
    CustomImportNoChangeSeal.receipt_sha256.label("no_change_receipt_sha256"),
    CustomImportNoChangeSeal.sealed_at.label("no_change_sealed_at"),
)

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
    *_SEAL_STATUS_COLUMNS,
    *_CURRENT_STATUS_COLUMNS,
    *_NO_CHANGE_STATUS_COLUMNS,
)

_EXECUTION_EVIDENCE_COLUMNS = (
    CustomImportExecution.execution_id,
    CustomImportExecution.dataset_id,
    CustomImportExecution.definition_revision_id,
    CustomImportExecution.schema_revision_id,
    CustomImportExecution.capture_bundle_id,
    CustomImportExecution.source_binding_revision_id.label("execution_source_binding_revision_id"),
    CustomImportExecution.mechanism,
    CustomImportExecution.state,
    CustomImportExecution.terminal_reason,
    CustomImportExecution.started_at,
    CustomImportExecution.finished_at,
    CustomImportExecution.created_at,
    CustomImportExecution.updated_at,
    CustomImportDefinitionRevision.definition_revision_id.label("definition_id"),
    CustomImportDefinitionRevision.dataset_id.label("definition_dataset_id"),
    CustomImportDefinitionRevision.schema_revision_id.label("definition_schema_revision_id"),
    CustomImportDefinitionRevision.definition_sha256,
    CustomImportSchemaRevision.schema_revision_id.label("stored_schema_revision_id"),
    CustomImportSchemaRevision.dataset_id.label("schema_dataset_id"),
    CustomImportSchemaRevision.schema_sha256,
    CustomImportSourceBindingRevision.source_binding_revision_id.label("binding_revision_id"),
    CustomImportSourceBindingRevision.dataset_id.label("binding_dataset_id"),
    CustomImportSourceBindingRevision.definition_revision_id.label("binding_definition_revision_id"),
    CustomImportSourceBindingRevision.schema_revision_id.label("binding_schema_revision_id"),
    CustomImportSourceBindingRevision.definition_sha256.label("binding_definition_sha256"),
    CustomImportSourceBindingRevision.schema_sha256.label("binding_schema_sha256"),
    CustomImportSourceBindingRevision.binding_sha256.label("source_binding_sha256"),
    CustomImportCaptureBundle.capture_bundle_id.label("capture_id"),
    CustomImportCaptureBundle.dataset_id.label("capture_dataset_id"),
    CustomImportCaptureBundle.definition_revision_id.label("capture_definition_revision_id"),
    CustomImportCaptureBundle.schema_revision_id.label("capture_schema_revision_id"),
    CustomImportCaptureBundle.manifest_sha256.label("capture_manifest_sha256"),
    CustomImportGeneration.generation_id.label("evidence_generation_id"),
    CustomImportGeneration.dataset_id.label("generation_dataset_id"),
    CustomImportGeneration.definition_revision_id.label("generation_definition_revision_id"),
    CustomImportGeneration.schema_revision_id.label("generation_schema_revision_id"),
    CustomImportGeneration.execution_id.label("generation_execution_id"),
    CustomImportGeneration.capture_bundle_id.label("generation_capture_bundle_id"),
    CustomImportGeneration.producing_fence.label("generation_producing_fence"),
    CustomImportGeneration.producing_token_sha256.label("generation_producing_token_sha256"),
    CustomImportGeneration.source_bundle_sha256.label("generation_source_bundle_sha256"),
    CustomImportGenerationSeal.generation_id.label("seal_generation_id"),
    CustomImportGenerationSeal.dataset_id.label("seal_dataset_id"),
    CustomImportGenerationSeal.definition_revision_id.label("seal_definition_revision_id"),
    CustomImportGenerationSeal.schema_revision_id.label("seal_schema_revision_id"),
    CustomImportGenerationSeal.execution_id.label("seal_execution_id"),
    CustomImportGenerationSeal.capture_bundle_id.label("seal_capture_bundle_id"),
    CustomImportGenerationSeal.sealing_token_sha256.label("seal_token_sha256"),
    *_SEAL_STATUS_COLUMNS,
    *_CURRENT_STATUS_COLUMNS,
    CustomImportNoChangeSeal.execution_id.label("no_change_execution_id"),
    CustomImportNoChangeSeal.dataset_id.label("no_change_dataset_id"),
    CustomImportNoChangeSeal.definition_revision_id.label("no_change_definition_revision_id"),
    CustomImportNoChangeSeal.schema_revision_id.label("no_change_schema_revision_id"),
    CustomImportNoChangeSeal.capture_bundle_id.label("no_change_capture_bundle_id"),
    CustomImportNoChangeSeal.candidate_generation_id.label("no_change_candidate_generation_id"),
    *_NO_CHANGE_STATUS_COLUMNS,
)

_GENERATION_EVIDENCE_FIELDS = (
    "evidence_generation_id",
    "generation_dataset_id",
    "generation_definition_revision_id",
    "generation_schema_revision_id",
    "generation_execution_id",
    "generation_capture_bundle_id",
    "generation_producing_fence",
    "generation_producing_token_sha256",
    "generation_source_bundle_sha256",
)
_SEAL_EVIDENCE_FIELDS = (
    "seal_generation_id",
    "seal_dataset_id",
    "seal_definition_revision_id",
    "seal_schema_revision_id",
    "seal_execution_id",
    "seal_capture_bundle_id",
    "seal_token_sha256",
    "seal_contract",
    "sealing_fence",
    "sealed_root_count",
    "sealed_family_count",
    "generation_family_count",
    "family_child_count",
    "winner_count",
    "profile_count",
    "root_scalar_count",
    "child_scalar_count",
    "materialization_sha256",
    "sealed_effective_output_sha256",
    "sealed_at",
)
_NO_CHANGE_EVIDENCE_FIELDS = (
    "no_change_execution_id",
    "no_change_dataset_id",
    "no_change_definition_revision_id",
    "no_change_schema_revision_id",
    "no_change_capture_bundle_id",
    "no_change_candidate_generation_id",
    "no_change_contract",
    "no_change_base_generation_id",
    "no_change_base_pointer_version",
    "no_change_effective_output_sha256",
    "no_change_receipt_sha256",
    "no_change_sealed_at",
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
class ExecutionEvidenceExecution:
    """One exact execution state without operational authority evidence."""

    execution_id: int
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    capture_bundle_id: int | None
    mechanism: str
    state: str
    failure_class: Literal["candidate_rejected", "other_failed", "canceled"] | None
    started_at: dt.datetime | None
    finished_at: dt.datetime | None
    created_at: dt.datetime
    updated_at: dt.datetime


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


@dataclass(frozen=True, slots=True)
class ExecutionEvidenceGeneration:
    """One exact execution generation with safe finality and pointer evidence."""

    generation_id: int
    source_bundle_sha256: str
    publication_state: PublicationState
    seal: GenerationSealStatus | None
    no_change: NoChangeStatus | None


@dataclass(frozen=True, slots=True)
class ExecutionEvidenceStatus:
    """One read-only execution projection for a retained source and generation."""

    execution: ExecutionEvidenceExecution
    definition_sha256: str
    schema_sha256: str
    source_binding_revision_id: int | None
    source_binding_sha256: str | None
    capture_manifest_sha256: str | None
    current: CurrentGenerationStatus | None
    generation: ExecutionEvidenceGeneration | None


def _positive_id(value: object) -> int:
    if type(value) is not int or not 0 < value <= MAX_BIGINT:
        raise OperatorInspectionError("custom import operator identifier is invalid")
    return value


def _stored_id(value: object) -> int:
    if type(value) is not int or not 0 < value <= MAX_BIGINT:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return value


def _stored_optional_id(value: object) -> int | None:
    return None if value is None else _stored_id(value)


def _is_stored_id(value: object, expected: int) -> bool:
    return _stored_id(value) == expected


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


def _failure_class(
    state: str, terminal_reason: object
) -> Literal["candidate_rejected", "other_failed", "canceled"] | None:
    if terminal_reason is not None and (type(terminal_reason) is not str or len(terminal_reason) > 64):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    if state == "failed":
        return "candidate_rejected" if terminal_reason == "candidate_rejected" else "other_failed"
    return "canceled" if state == "canceled" else None


def _evidence_execution_status(row) -> ExecutionEvidenceExecution:
    state = row["state"]
    mechanism = row["mechanism"]
    if state not in _EXECUTION_STATES or mechanism not in _MECHANISMS:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return ExecutionEvidenceExecution(
        execution_id=_stored_id(row["execution_id"]),
        dataset_id=_stored_id(row["dataset_id"]),
        definition_revision_id=_stored_id(row["definition_revision_id"]),
        schema_revision_id=_stored_id(row["schema_revision_id"]),
        capture_bundle_id=_stored_optional_id(row["capture_bundle_id"]),
        mechanism=mechanism,
        state=state,
        failure_class=_failure_class(state, row.get("terminal_reason")),
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


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


def _ever_published_expression(*, require_finality: bool = False):
    conditions = (
        CustomImportPublicationEvent.dataset_id == CustomImportGeneration.dataset_id,
        CustomImportPublicationEvent.execution_id == CustomImportGeneration.execution_id,
        CustomImportPublicationEvent.to_generation_id == CustomImportGeneration.generation_id,
        CustomImportPublicationEvent.event_kind.in_(("activated", "rolled_back")),
    )
    if require_finality:
        conditions += (CustomImportPublicationEvent.finality_contract == _FINALITY_CONTRACT,)
    return exists().where(*conditions).label("ever_published")


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


def _current_event_expression():
    return (
        exists()
        .where(
            CustomImportPublicationEvent.dataset_id == CustomImportGeneration.dataset_id,
            CustomImportPublicationEvent.definition_revision_id == CustomImportGeneration.definition_revision_id,
            CustomImportPublicationEvent.schema_revision_id == CustomImportGeneration.schema_revision_id,
            CustomImportPublicationEvent.execution_id == CustomImportGeneration.execution_id,
            CustomImportPublicationEvent.to_generation_id == CustomImportGeneration.generation_id,
            CustomImportPublicationEvent.committed_pointer_version == CustomImportCurrentGeneration.pointer_version,
            CustomImportPublicationEvent.event_kind.in_(("activated", "rolled_back")),
            CustomImportPublicationEvent.finality_contract == _FINALITY_CONTRACT,
        )
        .label("current_event_exists")
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


def _execution_evidence_statement(dataset_id: int, execution_id: int, candidate_generation_id: int | None):
    return (
        select(
            *_EXECUTION_EVIDENCE_COLUMNS,
            _ever_published_expression(require_finality=True),
            _no_change_event_expression(),
            _current_event_expression(),
        )
        .select_from(CustomImportExecution)
        .outerjoin(
            CustomImportDefinitionRevision,
            CustomImportDefinitionRevision.definition_revision_id == CustomImportExecution.definition_revision_id,
        )
        .outerjoin(
            CustomImportSchemaRevision,
            CustomImportSchemaRevision.schema_revision_id == CustomImportExecution.schema_revision_id,
        )
        .outerjoin(
            CustomImportSourceBindingRevision,
            CustomImportSourceBindingRevision.source_binding_revision_id
            == CustomImportExecution.source_binding_revision_id,
        )
        .outerjoin(
            CustomImportCaptureBundle,
            CustomImportCaptureBundle.capture_bundle_id == CustomImportExecution.capture_bundle_id,
        )
        .outerjoin(
            CustomImportGeneration,
            and_(
                CustomImportGeneration.execution_id == CustomImportExecution.execution_id,
                candidate_generation_id is None or CustomImportGeneration.generation_id == candidate_generation_id,
            ),
        )
        .outerjoin(
            CustomImportGenerationSeal,
            CustomImportGenerationSeal.generation_id == CustomImportGeneration.generation_id,
        )
        .outerjoin(
            CustomImportCurrentGeneration,
            CustomImportCurrentGeneration.dataset_id == CustomImportExecution.dataset_id,
        )
        .outerjoin(
            CustomImportNoChangeSeal,
            and_(
                CustomImportNoChangeSeal.execution_id == CustomImportExecution.execution_id,
                CustomImportNoChangeSeal.candidate_generation_id == CustomImportGeneration.generation_id,
            ),
        )
        .where(
            CustomImportExecution.dataset_id == dataset_id,
            CustomImportExecution.execution_id == execution_id,
        )
        .limit(2)
        .execution_options(autoflush=False)
    )


def _evidence_revisions(evidence_snapshot, execution: ExecutionEvidenceExecution) -> tuple[str, str]:
    if (
        not _is_stored_id(evidence_snapshot["definition_id"], execution.definition_revision_id)
        or not _is_stored_id(evidence_snapshot["definition_dataset_id"], execution.dataset_id)
        or not _is_stored_id(evidence_snapshot["definition_schema_revision_id"], execution.schema_revision_id)
        or not _is_stored_id(evidence_snapshot["stored_schema_revision_id"], execution.schema_revision_id)
        or not _is_stored_id(evidence_snapshot["schema_dataset_id"], execution.dataset_id)
    ):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return _digest(evidence_snapshot["definition_sha256"]), _digest(evidence_snapshot["schema_sha256"])


def _evidence_source_binding(
    evidence_snapshot,
    execution: ExecutionEvidenceExecution,
    definition_sha256: str,
    schema_sha256: str,
) -> tuple[int | None, str | None]:
    binding_id = _stored_optional_id(evidence_snapshot["execution_source_binding_revision_id"])
    if binding_id is None:
        if any(
            evidence_snapshot[field] is not None
            for field in (
                "binding_revision_id",
                "binding_dataset_id",
                "binding_definition_revision_id",
                "binding_schema_revision_id",
                "binding_definition_sha256",
                "binding_schema_sha256",
                "source_binding_sha256",
            )
        ):
            raise OperatorInvariantError("custom import operator evidence is invalid")
        return None, None
    if (
        not _is_stored_id(evidence_snapshot["binding_revision_id"], binding_id)
        or not _is_stored_id(evidence_snapshot["binding_dataset_id"], execution.dataset_id)
        or not _is_stored_id(evidence_snapshot["binding_definition_revision_id"], execution.definition_revision_id)
        or not _is_stored_id(evidence_snapshot["binding_schema_revision_id"], execution.schema_revision_id)
        or _digest(evidence_snapshot["binding_definition_sha256"]) != definition_sha256
        or _digest(evidence_snapshot["binding_schema_sha256"]) != schema_sha256
    ):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return binding_id, _digest(evidence_snapshot["source_binding_sha256"])


def _evidence_capture(evidence_snapshot, execution: ExecutionEvidenceExecution) -> str | None:
    capture_bundle_id = execution.capture_bundle_id
    if capture_bundle_id is None:
        if any(
            evidence_snapshot[field] is not None
            for field in (
                "capture_id",
                "capture_dataset_id",
                "capture_definition_revision_id",
                "capture_schema_revision_id",
                "capture_manifest_sha256",
            )
        ):
            raise OperatorInvariantError("custom import operator evidence is invalid")
        return None
    if (
        not _is_stored_id(evidence_snapshot["capture_id"], capture_bundle_id)
        or not _is_stored_id(evidence_snapshot["capture_dataset_id"], execution.dataset_id)
        or not _is_stored_id(evidence_snapshot["capture_definition_revision_id"], execution.definition_revision_id)
        or not _is_stored_id(evidence_snapshot["capture_schema_revision_id"], execution.schema_revision_id)
    ):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return _digest(evidence_snapshot["capture_manifest_sha256"])


def _evidence_seal_status(evidence_snapshot) -> GenerationSealStatus | None:
    seal = _seal_status(evidence_snapshot)
    if seal is None:
        return None
    counts = (
        seal.sealing_fence,
        seal.root_count,
        seal.family_count,
        seal.generation_family_count,
        seal.family_child_count,
        seal.winner_count,
        seal.profile_count,
        seal.root_scalar_count,
        seal.child_scalar_count,
    )
    if type(counts[0]) is not int or counts[0] <= 0 or any(type(count) is not int or count < 0 for count in counts[1:]):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return seal


def _evidence_no_change_status(evidence_snapshot) -> NoChangeStatus | None:
    no_change = _no_change_status(evidence_snapshot)
    if no_change is None:
        return None
    if (
        type(no_change.base_generation_id) is not int
        or no_change.base_generation_id <= 0
        or type(no_change.base_pointer_version) is not int
        or no_change.base_pointer_version < 0
    ):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return no_change


def _evidence_generation_seal(
    evidence_snapshot,
    execution: ExecutionEvidenceExecution,
    generation_id: int,
) -> GenerationSealStatus | None:
    seal_generation_id = evidence_snapshot["seal_generation_id"]
    if seal_generation_id is None:
        if any(evidence_snapshot[field] is not None for field in _SEAL_EVIDENCE_FIELDS):
            raise OperatorInvariantError("custom import operator evidence is invalid")
        return None
    if (
        not _is_stored_id(seal_generation_id, generation_id)
        or not _is_stored_id(evidence_snapshot["seal_dataset_id"], execution.dataset_id)
        or not _is_stored_id(evidence_snapshot["seal_definition_revision_id"], execution.definition_revision_id)
        or not _is_stored_id(evidence_snapshot["seal_schema_revision_id"], execution.schema_revision_id)
        or not _is_stored_id(evidence_snapshot["seal_execution_id"], execution.execution_id)
        or not _is_stored_id(evidence_snapshot["seal_capture_bundle_id"], execution.capture_bundle_id)
        or not _is_stored_id(evidence_snapshot["generation_producing_fence"], evidence_snapshot["sealing_fence"])
        or _digest(evidence_snapshot["generation_producing_token_sha256"])
        != _digest(evidence_snapshot["seal_token_sha256"])
    ):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    seal = _evidence_seal_status(evidence_snapshot)
    if seal is None:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return seal


def _evidence_generation_no_change(
    evidence_snapshot,
    execution: ExecutionEvidenceExecution,
    generation_id: int,
) -> NoChangeStatus | None:
    no_change_execution_id = evidence_snapshot["no_change_execution_id"]
    if no_change_execution_id is None:
        if any(evidence_snapshot[field] is not None for field in _NO_CHANGE_EVIDENCE_FIELDS):
            raise OperatorInvariantError("custom import operator evidence is invalid")
        return None
    if (
        not _is_stored_id(no_change_execution_id, execution.execution_id)
        or not _is_stored_id(evidence_snapshot["no_change_dataset_id"], execution.dataset_id)
        or not _is_stored_id(evidence_snapshot["no_change_definition_revision_id"], execution.definition_revision_id)
        or not _is_stored_id(evidence_snapshot["no_change_schema_revision_id"], execution.schema_revision_id)
        or not _is_stored_id(evidence_snapshot["no_change_capture_bundle_id"], execution.capture_bundle_id)
        or not _is_stored_id(evidence_snapshot["no_change_candidate_generation_id"], generation_id)
    ):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    no_change = _evidence_no_change_status(evidence_snapshot)
    if no_change is None:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return no_change


def _evidence_publication_state(evidence_snapshot, generation_id, current, seal, no_change) -> PublicationState:
    ever_published = evidence_snapshot["ever_published"]
    no_change_event_exists = evidence_snapshot["no_change_event_exists"]
    current_event_exists = evidence_snapshot["current_event_exists"]
    if any(
        type(exists_value) is not bool
        for exists_value in (ever_published, no_change_event_exists, current_event_exists)
    ):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    publication_state = _publication_state(
        generation_id=generation_id,
        current=current,
        seal=seal,
        no_change=no_change,
        ever_published=ever_published,
        no_change_event_exists=no_change_event_exists,
    )
    if publication_state == "current" and not current_event_exists:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    return publication_state


def _evidence_generation(
    evidence_snapshot,
    execution: ExecutionEvidenceExecution,
    current: CurrentGenerationStatus | None,
) -> ExecutionEvidenceGeneration | None:
    """Require exact execution, seal, event, and pointer links for one candidate."""

    generation_id = evidence_snapshot["evidence_generation_id"]
    if generation_id is None:
        if any(
            evidence_snapshot[field] is not None
            for field in _GENERATION_EVIDENCE_FIELDS + _SEAL_EVIDENCE_FIELDS + _NO_CHANGE_EVIDENCE_FIELDS
        ):
            raise OperatorInvariantError("custom import operator evidence is invalid")
        if execution.state in {"completed", "no_change"}:
            raise OperatorInvariantError("custom import operator evidence is invalid")
        return None
    generation_id = _stored_id(generation_id)
    if (
        execution.capture_bundle_id is None
        or not _is_stored_id(evidence_snapshot["generation_dataset_id"], execution.dataset_id)
        or not _is_stored_id(evidence_snapshot["generation_definition_revision_id"], execution.definition_revision_id)
        or not _is_stored_id(evidence_snapshot["generation_schema_revision_id"], execution.schema_revision_id)
        or not _is_stored_id(evidence_snapshot["generation_execution_id"], execution.execution_id)
        or not _is_stored_id(evidence_snapshot["generation_capture_bundle_id"], execution.capture_bundle_id)
    ):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    source_bundle_sha256 = _digest(evidence_snapshot["generation_source_bundle_sha256"])
    seal = _evidence_generation_seal(evidence_snapshot, execution, generation_id)
    if seal is None and execution.state in {"completed", "no_change"}:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    if (
        current is not None
        and current.generation_id == generation_id
        and (
            current.definition_revision_id != execution.definition_revision_id
            or current.schema_revision_id != execution.schema_revision_id
        )
    ):
        raise OperatorInvariantError("custom import operator evidence is invalid")
    no_change = _evidence_generation_no_change(evidence_snapshot, execution, generation_id)
    publication_state = _evidence_publication_state(evidence_snapshot, generation_id, current, seal, no_change)
    return ExecutionEvidenceGeneration(
        generation_id=generation_id,
        source_bundle_sha256=source_bundle_sha256,
        publication_state=publication_state,
        seal=seal,
        no_change=no_change,
    )


async def inspect_execution_evidence(
    session: AsyncSession,
    *,
    dataset_id: int,
    execution_id: int,
    candidate_generation_id: int | None = None,
) -> ExecutionEvidenceStatus:
    """Read one exact execution and its retained evidence in one statement.

    Receipt callers can identify one candidate; without that ID, ambiguous
    recovery evidence is rejected rather than selected arbitrarily.
    """

    dataset_id = _positive_id(dataset_id)
    execution_id = _positive_id(execution_id)
    if candidate_generation_id is not None:
        candidate_generation_id = _positive_id(candidate_generation_id)
    _require_transaction(session)
    evidence_rows = (
        (await session.execute(_execution_evidence_statement(dataset_id, execution_id, candidate_generation_id)))
        .mappings()
        .all()
    )
    if not evidence_rows:
        raise OperatorObjectNotFound("custom import execution was not found")
    if len(evidence_rows) != 1:
        raise OperatorInvariantError("custom import operator evidence is ambiguous")
    evidence_snapshot = evidence_rows[0]
    if candidate_generation_id is not None and evidence_snapshot["evidence_generation_id"] is None:
        raise OperatorObjectNotFound("custom import generation was not found")
    execution = _evidence_execution_status(evidence_snapshot)
    if execution.dataset_id != dataset_id or execution.execution_id != execution_id:
        raise OperatorInvariantError("custom import operator evidence is invalid")
    current = _current_status(evidence_snapshot)
    definition_sha256, schema_sha256 = _evidence_revisions(evidence_snapshot, execution)
    source_binding_revision_id, source_binding_sha256 = _evidence_source_binding(
        evidence_snapshot,
        execution,
        definition_sha256,
        schema_sha256,
    )
    return ExecutionEvidenceStatus(
        execution=execution,
        definition_sha256=definition_sha256,
        schema_sha256=schema_sha256,
        source_binding_revision_id=source_binding_revision_id,
        source_binding_sha256=source_binding_sha256,
        capture_manifest_sha256=_evidence_capture(evidence_snapshot, execution),
        current=current,
        generation=_evidence_generation(evidence_snapshot, execution, current),
    )


__all__ = (
    "CurrentGenerationStatus",
    "ExecutionEvidenceExecution",
    "ExecutionEvidenceGeneration",
    "ExecutionEvidenceStatus",
    "ExecutionStatus",
    "GenerationSealStatus",
    "GenerationStatus",
    "LeaseStatus",
    "NoChangeStatus",
    "OperatorInspectionError",
    "OperatorInvariantError",
    "OperatorObjectNotFound",
    "OperatorTransactionRequired",
    "inspect_execution_evidence",
    "inspect_execution",
    "inspect_generation",
)
