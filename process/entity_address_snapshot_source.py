# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Clone one pinned unified-address generation before native archive export.

The live generation is pinned only while its exact seven-relation family is
copied to a UUID-owned schema.  A second pin freezes that committed clone while
the caller runs its bounded ``pg_dump``.  The dump therefore names only isolated
relations and can later restore to the same owned qualified names without ever
targeting serving relations.  Stage cleanup deliberately remains caller-owned.
"""

from __future__ import annotations

import asyncio
import importlib
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import text

from process.entity_address_snapshot_alias import (
    EntityAddressAliasSemanticReceipt,
    capture_entity_address_alias_semantic_receipt,
)
from process.entity_address_snapshot_receipt import (
    EntityAddressArchiveReceipt,
    capture_entity_address_archive_receipt,
)
from process.entity_address_snapshot_serving import (
    EntityAddressObservedServingCapture,
    capture_entity_address_observed_serving,
    observe_entity_address_serving,
    validate_entity_address_observed_serving_capture,
)

entity_address_unified = importlib.import_module("process.entity_address_unified")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CONTRACT = "entity_address_unified.postgres.v1"
_STAGE_SCHEMA_PREFIX = "entity_address_archive_"
_SNAPSHOT_TOKEN = re.compile(r"^[0-9A-Fa-f-]+$")
_EVIDENCE_TABLE_NAME = entity_address_unified.EntityAddressEvidence.__tablename__
_EVIDENCE_ID_COLUMN = "evidence_id"
_EVIDENCE_SEQUENCE_NAME = f"{_EVIDENCE_TABLE_NAME}_{_EVIDENCE_ID_COLUMN}_seq"


@dataclass(frozen=True)
class EntityAddressArchiveRelation:
    """One portable physical relation selected from the completed generation."""

    model_name: str
    table_name: str


@dataclass(frozen=True)
class EntityAddressArchiveSourceCapture:
    """A caller-held source transaction pin for one exact relation family."""

    contract: str
    schema_name: str
    relations: tuple[EntityAddressArchiveRelation, ...]
    postgres_snapshot: str
    observed_serving: EntityAddressObservedServingCapture | None = None


@dataclass(frozen=True)
class EntityAddressArchiveSourceManifest:
    """Portable source relation identity retained after the source transaction ends."""

    contract: str
    schema_name: str
    relations: tuple[EntityAddressArchiveRelation, ...]


@dataclass(frozen=True)
class EntityAddressArchiveStageCapture:
    """One UUID-owned clone pinned for an isolated native archive export."""

    contract: str
    dataset_id: UUID
    schema_name: str
    relations: tuple[EntityAddressArchiveRelation, ...]
    postgres_snapshot: str


@dataclass(frozen=True)
class EntityAddressArchiveStageManifest:
    """The caller-owned clone identity retained after the dump transaction ends."""

    contract: str
    dataset_id: UUID
    schema_name: str
    relations: tuple[EntityAddressArchiveRelation, ...]


def entity_address_archive_relations() -> tuple[EntityAddressArchiveRelation, ...]:
    """Return the exact main-plus-six-support model family in stable order."""

    models = (
        entity_address_unified.EntityAddressUnified,
        *entity_address_unified.SUPPORT_TABLE_MODELS,
    )
    relations = tuple(EntityAddressArchiveRelation(model.__name__, model.__tablename__) for model in models)
    if len(relations) != 7 or len({relation.table_name for relation in relations}) != len(relations):
        raise RuntimeError("entity-address archive relation family is incomplete")
    if any(not _IDENTIFIER.fullmatch(relation.table_name) for relation in relations):
        raise RuntimeError("entity-address archive relation family is invalid")
    return relations


def _schema_name(schema_name: str) -> str:
    if not isinstance(schema_name, str):
        raise ValueError("entity-address archive source requires a schema name")
    normalized = entity_address_unified._validate_schema_name(schema_name)
    if not _IDENTIFIER.fullmatch(normalized) or len(normalized.encode("utf-8")) > 63:
        raise ValueError("entity-address archive source requires a safe schema name")
    return normalized


def _quoted_identifier(value: str) -> str:
    return f'"{value}"'


def entity_address_archive_stage_schema(dataset_id: UUID) -> str:
    """Return the only isolated schema name admitted for one archive dataset."""

    if not isinstance(dataset_id, UUID):
        raise ValueError("entity-address archive stage requires a UUID dataset_id")
    return _STAGE_SCHEMA_PREFIX + dataset_id.hex


async def _export_postgres_snapshot(session) -> str:
    snapshot = (await session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
    if not isinstance(snapshot, str) or not _SNAPSHOT_TOKEN.fullmatch(snapshot):
        raise RuntimeError("entity-address archive source did not export a PostgreSQL snapshot")
    return snapshot


async def _capture_stage_relations(
    session,
    *,
    schema: str,
    relations: tuple[EntityAddressArchiveRelation, ...],
) -> str:
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    for relation in relations:
        table_ref = f"{_quoted_identifier(schema)}.{_quoted_identifier(relation.table_name)}"
        await session.execute(text(f"LOCK TABLE {table_ref} IN SHARE MODE"))
    return await _export_postgres_snapshot(session)


async def capture_entity_address_archive_source(
    session,
    *,
    schema_name: str,
    queued_serving_capture: Mapping[str, Any] | EntityAddressObservedServingCapture | None = None,
) -> EntityAddressArchiveSourceCapture:
    """Lock the closed model family and export a snapshot for native ``pg_dump``.

    The caller must own an open transaction and keep it open until the archive
    copy has completed.  A failed lock/export leaves no descriptor claiming a
    portable copy; transaction rollback releases all generation locks.
    """

    schema = _schema_name(schema_name)
    relations = entity_address_archive_relations()
    if queued_serving_capture is None:
        snapshot = await _capture_stage_relations(session, schema=schema, relations=relations)
        return EntityAddressArchiveSourceCapture(_CONTRACT, schema, relations, snapshot)
    queued = validate_entity_address_observed_serving_capture(
        queued_serving_capture,
        schema_name=schema,
    )
    observed = await observe_entity_address_serving(
        session,
        schema_name=schema,
        apply_queue_bounds=False,
    )
    if observed != queued:
        raise RuntimeError("entity-address queued serving identity changed")
    snapshot = await _export_postgres_snapshot(session)
    return EntityAddressArchiveSourceCapture(_CONTRACT, schema, relations, snapshot, observed)


async def _clone_entity_address_evidence_sequence(session, *, stage_schema: str) -> None:
    """Replace the source-bound BIGSERIAL default with its exact clone-owned sequence."""

    stage_table_ref = f"{_quoted_identifier(stage_schema)}.{_quoted_identifier(_EVIDENCE_TABLE_NAME)}"
    stage_sequence_ref = f"{_quoted_identifier(stage_schema)}.{_quoted_identifier(_EVIDENCE_SEQUENCE_NAME)}"
    await session.execute(text(f"CREATE SEQUENCE {stage_sequence_ref}"))
    await session.execute(
        text(
            f"ALTER SEQUENCE {stage_sequence_ref} OWNED BY {stage_table_ref}.{_quoted_identifier(_EVIDENCE_ID_COLUMN)}"
        )
    )
    await session.execute(
        text(
            f"ALTER TABLE {stage_table_ref} ALTER COLUMN {_quoted_identifier(_EVIDENCE_ID_COLUMN)} "
            f"SET DEFAULT nextval('{stage_sequence_ref}'::regclass)"
        )
    )


async def _clone_entity_address_archive_source(
    session,
    *,
    source_capture: EntityAddressArchiveSourceCapture,
    stage_schema: str,
) -> None:
    """Copy the exact captured family under the source transaction snapshot."""

    snapshot = source_capture.postgres_snapshot
    if not _SNAPSHOT_TOKEN.fullmatch(snapshot):
        raise RuntimeError("entity-address archive source snapshot is invalid")
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await session.execute(text(f"SET TRANSACTION SNAPSHOT '{snapshot}'"))
    await session.execute(text(f"CREATE SCHEMA {_quoted_identifier(stage_schema)}"))
    for relation in source_capture.relations:
        source_ref = f"{_quoted_identifier(source_capture.schema_name)}.{_quoted_identifier(relation.table_name)}"
        stage_ref = f"{_quoted_identifier(stage_schema)}.{_quoted_identifier(relation.table_name)}"
        await session.execute(text(f"CREATE TABLE {stage_ref} (LIKE {source_ref} INCLUDING ALL)"))
        if relation.table_name == _EVIDENCE_TABLE_NAME:
            await _clone_entity_address_evidence_sequence(session, stage_schema=stage_schema)
        await session.execute(text(f"INSERT INTO {stage_ref} SELECT * FROM {source_ref}"))


async def _capture_entity_address_archive_stage(
    session,
    *,
    dataset_id: UUID,
) -> EntityAddressArchiveStageCapture:
    """Lock the committed UUID-owned clone while its archive is copied."""

    schema = entity_address_archive_stage_schema(dataset_id)
    relations = entity_address_archive_relations()
    snapshot = await _capture_stage_relations(session, schema=schema, relations=relations)
    return EntityAddressArchiveStageCapture(
        _CONTRACT,
        dataset_id,
        schema,
        relations,
        snapshot,
    )


async def export_entity_address_archive_source(
    session_factory,
    *,
    schema_name: str,
    archive_copy: Callable[[EntityAddressArchiveSourceCapture], Awaitable[None]],
    queued_serving_capture: Mapping[str, Any] | EntityAddressObservedServingCapture | None = None,
) -> EntityAddressArchiveSourceManifest:
    """Run ``archive_copy`` while a pinned source generation remains available.

    ``archive_copy`` must consume the snapshot synchronously, for example by
    awaiting a subprocess-backed ``pg_dump``.  The returned manifest contains
    no snapshot token because the transaction has ended and released its
    locks.  It identifies source relations only; an admitted source-clone
    lifecycle remains necessary for a differently named destination stage.
    """

    async with session_factory() as session, session.begin():
        capture = await capture_entity_address_archive_source(
            session,
            schema_name=schema_name,
            queued_serving_capture=queued_serving_capture,
        )
        await archive_copy(capture)
    return EntityAddressArchiveSourceManifest(capture.contract, capture.schema_name, capture.relations)


async def export_entity_address_archive_stage(
    session_factory,
    *,
    schema_name: str,
    dataset_id: UUID,
    archive_copy: Callable[[EntityAddressArchiveStageCapture], Awaitable[None]],
    queued_serving_capture: Mapping[str, Any] | EntityAddressObservedServingCapture | None = None,
    stage_created: Callable[[object], Awaitable[None]] | None = None,
    source_captured: Callable[[object, EntityAddressArchiveSourceCapture], Awaitable[None]] | None = None,
) -> EntityAddressArchiveStageManifest:
    """Clone and dump the exact family without passing live names to ``pg_dump``.

    The UUID-derived schema is intentionally not dropped here.  The coordinator
    that owns ``dataset_id`` must retain it through restore verification and clean
    it up by that exact derived name on every terminal path.
    """

    stage_schema = entity_address_archive_stage_schema(dataset_id)
    async with session_factory() as source_session, source_session.begin():
        source_capture = await capture_entity_address_archive_source(
            source_session,
            schema_name=schema_name,
            queued_serving_capture=queued_serving_capture,
        )
        if source_captured is not None:
            await source_captured(source_session, source_capture)
        async with session_factory() as clone_session, clone_session.begin():
            await _clone_entity_address_archive_source(
                clone_session,
                source_capture=source_capture,
                stage_schema=stage_schema,
            )
            if stage_created is not None:
                await stage_created(clone_session)
    async with session_factory() as stage_session, stage_session.begin():
        capture = await _capture_entity_address_archive_stage(
            stage_session,
            dataset_id=dataset_id,
        )
        await archive_copy(capture)
    return EntityAddressArchiveStageManifest(
        capture.contract, capture.dataset_id, capture.schema_name, capture.relations
    )


async def export_entity_address_archive_with_receipt(
    session_factory,
    *,
    schema_name: str,
    dataset_id: UUID,
    queued_serving_capture: Mapping[str, Any] | EntityAddressObservedServingCapture,
    archive_copy: Callable[[EntityAddressArchiveStageCapture], Awaitable[None]],
) -> tuple[
    EntityAddressArchiveStageManifest,
    EntityAddressArchiveReceipt,
    EntityAddressAliasSemanticReceipt,
]:
    """Bind the native receipt and dump to the same pinned, committed clone.

    Receipt capture starts a fresh transaction while the existing stage pin
    blocks writers. The live generation is no longer pinned during either the
    receipt scan or archive copy. This wrapper cleans only its locally created
    clone on success, failure, and cancellation; a preexisting schema collision
    never grants cleanup authority.
    """

    ownership = importlib.import_module("process.entity_address_snapshot_ownership")
    captured_receipts = []
    captured_alias_receipts = []
    owned_stages = []

    async def _record_created_stage(session) -> None:
        owned_stages.append(
            await ownership.capture_created_entity_address_archive_stage(session, dataset_id=dataset_id)
        )

    async def _capture_and_copy(capture: EntityAddressArchiveStageCapture) -> None:
        async with session_factory() as session, session.begin():
            captured_receipts.append(
                await capture_entity_address_archive_receipt(session, schema_name=capture.schema_name)
            )
        await archive_copy(capture)

    async def _capture_alias_receipt(session, _capture: EntityAddressArchiveSourceCapture) -> None:
        captured_alias_receipts.append(
            await capture_entity_address_alias_semantic_receipt(
                session,
                schema_name=schema_name,
            )
        )

    try:
        manifest = await export_entity_address_archive_stage(
            session_factory,
            schema_name=schema_name,
            dataset_id=dataset_id,
            queued_serving_capture=queued_serving_capture,
            archive_copy=_capture_and_copy,
            stage_created=_record_created_stage,
            source_captured=_capture_alias_receipt,
        )
    finally:
        if owned_stages:
            await _cleanup_owned_archive_stage(session_factory, ownership, owned_stages[0])
    if len(captured_receipts) != 1:
        raise RuntimeError("entity-address archive stage receipt is unavailable")
    if len(captured_alias_receipts) != 1:
        raise RuntimeError("entity-address archive alias receipt is unavailable")
    return manifest, captured_receipts[0], captured_alias_receipts[0]


async def _cleanup_owned_archive_stage(session_factory, ownership, owner) -> None:
    """Drain the exact cleanup even if the exporting task is cancelled again."""

    async def _cleanup() -> None:
        async with session_factory() as session, session.begin():
            await ownership.cleanup_entity_address_archive_stage(session, owner=owner)

    cleanup_task = asyncio.create_task(_cleanup())
    is_cancelled = False
    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            is_cancelled = True
    cleanup_task.result()
    if is_cancelled:
        raise asyncio.CancelledError


__all__ = [
    "EntityAddressArchiveRelation",
    "EntityAddressArchiveSourceCapture",
    "EntityAddressArchiveSourceManifest",
    "EntityAddressArchiveStageCapture",
    "EntityAddressArchiveStageManifest",
    "EntityAddressObservedServingCapture",
    "capture_entity_address_archive_source",
    "capture_entity_address_observed_serving",
    "entity_address_archive_stage_schema",
    "entity_address_archive_relations",
    "export_entity_address_archive_source",
    "export_entity_address_archive_stage",
    "export_entity_address_archive_with_receipt",
    "validate_entity_address_observed_serving_capture",
]
