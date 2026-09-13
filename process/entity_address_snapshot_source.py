# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Clone one pinned unified-address generation before native archive export.

The live generation is pinned only while its exact seven-relation family is
copied to a UUID-owned schema.  A second pin freezes that committed clone while
the caller runs its bounded ``pg_dump``.  The dump therefore names only isolated
relations and can later restore to the same owned qualified names without ever
targeting serving relations.  Stage cleanup deliberately remains caller-owned.
"""

from __future__ import annotations

import importlib
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text

entity_address_unified = importlib.import_module("process.entity_address_unified")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CONTRACT = "entity_address_unified.postgres.v1"
_STAGE_SCHEMA_PREFIX = "entity_address_archive_"
_SNAPSHOT_TOKEN = re.compile(r"^[0-9A-Fa-f-]+$")


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
    if not _IDENTIFIER.fullmatch(normalized):
        raise ValueError("entity-address archive source requires a safe schema name")
    return normalized


def _quoted_identifier(value: str) -> str:
    return f'"{value}"'


def entity_address_archive_stage_schema(dataset_id: UUID) -> str:
    """Return the only isolated schema name admitted for one archive dataset."""

    if not isinstance(dataset_id, UUID):
        raise ValueError("entity-address archive stage requires a UUID dataset_id")
    return _STAGE_SCHEMA_PREFIX + dataset_id.hex


async def _capture_relations(
    session,
    *,
    schema: str,
    relations: tuple[EntityAddressArchiveRelation, ...],
) -> str:
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    for relation in relations:
        table_ref = f"{_quoted_identifier(schema)}.{_quoted_identifier(relation.table_name)}"
        await session.execute(text(f"LOCK TABLE {table_ref} IN SHARE MODE"))
    snapshot = (await session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
    if not isinstance(snapshot, str) or not _SNAPSHOT_TOKEN.fullmatch(snapshot):
        raise RuntimeError("entity-address archive source did not export a PostgreSQL snapshot")
    return snapshot


async def capture_entity_address_archive_source(session, *, schema_name: str) -> EntityAddressArchiveSourceCapture:
    """Lock the closed model family and export a snapshot for native ``pg_dump``.

    The caller must own an open transaction and keep it open until the archive
    copy has completed.  A failed lock/export leaves no descriptor claiming a
    portable copy; transaction rollback releases all generation locks.
    """

    schema = _schema_name(schema_name)
    relations = entity_address_archive_relations()
    snapshot = await _capture_relations(session, schema=schema, relations=relations)
    return EntityAddressArchiveSourceCapture(_CONTRACT, schema, relations, snapshot)


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
        await session.execute(text(f"INSERT INTO {stage_ref} SELECT * FROM {source_ref}"))


async def _capture_entity_address_archive_stage(
    session,
    *,
    dataset_id: UUID,
) -> EntityAddressArchiveStageCapture:
    """Lock the committed UUID-owned clone while its archive is copied."""

    schema = entity_address_archive_stage_schema(dataset_id)
    relations = entity_address_archive_relations()
    snapshot = await _capture_relations(session, schema=schema, relations=relations)
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
) -> EntityAddressArchiveSourceManifest:
    """Run ``archive_copy`` while a pinned source generation remains available.

    ``archive_copy`` must consume the snapshot synchronously, for example by
    awaiting a subprocess-backed ``pg_dump``.  The returned manifest contains
    no snapshot token because the transaction has ended and released its
    locks.  It identifies source relations only; an admitted source-clone
    lifecycle remains necessary for a differently named destination stage.
    """

    async with session_factory() as session, session.begin():
        capture = await capture_entity_address_archive_source(session, schema_name=schema_name)
        await archive_copy(capture)
    return EntityAddressArchiveSourceManifest(capture.contract, capture.schema_name, capture.relations)


async def stage_and_export_entity_address_archive_source(
    session_factory,
    *,
    schema_name: str,
    dataset_id: UUID,
    archive_copy: Callable[[EntityAddressArchiveStageCapture], Awaitable[None]],
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
        )
        async with session_factory() as clone_session, clone_session.begin():
            await _clone_entity_address_archive_source(
                clone_session,
                source_capture=source_capture,
                stage_schema=stage_schema,
            )
        async with session_factory() as stage_session, stage_session.begin():
            capture = await _capture_entity_address_archive_stage(
                stage_session,
                dataset_id=dataset_id,
            )
            await archive_copy(capture)
    return EntityAddressArchiveStageManifest(
        capture.contract, capture.dataset_id, capture.schema_name, capture.relations
    )


__all__ = [
    "EntityAddressArchiveRelation",
    "EntityAddressArchiveSourceCapture",
    "EntityAddressArchiveSourceManifest",
    "EntityAddressArchiveStageCapture",
    "EntityAddressArchiveStageManifest",
    "capture_entity_address_archive_source",
    "entity_address_archive_stage_schema",
    "entity_address_archive_relations",
    "export_entity_address_archive_source",
    "stage_and_export_entity_address_archive_source",
]
