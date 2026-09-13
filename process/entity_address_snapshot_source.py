# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Hold one real unified-address generation while a source archive is copied.

The returned PostgreSQL snapshot is usable only while the caller keeps its
transaction open.  SHARE locks prevent a concurrent cutover or cleanup from
replacing any selected relation; relation names, not local OIDs, are the
portable source manifest.  This module deliberately does not claim that an
archive of live table names can restore into a differently named destination
stage; that needs an admitted source-clone retention lifecycle.
"""

from __future__ import annotations

import importlib
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from sqlalchemy import text

entity_address_unified = importlib.import_module("process.entity_address_unified")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CONTRACT = "entity_address_unified.postgres.v1"


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


def entity_address_archive_relations() -> tuple[EntityAddressArchiveRelation, ...]:
    """Return the exact main-plus-six-support model family in stable order."""

    models = (entity_address_unified.EntityAddressUnified, *entity_address_unified.SUPPORT_TABLE_MODELS)
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


async def capture_entity_address_archive_source(session, *, schema_name: str) -> EntityAddressArchiveSourceCapture:
    """Lock the closed model family and export a snapshot for native ``pg_dump``.

    The caller must own an open transaction and keep it open until the archive
    copy has completed.  A failed lock/export leaves no descriptor claiming a
    portable copy; transaction rollback releases all generation locks.
    """

    schema = _schema_name(schema_name)
    relations = entity_address_archive_relations()
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    for relation in relations:
        table_ref = f"{_quoted_identifier(schema)}.{_quoted_identifier(relation.table_name)}"
        await session.execute(text(f"LOCK TABLE {table_ref} IN SHARE MODE"))
    snapshot = (await session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
    if not isinstance(snapshot, str) or not snapshot:
        raise RuntimeError("entity-address archive source did not export a PostgreSQL snapshot")
    return EntityAddressArchiveSourceCapture(_CONTRACT, schema, relations, snapshot)


async def export_entity_address_archive_source(
    session_factory,
    *,
    schema_name: str,
    archive_copy: Callable[[EntityAddressArchiveSourceCapture], Awaitable[None]],
) -> EntityAddressArchiveSourceManifest:
    """Run ``archive_copy`` while a pinned source generation remains available.

    ``archive_copy`` must consume the snapshot synchronously, for example by
    awaiting a subprocess-backed ``pg_dump``.  The returned manifest contains
    no snapshot token because the transaction has rolled back and released its
    locks.  It identifies source relations only; an admitted source-clone
    lifecycle remains necessary for a differently named destination stage.
    """

    async with session_factory() as session, session.begin():
        capture = await capture_entity_address_archive_source(session, schema_name=schema_name)
        await archive_copy(capture)
    return EntityAddressArchiveSourceManifest(capture.contract, capture.schema_name, capture.relations)


__all__ = [
    "EntityAddressArchiveRelation",
    "EntityAddressArchiveSourceCapture",
    "EntityAddressArchiveSourceManifest",
    "capture_entity_address_archive_source",
    "entity_address_archive_relations",
    "export_entity_address_archive_source",
]
