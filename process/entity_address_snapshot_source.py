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
import math
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import MetaData, select, text

from db.models import AddressAliasV1
from process import entity_address_snapshot_alias as alias_authority
from process import reference_family_archive as family_archive
from process.entity_address_snapshot_alias import (
    EntityAddressAliasSemanticReceipt,
    capture_entity_address_alias_semantic_receipt,
)
from process.entity_address_snapshot_receipt import (
    CONTRACT,
    LEGACY_CONTRACT,
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


@dataclass(frozen=True)
class EntityAddressSourceCopy:
    """Trusted-local native COPY callback and fixed whole-stage resource limits."""

    copy_rows: Callable[..., Awaitable[int]]
    max_bytes: int
    timeout: float

    def __post_init__(self):
        _require_source_copy(self.copy_rows, self.max_bytes, self.timeout)


@dataclass
class _EntityAddressExportEvidence:
    """Collect source and stage receipts across the owned export lifecycle."""

    session_factory: Any
    schema_name: str
    dataset_id: UUID
    archive_copy: Callable[[EntityAddressArchiveStageCapture], Awaitable[None]]
    ownership: Any
    contract: str = LEGACY_CONTRACT
    archive_receipts: list[EntityAddressArchiveReceipt] = field(default_factory=list)
    alias_receipts: list[EntityAddressAliasSemanticReceipt] = field(default_factory=list)
    owned_stages: list[Any] = field(default_factory=list)

    async def record_created_stage(self, session) -> None:
        """Retain exact ownership only after this export creates its stage."""

        self.owned_stages.append(
            await self.ownership.capture_created_entity_address_archive_stage(
                session,
                dataset_id=self.dataset_id,
            )
        )

    async def capture_stage_and_copy(self, capture: EntityAddressArchiveStageCapture) -> None:
        """Hash the pinned stage before forwarding it to the native dump."""

        async with self.session_factory() as session, session.begin():
            self.archive_receipts.append(
                await capture_entity_address_archive_receipt(
                    session,
                    schema_name=capture.schema_name,
                    contract=self.contract,
                )
            )
        await self.archive_copy(capture)

    async def capture_source_aliases(self, session, _capture: EntityAddressArchiveSourceCapture) -> None:
        """Hash active source aliases while the serving observation remains pinned."""

        if self.contract == CONTRACT:
            self.alias_receipts.append(
                await alias_authority.capture_entity_address_alias_authority_receipt(
                    session, schema_name=self.schema_name
                )
            )
            return
        self.alias_receipts.append(
            await capture_entity_address_alias_semantic_receipt(
                session,
                schema_name=self.schema_name,
            )
        )

    def bound_receipts(
        self,
        manifest: EntityAddressArchiveStageManifest,
    ) -> tuple[
        EntityAddressArchiveStageManifest,
        EntityAddressArchiveReceipt,
        EntityAddressAliasSemanticReceipt,
    ]:
        """Return exactly one archive receipt and one alias receipt."""

        if len(self.archive_receipts) != 1:
            raise RuntimeError("entity-address archive stage receipt is unavailable")
        if len(self.alias_receipts) != 1:
            raise RuntimeError("entity-address archive alias receipt is unavailable")
        return manifest, self.archive_receipts[0], self.alias_receipts[0]


def entity_address_archive_relations(contract=LEGACY_CONTRACT) -> tuple[EntityAddressArchiveRelation, ...]:
    """Return the exact main-plus-six-support model family in stable order."""

    models = (
        entity_address_unified.EntityAddressUnified,
        *entity_address_unified.SUPPORT_TABLE_MODELS,
    )
    if contract not in (CONTRACT, LEGACY_CONTRACT):
        raise ValueError("entity-address archive contract is unsupported")
    if contract == CONTRACT:
        models += (alias_authority.EntityAddressAliasAuthority,)
    relations = tuple(EntityAddressArchiveRelation(model.__name__, model.__tablename__) for model in models)
    if len({relation.table_name for relation in relations}) != len(relations):
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
    alias_schema=None,
) -> str:
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    if alias_schema is not None:
        await alias_authority._lock_alias_relations(session, alias_schema)
    for relation in relations:
        table_ref = f"{_quoted_identifier(schema)}.{_quoted_identifier(relation.table_name)}"
        await session.execute(text(f"LOCK TABLE {table_ref} IN SHARE MODE"))
    return await _export_postgres_snapshot(session)


async def capture_entity_address_archive_source(
    session,
    *,
    schema_name: str,
    queued_serving_capture: Mapping[str, Any] | EntityAddressObservedServingCapture | None = None,
    contract=LEGACY_CONTRACT,
) -> EntityAddressArchiveSourceCapture:
    """Lock the closed model family and export a snapshot for native ``pg_dump``.

    The caller must own an open transaction and keep it open until the archive
    copy has completed.  A failed lock/export leaves no descriptor claiming a
    portable copy; transaction rollback releases all generation locks.
    """

    schema = _schema_name(schema_name)
    relations = entity_address_archive_relations(contract)
    if queued_serving_capture is None:
        snapshot = await _capture_stage_relations(
            session,
            schema=schema,
            relations=entity_address_archive_relations(),
            **({"alias_schema": schema} if contract == CONTRACT else {}),
        )
        return EntityAddressArchiveSourceCapture(contract, schema, relations, snapshot)
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
    if contract == CONTRACT:
        await alias_authority._lock_alias_relations(session, schema)
    snapshot = await _export_postgres_snapshot(session)
    return EntityAddressArchiveSourceCapture(contract, schema, relations, snapshot, observed)


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
    source_copy=None,
    copy_deadline=None,
    on_precreated=None,
) -> None:
    """Copy the exact captured family under the source transaction snapshot."""

    snapshot = source_capture.postgres_snapshot
    if not _SNAPSHOT_TOKEN.fullmatch(snapshot):
        raise RuntimeError("entity-address archive source snapshot is invalid")
    if source_capture.contract == CONTRACT:
        _require_copy_bundle(source_copy)
        if not callable(on_precreated):
            raise ValueError("entity-address v2 source requires protected empty-stage custody")
        if copy_deadline is None or copy_deadline <= asyncio.get_running_loop().time():
            raise TimeoutError("entity-address source COPY deadline exceeded")
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await session.execute(text(f"SET TRANSACTION SNAPSHOT '{snapshot}'"))
    await session.execute(text(f"CREATE SCHEMA {_quoted_identifier(stage_schema)}"))
    if source_capture.contract == CONTRACT:
        await _clone_set_validated_source(
            session,
            source_capture,
            stage_schema,
            source_copy=source_copy,
            copy_deadline=copy_deadline,
            on_precreated=on_precreated,
        )
        return
    for relation in source_capture.relations:
        source_ref = f"{_quoted_identifier(source_capture.schema_name)}.{_quoted_identifier(relation.table_name)}"
        stage_ref = f"{_quoted_identifier(stage_schema)}.{_quoted_identifier(relation.table_name)}"
        await session.execute(text(f"CREATE TABLE {stage_ref} (LIKE {source_ref} INCLUDING ALL)"))
        if relation.table_name == _EVIDENCE_TABLE_NAME:
            await _clone_entity_address_evidence_sequence(session, stage_schema=stage_schema)
        await session.execute(text(f"INSERT INTO {stage_ref} SELECT * FROM {source_ref}"))


async def _create_clone_model_indexes(session, schema, models):
    """Use the same trusted DDL as v2 restore, not deparsed index expressions."""
    from process.entity_address_snapshot_restore import _additional_index_sql

    for model in models:
        for index in getattr(model, "__my_additional_indexes__", ()) or ():
            await session.execute(
                text(
                    _additional_index_sql(
                        schema_name=schema,
                        table_name=model.__tablename__,
                        stage_table_name=model.__tablename__,
                        index=index,
                    )
                )
            )


def _require_source_copy(copy_source_rows, max_copy_bytes, copy_timeout):
    """V2 must have a bounded trusted-local native copier before any candidate work."""
    if not callable(copy_source_rows):
        raise ValueError("entity-address v2 source requires native COPY")
    if type(max_copy_bytes) is not int or not 0 < max_copy_bytes < 2**63:
        raise ValueError("entity-address source COPY byte limit is invalid")
    if type(copy_timeout) not in (int, float) or not math.isfinite(copy_timeout) or not 0 < copy_timeout <= 86400:
        raise ValueError("entity-address source COPY timeout is invalid")


def _require_copy_bundle(source_copy):
    """No callback or limit may originate from request or archive metadata."""
    if not isinstance(source_copy, EntityAddressSourceCopy):
        raise ValueError("entity-address v2 source requires bounded native COPY")
    _require_source_copy(source_copy.copy_rows, source_copy.max_bytes, source_copy.timeout)


async def _copy_pinned_model_rows(
    session,
    model,
    *,
    source_schema,
    target_schema,
    copy_source_rows,
    max_bytes,
    deadline,
    active_aliases=False,
):
    """Compile only model-defined columns and the fixed active-alias projection."""
    from sqlalchemy.dialects.postgresql import asyncpg

    source_table = model.__table__.to_metadata(MetaData(), schema=source_schema)
    columns = (
        alias_authority._SEMANTIC_COLUMNS if active_aliases else tuple(column.name for column in source_table.columns)
    )
    statement = select(*(source_table.c[name] for name in columns))
    if active_aliases:
        statement = statement.where(source_table.c.revoked_at.is_(None))
    remaining_timeout = deadline - asyncio.get_running_loop().time()
    if remaining_timeout <= 0:
        raise TimeoutError("entity-address source COPY deadline exceeded")
    size_bytes = await copy_source_rows(
        session,
        str(statement.compile(dialect=asyncpg.dialect())),
        schema_name=target_schema,
        table_name=alias_authority.AUTHORITY_TABLE if active_aliases else model.__tablename__,
        columns=columns,
        max_bytes=max_bytes,
        timeout=remaining_timeout,
    )
    if type(size_bytes) is not int or not 0 <= size_bytes <= max_bytes:
        raise RuntimeError("entity-address source COPY byte accounting is invalid")
    return max_bytes - size_bytes


async def _precreate_source_heaps(session, schema, models):
    """Create the complete empty family before custody or payload can be admitted."""
    spec = family_archive.ReferenceFamilySpec(
        "entity-address-unified", (*models, alias_authority.EntityAddressAliasAuthority)
    )
    await family_archive._create_model_heaps(session, spec, schema, create_indexes=False, ordinary_heaps=True)
    await _clone_entity_address_evidence_sequence(session, stage_schema=schema)


async def _clone_set_validated_source(session, capture, schema, *, source_copy, copy_deadline, on_precreated):
    """Load heaps, complete installed indexes, then compare the exact pinned sets."""
    models = (
        entity_address_unified.EntityAddressUnified,
        *entity_address_unified.SUPPORT_TABLE_MODELS,
    )
    await _precreate_source_heaps(session, schema, models)
    await on_precreated(session)
    max_copy_bytes = source_copy.max_bytes
    for model in models:
        max_copy_bytes = await _copy_pinned_model_rows(
            session,
            model,
            source_schema=capture.schema_name,
            target_schema=schema,
            copy_source_rows=source_copy.copy_rows,
            max_bytes=max_copy_bytes,
            deadline=copy_deadline,
        )
    await _copy_pinned_model_rows(
        session,
        AddressAliasV1,
        source_schema=capture.schema_name,
        target_schema=schema,
        copy_source_rows=source_copy.copy_rows,
        max_bytes=max_copy_bytes,
        deadline=copy_deadline,
        active_aliases=True,
    )
    metadata = MetaData(schema=schema)
    for model in models:
        table = model.__table__.to_metadata(metadata, schema=schema)
        await family_archive._create_table_constraints(session, table)
    await _create_clone_model_indexes(session, schema, models)
    await family_archive._create_table_constraints(
        session,
        alias_authority.EntityAddressAliasAuthority.__table__.to_metadata(MetaData(schema=schema), schema=schema),
    )
    for table in metadata.tables.values():
        await family_archive._create_table_constraints(session, table, backing_indexes=False)
    await family_archive._validate_model_foreign_keys(session, metadata)
    for model in models:
        if not await family_archive._is_model_table_equal(
            session,
            model,
            left_schema=capture.schema_name,
            left_name=model.__tablename__,
            right_schema=schema,
            right_name=model.__tablename__,
        ):
            raise RuntimeError("entity-address pinned source content differs")
    await alias_authority.require_matching_entity_address_alias_authority(
        session, authority_schema=schema, alias_schema=capture.schema_name
    )


async def prepare_entity_address_archive_source(
    session,
    *,
    source_capture,
    dataset_id,
    source_copy,
    on_precreated,
):
    """Build and check one protected family without committing its caller's custody."""
    if source_capture.contract != CONTRACT:
        raise ValueError("entity-address protected source requires the set contract")
    _require_copy_bundle(source_copy)
    async with asyncio.timeout(source_copy.timeout) as deadline:
        await _clone_entity_address_archive_source(
            session,
            source_capture=source_capture,
            stage_schema=entity_address_archive_stage_schema(dataset_id),
            source_copy=source_copy,
            copy_deadline=deadline.when(),
            on_precreated=on_precreated,
        )
        return await capture_entity_address_archive_receipt(
            session,
            schema_name=entity_address_archive_stage_schema(dataset_id),
            contract=CONTRACT,
        )


async def _capture_entity_address_archive_stage(
    session,
    *,
    dataset_id: UUID,
    contract=LEGACY_CONTRACT,
) -> EntityAddressArchiveStageCapture:
    """Lock the committed UUID-owned clone while its archive is copied."""

    schema = entity_address_archive_stage_schema(dataset_id)
    relations = entity_address_archive_relations(contract)
    snapshot = await _capture_stage_relations(session, schema=schema, relations=relations)
    return EntityAddressArchiveStageCapture(
        contract,
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
    contract=LEGACY_CONTRACT,
) -> EntityAddressArchiveSourceManifest:
    """Run ``archive_copy`` while a pinned source generation remains available.

    ``archive_copy`` must consume the snapshot synchronously, for example by
    awaiting a subprocess-backed ``pg_dump``.  The returned manifest contains
    no snapshot token because the transaction has ended and released its
    locks.  It identifies source relations only; an admitted source-clone
    lifecycle remains necessary for a differently named destination stage.
    """

    if contract == CONTRACT:
        raise ValueError("entity-address v2 source requires the bounded native COPY coordinator")
    async with session_factory() as session, session.begin():
        capture = await capture_entity_address_archive_source(
            session,
            schema_name=schema_name,
            queued_serving_capture=queued_serving_capture,
            contract=contract,
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
    contract=LEGACY_CONTRACT,
) -> EntityAddressArchiveStageManifest:
    """Clone and dump the exact family without passing live names to ``pg_dump``.

    The UUID-derived schema is intentionally not dropped here.  The coordinator
    that owns ``dataset_id`` must retain it through restore verification and clean
    it up by that exact derived name on every terminal path.
    """

    stage_schema = entity_address_archive_stage_schema(dataset_id)
    if contract == CONTRACT:
        raise ValueError("entity-address v2 stage requires the bounded native COPY coordinator")
    async with session_factory() as source_session, source_session.begin():
        source_capture = await capture_entity_address_archive_source(
            source_session,
            schema_name=schema_name,
            queued_serving_capture=queued_serving_capture,
            contract=contract,
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
            contract=contract,
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
    contract=LEGACY_CONTRACT,
    source_copy: EntityAddressSourceCopy | None = None,
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

    if contract == CONTRACT:
        raise ValueError("entity-address v2 export requires a retained protected source")
    ownership = importlib.import_module("process.entity_address_snapshot_ownership")
    evidence = _EntityAddressExportEvidence(
        session_factory=session_factory,
        schema_name=schema_name,
        dataset_id=dataset_id,
        archive_copy=archive_copy,
        ownership=ownership,
        contract=contract,
    )

    try:
        manifest = await export_entity_address_archive_stage(
            session_factory,
            schema_name=schema_name,
            dataset_id=dataset_id,
            queued_serving_capture=queued_serving_capture,
            archive_copy=evidence.capture_stage_and_copy,
            stage_created=evidence.record_created_stage,
            source_captured=evidence.capture_source_aliases,
            contract=contract,
        )
    finally:
        if evidence.owned_stages:
            await _cleanup_owned_archive_stage(session_factory, ownership, evidence.owned_stages[0])
    return evidence.bound_receipts(manifest)


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
    "EntityAddressSourceCopy",
    "EntityAddressObservedServingCapture",
    "capture_entity_address_archive_source",
    "capture_entity_address_observed_serving",
    "entity_address_archive_stage_schema",
    "entity_address_archive_relations",
    "export_entity_address_archive_source",
    "export_entity_address_archive_stage",
    "export_entity_address_archive_with_receipt",
    "prepare_entity_address_archive_source",
    "validate_entity_address_observed_serving_capture",
]
