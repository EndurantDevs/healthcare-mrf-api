# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed persisted identity checks for custom-import extension reads."""

from __future__ import annotations

import hmac
from dataclasses import dataclass, field

from sqlalchemy import and_, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportDefinitionRevision,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportPublicationEvent,
    CustomImportSchemaRevision,
)
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.failure_diagnostics import (
    _failure_chain,
    _static_attribute,
    database_sqlstate,
)
from process.custom_import.publication import (
    FINALITY_EVENT_CONTRACT,
    PublicationConflict,
    verify_publication_event_material,
)
from process.custom_import.read_contracts import (
    CustomImportReadUnavailableError,
    PinnedReadTarget,
)
from process.custom_import.storage_layout import SNAPSHOT_MODELS

_PINNED_LAYOUT_SQL = """
    WITH pinned AS MATERIALIZED (SELECT {invocation} AS family_id)
    SELECT pinned.family_id,
           pinned.family_id IS NOT NULL
           AND census.legacy_count = cardinality(CAST(:legacy_names AS text[])) AS legacy_snapshot,
           CASE WHEN pinned.family_id IS NULL
                     OR census.legacy_count = cardinality(CAST(:legacy_names AS text[]))
                THEN '[]'::jsonb
                ELSE (
                    SELECT coalesce(jsonb_agg(jsonb_build_array(expected.role_slot, c.oid::bigint)
                                             ORDER BY expected.role_slot), '[]'::jsonb)
                    FROM unnest(CAST(:block_slots AS integer[]), CAST(:block_names AS text[]))
                         AS expected(role_slot, table_name)
                    JOIN pg_catalog.pg_namespace n ON n.nspname = 'ci_snapshot_' || pinned.family_id::text
                    JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = expected.table_name
                )
           END AS block_role_oids
    FROM pinned
    CROSS JOIN LATERAL (
        SELECT count(*) AS legacy_count
        FROM pg_catalog.pg_namespace n
        JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
        WHERE n.nspname = 'ci_snapshot_' || pinned.family_id::text
          AND c.relname = ANY(CAST(:legacy_names AS text[]))
          AND c.relkind = 'r' AND c.relpersistence = 'p'
    ) census
"""


@dataclass(frozen=True, slots=True)
class GenerationSnapshotBinding:
    """A fresh native pin and positive legacy-layout evidence from the same query.

    False leaves the physical layout unresolved; it never authorizes a fallback.
    """

    family_id: int | None
    legacy_snapshot: bool
    block_role_oids: tuple[tuple[int, int], ...] = ()
    _session: object = field(default=None, repr=False, compare=False)
    _transactions: tuple[object | None, ...] | None = field(default=None, repr=False, compare=False)

    async def checked_block_oids(self, session):
        """Require this receipt's live pin before its OIDs can authorize catalog reads."""
        if _snapshot_family_id(self.family_id) is None or self.legacy_snapshot is not False:
            raise CustomImportReadUnavailableError("generation snapshot block evidence is invalid")
        if (
            self._session is not session
            or type(self._transactions) is not tuple
            or len(self._transactions) != 4
            or not _is_same_read_transaction(self._transactions, await _read_transaction_identity(session))
        ):
            raise CustomImportReadUnavailableError("generation snapshot pin transaction differs")
        if type(self.block_role_oids) is not tuple or any(type(row) is not tuple for row in self.block_role_oids):
            raise CustomImportReadUnavailableError("generation snapshot role evidence is invalid")
        return dict(_checked_block_role_oids(self.block_role_oids))


async def resolve_generation_snapshot(session: AsyncSession, pinned_target: PinnedReadTarget) -> int | None:
    """Resolve a sealed generation through the protected, OID-checked registry.

    Only an explicit NULL permits canonical legacy storage. Missing functions,
    invalid bindings, or SQL errors must never select another storage family.
    """

    result = await _resolve_snapshot_result(session, pinned_target, include_layout=False)
    return _snapshot_family_id(result.scalar_one())


async def resolve_generation_snapshot_binding(
    session: AsyncSession, pinned_target: PinnedReadTarget
) -> GenerationSnapshotBinding:
    """Pin once, then identify only the complete original fifteen-role layout.

    Native resolution validates relation custody before and after its leaf locks.
    Its block-layout branch forbids extra data relations, so a complete legacy
    census cannot identify a block family. Landing/origin tables and indexes do
    not affect this census. All other layouts still need full block resolution.
    """

    # Connection acquisition starts a fresh caller-owned transaction when needed.
    await session.connection()
    transactions = await _read_transaction_identity(session)
    if transactions is None:
        raise CustomImportReadUnavailableError("generation snapshot pin requires an active transaction")
    generation_result = await _resolve_snapshot_result(session, pinned_target, include_layout=True)
    family_id, legacy_snapshot, role_oids = generation_result.one()
    family_id = _snapshot_family_id(family_id)
    if type(legacy_snapshot) is not bool or (family_id is None and legacy_snapshot):
        raise CustomImportReadUnavailableError("generation snapshot layout evidence is invalid")
    if family_id is None or legacy_snapshot:
        if type(role_oids) is not list or role_oids:
            raise CustomImportReadUnavailableError("generation snapshot role evidence is invalid")
        block_role_oids = ()
    else:
        block_role_oids = _checked_block_role_oids(role_oids)
    if not _is_same_read_transaction(transactions, await _read_transaction_identity(session)):
        raise CustomImportReadUnavailableError("generation snapshot pin transaction differs")
    return GenerationSnapshotBinding(family_id, legacy_snapshot, block_role_oids, session, transactions)


def _checked_block_role_oids(rows):
    from process.custom_import.family_block_layout import ROLE_SPECS

    if (
        type(rows) not in (list, tuple)
        or len(rows) != len(ROLE_SPECS)
        or any(
            type(row) not in (list, tuple)
            or len(row) != 2
            or any(type(value) is not int or value <= 0 for value in row)
            for row in rows
        )
        or {slot for slot, _ in rows} != {slot for slot, _ in ROLE_SPECS.values()}
        or len({oid for _, oid in rows}) != len(rows)
    ):
        raise CustomImportReadUnavailableError("generation snapshot role evidence is invalid")
    return tuple(sorted(tuple(row) for row in rows))


def _snapshot_family_id(family_id):
    if family_id is not None and (type(family_id) is not int or not 0 < family_id < 2**63):
        raise CustomImportReadUnavailableError("generation snapshot identity is invalid")
    return family_id


async def _resolve_snapshot_result(session, pinned_target, *, include_layout):
    connection = await session.connection()
    model_schema = CustomImportGeneration.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = schema_map.get(model_schema, model_schema)
    if not schema:
        raise CustomImportReadUnavailableError("generation snapshot resolver requires an explicit model schema")
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    invocation = (
        f"{quoted}.resolve_custom_import_generation_snapshot("
        "CAST(:generation_id AS bigint), CAST(:dataset_id AS bigint), "
        "CAST(:definition_revision_id AS bigint), CAST(:schema_revision_id AS bigint))"
    )
    statement = f"SELECT {invocation}"
    parameters_by_name = {
        "generation_id": pinned_target.generation_id,
        "dataset_id": pinned_target.dataset_id,
        "definition_revision_id": pinned_target.definition_revision_id,
        "schema_revision_id": pinned_target.schema_revision_id,
    }
    if include_layout:
        from process.custom_import.family_block_layout import ROLE_SPECS

        parameters_by_name["legacy_names"] = [model.__tablename__ for model in SNAPSHOT_MODELS]
        parameters_by_name["block_slots"] = [slot for slot, _ in ROLE_SPECS.values()]
        parameters_by_name["block_names"] = [name for _, name in ROLE_SPECS.values()]
        statement = _PINNED_LAYOUT_SQL.format(invocation=invocation)
    try:
        return await session.execute(text(statement), parameters_by_name)
    except DBAPIError as error:
        if _is_snapshot_error(error, "55000", "custom_import_snapshot_retired"):
            raise CustomImportReadUnavailableError("pinned generation storage is retired") from error
        raise


def _is_snapshot_error(error: DBAPIError, code: str, message: str) -> bool:
    return database_sqlstate(error) == code and any(
        _static_attribute(cause, "message") == message for cause in _failure_chain(error)
    )


async def _read_transaction_identity(session) -> tuple[object | None, ...] | None:
    """Include the connection root so externally ended transactions cannot reuse metadata."""

    if not all(
        callable(getattr(session, name, None)) for name in ("get_transaction", "get_nested_transaction", "connection")
    ):
        return None
    transaction = session.get_transaction()
    if transaction is None or not transaction.is_active:
        return None
    connection = await session.connection()
    if not all(callable(getattr(connection, name, None)) for name in ("get_transaction", "get_nested_transaction")):
        return None
    transactions = (
        transaction,
        session.get_nested_transaction(),
        connection.get_transaction(),
        connection.get_nested_transaction(),
    )
    if transactions[2] is None or any(value is not None and not value.is_active for value in transactions):
        return None
    return transactions


def _is_same_read_transaction(first, second) -> bool:
    return (
        first is not None
        and second is not None
        and all(left is right for left, right in zip(first, second, strict=True))
    )


async def verify_generation_storage(session: AsyncSession, generation) -> None:
    """Pin sealed storage before accepting a retained seal or generation status."""

    await resolve_generation_snapshot(
        session,
        PinnedReadTarget(
            dataset_id=generation.dataset_id,
            generation_id=generation.generation_id,
            definition_revision_id=generation.definition_revision_id,
            schema_revision_id=generation.schema_revision_id,
            profile_id="default",
        ),
    )


def verified_definition(
    definition_row: CustomImportDefinitionRevision,
    schema_row: CustomImportSchemaRevision,
) -> CustomImportDefinition:
    """Return a definition only when both definition and schema identities match."""

    try:
        definition = CustomImportDefinition.from_json(definition_row.canonical_definition)
        expected_digest = bytes.fromhex(definition.digest)
        expected_schema_digest = bytes.fromhex(definition.schema_digest)
    except ValueError, TypeError:
        raise CustomImportReadUnavailableError("persisted definition is not a valid v1 read contract") from None
    if (
        definition_row.contract_version != "custom-import/v1"
        or definition.canonical != definition_row.canonical_definition
        or not hmac.compare_digest(bytes(definition_row.definition_sha256), expected_digest)
        or definition.definition_revision != definition_row.revision_number
        or definition.schema_revision != schema_row.revision_number
        or definition.schema_canonical != schema_row.canonical_schema
        or not hmac.compare_digest(bytes(schema_row.schema_sha256), expected_schema_digest)
    ):
        raise CustomImportReadUnavailableError("persisted definition identity is invalid")
    return definition


async def verify_published_generation(session: AsyncSession, pinned_target: PinnedReadTarget) -> None:
    """Require an exact sealed generation with a canonical finality event."""

    generation_id = (
        await session.execute(
            select(CustomImportGenerationSeal.generation_id)
            .select_from(CustomImportGeneration)
            .join(
                CustomImportGenerationSeal,
                and_(
                    CustomImportGenerationSeal.generation_id == CustomImportGeneration.generation_id,
                    CustomImportGenerationSeal.dataset_id == CustomImportGeneration.dataset_id,
                    CustomImportGenerationSeal.definition_revision_id == CustomImportGeneration.definition_revision_id,
                    CustomImportGenerationSeal.schema_revision_id == CustomImportGeneration.schema_revision_id,
                ),
            )
            .where(
                CustomImportGeneration.generation_id == pinned_target.generation_id,
                CustomImportGeneration.dataset_id == pinned_target.dataset_id,
                CustomImportGeneration.definition_revision_id == pinned_target.definition_revision_id,
                CustomImportGeneration.schema_revision_id == pinned_target.schema_revision_id,
                CustomImportGenerationSeal.seal_contract == "custom-import-generation-seal/v1",
            )
        )
    ).scalar_one_or_none()
    if generation_id != pinned_target.generation_id:
        raise CustomImportReadUnavailableError("pinned generation is not eligible for extension reads")
    event = (
        (
            await session.execute(
                select(CustomImportPublicationEvent)
                .where(
                    CustomImportPublicationEvent.dataset_id == pinned_target.dataset_id,
                    CustomImportPublicationEvent.definition_revision_id == pinned_target.definition_revision_id,
                    CustomImportPublicationEvent.schema_revision_id == pinned_target.schema_revision_id,
                    CustomImportPublicationEvent.to_generation_id == pinned_target.generation_id,
                    CustomImportPublicationEvent.finality_contract == FINALITY_EVENT_CONTRACT,
                )
                .order_by(CustomImportPublicationEvent.publication_event_id)
                .limit(1)
            )
        )
        .scalars()
        .first()
    )
    if event is None:
        raise CustomImportReadUnavailableError("pinned generation is not eligible for extension reads")
    try:
        verify_publication_event_material(event)
    except PublicationConflict:
        raise CustomImportReadUnavailableError("pinned generation is not eligible for extension reads") from None
