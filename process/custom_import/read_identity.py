# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Logical persisted bindings for custom-import extension reads."""

from __future__ import annotations

import hmac
from dataclasses import dataclass

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

_PINNED_LAYOUT_SQL = """
    WITH pinned AS MATERIALIZED (SELECT {invocation} AS family_id),
         named AS (SELECT family_id, 'ci_snapshot_' || family_id::text AS schema_name FROM pinned)
    SELECT family_id,
           CASE WHEN family_id IS NULL THEN false ELSE
               pg_catalog.to_regclass(pg_catalog.format('%I.custom_import_root_revision', schema_name)) IS NOT NULL
               OR pg_catalog.to_regclass(
               pg_catalog.format('%I.%I', schema_name, CAST(:block_family_name AS text))
           ) IS NULL END AS legacy_snapshot,
           schema_name
    FROM named
"""


@dataclass(frozen=True, slots=True)
class GenerationSnapshotBinding:
    """One authorized logical snapshot and its schema-qualified storage layout."""

    family_id: int | None
    legacy_snapshot: bool
    schema_name: str | None = None

    def __post_init__(self):
        _snapshot_family_id(self.family_id)
        if type(self.legacy_snapshot) is not bool or (self.family_id is None and self.legacy_snapshot):
            raise CustomImportReadUnavailableError("generation snapshot layout is invalid")
        if self.family_id is None:
            if self.schema_name is not None:
                raise CustomImportReadUnavailableError("canonical storage cannot select a snapshot schema")
        elif type(self.schema_name) is not str or not self.schema_name or "\x00" in self.schema_name:
            raise CustomImportReadUnavailableError("generation snapshot schema name is invalid")


async def resolve_generation_snapshot(session: AsyncSession, pinned_target: PinnedReadTarget) -> int | None:
    """Resolve an authorized generation through the existing snapshot entrypoint.

    Only an explicit NULL permits canonical legacy storage. Missing functions,
    invalid bindings, or SQL errors must never select another storage family.
    """

    result = await _resolve_snapshot_result(session, pinned_target, include_layout=False)
    return _snapshot_family_id(result.scalar_one())


async def resolve_generation_snapshot_binding(
    session: AsyncSession, pinned_target: PinnedReadTarget
) -> GenerationSnapshotBinding:
    """Resolve a snapshot once and distinguish its named block-family relation."""
    result = await _resolve_snapshot_result(session, pinned_target, include_layout=True)
    family_id, legacy_snapshot, schema_name = result.one()
    return GenerationSnapshotBinding(family_id, legacy_snapshot, schema_name)


def _snapshot_family_id(family_id):
    if family_id is not None and (type(family_id) is not int or not 0 < family_id < 2**63):
        raise CustomImportReadUnavailableError("generation snapshot identity is invalid")
    return family_id


def named_layout_query(invocation, parameters):
    """Resolve the registered layout in the same statement as its generation lock."""
    from process.custom_import.family_block_layout import ROLE_SPECS

    return _PINNED_LAYOUT_SQL.format(invocation=invocation), parameters | {"block_family_name": ROLE_SPECS["family"][1]}


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
        statement, parameters_by_name = named_layout_query(invocation, parameters_by_name)
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


async def verify_generation_storage(session: AsyncSession, generation) -> GenerationSnapshotBinding:
    """Pin sealed storage before accepting a retained seal or generation status."""

    return await resolve_generation_snapshot_binding(
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
