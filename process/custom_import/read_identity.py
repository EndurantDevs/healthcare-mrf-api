# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed persisted identity checks for custom-import extension reads."""

from __future__ import annotations

import hmac

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportDefinitionRevision,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportPublicationEvent,
    CustomImportSchemaRevision,
)
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.publication import (
    FINALITY_EVENT_CONTRACT,
    PublicationConflict,
    verify_publication_event_material,
)
from process.custom_import.read_contracts import (
    CustomImportReadUnavailableError,
    PinnedReadTarget,
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
