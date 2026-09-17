# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed persisted identity checks for custom-import extension reads."""

from __future__ import annotations

import hmac

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCurrentGeneration,
    CustomImportDefinitionRevision,
    CustomImportSchemaRevision,
)
from process.custom_import.definition import CustomImportDefinition
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


async def verify_current_generation(
    session: AsyncSession,
    target: PinnedReadTarget,
    expected_pointer_version: int,
) -> None:
    """Reject a read when publication moved during its database work."""

    pointer_version = await session.scalar(
        select(CustomImportCurrentGeneration.pointer_version).where(
            CustomImportCurrentGeneration.dataset_id == target.dataset_id,
            CustomImportCurrentGeneration.definition_revision_id == target.definition_revision_id,
            CustomImportCurrentGeneration.schema_revision_id == target.schema_revision_id,
            CustomImportCurrentGeneration.generation_id == target.generation_id,
        )
    )
    if type(pointer_version) is not int or pointer_version != expected_pointer_version:
        raise CustomImportReadUnavailableError("pinned generation changed during the extension read")
