# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bind verified query capability content into the existing finality receipts."""

import hashlib
from dataclasses import replace

from sqlalchemy import select

from db.models.custom_import import CustomImportDefinitionRevision, CustomImportGenerationSeal
from db.models.custom_import_query_projection import CustomImportQueryProjection
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.query_projection_binding import QueryProjectionUnavailable, lock_projection_tables
from process.custom_import.query_projection_evidence import completed_projection_evidence, read_seal_projection_evidence
from process.custom_import.query_projection_values import configured_fields


def bind_materialization(materialization, evidence):
    """Make legacy upgrades visible while identical projected outputs converge."""

    if evidence is None:
        return materialization
    document_by_name = {
        "contract": evidence.contract,
        "capability_sha256": evidence.capability_sha256.hex(),
        "projection_sha256": evidence.projection_sha256.hex(),
        "root_count": evidence.root_count,
        "child_count": evidence.child_count,
    }
    encoded = canonical_json(document_by_name).encode("utf-8")
    hashes_by_name = {
        name: hashlib.sha256(
            b"custom-import/v1\0query-projection-materialization/v1\0" + getattr(materialization, name) + encoded
        ).digest()
        for name in ("materialization_sha256", "effective_output_sha256")
    }
    return replace(materialization, **hashes_by_name, query_projection_evidence=evidence)


def _block_definition(binding, definition=None):
    from process.custom_import.family_block_binding import FamilyBlockBinding

    if type(binding) is not FamilyBlockBinding:
        raise QueryProjectionUnavailable("family block projection binding type differs")
    if definition is not None and definition.digest != binding.layout.definition.digest:
        raise QueryProjectionUnavailable("family block projection definition differs")
    return binding.layout.definition


async def load_generation_evidence(session, generation, definition=None, *, frozen=True, family_block_binding=None):
    """Accept legacy sealed snapshots; require complete evidence for new capabilities."""

    if family_block_binding is not None:
        definition = _block_definition(family_block_binding, definition)
    seal = await session.get(CustomImportGenerationSeal, generation.generation_id)
    if seal is not None and read_seal_projection_evidence(seal) is None:
        if family_block_binding is not None and configured_fields(family_block_binding.layout.definition):
            raise QueryProjectionUnavailable("family block projection completion evidence is missing")
        return None
    if definition is None:
        revision = await session.get(CustomImportDefinitionRevision, generation.definition_revision_id)
        if revision is None:
            raise QueryProjectionUnavailable("projection definition revision is missing")
        definition = CustomImportDefinition.from_json(revision.canonical_definition)
    if not configured_fields(definition):
        if seal is not None:
            raise QueryProjectionUnavailable("unconfigured projection seal is advertised")
        return None
    manifest = (
        await session.scalars(
            select(CustomImportQueryProjection)
            .where(CustomImportQueryProjection.generation_id == generation.generation_id)
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    if manifest is None:
        raise QueryProjectionUnavailable("required query projection manifest is missing")
    evidence = completed_projection_evidence(manifest, generation=generation, definition=definition, seal=seal)
    if family_block_binding is not None:
        from process.custom_import.family_block_binding import check_projection_manifest

        check_projection_manifest(family_block_binding, manifest)
        # The caller has just pinned this logged/frozen layout and every native index.
        return evidence
    await lock_projection_tables(session, manifest, definition, require_complete=frozen)
    return evidence


async def bind_generation_materialization(
    session, generation, materialization, definition=None, *, family_block_binding=None
):
    """Share one commitment across compact, exhaustive and legacy verification."""

    binding_by_name = {} if family_block_binding is None else {"family_block_binding": family_block_binding}
    if session.in_transaction():
        return bind_materialization(
            materialization,
            await load_generation_evidence(session, generation, definition, **binding_by_name),
        )
    async with session.begin():
        return bind_materialization(
            materialization,
            await load_generation_evidence(session, generation, definition, **binding_by_name),
        )


async def validate_sealed_projection(session, generation, seal, definition=None, *, family_block_binding=None):
    """Avoid an extra metadata lookup only for an explicitly legacy immutable seal."""

    if family_block_binding is not None:
        definition = _block_definition(family_block_binding, definition)
    if read_seal_projection_evidence(seal) is None:
        if family_block_binding is not None and configured_fields(family_block_binding.layout.definition):
            raise QueryProjectionUnavailable("family block projection completion evidence is missing")
        return None
    binding_by_name = {} if family_block_binding is None else {"family_block_binding": family_block_binding}
    return await load_generation_evidence(session, generation, definition, **binding_by_name)
