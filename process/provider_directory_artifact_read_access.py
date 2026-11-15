# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve incumbent reads while private derived stages enter a common cut."""

from dataclasses import dataclass

from db.plan_release_storage import storage_relation


async def preserve_artifact_read_access(capability, stages):
    """Preserve actual reader grants for the declared artifact names using native publication."""
    from process.native_publication_read_access import preserve_publication_reads

    for stage in stages:
        if stage.schema != capability.policy.schema:
            raise RuntimeError("provider_directory_artifact_read_access_schema_changed")
        await preserve_publication_reads(
            capability.connection, schema=stage.schema, live_table=stage.target_relation, stage_table=stage.stage_table
        )


@dataclass(frozen=True)
class _OrdinaryReadAccess:
    """Portable logical names whose existing native read grants must survive a swap."""

    schema: str
    live_table: str
    stage_table: str


async def _ordinary_access(database, schema, live_table, stage_table, *, exclusive=False):
    """Use the actual native transaction and finite declared names without identity attestation."""
    from process.reference_family_archive import _native_model_copy_driver

    storage_relation(schema, live_table)
    storage_relation(schema, stage_table)
    if live_table == stage_table:
        raise RuntimeError("ordinary_address_read_access_relations_changed")
    binding = database._transaction_binding()
    if binding is None or not binding.session.in_transaction():
        raise RuntimeError("ordinary_address_read_access_transaction_required")
    return await _native_model_copy_driver(binding.session, ("execute", "fetch", "is_in_transaction"))


async def prepare_ordinary_read_access(database, schema, swaps):
    """Preserve reads for the genuine ordinary Address swap inventory."""
    return await _prepare_read_relations(
        database, schema, ((swap.live_cls.__main_table__, swap.stage_cls.__tablename__) for swap in swaps)
    )


async def prepare_ordinary_artifact_reads(database, schema, stages):
    """Preserve reads for exact ordinary prepared artifacts in their original schema."""
    stages = tuple(stages)
    if any(stage.schema != schema for stage in stages):
        raise RuntimeError("ordinary_address_read_access_schema_changed")
    return await _prepare_read_relations(
        database, schema, ((stage.target_relation, stage.stage_table) for stage in stages)
    )


async def _prepare_read_relations(database, schema, relation_pairs):
    """Replay only existing reader grants using the common named native publication path."""
    from process.native_publication_read_access import preserve_publication_reads

    selections = []
    for live, stage in relation_pairs:
        driver = await _ordinary_access(database, schema, live, stage)
        await preserve_publication_reads(driver, schema=schema, live_table=live, stage_table=stage)
        selections.append(_OrdinaryReadAccess(schema, live, stage))
    return tuple(selections)


async def recheck_ordinary_read_access(database, proofs):
    """Refresh actual read grants by name in the current native publication transaction."""
    from process.native_publication_read_access import preserve_publication_reads

    for selection in proofs:
        if type(selection) is not _OrdinaryReadAccess:
            raise RuntimeError("ordinary_address_read_access_names_required")
        driver = await _ordinary_access(database, selection.schema, selection.live_table, selection.stage_table)
        await preserve_publication_reads(
            driver, schema=selection.schema, live_table=selection.live_table, stage_table=selection.stage_table
        )
