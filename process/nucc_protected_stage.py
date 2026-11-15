# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Isolated named NUCC heaps, bounded COPY and closed model validation."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text

from process import nucc_snapshot_partition as partition
from process import reference_family_archive as native

CONTRACT = "nucc-protected-stage.v3"
MAX_BATCH_ROWS = 5000


def _require(condition, reason):
    if not condition:
        raise native.ReferenceFamilyArchiveError("NUCC protected stage " + reason)


def _relation(ownership):
    _require(
        isinstance(ownership, native.ReferenceFamilyStageOwnership)
        and ownership.importer_id == "nucc"
        and ownership.sequence_names == ()
        and ownership.auxiliary_oid is None
        and ownership.schema_name == native.reference_family_stage_schema(ownership.dataset_id)
        and ownership.relation_names == (partition.CHILD_TABLE,),
        "ownership differs",
    )
    return partition.relation_sql((ownership.schema_name, partition.CHILD_TABLE))


async def _source_actor(publisher_session, source_session, legacy_owner=None):
    from process.nucc_protected_control import _source_transaction

    native._require_transaction(publisher_session)
    return await _source_transaction(source_session)


async def precreate_nucc_stage(session, *, dataset_id, source_session=None):
    """Create empty isolated heaps and grant only configured Source candidate INSERT."""
    native._require_transaction(session)
    _require(type(dataset_id) is UUID, "dataset differs")
    async with session.begin_nested():
        await session.connection()
        return await _precreate_nucc_stage(session, dataset_id, source_session)


async def _precreate_nucc_stage(session, dataset_id, source_session, legacy_owner=None):
    spec = native.reference_family_spec("nucc")
    schema = native.reference_family_stage_schema(dataset_id)
    await native._create_model_family(session, spec, schema, create_indexes=False, ordinary_heaps=True)
    ownership = await native.capture_model_family_stage_ownership(session, spec, dataset_id)
    await native.seal_model_family_storage(session, ownership, require_empty=True, freeze_schema=False)
    if source_session is not None:
        actor = await _source_actor(session, source_session)
        role = native._quoted(native._schema_name(actor["role"]))
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO {role}'))
        await session.execute(text(f"GRANT INSERT ON {_relation(ownership)} TO {role}"))
    return ownership


async def copy_nucc_stage(session, ownership, batch_records):
    """Use the caller's native privileges for one bounded fixed-model COPY."""
    relation = _relation(ownership)
    native._require_transaction(session)
    _require(type(batch_records) in (list, tuple) and 0 < len(batch_records) <= MAX_BATCH_ROWS, "batch exceeds bound")
    await native.verify_reference_family_stage_ownership(session, ownership)
    model = native.reference_family_spec("nucc").model_types[0]

    async def copy():
        """Copy one bounded fixed-model batch using the active native driver."""
        return await native.native_copy_record_batch(
            session,
            model,
            schema_name=ownership.schema_name,
            table_name=partition.CHILD_TABLE,
            columns=tuple(model.__table__.columns.keys()),
            records=batch_records,
        )

    granted = await session.scalar(
        text("SELECT has_table_privilege(current_user,:relation,'INSERT')"), {"relation": relation}
    )
    if granted is True:
        return await copy()
    async with session.begin_nested():
        await session.connection()
        await session.execute(text(f"GRANT INSERT ON {relation} TO CURRENT_USER"))
        copied = await copy()
        await session.execute(text(f"REVOKE INSERT ON {relation} FROM CURRENT_USER"))
        return copied


async def _close_candidate_grants(session, ownership, legacy_owner=None, source_session=None):
    await session.execute(text(f"LOCK TABLE ONLY {_relation(ownership)} IN ACCESS SHARE MODE NOWAIT"))
    if source_session is not None:
        actor = await _source_actor(session, source_session)
        await session.execute(
            text(f"REVOKE INSERT ON {_relation(ownership)} FROM {native._quoted(native._schema_name(actor['role']))}")
        )
    await native.seal_model_family_storage(session, ownership, freeze_schema=False)


async def finish_nucc_stage(session, ownership, *, source_session=None):
    """Close candidate writes before indexes and complete content validation."""
    native._require_transaction(session)
    _relation(ownership)
    async with session.begin_nested():
        await session.connection()
        return await _finish_nucc_stage(session, ownership, source_session)


async def _finish_nucc_stage(session, ownership, source_session, legacy_owner=None):
    await native.verify_reference_family_stage_ownership(session, ownership)
    await _close_candidate_grants(session, ownership, source_session=source_session)
    await native.complete_reference_family_restore(session, ownership)
    await native.seal_model_family_storage(session, ownership)
    return await revalidate_nucc_stage(session, ownership)


async def revalidate_nucc_stage(session, ownership):
    """Recheck named model, indexed contents and logical fingerprints in a savepoint."""
    native._require_transaction(session)
    _relation(ownership)
    async with session.begin_nested():
        await session.connection()
        return await _revalidate_nucc_stage(session, ownership)


async def _revalidate_nucc_stage(session, ownership, legacy_owner=None):
    await native.verify_reference_family_stage_ownership(session, ownership)
    await session.execute(text(f"LOCK TABLE ONLY {_relation(ownership)} IN ACCESS SHARE MODE NOWAIT"))
    child_name = ownership.schema_name, partition.CHILD_TABLE
    child = await partition._capture_incoming(session, child_name)
    validation = await native.validate_nucc_reference_set(session, ownership=ownership)
    count, semantic = await native._projected_row_identity(session, *child_name, row_json_sql="to_jsonb(row_value)")
    _require(count == validation["row_count"], "content census changed")
    _require(await partition._capture_incoming(session, child_name) == child, "candidate layout changed")
    address = await native._relation_oid(session, *child_name)
    return {
        "contract": CONTRACT,
        "dataset_id": str(ownership.dataset_id),
        "child": child,
        "validation": validation,
        "semantic_sha256": semantic,
        "schema_sha256": await native._family_schema_identity(session, "nucc", address, *child_name),
    }
