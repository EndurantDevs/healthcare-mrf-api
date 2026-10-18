# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owner-created NUCC heaps, bounded COPY and closed indexed candidate validation."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text

from process import nucc_snapshot_partition as partition
from process import reference_family_archive as native
from process.entity_address_snapshot_preparation import _seal_published_relation

CONTRACT = "nucc-protected-stage.v3"
MAX_BATCH_ROWS = 5000


def _require(condition, reason):
    if not condition:
        raise native.ReferenceFamilyArchiveError("NUCC protected stage " + reason)


def _relation(ownership):
    _require(
        isinstance(ownership, native.ReferenceFamilyStageOwnership)
        and ownership.importer_id == "nucc"
        and ownership.sequence_oids == ()
        and ownership.auxiliary_oid is None
        and ownership.schema_name == native.reference_family_stage_schema(ownership.dataset_id)
        and tuple(name for name, _oid in ownership.relation_oids) == (partition.CHILD_TABLE,),
        "ownership differs",
    )
    return f'"{ownership.schema_name}"."{partition.CHILD_TABLE}"'


async def _source_actor(publisher_session, source_session, owner_oid):
    from process.nucc_protected_control import _source_transaction

    actor_by_field = await _source_transaction(source_session)
    database_oid = await publisher_session.scalar(
        text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")
    )
    _require(
        actor_by_field["database_oid"] == database_oid and actor_by_field["owner_oid"] == owner_oid,
        "Source location differs",
    )
    return actor_by_field


async def precreate_nucc_stage(session, *, dataset_id, source_session=None):
    """Create isolated heaps without indexes; optionally grant the genuine Source actor only COPY INSERT."""
    native._require_transaction(session)
    _require(type(dataset_id) is UUID, "dataset differs")
    owner_oid = await native.protected_publisher_owner(session)
    async with session.begin_nested():
        await session.connection()
        return await _precreate_nucc_stage(session, dataset_id, source_session, owner_oid)


async def _precreate_nucc_stage(session, dataset_id, source_session, owner_oid):
    spec = native.reference_family_spec("nucc")
    schema = native.reference_family_stage_schema(dataset_id)
    await native._create_model_family(
        session,
        spec,
        schema,
        create_indexes=False,
        ordinary_heaps=True,
        protected_owner_oid=owner_oid,
    )
    ownership = await native.capture_model_family_stage_ownership(session, spec, dataset_id)
    await native.seal_model_family_storage(session, ownership, owner_oid, require_empty=True)
    if source_session is not None:
        actor = await _source_actor(session, source_session, owner_oid)
        role = await session.scalar(
            text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:actor"),
            {"actor": actor["actor_oid"]},
        )
        _require(type(role) is str, "Source role unavailable")
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO {role}'))
        await session.execute(text(f"GRANT INSERT ON {_relation(ownership)} TO {role}"))
    return ownership


async def copy_nucc_stage(session, ownership, batch_records):
    """COPY one bounded model batch; native privileges enforce the previously granted exact heap."""
    relation = _relation(ownership)
    native._require_transaction(session)
    _require(type(batch_records) in (list, tuple) and 0 < len(batch_records) <= MAX_BATCH_ROWS, "batch exceeds bound")
    await native.verify_reference_family_stage_ownership(session, ownership)
    owner_oid = await session.scalar(
        text("SELECT nspowner::bigint FROM pg_namespace WHERE nspname='hp_snapshot_retention'")
    )
    actor = await session.scalar(text("SELECT (SELECT oid FROM pg_roles WHERE rolname=current_user)::bigint"))
    original = await session.scalar(text("SELECT session_user=current_user"))
    _require(original is True, "original COPY actor required")
    is_publisher = await session.scalar(
        text("SELECT pg_has_role(CAST(:actor AS oid),CAST(:owner AS oid),'USAGE')"),
        {"actor": actor, "owner": owner_oid},
    )
    if is_publisher is True:
        _require(await native.protected_publisher_owner(session) == owner_oid, "Publisher differs")
    else:
        from process.nucc_protected_control import _source_transaction

        await _source_transaction(session)
        _require(
            await session.scalar(text(f"SELECT has_table_privilege(current_user,'{relation}','INSERT')")) is True,
            "COPY grant unavailable",
        )
    model = native.reference_family_spec("nucc").model_types[0]
    return await native.native_copy_record_batch(
        session,
        model,
        schema_name=ownership.schema_name,
        table_name=partition.CHILD_TABLE,
        columns=tuple(model.__table__.columns.keys()),
        records=batch_records,
    )


async def _close_candidate_grants(session, ownership, owner_oid, source_session):
    relation = _relation(ownership)
    oid = dict(ownership.relation_oids)[partition.CHILD_TABLE]
    entries = (
        await session.execute(
            text("""SELECT a.grantee::bigint,a.grantor::bigint,
 a.privilege_type,a.is_grantable FROM pg_class c,
 LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
 WHERE c.oid=:oid ORDER BY a.grantee,a.privilege_type"""),
            {"oid": oid},
        )
    ).all()
    source_actor = None if source_session is None else await _source_actor(session, source_session, owner_oid)
    publisher_oid = await session.scalar(text("SELECT (SELECT oid FROM pg_roles WHERE rolname=current_user)::bigint"))
    _require(type(publisher_oid) is int and 0 < publisher_oid != owner_oid, "Publisher identity differs")
    expected_acl_edges = {(owner_oid, owner_oid, right, False) for right in partition._OWNER_RIGHTS}
    expected_acl_edges.add((publisher_oid, owner_oid, "SELECT", False))
    if source_actor is not None:
        expected_acl_edges.add((source_actor["actor_oid"], owner_oid, "INSERT", False))
    _require(set(entries) == expected_acl_edges and len(entries) == len(expected_acl_edges), "candidate grants differ")
    await session.execute(text(f"LOCK TABLE ONLY {relation} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    if source_actor is not None:
        role = await session.scalar(
            text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:oid"), {"oid": source_actor["actor_oid"]}
        )
        _require(type(role) is str, "Source role unavailable")
        await session.execute(text(f"REVOKE INSERT ON {relation} FROM {role}"))
    await _seal_published_relation(session, oid, owner_oid)


async def finish_nucc_stage(session, ownership, *, source_session=None):
    """Close candidate writes, finish all model indexes, then validate the complete isolated set."""
    native._require_transaction(session)
    _relation(ownership)
    owner_oid = await native.protected_publisher_owner(session)
    async with session.begin_nested():
        await session.connection()
        return await _finish_nucc_stage(session, ownership, source_session, owner_oid)


async def _finish_nucc_stage(session, ownership, source_session, owner_oid):
    await native.verify_reference_family_stage_ownership(session, ownership)
    await _close_candidate_grants(session, ownership, owner_oid, source_session)
    await native.complete_reference_family_restore(session, ownership)
    return await revalidate_nucc_stage(session, ownership)


async def revalidate_nucc_stage(session, ownership):
    """Recheck a complete closed candidate across transactions without changing data or indexes."""
    native._require_transaction(session)
    _relation(ownership)
    owner_oid = await native.protected_publisher_owner(session)
    async with session.begin_nested():
        await session.connection()
        return await _revalidate_nucc_stage(session, ownership, owner_oid)


async def _revalidate_nucc_stage(session, ownership, owner_oid):
    await native.verify_reference_family_stage_ownership(session, ownership)
    oid = dict(ownership.relation_oids)[partition.CHILD_TABLE]
    await session.execute(text(f"LOCK TABLE ONLY {_relation(ownership)} IN SHARE MODE NOWAIT"))
    child = await partition._capture_incoming(session, oid, owner_oid)
    validation = await native.validate_nucc_reference_set(session, ownership=ownership)
    count, semantic = await native._projected_row_identity(
        session,
        ownership.schema_name,
        partition.CHILD_TABLE,
        row_json_sql="to_jsonb(row_value)",
    )
    _require(count == validation["row_count"], "content census changed")
    _require(await partition._capture_incoming(session, oid, owner_oid) == child, "candidate catalog changed")
    return {
        "contract": CONTRACT,
        "dataset_id": str(ownership.dataset_id),
        "child": child,
        "validation": validation,
        "semantic_sha256": semantic,
        "schema_sha256": await native._family_schema_identity(
            session,
            "nucc",
            oid,
            ownership.schema_name,
            partition.CHILD_TABLE,
        ),
    }
