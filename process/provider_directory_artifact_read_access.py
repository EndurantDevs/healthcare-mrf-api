# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve incumbent reads while private derived stages enter a common cut."""

import asyncio
from dataclasses import dataclass

from db.plan_release_storage import storage_identifier, storage_relation
from process.code_catalog_snapshot import _relation_access


async def preserve_artifact_read_access(capability, stages):
    """Replay only existing SELECT under the genuine owner's complete native locks."""
    for stage in stages:
        if stage.schema != capability.policy.schema or stage.build_fence is None:
            raise RuntimeError("provider_directory_artifact_read_access_fence_required")
        old_oid = await capability.connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", storage_relation(stage.schema, stage.target_relation)
        )
        new_oid = await capability.connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", storage_relation(stage.schema, stage.stage_table)
        )
        if old_oid is None or old_oid != stage.build_fence.target_oid or new_oid is None or old_oid == new_oid:
            raise RuntimeError("provider_directory_artifact_read_access_identity_changed")
        plain_tables = await capability.connection.fetchval(
            "SELECT count(*)=2 AND bool_and(c.relkind='r' AND c.relpersistence='p' "
            "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
            "AND NOT EXISTS(SELECT FROM pg_policy WHERE polrelid=c.oid)) "
            "FROM pg_class c WHERE c.oid=ANY($1::oid[])",
            [old_oid, new_oid],
        )
        if plain_tables is not True:
            raise RuntimeError("provider_directory_artifact_read_access_unsupported_relation")
        locked = await capability.connection.fetchval(
            "SELECT count(DISTINCT relation) FROM pg_locks WHERE pid=pg_backend_pid() "
            "AND locktype='relation' AND mode='AccessExclusiveLock' AND granted AND relation=ANY($1::oid[])",
            [old_oid, new_oid],
        )
        if locked != 2:
            raise RuntimeError("provider_directory_artifact_read_access_locks_required")
        incumbent = await _relation_access(capability.session, old_oid)
        successor = await _relation_access(capability.session, new_oid)
        if incumbent.owner_name != capability.policy.owner_role or successor.owner_name != capability.policy.owner_role:
            raise RuntimeError("provider_directory_artifact_read_access_owner_changed")
        if any(grant.grantee_name != successor.owner_name for grant in successor.grants):
            raise RuntimeError("provider_directory_artifact_read_access_stage_not_private")
        await _replay_select(capability.connection, stage.schema, stage.stage_table, incumbent)


async def _replay_select(connection, schema, stage_table, incumbent):
    target = storage_relation(schema, stage_table)
    for grant in incumbent.grants:
        if grant.privilege_type != "SELECT" or grant.grantee_name == incumbent.owner_name:
            continue
        principal = "PUBLIC" if grant.grantee_name is None else storage_identifier(grant.grantee_name)
        column = "" if grant.column_name is None else f" ({storage_identifier(grant.column_name)})"
        option = " WITH GRANT OPTION" if grant.is_grantable else ""
        await connection.execute(f"GRANT SELECT{column} ON TABLE {target} TO {principal}{option}")


@dataclass(frozen=True)
class _OrdinaryReadAccess:
    """Keep exact read replay bound to its original ordinary transaction and heaps."""

    session: object
    task: object
    actor_json: str
    schema: str
    live_table: str
    stage_table: str
    relations_json: str
    incumbent: object
    successor: object


async def _ordinary_access(database, schema, live_table, stage_table, *, exclusive=False):
    """Capture finite SELECT scope under the caller's native publication locks."""
    from process.native_publication_read_access import _RELATIONS_SQL, _named_layout
    from process.provider_directory_output_snapshot_catalog import encoded
    from process.provider_directory_serving_recovery_receipts import ordinary

    session, driver, actor = await ordinary(database)
    storage_relation(schema, live_table)
    storage_relation(schema, stage_table)
    if live_table == stage_table:
        raise RuntimeError("ordinary_address_read_access_relations_changed")
    relations = [dict(entry) for entry in await driver.fetch(_RELATIONS_SQL, schema, [live_table, stage_table])]
    by_name = {entry["name"]: entry for entry in relations}
    if len(relations) != 2 or set(by_name) != {live_table, stage_table} or by_name[stage_table]["oid"] is None:
        raise RuntimeError("ordinary_address_read_access_relations_changed")
    for entry in relations:
        if entry["oid"] is not None and (
            entry["relkind"] != "r"
            or entry["relpersistence"] != "p"
            or entry["relrowsecurity"]
            or entry["relforcerowsecurity"]
            or entry.get("relispartition", False)
            or entry.get("inherited", False)
        ):
            raise RuntimeError("ordinary_address_read_access_unsupported_relation")
    oids = [entry["oid"] for entry in relations if entry["oid"] is not None]
    modes = ["AccessExclusiveLock"] if exclusive else ["ShareLock", "AccessExclusiveLock"]
    count = await driver.fetchval(
        "SELECT count(DISTINCT relation) FROM pg_locks WHERE pid=pg_backend_pid() "
        "AND locktype='relation' AND mode=ANY($1::text[]) AND granted AND relation=ANY($2::oid[])",
        modes,
        oids,
    )
    if count != len(oids):
        raise RuntimeError("ordinary_address_read_access_locks_required")
    incumbent = (
        await _ordinary_select_access(driver, by_name[live_table]["oid"]) if by_name[live_table]["oid"] else None
    )
    successor = await _ordinary_select_access(driver, by_name[stage_table]["oid"])
    transaction_by_field = {key: actor_field for key, actor_field in actor.items() if key in {"backend", "xid"}}
    return session, driver, encoded(transaction_by_field), encoded(_named_layout(relations)), incumbent, successor


async def _ordinary_select_access(driver, relation_oid):
    from process.code_catalog_snapshot import _AccessGrant, _RelationAccess
    from process.native_publication_read_access import _READS_SQL

    grants = []
    for entry in await driver.fetch(_READS_SQL, relation_oid):
        if entry["privilege_type"] != "SELECT":
            continue
        principal = None if entry["grantee"] == 0 else entry["rolname"]
        if principal is not None:
            storage_identifier(principal)
        elif entry["grantee"] != 0:
            raise RuntimeError("ordinary_address_read_access_grant_changed")
        column = entry["column_name"]
        if column is not None:
            storage_identifier(column)
        grants.append(_AccessGrant(column, principal, "SELECT", bool(entry["is_grantable"]), ""))
    return _RelationAccess("", tuple(grants))


def _select_scope(*accesses):
    """Keep named reads and their grant options, without owner or grantor admission."""
    grants_by_principal = {}
    for access in accesses:
        for grant in () if access is None else access.grants:
            if grant.privilege_type == "SELECT":
                key = grant.column_name, grant.grantee_name
                grants_by_principal[key] = grants_by_principal.get(key, False) or grant.is_grantable
    return frozenset((*key, grantable) for key, grantable in grants_by_principal.items())


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
    """Replay incumbent SELECT inside the final transaction, before any long stage hash."""
    proofs = []
    for live, stage in relation_pairs:
        session, driver, actor, relations, incumbent, successor = await _ordinary_access(database, schema, live, stage)
        if incumbent is not None:
            await _replay_select(driver, schema, stage, incumbent)
        after = await _ordinary_access(database, schema, live, stage)
        expected_grants = _select_scope(successor, incumbent)
        if (
            after[:4] != (session, driver, actor, relations)
            or _select_scope(after[4]) != _select_scope(incumbent)
            or _select_scope(after[5]) != expected_grants
        ):
            raise RuntimeError("ordinary_address_read_access_grants_changed")
        proofs.append(
            _OrdinaryReadAccess(
                session, asyncio.current_task(), actor, schema, live, stage, relations, incumbent, after[5]
            )
        )
    return tuple(proofs)


async def recheck_ordinary_read_access(database, proofs):
    """Check selected reads and original transaction again under final native locks."""
    for proof in proofs:
        if type(proof) is not _OrdinaryReadAccess or proof.task is not asyncio.current_task():
            raise RuntimeError("ordinary_address_read_access_writer_changed")
        session, _driver, actor, relations, incumbent, successor = await _ordinary_access(
            database, proof.schema, proof.live_table, proof.stage_table, exclusive=True
        )
        if (
            session is not proof.session
            or actor != proof.actor_json
            or relations != proof.relations_json
            or _select_scope(incumbent) != _select_scope(proof.incumbent)
            or _select_scope(successor) != _select_scope(proof.successor)
        ):
            raise RuntimeError("ordinary_address_read_access_grants_changed")
