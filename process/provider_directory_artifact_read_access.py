# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve incumbent reads while private derived stages enter a common cut."""

import asyncio
from dataclasses import dataclass

from db.plan_release_storage import storage_identifier, storage_relation
from process.code_catalog_snapshot import _relation_access

_INHERITED_OWNER_USAGE_SQL = """WITH RECURSIVE inherited(oid) AS (
 SELECT current_user::regrole::oid UNION
 SELECT membership.roleid FROM pg_catalog.pg_auth_members membership
 JOIN inherited ON membership.member=inherited.oid WHERE membership.inherit_option)
 SELECT current_setting('transaction_isolation')='read committed'
 AND pg_catalog.pg_has_role(current_user,$1::oid,'USAGE')
 AND EXISTS(SELECT FROM inherited WHERE oid=$1::oid)"""


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
    """Capture plain same-owner heaps only under the caller's native publication locks."""
    from process.native_publication_read_access import _RELATIONS_SQL
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
    owner = by_name[stage_table]["relowner"]
    for entry in relations:
        if entry["oid"] is not None and (
            entry["relowner"] != owner
            or entry["relkind"] != "r"
            or entry["relpersistence"] != "p"
            or entry["relrowsecurity"]
            or entry["relforcerowsecurity"]
        ):
            raise RuntimeError("ordinary_address_read_access_owner_changed")
    if any(entry["oid"] is not None and not entry["owned"] for entry in relations):
        if await driver.fetchval(_INHERITED_OWNER_USAGE_SQL, owner) is not True:
            raise RuntimeError("ordinary_address_read_access_owner_changed")
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
    incumbent = await _relation_access(session, by_name[live_table]["oid"]) if by_name[live_table]["oid"] else None
    successor = await _relation_access(session, by_name[stage_table]["oid"])
    return session, driver, encoded(actor), encoded(relations), incumbent, successor


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
        if any(grant.grantee_name != successor.owner_name for grant in successor.grants):
            raise RuntimeError("ordinary_address_read_access_stage_not_private")
        if incumbent is not None:
            await _replay_select(driver, schema, stage, incumbent)
        after = await _ordinary_access(database, schema, live, stage)
        expected_grants = (
            set(successor.grants)
            | {
                grant
                for grant in incumbent.grants
                if grant.privilege_type == "SELECT" and grant.grantee_name != incumbent.owner_name
            }
            if incumbent is not None
            else set(successor.grants)
        )
        if (
            after[:4] != (session, driver, actor, relations)
            or after[4] != incumbent
            or set(after[5].grants) != expected_grants
        ):
            raise RuntimeError("ordinary_address_read_access_grants_changed")
        proofs.append(
            _OrdinaryReadAccess(
                session, asyncio.current_task(), actor, schema, live, stage, relations, incumbent, after[5]
            )
        )
    return tuple(proofs)


async def recheck_ordinary_read_access(database, proofs):
    """Check exact reads and physical writer again under final AccessExclusive locks."""
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
            or incumbent != proof.incumbent
            or successor != proof.successor
        ):
            raise RuntimeError("ordinary_address_read_access_grants_changed")
