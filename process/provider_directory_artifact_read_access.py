# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve incumbent reads while private derived stages enter a common cut."""

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
        await _replay_select(capability, stage, incumbent)


async def _replay_select(capability, stage, incumbent):
    target = storage_relation(stage.schema, stage.stage_table)
    for grant in incumbent.grants:
        if grant.privilege_type != "SELECT" or grant.grantee_name == incumbent.owner_name:
            continue
        principal = "PUBLIC" if grant.grantee_name is None else storage_identifier(grant.grantee_name)
        column = "" if grant.column_name is None else f" ({storage_identifier(grant.column_name)})"
        option = " WITH GRANT OPTION" if grant.is_grantable else ""
        await capability.connection.execute(f"GRANT SELECT{column} ON TABLE {target} TO {principal}{option}")
