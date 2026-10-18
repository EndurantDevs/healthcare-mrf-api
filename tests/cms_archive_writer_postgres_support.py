# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Enroll only the canonical archive writer before native fixture signing."""

from dataclasses import replace

from process import provider_directory_cms_native_registration as registration
from process.plan_release_storage_provision import _close_relation_grants


async def _archive_inventory(connection, candidate):
    inventory = await registration._native_candidate_inventory(connection, candidate, candidate.target_schema)
    inventory["relation"] = {
        key: value for key, value in inventory["relation"].items() if key not in {"relowner", "relacl"}
    }
    inventory["columns"] = [
        {key: value for key, value in row.items() if key != "attacl"} for row in inventory["columns"]
    ]
    relation = f'"{candidate.stage_schema}"."{candidate.stage_table}"'
    inventory["rows"] = await connection.fetchval(
        f"SELECT COALESCE(jsonb_agg(to_jsonb(archived) ORDER BY address_key),'[]'::jsonb) FROM {relation} archived"
    )
    return inventory


async def enroll_archive_writer(connection, policy, candidate):
    """Preserve the exact archive while using the existing independent-admin custody cut."""
    assert (
        candidate.target_schema,
        candidate.target_table,
        candidate.stage_schema,
        candidate.stage_table,
    ) == (policy.schema, "address_archive_v2", policy.schema, "address_archive_v2")
    assert connection.is_in_transaction()
    actor_values = tuple(await connection.fetchrow("SELECT session_user::text,current_user::text,pg_backend_pid()"))
    assert actor_values[0] == actor_values[1] and actor_values[0] not in (
        policy.owner_role,
        policy.publisher_role,
        *policy.runtime_roles,
    )
    relation = f'"{policy.schema}"."address_archive_v2"'
    await connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
    before = await _archive_inventory(connection, candidate)
    await _close_relation_grants(
        connection,
        {
            "oid": candidate.relation_oid,
            "nspname": candidate.stage_schema,
            "relname": candidate.stage_table,
        },
        policy.owner_role,
        policy.runtime_roles,
    )
    owner_oid = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", policy.owner_role)
    current = await registration.capture_native_input_candidate(
        connection,
        ledger_schema=policy.schema,
        target_schema=policy.schema,
        target_table="address_archive_v2",
        stage_schema=policy.schema,
        stage_table="address_archive_v2",
    )
    assert current == replace(candidate, owner_oid=owner_oid)
    assert await _archive_inventory(connection, current) == before
    assert not await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_roles role WHERE role.rolname=ANY($2::text[]) AND "
        "(has_table_privilege(role.oid,$1::oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
        "OR has_any_column_privilege(role.oid,$1::oid,'INSERT,UPDATE,REFERENCES')))",
        candidate.relation_oid,
        list(policy.runtime_roles),
    )
    assert (
        tuple(await connection.fetchrow("SELECT session_user::text,current_user::text,pg_backend_pid()"))
        == actor_values
    )
    return current
