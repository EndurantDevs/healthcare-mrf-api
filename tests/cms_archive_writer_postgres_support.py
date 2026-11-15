# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Enroll only the canonical archive writer before native fixture signing."""

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
    relation = f'"{policy.schema}"."address_archive_v2"'
    await connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
    before = await _archive_inventory(connection, candidate)
    await _close_relation_grants(
        connection,
        {
            "oid": await connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation),
            "nspname": candidate.stage_schema,
            "relname": candidate.stage_table,
        },
        policy.owner_role,
        policy.runtime_roles,
    )
    current = await registration.capture_native_input_candidate(
        connection,
        ledger_schema=policy.schema,
        target_schema=policy.schema,
        target_table="address_archive_v2",
        stage_schema=policy.schema,
        stage_table="address_archive_v2",
    )
    assert current == candidate
    assert await _archive_inventory(connection, current) == before
    return current
