# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read the complete named private payload group through PostgreSQL permissions."""

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_workspace as workspace

CONTRACT = "cms-private-payload-names-v2"


def _require(condition, code):
    if not condition:
        raise ValueError("cms_snapshot_custody_" + code)


async def _observe(connection, source_binding, retained, owner_role, migration_role):
    policy = workspace._policy(retained["prepared_policy"])
    names = [retained["tables"][root]["name"] for root in workspace.storage.RELATIONS]
    observed = await workspace.storage._capture(connection, policy, names)
    return {
        "contract": CONTRACT,
        "tables": {root: observed[retained["tables"][root]["name"]] for root in workspace.storage.RELATIONS},
    }


async def require_cms_payload_snapshot_custody(
    connection, *, source_binding, prepared_inputs, handoff, expected_workspace, owner_role, migration_role
):
    """Keep the historical entrypoint as a named read, without issuing write authority."""
    storage_identifier(owner_role)
    storage_identifier(migration_role)
    retained, _binding, _receipts = workspace._retained_workspace(
        source_binding, prepared_inputs, handoff, expected_workspace
    )
    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    names = sorted(
        storage_relation(retained["prepared_policy"]["schema"], retained["tables"][root]["name"])
        for root in workspace.storage.RELATIONS
    )
    async with workspace._savepoint(connection):
        await connection.execute(
            "LOCK TABLE " + ",".join("ONLY " + name for name in names) + " IN ACCESS SHARE MODE NOWAIT"
        )
        observed = await _observe(connection, source_binding, retained, owner_role, migration_role)
        current, _binding, _receipts = workspace._retained_workspace(
            source_binding, prepared_inputs, handoff, expected_workspace
        )
        _require(current == retained, "inventory_changed")
    return observed
