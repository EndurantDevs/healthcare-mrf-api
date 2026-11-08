# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original-admin closure of exactly six prepared private payload writers.

The CMS operator must authenticate its protected profile/claim and complete
content/callable/role custody separately. This primitive neither commits nor
attaches tables, records publication or supplies CMS activation authority.
"""

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_custody as custody

_DEPENDENCIES_SQL = """SELECT NOT EXISTS(SELECT FROM pg_catalog.pg_constraint k WHERE
 (k.conrelid=ANY($1::oid[]) AND (k.contype NOT IN ('p','u','n','c') OR NOT k.convalidated
  OR NOT coalesce((to_jsonb(k)->>'conenforced')::boolean,true)))
 OR (k.confrelid=ANY($1::oid[]) AND k.contype='f'))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_rewrite r ON r.oid=d.objid
 WHERE d.classid='pg_catalog.pg_rewrite'::regclass AND d.refclassid='pg_catalog.pg_class'::regclass
 AND d.refobjid=ANY($1::oid[]))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_class c ON c.oid=d.objid
 WHERE d.classid='pg_catalog.pg_class'::regclass AND d.refclassid='pg_catalog.pg_class'::regclass
 AND d.refobjid=ANY($1::oid[]) AND c.relkind='S' AND d.deptype IN ('a','i'))"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_snapshot_seal_" + code)


async def _original_admin(connection, source_binding, inputs, retained, owner_role, migration_role, *, isolations):
    actor_by_field = dict(await connection.fetchrow(workspace.context._ORIGIN_SQL))
    excluded_roles = {owner_role, migration_role}
    for policy in (inputs["source_policy"], retained["prepared_policy"]):
        excluded_roles.update((policy["owner_role"], policy["publisher_role"], *policy["runtime_roles"]))
    _require(
        actor_by_field["session"] == actor_by_field["current"]
        and actor_by_field["current"] not in excluded_roles
        and await connection.fetchval("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=session_user") is True
        and actor_by_field["backend"] == connection.get_server_pid()
        and actor_by_field["database_oid"] == source_binding.database_oid
        and actor_by_field["isolation"] in isolations
        and actor_by_field["readonly"] == "off"
        and actor_by_field["replication_role"] == "origin"
        and bool(actor_by_field["transaction"]),
        "independent_original_admin_required",
    )
    return actor_by_field


async def _admin(connection, source_binding, inputs, retained, owner_role, migration_role):
    return await _original_admin(
        connection, source_binding, inputs, retained, owner_role, migration_role, isolations=("read committed",)
    )


async def _reader_admin(connection, source_binding, inputs, retained, owner_role, migration_role):
    return await _original_admin(
        connection,
        source_binding,
        inputs,
        retained,
        owner_role,
        migration_role,
        isolations=("read committed", "repeatable read"),
    )


async def _close(connection, policy, retained, owner_role):
    owner = storage_identifier(owner_role)
    for root in workspace.storage.RELATIONS:
        relation = storage_relation(policy.schema, retained["tables"][root]["name"])
        await connection.execute(f"ALTER TABLE {relation} OWNER TO {owner}")
    await connection.execute("SET LOCAL ROLE " + owner)
    for root in workspace.storage.RELATIONS:
        name = retained["tables"][root]["name"]
        await workspace.storage._close_grants(connection, policy, name)
        await connection.execute(
            "GRANT SELECT ON "
            + storage_relation(policy.schema, name)
            + " TO "
            + storage_identifier(policy.publisher_role)
        )
    await connection.execute("RESET ROLE")


async def seal_prepared_cms_payload_snapshots(
    connection, *, source_binding, prepared_inputs, handoff, expected_workspace, owner_role, owner_oid, migration_role
):
    """Close exact private outputs under the caller's CMS-specific admin authority.

    All six heaps are locked NOWAIT before the first ownership change. Refusal
    or cancellation rolls back this savepoint; the caller owns outer commit and
    protected seal accounting. Existing names/OIDs/layouts must match: this never
    repairs drift, adopts frozen Source tables or transfers identity outputs.
    """
    storage_identifier(owner_role)
    storage_identifier(migration_role)
    _require(type(owner_oid) is int and 0 < owner_oid < 2**32, "owner_required")
    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    inputs = workspace._copy(prepared_inputs)
    retained, _binding, _receipts = workspace._retained_workspace(source_binding, inputs, handoff, expected_workspace)
    actor = await _admin(connection, source_binding, inputs, retained, owner_role, migration_role)
    policy = workspace._policy(retained["prepared_policy"])
    names = [retained["tables"][root]["name"] for root in workspace.storage.RELATIONS]
    relations = sorted(storage_relation(policy.schema, name) for name in names)
    async with workspace._savepoint(connection):
        await connection.execute(
            "LOCK TABLE " + ",".join("ONLY " + name for name in relations) + " IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
        await custody._principals(connection, source_binding, retained, owner_role, owner_oid, migration_role)
        original = await workspace.storage._capture(connection, policy, names)
        _require(
            all(
                original[retained["tables"][root]["name"]] == retained["tables"][root]
                for root in workspace.storage.RELATIONS
            ),
            "layout_changed",
        )
        _require(
            await connection.fetchval(_DEPENDENCIES_SQL, [original[name]["oid"] for name in names]) is True,
            "unsupported_dependency",
        )
        await _close(connection, policy, retained, owner_role)
        evidence = await custody._observe(connection, source_binding, retained, owner_role, owner_oid, migration_role)
        _require(
            await _admin(connection, source_binding, inputs, retained, owner_role, migration_role) == actor,
            "transaction_changed",
        )
        current, _binding, _receipts = workspace._retained_workspace(
            source_binding, prepared_inputs, handoff, expected_workspace
        )
        _require(current == retained and prepared_inputs == inputs, "inputs_changed")
    return evidence
