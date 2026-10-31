# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original-admin reads of exact unsealed CMS private outputs; no write authority.

The caller authenticates Source/profile/claim/allocation provenance and owns
deadlines, cancellation cleanup and the outer transaction. These observations
do not seal outputs, publish a candidate or establish protected CMS authority.
"""

import asyncio
from contextlib import asynccontextmanager

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_context as context
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_seal as seal


def _require(condition, code):
    if not condition:
        raise ValueError("cms_admin_workspace_" + code)


def _inputs(source_binding, prepared_inputs):
    """Accept only the canonical four-field input observation, without an actor claim."""
    _require(
        type(prepared_inputs) is dict
        and set(prepared_inputs) == {"source_storage", "source_policy", "prepared_policy", "frozen_inventory"},
        "inputs_changed",
    )
    source_policy = workspace._policy(prepared_inputs["source_policy"])
    prepared_policy = workspace._policy(prepared_inputs["prepared_policy"])
    expected = context._inputs(source_binding, source_policy, prepared_policy, prepared_inputs["frozen_inventory"])
    _require(expected == prepared_inputs, "inputs_changed")
    return expected, source_policy, prepared_policy


async def _observe_native(
    connection, source_binding, expected, handoff, expected_workspace, migration_role, *, reader=False
):
    """Recompute actual input custody, retained output identity and independent admin origin."""
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    namespace_role = await connection.fetchval(
        "SELECT rolname::text FROM pg_catalog.pg_roles WHERE oid=$1::oid", source_binding.namespace_owner_oid
    )
    _require(type(namespace_role) is str, "namespace_owner_missing")
    storage_identifier(namespace_role)
    source_policy = workspace._policy(expected["source_policy"])
    policy = workspace._policy(expected["prepared_policy"])
    coordinates = await context._observe(
        connection, source_binding, source_policy, policy, expected["frozen_inventory"]
    )
    native_inputs_by_field = {**expected, "native_coordinates": coordinates}
    retained, binding, receipts = workspace._retained_workspace(
        source_binding, native_inputs_by_field, handoff, expected_workspace
    )
    actor_reader = seal._reader_admin if reader else seal._admin
    actor = await actor_reader(
        connection, source_binding, native_inputs_by_field, retained, namespace_role, migration_role
    )
    _require(
        actor["current"] not in {*source_policy.control_runtime_roles, *policy.control_runtime_roles},
        "independent_original_admin_required",
    )
    return {**native_inputs_by_field, "origin": actor}, retained, binding, receipts


async def _pin_relations(connection, policy, expected, receipts):
    """Take nonrecursive read pins on exactly the authenticated input/output names."""
    names = [input_relation["relation_name"] for input_relation in expected["frozen_inventory"]["relations"]]
    names.extend(receipt["name"] for receipt in receipts.values())
    relations = sorted(storage_relation(policy.schema, name) for name in names)
    _require(len(set(relations)) == 19, "input_output_alias")
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + relation for relation in relations) + " IN SHARE MODE NOWAIT"
    )


@asynccontextmanager
async def open_admin_cms_workspace(
    database,
    session,
    connection,
    *,
    source_binding,
    prepared_inputs,
    handoff,
    expected_workspace,
    migration_role,
):
    """Pin and read eleven retained outputs and eight frozen inputs on the original admin.

    This performs SELECTs and one exact ONLY/SHARE NOWAIT lock. The original
    READ COMMITTED transaction stays caller-owned; locks last until its end.
    Existing Publisher factories remain unchanged, and the local workspace
    records the genuine admin actor rather than impersonating its Publisher.
    Use read_prepared_cms_outputs to recompute content under these pins.
    """
    storage_identifier(migration_role)
    _require(isinstance(database, context.Database) and isinstance(session, context.AsyncSession), "session_required")
    expected, _source_policy, policy = _inputs(source_binding, prepared_inputs)
    task, transaction = asyncio.current_task(), session.get_transaction()
    _require(task is not None and transaction is not None, "original_transaction_required")
    await context._physical(database, session, connection, task, transaction)

    async def observe():
        """Recheck actual coordinates and independent admin on the same borrowed transaction."""
        _require(_inputs(source_binding, prepared_inputs)[0] == expected, "inputs_changed")
        await context._physical(database, session, connection, task, transaction)
        observed = await _observe_native(
            connection, source_binding, expected, handoff, expected_workspace, migration_role
        )
        await context._physical(database, session, connection, task, transaction)
        _require(_inputs(source_binding, prepared_inputs)[0] == expected, "inputs_changed")
        return observed

    original, retained, binding, receipts = await observe()
    current = workspace.CMSPreparedWorkspace(database, session, connection, source_binding, original, binding, receipts)
    current._indexed, current._readonly = True, True
    try:
        await current.verify_current()
        await _pin_relations(connection, policy, expected, receipts)
        _require((await observe())[:2] == (original, retained), "native_evidence_changed")
        await current.verify_current()
        yield current
        _require(
            current._indexed is True
            and current._readonly is True
            and current._inputs == original
            and current.binding == retained,
            "workspace_changed",
        )
        _require((await observe())[:2] == (original, retained), "native_evidence_changed")
        await current.verify_current()
    finally:
        current._active = False
