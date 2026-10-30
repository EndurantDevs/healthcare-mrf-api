# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Locked, fresh maintenance reads of independently sealed inventory metadata."""

from process import network_candidate_custody as custody
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from process.registry_source_inventory_store import (
    INVENTORY_TABLE,
    MAX_INVENTORY_PAGE_BYTES,
    _fold_inventory,
    _physical_context,
)


async def _sealed_snapshot(connection, copy_target, control_schema):
    control = await custody._control(connection, control_schema)
    candidate = await custody._candidate(connection, copy_target, control_schema)
    seal = await custody._seal(connection, copy_target, control_schema)
    verified = await custody._verify(connection, copy_target, control_schema, control, candidate, seal)
    return control, candidate, seal, verified


async def read_sealed_source_inventory_for_maintenance(connection, copy_target, *, control_schema):
    """Fold immutable metadata under maintenance locks in a fresh RC transaction.

    Cleanup checks run before and after their exact exclusive target locks, so
    they must not reuse an older repeatable-read snapshot. Full native custody
    verifies and locks the independently sealed heaps before this fold. A second
    full verification checks fresh control, seal and normalized candidate evidence;
    publication and its matching checkpoint may advance without changing that evidence.
    The caller keeps the transaction and all locks until its cleanup decision ends.
    This grants neither Source admission nor API read authority.
    """
    custody._require(connection.is_in_transaction())
    custody._require(await connection.fetchval("SHOW transaction_isolation") == "read committed")
    initial_control, candidate, initial_seal, verified = await _sealed_snapshot(connection, copy_target, control_schema)
    inventory = verify_source_recipe_metadata(candidate).inventory
    custody._require(inventory is not None)
    inventory_oid = verified["receipt"]["closure"]["relation_oids"][INVENTORY_TABLE]
    relation = await _physical_context(connection, copy_target, inventory_oid, control_schema)
    entries = []
    await _fold_inventory(connection, relation, inventory.root, 100, MAX_INVENTORY_PAGE_BYTES, entries.append)
    final_control, final_candidate, final_seal, final_verified = await _sealed_snapshot(
        connection, copy_target, control_schema
    )
    custody._require(
        final_control == initial_control
        and dict(final_seal) == dict(initial_seal)
        and final_verified == verified
        and verify_source_recipe_metadata(final_candidate).inventory == inventory
    )
    return final_candidate, final_verified, tuple(entries)
