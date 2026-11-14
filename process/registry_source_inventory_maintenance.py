# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh maintenance reads of complete named source inventory data."""

from uuid import UUID

from process.network_membership_candidate_lifecycle import _control_namespace, _require_transaction
from process.registry_source_inventory_contract import RegistrySourceInventoryError
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from process.registry_source_inventory_store import (
    INVENTORY_TABLE,
    MAX_INVENTORY_PAGE_BYTES,
    SourceInventoryStoreReceipt,
    _fold_inventory,
    _relation,
)
from process.registry_source_recipe_store import _decode_document


async def _inventory_snapshot(connection, copy_target, control_schema):
    candidate = await connection.fetchrow(
        f"SELECT * FROM {_control_namespace(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
    )
    fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    if (
        candidate is None
        or any(str(candidate[field]) != getattr(copy_target, field) for field in fields)
        or candidate["state"] not in {"ready", "published"}
    ):
        raise RegistrySourceInventoryError("registry_source_inventory_scope_invalid")
    metadata = verify_source_recipe_metadata(candidate)
    if metadata.inventory is None:
        raise RegistrySourceInventoryError("registry_source_inventory_scope_invalid")
    return dict(candidate), metadata


async def read_sealed_source_inventory_for_maintenance(connection, copy_target, *, control_schema):
    """Fold complete inventory data in the caller's fresh read-committed transaction.

    The named read lock remains held for the caller's cleanup decision. Logical
    candidate scope and complete inventory metadata must match before and after
    the bounded fold; publication state and its checkpoint may advance.
    """
    _require_transaction(connection, copy_target)
    if await connection.fetchval("SHOW transaction_isolation") != "read committed":
        raise RegistrySourceInventoryError("registry_source_inventory_snapshot_invalid")
    initial_candidate, initial_metadata = await _inventory_snapshot(connection, copy_target, control_schema)
    inventory = initial_metadata.inventory
    relation = _relation(copy_target)
    await connection.execute(f"LOCK TABLE ONLY {relation} IN ACCESS SHARE MODE NOWAIT")
    entries = []
    await _fold_inventory(connection, relation, inventory.root, 100, MAX_INVENTORY_PAGE_BYTES, entries.append)
    final_candidate, final_metadata = await _inventory_snapshot(connection, copy_target, control_schema)
    if final_metadata != initial_metadata or _decode_document(
        final_candidate["source_generations"]
    ) != _decode_document(initial_candidate["source_generations"]):
        raise RegistrySourceInventoryError("registry_source_inventory_root_changed")
    receipt = SourceInventoryStoreReceipt(copy_target.schema_name, INVENTORY_TABLE, inventory.root)
    return final_candidate, receipt, tuple(entries)
