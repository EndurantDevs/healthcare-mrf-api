# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact preparation-only exchange and separately authenticated terminal cleanup."""

from db.plan_release_storage import PIN_TABLE, storage_relation
from process.plan_release_preparation_pin_contract import PreparationPinHeap, PreparationPinReceipt, require
from process.plan_release_preparation_pins import (
    _attach,
    _authorize,
    _entry,
    _heap,
    _owner,
    _pin_options,
    _planned_inputs,
    _post_storage,
    _publisher,
    _roles,
    _unserved,
)


def _receipt(receipt):
    require(type(receipt) is PreparationPinReceipt, "pin_receipt_required")
    return receipt.validate()


async def _retained_rows_equal(connection, old_heap, new_heap, retained):
    fields = "serving_revision_id,snapshot_id,reason,created_at"
    old_rows = f"SELECT {fields} FROM ONLY {storage_relation(old_heap.schema, old_heap.name)} WHERE serving_revision_id=ANY($1::text[])"
    new_rows = f"SELECT {fields} FROM ONLY {storage_relation(new_heap.schema, new_heap.name)}"
    require(
        not await connection.fetchval(
            f"SELECT EXISTS(({old_rows} EXCEPT {new_rows}) UNION ALL ({new_rows} EXCEPT {old_rows}))", list(retained)
        ),
        "replacement_pin_rows_changed",
    )
    actual = await connection.fetchval(
        f"SELECT array_agg(DISTINCT serving_revision_id ORDER BY serving_revision_id) FROM ONLY {storage_relation(new_heap.schema, new_heap.name)}"
    )
    require(actual == list(retained), "replacement_revision_cohort_changed")


async def _detach(connection, storage_schema, heap):
    await connection.execute(
        f"ALTER TABLE {storage_relation(storage_schema, PIN_TABLE)} DETACH PARTITION {storage_relation(heap.schema, heap.name)}"
    )


async def exchange_preparation_pins(
    connection,
    storage_schema,
    receipt,
    replacement_heap,
    *,
    owner_role,
    publisher_role,
    reader_roles,
    **admission_options,
):
    """Exchange only settled future members; leave old detached storage for cleanup."""
    require(connection.is_in_transaction(), "caller_transaction_required")
    _receipt(receipt)
    require(type(replacement_heap) is PreparationPinHeap, "replacement_receipt_required")
    authority, control_reader_roles = _pin_options(admission_options)
    roles = _roles(owner_role, publisher_role, reader_roles, control_reader_roles)
    async with connection.transaction():
        root_oid, owner_oid = await _entry(connection, storage_schema, receipt.scope, authority, roles)
        before = await _authorize(
            connection, receipt.scope, authority, action="exchange", phase="before", observed=receipt
        )
        require(
            before.preparation_status == "sealed" and before.prepared_pin_heap == replacement_heap,
            "exchange_not_authorized",
        )
        retained = before.require_settled()
        require(retained and set(retained) < set(receipt.retained_revision_ids), "exchange_members_invalid")
        replacement_receipt = PreparationPinReceipt(
            receipt.scope, replacement_heap, root_oid, owner_oid, retained
        ).validate()
        is_replay = before.registered_receipt == replacement_receipt and receipt in before.detached_receipts
        require(is_replay or before.registered_receipt == receipt, "registered_pin_custody_changed")
        require((receipt.root_oid, receipt.owner_oid) == (root_oid, owner_oid), "root_identity_changed")
        await _owner(connection, roles)
        await _unserved(connection, storage_schema, receipt.scope.revision_ids)
        await _heap(
            connection, storage_schema, receipt.heap, roles, pin=True, parent_oid=None if is_replay else root_oid
        )
        await _heap(
            connection, storage_schema, replacement_heap, roles, pin=True, parent_oid=root_oid if is_replay else None
        )
        await _heap(connection, storage_schema, before.binding_heap, roles, pin=False)
        await _planned_inputs(connection, storage_schema, receipt.scope, before.binding_heap, receipt.heap)
        await _retained_rows_equal(connection, receipt.heap, replacement_heap, retained)
        if not is_replay:
            await _detach(connection, storage_schema, receipt.heap)
            await _attach(connection, storage_schema, replacement_heap, retained)
        await _publisher(connection, roles)
        after = await _authorize(
            connection, receipt.scope, authority, action="exchange", phase="after", observed=replacement_receipt
        )
        require(
            after.registered_receipt == replacement_receipt and receipt in after.detached_receipts,
            "exchange_not_persisted",
        )
        await _post_storage(connection, storage_schema, roles, root_oid, owner_oid)
        return replacement_receipt


def _abandoned(before):
    require(
        before.preparation_status == "abandoned"
        and before.writers_closed
        and before.terminal_intent_digest is not None,
        "explicit_terminal_abandonment_required",
    )
    before.require_settled()


async def retire_preparation_pins(
    connection,
    storage_schema,
    receipt,
    *,
    owner_role,
    publisher_role,
    reader_roles,
    authority=None,
    control_reader_roles=(),
):
    """Release only explicitly abandoned, terminal, never-served preparation pins."""
    _receipt(receipt)
    roles = _roles(owner_role, publisher_role, reader_roles, control_reader_roles)
    async with connection.transaction():
        root_oid, owner_oid = await _entry(connection, storage_schema, receipt.scope, authority, roles)
        before = await _authorize(
            connection, receipt.scope, authority, action="retire", phase="before", observed=receipt
        )
        _abandoned(before)
        is_replay = before.registered_receipt is None and receipt in before.detached_receipts
        require(is_replay or before.registered_receipt == receipt, "registered_pin_custody_changed")
        require((receipt.root_oid, receipt.owner_oid) == (root_oid, owner_oid), "root_identity_changed")
        await _owner(connection, roles)
        await _unserved(connection, storage_schema, receipt.scope.revision_ids)
        await _heap(
            connection, storage_schema, receipt.heap, roles, pin=True, parent_oid=None if is_replay else root_oid
        )
        if not is_replay:
            await _detach(connection, storage_schema, receipt.heap)
        await _publisher(connection, roles)
        after = await _authorize(connection, receipt.scope, authority, action="retire", phase="after", observed=receipt)
        require(after.registered_receipt is None and receipt in after.detached_receipts, "retirement_not_persisted")
        await _post_storage(connection, storage_schema, roles, root_oid, owner_oid)


async def _cleanup_coverage(connection, storage_schema, before, receipt, roles, root_oid):
    retained = before.require_settled()
    current = before.registered_receipt
    if current is None:
        _abandoned(before)
        await _unserved(connection, storage_schema, receipt.scope.revision_ids)
        return
    require(before.preparation_status == "published" and before.writers_closed, "serving_cut_not_completed")
    require(
        current.retained_revision_ids == retained
        and current.scope == receipt.scope
        and current.root_oid == root_oid
        and current.owner_oid == receipt.owner_oid,
        "replacement_custody_changed",
    )
    await _heap(connection, storage_schema, current.heap, roles, pin=True, parent_oid=root_oid)
    await _unserved(connection, storage_schema, tuple(sorted(set(receipt.scope.revision_ids) - set(retained))))
    await _retained_rows_equal(connection, receipt.heap, current.heap, retained)


async def cleanup_detached_preparation_pin_heap(
    connection,
    storage_schema,
    receipt,
    *,
    owner_role,
    publisher_role,
    reader_roles,
    authority=None,
    control_reader_roles=(),
):
    """Drop exactly one authenticated obsolete heap, never discover generic orphans."""
    _receipt(receipt)
    roles = _roles(owner_role, publisher_role, reader_roles, control_reader_roles)
    async with connection.transaction():
        root_oid, owner_oid = await _entry(connection, storage_schema, receipt.scope, authority, roles)
        before = await _authorize(
            connection, receipt.scope, authority, action="cleanup", phase="before", observed=receipt
        )
        require((receipt.root_oid, receipt.owner_oid) == (root_oid, owner_oid), "root_identity_changed")
        require(before.registered_receipt != receipt, "active_pin_custody")
        if receipt in before.cleaned_receipts:
            before.require_settled()
            require(
                before.writers_closed and before.preparation_status in {"published", "abandoned"},
                "cleanup_terminal_state_changed",
            )
            require(
                await connection.fetchval(
                    "SELECT to_regclass($1)", storage_relation(receipt.heap.schema, receipt.heap.name)
                )
                is None,
                "cleaned_heap_name_reused",
            )
            return
        require(receipt in before.detached_receipts, "detached_owner_missing")
        await _owner(connection, roles)
        await _heap(connection, storage_schema, receipt.heap, roles, pin=True)
        await _cleanup_coverage(connection, storage_schema, before, receipt, roles, root_oid)
        await connection.execute(f"DROP TABLE {storage_relation(receipt.heap.schema, receipt.heap.name)}")
        await _publisher(connection, roles)
        after = await _authorize(
            connection, receipt.scope, authority, action="cleanup", phase="after", observed=receipt
        )
        require(
            after.registered_receipt == before.registered_receipt
            and receipt not in after.detached_receipts
            and receipt in after.cleaned_receipts,
            "cleanup_not_persisted",
        )
        await _post_storage(connection, storage_schema, roles, root_oid, owner_oid)
