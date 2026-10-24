# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact preparation cleanup and authenticated operations on retained serving history."""

from dataclasses import asdict, dataclass

from db.plan_release_storage import BINDING_TABLE, PIN_TABLE, REVISION_TABLE, storage_relation
from process.plan_release_preparation_pin_catalog import capture_preparation_pin_heap
from process.plan_release_preparation_pin_contract import (
    PlanReleaseHistoryAuthority,
    PlanReleasePublication,
    PreparationPinHeap,
    PreparationPinReceipt,
    canonical_json,
    digest,
    require,
)
from process.plan_release_preparation_pins import (
    _attach,
    _authorize,
    _entry,
    _heap,
    _lock_current_header,
    _original_publisher_identity,
    _owner,
    _pin_options,
    _planned_inputs,
    _post_storage,
    _publication_options,
    _publisher,
    _roles,
    _unserved,
)
from process.plan_release_storage_custody import _document, require_plan_release_storage_custody


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


@dataclass(frozen=True)
class PlanReleaseHistoryCut:
    storage_schema: str
    publication: PlanReleasePublication
    header: dict
    binding_heap: PreparationPinHeap
    pin_heap: PreparationPinHeap
    pin_revision_ids: tuple[str, ...]
    pin_attached: bool
    binding_root_oid: int
    pin_root_oid: int
    owner_oid: int
    original_identity: tuple

    def payload(self):
        """Bind actual retained storage and header state to the original transaction."""
        return asdict(self)


async def _history_header(connection, storage_schema, revision_id):
    relation = storage_relation(storage_schema, REVISION_TABLE)
    row = await connection.fetchval(
        f"SELECT to_jsonb(header)||jsonb_build_object('created_at',extract(epoch FROM created_at)::text,"
        "'published_at',extract(epoch FROM published_at)::text,'retired_at',extract(epoch FROM retired_at)::text) "
        f"FROM {relation} header WHERE serving_revision_id=$1",
        revision_id,
    )
    require(row is not None, "history_header_missing")
    return _document(row)


async def _attached_history_heap(connection, storage_schema, revision_id, roles, *, pin):
    root = storage_relation(storage_schema, PIN_TABLE if pin else BINDING_TABLE)
    children = await connection.fetch(
        f"SELECT DISTINCT tableoid::oid AS oid FROM {root} WHERE serving_revision_id=$1", revision_id
    )
    require(len(children) == 1, "history_child_missing")
    name = await connection.fetchrow(
        "SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1",
        children[0]["oid"],
    )
    return await capture_preparation_pin_heap(
        connection,
        (name["nspname"], name["relname"]),
        storage_schema=storage_schema,
        pin=pin,
        parent_oid=await connection.fetchval("SELECT to_regclass($1)::oid", root),
        **roles,
    )


async def _history_pin_coverage(connection, binding_heap, pin_heap, revision_id):
    binding = storage_relation(binding_heap.schema, binding_heap.name)
    pin = storage_relation(pin_heap.schema, pin_heap.name)
    require(
        not await connection.fetchval(
            f"SELECT EXISTS((SELECT snapshot_id FROM {binding} WHERE serving_revision_id=$1 "
            f"EXCEPT SELECT snapshot_id FROM {pin} WHERE serving_revision_id=$1) UNION ALL "
            f"(SELECT snapshot_id FROM {pin} WHERE serving_revision_id=$1 EXCEPT "
            f"SELECT snapshot_id FROM {binding} WHERE serving_revision_id=$1))",
            revision_id,
        ),
        "history_pin_coverage_changed",
    )


async def _history_pin_heap(connection, storage_schema, revision_id, detached_pin_heap, roles):
    if detached_pin_heap is None:
        return await _attached_history_heap(connection, storage_schema, revision_id, roles, pin=True)
    require(type(detached_pin_heap) is PreparationPinHeap, "history_pin_identity_invalid")
    pin = await _heap(connection, storage_schema, detached_pin_heap, roles, pin=True)
    require(
        not await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {storage_relation(storage_schema, PIN_TABLE)} WHERE serving_revision_id=$1)",
            revision_id,
        ),
        "history_pin_already_attached",
    )
    return pin


async def _capture_history_cut(connection, storage_schema, revision_id, detached_pin_heap, roles):
    """Lock and capture actual retained relations without inventing preparation authority."""
    original = await _original_publisher_identity(connection, roles)
    custody = await require_plan_release_storage_custody(connection, storage_schema, **roles)
    await connection.execute("SET LOCAL lock_timeout='500ms'")
    await connection.execute("SET LOCAL statement_timeout='5s'")
    root_oid_by_name = {
        relation["relname"]: relation["oid"]
        for relation in custody["relations"]
        if relation["nspname"] == storage_schema
    }
    await _owner(connection, roles)
    await connection.execute(
        f"LOCK TABLE {storage_relation(storage_schema, REVISION_TABLE)} IN SHARE ROW EXCLUSIVE MODE"
    )
    header = await _history_header(connection, storage_schema, revision_id)
    require(
        header["serving_status"] in {"published", "retired"} and header["release_status"] != "candidate",
        "history_not_published",
    )
    publication = PlanReleasePublication(
        **{name: header[name] for name in PlanReleasePublication.__dataclass_fields__}
    ).validate()
    binding = await _attached_history_heap(connection, storage_schema, revision_id, roles, pin=False)
    require(
        await connection.fetchval(
            f"SELECT count(*) FROM {storage_relation(binding.schema, binding.name)} WHERE serving_revision_id=$1",
            revision_id,
        )
        == publication.expected_binding_count,
        "history_binding_count_changed",
    )
    pin = await _history_pin_heap(connection, storage_schema, revision_id, detached_pin_heap, roles)
    await _history_pin_coverage(connection, binding, pin, revision_id)
    revision_ids = tuple(
        await connection.fetchval(
            f"SELECT array_agg(DISTINCT serving_revision_id ORDER BY serving_revision_id) FROM {storage_relation(pin.schema, pin.name)}"
        )
    )
    await _publisher(connection, roles)
    require(await _original_publisher_identity(connection, roles) == original, "original_transaction_changed")
    return PlanReleaseHistoryCut(
        storage_schema,
        publication,
        header,
        binding,
        pin,
        revision_ids,
        detached_pin_heap is None,
        root_oid_by_name[BINDING_TABLE],
        root_oid_by_name[PIN_TABLE],
        custody["owner_oid"],
        original,
    )


async def capture_plan_release_history(
    connection,
    storage_schema,
    serving_revision_id,
    *,
    detached_pin_heap=None,
    owner_role,
    publisher_role,
    reader_roles,
    control_reader_roles=(),
):
    """Capture actual history or an authenticated saved pin in the original Publisher transaction.

    The schema-wide header fence lasts until the caller commits or rolls back.
    Lock waits are capped at 500 ms and SQL statements at 5 s; caller work and
    authority callbacks have no such bound. Keep the transaction short, with no
    network work. After a lock timeout, roll it back; a new transaction must
    recapture history and reauthorize it.
    """
    roles = _roles(owner_role, publisher_role, reader_roles, control_reader_roles)
    await _original_publisher_identity(connection, roles)
    try:
        async with connection.transaction():
            return await _capture_history_cut(connection, storage_schema, serving_revision_id, detached_pin_heap, roles)
    finally:
        if connection.is_in_transaction():
            await _publisher(connection, roles)


def _require_history_intent(cut, operation, publication):
    require(type(cut) is PlanReleaseHistoryCut and type(publication) is PlanReleasePublication, "history_cut_required")
    publication.validate()
    require(operation in {"repair", "withdraw", "detach", "reactivate"}, "history_operation_invalid")
    before = cut.publication.payload()
    after = publication.payload()
    if operation == "repair":
        before.pop("source_manifest")
        after.pop("source_manifest")
        require(
            cut.header["is_current"] and cut.header["release_status"] == cut.header["serving_status"] == "published",
            "history_repair_not_current",
        )
    require(before == after, "history_publication_changed")


async def _authorize_history(connection, cut, authority, intent, *, phase):
    operation, publication, expected_current = intent
    witness = await authority(connection, cut, action=operation, phase=phase, observed=cut.pin_heap)
    require(type(witness) is PlanReleaseHistoryAuthority, "history_authority_required")
    witness.validate()
    eligible = cut.pin_revision_ids if operation == "detach" else (publication.serving_revision_id,)
    detached = (
        not cut.pin_attached
        if phase == "before"
        else operation == "detach" or not cut.pin_attached and operation != "reactivate"
    )
    require(
        witness.cut_digest == digest(cut.payload())
        and witness.publication_digest == digest(publication.payload())
        and witness.operation == operation
        and witness.expected_current == expected_current
        and witness.eligible_revision_ids == eligible
        and witness.detached_pin_heap == (cut.pin_heap if detached else None),
        "history_authority_changed",
    )
    return witness


async def _require_history_storage(connection, cut, roles, *, pin_attached):
    await _owner(connection, roles)
    for name, expected in ((BINDING_TABLE, cut.binding_root_oid), (PIN_TABLE, cut.pin_root_oid)):
        require(
            await connection.fetchval("SELECT to_regclass($1)::oid", storage_relation(cut.storage_schema, name))
            == expected,
            "history_root_changed",
        )
    await _heap(connection, cut.storage_schema, cut.binding_heap, roles, pin=False, parent_oid=cut.binding_root_oid)
    await _heap(
        connection,
        cut.storage_schema,
        cut.pin_heap,
        roles,
        pin=True,
        parent_oid=cut.pin_root_oid if pin_attached else None,
    )
    revision_ids = await connection.fetchval(
        f"SELECT array_agg(DISTINCT serving_revision_id ORDER BY serving_revision_id) FROM ONLY {storage_relation(cut.pin_heap.schema, cut.pin_heap.name)}"
    )
    require(tuple(revision_ids or ()) == cut.pin_revision_ids, "history_pin_revision_cohort_changed")
    await _history_pin_coverage(connection, cut.binding_heap, cut.pin_heap, cut.publication.serving_revision_id)
    await _publisher(connection, roles)
    await _post_storage(connection, cut.storage_schema, roles, cut.pin_root_oid, cut.owner_oid)


async def _detach_history_pin(connection, cut):
    require(
        not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM unnest($1::text[]) expected(serving_revision_id) "
            f"LEFT JOIN {storage_relation(cut.storage_schema, REVISION_TABLE)} header USING(serving_revision_id) "
            "WHERE header.serving_revision_id IS NULL OR header.is_current OR header.serving_status<>'retired')",
            list(cut.pin_revision_ids),
        ),
        "history_pin_still_serving",
    )
    if cut.pin_attached:
        await connection.execute(
            f"LOCK TABLE {storage_relation(cut.pin_heap.schema, cut.pin_heap.name)} IN ACCESS EXCLUSIVE MODE"
        )
        await _detach(connection, cut.storage_schema, cut.pin_heap)


async def _change_history_header(connection, cut, operation, publication):
    relation = storage_relation(cut.storage_schema, REVISION_TABLE)
    if operation == "detach":
        await _detach_history_pin(connection, cut)
    elif operation == "repair":
        await connection.execute(
            f"UPDATE {relation} SET source_manifest=$2::jsonb WHERE serving_revision_id=$1",
            publication.serving_revision_id,
            canonical_json(publication.source_manifest),
        )
    elif operation == "withdraw":
        await connection.execute(
            f"UPDATE {relation} SET release_status='withdrawn',serving_status='retired',is_current=false,"
            "retired_at=coalesce(retired_at,transaction_timestamp()) WHERE serving_revision_id=$1",
            publication.serving_revision_id,
        )
    else:
        if not cut.pin_attached:
            await _attach(connection, cut.storage_schema, cut.pin_heap, cut.pin_revision_ids)
        await connection.execute(
            f"UPDATE {relation} SET release_status=CASE WHEN plan_release_id<>$2 THEN 'superseded' ELSE release_status END,"
            "is_current=false,serving_status='retired',retired_at=coalesce(retired_at,transaction_timestamp()) "
            "WHERE healthporta_plan_id=$1 AND is_current AND serving_revision_id<>$3",
            publication.healthporta_plan_id,
            publication.plan_release_id,
            publication.serving_revision_id,
        )
        await connection.execute(
            f"UPDATE {relation} SET release_status='published',serving_status='published',is_current=true,"
            "published_at=coalesce(published_at,transaction_timestamp()),retired_at=NULL WHERE serving_revision_id=$1",
            publication.serving_revision_id,
        )


async def _require_history_context(connection, cut, roles, publication, original_digests):
    require(
        await _original_publisher_identity(connection, roles) == cut.original_identity, "original_transaction_changed"
    )
    require((digest(cut.payload()), digest(publication.payload())) == original_digests, "history_cut_changed")


async def transition_plan_release_history(
    connection, storage_schema, cut, *, operation, publication, expected_current, **history_options
):
    """Perform one fixed retained-history operation with exact native and protected authority.

    A lock timeout requires rolling back the original caller transaction;
    retry with a newly captured cut and fresh authority in a new transaction.
    Successful savepoint release does not release the captured header fence.
    """
    _require_history_intent(cut, operation, publication)
    require(cut.storage_schema == storage_schema, "history_schema_changed")
    roles, authority = _publication_options(history_options)
    require(
        await _original_publisher_identity(connection, roles) == cut.original_identity, "original_transaction_changed"
    )
    intent = (operation, publication, expected_current)
    original_digests = (digest(cut.payload()), digest(publication.payload()))
    try:
        async with connection.transaction():
            await _require_history_context(connection, cut, roles, publication, original_digests)
            await _require_history_storage(connection, cut, roles, pin_attached=cut.pin_attached)
            await _authorize_history(connection, cut, authority, intent, phase="before")
            await _require_history_context(connection, cut, roles, publication, original_digests)
            await _owner(connection, roles)
            require(
                await _history_header(connection, storage_schema, publication.serving_revision_id) == cut.header,
                "history_header_changed",
            )
            await _lock_current_header(connection, storage_schema, publication, expected_current)
            await _change_history_header(connection, cut, operation, publication)
            changed_header = await _history_header(connection, storage_schema, publication.serving_revision_id)
            await _publisher(connection, roles)
            await _authorize_history(connection, cut, authority, intent, phase="after")
            await _require_history_context(connection, cut, roles, publication, original_digests)
            pin_attached = operation != "detach" and (cut.pin_attached or operation == "reactivate")
            await _require_history_storage(connection, cut, roles, pin_attached=pin_attached)
            await _owner(connection, roles)
            require(
                await _history_header(connection, storage_schema, publication.serving_revision_id) == changed_header,
                "history_header_changed",
            )
            await _publisher(connection, roles)
            await _require_history_context(connection, cut, roles, publication, original_digests)
    finally:
        if connection.is_in_transaction():
            await _publisher(connection, roles)
