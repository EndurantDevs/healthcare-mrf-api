# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Short native retention cuts between isolated preparation and durable jobs."""

from db.plan_release_storage import BINDING_TABLE, PIN_TABLE, REVISION_TABLE, storage_identifier, storage_relation
from process.plan_release_preparation_pin_catalog import require_preparation_pin_heap
from process.plan_release_preparation_pin_contract import (
    PlanReleasePublication,
    PreparationPinAuthority,
    PreparationPinInputs,
    PreparationPinReceipt,
    PreparationPinScope,
    canonical_json,
    digest,
    require,
)
from process.plan_release_storage_custody import require_plan_release_storage_custody
from process.ptg_parts.ptg2_lifecycle_lock import PTG2_SOURCE_LIFECYCLE_LOCK_NAMESPACE, PTG2_SOURCE_POINTER_GC_LOCK_KEY


def _roles(owner_role, publisher_role, reader_roles, control_reader_roles=()):
    return dict(
        owner_role=owner_role,
        publisher_role=publisher_role,
        reader_roles=reader_roles,
        control_reader_roles=control_reader_roles,
    )


def _pin_options(options_by_name):
    """Keep authority and the explicit control subset as closed optional keywords."""
    require(set(options_by_name) <= {"authority", "control_reader_roles"}, "unknown_pin_options")
    return options_by_name.get("authority"), options_by_name.get("control_reader_roles", ())


async def _authorize(connection, scope, authority, *, action, phase, observed):
    witness = await authority(connection, scope, action=action, phase=phase, observed=observed)
    require(type(witness) is PreparationPinAuthority, "native_authority_witness_required")
    return witness.validate(scope)


async def _entry(connection, storage_schema, scope, authority, roles):
    require(connection.is_in_transaction(), "caller_transaction_required")
    require(type(scope) is PreparationPinScope and callable(authority), "native_authority_required")
    scope.validate()
    baseline = await require_plan_release_storage_custody(connection, storage_schema, **roles)
    await connection.execute("SET LOCAL lock_timeout='500ms'")
    await connection.execute("SET LOCAL statement_timeout='5s'")
    await _owner(connection, roles)
    roots = ",".join(
        "ONLY " + storage_relation(storage_schema, name) for name in (PIN_TABLE, BINDING_TABLE, REVISION_TABLE)
    )
    await connection.execute("LOCK TABLE " + roots + " IN ACCESS SHARE MODE")
    await _publisher(connection, roles)
    await connection.execute("SELECT pg_advisory_xact_lock_shared(hashtext($1))", PTG2_SOURCE_POINTER_GC_LOCK_KEY)
    for source_key in scope.source_keys:
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            PTG2_SOURCE_LIFECYCLE_LOCK_NAMESPACE + ":" + source_key,
        )
    root_oid = next(
        row["oid"] for row in baseline["relations"] if row["nspname"] == storage_schema and row["relname"] == PIN_TABLE
    )
    return root_oid, baseline["owner_oid"]


async def _owner(connection, roles):
    await connection.execute(f"SET LOCAL ROLE {storage_identifier(roles['owner_role'])}")


async def _publisher(connection, roles):
    await connection.execute(f"SET LOCAL ROLE {storage_identifier(roles['publisher_role'])}")


async def _unserved(connection, storage_schema, revision_ids):
    header_relation = storage_relation(storage_schema, REVISION_TABLE)
    binding_relation = storage_relation(storage_schema, BINDING_TABLE)
    require(
        not await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {header_relation} WHERE serving_revision_id=ANY($1::text[])) "
            f"OR EXISTS(SELECT FROM {binding_relation} WHERE serving_revision_id=ANY($1::text[]))",
            list(revision_ids),
        ),
        "revision_has_serving_history",
    )


async def _planned_inputs(connection, storage_schema, scope, binding_heap, pin_heap):
    binding_relation = storage_relation(binding_heap.schema, binding_heap.name)
    pin_relation = storage_relation(pin_heap.schema, pin_heap.name)
    snapshots = storage_relation(storage_schema, "ptg2_snapshot")
    plan_scopes = storage_relation(storage_schema, "ptg2_v3_snapshot_plan_scope")
    require(
        not await connection.fetchval(
            f"WITH expected AS (SELECT unnest($1::text[]) AS serving_revision_id) SELECT EXISTS("
            f"(SELECT DISTINCT serving_revision_id FROM {binding_relation} EXCEPT SELECT * FROM expected) UNION ALL "
            f"(SELECT * FROM expected EXCEPT SELECT DISTINCT serving_revision_id FROM {binding_relation}))",
            list(scope.revision_ids),
        ),
        "binding_revision_cohort_changed",
    )
    require(
        not await connection.fetchval(
            f"SELECT EXISTS((SELECT serving_revision_id,snapshot_id FROM {binding_relation} EXCEPT SELECT serving_revision_id,snapshot_id FROM {pin_relation}) UNION ALL "
            f"(SELECT serving_revision_id,snapshot_id FROM {pin_relation} EXCEPT SELECT serving_revision_id,snapshot_id FROM {binding_relation}))"
        ),
        "pin_binding_antijoin_failed",
    )
    require(
        not await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {binding_relation} b LEFT JOIN {snapshots} s USING(snapshot_id) "
            "WHERE s.snapshot_id IS NULL OR s.status<>'published' OR s.manifest->'serving_index'->>'source_key' IS DISTINCT FROM b.source_key "
            f"OR NOT EXISTS(SELECT FROM {plan_scopes} p WHERE p.snapshot_id=b.snapshot_id AND p.plan_id=b.plan_id "
            "AND (b.plan_market_type IS NULL OR p.plan_market_type=b.plan_market_type)))"
        ),
        "physical_source_antijoin_failed",
    )
    actual_sources = await connection.fetchval(
        f"SELECT array_agg(DISTINCT source_key ORDER BY source_key) FROM {binding_relation}"
    )
    require(actual_sources == list(scope.source_keys), "source_cohort_changed")


def _bounds(revision_ids):
    # All values are already canonical identifiers, never caller SQL.
    return ",".join("'" + revision + "'" for revision in revision_ids)


async def _attach(connection, storage_schema, heap, revision_ids):
    await connection.execute(
        f"ALTER TABLE {storage_relation(storage_schema, PIN_TABLE)} ATTACH PARTITION {storage_relation(heap.schema, heap.name)} FOR VALUES IN ({_bounds(revision_ids)})"
    )


async def _heap(connection, storage_schema, expected, roles, *, pin, parent_oid=None):
    return await require_preparation_pin_heap(
        connection, expected, storage_schema=storage_schema, pin=pin, parent_oid=parent_oid, **roles
    )


async def _post_storage(connection, storage_schema, roles, root_oid, owner_oid):
    proof = await require_plan_release_storage_custody(connection, storage_schema, **roles)
    root = next(row for row in proof["relations"] if row["nspname"] == storage_schema and row["relname"] == PIN_TABLE)
    require((root["oid"], proof["owner_oid"]) == (root_oid, owner_oid), "root_identity_changed")


async def retain_preparation_pins(
    connection,
    storage_schema,
    scope,
    inputs,
    *,
    owner_role,
    publisher_role,
    reader_roles,
    **admission_options,
):
    """Commit future pin custody before any network request; never switch serving."""
    authority, control_reader_roles = _pin_options(admission_options)
    roles = _roles(owner_role, publisher_role, reader_roles, control_reader_roles)
    require(type(inputs) is PreparationPinInputs, "native_heap_receipts_required")
    inputs.validate()
    binding_heap, pin_heap = inputs.binding_heap, inputs.pin_heap
    async with connection.transaction():
        root_oid, owner_oid = await _entry(connection, storage_schema, scope, authority, roles)
        before = await _authorize(connection, scope, authority, action="retain", phase="before", observed=None)
        require(before.preparation_status in {"admitting", "sealed"}, "preparation_not_active")
        require(
            before.binding_heap == binding_heap and before.prepared_pin_heap == pin_heap,
            "protected_heap_inventory_changed",
        )
        await _owner(connection, roles)
        await _unserved(connection, storage_schema, scope.revision_ids)
        await _heap(connection, storage_schema, binding_heap, roles, pin=False)
        registered = before.registered_receipt
        if registered is None:
            require(
                before.preparation_status == "admitting"
                and not before.detached_receipts
                and not before.cleaned_receipts
                and all(
                    admission.status == "reserved" and admission.dispatch_count == 0 and admission.engine_run_id is None
                    for admission in before.admissions
                ),
                "late_pin_admission_refused",
            )
            await _heap(connection, storage_schema, pin_heap, roles, pin=True)
            await _planned_inputs(connection, storage_schema, scope, binding_heap, pin_heap)
            await _attach(connection, storage_schema, pin_heap, scope.revision_ids)
            receipt = PreparationPinReceipt(scope, pin_heap, root_oid, owner_oid, scope.revision_ids).validate()
        else:
            require(
                registered == PreparationPinReceipt(scope, pin_heap, root_oid, owner_oid, scope.revision_ids),
                "registered_pin_custody_changed",
            )
            await _heap(connection, storage_schema, pin_heap, roles, pin=True, parent_oid=root_oid)
            await _planned_inputs(connection, storage_schema, scope, binding_heap, pin_heap)
            receipt = registered
        await _publisher(connection, roles)
        after = await _authorize(connection, scope, authority, action="retain", phase="after", observed=receipt)
        require(after.registered_receipt == receipt, "pin_enrollment_not_persisted")
        await _post_storage(connection, storage_schema, roles, root_oid, owner_oid)
        return receipt


async def require_preparation_pins(
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
    """Accept only this exact retained cohort before the caller's one serving cut."""
    require(connection.is_in_transaction(), "caller_transaction_required")
    require(type(receipt) is PreparationPinReceipt, "pin_receipt_required")
    receipt.validate()
    roles = _roles(owner_role, publisher_role, reader_roles, control_reader_roles)
    async with connection.transaction():
        root_oid, owner_oid = await _entry(connection, storage_schema, receipt.scope, authority, roles)
        before = await _authorize(
            connection, receipt.scope, authority, action="serving", phase="before", observed=receipt
        )
        require(
            before.registered_receipt == receipt and (root_oid, owner_oid) == (receipt.root_oid, receipt.owner_oid),
            "registered_pin_custody_changed",
        )
        retained = before.require_settled()
        require(
            before.preparation_status == "sealed" and retained == receipt.retained_revision_ids,
            "pin_serving_members_changed",
        )
        await _owner(connection, roles)
        await _unserved(connection, storage_schema, receipt.retained_revision_ids)
        await _heap(connection, storage_schema, receipt.heap, roles, pin=True, parent_oid=root_oid)
        await _publisher(connection, roles)
        after = await _authorize(
            connection, receipt.scope, authority, action="serving", phase="after", observed=receipt
        )
        require(after.registered_receipt == receipt, "pin_serving_custody_changed")
        await _post_storage(connection, storage_schema, roles, root_oid, owner_oid)
        return receipt


def _publication_options(options_by_name):
    required_names = {"owner_role", "publisher_role", "reader_roles", "authority"}
    require(
        required_names <= set(options_by_name) <= required_names | {"control_reader_roles"},
        "publication_options_invalid",
    )
    roles = _roles(
        *(options_by_name[name] for name in ("owner_role", "publisher_role", "reader_roles")),
        options_by_name.get("control_reader_roles", ()),
    )
    require(callable(options_by_name["authority"]), "native_authority_required")
    return roles, options_by_name["authority"]


async def _original_publisher_identity(connection, roles):
    require(connection.is_in_transaction(), "caller_transaction_required")
    identity = await connection.fetchrow(
        "SELECT (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,"
        "pg_backend_pid() AS backend_pid,pg_current_xact_id()::text AS transaction_id,"
        "session_user::text AS session,current_user::text AS current"
    )
    require(identity["session"] == identity["current"] == roles["publisher_role"], "original_publisher_required")
    return tuple(identity[name] for name in ("database_oid", "backend_pid", "transaction_id", "session", "current"))


def _publication_witness(witness, publication, inputs, receipt, *, published):
    require(
        witness.publication_digest == digest(publication.payload())
        and publication.binding_set_digest == receipt.scope.binding_inputs_digest,
        "publication_authority_changed",
    )
    require(
        witness.registered_receipt == receipt
        and witness.binding_heap == inputs.binding_heap
        and witness.prepared_pin_heap == inputs.pin_heap
        and receipt.heap == inputs.pin_heap,
        "publication_inventory_changed",
    )
    require(
        witness.preparation_status == ("published" if published else "sealed")
        and (not published or witness.writers_closed)
        and witness.require_settled() == receipt.retained_revision_ids == (publication.serving_revision_id,)
        and receipt.scope.revision_ids == (publication.serving_revision_id,)
        and inputs.binding_heap.row_count == publication.expected_binding_count,
        "publication_cohort_changed",
    )


async def _lock_current_header(connection, storage_schema, publication, expected_current):
    """Fence absent headers as well as rows against native header writers.

    This serializes plans in the same schema through the caller's transaction.
    Native binding ATTACH also serializes publication on its shared parent.
    """
    require(
        expected_current is None
        or type(expected_current) is tuple
        and len(expected_current) == 2
        and all(type(value) is str for value in expected_current),
        "current_header_identity_invalid",
    )
    relation = storage_relation(storage_schema, REVISION_TABLE)
    await connection.execute(f"LOCK TABLE {relation} IN SHARE ROW EXCLUSIVE MODE")
    headers = await connection.fetch(
        f"SELECT serving_revision_id,binding_set_digest FROM {relation} WHERE healthporta_plan_id=$1 AND is_current FOR UPDATE",
        publication.healthporta_plan_id,
    )
    actual = (
        tuple(headers[0][name] for name in ("serving_revision_id", "binding_set_digest")) if len(headers) == 1 else None
    )
    require(len(headers) <= 1 and actual == expected_current, "current_header_changed")


async def _write_publication_header(connection, storage_schema, publication):
    relation = storage_relation(storage_schema, REVISION_TABLE)
    await connection.execute(
        f"UPDATE {relation} SET release_status=CASE WHEN plan_release_id<>$2 THEN 'superseded' ELSE release_status END,"
        "is_current=false,serving_status='retired',retired_at=coalesce(retired_at,transaction_timestamp()) "
        "WHERE healthporta_plan_id=$1 AND is_current",
        publication.healthporta_plan_id,
        publication.plan_release_id,
    )
    await connection.execute(
        f"INSERT INTO {relation}(serving_revision_id,plan_release_id,healthporta_plan_id,plan_version_id,release_month,"
        "expected_binding_count,binding_set_digest,source_manifest,release_status,serving_status,is_current,published_at) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,'published','published',true,transaction_timestamp())",
        publication.serving_revision_id,
        publication.plan_release_id,
        publication.healthporta_plan_id,
        publication.plan_version_id,
        publication.release_month,
        publication.expected_binding_count,
        publication.binding_set_digest,
        canonical_json(publication.source_manifest),
    )


async def _publish_native_binding(connection, storage_schema, publication, inputs, receipt, expected_current, roles):
    await _owner(connection, roles)
    await _heap(connection, storage_schema, inputs.binding_heap, roles, pin=False)
    await _planned_inputs(connection, storage_schema, receipt.scope, inputs.binding_heap, receipt.heap)
    await _lock_current_header(connection, storage_schema, publication, expected_current)
    await connection.execute(
        f"ALTER TABLE {storage_relation(storage_schema, BINDING_TABLE)} ATTACH PARTITION "
        f"{storage_relation(inputs.binding_heap.schema, inputs.binding_heap.name)} "
        f"FOR VALUES IN ({_bounds(receipt.retained_revision_ids)})"
    )
    await _write_publication_header(connection, storage_schema, publication)
    await _publisher(connection, roles)


async def _require_published_header(connection, storage_schema, publication, roles):
    await _owner(connection, roles)
    row = await connection.fetchrow(
        f"SELECT serving_revision_id,plan_release_id,healthporta_plan_id,plan_version_id,release_month,"
        f"expected_binding_count,binding_set_digest,source_manifest FROM {storage_relation(storage_schema, REVISION_TABLE)} "
        "WHERE serving_revision_id=$1 AND is_current AND release_status='published' AND serving_status='published' "
        "AND published_at IS NOT NULL AND retired_at IS NULL",
        publication.serving_revision_id,
    )
    await _publisher(connection, roles)
    require(row is not None, "published_header_changed")
    from process.plan_release_storage_custody import _document

    actual_by_field = dict(row)
    actual_by_field["source_manifest"] = _document(actual_by_field["source_manifest"])
    require(actual_by_field == publication.payload(), "published_header_changed")


async def _require_published_inventory(connection, storage_schema, inputs, receipt, roles):
    await _owner(connection, roles)
    binding_root = await connection.fetchval(
        "SELECT to_regclass($1)::oid", storage_relation(storage_schema, BINDING_TABLE)
    )
    await _heap(connection, storage_schema, inputs.binding_heap, roles, pin=False, parent_oid=binding_root)
    await _heap(connection, storage_schema, receipt.heap, roles, pin=True, parent_oid=receipt.root_oid)
    await _publisher(connection, roles)
    await _post_storage(connection, storage_schema, roles, receipt.root_oid, receipt.owner_oid)


async def publish_prepared_plan_release(
    connection, storage_schema, publication, *, inputs, receipt, expected_current, **publication_options
):
    """Attach one authorized binding and header atomically with caller-owned metadata.

    Header writes serialize across plans. Lock waits are capped at 500 ms and
    each SQL statement at 5 s; neither bounds callbacks or the outer transaction.
    Keep that transaction short, with no network work. After a lock timeout,
    roll it back and retry with freshly authorized current-header state in a
    new original Publisher transaction, not just a new helper savepoint.
    """
    require(type(publication) is PlanReleasePublication, "publication_required")
    require(
        type(inputs) is PreparationPinInputs and type(receipt) is PreparationPinReceipt, "native_heap_receipts_required"
    )
    publication.validate()
    inputs.validate()
    receipt.validate()
    roles, authority = _publication_options(publication_options)
    original = await _original_publisher_identity(connection, roles)
    publication_digest = digest(publication.payload())
    try:
        async with connection.transaction():
            await require_preparation_pins(connection, storage_schema, receipt, authority=authority, **roles)
            before = await _authorize(
                connection, receipt.scope, authority, action="publish", phase="before", observed=receipt
            )
            require(await _original_publisher_identity(connection, roles) == original, "original_transaction_changed")
            _publication_witness(before, publication, inputs, receipt, published=False)
            require(digest(publication.payload()) == publication_digest, "publication_header_changed")
            await _publish_native_binding(
                connection, storage_schema, publication, inputs, receipt, expected_current, roles
            )
            after = await _authorize(
                connection, receipt.scope, authority, action="publish", phase="after", observed=receipt
            )
            require(await _original_publisher_identity(connection, roles) == original, "original_transaction_changed")
            _publication_witness(after, publication, inputs, receipt, published=True)
            require(digest(publication.payload()) == publication_digest, "publication_header_changed")
            await _require_published_header(connection, storage_schema, publication, roles)
            await _require_published_inventory(connection, storage_schema, inputs, receipt, roles)
            require(await _original_publisher_identity(connection, roles) == original, "original_transaction_changed")
    finally:
        if connection.is_in_transaction():
            await _publisher(connection, roles)
