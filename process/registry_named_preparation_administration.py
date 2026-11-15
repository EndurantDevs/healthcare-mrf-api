# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded administrator freeze and fenced release of named preparations."""

import asyncio
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.exc import ResourceClosedError

from process.registry_named_preparation_catalog import locked_claim
from process.registry_named_preparation_lifecycle import observe_frozen
from process.registry_named_preparation_records import relation_inventory, require
from process.registry_named_preparation_transactions import preparation_mutation

_CLEANUP_SECONDS = 5


async def _invalidate_owner(owner):
    """Let SQLAlchemy discard its connection without accessing a pooled driver."""
    invalidation = asyncio.create_task(owner.invalidate())
    done, _pending = await asyncio.wait({invalidation}, timeout=_CLEANUP_SECONDS)
    if not done:
        invalidation.cancel()
        invalidation.add_done_callback(_consume_cleanup_result)
        raise TimeoutError("fhir_source_transactions_invalidation_timeout")
    try:
        invalidation.result()
    except ResourceClosedError:
        return


def _consume_cleanup_result(task):
    """Observe a late local cleanup result after its physical owner has been discarded."""
    if not task.cancelled():
        task.exception()


async def _drain_discarded_cleanup(task):
    """Never let cancellation suppression keep discarded physical capacity admitted."""
    done, _pending = await asyncio.wait({task}, timeout=_CLEANUP_SECONDS)
    if not done:
        task.cancel()
        task.add_done_callback(_consume_cleanup_result)
    else:
        _consume_cleanup_result(task)


async def _bounded_cleanup(operation, owner):
    cleanup = asyncio.create_task(operation)
    try:
        done, _pending = await asyncio.wait({cleanup}, timeout=_CLEANUP_SECONDS)
        if not done:
            raise TimeoutError("fhir_source_transactions_cleanup_timeout")
        cleanup.result()
    except BaseException:
        try:
            await _invalidate_owner(owner)
        finally:
            cleanup.cancel()
            await _drain_discarded_cleanup(cleanup)
        raise


async def _finish_owned_cleanup(operation, interruption):
    """Drain bounded cleanup through repeated cancellation without abandoning its owner."""
    cleanup = asyncio.create_task(operation)
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as error:
            interruption = interruption or error
        except BaseException:
            break
    if interruption is not None:
        try:
            cleanup.result()
        except BaseException as error:
            raise interruption from error
        raise interruption
    cleanup.result()


@asynccontextmanager
async def _owned_transaction(context, owner):
    """Commit inline under the caller deadline; bound rollback and discard on interruption."""
    await context.start()
    try:
        yield context
    except BaseException as error:
        await _finish_owned_cleanup(_bounded_cleanup(context.rollback(), owner), error)
    else:
        try:
            async with asyncio.timeout(_CLEANUP_SECONDS):
                await context.commit()
        except BaseException as error:
            await _finish_owned_cleanup(_invalidate_owner(owner), error)


def _require(condition, code):
    if not condition:
        raise ValueError("fhir_source_operator_" + code)


async def _freeze_registry_generation(connection, config, preparation_id):
    """Freeze a genuine named registry candidate with the existing admin connection."""
    from process.registry_named_preparation_records import local_uuid
    from process.registry_named_preparation_store import NamedPreparationStore, named_preparation_role_config

    _require(
        set(config)
        in (
            {"kind", "candidate_id", "catalog_schema", "storage_schema", "preparation_roles"},
            {"version", "kind", "candidate_id", "catalog_schema", "storage_schema", "preparation_roles"},
        )
        and config.get("version", 2) == 2
        and not connection.in_transaction(),
        "config_required",
    )
    _require(str(local_uuid(preparation_id)) == config["candidate_id"], "generation_schema_changed")
    roles = await named_preparation_role_config(config["kind"], config["preparation_roles"])
    async with asyncio.timeout(15), _owned_transaction(connection.begin(), connection):
        await connection.execute(text("SET LOCAL statement_timeout='3s'"))
        await connection.execute(text("SET LOCAL lock_timeout='1s'"))
        driver = (await connection.get_raw_connection()).driver_connection
        store = NamedPreparationStore(driver, roles, kind=config["kind"])
        preparation_row = await store.read_named(preparation_id)
        _require(
            preparation_row is not None
            and preparation_row["state"] in {"handed_off", "frozen", "validated", "published"},
            "preparation_not_handed_off",
        )
        document = preparation_row["handoff"]
        _require(
            all(document[field] == config[field] for field in ("catalog_schema", "storage_schema")),
            "generation_schema_changed",
        )
        if preparation_row["state"] in {"frozen", "validated", "published"}:
            return {"preparation_id": preparation_id, "state": preparation_row["state"]}
        claim = await _administrative_preparation_claim(store, preparation_row)
        frozen = await freeze_preparation(store, claim)
        return {"preparation_id": preparation_id, "state": frozen["state"]}


async def _release_registry_generation(connection, config, preparation_id):
    """Release one unapproved office generation through the existing administrator."""
    from process.registry_named_preparation_records import local_uuid
    from process.registry_named_preparation_store import named_preparation_role_config

    _require(
        set(config)
        in (
            {"kind", "candidate_id", "catalog_schema", "storage_schema", "preparation_roles"},
            {"version", "kind", "candidate_id", "catalog_schema", "storage_schema", "preparation_roles"},
        )
        and config.get("version", 2) == 2
        and config.get("kind") == "office"
        and not connection.in_transaction(),
        "config_required",
    )
    _require(str(local_uuid(preparation_id)) == config["candidate_id"], "generation_schema_changed")
    roles = await named_preparation_role_config("office", config["preparation_roles"])
    from process.registry_ptg_office_retention import release_named_office_preparation

    async def cancel_preparation(driver, document):
        """Bind Source cancellation to Core's original fenced release transaction."""
        return await _cancel_named_office_preparation(driver, config, roles, document)

    async with asyncio.timeout(15):
        return await release_named_office_preparation(
            connection,
            preparation_id,
            control_schema=config["catalog_schema"],
            storage_schema=config["storage_schema"],
            cancel_preparation=cancel_preparation,
        )


async def _cancel_named_office_preparation(driver, config, roles, document):
    """Cancel the exact handoff in Core's fenced release transaction, preserving claim history."""
    from process.registry_named_preparation_records import digest, require
    from process.registry_named_preparation_store import NamedPreparationStore, named_preparation_document

    store = NamedPreparationStore(driver, roles, kind="office")
    preparation_by_field = await store.read_named(config["candidate_id"])
    require(preparation_by_field is not None and preparation_by_field["handoff"] is not None, "preparation_missing")
    handoff = named_preparation_document(preparation_by_field["handoff"], "office")
    require(
        all(handoff[field] == config[field] for field in ("candidate_id", "catalog_schema", "storage_schema"))
        and preparation_by_field["admission_sha256"] == digest(preparation_by_field["admission"])
        and all(
            preparation_by_field["admission"].get(field) == expected
            for field, expected in {
                "node_id": config["catalog_schema"],
                "dataset_key": config["candidate_id"],
                "run_id": config["candidate_id"],
                "request_sha256": digest(handoff),
            }.items()
        ),
        "named_candidate_changed",
    )
    require(document is None or named_preparation_document(document, "office") == handoff, "handoff_changed")
    reason = digest({"operation": "release", "handoff": handoff})
    if preparation_by_field["state"] == "abandoned":
        require(
            preparation_by_field["cancellation_requested"] and preparation_by_field["abandonment_sha256"] == reason,
            "abandonment_conflict",
        )
    else:
        require(document is not None, "named_ready_changed")
        require(preparation_by_field["state"] in {"handed_off", "frozen", "validated"}, "preparation_terminal")
        claim = await _administrative_preparation_claim(store, preparation_by_field)
        await store.request_cancel(
            str(preparation_by_field["preparation_id"]), preparation_by_field["admission_sha256"]
        )
        preparation_by_field = await store.abandon(claim, reason_sha256=reason)
    return {"handoff": handoff, "state": preparation_by_field["state"]}


async def _administrative_preparation_claim(store, preparation_row):
    """Reuse the current claim or claim the same handed-off preparation after expiry."""
    from uuid import uuid4

    from process.registry_named_preparation_records import PreparationClaim

    live = await store.connection.fetchval(
        "SELECT $1::timestamptz>clock_timestamp()",
        preparation_row["claim_expires_at"],
    )
    if not live:
        preparation_row = await store.claim(
            str(preparation_row["preparation_id"]), preparation_row["admission_sha256"], str(uuid4())
        )
    return PreparationClaim(
        str(preparation_row["preparation_id"]), str(preparation_row["claim_token"]), preparation_row["claim_attempt"]
    )


async def _freeze_named_registry(store, claim, prepared, inventory):
    """Use configured administrative rights for the complete named registry generation."""
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
    from process.registry_named_preparation_store import named_preparation_document, require_named_ready

    document = named_preparation_document(prepared["handoff"], store.named_kind)
    await require_named_ready(store.connection, document)
    expected_inventory = relation_inventory(
        {
            "relations": [
                {"schema_name": table["schema"], "relation_name": table["name"]} for table in document["tables"]
            ]
        }
    )
    require(relation_inventory(inventory) == expected_inventory, "generation_schema_changed")
    require(
        bool(getattr(store, "namespace_owner_role", None)) and bool(getattr(store, "application_roles", ())),
        "named_admin_config_required",
    )
    for schema_name in sorted({relation["schema_name"] for relation in inventory["relations"]}):
        await freeze_fhir_source_snapshot(
            store.connection,
            schema_name,
            namespace_owner=store.namespace_owner_role,
            reader_roles=store.application_roles,
            application_roles=store.application_roles,
        )
    await observe_frozen(store, claim)
    return await store.yield_named_claim(claim)


@preparation_mutation
async def freeze_preparation(store, claim):
    """Close application writes on the exact declared schemas before validation."""
    prepared = await locked_claim(store, claim)
    require(prepared["state"] in {"handed_off", "frozen", "validated"}, "intake_required")
    if prepared["state"] in {"frozen", "validated"}:
        return prepared
    inventory = relation_inventory(prepared["intake_inventory"])
    return await _freeze_named_registry(store, claim, prepared, inventory)
