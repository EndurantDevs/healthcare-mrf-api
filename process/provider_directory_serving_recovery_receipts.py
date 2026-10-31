# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed native receipts for ordinary provider serving recovery."""

import asyncio
import copy
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process import entity_address_result_generation as generations
from process.provider_directory_output_snapshot_catalog import OUTPUT_CATALOG_SQL, decoded, encoded
from process.provider_directory_projection_stage import _copy_driver

ROOTS = ("provider_directory_address_overlay", "entity_address_unified")
JOURNAL = "provider_directory_serving_recovery"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
PREPARATION_SECONDS = 2 * 60 * 60


@dataclass(frozen=True)
class PreparedRelation:
    """A content receipt belongs to its actual locked transaction and task."""

    session: object
    task: object
    actor_json: str
    schema: str
    receipt_json: str
    elapsed_seconds: float
    heap_bytes: int
    total_bytes: int


def require(condition, code):
    """Refuse an incomplete ordinary serving proof before publication."""
    if not condition:
        raise ValueError("provider_directory_serving_recovery_" + code)


def qualified(schema, name):
    """Quote only validated native identifiers."""
    return storage_relation(schema, name)


async def ordinary(database):
    """Use the caller's original ordinary transaction without rebinding it."""
    from process import provider_directory_generic_source_policy as source

    binding = database._transaction_binding()
    require(binding is not None and binding.session.in_transaction(), "ordinary_transaction_required")
    require(source._PUBLISHER.get() is None and source._OWNER.get() is None, "ordinary_actor_required")
    driver = await _copy_driver(binding.session)
    if not driver.is_in_transaction():
        await binding.session.execute(text("SELECT 1"))
    require(driver.is_in_transaction(), "physical_transaction_required")
    actor_by_field = dict(
        await driver.fetchrow(
            "SELECT session_user::text AS session,current_user::text AS current,"
            "current_user::regrole::oid::bigint AS owner,pg_backend_pid() AS backend,"
            "pg_current_xact_id()::text AS xid,(SELECT oid::bigint FROM pg_database "
            "WHERE datname=current_database()) AS database_oid"
        )
    )
    require(actor_by_field["session"] == actor_by_field["current"], "ordinary_actor_required")
    return binding.session, driver, actor_by_field


def _require_heap(native, owner):
    topology = native["topology"]
    require(
        native["relowner"] == owner
        and topology["kind"] == "r"
        and topology["persistence"] == "p"
        and not any(topology[key] for key in ("partition", "rls", "force_rls", "parents", "children", "hooks", "rules"))
        and bool(native["columns"])
        and all(index["valid"] and index["ready"] and index["live"] for index in native["indexes"])
        and all(constraint["validated"] for constraint in native["constraints"]),
        "native_heap_changed",
    )
    for grant in native["acl"]["table"]:
        require(grant[0] == owner or grant[2:] == ["SELECT", False], "native_acl_changed")
    for grant in native["acl"]["columns"]:
        require(grant[1] == owner or grant[3:] == ["SELECT", False], "native_acl_changed")


@asynccontextmanager
async def normalized_receipt_scope(database, session, schema):
    """Canonicalize receipt reads and restore the original transaction's exact GUCs."""
    from process.entity_address_snapshot_destination import _preserve_receipt_settings
    from process.entity_address_snapshot_receipt import _normalize_receipt_session

    binding = database._transaction_binding()
    require(binding is not None and binding.session is session, "receipt_transaction_changed")
    async with _preserve_receipt_settings(database=database):
        await _normalize_receipt_session(session, schema)
        yield


async def catalog(database, schema, names):
    """Capture stable native catalog fields, excluding vacuum and planner statistics."""
    from process.mrf_address_publication import require_native_read_catalog

    session, driver, actor = await ordinary(database)
    async with normalized_receipt_scope(database, session, schema):
        rows = await driver.fetch(OUTPUT_CATALOG_SQL, schema, list(names), actor["owner"], [])
        require(len(rows) == len(names), "relation_missing")
        await require_native_read_catalog(session, tuple(row["oid"] for row in rows))
        catalog_by_name = {}
        for row in rows:
            native_by_field = {
                key: decoded(row[key]) if key in {"topology", "columns", "constraints", "indexes", "acl"} else row[key]
                for key in row.keys()
                if key not in {"acl_closed", "readers_closed", "readers_select"}
            }
            _require_heap(native_by_field, actor["owner"])
            catalog_by_name[native_by_field["relname"]] = native_by_field
        return catalog_by_name


async def relation(database, schema, name, *, content=True):
    """Bind a complete canonical row multiset to one real native heap and indexes."""
    session, _driver, _actor = await ordinary(database)
    from process.entity_address_snapshot_receipt import _row_identity

    native = (await catalog(database, schema, (name,)))[name]
    receipt_by_field = {"native": native}
    if content:
        from process.entity_address_cutover_contract import preserve_transaction_sql_settings
        from process.entity_address_unified import _sql_literal

        async with (
            normalized_receipt_scope(database, session, schema),
            preserve_transaction_sql_settings(database, ["statement_timeout"], _sql_literal),
        ):
            await database.status(f"SET LOCAL statement_timeout='{PREPARATION_SECONDS * 1000}ms'")
            count, row_digest = await _row_identity(session, schema, name)
        receipt_by_field.update(rows=count, data_sha256=row_digest)
    return receipt_by_field


async def _require_freeze(driver, oid):
    require(
        await driver.fetchval(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() "
            "AND relation=$1::oid AND mode IN ('ShareLock','AccessExclusiveLock') AND granted)",
            oid,
        ),
        "preparation_lock_required",
    )


async def prepare_relation(database, schema, name):
    """Hash a frozen heap before any live AccessExclusive cutover starts."""
    started = time.monotonic()
    session, driver, actor = await ordinary(database)
    native = (await catalog(database, schema, (name,)))[name]
    await _require_freeze(driver, native["oid"])
    captured = await relation(database, schema, name)
    require(captured["native"] == native and (await ordinary(database))[2] == actor, "preparation_changed")
    sizes = await driver.fetchrow(
        "SELECT pg_catalog.pg_relation_size($1::oid)::bigint AS heap_bytes,"
        "pg_catalog.pg_total_relation_size($1::oid)::bigint AS total_bytes",
        native["oid"],
    )
    return PreparedRelation(
        session,
        asyncio.current_task(),
        encoded(actor),
        schema,
        encoded(captured),
        time.monotonic() - started,
        sizes["heap_bytes"],
        sizes["total_bytes"],
    )


def preparation_metrics(proof):
    """Report actual digest phase cost and capacity without treating it as authority."""
    return {
        "rows": decoded(proof.receipt_json)["rows"],
        "elapsed_seconds": proof.elapsed_seconds,
        "heap_bytes": proof.heap_bytes,
        "total_bytes": proof.total_bytes,
    }


async def recheck_prepared(database, proof, name):
    """Carry content only while original task, native transaction and locks still hold."""
    session, driver, actor = await ordinary(database)
    require(
        type(proof) is PreparedRelation
        and proof.session is session
        and proof.task is asyncio.current_task()
        and proof.actor_json == encoded(actor),
        "prepared_transaction_changed",
    )
    expected = decoded(proof.receipt_json)
    await _require_freeze(driver, expected["native"]["oid"])
    actual = await relation(database, proof.schema, name, content=False)
    require_relation(expected, actual, renamed=True, content=False)
    actual.update(rows=expected["rows"], data_sha256=expected["data_sha256"])
    return actual


async def prepared_state(database, schema, proofs_by_root):
    """Capture fresh installed metadata without rescanning any frozen bulk relation."""
    roots_by_name = {}
    for root in ROOTS:
        roots_by_name[root] = await recheck_prepared(database, proofs_by_root[root], root)
    return {"metadata": await metadata(database, schema), "roots": roots_by_name}


def normalized_native(native):
    """Ignore only OID-derived table/index names changed by the verified rotation."""
    result = copy.deepcopy(native)
    result["relname"] = "heap:" + str(native["oid"])
    for field in ("indexes", "constraints"):
        for item in result[field]:
            item["name"] = field + ":" + str(item["oid"])
    return result


def require_relation(expected, actual, *, renamed=False, content=True):
    """Compare every stored catalog field and, when requested, all row contents."""
    require(type(expected) is dict and type(actual) is dict, "receipt_invalid")
    expected_native, actual_native = expected["native"], actual["native"]
    if renamed:
        expected_native, actual_native = map(normalized_native, (expected_native, actual_native))
    require(expected_native == actual_native, "relation_changed")
    if content:
        require(
            type(expected.get("rows")) is int
            and expected["rows"] >= 0
            and type(expected.get("data_sha256")) is str
            and _HASH.fullmatch(expected["data_sha256"])
            and (expected["rows"], expected["data_sha256"]) == (actual.get("rows"), actual.get("data_sha256")),
            "content_changed",
        )


def validate_state(state_by_field):
    """Decode only the fixed complete serving receipt, never arbitrary object names."""
    require(type(state_by_field) is dict and set(state_by_field) == {"metadata", "roots"}, "state_invalid")
    metadata = state_by_field["metadata"]
    require(
        type(metadata) is dict
        and set(metadata)
        == {"address", "alias", "overlay_alias", "profile", "doctors", "geo", "relations", "common_receipt"},
        "state_invalid",
    )
    require(type(state_by_field["roots"]) is dict and set(state_by_field["roots"]) == set(ROOTS), "state_invalid")
    for root, receipt in state_by_field["roots"].items():
        require(
            type(receipt) is dict
            and set(receipt) == {"native", "rows", "data_sha256"}
            and receipt["native"]["relname"] == root,
            "state_invalid",
        )
        require_relation(receipt, receipt)
    relations = metadata["relations"]
    require(
        type(relations) is list
        and len(relations) == 7
        and len(set(relations)) == 7
        and all(type(oid) is int and 0 < oid < 2**32 for oid in relations)
        and relations[0] == state_by_field["roots"][ROOTS[1]]["native"]["oid"],
        "state_invalid",
    )
    address = metadata["address"]
    require(
        type(address) is dict
        and address.get("singleton") is True
        and type(address.get("local_generation")) is int
        and address["local_generation"] >= 0
        and (address.get("relation_oids") is None or address["relation_oids"] == relations),
        "state_invalid",
    )
    require(type(metadata["alias"]) is dict and type(metadata["alias"].get("generation")) is int, "state_invalid")
    return state_by_field


async def _optional_json(driver, schema, table, selection, predicate):
    name = qualified(schema, table)
    if await driver.fetchval("SELECT to_regclass($1)::oid", name) is None:
        return None
    rows = await driver.fetch(f"SELECT {selection} AS value FROM {name} AS state WHERE {predicate}")
    require(len(rows) <= 1, "authority_ambiguous")
    return decoded(rows[0]["value"]) if rows else None


async def metadata(database, schema):
    """Capture serving authorities without treating mutable build candidates as active state."""
    session, driver, _actor = await ordinary(database)
    async with normalized_receipt_scope(database, session, schema):
        selections = (
            ("address", generations.TABLE_NAME, "to_jsonb(state)", "singleton"),
            ("alias", "address_alias_state_v1", "to_jsonb(state)", "singleton"),
            (
                "overlay_alias",
                "address_alias_artifact_state_v1",
                "to_jsonb(state)",
                "artifact_name='provider_directory_address_overlay'",
            ),
            ("profile", "provider_directory_profile_serving_generation", "to_jsonb(state)", "singleton_key='global'"),
            ("doctors", "reference_family_result_generation", "to_jsonb(state)", "importer_id='cms-doctors'"),
            (
                "geo",
                "entity_address_geo_assurance_state",
                "jsonb_build_object('version',active_geo_assurance_version,"
                "'table_oid',active_table_oid::bigint,'signature',active_relation_signature,'bindings',active_dependency_bindings)",
                "singleton",
            ),
        )
        metadata_by_field = {}
        for key, table, selection, predicate in selections:
            metadata_by_field[key] = await _optional_json(driver, schema, table, selection, predicate)
        metadata_by_field["relations"] = list(await generations._current_relation_oids(database, schema))
        table = qualified(schema, "provider_directory_cms_serving_receipt")
        if await driver.fetchval("SELECT to_regclass($1)::oid", table) is None:
            metadata_by_field["common_receipt"] = None
        else:
            receipt_rows = await driver.fetch(
                f"SELECT receipt_id,payload FROM {table} receipt WHERE NOT EXISTS "
                f"(SELECT FROM {table} successor WHERE successor.predecessor_receipt_id=receipt.receipt_id)"
            )
            require(len(receipt_rows) <= 1, "common_history_changed")
            metadata_by_field["common_receipt"] = (
                {"receipt_id": receipt_rows[0]["receipt_id"], "payload": decoded(receipt_rows[0]["payload"])}
                if receipt_rows
                else None
            )
        require(
            metadata_by_field["address"] is not None and metadata_by_field["alias"] is not None, "authority_missing"
        )
        return metadata_by_field


async def state(database, schema):
    """Capture complete content and native authority only while readers remain admitted."""
    roots_by_name = {}
    for root in ROOTS:
        roots_by_name[root] = await relation(database, schema, root)
    return {"metadata": await metadata(database, schema), "roots": roots_by_name}


async def lock_metadata(database, schema):
    """Pin fixed authorities without requiring UPDATE on read-only reference ledgers."""
    _session, driver, _actor = await ordinary(database)
    await driver.execute(
        "LOCK TABLE "
        + ",".join(qualified(schema, name) for name in generations.RELATION_NAMES[1:])
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    for table, predicate in (
        ("address_alias_state_v1", "singleton"),
        ("reference_family_result_generation", "importer_id='cms-doctors'"),
        ("provider_directory_profile_serving_generation", "singleton_key='global'"),
    ):
        name = qualified(schema, table)
        if await driver.fetchval("SELECT to_regclass($1)::oid", name) is not None:
            await driver.execute(f"LOCK TABLE {name} IN SHARE MODE NOWAIT")
            await driver.fetch(f"SELECT 1 FROM {name} WHERE {predicate}")
    await generations._locked_authority(database, schema)
    for table, predicate in (
        ("address_alias_artifact_state_v1", "artifact_name='provider_directory_address_overlay'"),
        ("entity_address_geo_assurance_state", "singleton"),
    ):
        rows = await driver.fetch(f"SELECT 1 FROM {qualified(schema, table)} WHERE {predicate} FOR UPDATE NOWAIT")
        require(len(rows) == 1, "metadata_authority_missing")
    name = qualified(schema, "provider_directory_cms_serving_receipt")
    if await driver.fetchval("SELECT to_regclass($1)::oid", name) is not None:
        await driver.execute(f"LOCK TABLE {name} IN SHARE MODE NOWAIT")


async def lock(database, schema, names, *, mode="ACCESS EXCLUSIVE", wait=False):
    """Lock a fixed or previously authenticated native set in deterministic order."""
    require(mode in {"SHARE", "ACCESS EXCLUSIVE"}, "lock_mode_invalid")
    require(type(wait) is bool and (not wait or mode == "ACCESS EXCLUSIVE"), "lock_wait_invalid")
    _session, driver, _actor = await ordinary(database)
    await driver.execute(
        "LOCK TABLE "
        + ",".join(qualified(schema, name) for name in sorted(set(names)))
        + " IN "
        + mode
        + " MODE"
        + ("" if wait else " NOWAIT")
    )


def retained_name(oid, *, index=False):
    """Derive collision-free retained names from native object identities."""
    require(type(oid) is int and 0 < oid < 2**32, "oid_invalid")
    return ("pdsrv_i_" if index else "pdsrv_r_") + format(oid, "x")


async def rename(database, schema, original, replacement, *, index=False):
    """Never overwrite an occupied relation name during a metadata-only rotation."""
    _session, driver, _actor = await ordinary(database)
    require(
        await driver.fetchval("SELECT to_regclass($1)::oid", qualified(schema, replacement)) is None,
        "retained_name_occupied",
    )
    await driver.execute(
        "ALTER "
        + ("INDEX " if index else "TABLE ")
        + qualified(schema, original)
        + " RENAME TO "
        + storage_identifier(replacement)
    )


async def retain(database, schema, expected):
    """Preserve a locked original heap and all its native indexes without copying rows."""
    native = expected["native"]
    actual = await relation(database, schema, native["relname"], content=False)
    require_relation(expected, actual, content=False)
    _session, driver, _actor = await ordinary(database)
    require(
        await driver.fetchval(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND relation=$1::oid AND mode='AccessExclusiveLock' AND granted)",
            native["oid"],
        ),
        "original_lock_required",
    )
    for index in native["indexes"]:
        await rename(database, schema, index["name"], retained_name(index["oid"], index=True), index=True)
    await rename(database, schema, native["relname"], retained_name(native["oid"]))


async def restore(database, schema, expected, *, prepared):
    """Restore authenticated original names after the replaced successor is retained."""
    native = expected["native"]
    retained = retained_name(native["oid"])
    actual = await recheck_prepared(database, prepared, retained)
    require_relation(expected, actual, renamed=True)
    await rename(database, schema, retained, native["relname"])
    for index in native["indexes"]:
        await rename(database, schema, retained_name(index["oid"], index=True), index["name"], index=True)
