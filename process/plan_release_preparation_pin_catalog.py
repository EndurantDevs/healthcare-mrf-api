# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native catalog and COPY receipts for closed preparation-owned heaps."""

import hashlib
from dataclasses import asdict

from db.plan_release_storage import BINDING_COLUMNS, PIN_COLUMNS, storage_identifier, storage_relation
from process.plan_release_preparation_pin_contract import PreparationPinHeap, digest, require
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import _document, _verify_payload, storage_roles


def _definition(relation):
    return {
        "columns": _document(relation["columns"]),
        "constraints": _document(relation["constraints"]),
        "kind": relation["relkind"],
        "persistence": relation["relpersistence"],
    }


async def preparation_heap_catalog(
    connection, schema_name, table_name, *, owner_oid, reader_oids, pin, parent_oid=None
):
    """Authenticate exact native ownership, shape, indexes and closed ACLs."""
    relation_name = storage_relation(schema_name, table_name)
    await connection.execute(f"LOCK TABLE ONLY {relation_name} IN ACCESS SHARE MODE")
    rows = await connection.fetch(CATALOG_SQL, [relation_name], owner_oid, reader_oids)
    require(len(rows) == 1, "heap_missing_or_inherited")
    relation = rows[0]
    require(
        relation["relowner"] == owner_oid and relation["relkind"] == "r" and relation["relpersistence"] == "p",
        "heap_owner_changed",
    )
    require(not relation["relrowsecurity"] and not relation["relforcerowsecurity"], "heap_security_changed")
    require(
        all(relation[field] is True for field in ("inert", "native_checks", "direct_acl_closed", "readers_closed")),
        "heap_custody_open",
    )
    require(
        relation["parents"] == ([] if parent_oid is None else [parent_oid])
        and relation["relispartition"] is (parent_oid is not None),
        "heap_parent_changed",
    )
    _verify_payload(relation, pin=pin)
    return relation


async def _copy_digest(connection, schema_name, table_name, *, pin):
    """Account for native ordered COPY chunks, without Python row preparation."""
    columns = PIN_COLUMNS if pin else BINDING_COLUMNS
    fields = ",".join(storage_identifier(column[0]) for column in columns)
    key = "serving_revision_id,snapshot_id" if pin else "serving_revision_id,role,binding_ordinal"
    hasher = hashlib.sha256()

    async def consume(chunk):
        """Account for one bounded native COPY buffer."""
        hasher.update(chunk)

    result = await connection.copy_from_query(
        f"SELECT {fields} FROM ONLY {storage_relation(schema_name, table_name)} ORDER BY {key}",
        output=consume,
        format="binary",
    )
    require(type(result) is str and result.startswith("COPY "), "copy_receipt_invalid")
    return int(result[5:]), hasher.hexdigest()


async def capture_preparation_pin_heap(
    connection,
    heap_identity,
    *,
    storage_schema,
    owner_role,
    publisher_role,
    reader_roles,
    pin,
    **custody_options,
):
    """Read a sealed native image; the callback separately binds its durable owner."""
    parent_oid, control_reader_roles = _heap_options(custody_options)
    schema_name, table_name = heap_identity
    owner_oid, reader_oids = await storage_roles(
        connection, storage_schema, owner_role, publisher_role, reader_roles, control_reader_roles
    )
    relation = await preparation_heap_catalog(
        connection,
        schema_name,
        table_name,
        owner_oid=owner_oid,
        reader_oids=reader_oids,
        pin=pin,
        parent_oid=parent_oid,
    )
    row_count, payload_digest = await _copy_digest(connection, schema_name, table_name, pin=pin)
    filenode = await connection.fetchval("SELECT pg_relation_filenode($1::oid)::bigint", relation["oid"])
    database_oid = await connection.fetchval("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")
    return PreparationPinHeap(
        schema_name,
        table_name,
        relation["oid"],
        database_oid,
        filenode,
        digest(_definition(relation)),
        digest(_document(relation["indexes"])),
        row_count,
        payload_digest,
    ).validate()


async def require_preparation_pin_heap(
    connection,
    expected,
    *,
    storage_schema,
    owner_role,
    publisher_role,
    reader_roles,
    pin,
    **custody_options,
):
    """Recheck the entire sealed native image under its relation lock."""
    parent_oid, control_reader_roles = _heap_options(custody_options)
    require(type(expected) is PreparationPinHeap, "heap_receipt_required")
    expected.validate()
    actual = await capture_preparation_pin_heap(
        connection,
        (expected.schema, expected.name),
        storage_schema=storage_schema,
        owner_role=owner_role,
        publisher_role=publisher_role,
        reader_roles=reader_roles,
        control_reader_roles=control_reader_roles,
        pin=pin,
        parent_oid=parent_oid,
    )
    require(asdict(actual) == asdict(expected), "heap_receipt_changed")
    return actual


def _heap_options(options_by_name):
    """Keep the two optional custody keywords closed and source compatible."""
    require(set(options_by_name) <= {"parent_oid", "control_reader_roles"}, "unknown_heap_options")
    return options_by_name.get("parent_oid"), options_by_name.get("control_reader_roles", ())
