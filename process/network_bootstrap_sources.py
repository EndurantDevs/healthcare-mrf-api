# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close an exact live EUA/NPI capture without claiming original ingestion lineage.

The two parents share one caller-owned MVCC snapshot. ACCESS SHARE protects their
native identities while routine source writes continue. Only newly created
heaps change ownership; legacy guards and generation authority remain intact.
"""

from __future__ import annotations

import hashlib
import json
import struct
import tempfile
from dataclasses import asdict, dataclass, replace
from types import SimpleNamespace
from uuid import UUID

import asyncpg

from db.models import EntityAddressUnified, NPIData
from process.entity_address_result_generation import validate_entity_address_result_generation_authority
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_custom_address_source import NetworkCustomAddressSourceError, _closed, _digest, _source_identity
from process.network_membership_writer_closure import NetworkWriterClosureError, _check_roles, _transfer_and_grant
from process.npi_result_generation import validate_npi_result_generation_authority

_COPY_BATCH_ROWS = 4096
_COPY_BATCH_BYTES = 64 * 1024 * 1024
_COPY_FRAMING_BYTES = 21


class _CopyBatchTooLarge(ValueError):
    """Retry fewer rows before accepting a bounded native COPY batch."""


class NetworkBootstrapSourceError(ValueError):
    """The requested capture, parent identity or retained closure is unavailable."""


@dataclass(frozen=True)
class NetworkBootstrapSourceSpecification:
    bootstrap_id: str
    address_schema: str
    npi_schema: str
    address_table_oid: int
    npi_table_oid: int


@dataclass(frozen=True)
class BootstrapParentReceipt:
    schema_name: str
    table_name: str
    schema_oid: int
    table_oid: int
    owner_oid: int
    authority_table_oid: int
    authority_json: str
    shape_sha256: str
    row_count: int
    rows_sha256: str


@dataclass(frozen=True)
class NetworkBootstrapSourceReceipt:
    specification: NetworkBootstrapSourceSpecification
    owner_role: str
    runtime_roles: tuple[str, ...]
    parent_receipts: tuple[BootstrapParentReceipt, ...]
    capture_snapshot: str
    captured_at: str
    schema_oid: int
    owner_oid: int
    relation_oids: tuple[tuple[str, int], ...]
    index_oids: tuple[tuple[str, int], ...]
    generation_sha256: str

    @property
    def address_source(self):
        """Return the exact protected address pin for composition."""
        return PinnedAddressSource(_schema_name(self.specification), "entity_address_unified", self.generation_sha256)

    @property
    def npi_source(self):
        """Return the matching protected NPI pin from the same capture."""
        return PinnedAddressSource(_schema_name(self.specification), "npi", self.generation_sha256)

    def as_dict(self):
        """Return the bounded immutable capture receipt."""
        return {"component": "network_bootstrap_sources", "revision": 1, **asdict(self)}


def _schema_name(specification):
    if type(specification) is not NetworkBootstrapSourceSpecification:
        raise NetworkBootstrapSourceError("An exact bootstrap specification is required")
    try:
        identity = UUID(specification.bootstrap_id)
    except ValueError, TypeError, AttributeError:
        raise NetworkBootstrapSourceError("Bootstrap identity is invalid") from None
    if not identity.int or str(identity) != specification.bootstrap_id:
        raise NetworkBootstrapSourceError("Bootstrap identity is invalid")
    for schema_name in (specification.address_schema, specification.npi_schema):
        _identifier(schema_name)
    for relation_oid in (specification.address_table_oid, specification.npi_table_oid):
        if type(relation_oid) is not int or not 1 <= relation_oid <= 4294967295:
            raise NetworkBootstrapSourceError("Expected parent OID is invalid")
    schema_name = "network_bootstrap_" + identity.hex
    if schema_name in (specification.address_schema, specification.npi_schema):
        raise NetworkBootstrapSourceError("Capture parents must be external")
    return schema_name


def _roles(owner_role, runtime_roles):
    _identifier(owner_role)
    if (
        type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or tuple(sorted(set(runtime_roles))) != runtime_roles
    ):
        raise NetworkBootstrapSourceError("Runtime roles must be a sorted unique bounded tuple")
    for role_name in runtime_roles:
        _identifier(role_name)
    if owner_role in runtime_roles:
        raise NetworkBootstrapSourceError("Protected owner cannot be a runtime role")
    return {"owner_role": owner_role, "loader_roles": [], "reader_roles": list(runtime_roles)}


async def _transaction(connection):
    if not connection.is_in_transaction() or await connection.fetchval("SHOW transaction_isolation") not in {
        "repeatable read",
        "serializable",
    }:
        raise NetworkBootstrapSourceError("Bootstrap requires a caller repeatable-read transaction")


async def _shape(connection, table_oid):
    columns = await connection.fetch(
        """SELECT attname,atttypid::regtype::text AS type,atttypmod,attnotnull,
      pg_get_expr(d.adbin,d.adrelid) AS default_expr FROM pg_attribute a LEFT JOIN pg_attrdef d
      ON d.adrelid=a.attrelid AND d.adnum=a.attnum
      WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped ORDER BY attnum""",
        table_oid,
    )
    return tuple(column["attname"] for column in columns), _digest([dict(column) for column in columns])


async def _content(connection, schema_name, table_name):
    """Stream compact native chunk hashes, with at most 4096 hashes per aggregate."""
    query = f"""WITH hashes AS MATERIALIZED (
      SELECT encode(sha256(convert_to(to_jsonb(source_row)::text,'UTF8')),'hex') AS fingerprint
      FROM {_identifier(schema_name)}.{_identifier(table_name)} source_row
    ), ordered AS (
      SELECT fingerprint,row_number() OVER(ORDER BY fingerprint COLLATE "C")-1 AS ordinal FROM hashes
    ) SELECT ordinal/4096 AS chunk_ordinal,count(*)::bigint AS row_count,
      encode(sha256(convert_to(string_agg(fingerprint,'' ORDER BY ordinal),'UTF8')),'hex') AS fingerprint
      FROM ordered GROUP BY ordinal/4096 ORDER BY chunk_ordinal"""
    digest = hashlib.sha256(b"entity-address-row-chunks/v1\0")
    total_rows, next_chunk = 0, 0
    async for chunk in connection.cursor(query, prefetch=32):
        if chunk["chunk_ordinal"] != next_chunk or not 1 <= chunk["row_count"] <= 4096:
            raise NetworkBootstrapSourceError("Native content accounting is invalid")
        digest.update(struct.pack(">I", chunk["row_count"]))
        digest.update(bytes.fromhex(chunk["fingerprint"]))
        total_rows += chunk["row_count"]
        next_chunk += 1
    return total_rows, digest.hexdigest()


async def _parent(connection, schema_name, table_name, expected_oid):
    authority_name = (
        "entity_address_result_generation" if table_name == "entity_address_unified" else "npi_result_generation"
    )
    parent = f"{_identifier(schema_name)}.{_identifier(table_name)}"
    authority = f"{_identifier(schema_name)}.{_identifier(authority_name)}"
    await connection.execute(f"LOCK TABLE {parent},{authority} IN ACCESS SHARE MODE NOWAIT")
    locked_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", parent)
    catalog = await connection.fetchrow(
        """SELECT n.oid::bigint AS schema_oid,c.oid::bigint AS table_oid,
      c.relowner::bigint AS owner_oid,c.relkind,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity
      FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid WHERE n.nspname=$1 AND c.relname=$2""",
        schema_name,
        table_name,
    )
    if (
        catalog is None
        or locked_oid != expected_oid
        or catalog["table_oid"] != expected_oid
        or catalog["relkind"] != b"r"
        or catalog["relpersistence"] != b"p"
        or catalog["relrowsecurity"]
        or catalog["relforcerowsecurity"]
    ):
        raise NetworkBootstrapSourceError("Exact native parent identity differs")
    columns, shape_sha256 = await _shape(connection, expected_oid)
    model = EntityAddressUnified if table_name == "entity_address_unified" else NPIData
    if columns != tuple(model.__table__.columns.keys()):
        raise NetworkBootstrapSourceError("Parent model shape differs")
    validator = (
        validate_entity_address_result_generation_authority
        if table_name == "entity_address_unified"
        else validate_npi_result_generation_authority
    )
    authority_row = await connection.fetchrow(f"SELECT * FROM {authority} WHERE singleton IS TRUE")
    try:
        authority_document = validator(dict(authority_row) if authority_row else None).as_dict()
    except ValueError, RuntimeError:
        raise NetworkBootstrapSourceError("Parent generation authority is unavailable") from None
    if authority_document["local_generation"] != 0 or authority_document["serving_generation"] is not None:
        raise NetworkBootstrapSourceError("Bootstrap requires an explicit untracked generation-zero parent")
    if authority_document.get("canonical_provenance") is not None or authority_document["relation_oids"] is not None:
        raise NetworkBootstrapSourceError("Parent publication lineage is not a bootstrap input")
    authority_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", authority)
    row_count, rows_sha256 = await _content(connection, schema_name, table_name)
    return BootstrapParentReceipt(
        schema_name,
        table_name,
        catalog["schema_oid"],
        expected_oid,
        catalog["owner_oid"],
        authority_oid,
        json.dumps(authority_document, sort_keys=True, separators=(",", ":")),
        shape_sha256,
        row_count,
        rows_sha256,
    )


async def _catalog(connection, schema_name):
    namespace = await connection.fetchrow(
        "SELECT oid::bigint,nspowner::bigint FROM pg_namespace WHERE nspname=$1", schema_name
    )
    if namespace is None:
        raise NetworkBootstrapSourceError("Retained bootstrap namespace is absent")
    relations = await connection.fetch(
        """SELECT c.oid::bigint,c.relname,c.relowner::bigint,c.relkind,
      c.relpersistence,c.relrowsecurity,c.relforcerowsecurity,c.relhastriggers,i.indrelid::bigint,
      i.indisvalid,i.indisready FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid
      WHERE c.relnamespace=$1::oid ORDER BY c.relname LIMIT 9""",
        namespace["oid"],
    )
    heap_oids_by_name = {relation["relname"]: relation["oid"] for relation in relations if relation["relkind"] == b"r"}
    if (
        set(heap_oids_by_name) != {"entity_address_unified", "npi"}
        or len(relations) != 4
        or any(
            relation["relkind"] not in {b"r", b"i"}
            or relation["relpersistence"] != b"p"
            or relation["relrowsecurity"]
            or relation["relforcerowsecurity"]
            or relation["relhastriggers"]
            or (
                relation["relkind"] == b"i"
                and (
                    relation["indrelid"] not in heap_oids_by_name.values()
                    or not relation["indisvalid"]
                    or not relation["indisready"]
                )
            )
            for relation in relations
        )
    ):
        raise NetworkBootstrapSourceError("Retained bootstrap object inventory differs")
    if await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=$1::oid)", namespace["oid"]):
        raise NetworkBootstrapSourceError("Retained bootstrap contains stored routines")
    return {
        "namespace": dict(namespace),
        "relations": [dict(relation) for relation in relations],
        "heaps": heap_oids_by_name,
    }


def _generation(receipt):
    document = receipt.as_dict()
    document.pop("generation_sha256")
    return _digest(document)


def _decode(document):
    if type(document) is not str or len(document.encode()) > 16384:
        raise NetworkBootstrapSourceError("Retained bootstrap receipt is unavailable")
    try:
        fields = json.loads(document)
        if type(fields) is not dict:
            raise ValueError
        component, revision = fields.pop("component"), fields.pop("revision")
        if component != "network_bootstrap_sources" or type(revision) is not int or revision != 1:
            raise ValueError
        fields["specification"] = NetworkBootstrapSourceSpecification(**fields["specification"])
        fields["parent_receipts"] = tuple(BootstrapParentReceipt(**parent) for parent in fields["parent_receipts"])
        fields["runtime_roles"] = tuple(fields["runtime_roles"])
        for key in ("relation_oids", "index_oids"):
            fields[key] = tuple(tuple(identity) for identity in fields[key])
        receipt = NetworkBootstrapSourceReceipt(**fields)
        if (
            tuple(parent.table_name for parent in receipt.parent_receipts) != ("entity_address_unified", "npi")
            or any(type(parent.row_count) is not int or parent.row_count < 0 for parent in receipt.parent_receipts)
            or _generation(receipt) != receipt.generation_sha256
        ):
            raise ValueError
        return receipt
    except KeyError, TypeError, ValueError:
        raise NetworkBootstrapSourceError("Retained bootstrap receipt is invalid") from None


async def _verify_capture(connection, receipt, *, verify_content):
    """Check native custody; fresh uncommitted copies already proved content parity."""
    await _transaction(connection)
    if type(receipt) is not NetworkBootstrapSourceReceipt:
        raise NetworkBootstrapSourceError("An immutable bootstrap receipt is required")
    schema_name = _schema_name(receipt.specification)
    _roles(receipt.owner_role, receipt.runtime_roles)
    stored = await connection.fetchval(
        "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", schema_name
    )
    if _decode(stored) != receipt:
        raise NetworkBootstrapSourceError("Retained bootstrap recipe differs")
    catalog = await _catalog(connection, schema_name)
    current_owner_oid = await connection.fetchval(
        "SELECT oid::bigint FROM pg_roles WHERE rolname=$1", receipt.owner_role
    )
    indexes = tuple(
        (relation["relname"], relation["oid"]) for relation in catalog["relations"] if relation["relkind"] == b"i"
    )
    if (
        current_owner_oid != receipt.owner_oid
        or catalog["namespace"] != {"oid": receipt.schema_oid, "nspowner": receipt.owner_oid}
        or tuple(sorted(catalog["heaps"].items())) != receipt.relation_oids
        or indexes != receipt.index_oids
        or any(relation["relowner"] != receipt.owner_oid for relation in catalog["relations"])
    ):
        raise NetworkBootstrapSourceError("Retained native bootstrap identities differ")
    try:
        await _closed(connection, receipt.schema_oid, receipt.owner_oid, receipt.runtime_roles, None)
        for parent in receipt.parent_receipts:
            pin = receipt.address_source if parent.table_name == "entity_address_unified" else receipt.npi_source
            identity = await _source_identity(
                connection, pin, receipt.runtime_roles, address=parent.table_name == "entity_address_unified"
            )
            if identity[3] != parent.shape_sha256 or (
                verify_content
                and await _content(connection, schema_name, parent.table_name) != (parent.row_count, parent.rows_sha256)
            ):
                raise NetworkBootstrapSourceError("Retained bootstrap content or shape differs")
    except NetworkCustomAddressSourceError:
        raise NetworkBootstrapSourceError("Retained bootstrap ownership or privileges differ") from None
    return receipt


async def verify_network_bootstrap_sources(connection, receipt):
    """Revalidate complete retained content and custody without recapturing live parents."""
    return await _verify_capture(connection, receipt, verify_content=True)


async def _copy_key(connection, parent):
    """Require the existing native primary index; never add an index to a parent."""
    key = "location_key" if parent.table_name == "entity_address_unified" else "npi"
    indexed = await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_index i JOIN pg_attribute a
        ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
        WHERE i.indrelid=$1::oid AND i.indisprimary AND i.indisvalid AND i.indisready
        AND i.indnkeyatts=1 AND a.attname=$2 AND a.attnotnull)""",
        parent.table_oid,
        key,
    )
    if indexed is not True:
        raise NetworkBootstrapSourceError("Parent requires its exact native primary key")
    return key


async def _copy_native_batch(
    connection, query, arguments, schema_name, table_name, expected_count, *, byte_limit=None, import_admission=None
):
    """Bound an anonymous binary spool and check both native counts and consumption."""
    byte_limit = _COPY_BATCH_BYTES if byte_limit is None else byte_limit
    if type(byte_limit) is not int or not 0 < byte_limit <= 64 * 1024 * 1024:
        raise NetworkBootstrapSourceError("Native COPY byte bound is invalid")
    if import_admission is not None and not callable(import_admission):
        raise NetworkBootstrapSourceError("Native COPY import admission is invalid")
    with tempfile.TemporaryFile(mode="w+b") as copy_file:
        spool_by_name = {"bytes": 0, "oversized": False, "failed": False}

        async def write_chunk(chunk):
            """Discard excess chunks without overflowing the spool or cancelling COPY."""
            if spool_by_name["failed"]:
                return
            if spool_by_name["oversized"] or spool_by_name["bytes"] + len(chunk) > byte_limit:
                spool_by_name["oversized"] = True
                return
            try:
                written = copy_file.write(chunk)
            except OSError:
                spool_by_name["failed"] = True
                return
            if written != len(chunk):
                spool_by_name["failed"] = True
                return
            spool_by_name["bytes"] += written

        async with connection.transaction():
            exported = await connection.copy_from_query(query, *arguments, output=write_chunk, format="binary")
            if spool_by_name["failed"]:
                raise NetworkBootstrapSourceError("Native COPY spool write was incomplete")
            if spool_by_name["oversized"]:
                raise _CopyBatchTooLarge()
            if exported != f"COPY {expected_count}" or copy_file.tell() != spool_by_name["bytes"]:
                raise NetworkBootstrapSourceError("Native COPY export accounting differs")
            copy_file.seek(0)
            if import_admission is not None:
                await import_admission("before_import")
            imported = await connection.copy_to_table(
                table_name, schema_name=schema_name, source=copy_file, format="binary"
            )
            if imported != f"COPY {expected_count}" or copy_file.tell() != spool_by_name["bytes"]:
                raise NetworkBootstrapSourceError("Native COPY import accounting differs")
            if import_admission is not None:
                await import_admission("after_import")


async def _copy_parent_rows(connection, schema_name, parent):
    """Use indexed key ranges in one MVCC snapshot, reducing wide batches on overflow."""
    key = await _copy_key(connection, parent)
    parent_table = f"{_identifier(parent.schema_name)}.{_identifier(parent.table_name)}"
    batch_rows, copied_rows, last_key = _COPY_BATCH_ROWS, 0, None
    while copied_rows < parent.row_count:
        predicate = "TRUE" if last_key is None else f"{key}>$1"
        parameters = () if last_key is None else (last_key,)
        bounds = await connection.fetchrow(
            f"""SELECT count(*)::bigint AS row_count,max({key}) AS last_key,
            COALESCE(sum(octet_length(record_send(batch))::bigint),0)::bigint AS native_bytes
            FROM (SELECT * FROM {parent_table} WHERE {predicate} ORDER BY {key} LIMIT {batch_rows}) batch""",
            *parameters,
        )
        count = bounds["row_count"]
        if not 1 <= count <= batch_rows or copied_rows + count > parent.row_count:
            raise NetworkBootstrapSourceError("Native COPY keyset accounting differs")
        # Native record_send includes type OIDs, so it bounds COPY row bytes above.
        if bounds["native_bytes"] + _COPY_FRAMING_BYTES > _COPY_BATCH_BYTES:
            if count == 1:
                raise NetworkBootstrapSourceError("One native row exceeds the 64 MiB COPY bound")
            batch_rows = max(1, count // 2)
            continue
        upper_parameter = len(parameters) + 1
        query = f"SELECT * FROM {parent_table} WHERE {predicate} AND {key}<=${upper_parameter} ORDER BY {key}"
        try:
            await _copy_native_batch(
                connection, query, (*parameters, bounds["last_key"]), schema_name, parent.table_name, count
            )
        except _CopyBatchTooLarge:
            if count == 1:
                raise NetworkBootstrapSourceError("One native row exceeds the 64 MiB COPY bound") from None
            batch_rows = max(1, count // 2)
            continue
        copied_rows += count
        last_key = bounds["last_key"]


async def _copy_parent_heaps(connection, schema_name, parents):
    """COPY complete native rows in bounded batches and prove exact retained content."""
    namespace = _identifier(schema_name)
    for parent in parents:
        parent_table = f"{_identifier(parent.schema_name)}.{_identifier(parent.table_name)}"
        candidate_table = f"{namespace}.{_identifier(parent.table_name)}"
        await connection.execute(
            f"CREATE TABLE {candidate_table} (LIKE {parent_table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"
        )
        await _copy_parent_rows(connection, schema_name, parent)
        if await _content(connection, schema_name, parent.table_name) != (parent.row_count, parent.rows_sha256):
            raise NetworkBootstrapSourceError("Native copy content differs from the capture")
        key = "location_key" if parent.table_name == "entity_address_unified" else "npi"
        await connection.execute(f"ALTER TABLE {candidate_table} ADD PRIMARY KEY ({key})")


async def _persist_capture(connection, receipt):
    """Store the closed receipt before commit and check native custody without rescanning rows."""
    namespace = _identifier(_schema_name(receipt.specification))
    owner_role = receipt.owner_role
    receipt = replace(receipt, generation_sha256=_generation(receipt))
    document = json.dumps(receipt.as_dict(), sort_keys=True, separators=(",", ":"))
    if len(document.encode()) > 16384:
        raise NetworkBootstrapSourceError("Bootstrap receipt exceeds its bound")
    publisher = await connection.fetchval("SELECT quote_ident(current_user)")
    quoted = await connection.fetchval("SELECT quote_literal($1::text)", document)
    await connection.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
    await connection.execute(f"COMMENT ON SCHEMA {namespace} IS {quoted}")
    await connection.execute(f"SET LOCAL ROLE {publisher}")
    return await _verify_capture(connection, receipt, verify_content=False)


async def prepare_network_bootstrap_sources(connection, specification, *, owner_role, runtime_roles):
    """Prepare once, atomically close the capture, or replay its exact protected receipt."""
    schema_name = _schema_name(specification)
    roles = _roles(owner_role, runtime_roles)
    await _transaction(connection)
    try:
        async with connection.transaction():
            stored = await connection.fetchval(
                "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", schema_name
            )
            if stored is not None:
                receipt = _decode(stored)
                if (receipt.specification, receipt.owner_role, receipt.runtime_roles) != (
                    specification,
                    owner_role,
                    runtime_roles,
                ):
                    raise NetworkBootstrapSourceError("Retained bootstrap recipe differs")
                return await verify_network_bootstrap_sources(connection, receipt)
            await _check_roles(connection, roles)
            parents = (
                await _parent(
                    connection, specification.address_schema, "entity_address_unified", specification.address_table_oid
                ),
                await _parent(connection, specification.npi_schema, "npi", specification.npi_table_oid),
            )
            capture = await connection.fetchrow(
                "SELECT pg_current_snapshot()::text AS snapshot,transaction_timestamp()::text AS captured_at"
            )
            if len(capture["snapshot"].encode()) > 8192:
                raise NetworkBootstrapSourceError("Capture snapshot exceeds the receipt bound")
            namespace = _identifier(schema_name)
            await connection.execute(f"CREATE SCHEMA {namespace}")
            await _copy_parent_heaps(connection, schema_name, parents)
            catalog = await _catalog(connection, schema_name)
            await _transfer_and_grant(connection, SimpleNamespace(schema_name=schema_name), roles, catalog)
            owner_oid = await connection.fetchval("SELECT oid::bigint FROM pg_roles WHERE rolname=$1", owner_role)
            await _closed(connection, catalog["namespace"]["oid"], owner_oid, runtime_roles, None)
            receipt = NetworkBootstrapSourceReceipt(
                specification,
                owner_role,
                runtime_roles,
                parents,
                capture["snapshot"],
                capture["captured_at"],
                catalog["namespace"]["oid"],
                owner_oid,
                tuple(sorted(catalog["heaps"].items())),
                tuple(
                    (relation["relname"], relation["oid"])
                    for relation in catalog["relations"]
                    if relation["relkind"] == b"i"
                ),
                "",
            )
            return await _persist_capture(connection, receipt)
    except asyncpg.PostgresError, OSError:
        raise NetworkBootstrapSourceError("Native bootstrap preparation failed") from None
    except NetworkCustomAddressSourceError, NetworkWriterClosureError:
        raise NetworkBootstrapSourceError("Protected bootstrap roles or privileges are unavailable") from None
