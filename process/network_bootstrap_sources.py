# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capture named generation-zero EUA/NPI data in one caller-owned MVCC snapshot."""

from __future__ import annotations

import asyncio
import hashlib
import json
import struct
import tempfile
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from uuid import UUID

import asyncpg

from db.models import EntityAddressUnified, NPIData
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_cms_registry_address_equivalence import _declared_type
from process.network_custom_address_source import _digest
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
from process.provider_directory_proof_store import _drain_proof_steps_async, _merged_spool_lines, _RecordSpool

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


@dataclass(frozen=True)
class BootstrapParentReceipt:
    schema_name: str
    table_name: str
    row_count: int
    rows_sha256: str


@dataclass(frozen=True)
class NetworkBootstrapSourceReceipt:
    specification: NetworkBootstrapSourceSpecification
    parent_receipts: tuple[BootstrapParentReceipt, ...]
    captured_at: str
    generation_sha256: str

    @property
    def address_source(self):
        """Return the named address edition for composition."""
        return PinnedAddressSource(_schema_name(self.specification), "entity_address_unified", self.generation_sha256)

    @property
    def npi_source(self):
        """Return the matching NPI edition from the same capture."""
        return PinnedAddressSource(_schema_name(self.specification), "npi", self.generation_sha256)

    def as_dict(self):
        """Return portable names, capture time and complete data accounting."""
        return {
            "component": "network_bootstrap_sources",
            "revision": 2,
            "bootstrap_id": self.specification.bootstrap_id,
            "parent_receipts": [asdict(parent) for parent in self.parent_receipts],
            "captured_at": self.captured_at,
            "generation_sha256": self.generation_sha256,
        }


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


async def _shape(connection, schema_name, table_name):
    """Read actual named columns and declared types without storing native identity."""
    relation = f"{_identifier(schema_name)}.{_identifier(table_name)}"
    columns = await connection.fetch(
        "SELECT attname,format_type(atttypid,atttypmod) AS type FROM pg_attribute "
        "WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        relation,
    )
    return tuple((column["attname"], column["type"]) for column in columns)


async def _content(connection, schema_name, table_name):
    """Sort only native row fingerprints in bounded runs, preserving the chunk commitment."""
    query = f"""SELECT sha256(convert_to(to_jsonb(source_row)::text,'UTF8')) AS fingerprint
      FROM {_identifier(schema_name)}.{_identifier(table_name)} source_row
    """
    digest = hashlib.sha256(b"entity-address-row-chunks/v1\0")
    total_rows, chunk_rows, chunk = 0, 0, hashlib.sha256()
    with tempfile.TemporaryDirectory(prefix="network-content-") as directory:
        spool = _RecordSpool(Path(directory))
        async for fingerprint_row in connection.cursor(query, prefetch=_COPY_BATCH_ROWS):
            fingerprint = fingerprint_row["fingerprint"]
            if type(fingerprint) is not bytes or len(fingerprint) != 32:
                raise NetworkBootstrapSourceError("Native content accounting is invalid")
            spool.add(fingerprint.hex().encode("ascii"))
        paths = await _drain_proof_steps_async(spool.bounded_path_steps())
        with _merged_spool_lines(spool, paths) as fingerprints:
            for fingerprint in fingerprints:
                chunk.update(fingerprint)
                chunk_rows += 1
                if chunk_rows == 4096:
                    digest.update(struct.pack(">I", chunk_rows))
                    digest.update(chunk.digest())
                    total_rows += chunk_rows
                    chunk_rows, chunk = 0, hashlib.sha256()
                    await asyncio.sleep(0)
        if chunk_rows:
            digest.update(struct.pack(">I", chunk_rows))
            digest.update(chunk.digest())
            total_rows += chunk_rows
    return total_rows, digest.hexdigest()


async def _parent_shape(connection, schema_name, table_name, model):
    """Require every named model column and its actual declared type."""
    columns = await _shape(connection, schema_name, table_name)
    expected_types_by_column = {column.name: _declared_type(column) for column in model.__table__.columns}
    if len(columns) != len(expected_types_by_column) or {name for name, _ in columns} != set(expected_types_by_column):
        raise NetworkBootstrapSourceError("Parent model shape differs")
    if dict(columns) != expected_types_by_column:
        raise NetworkBootstrapSourceError("Parent model type differs")


async def _parent(connection, schema_name, table_name):
    """Admit a named, explicitly untracked generation-zero parent."""
    authority_name = (
        "entity_address_result_generation" if table_name == "entity_address_unified" else "npi_result_generation"
    )
    parent = f"{_identifier(schema_name)}.{_identifier(table_name)}"
    authority = f"{_identifier(schema_name)}.{_identifier(authority_name)}"
    await connection.execute(f"LOCK TABLE {parent},{authority} IN ACCESS SHARE MODE NOWAIT")
    model = EntityAddressUnified if table_name == "entity_address_unified" else NPIData
    await _parent_shape(connection, schema_name, table_name, model)
    authority_rows = await connection.fetch(f"SELECT * FROM {authority} LIMIT 2")
    if len(authority_rows) != 1:
        raise NetworkBootstrapSourceError("Parent generation authority is unavailable")
    authority_by_field = dict(authority_rows[0])
    try:
        lineage = str(authority_by_field["local_lineage_id"])
        if not UUID(lineage).int or str(UUID(lineage)) != lineage or authority_by_field["singleton"] is not True:
            raise ValueError
    except KeyError, TypeError, ValueError:
        raise NetworkBootstrapSourceError("Parent generation authority is unavailable") from None
    if type(authority_by_field.get("local_generation")) is not int or authority_by_field["local_generation"] != 0:
        raise NetworkBootstrapSourceError("Bootstrap requires an explicit untracked generation-zero parent")
    logical_fields = {"origin_lineage_id", "origin_generation", "published_at"}
    if table_name == "npi":
        logical_fields.update(
            (
                "canonical_publication_ref",
                "canonical_publication_generation",
                "canonical_chain_ref",
                "canonical_import_date",
            )
        )
    if not logical_fields <= authority_by_field.keys():
        raise NetworkBootstrapSourceError("Parent generation authority is unavailable")
    if any(authority_by_field[field] is not None for field in logical_fields):
        raise NetworkBootstrapSourceError("Parent publication lineage is not a bootstrap input")
    row_count, rows_sha256 = await _content(connection, schema_name, table_name)
    return BootstrapParentReceipt(schema_name, table_name, row_count, rows_sha256)


def _generation(receipt):
    document = receipt.as_dict()
    document.pop("generation_sha256")
    return _digest(document)


def _decode(document):
    """Read current or exact historical receipts without carrying physical metadata forward."""
    from process.network_bootstrap_address_artifact import _receipt

    if type(document) is not str or len(document.encode()) > 16384:
        raise NetworkBootstrapSourceError("Retained bootstrap receipt is unavailable")
    try:
        fields = json.loads(document)
        if (
            type(fields) is not dict
            or fields.get("component") != "network_bootstrap_sources"
            or type(fields.get("revision")) is not int
            or fields["revision"] not in (1, 2)
        ):
            raise ValueError
        is_legacy = fields["revision"] == 1
        fields = _receipt(fields)
        parents = tuple(BootstrapParentReceipt(**parent) for parent in fields["parent_receipts"])
        receipt = NetworkBootstrapSourceReceipt(
            NetworkBootstrapSourceSpecification(fields["bootstrap_id"], parents[0].schema_name, parents[1].schema_name),
            parents,
            fields["captured_at"],
            fields["generation_sha256"],
        )
        _schema_name(receipt.specification)
        if not is_legacy and _generation(receipt) != receipt.generation_sha256:
            raise ValueError
        return receipt
    except KeyError, TypeError, ValueError:
        raise NetworkBootstrapSourceError("Retained bootstrap receipt is invalid") from None


async def _verify_capture(connection, receipt, *, verify_content):
    """Validate the stored logical edition and actual named schema and data."""
    await _transaction(connection)
    if type(receipt) is not NetworkBootstrapSourceReceipt:
        raise NetworkBootstrapSourceError("An immutable bootstrap receipt is required")
    schema_name = _schema_name(receipt.specification)
    stored = await connection.fetchval(
        "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", schema_name
    )
    if _decode(stored) != receipt:
        raise NetworkBootstrapSourceError("Retained bootstrap recipe differs")
    for parent in receipt.parent_receipts:
        model = EntityAddressUnified if parent.table_name == "entity_address_unified" else NPIData
        await _parent_shape(connection, schema_name, parent.table_name, model)
        await connection.execute(f"SELECT * FROM {_identifier(schema_name)}.{_identifier(parent.table_name)} LIMIT 0")
        if verify_content and await _content(connection, schema_name, parent.table_name) != (
            parent.row_count,
            parent.rows_sha256,
        ):
            raise NetworkBootstrapSourceError("Retained bootstrap content differs")
    return receipt


async def verify_network_bootstrap_sources(connection, receipt):
    """Revalidate complete retained data without recapturing mutable parents."""
    try:
        return await _verify_capture(connection, receipt, verify_content=True)
    except asyncpg.PostgresError:
        raise NetworkBootstrapSourceError("Named bootstrap data is unavailable") from None


async def _copy_key(connection, parent):
    """Require the existing native primary index; never add an index to a parent."""
    key = "location_key" if parent.table_name == "entity_address_unified" else "npi"
    indexed = await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_index i JOIN pg_attribute a
        ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
        WHERE i.indrelid=to_regclass($1) AND i.indisprimary AND i.indisvalid AND i.indisready
        AND i.indnkeyatts=1 AND a.attname=$2 AND a.attnotnull)""",
        f"{_identifier(parent.schema_name)}.{_identifier(parent.table_name)}",
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
    return await _copy_native_spool(
        connection,
        query,
        arguments,
        schema_name,
        table_name,
        expected_count,
        byte_limit=byte_limit,
        import_admission=import_admission,
    )


async def _copy_native_spool(
    connection, query, arguments, schema_name, table_name, expected_count, *, byte_limit, import_admission=None
):
    """Copy a caller-bounded spool with exact native counts and consumption."""
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
    """Copy exact complete rows and prepare their search profile before closure."""
    from process.network_membership_serving_indexes import _build_serving_indexes, _serving_plan

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
    await _build_serving_indexes(connection, schema_name, _serving_plan(schema_name))


async def _persist_capture(connection, receipt):
    """Store the data receipt before native DCL closes the new snapshot."""
    namespace = _identifier(_schema_name(receipt.specification))
    receipt = replace(receipt, generation_sha256=_generation(receipt))
    document = json.dumps(receipt.as_dict(), sort_keys=True, separators=(",", ":"))
    if len(document.encode()) > 16384:
        raise NetworkBootstrapSourceError("Bootstrap receipt exceeds its bound")
    quoted = await connection.fetchval("SELECT quote_literal($1::text)", document)
    await connection.execute(f"COMMENT ON SCHEMA {namespace} IS {quoted}")
    return receipt


async def prepare_network_bootstrap_sources(connection, specification, *, owner_role, runtime_roles):
    """Copy once, freeze with native DCL, or replay the existing named edition."""
    schema_name = _schema_name(specification)
    _roles(owner_role, runtime_roles)
    await _transaction(connection)
    try:
        async with connection.transaction():
            stored = await connection.fetchval(
                "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", schema_name
            )
            if stored is not None:
                receipt = _decode(stored)
                if receipt.specification != specification:
                    raise NetworkBootstrapSourceError("Retained bootstrap recipe differs")
                return await verify_network_bootstrap_sources(connection, receipt)
            parents = (
                await _parent(connection, specification.address_schema, "entity_address_unified"),
                await _parent(connection, specification.npi_schema, "npi"),
            )
            captured_at = await connection.fetchval("SELECT transaction_timestamp()::text")
            await connection.execute(f"CREATE SCHEMA {_identifier(schema_name)}")
            await _copy_parent_heaps(connection, schema_name, parents)
            receipt = await _persist_capture(
                connection, NetworkBootstrapSourceReceipt(specification, parents, captured_at, "")
            )
            await _verify_capture(connection, receipt, verify_content=False)
            await freeze_fhir_source_snapshot(
                connection,
                schema_name,
                namespace_owner=owner_role,
                reader_roles=runtime_roles,
                application_roles=runtime_roles,
            )
            return receipt
    except asyncpg.PostgresError, OSError:
        raise NetworkBootstrapSourceError("Native bootstrap preparation failed") from None
