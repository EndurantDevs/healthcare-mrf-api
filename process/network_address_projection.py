# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Project exact provider/site memberships into an isolated address candidate."""

import json
import os
import re
from dataclasses import dataclass
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_membership_copy import MembershipCopyTarget


class NetworkAddressProjectionError(ValueError):
    """Candidate projection failed without changing serving data."""


def _identifier(name):
    if type(name) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", name) is None:
        raise NetworkAddressProjectionError("Projection SQL identifier is invalid")
    return '"' + name + '"'


@dataclass(frozen=True)
class PinnedAddressSource:
    schema_name: str
    table_name: str
    generation_id: str

    def __post_init__(self):
        _identifier(self.schema_name)
        _identifier(self.table_name)
        if (
            type(self.generation_id) is not str
            or not 1 <= len(self.generation_id) <= 128
            or any(ord(character) < 32 or ord(character) == 127 for character in self.generation_id)
        ):
            raise NetworkAddressProjectionError("Pinned address generation is invalid")


@dataclass(frozen=True)
class NetworkAddressProjectionReceipt:
    candidate_id: str
    schema_name: str
    source_generation: str
    address_rows: int
    membership_rows: int
    distinct_memberships: int
    projected_locations: int
    orphan_bindings: int


async def _lock_candidate(connection, copy_target, address_source, control_schema):
    candidate = await connection.fetchrow(
        f"SELECT * FROM {_identifier(control_schema)}.network_membership_candidate WHERE candidate_id=$1 FOR UPDATE",
        UUID(copy_target.candidate_id),
    )
    identity_fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    if candidate is None or any(str(candidate[field]) != getattr(copy_target, field) for field in identity_fields):
        raise NetworkAddressProjectionError("Candidate ownership does not match the exact projection target")
    if candidate["state"] not in {"sealed", "validated"}:
        raise NetworkAddressProjectionError("Candidate is not sealed or validated")
    source_generations = candidate["source_generations"]
    if isinstance(source_generations, str):
        source_generations = json.loads(source_generations)
    if source_generations.get("unified_address") != address_source.generation_id:
        raise NetworkAddressProjectionError("Pinned address source generation mismatch")
    return candidate


async def _membership_accounting(connection, membership_table, binding_table, address_table):
    accounting = await connection.fetchrow(f"""
        WITH binding_matches AS MATERIALIZED (
            SELECT b.*, a.location_key AS matched_location_key
            FROM {binding_table} b LEFT JOIN {address_table} a
              ON a.location_key=b.location_key AND a.entity_type=b.entity_type AND a.entity_id=b.entity_id
        ), membership_matches AS (
            SELECT m.network_id, b.matched_location_key
            FROM {membership_table} m LEFT JOIN binding_matches b
              ON b.provider_system=m.provider_system AND b.provider_id=m.provider_id AND b.location_id=m.location_id
        )
        SELECT count(*) AS membership_rows,
            count(*) FILTER (WHERE matched_location_key IS NULL OR network_id IS NULL OR network_id <= 0)
                AS invalid_memberships,
            count(DISTINCT (matched_location_key,network_id)) FILTER (WHERE matched_location_key IS NOT NULL)
                AS distinct_memberships,
            count(DISTINCT matched_location_key) AS projected_locations,
            (SELECT count(*) FROM binding_matches WHERE matched_location_key IS NULL) AS orphan_bindings
        FROM membership_matches
    """)
    if accounting["invalid_memberships"]:
        raise NetworkAddressProjectionError("Selected memberships lack an exact provider/site/address binding")
    return accounting


async def _projection_columns(connection, address_table):
    columns = await connection.fetch(
        """SELECT a.attname, EXISTS(SELECT 1 FROM pg_index i
        WHERE i.indrelid=a.attrelid AND i.indisprimary AND i.indisvalid AND i.indisready
            AND i.indnkeyatts=1 AND i.indkey[0]=a.attnum AND a.attname='location_key' AND a.attnotnull) AS copy_key
        FROM pg_attribute a WHERE a.attrelid=to_regclass($1) AND a.attnum>0 AND NOT a.attisdropped
        ORDER BY a.attnum""",
        address_table,
    )
    if not any(column["copy_key"] for column in columns):
        raise NetworkAddressProjectionError("Pinned address source requires its native location primary key")
    expressions = [
        "COALESCE(selected.network_ids,'{}'::integer[]) AS canonical_network_ids"
        if column["attname"] == "canonical_network_ids"
        else "a." + _identifier(column["attname"])
        for column in columns
    ]
    if not any(column["attname"] == "canonical_network_ids" for column in columns):
        expressions.append("COALESCE(selected.network_ids,'{}'::integer[]) AS canonical_network_ids")
    return ",".join(expressions)


async def _copy_projection_rows(connection, schema_name, projected_query):
    # Import at the call site: bootstrap sources reuse the pinned source DTO above.
    from process.network_bootstrap_sources import (
        _COPY_BATCH_BYTES,
        _COPY_BATCH_ROWS,
        _COPY_FRAMING_BYTES,
        NetworkBootstrapSourceError,
        _copy_native_batch,
        _CopyBatchTooLarge,
    )

    copied_rows, last_key, batch_rows = 0, None, _COPY_BATCH_ROWS
    while True:
        predicate, arguments = ("TRUE", ()) if last_key is None else ("a.location_key>$1", (last_key,))
        batch_query = f"{projected_query} WHERE {predicate} ORDER BY a.location_key LIMIT {batch_rows}"
        bounds = await connection.fetchrow(
            "SELECT count(*)::bigint AS row_count,max(location_key) AS last_key,"
            "COALESCE(sum(octet_length(record_send(batch))::bigint),0)::bigint AS native_bytes "
            f"FROM ({batch_query}) batch",
            *arguments,
        )
        count = bounds["row_count"]
        if count == 0:
            return copied_rows
        if bounds["native_bytes"] + _COPY_FRAMING_BYTES > _COPY_BATCH_BYTES:
            if count == 1:
                raise NetworkAddressProjectionError("One projected row exceeds the 64 MiB COPY bound")
            batch_rows = max(1, count // 2)
            continue
        query = f"{projected_query} WHERE {predicate} AND a.location_key<=${len(arguments) + 1} ORDER BY a.location_key"
        try:
            await _copy_native_batch(
                connection, query, (*arguments, bounds["last_key"]), schema_name, "entity_address_unified", count
            )
        except _CopyBatchTooLarge:
            if count == 1:
                raise NetworkAddressProjectionError("One projected row exceeds the 64 MiB COPY bound") from None
            batch_rows = max(1, count // 2)
            continue
        except NetworkBootstrapSourceError as error:
            raise NetworkAddressProjectionError(str(error)) from error
        copied_rows += count
        last_key = bounds["last_key"]


async def _build_projection(connection, schema_name, projection_table, address_table, membership_table, binding_table):
    columns = await _projection_columns(connection, address_table)
    selected_table = _identifier(schema_name) + ".network_projection_memberships"
    await connection.execute(f"""
        CREATE TABLE {selected_table} AS
        SELECT b.location_key, b.entity_type, b.entity_id,
                array_agg(DISTINCT m.network_id ORDER BY m.network_id) AS network_ids
        FROM {membership_table} m JOIN {binding_table} b
              ON b.provider_system=m.provider_system AND b.provider_id=m.provider_id AND b.location_id=m.location_id
        GROUP BY b.location_key,b.entity_type,b.entity_id
    """)
    await connection.execute(f"ALTER TABLE {selected_table} ADD PRIMARY KEY(location_key,entity_type,entity_id)")
    projected_query = (
        f"SELECT {columns} FROM {address_table} a LEFT JOIN {selected_table} selected "
        "ON a.location_key=selected.location_key AND a.entity_type=selected.entity_type AND a.entity_id=selected.entity_id"
    )
    await connection.execute(f"CREATE TABLE {projection_table} AS {projected_query} WITH NO DATA")
    await connection.execute(f"""ALTER TABLE {projection_table}
        ALTER COLUMN canonical_network_ids TYPE INTEGER[],
        ALTER COLUMN canonical_network_ids SET DEFAULT '{{}}'::integer[],
        ALTER COLUMN canonical_network_ids SET NOT NULL""")
    copied_rows = await _copy_projection_rows(connection, schema_name, projected_query)
    await connection.execute(f"DROP TABLE {selected_table}")
    await connection.execute(f"ALTER TABLE {projection_table} ADD PRIMARY KEY(location_key)")
    await connection.execute(
        f"CREATE INDEX canonical_network_ids_gin ON {projection_table} USING GIN(canonical_network_ids gin__int_ops)"
    )
    await connection.execute(f"ANALYZE {projection_table}")
    address_rows = await connection.fetchval(f"SELECT count(*) FROM {projection_table}")
    if address_rows != copied_rows:
        raise NetworkAddressProjectionError("Projected COPY accounting differs")
    return address_rows


async def _project_candidate(connection, copy_target, address_source, control_schema):
    candidate_schema = _identifier(copy_target.schema_name)
    address_table = f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
    membership_table = f"{candidate_schema}.network_membership"
    binding_table = f"{candidate_schema}.provider_location_binding"
    projection_table = f"{candidate_schema}.entity_address_unified"
    async with connection.transaction():
        candidate = await _lock_candidate(connection, copy_target, address_source, control_schema)
        await connection.execute(f"LOCK TABLE {membership_table},{binding_table} IN SHARE MODE")
        await connection.execute(
            f"CREATE INDEX IF NOT EXISTS network_membership_projection_idx ON {membership_table}"
            "(provider_system,provider_id,location_id,network_id)"
        )
        accounting = await _membership_accounting(connection, membership_table, binding_table, address_table)
        if (
            accounting["membership_rows"] != candidate["accepted_rows"]
            or candidate["accepted_rows"] != candidate["expected_rows"]
        ):
            raise NetworkAddressProjectionError("Candidate membership accounting is incomplete")
        address_rows = await _build_projection(
            connection, copy_target.schema_name, projection_table, address_table, membership_table, binding_table
        )
    return NetworkAddressProjectionReceipt(
        copy_target.candidate_id,
        copy_target.schema_name,
        address_source.generation_id,
        address_rows,
        accounting["membership_rows"],
        accounting["distinct_memberships"],
        accounting["projected_locations"],
        accounting["orphan_bindings"],
    )


async def project_network_address_arrays(connection, copy_target, source, *, control_schema=None):
    """Build only a candidate under the caller transaction and a local savepoint.

    The source DTO is trusted operator input for a retained immutable physical
    table. Location keys are legacy text keys; site UUIDs join through explicit
    reviewed bindings. Publication, compatibility readers and write privileges
    belong to the integrating control path.
    """
    if type(copy_target) is not MembershipCopyTarget or type(source) is not PinnedAddressSource:
        raise NetworkAddressProjectionError("Trusted candidate and pinned address source are required")
    if not connection.is_in_transaction():
        raise NetworkAddressProjectionError("Projection requires a caller-owned transaction")
    if source.schema_name == copy_target.schema_name:
        raise NetworkAddressProjectionError("Pinned source must be outside the projection candidate")
    return await _project_candidate(connection, copy_target, source, control_schema or registry_schema())
