# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare immutable office slices without copying the pinned address heap."""

import hashlib
from dataclasses import dataclass
from uuid import UUID, uuid4

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier, _lock_candidate
from process.network_membership_copy import MembershipCopyTarget

SLICE_CONTRACT_VERSION = 1
OFFICE_BUCKET_COUNT = 64
_BATCH_ROWS = 4096
_METADATA_BATCH_ROWS = 256
_COPY_BYTE_LIMIT = 16 * 1024 * 1024
_DIGEST_DOMAIN = b"network-office-slice-v1\0"


class NetworkMembershipSliceError(ValueError):
    """Compact preparation failed without publishing a generation."""


@dataclass(frozen=True)
class PinnedMembershipSlices:
    candidate_id: str
    schema_name: str
    ref_map_oid: int
    slice_contract_version: int = SLICE_CONTRACT_VERSION

    def __post_init__(self):
        _identifier(self.schema_name)
        try:
            candidate_id = UUID(self.candidate_id) if type(self.candidate_id) is str else None
        except ValueError:
            candidate_id = None
        if (
            candidate_id is None
            or candidate_id.int == 0
            or str(candidate_id) != self.candidate_id
            or self.schema_name != "network_candidate_" + candidate_id.hex
            or type(self.ref_map_oid) is not int
            or not 0 < self.ref_map_oid <= 4294967295
            or type(self.slice_contract_version) is not int
            or self.slice_contract_version != SLICE_CONTRACT_VERSION
        ):
            raise NetworkMembershipSliceError("Pinned slice identity or contract is invalid")


@dataclass(frozen=True)
class AffectedMembershipBuckets:
    schema_name: str
    table_name: str
    relation_oid: int

    def __post_init__(self):
        _identifier(self.schema_name)
        _identifier(self.table_name)
        if type(self.relation_oid) is not int or not 0 < self.relation_oid <= 4294967295:
            raise NetworkMembershipSliceError("Affected bucket relation identity is invalid")


@dataclass(frozen=True)
class NetworkMembershipSliceReceipt:
    candidate_id: str
    source_generation: str
    slice_contract_version: int
    membership_rows: int
    distinct_memberships: int
    matched_locations: int
    total_slices: int
    reused_slices: int
    changed_slices: int
    removed_slices: int
    copied_edge_rows: int
    ref_map_oid: int
    edge_heap_oid: int


async def _pin_previous(connection, previous, copy_target):
    if previous is None:
        return None
    if type(previous) is not PinnedMembershipSlices or previous.candidate_id == copy_target.candidate_id:
        raise NetworkMembershipSliceError("Prior slices must identify a different immutable candidate")
    table = _identifier(previous.schema_name) + ".network_slice_ref"
    if await connection.fetchval("SELECT to_regclass($1)::oid", table) != previous.ref_map_oid:
        raise NetworkMembershipSliceError("Pinned slice reference relation changed")
    await connection.execute(f"LOCK TABLE {table} IN ACCESS SHARE MODE")
    if await connection.fetchval("SELECT to_regclass($1)::oid", table) != previous.ref_map_oid:
        raise NetworkMembershipSliceError("Pinned slice reference relation changed")
    invalid = await connection.fetchval(f"""SELECT EXISTS(
        SELECT 1 FROM {table} WHERE network_id IS NULL OR network_id NOT BETWEEN 1 AND 2147483647
            OR office_bucket IS NULL OR office_bucket NOT BETWEEN 0 AND 63
            OR artifact_candidate_id IS NULL OR artifact_candidate_id='00000000-0000-0000-0000-000000000000'
            OR slice_id IS NULL OR slice_id='00000000-0000-0000-0000-000000000000'
            OR row_count IS NULL OR row_count<=0 OR content_sha256 IS NULL OR content_sha256!~'^[0-9a-f]{{64}}$'
        UNION ALL SELECT 1 FROM {table} GROUP BY network_id,office_bucket HAVING count(*)<>1)
    """)
    if invalid:
        raise NetworkMembershipSliceError("Pinned slice references are invalid")
    return table


async def _pin_affected(connection, affected, copy_target, previous):
    if affected is None:
        return None
    if (
        type(affected) is not AffectedMembershipBuckets
        or affected.schema_name != copy_target.schema_name
        or previous is None
    ):
        raise NetworkMembershipSliceError("Affected buckets require an owned relation and a prior generation")
    table = _identifier(affected.schema_name) + "." + _identifier(affected.table_name)
    if await connection.fetchval("SELECT to_regclass($1)::oid", table) != affected.relation_oid:
        raise NetworkMembershipSliceError("Affected bucket relation changed")
    await connection.execute(f"LOCK TABLE {table} IN SHARE MODE")
    if await connection.fetchval("SELECT to_regclass($1)::oid", table) != affected.relation_oid:
        raise NetworkMembershipSliceError("Affected bucket relation changed")
    if await connection.fetchval(f"""SELECT EXISTS(
        SELECT 1 FROM {table} WHERE network_id IS NULL OR network_id NOT BETWEEN 1 AND 2147483647
            OR office_bucket IS NULL OR office_bucket NOT BETWEEN 0 AND 63
        UNION ALL SELECT 1 FROM {table} GROUP BY network_id,office_bucket HAVING count(*)<>1)
    """):
        raise NetworkMembershipSliceError("Affected bucket keys are invalid")
    return table


async def _matched_facts(
    connection, candidate_schema, address_table, work_table, candidate, affected_table, office_relation=None
):
    indexed_address = await connection.fetchval(
        """SELECT EXISTS(
        SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
        WHERE i.indrelid=to_regclass($1) AND i.indisprimary AND i.indisvalid AND i.indisready
            AND i.indnkeyatts=1 AND a.attname='location_key' AND a.attnotnull)
    """,
        address_table,
    )
    if not indexed_address:
        raise NetworkMembershipSliceError("Pinned addresses require the native location primary key")
    affected_filter = (
        ""
        if affected_table is None
        else (
            f"WHERE EXISTS(SELECT 1 FROM {affected_table} changed WHERE changed.network_id=m.network_id "
            "AND changed.office_bucket=(get_byte(sha256(convert_to(b.location_key,'UTF8')),0)&63))"
        )
    )
    await connection.execute(f"""CREATE TEMP TABLE {work_table} ON COMMIT DROP AS
        SELECT m.network_id,a.location_key,
            (get_byte(sha256(convert_to(a.location_key,'UTF8')),0)&63)::integer AS office_bucket,
            count(*)::bigint AS membership_rows
        FROM {candidate_schema}.network_membership m
        LEFT JOIN {candidate_schema}.provider_location_binding b
          ON b.provider_system=m.provider_system AND b.provider_id=m.provider_id AND b.location_id=m.location_id
        LEFT JOIN LATERAL (
            SELECT a.location_key FROM {office_relation or address_table} a WHERE a.location_key=b.location_key
                AND a.entity_type=b.entity_type AND a.entity_id=b.entity_id LIMIT 1
        ) a ON TRUE {affected_filter}
        GROUP BY m.network_id,a.location_key
    """)
    accounting = await connection.fetchrow(f"""SELECT COALESCE(sum(membership_rows),0)::bigint AS membership_rows,
        COALESCE(sum(membership_rows) FILTER (WHERE network_id IS NULL OR network_id NOT BETWEEN 1 AND 2147483647
            OR location_key IS NULL OR length(location_key) NOT BETWEEN 1 AND 64),0)::bigint AS invalid_memberships,
        count(*) AS distinct_memberships,count(DISTINCT location_key) AS matched_locations FROM {work_table}
    """)
    if accounting["invalid_memberships"]:
        raise NetworkMembershipSliceError("Selected memberships lack an exact provider/site/address binding")
    if candidate["accepted_rows"] != candidate["expected_rows"] or (
        affected_table is None and accounting["membership_rows"] != candidate["accepted_rows"]
    ):
        raise NetworkMembershipSliceError("Candidate membership accounting is incomplete")
    await connection.execute(f'CREATE UNIQUE INDEX ON {work_table}(network_id,office_bucket,location_key COLLATE "C")')
    await connection.execute(f"ANALYZE {work_table}")
    return accounting


async def _create_slice_relations(connection, candidate_schema):
    await connection.execute(f"""CREATE TABLE {candidate_schema}.network_slice_ref(
        network_id integer NOT NULL CHECK(network_id>0),
        office_bucket integer NOT NULL CHECK(office_bucket BETWEEN 0 AND 63),
        artifact_candidate_id uuid NOT NULL CHECK(artifact_candidate_id<>'00000000-0000-0000-0000-000000000000'),
        slice_id uuid NOT NULL CHECK(slice_id<>'00000000-0000-0000-0000-000000000000'),
        row_count bigint NOT NULL CHECK(row_count>0),
        content_sha256 varchar(64) NOT NULL CHECK(content_sha256~'^[0-9a-f]{{64}}$'),
        PRIMARY KEY(network_id,office_bucket))
    """)
    await connection.execute(f"""CREATE TABLE {candidate_schema}.network_office_edge(
        slice_id uuid NOT NULL CHECK(slice_id<>'00000000-0000-0000-0000-000000000000'),
        location_key varchar(64) NOT NULL CHECK(length(location_key)>0), PRIMARY KEY(slice_id,location_key))
    """)
    await connection.execute(
        f"CREATE INDEX network_office_edge_location_idx ON {candidate_schema}.network_office_edge(location_key,slice_id)"
    )


async def _copy_ref_page(connection, copy_target, page, previous_table):
    prior_by_key = {}
    if previous_table:
        prior_by_key = {
            (entry["network_id"], entry["office_bucket"]): entry
            for entry in await connection.fetch(
                f"SELECT p.* FROM {previous_table} p JOIN unnest($1::integer[],$2::integer[]) changed(network_id,office_bucket) "
                "USING(network_id,office_bucket)",
                [row[0] for row in page],
                [row[1] for row in page],
            )
        }
    records, reused = [], 0
    for network_id, bucket, row_count, digest in page:
        prior = prior_by_key.get((network_id, bucket))
        can_reuse = prior is not None and prior["row_count"] == row_count and prior["content_sha256"] == digest
        artifact = prior["artifact_candidate_id"] if can_reuse else UUID(copy_target.candidate_id)
        slice_id = prior["slice_id"] if can_reuse else uuid4()
        records.append((network_id, bucket, artifact, slice_id, row_count, digest))
        reused += int(can_reuse)
    await connection.copy_records_to_table("network_slice_ref", schema_name=copy_target.schema_name, records=records)
    return reused


async def _prepare_refs(connection, copy_target, work_table, previous_table):
    current_key, digest, row_count = None, None, 0
    page, total_slices, reused_slices = [], 0, 0
    query = f"""SELECT network_id,office_bucket,
        int4send(octet_length(convert_to(location_key,'UTF8')))||convert_to(location_key,'UTF8') AS frame
        FROM {work_table} ORDER BY network_id,office_bucket,location_key COLLATE "C"
    """
    async for entry in connection.cursor(query, prefetch=_BATCH_ROWS):
        key = (entry["network_id"], entry["office_bucket"])
        if key != current_key:
            if current_key is not None:
                page.append((*current_key, row_count, digest.hexdigest()))
            if len(page) == _METADATA_BATCH_ROWS:
                reused_slices += await _copy_ref_page(connection, copy_target, page, previous_table)
                total_slices += len(page)
                page.clear()
            current_key, digest, row_count = key, hashlib.sha256(_DIGEST_DOMAIN), 0
        digest.update(entry["frame"])
        row_count += 1
    if current_key is not None:
        page.append((*current_key, row_count, digest.hexdigest()))
    if page:
        reused_slices += await _copy_ref_page(connection, copy_target, page, previous_table)
        total_slices += len(page)
    return total_slices, reused_slices


async def _copy_changed_edges(connection, copy_target, work_table):
    from process.network_bootstrap_sources import NetworkBootstrapSourceError, _copy_native_batch

    ref_table = _identifier(copy_target.schema_name) + ".network_slice_ref"
    last_network, last_bucket, last_key = 0, -1, ""
    copied_rows = 0
    while True:
        selected = f"""SELECT f.network_id,f.office_bucket,f.location_key,r.slice_id FROM {work_table} f
            JOIN {ref_table} r USING(network_id,office_bucket) WHERE r.artifact_candidate_id=$1
                AND (f.network_id,f.office_bucket,f.location_key COLLATE "C")>($2,$3,$4 COLLATE "C")
            ORDER BY f.network_id,f.office_bucket,f.location_key COLLATE "C" LIMIT {_BATCH_ROWS}"""
        arguments = (UUID(copy_target.candidate_id), last_network, last_bucket, last_key)
        bounds = await connection.fetchrow(
            f"SELECT count(*) OVER() AS row_count,* FROM ({selected}) batch "
            'ORDER BY network_id DESC,office_bucket DESC,location_key COLLATE "C" DESC LIMIT 1',
            *arguments,
        )
        if bounds is None:
            return copied_rows
        try:
            await _copy_native_batch(
                connection,
                f"SELECT slice_id,location_key FROM ({selected}) batch",
                arguments,
                copy_target.schema_name,
                "network_office_edge",
                bounds["row_count"],
                byte_limit=_COPY_BYTE_LIMIT,
            )
        except NetworkBootstrapSourceError as error:
            raise NetworkMembershipSliceError(str(error)) from error
        copied_rows += bounds["row_count"]
        last_network, last_bucket, last_key = bounds["network_id"], bounds["office_bucket"], bounds["location_key"]


async def _office_relation(connection, copy_target, address_source, office_sources):
    if office_sources is None:
        return _identifier(address_source.schema_name) + "." + _identifier(address_source.table_name), None
    from process.network_compact_offices import (
        CompactOfficeSources,
        compact_base_relation_sql,
        pin_compact_office_sources,
    )

    if type(office_sources) is not CompactOfficeSources or office_sources.base != address_source:
        raise NetworkMembershipSliceError("Compact office source differs from the pinned addresses")
    relation = await pin_compact_office_sources(connection, copy_target, office_sources)
    return compact_base_relation_sql(office_sources), relation


async def _prepare_slices(connection, copy_target, address_source, previous, affected, control_schema, office_sources):
    """Prepare one complete immutable slice payload and its physical receipt."""
    schema = _identifier(copy_target.schema_name)
    work_table = _identifier("network_slice_work_" + uuid4().hex)
    async with connection.transaction():
        candidate = await _lock_candidate(connection, copy_target, address_source, control_schema)
        await connection.execute(
            f"LOCK TABLE {schema}.network_membership,{schema}.provider_location_binding IN SHARE MODE"
        )
        previous_table = await _pin_previous(connection, previous, copy_target)
        affected_table = await _pin_affected(connection, affected, copy_target, previous)
        address_table, office_relation = await _office_relation(connection, copy_target, address_source, office_sources)
        accounting = await _matched_facts(
            connection, schema, address_table, work_table, candidate, affected_table, office_relation
        )
        await _create_slice_relations(connection, schema)
        carried_slices = 0
        if affected_table:
            await connection.execute(f"""INSERT INTO {schema}.network_slice_ref SELECT p.* FROM {previous_table} p
                WHERE NOT EXISTS(SELECT 1 FROM {affected_table} changed
                    WHERE changed.network_id=p.network_id AND changed.office_bucket=p.office_bucket)""")
            carried_slices = await connection.fetchval(f"SELECT count(*) FROM {schema}.network_slice_ref")
        total_slices, reused_slices = await _prepare_refs(connection, copy_target, work_table, previous_table)
        copied_rows = await _copy_changed_edges(connection, copy_target, work_table)
        counts = await connection.fetchrow(
            "SELECT COALESCE(sum(row_count) FILTER(WHERE artifact_candidate_id=$1),0)::bigint AS changed_rows,"
            f"COALESCE(sum(row_count),0)::bigint AS distinct_memberships FROM {schema}.network_slice_ref",
            UUID(copy_target.candidate_id),
        )
        if copied_rows != counts["changed_rows"]:
            raise NetworkMembershipSliceError("Changed edge COPY accounting differs")
        removed_slices = (
            0
            if previous_table is None
            else await connection.fetchval(f"""SELECT count(*) FROM {previous_table} p
            WHERE NOT EXISTS(SELECT 1 FROM {schema}.network_slice_ref r
                WHERE r.network_id=p.network_id AND r.office_bucket=p.office_bucket)""")
        )
        await connection.execute(f"ANALYZE {schema}.network_slice_ref")
        await connection.execute(f"ANALYZE {schema}.network_office_edge")
        await connection.execute(f"DROP TABLE {work_table}")
        ref_oid, edge_oid = await _slice_relation_oids(connection, copy_target.schema_name)
    return NetworkMembershipSliceReceipt(
        copy_target.candidate_id,
        address_source.generation_id,
        SLICE_CONTRACT_VERSION,
        candidate["accepted_rows"],
        counts["distinct_memberships"],
        accounting["matched_locations"],
        total_slices + carried_slices,
        reused_slices + carried_slices,
        total_slices - reused_slices,
        removed_slices,
        copied_rows,
        ref_oid,
        edge_oid,
    )


async def _slice_relation_oids(connection, schema_name):
    return await connection.fetchrow(
        "SELECT to_regclass($1)::oid,to_regclass($2)::oid",
        schema_name + ".network_slice_ref",
        schema_name + ".network_office_edge",
    )


async def prepare_network_membership_slices(
    connection, copy_target, source, *, previous=None, affected_buckets=None, control_schema=None, office_sources=None
):
    """Prepare compact relations under a caller transaction and a local savepoint.

    The integrating path must verify closed write authority and custody of the
    candidate, prior refs/artifacts and pinned addresses before calling. This
    helper validates facts and physical reference identity, but grants no writer
    or publication authority. It neither reads old edge heaps nor copies addresses.
    Bucket version 1 is the first SHA256 UTF-8 location-key byte masked with 63.
    Content version 1 hashes a domain followed by C-ordered, int32-length-framed
    UTF-8 keys. Affected buckets are trusted complete change coverage; the caller
    must validate untouched facts before selective preparation. Matched locations
    count only examined facts when affected buckets are supplied.
    """
    if type(copy_target) is not MembershipCopyTarget or type(source) is not PinnedAddressSource:
        raise NetworkMembershipSliceError("Trusted candidate and pinned address source are required")
    if not connection.is_in_transaction():
        raise NetworkMembershipSliceError("Slice preparation requires a caller-owned transaction")
    if source.schema_name == copy_target.schema_name:
        raise NetworkMembershipSliceError("Pinned addresses must be outside the slice candidate")
    return await _prepare_slices(
        connection, copy_target, source, previous, affected_buckets, control_schema or registry_schema(), office_sources
    )
