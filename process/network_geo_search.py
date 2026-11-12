# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build and verify a narrow immutable search projection from exact compact offices."""

import hashlib
import json
import re
from dataclasses import asdict
from uuid import UUID

from process.network_compact_layout import CompactAddressRelation, CompactNetworkLayout, compact_identifier
from process.network_index_catalog import index_catalog_statement
from process.network_membership_copy import MembershipCopyTarget

TABLE = "network_geo_search"
CONTRACT = "network_geo_search.postgres.v1"
SEARCH_COLUMNS = (
    "location_key",
    "npi",
    "address_key",
    "lat",
    "long",
    "taxonomy_array",
    "type",
    "address_precision",
    "row_origin",
    "state_name",
    "zip5",
    "postal_code",
    "procedures_array",
    "medications_array",
    "plans_network_array",
    "aca_plan_array",
    "aca_network_array",
    "ptg_plan_array",
    "ptg_source_array",
    "group_plan_array",
)
_COLUMNS = (*SEARCH_COLUMNS, "geo", "canonical_network_ids")
_BATCH_ROWS = 2000
_SCOPE_FIELDS = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
_FIELDS = {"contract", "scope", "table_name", "table_oid", "row_count", "sources", "indexes", "sha256"}
_INDEX_FIELDS = ("index_oid", "table_oid", "definition")
_GEO = "CASE WHEN a.lat IS NOT NULL AND a.long IS NOT NULL THEN public.Geography(public.ST_MakePoint(a.long::double precision,a.lat::double precision)) END"
_PREDICATE = "type IN ('primary','secondary','practice','site') AND COALESCE(address_precision,'')<>'city_zip' AND lat IS NOT NULL AND long IS NOT NULL"


class NetworkGeoSearchError(ValueError):
    """Search preparation or its immutable receipt differs from the exact source."""


def _require(condition):
    if not condition:
        raise NetworkGeoSearchError("Network geo search identity or parity differs")


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _qualified(relation):
    return compact_identifier(relation.schema_name) + "." + compact_identifier(relation.table_name)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


def _is_oid(value):
    return type(value) is int and 0 < value < 2**32


def _descriptor(value):
    _require(type(value) is dict and set(value) == _FIELDS and value["contract"] == CONTRACT)
    _require(type(value["scope"]) is dict and set(value["scope"]) == set(_SCOPE_FIELDS))
    try:
        target = MembershipCopyTarget(**value["scope"])
        sources = value["sources"]
        _require(type(sources) is dict and set(sources) == {"base", "custom", "ref_map", "artifacts"})
        from process.network_compact_layout import CompactEdgeArtifact

        layout = CompactNetworkLayout(
            CompactAddressRelation(**sources["ref_map"]),
            CompactAddressRelation(**sources["base"]),
            tuple(CompactEdgeArtifact(**entry) for entry in sources["artifacts"]),
            None if sources["custom"] is None else CompactAddressRelation(**sources["custom"]),
        )
    except (TypeError, KeyError, ValueError) as error:
        raise NetworkGeoSearchError("Network geo search source identity is invalid") from error
    _require(layout.ref_map.schema_name == target.schema_name and value["table_name"] == TABLE)
    _require(_is_oid(value["table_oid"]) and type(value["row_count"]) is int and value["row_count"] >= 0)
    _require(type(value["indexes"]) is list and len(value["indexes"]) == 2)
    for index in value["indexes"]:
        _require(type(index) is dict and set(index) == {"name", *_INDEX_FIELDS})
        _require(_is_oid(index["index_oid"]) and index["table_oid"] == value["table_oid"])
        _require(type(index["definition"]) is str and 0 < len(index["definition"]) < 8192)
    _require({index["name"] for index in value["indexes"]} == {TABLE + "_pkey", TABLE + "_gn"})
    _require(len({index["index_oid"] for index in value["indexes"]}) == 2)
    _require(value["sha256"] == _digest({key: entry for key, entry in value.items() if key != "sha256"}))
    return value


def geo_search_receipt(candidate):
    """Absent legacy receipts stay absent; present receipts bind the candidate and inputs."""
    report = _json(candidate.get("validation_json")) or {}
    serving = report.get("serving_readiness") or {}
    if "geo_search" not in serving:
        return None
    descriptor = _descriptor(serving["geo_search"])
    _require(descriptor["scope"] == {field: str(candidate[field]) for field in _SCOPE_FIELDS})
    sources = descriptor["sources"]
    canonical = report.get("candidate_readiness", {})
    _require(sources["ref_map"]["table_oid"] == canonical.get("ref_map_oid"))
    _require(sources["base"]["table_oid"] == serving.get("address_artifact", {}).get("table", {}).get("oid"))
    expected_artifacts = [
        {key: entry[key] for key in ("candidate_id", "schema_name", "table_oid")}
        for entry in serving.get("edge_artifacts", ())
    ]
    _require(sources["artifacts"] == expected_artifacts)
    return json.loads(json.dumps(descriptor))


def geo_search_relation(candidate):
    """Return a coordinate only after the candidate writer closure pins its indexes."""
    descriptor = geo_search_receipt(candidate)
    if descriptor is None:
        return None
    closure = _json(candidate["validation_json"]).get("writer_closure", {})
    _require(closure.get("relation_oids", {}).get(TABLE) == descriptor["table_oid"])
    for index in descriptor["indexes"]:
        _require(closure.get("index_oids", {}).get(index["name"]) == index["index_oid"])
        _require(closure.get("index_table_oids", {}).get(index["name"]) == descriptor["table_oid"])
    return CompactAddressRelation(candidate["schema_name"], TABLE, descriptor["table_oid"])


def require_geo_search_index_records(indexes, descriptor):
    """Require the same live native indexes from the caller's complete catalog inventory."""
    _descriptor(descriptor)
    _require_index_profile(descriptor)
    by_oid = {index["index_oid"]: index for index in indexes}
    for expected in descriptor["indexes"]:
        actual = by_oid.get(expected["index_oid"])
        _require(actual is not None and all(actual.get(field) == expected[field] for field in _INDEX_FIELDS))
        _require(all(actual.get(flag) is True for flag in ("indisvalid", "indisready", "indislive")))


def _normalized_index_definition(definition):
    """Remove only native deparser decoration, preserving operators and all key values."""
    definition = re.sub(r"::(?:character varying|text)(?:\[\])?", "", definition)
    return re.sub(r'[()\s"]+', "", definition.replace("public.gist__intbig_ops", "gist__intbig_ops"))


def _require_index_profile(descriptor):
    table = descriptor["scope"]["schema_name"] + "." + TABLE
    predicate = "type=ANY ARRAY['primary','secondary','practice','site'] AND COALESCE(address_precision,'')<>'city_zip' AND lat IS NOT NULL AND long IS NOT NULL"
    expected_by_name = {
        TABLE + "_pkey": f"CREATE UNIQUE INDEX {TABLE}_pkey ON {table} USING btree(location_key)",
        TABLE
        + "_gn": f"CREATE INDEX {TABLE}_gn ON {table} USING gist(geo,canonical_network_ids gist__intbig_ops) WHERE {predicate}",
    }
    for index in descriptor["indexes"]:
        expected_definition = expected_by_name[index["name"]]
        _require(
            re.findall(r"'(?:''|[^'])*'", index["definition"]) == re.findall(r"'(?:''|[^'])*'", expected_definition)
        )
        _require(_normalized_index_definition(index["definition"]) == _normalized_index_definition(expected_definition))


async def _index_records(connection, table_oid):
    statement = index_catalog_statement(
        """SELECT c.relname AS name,i.indexrelid::bigint AS index_oid,i.indrelid::bigint AS table_oid,
        i.indisvalid,i.indisready,i.indislive,pg_get_indexdef(i.indexrelid) AS definition
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=$1::oid ORDER BY c.relname""",
    )
    return sorted(_json(await connection.fetchval(statement, table_oid)), key=lambda index: index["name"])


async def verify_geo_search_indexes(connection, descriptor):
    """Recheck physical identity under the existing transaction before publication."""
    _descriptor(descriptor)
    _require(connection.is_in_transaction())
    relation = CompactAddressRelation(descriptor["scope"]["schema_name"], TABLE, descriptor["table_oid"])
    await _pin_relation(connection, relation)
    require_geo_search_index_records(await _index_records(connection, descriptor["table_oid"]), descriptor)


async def _pin_relation(connection, relation):
    qualified = _qualified(relation)
    _require(await connection.fetchval("SELECT to_regclass($1)::oid::bigint", qualified) == relation.table_oid)
    await connection.execute(f"LOCK TABLE ONLY {qualified} IN ACCESS SHARE MODE")
    _require(await connection.fetchval("SELECT to_regclass($1)::oid::bigint", qualified) == relation.table_oid)


async def _has_compatible_sources(connection, layout):
    """Only missing columns mean an old profile; incompatible present types fail closed."""
    profiles = []
    for relation in (layout.base, *((layout.custom,) if layout.custom is not None else ())):
        await _pin_relation(connection, relation)
        attributes = await connection.fetch(
            """SELECT attname,atttypid,atttypmod,attcollation FROM pg_attribute
            WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped AND attname=ANY($2::text[])""",
            relation.table_oid,
            SEARCH_COLUMNS,
        )
        columns_by_name = {
            entry["attname"]: tuple(entry[field] for field in ("atttypid", "atttypmod", "attcollation"))
            for entry in attributes
        }
        if set(columns_by_name) != set(SEARCH_COLUMNS):
            return False
        profiles.append(columns_by_name)
    _require(all(columns_by_name == profiles[0] for columns_by_name in profiles))
    return True


def _batch_ctes(layout, source_relation, *, is_base, has_after=False):
    columns = ",".join("a." + compact_identifier(name) for name in SEARCH_COLUMNS)
    override = ""
    if is_base:
        override = " WHERE a.row_origin<>'network_custom'"
        if layout.custom is not None:
            override += f" AND NOT EXISTS(SELECT FROM ONLY {_qualified(layout.custom)} custom WHERE custom.location_key=a.location_key)"
    memberships = []
    for index, artifact in enumerate(layout.artifacts, 2):
        edge = compact_identifier(artifact.schema_name) + ".network_office_edge"
        memberships.append(f"""SELECT a.location_key,ref.network_id FROM effective a
          JOIN LATERAL (SELECT slice_id FROM ONLY {edge} edge
            WHERE edge.location_key=a.location_key OFFSET 0) edge ON TRUE
          JOIN ONLY {_qualified(layout.ref_map)} ref ON ref.slice_id=edge.slice_id
          AND ref.artifact_candidate_id=${index}::uuid
          AND ref.office_bucket=(get_byte(sha256(convert_to(a.location_key,'UTF8')),0)&63)""")
    membership_sql = (
        " UNION ALL ".join(memberships) or "SELECT NULL::text AS location_key,NULL::integer AS network_id WHERE FALSE"
    )
    keyset_predicate = "a.location_key>$1::text" if has_after else "$1::text IS NULL"
    return f"""source_page AS MATERIALIZED (
      SELECT {columns} FROM ONLY {_qualified(source_relation)} a WHERE {keyset_predicate}
      ORDER BY a.location_key LIMIT {_BATCH_ROWS}
    ), effective AS MATERIALIZED (SELECT {columns} FROM source_page a{override}),
    memberships AS ({membership_sql}), network_arrays AS (
      SELECT location_key,array_agg(DISTINCT network_id ORDER BY network_id) AS canonical_network_ids
      FROM memberships GROUP BY location_key
    ), expected AS MATERIALIZED (
      SELECT {columns},{_GEO} AS geo,network_arrays.canonical_network_ids
      FROM effective a JOIN network_arrays USING(location_key)
    )"""


async def _source_pass(connection, target_table, layout, source, *, is_base, verify):
    """Keep both generation and exact comparison bounded to one physical key page."""
    columns = ",".join(compact_identifier(name) for name in _COLUMNS)
    if verify:
        tail = f""", actual AS MATERIALIZED (
          SELECT p.* FROM ONLY {target_table} p JOIN effective e USING(location_key)
        ) SELECT (SELECT max(location_key) FROM source_page) AS last_key,
          (SELECT count(*) FROM expected) AS row_count,
          EXISTS((SELECT {columns} FROM expected EXCEPT SELECT {columns} FROM actual)
            UNION ALL (SELECT {columns} FROM actual EXCEPT SELECT {columns} FROM expected)) AS differs"""
    else:
        tail = f""", inserted AS (INSERT INTO {target_table} ({columns}) SELECT {columns} FROM expected
          RETURNING location_key)
        SELECT (SELECT max(location_key) FROM source_page) AS last_key,
          (SELECT count(*) FROM inserted) AS row_count,FALSE AS differs"""
    statements = tuple(
        "WITH " + _batch_ctes(layout, source, is_base=is_base, has_after=has_after) + tail
        for has_after in (False, True)
    )
    after, total = None, 0
    while True:
        outcome = await connection.fetchrow(
            statements[after is not None], after, *(UUID(artifact.candidate_id) for artifact in layout.artifacts)
        )
        _require(outcome["differs"] is False)
        if outcome["last_key"] is None:
            return total
        _require(after is None or outcome["last_key"] != after)
        after, total = outcome["last_key"], total + outcome["row_count"]


async def _build_projection(connection, target, layout):
    table = compact_identifier(target.schema_name) + "." + compact_identifier(TABLE)
    columns = ",".join("a." + compact_identifier(name) for name in SEARCH_COLUMNS)
    await connection.execute(
        f"CREATE TABLE {table} AS SELECT {columns},{_GEO} AS geo,ARRAY[]::integer[] AS canonical_network_ids FROM ONLY {_qualified(layout.base)} a WITH NO DATA"
    )
    await connection.execute(
        f"ALTER TABLE {table} ADD PRIMARY KEY(location_key), ALTER COLUMN canonical_network_ids SET NOT NULL, ADD CHECK(cardinality(canonical_network_ids)>0 AND array_position(canonical_network_ids,NULL) IS NULL)"
    )
    sources = ((layout.base, True),) + (((layout.custom, False),) if layout.custom is not None else ())
    inserted = sum(
        [
            await _source_pass(connection, table, layout, source, is_base=is_base, verify=False)
            for source, is_base in sources
        ]
    )
    verified = sum(
        [
            await _source_pass(connection, table, layout, source, is_base=is_base, verify=True)
            for source, is_base in sources
        ]
    )
    _require(inserted == verified == await connection.fetchval(f"SELECT count(*) FROM ONLY {table}"))
    await connection.execute(
        f"CREATE INDEX {TABLE}_gn ON {table} USING gist (geo,canonical_network_ids public.gist__intbig_ops) WHERE {_PREDICATE}"
    )
    await connection.execute(f"ANALYZE {table}")
    return await connection.fetchval("SELECT to_regclass($1)::oid::bigint", table), verified


async def prepare_network_geo_search(connection, copy_target, layout):
    """Build only inside the caller's frozen candidate transaction and original limits."""
    _require(type(copy_target) is MembershipCopyTarget and type(layout) is CompactNetworkLayout)
    _require(connection.is_in_transaction() and layout.ref_map.schema_name == copy_target.schema_name)
    if not await _has_compatible_sources(connection, layout):
        return None
    await _pin_relation(connection, layout.ref_map)
    referenced_artifacts = await connection.fetch(
        f"SELECT DISTINCT artifact_candidate_id::text AS candidate_id FROM ONLY {_qualified(layout.ref_map)}"
    )
    _require(
        {entry["candidate_id"] for entry in referenced_artifacts}
        == {artifact.candidate_id for artifact in layout.artifacts}
    )
    for artifact in layout.artifacts:
        await _pin_relation(
            connection, CompactAddressRelation(artifact.schema_name, "network_office_edge", artifact.table_oid)
        )
    async with connection.transaction():
        table_oid, count = await _build_projection(connection, copy_target, layout)
        descriptor_by_field = {
            "contract": CONTRACT,
            "scope": {field: getattr(copy_target, field) for field in _SCOPE_FIELDS},
            "table_name": TABLE,
            "table_oid": table_oid,
            "row_count": count,
            "sources": json.loads(json.dumps(asdict(layout))),
            "indexes": [
                {field: index_record[field] for field in ("name", *_INDEX_FIELDS)}
                for index_record in await _index_records(connection, table_oid)
            ],
        }
        descriptor_by_field["sha256"] = _digest(descriptor_by_field)
        await verify_geo_search_indexes(connection, descriptor_by_field)
        return descriptor_by_field
