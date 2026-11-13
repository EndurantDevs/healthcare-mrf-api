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
_FIELDS = {"contract", "scope", "table_name", "row_count", "sources", "indexes", "sha256"}
_INDEX_FIELDS = ("definition",)
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


def _descriptor(value):
    _require(type(value) is dict and set(value) in (_FIELDS, _FIELDS | {"table_oid"}) and value["contract"] == CONTRACT)
    _require(type(value["scope"]) is dict and set(value["scope"]) == set(_SCOPE_FIELDS))
    target = MembershipCopyTarget(**value["scope"])
    sources = value["sources"]
    _require(type(sources) is dict and set(sources) == {"base", "custom", "ref_map", "artifacts"})
    from process.network_compact_layout import CompactEdgeArtifact

    def relation(document):
        """Decode a schema-qualified search input without historical physical fields."""
        return CompactAddressRelation(document["schema_name"], document["table_name"])

    layout = CompactNetworkLayout(
        relation(sources["ref_map"]),
        relation(sources["base"]),
        tuple(CompactEdgeArtifact(entry["candidate_id"], entry["schema_name"]) for entry in sources["artifacts"]),
        None if sources["custom"] is None else relation(sources["custom"]),
    )
    _require(layout.ref_map.schema_name == target.schema_name and value["table_name"] == TABLE)
    _require(type(value["row_count"]) is int and value["row_count"] >= 0)
    _require(type(value["indexes"]) is list and len(value["indexes"]) == 2)
    indexes = [{field: index[field] for field in ("name", "definition")} for index in value["indexes"]]
    _require({index["name"] for index in indexes} == {TABLE + "_pkey", TABLE + "_gn"})
    _require(all(type(index["definition"]) is str and 0 < len(index["definition"]) < 8192 for index in indexes))
    return {
        **{key: value[key] for key in _FIELDS},
        "sources": json.loads(json.dumps(asdict(layout))),
        "indexes": indexes,
    }


def geo_search_receipt(candidate):
    """Read the published search projection's logical scope and qualified inputs."""
    report = _json(candidate.get("validation_json")) or {}
    serving = report.get("serving_readiness") or {}
    if "geo_search" not in serving:
        return None
    descriptor = _descriptor(serving["geo_search"])
    _require(descriptor["scope"] == {field: str(candidate[field]) for field in _SCOPE_FIELDS})
    expected_artifacts = [
        {key: entry[key] for key in ("candidate_id", "schema_name")} for entry in serving.get("edge_artifacts", ())
    ]
    _require(descriptor["sources"]["artifacts"] == expected_artifacts)
    return json.loads(json.dumps(descriptor))


def geo_search_relation(candidate):
    """Return the validated published search relation when one is available."""
    descriptor = geo_search_receipt(candidate)
    if descriptor is None:
        return None
    return CompactAddressRelation(candidate["schema_name"], descriptor["table_name"])


def require_geo_search_index_records(indexes, descriptor):
    """Check the required search index definitions before publication."""
    descriptor = _descriptor(descriptor)
    _require_index_profile(descriptor)
    by_name = {index["name"]: index for index in indexes}
    for expected in descriptor["indexes"]:
        actual = by_name.get(expected["name"])
        _require(actual is not None and actual["definition"] == expected["definition"])
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


async def _index_records(connection, relation_name):
    statement = index_catalog_statement(
        """SELECT c.relname AS name,i.indisvalid,i.indisready,i.indislive,
        pg_get_indexdef(i.indexrelid) AS definition
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        WHERE i.indrelid=to_regclass($1::text) ORDER BY c.relname""",
    )
    return sorted(_json(await connection.fetchval(statement, relation_name)), key=lambda index: index["name"])


async def verify_geo_search_indexes(connection, descriptor):
    """Check index readiness by the selected schema-qualified search table."""
    descriptor = _descriptor(descriptor)
    relation = CompactAddressRelation(descriptor["scope"]["schema_name"], descriptor["table_name"])
    require_geo_search_index_records(await _index_records(connection, _qualified(relation)), descriptor)


async def _has_compatible_sources(connection, layout):
    """Check column types needed by the bounded search projection."""
    profiles = []
    for relation in (layout.base, *((layout.custom,) if layout.custom is not None else ())):
        attributes = await connection.fetch(
            """SELECT attname,format_type(atttypid,atttypmod) AS type_name
            FROM pg_attribute WHERE attrelid=to_regclass($1::text)
              AND attnum>0 AND NOT attisdropped AND attname=ANY($2::text[])""",
            _qualified(relation),
            SEARCH_COLUMNS,
        )
        columns_by_name = {entry["attname"]: entry["type_name"] for entry in attributes}
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
    return verified


async def prepare_network_geo_search(connection, copy_target, layout):
    """Build only inside the caller's frozen candidate transaction and original limits."""
    _require(type(copy_target) is MembershipCopyTarget and type(layout) is CompactNetworkLayout)
    _require(connection.is_in_transaction() and layout.ref_map.schema_name == copy_target.schema_name)
    if not await _has_compatible_sources(connection, layout):
        return None
    referenced_artifacts = await connection.fetch(
        f"SELECT DISTINCT artifact_candidate_id::text AS candidate_id FROM ONLY {_qualified(layout.ref_map)}"
    )
    _require(
        {entry["candidate_id"] for entry in referenced_artifacts}
        == {artifact.candidate_id for artifact in layout.artifacts}
    )
    async with connection.transaction():
        count = await _build_projection(connection, copy_target, layout)
        descriptor_by_field = {
            "contract": CONTRACT,
            "scope": {field: getattr(copy_target, field) for field in _SCOPE_FIELDS},
            "table_name": TABLE,
            "row_count": count,
            "sources": json.loads(json.dumps(asdict(layout))),
            "indexes": [
                {field: index_record[field] for field in ("name", *_INDEX_FIELDS)}
                for index_record in await _index_records(
                    connection, compact_identifier(copy_target.schema_name) + "." + TABLE
                )
            ],
        }
        descriptor_by_field["sha256"] = _digest(descriptor_by_field)
        await verify_geo_search_indexes(connection, descriptor_by_field)
        return descriptor_by_field
