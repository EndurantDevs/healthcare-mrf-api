# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded reads of a compact edition using portable schema-qualified names."""

from db.plan_release_storage import storage_relation
from process import provider_directory_compact_payload as compact
from process import provider_directory_generic_candidate as candidate

FORMAT_TABLE = "provider_directory_payload_format"
OBSERVATION_ALIASES = "provider_directory_resource_aliases"
OUTPUT_ALIASES = "provider_directory_resource_output_aliases"
REFERENCE_SETS = "provider_directory_resource_reference_sets"
BODY_BLOCKS = "provider_directory_resource_body_blocks"
SHARED_BODIES = "provider_directory_resource_shared_bodies"
REFERENCE_TOKENS = "provider_directory_resource_reference_tokens"


def _require(condition, code):
    if not condition:
        raise candidate.GenericCandidateError("provider_directory_compact_edition_" + code)


async def compact_edition_tables(connection, schema):
    """Distinguish explicit compact layout metadata from an ordinary edition."""
    layout = storage_relation(schema, FORMAT_TABLE)
    aliases = storage_relation(schema, OUTPUT_ALIASES)
    presence = await connection.fetchrow(
        "SELECT to_regclass($1)::text IS NOT NULL AS format_present,"
        "to_regclass($2)::text IS NOT NULL AS aliases_present",
        layout,
        aliases,
    )
    _require(presence is not None, "layout_unavailable")
    if not presence["format_present"]:
        _require(not presence["aliases_present"], "format_missing")
        return None
    formats = await connection.fetch(f"SELECT * FROM {layout} LIMIT 2")
    _require(len(formats) == 1 and set(dict(formats[0])) == {"format_id"}, "format_changed")
    format_id = formats[0]["format_id"]
    _require(format_id in (compact.FORMAT, compact.SHARED_BODY_FORMAT, compact.COMPACT_V2_FORMAT), "format_changed")
    _require(presence["aliases_present"], "aliases_missing")
    if format_id == compact.FORMAT:
        return compact.CompactPayloadTables(schema, OUTPUT_ALIASES, REFERENCE_SETS, BODY_BLOCKS)
    if format_id == compact.SHARED_BODY_FORMAT:
        has_tokens = await connection.fetchval(
            "SELECT to_regclass($1)::text IS NOT NULL", storage_relation(schema, REFERENCE_TOKENS)
        )
        return compact.CompactPayloadTables(
            schema,
            OUTPUT_ALIASES,
            REFERENCE_SETS,
            BODY_BLOCKS,
            SHARED_BODIES,
            REFERENCE_TOKENS if has_tokens else None,
        )
    return compact.CompactPayloadTables(
        schema,
        OUTPUT_ALIASES,
        REFERENCE_SETS,
        BODY_BLOCKS,
        SHARED_BODIES,
        REFERENCE_TOKENS,
        is_v2=True,
    )


async def _edition_page_keys(
    connection, tables, dataset_id, *, resource_types, after=("", ""), page_rows=1000, page_bytes, npi=None
):
    """Select physical keys, NPI and byte charges before reconstructing JSON."""
    _require(connection.is_in_transaction(), "transaction_required")
    _require(type(dataset_id) is str and 0 < len(dataset_id) <= 96, "dataset")
    _require(type(page_rows) is int and 1 <= page_rows <= candidate.COPY_MAX_ROWS, "page_rows")
    _require(type(page_bytes) is int and 0 < page_bytes <= candidate.COPY_MAX_BYTES, "page_bytes")
    _require(
        type(after) is tuple and len(after) == 2 and all(type(cursor_value) is str for cursor_value in after), "cursor"
    )
    _require(
        type(resource_types) is tuple
        and bool(resource_types)
        and all(type(resource_type) is str and 0 < len(resource_type) <= 64 for resource_type in resource_types)
        and len(set(resource_types)) == len(resource_types),
        "resource_types",
    )
    _require(npi is None or type(npi) is str, "npi")
    npi_filter = " AND npi_text=$6 AND resource_type IN('Practitioner','PractitionerRole')" if npi is not None else ""
    select = f"SELECT resource_type,resource_id,batch_ordinal,row_ordinal,read_bytes FROM {tables.relation('aliases')} "
    order = ' ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT $5'
    # Two branches keep both the cursor and the type list as index keys.
    keys = await connection.fetch(
        f"SELECT * FROM (({select}"
        'WHERE dataset_id=$1 AND resource_type COLLATE "C"=$3::text AND $3::text=ANY($2::text[]) '
        'AND resource_id COLLATE "C">$4::text COLLATE "C"'
        f"{npi_filter}{order}) UNION ALL ({select}"
        'WHERE dataset_id=$1 AND resource_type COLLATE "C"=ANY($2::text[]) '
        'AND resource_type COLLATE "C">$3::text COLLATE "C"'
        f"{npi_filter}{order})) cursor_page{order}",
        dataset_id,
        list(resource_types),
        *after,
        page_rows,
        *((npi,) if npi is not None else ()),
    )
    selected, charged = [], candidate.COPY_FRAME_BYTES
    for key in keys:
        _require(type(key["read_bytes"]) is int and key["read_bytes"] > 0, "read_bytes")
        if charged + key["read_bytes"] > page_bytes:
            _require(bool(selected), "row_too_large")
            break
        selected.append(key)
        charged += key["read_bytes"]
    return selected


async def compact_edition_page(
    connection, tables, dataset_id, *, resource_types, after=("", ""), page_rows=1000, page_bytes, npi=None
):
    """Reconstruct only the physical keys admitted by the shared byte budget."""
    selected = await _edition_page_keys(
        connection,
        tables,
        dataset_id,
        resource_types=resource_types,
        after=after,
        page_rows=page_rows,
        page_bytes=page_bytes,
        npi=npi,
    )
    return await compact.read_compact_payload_keys(connection, tables, dataset_id, selected) if selected else []


async def compact_catalog_page(connection, tables, dataset_id, *, resource_types, after, page_rows, page_bytes):
    """Keep the catalog's JSONB normalization after bounding physical keys and bytes."""
    selected = await _edition_page_keys(
        connection,
        tables,
        dataset_id,
        resource_types=resource_types,
        after=after,
        page_rows=page_rows,
        page_bytes=page_bytes,
    )
    if not selected:
        return []
    resources = compact.compact_resource_sql(tables, observations=True, alias_cte="selected")
    rows = await connection.fetch(
        f"WITH selected AS MATERIALIZED (SELECT * FROM {tables.relation('aliases')} "
        "WHERE dataset_id=$1 AND (batch_ordinal,row_ordinal) IN "
        "(SELECT * FROM unnest($2::bigint[],$3::integer[]))) "
        "SELECT r.resource_type,r.resource_id,r.payload_hash,r.payload_json::jsonb::text AS payload_json,"
        f"a.read_bytes AS row_bytes FROM ({resources}) r JOIN selected a "
        "USING(dataset_id,batch_ordinal,row_ordinal) "
        'ORDER BY r.resource_type COLLATE "C",r.resource_id COLLATE "C"',
        dataset_id,
        [key["batch_ordinal"] for key in selected],
        [key["row_ordinal"] for key in selected],
    )
    _require(len(rows) == len(selected), "read_rows_missing")
    return rows
