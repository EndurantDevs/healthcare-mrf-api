# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native preparation and content checks for typed membership slices."""

from uuid import UUID

from process.network_address_projection import _identifier
from process.registry_typed_membership_parity import COLUMNS, ORDER, _last_page_key, require_order_index

_DOMAIN = b"network-office-slice-v1\0"
_METADATA_ROWS = 256


def _parameters(start):
    types = ("integer", 'text COLLATE "C"', 'text COLLATE "C"', "uuid", 'text COLLATE "C"')
    return "(" + ",".join(f"${index}::{kind}" for index, kind in enumerate(types, start)) + ")"


def _page_range(after, upper):
    columns = "(" + ",".join("m." + term for term in ORDER.split(",")) + ")"
    if after is None:
        return columns + "<=" + _parameters(1), upper
    return columns + ">" + _parameters(1) + " AND " + columns + "<=" + _parameters(6), (*after, *upper)


async def _insert_matched_page(connection, tables, bounds, affected):
    membership, bindings, addresses, work, locations = tables
    condition, arguments = bounds
    affected_filter = (
        "TRUE"
        if affected is None
        else f"EXISTS(SELECT FROM {affected} changed WHERE changed.network_id=m.network_id AND changed.office_bucket=(get_byte(sha256(convert_to(b.location_key,'UTF8')),0)&63))"
    )
    ordered_fields = ",".join("m." + term for term in ORDER.split(","))
    await connection.execute(
        f"""WITH source_page AS NOT MATERIALIZED (
      SELECT m.* FROM {membership} m WHERE {condition} ORDER BY {ordered_fields} FETCH FIRST 5000 ROWS WITH TIES
    ), matched AS NOT MATERIALIZED (
      SELECT m.network_id,a.location_key,(get_byte(sha256(convert_to(a.location_key,'UTF8')),0)&63)::integer AS office_bucket
      FROM source_page m LEFT JOIN {bindings} b USING(provider_system,provider_id,location_id)
      LEFT JOIN {addresses} a ON a.location_key=b.location_key AND a.entity_type=b.entity_type AND a.entity_id=b.entity_id
      WHERE {affected_filter}
    ), accumulated AS (
      INSERT INTO {work} AS prior SELECT network_id,location_key,office_bucket,count(*)::bigint
      FROM matched GROUP BY network_id,location_key,office_bucket
      ON CONFLICT(network_id,office_bucket,location_key) DO UPDATE SET membership_rows=prior.membership_rows+excluded.membership_rows
      RETURNING location_key
    ) INSERT INTO {locations} SELECT DISTINCT location_key FROM accumulated WHERE location_key IS NOT NULL ON CONFLICT DO NOTHING""",
        *arguments,
    )


async def matched_facts(connection, schema, addresses, work, candidate, affected):
    """Count every duplicate in indexed inclusive ranges; aggregate only one key page."""
    from process.network_membership_slices import NetworkMembershipSliceError

    membership = schema + ".network_membership"
    await require_order_index(connection, membership)
    locations = _identifier(work.strip('"') + "_locations")
    await connection.execute(f"""CREATE TEMP TABLE {work}(network_id integer,location_key text COLLATE "C",office_bucket integer,membership_rows bigint NOT NULL,
      UNIQUE NULLS NOT DISTINCT(network_id,office_bucket,location_key)) ON COMMIT DROP""")
    await connection.execute(f'CREATE TEMP TABLE {locations}(location_key text COLLATE "C" PRIMARY KEY) ON COMMIT DROP')
    after = None
    while (
        endpoint := await _last_page_key(connection, "SELECT " + COLUMNS + " FROM " + membership, after)
    ) is not None:
        upper_keys = tuple(endpoint)
        await _insert_matched_page(
            connection,
            (membership, schema + ".provider_location_binding", addresses, work, locations),
            _page_range(after, upper_keys),
            affected,
        )
        after = upper_keys
    accounting = await connection.fetchrow(f"""SELECT COALESCE(sum(membership_rows),0)::bigint AS membership_rows,
      COALESCE(sum(membership_rows) FILTER(WHERE network_id IS NULL OR network_id NOT BETWEEN 1 AND 2147483647 OR location_key IS NULL OR length(location_key) NOT BETWEEN 1 AND 64),0)::bigint AS invalid_memberships,
      count(*) AS distinct_memberships,(SELECT count(*) FROM {locations}) AS matched_locations FROM {work}""")
    if accounting["invalid_memberships"]:
        raise NetworkMembershipSliceError("Selected memberships lack an exact provider/site/address binding")
    if candidate["accepted_rows"] != candidate["expected_rows"] or (
        affected is None and accounting["membership_rows"] != candidate["accepted_rows"]
    ):
        raise NetworkMembershipSliceError("Candidate membership accounting is incomplete")
    await connection.execute(f"DROP TABLE {locations}")
    await connection.execute(f"ANALYZE {work}")
    return accounting


def _content_aggregate(key):
    encoded = f"convert_to({key},'UTF8')"
    frame = f"int4send(octet_length({encoded}))||{encoded}"
    return f"encode(sha256($3::bytea||coalesce(string_agg({frame},''::bytea ORDER BY {key} COLLATE \"C\"),''::bytea)),'hex')"


async def prepare_refs(connection, target, work, previous):
    """Hash C-ordered int4-framed keys in PostgreSQL, returning only slice metadata."""
    from process.network_membership_slices import _copy_ref_page

    after, total, reused = (0, -1), 0, 0
    while True:
        page = await connection.fetch(
            f"""WITH keys AS MATERIALIZED (
          SELECT network_id,office_bucket FROM {work} WHERE (network_id,office_bucket)>($1,$2)
          GROUP BY network_id,office_bucket ORDER BY network_id,office_bucket LIMIT {_METADATA_ROWS})
          SELECT keys.network_id,keys.office_bucket,facts.row_count,facts.content_sha256 FROM keys CROSS JOIN LATERAL (
            SELECT count(*)::bigint AS row_count,{_content_aggregate("location_key")} AS content_sha256 FROM {work}
            WHERE network_id=keys.network_id AND office_bucket=keys.office_bucket) facts ORDER BY keys.network_id,keys.office_bucket""",
            *after,
            _DOMAIN,
        )
        if not page:
            return total, reused
        reused += await _copy_ref_page(connection, target, [tuple(entry) for entry in page], previous)
        total += len(page)
        after = page[-1]["network_id"], page[-1]["office_bucket"]


async def verify_digests(connection, target):
    """Check each new indexed slice's full bytes, count and native office bucket."""
    from process.network_compact_readiness import _require

    schema, current = _identifier(target.schema_name), UUID(target.candidate_id)
    after = (0, -1)
    while True:
        page = await connection.fetch(
            f"""WITH refs AS MATERIALIZED (
          SELECT * FROM {schema}.network_slice_ref WHERE artifact_candidate_id=$4 AND (network_id,office_bucket)>($1,$2)
          ORDER BY network_id,office_bucket LIMIT {_METADATA_ROWS})
          SELECT refs.network_id,refs.office_bucket,refs.row_count,refs.content_sha256,facts.* FROM refs CROSS JOIN LATERAL (
            SELECT count(*)::bigint AS actual_count,{_content_aggregate("location_key")} AS actual_digest,
              coalesce(bool_and(length(location_key)>0 AND (get_byte(sha256(convert_to(location_key,'UTF8')),0)&63)=refs.office_bucket),false) AS valid_bucket
            FROM {schema}.network_office_edge WHERE slice_id=refs.slice_id) facts ORDER BY refs.network_id,refs.office_bucket""",
            *after,
            _DOMAIN,
            current,
        )
        if not page:
            return
        for entry in page:
            _require(
                entry["valid_bucket"] is True
                and (entry["actual_count"], entry["actual_digest"]) == (entry["row_count"], entry["content_sha256"])
            )
        after = page[-1]["network_id"], page[-1]["office_bucket"]
