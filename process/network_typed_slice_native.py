# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native preparation and content checks for typed membership slices."""

from uuid import UUID

from process.network_address_projection import _identifier
from process.registry_typed_membership_parity import (
    COLUMNS,
    ORDER,
    _last_page_key,
    require_not_null_keys,
    require_order_index,
)

_DOMAIN = b"network-office-slice-v1\0"
_METADATA_ROWS = 4096
_MATCH_ROWS = 100_000


def _parameters(start):
    types = ("integer", 'text COLLATE "C"', 'text COLLATE "C"', "uuid", 'text COLLATE "C"')
    return "(" + ",".join(f"${index}::{kind}" for index, kind in enumerate(types, start)) + ")"


def _page_range(after, upper):
    columns = "(" + ",".join("m." + term for term in ORDER.split(",")) + ")"
    if after is None:
        return columns + "<=" + _parameters(1), upper
    return columns + ">" + _parameters(1) + " AND " + columns + "<=" + _parameters(6), (*after, *upper)


def _matched_page_sql(tables, condition, affected, *, bindings_verified=False):
    membership, bindings, addresses, work, locations = tables
    affected_filter = (
        "TRUE"
        if affected is None
        else f"EXISTS(SELECT FROM {affected} changed WHERE changed.network_id=m.network_id AND changed.office_bucket=(get_byte(sha256(convert_to(b.location_key,'UTF8')),0)&63))"
    )
    if bindings_verified:
        key_expr = "b.location_key"
        address_join = ""
        binding_cols = "location_key"
    else:
        key_expr = "a.location_key"
        address_join = f"\n      LEFT JOIN {addresses} a ON a.location_key=b.location_key AND a.entity_type=b.entity_type AND a.entity_id=b.entity_id"
        binding_cols = "location_key,entity_type,entity_id"
    return f"""WITH subjects AS NOT MATERIALIZED (
      SELECT m.network_id,
        m.provider_system COLLATE "C" AS provider_system,
        m.provider_id COLLATE "C" AS provider_id,
        m.location_id,
        count(*)::bigint AS membership_rows
      FROM {membership} m WHERE {condition}
      GROUP BY m.network_id,m.provider_system COLLATE "C",m.provider_id COLLATE "C",m.location_id
    ), matched AS NOT MATERIALIZED (
      SELECT m.network_id,
        {key_expr}::text COLLATE "C" AS location_key,
        (get_byte(sha256(convert_to({key_expr},'UTF8')),0)&63)::integer AS office_bucket,
        m.membership_rows
      FROM subjects m LEFT JOIN LATERAL (
        SELECT {binding_cols} FROM {bindings} binding
        WHERE (binding.provider_system,binding.provider_id,binding.location_id)=(m.provider_system COLLATE "default",m.provider_id COLLATE "default",m.location_id)
        OFFSET 0
      ) b ON TRUE{address_join}
      WHERE {affected_filter}
    ), accumulated AS (
      INSERT INTO {work} AS prior
      SELECT network_id,location_key,office_bucket,sum(membership_rows)::bigint
      FROM matched
      GROUP BY network_id,office_bucket,location_key
      ORDER BY network_id,office_bucket,location_key
      ON CONFLICT(network_id,office_bucket,location_key) DO UPDATE SET membership_rows=prior.membership_rows+excluded.membership_rows
      RETURNING location_key
    ) INSERT INTO {locations} SELECT DISTINCT location_key FROM accumulated WHERE location_key IS NOT NULL ON CONFLICT DO NOTHING"""


async def _insert_matched_page(
    connection, tables, bounds, affected, *, statements_by_condition=None, bindings_verified=False
):
    condition, arguments = bounds
    statement = statements_by_condition.get(condition) if statements_by_condition is not None else None
    if statement is None:
        statement = await connection.prepare(
            _matched_page_sql(tables, condition, affected, bindings_verified=bindings_verified)
        )
        if statements_by_condition is not None:
            statements_by_condition[condition] = statement
    await statement.fetch(*arguments)


async def build_matched_facts(connection, schema, addresses, work, affected, *, bindings_verified=False):
    """Count every duplicate in indexed inclusive ranges; aggregate only one key page."""
    membership = schema + ".network_membership"
    await require_order_index(connection, membership)
    await require_not_null_keys(connection, membership)
    locations = _identifier(work.strip('"') + "_locations")
    await connection.execute(f"""CREATE TEMP TABLE {work}(network_id integer,location_key text COLLATE "C",office_bucket integer,membership_rows bigint NOT NULL,
      UNIQUE NULLS NOT DISTINCT(network_id,office_bucket,location_key)) ON COMMIT DROP""")
    await connection.execute(f'CREATE TEMP TABLE {locations}(location_key text COLLATE "C" PRIMARY KEY) ON COMMIT DROP')
    tables = (membership, schema + ".provider_location_binding", addresses, work, locations)
    statements_by_condition = {}
    after = None
    while (
        endpoint := await _last_page_key(
            connection, "SELECT " + COLUMNS + " FROM " + membership, after, page_rows=_MATCH_ROWS
        )
    ) is not None:
        upper_keys = tuple(endpoint)
        await _insert_matched_page(
            connection,
            tables,
            _page_range(after, upper_keys),
            affected,
            statements_by_condition=statements_by_condition,
            bindings_verified=bindings_verified,
        )
        after = upper_keys
    accounting_by_field = dict(
        await connection.fetchrow(f"""SELECT COALESCE(sum(membership_rows),0)::bigint AS membership_rows,
      COALESCE(sum(membership_rows) FILTER(WHERE network_id IS NULL OR network_id<=0 OR location_key IS NULL),0)::bigint AS invalid_memberships,
      COALESCE(sum(membership_rows) FILTER(WHERE length(location_key) NOT BETWEEN 1 AND 64),0)::bigint AS invalid_location_keys,
      count(*) FILTER(WHERE location_key IS NOT NULL) AS distinct_memberships,
      (SELECT count(*) FROM {locations}) AS matched_locations FROM {work}""")
    )
    await connection.execute(f"DROP TABLE {locations}")
    await connection.execute(f"ANALYZE {work}")
    return accounting_by_field


def require_complete_facts(accounting_by_field, candidate, affected):
    """Require exact bindings and complete closed-row accounting before slice reuse."""
    from process.network_membership_slices import NetworkMembershipSliceError

    if accounting_by_field["invalid_memberships"] or accounting_by_field["invalid_location_keys"]:
        raise NetworkMembershipSliceError("Selected memberships lack an exact provider/site/address binding")
    if candidate["accepted_rows"] != candidate["expected_rows"] or (
        affected is None and accounting_by_field["membership_rows"] != candidate["accepted_rows"]
    ):
        raise NetworkMembershipSliceError("Candidate membership accounting is incomplete")


async def matched_facts(connection, schema, addresses, work, candidate, affected):
    """Build exact native facts and enforce the complete selected-slice contract."""
    accounting_by_field = await build_matched_facts(connection, schema, addresses, work, affected)
    require_complete_facts(accounting_by_field, candidate, affected)
    return accounting_by_field


def _content_aggregate(key, domain_param="$3"):
    encoded = f"convert_to({key},'UTF8')"
    frame = f"int4send(octet_length({encoded}))||{encoded}"
    return f"encode(sha256({domain_param}::bytea||coalesce(string_agg({frame},''::bytea ORDER BY {key} COLLATE \"C\"),''::bytea)),'hex')"


async def _ref_metadata_page(connection, work, after):
    return await connection.fetch(
        f"""WITH keys AS MATERIALIZED (
          SELECT network_id,office_bucket FROM {work} WHERE (network_id,office_bucket)>($1,$2)
          GROUP BY network_id,office_bucket ORDER BY network_id,office_bucket LIMIT {_METADATA_ROWS})
          SELECT keys.network_id,keys.office_bucket,facts.row_count,facts.content_sha256 FROM keys CROSS JOIN LATERAL (
            SELECT count(*)::bigint AS row_count,{_content_aggregate("location_key")} AS content_sha256 FROM {work}
            WHERE network_id=keys.network_id AND office_bucket=keys.office_bucket) facts ORDER BY keys.network_id,keys.office_bucket""",
        *after,
        _DOMAIN,
    )


async def prepare_refs(connection, target, work, previous):
    """Hash C-ordered int4-framed keys in PostgreSQL, returning only slice metadata."""
    if previous is None:
        refs = _identifier(target.schema_name) + ".network_slice_ref"
        status = await connection.execute(
            f"""INSERT INTO {refs}(network_id,office_bucket,artifact_candidate_id,slice_id,row_count,content_sha256)
            SELECT network_id,office_bucket,$1::uuid,gen_random_uuid(),count(*)::bigint,
              {_content_aggregate("location_key", "$2")}
            FROM {work}
            GROUP BY network_id,office_bucket
            ORDER BY network_id,office_bucket""",
            UUID(target.candidate_id),
            _DOMAIN,
        )
        return int(status.removeprefix("INSERT 0 ")), 0
    from process.network_membership_slices import _copy_ref_page

    after, total, reused = (0, -1), 0, 0
    while True:
        page = await _ref_metadata_page(connection, work, after)
        if not page:
            return total, reused
        reused += await _copy_ref_page(connection, target, [tuple(entry) for entry in page], previous)
        total += len(page)
        after = page[-1]["network_id"], page[-1]["office_bucket"]


async def verify_work_refs(connection, target, work):
    """Compare independent full facts with content-verified refs, including extra keys."""
    from process.network_compact_readiness import _require

    refs = _identifier(target.schema_name) + ".network_slice_ref"
    after, total = (0, -1), 0
    while page := await _ref_metadata_page(connection, work, after):
        observed = await connection.fetch(
            f"""SELECT r.network_id,r.office_bucket,r.row_count,r.content_sha256 FROM {refs} r
            JOIN unnest($1::integer[],$2::integer[]) keys(network_id,office_bucket)
            USING(network_id,office_bucket) ORDER BY r.network_id,r.office_bucket""",
            [entry["network_id"] for entry in page],
            [entry["office_bucket"] for entry in page],
        )
        _require(list(map(tuple, page)) == list(map(tuple, observed)))
        total += len(page)
        after = page[-1]["network_id"], page[-1]["office_bucket"]
    _require(total == await connection.fetchval(f"SELECT count(*) FROM {refs}"))
