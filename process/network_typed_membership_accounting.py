# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction-local accounting work for a complete typed compact candidate."""

from uuid import uuid4

from process.network_address_projection import NetworkAddressProjectionError, _identifier
from process.network_compact_offices import compact_bound_office_sql
from process.network_typed_slice_native import build_matched_facts
from process.registry_typed_office_parity import _KEYS, _has_pair_key_index, _upper_bound

_ORPHAN_KEY_PAGE_ROWS = 100_000


async def _has_location_key_btree(connection, bindings):
    """Check for a ready default-collation btree leading on location_key."""
    return (
        await connection.fetchval(
            """SELECT EXISTS(SELECT FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            JOIN pg_am am ON am.oid=c.relam JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
            WHERE i.indrelid=to_regclass($1) AND i.indisvalid AND i.indisready AND i.indislive
              AND i.indpred IS NULL AND i.indexprs IS NULL AND am.amname='btree'
              AND a.attname='location_key' AND i.indcollation[0]=a.attcollation AND i.indoption[0]=0)""",
            bindings,
        )
        is True
    )


async def _audit_bindings_by_location_key(connection, bindings, addresses):
    """Group aliases in location_key order so address probes are sequential and check 1:1 multiplicity."""
    statements = [
        await connection.prepare(
            f"""WITH key_page AS MATERIALIZED (
              SELECT location_key FROM {bindings}
              WHERE {"location_key>$1::text" if has_after else "$1::text IS NULL"}
              GROUP BY location_key ORDER BY location_key LIMIT {_ORPHAN_KEY_PAGE_ROWS}
            ), bounds AS MATERIALIZED (
              SELECT min(location_key) AS first_key, max(location_key) AS last_key FROM key_page
            ), grouped AS (
              SELECT b.location_key,b.entity_type,b.entity_id,count(*)::bigint AS binding_rows
              FROM bounds CROSS JOIN LATERAL (
                SELECT b.location_key,b.entity_type,b.entity_id FROM {bindings} b
                WHERE {"b.location_key>$1::text" if has_after else "$1::text IS NULL AND b.location_key>=bounds.first_key"}
                  AND b.location_key<=bounds.last_key
              ) b
              GROUP BY b.location_key,b.entity_type,b.entity_id
            ), probed AS (
              SELECT b.binding_rows,(SELECT count(*) FROM {addresses} a WHERE
                (a.location_key,a.entity_type,a.entity_id)=(b.location_key,b.entity_type,b.entity_id)) AS matches
              FROM grouped b
            ) SELECT (SELECT last_key FROM bounds) AS last_key,
              coalesce(sum(binding_rows) FILTER(WHERE matches=0),0)::bigint AS orphans,
              coalesce(bool_or(matches>1),false) AS has_duplicates
            FROM probed"""
        )
        for has_after in (False, True)
    ]
    after, total_orphans, has_duplicates = None, 0, False
    while True:
        page_summary = await statements[after is not None].fetchrow(after)
        if page_summary["last_key"] is None:
            return total_orphans, has_duplicates
        total_orphans += page_summary["orphans"]
        has_duplicates = has_duplicates or page_summary["has_duplicates"]
        after = page_summary["last_key"]


async def _orphan_bindings_audit(connection, bindings, addresses):
    """Check all binding keys, including unused aliases, and report exact 1:1 match safety."""
    if not await _has_pair_key_index(connection, bindings, primary=True):
        raise ValueError("registry_typed_binding_domain_index_required")
    if await _has_location_key_btree(connection, bindings):
        orphan_count, has_duplicates = await _audit_bindings_by_location_key(connection, bindings, addresses)
        return orphan_count, orphan_count == 0 and not has_duplicates
    after, total = None, 0
    while (upper := await _upper_bound(connection, [f"SELECT {_KEYS} FROM {bindings}"], after)) is not None:
        low = "TRUE" if after is None else f"({_KEYS})>($1::text,$2::text,$3::uuid)"
        offset = 1 if after is None else 4
        scope = low + f" AND ({_KEYS})<=(${offset}::text,${offset + 1}::text,${offset + 2}::uuid)"
        total += await connection.fetchval(
            f"""WITH page AS MATERIALIZED (
              SELECT location_key,entity_type,entity_id FROM {bindings}
              WHERE {scope} ORDER BY {_KEYS})
            SELECT count(*) FROM page b WHERE NOT EXISTS(
              SELECT FROM {addresses} a WHERE
              (a.location_key,a.entity_type,a.entity_id)=(b.location_key,b.entity_type,b.entity_id))""",
            *((() if after is None else after) + upper),
        )
        after = upper
    return total, False


async def _orphan_bindings(connection, bindings, addresses):
    """Check all binding keys, including unused aliases and the last partial page."""
    orphan_count, _ = await _orphan_bindings_audit(connection, bindings, addresses)
    return orphan_count


async def prepare_accounting_work(connection, schema, office_sources):
    """The caller pins offices and retains this work only in its own transaction."""
    work = _identifier("network_slice_work_" + uuid4().hex)
    addresses = compact_bound_office_sql(office_sources)
    orphan_bindings, bindings_verified = await _orphan_bindings_audit(
        connection, schema + ".provider_location_binding", addresses
    )
    counts = await build_matched_facts(connection, schema, addresses, work, None, bindings_verified=bindings_verified)
    counts["projected_locations"] = counts["matched_locations"]
    counts["orphan_bindings"] = orphan_bindings
    if counts["invalid_memberships"]:
        raise NetworkAddressProjectionError("Selected memberships lack an exact provider/site/address binding")
    return work, counts
