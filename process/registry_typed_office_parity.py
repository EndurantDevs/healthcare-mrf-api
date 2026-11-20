# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Indexed, bounded office correspondence for independently checked typed sources."""

from process.network_address_projection import _identifier
from process.network_custom_address_source import _APPROVED_CTE

_KEYS = "provider_system,provider_id,location_id"
_PAGE_SIZE = 5000


async def create_source_pair_index(connection, relation):
    """Install the lookup while the isolated candidate source heap is still empty."""
    if await connection.fetchval(f"SELECT EXISTS(SELECT FROM {relation})"):
        raise ValueError("registry_typed_office_index_requires_empty_heap")
    await connection.execute(f"CREATE INDEX IF NOT EXISTS typed_source_office_idx ON {relation}({_KEYS})")


async def _upper_bound(connection, queries, after, *, parameters=()):
    offset = len(parameters) + 1
    boundary = f"(${offset}::text,${offset + 1}::text,${offset + 2}::uuid)"
    predicate = "TRUE" if after is None else f"({_KEYS})>{boundary}"
    endpoints = " UNION ALL ".join(
        f"(SELECT {_KEYS} FROM (SELECT {_KEYS} FROM ({query}) rows "
        f"WHERE {predicate} ORDER BY {_KEYS} LIMIT {_PAGE_SIZE}) page "
        "ORDER BY provider_system DESC,provider_id DESC,location_id DESC LIMIT 1)"
        for query in queries
    )
    row = await connection.fetchrow(
        f"SELECT {_KEYS} FROM ({endpoints}) endpoints ORDER BY {_KEYS} LIMIT 1",
        *parameters,
        *(() if after is None else after),
    )
    return None if row is None else tuple(row)


async def _has_pair_key_index(connection, relation, *, primary=False):
    """Require the native bounded path without modifying retained heaps."""
    return await connection.fetchval(
        """SELECT EXISTS(SELECT FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_am am ON am.oid=c.relam WHERE i.indrelid=to_regclass($1)
          AND i.indisvalid AND i.indisready AND i.indislive AND (NOT $3::boolean OR i.indisprimary)
          AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts=3 AND i.indnatts=3
          AND am.amname='btree' AND NOT EXISTS(SELECT FROM unnest(i.indoption) option WHERE option<>0)
          AND ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY k(attnum,n)
            JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum ORDER BY k.n)=$2::text[]
          AND ARRAY(SELECT collation_oid FROM unnest(i.indcollation) WITH ORDINALITY v(collation_oid,n) ORDER BY n)
            =ARRAY(SELECT a.attcollation FROM unnest(i.indkey) WITH ORDINALITY k(attnum,n)
              JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum ORDER BY k.n))""",
        relation,
        _KEYS.split(","),
        primary,
    )


async def has_binding_domain_difference(connection, receipt, carrier):
    """Compare the complete binding set in indexed ranges, including expected-only tails."""
    namespace = _identifier(receipt.target.schema_name)
    source_relation = namespace + ".network_membership_source"
    actual_relation = namespace + ".provider_location_binding"
    if not await _has_pair_key_index(connection, source_relation) or not await _has_pair_key_index(
        connection, actual_relation, primary=True
    ):
        raise ValueError("registry_typed_binding_domain_index_required")
    custom = _APPROVED_CTE.format(control=_identifier(receipt.control_schema)) + f"SELECT {_KEYS} FROM custom"
    queries = (
        f"SELECT DISTINCT ON ({_KEYS}) {_KEYS} FROM {source_relation} ORDER BY {_KEYS}",
        custom,
        carrier.canonical_binding_sql,
        f"SELECT {_KEYS} FROM {actual_relation}",
    )
    after = None
    while (
        upper := await _upper_bound(connection, queries, after, parameters=(receipt.approved_revision,))
    ) is not None:
        low = "TRUE" if after is None else f"({_KEYS})>($2::text,$3::text,$4::uuid)"
        offset = 2 if after is None else 5
        scope = low + f" AND ({_KEYS})<=(${offset}::text,${offset + 1}::text,${offset + 2}::uuid)"
        expected = " UNION ".join(f"SELECT {_KEYS} FROM ({query}) rows WHERE {scope}" for query in queries[:3])
        differs = await connection.fetchval(
            f"""WITH expected AS MATERIALIZED ({expected}), actual AS MATERIALIZED (
              SELECT {_KEYS} FROM {actual_relation} WHERE {scope})
            SELECT EXISTS((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
              UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))""",
            receipt.approved_revision,
            *((() if after is None else after) + upper),
        )
        if differs:
            return True
        after = upper
    return False
