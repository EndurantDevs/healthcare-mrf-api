# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded ordered multiset checks for the typed contribution protocol."""

from process.network_address_projection import _identifier

FIELDS = ("network_id", "provider_system", "provider_id", "location_id", "evidence_id")
ORDER = ",".join(
    name + (' COLLATE "C"' if name in {"provider_system", "provider_id", "evidence_id"} else "") for name in FIELDS
)
COLUMNS = ",".join(FIELDS)
_PARAMETERS = '($1::integer,$2::text COLLATE "C",$3::text COLLATE "C",$4::uuid,$5::text COLLATE "C")'


def typed_candidate(candidate):
    from process.registry_source_contribution_store import verify_registry_source_contributions

    receipt = verify_registry_source_contributions(candidate)
    return receipt is not None and receipt["revision"] == 6


async def create_order_index(connection, relation, name):
    """Create on an empty producer heap, so index construction cannot sort the full publication."""
    if await connection.fetchval("SELECT EXISTS(SELECT FROM " + relation + ")"):
        raise ValueError("registry_typed_parity_index_requires_empty_heap")
    await connection.execute("CREATE INDEX IF NOT EXISTS " + _identifier(name) + " ON " + relation + "(" + ORDER + ")")


async def require_order_index(connection, relation):
    """Require the native bounded-range path without rebuilding a closed or historical heap."""
    valid = await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam WHERE i.indrelid=to_regclass($1) AND i.indisvalid AND i.indisready AND i.indislive AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts=5 AND i.indnatts=5 AND am.amname='btree' AND ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY k(attnum,n) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum ORDER BY k.n)=$2::text[] AND ARRAY(SELECT coalesce(c.collname::text,'') FROM unnest(i.indcollation) WITH ORDINALITY v(oid,n) LEFT JOIN pg_collation c ON c.oid=v.oid ORDER BY v.n)=ARRAY['','C','C','','C']::text[] AND NOT EXISTS(SELECT FROM unnest(i.indoption) option WHERE option<>0))",
        relation,
        list(FIELDS),
    )
    if valid is not True:
        raise ValueError("registry_typed_parity_index_required")


async def _last_page_key(connection, query, after):
    condition = "TRUE" if after is None else "(" + ORDER + ")>" + _PARAMETERS
    return await connection.fetchrow(
        "SELECT "
        + COLUMNS
        + " FROM (SELECT "
        + COLUMNS
        + " FROM ("
        + query
        + ") rows WHERE "
        + condition
        + " ORDER BY "
        + ORDER
        + " LIMIT 5000) page ORDER BY "
        + ",".join(term + " DESC" for term in ORDER.split(","))
        + " LIMIT 1",
        *(() if after is None else after),
    )


async def multiset_differs(connection, actual, expected):
    """Compare every full row and duplicate count using at most 5,000 distinct keys per side/range."""
    after = None
    while True:
        left = await _last_page_key(connection, actual, after)
        right = await _last_page_key(connection, expected, after)
        if left is None and right is None:
            return False
        # PostgreSQL defines the ordering, including C-collated text and UUIDs; Python never sorts row keys.
        choices = [tuple(value) for value in (left, right) if value is not None]
        if len(choices) == 2:
            upper = tuple(
                await connection.fetchrow(
                    "SELECT "
                    + COLUMNS
                    + " FROM (VALUES ($1::integer,$2::text,$3::text,$4::uuid,$5::text),($6::integer,$7::text,$8::text,$9::uuid,$10::text)) endpoints("
                    + COLUMNS
                    + ") ORDER BY "
                    + ORDER
                    + " LIMIT 1",
                    *choices[0],
                    *choices[1],
                )
            )
        else:
            upper = choices[0]
        low = "TRUE" if after is None else "(" + ORDER + ")>" + _PARAMETERS
        high_parameters = (
            _PARAMETERS
            if after is None
            else _PARAMETERS.replace("$5", "$10")
            .replace("$4", "$9")
            .replace("$3", "$8")
            .replace("$2", "$7")
            .replace("$1::", "$6::")
        )
        scope = low + " AND (" + ORDER + ")<=" + high_parameters
        grouped = lambda query: (
            "SELECT " + ORDER + ",count(*) AS amount FROM (" + query + ") rows WHERE " + scope + " GROUP BY " + ORDER
        )
        differs = await connection.fetchval(
            "WITH actual AS ("
            + grouped(actual)
            + "),expected AS ("
            + grouped(expected)
            + ") SELECT EXISTS(SELECT FROM actual FULL JOIN expected USING("
            + COLUMNS
            + ") WHERE actual.amount IS DISTINCT FROM expected.amount)",
            *((() if after is None else after) + upper),
        )
        if differs:
            return True
        after = upper
