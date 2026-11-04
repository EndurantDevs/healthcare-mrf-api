# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Indexed, bounded office correspondence for independently checked typed sources."""

from process.network_address_projection import _identifier
from process.network_custom_address_source import _APPROVED_CTE

_KEYS = "provider_system,provider_id,location_id"
_COLUMNS = _KEYS + ",location_key,entity_type,entity_id"
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


async def _has_pair_row_difference(connection, target_namespace, expected, revision, control, preservation):
    source_relation = f"{target_namespace}.network_membership_source"
    queries = [f"SELECT {_KEYS} FROM {source_relation}", f"SELECT {_COLUMNS} FROM {expected}"]
    if preservation is not None:
        queries.append(preservation.binding_query)
    after = None
    while (upper := await _upper_bound(connection, queries, after)) is not None:
        # $1 belongs to the unchanged approved-custom CTE. Every input is narrowed
        # before deduplication; duplicate source evidence never expands this join.
        low = "TRUE" if after is None else f"({_KEYS})>($2::text,$3::text,$4::uuid)"
        offset = 2 if after is None else 5
        scope = low + f" AND ({_KEYS})<=(${offset}::text,${offset + 1}::text,${offset + 2}::uuid)"
        expected_rows = " UNION ".join(f"SELECT {_COLUMNS} FROM ({query}) rows WHERE {scope}" for query in queries[1:])
        differs = await connection.fetchval(
            _APPROVED_CTE.format(control=control)
            + f""", expected AS MATERIALIZED ({expected_rows}),
            source_pairs AS MATERIALIZED (
              SELECT DISTINCT {_KEYS} FROM {source_relation} WHERE {scope}
            ), expected_final AS (
              SELECT expected.provider_system,expected.provider_id,expected.location_id,
                coalesce(custom.location_key,expected.location_key) AS location_key,
                CASE WHEN custom.location_key IS NULL THEN expected.entity_type
                  ELSE custom.provider_system END AS entity_type,
                CASE WHEN custom.location_key IS NULL THEN expected.entity_id
                  ELSE custom.provider_id END AS entity_id
              FROM expected LEFT JOIN custom USING({_KEYS})
            ) SELECT
              EXISTS(SELECT FROM expected GROUP BY {_KEYS} HAVING count(*)<>1)
              OR (SELECT count(*) FROM expected)<>(SELECT count(*) FROM source_pairs)
              OR EXISTS(SELECT FROM source_pairs LEFT JOIN expected USING({_KEYS})
                WHERE expected.location_id IS NULL)
              OR EXISTS(SELECT FROM expected_final expected LEFT JOIN LATERAL (
                SELECT location_key,entity_type,entity_id FROM {target_namespace}.provider_location_binding binding
                WHERE (binding.provider_system,binding.provider_id,binding.location_id)
                  =(expected.provider_system,expected.provider_id,expected.location_id) OFFSET 0
              ) actual ON true WHERE (actual.location_key,actual.entity_type,actual.entity_id)
                IS DISTINCT FROM (expected.location_key,expected.entity_type,expected.entity_id))""",
            revision,
            *((() if after is None else after) + upper),
        )
        if differs:
            return True
        after = upper
    return False


async def _has_office_key_collision(connection, bindings):
    after = None
    while True:
        lower = "TRUE" if after is None else "location_key>$1::text"
        upper = await connection.fetchval(
            f"SELECT max(location_key) FROM (SELECT DISTINCT location_key FROM {bindings} "
            f"WHERE {lower} ORDER BY location_key LIMIT {_PAGE_SIZE}) page",
            *(() if after is None else (after,)),
        )
        if upper is None:
            return False
        scope = lower + (" AND location_key<=$1::text" if after is None else " AND location_key<=$2::text")
        # min/max retains the old distinct-identity collision rule without a
        # per-office DISTINCT sort: even millions of coherent aliases use O(1) state.
        mismatch = " OR ".join(
            f"min({field})<>max({field})" for field in ("provider_system", "provider_id", "entity_type", "entity_id")
        )
        if await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {bindings} WHERE {scope} GROUP BY location_key "
            f"HAVING {mismatch} OR (bool_or(provider_system='manual') AND count(*)>1))",
            *((upper,) if after is None else (after, upper)),
        ):
            return True
        after = upper


async def typed_binding_parity(connection, copy_target, expected, candidate, schema, *, preservation=None):
    """Keep exact source coverage, custom overrides and alias collision rejection."""
    from process.registry_initial_source_composition import RegistryInitialSourceError

    namespace = _identifier(copy_target.schema_name)
    if await _has_pair_row_difference(
        connection, namespace, expected, candidate["approved_custom_revision"], _identifier(schema), preservation
    ) or await _has_office_key_collision(connection, namespace + ".provider_location_binding"):
        raise RegistryInitialSourceError("registry_source_office_bindings_differ")
