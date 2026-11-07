# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native spatial search index checks with stable physical relation identity."""

_GEO_TAXONOMY_INDEX_SQL = """
    SELECT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_index AS required_meta
          JOIN pg_catalog.pg_class AS required_index
            ON required_index.oid = required_meta.indexrelid
          JOIN pg_catalog.pg_namespace AS index_namespace
            ON index_namespace.oid = required_index.relnamespace
          JOIN pg_catalog.pg_class AS stage_table
            ON stage_table.oid = required_meta.indrelid
          JOIN pg_catalog.pg_namespace AS table_namespace
            ON table_namespace.oid = stage_table.relnamespace
          JOIN pg_catalog.pg_am AS access_method
            ON access_method.oid = required_index.relam
          JOIN pg_catalog.pg_class AS geo_index
            ON geo_index.relnamespace = required_index.relnamespace
           {geo_selector}
          JOIN pg_catalog.pg_index AS geo_meta
            ON geo_meta.indexrelid = geo_index.oid
           AND geo_meta.indrelid = required_meta.indrelid
         WHERE index_namespace.nspname = {schema_literal}
           AND table_namespace.nspname = {schema_literal}
           AND stage_table.relname = {table_literal}
           {required_selector}
           AND required_meta.indisvalid IS TRUE
           AND required_meta.indisready IS TRUE
           AND required_meta.indislive IS TRUE
           AND required_meta.indisunique IS FALSE
           AND required_meta.indnkeyatts = 2
           AND required_meta.indnatts = 2
           AND access_method.amname = 'gist'
           AND geo_meta.indisvalid IS TRUE
           AND geo_meta.indisready IS TRUE
           AND geo_meta.indislive IS TRUE
           AND geo_meta.indnkeyatts = 1 AND geo_meta.indnatts = 1
           AND (SELECT am.amname FROM pg_catalog.pg_am am WHERE am.oid=geo_index.relam) = 'gist'
           AND (SELECT opcname FROM pg_catalog.pg_opclass WHERE oid=geo_meta.indclass[0]) = 'gist_geography_ops'
           AND pg_get_indexdef(required_meta.indexrelid, 1, TRUE)
               = pg_get_indexdef(geo_meta.indexrelid, 1, TRUE)
           AND pg_get_indexdef(required_meta.indexrelid, 2, TRUE) = 'taxonomy_array'
           AND pg_get_expr(required_meta.indpred, required_meta.indrelid)
               = pg_get_expr(geo_meta.indpred, geo_meta.indrelid)
           AND (
                SELECT array_agg(opclass.opcname::text ORDER BY key.position)
                  FROM unnest(required_meta.indclass)
                       WITH ORDINALITY AS key(opclass_oid, position)
                  JOIN pg_catalog.pg_opclass AS opclass
                    ON opclass.oid = key.opclass_oid
                 WHERE key.position <= required_meta.indnkeyatts
               ) = ARRAY['gist_geography_ops', 'gist__intbig_ops']::text[]
    );
"""


def required_geo_taxonomy_index_sql(schema_literal, table_literal, required_literal, geo_literal, *, table_oid=None):
    """Bind SQL literals quoted by the existing stage helper and a closed native OID."""
    if table_oid is not None and (type(table_oid) is not int or not 0 < table_oid <= 4294967295):
        raise ValueError("Geo search table identity is invalid")
    required_selector = (
        f"AND required_index.relname = {required_literal}"
        if table_oid is None
        else f"AND stage_table.oid = {table_oid}"
    )
    geo_selector = f"AND geo_index.relname = {geo_literal}" if table_oid is None else ""
    return _GEO_TAXONOMY_INDEX_SQL.format(
        schema_literal=schema_literal,
        table_literal=table_literal,
        required_selector=required_selector,
        geo_selector=geo_selector,
    )
