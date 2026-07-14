# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL catalog query for complete index identities."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from db.migration_expression_adoption import _database_connection


INDEX_CATALOG_QUERY = sa.text(
    """
    SELECT index_record.indisvalid,
           index_record.indisready,
           index_record.indisunique,
           index_record.indnullsnotdistinct,
           index_record.indisexclusion,
           index_record.indimmediate,
           index_record.indexprs IS NOT NULL AS expression_keys,
           access_method.amname AS access_method,
           pg_get_expr(index_record.indpred, index_record.indrelid) AS predicate,
           COALESCE(
               ARRAY(
                   SELECT COALESCE(
                              attribute_record.attname,
                              pg_get_indexdef(
                                  index_record.indexrelid,
                                  key_record.position::integer,
                                  true
                              )
                          )
                     FROM unnest(index_record.indkey)
                          WITH ORDINALITY AS key_record(attnum, position)
                     LEFT JOIN pg_attribute AS attribute_record
                       ON attribute_record.attrelid = index_record.indrelid
                      AND attribute_record.attnum = key_record.attnum
                    WHERE key_record.position <= index_record.indnkeyatts
                    ORDER BY key_record.position
               ),
               ARRAY[]::text[]
           ) AS key_columns,
           COALESCE(
               ARRAY(
                   SELECT attribute_record.attname
                     FROM unnest(index_record.indkey)
                          WITH ORDINALITY AS key_record(attnum, position)
                     JOIN pg_attribute AS attribute_record
                       ON attribute_record.attrelid = index_record.indrelid
                      AND attribute_record.attnum = key_record.attnum
                    WHERE key_record.position > index_record.indnkeyatts
                    ORDER BY key_record.position
               ),
               ARRAY[]::text[]
           ) AS include_columns,
           COALESCE(
               ARRAY(
                   SELECT format(
                              '%I.%I',
                              opclass_namespace.nspname,
                              opclass_record.opcname
                          )
                     FROM unnest(index_record.indclass)
                          WITH ORDINALITY AS class_record(opclass_oid, position)
                     JOIN pg_opclass AS opclass_record
                       ON opclass_record.oid = class_record.opclass_oid
                     JOIN pg_namespace AS opclass_namespace
                       ON opclass_namespace.oid = opclass_record.opcnamespace
                    WHERE class_record.position <= index_record.indnkeyatts
                    ORDER BY class_record.position
               ),
               ARRAY[]::text[]
           ) AS operator_classes,
           COALESCE(
               ARRAY(
                   SELECT CASE
                              WHEN collation_record.oid IS NULL THEN ''
                              ELSE format(
                                  '%I.%I',
                                  collation_namespace.nspname,
                                  collation_record.collname
                              )
                          END
                     FROM unnest(index_record.indcollation)
                          WITH ORDINALITY AS collation_key(collation_oid, position)
                     LEFT JOIN pg_collation AS collation_record
                       ON collation_record.oid = collation_key.collation_oid
                     LEFT JOIN pg_namespace AS collation_namespace
                       ON collation_namespace.oid = collation_record.collnamespace
                    WHERE collation_key.position <= index_record.indnkeyatts
                    ORDER BY collation_key.position
               ),
               ARRAY[]::text[]
           ) AS collations,
           COALESCE(
               ARRAY(
                   SELECT option_record.option_value::integer
                     FROM unnest(index_record.indoption)
                          WITH ORDINALITY AS option_record(option_value, position)
                    WHERE option_record.position <= index_record.indnkeyatts
                    ORDER BY option_record.position
               ),
               ARRAY[]::integer[]
           ) AS sort_options,
           COALESCE(
               ARRAY(
                   SELECT option_value
                     FROM unnest(
                              COALESCE(
                                  index_relation.reloptions,
                                  ARRAY[]::text[]
                              )
                          ) AS option_value
                    ORDER BY option_value
               ),
               ARRAY[]::text[]
           ) AS relation_options
      FROM pg_index AS index_record
      JOIN pg_class AS table_record
        ON table_record.oid = index_record.indrelid
      JOIN pg_class AS index_relation
        ON index_relation.oid = index_record.indexrelid
      JOIN pg_am AS access_method
        ON access_method.oid = index_relation.relam
      JOIN pg_namespace AS namespace_record
        ON namespace_record.oid = table_record.relnamespace
     WHERE namespace_record.nspname = :schema
       AND table_record.relname = :table_name
       AND index_relation.relname = :index_name
    """
)


def _index_catalog_record(
    operations: Any,
    index_name: str,
    table_name: str,
    schema: str,
) -> dict[str, Any] | None:
    """Return a complete named-index catalog record when it exists."""
    database_connection = _database_connection(operations)
    if database_connection is None:
        return None
    if database_connection.dialect.name != "postgresql":
        raise RuntimeError("index_shape_validation_requires_postgresql")
    catalog_query_result = database_connection.execute(
        INDEX_CATALOG_QUERY,
        {
            "schema": schema,
            "table_name": table_name,
            "index_name": index_name,
        },
    )
    return catalog_query_result.mappings().one_or_none()

