# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Use declared names for the existing NPI result revision boundary."""

import os

from sqlalchemy import text

from alembic import op
from db.plan_release_storage import storage_identifier

revision = "20261114160000_npi_named_generation"
down_revision = "20261114130000_named_preparation_catalog"
branch_labels = None
depends_on = None

_NAMES = ("npi", "npi_address", "npi_taxonomy", "npi_taxonomy_group", "npi_other_identifier", "npi_phone_staffing")


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("database schema configuration differs")
    schema = runtime or legacy or "mrf"
    storage_identifier(schema)
    return schema


def _replace_revision_body(schema):
    """Keep the existing statement revision guard, replacing only its storage predicate."""
    state = storage_identifier(schema) + '."npi_result_generation"'
    function = storage_identifier(schema) + '."advance_npi_result_generation"'
    op.execute(f"""CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        BEGIN
            UPDATE {state}
               SET local_generation = local_generation + 1,
                   origin_lineage_id = CASE
                       WHEN origin_lineage_id IS NULL THEN NULL
                       ELSE local_lineage_id
                   END,
                   origin_generation = CASE
                       WHEN origin_lineage_id IS NULL THEN NULL
                       ELSE local_generation + 1
                   END,
                   published_at = CASE
                       WHEN origin_lineage_id IS NULL THEN NULL
                       ELSE transaction_timestamp()
                   END
             WHERE singleton IS TRUE
               AND serving_schema IS NOT NULL
               AND TG_TABLE_SCHEMA = serving_schema
               AND TG_TABLE_NAME = ANY(serving_relation_names);
            RETURN NULL;
        END; $function$;""")


def upgrade():
    """Preserve lineage, provenance and revision guards while mapping declared names.

    A table that already has the named columns, for example in a copied database, keeps them; the counter
    body and the shape check, which validates the stored row, are installed either way.
    """
    schema = _schema()
    table = storage_identifier(schema) + '."npi_result_generation"'
    op.execute(
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS serving_schema varchar(63), "
        "ADD COLUMN IF NOT EXISTS serving_relation_names text[]"
    )
    op.execute(
        text(
            f"UPDATE {table} SET serving_schema=:schema, serving_relation_names=:names WHERE origin_lineage_id IS NOT NULL"
        ).bindparams(schema=schema, names=list(_NAMES))
    )
    _replace_revision_body(schema)
    op.execute(
        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS npi_result_generation_shape_check, "
        "DROP COLUMN IF EXISTS relation_oids"
    )
    names = "ARRAY[" + ",".join("'" + name + "'" for name in _NAMES) + "]::text[]"
    op.execute(f"""ALTER TABLE {table} ADD CONSTRAINT npi_result_generation_shape_check CHECK (
      singleton IS TRUE AND local_generation BETWEEN 0 AND 9223372036854775807 AND (
        (origin_lineage_id IS NULL AND origin_generation IS NULL AND published_at IS NULL
         AND serving_schema IS NULL AND serving_relation_names IS NULL) OR
        (origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL
         AND origin_generation BETWEEN 1 AND 9223372036854775807 AND published_at IS NOT NULL
         AND serving_schema IS NOT NULL AND serving_schema ~ '^[A-Za-z_][A-Za-z0-9_]{{0,62}}$'
         AND serving_relation_names IS NOT NULL AND serving_relation_names={names})) AND (
        (canonical_publication_ref IS NULL AND canonical_publication_generation IS NULL
         AND canonical_chain_ref IS NULL AND canonical_import_date IS NULL) OR
        (canonical_publication_ref IS NOT NULL AND canonical_publication_generation IS NOT NULL
         AND canonical_publication_generation BETWEEN 1 AND 9007199254740991
         AND canonical_chain_ref IS NOT NULL AND canonical_import_date IS NOT NULL)))""")


def downgrade():
    """Refuse reversal that would discard named publication metadata."""
    raise RuntimeError("Named NPI publication requires explicit retirement")
