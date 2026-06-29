"""Retire leftover PTG address bridge objects.

Revision ID: 20260629205000_retire_ptg_address_leftovers
Revises: 20260629190000_retire_ptg_address
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260629205000_retire_ptg_address_leftovers"
down_revision = "20260629190000_retire_ptg_address"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, relation: str) -> str:
    return f"{_q(schema)}.{_q(relation)}"


def _drop_relation_sql(schema: str, relation: str) -> str:
    relation_ref = _qt(schema, relation)
    return f"""
    DO $$
    DECLARE
        relation_kind "char";
    BEGIN
        SELECT c.relkind INTO relation_kind
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = {schema!r}
           AND c.relname = {relation!r};

        IF relation_kind IN ('r', 'p') THEN
            EXECUTE 'DROP TABLE IF EXISTS {relation_ref} CASCADE';
        ELSIF relation_kind = 'v' THEN
            EXECUTE 'DROP VIEW IF EXISTS {relation_ref} CASCADE';
        ELSIF relation_kind = 'm' THEN
            EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS {relation_ref} CASCADE';
        END IF;
    END $$;
    """


def upgrade():
    schema = _schema()
    op.execute(_drop_relation_sql(schema, "entity_address_ptg_bridge"))
    op.execute(_drop_relation_sql(schema, "ptg_provider_directory_address_corroboration"))


def downgrade():
    pass
