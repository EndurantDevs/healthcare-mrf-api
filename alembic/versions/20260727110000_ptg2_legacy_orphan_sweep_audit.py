"""Add immutable legacy PTG orphan-sweep audit evidence.

Revision ID: 20260727110000_ptg2_legacy_orphan_sweep_audit
Revises: 20260727100000_ptg2_provider_tax_identity
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260727110000_ptg2_legacy_orphan_sweep_audit"
down_revision = "20260727100000_ptg2_provider_tax_identity"
branch_labels = None
depends_on = None

_TABLE = "ptg2_legacy_orphan_sweep_audit"
_FUNCTION = "guard_ptg2_legacy_orphan_sweep_audit"
_ROW_TRIGGER = "ptg2_legacy_orphan_sweep_audit_row_guard"
_TRUNCATE_TRIGGER = "ptg2_legacy_orphan_sweep_audit_truncate_guard"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade() -> None:
    """Install one append-only record per exact applied cleanup plan."""

    schema = _schema()
    table = _qt(schema, _TABLE)
    function = f"{_q(schema)}.{_q(_FUNCTION)}"
    op.execute(
        f"""
        CREATE TABLE {table} (
            audit_id char(64) PRIMARY KEY,
            contract varchar(64) NOT NULL,
            actor varchar(128) NOT NULL,
            plan_digest bytea NOT NULL UNIQUE,
            authority_digest bytea NOT NULL,
            catalog_digest bytea NOT NULL,
            candidate_suffix_count integer NOT NULL,
            root_table_count integer NOT NULL,
            dependent_relation_count integer NOT NULL,
            snapshot_count integer NOT NULL,
            nonempty_table_count integer NOT NULL,
            total_bytes bigint NOT NULL,
            root_relation_oids bigint[] NOT NULL,
            snapshot_ids text[] NOT NULL,
            proof jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT {_q('ptg2_legacy_orphan_sweep_contract_check')}
                CHECK (contract = 'ptg2_legacy_orphan_sweep_v1'),
            CONSTRAINT {_q('ptg2_legacy_orphan_sweep_digest_check')}
                CHECK (
                    audit_id ~ '^[0-9a-f]{{64}}$'
                    AND octet_length(plan_digest) = 32
                    AND octet_length(authority_digest) = 32
                    AND octet_length(catalog_digest) = 32
                ),
            CONSTRAINT {_q('ptg2_legacy_orphan_sweep_actor_check')}
                CHECK (
                    actor ~ '^[A-Za-z0-9][A-Za-z0-9._:@/-]{{0,127}}$'
                ),
            CONSTRAINT {_q('ptg2_legacy_orphan_sweep_count_check')}
                CHECK (
                    candidate_suffix_count > 0
                    AND root_table_count > 0
                    AND dependent_relation_count >= 0
                    AND snapshot_count >= 0
                    AND nonempty_table_count >= 0
                    AND nonempty_table_count <= root_table_count
                    AND total_bytes >= 0
                    AND cardinality(root_relation_oids) = root_table_count
                    AND cardinality(snapshot_ids) = snapshot_count
                ),
            CONSTRAINT {_q('ptg2_legacy_orphan_sweep_proof_check')}
                CHECK (
                    jsonb_typeof(proof) = 'object'
                    AND proof->>'contract' =
                        'ptg2_legacy_orphan_sweep_v1'
                )
        )
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'PTG2_LEGACY_SWEEP_AUDIT_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q(_ROW_TRIGGER)}
        BEFORE UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION {function}()
        """
    )
    op.execute(
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(_ROW_TRIGGER)}
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q(_TRUNCATE_TRIGGER)}
        BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION {function}()
        """
    )
    op.execute(
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(_TRUNCATE_TRIGGER)}
        """
    )


def downgrade() -> None:
    """Remove the audit contract only when no cleanup evidence exists."""

    schema = _schema()
    table = _qt(schema, _TABLE)
    function = f"{_q(schema)}.{_q(_FUNCTION)}"
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {table}) THEN
                RAISE EXCEPTION
                    'PTG2_LEGACY_SWEEP_AUDIT_DOWNGRADE_REFUSED'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    op.execute(f"DROP TRIGGER {_q(_TRUNCATE_TRIGGER)} ON {table}")
    op.execute(f"DROP TRIGGER {_q(_ROW_TRIGGER)} ON {table}")
    op.execute(f"DROP FUNCTION {function}()")
    op.execute(f"DROP TABLE {table}")
