"""Allow independent PTG layout builds before a short seal-time CAS.

Revision ID: 20260810120000_ptg2_layout_build_candidates
Revises: 20260810110000_ptg_wave_receipt_authority
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260810120000_ptg2_layout_build_candidates"
down_revision = "20260810110000_ptg_wave_receipt_authority"
branch_labels = None
depends_on = None

_COMPAT_FUNCTION = "ptg2_capture_building_layout_fingerprint"
_COMPAT_TRIGGER = "ptg2_layout_fingerprint_build_candidate_trigger"


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
    schema = _schema()
    table = "ptg2_layout_build_candidate"
    op.execute(
        f"""
        CREATE TABLE {_qt(schema, table)} (
            snapshot_key bigint NOT NULL,
            semantic_fingerprint bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            cleanup_pending_at timestamptz,
            canonical_snapshot_key bigint,
            CONSTRAINT {_q(f"{table}_pkey")} PRIMARY KEY (snapshot_key),
            CONSTRAINT {_q(f"{table}_snapshot_key_fkey")}
                FOREIGN KEY (snapshot_key)
                REFERENCES {_qt(schema, "ptg2_v3_snapshot_layout")} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q(f"{table}_digest_check")}
                CHECK (octet_length(semantic_fingerprint) = 32),
            CONSTRAINT {_q(f"{table}_cleanup_check")}
                CHECK (
                    (cleanup_pending_at IS NULL
                     AND canonical_snapshot_key IS NULL)
                    OR
                    (cleanup_pending_at IS NOT NULL
                     AND canonical_snapshot_key IS NOT NULL
                     AND canonical_snapshot_key <> snapshot_key)
                )
        )
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q(f"{table}_fingerprint_idx")}
            ON {_qt(schema, table)} (semantic_fingerprint, snapshot_key)
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q(f"{table}_cleanup_pending_idx")}
            ON {_qt(schema, table)} (cleanup_pending_at, snapshot_key)
         WHERE cleanup_pending_at IS NOT NULL
        """
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT layout.snapshot_key
                  FROM {_qt(schema, "ptg2_v3_snapshot_layout")} AS layout
                  LEFT JOIN {_qt(schema, "ptg2_v3_layout_fingerprint")} AS fingerprint
                    ON fingerprint.snapshot_key = layout.snapshot_key
                 WHERE layout.state = 'building'
                 GROUP BY layout.snapshot_key
                HAVING COUNT(fingerprint.semantic_fingerprint) <> 1
            ) THEN
                RAISE EXCEPTION
                    'building PTG layout has ambiguous semantic fingerprints';
            END IF;
        END
        $$
        """
    )
    op.execute(
        f"""
        INSERT INTO {_qt(schema, table)}
            (snapshot_key, semantic_fingerprint, created_at)
        SELECT fingerprint.snapshot_key,
               fingerprint.semantic_fingerprint,
               fingerprint.created_at
          FROM {_qt(schema, "ptg2_v3_layout_fingerprint")} AS fingerprint
          JOIN {_qt(schema, "ptg2_v3_snapshot_layout")} AS layout
            ON layout.snapshot_key = fingerprint.snapshot_key
         WHERE layout.state = 'building'
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {_qt(schema, _COMPAT_FUNCTION)}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM {_qt(schema, "ptg2_v3_snapshot_layout")} AS layout
                 WHERE layout.snapshot_key = NEW.snapshot_key
                   AND layout.state = 'building'
            ) THEN
                INSERT INTO {_qt(schema, table)}
                    (snapshot_key, semantic_fingerprint, created_at)
                VALUES
                    (NEW.snapshot_key, NEW.semantic_fingerprint,
                     transaction_timestamp())
                ON CONFLICT (snapshot_key) DO UPDATE
                    SET semantic_fingerprint = EXCLUDED.semantic_fingerprint
                  WHERE {_q(table)}.semantic_fingerprint =
                        EXCLUDED.semantic_fingerprint
                    AND {_q(table)}.cleanup_pending_at IS NULL;
                IF NOT EXISTS (
                    SELECT 1
                      FROM {_qt(schema, table)} AS candidate
                     WHERE candidate.snapshot_key = NEW.snapshot_key
                       AND candidate.semantic_fingerprint =
                           NEW.semantic_fingerprint
                       AND candidate.cleanup_pending_at IS NULL
                ) THEN
                    RAISE EXCEPTION
                        'building PTG layout candidate compatibility conflict';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q(_COMPAT_TRIGGER)}
        AFTER INSERT OR UPDATE OF snapshot_key, semantic_fingerprint
        ON {_qt(schema, "ptg2_v3_layout_fingerprint")}
        FOR EACH ROW
        EXECUTE FUNCTION {_qt(schema, _COMPAT_FUNCTION)}()
        """
    )


def downgrade() -> None:
    schema = _schema()
    op.execute(
        f"""
        DO $$
        BEGIN
            IF to_regclass('{_qt(schema, 'ptg2_layout_build_candidate')}')
                   IS NOT NULL
               AND EXISTS (
                    SELECT 1
                      FROM {_qt(schema, 'ptg2_layout_build_candidate')}
                     LIMIT 1
               )
            THEN
                RAISE EXCEPTION
                    'refusing to downgrade active PTG layout candidates';
            END IF;
        END
        $$
        """
    )
    op.execute(
        f"DROP TRIGGER IF EXISTS {_q(_COMPAT_TRIGGER)} ON "
        f"{_qt(schema, 'ptg2_v3_layout_fingerprint')}"
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS {_qt(schema, _COMPAT_FUNCTION)}()"
    )
    op.execute(
        f"DROP TABLE IF EXISTS "
        f"{_qt(schema, 'ptg2_layout_build_candidate')}"
    )
