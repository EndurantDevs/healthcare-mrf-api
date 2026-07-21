"""Add bounded multipart persistence for strict PTG V3 source witnesses.

Revision ID: 20260721150000_ptg2_source_witness_parts
Revises: 20260721100000_provider_directory_profile_selection_attestation
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260721150000_ptg2_source_witness_parts"
down_revision = "20260721100000_provider_directory_profile_selection_attestation"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade() -> None:
    schema = _schema()
    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_source_audit_witness_part")} (
            snapshot_key bigint NOT NULL,
            part_number integer NOT NULL,
            part_sha256 bytea NOT NULL,
            payload bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_part_pkey")}
                PRIMARY KEY (snapshot_key, part_number),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_part_parent_fkey")}
                FOREIGN KEY (snapshot_key)
                REFERENCES {_qt(schema, "ptg2_v3_source_audit_witness")}
                    (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_part_number_check")}
                CHECK (part_number BETWEEN 1 AND 7),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_part_sha256_check")}
                CHECK (octet_length(part_sha256) = 32),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_part_payload_check")}
                CHECK (
                    octet_length(payload) BETWEEN 1 AND 67108864
                )
        );
        """
    )


def downgrade() -> None:
    op.execute(
        f"DROP TABLE IF EXISTS "
        f"{_qt(_schema(), 'ptg2_v3_source_audit_witness_part')};"
    )
