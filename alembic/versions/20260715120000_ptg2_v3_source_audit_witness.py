"""Persist bounded source witnesses for strict PTG V3 candidate audits.

Revision ID: 20260715120000_ptg2_v3_source_audit_witness
Revises: 20260714160000_provider_directory_dataset_rehydration
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260715120000_ptg2_v3_source_audit_witness"
down_revision = "20260714160000_provider_directory_dataset_rehydration"
branch_labels = None
depends_on = None


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
    op.execute(
        f"""
        ALTER TABLE {_qt(schema, "ptg2_v3_candidate_audit_attestation")}
            ADD COLUMN source_witness_digest bytea
        """
    )
    op.execute(
        f"""
        ALTER TABLE {_qt(schema, "ptg2_v3_candidate_audit_attestation")}
            ADD CONSTRAINT
                {_q("ptg2_v3_candidate_audit_attestation_witness_check")}
            CHECK (
                source_witness_digest IS NULL
                OR octet_length(source_witness_digest) = 32
            );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {_qt(schema, "ptg2_v3_source_audit_witness")} (
            snapshot_key bigint NOT NULL,
            contract varchar(64) NOT NULL,
            selection_method varchar(64) NOT NULL,
            source_set_digest bytea NOT NULL,
            sample_digest bytea NOT NULL,
            queryable_occurrence_population_count bigint NOT NULL,
            provider_population_count bigint NOT NULL,
            occurrence_witness_count integer NOT NULL,
            provider_witness_count integer NOT NULL,
            payload_sha256 bytea NOT NULL,
            payload bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_pkey")}
                PRIMARY KEY (snapshot_key),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_source_set_digest_check")}
                CHECK (octet_length(source_set_digest) = 32),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_sample_digest_check")}
                CHECK (octet_length(sample_digest) = 32),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_payload_sha256_check")}
                CHECK (octet_length(payload_sha256) = 32),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_occurrence_population_check")}
                CHECK (
                    queryable_occurrence_population_count
                    >= occurrence_witness_count
                ),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_provider_population_check")}
                CHECK (provider_population_count >= provider_witness_count),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_occurrence_count_check")}
                CHECK (occurrence_witness_count BETWEEN 1 AND 2048),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_provider_count_check")}
                CHECK (provider_witness_count BETWEEN 0 AND 2048),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_total_count_check")}
                CHECK (
                    occurrence_witness_count + provider_witness_count <= 2048
                ),
            CONSTRAINT {_q("ptg2_v3_source_audit_witness_payload_check")}
                CHECK (octet_length(payload) > 0)
        );
        """
    )


def downgrade() -> None:
    schema = _schema()
    op.execute(
        f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_v3_source_audit_witness')};"
    )
    op.execute(
        f"""
        ALTER TABLE {_qt(schema, "ptg2_v3_candidate_audit_attestation")}
            DROP CONSTRAINT IF EXISTS
                {_q("ptg2_v3_candidate_audit_attestation_witness_check")}
        """
    )
    op.execute(
        f"""
        ALTER TABLE {_qt(schema, "ptg2_v3_candidate_audit_attestation")}
            DROP COLUMN IF EXISTS source_witness_digest
        """
    )
