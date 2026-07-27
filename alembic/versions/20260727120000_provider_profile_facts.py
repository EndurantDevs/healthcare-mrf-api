"""Add source-neutral provider profile facts and live projection.

Revision ID: 20260727120000_provider_profile_facts
Revises: 20260727100000_ptg2_provider_tax_identity
"""

from __future__ import annotations

import os
import re

from alembic import op
import sqlalchemy as sa

revision = "20260727120000_provider_profile_facts"
down_revision = "20260727110000_ptg2_legacy_orphan_sweep_audit"
branch_labels = None
depends_on = None


def _schema() -> str:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("provider_profile_schema_invalid")
    return schema


def upgrade() -> None:
    schema = _schema()
    op.create_table(
        "provider_profile_import_run",
        sa.Column("run_id", sa.String(64), primary_key=True),
        sa.Column("source_key", sa.String(96), nullable=False),
        sa.Column("jurisdiction", sa.String(16), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("source_manifest", sa.JSON()),
        sa.Column("metrics", sa.JSON()),
        sa.Column("error", sa.JSON()),
        sa.Column("started_at", sa.TIMESTAMP()),
        sa.Column("finished_at", sa.TIMESTAMP()),
        schema=schema,
        if_not_exists=True,
    )
    op.create_table(
        "provider_profile_artifact",
        sa.Column("artifact_id", sa.String(64), primary_key=True),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("source_key", sa.String(96), nullable=False),
        sa.Column("file_name", sa.String(256), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("category", sa.String(64), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("content_bytes", sa.BigInteger(), nullable=False),
        sa.Column("header", sa.JSON()),
        sa.Column("downloaded_at", sa.TIMESTAMP()),
        sa.Column("metadata_json", sa.JSON()),
        sa.UniqueConstraint(
            "run_id", "source_key", name="provider_profile_artifact_run_source_uq"
        ),
        schema=schema,
        if_not_exists=True,
    )
    op.create_table(
        "provider_profile_source_record",
        sa.Column("record_id", sa.String(64), primary_key=True),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("artifact_id", sa.String(64), nullable=False),
        sa.Column("source_key", sa.String(96), nullable=False),
        sa.Column("source_record_key", sa.String(256), nullable=False),
        sa.Column("profession_code", sa.String(32)),
        sa.Column("license_id", sa.String(64)),
        sa.Column("license_number", sa.String(96)),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("normalized_payload", sa.JSON()),
        sa.Column("matched_npi", sa.BigInteger()),
        sa.Column("match_status", sa.String(32), nullable=False),
        sa.Column("match_evidence", sa.JSON()),
        sa.Column("row_number", sa.Integer()),
        sa.UniqueConstraint(
            "run_id",
            "source_key",
            "source_record_key",
            name="provider_profile_source_record_run_key_uq",
        ),
        schema=schema,
        if_not_exists=True,
    )
    op.create_index(
        "provider_profile_source_record_npi_idx",
        "provider_profile_source_record",
        ["matched_npi", "match_status"],
        schema=schema,
        if_not_exists=True,
    )
    op.create_table(
        "provider_profile_fact",
        sa.Column("fact_id", sa.String(64), primary_key=True),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("npi", sa.BigInteger()),
        sa.Column("source_record_id", sa.String(64), nullable=False),
        sa.Column("logical_fact_key", sa.String(64), nullable=False),
        sa.Column("category", sa.String(64), nullable=False),
        sa.Column("fact_type", sa.String(96), nullable=False),
        sa.Column("display", sa.Text(), nullable=False),
        sa.Column("value_json", sa.JSON(), nullable=False),
        sa.Column("availability", sa.String(32), nullable=False),
        sa.Column("assertion_type", sa.String(32), nullable=False),
        sa.Column("verification_status", sa.String(32), nullable=False),
        sa.Column("effective_start", sa.String(32)),
        sa.Column("effective_end", sa.String(32)),
        sa.Column("source_json", sa.JSON(), nullable=False),
        sa.Column("sensitive", sa.Boolean(), nullable=False),
        sa.Column("public_default", sa.Boolean(), nullable=False),
        sa.Column("published_at", sa.TIMESTAMP()),
        schema=schema,
        if_not_exists=True,
    )
    # Reconcile local/prototype tables that pre-date logical fact identity.
    # create_table(if_not_exists=True) deliberately preserves their rows, so
    # the column repair must happen before indexes depend on it.
    op.execute(
        f"ALTER TABLE {schema}.provider_profile_fact "
        "ADD COLUMN IF NOT EXISTS logical_fact_key varchar(64)"
    )
    op.execute(
        f"UPDATE {schema}.provider_profile_fact "
        "SET logical_fact_key = fact_id WHERE logical_fact_key IS NULL"
    )
    op.execute(
        f"ALTER TABLE {schema}.provider_profile_fact "
        "ALTER COLUMN logical_fact_key SET NOT NULL"
    )
    op.create_index(
        "provider_profile_fact_npi_category_idx",
        "provider_profile_fact",
        ["npi", "category"],
        schema=schema,
        if_not_exists=True,
    )
    op.create_index(
        "provider_profile_fact_run_npi_idx",
        "provider_profile_fact",
        ["run_id", "npi"],
        schema=schema,
        if_not_exists=True,
    )
    op.create_index(
        "provider_profile_fact_logical_key_idx",
        "provider_profile_fact",
        ["logical_fact_key", "npi"],
        schema=schema,
        if_not_exists=True,
    )
    op.create_table(
        "provider_profile_projection",
        sa.Column("npi", sa.BigInteger(), primary_key=True, autoincrement=False),
        sa.Column("generation_id", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.String(32), nullable=False),
        sa.Column("profile_json", sa.JSON(), nullable=False),
        sa.Column("evidence_json", sa.JSON()),
        sa.Column("source_keys", sa.JSON(), nullable=False),
        sa.Column("published_at", sa.TIMESTAMP(), nullable=False),
        schema=schema,
        if_not_exists=True,
    )


def downgrade() -> None:
    schema = _schema()
    bind = op.get_bind()
    projection_tables = [
        str(row[0])
        for row in bind.execute(
            sa.text(
                """
                SELECT tablename
                  FROM pg_catalog.pg_tables
                 WHERE schemaname = :schema
                   AND (
                        tablename = 'provider_profile_projection'
                        OR tablename = 'provider_profile_projection_old'
                        OR tablename LIKE 'provider_profile_projection_%'
                   )
                """
            ),
            {"schema": schema},
        )
    ]
    allowed_projection_name = re.compile(
        r"provider_profile_projection(?:_old|_[a-f0-9]{16})?"
    )
    projection_tables = [
        table_name
        for table_name in projection_tables
        if allowed_projection_name.fullmatch(table_name)
    ]
    projection_tables.sort(
        key=lambda table_name: (
            table_name == "provider_profile_projection_old",
            table_name,
        )
    )
    for table_name in projection_tables:
        op.drop_table(table_name, schema=schema, if_exists=True)
    for table in (
        "provider_profile_fact",
        "provider_profile_source_record",
        "provider_profile_artifact",
        "provider_profile_import_run",
    ):
        op.drop_table(table, schema=schema, if_exists=True)
