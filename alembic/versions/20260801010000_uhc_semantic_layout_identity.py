"""Bind UHC semantic builds to the exact admitted raw layout.

Revision ID: 20260801010000_uhc_semantic_layout_identity
Revises: 20260730110000_provider_directory_profile_delta
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260801010000_uhc_semantic_layout_identity"
down_revision = "20260730110000_provider_directory_profile_delta"
branch_labels = None
depends_on = None


_TABLE = "provider_directory_uhc_semantic_build"
_IDENTITY_KEY = "provider_directory_uhc_semantic_build_identity_key"
_LEGACY_IDENTITY_KEY = (
    "provider_directory_uhc_semantic_build_legacy_identity_key"
)
_LAYOUT_IDENTITY_CHECK = (
    "provider_directory_uhc_semantic_build_layout_identity_check"
)


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _qualified(table: str) -> str:
    schema = _schema().replace('"', '""')
    relation = table.replace('"', '""')
    return f'"{schema}"."{relation}"'


def _new_identity_columns() -> tuple[str, ...]:
    return (
        "catalog_set_sha256",
        "source_file_id",
        "artifact_sha256",
        "raw_contract_version",
        "raw_range_count",
        "manifest_sha256",
        "range_set_sha256",
        "raw_record_count",
        "raw_producer_build_id",
        "semantic_contract_id",
        "semantic_contract_version",
        "encoder_sha256",
        "semantic_verifier_sha256",
    )


def upgrade() -> None:
    schema = _schema()
    op.add_column(
        _TABLE,
        sa.Column("manifest_sha256", sa.String(length=64)),
        schema=schema,
    )
    op.add_column(
        _TABLE,
        sa.Column("range_set_sha256", sa.String(length=64)),
        schema=schema,
    )
    op.add_column(
        _TABLE,
        sa.Column("raw_record_count", sa.BigInteger()),
        schema=schema,
    )
    op.add_column(
        _TABLE,
        sa.Column("raw_producer_build_id", sa.String(length=256)),
        schema=schema,
    )
    op.add_column(
        _TABLE,
        sa.Column("semantic_verifier_sha256", sa.String(length=64)),
        schema=schema,
    )
    build_table = _qualified(_TABLE)
    layout_table = _qualified("provider_directory_uhc_raw_layout")
    op.execute(
        f"""
        UPDATE {build_table} AS build
           SET manifest_sha256=layout.manifest_sha256,
               range_set_sha256=layout.range_set_sha256,
               raw_record_count=layout.record_count,
               raw_producer_build_id=layout.producer_build_id
          FROM {layout_table} AS layout
         WHERE layout.artifact_sha256=build.artifact_sha256
           AND layout.contract_version=build.raw_contract_version
           AND layout.range_count=build.raw_range_count
        """
    )
    for column in (
        "manifest_sha256",
        "range_set_sha256",
        "raw_record_count",
        "raw_producer_build_id",
    ):
        op.alter_column(
            _TABLE,
            column,
            nullable=False,
            schema=schema,
        )
    op.create_check_constraint(
        _LAYOUT_IDENTITY_CHECK,
        _TABLE,
        "manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND range_set_sha256 ~ '^[0-9a-f]{64}$' "
        "AND raw_record_count > 0 "
        "AND raw_producer_build_id <> '' "
        "AND (semantic_verifier_sha256 IS NULL OR "
        "semantic_verifier_sha256 ~ '^[0-9a-f]{64}$') "
        "AND (semantic_contract_version < 3 OR "
        "(semantic_verifier_sha256 IS NOT NULL AND "
        "semantic_verifier_sha256 ~ '^[0-9a-f]{64}$'))",
        schema=schema,
    )
    op.drop_constraint(
        _IDENTITY_KEY,
        _TABLE,
        type_="unique",
        schema=schema,
    )
    op.create_unique_constraint(
        _IDENTITY_KEY,
        _TABLE,
        _new_identity_columns(),
        schema=schema,
    )
    op.create_index(
        _LEGACY_IDENTITY_KEY,
        _TABLE,
        (
            "catalog_set_sha256",
            "source_file_id",
            "semantic_contract_id",
            "semantic_contract_version",
            "encoder_sha256",
        ),
        unique=True,
        schema=schema,
        postgresql_where=sa.text("semantic_verifier_sha256 IS NULL"),
    )


def downgrade() -> None:
    schema = _schema()
    build_table = _qualified(_TABLE)
    op.execute(
        f"""
        DO $$
        BEGIN
          IF EXISTS (
              SELECT 1
                FROM {build_table}
               GROUP BY catalog_set_sha256, source_file_id,
                        semantic_contract_id, semantic_contract_version,
                        encoder_sha256
              HAVING count(*) > 1
          ) THEN
            RAISE EXCEPTION
              'cannot downgrade: exact UHC semantic layouts would collapse';
          END IF;
        END
        $$
        """
    )
    op.drop_index(_LEGACY_IDENTITY_KEY, table_name=_TABLE, schema=schema)
    op.drop_constraint(
        _IDENTITY_KEY,
        _TABLE,
        type_="unique",
        schema=schema,
    )
    op.create_unique_constraint(
        _IDENTITY_KEY,
        _TABLE,
        (
            "catalog_set_sha256",
            "source_file_id",
            "semantic_contract_id",
            "semantic_contract_version",
            "encoder_sha256",
        ),
        schema=schema,
    )
    op.drop_constraint(
        _LAYOUT_IDENTITY_CHECK,
        _TABLE,
        type_="check",
        schema=schema,
    )
    for column in (
        "semantic_verifier_sha256",
        "raw_producer_build_id",
        "raw_record_count",
        "range_set_sha256",
        "manifest_sha256",
    ):
        op.drop_column(_TABLE, column, schema=schema)
