"""Scope Provider Directory pagination checkpoints to an acquisition root.

Revision ID: 20260710143000_provider_directory_pagination_root_identity
Revises: 20260710110000_provider_directory_bulk_checkpoints
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260710143000_provider_directory_pagination_root_identity"
down_revision = "20260710110000_provider_directory_bulk_checkpoints"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    endpoint_dataset = sa.table(
        "provider_directory_endpoint_dataset",
        sa.column("dataset_id", sa.String(length=96)),
        sa.column("import_run_id", sa.String(length=64)),
        sa.column("acquisition_root_run_id", sa.String(length=64)),
        schema=schema,
    )
    checkpoint = sa.table(
        "provider_directory_pagination_checkpoint",
        sa.column("dataset_id", sa.String(length=96)),
        sa.column("acquisition_root_run_id", sa.String(length=64)),
        sa.column("retry_of_run_id", sa.String(length=64)),
        schema=schema,
    )
    persisted_checkpoint_root = (
        sa.select(sa.func.min(checkpoint.c.acquisition_root_run_id))
        .where(checkpoint.c.dataset_id == endpoint_dataset.c.dataset_id)
        .where(checkpoint.c.acquisition_root_run_id.is_not(None))
        .correlate(endpoint_dataset)
        .scalar_subquery()
    )
    op.execute(
        endpoint_dataset.update()
        .where(endpoint_dataset.c.acquisition_root_run_id.is_(None))
        .values(acquisition_root_run_id=persisted_checkpoint_root)
    )
    unresolved_adopted_checkpoint = (
        sa.select(sa.literal(1))
        .where(checkpoint.c.dataset_id == endpoint_dataset.c.dataset_id)
        .where(checkpoint.c.acquisition_root_run_id.is_(None))
        .where(checkpoint.c.retry_of_run_id.is_not(None))
        .correlate(endpoint_dataset)
        .exists()
    )
    op.execute(
        endpoint_dataset.update()
        .where(endpoint_dataset.c.acquisition_root_run_id.is_(None))
        .where(endpoint_dataset.c.import_run_id.is_not(None))
        .where(~unresolved_adopted_checkpoint)
        .values(acquisition_root_run_id=endpoint_dataset.c.import_run_id)
    )
    op.execute(
        checkpoint.update()
        .where(checkpoint.c.acquisition_root_run_id.is_(None))
        .values(
            acquisition_root_run_id=sa.select(
                endpoint_dataset.c.acquisition_root_run_id
            )
            .where(endpoint_dataset.c.dataset_id == checkpoint.c.dataset_id)
            .correlate(checkpoint)
            .scalar_subquery()
        )
    )
    op.execute(
        sa.text(
            f"""
            DO $migration$
            BEGIN
                IF EXISTS (
                    SELECT 1
                      FROM {schema}.provider_directory_pagination_checkpoint AS checkpoint
                      LEFT JOIN {schema}.provider_directory_endpoint_dataset AS dataset
                        ON dataset.dataset_id = checkpoint.dataset_id
                     WHERE checkpoint.acquisition_root_run_id IS NULL
                        OR checkpoint.acquisition_root_run_id IS DISTINCT FROM
                           dataset.acquisition_root_run_id
                ) THEN
                    RAISE EXCEPTION
                        'provider_directory_pagination_checkpoint_root_backfill_failed';
                END IF;
            END
            $migration$;
            """
        )
    )
    op.alter_column(
        "provider_directory_pagination_checkpoint",
        "acquisition_root_run_id",
        existing_type=sa.String(length=64),
        nullable=False,
        schema=schema,
    )
    op.drop_constraint(
        "provider_directory_pagination_checkpoint_pkey",
        "provider_directory_pagination_checkpoint",
        type_="primary",
        schema=schema,
    )
    op.create_primary_key(
        "provider_directory_pagination_checkpoint_pkey",
        "provider_directory_pagination_checkpoint",
        [
            "canonical_api_base",
            "resource_type",
            "source_scope_hash",
            "acquisition_root_run_id",
        ],
        schema=schema,
    )
    op.create_index(
        "provider_directory_pagination_checkpoint_root_updated_idx",
        "provider_directory_pagination_checkpoint",
        ["acquisition_root_run_id", "updated_at"],
        schema=schema,
    )


def downgrade():
    schema = _schema()
    op.execute(
        sa.text(
            f"""
            DO $migration$
            BEGIN
                IF EXISTS (
                    SELECT 1
                      FROM {schema}.provider_directory_pagination_checkpoint
                     GROUP BY canonical_api_base, resource_type, source_scope_hash
                    HAVING count(*) > 1
                ) THEN
                    RAISE EXCEPTION
                        'provider_directory_pagination_root_downgrade_requires_archive';
                END IF;
            END
            $migration$;
            """
        )
    )
    op.drop_index(
        "provider_directory_pagination_checkpoint_root_updated_idx",
        table_name="provider_directory_pagination_checkpoint",
        schema=schema,
    )
    op.drop_constraint(
        "provider_directory_pagination_checkpoint_pkey",
        "provider_directory_pagination_checkpoint",
        type_="primary",
        schema=schema,
    )
    op.create_primary_key(
        "provider_directory_pagination_checkpoint_pkey",
        "provider_directory_pagination_checkpoint",
        ["canonical_api_base", "resource_type", "source_scope_hash"],
        schema=schema,
    )
