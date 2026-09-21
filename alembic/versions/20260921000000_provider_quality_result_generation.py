# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit the exact eight-table provider-quality serving generation.

Revision ID: 20260921000000_provider_quality_result_generation
Revises: 20260920170000_mrf_publication_receipt
"""

import importlib.util
from pathlib import Path
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260921000000_provider_quality_result_generation"
down_revision = "20260920170000_mrf_publication_receipt"
branch_labels = None
depends_on = None


def _terminology_migration():
    path = Path(__file__).with_name("20260920150000_terminology_result_generation.py")
    spec = importlib.util.spec_from_file_location("provider_quality_generation_predecessor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _counts(previous):
    return {
        **previous._COUNTS,
        "terminology-synonyms": 1,
        "geo-census": 1,
    }


def upgrade():
    """Add the exact provider-quality serving-family generation shape and seed."""

    previous = _terminology_migration()
    schema = previous._schema()
    previous._replace(schema, {**_counts(previous), "provider-quality": 8})
    op.execute(
        sa.text(
            f'INSERT INTO "{schema}".reference_family_result_generation '
            "(importer_id,local_lineage_id,local_generation) VALUES ('provider-quality',:lineage_id,0)"
        ).bindparams(sa.bindparam("lineage_id", value=uuid4(), type_=postgresql.UUID(as_uuid=True)))
    )


def downgrade():
    """Remove an unused provider-quality seed and restore the prior shape."""

    previous = _terminology_migration()
    schema = previous._schema()
    table = f'"{schema}".reference_family_result_generation'
    generation = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT local_generation,origin_generation FROM {table} "
                "WHERE importer_id='provider-quality' FOR UPDATE"
            )
        )
        .one()
    )
    if generation.local_generation != 0 or generation.origin_generation is not None:
        raise RuntimeError("provider-quality generation evidence prevents downgrade")
    op.execute(f"DELETE FROM {table} WHERE importer_id='provider-quality'")
    previous._replace(schema, _counts(previous))
