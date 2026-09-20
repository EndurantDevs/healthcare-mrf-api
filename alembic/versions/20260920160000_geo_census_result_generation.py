# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit census result authority independently of its producer's TIGER output.

Revision ID: 20260920160000_geo_census_result_generation
Revises: 20260920150000_terminology_result_generation
"""

import importlib.util
from pathlib import Path
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260920160000_geo_census_result_generation"
down_revision = "20260920150000_terminology_result_generation"
branch_labels = None
depends_on = None


def _previous():
    path = Path(__file__).with_name("20260920150000_terminology_result_generation.py")
    spec = importlib.util.spec_from_file_location("census_generation_predecessor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def upgrade():
    previous = _previous()
    schema = previous._schema()
    previous._replace(schema, {**previous._COUNTS, "terminology-synonyms": 1, "geo-census": 1})
    op.execute(
        sa.text(
            f'INSERT INTO "{schema}".reference_family_result_generation '
            "(importer_id,local_lineage_id,local_generation) VALUES ('geo-census',:lineage_id,0)"
        ).bindparams(sa.bindparam("lineage_id", value=uuid4(), type_=postgresql.UUID(as_uuid=True)))
    )


def downgrade():
    previous = _previous()
    schema = previous._schema()
    table = f'"{schema}".reference_family_result_generation'
    generation = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT local_generation,origin_generation FROM {table} "
                "WHERE importer_id='geo-census' FOR UPDATE"
            )
        )
        .one()
    )
    if generation.local_generation != 0 or generation.origin_generation is not None:
        raise RuntimeError("census generation evidence prevents downgrade")
    op.execute(f"DELETE FROM {table} WHERE importer_id='geo-census'")
    previous._replace(schema, {**previous._COUNTS, "terminology-synonyms": 1})
