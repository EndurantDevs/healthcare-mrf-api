# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seed generation authority for the seven-table clinical reference result.

Revision ID: 20260923022000_clinical_reference_result_generation
Revises: 20260922010000_custom_import_execution_request_identity
"""

import importlib.util
from pathlib import Path
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260923022000_clinical_reference_result_generation"
down_revision = "20260922010000_custom_import_execution_request_identity"
branch_labels = None
depends_on = None


def _generation_shape():
    path = Path(__file__).with_name("20260923000000_facility_address_contribution.py")
    spec = importlib.util.spec_from_file_location("clinical_generation_predecessor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module._generation_shape()


def upgrade():
    """Admit the seven-table clinical result authority without claiming legacy rows."""
    previous, counts = _generation_shape()
    schema = previous._schema()
    previous._replace(schema, {**counts, "facility-anchors": 2, "clinical-reference": 7})
    op.execute(
        sa.text(
            f'INSERT INTO "{schema}".reference_family_result_generation '
            "(importer_id,local_lineage_id,local_generation) VALUES ('clinical-reference',:lineage_id,0)"
        ).bindparams(sa.bindparam("lineage_id", value=uuid4(), type_=postgresql.UUID(as_uuid=True)))
    )


def downgrade():
    """Remove only an unused clinical generation seed and restore the prior CHECK."""
    previous, counts = _generation_shape()
    schema = previous._schema()
    table = f'"{schema}".reference_family_result_generation'
    generation = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT local_generation,origin_generation FROM {table} "
                "WHERE importer_id='clinical-reference' FOR UPDATE"
            )
        )
        .one()
    )
    if generation.local_generation != 0 or generation.origin_generation is not None:
        raise RuntimeError("clinical reference generation evidence prevents downgrade")
    op.execute(f"DELETE FROM {table} WHERE importer_id='clinical-reference'")
    previous._replace(schema, {**counts, "facility-anchors": 2})
