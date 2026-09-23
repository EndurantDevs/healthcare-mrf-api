# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind facility publication to its source-only address contribution artifact."""

import importlib.util
from pathlib import Path
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260923000000_facility_address_contribution"
down_revision = "20260922010000_source_profile_archive_pins"
branch_labels = None
depends_on = None


def _generation_shape():
    path = Path(__file__).with_name("20260921000000_provider_quality_result_generation.py")
    spec = importlib.util.spec_from_file_location("facility_generation_predecessor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    previous = module._terminology_migration()
    return previous, {**module._counts(previous), "provider-quality": 8}


def upgrade():
    """Create empty capture storage without assigning authority to legacy rows."""
    previous, counts = _generation_shape()
    schema = previous._schema()
    op.create_table(
        "facility_address_contribution",
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("address_key", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("kind", "address_key"),
        schema=schema,
    )
    previous._replace(schema, {**counts, "facility-anchors": 2})
    op.execute(
        sa.text(
            f'INSERT INTO "{schema}".reference_family_result_generation '
            "(importer_id,local_lineage_id,local_generation) VALUES ('facility-anchors',:lineage_id,0)"
        ).bindparams(sa.bindparam("lineage_id", value=uuid4(), type_=postgresql.UUID(as_uuid=True)))
    )


def downgrade():
    """Remove only an empty, never-published contribution artifact and seed."""
    previous, counts = _generation_shape()
    schema = previous._schema()
    table = f'"{schema}".facility_address_contribution'
    authority = f'"{schema}".reference_family_result_generation'
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    connection = op.get_bind()
    generation = connection.execute(
        sa.text(
            f"SELECT local_generation,origin_generation FROM {authority} "
            "WHERE importer_id='facility-anchors' FOR UPDATE"
        )
    ).one()
    populated = connection.execute(sa.text(f"SELECT EXISTS(SELECT 1 FROM {table})")).scalar_one()
    if populated or generation.local_generation != 0 or generation.origin_generation is not None:
        raise RuntimeError("facility address contribution evidence prevents downgrade")
    op.execute(f"DELETE FROM {authority} WHERE importer_id='facility-anchors'")
    previous._replace(schema, counts)
    op.drop_table("facility_address_contribution", schema=schema)
