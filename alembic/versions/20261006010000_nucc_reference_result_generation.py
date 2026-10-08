# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit an uninitialized, exact one-table taxonomy generation."""

import importlib.util
from pathlib import Path
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20261006010000_nucc_reference_result_generation"
down_revision = "20261007000000_custom_import_rejection_anti_joins"
branch_labels = None
depends_on = None


def _migration(filename):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location("taxonomy_generation_predecessor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _support():
    previous, counts = _migration("20260929000000_cms_doctor_group_site.py")._shape_support()
    guards = _migration("20260929040000_reference_source_generation_guard.py")
    return previous, {**counts, "cms-doctors": 3, "mrf": 12}, guards


def upgrade():
    """Add only a seed; a verified publication establishes source authority."""
    previous, counts, guards = _support()
    schema = guards._schema()
    table = f'"{schema}".reference_family_result_generation'
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
    previous._replace(schema, {**counts, "nucc": 1})
    op.execute(
        sa.text(
            f"INSERT INTO {table} (importer_id,local_lineage_id,local_generation) VALUES ('nucc',:lineage_id,0)"
        ).bindparams(sa.bindparam("lineage_id", value=uuid4(), type_=postgresql.UUID(as_uuid=True)))
    )


def downgrade():
    """Never erase a local, adopted or revision-tracked taxonomy generation."""
    previous, counts, guards = _support()
    schema = guards._schema()
    table = f'"{schema}".reference_family_result_generation'
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
    retained = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT EXISTS(SELECT 1 FROM {table} WHERE importer_id='nucc' AND "
                "(local_generation<>0 OR origin_lineage_id IS NOT NULL OR relation_oids IS NOT NULL "
                "OR source_revision_tracked))"
            )
        )
        .scalar_one()
    )
    if retained:
        raise RuntimeError("taxonomy generation evidence prevents downgrade")
    op.execute(f"DELETE FROM {table} WHERE importer_id='nucc'")
    guards._flush_cms_transition(schema)
    previous._replace(schema, counts)
