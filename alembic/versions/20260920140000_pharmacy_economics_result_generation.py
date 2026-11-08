# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit the exact one-table pharmacy economics serving generation.

Revision ID: 20260920140000_pharmacy_economics_result_generation
Revises: 20260920130000_geo_result_generation
"""

from __future__ import annotations

import importlib.util
import os
import re
from pathlib import Path
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260920140000_pharmacy_economics_result_generation"
down_revision = "20260920130000_geo_result_generation"
branch_labels = None
depends_on = None

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_TABLE = "reference_family_result_generation"
_CONSTRAINT = "reference_family_result_generation_shape_check"
_COUNTS = {
    "cms-doctors": 2,
    "mrf": 13,
    "mrf-address": 2,
    "plan-attributes": 4,
    "places-zcta": 1,
    "lodes": 1,
    "tiger": 2,
    "medicare-enrollment": 2,
    "geo": 1,
}


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("database schema configuration differs")
    schema = runtime or legacy or "mrf"
    if not _IDENTIFIER.fullmatch(schema) or len(schema.encode()) > 63:
        raise RuntimeError("reference family schema is invalid")
    return schema


def _shape(counts: dict[str, int]) -> str:
    predecessor = Path(__file__).with_name("20260920130000_geo_result_generation.py")
    spec = importlib.util.spec_from_file_location("geo_result_generation_shape", predecessor)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._shape_check(counts)


def _replace(schema: str, counts: dict[str, int]) -> None:
    op.execute(f'ALTER TABLE "{schema}"."{_TABLE}" DROP CONSTRAINT "{_CONSTRAINT}"')
    op.create_check_constraint(_CONSTRAINT, _TABLE, _shape(counts), schema=schema)


def upgrade() -> None:
    """Add pharmacy economics generation authority."""

    schema = _schema()
    _replace(schema, {**_COUNTS, "pharmacy-economics": 1})
    op.execute(
        sa.text(
            f'INSERT INTO "{schema}"."{_TABLE}" (importer_id,local_lineage_id,local_generation) '
            "VALUES ('pharmacy-economics',:lineage_id,0)"
        ).bindparams(sa.bindparam("lineage_id", value=uuid4(), type_=postgresql.UUID(as_uuid=True)))
    )


def downgrade() -> None:
    """Remove unused pharmacy economics generation authority."""

    schema = _schema()
    table = f'"{schema}"."{_TABLE}"'
    retained = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {table} WHERE importer_id='pharmacy-economics' "
                "AND (local_generation <> 0 OR origin_generation IS NOT NULL))"
            )
        )
        .scalar_one()
    )
    if retained:
        raise RuntimeError("pharmacy economics generation evidence prevents downgrade")
    op.execute(f"DELETE FROM {table} WHERE importer_id='pharmacy-economics'")
    _replace(schema, _COUNTS)
