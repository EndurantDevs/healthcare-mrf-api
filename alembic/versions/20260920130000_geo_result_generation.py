# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit the closed one-table geo replacement family.

Revision ID: 20260920130000_geo_result_generation
Revises: 20260920120000_mrf_address_result_generation
"""

from __future__ import annotations

import os
import re
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260920130000_geo_result_generation"
down_revision = "20260920120000_mrf_address_result_generation"
branch_labels = None
depends_on = None

_CARDINALITY = {
    "cms-doctors": 2,
    "mrf": 13,
    "mrf-address": 2,
    "plan-attributes": 4,
    "places-zcta": 1,
    "lodes": 1,
    "tiger": 2,
    "medicare-enrollment": 2,
}
_CONSTRAINT = "reference_family_result_generation_shape_check"


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    schema = runtime or legacy or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) or len(schema.encode()) > 63:
        raise RuntimeError("geo result generation schema is invalid")
    return schema


def _shape_check(cardinality: dict[str, int]) -> str:
    admitted = ", ".join(f"'{name}'" for name in cardinality)
    shapes = " OR ".join(
        f"(importer_id = '{name}' AND cardinality(relation_oids) = {count} AND "
        + (
            " AND ".join(
                f"relation_oids[{left}] <> relation_oids[{right}]"
                for left in range(1, count)
                for right in range(left + 1, count + 1)
            )
            or "TRUE"
        )
        + ")"
        for name, count in cardinality.items()
    )
    return (
        f"importer_id IN ({admitted}) AND local_generation >= 0 AND ("
        "(origin_lineage_id IS NULL AND origin_generation IS NULL "
        "AND published_at IS NULL AND relation_oids IS NULL) OR ("
        "origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL "
        "AND origin_generation > 0 AND published_at IS NOT NULL "
        "AND relation_oids IS NOT NULL AND array_ndims(relation_oids) = 1 "
        "AND array_lower(relation_oids, 1) = 1 "
        f"AND ({shapes}) "
        "AND array_position(relation_oids, NULL) IS NULL "
        "AND 0 < ALL(relation_oids) AND 4294967295 >= ALL(relation_oids)))"
    )


def _replace_shape(schema: str, cardinality: dict[str, int]) -> None:
    op.execute(f'ALTER TABLE "{schema}".reference_family_result_generation DROP CONSTRAINT "{_CONSTRAINT}"')
    op.create_check_constraint(
        _CONSTRAINT,
        "reference_family_result_generation",
        _shape_check(cardinality),
        schema=schema,
    )


def upgrade() -> None:
    """Admit an unpublished geo generation without altering prior families."""

    schema = _schema()
    _replace_shape(schema, {**_CARDINALITY, "geo": 1})
    op.execute(
        sa.text(
            f'INSERT INTO "{schema}".reference_family_result_generation '
            "(importer_id, local_lineage_id, local_generation) VALUES ('geo', :lineage_id, 0)"
        ).bindparams(sa.bindparam("lineage_id", value=uuid4(), type_=postgresql.UUID(as_uuid=True)))
    )


def downgrade() -> None:
    """Remove only unpublished geo authority and preserve recorded evidence."""

    schema = _schema()
    table = f'"{schema}".reference_family_result_generation'
    retained = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {table} WHERE importer_id='geo' "
                "AND (local_generation <> 0 OR origin_generation IS NOT NULL))"
            )
        )
        .scalar_one()
    )
    if retained:
        raise RuntimeError("geo result generation evidence prevents downgrade")
    op.execute(f"DELETE FROM {table} WHERE importer_id='geo'")
    _replace_shape(schema, _CARDINALITY)
