# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add the closed MRF replacement family to generation authority.

Revision ID: 20260914130000_mrf_result_generation
Revises: 20260917120000_entity_address_service_network,
20260914110000_reference_family_result_generation
"""

from __future__ import annotations

import os
import re
from uuid import uuid4

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "20260914130000_mrf_result_generation"
down_revision = (
    "20260917120000_entity_address_service_network",
    "20260914110000_reference_family_result_generation",
)
branch_labels = None
depends_on = None

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_STATE_TABLE = "reference_family_result_generation"
_SHAPE_CONSTRAINT = "reference_family_result_generation_shape_check"
_REFERENCE_CARDINALITY = {
    "plan-attributes": 4,
    "places-zcta": 1,
    "lodes": 1,
    "medicare-enrollment": 2,
}
_MRF_CARDINALITY = 13


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    schema = runtime_schema or legacy_schema or "mrf"
    if not _IDENTIFIER.fullmatch(schema) or len(schema.encode("utf-8")) > 63:
        raise RuntimeError("MRF result generation schema is invalid")
    return schema


def _quoted(value: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise RuntimeError("MRF result generation identifier is invalid")
    return f'"{value}"'


def _distinct_oid_expression(cardinality: int) -> str:
    comparisons = [
        f"relation_oids[{left}] <> relation_oids[{right}]"
        for left in range(1, cardinality)
        for right in range(left + 1, cardinality + 1)
    ]
    return " AND ".join(comparisons) or "TRUE"


def _family_expression(importer_id: str, cardinality: int) -> str:
    return (
        f"(importer_id = '{importer_id}' AND cardinality(relation_oids) = {cardinality} "
        f"AND {_distinct_oid_expression(cardinality)})"
    )


def _shape_check(cardinality_by_importer: dict[str, int]) -> str:
    admitted = ", ".join(f"'{name}'" for name in cardinality_by_importer)
    family_shapes = " OR ".join(
        _family_expression(importer_id, cardinality) for importer_id, cardinality in cardinality_by_importer.items()
    )
    return (
        f"importer_id IN ({admitted}) AND local_generation >= 0 AND ("
        "(origin_lineage_id IS NULL AND origin_generation IS NULL "
        "AND published_at IS NULL AND relation_oids IS NULL) OR ("
        "origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL "
        "AND origin_generation > 0 AND published_at IS NOT NULL "
        "AND relation_oids IS NOT NULL AND array_ndims(relation_oids) = 1 "
        "AND array_lower(relation_oids, 1) = 1 "
        f"AND ({family_shapes}) "
        "AND array_position(relation_oids, NULL) IS NULL "
        "AND 0 < ALL(relation_oids) AND 4294967295 >= ALL(relation_oids)))"
    )


def _replace_shape_constraint(schema: str, cardinality_by_importer: dict[str, int]) -> None:
    table = f"{_quoted(schema)}.{_quoted(_STATE_TABLE)}"
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {_quoted(_SHAPE_CONSTRAINT)}")
    op.create_check_constraint(
        _SHAPE_CONSTRAINT,
        _STATE_TABLE,
        _shape_check(cardinality_by_importer),
        schema=schema,
    )


def upgrade() -> None:
    """Admit one generation-less MRF row without inventing legacy history."""

    schema = _schema()
    cardinality_by_importer = {"mrf": _MRF_CARDINALITY, **_REFERENCE_CARDINALITY}
    _replace_shape_constraint(schema, cardinality_by_importer)
    table = f"{_quoted(schema)}.{_quoted(_STATE_TABLE)}"
    op.execute(
        sa.text(
            f"INSERT INTO {table} (importer_id, local_lineage_id, local_generation) VALUES ('mrf', :lineage_id, 0)"
        ).bindparams(
            sa.bindparam(
                "lineage_id",
                value=uuid4(),
                type_=postgresql.UUID(as_uuid=True),
            )
        )
    )


def downgrade() -> None:
    """Refuse to erase recorded MRF publication or adoption evidence."""

    schema = _schema()
    table = f"{_quoted(schema)}.{_quoted(_STATE_TABLE)}"
    retained = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {table} WHERE importer_id='mrf' "
                "AND (local_generation <> 0 OR origin_generation IS NOT NULL))"
            )
        )
        .scalar_one()
    )
    if retained:
        raise RuntimeError("MRF result generation evidence prevents downgrade")
    op.execute(f"DELETE FROM {table} WHERE importer_id='mrf'")
    _replace_shape_constraint(schema, _REFERENCE_CARDINALITY)
