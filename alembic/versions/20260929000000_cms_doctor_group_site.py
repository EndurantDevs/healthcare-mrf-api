# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve CMS Doctors clinician/group/site observations in its serving family.

Revision ID: 20260929000000_cms_doctor_group_site
Revises: 20260923030000_custom_import_source_binding
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa

from alembic import op

revision = "20260929000000_cms_doctor_group_site"
down_revision = "20260923030000_custom_import_source_binding"
branch_labels = None
depends_on = None

_TABLE = "cms_doctor_group_site"
_AUTHORITY = "reference_family_result_generation"
_SHAPE = "reference_family_result_generation_shape_check"


def _shape_support():
    path = Path(__file__).with_name("20260923000000_facility_address_contribution.py")
    spec = importlib.util.spec_from_file_location("cms_group_site_shape_predecessor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    previous, counts = module._generation_shape()
    return previous, {**counts, "facility-anchors": 2}


def upgrade() -> None:
    """Add group/site observations and advance existing clinician publications."""
    previous, counts = _shape_support()
    schema = previous._schema()
    op.create_table(
        _TABLE,
        sa.Column("row_number", sa.BigInteger(), primary_key=True, autoincrement=False),
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("ind_enrl_id", sa.String(64)),
        sa.Column("org_pac_id", sa.String(64)),
        sa.Column("adrs_id", sa.Text()),
        sa.Column("facility_name", sa.Text()),
        sa.Column("num_org_mem", sa.Integer()),
        sa.Column("address_checksum", sa.BigInteger()),
        sa.Column("generation_id", sa.String(64), nullable=False),
        sa.Column("source_json", sa.JSON(), nullable=False),
        sa.Column("observed_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("membership_start_at", sa.TIMESTAMP()),
        sa.Column("membership_end_at", sa.TIMESTAMP()),
        schema=schema,
    )
    for suffix, columns in (
        ("npi", ["npi"]),
        ("org", ["org_pac_id"]),
    ):
        op.create_index(f"{_TABLE}_idx_{suffix}", _TABLE, columns, schema=schema)

    # Existing two-relation publication evidence is explicitly advanced to a new
    # local generation containing the empty, newly admitted relation.
    authority = f'"{schema}"."{_AUTHORITY}"'
    group_regclass = f'"{schema}"."{_TABLE}"'
    op.execute(f"LOCK TABLE {authority} IN ACCESS EXCLUSIVE MODE")
    op.execute(f'ALTER TABLE {authority} DROP CONSTRAINT "{_SHAPE}"')
    op.execute(
        sa.text(
            f"UPDATE {authority} SET local_generation=local_generation+1, "
            "origin_lineage_id=local_lineage_id, origin_generation=local_generation+1, "
            "published_at=transaction_timestamp(), "
            f"relation_oids=array_append(relation_oids, '{group_regclass}'::regclass::oid::bigint) "
            "WHERE importer_id='cms-doctors' AND relation_oids IS NOT NULL"
        )
    )
    op.create_check_constraint(_SHAPE, _AUTHORITY, previous._shape({**counts, "cms-doctors": 3}), schema=schema)


def downgrade() -> None:
    """Remove the relation only when no clinician generation has been published."""
    previous, counts = _shape_support()
    schema = previous._schema()
    authority = f'"{schema}"."{_AUTHORITY}"'
    op.execute(f"LOCK TABLE {authority} IN ACCESS EXCLUSIVE MODE")
    active = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT local_generation<>0 OR relation_oids IS NOT NULL FROM {authority} "
                "WHERE importer_id='cms-doctors'"
            )
        )
        .scalar_one()
    )
    if active:
        raise RuntimeError("CMS Doctors generation evidence prevents downgrade")
    op.execute(f'ALTER TABLE {authority} DROP CONSTRAINT "{_SHAPE}"')
    op.create_check_constraint(_SHAPE, _AUTHORITY, previous._shape(counts), schema=schema)
    op.drop_table(_TABLE, schema=schema)
