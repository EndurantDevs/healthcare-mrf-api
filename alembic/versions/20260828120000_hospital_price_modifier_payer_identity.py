# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allow anonymous modifier adjustments without relaxing ordinary payer facts.

Revision ID: 20260828120000_hospital_price_modifier_payer_identity
Revises: 20260827210000_entity_address_geo_taxonomy
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260828120000_hospital_price_modifier_payer_identity"
down_revision = "20260827210000_entity_address_geo_taxonomy"
branch_labels = None
depends_on = None


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def upgrade() -> None:
    table = f'{_q(_schema())}."hospital_price_modifier_payer"'
    constraint = "hospital_price_modifier_payer_shape_check"
    context = op.get_context()
    with context.autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            f"ALTER TABLE {table} "
            "ALTER COLUMN payer_name DROP NOT NULL, "
            "ALTER COLUMN plan_name DROP NOT NULL, "
            f"DROP CONSTRAINT {_q(constraint)}, "
            f"ADD CONSTRAINT {_q(constraint)} CHECK ("
            "payer_ordinal >= 0 AND "
            "((payer_name IS NULL AND plan_name IS NULL) OR "
            "(payer_name IS NOT NULL AND plan_name IS NOT NULL "
            "AND btrim(payer_name) <> '' AND btrim(plan_name) <> '')) "
            "AND (description IS NULL OR btrim(description) <> '') "
            "AND (standard_charge_dollar IS NULL OR standard_charge_dollar > 0) "
            "AND (standard_charge_percentage IS NULL "
            "OR standard_charge_percentage > 0) "
            "AND (standard_charge_algorithm IS NULL "
            "OR btrim(standard_charge_algorithm) <> '') "
            "AND (description IS NOT NULL OR standard_charge_dollar IS NOT NULL "
            "OR standard_charge_percentage IS NOT NULL "
            "OR standard_charge_algorithm IS NOT NULL)) NOT VALID"
        )
        op.execute("RESET lock_timeout")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        f"ALTER TABLE {table} VALIDATE CONSTRAINT {_q(constraint)}"
    )


def downgrade() -> None:
    """Keep nullable paired identity required by the predecessor parser."""

    return None
