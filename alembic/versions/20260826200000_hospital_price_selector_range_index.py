# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Index packed hospital-price selector ranges.

Revision ID: 20260826200000_hospital_price_selector_range_index
Revises: 20260825150000_plan_pricing_card_projection
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260826200000_hospital_price_selector_range_index"
down_revision = "20260825150000_plan_pricing_card_projection"
branch_labels = None
depends_on = None


_INDEX_NAME = "hospital_price_data_block_selector_secondary_lookup_idx"


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
    """Cover selector ownership lookups by their stored range boundary."""

    op.execute(
        f"""CREATE INDEX {_q(_INDEX_NAME)}
        ON {_q(_schema())}."hospital_price_data_block"
            (version_id, block_kind, key_sha256, secondary_first)
        WHERE block_kind IN (3, 4);"""
    )


def downgrade() -> None:
    """Remove only the additive selector range index."""

    op.execute(f"DROP INDEX {_q(_schema())}.{_q(_INDEX_NAME)};")
