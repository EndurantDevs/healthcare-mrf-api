# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pack multiple hospital-price selector keys into authenticated pages.

Revision ID: 20260827160000_hospital_price_selector_page_packing
Revises: 20260827120000_hospital_price_source_format
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260827160000_hospital_price_selector_page_packing"
down_revision = "20260827120000_hospital_price_source_format"
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
    """Add v2 selector-pack counts and relax only their immutable shapes."""

    schema = _q(_schema())
    root = f'{schema}."hospital_price_packed_root"'
    block = f'{schema}."hospital_price_data_block"'
    op.execute(
        f"""ALTER TABLE {root}
        ADD COLUMN code_selector_block_count bigint,
        ADD COLUMN payer_plan_selector_block_count bigint"""
    )
    op.execute(
        f"ALTER TABLE {root} DISABLE TRIGGER "
        "hospital_price_packed_root_reject_update"
    )
    op.execute(
        f"""UPDATE {root} root SET
          code_selector_block_count=(SELECT COUNT(*) FROM {block} child
            WHERE child.version_id=root.version_id AND child.block_kind=3),
          payer_plan_selector_block_count=(SELECT COUNT(*) FROM {block} child
            WHERE child.version_id=root.version_id AND child.block_kind=4)"""
    )
    op.execute(
        f"ALTER TABLE {root} ENABLE TRIGGER "
        "hospital_price_packed_root_reject_update"
    )
    op.execute(
        f"""ALTER TABLE {root}
        ALTER COLUMN code_selector_block_count SET NOT NULL,
        ALTER COLUMN payer_plan_selector_block_count SET NOT NULL,
        DROP CONSTRAINT hospital_price_packed_root_format_check,
        DROP CONSTRAINT hospital_price_packed_root_counts_check,
        ADD CONSTRAINT hospital_price_packed_root_format_check
          CHECK (format_version IN (1, 2)),
        ADD CONSTRAINT hospital_price_packed_root_counts_check CHECK (
          service_count > 0
          AND charge_count >= service_count
          AND service_block_count BETWEEN 1 AND charge_count
          AND code_selector_key_count > 0
          AND code_selector_page_count BETWEEN code_selector_key_count
              AND code_selector_ref_count
          AND code_selector_block_count BETWEEN 1 AND code_selector_page_count
          AND code_selector_ref_count >= charge_count
          AND ((fact_count = 0
            AND fact_block_count = 0
            AND payer_plan_selector_key_count = 0
            AND payer_plan_selector_ref_count = 0
            AND payer_plan_selector_page_count = 0
            AND payer_plan_selector_block_count = 0)
          OR (fact_count > 0
            AND fact_block_count BETWEEN 1 AND fact_count
            AND payer_plan_selector_key_count > 0
            AND payer_plan_selector_page_count BETWEEN
                payer_plan_selector_key_count AND payer_plan_selector_ref_count
            AND payer_plan_selector_block_count BETWEEN 1
                AND payer_plan_selector_page_count
            AND payer_plan_selector_ref_count = fact_count))
        )"""
    )
    op.execute(
        f"""ALTER TABLE {block}
        DROP CONSTRAINT hospital_price_data_block_kind_shape_check,
        ADD CONSTRAINT hospital_price_data_block_kind_shape_check CHECK (
          (block_kind = 1
            AND logical_count BETWEEN 1 AND 512
            AND secondary_count BETWEEN 1 AND 512
            AND page_index = 0 AND page_count = 0
            AND key_sha256 IS NULL AND parent_sha256 IS NULL)
          OR (block_kind = 2
            AND logical_count BETWEEN 1 AND 512
            AND secondary_first = 0 AND secondary_count = 0
            AND page_index = 0 AND page_count = 0
            AND key_sha256 IS NULL AND parent_sha256 IS NULL)
          OR (block_kind = 3
            AND logical_first < 1000000
            AND logical_count BETWEEN 1 AND 256
            AND secondary_count BETWEEN 1 AND 524288
            AND page_count > 0 AND page_index < page_count
            AND key_sha256 IS NOT NULL
            AND (logical_count = 1 OR (page_index = 0 AND page_count = 1
              AND parent_sha256 IS NOT NULL)))
          OR (block_kind = 4
            AND logical_first < 1000000
            AND logical_count BETWEEN 1 AND 256
            AND secondary_count BETWEEN 1 AND 524288
            AND page_count > 0 AND page_index < page_count
            AND key_sha256 IS NOT NULL AND parent_sha256 IS NOT NULL
            AND (logical_count = 1 OR (page_index = 0 AND page_count = 1)))
        )"""
    )


def downgrade() -> None:
    """Remove v2 rows only when restoring the one-key v1 constraint."""

    schema = _q(_schema())
    root = f'{schema}."hospital_price_packed_root"'
    block = f'{schema}."hospital_price_data_block"'
    op.execute(
        f"""DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM {root} WHERE format_version = 2) THEN
            RAISE EXCEPTION 'cannot downgrade while hospital selector v2 roots exist';
          END IF;
        END $$"""
    )
    op.execute(
        f"""ALTER TABLE {block}
        DROP CONSTRAINT hospital_price_data_block_kind_shape_check,
        ADD CONSTRAINT hospital_price_data_block_kind_shape_check CHECK (
          (block_kind = 1
            AND logical_count BETWEEN 1 AND 512
            AND secondary_count BETWEEN 1 AND 512
            AND page_index = 0 AND page_count = 0
            AND key_sha256 IS NULL AND parent_sha256 IS NULL)
          OR (block_kind = 2
            AND logical_count BETWEEN 1 AND 512
            AND secondary_first = 0 AND secondary_count = 0
            AND page_index = 0 AND page_count = 0
            AND key_sha256 IS NULL AND parent_sha256 IS NULL)
          OR (block_kind = 3
            AND logical_first < 1000000 AND logical_count = 1
            AND secondary_count BETWEEN 1 AND 524288
            AND page_count > 0 AND page_index < page_count
            AND key_sha256 IS NOT NULL AND parent_sha256 IS NULL)
          OR (block_kind = 4
            AND logical_first < 1000000 AND logical_count = 1
            AND secondary_count BETWEEN 1 AND 524288
            AND page_count > 0 AND page_index < page_count
            AND key_sha256 IS NOT NULL AND parent_sha256 IS NOT NULL)
        )"""
    )
    op.execute(
        f"""ALTER TABLE {root}
        DROP CONSTRAINT hospital_price_packed_root_format_check,
        DROP CONSTRAINT hospital_price_packed_root_counts_check,
        ADD CONSTRAINT hospital_price_packed_root_format_check
          CHECK (format_version = 1),
        ADD CONSTRAINT hospital_price_packed_root_counts_check CHECK (
          service_count > 0
          AND charge_count >= service_count
          AND service_block_count BETWEEN 1 AND charge_count
          AND code_selector_key_count > 0
          AND code_selector_key_count <= code_selector_page_count
          AND code_selector_page_count <= code_selector_ref_count
          AND code_selector_ref_count >= charge_count
          AND ((fact_count = 0
            AND fact_block_count = 0
            AND payer_plan_selector_key_count = 0
            AND payer_plan_selector_ref_count = 0
            AND payer_plan_selector_page_count = 0)
          OR (fact_count > 0
            AND fact_block_count BETWEEN 1 AND fact_count
            AND payer_plan_selector_key_count > 0
            AND payer_plan_selector_key_count <= payer_plan_selector_page_count
            AND payer_plan_selector_page_count <= payer_plan_selector_ref_count
            AND payer_plan_selector_ref_count = fact_count))
        )"""
    )
    op.execute(
        f"""ALTER TABLE {root}
        DROP COLUMN payer_plan_selector_block_count,
        DROP COLUMN code_selector_block_count"""
    )
