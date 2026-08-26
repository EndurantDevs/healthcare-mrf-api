# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add compact hospital-price root and packed data-block storage.

Revision ID: 20260826090000_hospital_price_packed_blocks
Revises: 20260825120000_ptg_v4_finalizer_map_pack
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260826090000_hospital_price_packed_blocks"
down_revision = "20260825120000_ptg_v4_finalizer_map_pack"
branch_labels = None
depends_on = None


_MAX_PAYLOAD_BYTES = 4 * 1024 * 1024 + 64 * 1024 + 72


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


_UPGRADE_SQL = (
    """
        CREATE FUNCTION {reject_update}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'hospital price packed storage is immutable'
                USING ERRCODE = '55000';
        END;
        $$;
    """,
    """
        CREATE TABLE {root} (
            version_id varchar(64) NOT NULL,
            format_version smallint NOT NULL DEFAULT 1,
            service_count bigint NOT NULL,
            charge_count bigint NOT NULL,
            fact_count bigint NOT NULL,
            code_selector_key_count bigint NOT NULL,
            payer_plan_selector_key_count bigint NOT NULL,
            code_selector_ref_count bigint NOT NULL,
            payer_plan_selector_ref_count bigint NOT NULL,
            service_block_count bigint NOT NULL,
            fact_block_count bigint NOT NULL,
            code_selector_page_count bigint NOT NULL,
            payer_plan_selector_page_count bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT "hospital_price_packed_root_pkey"
                PRIMARY KEY (version_id),
            CONSTRAINT "hospital_price_packed_root_version_fkey"
                FOREIGN KEY (version_id) REFERENCES {version} (version_id)
                ON DELETE CASCADE,
            CONSTRAINT "hospital_price_packed_root_format_check"
                CHECK (format_version = 1),
            CONSTRAINT "hospital_price_packed_root_counts_check" CHECK (
                service_count > 0
                AND charge_count >= service_count
                AND service_block_count BETWEEN 1 AND charge_count
                AND code_selector_key_count > 0
                AND code_selector_key_count <= code_selector_page_count
                AND code_selector_page_count <= code_selector_ref_count
                AND code_selector_ref_count >= charge_count
                AND (
                    (
                        fact_count = 0
                        AND fact_block_count = 0
                        AND payer_plan_selector_key_count = 0
                        AND payer_plan_selector_ref_count = 0
                        AND payer_plan_selector_page_count = 0
                    ) OR (
                        fact_count > 0
                        AND fact_block_count BETWEEN 1 AND fact_count
                        AND payer_plan_selector_key_count > 0
                        AND payer_plan_selector_key_count
                            <= payer_plan_selector_page_count
                        AND payer_plan_selector_page_count
                            <= payer_plan_selector_ref_count
                        AND payer_plan_selector_ref_count = fact_count
                    )
                )
            )
        );
    """,
    """
        CREATE TABLE {block} (
            version_id varchar(64) NOT NULL,
            block_kind smallint NOT NULL,
            block_ordinal bigint NOT NULL,
            logical_first bigint NOT NULL,
            logical_count integer NOT NULL,
            secondary_first bigint NOT NULL,
            secondary_count integer NOT NULL,
            page_index integer NOT NULL,
            page_count integer NOT NULL,
            key_sha256 bytea,
            parent_sha256 bytea,
            payload_sha256 bytea NOT NULL,
            payload bytea NOT NULL,
            CONSTRAINT "hospital_price_data_block_pkey"
                PRIMARY KEY (version_id, block_kind, block_ordinal),
            CONSTRAINT "hospital_price_data_block_root_fkey"
                FOREIGN KEY (version_id) REFERENCES {root} (version_id)
                ON DELETE CASCADE,
            CONSTRAINT "hospital_price_data_block_common_check" CHECK (
                block_kind BETWEEN 1 AND 4
                AND block_ordinal >= 0
                AND logical_first >= 0
                AND logical_count > 0
                AND secondary_first >= 0
                AND secondary_count >= 0
                AND page_index >= 0
                AND page_count >= 0
                AND (key_sha256 IS NULL OR octet_length(key_sha256) = 32)
                AND (
                    parent_sha256 IS NULL
                    OR octet_length(parent_sha256) = 32
                )
            ),
            CONSTRAINT "hospital_price_data_block_payload_check" CHECK (
                octet_length(payload_sha256) = 32
                AND payload_sha256 = pg_catalog.sha256(payload)
                AND octet_length(payload) BETWEEN 1 AND {max_payload_bytes}
            ),
            CONSTRAINT "hospital_price_data_block_kind_shape_check" CHECK (
                (
                    block_kind = 1
                    AND logical_count BETWEEN 1 AND 512
                    AND secondary_count BETWEEN 1 AND 512
                    AND page_index = 0 AND page_count = 0
                    AND key_sha256 IS NULL AND parent_sha256 IS NULL
                ) OR (
                    block_kind = 2
                    AND logical_count BETWEEN 1 AND 512
                    AND secondary_first = 0 AND secondary_count = 0
                    AND page_index = 0 AND page_count = 0
                    AND key_sha256 IS NULL AND parent_sha256 IS NULL
                ) OR (
                    block_kind = 3
                    AND logical_first < 1000000
                    AND logical_count = 1
                    AND secondary_count BETWEEN 1 AND 524288
                    AND page_count > 0 AND page_index < page_count
                    AND key_sha256 IS NOT NULL
                    AND parent_sha256 IS NULL
                ) OR (
                    block_kind = 4
                    AND logical_first < 1000000
                    AND logical_count = 1
                    AND secondary_count BETWEEN 1 AND 524288
                    AND page_count > 0 AND page_index < page_count
                    AND key_sha256 IS NOT NULL
                    AND parent_sha256 IS NOT NULL
                )
            )
        );
    """,
    """
        CREATE UNIQUE INDEX "hospital_price_data_block_selector_ordinal_key"
            ON {block} (version_id, logical_first, page_index)
            WHERE block_kind IN (3, 4);
    """,
    """
        CREATE INDEX "hospital_price_data_block_selector_lookup_idx"
            ON {block} (
                version_id, block_kind, key_sha256, logical_first, page_index
            ) WHERE block_kind IN (3, 4);
    """,
    """
        CREATE INDEX "hospital_price_data_block_parent_lookup_idx"
            ON {block} (
                version_id, parent_sha256, logical_first, page_index
            ) WHERE block_kind = 4;
    """,
    """
        CREATE INDEX "hospital_price_data_block_charge_range_idx"
            ON {block} (version_id, secondary_first DESC)
            WHERE block_kind = 1;
    """,
    """
        CREATE INDEX "hospital_price_data_block_fact_range_idx"
            ON {block} (version_id, logical_first DESC)
            WHERE block_kind = 2;
    """,
    """
        ALTER TABLE {block} ALTER COLUMN payload SET STORAGE EXTERNAL;
    """,
    """
        CREATE TRIGGER "hospital_price_packed_root_reject_update"
        BEFORE UPDATE ON {root} FOR EACH ROW
        EXECUTE FUNCTION {reject_update}();
    """,
    """
        CREATE TRIGGER "hospital_price_data_block_reject_update"
        BEFORE UPDATE ON {block} FOR EACH ROW
        EXECUTE FUNCTION {reject_update}();
    """,
)


def _migration_names(schema: str) -> dict[str, str]:
    return {
        "version": _qt(schema, "hospital_price_version"),
        "root": _qt(schema, "hospital_price_packed_root"),
        "block": _qt(schema, "hospital_price_data_block"),
        "reject_update": _qt(schema, "hospital_price_reject_packed_update"),
        "max_payload_bytes": str(_MAX_PAYLOAD_BYTES),
    }


def upgrade() -> None:
    """Add the packed root, blocks, collision-safe lookups, and range indexes."""

    names = _migration_names(_schema())
    for statement in _UPGRADE_SQL:
        op.execute(statement.format_map(names))


def downgrade() -> None:
    """Remove only the additive packed hospital-price storage."""

    names = _migration_names(_schema())
    op.execute(f"LOCK TABLE {names['root']} IN ACCESS EXCLUSIVE MODE;")
    op.execute(
        f"""DO $hospital_price_packed_downgrade$
        BEGIN
            IF EXISTS (SELECT 1 FROM {names['root']} LIMIT 1) THEN
                RAISE EXCEPTION
                    'HOSPITAL_PRICE_PACKED_DOWNGRADE_BLOCKED: packed versions exist'
                    USING ERRCODE = '55000';
            END IF;
        END
        $hospital_price_packed_downgrade$;"""
    )
    op.execute(f"DROP TABLE IF EXISTS {names['block']};")
    op.execute(f"DROP TABLE IF EXISTS {names['root']};")
    op.execute(f"DROP FUNCTION IF EXISTS {names['reject_update']}();")
