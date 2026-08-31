# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Represent source-faithful legacy hospital MRF headers.

Revision ID: 20260831100000_hospital_price_legacy_header
Revises: 20260830100000_provider_directory_rooted_partial_lineage
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260831100000_hospital_price_legacy_header"
down_revision = "20260830100000_provider_directory_rooted_partial_lineage"
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
    """Keep fields absent from declared legacy CMS profiles absent."""

    table = f'{_q(_schema())}."hospital_price_version"'
    op.execute(f"ALTER TABLE {table} ALTER COLUMN attester_name DROP NOT NULL;")
    op.execute(
        f"ALTER TABLE {table} "
        "DROP CONSTRAINT hospital_price_version_shape_check;"
    )
    op.execute(
        f"ALTER TABLE {table} "
        "ADD CONSTRAINT hospital_price_version_shape_check CHECK ("
        "version_id ~ '^[0-9a-f]{64}$' "
        "AND semantic_sha256 ~ '^[0-9a-f]{64}$' "
        "AND source_format IN ('json', 'csv-tall', 'csv-wide') "
        "AND ((parser_contract_sha256 IN ("
        "'6de516d11a99e85c00b9fe6488698a2a165436bf39d4351a0c54f58729150a66', "
        "'3857e492234361a91ebf6baa8c0c0d8832427b4bf5fce87729f15cd767c9be75') "
        "AND npi_count > 0 AND attester_name IS NOT NULL) OR ("
        "parser_contract_sha256 = "
        "'0048bd71229567de7ab5cbed73e7547d6718140dd8c4c9e39e3816c9798b8699' "
        "AND ((source_format = 'json' AND template_version IN "
        "('2.2.0', '2.2.1', '3.0.0')) OR (source_format IN "
        "('csv-tall', 'csv-wide') AND template_version IN "
        "('2.0.0', '2.2.0', '2.2.1', '3.0.0'))) "
        "AND ((template_version = '3.0.0' AND npi_count > 0 "
        "AND attester_name IS NOT NULL) OR (template_version IN "
        "('2.0.0', '2.2.0', '2.2.1') AND npi_count = 0 "
        "AND attester_name IS NULL)))) "
        "AND location_count > 0 AND license_count > 0 "
        "AND service_count > 0 AND charge_count > 0 "
        "AND payer_charge_count >= 0);"
    )


def downgrade() -> None:
    """Retain nullable source evidence rather than destroy valid legacy rows."""

    return None
