# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit the lossless negotiated-rate-term parser contract.

Revision ID: 20260902160000_hospital_price_rate_term
Revises: 20260902103500_hospital_price_count_invariants
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260902160000_hospital_price_rate_term"
down_revision = "20260902103500_hospital_price_count_invariants"
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
    """Admit v6 while preserving every prior parser and count guard."""

    schema = _q(_schema())
    modifier_payer = f'{schema}."hospital_price_modifier_payer"'
    op.execute(
        f"ALTER TABLE {modifier_payer} "
        "ALTER COLUMN payer_name DROP NOT NULL, "
        "ALTER COLUMN plan_name DROP NOT NULL, "
        "ADD COLUMN negotiated_rate_term text, "
        "DROP CONSTRAINT hospital_price_modifier_payer_shape_check, "
        "ADD CONSTRAINT hospital_price_modifier_payer_shape_check CHECK ("
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
        "OR standard_charge_algorithm IS NOT NULL) "
        "AND (negotiated_rate_term IS NULL OR "
        "(payer_name IS NOT NULL AND plan_name IS NOT NULL "
        "AND btrim(negotiated_rate_term) <> ''))) NOT VALID;"
    )
    op.execute(
        f"ALTER TABLE {modifier_payer} "
        "VALIDATE CONSTRAINT hospital_price_modifier_payer_shape_check;"
    )
    table = f'{schema}."hospital_price_version"'
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
        "AND attester_name IS NULL))) OR ("
        "parser_contract_sha256 = "
        "'b432ff0aa9aec898d59d303344c63dd3805f37608a81dfd0118c99019afc16a1' "
        "AND ((source_format = 'json' AND template_version IN "
        "('2.2.0', '2.2.1', '3.0.0')) OR (source_format IN "
        "('csv-tall', 'csv-wide') AND template_version IN "
        "('2.0.0', '2.2.0', '2.2.1', '3.0.0'))) "
        "AND ((template_version = '3.0.0' AND npi_count > 0 "
        "AND attester_name IS NOT NULL) OR (template_version IN "
        "('2.0.0', '2.2.0', '2.2.1') AND ((source_format = 'json' "
        "AND npi_count = 0 AND attester_name IS NULL) OR "
        "(source_format IN ('csv-tall', 'csv-wide') AND npi_count >= 0 "
        "AND (attester_name IS NULL OR btrim(attester_name) <> ''))))) OR ("
        "parser_contract_sha256 IN ("
        "'1a632748216eb5373e2c55a29f328c2ce81aee3d3ae13e024bbc1c300fa10173', "
        "'d2725216821ac8aa9b9405f2a95e50e3899c524eb23b0663bdce15279498ad39') "
        "AND ((source_format = 'json' AND template_version IN "
        "('2.2.0', '2.2.1', '3.0.0')) OR (source_format IN "
        "('csv-tall', 'csv-wide') AND template_version IN "
        "('2', '2.0.0', '2.2.0', '2.2.1', '3.0.0'))) "
        "AND ((template_version = '3.0.0' AND npi_count > 0 "
        "AND attester_name IS NOT NULL) OR (template_version IN "
        "('2', '2.0.0', '2.2.0', '2.2.1') AND "
        "((source_format = 'json' AND npi_count = 0 "
        "AND attester_name IS NULL) OR (source_format IN "
        "('csv-tall', 'csv-wide') AND npi_count >= 0 "
        "AND (attester_name IS NULL OR btrim(attester_name) <> '')))))))) "
        "AND location_count > 0 AND license_count > 0 "
        "AND service_count > 0 AND charge_count > 0 "
        "AND payer_charge_count >= 0);"
    )


def downgrade() -> None:
    """Preserve forward-compatible term data while moving the revision stamp."""

    return None
