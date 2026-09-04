# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retire Michigan's superseded reviewed acquisition generation.

Revision ID: 20260904223000_provider_directory_michigan_generation_retirement
Revises: 20260905130000_hospital_price_csv_3_0_1
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260904223000_provider_directory_michigan_generation_retirement"
down_revision = "20260905130000_hospital_price_csv_3_0_1"
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


_RETIREMENT_SQL_TEMPLATE = """
    DO $migration$
    DECLARE
        source_endpoint_id text;
        source_api_base text;
        source_metadata jsonb;
        affected_rows bigint;
    BEGIN
        SELECT endpoint_id, canonical_api_base, metadata_json::jsonb
          INTO source_endpoint_id, source_api_base, source_metadata
          FROM {source_table}
         WHERE source_id = 'pdfhir_75511676b61b2bddb6f94322'
         FOR UPDATE;
        IF NOT FOUND THEN
            RETURN;
        END IF;
        IF NOT (
            source_metadata ? 'provider_directory_candidate_status'
            OR source_metadata ? 'provider_directory_verification_campaign_id'
        ) THEN
            RETURN;
        END IF;
        IF source_endpoint_id IS DISTINCT FROM
                   'cce4c9f158fb638bf43b5c659a2b5526aa12f2fc5cca247c622442cd537e4510'
           OR source_api_base IS DISTINCT FROM
                   'https://mi.fhir.mhbapp.com/pd/api/v1'
           OR source_metadata ->> 'provider_directory_configured_endpoint_id'
                  IS DISTINCT FROM
                   'ec3b30a95396d3e30e7a433d523a775de9075543a39e058948c6215650cea684'
           OR source_metadata ->> 'provider_directory_override'
                  IS DISTINCT FROM
                   'michigan_mhbapp_public_provider_directory'
           OR source_metadata ->> 'provider_directory_acquisition_enabled'
                  IS DISTINCT FROM 'true'
           OR source_metadata ->> 'provider_directory_candidate_status'
                  IS DISTINCT FROM
                   'pending_two_matching_exhaustive_acquisitions'
           OR source_metadata ->> 'provider_directory_verification_campaign_id'
                  IS DISTINCT FROM
                   'provider-directory-michigan-2026-07-19-v1'
           OR source_metadata ? 'provider_directory_reviewed_root_policy_v1'
           OR source_metadata ? 'provider_directory_reviewed_subset_activation_v1'
           OR source_metadata ? 'provider_directory_reviewed_subset_activation_v2'
        THEN
            RAISE EXCEPTION
                'provider_directory_michigan_generation_retirement_state_invalid'
                USING ERRCODE = '55000';
        END IF;
        UPDATE {source_table}
           SET metadata_json = (
                   source_metadata
                   - 'provider_directory_candidate_status'
                   - 'provider_directory_verification_campaign_id'
               )::json,
               updated_at = pg_catalog.transaction_timestamp()
         WHERE source_id = 'pdfhir_75511676b61b2bddb6f94322'
           AND endpoint_id = source_endpoint_id;
        GET DIAGNOSTICS affected_rows = ROW_COUNT;
        IF affected_rows <> 1 THEN
            RAISE EXCEPTION
                'provider_directory_michigan_generation_retirement_cas_failed'
                USING ERRCODE = '40001';
        END IF;
    END;
    $migration$;
    """


def _retirement_sql(schema: str) -> str:
    """Render the exact fenced Michigan source retirement statement."""

    source_table = f"{_q(schema)}.{_q('provider_directory_source')}"
    return _RETIREMENT_SQL_TEMPLATE.format(source_table=source_table)


def upgrade() -> None:
    """Retire only the obsolete Michigan candidate-generation metadata."""

    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(_retirement_sql(_schema()))


def downgrade() -> None:
    """Keep obsolete review state retired so ordinary acquisition stays valid."""

    return None
