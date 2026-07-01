"""Add Provider Directory network catalog table.

Revision ID: 20260701110000_provider_directory_network_catalog
Revises: 20260701100000_entity_address_unified_inferred_npi_index
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260701110000_provider_directory_network_catalog"
down_revision = "20260701100000_entity_address_unified_inferred_npi_index"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade():
    schema = _schema()
    table = _qt(schema, "provider_directory_network_catalog")
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            source_id varchar(64) NOT NULL,
            network_resource_id varchar(256) NOT NULL,
            provider_directory_network_name varchar(512) NOT NULL,
            provider_directory_network_key varchar NOT NULL,
            provider_directory_issuer_key varchar,
            provider_directory_issuer_network_match_key varchar,
            aliases jsonb NOT NULL DEFAULT '[]'::jsonb,
            refs jsonb NOT NULL DEFAULT '[]'::jsonb,
            source_resource_counts jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            insurance_plan_ref_count bigint NOT NULL DEFAULT 0,
            practitioner_role_ref_count bigint NOT NULL DEFAULT 0,
            organization_affiliation_ref_count bigint NOT NULL DEFAULT 0,
            distinct_ref_count bigint NOT NULL DEFAULT 0,
            source_org_name varchar(256),
            source_plan_name varchar(512),
            canonical_api_base text,
            observed_at timestamp,
            published_at timestamp NOT NULL DEFAULT now(),
            PRIMARY KEY (source_id, network_resource_id)
        );
        """
    )
    op.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS provider_directory_network_catalog_source_network_idx
            ON {table} (source_id, network_resource_id);
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_network_catalog_source_idx
            ON {table} (source_id);
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_network_catalog_network_key_idx
            ON {table} (provider_directory_network_key);
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_network_catalog_issuer_network_key_idx
            ON {table} (provider_directory_issuer_network_match_key)
         WHERE provider_directory_issuer_network_match_key IS NOT NULL;
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_network_catalog_name_idx
            ON {table} (provider_directory_network_name);
        """
    )


def downgrade():
    schema = _schema()
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'provider_directory_network_catalog')};")
