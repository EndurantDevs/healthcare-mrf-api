"""Reconcile the published ALOHR dataset resource contract.

Revision ID: 20260714161000_alohr_contract_profile
Revises: 20260714150000_provider_directory_pagination_census
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260714161000_alohr_contract_profile"
down_revision = "20260714150000_provider_directory_pagination_census"
branch_labels = None
depends_on = None


ALOHR_SOURCE_ID = "pdfhir_0f81c146991b27031b1ec366"
CORRECT_PROFILE = (
    "Location",
    "Organization",
    "Practitioner",
    "PractitionerRole",
)
STALE_PROFILE = (
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
)


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _json_array(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(f'"{value}"' for value in values) + "]"


def _repair_sql(schema: str) -> str:
    correct_json = _json_array(CORRECT_PROFILE)
    stale_json = _json_array(STALE_PROFILE)
    correct_array = ", ".join(f"'{value}'" for value in CORRECT_PROFILE)
    return f"""
        UPDATE {schema}.provider_directory_endpoint_dataset AS dataset
           SET publication_metadata_json = jsonb_set(
                   jsonb_set(
                       jsonb_set(
                           dataset.publication_metadata_json
                               #- '{{resource_diagnostics,OrganizationAffiliation}}'
                               #- '{{completion_proof_v1,resource_diagnostics,OrganizationAffiliation}}',
                           '{{selected_resources}}',
                           '{correct_json}'::jsonb,
                           false
                       ),
                       '{{expected_resources}}',
                       '{correct_json}'::jsonb,
                       false
                   ),
                   '{{completion_proof_v1,selected_resources}}',
                   '{correct_json}'::jsonb,
                   false
               )
          FROM {schema}.provider_directory_source AS source
         WHERE source.source_id = '{ALOHR_SOURCE_ID}'
           AND source.endpoint_id = dataset.endpoint_id
           AND dataset.is_current IS TRUE
           AND dataset.status = 'published'
           AND dataset.publication_metadata_json -> 'selected_resources'
               = '{stale_json}'::jsonb
           AND dataset.publication_metadata_json -> 'expected_resources'
               = '{stale_json}'::jsonb
           AND dataset.publication_metadata_json
               #> '{{completion_proof_v1,selected_resources}}'
               = '{stale_json}'::jsonb
           AND (
                SELECT array_agg(DISTINCT resource.resource_type ORDER BY resource.resource_type)
                  FROM {schema}.provider_directory_dataset_resource AS resource
                 WHERE resource.dataset_id = dataset.dataset_id
                   AND resource.resource_type NOT LIKE 'LU:%:pass:%'
               ) = ARRAY[{correct_array}]::varchar[];
    """


def upgrade() -> None:
    op.execute(sa.text(_repair_sql(_schema())))


def downgrade() -> None:
    # Reintroducing a resource claim unsupported by the stored dataset is unsafe.
    return None
