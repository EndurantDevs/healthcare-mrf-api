# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Composable registry fixtures for Flex publication PostgreSQL proofs."""

from __future__ import annotations

import json

from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_API_BASE,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
)
from tests.formulary_fhir_twin_admission_pg_support import quoted


async def extend_flex_publication_foundation(connection, schema_name: str) -> None:
    """Add production columns used by the focused publication migrations."""

    schema = quoted(schema_name)
    await connection.execute(
        f"""
        ALTER TABLE {schema}.provider_directory_api_endpoint
            ADD COLUMN canonical_api_base text,
            ADD COLUMN metadata_json jsonb,
            ADD COLUMN endpoint_signature_hash varchar(64);
        ALTER TABLE {schema}.provider_directory_source
            ADD COLUMN canonical_api_base text,
            ADD COLUMN requires_registration boolean,
            ADD COLUMN requires_api_key boolean,
            ADD COLUMN auth_type text,
            ADD COLUMN metadata_json jsonb;
        ALTER TABLE {schema}.provider_directory_endpoint_dataset
            ADD COLUMN import_run_id varchar(64),
            ADD COLUMN previous_dataset_id varchar(96),
            ADD COLUMN created_at timestamp,
            ADD COLUMN validated_at timestamp,
            ADD COLUMN published_at timestamp,
            ADD COLUMN superseded_at timestamp,
            ADD COLUMN completion_proof_required_version integer,
            ADD COLUMN completion_proof_json jsonb,
            ADD COLUMN completion_proof_sha256 varchar(64);
        ALTER TABLE {schema}.provider_directory_dataset_resource
            ADD COLUMN acquired_resource_sha256 varchar(64);
        CREATE UNIQUE INDEX provider_directory_endpoint_dataset_current_idx
            ON {schema}.provider_directory_endpoint_dataset(endpoint_id)
            WHERE is_current IS TRUE;
        CREATE UNIQUE INDEX provider_directory_endpoint_dataset_root_idx
            ON {schema}.provider_directory_endpoint_dataset(
                endpoint_id, acquisition_root_run_id
            ) WHERE acquisition_root_run_id IS NOT NULL;
        """
    )


def _registry_metadata_json() -> tuple[str, str]:
    endpoint_metadata_by_field = {
        "authority_id": "unitedhealthcare",
        "resource_types": ["Practitioner"],
    }
    source_metadata_by_field = {
        "provider_directory_acquisition_enabled": False,
        "provider_directory_acquisition_mode": "manual",
        "provider_directory_authority_id": "unitedhealthcare",
        "provider_directory_connector_id": (
            "pdufpc_16ebdbf260dc9815ae38830a6991fea5d6533ab8db7389da"
        ),
        "provider_directory_endpoint_collection_complete": False,
        "provider_directory_endpoint_complete": False,
        "provider_directory_query_contract_id": (
            "healthporta.provider-directory.uhc-flex-practitioner-exact-npi.v1"
        ),
        "provider_directory_resource_types": ["Practitioner"],
    }
    return (
        json.dumps(endpoint_metadata_by_field),
        json.dumps(source_metadata_by_field),
    )


async def _insert_registry_pair(
    connection,
    schema: str,
    *,
    source_id: str,
    endpoint_id: str,
    api_base: str,
    endpoint_signature_hash: str,
) -> None:
    endpoint_metadata_json, source_metadata_json = _registry_metadata_json()
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_api_endpoint "
        "(endpoint_id, canonical_api_base, metadata_json, "
        "endpoint_signature_hash) VALUES ($1, $2, $3::jsonb, $4)",
        endpoint_id,
        api_base,
        endpoint_metadata_json,
        endpoint_signature_hash,
    )
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_source "
        "(source_id, endpoint_id, canonical_api_base, requires_registration, "
        "requires_api_key, auth_type, metadata_json) "
        "VALUES ($1, $2, $3, false, false, 'none', $4::jsonb)",
        source_id,
        endpoint_id,
        api_base,
        source_metadata_json,
    )


async def seed_exact_publication_registry(
    connection,
    schema_name: str,
    *,
    include_rooted: bool = False,
) -> None:
    """Seed the exact legacy pair and optionally its rooted successor pair."""

    schema = quoted(schema_name)
    legacy_endpoint = uhc_flex_practitioner_endpoint_identity()
    await _insert_registry_pair(
        connection,
        schema,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        endpoint_id=legacy_endpoint.endpoint_id,
        api_base=UHC_FLEX_PRACTITIONER_API_BASE,
        endpoint_signature_hash=legacy_endpoint.endpoint_signature_hash,
    )
    if include_rooted:
        await _insert_registry_pair(
            connection,
            schema,
            source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
            endpoint_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
            api_base=PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
            endpoint_signature_hash=(
                PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
            ),
        )
