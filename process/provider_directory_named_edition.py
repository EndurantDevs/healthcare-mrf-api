# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Locate immutable Source editions through the catalog pointer."""

import hashlib

from db.plan_release_storage import storage_relation

POINTER_SCHEMA = "provider_directory_editions"
POINTER_TABLE = "current_edition"
HEADER = "provider_directory_endpoint_dataset"


def edition_schema_name(dataset_id):
    """Derive a bounded SQL name from the logical dataset identifier."""
    return "directory_edition_" + hashlib.sha256(dataset_id.encode()).hexdigest()[:32]


async def current_edition(connection, catalog_schema, endpoint_id):
    """Resolve the edition in the caller's read transaction."""
    pointer = storage_relation(POINTER_SCHEMA, POINTER_TABLE)
    return await connection.fetchrow(
        f"SELECT dataset_id,schema_name,previous_dataset_id,previous_schema_name FROM {pointer} "
        "WHERE catalog_schema=$1 AND endpoint_id=$2",
        catalog_schema,
        endpoint_id,
    )


async def find_current_edition(connection, catalog_schema, endpoint_id):
    """Allow existing installations to read legacy catalog editions before provisioning."""
    pointer = storage_relation(POINTER_SCHEMA, POINTER_TABLE)
    if await connection.fetchval("SELECT to_regclass($1)::text", pointer) is None:
        return None
    return await current_edition(connection, catalog_schema, endpoint_id)


async def read_edition_header(connection, schema, dataset_id):
    """Read one immutable edition without consulting database-local metadata."""
    header = storage_relation(schema, HEADER)
    return await connection.fetchrow(
        f"SELECT dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,resource_count,"
        f"publication_metadata_json FROM {header} "
        "WHERE dataset_id=$1 AND status='published' AND published_at IS NOT NULL",
        dataset_id,
    )
