# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure metadata belongs to the exact committed isolated SOURCE candidate."""

import json

from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_source_storage_binding import require_storage_schema


async def mark_source_candidate(directory, candidate, status, metadata):
    """Keep historical no-op/immutability rules without completing or publishing payloads."""
    identity = storage.candidate_identity(directory, candidate)
    async with source_transaction(directory.db):
        publisher = bound_publisher()
        connection, policy = publisher.connection, publisher.policy
        require_storage_schema(publisher, directory._schema())
        header = storage_relation(policy.schema, storage.HEADER)
        previous = await connection.fetchrow(
            f"SELECT endpoint_id,acquisition_root_run_id,status,is_current,publication_metadata_json,"
            f"candidate_storage_json FROM {header} WHERE dataset_id=$1 FOR UPDATE",
            candidate.dataset_id,
        )
        if not _is_mutable_header(previous, identity, directory.IMMUTABLE_ENDPOINT_DATASET_STATUSES):
            return
        binding = storage._json(previous["candidate_storage_json"])
        storage._validate_binding(binding, identity, phases=("loading", "prepared"))
        previous_metadata = storage._json(previous["publication_metadata_json"])
        storage._require(
            type(previous_metadata) is dict
            and all(
                previous_metadata.get(key) == expected_value for key, expected_value in identity["metadata"].items()
            ),
            "acquisition_changed",
        )
        await storage._verify_tables(connection, policy, binding)
        resource_count = await _committed_resource_count(connection, policy, binding)
        updated = await connection.execute(
            f"UPDATE {header} SET status=$2,resource_count=$3,publication_metadata_json=$4::jsonb "
            "WHERE dataset_id=$1 AND endpoint_id=$5 AND acquisition_root_run_id IS NOT DISTINCT FROM $6 "
            "AND status=$7 AND NOT is_current AND candidate_storage_json IS NOT DISTINCT FROM $8::jsonb "
            "AND publication_metadata_json::jsonb IS NOT DISTINCT FROM $9::jsonb",
            candidate.dataset_id,
            status,
            resource_count,
            json.dumps(metadata, sort_keys=True, default=directory._json_default),
            identity["endpoint_id"],
            identity["acquisition_root_run_id"],
            previous["status"],
            storage._encoded(binding),
            storage._encoded(previous_metadata),
        )
        storage._require(updated == "UPDATE 1", "failure_header_compare_failed")


def _is_mutable_header(previous, identity, immutable_statuses):
    return (
        previous is not None
        and previous["endpoint_id"] == identity["endpoint_id"]
        and previous["acquisition_root_run_id"] == identity["acquisition_root_run_id"]
        and previous["is_current"] is False
        and previous["status"] not in immutable_statuses
    )


async def _committed_resource_count(connection, policy, binding):
    """Fold complete keys with the same cutoffs as completion, in one aggregate."""
    if binding["phase"] == "prepared":
        table = storage_relation(policy.schema, binding["tables"][storage.RESOURCE]["name"])
        count = await connection.fetchval(
            f"SELECT count(*) FROM {table} WHERE dataset_id=$1 AND resource_type NOT LIKE 'LU:%:pass:%'",
            binding["identity"]["dataset_id"],
        )
    else:
        table = storage_relation(policy.schema, binding["tables"]["resource_input"]["name"])
        count = await connection.fetchval(
            f'SELECT count(*) FROM (SELECT DISTINCT ON (resource_type COLLATE "C",resource_id COLLATE "C") '
            f"resource_type,resource_id FROM {table} WHERE dataset_id=$1 AND batch_ordinal<= $4 "
            "AND batch_ordinal>greatest($2::bigint,coalesce(($3::jsonb->>resource_type)::bigint,0)) "
            "AND resource_type NOT LIKE 'LU:%:pass:%' "
            'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal DESC,row_ordinal DESC) latest',
            binding["identity"]["dataset_id"],
            binding["resets"].get("*", 0),
            storage._encoded(binding["resets"]),
            binding["ordinal"],
        )
    storage._require(type(count) is int and count >= 0, "failure_resource_count_invalid")
    return count
