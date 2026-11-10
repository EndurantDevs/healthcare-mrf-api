# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure metadata belongs to the exact committed isolated SOURCE candidate."""

import json

from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_source_storage_binding import require_storage_schema
from process.vsp_directory_integration import is_failed_capture_retry, is_rearmed_capture_retry


def is_retained_vsp_capture_retry(candidate):
    """Identify a candidate admitted for a retained capture rather than a reset."""
    context = candidate.checkpoint_context
    return bool(candidate.repair_empty_orphan and context and context.require_retained_vsp_capture)


async def orphan_candidate_status(directory, existing_by_field, candidate):
    """Return the expected status and whether the isolated empty proof passed."""
    checkpoint = candidate.checkpoint_context
    if is_retained_vsp_capture_retry(candidate):
        if not is_failed_capture_retry(existing_by_field, checkpoint):
            raise RuntimeError("provider_directory_endpoint_dataset_candidate_stale")
        return directory.ENDPOINT_DATASET_FAILED, await storage.has_empty_isolated_vsp_storage(
            directory, candidate, existing_by_field
        )
    metadata = existing_by_field.get("publication_metadata_json")
    if isinstance(metadata, dict) and metadata.get("error") == "vsp_prepared_candidate_required":
        if not is_rearmed_capture_retry(existing_by_field, checkpoint):
            raise RuntimeError("provider_directory_endpoint_dataset_candidate_stale")
        return directory.ENDPOINT_DATASET_ACQUIRING, await storage.has_empty_isolated_vsp_storage(
            directory, candidate, existing_by_field
        )
    return directory.ENDPOINT_DATASET_ACQUIRING, False


async def has_verified_empty_orphan(directory, existing_by_field, candidate):
    """Check the orphan's identity and return whether its isolated storage was proved empty."""
    if not candidate.repair_empty_orphan:
        return False
    status, has_empty_isolated_storage = await orphan_candidate_status(directory, existing_by_field, candidate)
    directory._assert_empty_orphan_candidate_identity(existing_by_field, candidate, expected_status=status)
    return has_empty_isolated_storage


def should_prepare_candidate(directory, candidate, has_empty_isolated_storage):
    """Preserve the storage of finalized candidates and verified retries."""
    return not (
        directory._is_finalized_endpoint_dataset_candidate(candidate)
        or has_empty_isolated_storage
        or is_retained_vsp_capture_retry(candidate)
    )


def assert_empty_orphan_candidate_identity(directory, existing_by_field, candidate, expected_status):
    """Require an empty orphan row to retain its exact acquisition identity."""
    expected_metadata_by_field = directory._empty_orphan_expected_metadata(candidate)
    existing_metadata = existing_by_field.get("publication_metadata_json")
    has_exact_metadata = isinstance(existing_metadata, dict) and all(
        existing_metadata.get(key) == expected_field_value
        for key, expected_field_value in expected_metadata_by_field.items()
    )
    if (
        candidate.reviewed_root_policy is not None
        and not candidate.reviewed_root_policy.is_twin_root_required
        and isinstance(existing_metadata, dict)
        and any(
            field_name in existing_metadata
            for field_name in (
                directory.TWIN_ROOT_VERIFICATION_ROLE_KEY,
                directory.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY,
                directory.TWIN_ROOT_VERIFICATION_METADATA_KEY,
            )
        )
    ):
        has_exact_metadata = False
    if (
        not existing_by_field
        or directory._clean_text(existing_by_field.get("endpoint_id")) != candidate.endpoint_id
        or directory._clean_text(existing_by_field.get("status")) != expected_status
        or not has_exact_metadata
        or directory._dataset_resource_hash_contract(existing_by_field) != candidate.resource_hash_contract
    ):
        raise RuntimeError("provider_directory_endpoint_dataset_orphan_identity_mismatch")


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
