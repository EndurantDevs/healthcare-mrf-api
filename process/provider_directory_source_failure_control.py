# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure and publication acknowledgement belong to the exact isolated SOURCE candidate."""

import json

from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff, require_storage_schema


async def acknowledge_source_publication(directory, handoff, frozen_header):
    """Acknowledge protected readback without publishing or changing the sealed snapshot."""
    storage._require(type(handoff) is FHIRSourceCandidateHandoff, "publication_acknowledgement_handoff_required")
    async with source_transaction(directory.db):
        publisher = bound_publisher()
        storage._require(
            require_storage_schema(publisher, directory._schema()) is not None, "isolated_storage_required"
        )
        document = handoff.document
        storage.validate_handoff_document(document, storage.storage_document(publisher))
        _require_publication_header(document, frozen_header)
        await _acknowledge_header(publisher, document["identity"]["dataset_id"], frozen_header)


def _require_publication_header(document, frozen_header):
    identity = document["identity"]
    metadata = storage._json(frozen_header.get("publication_metadata_json")) if type(frozen_header) is dict else None
    storage._require(
        type(frozen_header) is dict
        and frozen_header.get("status") == "validated"
        and frozen_header.get("is_current") is False
        and frozen_header.get("validated_at") is not None
        and frozen_header.get("published_at") is None
        and frozen_header.get("superseded_at") is None
        and frozen_header.get("content_proof_admission_version") == 1
        and frozen_header.get("content_proof_admission_kind") in {"generic", "uhc_canonical"}
        and storage._json(frozen_header.get("candidate_storage_json")) == document
        and all(
            frozen_header.get(field) == identity[field]
            for field in ("dataset_id", "endpoint_id", "acquisition_root_run_id")
        )
        and {field: frozen_header.get(field) for field in storage.HANDOFF_HEADER_FIELDS} == document["admission"]
        and type(metadata) is dict
        and all(metadata.get(field) == value for field, value in identity["metadata"].items()),
        "publication_acknowledgement_header_changed",
    )


async def _acknowledge_header(publisher, dataset_id, frozen_header):
    header = storage_relation(publisher.policy.schema, storage.HEADER)
    previous = await publisher.connection.fetchrow(
        f"SELECT parent.status,"
        "ARRAY(SELECT jsonb_object_keys($2::jsonb) ORDER BY 1)="
        "ARRAY(SELECT jsonb_object_keys(to_jsonb(expected)) ORDER BY 1) "
        "AND to_jsonb(parent)-'status'=to_jsonb(expected)-'status' AS matches "
        f"FROM {header} parent CROSS JOIN jsonb_populate_record(NULL::{header},$2::jsonb) expected "
        "WHERE parent.dataset_id=$1 FOR UPDATE OF parent",
        dataset_id,
        storage._encoded(frozen_header),
    )
    storage._require(
        previous is not None
        and previous["matches"] is True
        and previous["status"] in {"validated", "publication_acknowledged"},
        "publication_acknowledgement_parent_changed",
    )
    if previous["status"] == "publication_acknowledged":
        return
    updated = await publisher.connection.execute(
        f"UPDATE {header} SET status='publication_acknowledged' WHERE dataset_id=$1 AND status='validated'",
        dataset_id,
    )
    storage._require(updated == "UPDATE 1", "publication_acknowledgement_compare_failed")


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
