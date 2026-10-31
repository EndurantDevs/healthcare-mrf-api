# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete CMS intake evidence before protected identity and coverage preparation."""

import datetime as dt
import re

from db.plan_release_storage import storage_relation
from process import cms_npd_source
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_registry_network_metadata import fhir_network_binding_metadata
from process.provider_directory_resource_hash import SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT

_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_FILE_FIELDS = {"sha256", "compressed_bytes", "original_bytes", "row_count", "distinct_count"}


def require_cms_handoff_identity(identity, catalog_schema):
    """Require the exact eight-file producer declaration, never a release-shaped REST task."""
    metadata = identity["metadata"]
    release = metadata.get("source_release")
    resources = sorted(kind for _, kind in cms_npd_source.RESOURCE_FILES)
    storage._require(
        metadata.get("source_ids") == ["cms-npd"]
        and metadata.get("selected_resources") == resources
        and metadata.get("expected_resources") == resources
        and metadata.get("resource_hash_contract") == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        and type(release) is dict
        and set(release) == {"source_id", "generated_at", "manifest_sha256", "vector_sha256", "files"}
        and release["source_id"] == "cms-npd"
        and all(
            type(release[field]) is str and _DIGEST.fullmatch(release[field])
            for field in ("manifest_sha256", "vector_sha256")
        )
        and type(release["generated_at"]) is str
        and metadata.get("semantic_projection_as_of") == release["generated_at"]
        and type(release["files"]) is dict
        and set(release["files"]) == {name for name, _ in cms_npd_source.RESOURCE_FILES},
        "isolated_cms_release_invalid",
    )
    try:
        storage._require(
            dt.date.fromisoformat(release["generated_at"]).isoformat() == release["generated_at"],
            "isolated_cms_release_invalid",
        )
    except ValueError as error:
        raise storage.GenericCandidateError("provider_directory_generic_source_isolated_cms_release_invalid") from error
    for file_by_field in release["files"].values():
        storage._require(
            type(file_by_field) is dict
            and set(file_by_field) == _FILE_FIELDS
            and type(file_by_field["sha256"]) is str
            and _DIGEST.fullmatch(file_by_field["sha256"])
            and all(
                type(file_by_field[field]) is int and 0 <= file_by_field[field] < 2**63
                for field in _FILE_FIELDS - {"sha256"}
            )
            and file_by_field["compressed_bytes"] > 0
            and file_by_field["distinct_count"] <= file_by_field["row_count"],
            "isolated_cms_release_invalid",
        )
    storage._require(
        metadata.get("network_bindings")
        == fhir_network_binding_metadata(
            source_id="cms-npd",
            dataset_schema=catalog_schema,
            dataset_id=identity["dataset_id"],
            producer_id="cms-npd-bulk-fhir-v1",
            alias_scope="cms-npd",
        ),
        "isolated_cms_producer_changed",
    )
    return release


async def validate_cms_handoff_rows(connection, policy, document):
    """Check every retained family and raw twin; graph/identity preparation stays pending."""
    storage._require(
        document["contract"] == storage.CMS_ISOLATED_CONTRACT
        and document["auxiliary"] is None
        and document["relationship_sha256"] is None,
        "isolated_cms_finalization_unexpected",
    )
    storage._require(policy.schema == document["storage"]["storage_schema"], "storage_binding_changed")
    storage._require(connection.is_in_transaction() is True, "transaction_required")
    release = require_cms_handoff_identity(document["identity"], document["storage"]["catalog_schema"])
    dataset_id = document["identity"]["dataset_id"]
    expected_by_family = {
        kind: release["files"][name]["distinct_count"] for name, kind in cms_npd_source.RESOURCE_FILES
    }
    for root in (storage.RESOURCE, storage.WITNESS):
        table = storage_relation(policy.schema, document["tables"][root]["name"])
        family_counts = await connection.fetch(
            f"SELECT resource_type,count(*) AS count FROM {table} GROUP BY resource_type"
        )
        observed_by_family = {family_count["resource_type"]: family_count["count"] for family_count in family_counts}
        storage._require(
            len(observed_by_family) == len(family_counts)
            and not set(observed_by_family) - set(expected_by_family)
            and all(observed_by_family.get(kind, 0) == count for kind, count in expected_by_family.items()),
            "isolated_cms_family_counts_changed",
        )
        mismatch = await connection.fetchval(f"SELECT EXISTS(SELECT FROM {table} WHERE dataset_id<>$1)", dataset_id)
        storage._require(mismatch is False, "dataset_changed")
    graph = storage_relation(policy.schema, document["tables"][storage.RELATIONSHIP]["name"])
    storage._require(
        await connection.fetchval(f"SELECT EXISTS(SELECT FROM {graph})") is False,
        "isolated_cms_finalization_unexpected",
    )
    storage._require(connection.is_in_transaction() is True, "transaction_changed")
