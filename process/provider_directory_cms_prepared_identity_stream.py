# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Populate prepared identity maps from the exact frozen CMS raw witnesses."""

import hashlib
import json

from db.plan_release_storage import storage_relation
from process import cms_npd_source
from process import provider_directory_cms_prepared_context as context
from process import provider_directory_cms_prepared_identity as identity
from process import provider_directory_cms_prepared_network as network
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_cms_prepared_workspace import CMSPreparedWorkspace, _policy
from process.provider_directory_generic_source_policy import _savepoint
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff

_FAMILIES = ("Organization", "Location", "InsurancePlan", "PractitionerRole")
_PAGE_ROWS = 1_000
_PAGE_BYTES = 24 * 1024 * 1024
_PAGE = """WITH page AS MATERIALIZED (
 SELECT resource_id,source_id,release_id,raw_payload_sha256,raw_payload_json,
        pg_catalog.octet_length(raw_payload_json::text) AS payload_bytes
 FROM {witness} WHERE dataset_id=$1 AND resource_type=$2 AND resource_id>$3
 ORDER BY resource_id LIMIT $4
), bounded AS (
 SELECT *,row_number() OVER (ORDER BY resource_id) AS ordinal,
        sum(payload_bytes) OVER (ORDER BY resource_id) AS decoded_bytes FROM page
)
SELECT resource_id,source_id,release_id,raw_payload_sha256,raw_payload_json FROM bounded
WHERE ordinal=1 OR decoded_bytes<=$5 ORDER BY resource_id"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_identity_stream_" + code)


def _resource(row, family, release):
    raw = row["raw_payload_json"]
    resource = json.loads(raw) if isinstance(raw, str) else raw
    _require(
        row["source_id"] == "cms-npd"
        and row["release_id"] == release
        and type(resource) is dict
        and resource.get("resourceType") == family
        and resource.get("id") == row["resource_id"],
        "raw_identity_changed",
    )
    payload = storage._encoded(resource).encode()
    _require(len(payload) <= cms_npd_source.MAX_RESOURCE_LINE_BYTES, "raw_payload_unbounded")
    _require(hashlib.sha256(payload).hexdigest() == row["raw_payload_sha256"], "raw_payload_changed")
    return resource


async def _stage_batch(workspace, family, resources):
    if family in {"Organization", "Location"}:
        await identity.stage_prepared_entity_batch(workspace, resources=resources)
    else:
        await identity.stage_prepared_resource_identity_batch(
            workspace, resource_type=family, resource_ids=[resource["id"] for resource in resources]
        )
    if family in {"Organization", "InsurancePlan"}:
        await network.record_prepared_network_batch(workspace, resource_type=family, resources=resources)


async def _frozen_coordinates(workspace):
    """Compare the exact prepared-owned frozen native inventory, not only names."""
    observed = await context._observe(
        workspace.connection,
        workspace.source_binding,
        _policy(workspace._inputs["source_policy"]),
        workspace.policy,
        workspace._inputs["frozen_inventory"],
    )
    _require(observed == workspace._inputs["native_coordinates"], "frozen_coordinates_changed")


async def stage_prepared_identities(workspace, handoff):
    """Complete the four identity families with bounded reads and atomic retry.

    The caller must authenticate frozen content/profile/claim beforehand. This
    operation preserves the eight input tables and performs no canonical write.
    """
    _require(type(workspace) is CMSPreparedWorkspace and type(handoff) is FHIRSourceCandidateHandoff, "scope_required")
    await workspace.verify_current()
    _require(not workspace.binding["indexed"], "already_indexed")
    _require(handoff.document_sha256 == workspace.binding["handoff_sha256"], "handoff_changed")
    document = handoff.document
    declared = document["tables"][storage.WITNESS]
    witness = storage_relation(workspace.policy.schema, declared["name"])
    frozen_names_by_oid = {
        native_record["relation_oid"]: native_record["relation_name"]
        for native_record in workspace._inputs["frozen_inventory"]["relations"]
    }
    _require(frozen_names_by_oid.get(declared["oid"]) == declared["name"], "witness_changed")
    release = workspace.identity["metadata"]["source_release"]
    family_counts_by_type = {}
    async with _savepoint(workspace.connection):
        await _frozen_coordinates(workspace)
        for family in _FAMILIES:
            after, count = "", 0
            while witness_rows := await workspace.connection.fetch(
                _PAGE.format(witness=witness), workspace.identity["dataset_id"], family, after, _PAGE_ROWS, _PAGE_BYTES
            ):
                await workspace.verify_current()
                resources = [
                    _resource(native_record, family, release["vector_sha256"]) for native_record in witness_rows
                ]
                await _stage_batch(workspace, family, resources)
                after, count = witness_rows[-1]["resource_id"], count + len(witness_rows)
            file = next(name for name, kind in cms_npd_source.RESOURCE_FILES if kind == family)
            _require(count == release["files"][file]["distinct_count"], "family_count_changed")
            family_counts_by_type[family] = count
        await workspace.verify_current()
        await _frozen_coordinates(workspace)
    return {"handoff_sha256": handoff.document_sha256, "resource_counts": family_counts_by_type}
