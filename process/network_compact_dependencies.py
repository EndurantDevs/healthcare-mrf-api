# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve named compact dependencies and their logical candidate readiness."""

import json
from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import (
    CompactAddressRelation,
    CompactEdgeArtifact,
    CompactNetworkLayout,
    bind_compact_geo_search_candidate,
)
from process.network_compact_readiness import _require, compact_readiness
from process.network_membership_validation import _json_object, _scope_report


async def _retained_edge(connection, candidate_id, control_schema, *, api_read=False):
    from process.network_address_artifact import _require_api_read_context

    await _require_api_read_context(connection, api_read)
    candidate = await connection.fetchrow(
        f"SELECT * FROM {_identifier(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(candidate_id),
    )
    _require(candidate is not None and candidate["state"] in ("ready", "published"))
    report = _json_object(candidate["validation_json"])
    source = PinnedAddressSource(**report["address_source"])
    compact_readiness(candidate, report, _scope_report(candidate, source))
    return {"candidate_id": str(candidate["candidate_id"]), "schema_name": candidate["schema_name"]}


async def capture_compact_edge_artifacts(connection, target, *, control_schema):
    """Retain each referenced slice generation by its candidate and schema names."""
    references = await connection.fetch(
        f"SELECT DISTINCT artifact_candidate_id FROM {_identifier(target.schema_name)}.network_slice_ref "
        "ORDER BY artifact_candidate_id"
    )
    artifacts = []
    for row in references:
        identity = str(row["artifact_candidate_id"])
        if identity == target.candidate_id:
            artifact = CompactEdgeArtifact(identity, target.schema_name)
            artifacts.append({"candidate_id": artifact.candidate_id, "schema_name": artifact.schema_name})
        else:
            artifacts.append(await _retained_edge(connection, identity, control_schema))
    return artifacts


async def verify_compact_dependencies(connection, candidate, *, control_schema, api_read=False):
    """Read the declared address edition and retained edge generations."""
    from process.network_address_artifact import _require_api_read_context, verify_network_address_artifact

    await _require_api_read_context(connection, api_read)
    report = _json_object(candidate["validation_json"])
    base = await verify_network_address_artifact(
        connection, report["serving_readiness"]["address_artifact"], api_read=api_read
    )
    return await _verified_layout(connection, candidate, base, control_schema, api_read=api_read)


async def capture_compact_dependencies(connection, candidate, *, control_schema):
    """Resolve current or retained address data within the candidate's logical scope."""
    from process.network_address_artifact import address_artifact_data
    from process.network_compact_address_origin import resolve_compact_address_origin

    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    captured, base = await resolve_compact_address_origin(
        connection, candidate, address_source, control_schema=control_schema
    )
    _require(address_artifact_data(captured) == address_artifact_data(report["serving_readiness"]["address_artifact"]))
    return await _verified_layout(connection, candidate, base, control_schema)


async def _verified_layout(connection, candidate, base, control_schema, *, api_read=False):
    """Bind qualified relation names after complete candidate readiness validation."""
    from process.network_address_artifact import _require_api_read_context
    from process.network_compact_custom_preparation import compact_carrier_receipt, compact_custom_receipt

    await _require_api_read_context(connection, api_read)
    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    _canonical, serving = compact_readiness(candidate, report, _scope_report(candidate, address_source))
    artifacts = []
    for expected_by_field in serving["edge_artifacts"]:
        names = {"candidate_id", "schema_name"}
        _require(
            type(expected_by_field) is dict and set(expected_by_field) in (names, names | {"table_oid", "seal_sha256"})
        )
        expected_by_field = {key: expected_by_field[key] for key in names}
        artifact = CompactEdgeArtifact(**expected_by_field)
        if artifact.candidate_id == str(candidate["candidate_id"]):
            _require(artifact.schema_name == candidate["schema_name"])
        else:
            _require(
                await _retained_edge(connection, artifact.candidate_id, control_schema, api_read=api_read)
                == expected_by_field
            )
        artifacts.append(artifact)
    custom = compact_custom_receipt(candidate)
    carriers = compact_carrier_receipt(candidate)
    custom_relation = None
    if custom is not None:
        _require(custom.base_source == address_source)
        if carriers is not None:
            _require(carriers.base_source == custom.base_source)
        if custom.custom_pair_count or carriers is not None and carriers.materialized_address_rows:
            custom_relation = CompactAddressRelation(candidate["schema_name"], "entity_address_unified")
    ref_map = CompactAddressRelation(candidate["schema_name"], "network_slice_ref")
    layout = CompactNetworkLayout(ref_map, base, tuple(artifacts), custom_relation)
    return bind_compact_geo_search_candidate(layout, candidate)


async def compact_slice_ref_digest(connection, schema_name):
    """Hash the small reference map in bounded pages, independent of heap scan order."""
    import hashlib

    digest = hashlib.sha256(b"compact-network-reference-v1\0")
    async for row in connection.cursor(
        f"SELECT network_id,office_bucket,artifact_candidate_id::text,slice_id::text,row_count,content_sha256 "
        f"FROM {_identifier(schema_name)}.network_slice_ref ORDER BY network_id,office_bucket",
        prefetch=256,
    ):
        encoded = json.dumps(list(row), separators=(",", ":"), ensure_ascii=False).encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()
