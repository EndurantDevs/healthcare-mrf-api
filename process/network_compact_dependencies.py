# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify retained physical dependencies of an independently sealed compact candidate."""

import json
from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_compact_readiness import _require
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_validation import _json_object


async def _retained_edge(connection, candidate_id, control_schema, *, api_read=False):
    from process.network_address_artifact import _require_api_read_context
    from process.network_candidate_custody import (
        verify_network_candidate_custody,
        verify_network_candidate_read_metadata,
    )

    await _require_api_read_context(connection, api_read)
    candidate = await connection.fetchrow(
        f"SELECT * FROM {_identifier(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(candidate_id),
    )
    _require(candidate is not None and candidate["state"] in ("ready", "published"))
    target = MembershipCopyTarget(
        *(str(candidate[field]) for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"))
    )
    verifier = verify_network_candidate_read_metadata if api_read else verify_network_candidate_custody
    seal = await verifier(connection, target, control_schema=control_schema)
    _require(seal["receipt"]["revision"] == 4)
    return {
        "candidate_id": target.candidate_id,
        "schema_name": target.schema_name,
        "table_oid": seal["receipt"]["closure"]["relation_oids"]["network_office_edge"],
        "seal_sha256": seal["seal_sha256"],
    }


async def capture_compact_edge_artifacts(connection, target, *, control_schema):
    """Pin old slice heaps directly; their historical address dependencies are irrelevant."""
    references = await connection.fetch(
        f"SELECT DISTINCT artifact_candidate_id FROM {_identifier(target.schema_name)}.network_slice_ref "
        "ORDER BY artifact_candidate_id"
    )
    artifacts = []
    for row in references:
        identity = str(row["artifact_candidate_id"])
        if identity == target.candidate_id:
            oid = await connection.fetchval(
                "SELECT to_regclass($1)::oid", _identifier(target.schema_name) + ".network_office_edge"
            )
            artifacts.append(
                {"candidate_id": identity, "schema_name": target.schema_name, "table_oid": oid, "seal_sha256": None}
            )
        else:
            artifacts.append(await _retained_edge(connection, identity, control_schema))
    return artifacts


async def verify_compact_dependencies(connection, candidate, *, control_schema, api_read=False):
    """Resolve authenticated dependency metadata; API data consumption needs its separate physical guard."""
    from process.network_address_artifact import _require_api_read_context, verify_network_address_artifact

    await _require_api_read_context(connection, api_read)
    report = _json_object(candidate["validation_json"])
    serving = report["serving_readiness"]
    closure = report["writer_closure"]
    runtime_roles = tuple(sorted(set(closure["loader_roles"] + closure["reader_roles"])))
    read_options = {"api_read": True} if api_read else {}
    base = await verify_network_address_artifact(
        connection, serving["address_artifact"], runtime_roles=runtime_roles, **read_options
    )
    return await _verified_layout(connection, candidate, base, control_schema, **read_options)


async def capture_compact_dependencies(connection, candidate, *, control_schema):
    """Independently capture current address authority once within final sealing."""
    from process.network_compact_address_origin import resolve_compact_address_origin

    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    closure = report["writer_closure"]
    roles = tuple(sorted(set(closure["loader_roles"] + closure["reader_roles"])))
    captured, base = await resolve_compact_address_origin(
        connection, candidate, address_source, runtime_roles=roles, control_schema=control_schema
    )
    _require(captured == report["serving_readiness"]["address_artifact"])
    return await _verified_layout(connection, candidate, base, control_schema)


async def _verified_layout(connection, candidate, base, control_schema, *, api_read=False):
    """Bind every layout coordinate to verified metadata without authorizing data consumption."""
    from process.network_address_artifact import _require_api_read_context
    from process.network_compact_custom_preparation import compact_carrier_receipt, compact_custom_receipt

    await _require_api_read_context(connection, api_read)
    report = _json_object(candidate["validation_json"])
    serving, canonical = report["serving_readiness"], report["candidate_readiness"]
    closure = report["writer_closure"]
    _require(
        closure["relation_oids"].get("network_slice_ref") == canonical["ref_map_oid"]
        and closure["relation_oids"].get("network_office_edge") == canonical["edge_heap_oid"]
    )
    _require(base.table_oid == serving["address_artifact"]["table"]["oid"])
    artifacts = []
    for expected in serving["edge_artifacts"]:
        _require(
            type(expected) is dict and set(expected) == {"candidate_id", "schema_name", "table_oid", "seal_sha256"}
        )
        artifact = CompactEdgeArtifact(expected["candidate_id"], expected["schema_name"], expected["table_oid"])
        if artifact.candidate_id == str(candidate["candidate_id"]):
            _require(
                artifact.schema_name == candidate["schema_name"]
                and expected["seal_sha256"] is None
                and artifact.table_oid == canonical["edge_heap_oid"]
            )
        else:
            _require(
                await _retained_edge(connection, artifact.candidate_id, control_schema, api_read=api_read) == expected
            )
        artifacts.append(artifact)
    custom = compact_custom_receipt(candidate)
    carriers = compact_carrier_receipt(candidate)
    custom_relation = None
    if custom is not None:
        _require(
            custom.base_source == PinnedAddressSource(**report["address_source"])
            and custom.base_artifact_sha256 == serving["address_artifact"]["descriptor_sha256"]
            and custom.binding_table_oid == closure["relation_oids"].get("provider_location_binding")
            and custom.runtime_roles == tuple(sorted(set(closure["loader_roles"] + closure["reader_roles"])))
        )
        custom_oid = custom.address_table_oid
        if carriers is not None:
            _require(
                carriers.base_source == custom.base_source
                and carriers.runtime_roles == custom.runtime_roles
                and carriers.base_artifact_sha256 == custom.base_artifact_sha256
                and carriers.binding_table_oid == custom.binding_table_oid
                and (not custom.custom_pair_count or custom_oid == carriers.address_table_oid)
            )
            custom_oid = carriers.address_table_oid
        if custom.custom_pair_count or carriers is not None and carriers.materialized_address_rows:
            _require(custom_oid == closure["relation_oids"].get("entity_address_unified"))
            custom_relation = CompactAddressRelation(candidate["schema_name"], "entity_address_unified", custom_oid)
    return CompactNetworkLayout(
        CompactAddressRelation(candidate["schema_name"], "network_slice_ref", canonical["ref_map_oid"]),
        base,
        tuple(artifacts),
        custom_relation,
    )


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
