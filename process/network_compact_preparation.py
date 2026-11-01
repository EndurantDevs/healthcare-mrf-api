# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare compact membership storage against an existing protected address result."""

import json
from dataclasses import asdict, replace
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_compact_custom_preparation import prepare_compact_custom_contribution
from process.network_compact_dependencies import capture_compact_edge_artifacts, compact_slice_ref_digest
from process.network_compact_readiness import COMPONENT, compact_index_digest
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_slices import PinnedMembershipSlices, prepare_network_membership_slices
from process.network_membership_validation import _scope_report, validate_network_membership_candidate


async def _previous_slices(connection, candidate, control_schema):
    from process.network_serving_read import resolve_network_serving_manifest

    if candidate["expected_head"]:
        head = await resolve_network_serving_manifest(
            connection, generation_id=candidate["expected_head"], control_schema=control_schema
        )
        if head.compact_layout is not None:
            return PinnedMembershipSlices(head.candidate_id, head.schema_name, head.compact_layout.ref_map.table_oid)
    return None


def _slice_readiness(scope, report, slices):
    return {
        "component": "compact_office_slices",
        "readiness_revision": 1,
        "ready": True,
        "scope": scope,
        **{
            field: report[field]
            for field in ("membership_rows", "distinct_memberships", "projected_locations", "orphan_bindings")
        },
        "ref_map_oid": slices.ref_map_oid,
        "edge_heap_oid": slices.edge_heap_oid,
        "slices": asdict(slices),
    }


def _require_address_origin(candidate, artifact):
    generations = candidate["source_generations"]
    generations = json.loads(generations) if type(generations) is str else generations
    if generations.get("network_address_origin", artifact["descriptor_sha256"]) != artifact["descriptor_sha256"]:
        raise ValueError("registry_compact_address_origin_changed")


async def prepare_compact_network_readiness(
    connection, copy_target, address_source, *, runtime_roles, control_schema, office_authority=None, **source_options
):
    """The caller closes raw writers, owns the transaction and freezes final storage."""
    if set(source_options) - {"source_witness_runtime"}:
        raise ValueError("registry_source_runtime_options_invalid")
    from process.network_compact_address_origin import resolve_compact_address_origin

    candidate = await _locked_candidate(connection, copy_target, _identifier(control_schema))
    artifact, physical = await resolve_compact_address_origin(
        connection, candidate, address_source, runtime_roles=runtime_roles, control_schema=control_schema
    )
    _require_address_origin(candidate, artifact)
    office_options_by_name = dict(
        runtime_roles=runtime_roles, control_schema=control_schema, office_authority=office_authority, **source_options
    )
    offices = await prepare_compact_custom_contribution(
        connection, copy_target, candidate, address_source, **office_options_by_name
    )
    offices = replace(offices, physical_base=physical)
    candidate = await _locked_candidate(connection, copy_target, _identifier(control_schema))
    report = await validate_network_membership_candidate(
        connection, copy_target, address_source, control_schema=control_schema, office_sources=offices
    )
    slices = await prepare_network_membership_slices(
        connection,
        copy_target,
        address_source,
        previous=await _previous_slices(connection, candidate, control_schema),
        control_schema=control_schema,
        office_sources=offices,
    )
    scope = _scope_report(candidate, address_source)
    canonical_readiness_by_field = _slice_readiness(scope, report, slices)
    serving_readiness_by_field = {
        "component": COMPONENT,
        "readiness_revision": 1,
        "ready": True,
        "scope": scope,
        "index_profile": "compact_membership",
        "index_definition_sha256": await compact_index_digest(connection, copy_target.schema_name, candidate=candidate),
        "address_artifact": artifact,
        "edge_artifacts": await capture_compact_edge_artifacts(connection, copy_target, control_schema=control_schema),
        "slice_ref_sha256": await compact_slice_ref_digest(connection, copy_target.schema_name),
    }
    await connection.execute(
        f"UPDATE {_identifier(control_schema)}.network_membership_candidate "
        "SET state='ready',index_ready=true,validation_json=$2::jsonb WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        json.dumps(
            {
                **report,
                "candidate_readiness": canonical_readiness_by_field,
                "serving_readiness": serving_readiness_by_field,
            },
            sort_keys=True,
        ),
    )
    return serving_readiness_by_field
