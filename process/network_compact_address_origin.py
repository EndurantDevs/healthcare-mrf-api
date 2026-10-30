# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve current addresses or an exact independently sealed retained origin."""

from process.network_address_artifact import capture_network_address_artifact
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation
from process.network_compact_readiness import _require
from process.network_membership_validation import _json_object
from process.registry_retained_generation import resolve_registry_retained_generation


async def _retained_origin(connection, candidate, address_source, roles, control_schema):
    retained = await resolve_registry_retained_generation(connection, candidate, control_schema=control_schema)
    if retained is None:
        return None
    manifest, retained_candidate_by_field = retained
    report = _json_object(retained_candidate_by_field["validation_json"])
    descriptor = report.get("serving_readiness", {}).get("address_artifact")
    if type(descriptor) is not dict or descriptor.get("generation_id") != address_source.generation_id:
        return None
    _require(
        report["address_source"]
        == {
            "schema_name": address_source.schema_name,
            "table_name": address_source.table_name,
            "generation_id": address_source.generation_id,
        }
        and descriptor["runtime_roles"] == list(roles)
        and manifest.compact_layout is not None
    )
    return descriptor, manifest.compact_layout.base


async def resolve_compact_address_origin(connection, candidate, source, *, runtime_roles, control_schema):
    """A manifest digest locates evidence; only native custody authorizes reuse."""
    _require(type(source) is PinnedAddressSource and source.table_name == "entity_address_unified")
    generations = _json_object(candidate["source_generations"])
    _require(generations.get("unified_address") == source.generation_id)
    retained = await _retained_origin(connection, candidate, source, runtime_roles, control_schema)
    if retained is not None:
        return retained
    descriptor = await capture_network_address_artifact(connection, source, runtime_roles=runtime_roles)
    return descriptor, CompactAddressRelation(source.schema_name, source.table_name, descriptor["table"]["oid"])
