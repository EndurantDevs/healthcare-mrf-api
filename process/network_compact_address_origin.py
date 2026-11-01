# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve current addresses or an exact independently sealed retained origin."""

from process.network_address_artifact import capture_network_address_artifact
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation
from process.network_compact_readiness import _require, is_compact_candidate
from process.network_membership_validation import _json_object
from process.registry_retained_generation import resolve_registry_retained_generation


async def _is_legacy_bootstrap_origin(connection, candidate, address_source, control_schema):
    from process.network_serving_read import resolve_network_serving_manifest

    locator = _json_object(candidate["source_generations"]).get("retained_network_source")
    if locator is None or not address_source.schema_name.startswith("network_bootstrap_"):
        return False
    rows = await connection.fetch(
        f"SELECT m.generation_id,c.validation_json FROM {_identifier(control_schema)}.network_serving_manifest m "
        f"JOIN {_identifier(control_schema)}.network_membership_candidate c USING(candidate_id) "
        "WHERE m.manifest_sha256=$1 LIMIT 2",
        locator,
    )
    _require(len(rows) == 1)
    if is_compact_candidate(rows[0]):
        return False
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=rows[0]["generation_id"], control_schema=control_schema
    )
    _require(manifest.manifest_sha256 == locator)
    if manifest.compact_layout is not None:
        return False
    _require(PinnedAddressSource(**_json_object(rows[0]["validation_json"])["address_source"]) == address_source)
    # The legacy head locates the original bootstrap; fresh native capture authorizes its reuse.
    return True


async def _retained_origin(connection, candidate, address_source, roles, control_schema):
    if await _is_legacy_bootstrap_origin(connection, candidate, address_source, control_schema):
        return None
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
