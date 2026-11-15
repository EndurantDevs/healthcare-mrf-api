# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve current addresses or a named retained logical Source edition."""

from process.network_address_artifact import (
    _descriptor,
    address_artifact_data,
    capture_network_address_artifact,
    verify_network_address_artifact,
)
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation
from process.network_compact_readiness import _require, is_compact_candidate
from process.network_membership_validation import _json_object
from process.registry_retained_generation import resolve_registry_retained_generation


def _saved_address_artifact(candidate):
    report = _json_object(candidate["validation_json"])
    if report is None:
        return None
    _require(type(report) is dict)
    readiness = report.get("serving_readiness")
    if readiness is None:
        return None
    _require(type(readiness) is dict)
    return readiness.get("address_artifact")


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
    return True


async def _retained_origin(connection, candidate, address_source, control_schema):
    if await _is_legacy_bootstrap_origin(connection, candidate, address_source, control_schema):
        return None
    retained = await resolve_registry_retained_generation(connection, candidate, control_schema=control_schema)
    if retained is None:
        return None
    manifest, previous = retained
    report = _json_object(previous["validation_json"])
    descriptor = _saved_address_artifact(previous)
    if type(descriptor) is not dict or descriptor.get("generation_id") != address_source.generation_id:
        return None
    _require(PinnedAddressSource(**report["address_source"]) == address_source and manifest.compact_layout is not None)
    descriptor = _descriptor(descriptor)
    base = await verify_network_address_artifact(connection, descriptor)
    _require(descriptor["revision"] == 1 or manifest.compact_layout.base == base)
    return descriptor, base


async def resolve_compact_address_origin(connection, candidate, source, *, runtime_roles=(), control_schema):
    """Keep the candidate's address generation and its named publication locator bound."""
    _require(type(source) is PinnedAddressSource and source.table_name == "entity_address_unified")
    _require(_json_object(candidate["source_generations"]).get("unified_address") == source.generation_id)
    retained = await _retained_origin(connection, candidate, source, control_schema)
    if retained is not None:
        return retained
    from process import network_indexed_address_artifact as indexed

    if indexed.is_indexed_address_source(source):
        descriptor = await indexed.capture_indexed_address_artifact(connection, source, control_schema=control_schema)
        previous = await resolve_registry_retained_generation(connection, candidate, control_schema=control_schema)
        if previous is not None:
            previous_manifest, previous_candidate = previous
            prior = _saved_address_artifact(previous_candidate)
            _require(previous_manifest.compact_layout is not None and type(prior) is dict)
            if prior.get("contract") == indexed.CONTRACT:
                prior = prior["origin_artifact"]
            _require(address_artifact_data(descriptor["origin_artifact"]) == address_artifact_data(prior))
        base = CompactAddressRelation(source.schema_name, source.table_name)
    else:
        descriptor = await capture_network_address_artifact(connection, source)
        base = await verify_network_address_artifact(connection, descriptor)
    stored = _saved_address_artifact(candidate)
    if stored is not None and address_artifact_data(stored) == address_artifact_data(descriptor):
        descriptor = _descriptor(stored)
    return descriptor, base
