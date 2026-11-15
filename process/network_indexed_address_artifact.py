# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse named indexed address data after complete source-equivalence validation."""

import json
import re
from dataclasses import asdict

from process import network_address_artifact as artifact
from process import network_bootstrap_address_artifact as bootstrap
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation

CONTRACT = "network_indexed_address_artifact.postgres.v1"
_FIELDS = {
    "contract",
    "revision",
    "namespace",
    "table",
    "generation_id",
    "origin_artifact",
    "index_definition_sha256",
    "descriptor_sha256",
}
_LEGACY_FIELDS = {
    "contract",
    "database",
    "namespace",
    "protected_owner_oid",
    "table",
    "generation_id",
    "origin_artifact",
    "catalog",
    "index_definition_sha256",
    "runtime_roles",
    "dependency_oids",
    "descriptor_sha256",
}


def is_indexed_address_source(source):
    """Select the existing retained network-candidate schema convention."""
    return re.fullmatch(r"network_candidate_[0-9a-f]{32}", source.schema_name) is not None


def original_address_source(descriptor):
    """Keep the original bootstrap edition explicit when indexed storage is selected."""
    origin = _descriptor(descriptor)["origin_artifact"]
    return PinnedAddressSource(origin["namespace"]["name"], "entity_address_unified", origin["generation_id"])


def _descriptor(binding_by_field):
    artifact._require(type(binding_by_field) is dict, "indexed_descriptor_invalid")
    if set(binding_by_field) == _LEGACY_FIELDS:
        schema = binding_by_field["namespace"]["name"]
        binding_by_field = {
            "contract": CONTRACT,
            "revision": 1,
            "namespace": {"name": schema},
            "table": asdict(CompactAddressRelation(schema, "entity_address_unified")),
            **{
                name: binding_by_field[name]
                for name in ("generation_id", "origin_artifact", "index_definition_sha256", "descriptor_sha256")
            },
        }
    artifact._require(
        set(binding_by_field) == _FIELDS and binding_by_field["contract"] == CONTRACT, "indexed_descriptor_invalid"
    )
    artifact._require(
        type(binding_by_field["revision"]) is int and binding_by_field["revision"] in (1, 2),
        "indexed_descriptor_invalid",
    )
    origin = bootstrap._descriptor(binding_by_field["origin_artifact"])
    binding_by_field = {**binding_by_field, "origin_artifact": origin}
    artifact._require(
        type(binding_by_field["namespace"]) is dict and set(binding_by_field["namespace"]) == {"name"},
        "indexed_descriptor_invalid",
    )
    relation = CompactAddressRelation(**binding_by_field["table"])
    address_source = PinnedAddressSource(
        relation.schema_name, relation.table_name, artifact._sha(binding_by_field["generation_id"])
    )
    artifact._require(
        binding_by_field["namespace"]["name"] == relation.schema_name
        and relation.table_name == "entity_address_unified"
        and is_indexed_address_source(address_source),
        "indexed_descriptor_invalid",
    )
    artifact._sha(binding_by_field["index_definition_sha256"])
    artifact._sha(binding_by_field["descriptor_sha256"])
    if binding_by_field["revision"] == 2:
        artifact._require(
            artifact._digest({key: entry for key, entry in binding_by_field.items() if key != "descriptor_sha256"})
            == binding_by_field["descriptor_sha256"],
            "indexed_descriptor_invalid",
        )
    artifact._require(
        len(artifact._json(binding_by_field).encode()) <= artifact._MAX_DESCRIPTOR_BYTES, "indexed_descriptor_invalid"
    )
    return json.loads(artifact._json(binding_by_field))


async def _index_readiness(connection, source, *, api_read=False):
    from process.entity_address_unified import _required_geo_taxonomy_stage_index_sql
    from process.network_membership_candidate_indexes import _catalog_readiness
    from process.network_membership_serving_indexes import _index_digest, _serving_plan

    await artifact._require_capture_snapshot(connection)
    await _catalog_readiness(connection, source.schema_name)
    artifact._require(
        await connection.fetchval(_required_geo_taxonomy_stage_index_sql(source.schema_name, source.table_name))
        is True,
        "indexed_geo_profile_invalid",
    )
    return await _index_digest(
        connection, source.schema_name, _serving_plan(source.schema_name), qualified_definitions=True
    )


async def capture_indexed_address_artifact(connection, address_source, *, runtime_roles=(), control_schema):
    """Capture a named published address edition and its required native index definitions."""
    from process.network_membership_validation import _json_object
    from process.network_serving_read import resolve_retained_network_address_source

    artifact._require(
        type(address_source) is PinnedAddressSource and is_indexed_address_source(address_source),
        "indexed_source_invalid",
    )
    manifest = await resolve_retained_network_address_source(connection, address_source, control_schema=control_schema)
    artifact._require(
        manifest.compact_layout is None
        and manifest.address_relation == CompactAddressRelation(address_source.schema_name, address_source.table_name),
        "indexed_source_invalid",
    )
    report = _json_object(
        await connection.fetchval(
            f"SELECT validation_json FROM {_identifier(control_schema)}.network_membership_candidate WHERE candidate_id=$1::uuid",
            manifest.candidate_id,
        )
    )
    original = PinnedAddressSource(**report["address_source"])
    artifact._require(bootstrap.is_bootstrap_source(original), "indexed_origin_invalid")
    origin = await bootstrap.capture_bootstrap_address_artifact(connection, original, runtime_roles=runtime_roles)
    index_digest = await _index_readiness(connection, address_source)
    descriptor_by_field = {
        "contract": CONTRACT,
        "revision": 2,
        "namespace": {"name": address_source.schema_name},
        "table": asdict(CompactAddressRelation(address_source.schema_name, address_source.table_name)),
        "generation_id": address_source.generation_id,
        "origin_artifact": origin,
        "index_definition_sha256": index_digest,
    }
    descriptor_by_field["descriptor_sha256"] = artifact._digest(descriptor_by_field)
    return _descriptor(descriptor_by_field)


async def verify_indexed_address_artifact(connection, descriptor, *, runtime_roles=(), api_read=False):
    """Read the named edition while checking its supported index data and original Source scope."""
    descriptor = _descriptor(descriptor)
    await bootstrap.verify_bootstrap_address_artifact(connection, descriptor["origin_artifact"], api_read=api_read)
    relation = CompactAddressRelation(**descriptor["table"])
    source = PinnedAddressSource(relation.schema_name, relation.table_name, descriptor["generation_id"])
    index_digest = await _index_readiness(connection, source, api_read=api_read)
    if descriptor["index_definition_sha256"] != index_digest:
        from process.network_membership_serving_indexes import _index_digest, _serving_plan

        legacy_digest = await _index_digest(connection, source.schema_name, _serving_plan(source.schema_name))
        artifact._require(descriptor["index_definition_sha256"] == legacy_digest, "indexed_artifact_changed")
    return relation


async def prove_indexed_address_equivalence(connection, descriptor):
    """Compare every source row and noncanonical field under the original closed snapshot."""
    from process.network_membership_candidate_indexes import _next_projection_range, _projection_address_difference
    from process.network_membership_serving_indexes import _source_semantics

    relation = await verify_indexed_address_artifact(connection, descriptor)
    original = original_address_source(descriptor)
    source_table = _identifier(original.schema_name) + ".entity_address_unified"
    projected = _identifier(relation.schema_name) + ".entity_address_unified"
    await _source_semantics(connection, source_table, projected, replay=True)
    difference = await _projection_address_difference(connection, projected, source_table)
    counts = dict.fromkeys(
        ("source_rows", "address_rows", "missing_locations", "extra_locations", "changed_address_rows"), 0
    )
    lower = None
    while (upper := await _next_projection_range(connection, projected, source_table, lower)) is not None:
        predicate, arguments = (
            ("location_key<=$1", (upper,))
            if lower is None
            else ("location_key>$1 AND location_key<=$2", (lower, upper))
        )
        observed = await connection.fetchrow(
            f"""WITH source AS (SELECT * FROM {source_table} WHERE {predicate}),
          projected AS (SELECT * FROM {projected} WHERE {predicate})
          SELECT count(source.location_key) AS source_rows,count(projected.location_key) AS address_rows,
            count(*) FILTER(WHERE projected.location_key IS NULL) AS missing_locations,
            count(*) FILTER(WHERE source.location_key IS NULL) AS extra_locations,
            count(*) FILTER(WHERE source.location_key IS NOT NULL AND projected.location_key IS NOT NULL
              AND ({difference})) AS changed_address_rows
          FROM source FULL JOIN projected USING(location_key)""",
            *arguments,
        )
        for name in counts:
            counts[name] += observed[name]
        lower = upper
    receipt = bootstrap._receipt(descriptor["origin_artifact"]["bootstrap_receipt"])
    expected = next(
        parent["row_count"] for parent in receipt["parent_receipts"] if parent["table_name"] == "entity_address_unified"
    )
    artifact._require(
        counts["source_rows"] == counts["address_rows"] == expected
        and not any(counts[name] for name in ("missing_locations", "extra_locations", "changed_address_rows")),
        "indexed_content_differs",
    )
    return counts


async def prove_new_indexed_address_dependency(connection, candidate, descriptor, *, control_schema):
    """An independently sealed predecessor already proves the unchanged dependency."""
    from process.network_membership_validation import _json_object
    from process.registry_retained_generation import resolve_registry_retained_generation

    previous = await resolve_registry_retained_generation(connection, candidate, control_schema=control_schema)
    if previous is not None:
        manifest, retained = previous
        prior = _json_object(retained["validation_json"]).get("serving_readiness", {}).get("address_artifact")
        if manifest.compact_layout is not None and prior == descriptor:
            return
    await prove_indexed_address_equivalence(connection, descriptor)
