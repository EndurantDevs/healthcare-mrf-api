# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse an indexed address heap only after independent source-equivalence proof."""

import json
import re

from process import network_address_artifact as artifact
from process import network_bootstrap_address_artifact as bootstrap
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation

CONTRACT = "network_indexed_address_artifact.postgres.v1"
_FIELDS = {
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
    """Select only the existing retained candidate namespace for indexed adoption."""
    return re.fullmatch(r"network_candidate_[0-9a-f]{32}", source.schema_name) is not None


def original_address_source(descriptor):
    """Keep the bootstrap lineage explicit when the physical dependency changes."""
    origin = descriptor["origin_artifact"]
    return PinnedAddressSource(origin["namespace"]["name"], "entity_address_unified", origin["generation_id"])


def _descriptor(binding):
    artifact._require(type(binding) is dict and set(binding) == _FIELDS, "indexed_descriptor_invalid")
    artifact._require(
        binding["contract"] == CONTRACT
        and len(artifact._json(binding).encode()) <= artifact._MAX_DESCRIPTOR_BYTES
        and artifact._digest({key: entry for key, entry in binding.items() if key != "descriptor_sha256"})
        == binding["descriptor_sha256"],
        "indexed_descriptor_invalid",
    )
    origin = bootstrap._descriptor(binding["origin_artifact"])
    for name, fields in (
        ("database", {"oid", "name"}),
        ("namespace", {"name", "oid", "owner_oid"}),
        ("table", {"oid", "relfilenode"}),
    ):
        artifact._require(type(binding[name]) is dict and set(binding[name]) == fields, "indexed_descriptor_invalid")
        for field in fields - {"name"}:
            bootstrap._oid(binding[name][field])
    bootstrap._oid(binding["protected_owner_oid"])
    address_source = PinnedAddressSource(
        binding["namespace"]["name"], "entity_address_unified", binding["generation_id"]
    )
    artifact._require(
        is_indexed_address_source(address_source)
        and type(address_source.generation_id) is str
        and re.fullmatch(r"[0-9a-f]{64}", address_source.generation_id) is not None
        and binding["namespace"]["owner_oid"] == binding["protected_owner_oid"]
        and binding["database"] == origin["database"]
        and binding["runtime_roles"] == origin["runtime_roles"],
        "indexed_descriptor_invalid",
    )
    artifact._roles(tuple(binding["runtime_roles"]))
    catalog = binding["catalog"]
    artifact._require(
        type(catalog) is dict
        and set(catalog) == {"shapes", "indexes", "columns", "constraints"}
        and type(binding["index_definition_sha256"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", binding["index_definition_sha256"]) is not None,
        "indexed_catalog_invalid",
    )
    indexes = catalog["indexes"]
    artifact._require(type(indexes) is list and 0 < len(indexes) <= 128, "indexed_catalog_invalid")
    for index in indexes:
        artifact._require(
            type(index) is dict
            and index.get("relation_oid") == binding["table"]["oid"]
            and all(index.get(flag) is True for flag in ("indisvalid", "indisready", "indislive")),
            "indexed_catalog_invalid",
        )
        bootstrap._oid(index.get("index_oid"))
    dependencies = sorted({*origin["dependency_oids"], binding["table"]["oid"], *(i["index_oid"] for i in indexes)})
    artifact._require(
        type(binding["dependency_oids"]) is list
        and all(type(oid) is int for oid in binding["dependency_oids"])
        and binding["dependency_oids"] == dependencies,
        "indexed_dependencies_invalid",
    )
    return json.loads(artifact._json(binding))


async def _native_identity(connection, address_source, runtime_roles, *, origin_oid, expected_oid=None):
    from process.entity_address_native_publication import _catalog_evidence
    from process.entity_address_unified import _required_geo_taxonomy_stage_index_sql
    from process.mrf_address_publication import require_native_read_catalog_subjects
    from process.network_candidate_custody import _retained_address_runtime_closure
    from process.network_membership_candidate_indexes import _catalog_readiness
    from process.network_membership_serving_indexes import _index_digest, _serving_plan
    from process.network_membership_writer_closure import _protected_owner

    await artifact._require_capture_snapshot(connection)
    qualified = _identifier(address_source.schema_name) + ".entity_address_unified"
    oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", qualified)
    artifact._require(type(oid) is int and (expected_oid is None or oid == expected_oid), "indexed_table_changed")
    physical = await artifact._locked_table(connection, address_source.schema_name, oid)
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE oid=$1::oid", physical["owner_oid"])
    artifact._require(
        _protected_owner(owner)
        and physical["schema_owner_oid"] == physical["owner_oid"]
        and physical["relkind"] == "r"
        and physical["relpersistence"] == "p"
        and not any(
            physical[key] for key in ("relrowsecurity", "relforcerowsecurity", "relispartition", "rules", "inherited")
        ),
        "indexed_custody_invalid",
    )
    session = artifact._CatalogSession(connection)
    await artifact._role_custody(connection, physical, physical["owner_oid"], runtime_roles)
    await artifact.protected._require_no_untrusted_mutation(session, [oid], physical["owner_oid"])
    await _retained_address_runtime_closure(connection, address_source, runtime_roles)
    subjects = tuple(sorted(set((*runtime_roles, await connection.fetchval("SELECT current_user")))))
    await require_native_read_catalog_subjects(session, [oid, origin_oid], subjects)
    await _catalog_readiness(connection, address_source.schema_name)
    artifact._require(
        await connection.fetchval(
            _required_geo_taxonomy_stage_index_sql(address_source.schema_name, address_source.table_name)
        )
        is True,
        "indexed_geo_profile_invalid",
    )
    catalog = await _catalog_evidence(
        session,
        {
            "schema_name": address_source.schema_name,
            "stage_relations": [
                {"table_name": address_source.table_name, "stage_name": address_source.table_name, "relation_oid": oid}
            ],
        },
    )
    index_digest = await _index_digest(
        connection, address_source.schema_name, _serving_plan(address_source.schema_name), qualified_definitions=True
    )
    return physical, catalog, index_digest


async def capture_indexed_address_artifact(connection, address_source, *, runtime_roles, control_schema):
    """Capture closed native coordinates; only the new candidate's final proof authorizes reuse."""
    from process.network_membership_validation import _json_object
    from process.network_serving_read import resolve_retained_network_address_source

    artifact._require(
        type(address_source) is PinnedAddressSource and is_indexed_address_source(address_source),
        "indexed_source_invalid",
    )
    runtime_roles = artifact._roles(runtime_roles)
    manifest = await resolve_retained_network_address_source(connection, address_source, control_schema=control_schema)
    artifact._require(manifest.compact_layout is None, "indexed_source_invalid")
    report = _json_object(
        await connection.fetchval(
            f"SELECT validation_json FROM {_identifier(control_schema)}.network_membership_candidate WHERE candidate_id=$1::uuid",
            manifest.candidate_id,
        )
    )
    original = PinnedAddressSource(**report["address_source"])
    artifact._require(bootstrap.is_bootstrap_source(original), "indexed_origin_invalid")
    origin = await bootstrap.capture_bootstrap_address_artifact(connection, original, runtime_roles=runtime_roles)
    physical, catalog, index_digest = await _native_identity(
        connection,
        address_source,
        runtime_roles,
        origin_oid=origin["table"]["oid"],
        expected_oid=manifest.address_table_oid,
    )
    descriptor_by_field = {
        "contract": CONTRACT,
        "database": await artifact._database(connection),
        "namespace": {
            "name": address_source.schema_name,
            "oid": physical["schema_oid"],
            "owner_oid": physical["owner_oid"],
        },
        "protected_owner_oid": physical["owner_oid"],
        "table": {"oid": physical["oid"], "relfilenode": physical["relfilenode"]},
        "generation_id": address_source.generation_id,
        "origin_artifact": origin,
        "catalog": catalog,
        "index_definition_sha256": index_digest,
        "runtime_roles": list(runtime_roles),
        "dependency_oids": sorted(
            {*origin["dependency_oids"], physical["oid"], *(i["index_oid"] for i in catalog["indexes"])}
        ),
    }
    descriptor_by_field["descriptor_sha256"] = artifact._digest(descriptor_by_field)
    return _descriptor(descriptor_by_field)


async def verify_indexed_address_artifact(connection, descriptor, *, runtime_roles):
    """Authenticate an independently sealed descriptor without rereading legacy writer reports."""
    descriptor = _descriptor(descriptor)
    roles = artifact._roles(runtime_roles)
    artifact._require(descriptor["runtime_roles"] == list(roles), "roles_changed")
    await bootstrap.verify_bootstrap_address_artifact(connection, descriptor["origin_artifact"], runtime_roles=roles)
    address_source = PinnedAddressSource(
        descriptor["namespace"]["name"], "entity_address_unified", descriptor["generation_id"]
    )
    physical, catalog, index_digest = await _native_identity(
        connection,
        address_source,
        roles,
        origin_oid=descriptor["origin_artifact"]["table"]["oid"],
        expected_oid=descriptor["table"]["oid"],
    )
    artifact._require(
        descriptor["database"] == await artifact._database(connection)
        and descriptor["namespace"]
        == {"name": address_source.schema_name, "oid": physical["schema_oid"], "owner_oid": physical["owner_oid"]}
        and descriptor["table"]["relfilenode"] == physical["relfilenode"]
        and descriptor["catalog"] == json.loads(artifact._json(catalog)),
        "indexed_artifact_changed",
    )
    if descriptor["index_definition_sha256"] != index_digest:
        from process.network_membership_serving_indexes import _index_digest, _serving_plan

        legacy_digest = await _index_digest(
            connection, address_source.schema_name, _serving_plan(address_source.schema_name)
        )
        artifact._require(descriptor["index_definition_sha256"] == legacy_digest, "indexed_artifact_changed")
    return CompactAddressRelation(address_source.schema_name, address_source.table_name, physical["oid"])


async def prove_indexed_address_equivalence(connection, descriptor):
    """Compare every source row and noncanonical field under the original closed snapshot."""
    from process.network_membership_candidate_indexes import _next_projection_range, _projection_address_difference
    from process.network_membership_serving_indexes import _source_semantics

    relation = await verify_indexed_address_artifact(
        connection, descriptor, runtime_roles=tuple(descriptor["runtime_roles"])
    )
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
    receipt = bootstrap.bootstrap._decode(artifact._json(descriptor["origin_artifact"]["bootstrap_receipt"]))
    expected = next(
        parent.row_count for parent in receipt.parent_receipts if parent.table_name == "entity_address_unified"
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
