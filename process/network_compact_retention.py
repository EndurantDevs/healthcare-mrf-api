# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protect retained and prepared named network dependencies during explicit retirement."""

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation
from process.network_compact_readiness import is_compact_candidate
from process.network_membership_validation import _json_object

_PAGE_ROWS = 16
_TABLES = ("network_membership_candidate", "network_serving_manifest", "network_serving_control")
_MAX_METADATA_BYTES = 2 * 1024 * 1024


class NetworkCompactRetentionError(RuntimeError):
    """Named dependencies remain referenced or their logical metadata changed."""


def _require(condition, reason):
    if not condition:
        raise NetworkCompactRetentionError("network_compact_retention_" + reason)


def _requested_relations(relations):
    _require(type(relations) in {tuple, list} and bool(relations), "relations_invalid")
    _require(all(type(relation) is CompactAddressRelation for relation in relations), "relations_invalid")
    for relation in relations:
        relation.__post_init__()
    return frozenset((relation.schema_name, relation.table_name) for relation in relations)


async def _has_controls(connection, schema):
    present = await connection.fetchval(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) AND c.relkind='r'",
        schema,
        list(_TABLES),
    )
    _require(present in (0, len(_TABLES)), "controls_incomplete")
    return present == len(_TABLES)


def _projection_sql(namespace):
    """Project logical versions and external names while excluding data counters and physical fields."""
    artifact = "c.validation_json#>'{serving_readiness,address_artifact}'"
    projection = f"""jsonb_build_object('candidate_id',c.candidate_id,'schema_name',c.schema_name,'state',c.state,
      'source_generations',c.source_generations,
      'address_source',jsonb_build_object(
        'schema_name',c.validation_json#>'{{address_source,schema_name}}',
        'table_name',c.validation_json#>'{{address_source,table_name}}',
        'generation_id',c.validation_json#>'{{address_source,generation_id}}'),
      'address_artifact',jsonb_build_object(
        'schema_name',({artifact})#>'{{namespace,name}}','table_name',coalesce(
          ({artifact})#>'{{table,table_name}}',({artifact})#>'{{table,name}}'),
        'generation_id',({artifact})->'generation_id',
        'descriptor_sha256',({artifact})->'descriptor_sha256'),
      'edge_artifacts',(SELECT jsonb_agg(jsonb_build_object('candidate_id',edge->'candidate_id',
        'schema_name',edge->'schema_name') ORDER BY edge->>'candidate_id')
        FROM jsonb_array_elements(coalesce(c.validation_json#>'{{serving_readiness,edge_artifacts}}','[]'::jsonb)) edge))"""
    return f"""WITH candidates AS (
      SELECT c.candidate_id,{projection} AS logical FROM {namespace}.network_membership_candidate c
      WHERE c.state<>'rejected'), manifests AS (
      SELECT generation_id,jsonb_build_object('generation_id',generation_id,'candidate_id',candidate_id,
        'manifest_sha256',manifest_sha256,'eligible',eligible) AS logical FROM {namespace}.network_serving_manifest)
      SELECT jsonb_build_object(
        'candidates',encode(sha256(convert_to(coalesce((SELECT string_agg(logical::text,'' ORDER BY candidate_id)
          FROM candidates),''),'UTF8')),'hex'),
        'manifests',encode(sha256(convert_to(coalesce((SELECT string_agg(logical::text,'' ORDER BY generation_id)
          FROM manifests),''),'UTF8')),'hex'),
        'head',(SELECT jsonb_build_object('generation_id',generation_id,'metadata_generation',metadata_generation)
          FROM {namespace}.network_serving_control WHERE id=1))"""


async def _projection(connection, namespace):
    document = await connection.fetchval(_projection_sql(namespace))
    return _json_object(document)


def _page_sql(namespace):
    size = "+".join(
        f"octet_length(coalesce(c.{field}::text,''))"
        for field in ("source_generations", "source_recipes_json", "validation_json")
    )
    columns = [
        f"CASE WHEN ({size})<=$2 THEN c.{field} END AS {field}"
        if field in {"source_generations", "source_recipes_json", "validation_json"}
        else f"c.{field}"
        for field in (
            "dataset_id",
            "schema_id",
            "producer_id",
            "candidate_id",
            "schema_name",
            "state",
            "source_generations",
            "source_recipes_json",
            "validation_json",
            "schema_revision",
            "approved_custom_revision",
            "expected_head",
            "expected_rows",
            "accepted_rows",
            "index_ready",
        )
    ]
    return (
        "SELECT "
        + ",".join(columns)
        + f" ,({size})<=$2 AS bounded FROM {namespace}.network_membership_candidate c "
        + f"WHERE c.state<>'rejected' AND ($1::uuid IS NULL OR c.candidate_id>$1) ORDER BY c.candidate_id LIMIT {_PAGE_ROWS}"
    )


async def _retirement_inventory(connection, candidate):
    """Fold immutable inventory data while the caller fences logical control metadata before and after."""
    from process.registry_source_contribution_artifacts import _artifact_copy_target
    from process.registry_source_inventory_contract import MAX_INVENTORY_PAGE_BYTES
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_store import _fold_inventory, _relation

    reference = verify_source_recipe_metadata(candidate).inventory
    if reference is None:
        return None
    entries = []
    await _fold_inventory(
        connection,
        _relation(_artifact_copy_target(candidate)),
        reference.root,
        100,
        MAX_INVENTORY_PAGE_BYTES,
        entries.append,
    )
    return reference, tuple(entries)


async def _source_fact_dependencies(connection, candidate, schema, inventory_data):
    from process.registry_source_contribution_artifacts import (
        _owner_sources,
        _published_artifact_candidates,
        _resolved_owned_source,
        _selected_by_owner,
        load_published_artifact_validation,
    )
    from process.registry_source_contribution_store import verify_registry_source_contributions
    from process.registry_source_inventory_reference import verify_source_recipe_metadata

    receipt = verify_registry_source_contributions(candidate)
    if receipt is None:
        inventory = verify_source_recipe_metadata(candidate).inventory
        _require(inventory is None or not inventory.root.contribution_count, "references_incomplete")
        return set()
    source_descriptors = await _owner_sources(connection, candidate, schema, inventory_data=inventory_data)
    dependencies = set()
    for offset in range(0, len(source_descriptors), 100):
        selected = _selected_by_owner(source_descriptors[offset : offset + 100])
        identities = sorted(selected)
        for start in range(0, len(identities), 4):
            candidates = await _published_artifact_candidates(connection, schema, set(identities[start : start + 4]))
            for identity, original in candidates.items():
                evidence = await load_published_artifact_validation(connection, original, schema)
                descriptors = await _owner_sources(
                    connection, original, schema, inventory_data=await _retirement_inventory(connection, original)
                )
                for descriptor, reference in selected[identity]:
                    artifact = _resolved_owned_source(
                        original, evidence["generation_id"], descriptor, reference, sources=descriptors
                    )
                    dependencies.add((artifact.schema_name, artifact.reference.fact_table_name))
    return dependencies


def _named_relations(document):
    """Collect only schema-qualified relation coordinates from interpreted recipe data."""
    relations = set()
    if type(document) is dict:
        schema = document.get("schema_name", document.get("schema"))
        table = document.get("table_name", document.get("relation_name", document.get("name")))
        if type(schema) is str and type(table) is str:
            relation = CompactAddressRelation(schema, table)
            relations.add((relation.schema_name, relation.table_name))
        tables = document.get("tables")
        if type(schema) is str and type(tables) is dict:
            for table_name in tables.values():
                if type(table_name) is str:
                    relation = CompactAddressRelation(schema, table_name)
                    relations.add((relation.schema_name, relation.table_name))
        for value in document.values():
            relations.update(_named_relations(value))
    elif type(document) is list:
        for value in document:
            relations.update(_named_relations(value))
    return relations


async def _carrier_origin_dependencies(connection, carriers, schema):
    from process.network_published_api_pin import published_network_layout
    from process.network_serving_read import _pinned_manifest, _read_manifest

    dependencies = set()
    if carriers is None:
        return dependencies
    for origin in carriers.origin_references:
        entry = await _read_manifest(connection, _identifier(schema), origin[0], include_ineligible=True)
        _require(entry is not None, "origin_unavailable")
        previous = _json_object(entry["candidate_json"])
        manifest = _pinned_manifest(entry, layout=published_network_layout(previous))
        _require(manifest.manifest_sha256 == origin[1], "origin_changed")
        relation = manifest.address_relation
        dependencies.add((relation.schema_name, relation.table_name))
        if manifest.compact_layout is not None:
            for relation in (manifest.compact_layout.base, manifest.compact_layout.custom):
                if relation is not None:
                    dependencies.add((relation.schema_name, relation.table_name))
    return dependencies


async def _candidate_dependencies(connection, candidate, schema):
    """Resolve full named dependency metadata outside the short retirement lock."""
    from process.network_address_artifact import _descriptor
    from process.network_compact_custom_preparation import compact_carrier_receipt, compact_custom_receipt
    from process.network_published_api_pin import published_network_layout

    report = _json_object(candidate.get("validation_json")) or {}
    generations = _json_object(candidate["source_generations"])
    dependencies = set()
    if "unified_address" in generations:
        _require(type(report.get("address_source")) is dict, "references_incomplete")
        address_source = PinnedAddressSource(**report["address_source"])
        dependencies.add((address_source.schema_name, address_source.table_name))
    if is_compact_candidate(candidate):
        layout = published_network_layout(candidate)
        _require(layout is not None, "references_incomplete")
        for relation in (layout.base, layout.custom, layout.geo_search):
            if relation is not None:
                dependencies.add((relation.schema_name, relation.table_name))
        dependencies.update((artifact.schema_name, "network_office_edge") for artifact in layout.artifacts)
        dependencies.update(_named_relations(_descriptor(report["serving_readiness"]["address_artifact"])))
        custom, carriers = compact_custom_receipt(candidate), compact_carrier_receipt(candidate)
        for receipt in (custom, carriers):
            if receipt is not None:
                dependencies.update(_named_relations(receipt.as_dict()))
        dependencies.update(await _carrier_origin_dependencies(connection, carriers, schema))
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_recipe_store import canonical_registry_source_recipes, decode_registry_source_recipes

    inventory_data = await _retirement_inventory(connection, candidate)
    if inventory_data is None:
        recipes = decode_registry_source_recipes(candidate["source_recipes_json"])
    else:
        recipes = tuple(decode_registry_source_recipes("[" + entry.recipe_json + "]")[0] for entry in inventory_data[1])
        _require(len(recipes) == verify_source_recipe_metadata(candidate).recipe_count, "references_incomplete")
    import json

    dependencies.update(_named_relations(json.loads(canonical_registry_source_recipes(recipes))))
    dependencies.update(await _source_fact_dependencies(connection, candidate, schema, inventory_data))
    return dependencies


async def _walk_references(connection, requested, schema):
    namespace = _identifier(schema)
    after = None
    while True:
        rows = await connection.fetch(_page_sql(namespace), after, _MAX_METADATA_BYTES)
        if not rows:
            return
        for candidate_row in rows:
            _require(candidate_row["bounded"] is True, "candidate_metadata_bound")
            candidate_by_field = dict(candidate_row)
            _require(
                not any(relation[0] == candidate_by_field["schema_name"] for relation in requested),
                "artifact_referenced",
            )
            dependencies = await _candidate_dependencies(connection, candidate_by_field, schema)
            _require(not requested.intersection(dependencies), "artifact_referenced")
        after = rows[-1]["candidate_id"]


async def require_network_compact_artifacts_unreferenced(
    connection, relations, *, control_schema=None, expected_projection=None
):
    """Resolve references first, then compare only logical metadata under short NOWAIT control locks.

    The caller passes the first result to the second check after locking its exact
    retirement targets and keeps that transaction through the authorized DROP.
    Caller authorization governs deletion independently of the logical row-version comparison.
    """
    _require(connection.is_in_transaction(), "transaction_required")
    requested = _requested_relations(relations)
    schema = registry_schema() if control_schema is None else control_schema
    namespace = _identifier(schema)
    _require(await connection.fetchval("SHOW transaction_isolation") == "read committed", "fresh_snapshot_required")
    if not await _has_controls(connection, schema):
        _require(
            expected_projection is None
            or expected_projection == {"schema": schema, "relations": sorted(requested), "projection": None},
            "controls_changed",
        )
        return {"schema": schema, "relations": sorted(requested), "projection": None}
    if expected_projection is None:
        before = await _projection(connection, namespace)
        await _walk_references(connection, requested, schema)
        _require(await _projection(connection, namespace) == before, "references_changed")
        return {"schema": schema, "relations": sorted(requested), "projection": before}
    _require(
        type(expected_projection) is dict
        and expected_projection.get("schema") == schema
        and expected_projection.get("relations") == sorted(requested),
        "reference_scope_changed",
    )
    await connection.execute(
        "SELECT set_config('statement_timeout',LEAST(CASE WHEN setting::bigint=0 THEN 3000 ELSE setting::bigint END,3000)::text,true) "
        "FROM pg_settings WHERE name='statement_timeout'"
    )
    await connection.execute(
        "LOCK TABLE " + ",".join(namespace + "." + _identifier(name) for name in _TABLES) + " IN SHARE MODE NOWAIT"
    )
    _require(await _projection(connection, namespace) == expected_projection.get("projection"), "references_changed")
    return expected_projection
