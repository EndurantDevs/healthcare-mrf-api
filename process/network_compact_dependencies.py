# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve named compact dependencies and their logical candidate readiness."""

import hashlib
from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import (
    CompactAddressRelation,
    CompactEdgeArtifact,
    CompactNetworkLayout,
    bind_compact_geo_search_candidate,
    is_independent_network_candidate,
)
from process.network_compact_readiness import _digest, _require, compact_readiness
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


async def independent_network_storage_metadata(connection, candidate, base, *, control_schema):
    """Record the named serving heaps while both candidates are still isolated."""
    if not is_independent_network_candidate(candidate):
        return {}
    generations = _json_object(candidate["source_generations"])
    schemas = [candidate["schema_name"]]
    if base.schema_name.startswith("network_composition_"):
        schemas.append(base.schema_name)
    if metadata := generations.get("fhir_catalog"):
        catalog = await connection.fetchval(
            f"SELECT schema_name FROM {_identifier(control_schema)}.registry_fhir_catalog_snapshot "
            "WHERE metadata_generation=$1",
            metadata,
        )
        _require(catalog is not None)
        schemas.append(catalog)
    relations = await connection.fetch(
        "SELECT schemaname AS schema_name,tablename AS table_name FROM pg_catalog.pg_tables "
        "WHERE schemaname=ANY($1::text[]) ORDER BY schemaname,tablename",
        schemas,
    )
    named_relations = {(row["schema_name"], row["table_name"]) for row in relations}
    _require(set(schemas) <= {schema for schema, _ in named_relations})
    named_relations.add((control_schema, "registry_approved_record"))
    named_relations.add((base.schema_name, base.table_name))
    return {
        "storage_relations": [{"schema_name": schema, "table_name": table} for schema, table in sorted(named_relations)]
    }


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
    from process.network_compact_address_origin import resolve_compact_address_origin
    from process.network_indexed_address_artifact import address_dependency_data

    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    captured, base = await resolve_compact_address_origin(
        connection, candidate, address_source, control_schema=control_schema
    )
    _require(
        address_dependency_data(captured) == address_dependency_data(report["serving_readiness"]["address_artifact"])
    )
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
            _require(not is_independent_network_candidate(candidate))
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


async def compact_slice_ref_digest(connection, schema_name, *, include_reuse_keys=False):
    """Summarize references, with exact reuse keys for newly prepared typed candidates."""
    namespace = _identifier(schema_name)
    if include_reuse_keys:
        digest = hashlib.sha256()

        async def update(chunk):
            """Hash the ordered native reference rows as they arrive."""
            digest.update(chunk)

        await connection.copy_from_query(
            f"SELECT network_id,office_bucket,artifact_candidate_id,slice_id,row_count,content_sha256 "
            f"FROM {namespace}.network_slice_ref ORDER BY network_id,office_bucket",
            output=update,
            format="binary",
        )
        return digest.hexdigest()
    summary = await connection.fetchrow(
        f"SELECT count(*)::bigint AS rows,"
        f" count(DISTINCT network_id)::bigint AS networks,"
        f" count(DISTINCT slice_id)::bigint AS slices "
        f"FROM {namespace}.network_slice_ref"
    )
    return _digest(
        {
            "table": "network_slice_ref",
            "rows": int(summary["rows"]),
            "networks": int(summary["networks"]),
            "slices": int(summary["slices"]),
        }
    )
