# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read published network routing using schema-qualified relation names."""

from process.network_address_projection import _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_serving_read import NetworkServingReadUnavailable, _object, _pinned_manifest

_PUBLISHED_SQL = """SELECT jsonb_build_object(
 'candidate_id',candidate.candidate_id,'dataset_id',candidate.dataset_id,
 'schema_id',candidate.schema_id,'producer_id',candidate.producer_id,
 'schema_name',candidate.schema_name,'schema_revision',candidate.schema_revision,
 'source_generations',candidate.source_generations,
 'approved_custom_revision',candidate.approved_custom_revision,
 'expected_head',candidate.expected_head,'expected_rows',candidate.expected_rows,
 'accepted_rows',candidate.accepted_rows,'index_ready',candidate.index_ready,
 'source_recipes_json',candidate.source_recipes_json,
 'validation_json',candidate.validation_json)::text AS candidate_json,
 jsonb_build_object('candidate_id',manifest.candidate_id,'generation_id',manifest.generation_id,
 'schema_revision',manifest.schema_revision,'source_generations',manifest.source_generations,
 'approved_custom_revision',manifest.approved_custom_revision,
 'manifest_sha256',manifest.manifest_sha256,'eligible',manifest.eligible)::text AS manifest_json,
 {approved_rows} AS approved_rows,
 (SELECT {address_heap} FROM (SELECT candidate.validation_json#>'{{serving_readiness,address_artifact}}' AS artifact) a
  WHERE a.artifact->>'contract'='network_address_artifact.postgres.v1' AND a.artifact->>'revision'='2') AS address_heap
 FROM {control}.network_serving_manifest manifest
 JOIN {control}.network_membership_candidate candidate USING(candidate_id)
 WHERE manifest.generation_id=coalesce($1::bigint,
  (SELECT generation_id FROM {control}.network_serving_control WHERE id=1))
 AND manifest.eligible AND candidate.state='published'
 AND coalesce(octet_length(candidate.validation_json::text),0)<=1048576
 AND octet_length(candidate.source_generations::text)<=1048576
 AND coalesce(octet_length(candidate.source_recipes_json::text),0)<=1048576
 AND octet_length(manifest.source_generations::text)<=1048576"""

# Read the saved schema's logical generation inside the pin's existing statement budget.
_ADDRESS_HEAP_SQL = """jsonb_build_object(
 'authority',to_regclass(format('%I.entity_address_result_generation',a.artifact#>>'{table,schema_name}')) IS NOT NULL,
 'current',CASE WHEN to_regclass(format('%I.entity_address_result_generation',
  a.artifact#>>'{table,schema_name}')) IS NOT NULL THEN coalesce((xpath('/row/is_current/text()',query_to_xml(format(
  'SELECT local_lineage_id=%L::uuid AND local_generation=%L::bigint AND origin_lineage_id=%L::uuid '
  'AND origin_generation=%L::bigint AND published_at=%L::timestamptz AS is_current '
  'FROM %I.entity_address_result_generation WHERE singleton',
  a.artifact#>>'{authority,local_lineage_id}',a.artifact#>>'{authority,local_generation}',
  a.artifact#>>'{authority,origin_lineage_id}',a.artifact#>>'{authority,origin_generation}',
  a.artifact#>>'{authority,published_at}',a.artifact#>>'{table,schema_name}'),true,true,'')))[1]::text::boolean,false)
  ELSE false END,
 'live',to_regclass(format('%I.%I',a.artifact#>>'{table,schema_name}',a.artifact#>>'{table,table_name}')) IS NOT NULL,
 'retained',to_regclass(format('%I.%I',a.artifact#>>'{retained_table,schema_name}',
  a.artifact#>>'{retained_table,table_name}')) IS NOT NULL)"""


_APPROVED_ROWS_SQL = """(SELECT coalesce(sum(jsonb_array_length(record_json->'memberships_json')),0)::bigint
  FROM {control}.registry_approved_record WHERE approved_revision=candidate.approved_custom_revision
   AND record_kind='membership' AND record_json->'archived'='false'::jsonb)"""


def published_api_pin_statement(control_schema, *, approved=True):
    """Select the published pointer, optionally including its approved membership rows."""
    control = _identifier(control_schema)
    approved_rows = _APPROVED_ROWS_SQL.format(control=control) if approved else "NULL::bigint"
    return _PUBLISHED_SQL.format(control=control, approved_rows=approved_rows, address_heap=_ADDRESS_HEAP_SQL)


def _named_relation(document):
    return CompactAddressRelation(document["schema_name"], document["table_name"])


def published_network_layout(candidate, *, retained_base_name=None):
    """Decode the published layout while accepting historical metadata's extra fields."""
    if candidate["validation_json"] is None:
        return None
    report = _object(candidate["validation_json"])
    serving = _object(report["serving_readiness"])
    if serving.get("component") != "compact_network_serving":
        return None
    schema = candidate["schema_name"]
    search = serving.get("geo_search")
    sources_by_kind = search.get("sources") if type(search) is dict else None
    address = _object(report["address_source"])
    artifact = _object(serving["address_artifact"])
    namespace = artifact.get("namespace", {})
    table = artifact.get("table", {})
    artifact_base = CompactAddressRelation(
        namespace.get("name", address["schema_name"]), table.get("table_name", table.get("name", address["table_name"]))
    )
    if type(sources_by_kind) is dict:
        base = _named_relation(sources_by_kind["base"])
        custom = None if sources_by_kind.get("custom") is None else _named_relation(sources_by_kind["custom"])
    else:
        base = artifact_base
        direct = report.get("compact_custom_offices") or {}
        carriers = report.get("compact_address_carriers") or {}
        has_custom = direct.get("custom_pair_count", 0) > 0 or carriers.get("materialized_address_rows", 0) > 0
        custom = CompactAddressRelation(schema, "entity_address_unified") if has_custom else None
    if retained_base_name is not None and base == artifact_base:
        # A pinned generation keeps reading the heap that rotation retained for it.
        base = CompactAddressRelation(base.schema_name, retained_base_name)
    layout = CompactNetworkLayout(
        CompactAddressRelation(schema, "network_slice_ref"),
        base,
        tuple(
            CompactEdgeArtifact(artifact["candidate_id"], artifact["schema_name"])
            for artifact in serving["edge_artifacts"]
        ),
        custom,
    )
    if type(search) is dict:
        from process.network_compact_layout import bind_compact_geo_search

        bind_compact_geo_search(layout, CompactAddressRelation(schema, search["table_name"]))
    return layout


def _saved_address_artifact(candidate):
    if candidate is None or candidate["validation_json"] is None:
        return None
    return _object(_object(candidate["validation_json"]).get("serving_readiness") or {}).get("address_artifact")


def _historical_retained_base(candidate):
    """Name the heap rotation keeps for a report that still records the old base table identity."""
    report = _object(candidate["validation_json"]) if candidate["validation_json"] is not None else {}
    artifact = _object(report.get("serving_readiness") or {}).get("address_artifact")
    table = artifact.get("table") if type(artifact) is dict else None
    if type(table) is not dict or type(table.get("oid")) is not int:
        return None
    address = _object(report["address_source"])
    schema = artifact.get("namespace", {}).get("name", address["schema_name"])
    return CompactAddressRelation(
        schema, table.get("name", address["table_name"]) + "_retained_" + format(table["oid"], "x")
    )


async def _rotated_base_name(connection, entry):
    """Return the retained name when rotation has moved a historical report's base heap."""
    from process.network_address_artifact import CONTRACT, _descriptor, _require, pinned_address_heap

    candidate = None if entry is None else _object(entry["candidate_json"])
    artifact = _saved_address_artifact(candidate)
    if type(artifact) is dict and artifact.get("contract") == CONTRACT and artifact.get("revision") == 2:
        if "address_heap" not in entry.keys():
            relation = await pinned_address_heap(connection, artifact)
            return None if relation.table_name == artifact["table"]["table_name"] else relation.table_name
        descriptor, presence = _descriptor(artifact), _object(entry["address_heap"])
        is_current = presence["live"] is True and presence["current"] is True
        _require(is_current != (presence["retained"] is True), "retained_table_unavailable")
        return None if is_current else descriptor["retained_table"]["table_name"]
    retained = None if candidate is None else _historical_retained_base(candidate)
    if retained is None or not await connection.fetchval(
        "SELECT to_regclass($1) IS NOT NULL", _identifier(retained.schema_name) + "." + _identifier(retained.table_name)
    ):
        return None
    return retained.table_name


def _decode_published_pin(entry, schema, *, approved=True, retained_base_name=None):
    if entry is None:
        raise NetworkServingReadUnavailable("Published network generation is unavailable")
    candidate = _object(entry["candidate_json"])
    manifest = _pinned_manifest(
        entry, layout=published_network_layout(candidate, retained_base_name=retained_base_name)
    )
    if not approved:
        return manifest, None, None
    relation = CompactAddressRelation(schema, "registry_approved_record")
    approved = ApprovedMembershipSource(
        manifest.approved_custom_revision, manifest.source_generations["custom_membership"], entry["approved_rows"]
    )
    object.__setattr__(manifest, "_published_api_request", True)
    object.__setattr__(manifest, "_api_approved_source", approved)
    object.__setattr__(manifest, "_api_approved_relation", relation)
    return manifest, approved, relation


async def read_published_api_pin(connection, generation_id, schema, *, approved=True):
    """Read the requested or current logical generation in the caller's snapshot."""
    if generation_id is not None and (type(generation_id) is not int or not 0 < generation_id < 2**63):
        raise NetworkServingReadUnavailable("Published network generation selector is invalid")
    entry = await connection.fetchrow(published_api_pin_statement(schema, approved=approved), generation_id)
    # Publication refuses row filtered address heaps and they stay frozen, so reads need no re-check.
    return _decode_published_pin(
        entry, schema, approved=approved, retained_base_name=await _rotated_base_name(connection, entry)
    )


def active_published_api_pin(connection, manifest=None, *, control_schema=None):
    """Reuse the logical generation selected by this request's read context."""
    from process.registry_network_catalog_read import _PINNED_READ

    pinned = _PINNED_READ.get()
    if pinned is None:
        return None
    current, schema = pinned[-2:]
    if getattr(current, "_published_api_request", False) is not True:
        return None
    if manifest is not None and manifest != current or control_schema is not None and control_schema != schema:
        raise NetworkServingReadUnavailable("Published network generation differs from the request")
    return current
