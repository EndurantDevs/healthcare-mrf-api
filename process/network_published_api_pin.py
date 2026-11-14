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
 'validation_json',candidate.validation_json)::text AS candidate_json,
 jsonb_build_object('candidate_id',manifest.candidate_id,'generation_id',manifest.generation_id,
 'schema_revision',manifest.schema_revision,'source_generations',manifest.source_generations,
 'approved_custom_revision',manifest.approved_custom_revision,
 'manifest_sha256',manifest.manifest_sha256,'eligible',manifest.eligible)::text AS manifest_json,
 {approved_rows} AS approved_rows
 FROM {control}.network_serving_manifest manifest
 JOIN {control}.network_membership_candidate candidate USING(candidate_id)
 WHERE manifest.generation_id=coalesce($1::bigint,
  (SELECT generation_id FROM {control}.network_serving_control WHERE id=1))
 AND manifest.eligible AND candidate.state='published'
 AND coalesce(octet_length(candidate.validation_json::text),0)<=1048576
 AND octet_length(candidate.source_generations::text)<=1048576
 AND octet_length(manifest.source_generations::text)<=1048576"""


_APPROVED_ROWS_SQL = """(SELECT coalesce(sum(jsonb_array_length(record_json->'memberships_json')),0)::bigint
  FROM {control}.registry_approved_record WHERE approved_revision=candidate.approved_custom_revision
   AND record_kind='membership' AND record_json->'archived'='false'::jsonb)"""


def published_api_pin_statement(control_schema, *, approved=True):
    """Select the published pointer, optionally including its approved membership rows."""
    control = _identifier(control_schema)
    approved_rows = _APPROVED_ROWS_SQL.format(control=control) if approved else "NULL::bigint"
    return _PUBLISHED_SQL.format(control=control, approved_rows=approved_rows)


def _named_relation(document):
    return CompactAddressRelation(document["schema_name"], document["table_name"])


def published_network_layout(candidate):
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
    if type(sources_by_kind) is dict:
        base = _named_relation(sources_by_kind["base"])
        custom = None if sources_by_kind.get("custom") is None else _named_relation(sources_by_kind["custom"])
    else:
        address = _object(report["address_source"])
        artifact = _object(serving["address_artifact"])
        namespace = artifact.get("namespace", {})
        table = artifact.get("table", {})
        base = CompactAddressRelation(
            namespace.get("name", address["schema_name"]), table.get("name", address["table_name"])
        )
        direct = report.get("compact_custom_offices") or {}
        carriers = report.get("compact_address_carriers") or {}
        has_custom = direct.get("custom_pair_count", 0) > 0 or carriers.get("materialized_address_rows", 0) > 0
        custom = CompactAddressRelation(schema, "entity_address_unified") if has_custom else None
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


def _decode_published_pin(entry, schema, *, approved=True):
    if entry is None:
        raise NetworkServingReadUnavailable("Published network generation is unavailable")
    candidate = _object(entry["candidate_json"])
    manifest = _pinned_manifest(entry, layout=published_network_layout(candidate))
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
    return _decode_published_pin(entry, schema, approved=approved)


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
