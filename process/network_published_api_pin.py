# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read published routing metadata; publication owns the expensive custody audits."""

import json

from process.network_address_projection import _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_indexed_address_artifact import CONTRACT as INDEXED_CONTRACT
from process.network_serving_api_validation import _custom_address_relation
from process.network_serving_read import NetworkServingReadUnavailable, _pinned_manifest

_PUBLISHED_SQL = """SELECT to_jsonb(candidate)::text AS candidate_json,
 to_jsonb(manifest)::text AS manifest_json,
 jsonb_build_object('closure',seal.receipt_json->'closure','control',seal.receipt_json->'control',
   'index_definition_sha256',seal.receipt_json->'index_definition_sha256')::text AS receipt_json,
 seal.seal_sha256,physical.relname AS base_name,space.nspname AS base_schema,
 (physical.relrowsecurity OR physical.relforcerowsecurity) AS base_rls,
 to_regclass('{control}.registry_approved_record')::oid::bigint AS approved_oid,
 (SELECT coalesce(sum(jsonb_array_length(record_json->'memberships_json')),0)::bigint
  FROM {control}.registry_approved_record WHERE approved_revision=candidate.approved_custom_revision
   AND record_kind='membership' AND record_json->'archived'='false'::jsonb) AS approved_rows
 FROM {control}.network_serving_manifest manifest
 JOIN {control}.network_membership_candidate candidate USING(candidate_id)
 LEFT JOIN {control}.network_candidate_custody seal USING(candidate_id)
 LEFT JOIN pg_catalog.pg_class physical ON physical.oid=
  (candidate.validation_json->'serving_readiness'->'address_artifact'->'table'->>'oid')::oid
 LEFT JOIN pg_catalog.pg_namespace space ON space.oid=physical.relnamespace
 WHERE manifest.generation_id=coalesce($1::bigint,
  (SELECT generation_id FROM {control}.network_serving_control WHERE id=1))
 AND manifest.eligible AND candidate.state='published'"""


def published_api_pin_statement(control_schema):
    """Select the published pointer, never an uncommitted maximum generation."""
    return _PUBLISHED_SQL.format(control=_identifier(control_schema))


def _decode_published_pin(entry, schema):
    if entry is None:
        raise NetworkServingReadUnavailable("Published network generation is unavailable")
    candidate = json.loads(entry["candidate_json"])
    report = candidate["validation_json"]
    descriptor = report["serving_readiness"].get("address_artifact")
    if type(descriptor) is not dict or descriptor.get("contract") != INDEXED_CONTRACT:
        return None
    if entry["base_rls"] is not False:
        raise NetworkServingReadUnavailable("Published network address rows are unavailable")
    canonical = report["candidate_readiness"]
    layout = CompactNetworkLayout(
        CompactAddressRelation(candidate["schema_name"], "network_slice_ref", canonical["ref_map_oid"]),
        CompactAddressRelation(entry["base_schema"], entry["base_name"], descriptor["table"]["oid"]),
        tuple(
            CompactEdgeArtifact(artifact["candidate_id"], artifact["schema_name"], artifact["table_oid"])
            for artifact in report["serving_readiness"]["edge_artifacts"]
        ),
        _custom_address_relation(candidate, descriptor),
    )
    from process.network_compact_layout import bind_compact_geo_search_candidate

    layout = bind_compact_geo_search_candidate(layout, candidate)
    manifest = _pinned_manifest(
        entry,
        custody={
            "receipt": json.loads(entry["receipt_json"]),
            "seal_sha256": entry["seal_sha256"],
            "compact_layout": layout,
        },
    )
    relation = CompactAddressRelation(schema, "registry_approved_record", entry["approved_oid"])
    # Published approved revisions are append-only; approval creates a new revision.
    approved = ApprovedMembershipSource(
        manifest.approved_custom_revision, manifest.source_generations["custom_membership"], entry["approved_rows"]
    )
    object.__setattr__(manifest, "_published_api_request", True)
    object.__setattr__(manifest, "_api_approved_source", approved)
    object.__setattr__(manifest, "_api_approved_relation", relation)
    return manifest, approved, relation


async def read_published_api_pin(connection, generation_id, schema):
    """Use stored publication evidence; actual data queries report missing objects or access."""
    if generation_id is not None and (type(generation_id) is not int or not 0 < generation_id < 2**63):
        raise NetworkServingReadUnavailable("Published network generation selector is invalid")
    entry = await connection.fetchrow(published_api_pin_statement(schema), generation_id)
    return _decode_published_pin(entry, schema)


def active_published_api_pin(connection, manifest=None, *, control_schema=None):
    """Reuse only the exact manifest issued inside this connection's active API scope."""
    from process.registry_network_catalog_read import _PINNED_READ

    pinned = _PINNED_READ.get()
    if pinned is None or pinned[0] is not connection or getattr(pinned[1], "_published_api_request", False) is not True:
        return None
    session = getattr(pinned[1], "_api_request_session", None)
    if (
        (manifest is not None and manifest is not pinned[1])
        or (control_schema is not None and control_schema != pinned[2])
        or not connection.is_in_transaction()
        or session is None
        or session.get_transaction() is not getattr(pinned[1], "_api_request_transaction", None)
    ):
        raise NetworkServingReadUnavailable("Published network request snapshot differs")
    return pinned[1]
