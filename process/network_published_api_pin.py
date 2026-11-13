# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read published routing metadata; publication owns the expensive custody audits."""

import json

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_compact_custom_preparation import compact_carrier_receipt, compact_custom_receipt
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_indexed_address_artifact import CONTRACT as INDEXED_CONTRACT
from process.network_serving_read import NetworkServingReadUnavailable, _pinned_manifest

_PUBLISHED_SQL = """SELECT to_jsonb(candidate)::text AS candidate_json,
 to_jsonb(manifest)::text AS manifest_json,
 jsonb_build_object('closure',seal.receipt_json->'closure','control',(seal.receipt_json->'control')
   ||jsonb_build_object('candidate_table_oid',
     to_regclass('{control}.network_membership_candidate')::oid::bigint),
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
 LEFT JOIN pg_catalog.pg_class physical ON physical.oid=(
  -- Rotation keeps a published heap under its retained name and gives the recorded name to the replacement.
  SELECT named.oid FROM pg_catalog.pg_class named
   JOIN pg_catalog.pg_namespace home ON home.oid=named.relnamespace
  WHERE home.nspname=candidate.validation_json->'address_source'->>'schema_name' AND named.relkind='r'
   AND named.relname IN (candidate.validation_json->'address_source'->>'table_name',
    (candidate.validation_json->'address_source'->>'table_name')||'_retained_'||pg_catalog.to_hex(
     (candidate.validation_json->'serving_readiness'->'address_artifact'->'table'->>'oid')::bigint))
  ORDER BY named.relname=candidate.validation_json->'address_source'->>'table_name' LIMIT 1)
 LEFT JOIN pg_catalog.pg_namespace space ON space.oid=physical.relnamespace
 WHERE manifest.generation_id=coalesce($1::bigint,
  (SELECT generation_id FROM {control}.network_serving_control WHERE id=1))
 AND manifest.eligible AND candidate.state='published'"""


def published_api_pin_statement(control_schema):
    """Select the published pointer, never an uncommitted maximum generation."""
    return _PUBLISHED_SQL.format(control=_identifier(control_schema))


def _require(condition):
    if condition is not True:
        raise NetworkServingReadUnavailable("Protected API serving metadata is unavailable")


def _custom_address_relation(candidate, descriptor):
    report = candidate["validation_json"]
    closure = report["writer_closure"]
    custom, carrier = compact_custom_receipt(candidate), compact_carrier_receipt(candidate)
    if custom is None:
        return None
    _require(custom.base_source == PinnedAddressSource(**report["address_source"]))
    _require(custom.base_artifact_sha256 == descriptor["descriptor_sha256"])
    _require(custom.binding_table_oid == closure["relation_oids"].get("provider_location_binding"))
    _require(custom.runtime_roles == tuple(sorted(set(closure["loader_roles"] + closure["reader_roles"]))))
    address_oid = custom.address_table_oid
    if carrier is not None:
        _require(carrier.base_source == custom.base_source and carrier.runtime_roles == custom.runtime_roles)
        _require(
            carrier.base_artifact_sha256 == custom.base_artifact_sha256
            and carrier.binding_table_oid == custom.binding_table_oid
        )
        _require(not custom.custom_pair_count or address_oid == carrier.address_table_oid)
        address_oid = carrier.address_table_oid
    if custom.custom_pair_count or carrier is not None and carrier.materialized_address_rows:
        _require(address_oid == closure["relation_oids"].get("entity_address_unified"))
        return CompactAddressRelation(candidate["schema_name"], "entity_address_unified", address_oid)
    return None


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
