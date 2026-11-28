# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve a published network generation by its schema-qualified names."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

import asyncpg

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation, CompactNetworkLayout


class NetworkServingReadUnavailable(ValueError):
    """The selected published generation is unavailable."""


@dataclass(frozen=True)
class PinnedNetworkServingManifest:
    generation_id: int
    candidate_id: str
    schema_name: str
    schema_revision: int
    source_generations: dict[str, str]
    approved_custom_revision: int
    manifest_sha256: str
    address_relation: CompactAddressRelation

    @property
    def compact_layout(self):
        """Return the selected generation's compact routing layout, when present."""
        return getattr(self, "_compact_layout", None)

    @property
    def supports_location_lookup(self):
        """Report whether the selected generation supports location lookup."""
        return getattr(self, "_supports_location_lookup", False) is True

    @property
    def supports_membership_pages(self):
        """Report whether the selected generation supports membership paging."""
        return getattr(self, "_supports_membership_pages", False) is True

    @property
    def candidate_relation_pins(self):
        """Return the selected generation's complete schema-qualified query inputs."""
        relations = [
            CompactAddressRelation(self.schema_name, name)
            for name in ("network_membership", "provider_location_binding")
        ]
        layout = self.compact_layout
        if layout is not None:
            CompactNetworkLayout.__post_init__(layout)
            relations.extend((layout.ref_map, layout.base))
            relations.extend(
                CompactAddressRelation(artifact.schema_name, "network_office_edge") for artifact in layout.artifacts
            )
            if layout.custom is not None:
                relations.append(layout.custom)
            if layout.geo_search is not None:
                from process.network_compact_layout import bind_compact_geo_search

                bind_compact_geo_search(layout, layout.geo_search)
                relations.append(layout.geo_search)
            if layout.network_stats_schema is not None:
                from process.network_compact_layout import bind_compact_network_stats
                from process.network_stats_contract import network_stats_relations

                bind_compact_network_stats(layout, layout.network_stats_schema)
                relations.extend(network_stats_relations(layout.network_stats_schema))
        for relation in relations:
            CompactAddressRelation.__post_init__(relation)
        if len(set(relations)) != len(relations):
            raise NetworkServingReadUnavailable("Published network query relations must be unique")
        return tuple(relations)


def parse_canonical_network_ids(value: str) -> tuple[int, ...]:
    """Parse canonical network IDs used by the public filter."""
    if (
        type(value) is not str
        or len(value) > 1099
        or re.fullmatch(r"[1-9][0-9]{0,9}(?:,[1-9][0-9]{0,9}){0,99}", value) is None
    ):
        raise ValueError("canonical_network_ids_invalid")
    network_ids = tuple(int(token) for token in value.split(","))
    if len(set(network_ids)) != len(network_ids) or any(network_id > 2147483647 for network_id in network_ids):
        raise ValueError("canonical_network_ids_invalid")
    return tuple(sorted(network_ids))


def _object(value):
    value = json.loads(value) if isinstance(value, str) else value
    if type(value) is not dict:
        raise NetworkServingReadUnavailable("Published network metadata is invalid")
    return value


async def _read_manifest(connection, namespace, generation_id, *, include_ineligible=False):
    return await connection.fetchrow(
        """SELECT jsonb_build_object(
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
 'manifest_sha256',manifest.manifest_sha256,'eligible',manifest.eligible)::text AS manifest_json
 FROM {namespace}.network_serving_manifest manifest
 JOIN {namespace}.network_membership_candidate candidate USING(candidate_id)
 WHERE manifest.generation_id=coalesce($1::bigint,
  (SELECT generation_id FROM {namespace}.network_serving_control WHERE id=1))
 AND ($2::boolean OR manifest.eligible) AND candidate.state='published'
 AND coalesce(octet_length(candidate.validation_json::text),0)<=1048576
 AND octet_length(candidate.source_generations::text)<=1048576
 AND coalesce(octet_length(candidate.source_recipes_json::text),0)<=1048576
 AND octet_length(manifest.source_generations::text)<=1048576""".format(namespace=namespace),
        generation_id,
        include_ineligible,
    )


def _manifest_readiness(candidate, manifest):
    """A published generation keeps its original receipt format; today's writer rules never re-derive it."""
    from process.network_compact_readiness import compact_readiness, is_compact_candidate
    from process.network_membership_candidate_indexes import _ready_replay
    from process.network_membership_publication import (
        _complete_accounting,
        _publication_snapshot_tables,
        _retained_manifest,
    )
    from process.network_membership_validation import _candidate_identity_scope, _validated_replay

    report = _object(candidate["validation_json"])
    serving = _object(report["serving_readiness"])
    scope = _object(serving["scope"])
    address = PinnedAddressSource(**report["address_source"])
    identity_scope = _candidate_identity_scope(candidate, address)
    _validated_replay(candidate, identity_scope)
    _validated_replay(candidate, scope)
    _complete_accounting(report)
    if (
        not identity_scope.keys() <= scope.keys()
        or address.schema_name == candidate["schema_name"]
        or scope["source_generations"].get("unified_address") != address.generation_id
    ):
        raise NetworkServingReadUnavailable("Published network readiness differs")
    if is_compact_candidate(candidate):
        canonical, serving = compact_readiness(candidate, report, scope)
    else:
        canonical = _ready_replay(candidate, report, scope)
        if (
            serving.get("component") != "unified_address_serving"
            or type(serving.get("readiness_revision")) is not int
            or serving["readiness_revision"] != 1
            or serving.get("ready") is not True
            or serving.get("index_profile") != "serving"
            or type(serving.get("index_definition_sha256")) is not str
            or re.fullmatch(r"[0-9a-f]{64}", serving["index_definition_sha256"]) is None
        ):
            raise NetworkServingReadUnavailable("Published network readiness differs")
    return _retained_manifest(
        manifest, (scope, canonical, serving, _publication_snapshot_tables(candidate, report)), replayed=True
    )


def _pinned_manifest(entry, *, layout=None):
    """Keep the selected logical generation and its immutable routing names."""
    if entry is None:
        raise NetworkServingReadUnavailable("Published network generation is unavailable")
    candidate, manifest = _object(entry["candidate_json"]), _object(entry["manifest_json"])
    schema = candidate["schema_name"]
    _identifier(schema)
    generations = _object(manifest["source_generations"])
    if (
        str(manifest["candidate_id"]) != str(candidate["candidate_id"])
        or manifest["schema_revision"] != candidate["schema_revision"]
        or generations != _object(candidate["source_generations"])
        or manifest["approved_custom_revision"] != candidate["approved_custom_revision"]
        or type(manifest["generation_id"]) is not int
        or manifest["generation_id"] <= 0
        or type(manifest["approved_custom_revision"]) is not int
        or manifest["approved_custom_revision"] < 0
    ):
        raise NetworkServingReadUnavailable("Published network generation metadata differs")
    try:
        _manifest_readiness(candidate, manifest)
    except (KeyError, TypeError, ValueError) as error:
        raise NetworkServingReadUnavailable("Published network manifest digest differs") from error
    if layout is not None and type(layout) is not CompactNetworkLayout:
        raise NetworkServingReadUnavailable("Published network layout is invalid")
    pinned = PinnedNetworkServingManifest(
        generation_id=manifest["generation_id"],
        candidate_id=str(candidate["candidate_id"]),
        schema_name=schema,
        schema_revision=manifest["schema_revision"],
        source_generations=generations,
        approved_custom_revision=manifest["approved_custom_revision"],
        manifest_sha256=manifest["manifest_sha256"],
        address_relation=layout.base
        if layout is not None
        else CompactAddressRelation(schema, "entity_address_unified"),
    )
    if layout is not None:
        if layout.ref_map.schema_name != schema:
            raise NetworkServingReadUnavailable("Published network layout schema differs")
        object.__setattr__(pinned, "_compact_layout", layout)
        report = _object(candidate["validation_json"])
        serving = report["serving_readiness"]
        address_contract = serving.get("address_artifact", {}).get("contract")
        has_indexed_addresses = address_contract == "network_indexed_address_artifact.postgres.v1" or (
            address_contract == "network_address_artifact.postgres.v1" and serving.get("index_definition_revision") == 2
        )
        contributions = report.get("registry_source_contributions") or {}
        offices = report.get("registry_initial_source_offices") or {}
        has_membership_pages = contributions.get("revision") == 6 and offices.get("revision") == 6
        object.__setattr__(
            pinned, "_supports_location_lookup", has_indexed_addresses or serving.get("location_lookup") is True
        )
        object.__setattr__(
            pinned, "_supports_membership_pages", has_membership_pages or serving.get("membership_pages") is True
        )
    return pinned


async def resolve_network_serving_manifest(
    connection, *, generation_id: int | None = None, control_schema=None, api_read=False
):
    """Pin the current or requested published generation within the caller's transaction."""
    if type(api_read) is not bool:
        raise NetworkServingReadUnavailable("Network read mode is invalid")
    if generation_id is not None and (type(generation_id) is not int or not 0 < generation_id < 2**63):
        raise NetworkServingReadUnavailable("Network serving generation is invalid")
    if not connection.is_in_transaction():
        raise NetworkServingReadUnavailable("Network serving reads require an active transaction")
    try:
        namespace = _identifier(registry_schema() if control_schema is None else control_schema)
        if api_read:
            from process.network_published_api_pin import read_published_api_pin

            published = await read_published_api_pin(connection, generation_id, namespace[1:-1], approved=False)
            return published[0]
        entry = await _read_manifest(connection, namespace, generation_id)
        if entry is None:
            raise NetworkServingReadUnavailable("Published network generation is unavailable")
        return await _published_manifest(connection, entry)
    except (ValueError, TypeError, KeyError, asyncpg.PostgresError) as error:
        if isinstance(error, NetworkServingReadUnavailable):
            raise
        raise NetworkServingReadUnavailable("Published network metadata is unavailable") from error


async def resolve_retained_network_address_source(connection, address_source, *, control_schema):
    """Resolve the retained published address schema from logical generation metadata."""
    if type(address_source) is not PinnedAddressSource or not connection.is_in_transaction():
        raise NetworkServingReadUnavailable("Retained network address source is invalid")
    namespace = _identifier(control_schema)
    generation_id = await connection.fetchval(
        f"SELECT manifest.generation_id FROM {namespace}.network_serving_manifest manifest "
        f"JOIN {namespace}.network_membership_candidate candidate USING(candidate_id) "
        "WHERE candidate.schema_name=$1 AND manifest.manifest_sha256=$2",
        address_source.schema_name,
        address_source.generation_id,
    )
    if generation_id is None:
        raise NetworkServingReadUnavailable("Retained network address source is unavailable")
    entry = await _read_manifest(connection, namespace, generation_id, include_ineligible=True)
    if entry is None:
        raise NetworkServingReadUnavailable("Retained network address source is unavailable")
    return await _published_manifest(connection, entry)


async def _published_manifest(connection, entry):
    """Historical reports keep reading the base heap that rotation retained for them."""
    from process.network_published_api_pin import _rotated_base_name, published_network_layout

    layout = published_network_layout(
        _object(entry["candidate_json"]), retained_base_name=await _rotated_base_name(connection, entry)
    )
    return _pinned_manifest(entry, layout=layout)
