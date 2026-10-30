# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve exact retained serving identities without a mutable-table fallback."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_membership_publication import (
    _candidate_custody,
    _publication_readiness,
    _publication_writer_closure,
    _retained_manifest,
)
from process.network_membership_writer_closure import (
    _NATIVE_WRITER_PRIVILEGES_SQL,
    _check_identity,
    _protected_owner,
    _role_names,
)

_READONLY_WRITER_JOIN_SQL = f"""
        LEFT JOIN LATERAL (
          WITH closure_scope AS (
            SELECT physical_schema.oid AS schema_oid,protected_owner.oid AS owner_oid,
              ARRAY(SELECT jsonb_array_elements_text(
                CASE WHEN jsonb_typeof(candidate.validation_json->'writer_closure'->'loader_roles')='array'
                  THEN candidate.validation_json->'writer_closure'->'loader_roles' ELSE '[]'::jsonb END)
                UNION SELECT jsonb_array_elements_text(
                CASE WHEN jsonb_typeof(candidate.validation_json->'writer_closure'->'reader_roles')='array'
                  THEN candidate.validation_json->'writer_closure'->'reader_roles' ELSE '[]'::jsonb END)
                LIMIT 65) AS role_names,NULL::text[] AS relation_names)
          {_NATIVE_WRITER_PRIVILEGES_SQL}
        ) privileges ON true
"""


class NetworkServingReadUnavailable(ValueError):
    """No eligible, ready and physically retained serving identity is available."""


@dataclass(frozen=True)
class PinnedNetworkServingManifest:
    generation_id: int
    candidate_id: str
    schema_name: str
    schema_revision: int
    source_generations: dict[str, str]
    approved_custom_revision: int
    manifest_sha256: str
    address_table_oid: int

    @property
    def compact_layout(self):
        """Keep verified physical routing outside the public manifest wire format."""
        return getattr(self, "_compact_layout", None)

    @property
    def candidate_relation_pins(self):
        """Verified candidate data identities, outside the public manifest wire format."""
        return getattr(self, "_candidate_relation_pins", ())

    @property
    def custody_control(self):
        """Independent control identity for API snapshot checks; metadata is not payload authority."""
        return getattr(self, "_custody_control", None)


def parse_canonical_network_ids(value: str) -> tuple[int, ...]:
    """Parse the explicit network_ids selector, independently of legacy checksums."""
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


async def _read_manifest(connection, namespace, generation_id, *, include_ineligible=False):
    return await connection.fetchrow(
        f"""SELECT to_jsonb(candidate)::text AS candidate_json,to_jsonb(manifest)::text AS manifest_json,
          jsonb_build_object('schema_oid',physical_schema.oid::bigint,
            'schema_owner_matches',physical_schema.nspowner=protected_owner.oid,
            'owner_attributes',jsonb_build_object('rolcanlogin',protected_owner.rolcanlogin,
              'rolsuper',protected_owner.rolsuper,'rolcreatedb',protected_owner.rolcreatedb,
              'rolcreaterole',protected_owner.rolcreaterole,'rolreplication',protected_owner.rolreplication,
              'rolbypassrls',protected_owner.rolbypassrls),
            'relation_oids',inventory.relation_oids,'index_oids',inventory.index_oids,
            'index_table_oids',inventory.index_table_oids,'owners_match',inventory.owners_match,
            'unsupported_relations',inventory.unsupported_relations,'relation_count',inventory.relation_count,
            'has_routines',EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=physical_schema.oid),
            'address_table_oid',inventory.address_table_oid,'privileges_closed',privileges.closed)::text AS catalog_json
        FROM {namespace}.network_serving_manifest manifest
        JOIN {namespace}.network_membership_candidate candidate ON candidate.candidate_id=manifest.candidate_id
        LEFT JOIN pg_namespace physical_schema ON physical_schema.nspname=candidate.schema_name
        LEFT JOIN pg_roles protected_owner ON protected_owner.rolname=candidate.validation_json->'writer_closure'->>'owner_role'
        LEFT JOIN LATERAL (
          SELECT jsonb_object_agg(relation.relname,relation.oid::bigint) FILTER(WHERE relation.relkind='r') AS relation_oids,
            coalesce(jsonb_object_agg(relation.relname,relation.oid::bigint) FILTER(WHERE relation.relkind='i'),'{{}}'::jsonb)
              AS index_oids,
            coalesce(jsonb_object_agg(relation.relname,index_record.indrelid::bigint) FILTER(WHERE relation.relkind='i'),
              '{{}}'::jsonb) AS index_table_oids,
            bool_and(relation.relowner=protected_owner.oid AND relation.relpersistence='p'
              AND (relation.relkind<>'i' OR relation.relacl IS NULL)) AS owners_match,
            count(*) AS relation_count,
            count(*) FILTER(WHERE relation.relkind NOT IN ('r','i')) AS unsupported_relations,
            min(relation.oid::bigint) FILTER(WHERE relation.relname='entity_address_unified' AND relation.relkind='r')
              AS address_table_oid
          FROM (SELECT * FROM pg_class WHERE relnamespace=physical_schema.oid ORDER BY relname LIMIT 129) relation
          LEFT JOIN pg_index index_record ON index_record.indexrelid=relation.oid
        ) inventory ON true
        {_READONLY_WRITER_JOIN_SQL}
        WHERE manifest.generation_id=coalesce($1::bigint,
          (SELECT generation_id FROM {namespace}.network_serving_control WHERE id=1))
          AND (manifest.eligible OR $2::boolean) AND candidate.state='published'
          AND octet_length(candidate.validation_json::text)<=1048576""",
        generation_id,
        include_ineligible,
    )


def _verified_writer_closure(candidate, catalog):
    closure = _publication_writer_closure(candidate, candidate["validation_json"])
    current_by_field = {
        "component": "network_candidate_writer_closure",
        "revision": 1,
        "scope": {
            field: str(candidate[field])
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        },
        **_role_names(closure["owner_role"], tuple(closure["loader_roles"]), tuple(closure["reader_roles"])),
        **{field: catalog[field] for field in ("schema_oid", "relation_oids", "index_oids", "index_table_oids")},
    }
    _check_identity(current_by_field, closure, extending=False)
    if not _protected_owner(catalog["owner_attributes"]) or (
        catalog["schema_owner_matches"] is not True
        or catalog["owners_match"] is not True
        or catalog["unsupported_relations"] != 0
        or catalog["relation_count"] > 128
        or catalog["has_routines"] is not False
        or catalog["privileges_closed"] is not True
        or any(
            table_oid not in current_by_field["relation_oids"].values()
            for table_oid in current_by_field["index_table_oids"].values()
        )
    ):
        raise NetworkServingReadUnavailable("Network serving physical owner or runtime privileges are not protected")
    address_table_oid = closure["relation_oids"]["entity_address_unified"]
    if address_table_oid != catalog["address_table_oid"]:
        raise NetworkServingReadUnavailable("Network serving address table differs")
    return address_table_oid


def _pinned_manifest(entry, *, custody=None):
    """Authenticate the retained manifest and bind its protected physical dependencies."""
    candidate = json.loads(entry["candidate_json"])
    manifest = json.loads(entry["manifest_json"])
    readiness = _publication_readiness(candidate)
    retained = _retained_manifest(manifest, readiness, replayed=True)
    if retained["schema_name"] != "network_candidate_" + UUID(retained["candidate_id"]).hex:
        raise NetworkServingReadUnavailable("Network serving schema identity differs")
    from process.network_compact_readiness import is_compact_candidate

    is_compact = is_compact_candidate(candidate)
    if is_compact and (custody is None or custody.get("compact_layout") is None):
        raise NetworkServingReadUnavailable("Compact network dependencies lack protected custody")
    from process.registry_source_contribution_store import verify_registry_source_contributions
    from process.registry_source_inventory_reference import source_inventory_heaps

    if custody is None and (
        source_inventory_heaps(candidate) or verify_registry_source_contributions(candidate) is not None
    ):
        raise NetworkServingReadUnavailable("Source contribution dependencies lack protected custody")
    if custody is None:
        address_table_oid = _verified_writer_closure(candidate, json.loads(entry["catalog_json"]))
    else:
        if custody["receipt"]["index_definition_sha256"] != readiness[2]["index_definition_sha256"]:
            raise NetworkServingReadUnavailable("Sealed native index definitions differ from validation")
        address_table_oid = (
            custody["compact_layout"].base.table_oid
            if is_compact
            else custody["receipt"]["closure"]["relation_oids"]["entity_address_unified"]
        )
    pinned = PinnedNetworkServingManifest(
        **{
            field: retained[field]
            for field in (
                "generation_id",
                "candidate_id",
                "schema_name",
                "schema_revision",
                "source_generations",
                "approved_custom_revision",
                "manifest_sha256",
            )
        },
        address_table_oid=address_table_oid,
    )
    _attach_manifest_custody(pinned, custody, readiness, is_compact)
    return pinned


def _attach_manifest_custody(pinned, custody, readiness, is_compact):
    from process.network_compact_layout import CompactAddressRelation

    physical_closure = custody["receipt"]["closure"] if custody is not None else readiness[3]
    object.__setattr__(
        pinned,
        "_candidate_relation_pins",
        tuple(
            CompactAddressRelation(pinned.schema_name, name, oid)
            for name, oid in sorted(physical_closure["relation_oids"].items())
        ),
    )
    if custody is not None:
        # Keep custody proof outside serialized retained/public manifest contracts.
        object.__setattr__(pinned, "_custody_seal_sha256", custody["seal_sha256"])
        object.__setattr__(pinned, "_custody_control", dict(custody["receipt"]["control"]))
    if is_compact:
        object.__setattr__(pinned, "_compact_layout", custody["compact_layout"])


async def resolve_network_serving_manifest(
    connection, *, generation_id: int | None = None, control_schema=None, api_read=False
):
    """Pin the current or retained manifest and verify its physical custody.

    The caller owns the transaction and uses this same identity for every query
    in the request. An unavailable canonical generation never selects legacy data.
    """
    if type(api_read) is not bool:
        raise NetworkServingReadUnavailable("Network read verification mode is invalid")
    if generation_id is not None and (type(generation_id) is not int or not 0 < generation_id <= 9223372036854775807):
        raise NetworkServingReadUnavailable("Network serving generation is invalid")
    if not connection.is_in_transaction():
        raise NetworkServingReadUnavailable("Network serving reads require a caller-owned transaction")
    try:
        namespace = _identifier(control_schema if control_schema is not None else registry_schema())
        entry = await _read_manifest(connection, namespace, generation_id)
        if entry is None:
            raise NetworkServingReadUnavailable("Network serving manifest is unavailable")
        candidate = json.loads(entry["candidate_json"])
        custody = await _candidate_custody(
            connection, candidate, namespace[1:-1], **({"api_read": True} if api_read else {})
        )
        return _pinned_manifest(entry, custody=custody)
    except (ValueError, TypeError, KeyError, asyncpg.PostgresError) as error:
        if isinstance(error, NetworkServingReadUnavailable):
            raise
        raise NetworkServingReadUnavailable("Network serving manifest verification failed") from error


async def resolve_retained_network_address_source(connection, address_source, *, control_schema):
    """Verify retained physical custody without granting serving eligibility."""
    if (
        type(address_source) is not PinnedAddressSource
        or address_source.table_name != "entity_address_unified"
        or re.fullmatch(r"network_candidate_[0-9a-f]{32}", address_source.schema_name) is None
        or re.fullmatch(r"[0-9a-f]{64}", address_source.generation_id) is None
        or not connection.is_in_transaction()
    ):
        raise NetworkServingReadUnavailable("Retained network address source is invalid")
    try:
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
            raise NetworkServingReadUnavailable("Retained network address manifest is unavailable")
        candidate = json.loads(entry["candidate_json"])
        custody = await _candidate_custody(connection, candidate, control_schema)
        pinned = _pinned_manifest(entry, custody=custody)
        if pinned.schema_name != address_source.schema_name or pinned.manifest_sha256 != address_source.generation_id:
            raise NetworkServingReadUnavailable("Retained network address source changed")
        return pinned
    except (ValueError, TypeError, KeyError, asyncpg.PostgresError) as error:
        if isinstance(error, NetworkServingReadUnavailable):
            raise
        raise NetworkServingReadUnavailable("Retained network address verification failed") from error
