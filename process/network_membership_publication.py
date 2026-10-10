# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Expected-head publication of a trusted, fully ready membership candidate."""

import hashlib
import json
import os
import re
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_membership_candidate_indexes import _ready_replay
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_validation import _json_object, _scope_report, _validated_replay
from process.network_membership_writer_closure import _role_names, verify_network_candidate_writer_closure


class NetworkPublicationError(ValueError):
    """Publication guards failed without changing the head or candidate."""


def _publication_readiness(candidate):
    from process.registry_source_recipe_store import RegistrySourceRecipeError, verify_registry_source_recipes

    try:
        verify_registry_source_recipes({**candidate, "source_recipes_json": candidate.get("source_recipes_json", [])})
    except RegistrySourceRecipeError:
        raise NetworkPublicationError("Candidate source recipes differ from their retained digest") from None
    report = _json_object(candidate["validation_json"])
    if type(report) is not dict or type(report.get("address_source")) is not dict:
        raise NetworkPublicationError("Candidate lacks a pinned validation report")
    address_source = PinnedAddressSource(**report["address_source"])
    if address_source.schema_name == candidate["schema_name"]:
        raise NetworkPublicationError("Pinned source must be outside the candidate")
    scope_report = _scope_report(candidate, address_source)
    report = _validated_replay(candidate, scope_report)
    if scope_report["source_generations"].get("unified_address") != address_source.generation_id:
        raise NetworkPublicationError("Candidate source generation differs from validation")
    canonical_readiness = _ready_replay(candidate, report, scope_report)
    _complete_accounting(report)
    serving_readiness = report.get("serving_readiness")
    if (
        type(serving_readiness) is not dict
        or serving_readiness.get("component") != "unified_address_serving"
        or type(serving_readiness.get("readiness_revision")) is not int
        or serving_readiness.get("readiness_revision") != 1
        or serving_readiness.get("ready") is not True
        or serving_readiness.get("index_profile") != "serving"
        or json.dumps(serving_readiness.get("scope"), sort_keys=True) != json.dumps(scope_report, sort_keys=True)
        or type(serving_readiness.get("index_definition_sha256")) is not str
        or re.fullmatch(r"[0-9a-f]{64}", serving_readiness["index_definition_sha256"]) is None
    ):
        raise NetworkPublicationError("Candidate lacks matching full serving readiness")
    return scope_report, canonical_readiness, serving_readiness, _publication_writer_closure(candidate, report)


def _publication_writer_closure(candidate, report):
    closure = report.get("writer_closure")
    expected_scope_dict = {
        field: str(candidate[field])
        for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    }
    if (
        type(closure) is not dict
        or closure.get("component") != "network_candidate_writer_closure"
        or type(closure.get("revision")) is not int
        or closure["revision"] != 1
        or closure.get("scope") != expected_scope_dict
        or type(closure.get("loader_roles")) is not list
        or type(closure.get("reader_roles")) is not list
    ):
        raise NetworkPublicationError("Candidate lacks a final matching writer closure")
    roles_by_group = _role_names(
        closure.get("owner_role"), tuple(closure["loader_roles"]), tuple(closure["reader_roles"])
    )
    if any(closure[role_group] != expected for role_group, expected in roles_by_group.items()):
        raise NetworkPublicationError("Candidate writer roles differ from their canonical receipt")
    if type(closure.get("relation_oids")) is not dict or set(closure["relation_oids"]) != {
        "network_membership",
        "provider_location_binding",
        "entity_address_unified",
    }:
        raise NetworkPublicationError("Candidate writer closure lacks the projected address heap")
    return closure


def _complete_accounting(report):
    counted_fields = (
        "raw_rows",
        "batch_rows",
        "batch_count",
        "membership_rows",
        "distinct_memberships",
        "projected_locations",
        "orphan_bindings",
        "unknown_network_rows",
    )
    if (
        any(type(report.get(field)) is not int or report[field] < 0 for field in counted_fields)
        or any(report[field] != report["expected_rows"] for field in ("raw_rows", "batch_rows", "membership_rows"))
        or report["accepted_rows"] != report["expected_rows"]
        or report["unknown_network_rows"] != 0
        or (report["raw_rows"] > 0 and report["batch_count"] == 0)
        or report.get("accounting_errors") != []
        or report["distinct_memberships"] > report["membership_rows"]
        or report["projected_locations"] > report["distinct_memberships"]
    ):
        raise NetworkPublicationError("Candidate validation accounting is incomplete")


def _manifest_digest(generation_id, scope_report, canonical_readiness, serving_readiness, writer_closure):
    manifest_by_field = {
        "generation_id": generation_id,
        **{
            field: scope_report[field]
            for field in ("candidate_id", "schema_revision", "source_generations", "approved_custom_revision")
        },
        "candidate_readiness": canonical_readiness,
        "serving_readiness": serving_readiness,
        "writer_closure": writer_closure,
    }
    encoded = json.dumps(manifest_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _retained_manifest(manifest, readiness, *, replayed):
    scope_report, canonical_readiness, serving_readiness, writer_closure = readiness
    sources_by_name = _json_object(manifest["source_generations"])
    if (
        str(manifest["candidate_id"]) != scope_report["candidate_id"]
        or manifest["schema_revision"] != scope_report["schema_revision"]
        or sources_by_name != scope_report["source_generations"]
        or manifest["approved_custom_revision"] != scope_report["approved_custom_revision"]
        or manifest["manifest_sha256"]
        != _manifest_digest(
            manifest["generation_id"], scope_report, canonical_readiness, serving_readiness, writer_closure
        )
    ):
        raise NetworkPublicationError("Retained manifest differs from its immutable candidate")
    return {
        **{
            field: scope_report[field]
            for field in ("candidate_id", "dataset_id", "schema_id", "producer_id", "schema_name")
        },
        **{
            field: manifest[field]
            for field in ("generation_id", "schema_revision", "approved_custom_revision", "manifest_sha256", "eligible")
        },
        "source_generations": sources_by_name,
        "replayed": replayed,
    }


async def _lock_publication_controls(connection, namespace):
    revision_control = await connection.fetchrow(
        f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE"
    )
    serving_control = await connection.fetchrow(
        f"SELECT generation_id FROM {namespace}.network_serving_control WHERE id=1 FOR UPDATE"
    )
    if revision_control is None or serving_control is None:
        raise NetworkPublicationError("Publication singleton controls are missing")
    return revision_control["approved_revision"], serving_control["generation_id"] or 0


async def _insert_manifest(connection, namespace, candidate, readiness):
    generation_id = await connection.fetchval(
        "SELECT nextval(pg_get_serial_sequence($1,'generation_id'))", namespace + ".network_serving_manifest"
    )
    manifest_sha256 = _manifest_digest(generation_id, *readiness)
    manifest = await connection.fetchrow(
        f"INSERT INTO {namespace}.network_serving_manifest "
        "(generation_id,candidate_id,schema_revision,source_generations,approved_custom_revision,manifest_sha256) "
        "OVERRIDING SYSTEM VALUE VALUES($1,$2,$3,$4::jsonb,$5,$6) RETURNING *",
        generation_id,
        candidate["candidate_id"],
        candidate["schema_revision"],
        json.dumps(readiness[0]["source_generations"], sort_keys=True),
        candidate["approved_custom_revision"],
        manifest_sha256,
    )
    status = await connection.execute(
        f"UPDATE {namespace}.network_serving_control SET generation_id=$1 WHERE id=1 AND COALESCE(generation_id,0)=$2",
        generation_id,
        candidate["expected_head"],
    )
    if status != "UPDATE 1":
        raise NetworkPublicationError("Serving head changed before publication")
    await connection.execute(
        f"UPDATE {namespace}.network_membership_candidate SET state='published' WHERE candidate_id=$1",
        candidate["candidate_id"],
    )
    return _retained_manifest(manifest, readiness, replayed=False)


async def _publish_candidate(connection, copy_target, namespace):
    approved_revision, current_head = await _lock_publication_controls(connection, namespace)
    candidate = await _locked_candidate(connection, copy_target, namespace)
    if candidate["state"] not in {"ready", "published"}:
        raise NetworkPublicationError("Candidate must be fully ready before publication")
    readiness = _publication_readiness(candidate)
    closure = readiness[3]
    await verify_network_candidate_writer_closure(
        connection,
        copy_target,
        owner_role=closure["owner_role"],
        loader_roles=tuple(closure["loader_roles"]),
        reader_roles=tuple(closure["reader_roles"]),
        control_schema=namespace[1:-1],
    )
    manifest = await connection.fetchrow(
        f"SELECT * FROM {namespace}.network_serving_manifest WHERE candidate_id=$1", UUID(copy_target.candidate_id)
    )
    if candidate["state"] == "published":
        if manifest is None:
            raise NetworkPublicationError("Published candidate has no retained manifest")
        return _retained_manifest(manifest, readiness, replayed=True)
    if manifest is not None:
        raise NetworkPublicationError("Unpublished candidate already has a retained manifest")
    if approved_revision != candidate["approved_custom_revision"]:
        raise NetworkPublicationError("Approved custom revision changed before publication")
    if current_head != candidate["expected_head"]:
        raise NetworkPublicationError("Expected serving head changed before publication")
    return await _insert_manifest(connection, namespace, candidate, readiness)


async def publish_network_candidate(connection, copy_target, *, control_schema=None):
    """Atomically publish trusted readiness, or replay its retained manifest.

    Caller owns commit/rollback. Native writer closure is verified against the
    final retained receipt before initial publication and every replay.
    """
    if type(copy_target) is not MembershipCopyTarget:
        raise NetworkPublicationError("Trusted candidate scope is required")
    if not connection.is_in_transaction():
        raise NetworkPublicationError("Publication requires a caller-owned transaction")
    try:
        namespace = _identifier(control_schema if control_schema is not None else registry_schema())
        async with connection.transaction():
            return await _publish_candidate(connection, copy_target, namespace)
    except (ValueError, TypeError) as error:
        if isinstance(error, NetworkPublicationError):
            raise
        raise NetworkPublicationError(str(error)) from error
