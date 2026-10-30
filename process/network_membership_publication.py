# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Expected-head publication of a trusted, fully ready membership candidate."""

import hashlib
import json
import os
import re
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_candidate_custody import verify_network_candidate_custody
from process.network_membership_candidate_indexes import _ready_replay
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_validation import _json_object, _scope_report, _validated_replay
from process.network_membership_writer_closure import _role_names, verify_network_candidate_writer_closure


class NetworkPublicationError(ValueError):
    """Publication guards failed without changing the head or candidate."""


def _publication_readiness(candidate):
    from process.registry_source_inventory_reference import verify_source_recipe_metadata

    try:
        verify_source_recipe_metadata({**candidate, "source_recipes_json": candidate.get("source_recipes_json", [])})
    except ValueError:
        raise NetworkPublicationError("Candidate source recipes differ from their retained digest") from None
    report = _json_object(candidate.get("validation_json"))
    if type(report) is not dict or type(report.get("address_source")) is not dict:
        raise NetworkPublicationError("Candidate lacks a pinned validation report")
    address_source = PinnedAddressSource(**report["address_source"])
    if address_source.schema_name == candidate["schema_name"]:
        raise NetworkPublicationError("Pinned source must be outside the candidate")
    scope_report = _scope_report(candidate, address_source)
    report = _validated_replay(candidate, scope_report)
    if scope_report["source_generations"].get("unified_address") != address_source.generation_id:
        raise NetworkPublicationError("Candidate source generation differs from validation")
    _complete_accounting(report)
    from process.network_compact_readiness import compact_readiness, is_compact_candidate

    if is_compact_candidate(candidate):
        canonical, serving = compact_readiness(candidate, report, scope_report)
        return scope_report, canonical, serving, _publication_writer_closure(candidate, report)
    canonical_readiness = _ready_replay(candidate, report, scope_report)
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


publication_candidate_readiness = _publication_readiness


_publication_readiness = publication_candidate_readiness


def _publication_writer_closure(candidate, report):
    from process.registry_source_retention import (
        SOURCE_TABLE,
        RegistrySourceRetentionError,
        verify_registry_source_retention,
    )

    try:
        retention = verify_registry_source_retention(candidate)
    except RegistrySourceRetentionError:
        raise NetworkPublicationError("Candidate source retention differs from its retained digest") from None
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
    from process.network_compact_readiness import compact_serving_heaps, is_compact_candidate

    required_heaps = {"network_membership", "provider_location_binding"}
    required_heaps.update(
        compact_serving_heaps(candidate) if is_compact_candidate(candidate) else {"entity_address_unified"}
    )
    if retention is not None:
        required_heaps.add(SOURCE_TABLE)
    from process.registry_source_contribution_store import CONTRIBUTION_TABLE, verify_registry_source_contributions

    if verify_registry_source_contributions(candidate) is not None:
        required_heaps.add(CONTRIBUTION_TABLE)
    from process.registry_source_inventory_reference import source_inventory_heaps

    required_heaps.update(source_inventory_heaps(candidate))
    if type(closure.get("relation_oids")) is not dict or set(closure["relation_oids"]) != required_heaps:
        raise NetworkPublicationError("Candidate writer closure differs from its serving storage")
    return closure


def _complete_accounting(report):
    from process.registry_source_retention import RegistrySourceRetentionError, verify_registry_source_retention

    try:
        retention = verify_registry_source_retention(
            {
                "source_generations": report.get("source_generations", {}),
                "validation_json": report,
                "expected_rows": report.get("expected_rows"),
            }
        )
    except RegistrySourceRetentionError:
        raise NetworkPublicationError("Candidate retained source accounting is invalid") from None
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
        or any(report[field] != report["expected_rows"] for field in ("raw_rows", "membership_rows"))
        or report["accepted_rows"] != report["expected_rows"]
        or report["unknown_network_rows"] != 0
        or (retention is not None and (report["batch_rows"] != 0 or report["batch_count"] != 0))
        or (retention is None and report["batch_rows"] != report["expected_rows"])
        or (retention is None and report["raw_rows"] > 0 and report["batch_count"] == 0)
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


async def lock_publication_controls(connection, namespace):
    """Lock revision and serving singletons in publication order and return their current generations."""
    revision_control = await connection.fetchrow(
        f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE"
    )
    serving_control = await connection.fetchrow(
        f"SELECT generation_id FROM {namespace}.network_serving_control WHERE id=1 FOR UPDATE"
    )
    if revision_control is None or serving_control is None:
        raise NetworkPublicationError("Publication singleton controls are missing")
    return revision_control["approved_revision"], serving_control["generation_id"] or 0


_lock_publication_controls = lock_publication_controls


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


async def _verify_provider_custody(connection, candidate, closure):
    from process.network_em_provider_inputs import verify_candidate_em_provider_inputs

    runtime_roles = tuple(sorted(set(closure["loader_roles"] + closure["reader_roles"])))
    return await verify_candidate_em_provider_inputs(connection, candidate, runtime_roles=runtime_roles)


async def _candidate_custody(connection, candidate, control_schema, *, api_read=False):
    from process.registry_source_inventory_reference import source_inventory_heaps

    target = MembershipCopyTarget(
        *(str(candidate[field]) for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"))
    )
    if type(api_read) is not bool:
        raise NetworkPublicationError("Candidate read verification mode is invalid")
    verifier = verify_network_candidate_custody
    if api_read:
        from process.network_candidate_custody import verify_network_candidate_read_metadata

        verifier = verify_network_candidate_read_metadata
    custody = await verifier(
        connection, target, control_schema=control_schema, required=bool(source_inventory_heaps(candidate))
    )
    from process.network_compact_readiness import is_compact_candidate

    if custody is not None and is_compact_candidate(candidate):
        from process.network_compact_dependencies import verify_compact_dependencies

        layout = await verify_compact_dependencies(
            connection, candidate, control_schema=control_schema, **({"api_read": True} if api_read else {})
        )
        return {**custody, "compact_layout": layout}
    return custody


async def _publish_candidate(connection, copy_target, namespace, source_rollback_authority=None):
    """Publish only the locked ready candidate with exact custody and rollback authority."""
    approved_revision, current_head = await _lock_publication_controls(connection, namespace)
    candidate = await _locked_candidate(connection, copy_target, namespace)
    if candidate["state"] not in {"ready", "published"}:
        raise NetworkPublicationError("Candidate must be fully ready before publication")
    readiness = _publication_readiness(candidate)
    closure = readiness[3]
    custody = await _require_publication_custody(connection, copy_target, candidate, namespace, closure)
    await _verify_provider_custody(connection, candidate, closure)
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
    from process.network_serving_read import resolve_network_serving_manifest
    from process.registry_fhir_serving_preservation import fhir_preservation_obligation

    generations = _json_object(candidate["source_generations"])
    report = _json_object(candidate["validation_json"])
    is_historical_source = (
        report.get("registry_fhir_serving_preservation", {}).get("operation") == "source_rollback"
        or "registry_source_rollback" in report
    )
    if current_head and generations.get("retained_network_source") is not None:
        current = await resolve_network_serving_manifest(
            connection, generation_id=current_head, control_schema=namespace[1:-1]
        )
        is_historical_source = is_historical_source or generations["retained_network_source"] != current.manifest_sha256
    reviewed_source = None
    if is_historical_source or source_rollback_authority is not None:
        from process.registry_publication_execution import require_registry_source_rollback_publication_authority

        reviewed_source = await require_registry_source_rollback_publication_authority(
            connection, source_rollback_authority, copy_target, candidate, control_schema=namespace[1:-1]
        )
    predecessor = await fhir_preservation_obligation(
        connection, candidate, control_schema=namespace[1:-1], reviewed_source_manifest=reviewed_source
    )
    if predecessor is not None and custody is None:
        raise NetworkPublicationError("FHIR serving preservation requires independent candidate custody")
    return await _insert_manifest(connection, namespace, candidate, readiness)


async def _require_publication_custody(connection, copy_target, candidate, namespace, closure):
    custody = await _candidate_custody(connection, candidate, namespace[1:-1])
    if custody is None:
        from process.network_compact_readiness import is_compact_candidate

        if is_compact_candidate(candidate):
            raise NetworkPublicationError("Compact publication requires independent physical custody")
        from process.registry_source_contribution_store import verify_registry_source_contributions

        if verify_registry_source_contributions(candidate) is not None:
            raise NetworkPublicationError("Source contributions require independent physical custody")
        await verify_network_candidate_writer_closure(
            connection,
            copy_target,
            owner_role=closure["owner_role"],
            loader_roles=tuple(closure["loader_roles"]),
            reader_roles=tuple(closure["reader_roles"]),
            control_schema=namespace[1:-1],
        )
    return custody


async def publish_network_candidate(connection, copy_target, *, control_schema=None, source_rollback_authority=None):
    """Atomically publish trusted readiness, or replay its retained manifest.

    Caller owns commit/rollback. Protected custody or legacy native writer
    closure is verified before initial publication and every replay.
    """
    if type(copy_target) is not MembershipCopyTarget:
        raise NetworkPublicationError("Trusted candidate scope is required")
    if not connection.is_in_transaction():
        raise NetworkPublicationError("Publication requires a caller-owned transaction")
    try:
        namespace = _identifier(control_schema if control_schema is not None else registry_schema())
        async with connection.transaction():
            return await _publish_candidate(connection, copy_target, namespace, source_rollback_authority)
    except (ValueError, TypeError) as error:
        if isinstance(error, NetworkPublicationError):
            raise
        raise NetworkPublicationError(str(error)) from error
