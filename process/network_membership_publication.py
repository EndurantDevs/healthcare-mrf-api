# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Expected-head publication of a trusted, fully ready membership candidate."""

import hashlib
import json
import os
import re
from contextlib import AsyncExitStack
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_membership_candidate_indexes import _ready_replay
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_validation import _json_object, _scope_report, _validated_replay


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
        return scope_report, canonical, serving, _publication_snapshot_tables(candidate, report)
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
    return scope_report, canonical_readiness, serving_readiness, _publication_snapshot_tables(candidate, report)


publication_candidate_readiness = _publication_readiness


_publication_readiness = publication_candidate_readiness


def _publication_snapshot_tables(candidate, report):
    """Describe the named payload tables required by this complete data generation."""
    from process.network_compact_readiness import compact_serving_heaps, is_compact_candidate
    from process.registry_source_contribution_store import CONTRIBUTION_TABLE, verify_registry_source_contributions
    from process.registry_source_inventory_reference import source_inventory_heaps
    from process.registry_source_retention import SOURCE_TABLE, verify_registry_source_retention

    schema = candidate["schema_name"]
    _identifier(schema)
    required_tables = {"network_membership", "provider_location_binding"}
    required_tables.update(
        compact_serving_heaps(candidate) if is_compact_candidate(candidate) else {"entity_address_unified"}
    )
    if verify_registry_source_retention(candidate) is not None:
        required_tables.add(SOURCE_TABLE)
    if verify_registry_source_contributions(candidate) is not None:
        required_tables.add(CONTRIBUTION_TABLE)
    required_tables.update(source_inventory_heaps(candidate))
    return {
        "schema_name": schema,
        "tables": sorted(required_tables),
        "validated": report.get("final_validation") is not None,
    }


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


def _manifest_digest(generation_id, scope_report, canonical_readiness, serving_readiness, snapshot_tables):
    """Identify the named logical edition and its complete data accounting."""
    manifest_by_field = {
        "generation_id": generation_id,
        **{
            field: scope_report[field]
            for field in ("candidate_id", "schema_revision", "source_generations", "approved_custom_revision")
        },
        "schema_name": snapshot_tables["schema_name"],
        "tables": snapshot_tables["tables"],
        "accounting": {
            field: canonical_readiness[field]
            for field in ("membership_rows", "distinct_memberships", "projected_locations")
        },
        "serving_profile": serving_readiness["index_profile"],
    }
    encoded = json.dumps(manifest_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _retained_manifest(manifest, readiness, *, replayed):
    scope_report, canonical_readiness, serving_readiness, snapshot_tables = readiness
    sources_by_name = _json_object(manifest["source_generations"])
    if (
        str(manifest["candidate_id"]) != scope_report["candidate_id"]
        or manifest["schema_revision"] != scope_report["schema_revision"]
        or sources_by_name != scope_report["source_generations"]
        or manifest["approved_custom_revision"] != scope_report["approved_custom_revision"]
        or type(manifest["manifest_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", manifest["manifest_sha256"]) is None
        or snapshot_tables["validated"]
        and manifest["manifest_sha256"]
        != _manifest_digest(
            manifest["generation_id"], scope_report, canonical_readiness, serving_readiness, snapshot_tables
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


def named_network_preparation_roles(configured=None):
    """Use the worker's configured role names for the administrator permission step."""
    if configured is None:
        from process.registry_publication_worker import CONFIG_ENV, unique_json_fields

        encoded = os.environ.get(CONFIG_ENV)
        if type(encoded) is not str or len(encoded.encode()) > 1048576:
            raise NetworkPublicationError("Snapshot preparation roles are required")
        document = json.loads(encoded, object_pairs_hook=unique_json_fields)
        configured = document.get("preparation_roles") if type(document) is dict else None
    if type(configured) is not dict or set(configured) != {
        "namespace_owner_role",
        "publisher_role",
        "application_roles",
    }:
        raise NetworkPublicationError("Snapshot preparation role configuration is invalid")
    applications = configured["application_roles"]
    if type(applications) is not list or not 1 <= len(applications) <= 128:
        raise NetworkPublicationError("Snapshot application roles must be a bounded list")
    for role in (configured["namespace_owner_role"], configured["publisher_role"], *applications):
        _identifier(role)
    if (
        len(set(applications)) != len(applications)
        or configured["publisher_role"] not in applications
        or configured["namespace_owner_role"] in applications
    ):
        raise NetworkPublicationError("Snapshot writer and frozen owner roles must be distinct")
    return {
        "namespace_owner_role": configured["namespace_owner_role"],
        "publisher_role": configured["publisher_role"],
        "application_roles": sorted(applications),
    }


async def load_named_network_preparation(connection, candidate_id, *, control_schema=None):
    """Load portable resume data for one ready or published named generation."""
    schema = control_schema if control_schema is not None else registry_schema()
    namespace = _identifier(schema)
    identity = UUID(str(candidate_id))
    candidate_by_field = await connection.fetchrow(
        f"SELECT * FROM {namespace}.network_membership_candidate WHERE candidate_id=$1", identity
    )
    if candidate_by_field is None or candidate_by_field["state"] not in {"ready", "published"}:
        return None
    candidate_by_field = dict(candidate_by_field)
    storage_schema = candidate_by_field["schema_name"]
    _identifier(storage_schema)
    if storage_schema == schema:
        raise NetworkPublicationError("Generation storage must be isolated from publication controls")
    report = _json_object(candidate_by_field["validation_json"])
    if type(report) is not dict or type(report.get("address_source")) is not dict:
        raise NetworkPublicationError("Ready generation lacks named address scope")
    relations = await connection.fetch(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relkind IN ('r','p') ORDER BY c.relname",
        storage_schema,
    )
    names = [relation["relname"] for relation in relations]
    if not {"network_membership", "provider_location_binding"} <= set(names):
        raise NetworkPublicationError("Ready generation storage is incomplete")
    composition = await _named_address_composition(connection, candidate_by_field, report, schema)
    tables = [{"schema": storage_schema, "name": name} for name in names]
    snapshot = _network_preparation_snapshot(candidate_by_field, report)
    if composition is not None:
        composition_schema = composition["address_source"]["schema_name"]
        tables.extend(
            {"schema": composition_schema, "name": name}
            for name in ("entity_address_unified", "provider_location_binding")
        )
        snapshot["address_composition"] = composition
    return {
        "kind": "network",
        "candidate_id": str(identity),
        "catalog_schema": schema,
        "storage_schema": storage_schema,
        "tables": tables,
        "snapshot": snapshot,
    }


async def _named_address_composition(connection, candidate_by_field, report_by_field, control_schema):
    """Include the exact named address artifact in the generation's administrative freeze."""
    from process.network_custom_address_source import _from_document

    address = PinnedAddressSource(**report_by_field["address_source"])
    if not address.schema_name.startswith("network_composition_"):
        return None
    try:
        composition = _from_document(
            await connection.fetchval(
                "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
                address.schema_name,
            )
        )
    except ValueError, TypeError, KeyError:
        raise NetworkPublicationError("Address composition scope differs from the candidate") from None
    if (
        composition.address_source != address
        or composition.control_schema != control_schema
        or composition.approved_revision > candidate_by_field["approved_custom_revision"]
        or composition.generation_sha256
        != _json_object(candidate_by_field["source_generations"]).get("unified_address")
    ):
        raise NetworkPublicationError("Address composition scope differs from the candidate")
    relations = await connection.fetch(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relkind IN ('r','p') ORDER BY c.relname",
        address.schema_name,
    )
    if [relation["relname"] for relation in relations] != ["entity_address_unified", "provider_location_binding"]:
        raise NetworkPublicationError("Address composition tables are incomplete")
    return composition.as_dict()


def _network_preparation_snapshot(candidate, report):
    address = PinnedAddressSource(**report["address_source"])
    return {
        **{field: str(candidate[field]) for field in ("dataset_id", "schema_id", "producer_id")},
        **{
            field: candidate[field]
            for field in (
                "schema_revision",
                "expected_head",
                "approved_custom_revision",
                "expected_rows",
                "accepted_rows",
            )
        },
        "source_generations": _json_object(candidate["source_generations"]),
        "address_source": {
            "schema_name": address.schema_name,
            "table_name": address.table_name,
            "generation_id": address.generation_id,
        },
        "accounting": {
            field: report[field]
            for field in (
                "raw_rows",
                "batch_rows",
                "batch_count",
                "membership_rows",
                "distinct_memberships",
                "projected_locations",
                "orphan_bindings",
                "unknown_network_rows",
            )
        },
    }


class NetworkPreparationPending(RuntimeError):
    """A durable named candidate awaits its existing administrator permission step."""

    def __init__(self, candidate_id):
        self.candidate_id = str(UUID(str(candidate_id)))
        super().__init__("network_preparation_waiting_admin_freeze")


async def admit_network_preparation(connection, candidate_id, *, control_schema, roles=None):
    """Admit or resume the existing common lifecycle for this exact named candidate."""
    from directory_catalog_runtime import NamedPreparationStore, NativePreparationAdmission

    handoff = await load_named_network_preparation(connection, candidate_id, control_schema=control_schema)
    if handoff is None:
        raise NetworkPublicationError("Ready network preparation is required")
    identity = handoff["candidate_id"]
    request = NativePreparationAdmission(
        preparation_id=identity,
        node_id=control_schema,
        importer_id="network",
        dataset_key=identity,
        run_id=identity,
        generation_id=identity,
        publication_fence=identity,
        idempotency_key="network:" + identity,
        request_sha256=hashlib.sha256(
            json.dumps(handoff, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest(),
    )
    store = NamedPreparationStore(connection, named_network_preparation_roles(roles), kind="network")
    row, claim = await store.admit_named(request, handoff, attempt_id=identity)
    return store, row, claim


def _network_validation_reference(validation):
    """Keep the common lifecycle report bounded while the full data result remains on the candidate."""
    encoded = json.dumps(validation, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return {
        "candidate_id": validation["candidate_id"],
        "schema_name": validation["schema_name"],
        "validation_sha256": hashlib.sha256(encoded.encode()).hexdigest(),
    }


async def validate_named_network_preparation(
    connection, copy_target, *, control_schema, roles, source_witness_runtime=None, validation_snapshot=None
):
    """Validate frozen payloads without holding publication controls or a preparation claim."""
    from process.network_membership_validation import validate_network_candidate_final

    if connection.is_in_transaction():
        raise NetworkPublicationError("Network preparation requires phase-owned transactions")
    async with connection.transaction():
        store, preparation_by_field, claim = await admit_network_preparation(
            connection, copy_target.candidate_id, control_schema=control_schema, roles=roles
        )
        if claim is not None:
            await store.yield_named_claim(claim)
    if preparation_by_field["state"] == "handed_off":
        raise NetworkPreparationPending(copy_target.candidate_id)
    if preparation_by_field["state"] == "published":
        return {"state": "published", "candidate_id": copy_target.candidate_id}
    if preparation_by_field["state"] == "validated":
        candidate_by_field = dict(
            await connection.fetchrow(
                f"SELECT * FROM {_identifier(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
                UUID(copy_target.candidate_id),
            )
        )
        if preparation_by_field["validation"]["evidence"]["data"] != _network_validation_reference(
            _require_final_validation(candidate_by_field)
        ):
            raise NetworkPublicationError("Stored network data validation changed")
        return {"state": "validated", "candidate_id": copy_target.candidate_id}
    async with AsyncExitStack() as validation_scope:
        validation_connection, office_authority = connection, None
        if validation_snapshot is None:
            await validation_scope.enter_async_context(connection.transaction(isolation="repeatable_read"))
        else:
            validation_connection, office_authority = await validation_scope.enter_async_context(validation_snapshot())
        candidate_by_field = dict(
            await validation_connection.fetchrow(
                f"SELECT * FROM {_identifier(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
                UUID(copy_target.candidate_id),
            )
        )
        validation = await validate_network_candidate_final(
            validation_connection,
            copy_target,
            candidate_by_field,
            control_schema,
            office_authority=office_authority,
            source_witness_runtime=source_witness_runtime,
        )
    return await _retain_network_final_validation(connection, copy_target, validation, control_schema, roles)


async def _retain_network_final_validation(connection, copy_target, validation, control_schema, roles):
    """Record the data result under a fresh short claim after the complete scan."""
    async with connection.transaction():
        store, preparation_by_field, claim = await admit_network_preparation(
            connection, copy_target.candidate_id, control_schema=control_schema, roles=roles
        )
        if claim is None or preparation_by_field["state"] not in {"frozen", "validated"}:
            raise NetworkPublicationError("Frozen network preparation is required")
        await store.validate_named(
            claim,
            {
                "kind": "network",
                "candidate_id": copy_target.candidate_id,
                "data": _network_validation_reference(validation),
            },
        )
        status = await connection.execute(
            f"UPDATE {_identifier(control_schema)}.network_membership_candidate "
            "SET validation_json=jsonb_set(validation_json,ARRAY['final_validation'],$2::jsonb) "
            "WHERE candidate_id=$1 AND state='ready'",
            UUID(copy_target.candidate_id),
            json.dumps(validation, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        )
        if status != "UPDATE 1":
            raise NetworkPublicationError("Ready network preparation changed")
        await store.yield_named_claim(claim)
    return {"state": "validated", "candidate_id": copy_target.candidate_id}


async def lock_publication_controls(connection, namespace):
    """Lock revision and serving singletons in publication order and return their current generations."""
    revision_control = await connection.fetchrow(
        f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE NOWAIT"
    )
    serving_control = await connection.fetchrow(
        f"SELECT generation_id FROM {namespace}.network_serving_control WHERE id=1 FOR UPDATE NOWAIT"
    )
    if revision_control is None or serving_control is None:
        raise NetworkPublicationError("Publication singleton controls are missing")
    return revision_control["approved_revision"], serving_control["generation_id"] or 0


_lock_publication_controls = lock_publication_controls


async def _insert_manifest(connection, namespace, candidate, readiness, preparation_roles=None):
    store, preparation_by_field, claim = await admit_network_preparation(
        connection, str(candidate["candidate_id"]), control_schema=namespace[1:-1], roles=preparation_roles
    )
    if claim is None or preparation_by_field["state"] != "validated":
        raise NetworkPublicationError("Validated network preparation is required")
    if preparation_by_field["validation"]["evidence"]["data"] != _network_validation_reference(
        _require_final_validation(candidate)
    ):
        raise NetworkPublicationError("Stored network data validation changed")
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
    manifest = _retained_manifest(manifest, readiness, replayed=False)
    await store.publish_named(
        claim,
        {
            "kind": "network",
            "candidate_id": str(candidate["candidate_id"]),
            "fence_token": str(preparation_by_field["publication_fence"]),
            "data": manifest,
        },
    )
    return manifest


def _require_final_validation(candidate):
    """Require the completed data result for this exact ready logical generation."""
    report = _json_object(candidate["validation_json"])
    validation = report.get("final_validation") if type(report) is dict else None
    if (
        type(validation) is not dict
        or validation.get("candidate_id") != str(candidate["candidate_id"])
        or validation.get("schema_name") != candidate["schema_name"]
        or validation.get("complete") is not True
        or validation.get("source_generations") != _json_object(candidate["source_generations"])
    ):
        raise NetworkPublicationError("Generation data validation is incomplete")
    fields = (
        "raw_rows",
        "batch_rows",
        "batch_count",
        "membership_rows",
        "distinct_memberships",
        "projected_locations",
        "orphan_bindings",
        "unknown_network_rows",
    )
    if validation.get("counts") != {field: report[field] for field in fields}:
        raise NetworkPublicationError("Generation validation accounting changed")
    return validation


async def _publish_candidate(
    connection, copy_target, namespace, source_rollback_authority=None, preparation_roles=None
):
    """Switch the verified named generation and its control record atomically."""
    approved_revision, current_head = await _lock_publication_controls(connection, namespace)
    candidate = await _locked_candidate(connection, copy_target, namespace)
    if candidate["state"] not in {"ready", "published"}:
        raise NetworkPublicationError("Candidate must be fully ready before publication")
    readiness = _publication_readiness(candidate)
    manifest = await connection.fetchrow(
        f"SELECT * FROM {namespace}.network_serving_manifest WHERE candidate_id=$1", UUID(copy_target.candidate_id)
    )
    if candidate["state"] == "published":
        if manifest is None:
            raise NetworkPublicationError("Published candidate has no retained manifest")
        return _retained_manifest(manifest, readiness, replayed=True)
    if manifest is not None:
        raise NetworkPublicationError("Unpublished candidate already has a retained manifest")
    _require_final_validation(candidate)
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
    await fhir_preservation_obligation(
        connection, candidate, control_schema=namespace[1:-1], reviewed_source_manifest=reviewed_source
    )
    return await _insert_manifest(connection, namespace, candidate, readiness, preparation_roles)


async def publish_network_candidate(
    connection, copy_target, *, control_schema=None, source_rollback_authority=None, preparation_roles=None
):
    """Atomically publish the completed named generation or replay its retained manifest.

    The caller owns the transaction and the final client authorization checks.
    """
    if type(copy_target) is not MembershipCopyTarget:
        raise NetworkPublicationError("Trusted candidate scope is required")
    if not connection.is_in_transaction():
        raise NetworkPublicationError("Publication requires a caller-owned transaction")
    try:
        namespace = _identifier(control_schema if control_schema is not None else registry_schema())
        async with connection.transaction():
            return await _publish_candidate(
                connection, copy_target, namespace, source_rollback_authority, preparation_roles
            )
    except (ValueError, TypeError) as error:
        if isinstance(error, NetworkPublicationError):
            raise
        raise NetworkPublicationError(str(error)) from error
