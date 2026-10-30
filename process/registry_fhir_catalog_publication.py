# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Metadata-only operations in the existing durable publication queue."""

import hashlib
import re
from dataclasses import dataclass
from uuid import UUID, uuid4

from process.network_membership_publication import lock_publication_controls
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_fhir_catalog_custody import verify_fhir_catalog_snapshot
from process.registry_fhir_catalog_fingerprints import require_fhir_catalog_membership_reuse
from process.registry_publication_execution import (
    locked_publication_request_transaction,
    publication_request,
)
from process.registry_publication_queue import (
    existing_publication_request,
    finish_registry_publication_request,
    publication_document,
    publication_json,
    publication_request_metadata,
    queued_publication_receipt,
)
from process.registry_source_observation_store import source_observation_namespace

OPERATION = "publish_fhir_catalog"
# The legacy queue column is unused for service requests; it never authenticates a user.
NO_USER_SESSION = "0" * 64
_COMMAND_FIELDS = {
    "operation",
    "metadata_generation",
    "expected_metadata_generation",
    "expected_serving_generation",
    "expected_approved_revision",
    "standing_grant_sha256",
}


@dataclass(frozen=True)
class FHIRCatalogPublicationRuntime:
    authorize_metadata: object
    verify_sources: object
    source_fence: object

    def __post_init__(self):
        if not all(
            callable(callback) for callback in (self.authorize_metadata, self.verify_sources, self.source_fence)
        ):
            raise ValueError("registry_fhir_catalog_runtime_invalid")


def _digest(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("registry_fhir_catalog_command_invalid")
    return value


def validated_fhir_catalog_command(command):
    """Validate the closed metadata publication command and return an independent copy."""
    if type(command) is not dict or set(command) != _COMMAND_FIELDS or command["operation"] != OPERATION:
        raise ValueError("registry_fhir_catalog_command_invalid")
    for field in ("metadata_generation", "standing_grant_sha256"):
        _digest(command[field])
    if command["expected_metadata_generation"] is not None:
        _digest(command["expected_metadata_generation"])
    for field, minimum in (("expected_serving_generation", 1), ("expected_approved_revision", 0)):
        if field == "expected_serving_generation" and command[field] is None:
            continue
        if type(command[field]) is not int or not minimum <= command[field] < 2**63:
            raise ValueError("registry_fhir_catalog_command_invalid")
    return dict(command)


def _metadata(command):
    actor_by_field = {"kind": "catalog_source_service", "standing_grant_sha256": command["standing_grant_sha256"]}
    key = "fhir-catalog:" + command["metadata_generation"]
    return actor_by_field, key, publication_request_metadata(command, actor_by_field, NO_USER_SESSION, key)


async def enqueue_fhir_catalog_publication(connection, command, *, authorize_metadata, control_schema=None):
    """Accept only an authenticated standing metadata grant, never browser authority."""
    if not connection.is_in_transaction():
        raise ValueError("registry_fhir_catalog_transaction_required")
    command = validated_fhir_catalog_command(command)
    actor, key, metadata = _metadata(command)
    namespace = source_observation_namespace(control_schema)
    snapshot = await _snapshot(connection, namespace, command)
    await _authority(authorize_metadata, command, snapshot, connection)
    existing = await existing_publication_request(connection, namespace, metadata, key)
    if existing is not None:
        return existing
    row = await connection.fetchrow(
        f"INSERT INTO {namespace}.registry_publication_request "
        "(request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256) "
        "VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7) "
        "ON CONFLICT(actor_key,idempotency_key) DO NOTHING RETURNING *",
        uuid4(),
        metadata["actor_key"],
        NO_USER_SESSION,
        publication_json(actor),
        metadata["command_json"],
        key,
        metadata["request_sha256"],
    )
    if row is None:
        return await existing_publication_request(connection, namespace, metadata, key)
    return queued_publication_receipt(row, False)


async def _authority(authorize, command, snapshot, connection):
    if not callable(authorize):
        raise ValueError("registry_fhir_catalog_authority_required")
    actual = await authorize(command=dict(command), snapshot=snapshot, connection=connection)
    expected_by_field = {
        "authorized": True,
        "operation": OPERATION,
        "command_sha256": hashlib.sha256(publication_json(command).encode()).hexdigest(),
        "standing_grant_sha256": command["standing_grant_sha256"],
    }
    if type(actual) is not dict or actual.get("authorized") is not True or actual != expected_by_field:
        raise ValueError("registry_fhir_catalog_authority_invalid")


def _validated_request(record):
    command = validated_fhir_catalog_command(publication_document(record["command_json"]))
    actor, key, metadata = _metadata(command)
    if record["session_token_sha256"] != NO_USER_SESSION or record["idempotency_key"] != key:
        raise ValueError("registry_fhir_catalog_request_invalid")
    if publication_document(record["actor_json"]) != actor or any(
        record[field] != metadata[field] for field in ("actor_key", "request_sha256")
    ):
        raise ValueError("registry_fhir_catalog_request_invalid")
    return command


def validated_fhir_catalog_request_status(record):
    """Decode service requests without invoking any human actor/session decoder."""
    command = _validated_request(record)
    state, result = record["state"], publication_document(record["result_json"])
    if state in {"queued", "running"} and result is None:
        return command
    if state == "rejected" and result == {"error": {"code": "registry_publication_rejected"}}:
        return command
    expected = _result(command)
    if state != "completed" or type(result) is not dict or set(result) != set(expected) or result != expected:
        raise ValueError("registry_fhir_catalog_result_invalid")
    if (
        result["memberships_reused"] is not (command["expected_serving_generation"] is not None)
        or result["addresses_reused"] is not (command["expected_serving_generation"] is not None)
        or any(
            type(result[field]) is not int for field in ("canonical_entities_deleted", "canonical_entities_archived")
        )
    ):
        raise ValueError("registry_fhir_catalog_result_invalid")
    return command


def _result(command):
    return {
        "operation": OPERATION,
        "metadata_generation": command["metadata_generation"],
        "generation": str(command["expected_serving_generation"])
        if command["expected_serving_generation"] is not None
        else None,
        "memberships_reused": command["expected_serving_generation"] is not None,
        "addresses_reused": command["expected_serving_generation"] is not None,
        "canonical_entities_deleted": 0,
        "canonical_entities_archived": 0,
    }


async def _snapshot(connection, namespace, command):
    row = await connection.fetchrow(
        f"SELECT * FROM {namespace}.registry_fhir_catalog_snapshot WHERE metadata_generation=$1",
        command["metadata_generation"],
    )
    if row is None or any(
        row[field] != command[expected]
        for field, expected in (
            ("network_generation", "expected_serving_generation"),
            ("metadata_approved_revision", "expected_approved_revision"),
            ("standing_grant_sha256", "standing_grant_sha256"),
        )
    ):
        raise RegistryApprovalConflict("registry_fhir_catalog_snapshot_changed")
    await verify_fhir_catalog_snapshot(connection, row)
    return dict(row)


async def _compare_head(connection, namespace, command, snapshot):
    approved, _ = await lock_publication_controls(connection, namespace)
    head = await connection.fetchrow(
        f"SELECT generation_id,metadata_generation FROM {namespace}.network_serving_control WHERE id=1"
    )
    serving, metadata = head["generation_id"], head["metadata_generation"]
    if (approved, serving, metadata) != (
        command["expected_approved_revision"],
        command["expected_serving_generation"],
        command["expected_metadata_generation"],
    ):
        raise RegistryApprovalConflict("registry_fhir_catalog_source_changed")
    if serving is None:
        return
    manifest = await resolve_network_serving_manifest(connection, generation_id=serving, control_schema=namespace[1:-1])
    if manifest.manifest_sha256 != snapshot["manifest_sha256"]:
        raise RegistryApprovalConflict("registry_fhir_catalog_source_changed")
    await require_fhir_catalog_membership_reuse(connection, manifest, snapshot, control_schema=namespace[1:-1])


async def complete_queued_fhir_catalog_publication(
    connection,
    request_id: UUID,
    lease_id: UUID,
    *,
    authorize_metadata,
    verify_sources,
    source_fence,
    control_schema=None,
):
    """Fence source publishers before opening the final repeatable-read snapshot."""
    if connection.is_in_transaction() or not callable(source_fence):
        raise ValueError("registry_fhir_catalog_source_fence_required")
    namespace = source_observation_namespace(control_schema)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        record = await publication_request(connection, request_id, lease_id, namespace)
        command = _validated_request(record)
        snapshot = await _snapshot(connection, namespace, command)
        await _authority(authorize_metadata, command, snapshot, connection)
    async with source_fence(snapshot=snapshot, connection=connection):
        return await _complete_fenced(
            connection,
            request_id,
            lease_id,
            authorize_metadata=authorize_metadata,
            verify_sources=verify_sources,
            control_schema=control_schema,
        )


async def _complete_fenced(connection, request_id, lease_id, *, authorize_metadata, verify_sources, control_schema):
    """Publish one closed metadata child and finish its lease in the same transaction."""
    namespace = source_observation_namespace(control_schema)
    async with locked_publication_request_transaction(connection, request_id, lease_id, namespace) as record:
        command = _validated_request(record)
        snapshot = await _snapshot(connection, namespace, command)
        await _authority(authorize_metadata, command, snapshot, connection)
        await _compare_head(connection, namespace, command, snapshot)
        if not callable(verify_sources) or await verify_sources(snapshot=snapshot, connection=connection) is not True:
            raise RegistryApprovalConflict("registry_fhir_catalog_source_changed")
        await _authority(authorize_metadata, command, snapshot, connection)
        await connection.execute(
            f"UPDATE {namespace}.registry_fhir_catalog_snapshot "
            "SET published_at=coalesce(published_at,clock_timestamp()) WHERE metadata_generation=$1",
            command["metadata_generation"],
        )
        await connection.execute(
            f"UPDATE {namespace}.network_serving_control SET metadata_generation=$1 WHERE id=1",
            command["metadata_generation"],
        )
        result = _result(command)
        await finish_registry_publication_request(
            connection, request_id, lease_id, state="completed", result=result, control_schema=control_schema
        )
        return result
