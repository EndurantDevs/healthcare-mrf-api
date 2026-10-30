# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Queue exact approved membership refreshes without approving any draft records."""

import json
from dataclasses import dataclass
from uuid import UUID, uuid4

from process import network_address_artifact as address
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_membership_refresh_authority import RegistryMembershipRefreshAuthorityClient
from process.registry_membership_refresh_contract import (
    RegistryMembershipRefreshUnavailable,
    frozen_membership_refresh_envelope,
    membership_refresh_command_sha256,
    validated_membership_refresh_command,
    validated_refresh_actor,
)
from process.registry_publication_queue import _existing_request, _queued_receipt, _request_metadata
from process.registry_refresh_anchor_contract import _hash
from process.registry_source_observation_store import _namespace


@dataclass(frozen=True)
class RegistryMembershipRefreshAddressProfile:
    """A host-installed source schema and exact ordinary native read subjects."""

    schema_name: str
    runtime_roles: tuple[str, ...]

    def __post_init__(self):
        _identifier(self.schema_name)
        address._roles(self.runtime_roles)


async def _has_retained_address(connection, command_by_field, current, control_schema):
    if current["address_origin"] != command_by_field["address_artifact_sha256"]:
        return False
    pinned = await resolve_network_serving_manifest(
        connection, generation_id=current["generation_id"], control_schema=control_schema
    )
    if (
        pinned.compact_layout is None
        or pinned.generation_id != current["generation_id"]
        or pinned.manifest_sha256 != command_by_field["expected_serving_manifest_sha256"]
        or pinned.source_generations.get("network_address_origin") != command_by_field["address_artifact_sha256"]
    ):
        raise RegistryMembershipRefreshUnavailable("registry_membership_refresh_retained_address_unproved")
    _hash(getattr(pinned, "_custody_seal_sha256", None))
    return True


async def _require_address(connection, command_by_field, current, profile, control_schema):
    try:
        if await _has_retained_address(connection, command_by_field, current, control_schema):
            return
        authority = await address.generation.read_entity_address_result_generation_authority(
            address._CatalogSession(connection), schema_name=profile.schema_name
        )
        generation = address._digest(address._authority_document(authority))
        source = PinnedAddressSource(profile.schema_name, "entity_address_unified", generation)
        descriptor = await address.capture_network_address_artifact(
            connection, source, runtime_roles=profile.runtime_roles
        )
    except Exception as error:
        raise RegistryMembershipRefreshUnavailable("registry_membership_refresh_address_unavailable") from error
    if descriptor["descriptor_sha256"] != command_by_field["address_artifact_sha256"]:
        raise RegistryApprovalConflict("registry_membership_refresh_address_changed")


async def _current_heads(connection, command_by_field, namespace):
    current = await connection.fetchrow(
        f"SELECT revisions.approved_revision,coalesce(control.generation_id,0) AS generation_id,manifest.manifest_sha256,"
        "manifest.eligible,manifest.source_generations->>'network_address_origin' AS address_origin "
        f"FROM {namespace}.registry_revision_control revisions CROSS JOIN {namespace}.network_serving_control control "
        f"LEFT JOIN {namespace}.network_serving_manifest manifest ON manifest.generation_id=control.generation_id "
        "WHERE revisions.id=1 AND control.id=1"
    )
    expected = (
        command_by_field["expected_approved_revision"],
        command_by_field["expected_serving_generation"],
        command_by_field["expected_serving_manifest_sha256"],
    )
    if (
        current is None
        or tuple(current[name] for name in ("approved_revision", "generation_id", "manifest_sha256")) != expected
        or (current["generation_id"] and current["eligible"] is not True)
    ):
        raise RegistryApprovalConflict("registry_membership_refresh_head_changed")
    return current


async def enqueue_registry_membership_refresh(
    connection, envelope_by_field, *, authorize, address_profile, control_schema=None
):
    """Recheck live App authority for every enqueue/replay and native address identity."""
    envelope_by_field = frozen_membership_refresh_envelope(envelope_by_field)
    if not connection.is_in_transaction():
        raise ValueError("registry_queue_requires_caller_transaction")
    if (
        type(authorize) is not RegistryMembershipRefreshAuthorityClient
        or authorize.is_enabled is not True
        or type(address_profile) is not RegistryMembershipRefreshAddressProfile
    ):
        raise RegistryMembershipRefreshUnavailable("registry_membership_refresh_unconfigured")
    if await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}:
        raise ValueError("registry_membership_refresh_snapshot_required")
    await authorize(actor=envelope_by_field["actor"], command=envelope_by_field["command"])
    command_by_field = envelope_by_field["command"]
    key = command_by_field["idempotency_key"]
    stored_by_field = {name: field for name, field in command_by_field.items() if name != "idempotency_key"}
    metadata = _request_metadata(stored_by_field, envelope_by_field["actor"], None, key)
    namespace = _namespace(control_schema)
    async with connection.transaction():
        replay = await _existing_request(connection, namespace, metadata, key)
        if replay is not None:
            return replay
        current = await _current_heads(connection, command_by_field, namespace)
        await _require_address(connection, command_by_field, current, address_profile, control_schema)
        inserted = await connection.fetchrow(
            f"INSERT INTO {namespace}.registry_publication_request "
            "(request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256) "
            "VALUES($1,$2,NULL,$3::jsonb,$4::jsonb,$5,$6) ON CONFLICT(actor_key,idempotency_key) DO NOTHING RETURNING *",
            uuid4(),
            metadata["actor_key"],
            metadata["actor_json"],
            metadata["command_json"],
            key,
            metadata["request_sha256"],
        )
        if inserted is None:
            replay = await _existing_request(connection, namespace, metadata, key)
            if replay is None:
                raise RegistryApprovalConflict("registry_queue_idempotency_conflict")
            return replay
        return _queued_receipt(inserted, False)


def validated_membership_refresh_request_record(request_record):
    """Rebuild a sealed request identity without using a stored authority receipt."""
    command_by_field = request_record["command_json"]
    actor_by_field = request_record["actor_json"]
    if isinstance(command_by_field, str):
        command_by_field = json.loads(command_by_field)
    if isinstance(actor_by_field, str):
        actor_by_field = json.loads(actor_by_field)
    if (
        type(command_by_field) is not dict
        or "idempotency_key" in command_by_field
        or request_record["session_token_sha256"] is not None
    ):
        raise ValueError("registry_membership_refresh_stored_request_invalid")
    actor_by_field = validated_refresh_actor(actor_by_field)
    command = validated_membership_refresh_command(
        {**command_by_field, "idempotency_key": request_record["idempotency_key"]}
    )
    metadata = _request_metadata(command_by_field, actor_by_field, None, request_record["idempotency_key"])
    if any(metadata[name] != request_record[name] for name in ("actor_key", "request_sha256")):
        raise ValueError("registry_membership_refresh_stored_digest_invalid")
    return {"actor": actor_by_field, "command": command}


def validated_membership_refresh_request_status(request_record):
    """Expose only validated operation identity; typed completion proof is separate."""
    envelope_by_field = validated_membership_refresh_request_record(request_record)
    state = request_record["state"]
    result_by_field = request_record["result_json"]
    if isinstance(result_by_field, str):
        result_by_field = json.loads(result_by_field)
    if state in {"queued", "running"}:
        if result_by_field is not None:
            raise ValueError("registry_membership_refresh_result_invalid")
    elif state == "rejected":
        if result_by_field != {"error": {"code": "registry_publication_rejected"}}:
            raise ValueError("registry_membership_refresh_result_invalid")
    elif state == "completed":
        _validated_completion(result_by_field, envelope_by_field["command"])
    else:
        raise ValueError("registry_membership_refresh_state_invalid")
    return envelope_by_field["command"]


def _validated_completion(receipt, command):
    if type(receipt) is not dict or set(receipt) != {
        "operation",
        "previous_generation_id",
        "generation_id",
        "candidate_id",
        "manifest_sha256",
        "approved_revision",
        "command_sha256",
        "policy_revision",
        "orphan_bindings",
    }:
        raise ValueError("registry_membership_refresh_result_invalid")
    try:
        identity = UUID(receipt["candidate_id"])
        if not identity.int or str(identity) != receipt["candidate_id"]:
            raise ValueError
        for field in (
            "previous_generation_id",
            "generation_id",
            "approved_revision",
            "policy_revision",
            "orphan_bindings",
        ):
            if type(receipt[field]) is not int or not 0 <= receipt[field] < 2**63:
                raise ValueError
        _hash(receipt["manifest_sha256"])
        if (
            receipt["operation"] != "refresh_memberships"
            or receipt["previous_generation_id"] != command["expected_serving_generation"]
            or receipt["generation_id"] <= command["expected_serving_generation"]
            or receipt["approved_revision"] != command["expected_approved_revision"]
            or receipt["command_sha256"] != membership_refresh_command_sha256(command)
        ):
            raise ValueError
    except ValueError, TypeError, AttributeError:
        raise ValueError("registry_membership_refresh_result_invalid") from None
