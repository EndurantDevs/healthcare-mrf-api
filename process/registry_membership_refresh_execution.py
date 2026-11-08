# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish already approved memberships through the existing fenced lifecycle."""

from collections.abc import Callable
from dataclasses import dataclass, field
from uuid import UUID

from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_publication import _lock_publication_controls, publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_membership_refresh_authority import RegistryMembershipRefreshAuthorityClient
from process.registry_membership_refresh_contract import membership_refresh_command_sha256
from process.registry_membership_refresh_queue import (
    RegistryMembershipRefreshAddressProfile,
    _current_heads,
    _require_address,
    validated_membership_refresh_request_record,
)
from process.registry_publication_execution import (
    _locked_request_transaction,
    _request,
    prepare_registry_publication_candidate,
)
from process.registry_publication_queue import _document, finish_registry_publication_request
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from process.registry_source_observation_store import _namespace


@dataclass(frozen=True)
class RegistryMembershipRefreshRuntime:
    """Host-owned capabilities; a request cannot select a source reader or credentials."""

    authorize: RegistryMembershipRefreshAuthorityClient = field(repr=False)
    address_profile: RegistryMembershipRefreshAddressProfile
    resolve_sources: Callable = field(repr=False)
    pin_sources: Callable = field(repr=False)

    def __post_init__(self):
        if (
            type(self.authorize) is not RegistryMembershipRefreshAuthorityClient
            or self.authorize.is_enabled is not True
            or type(self.address_profile) is not RegistryMembershipRefreshAddressProfile
            or not callable(self.resolve_sources)
            or not callable(self.pin_sources)
        ):
            raise ValueError("registry_membership_refresh_runtime_invalid")


async def _fresh_inputs(connection, record, runtime, control_schema, authorize):
    envelope = validated_membership_refresh_request_record(record)
    command = envelope["command"]
    await authorize(**envelope)
    current = await _current_heads(connection, command, _namespace(control_schema))
    await _require_address(connection, command, current, runtime.address_profile, control_schema)
    source_pin = None
    if command["expected_serving_generation"]:
        source_pin = await resolve_network_serving_manifest(
            connection, generation_id=command["expected_serving_generation"], control_schema=control_schema
        )
    sources = await runtime.resolve_sources(connection, envelope, source_pin)
    if type(sources) is not RegistryCompositionAddressSources or sources.compact is not True:
        raise ValueError("registry_membership_refresh_sources_invalid")
    if (source_pin is None and sources.replacement_source_recipes) or (
        source_pin is not None and sources.initial_source_recipes
    ):
        raise ValueError("registry_membership_refresh_source_phase_invalid")
    return envelope, await authorize(**envelope), source_pin, sources


async def _candidate_scope(connection, target, command, schema):
    candidate = await connection.fetchrow(
        f"SELECT approved_custom_revision,expected_head,validation_json,source_generations,source_recipes_json "
        f"FROM {_namespace(schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(target.candidate_id),
    )
    if candidate is None or (candidate["approved_custom_revision"], candidate["expected_head"]) != (
        command["expected_approved_revision"],
        command["expected_serving_generation"],
    ):
        raise RegistryApprovalConflict("registry_membership_refresh_candidate_changed")
    generations = _document(candidate["source_generations"])
    if generations.get("network_address_origin") != command["address_artifact_sha256"]:
        raise RegistryApprovalConflict("registry_membership_refresh_address_changed")
    return dict(candidate)


async def complete_queued_membership_refresh(
    connection, request_record, *, runtime, writer_context, authorize, authorize_commit
):
    """Prepare without draft approval; atomically publish and finish the exact lease."""
    if type(runtime) is not RegistryMembershipRefreshRuntime or connection.is_in_transaction():
        raise ValueError("registry_membership_refresh_runtime_invalid")
    schema = writer_context.control_schema
    identity, lease = request_record["request_id"], request_record["lease_id"]
    initial_request = await _request(connection, identity, lease, _namespace(schema))
    async with connection.transaction(isolation="repeatable_read"):
        envelope, _authority, source_pin, address_sources = await _fresh_inputs(
            connection, initial_request, runtime, schema, authorize
        )
    command = envelope["command"]
    compose = (
        compose_registry_membership_candidate
        if writer_context.composition_runtime is None
        else writer_context.composition_runtime.compose
    )
    copy_target, address = await compose(
        connection,
        request_id=identity,
        approved_revision=command["expected_approved_revision"],
        expected_head=command["expected_serving_generation"],
        source_manifest=source_pin,
        address_sources=address_sources,
        writer_roles=writer_context.writer_roles,
        control_schema=schema,
    )
    await prepare_registry_publication_candidate(
        connection,
        copy_target,
        address,
        writer_roles=writer_context.writer_roles,
        control_schema=schema,
        composition_runtime=writer_context.composition_runtime,
    )
    await authorize(**envelope)
    if writer_context.seal_candidate is None:
        raise ValueError("registry_membership_refresh_custodian_required")
    await writer_context.seal_candidate(copy_target)
    async with runtime.pin_sources(connection, envelope, source_pin) as final_sources:
        return await _finish_refresh(
            connection,
            (identity, lease, initial_request),
            (envelope, address_sources, final_sources),
            copy_target,
            writer_context,
            authorize_commit,
        )


async def _finish_refresh(connection, request_scope, inputs, copy_target, writer_context, authorize_commit):
    identity, lease, initial_request = request_scope
    envelope, original_sources, final_sources = inputs
    command = envelope["command"]
    schema = writer_context.control_schema
    async with _locked_request_transaction(connection, identity, lease, _namespace(schema)) as locked:
        if locked["request_sha256"] != initial_request["request_sha256"]:
            raise RegistryApprovalConflict("registry_queue_request_changed")
        await _lock_publication_controls(connection, _namespace(schema))
        await _locked_candidate(connection, copy_target, _namespace(schema))
        current = await _current_heads(connection, command, _namespace(schema))
        await _require_address(
            connection, command, current, writer_context.membership_refresh_runtime.address_profile, schema
        )
        candidate = await _candidate_scope(connection, copy_target, command, schema)
        _require_prepared_recipes(candidate, original_sources, final_sources)
        authority = await authorize_commit(**envelope)
        manifest = await publish_network_candidate(connection, copy_target, control_schema=schema)
        receipt_by_field = _refresh_completion(command, copy_target, manifest, authority, candidate)
        await finish_registry_publication_request(
            connection, identity, lease, state="completed", result=receipt_by_field, control_schema=schema
        )
        return receipt_by_field


def _require_prepared_recipes(candidate, original_sources, final_sources):
    """The fenced inputs are a complete replacement; compare commitments, never hash subsets."""
    from process.registry_membership_refresh_sources import _recipe_commitment

    if (
        type(final_sources) is not RegistryCompositionAddressSources
        or final_sources.compact is not True
        or final_sources.base_source != original_sources.base_source
    ):
        raise RegistryApprovalConflict("registry_membership_refresh_sources_changed")
    expected_count, expected_sha256 = _recipe_commitment(final_sources.introduced_source_recipes)
    if _recipe_commitment(original_sources.introduced_source_recipes) != (expected_count, expected_sha256):
        raise RegistryApprovalConflict("registry_membership_refresh_sources_changed")
    actual = verify_source_recipe_metadata(candidate)
    if (actual.recipe_count, actual.recipes_sha256) != (expected_count, expected_sha256 if expected_count else None):
        raise RegistryApprovalConflict("registry_membership_refresh_candidate_recipes_changed")


def _refresh_completion(command, copy_target, manifest, authority, candidate):
    """Bind bounded completion metadata to the exact immutable refresh command."""
    return {
        "operation": "refresh_memberships",
        "previous_generation_id": command["expected_serving_generation"],
        "generation_id": manifest["generation_id"],
        "candidate_id": copy_target.candidate_id,
        "manifest_sha256": manifest["manifest_sha256"],
        "approved_revision": command["expected_approved_revision"],
        "command_sha256": membership_refresh_command_sha256(command),
        "policy_revision": authority["policy_revision"],
        "orphan_bindings": _document(candidate["validation_json"])["orphan_bindings"],
    }
