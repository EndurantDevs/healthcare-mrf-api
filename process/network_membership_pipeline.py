# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Phase-owned native preparation and atomic publication of a membership candidate."""

import json
import os
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier, project_network_address_arrays
from process.network_membership_candidate_indexes import prepare_network_candidate_indexes
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_publication import (
    NetworkPublicationError,
    _lock_publication_controls,
    _publication_readiness,
    _retained_manifest,
    publish_network_candidate,
)
from process.network_membership_serving_indexes import (
    _index_digest,
    _matching_inputs,
    _serving_plan,
    _source_semantics,
    prepare_network_serving_indexes,
)
from process.network_membership_validation import (
    _scope_report,
    _validated_replay,
    validate_network_membership_candidate,
)
from process.network_membership_writer_closure import (
    _role_names,
    freeze_network_candidate_writers,
    verify_network_candidate_writer_closure,
)
from process.network_serving_read import _pinned_manifest, _read_manifest


class NetworkMembershipPipelineError(ValueError):
    """A phase failed; previously committed closed candidate stages remain resumable."""


async def _inspect_candidate(connection, copy_target, control_schema):
    candidate = await connection.fetchrow(
        f"SELECT * FROM {_identifier(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
    )
    fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    if candidate is None or any(str(candidate[field]) != getattr(copy_target, field) for field in fields):
        raise NetworkMembershipPipelineError("Candidate ownership scope does not match")
    return dict(candidate)


async def _retain_closure(connection, copy_target, control_schema, closure):
    await connection.execute(
        f"UPDATE {_identifier(control_schema)}.network_membership_candidate "
        "SET validation_json=coalesce(validation_json,'{}'::jsonb)||jsonb_build_object('writer_closure',$1::jsonb) "
        "WHERE candidate_id=$2",
        json.dumps(closure, sort_keys=True, separators=(",", ":")),
        UUID(copy_target.candidate_id),
    )


async def _close_raw_phase(connection, copy_target, roles_by_group, control_schema):
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, _identifier(control_schema))
        if candidate["state"] in ("ready", "published"):
            return
        if candidate["state"] not in ("sealed", "validated"):
            raise NetworkMembershipPipelineError("Pipeline requires a sealed or prepared candidate")
        closure = await freeze_network_candidate_writers(
            connection, copy_target, **roles_by_group, control_schema=control_schema
        )
        await _retain_closure(connection, copy_target, control_schema, closure)


async def _prepare_phase(connection, copy_target, source, roles_by_group, control_schema):
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, _identifier(control_schema))
        if candidate["state"] == "published":
            return
        if candidate["state"] == "ready":
            _validated_replay(candidate, _scope_report(candidate, source))
            _publication_readiness(candidate)
            await verify_network_candidate_writer_closure(
                connection, copy_target, **roles_by_group, control_schema=control_schema
            )
            await prepare_network_serving_indexes(connection, copy_target, source, control_schema=control_schema)
            return
        if candidate["state"] not in ("sealed", "validated"):
            raise NetworkMembershipPipelineError("Candidate state changed before preparation")
        if not await connection.fetchval(
            "SELECT pg_has_role(current_user,$1::name,'USAGE')", roles_by_group["owner_role"]
        ):
            raise NetworkMembershipPipelineError("Publisher must inherit protected ownership for native preparation")
        await validate_network_membership_candidate(connection, copy_target, source, control_schema=control_schema)
        await project_network_address_arrays(connection, copy_target, source, control_schema=control_schema)
        await prepare_network_candidate_indexes(connection, copy_target, source, control_schema=control_schema)
        await prepare_network_serving_indexes(connection, copy_target, source, control_schema=control_schema)
        final_closure = await freeze_network_candidate_writers(
            connection, copy_target, **roles_by_group, control_schema=control_schema
        )
        await _retain_closure(connection, copy_target, control_schema, final_closure)


async def _publish_phase(connection, copy_target, roles_by_group, control_schema):
    async with connection.transaction():
        namespace = _identifier(control_schema)
        approved_revision, current_head = await _lock_publication_controls(connection, namespace)
        candidate = await _locked_candidate(connection, copy_target, namespace)
        if candidate["state"] == "published":
            return None
        if candidate["state"] != "ready":
            raise NetworkMembershipPipelineError("Candidate is not fully ready for publication")
        if approved_revision != candidate["approved_custom_revision"]:
            raise NetworkPublicationError("Approved custom revision changed before publication")
        if current_head != candidate["expected_head"]:
            raise NetworkPublicationError("Expected serving head changed before publication")
        closure = await freeze_network_candidate_writers(
            connection, copy_target, **roles_by_group, control_schema=control_schema
        )
        await _retain_closure(connection, copy_target, control_schema, closure)
        return await publish_network_candidate(connection, copy_target, control_schema=control_schema)


async def _published_replay(connection, copy_target, address_source, roles_by_group, control_schema):
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        candidate = await _inspect_candidate(connection, copy_target, control_schema)
        if candidate["state"] != "published":
            raise NetworkMembershipPipelineError("Candidate is not published for retained replay")
        _validated_replay(candidate, _scope_report(candidate, address_source))
        readiness = _publication_readiness(candidate)
        configured_roles = _role_names(**roles_by_group)
        if any(readiness[3][field] != expected for field, expected in configured_roles.items()):
            raise NetworkMembershipPipelineError("Configured writer roles differ from retained closure")
        manifest = await connection.fetchrow(
            f"SELECT * FROM {_identifier(control_schema)}.network_serving_manifest WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
        )
        if manifest is None:
            raise NetworkMembershipPipelineError("Published candidate lacks a retained manifest")
        entry = await _read_manifest(
            connection, _identifier(control_schema), manifest["generation_id"], include_ineligible=True
        )
        if entry is None:
            raise NetworkMembershipPipelineError("Published candidate is not physically retained")
        _pinned_manifest(entry)
        await _matching_inputs(connection, candidate, copy_target, address_source, control_schema)
        await _source_semantics(
            connection,
            f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}",
            f"{_identifier(copy_target.schema_name)}.entity_address_unified",
            replay=True,
        )
        index_digest = await _index_digest(connection, copy_target.schema_name, _serving_plan(copy_target.schema_name))
        if index_digest != readiness[2]["index_definition_sha256"]:
            raise NetworkMembershipPipelineError("Retained native serving index definitions changed")
        return _retained_manifest(manifest, readiness, replayed=True)


async def prepare_network_candidate(
    connection,
    copy_target,
    address_source,
    *,
    owner_role,
    loader_roles,
    reader_roles,
    control_schema=None,
):
    """Close writers and prepare an isolated candidate before final authorization.

    The connection must be outside a caller transaction. Preparation failures keep
    a closed sealed candidate; final CAS failures keep a closed ready candidate.
    Published candidates remain untouched; callers verify retained publication.
    """
    if type(copy_target) is not MembershipCopyTarget or type(address_source) is not PinnedAddressSource:
        raise NetworkMembershipPipelineError("Trusted candidate and pinned source are required")
    if connection.is_in_transaction():
        raise NetworkMembershipPipelineError("Phase-owned pipeline requires a connection outside a transaction")
    if address_source.schema_name == copy_target.schema_name:
        raise NetworkMembershipPipelineError("Pinned source must be outside the candidate")
    schema_name = control_schema if control_schema is not None else registry_schema()
    roles_by_group = {"owner_role": owner_role, "loader_roles": loader_roles, "reader_roles": reader_roles}
    _role_names(**roles_by_group)
    candidate = await _inspect_candidate(connection, copy_target, schema_name)
    if candidate["state"] == "published":
        return candidate
    await _close_raw_phase(connection, copy_target, roles_by_group, schema_name)
    await _prepare_phase(connection, copy_target, address_source, roles_by_group, schema_name)
    return await _inspect_candidate(connection, copy_target, schema_name)


async def prepare_and_publish_network_candidate(
    connection, copy_target, source, *, owner_role, loader_roles, reader_roles, control_schema=None
):
    """Commit isolated preparation, then publish with the existing short atomic CAS."""
    roles_by_group = {"owner_role": owner_role, "loader_roles": loader_roles, "reader_roles": reader_roles}
    schema_name = control_schema if control_schema is not None else registry_schema()
    candidate = await prepare_network_candidate(
        connection, copy_target, source, **roles_by_group, control_schema=schema_name
    )
    if candidate["state"] == "published":
        return await _published_replay(connection, copy_target, source, roles_by_group, schema_name)
    manifest = await _publish_phase(connection, copy_target, roles_by_group, schema_name)
    return (
        manifest
        if manifest is not None
        else await _published_replay(connection, copy_target, source, roles_by_group, schema_name)
    )
