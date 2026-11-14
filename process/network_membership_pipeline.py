# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Phase-owned native preparation and atomic publication of a membership candidate."""

import json
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier, project_network_address_arrays
from process.network_membership_candidate_indexes import prepare_network_candidate_indexes
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_publication import (
    _publication_readiness,
    _retained_manifest,
    publish_network_candidate,
)
from process.network_membership_serving_indexes import prepare_network_serving_indexes
from process.network_membership_validation import (
    _scope_report,
    _validated_replay,
    validate_network_membership_candidate,
)
from process.network_membership_writer_closure import _role_names
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


async def _prepare_phase(
    connection,
    copy_target,
    address_source,
    roles_by_group,
    control_schema,
    *,
    compact=False,
    source_witness_runtime=None,
):
    async with connection.transaction(isolation="repeatable_read"):
        await _prepare_phase_snapshot(
            connection,
            copy_target,
            address_source,
            roles_by_group,
            control_schema,
            compact=compact,
            **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
        )


async def _prepare_phase_snapshot(
    connection,
    copy_target,
    address_source,
    roles_by_group,
    control_schema,
    *,
    compact=False,
    **source_options,
):
    """Prepare using the caller's genuine fenced snapshot when source authority requires it."""
    if set(source_options) - {"source_witness_runtime", "office_authority"}:
        raise ValueError("registry_source_runtime_options_invalid")
    office_authority = source_options.pop("office_authority", None)
    proof_options_by_name = dict(control_schema=control_schema, office_authority=office_authority, **source_options)
    candidate = await _locked_candidate(connection, copy_target, _identifier(control_schema))
    if candidate["state"] == "published":
        return
    if candidate["state"] == "ready":
        _validated_replay(candidate, _scope_report(candidate, address_source))
        _publication_readiness(candidate)
        return
    if candidate["state"] not in ("sealed", "validated"):
        raise NetworkMembershipPipelineError("Candidate state changed before preparation")
    if compact:
        from process.network_compact_preparation import prepare_compact_network_readiness

        runtime_roles = tuple(sorted(set(roles_by_group["loader_roles"] + roles_by_group["reader_roles"])))
        await prepare_compact_network_readiness(
            connection, copy_target, address_source, runtime_roles=runtime_roles, **proof_options_by_name
        )
    else:
        await validate_network_membership_candidate(
            connection, copy_target, address_source, control_schema=control_schema
        )
        await project_network_address_arrays(connection, copy_target, address_source, control_schema=control_schema)
        await prepare_network_candidate_indexes(connection, copy_target, address_source, control_schema=control_schema)
        await prepare_network_serving_indexes(connection, copy_target, address_source, control_schema=control_schema)


async def _publish_phase(connection, copy_target, control_schema):
    async with connection.transaction(isolation="repeatable_read"):
        return await publish_network_candidate(connection, copy_target, control_schema=control_schema)


async def _published_replay(connection, copy_target, address_source, control_schema):
    """Read the retained logical publication in the caller's pinned snapshot."""
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        candidate = await _inspect_candidate(connection, copy_target, control_schema)
        if candidate["state"] != "published":
            raise NetworkMembershipPipelineError("Candidate is not published for retained replay")
        _validated_replay(candidate, _scope_report(candidate, address_source))
        readiness = _publication_readiness(candidate)
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
            raise NetworkMembershipPipelineError("Published candidate is unavailable")
        _pinned_manifest(entry)
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
    **source_options,
):
    """Build an isolated sealed candidate for the administrative permission handoff.

    Completed phases remain available for the same candidate's normal resume.
    Final data validation follows the administrator's generation freeze.
    """
    if set(source_options) - {"source_witness_runtime", "compact"}:
        raise ValueError("registry_source_runtime_options_invalid")
    compact = source_options.pop("compact", None)
    if type(copy_target) is not MembershipCopyTarget or type(address_source) is not PinnedAddressSource:
        raise NetworkMembershipPipelineError("Trusted candidate and pinned source are required")
    if compact is not None and type(compact) is not bool:
        raise NetworkMembershipPipelineError("Serving storage selection is invalid")
    if connection.is_in_transaction():
        raise NetworkMembershipPipelineError("Phase-owned pipeline requires a connection outside a transaction")
    if address_source.schema_name == copy_target.schema_name:
        raise NetworkMembershipPipelineError("Pinned source must be outside the candidate")
    schema_name = control_schema if control_schema is not None else registry_schema()
    roles_by_group = {"owner_role": owner_role, "loader_roles": loader_roles, "reader_roles": reader_roles}
    _role_names(**roles_by_group)
    candidate = await _inspect_candidate(connection, copy_target, schema_name)
    compact = _compact_selection(candidate, compact)
    if candidate["state"] == "published":
        return candidate
    await _prepare_phase(
        connection, copy_target, address_source, roles_by_group, schema_name, compact=compact, **source_options
    )
    return await _inspect_candidate(connection, copy_target, schema_name)


def _compact_selection(candidate, requested):
    from process.registry_compact_composition import COMPACT_FORMAT, COMPACT_FORMAT_KEY

    generations = candidate["source_generations"]
    generations = json.loads(generations) if type(generations) is str else generations
    if COMPACT_FORMAT_KEY in generations and generations[COMPACT_FORMAT_KEY] != COMPACT_FORMAT:
        raise NetworkMembershipPipelineError("Registered serving format is unsupported")
    has_registered_compact = generations.get(COMPACT_FORMAT_KEY) == COMPACT_FORMAT
    if requested is False and has_registered_compact:
        raise NetworkMembershipPipelineError("Registered compact serving cannot use legacy storage")
    return has_registered_compact if requested is None else requested


async def prepare_and_publish_network_candidate(
    connection,
    copy_target,
    source,
    *,
    owner_role,
    loader_roles,
    reader_roles,
    control_schema=None,
    seal_candidate=None,
):
    """Resume the existing preparation callback and publish after its completed handoff."""
    if seal_candidate is not None and not callable(seal_candidate):
        raise NetworkMembershipPipelineError("Candidate seal callback must be callable")
    roles_by_group = {"owner_role": owner_role, "loader_roles": loader_roles, "reader_roles": reader_roles}
    schema_name = control_schema if control_schema is not None else registry_schema()
    candidate = await prepare_network_candidate(
        connection, copy_target, source, **roles_by_group, control_schema=schema_name
    )
    if candidate["state"] == "published":
        return await _published_replay(connection, copy_target, source, schema_name)
    if seal_candidate is not None:
        prepared = await seal_candidate(copy_target)
        if isinstance(prepared, dict) and prepared.get("state") == "waiting_admin_freeze":
            return prepared
    manifest = await _publish_phase(connection, copy_target, schema_name)
    return manifest if manifest is not None else await _published_replay(connection, copy_target, source, schema_name)
