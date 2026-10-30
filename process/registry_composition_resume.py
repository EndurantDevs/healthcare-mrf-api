# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin a registered composition contract without authorizing execution or publication."""

from dataclasses import dataclass
from uuid import UUID, uuid5

import asyncpg

from process.network_candidate_custody import MAX_RECEIPT_BYTES
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_contribution_store import verify_registry_source_contributions

_SCOPE_FIELDS = ("dataset_id", "schema_id", "producer_id")
_RESUMABLE_STATES = frozenset(("open", "sealed", "validated", "ready", "published"))
_SIZE_SQL = "+".join(
    f"octet_length(coalesce(c.{field}::text,''))"
    for field in ("source_generations", "source_recipes_json", "validation_json")
)


class RegistryCompositionResumeError(ValueError):
    """A matching registered request is ambiguous, stale or inconsistent."""


@dataclass(frozen=True)
class RegisteredContributionResume:
    """The exact registered target to compare before creating or resuming a candidate."""

    target: MembershipCopyTarget
    contribution_revision: int

    def __post_init__(self):
        if (
            type(self.target) is not MembershipCopyTarget
            or type(self.contribution_revision) is not int
            or (self.contribution_revision not in (1, 2))
        ):
            raise RegistryCompositionResumeError("registry_composition_resume_invalid")


async def resolve_registered_legacy_mapping_marker(
    connection,
    request_id,
    approved_revision,
    expected_head,
    correspondence_sha256,
    *,
    default,
    control_schema=None,
):
    """Preserve both registered legacy marker shapes without changing a candidate UUID."""
    from process.network_membership_validation import _json_object
    from process.registry_source_recipe_composition import OFFICE_MAPPING_KEY

    if type(default) is not bool:
        raise RegistryCompositionResumeError("registry_composition_resume_invalid")
    registered = await _registered_candidate(connection, request_id, approved_revision, expected_head, control_schema)
    if registered is None:
        return default
    generations_by_source = _json_object(registered[1]["source_generations"])
    if OFFICE_MAPPING_KEY not in generations_by_source:
        return False
    if generations_by_source[OFFICE_MAPPING_KEY] != correspondence_sha256:
        raise RegistryCompositionResumeError("registry_composition_resume_mapping_changed")
    return True


def _request_scope(request_id, approved_revision, expected_head):
    if (
        type(request_id) is not UUID
        or not request_id.int
        or any(type(counter) is not int or not 0 <= counter < 2**63 for counter in (approved_revision, expected_head))
    ):
        raise RegistryCompositionResumeError("registry_composition_resume_scope_invalid")
    return tuple(uuid5(request_id, label) for label in ("dataset", "schema", "producer"))


def _registered_target(candidate, scope, approved_revision, expected_head):
    if candidate["bounded"] is not True:
        raise RegistryCompositionResumeError("registry_composition_resume_metadata_unbounded")
    if any(str(candidate[field]) != str(expected) for field, expected in zip(_SCOPE_FIELDS, scope, strict=True)):
        raise RegistryCompositionResumeError("registry_composition_resume_request_scope_changed")
    if candidate["approved_custom_revision"] != approved_revision or candidate["expected_head"] != expected_head:
        raise RegistryCompositionResumeError("registry_composition_resume_approval_scope_changed")
    if candidate["state"] not in _RESUMABLE_STATES:
        raise RegistryCompositionResumeError("registry_composition_resume_state_invalid")
    return MembershipCopyTarget(*(str(candidate[field]) for field in (*_SCOPE_FIELDS, "candidate_id", "schema_name")))


def _lookup_sql(namespace):
    bounded_fields = ",".join(
        f"CASE WHEN ({_SIZE_SQL})<=$4 THEN c.{field}::text END AS {field}"
        for field in ("source_generations", "source_recipes_json", "validation_json")
    )
    return f"""SELECT c.candidate_id,c.dataset_id,c.schema_id,c.producer_id,c.schema_name,c.state,
      c.approved_custom_revision,c.expected_head,c.expected_rows,({_SIZE_SQL})<=$4 AS bounded,{bounded_fields}
      FROM {namespace}.network_membership_candidate c
      WHERE c.dataset_id=$1 OR c.schema_id=$2 OR c.producer_id=$3 ORDER BY c.candidate_id LIMIT 2"""


async def _registered_candidate(connection, request_id, approved_revision, expected_head, control_schema):
    scope = _request_scope(request_id, approved_revision, expected_head)
    if not connection.is_in_transaction() or await connection.fetchval("SHOW transaction_isolation") not in {
        "repeatable read",
        "serializable",
    }:
        raise RegistryCompositionResumeError("registry_composition_resume_transaction_required")
    try:
        candidates = await connection.fetch(_lookup_sql(_control_namespace(control_schema)), *scope, MAX_RECEIPT_BYTES)
        if not candidates:
            return None
        if len(candidates) != 1:
            raise RegistryCompositionResumeError("registry_composition_resume_ambiguous")
        return _registered_target(candidates[0], scope, approved_revision, expected_head), dict(candidates[0])
    except RegistryCompositionResumeError:
        raise
    except asyncpg.PostgresError, ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryCompositionResumeError("registry_composition_resume_unavailable") from None


async def resolve_registered_contribution_resume(
    connection, request_id, approved_revision, expected_head, *, control_schema=None
):
    """Return the exact registered fact contract; no match alone permits default-v2 staging.

    This selector grants no lease, custody or permission to resume a terminal run.
    The caller must compare its recomputed target before creating any candidate.
    """
    registered = await _registered_candidate(connection, request_id, approved_revision, expected_head, control_schema)
    if registered is None:
        return None
    copy_target, candidate = registered
    try:
        receipt = verify_registry_source_contributions(candidate)
        if receipt is None:
            from process.registry_verified_office_policy import select_legacy_verified_office_inputs

            if await select_legacy_verified_office_inputs(connection, candidate, control_schema=control_schema) is None:
                raise RegistryCompositionResumeError("registry_composition_resume_unversioned")
            return RegisteredContributionResume(copy_target, 1)
        return RegisteredContributionResume(copy_target, 2 if receipt["revision"] in (3, 4) else receipt["revision"])
    except RegistryCompositionResumeError:
        raise
    except asyncpg.PostgresError, ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryCompositionResumeError("registry_composition_resume_unavailable") from None


async def require_registered_composition_target(
    connection, request_id, approved_revision, expected_head, copy_target, *, control_schema=None
):
    """Refuse a sibling UUID for a registered request, including manual-only contracts.

    No match performs no writes. Exact old unversioned/manual identities remain
    valid here; fact-version selection is the separate narrower operation above.
    This is an identity guard, never execution or publication authorization.
    """
    scope = _request_scope(request_id, approved_revision, expected_head)
    if type(copy_target) is not MembershipCopyTarget or any(
        getattr(copy_target, field) != str(expected) for field, expected in zip(_SCOPE_FIELDS, scope, strict=True)
    ):
        raise RegistryCompositionResumeError("registry_composition_resume_computed_scope_invalid")
    registered = await _registered_candidate(connection, request_id, approved_revision, expected_head, control_schema)
    if registered is not None and registered[0] != copy_target:
        raise RegistryCompositionResumeError("registry_composition_resume_target_changed")


async def resolve_registered_office_summary_revision(
    connection, request_id, approved_revision, expected_head, *, control_schema=None
):
    """Freeze the registered office contract before a new default can change target identity."""
    registered = await _registered_candidate(connection, request_id, approved_revision, expected_head, control_schema)
    if registered is None:
        return None
    from process.registry_initial_source_composition import verify_initial_source_office_receipt

    receipt = verify_initial_source_office_receipt(registered[1])
    return 1 if receipt is None else receipt["revision"]


async def resolve_registered_source_inventory(
    connection, request_id, approved_revision, expected_head, *, control_schema=None
):
    """Freeze the registered payload format, including empty and manual-only candidates."""
    registered = await _registered_candidate(connection, request_id, approved_revision, expected_head, control_schema)
    if registered is None:
        return None
    from process.registry_source_inventory_reference import verify_source_recipe_metadata

    try:
        return verify_source_recipe_metadata(registered[1]).inventory is not None
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryCompositionResumeError("registry_composition_resume_unavailable") from None


async def resolve_registered_legacy_office_inputs(
    connection, request_id, approved_revision, expected_head, *, control_schema=None
):
    """Return metadata only for the exact registered legacy verified-office policy."""
    registered = await _registered_candidate(connection, request_id, approved_revision, expected_head, control_schema)
    if registered is None:
        return None
    from process.registry_verified_office_policy import select_legacy_verified_office_inputs

    try:
        return await select_legacy_verified_office_inputs(connection, registered[1], control_schema=control_schema)
    except asyncpg.PostgresError, ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryCompositionResumeError("registry_composition_resume_unavailable") from None


async def resolve_registered_verified_office_inputs(
    connection, request_id, approved_revision, expected_head, *, control_schema=None
):
    """Select the exact registered v5 address inputs; metadata grants no native office authority."""
    registered = await _registered_candidate(connection, request_id, approved_revision, expected_head, control_schema)
    if registered is None:
        return None
    from process.registry_initial_source_composition import verify_initial_source_office_receipt
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_office_custody import _office_inputs

    try:
        candidate = registered[1]
        summary = verify_initial_source_office_receipt(candidate)
        contribution = verify_registry_source_contributions(candidate)
        if summary is None or summary["revision"] != 5:
            if contribution is not None and contribution["revision"] == 4:
                raise RegistryCompositionResumeError("registry_composition_resume_verified_offices_differ")
            return None
        address_source, runtime_roles = _office_inputs(candidate)
        if (
            verify_source_recipe_metadata(candidate).inventory is None
            or contribution is None
            or contribution["revision"] != 4
            or summary["origin_manifest_sha256"] is not None
            or address_source.generation_id != summary["address_generation_sha256"]
        ):
            raise RegistryCompositionResumeError("registry_composition_resume_verified_offices_differ")
        return address_source, runtime_roles
    except RegistryCompositionResumeError:
        raise
    except asyncpg.PostgresError, ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryCompositionResumeError("registry_composition_resume_unavailable") from None


async def resolve_registered_compact_format(
    connection, request_id, approved_revision, expected_head, *, control_schema=None
):
    """Freeze the explicit registered physical format; metadata grants no read authority."""
    registered = await _registered_candidate(connection, request_id, approved_revision, expected_head, control_schema)
    if registered is None:
        return None
    from process.registry_compact_composition import COMPACT_FORMAT, COMPACT_FORMAT_KEY
    from process.registry_source_selection_receipt import _generations

    try:
        generations = _generations(registered[1])
        if COMPACT_FORMAT_KEY not in generations:
            return False
        if type(generations[COMPACT_FORMAT_KEY]) is not str or generations[COMPACT_FORMAT_KEY] != COMPACT_FORMAT:
            raise RegistryCompositionResumeError("registry_composition_resume_format_unknown")
        return True
    except RegistryCompositionResumeError:
        raise
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryCompositionResumeError("registry_composition_resume_unavailable") from None
