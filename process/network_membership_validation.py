# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Indexed set validation for a complete sealed membership candidate."""

import json
import os
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_address_projection import (
    NetworkAddressProjectionError,
    PinnedAddressSource,
    _identifier,
    _lock_candidate,
    _membership_accounting,
)
from process.network_membership_copy import MembershipCopyTarget


class NetworkMembershipValidationError(ValueError):
    """Invalid candidates leave no newly committed validation or index state."""

    def __init__(self, message, report=None):
        super().__init__(message)
        self.report = report


def _json_object(encoded):
    return json.loads(encoded) if isinstance(encoded, str) else encoded


def _validated_sources(candidate):
    generations_by_source = _json_object(candidate["source_generations"])
    if type(generations_by_source) is not dict or len(generations_by_source) > 50:
        raise NetworkMembershipValidationError("Candidate source generations must be a bounded string map")
    for source_name, generation_id in generations_by_source.items():
        if (
            type(source_name) is not str
            or not source_name.strip()
            or len(source_name) > 128
            or type(generation_id) is not str
            or not generation_id.strip()
            or len(generation_id) > 256
            or any(not character.isprintable() for character in source_name + generation_id)
        ):
            raise NetworkMembershipValidationError("Candidate source generations require bounded opaque strings")
    return generations_by_source


def _scope_report(candidate, address_source):
    from process.registry_initial_source_composition import (
        SUMMARY_KEY as INITIAL_SUMMARY_KEY,
    )
    from process.registry_initial_source_composition import (
        RegistryInitialSourceError,
        verify_initial_source_office_receipt,
    )
    from process.registry_source_selection_receipt import (
        SUMMARY_KEY,
        RegistrySourceSelectionError,
        verify_registry_source_selection_receipt,
    )

    candidate_by_field = dict(candidate)
    try:
        source_selection = verify_registry_source_selection_receipt(candidate_by_field)
        initial_source_offices = verify_initial_source_office_receipt(candidate_by_field)
    except RegistrySourceSelectionError, RegistryInitialSourceError:
        raise NetworkMembershipValidationError("Candidate source selection differs from its immutable scope") from None
    identity_fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    return {
        **({SUMMARY_KEY: source_selection} if source_selection is not None else {}),
        **({INITIAL_SUMMARY_KEY: initial_source_offices} if initial_source_offices is not None else {}),
        **{field: str(candidate[field]) for field in identity_fields},
        "validation_revision": 1,
        "source_generations": _validated_sources(candidate),
        "address_source": {
            "schema_name": address_source.schema_name,
            "table_name": address_source.table_name,
            "generation_id": address_source.generation_id,
        },
        **{
            field: candidate[field]
            for field in (
                "schema_revision",
                "approved_custom_revision",
                "expected_head",
                "expected_rows",
                "accepted_rows",
            )
        },
    }


def _validated_replay(candidate, scope_report):
    original_report = _json_object(candidate["validation_json"])
    if (
        type(original_report) is not dict
        or original_report.get("valid") is not True
        or any(
            type(original_report.get(field)) is not type(expected) or original_report.get(field) != expected
            for field, expected in scope_report.items()
        )
    ):
        raise NetworkMembershipValidationError("Validated candidate report does not match the immutable pinned scope")
    return original_report


async def _native_accounting(connection, membership_table, control_schema, candidate_id):
    qualified_control = _identifier(control_schema)
    return await connection.fetchrow(
        f"""
        SELECT count(*) AS raw_rows,
            count(*) FILTER (WHERE identity.network_id IS NULL) AS unknown_network_rows,
            (SELECT COALESCE(sum(row_count),0) FROM {qualified_control}.network_membership_batch WHERE candidate_id=$1)
                AS batch_rows,
            (SELECT count(*) FROM {qualified_control}.network_membership_batch WHERE candidate_id=$1) AS batch_count
        FROM {membership_table} membership
        LEFT JOIN {qualified_control}.network_registry_identity identity ON identity.network_id=membership.network_id
    """,
        UUID(candidate_id),
    )


def _validation_report(scope_report, address_counts, native_counts):
    accounting_errors = []
    if native_counts["raw_rows"] != address_counts["membership_rows"]:
        accounting_errors.append("address_join_count_mismatch")
    if native_counts["raw_rows"] != scope_report["accepted_rows"]:
        accounting_errors.append("raw_accepted_count_mismatch")
    if scope_report["accepted_rows"] != scope_report["expected_rows"]:
        accounting_errors.append("accepted_expected_count_mismatch")
    if native_counts["batch_rows"] != native_counts["raw_rows"]:
        accounting_errors.append("batch_raw_count_mismatch")
    report_by_field = {
        **scope_report,
        "valid": not accounting_errors and native_counts["unknown_network_rows"] == 0,
        **{
            field: address_counts[field]
            for field in ("membership_rows", "distinct_memberships", "projected_locations", "orphan_bindings")
        },
        **{field: native_counts[field] for field in ("raw_rows", "batch_rows", "batch_count", "unknown_network_rows")},
        "accounting_errors": accounting_errors,
        "diagnostics": [],
    }
    if not report_by_field["valid"]:
        raise NetworkMembershipValidationError(
            "Candidate network relationships or accounting are invalid", report_by_field
        )
    return report_by_field


async def _validate_sealed(connection, copy_target, address_source, control_schema):
    candidate = await _lock_candidate(connection, copy_target, address_source, control_schema)
    scope_report = _scope_report(candidate, address_source)
    if candidate["state"] == "validated":
        return _validated_replay(candidate, scope_report)
    candidate_schema = _identifier(copy_target.schema_name)
    membership_table = f"{candidate_schema}.network_membership"
    binding_table = f"{candidate_schema}.provider_location_binding"
    address_table = f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
    await connection.execute(f"LOCK TABLE {membership_table},{binding_table} IN SHARE MODE")
    await connection.execute(
        f"CREATE INDEX IF NOT EXISTS network_membership_network_idx ON {membership_table}(network_id)"
    )
    await connection.execute(
        f"CREATE INDEX IF NOT EXISTS network_membership_projection_idx ON {membership_table}"
        "(provider_system,provider_id,location_id,network_id)"
    )
    address_counts = await _membership_accounting(connection, membership_table, binding_table, address_table)
    native_counts = await _native_accounting(connection, membership_table, control_schema, copy_target.candidate_id)
    report = _validation_report(scope_report, address_counts, native_counts)
    retained = _json_object(candidate["validation_json"])
    if type(retained) is dict and "writer_closure" in retained:
        report["writer_closure"] = retained["writer_closure"]
    await connection.execute(
        f"UPDATE {_identifier(control_schema)}.network_membership_candidate "
        "SET state='validated',validation_json=$2::jsonb WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        json.dumps(report, sort_keys=True, separators=(",", ":")),
    )
    return report


async def validate_network_membership_candidate(connection, copy_target, source, *, control_schema=None):
    """Return the successful pinned report; invalid candidates raise a typed error.

    Aggregate failures carry their report on the error. Ownership, source and
    exact-site failures disclose only a bounded error message. The caller keeps
    transaction ownership; failed savepoints roll back candidate index creation.
    Exact validated replay trusts the integrating owner's immutable writer closure.
    """
    if type(copy_target) is not MembershipCopyTarget or type(source) is not PinnedAddressSource:
        raise NetworkMembershipValidationError("Trusted candidate and pinned address source are required")
    if not connection.is_in_transaction():
        raise NetworkMembershipValidationError("Validation requires a caller-owned transaction")
    if source.schema_name == copy_target.schema_name:
        raise NetworkMembershipValidationError("Pinned source must be outside the validation candidate")
    try:
        async with connection.transaction():
            return await _validate_sealed(
                connection,
                copy_target,
                source,
                control_schema if control_schema is not None else registry_schema(),
            )
    except NetworkAddressProjectionError as error:
        raise NetworkMembershipValidationError(str(error)) from error
