# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Indexed set validation for a complete sealed membership candidate."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
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
from process.registry_source_retention import (
    SUMMARY_KEY as RETENTION_KEY,
)
from process.registry_source_retention import (
    RegistrySourceRetentionError,
    verify_registry_source_retention,
    verify_registry_source_retention_rows,
)


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


def _em_provider_receipt(candidate):
    from process.network_em_provider_inputs import _receipt

    generations_by_source = _validated_sources(candidate)
    validation_by_field = _json_object(candidate["validation_json"])
    has_marker = "em_provider_inputs" in generations_by_source
    has_receipt = type(validation_by_field) is dict and "em_provider_inputs" in validation_by_field
    if not has_marker and not has_receipt:
        return None
    try:
        if not has_marker or not has_receipt:
            raise ValueError
        receipt_by_field = validation_by_field["em_provider_inputs"]
        _receipt(receipt_by_field)
        if generations_by_source["em_provider_inputs"] != receipt_by_field["receipt_sha256"]:
            raise ValueError
    except ValueError, TypeError, KeyError:
        raise NetworkMembershipValidationError("Candidate provider inputs differ from their immutable scope") from None
    return receipt_by_field


def _compact_receipts(candidate):
    from process.network_compact_custom_preparation import (
        CARRIER_OFFICES_KEY,
        CUSTOM_OFFICES_KEY,
        compact_carrier_receipt,
        compact_custom_receipt,
    )

    custom = compact_custom_receipt(candidate)
    carriers = compact_carrier_receipt(candidate)
    return {
        **({CUSTOM_OFFICES_KEY: custom.as_dict()} if custom is not None else {}),
        **({CARRIER_OFFICES_KEY: carriers.as_dict()} if carriers is not None else {}),
    }


def _scope_report(candidate, address_source):
    """Verify every retained receipt before binding the candidate validation scope."""
    from process.registry_fhir_serving_preservation import (
        SUMMARY_KEY as PRESERVATION_KEY,
    )
    from process.registry_fhir_serving_preservation import (
        verify_fhir_serving_preservation,
    )
    from process.registry_initial_source_composition import (
        SUMMARY_KEY as INITIAL_SUMMARY_KEY,
    )
    from process.registry_initial_source_composition import (
        RegistryInitialSourceError,
        verify_initial_source_office_receipt,
    )
    from process.registry_publication_queue import (
        SOURCE_ROLLBACK_PROVENANCE,
        verify_registry_source_rollback_provenance,
    )
    from process.registry_source_selection_receipt import (
        SUMMARY_KEY,
        RegistrySourceSelectionError,
        verify_registry_source_selection_receipt,
    )

    candidate_by_field = dict(candidate)
    source_operation = verify_registry_source_rollback_provenance(candidate_by_field)
    preservation = verify_fhir_serving_preservation(candidate_by_field)
    provider_receipt = _em_provider_receipt(candidate_by_field)
    from process.registry_source_contribution_store import (
        SUMMARY_KEY as CONTRIBUTION_KEY,
    )
    from process.registry_source_contribution_store import (
        verify_registry_source_contributions,
    )

    contributions = verify_registry_source_contributions(candidate_by_field)
    try:
        source_selection = verify_registry_source_selection_receipt(candidate_by_field)
        initial_source_offices = verify_initial_source_office_receipt(candidate_by_field)
        source_retention = verify_registry_source_retention(candidate_by_field)
    except RegistrySourceSelectionError, RegistryInitialSourceError, RegistrySourceRetentionError:
        raise NetworkMembershipValidationError("Candidate source selection differs from its immutable scope") from None
    return {
        **({SOURCE_ROLLBACK_PROVENANCE: source_operation} if source_operation is not None else {}),
        **({PRESERVATION_KEY: preservation} if preservation is not None else {}),
        **({"em_provider_inputs": provider_receipt} if provider_receipt is not None else {}),
        **({CONTRIBUTION_KEY: contributions} if contributions is not None else {}),
        **_compact_receipts(candidate_by_field),
        **({RETENTION_KEY: source_retention} if source_retention is not None else {}),
        **({SUMMARY_KEY: source_selection} if source_selection is not None else {}),
        **({INITIAL_SUMMARY_KEY: initial_source_offices} if initial_source_offices is not None else {}),
        **_candidate_identity_scope(candidate, address_source),
    }


def _candidate_identity_scope(candidate, address_source):
    identity_fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    return {
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
        or (
            RETENTION_KEY in scope_report
            and any(
                type(original_report.get(field)) is not int or original_report[field] != 0
                for field in ("batch_rows", "batch_count")
            )
        )
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
    if RETENTION_KEY in scope_report:
        if native_counts["batch_rows"] != 0 or native_counts["batch_count"] != 0:
            accounting_errors.append("qualified_batch_accounting_nonzero")
    elif native_counts["batch_rows"] != native_counts["raw_rows"]:
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


async def _address_accounting(connection, copy_target, candidate, office_sources, relations):
    from process.registry_typed_membership_parity import is_typed_candidate

    if office_sources is not None and is_typed_candidate(candidate):
        from process.network_typed_membership_accounting import prepare_accounting_work

        prepared_work = await prepare_accounting_work(connection, _identifier(copy_target.schema_name), office_sources)
        return prepared_work[1], prepared_work
    return await _membership_accounting(connection, *relations), None


async def _validate_sealed(connection, copy_target, address_source, control_schema, office_sources=None):
    """Validate closed inputs, returning fresh work only for complete typed office facts."""
    from process.registry_source_contribution_store import verify_registry_source_contributions_rows

    candidate_by_field = dict(await _lock_candidate(connection, copy_target, address_source, control_schema))
    scope_report = _scope_report(candidate_by_field, address_source)
    from process.registry_source_inventory_store import _registered_inventory_entries

    entries = await _registered_inventory_entries(
        connection, copy_target, candidate_by_field, control_schema=control_schema
    )
    await verify_registry_source_contributions_rows(
        connection,
        copy_target,
        candidate_by_field,
        control_schema=control_schema,
        **({"inventory_entries": entries} if entries is not None else {}),
    )
    if RETENTION_KEY in scope_report:
        try:
            await verify_registry_source_retention_rows(connection, copy_target, candidate_by_field)
        except RegistrySourceRetentionError as error:
            raise NetworkMembershipValidationError(str(error)) from error
    if candidate_by_field["state"] == "validated":
        return _validated_replay(candidate_by_field, scope_report), None
    candidate_schema = _identifier(copy_target.schema_name)
    membership_table = f"{candidate_schema}.network_membership"
    binding_table = f"{candidate_schema}.provider_location_binding"
    address_table = f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
    if office_sources is not None:
        from process.network_compact_offices import CompactOfficeSources, pin_compact_office_sources

        if type(office_sources) is not CompactOfficeSources or office_sources.base != address_source:
            raise NetworkMembershipValidationError("Compact office source differs from the pinned addresses")
        address_table = await pin_compact_office_sources(connection, copy_target, office_sources)
    await connection.execute(f"LOCK TABLE {membership_table},{binding_table} IN SHARE MODE")
    await connection.execute(
        f"CREATE INDEX IF NOT EXISTS network_membership_network_idx ON {membership_table}(network_id)"
    )
    await connection.execute(
        f"CREATE INDEX IF NOT EXISTS network_membership_projection_idx ON {membership_table}"
        "(provider_system,provider_id,location_id,network_id)"
    )
    address_counts, prepared_work = await _address_accounting(
        connection, copy_target, candidate_by_field, office_sources, (membership_table, binding_table, address_table)
    )
    native_counts = await _native_accounting(connection, membership_table, control_schema, copy_target.candidate_id)
    report = _validation_report(scope_report, address_counts, native_counts)
    retained = _json_object(candidate_by_field["validation_json"])
    if type(retained) is dict and "writer_closure" in retained:
        report["writer_closure"] = retained["writer_closure"]
    await connection.execute(
        f"UPDATE {_identifier(control_schema)}.network_membership_candidate "
        "SET state='validated',validation_json=$2::jsonb WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        json.dumps(report, sort_keys=True, separators=(",", ":")),
    )
    return report, prepared_work


@asynccontextmanager
async def _validated_candidate(connection, copy_target, source, *, control_schema=None, office_sources=None):
    """Keep freshly verified typed work inside the validating savepoint."""
    if type(copy_target) is not MembershipCopyTarget or type(source) is not PinnedAddressSource:
        raise NetworkMembershipValidationError("Trusted candidate and pinned address source are required")
    if not connection.is_in_transaction():
        raise NetworkMembershipValidationError("Validation requires a caller-owned transaction")
    if source.schema_name == copy_target.schema_name:
        raise NetworkMembershipValidationError("Pinned source must be outside the validation candidate")
    try:
        async with connection.transaction():
            report, prepared_work = await _validate_sealed(
                connection,
                copy_target,
                source,
                control_schema if control_schema is not None else registry_schema(),
                office_sources,
            )
            yield report, prepared_work
            if prepared_work is not None:
                await connection.execute(f"DROP TABLE IF EXISTS {prepared_work[0]}")
    except NetworkAddressProjectionError as error:
        raise NetworkMembershipValidationError(str(error)) from error


async def validate_network_membership_candidate(
    connection, copy_target, source, *, control_schema=None, office_sources=None
):
    """Return the successful pinned report; invalid candidates raise a typed error.

    Aggregate failures carry their report on the error. Ownership, source and
    exact-site failures disclose only a bounded error message. The caller keeps
    transaction ownership; failed savepoints roll back candidate index creation.
    Exact validated replay trusts the integrating owner's immutable writer closure.
    """
    async with _validated_candidate(
        connection, copy_target, source, control_schema=control_schema, office_sources=office_sources
    ) as (report, _):
        return report


@asynccontextmanager
async def _preservation_scope(
    connection, copy_target, candidate, schema, inventory_entries, *, source_witness_runtime=None
):
    """Keep exact carry selections alive through final data checks, then drain their temporary tables."""
    from process.network_sampled_custody import inherit_preserved_sampling
    from process.registry_fhir_serving_preservation import (
        cleanup_fhir_serving_preservation,
        has_fresh_carried_typed_data,
        rederive_fhir_serving_preservation,
        verify_fhir_preservation_target_rows,
    )

    preservation = await rederive_fhir_serving_preservation(
        connection,
        copy_target,
        candidate,
        control_schema=schema,
        inventory_entries=inventory_entries,
        source_witness_runtime=source_witness_runtime,
    )
    primary = None
    try:
        if preservation is not None:
            await verify_fhir_preservation_target_rows(
                connection, copy_target, candidate, preservation, control_schema=schema
            )
        freshly_checked = await has_fresh_carried_typed_data(
            connection, preservation, control_schema=schema, source_witness_runtime=source_witness_runtime
        )
        if not freshly_checked:
            await inherit_preserved_sampling(connection, preservation, source_witness_runtime, schema)
        yield preservation
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            await cleanup_fhir_serving_preservation(connection, preservation, primary=primary)
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note("FHIR preservation cleanup failed; caller rollback remains required.")
            primary._registry_preservation_cleanup_error = cleanup_error


async def _validate_full_candidate_data(connection, copy_target, candidate, schema):
    """Recheck complete native accounting, address parity and structural serving indexes."""
    from process.network_membership_serving_indexes import (
        _index_digest,
        _matching_inputs,
        _serving_plan,
        _source_semantics,
    )

    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    validated, scope = await _matching_inputs(connection, candidate, copy_target, address_source, schema)
    serving = report.get("serving_readiness")
    if (
        type(serving) is not dict
        or serving.get("component") != "unified_address_serving"
        or type(serving.get("readiness_revision")) is not int
        or serving["readiness_revision"] != 1
        or serving.get("ready") is not True
        or serving.get("index_profile") != "serving"
        or serving.get("scope") != scope
    ):
        raise NetworkMembershipValidationError("Candidate lacks matching full serving readiness")
    await _source_semantics(
        connection,
        f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}",
        f"{_identifier(copy_target.schema_name)}.entity_address_unified",
        replay=True,
    )
    current_indexes = await _index_digest(connection, copy_target.schema_name, _serving_plan(copy_target.schema_name))
    if current_indexes != serving.get("index_definition_sha256"):
        raise NetworkMembershipValidationError("Candidate serving index definitions changed")
    return validated


async def _final_proof(
    connection,
    copy_target,
    candidate,
    schema,
    *,
    office_authority=None,
    inventory_entries=None,
    source_witness_runtime=None,
):
    """Reconstruct complete candidate data from named inputs without a control-row publication lock."""
    from process.network_compact_readiness import is_compact_candidate
    from process.network_em_provider_inputs import verify_candidate_em_provider_inputs
    from process.network_source_contribution_custody import verify_independent_source_contributions
    from process.registry_source_office_custody import verify_independent_initial_source_offices

    source_options_by_name = {"source_witness_runtime": source_witness_runtime}
    async with _preservation_scope(
        connection, copy_target, candidate, schema, inventory_entries, **source_options_by_name
    ) as preservation:
        await verify_registry_source_retention_rows(connection, copy_target, candidate)
        source_preservation = await verify_independent_source_contributions(
            connection,
            copy_target,
            candidate,
            control_schema=schema,
            office_authority=office_authority,
            preservation=preservation,
            **source_options_by_name,
            **({"inventory_entries": inventory_entries} if inventory_entries is not None else {}),
        )
        if inventory_entries is None and source_preservation is None:
            await verify_independent_initial_source_offices(
                connection,
                copy_target,
                candidate,
                control_schema=schema,
                office_authority=office_authority,
                preservation=preservation,
            )
        if is_compact_candidate(candidate):
            from process.network_compact_validation import verify_compact_final_proof

            validated = await verify_compact_final_proof(
                connection,
                copy_target,
                candidate,
                control_schema=schema,
                office_authority=office_authority,
                **source_options_by_name,
            )
        else:
            validated = await _validate_full_candidate_data(connection, copy_target, candidate, schema)
        await verify_candidate_em_provider_inputs(connection, candidate)
        return validated


def _candidate_source_runtime(source_witness_runtime, copy_target):
    """Create this candidate's sampling run from the configured deterministic policy."""
    from process.network_sampled_custody import SampledValidationRun
    from process.registry_source_recipe_composition import SourceWitnessRuntime

    if source_witness_runtime is not None:
        if type(source_witness_runtime) is not SourceWitnessRuntime:
            raise NetworkMembershipValidationError("Final validation requires configured Source data services")
        source_witness_runtime.__post_init__()
        if source_witness_runtime.semantic_policy is not None:
            source_witness_runtime = replace(
                source_witness_runtime,
                semantic_policy=None,
                semantic_validation=SampledValidationRun(
                    source_witness_runtime.semantic_policy, copy_target.candidate_id
                ),
            )
    return source_witness_runtime


async def validate_network_candidate_final(
    connection, copy_target, candidate, control_schema, *, office_authority=None, source_witness_runtime=None
):
    """Validate named snapshot data completely before the caller's short atomic publication transaction."""
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_store import load_source_inventory

    if (
        type(copy_target) is not MembershipCopyTarget
        or not connection.is_in_transaction()
        or await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}
        or candidate["state"] not in {"ready", "published"}
        or any(
            str(candidate[field]) != getattr(copy_target, field)
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        )
    ):
        raise NetworkMembershipValidationError(
            "Final validation requires the exact ready candidate in a repeatable snapshot"
        )
    _identifier(control_schema)
    metadata = verify_source_recipe_metadata(candidate)
    entries = None
    if metadata.inventory is not None:
        _, entries = await load_source_inventory(connection, copy_target, candidate, control_schema=control_schema)
    source_witness_runtime = _candidate_source_runtime(source_witness_runtime, copy_target)
    validated = await _final_proof(
        connection,
        copy_target,
        candidate,
        control_schema,
        office_authority=office_authority,
        inventory_entries=entries,
        source_witness_runtime=source_witness_runtime,
    )
    sampling = None if source_witness_runtime is None else source_witness_runtime.semantic_validation
    semantic = None if sampling is None else await sampling.finish(connection, copy_target.candidate_id)
    count_fields = (
        "membership_rows",
        "distinct_memberships",
        "projected_locations",
        "orphan_bindings",
        "raw_rows",
        "batch_rows",
        "batch_count",
        "unknown_network_rows",
    )
    return {
        "candidate_id": copy_target.candidate_id,
        "schema_name": copy_target.schema_name,
        "complete": True,
        "source_generations": _validated_sources(candidate),
        "counts": {field: validated[field] for field in count_fields},
        "semantic_validation": semantic,
    }
