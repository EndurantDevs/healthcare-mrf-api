# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical address candidate readiness, separate from full serving readiness."""

import json
import os
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_address_projection import (
    NetworkAddressProjectionError,
    PinnedAddressSource,
    _identifier,
    _membership_accounting,
)
from process.network_membership_candidate_lifecycle import MembershipCandidateError, _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_validation import (
    NetworkMembershipValidationError,
    _native_accounting,
    _scope_report,
    _validated_replay,
    _validation_report,
)


class NetworkCandidateIndexesError(ValueError):
    """Candidate canonical readiness failed without advancing its control state."""


async def _catalog_readiness(connection, schema_name):
    index_state = await connection.fetchrow(
        """
        SELECT c.relkind='r' AND canonical.atttypid='integer[]'::regtype AND canonical.attnotnull
                AND canonical.attndims=1 AS has_canonical_column,
            EXISTS(SELECT 1 FROM pg_index index_record
                WHERE index_record.indrelid=c.oid AND index_record.indisprimary AND index_record.indisunique
                AND index_record.indisvalid AND index_record.indisready AND index_record.indislive
                AND index_record.indnkeyatts=1 AND index_record.indnatts=1
                AND index_record.indkey[0]=location.attnum
                AND index_record.indexprs IS NULL AND index_record.indpred IS NULL) AS has_location_pk,
            EXISTS(SELECT 1 FROM pg_index index_record
                JOIN pg_class index_table ON index_table.oid=index_record.indexrelid
                JOIN pg_am access_method ON access_method.oid=index_table.relam
                JOIN pg_opclass operator_class ON operator_class.oid=index_record.indclass[0]
                JOIN pg_depend dependency ON dependency.classid='pg_opclass'::regclass
                    AND dependency.objid=operator_class.oid AND dependency.refclassid='pg_extension'::regclass
                    AND dependency.deptype='e'
                JOIN pg_extension extension ON extension.oid=dependency.refobjid AND extension.extname='intarray'
                WHERE index_record.indrelid=c.oid AND index_record.indisvalid AND index_record.indisready
                AND index_record.indislive AND access_method.amname='gin'
                AND operator_class.opcname='gin__int_ops' AND operator_class.opcintype='integer[]'::regtype
                AND index_record.indnkeyatts=1 AND index_record.indnatts=1
                AND index_record.indkey[0]=canonical.attnum
                AND index_record.indexprs IS NULL AND index_record.indpred IS NULL) AS has_canonical_gin
        FROM pg_class c JOIN pg_namespace namespace ON namespace.oid=c.relnamespace
        LEFT JOIN pg_attribute canonical ON canonical.attrelid=c.oid AND canonical.attname='canonical_network_ids'
            AND canonical.attnum>0 AND NOT canonical.attisdropped
        LEFT JOIN pg_attribute location ON location.attrelid=c.oid AND location.attname='location_key'
            AND location.attnum>0 AND NOT location.attisdropped
        WHERE namespace.nspname=$1 AND c.relname='entity_address_unified'
    """,
        schema_name,
    )
    if index_state is None or not all(
        index_state[field] is True for field in ("has_canonical_column", "has_location_pk", "has_canonical_gin")
    ):
        raise NetworkCandidateIndexesError("Candidate column, primary key or native canonical GIN is not ready")
    return dict(index_state)


async def _projection_parity(connection, projection_table, address_table, membership_table, binding_table):
    parity_counts = await connection.fetchrow(f"""
        WITH expected_networks AS (
            SELECT binding.location_key,binding.entity_type,binding.entity_id,
                array_agg(DISTINCT membership.network_id ORDER BY membership.network_id) AS network_ids
            FROM {membership_table} membership JOIN {binding_table} binding
              ON binding.provider_system=membership.provider_system AND binding.provider_id=membership.provider_id
              AND binding.location_id=membership.location_id
            GROUP BY binding.location_key,binding.entity_type,binding.entity_id
        )
        SELECT count(source.location_key) AS source_rows, count(projected.location_key) AS address_rows,
            count(*) FILTER (WHERE source.location_key IS NOT NULL AND projected.location_key IS NULL) AS missing_locations,
            count(*) FILTER (WHERE source.location_key IS NULL) AS extra_locations,
            count(*) FILTER (WHERE source.location_key IS NOT NULL AND projected.location_key IS NOT NULL
                AND (to_jsonb(source)-'canonical_network_ids') IS DISTINCT FROM
                    (to_jsonb(projected)-'canonical_network_ids')) AS changed_address_rows,
            count(*) FILTER (WHERE projected.location_key IS NOT NULL AND projected.canonical_network_ids
                IS DISTINCT FROM COALESCE(expected.network_ids,'{{}}'::integer[])) AS changed_network_arrays
        FROM {address_table} source FULL JOIN {projection_table} projected ON projected.location_key=source.location_key
        LEFT JOIN expected_networks expected ON expected.location_key=source.location_key
            AND expected.entity_type=source.entity_type AND expected.entity_id=source.entity_id
    """)
    if any(
        parity_counts[field]
        for field in ("missing_locations", "extra_locations", "changed_address_rows", "changed_network_arrays")
    ):
        raise NetworkCandidateIndexesError("Candidate address rows or exact canonical arrays differ from pinned inputs")
    return dict(parity_counts)


async def _matching_validation(connection, candidate, copy_target, address_source, control_schema):
    scope_report = _scope_report(candidate, address_source)
    if scope_report["source_generations"].get("unified_address") != address_source.generation_id:
        raise NetworkCandidateIndexesError("Pinned source generation differs from the candidate")
    retained_report = _validated_replay(candidate, scope_report)
    if candidate["state"] == "ready":
        return retained_report, scope_report
    namespace = _identifier(copy_target.schema_name)
    address_counts = await _membership_accounting(
        connection,
        f"{namespace}.network_membership",
        f"{namespace}.provider_location_binding",
        f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}",
    )
    native_counts = await _native_accounting(
        connection, f"{namespace}.network_membership", control_schema, copy_target.candidate_id
    )
    current_report = _validation_report(scope_report, address_counts, native_counts)
    if any(
        type(retained_report.get(field)) is not type(expected) or retained_report.get(field) != expected
        for field, expected in current_report.items()
    ):
        raise NetworkCandidateIndexesError("Candidate relationships or accounting changed after validation")
    return retained_report, scope_report


def _ready_replay(candidate, retained_report, scope_report):
    readiness = retained_report.get("candidate_readiness")
    if candidate["index_ready"] is not True or type(readiness) is not dict or readiness.get("ready") is not True:
        raise NetworkCandidateIndexesError("Ready candidate lacks its canonical readiness receipt")
    if (
        readiness.get("component") != "canonical_address_projection"
        or type(readiness.get("readiness_revision")) is not int
        or readiness.get("readiness_revision") != 1
    ):
        raise NetworkCandidateIndexesError("Ready candidate receipt has a different component contract")
    if json.dumps(readiness.get("scope"), sort_keys=True) != json.dumps(scope_report, sort_keys=True) or any(
        type(readiness.get(field)) is not type(retained_report.get(field))
        or readiness.get(field) != retained_report.get(field)
        for field in ("membership_rows", "distinct_memberships", "projected_locations")
    ):
        raise NetworkCandidateIndexesError("Ready candidate receipt differs from its retained validation")
    index_checks = readiness.get("index_checks")
    if (
        type(index_checks) is not dict
        or set(index_checks) != {"has_canonical_column", "has_location_pk", "has_canonical_gin"}
        or not all(flag is True for flag in index_checks.values())
    ):
        raise NetworkCandidateIndexesError("Ready candidate receipt has invalid index checks")
    parity_fields = (
        "source_rows",
        "address_rows",
        "missing_locations",
        "extra_locations",
        "changed_address_rows",
        "changed_network_arrays",
    )
    if (
        any(type(readiness.get(field)) is not int or readiness[field] < 0 for field in parity_fields)
        or readiness["source_rows"] != readiness["address_rows"]
        or any(readiness[field] for field in parity_fields[2:])
    ):
        raise NetworkCandidateIndexesError("Ready candidate receipt has invalid projection counts")
    return readiness


async def _prepare_candidate(connection, copy_target, address_source, control_schema):
    candidate = await _locked_candidate(connection, copy_target, _identifier(control_schema))
    if candidate["state"] not in {"validated", "ready"}:
        raise NetworkCandidateIndexesError("Candidate must be validated before canonical readiness")
    namespace = _identifier(copy_target.schema_name)
    projection_table = f"{namespace}.entity_address_unified"
    if candidate["state"] == "validated":
        await connection.execute(
            f"LOCK TABLE {projection_table},{namespace}.network_membership,{namespace}.provider_location_binding IN SHARE MODE"
        )
    retained_report, scope_report = await _matching_validation(
        connection, candidate, copy_target, address_source, control_schema
    )
    if candidate["state"] == "ready":
        return _ready_replay(candidate, retained_report, scope_report)
    index_checks = await _catalog_readiness(connection, copy_target.schema_name)
    parity_counts = await _projection_parity(
        connection,
        projection_table,
        f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}",
        f"{namespace}.network_membership",
        f"{namespace}.provider_location_binding",
    )
    await connection.execute(f"ANALYZE {projection_table}")
    readiness_by_field = {
        "component": "canonical_address_projection",
        "readiness_revision": 1,
        "ready": True,
        "scope": scope_report,
        "index_checks": index_checks,
        **parity_counts,
        **{
            field: retained_report[field]
            for field in ("membership_rows", "distinct_memberships", "projected_locations")
        },
    }
    await connection.execute(
        f"UPDATE {_identifier(control_schema)}.network_membership_candidate "
        "SET state='ready',index_ready=true,validation_json=$2::jsonb WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        json.dumps(retained_report | {"candidate_readiness": readiness_by_field}, sort_keys=True),
    )
    return readiness_by_field


async def prepare_network_candidate_indexes(connection, copy_target, source, *, control_schema=None):
    """Verify canonical candidate parity/indexes; never publish serving state.

    The cached ready replay depends on the integrating control's immutable writer
    closure. Full NPI/geo/serving index compatibility requires a separate receipt.
    """
    if type(copy_target) is not MembershipCopyTarget or type(source) is not PinnedAddressSource:
        raise NetworkCandidateIndexesError("Trusted candidate and pinned source are required")
    if not connection.is_in_transaction():
        raise NetworkCandidateIndexesError("Readiness requires a caller-owned transaction")
    if source.schema_name == copy_target.schema_name:
        raise NetworkCandidateIndexesError("Pinned source must be outside the candidate")
    try:
        async with connection.transaction():
            return await _prepare_candidate(
                connection,
                copy_target,
                source,
                control_schema if control_schema is not None else registry_schema(),
            )
    except (NetworkAddressProjectionError, MembershipCandidateError, NetworkMembershipValidationError) as error:
        raise NetworkCandidateIndexesError(str(error)) from error
