# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent final proof before compact candidate custody is transferred."""

from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier, _membership_accounting
from process.network_compact_dependencies import capture_compact_dependencies, compact_slice_ref_digest
from process.network_compact_offices import CompactOfficeSources, pin_compact_office_sources
from process.network_compact_readiness import _require, compact_index_digest, compact_readiness
from process.network_compact_slice_validation import verify_compact_slice_contents
from process.network_membership_validation import _json_object, _native_accounting, _scope_report, _validation_report


def _actual_memberships(layout):
    refs = _identifier(layout.ref_map.schema_name) + ".network_slice_ref"
    arms = [
        f"SELECT r.network_id,e.location_key FROM {refs} r "
        f"JOIN {_identifier(artifact.schema_name)}.network_office_edge e ON e.slice_id=r.slice_id "
        f"WHERE r.artifact_candidate_id=${position}::uuid"
        for position, artifact in enumerate(layout.artifacts, 1)
    ]
    return (
        " UNION ALL ".join(arms)
        if arms
        else "SELECT NULL::integer AS network_id,NULL::text AS location_key WHERE FALSE"
    )


async def verify_compact_final_proof(connection, copy_target, candidate, *, control_schema, office_authority=None):
    """Recheck original address authority and full membership parity before sealing."""
    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    scope = _scope_report(candidate, address_source)
    _, serving = compact_readiness(candidate, report, scope)
    layout = await capture_compact_dependencies(connection, candidate, control_schema=control_schema)
    await _verify_custom_contribution(connection, candidate, layout, control_schema, office_authority)
    await verify_compact_slice_contents(connection, copy_target, layout)
    namespace = _identifier(copy_target.schema_name)
    from process.network_compact_custom_preparation import compact_carrier_receipt

    address = await pin_compact_office_sources(
        connection,
        copy_target,
        CompactOfficeSources(address_source, layout.custom, layout.base, compact_carrier_receipt(candidate)),
    )
    counts = await _membership_accounting(
        connection, namespace + ".network_membership", namespace + ".provider_location_binding", address
    )
    native = await _native_accounting(
        connection, namespace + ".network_membership", control_schema, copy_target.candidate_id
    )
    validation = _validation_report(scope, counts, native)
    _require(all(report.get(field) == expected_value for field, expected_value in validation.items()))
    _require(
        await compact_index_digest(connection, copy_target.schema_name, candidate=candidate)
        == serving["index_definition_sha256"]
    )
    _require(await compact_slice_ref_digest(connection, copy_target.schema_name) == serving["slice_ref_sha256"])
    artifact_ids = [artifact.candidate_id for artifact in layout.artifacts]
    _require(
        await connection.fetchval(
            f"SELECT NOT EXISTS(SELECT FROM {namespace}.network_slice_ref "
            "WHERE NOT artifact_candidate_id=ANY($1::uuid[]))",
            artifact_ids,
        )
    )
    mismatch = await connection.fetchval(
        f"""WITH expected AS (
        SELECT DISTINCT m.network_id,b.location_key FROM {namespace}.network_membership m
        JOIN {namespace}.provider_location_binding b USING(provider_system,provider_id,location_id)
    ), actual AS ({_actual_memberships(layout)})
    SELECT EXISTS((SELECT * FROM expected EXCEPT SELECT * FROM actual)
        UNION ALL (SELECT * FROM actual EXCEPT SELECT * FROM expected))""",
        *(UUID(artifact.candidate_id) for artifact in layout.artifacts),
    )
    _require(mismatch is False)


async def _verify_custom_contribution(connection, candidate, layout, control_schema, office_authority):
    from process.network_compact_custom_preparation import (
        compact_custom_receipt,
        verify_compact_custom_approval,
        verify_compact_custom_contribution,
    )

    receipt = compact_custom_receipt(candidate)
    if receipt is not None:
        await verify_compact_custom_approval(connection, candidate, control_schema)
        custom = await verify_compact_custom_contribution(
            connection,
            candidate,
            runtime_roles=receipt.runtime_roles,
            control_schema=control_schema,
            office_authority=office_authority,
        )
        _require(custom == layout.custom)
