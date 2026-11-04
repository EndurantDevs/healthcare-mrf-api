# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent final proof before compact candidate custody is transferred."""

from uuid import UUID

from process.network_address_projection import (
    PinnedAddressSource,
    _has_binding_location_index,
    _identifier,
    _membership_accounting,
)
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


async def _membership_sets_differ(connection, namespace, layout):
    """Probe both complete sets without sorting duplicate source evidence."""
    if not await _has_binding_location_index(connection, namespace + ".provider_location_binding"):
        return await connection.fetchval(
            f"""WITH expected AS (
            SELECT DISTINCT m.network_id,b.location_key FROM {namespace}.network_membership m
            JOIN {namespace}.provider_location_binding b USING(provider_system,provider_id,location_id)
        ), actual AS ({_actual_memberships(layout)})
        SELECT EXISTS((SELECT * FROM expected EXCEPT SELECT * FROM actual)
            UNION ALL (SELECT * FROM actual EXCEPT SELECT * FROM expected))""",
            *(UUID(artifact.candidate_id) for artifact in layout.artifacts),
        )
    refs = _identifier(layout.ref_map.schema_name) + ".network_slice_ref"
    matched_queries, extra_queries = [], []
    for position, artifact in enumerate(layout.artifacts, 1):
        edges = _identifier(artifact.schema_name) + ".network_office_edge"
        matched_queries.append(f"""SELECT 1 FROM {refs} r CROSS JOIN LATERAL (
          SELECT 1 FROM {edges} e WHERE e.slice_id=r.slice_id AND e.location_key=b.location_key OFFSET 0
        ) edge WHERE r.network_id=m.network_id
          AND r.office_bucket=(get_byte(sha256(convert_to(b.location_key,'UTF8')),0)&63)
          AND r.artifact_candidate_id=${position}::uuid OFFSET 0""")
        extra_queries.append(f"""SELECT 1 FROM {refs} r CROSS JOIN LATERAL (
          SELECT e.location_key FROM {edges} e WHERE e.slice_id=r.slice_id OFFSET 0
        ) edge WHERE r.artifact_candidate_id=${position}::uuid
          AND NOT EXISTS(SELECT 1 FROM {namespace}.provider_location_binding b
            WHERE b.location_key=edge.location_key AND EXISTS(
              SELECT 1 FROM {namespace}.network_membership m
              WHERE (m.provider_system,m.provider_id,m.location_id)=
                    (b.provider_system,b.provider_id,b.location_id)
                AND m.network_id=r.network_id OFFSET 0) OFFSET 0)""")
    matches_query = " UNION ALL ".join("(" + query + ")" for query in matched_queries) or "SELECT 1 WHERE FALSE"
    extras_query = " UNION ALL ".join("(" + query + ")" for query in extra_queries) or "SELECT 1 WHERE FALSE"
    return await connection.fetchval(
        f"""SELECT EXISTS(SELECT 1 FROM {namespace}.network_membership m CROSS JOIN LATERAL (
          SELECT b.location_key FROM {namespace}.provider_location_binding b
          WHERE (b.provider_system,b.provider_id,b.location_id)=(m.provider_system,m.provider_id,m.location_id)
          OFFSET 0
        ) b WHERE NOT EXISTS({matches_query})) OR EXISTS({extras_query})""",
        *(UUID(artifact.candidate_id) for artifact in layout.artifacts),
    )


async def verify_compact_final_proof(
    connection, copy_target, candidate, *, control_schema, office_authority=None, source_witness_runtime=None
):
    """Recheck original address authority and full membership parity before sealing."""
    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    scope = _scope_report(candidate, address_source)
    _, serving = compact_readiness(candidate, report, scope)
    layout = await capture_compact_dependencies(connection, candidate, control_schema=control_schema)
    await _verify_custom_contribution(
        connection,
        candidate,
        layout,
        control_schema,
        office_authority,
        **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
    )
    await verify_compact_slice_contents(connection, copy_target, layout, candidate=candidate)
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
    _require(await _membership_sets_differ(connection, namespace, layout) is False)


async def _verify_custom_contribution(
    connection, candidate, layout, control_schema, office_authority, *, source_witness_runtime=None
):
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
            **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
        )
        _require(custom == layout.custom)
