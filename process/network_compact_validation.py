# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based data checks before compact candidate publication."""

from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _has_binding_location_index, _identifier
from process.network_compact_offices import CompactOfficeSources
from process.network_compact_readiness import _require, compact_readiness
from process.network_compact_slice_validation import verify_compact_slice_contents
from process.network_membership_validation import _json_object, _scope_report
from process.network_published_api_pin import published_network_layout
from process.registry_typed_membership_parity import is_typed_candidate


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
    """Check slice reuse, reference ownership and membership-to-slice consistency before publication."""
    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    compact_readiness(candidate, report, _scope_report(candidate, address_source))
    layout = published_network_layout(candidate)
    await verify_compact_slice_contents(connection, copy_target, layout)
    namespace = _identifier(copy_target.schema_name)
    artifact_ids = [artifact.candidate_id for artifact in layout.artifacts]
    _require(
        await connection.fetchval(
            f"SELECT NOT EXISTS(SELECT FROM {namespace}.network_slice_ref "
            "WHERE NOT artifact_candidate_id=ANY($1::uuid[]))",
            artifact_ids,
        )
    )
    if is_typed_candidate(candidate):
        from process.network_compact_custom_preparation import compact_carrier_receipt
        from process.network_typed_membership_accounting import prepare_accounting_work
        from process.network_typed_slice_native import require_complete_facts, verify_work_refs

        offices = CompactOfficeSources(address_source, layout.custom, layout.base, compact_carrier_receipt(candidate))
        work, counts = await prepare_accounting_work(connection, namespace, offices)
        require_complete_facts(counts, candidate, None)
        await verify_work_refs(connection, copy_target, work)
        await connection.execute(f"DROP TABLE {work}")
    else:
        _require(await _membership_sets_differ(connection, namespace, layout) is False)
    # Accounting was validated and stored when the candidate was sealed.
    return report
