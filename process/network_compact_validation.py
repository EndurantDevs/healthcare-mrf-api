# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based data checks before compact candidate publication."""

from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _has_binding_location_index, _identifier
from process.network_compact_dependencies import compact_slice_ref_digest
from process.network_compact_offices import CompactOfficeSources
from process.network_compact_readiness import _require, compact_readiness
from process.network_compact_slice_validation import verify_compact_slice_contents
from process.network_membership_validation import _json_object, _scope_report
from process.network_published_api_pin import published_network_layout
from process.registry_typed_membership_parity import is_typed_candidate, require_not_null_keys, require_order_index


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


def _indexed_slice_probes(namespace, layout):
    matching, extra = [], []
    for position, artifact in enumerate(layout.artifacts, 1):
        edges = _identifier(artifact.schema_name) + ".network_office_edge"
        matching.append(f"""(r.artifact_candidate_id=${position}::uuid AND EXISTS(
            SELECT FROM {edges} e WHERE e.slice_id=r.slice_id AND e.location_key=b.location_key))""")
        extra.append(f"""SELECT FROM {_identifier(layout.ref_map.schema_name)}.network_slice_ref r CROSS JOIN LATERAL (
          SELECT e.location_key FROM {edges} e WHERE e.slice_id=r.slice_id OFFSET 0
        ) edge WHERE r.artifact_candidate_id=${position}::uuid AND (
          r.office_bucket<>(get_byte(sha256(convert_to(edge.location_key,'UTF8')),0)&63)
          OR NOT EXISTS(SELECT FROM {namespace}.provider_location_binding b
            WHERE b.location_key=edge.location_key AND EXISTS(
              SELECT FROM {namespace}.network_membership m
              WHERE (m.provider_system,m.provider_id,m.location_id,m.network_id)=
                    (b.provider_system,b.provider_id,b.location_id,r.network_id) OFFSET 0) OFFSET 0))""")
    return " OR ".join(matching) or "FALSE", " UNION ALL ".join(extra) or "SELECT 1 WHERE FALSE"


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
    matches, extra = _indexed_slice_probes(namespace, layout)
    return await connection.fetchval(
        f"""SELECT EXISTS(SELECT 1 FROM {namespace}.network_membership m CROSS JOIN LATERAL (
          SELECT b.location_key FROM {namespace}.provider_location_binding b
          WHERE (b.provider_system,b.provider_id,b.location_id)=(m.provider_system,m.provider_id,m.location_id)
          OFFSET 0
        ) b WHERE NOT EXISTS(SELECT FROM {refs} r WHERE r.network_id=m.network_id
          AND r.office_bucket=(get_byte(sha256(convert_to(b.location_key,'UTF8')),0)&63)
          AND ({matches}))) OR EXISTS({extra})""",
        *(UUID(artifact.candidate_id) for artifact in layout.artifacts),
    )


async def _typed_memberships_match(connection, namespace, layout, expected_rows):
    """Account for all evidence and check its exact covering edges with indexed lookups."""
    refs = _identifier(layout.ref_map.schema_name) + ".network_slice_ref"
    matches, extra = _indexed_slice_probes(namespace, layout)
    return await connection.fetchval(
        f"""WITH subjects AS NOT MATERIALIZED (
          SELECT provider_system,provider_id,location_id,network_id,count(*) AS evidence_rows
          FROM {namespace}.network_membership GROUP BY provider_system,provider_id,location_id,network_id
          ORDER BY provider_system,provider_id,location_id,network_id
        ) SELECT coalesce(sum(m.evidence_rows),0)=${len(layout.artifacts) + 1}::bigint
          AND coalesce(bool_and(coalesce(binding.matched,false)),true) AND NOT EXISTS({extra})
        FROM subjects m LEFT JOIN LATERAL (
          SELECT m.network_id>0 AND length(b.location_key) BETWEEN 1 AND 64 AND ({matches}) AS matched
          FROM {namespace}.provider_location_binding b LEFT JOIN {refs} r ON r.network_id=m.network_id
            AND r.office_bucket=(get_byte(sha256(convert_to(b.location_key,'UTF8')),0)&63)
          WHERE (b.provider_system,b.provider_id,b.location_id)=(m.provider_system,m.provider_id,m.location_id) OFFSET 0
        ) binding ON TRUE""",
        *(UUID(artifact.candidate_id) for artifact in layout.artifacts),
        expected_rows,
    )


async def _verify_typed_memberships(connection, copy_target, candidate, layout, address_source, serving):
    from process.network_compact_custom_preparation import compact_carrier_receipt
    from process.network_compact_offices import compact_bound_office_sql
    from process.network_typed_membership_accounting import _orphan_bindings

    _require(candidate["accepted_rows"] == candidate["expected_rows"])
    offices = CompactOfficeSources(address_source, layout.custom, layout.base, compact_carrier_receipt(candidate))
    namespace = _identifier(copy_target.schema_name)
    binding_table = namespace + ".provider_location_binding"
    if serving.get("slice_ref_revision") == 2 and await _has_binding_location_index(connection, binding_table):
        _require(
            serving["slice_ref_sha256"]
            == await compact_slice_ref_digest(connection, copy_target.schema_name, include_reuse_keys=True)
        )
        await require_order_index(connection, namespace + ".network_membership")
        await require_not_null_keys(connection, namespace + ".network_membership")
        _require(await _orphan_bindings(connection, binding_table, compact_bound_office_sql(offices)) == 0)
        _require(await _typed_memberships_match(connection, namespace, layout, candidate["accepted_rows"]))
    else:
        # Older ready candidates lack exact reuse metadata and may lack the inverse binding index.
        from process.network_typed_membership_accounting import prepare_accounting_work
        from process.network_typed_slice_native import require_complete_facts, verify_work_refs

        work, counts = await prepare_accounting_work(connection, namespace, offices)
        require_complete_facts(counts, candidate, None)
        await verify_work_refs(connection, copy_target, work)
        await connection.execute(f"DROP TABLE {work}")


async def verify_compact_final_proof(
    connection, copy_target, candidate, *, control_schema, office_authority=None, source_witness_runtime=None
):
    """Check slice reuse, reference ownership and membership-to-slice consistency before publication."""
    report = _json_object(candidate["validation_json"])
    address_source = PinnedAddressSource(**report["address_source"])
    _, serving = compact_readiness(candidate, report, _scope_report(candidate, address_source))
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
        await _verify_typed_memberships(connection, copy_target, candidate, layout, address_source, serving)
    else:
        _require(await _membership_sets_differ(connection, namespace, layout) is False)
    # Accounting was validated and stored when the candidate was sealed.
    return report
