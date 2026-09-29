# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded retained-resource reads and exact release-bound identity joins."""

import re

from sqlalchemy import text

from api.provider_directory_entities_contract import DirectoryReadError

KINDS = {
    "organizations": "Organization",
    "sites": "Location",
    "plans": "InsurancePlan",
    "networks": "Organization",
    "practitioner-roles": "PractitionerRole",
}
_RESOURCE = "provider_directory_dataset_resource"
_BINDING = "provider_directory_entity_source_binding"
_EVIDENCE = "provider_directory_entity_release_evidence"
_NETWORK = "provider_directory_insurance_network_source_binding"
_NETWORK_EVIDENCE = "provider_directory_insurance_network_plan_evidence"
_IDENTITY = "provider_directory_resource_identity"


def _binding(kind):
    if kind == "networks":
        return _NETWORK, "network_id"
    if kind in {"plans", "practitioner-roles"}:
        return _IDENTITY, "entity_id"
    return _BINDING, "organization_id" if kind == "organizations" else "site_id"


def _network_witness(schema, resource_alias="r"):
    return f"""SELECT n.insurance_plan_resource_id FROM {schema}.{_NETWORK_EVIDENCE} n
        JOIN {schema}.{_RESOURCE} p ON p.dataset_id=:dataset_id AND p.resource_type='InsurancePlan'
          AND p.resource_id=n.insurance_plan_resource_id
          AND (p.acquired_resource_sha256 IS NULL OR p.acquired_resource_sha256=n.plan_payload_sha256)
        WHERE n.source_id='cms-npd' AND n.release_id=:release_id
          AND n.network_resource_id={resource_alias}.resource_id
          AND p.payload_json::jsonb->'network_refs' @> jsonb_build_array('Organization/' || {resource_alias}.resource_id)
        ORDER BY n.insurance_plan_resource_id LIMIT 1"""


def _release_join(schema, kind):
    if kind in {"organizations", "sites", "networks"}:
        return f"""JOIN {schema}.{_EVIDENCE} e ON e.source_id='cms-npd'
            AND e.resource_type=r.resource_type AND e.resource_id=r.resource_id
            AND e.release_id=:release_id
            AND (r.acquired_resource_sha256 IS NULL OR e.payload_sha256=r.acquired_resource_sha256)"""
    return ""


async def require_cms_bindings(session, schema, generation, kind):
    """Reject missing identities rather than returning a deceptively partial source page."""
    binding, id_column = _binding(kind)
    scope = f"AND EXISTS ({_network_witness(schema)})" if kind == "networks" else ""
    missing = (
        await session.execute(
            text(f"""
        SELECT EXISTS (SELECT 1 FROM {schema}.{_RESOURCE} r
        WHERE r.dataset_id=:dataset_id AND r.resource_type=:resource_type {scope}
          AND NOT EXISTS (SELECT 1 FROM {schema}.{binding} b {_release_join(schema, kind)}
            WHERE b.source_id='cms-npd' AND b.resource_type=r.resource_type
              AND b.resource_id=r.resource_id AND b.{id_column} IS NOT NULL))
    """),
            {**generation, "resource_type": KINDS[kind]},
        )
    ).scalar_one()
    if missing:
        raise DirectoryReadError(503)
    ambiguous = (
        await session.execute(
            text(f"""
        SELECT EXISTS (SELECT 1 FROM {schema}.{binding} b
        JOIN {schema}.{_RESOURCE} r ON r.dataset_id=:dataset_id
          AND r.resource_type=b.resource_type AND r.resource_id=b.resource_id
        WHERE b.source_id='cms-npd' AND b.resource_type=:resource_type {scope}
          AND b.{id_column} IS NOT NULL
        GROUP BY b.{id_column} HAVING count(*)>1)
    """),
            {**generation, "resource_type": KINDS[kind]},
        )
    ).scalar_one()
    if ambiguous:
        raise DirectoryReadError(503)
    if kind == "networks":
        await _require_network_witnesses(session, schema, generation)


async def _require_network_witnesses(session, schema, generation):
    """Do not omit an explicit local plan/network reference whose binding is absent."""
    missing = (
        await session.execute(
            text(f"""
        SELECT EXISTS (SELECT 1 FROM {schema}.{_RESOURCE} p
        CROSS JOIN LATERAL jsonb_array_elements_text(CASE
          WHEN jsonb_typeof(p.payload_json::jsonb->'network_refs')='array'
          THEN p.payload_json::jsonb->'network_refs' ELSE '[]'::jsonb END) ref(value)
        JOIN {schema}.{_RESOURCE} r ON r.dataset_id=p.dataset_id AND r.resource_type='Organization'
          AND ref.value='Organization/' || r.resource_id
        WHERE p.dataset_id=:dataset_id AND p.resource_type='InsurancePlan'
          AND NOT EXISTS (SELECT 1 FROM {schema}.{_NETWORK_EVIDENCE} n
            WHERE n.source_id='cms-npd' AND n.release_id=:release_id
              AND n.network_resource_id=r.resource_id AND n.insurance_plan_resource_id=p.resource_id
              AND (p.acquired_resource_sha256 IS NULL OR n.plan_payload_sha256=p.acquired_resource_sha256)))
    """),
            generation,
        )
    ).scalar_one()
    if missing:
        raise DirectoryReadError(503)


async def cms_entity_rows(session, schema, query, generation, position=None):
    """Seek indexed stable identities, opening only the selected resource projections."""
    binding, id_column = _binding(query.kind)
    network_join = f"JOIN LATERAL ({_network_witness(schema)}) witness ON true" if query.kind == "networks" else ""
    plan_evidence = "witness.insurance_plan_resource_id" if query.kind == "networks" else "NULL::text"
    alias_scope = f"AND EXISTS ({_network_witness(schema, 'selected')})" if query.kind == "networks" else ""
    statement = f"""
        SELECT b.{id_column} AS entity_id, r.resource_type, r.resource_id,
          left(r.payload_json->>'name', 2049) AS name, CASE WHEN jsonb_typeof(r.payload_json::jsonb->'active')='boolean' THEN r.payload_json->>'active' END AS active,
          left(r.payload_json->>'status', 32) AS status, left(r.payload_json->>'period_start', 40) AS period_start,
          left(r.payload_json->>'period_end', 40) AS period_end, {plan_evidence} AS plan_evidence,
          (SELECT count(*) FROM {schema}.{binding} aliases
            JOIN {schema}.{_RESOURCE} selected ON selected.dataset_id=:dataset_id
              AND selected.resource_type=aliases.resource_type AND selected.resource_id=aliases.resource_id
            WHERE aliases.source_id='cms-npd' AND aliases.resource_type=b.resource_type
              AND aliases.{id_column}=b.{id_column} {alias_scope}) AS identity_count
        FROM {schema}.{binding} b
        JOIN {schema}.{_RESOURCE} r ON r.dataset_id=:dataset_id AND r.resource_type=b.resource_type
          AND r.resource_id=b.resource_id
        {_release_join(schema, query.kind)} {network_join}
        WHERE b.source_id='cms-npd' AND b.resource_type=:resource_type
          AND (CAST(:entity_id AS uuid) IS NULL OR b.{id_column}=CAST(:entity_id AS uuid))
          AND (CAST(:position AS uuid) IS NULL OR b.{id_column}>CAST(:position AS uuid))
        ORDER BY b.{id_column} LIMIT :page_size
    """
    entity_rows = (
        (
            await session.execute(
                text(statement),
                {
                    **generation,
                    "resource_type": KINDS[query.kind],
                    "entity_id": query.entity_id,
                    "position": position,
                    "page_size": query.limit + 1,
                },
            )
        )
        .mappings()
        .all()
    )
    if any(entity_row["identity_count"] != 1 for entity_row in entity_rows):
        raise DirectoryReadError(503)
    return entity_rows


# Only explicit outgoing assertions whose target kind exists in the gateway contract.
RELATION_FIELDS = {
    "organizations": (("partOf", "organization-part-of", "organizations"),),
    "sites": (
        ("managingOrganization", "site-managing-organization", "organizations"),
        ("partOf", "site-part-of", "sites"),
    ),
    "plans": (
        ("ownedBy", "plan-owned-by", "organizations"),
        ("administeredBy", "plan-administered-by", "organizations"),
        ("network", "plan-network", "networks"),
        ("coverageArea", "plan-coverage-area", "sites"),
        ("plan.network", "plan-network", "networks"),
        ("plan.coverageArea", "plan-coverage-area", "sites"),
        ("coverage.network", "plan-network", "networks"),
    ),
    "practitioner-roles": (
        ("organization", "role-organization", "organizations"),
        ("location", "role-site", "sites"),
        ("network", "role-network", "networks"),
        ("insurancePlan", "role-plan", "plans"),
    ),
}


def _relationship_fields(kind):
    return ", ".join(
        f"('{field}', '{relation}', '{target_kind}')" for field, relation, target_kind in RELATION_FIELDS[kind]
    )


async def cms_relationship_rows(session, schema, query, generation, resource_id, position):
    """Page exact source-qualified links; preserve the admission-time resolution."""
    statement = f"""WITH fields(reference_field, relationship_type, target_kind) AS
        (VALUES {_relationship_fields(query.kind)}), numbered AS (
          SELECT row_number() OVER (ORDER BY link.reference_field, link.parent_ordinal,
                   link.reference_ordinal) AS position,
                 fields.relationship_type, fields.target_kind,
                 left(link.target_reference, 513) AS reference, link.resolution_status,
                 left(link.period_start, 40) AS period_start,
                 left(link.period_end, 40) AS period_end
          FROM {schema}.provider_directory_cms_npd_relationship link
          JOIN fields USING (reference_field)
          JOIN {schema}.{_RESOURCE} r ON r.dataset_id=link.dataset_id
            AND r.resource_type=link.resource_type AND r.resource_id=link.resource_id
            AND r.payload_hash=link.source_payload_hash
          WHERE link.dataset_id=:dataset_id AND link.source_id='cms-npd'
            AND link.release_id=:release_id AND link.resource_type=:resource_type
            AND link.resource_id=:resource_id
        ) SELECT * FROM numbered WHERE position>:position
        ORDER BY position LIMIT :page_size"""
    return (
        (
            await session.execute(
                text(statement),
                {
                    **generation,
                    "resource_type": KINDS[query.kind],
                    "resource_id": resource_id,
                    "position": position or 0,
                    "page_size": query.limit + 1,
                },
            )
        )
        .mappings()
        .all()
    )


async def cms_network_plan_rows(session, schema, query, generation, resource_id, position):
    """Page the exact plan witnesses which establish the network role."""
    return (
        (
            await session.execute(
                text(f"""
        SELECT b.entity_id AS position, n.insurance_plan_resource_id AS evidence_id,
          'network-plan'::text AS relationship_type, 'plans'::text AS target_kind,
          'InsurancePlan/' || n.insurance_plan_resource_id AS reference
        FROM {schema}.{_NETWORK_EVIDENCE} n
        JOIN {schema}.{_RESOURCE} p ON p.dataset_id=:dataset_id AND p.resource_type='InsurancePlan'
          AND p.resource_id=n.insurance_plan_resource_id
          AND (p.acquired_resource_sha256 IS NULL OR p.acquired_resource_sha256=n.plan_payload_sha256)
        JOIN {schema}.{_IDENTITY} b ON b.source_id='cms-npd' AND b.resource_type='InsurancePlan'
          AND b.resource_id=p.resource_id
        WHERE n.source_id='cms-npd' AND n.release_id=:release_id AND n.network_resource_id=:resource_id
          AND p.payload_json::jsonb->'network_refs' @> jsonb_build_array('Organization/' || n.network_resource_id)
          AND (CAST(:position AS uuid) IS NULL OR b.entity_id>CAST(:position AS uuid))
        ORDER BY b.entity_id LIMIT :page_size
    """),
                {**generation, "resource_id": resource_id, "position": position, "page_size": query.limit + 1},
            )
        )
        .mappings()
        .all()
    )


async def cms_target_identity(session, schema, generation, kind, reference):
    """Resolve only an exact relative reference present in this accepted source release."""
    resource_type = KINDS[kind]
    prefix = resource_type + "/"
    if not reference.startswith(prefix) or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", reference[len(prefix) :]) is None:
        return None, "unresolved"
    binding, id_column = _binding(kind)
    scope = f"AND EXISTS ({_network_witness(schema)})" if kind == "networks" else ""
    rows = (
        await session.execute(
            text(f"""
        SELECT b.{id_column} AS entity_id FROM {schema}.{_RESOURCE} r
        LEFT JOIN {schema}.{binding} b ON b.source_id='cms-npd' AND b.resource_type=r.resource_type
          AND b.resource_id=r.resource_id
        {_release_join(schema, kind)}
        WHERE r.dataset_id=:dataset_id AND r.resource_type=:resource_type AND r.resource_id=:resource_id {scope}
        LIMIT 2
    """),
            {**generation, "resource_type": resource_type, "resource_id": reference[len(prefix) :]},
        )
    ).all()
    if len(rows) > 1:
        return None, "conflict"
    if not rows:
        return None, "unresolved"
    if rows[0][0] is None:
        raise DirectoryReadError(503)
    return str(rows[0][0]), "resolved"
