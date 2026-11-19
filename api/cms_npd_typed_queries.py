# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Entity and relationship reads over the typed tables of the current CMS NPD release."""

import re

from sqlalchemy import text

from api.provider_directory_cms_generation import SOURCE
from api.provider_directory_entities_contract import DirectoryReadError, opaque_directory_key
from api.provider_directory_medical_groups import _schema_name
from process.cms_npd_typed_schema import plan_names_network

KINDS = {
    "organizations": "Organization",
    "sites": "Location",
    "plans": "InsurancePlan",
    "networks": "Organization",
    "practitioner-roles": "PractitionerRole",
}
_RELEASE_SCHEMA = re.compile(r"cms_npd_[0-9a-f]{16}\Z")

# kind: (id column, remaining record columns, FROM clause); every kind projects the same record.
_ENTITIES = {
    "organizations": (
        "t.entity_id",
        "t.id AS resource_id, left(t.name, 2049) AS name, t.active::text AS active, NULL::text AS status, "
        "NULL::text AS period_start, NULL::text AS period_end, NULL::text AS plan_evidence",
        "{s}.organization t",
    ),
    "sites": (
        "t.id",
        "t.id::text AS resource_id, left(t.name, 2049) AS name, NULL::text AS active, left(t.status, 32) AS status, "
        "NULL::text AS period_start, NULL::text AS period_end, NULL::text AS plan_evidence",
        "{s}.location t",
    ),
    "plans": (
        "t.id",
        "t.id::text AS resource_id, left(t.name, 2049) AS name, NULL::text AS active, left(t.status, 32) AS status, "
        "t.period_start::text AS period_start, t.period_end::text AS period_end, NULL::text AS plan_evidence",
        "{s}.insurance_plan t",
    ),
    "practitioner-roles": (
        "t.id",
        "t.id::text AS resource_id, NULL::text AS name, t.active::text AS active, NULL::text AS status, "
        "t.period_start::text AS period_start, t.period_end::text AS period_end, NULL::text AS plan_evidence",
        "{s}.practitioner_role t",
    ),
    "networks": (
        "n.entity_id",
        "t.id AS resource_id, left(t.name, 2049) AS name, t.active::text AS active, NULL::text AS status, "
        "NULL::text AS period_start, NULL::text AS period_end, n.first_plan_id::text AS plan_evidence",
        "{s}.network n JOIN {s}.organization t ON t.id=n.organization_id",
    ),
}

# Outgoing edges of one parent: (field_no, ord, relationship_type, target_kind, target_id, period_start, period_end);
# NULL target = unresolved. Fields the source does not fill yield no rows.
_PLAN = "FROM {s}.insurance_plan p"
_PLAN_ID = "WHERE p.id=CAST(:resource_id AS uuid)"
_PLAN_PERIOD = "p.period_start::text, p.period_end::text"
_ROLE = "FROM {s}.practitioner_role r"
_ROLE_ID = "WHERE r.id=CAST(:resource_id AS uuid)"
_ROLE_PERIOD = "r.period_start::text, r.period_end::text"
_EDGES = {
    "organizations": """
        SELECT 1, 0, 'organization-part-of', 'organizations', t.entity_id::text, NULL::text, NULL::text
        FROM {s}.organization o LEFT JOIN {s}.organization t ON t.id=o.part_of
        WHERE o.id=:resource_id AND o.part_of IS NOT NULL""",
    "sites": """
        SELECT 1, 0, 'site-managing-organization', 'organizations', t.entity_id::text, NULL::text, NULL::text
        FROM {s}.location l LEFT JOIN {s}.organization t ON t.id=l.organization_id
        WHERE l.id=CAST(:resource_id AS uuid) AND l.organization_id IS NOT NULL
        UNION ALL
        SELECT 2, 0, 'site-part-of', 'sites', t.id::text, NULL::text, NULL::text
        FROM {s}.location l LEFT JOIN {s}.location t ON t.id=l.part_of
        WHERE l.id=CAST(:resource_id AS uuid) AND l.part_of IS NOT NULL""",
    "plans": f"""
        SELECT 1, 0, 'plan-owned-by', 'organizations', t.entity_id::text, {_PLAN_PERIOD}
        {_PLAN} LEFT JOIN {{s}}.organization t ON t.id=p.owned_by {_PLAN_ID} AND p.owned_by IS NOT NULL
        UNION ALL
        SELECT 2, ref.ord, 'plan-network', 'networks', w.entity_id::text, {_PLAN_PERIOD}
        {_PLAN} CROSS JOIN LATERAL unnest(p.network_ids) WITH ORDINALITY ref(id, ord)
        LEFT JOIN {{s}}.network w ON w.organization_id=ref.id {_PLAN_ID} AND ref.id IS NOT NULL
        UNION ALL
        SELECT 3, 0, 'plan-administered-by', 'organizations', t.entity_id::text, {_PLAN_PERIOD}
        {_PLAN} LEFT JOIN {{s}}.organization t ON t.id=p.administered_by
        {_PLAN_ID} AND p.administered_by IS NOT NULL
        UNION ALL
        SELECT 4, ref.ord, 'plan-coverage-area', 'sites', l.id::text, {_PLAN_PERIOD}
        {_PLAN} CROSS JOIN LATERAL unnest(p.coverage_area_ids) WITH ORDINALITY ref(id, ord)
        LEFT JOIN {{s}}.location l ON l.id=ref.id {_PLAN_ID} AND ref.id IS NOT NULL
        UNION ALL
        SELECT CASE nested.field WHEN 'plan.network' THEN 5 WHEN 'plan.coverageArea' THEN 6 ELSE 7 END, ref.ord,
          CASE nested.field WHEN 'plan.coverageArea' THEN 'plan-coverage-area' ELSE 'plan-network' END,
          CASE nested.field WHEN 'plan.coverageArea' THEN 'sites' ELSE 'networks' END,
          coalesce(w.entity_id, l.id)::text, ref.value->>'period_start', ref.value->>'period_end'
        {_PLAN} CROSS JOIN LATERAL jsonb_array_elements(p.nested_references) WITH ORDINALITY ref(value, ord)
        CROSS JOIN LATERAL (SELECT ref.value->>'field' AS field, ref.value->>'target' AS target) nested
        LEFT JOIN {{s}}.network w ON nested.field<>'plan.coverageArea' AND w.organization_id=nested.target
        LEFT JOIN {{s}}.location l
          ON l.id=CASE WHEN nested.field='plan.coverageArea' THEN CAST(nested.target AS uuid) END
        {_PLAN_ID}""",
    "practitioner-roles": f"""
        SELECT 1, 0, 'role-practitioner', 'providers', p.npi::text, {_ROLE_PERIOD}
        {_ROLE} LEFT JOIN {{s}}.practitioner p
          ON p.npi=r.practitioner_npi AND p.npi_explicit AND p.old_npis IS NULL
        {_ROLE_ID} AND r.practitioner_npi IS NOT NULL
        UNION ALL
        SELECT 2, 0, 'role-organization', 'organizations', t.entity_id::text, {_ROLE_PERIOD}
        {_ROLE} LEFT JOIN {{s}}.organization t ON t.id=r.organization_id
        {_ROLE_ID} AND r.organization_id IS NOT NULL
        UNION ALL
        SELECT 3, ref.ord, 'role-site', 'sites', l.id::text, {_ROLE_PERIOD}
        {_ROLE} CROSS JOIN LATERAL unnest(r.location_ids) WITH ORDINALITY ref(id, ord)
        LEFT JOIN {{s}}.location l ON l.id=ref.id {_ROLE_ID} AND ref.id IS NOT NULL
        UNION ALL
        SELECT 4, 0, 'role-network', 'networks', w.entity_id::text, {_ROLE_PERIOD}
        {_ROLE} LEFT JOIN {{s}}.network w ON w.organization_id=r.network_id {_ROLE_ID} AND r.network_id IS NOT NULL
        UNION ALL
        SELECT 5, ref.ord, 'role-network', 'networks', w.entity_id::text, {_ROLE_PERIOD}
        {_ROLE} CROSS JOIN LATERAL unnest(r.network_ids) WITH ORDINALITY ref(id, ord)
        LEFT JOIN {{s}}.network w ON w.organization_id=ref.id {_ROLE_ID} AND ref.id IS NOT NULL
        UNION ALL
        SELECT 6, ref.ord, 'role-plan', 'plans', p.id::text, {_ROLE_PERIOD}
        {_ROLE} CROSS JOIN LATERAL unnest(r.insurance_plan_ids) WITH ORDINALITY ref(id, ord)
        LEFT JOIN {{s}}.insurance_plan p ON p.id=ref.id {_ROLE_ID} AND ref.id IS NOT NULL""",
}


async def current_cms_release(session, key):
    """Pin the one current release; its schema name is checked before it reaches any SQL text."""
    release_rows = (
        (
            await session.execute(
                text(
                    f"SELECT release_id, schema_name, published_at FROM {_schema_name()}.cms_npd_release "
                    "WHERE status='current' LIMIT 2"
                )
            )
        )
        .mappings()
        .all()
    )
    if len(release_rows) != 1:
        raise DirectoryReadError(503)
    release = release_rows[0]
    schema_name = release["schema_name"]
    if not isinstance(schema_name, str) or not _RELEASE_SCHEMA.fullmatch(schema_name):
        raise DirectoryReadError(503)
    published_at = release["published_at"]
    return {
        "release_id": release["release_id"],
        "schema": f'"{schema_name}"',
        "observed_at": published_at,
        "generation_id": None
        if key is None
        else opaque_directory_key(key, "gen_", SOURCE, release["release_id"], schema_name, published_at.isoformat()),
    }


async def cms_entity_rows(session, query, generation, position=None):
    """Seek one page (plus a lookahead row) of one kind in id order."""
    id_column, columns, source = _ENTITIES[query.kind]
    statement = f"""
        SELECT {id_column} AS entity_id, '{KINDS[query.kind]}'::text AS resource_type, {columns}
        FROM {source.format(s=generation["schema"])}
        WHERE (CAST(:entity_id AS uuid) IS NULL OR {id_column}=CAST(:entity_id AS uuid))
          AND (CAST(:position AS uuid) IS NULL OR {id_column}>CAST(:position AS uuid))
        ORDER BY {id_column} LIMIT :page_size"""
    parameters_by_name = {"entity_id": query.entity_id, "position": position, "page_size": query.limit + 1}
    return (await session.execute(text(statement), parameters_by_name)).mappings().all()


async def cms_relationship_rows(session, query, generation, resource_id, position):
    """Number the parent's outgoing references and resolve each target in this release."""
    statement = f"""
        SELECT * FROM (
          SELECT row_number() OVER (ORDER BY field_no, ord) AS position, relationship_type, target_kind, target_id,
            period_start, period_end
          FROM ({_EDGES[query.kind].format(s=generation["schema"])})
            edges(field_no, ord, relationship_type, target_kind, target_id, period_start, period_end)
        ) numbered WHERE position>:position ORDER BY position LIMIT :page_size"""
    parameters_by_name = {"resource_id": resource_id, "position": position or 0, "page_size": query.limit + 1}
    return (await session.execute(text(statement), parameters_by_name)).mappings().all()


async def cms_network_plan_rows(session, query, generation, resource_id, position):
    """Page the plans naming this network organization, in plan id order."""
    statement = f"""
        SELECT p.id AS position, p.id::text AS evidence_id, 'network-plan'::text AS relationship_type,
          'plans'::text AS target_kind, p.id::text AS target_id
        FROM {generation["schema"]}.insurance_plan p
        WHERE {plan_names_network("p", "CAST(:resource_id AS text)")}
          AND (CAST(:position AS uuid) IS NULL OR p.id>CAST(:position AS uuid))
        ORDER BY p.id LIMIT :page_size"""
    parameters_by_name = {"resource_id": resource_id, "position": position, "page_size": query.limit + 1}
    return (await session.execute(text(statement), parameters_by_name)).mappings().all()
