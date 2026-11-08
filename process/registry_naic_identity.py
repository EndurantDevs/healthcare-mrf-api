# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allocate typed company-listing identities from a complete closed landing."""

from uuid import uuid4

from process.registry_naic_observations import naic_resolution_sql
from process.registry_source_observation_store import _TRIM_CHARACTERS, _identifier


async def materialize_naic_identities(connection, namespace, relation, landing):
    """Only HLTH creates insurer heads; other company rows retain honest gaps."""
    from process.registry_identity_materialization import (
        _candidate_totals,
        _group_candidates,
        _insert_heads,
        _validate_binding_references,
    )

    staging = _identifier("registry_naic_source_" + uuid4().hex)
    companies = _identifier("registry_naic_companies_" + uuid4().hex)
    await connection.execute(
        f"CREATE TEMP TABLE {staging} ON COMMIT DROP AS " + naic_resolution_sql(namespace, relation), _TRIM_CHARACTERS
    )
    await _validate_binding_references(connection, namespace, staging)
    await connection.execute(f"""CREATE TEMP TABLE {companies} ON COMMIT DROP AS
      SELECT naic_company,MIN(ein) AS ein,
        MIN(legal_name) FILTER(WHERE member_name='HLTH.csv') AS legal_name,
        'source_company'::text AS display_name_origin,
        (array_agg(source_record_key ORDER BY source_record_key))[1] AS evidence_key,
        NOT bool_or(company_conflicting) AS compatible,
        bool_or(member_name='HLTH.csv') AS has_health_evidence,
        COALESCE(bool_and(legal_name IS NOT NULL AND legal_name<>'' AND char_length(legal_name)<=512)
          FILTER(WHERE member_name='HLTH.csv'),false) AS supported_name,
        (array_agg(company_id ORDER BY company_id) FILTER(WHERE company_id IS NOT NULL))[1] AS company_id,
        false AS is_new
      FROM {staging} WHERE status<>'rejected' AND naic_company IS NOT NULL GROUP BY naic_company""")
    await connection.execute(f"""UPDATE {companies} SET company_id=gen_random_uuid(),is_new=true
      WHERE compatible AND has_health_evidence AND supported_name AND company_id IS NULL""")
    groups = await _group_candidates(connection, namespace, staging)
    companies_created, groups_created = await _insert_heads(connection, namespace, companies, groups)
    bindings_created = await _bind_naic_candidates(connection, namespace, companies, groups, landing.snapshot_id)
    totals = await _candidate_totals(connection, companies, groups)
    totals["unresolved_gpnm_companies"] = await connection.fetchval(
        f"SELECT COUNT(*) FROM {companies} WHERE NOT has_health_evidence AND company_id IS NULL"
    )
    await connection.execute(f"DROP TABLE {staging},{companies},{groups}")
    return totals | dict(
        source_rows=landing.expected_rows,
        companies_created=companies_created,
        groups_created=groups_created,
        bindings_created=bindings_created,
        replayed=False,
    )


async def _bind_naic_candidates(connection, namespace, companies, groups, snapshot_id):
    from process.registry_identity_materialization import RegistryMaterializationError

    bindings_created = await connection.fetchval(
        f"""WITH candidates AS (
      SELECT 'company' AS kind,'naic_company' AS system,naic_company AS identifier,company_id AS entity_id,evidence_key
        FROM {companies} WHERE compatible AND company_id IS NOT NULL
      UNION ALL SELECT 'company','ein',ein,company_id,evidence_key FROM {companies}
        WHERE compatible AND company_id IS NOT NULL AND ein IS NOT NULL
      UNION ALL SELECT 'group','naic_group',group_code,group_id,evidence_key FROM {groups} WHERE group_id IS NOT NULL),
      inserted AS (INSERT INTO {namespace}.registry_identifier_binding
        (entity_kind,identifier_system,identifier_value,entity_id,snapshot_id,evidence_key)
        SELECT kind,system,identifier,entity_id,$1,evidence_key FROM candidates ORDER BY kind,system,identifier
        ON CONFLICT(entity_kind,identifier_system,identifier_value) DO NOTHING RETURNING 1)
      SELECT COUNT(*) FROM inserted""",
        snapshot_id,
    )
    invalid = await connection.fetchval(f"""SELECT EXISTS(
      SELECT 1 FROM {companies} c WHERE compatible AND company_id IS NOT NULL AND (
        NOT EXISTS(SELECT 1 FROM {namespace}.company_registry h WHERE h.company_id=c.company_id)
        OR NOT EXISTS(SELECT 1 FROM {namespace}.registry_identifier_binding b WHERE b.entity_kind='company'
          AND b.identifier_system='naic_company' AND b.identifier_value=c.naic_company AND b.entity_id=c.company_id)
        OR (ein IS NOT NULL AND NOT EXISTS(SELECT 1 FROM {namespace}.registry_identifier_binding b WHERE b.entity_kind='company'
          AND b.identifier_system='ein' AND b.identifier_value=c.ein AND b.entity_id=c.company_id)))
      UNION ALL SELECT 1 FROM {groups} g WHERE group_id IS NOT NULL AND (
        NOT EXISTS(SELECT 1 FROM {namespace}.company_group_registry h WHERE h.group_id=g.group_id AND h.group_kind='naic_group')
        OR NOT EXISTS(SELECT 1 FROM {namespace}.registry_identifier_binding b WHERE b.entity_kind='group'
          AND b.identifier_system='naic_group' AND b.identifier_value=g.group_code AND b.entity_id=g.group_id)))""")
    if invalid:
        raise RegistryMaterializationError("Company listing identity sets do not reconcile")
    return bindings_created
