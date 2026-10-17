# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound exact current control rows before hashing a scoped retained read."""

from sqlalchemy import text

from api.provider_directory_cms_network_scope import cms_network_predicate
from api.provider_directory_entities_contract import DirectoryReadError, opaque_directory_key


async def scoped_cms_control_generation(session, schema, key, generation):
    """Include pointer edits, complete decisions, closures and payer existence."""
    source_schema = generation["network_scope"].source_schema
    selected = cms_network_predicate(generation, "organizations", "r.resource_id")
    include_sites = generation["scope_kind"] in {"sites", "practitioner-roles"} or (
        generation["scope_kind"] == "plans" and generation["scope_shape"] == "relationships"
    )
    sites = cms_network_predicate(generation, "sites", "r.resource_id") if include_sites else " AND false"
    statement = f"""WITH selected AS MATERIALIZED (
      SELECT b.organization_id AS entity_id,r.resource_id,r.resource_type
      FROM {source_schema}.provider_directory_dataset_resource r
      JOIN {source_schema}.provider_directory_entity_source_binding b
        ON b.source_id='cms-npd' AND b.resource_type='Organization' AND b.resource_id=r.resource_id
      WHERE r.dataset_id=:dataset_id AND r.resource_type='Organization' {selected}
      UNION ALL SELECT b.site_id,r.resource_id,r.resource_type
      FROM {source_schema}.provider_directory_dataset_resource r
      JOIN {source_schema}.provider_directory_entity_source_binding b
        ON b.source_id='cms-npd' AND b.resource_type='Location' AND b.resource_id=r.resource_id
      WHERE r.dataset_id=:dataset_id AND r.resource_type='Location' {sites}
    ), redirects AS MATERIALIZED (
      SELECT to_jsonb(d) AS value FROM {schema}.provider_directory_entity_redirect_decision d
      WHERE d.source_id='cms-npd' AND to_jsonb(d)->>'old_entity_id' IN
        (SELECT entity_id::text FROM selected)
    ), payer_bindings AS MATERIALIZED (
      SELECT b.* FROM {schema}.provider_directory_mrf_payer_binding b
      WHERE b.source_id='cms-npd' AND b.resource_id IN (SELECT resource_id FROM selected)
    ), payer_decisions AS MATERIALIZED (
      SELECT to_jsonb(d) AS value FROM {schema}.provider_directory_mrf_payer_review_decision d
      WHERE d.source_id='cms-npd' AND d.resource_id IN (SELECT resource_id FROM selected)
    ), rows AS MATERIALIZED (
      SELECT 'redirect' AS kind,to_jsonb(p) AS value FROM {schema}.provider_directory_entity_redirect p
      WHERE p.source_id='cms-npd' AND p.old_entity_id IN (SELECT entity_id FROM selected)
      UNION ALL SELECT 'redirect-decision',value FROM redirects
      UNION ALL SELECT 'redirect-closure',to_jsonb(d) FROM {schema}.provider_directory_entity_redirect_decision d
        WHERE to_jsonb(d)->>'prior_decision_id' IN (SELECT value->>'decision_id' FROM redirects)
      UNION ALL SELECT 'payer-binding',to_jsonb(b) FROM payer_bindings b
      UNION ALL SELECT 'payer-decision',value FROM payer_decisions
      UNION ALL SELECT 'payer-closure',to_jsonb(d) FROM {schema}.provider_directory_mrf_payer_review_decision d
        WHERE d.prior_decision_id::text IN (SELECT value->>'decision_id' FROM payer_decisions)
      UNION ALL SELECT 'payer',jsonb_build_object('payer_id',p.payer_id) FROM {schema}.mrf_payer p
        WHERE p.payer_id IN (SELECT payer_id FROM payer_bindings)
      LIMIT 5001
    ), bounds AS (
      SELECT count(*) AS rows,coalesce(sum(octet_length(value::text)+octet_length(kind)),0) AS bytes FROM rows
    ) SELECT CASE WHEN rows<=5000 AND bytes<=1048576 THEN
      (SELECT encode(sha256(convert_to(coalesce(jsonb_agg(jsonb_build_array(kind,value)
        ORDER BY kind,value::text),'[]'::jsonb)::text,'UTF8')),'hex') FROM rows) END FROM bounds"""
    digest = await session.scalar(text(statement), generation)
    if digest is None:
        raise DirectoryReadError(503)
    return {
        **generation,
        "generation_id": opaque_directory_key(key, "gen_", generation["generation_id"], "cms-controls-v1", digest),
    }
