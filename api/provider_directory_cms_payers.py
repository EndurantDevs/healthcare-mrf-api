# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Existing payer identities, only through active reviewed CMS company bindings."""

from sqlalchemy import text

from api.provider_directory_entities_contract import opaque_directory_key


def _reviewed_rows(schema):
    return f"""
        SELECT b.payer_id, r.resource_id, e.organization_id,
          left(r.payload_json->>'name',2049) AS name,
          CASE WHEN jsonb_typeof(r.payload_json::jsonb->'active')='boolean'
               THEN r.payload_json->>'active' END AS active
        FROM {schema}.provider_directory_mrf_payer_binding b
        JOIN {schema}.provider_directory_mrf_payer_review_decision d
          ON d.decision_id=b.binding_decision_id AND d.action='bind'
          AND d.source_id=b.source_id AND d.resource_type=b.resource_type
          AND d.resource_id=b.resource_id AND d.payer_id=b.payer_id
        JOIN {schema}.mrf_payer p ON p.payer_id=b.payer_id
        JOIN {schema}.provider_directory_dataset_resource r
          ON r.dataset_id=:dataset_id AND r.resource_type='Organization' AND r.resource_id=b.resource_id
          AND (r.acquired_resource_sha256 IS NULL OR r.acquired_resource_sha256=d.source_payload_sha256)
        JOIN {schema}.provider_directory_entity_source_binding e
          ON e.source_id=b.source_id AND e.resource_type='Organization' AND e.resource_id=b.resource_id
        JOIN {schema}.provider_directory_entity_release_evidence facts
          ON facts.source_id=b.source_id AND facts.resource_type='Organization'
          AND facts.resource_id=b.resource_id AND facts.release_id=:release_id
          AND facts.payload_sha256=d.source_payload_sha256
        WHERE b.source_id='cms-npd' AND b.resource_type='Organization'
          AND b.payer_id ~ '^[A-Za-z0-9_-]{{1,64}}$'
          AND NOT EXISTS (SELECT 1 FROM {schema}.provider_directory_mrf_payer_review_decision closure
              WHERE closure.prior_decision_id=d.decision_id AND closure.action='close')
    """


async def payer_generation(session, schema, key, generation):
    """Pin immutable review decisions as well as CMS publication, including closures."""
    decision_count = (
        await session.execute(
            text(f"""
        SELECT count(*) FROM {schema}.provider_directory_mrf_payer_review_decision WHERE source_id='cms-npd'
    """)
        )
    ).scalar_one()
    return {
        **generation,
        "generation_id": opaque_directory_key(
            key,
            "gen_",
            generation["generation_id"],
            "reviewed-payers",
            decision_count,
        ),
    }


async def cms_payer_rows(session, schema, query, generation, position=None):
    """Seek existing opaque payer IDs and label conflicting CMS assertions explicitly."""
    return (
        (
            await session.execute(
                text(f"""
        WITH reviewed AS NOT MATERIALIZED ({_reviewed_rows(schema)}), page AS (
            SELECT DISTINCT payer_id COLLATE "C" AS payer_id FROM reviewed
            WHERE (CAST(:entity_id AS text) IS NULL OR payer_id=:entity_id)
              AND (CAST(:position AS text) IS NULL OR payer_id COLLATE "C">CAST(:position AS text) COLLATE "C")
            ORDER BY payer_id LIMIT :page_size
        )
        SELECT page.payer_id AS entity_id, 'Organization'::text AS resource_type,
          min(r.resource_id) AS resource_id,
          CASE WHEN min(r.name)=max(r.name)
               THEN min(r.name) END AS name,
          CASE WHEN min(r.active)=max(r.active)
               THEN min(r.active) END AS active,
          CASE WHEN min(r.name) IS DISTINCT FROM max(r.name)
               THEN 'conflict' ELSE 'unknown' END AS status,
          NULL::text AS period_start, NULL::text AS period_end, NULL::text AS plan_evidence
        FROM page JOIN reviewed r ON r.payer_id=page.payer_id GROUP BY page.payer_id
        ORDER BY page.payer_id COLLATE "C"
    """),
                {**generation, "entity_id": query.entity_id, "position": position, "page_size": query.limit + 1},
            )
        )
        .mappings()
        .all()
    )


async def cms_payer_organization_rows(session, schema, query, generation, resource_id, position):
    """Expose the exact reviewed link, never payer ownership of a plan or network."""
    return (
        (
            await session.execute(
                text(f"""
        SELECT r.organization_id AS position, 'payer-source-organization'::text AS relationship_type,
          'organizations'::text AS target_kind, 'Organization/' || r.resource_id AS reference,
          r.resource_id AS evidence_id
        FROM ({_reviewed_rows(schema)}) r WHERE r.payer_id=:entity_id
          AND (CAST(:position AS uuid) IS NULL OR r.organization_id>CAST(:position AS uuid))
        ORDER BY r.organization_id LIMIT :page_size
    """),
                {**generation, "entity_id": query.entity_id, "position": position, "page_size": query.limit + 1},
            )
        )
        .mappings()
        .all()
    )
