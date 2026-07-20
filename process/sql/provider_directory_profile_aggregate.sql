INSERT INTO {{TARGET_REF}} ("npi", "profile_json", "evidence_json", "source_ids", "endpoint_ids", "dataset_ids", "source_count", "independent_source_count", "fact_count", "generation_id", "published_at")
        WITH affected_npis AS MATERIALIZED (
            {{AFFECTED_NPIS_SQL}}
        ), scoped_evidence AS MATERIALIZED (
            SELECT evidence.*,
                   row_number() OVER (
                       PARTITION BY evidence.npi, evidence.fact_type,
                                    evidence.fact_key
                       ORDER BY evidence.observed_at DESC NULLS LAST,
                                evidence.source_id, evidence.resource_id
                   ) AS evidence_rank,
                   count(*) OVER (
                       PARTITION BY evidence.npi, evidence.fact_type,
                                    evidence.fact_key
                   ) AS evidence_count
              FROM {{EVIDENCE_REF}} AS evidence
              JOIN affected_npis ON affected_npis.npi = evidence.npi
        ), facts AS MATERIALIZED (
            SELECT npi, fact_type, fact_key,
                   (array_agg(
                       value_json
                       ORDER BY length(value_json::text) DESC,
                                observed_at DESC NULLS LAST,
                                source_id
                   ))[1] AS value_json,
                   array_agg(DISTINCT source_id ORDER BY source_id) AS source_ids,
                   count(DISTINCT source_id)::integer AS source_count,
                   count(DISTINCT endpoint_id)::integer AS independent_source_count,
                   max(evidence_count)::integer AS evidence_count,
                   jsonb_agg(
                       jsonb_strip_nulls(
                           jsonb_build_object(
                               'source_id', source_id,
                               'endpoint_id', endpoint_id,
                               'dataset_id', dataset_id,
                               'api_base', regexp_replace(
                                   regexp_replace(canonical_api_base, '[?#].*$', ''),
                                   '^([^:/?#]+://)[^/?#@]*@',
                                   '\1'
                               ),
                               'org_name', source_org_name,
                               'plan_name', source_plan_name,
                               'resource_type', resource_type,
                               'resource_id', resource_id,
                               'role_resource_id', role_resource_id,
                               'active', active,
                               'effective_start', effective_start,
                               'effective_end', effective_end,
                               'observed_at', observed_at
                           )
                       )
                       ORDER BY observed_at DESC NULLS LAST,
                                source_id, resource_id
                   ) FILTER (WHERE evidence_rank <= {{PROFILE_FACT_EVIDENCE_LIMIT}})
                       AS evidence_json
              FROM scoped_evidence
             GROUP BY npi, fact_type, fact_key
        ), ranked_facts AS MATERIALIZED (
            SELECT facts.*,
                   row_number() OVER (
                       PARTITION BY npi, fact_type
                       ORDER BY independent_source_count DESC,
                                source_count DESC, fact_key
                   ) AS fact_rank,
                   count(*) OVER (PARTITION BY npi, fact_type) AS fact_type_count
              FROM facts
        ), categories AS MATERIALIZED (
            SELECT npi, fact_type,
                   max(fact_type_count)::integer AS total,
                   jsonb_agg(
                       jsonb_build_object(
                           'value', value_json,
                           'source_ids', to_jsonb(source_ids),
                           'source_count', source_count,
                           'independent_source_count', independent_source_count
                       )
                       ORDER BY independent_source_count DESC,
                                source_count DESC, fact_key
                   ) FILTER (WHERE fact_rank <= {{PROFILE_FACT_LIMIT}}) AS compact_items,
                   jsonb_agg(
                       jsonb_build_object(
                           'value', value_json,
                           'source_ids', to_jsonb(source_ids),
                           'source_count', source_count,
                           'independent_source_count', independent_source_count,
                           'evidence_count', evidence_count,
                           'evidence_truncated', evidence_count > {{PROFILE_FACT_EVIDENCE_LIMIT}},
                           'evidence', evidence_json
                       )
                       ORDER BY independent_source_count DESC,
                                source_count DESC, fact_key
                   ) FILTER (WHERE fact_rank <= {{PROFILE_FACT_LIMIT}}) AS evidence_items
              FROM ranked_facts
             GROUP BY npi, fact_type
        ), profile_categories AS MATERIALIZED (
            SELECT npi,
                   jsonb_object_agg(
                       fact_type,
                       jsonb_build_object(
                           'total', total,
                           'truncated', total > {{PROFILE_FACT_LIMIT}},
                           'items', COALESCE(compact_items, '[]'::jsonb)
                       )
                       ORDER BY fact_type
                   ) AS compact_categories,
                   jsonb_object_agg(
                       fact_type,
                       jsonb_build_object(
                           'total', total,
                           'truncated', total > {{PROFILE_FACT_LIMIT}},
                           'items', COALESCE(evidence_items, '[]'::jsonb)
                       )
                       ORDER BY fact_type
                   ) AS evidence_categories,
                   sum(total)::integer AS fact_count
              FROM categories
             GROUP BY npi
        ), profile_source_rows AS MATERIALIZED (
            SELECT DISTINCT evidence.npi, evidence.source_id,
                   evidence.endpoint_id, evidence.dataset_id,
                   evidence.canonical_api_base, evidence.source_org_name,
                   evidence.source_plan_name
              FROM {{EVIDENCE_REF}} AS evidence
              JOIN affected_npis ON affected_npis.npi = evidence.npi
        ), profile_sources AS MATERIALIZED (
            SELECT evidence.npi,
                   array_agg(DISTINCT evidence.source_id ORDER BY evidence.source_id) AS source_ids,
                   array_agg(DISTINCT evidence.endpoint_id ORDER BY evidence.endpoint_id) AS endpoint_ids,
                   array_agg(DISTINCT evidence.dataset_id ORDER BY evidence.dataset_id) AS dataset_ids,
                   count(DISTINCT evidence.source_id)::integer AS source_count,
                   count(DISTINCT evidence.endpoint_id)::integer AS independent_source_count,
                   jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
                       'source_id', evidence.source_id,
                       'endpoint_id', evidence.endpoint_id,
                       'dataset_id', evidence.dataset_id,
                       'api_base', regexp_replace(
                           regexp_replace(
                               evidence.canonical_api_base,
                               '[?#].*$',
                               ''
                           ),
                           '^([^:/?#]+://)[^/?#@]*@',
                           '\1'
                       ),
                       'org_name', evidence.source_org_name,
                       'plan_name', evidence.source_plan_name
                   )) ORDER BY evidence.source_id, evidence.endpoint_id,
                               evidence.dataset_id) AS sources
              FROM profile_source_rows AS evidence
             GROUP BY evidence.npi
        )
        SELECT profile_sources.npi,
               jsonb_build_object(
                   'schema_version', {{PROFILE_SCHEMA_VERSION}},
                   'sources', profile_sources.sources,
                   'source_count', profile_sources.source_count,
                   'independent_source_count', profile_sources.independent_source_count,
                   'facts', profile_categories.compact_categories
               ) AS profile_json,
               jsonb_build_object(
                   'schema_version', {{PROFILE_SCHEMA_VERSION}},
                   'sources', profile_sources.sources,
                   'source_count', profile_sources.source_count,
                   'independent_source_count', profile_sources.independent_source_count,
                   'facts', profile_categories.evidence_categories
               ) AS evidence_json,
               profile_sources.source_ids,
               profile_sources.endpoint_ids,
               profile_sources.dataset_ids,
               profile_sources.source_count,
               profile_sources.independent_source_count,
               profile_categories.fact_count,
               CAST(:generation_id AS varchar),
               now()
          FROM profile_sources
          JOIN profile_categories
            ON profile_categories.npi = profile_sources.npi
        ON CONFLICT (npi) DO NOTHING;
