-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- Set-wise candidate work validation retains the original logical predicates.
WITH expected_header AS MATERIALIZED (
    SELECT * FROM __SCHEMA__.provider_directory_rooted_graph_acquisition
    WHERE acquisition_id=:acquisition_id AND status='building'
)
SELECT coalesce(jsonb_agg(failure.code),'[]'::jsonb) AS invalid
FROM expected_header CROSS JOIN LATERAL (
SELECT 'provider_directory_rooted_graph_work_invalid' AS code WHERE EXISTS (
        SELECT 1 FROM pg_temp.pdrg_work_stage AS incoming
        WHERE incoming.acquisition_id IS DISTINCT FROM :acquisition_id
           OR incoming.scope_id IS DISTINCT FROM expected_header.scope_id
           OR incoming.status<>'pending' OR incoming.attempt_count<>0 OR incoming.pagination_terminal
           OR incoming.created_at IS DISTINCT FROM transaction_timestamp()
           OR incoming.updated_at IS DISTINCT FROM transaction_timestamp()
           OR incoming.query_identity_json_text::jsonb IS DISTINCT FROM jsonb_build_object(
               'kind',incoming.kind,'page_size',CASE WHEN incoming.kind='direct_read' THEN NULL::integer ELSE 100 END,
               'pagination',CASE WHEN incoming.kind='direct_read' THEN 'forbidden' ELSE 'same-origin-source-issued-until-terminal' END,
               'reference',CASE WHEN incoming.reference_type IS NULL THEN NULL ELSE incoming.reference_type||'/'||incoming.reference_id END,
               'resource_type',incoming.resource_type,'search_parameter',incoming.search_parameter)
           OR incoming.query_identity_sha256 IS DISTINCT FROM encode(sha256(convert_to(incoming.query_identity_json_text,'UTF8')),'hex')
           OR incoming.query_id IS DISTINCT FROM 'pdrgq_' || substr(encode(sha256(convert_to(
               'healthporta.provider-directory.rooted-graph-identity.v1' || chr(31) || incoming.scope_id || chr(31)
               || incoming.query_identity_json_text,'UTF8')),'hex'),1,48)
    )
UNION ALL
SELECT 'provider_directory_rooted_graph_root_work_invalid' AS code WHERE :action='initialize' AND EXISTS (
        SELECT 1 FROM pg_temp.pdrg_work_stage AS incoming
        LEFT JOIN __ROOT_RESOURCE__ AS member
          ON member.dataset_id=expected_header.root_dataset_id AND member.resource_type='Practitioner'
         AND member.resource_id=incoming.reference_id
        WHERE incoming.resource_type<>'PractitionerRole' OR member.resource_id IS NULL
    )
UNION ALL
SELECT 'provider_directory_rooted_graph_root_work_invalid' AS code WHERE :action='derive' AND EXISTS (
        SELECT 1 FROM pg_temp.pdrg_work_stage WHERE resource_type='PractitionerRole' OR kind='full_insurance_plan_census'
    )
UNION ALL
SELECT 'provider_directory_rooted_graph_root_closure_frozen' AS code WHERE :action='derive' AND EXISTS (SELECT 1 FROM pg_temp.pdrg_work_stage WHERE closure_scope='root')
       AND EXISTS (SELECT 1 FROM __SCHEMA__.provider_directory_rooted_graph_work
                   WHERE acquisition_id=:acquisition_id AND kind='full_insurance_plan_census')
UNION ALL
SELECT 'provider_directory_rooted_graph_root_fixed_point_incomplete' AS code WHERE EXISTS (SELECT 1 FROM pg_temp.pdrg_work_stage AS incoming WHERE (:action = 'census' AND (
                incoming.kind <> 'full_insurance_plan_census'
                OR NOT EXISTS (
                    SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_work" AS root_query
                     WHERE root_query.acquisition_id = incoming.acquisition_id
                       AND root_query.closure_scope = 'root'
                )
                OR EXISTS (
                    SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_work" AS root_query
                     WHERE root_query.acquisition_id = incoming.acquisition_id
                       AND (root_query.closure_scope <> 'root'
                            OR NOT (root_query.status = 'completed' OR (root_query.status = 'error' AND root_query.error_code = 'transport_timeout')))
                )
                OR EXISTS (
                    SELECT member.resource_id
                      FROM __SCHEMA__."provider_directory_rooted_graph_acquisition" AS header
                      JOIN __ROOT_RESOURCE__ AS member
                        ON member.dataset_id = header.root_dataset_id
                     WHERE header.acquisition_id = incoming.acquisition_id
                       AND member.resource_type = 'Practitioner'
                    EXCEPT
                    SELECT root_query.reference_id FROM __SCHEMA__."provider_directory_rooted_graph_work" AS root_query
                     WHERE root_query.acquisition_id = incoming.acquisition_id
                       AND root_query.kind = 'exact_reference_search'
                       AND root_query.resource_type = 'PractitionerRole'
                       AND root_query.closure_scope = 'root'
                       AND (root_query.status = 'completed' OR (root_query.status = 'error' AND root_query.error_code = 'transport_timeout'))
                )
                OR EXISTS (
                    SELECT root_query.reference_id FROM __SCHEMA__."provider_directory_rooted_graph_work" AS root_query
                     WHERE root_query.acquisition_id = incoming.acquisition_id
                       AND root_query.kind = 'exact_reference_search'
                       AND root_query.resource_type = 'PractitionerRole'
                       AND root_query.closure_scope = 'root'
                    EXCEPT
                    SELECT member.resource_id
                      FROM __SCHEMA__."provider_directory_rooted_graph_acquisition" AS header
                      JOIN __ROOT_RESOURCE__ AS member
                        ON member.dataset_id = header.root_dataset_id
                     WHERE header.acquisition_id = incoming.acquisition_id
                       AND member.resource_type = 'Practitioner'
                )
                OR EXISTS (
                    SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_edge" AS reference_edge
                      JOIN __SCHEMA__."provider_directory_rooted_graph_work" AS source_query
                        ON source_query.acquisition_id =
                           reference_edge.acquisition_id
                       AND source_query.query_id = reference_edge.query_id
                       AND source_query.attempt_count = reference_edge.attempt
                     WHERE reference_edge.acquisition_id = incoming.acquisition_id
                       AND reference_edge.closure_scope = 'root'
                       AND source_query.status = 'completed'
                       AND reference_edge.target_resource_type IN (
                           'Organization', 'Location', 'HealthcareService', 'Endpoint'
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_work" AS target_query
                            WHERE target_query.acquisition_id = incoming.acquisition_id
                              AND target_query.kind = 'direct_read'
                              AND target_query.reference_type =
                                  reference_edge.target_resource_type
                              AND target_query.reference_id =
                                  reference_edge.target_resource_id
                              AND target_query.closure_scope = 'root'
                              AND (target_query.status = 'completed' OR (target_query.status = 'error' AND target_query.error_code = 'transport_timeout'))
                       )
                )
                OR EXISTS (
                    SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_resource" AS organization
                      JOIN __SCHEMA__."provider_directory_rooted_graph_work" AS organization_query
                        ON organization_query.acquisition_id =
                           organization.acquisition_id
                       AND organization_query.query_id = organization.query_id
                       AND organization_query.attempt_count = organization.attempt
                     WHERE organization.acquisition_id = incoming.acquisition_id
                       AND organization.resource_type = 'Organization'
                       AND organization.closure_scope = 'root'
                       AND organization_query.status = 'completed'
                       AND NOT EXISTS (
                           SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_work" AS affiliation_query
                            WHERE affiliation_query.acquisition_id =
                                  incoming.acquisition_id
                              AND affiliation_query.kind = 'exact_reference_search'
                              AND affiliation_query.resource_type =
                                  'OrganizationAffiliation'
                              AND affiliation_query.reference_id =
                                  organization.resource_id
                              AND affiliation_query.closure_scope = 'root'
                              AND (affiliation_query.status = 'completed' OR (affiliation_query.status = 'error' AND affiliation_query.error_code = 'transport_timeout'))
                       )
                )
            ) ))
UNION ALL
SELECT 'provider_directory_rooted_graph_discovery_invalid' AS code WHERE :action='derive' AND EXISTS (
        SELECT query_id FROM pg_temp.pdrg_work_stage WHERE kind='direct_read'
        EXCEPT
        SELECT incoming.query_id FROM pg_temp.pdrg_work_stage AS incoming
        JOIN __SCHEMA__.provider_directory_rooted_graph_edge AS proof
          ON proof.acquisition_id=:acquisition_id AND proof.query_id=incoming.discovered_by_query_id
         AND proof.source_resource_type=incoming.discovered_source_type
         AND proof.source_resource_id=incoming.discovered_source_id
         AND proof.edge_sha256=incoming.discovered_edge_sha256
         AND proof.target_resource_type=incoming.reference_type AND proof.target_resource_id=incoming.reference_id
         AND proof.closure_scope=incoming.closure_scope
        JOIN __SCHEMA__.provider_directory_rooted_graph_work AS parent
          ON parent.acquisition_id=proof.acquisition_id AND parent.query_id=proof.query_id
         AND parent.attempt_count=proof.attempt AND parent.status='completed'
         AND parent.terminal_at=transaction_timestamp()
        WHERE incoming.kind='direct_read'
    )
UNION ALL
SELECT 'provider_directory_rooted_graph_discovery_invalid' AS code WHERE :action='derive' AND EXISTS (
        SELECT query_id FROM pg_temp.pdrg_work_stage WHERE resource_type='OrganizationAffiliation'
        EXCEPT
        SELECT incoming.query_id FROM pg_temp.pdrg_work_stage AS incoming
        JOIN __SCHEMA__.provider_directory_rooted_graph_resource AS proof
          ON proof.acquisition_id=:acquisition_id AND proof.query_id=incoming.discovered_by_query_id
         AND proof.resource_type='Organization' AND proof.resource_id=incoming.reference_id
         AND proof.closure_scope=incoming.closure_scope
        JOIN __SCHEMA__.provider_directory_rooted_graph_work AS parent
          ON parent.acquisition_id=proof.acquisition_id AND parent.query_id=proof.query_id
         AND parent.attempt_count=proof.attempt AND parent.status='completed'
         AND parent.terminal_at=transaction_timestamp()
        WHERE incoming.resource_type='OrganizationAffiliation'
    )
) failure;
