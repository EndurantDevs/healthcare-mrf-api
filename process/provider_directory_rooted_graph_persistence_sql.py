# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set-wise root initialization and root-closure SQL."""

from process.provider_directory_rooted_graph_store_support import (
    ACQUISITION_TABLE,
    DATASET_RESOURCE_TABLE,
    EDGE_TABLE,
    ENDPOINT_TABLE,
    RESOURCE_TABLE,
    WORK_TABLE,
    table_ref,
)


def _root_network_references_sql() -> str:
    edge = table_ref(EDGE_TABLE)
    work = table_ref(WORK_TABLE)
    return f"""
        ARRAY(
            SELECT DISTINCT 'Organization/' || anchor.target_resource_id
              FROM {edge} AS anchor
              JOIN {work} AS anchor_query
                ON anchor_query.acquisition_id = anchor.acquisition_id
               AND anchor_query.query_id = anchor.query_id
               AND anchor_query.attempt_count = anchor.attempt
             WHERE anchor.acquisition_id = :acquisition_id
               AND anchor.closure_scope = 'root'
               AND anchor_query.status = 'completed'
               AND anchor.target_resource_type = 'Organization'
               AND (
                   (anchor.source_resource_type IN (
                       'PractitionerRole', 'OrganizationAffiliation'
                   ) AND anchor.field_path LIKE 'network[%')
                   OR (
                       anchor.source_resource_type = 'PractitionerRole'
                       AND anchor.field_path LIKE 'extension[%.valueReference'
                   )
               )
             ORDER BY 1
        )
    """


def _terminal_work_sql(alias: str) -> str:
    return (
        f"({alias}.status = 'completed' OR ("
        f"{alias}.status = 'error' AND "
        f"COALESCE({alias}.error_code, '') = 'transport_timeout'))"
    )


def _root_work_drained_sql() -> str:
    work = table_ref(WORK_TABLE)
    dataset_resource = table_ref(DATASET_RESOURCE_TABLE)
    return f"""
        EXISTS (
            SELECT 1 FROM {work} AS root_query
             WHERE root_query.acquisition_id = :acquisition_id
               AND root_query.closure_scope = 'root'
        )
        AND NOT EXISTS (
            SELECT 1 FROM {work} AS root_query
             WHERE root_query.acquisition_id = :acquisition_id
               AND root_query.closure_scope = 'root'
               AND NOT {_terminal_work_sql('root_query')}
        )
        AND NOT EXISTS (
            SELECT member.resource_id
              FROM {dataset_resource} AS member
             WHERE member.dataset_id = header.root_dataset_id
               AND member.resource_type = 'Practitioner'
            EXCEPT
            SELECT root_query.reference_id
              FROM {work} AS root_query
             WHERE root_query.acquisition_id = :acquisition_id
               AND root_query.kind = 'exact_reference_search'
               AND root_query.resource_type = 'PractitionerRole'
               AND root_query.closure_scope = 'root'
               AND {_terminal_work_sql('root_query')}
        )
        AND NOT EXISTS (
            SELECT root_query.reference_id
              FROM {work} AS root_query
             WHERE root_query.acquisition_id = :acquisition_id
               AND root_query.kind = 'exact_reference_search'
               AND root_query.resource_type = 'PractitionerRole'
               AND root_query.closure_scope = 'root'
            EXCEPT
            SELECT member.resource_id
              FROM {dataset_resource} AS member
             WHERE member.dataset_id = header.root_dataset_id
               AND member.resource_type = 'Practitioner'
        )
    """


def _direct_reference_closure_sql() -> str:
    edge = table_ref(EDGE_TABLE)
    work = table_ref(WORK_TABLE)
    return f"""
        NOT EXISTS (
            SELECT 1 FROM {edge} AS reference_edge
              JOIN {work} AS source_query
                ON source_query.acquisition_id = reference_edge.acquisition_id
               AND source_query.query_id = reference_edge.query_id
               AND source_query.attempt_count = reference_edge.attempt
             WHERE reference_edge.acquisition_id = :acquisition_id
               AND reference_edge.closure_scope = 'root'
               AND source_query.status = 'completed'
               AND reference_edge.target_resource_type IN (
                   'Organization', 'Location', 'HealthcareService', 'Endpoint'
               )
               AND NOT EXISTS (
                   SELECT 1 FROM {work} AS target_query
                    WHERE target_query.acquisition_id = :acquisition_id
                      AND target_query.kind = 'direct_read'
                      AND target_query.reference_type =
                          reference_edge.target_resource_type
                      AND target_query.reference_id =
                          reference_edge.target_resource_id
                      AND target_query.closure_scope = 'root'
                      AND {_terminal_work_sql('target_query')}
               )
        )
    """


def _organization_affiliation_closure_sql() -> str:
    resource = table_ref(RESOURCE_TABLE)
    work = table_ref(WORK_TABLE)
    return f"""
        NOT EXISTS (
            SELECT 1 FROM {resource} AS organization
              JOIN {work} AS organization_query
                ON organization_query.acquisition_id = organization.acquisition_id
               AND organization_query.query_id = organization.query_id
               AND organization_query.attempt_count = organization.attempt
             WHERE organization.acquisition_id = :acquisition_id
               AND organization.resource_type = 'Organization'
               AND organization.closure_scope = 'root'
               AND organization_query.status = 'completed'
               AND NOT EXISTS (
                   SELECT 1 FROM {work} AS affiliation_query
                    WHERE affiliation_query.acquisition_id = :acquisition_id
                      AND affiliation_query.kind = 'exact_reference_search'
                      AND affiliation_query.resource_type =
                          'OrganizationAffiliation'
                      AND affiliation_query.reference_id = organization.resource_id
                      AND affiliation_query.closure_scope = 'root'
                      AND {_terminal_work_sql('affiliation_query')}
               )
        )
    """


def root_closure_sql() -> str:
    """Prove known root work drained, not descendants of timed-out queries."""

    acquisition = table_ref(ACQUISITION_TABLE)
    endpoint = table_ref(ENDPOINT_TABLE)
    work = table_ref(WORK_TABLE)
    return f"""
        SELECT endpoint.canonical_api_base,
               {_root_network_references_sql()} AS root_network_references,
               (
                   {_root_work_drained_sql()}
                   AND {_direct_reference_closure_sql()}
                   AND {_organization_affiliation_closure_sql()}
               ) AS root_frontier_drained,
               (
                   SELECT count(*)::bigint FROM {work} AS census_query
                    WHERE census_query.acquisition_id = :acquisition_id
                      AND census_query.kind = 'full_insurance_plan_census'
               ) AS census_count
          FROM {acquisition} AS header
          JOIN {endpoint} AS endpoint
            ON endpoint.endpoint_id = header.acquisition_endpoint_id
         WHERE header.acquisition_id = :acquisition_id
           AND header.status = 'building';
    """


__all__ = ("root_closure_sql",)
