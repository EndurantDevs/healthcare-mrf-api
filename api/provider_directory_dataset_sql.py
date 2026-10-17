# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dataset-fenced Provider Directory resources and readiness capabilities."""

from db.provider_directory_readiness import dataset_relation_ready_predicate

PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE = "provider_directory_dataset_insurance_plan"
PROVIDER_DIRECTORY_DATASET_RELATION_VERSION = "1"

PROVIDER_DIRECTORY_EVIDENCE_CAPABILITY_SQL = """
    WITH requested_columns AS (
        SELECT column_table_name, column_name
          FROM unnest(
               CAST(:column_table_names AS varchar[]),
               CAST(:column_names AS varchar[])
          ) AS requested(column_table_name, column_name)
    )
    SELECT requested.table_name,
           to_regclass(:schema || '.' || requested.table_name)
               IS NOT NULL AS is_available
      FROM unnest(CAST(:table_names AS varchar[]))
           AS requested(table_name)
    UNION ALL
    SELECT requested.column_table_name || '.' || requested.column_name,
           EXISTS (
               SELECT 1
                 FROM pg_attribute
                WHERE attrelid = to_regclass(
                      :schema || '.' || requested.column_table_name
                )
                  AND attname = requested.column_name
                  AND attnum > 0
                  AND NOT attisdropped
           ) AS is_available
      FROM requested_columns AS requested
    UNION ALL
    SELECT requested.index_name,
           EXISTS (
               SELECT 1
                 FROM pg_catalog.pg_index AS index_record
                WHERE index_record.indexrelid = to_regclass(:schema || '.' || requested.index_name)
                  AND index_record.indrelid = to_regclass(:schema || '.provider_directory_endpoint_dataset')
                  AND index_record.indisvalid AND index_record.indisready AND index_record.indislive
           ) AS is_available
      FROM unnest(CAST(:readiness_index_names AS varchar[])) AS requested(index_name)
"""


def _dataset_relation_ready_sql(metadata_key: str) -> str:
    return f"""
        COALESCE(dataset.{metadata_key}_metadata ->> 'complete', 'false') = 'true'
        AND COALESCE(dataset.{metadata_key}_metadata ->> 'version', '') = '{PROVIDER_DIRECTORY_DATASET_RELATION_VERSION}'
        AND COALESCE(dataset.{metadata_key}_metadata ->> 'dataset_id', '') = dataset.dataset_id"""


def _indexed_dataset_relation_ready_sql(schema: str, metadata_key: str) -> str:
    predicate = dataset_relation_ready_predicate(
        metadata_key, alias="ready", version=PROVIDER_DIRECTORY_DATASET_RELATION_VERSION
    )
    return (
        f"EXISTS (SELECT 1 FROM {schema}.provider_directory_endpoint_dataset AS ready "
        f"WHERE ready.dataset_id = dataset.dataset_id AND {predicate})"
    )


def _provider_directory_current_plan_resources_sql(
    schema: str,
    has_dataset_insurance_plan: bool,
) -> str:
    """Read immutable plan payloads from the compact projection when available."""
    if not has_dataset_insurance_plan:
        return """
    current_plan_resources AS NOT MATERIALIZED (
        SELECT resource.*
          FROM current_resources AS resource
         WHERE resource.resource_type = 'InsurancePlan'
    )
        """
    return f"""
    current_plan_resources AS NOT MATERIALIZED (
        SELECT source.source_id, source.canonical_api_base,
               dataset.dataset_id, dataset.run_id,
               dataset.published_at AS dataset_published_at,
               dataset.dataset_network_plan_complete,
               dataset.dataset_affiliation_organization_complete,
               'InsurancePlan'::varchar AS resource_type,
               resource.resource_id,
               resource.payload_json::jsonb AS payload_json
          FROM {schema}.provider_directory_source AS source
          JOIN current_datasets AS dataset
            ON dataset.endpoint_id = source.endpoint_id
          JOIN {schema}.{PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE} AS resource
            ON resource.dataset_id = dataset.dataset_id
    )
    """


_CURRENT_RESOURCE_CTES_SQL = """
    current_endpoint_counts AS MATERIALIZED (
        SELECT dataset.endpoint_id
          FROM {schema}.provider_directory_endpoint_dataset AS dataset
         WHERE dataset.is_current IS TRUE
      GROUP BY dataset.endpoint_id
        HAVING COUNT(*) = 1
    ), current_dataset_metadata AS MATERIALIZED (
        SELECT dataset.endpoint_id, dataset.dataset_id,
               COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar AS run_id,
               dataset.published_at{metadata_projection_sql}
          FROM {schema}.provider_directory_endpoint_dataset AS dataset
          JOIN current_endpoint_counts AS current_endpoint
            ON current_endpoint.endpoint_id = dataset.endpoint_id
         WHERE dataset.is_current IS TRUE
           AND dataset.endpoint_id IN (
               SELECT requested_source.endpoint_id
                 FROM {schema}.provider_directory_source AS requested_source
                WHERE requested_source.source_id = ANY(:source_ids)
           )
           AND dataset.status = 'published'
           AND dataset.published_at IS NOT NULL
           AND dataset.superseded_at IS NULL
           AND COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id) IS NOT NULL
    ), current_datasets AS MATERIALIZED (
        SELECT dataset.endpoint_id, dataset.dataset_id, dataset.run_id,
               dataset.published_at,
               ({network_plan_ready_sql}) AS dataset_network_plan_complete,
               ({affiliation_ready_sql})
                   AS dataset_affiliation_organization_complete
          FROM current_dataset_metadata AS dataset
    ), current_resources AS NOT MATERIALIZED (
        SELECT source.source_id, source.canonical_api_base,
               dataset.dataset_id, dataset.run_id,
               dataset.published_at AS dataset_published_at,
               dataset.dataset_network_plan_complete,
               dataset.dataset_affiliation_organization_complete,
               resource.resource_type, resource.resource_id,
               resource.payload_json::jsonb AS payload_json
          FROM {schema}.provider_directory_source AS source
          JOIN current_datasets AS dataset
            ON dataset.endpoint_id = source.endpoint_id
          JOIN {schema}.provider_directory_dataset_resource AS resource
            ON resource.dataset_id = dataset.dataset_id
    ), {current_plan_resources_sql}
"""


def _provider_directory_current_resource_ctes_sql(
    schema: str,
    has_dataset_insurance_plan: bool = False,
    has_dataset_readiness_indexes: bool = False,
) -> str:
    """Resolve resources through the one current, fully published endpoint dataset."""
    network_plan_ready_sql = _dataset_relation_ready_sql("dataset_network_plan")
    affiliation_ready_sql = _dataset_relation_ready_sql("dataset_affiliation_organization")
    metadata_projection_sql = """,
               dataset.publication_metadata_json::jsonb
                   -> 'dataset_network_plan' AS dataset_network_plan_metadata,
               dataset.publication_metadata_json::jsonb
                   -> 'dataset_affiliation_organization' AS dataset_affiliation_organization_metadata"""
    if has_dataset_readiness_indexes:
        metadata_projection_sql = ""
        network_plan_ready_sql = _indexed_dataset_relation_ready_sql(schema, "dataset_network_plan")
        affiliation_ready_sql = _indexed_dataset_relation_ready_sql(schema, "dataset_affiliation_organization")
    return _CURRENT_RESOURCE_CTES_SQL.format(
        schema=schema,
        metadata_projection_sql=metadata_projection_sql,
        network_plan_ready_sql=network_plan_ready_sql,
        affiliation_ready_sql=affiliation_ready_sql,
        current_plan_resources_sql=_provider_directory_current_plan_resources_sql(schema, has_dataset_insurance_plan),
    )
