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
    has_dataset_scope: bool = False,
) -> str:
    """Read immutable plan payloads from the compact projection when available."""
    source_projection_sql, source_join_sql = _dataset_source_sql(schema, has_dataset_scope)
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
        SELECT {source_projection_sql},
               dataset.dataset_id, dataset.run_id,
               dataset.published_at AS dataset_published_at,
               dataset.dataset_network_plan_complete,
               dataset.dataset_affiliation_organization_complete,
               'InsurancePlan'::varchar AS resource_type,
               resource.resource_id,
               resource.payload_json::jsonb AS payload_json
          {source_join_sql}
          JOIN {schema}.{PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE} AS resource
            ON resource.dataset_id = dataset.dataset_id
    )
    """


_CURRENT_DATASET_METADATA_CTES_SQL = """
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
    )
"""


_CURRENT_RESOURCE_CTES_SQL = """
    {dataset_metadata_ctes_sql}, current_datasets AS MATERIALIZED (
        SELECT dataset.endpoint_id, dataset.dataset_id, dataset.run_id,
               dataset.published_at{source_metadata_projection_sql},
               ({network_plan_ready_sql}) AS dataset_network_plan_complete,
               ({affiliation_ready_sql})
                   AS dataset_affiliation_organization_complete
          FROM current_dataset_metadata AS dataset
    ), current_resources AS NOT MATERIALIZED (
        SELECT {source_projection_sql},
               dataset.dataset_id, dataset.run_id,
               dataset.published_at AS dataset_published_at,
               dataset.dataset_network_plan_complete,
               dataset.dataset_affiliation_organization_complete,
               resource.resource_type, resource.resource_id,
               resource.payload_json::jsonb AS payload_json
          {source_join_sql}
          JOIN {schema}.provider_directory_dataset_resource AS resource
            ON resource.dataset_id = dataset.dataset_id
    ), {current_plan_resources_sql}
"""


def _provider_directory_current_resource_ctes_sql(
    schema: str,
    has_dataset_insurance_plan: bool = False,
    has_dataset_readiness_indexes: bool = False,
    has_dataset_scope: bool = False,
) -> str:
    """Resolve resources through the one current, fully published endpoint dataset."""
    network_plan_ready_sql = _dataset_relation_ready_sql("dataset_network_plan")
    affiliation_ready_sql = _dataset_relation_ready_sql("dataset_affiliation_organization")
    metadata_projection_sql = """,
               dataset.publication_metadata_json::jsonb
                   -> 'dataset_network_plan' AS dataset_network_plan_metadata,
               dataset.publication_metadata_json::jsonb
                   -> 'dataset_affiliation_organization' AS dataset_affiliation_organization_metadata"""
    if has_dataset_readiness_indexes and not has_dataset_scope:
        metadata_projection_sql = ""
        network_plan_ready_sql = _indexed_dataset_relation_ready_sql(schema, "dataset_network_plan")
        affiliation_ready_sql = _indexed_dataset_relation_ready_sql(schema, "dataset_affiliation_organization")
    source_projection_sql, source_join_sql = _dataset_source_sql(schema, has_dataset_scope)
    dataset_metadata_ctes_sql = (
        _selected_dataset_metadata_ctes_sql(schema, metadata_projection_sql)
        if has_dataset_scope
        else _CURRENT_DATASET_METADATA_CTES_SQL.format(schema=schema, metadata_projection_sql=metadata_projection_sql)
    )
    return _CURRENT_RESOURCE_CTES_SQL.format(
        dataset_metadata_ctes_sql=dataset_metadata_ctes_sql,
        source_metadata_projection_sql=(", dataset.source_id, dataset.canonical_api_base" if has_dataset_scope else ""),
        source_projection_sql=source_projection_sql,
        source_join_sql=source_join_sql,
        schema=schema,
        metadata_projection_sql=metadata_projection_sql,
        network_plan_ready_sql=network_plan_ready_sql,
        affiliation_ready_sql=affiliation_ready_sql,
        current_plan_resources_sql=_provider_directory_current_plan_resources_sql(
            schema, has_dataset_insurance_plan, has_dataset_scope
        ),
    )


def _dataset_source_sql(schema: str, has_dataset_scope: bool) -> tuple[str, str]:
    if has_dataset_scope:
        return "dataset.source_id, dataset.canonical_api_base", "FROM current_datasets AS dataset"
    return (
        "source.source_id, source.canonical_api_base",
        f"""
          FROM {schema}.provider_directory_source AS source
          JOIN current_datasets AS dataset
            ON dataset.endpoint_id = source.endpoint_id""",
    )


def _published_dataset_predicate_sql(alias: str = "dataset") -> str:
    return f"""
        {alias}.published_at IS NOT NULL
        AND COALESCE({alias}.acquisition_root_run_id, {alias}.import_run_id) IS NOT NULL
        AND (({alias}.is_current IS TRUE AND {alias}.status = 'published'
              AND {alias}.superseded_at IS NULL)
             OR ({alias}.is_current IS FALSE AND {alias}.status = 'superseded'
                 AND {alias}.superseded_at >= {alias}.published_at))
    """


def _publication_source_ids_sql(alias: str = "dataset") -> str:
    source_ids = (
        f"COALESCE({alias}.publication_metadata_summary_json -> 'source_ids', "
        f"{alias}.publication_metadata_json::jsonb -> 'source_ids')"
    )
    return f"CASE WHEN jsonb_typeof({source_ids}) = 'array' THEN {source_ids} ELSE '[]'::jsonb END"


def _selected_dataset_metadata_ctes_sql(schema: str, metadata_projection_sql: str) -> str:
    return f"""
    requested_dataset_scope AS MATERIALIZED (
        SELECT source_id, MIN(dataset_id) AS dataset_id
          FROM unnest(CAST(:evidence_source_ids AS varchar[]), CAST(:evidence_dataset_ids AS varchar[]))
               AS requested(source_id, dataset_id)
      GROUP BY source_id
        HAVING COUNT(DISTINCT dataset_id) = 1
    ), current_dataset_metadata AS MATERIALIZED (
        SELECT requested.source_id, endpoint.canonical_api_base,
               dataset.endpoint_id, dataset.dataset_id,
               COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar AS run_id,
               dataset.published_at{metadata_projection_sql}
          FROM requested_dataset_scope AS requested
          JOIN {schema}.provider_directory_endpoint_dataset AS dataset
            ON dataset.dataset_id = requested.dataset_id
          JOIN {schema}.provider_directory_api_endpoint AS endpoint
            ON endpoint.endpoint_id = dataset.endpoint_id
         WHERE ({_published_dataset_predicate_sql()})
           AND (({_publication_source_ids_sql()}) ? requested.source_id
                OR (dataset.publication_metadata_summary_json -> 'source_ids' IS NULL
                    AND dataset.publication_metadata_json::jsonb -> 'source_ids' IS NULL))
    )
    """


def _provider_directory_overlay_dataset_ctes_sql(schema: str) -> str:
    """Resolve exact row provenance through immutable header Source membership."""
    return f"""
    published_root_counts AS MATERIALIZED (
        SELECT endpoint_id, COALESCE(acquisition_root_run_id, import_run_id)::varchar AS run_id
          FROM {schema}.provider_directory_endpoint_dataset
         WHERE published_at IS NOT NULL
           AND COALESCE(acquisition_root_run_id, import_run_id) IS NOT NULL
      GROUP BY endpoint_id, COALESCE(acquisition_root_run_id, import_run_id)::varchar
        HAVING COUNT(*) = 1
    ), retained_datasets AS MATERIALIZED (
        SELECT dataset.endpoint_id, dataset.dataset_id, published_root.run_id
          FROM {schema}.provider_directory_endpoint_dataset AS dataset
          JOIN published_root_counts AS published_root
            ON published_root.endpoint_id = dataset.endpoint_id
           AND published_root.run_id = COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)
         WHERE ({_published_dataset_predicate_sql()})
    ), publication_sources AS MATERIALIZED (
        SELECT DISTINCT publication_source.source_id, retained.endpoint_id, retained.dataset_id,
               retained.run_id, endpoint.canonical_api_base
          FROM retained_datasets AS retained
          JOIN {schema}.provider_directory_endpoint_dataset AS dataset
            ON dataset.dataset_id = retained.dataset_id
          JOIN {schema}.provider_directory_api_endpoint AS endpoint
            ON endpoint.endpoint_id = retained.endpoint_id
         CROSS JOIN LATERAL jsonb_array_elements_text({_publication_source_ids_sql()})
               AS publication_source(source_id)
    ), unambiguous_source_roots AS MATERIALIZED (
        SELECT source_id, run_id
          FROM publication_sources
      GROUP BY source_id, run_id
        HAVING COUNT(DISTINCT dataset_id) = 1
    ), current_datasets AS NOT MATERIALIZED (
        SELECT publication.*
          FROM publication_sources AS publication
          JOIN unambiguous_source_roots AS source_root
            ON source_root.source_id = publication.source_id
           AND source_root.run_id = publication.run_id
    )
    """


def _provider_directory_network_catalog_cte_sql() -> str:
    """Use the maintained catalog's immutable Organization gate and name."""
    return """
    pinned_network_catalog AS NOT MATERIALIZED (
        SELECT source_id, resource_id AS network_resource_id,
               NULLIF(BTRIM(name), '')::varchar AS provider_directory_network_name
          FROM current_organizations
         WHERE active IS DISTINCT FROM false
           AND NULLIF(regexp_replace(lower(COALESCE(name, '')), '[^a-z0-9]+', '', 'g'), '') IS NOT NULL
    )
        """


def _scoped_role_evidence_sql(directory, schema: str, table_flags) -> str:
    """Build role enrichment from one verified retained dataset per Source."""
    return directory._PROVIDER_DIRECTORY_ROLE_EVIDENCE_SQL_TEMPLATE.format(
        current_resource_ctes_sql=_provider_directory_current_resource_ctes_sql(
            schema, table_flags[PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE], has_dataset_scope=True
        ),
        current_typed_resource_ctes_sql=directory._current_typed_resource_ctes_sql(True),
        role_ctes_sql=directory._provider_directory_role_ctes_sql(
            schema,
            table_flags["provider_directory_organization_affiliation"],
            True,
            table_flags[directory.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE],
            table_flags[directory.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_TABLE],
            table_flags[PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE],
            directory._has_provider_directory_plan_scalars(table_flags),
            catalog_relation="pinned_network_catalog",
        ),
        evidence_union_sql=directory._provider_directory_evidence_union_sql(
            schema, True, catalog_relation="pinned_network_catalog"
        ),
        plan_payload_sql=directory._provider_directory_plan_evidence_payload_sql("plan"),
        max_evidence_rows=directory.MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS,
    )
