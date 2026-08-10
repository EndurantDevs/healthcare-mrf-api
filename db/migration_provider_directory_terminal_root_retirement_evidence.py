# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Frozen evidence SQL for terminal Provider Directory root retirement."""

from __future__ import annotations


STATUS = "acquisition_retired"
MARKER = "provider_directory_terminal_root_retirement_v1"
EVIDENCE_FUNCTION = "provider_directory_terminal_root_retirement_evidence"
RELATION_EVIDENCE_FUNCTION = (
    "provider_directory_terminal_root_retirement_relation_evidence"
)
_RELATION_SPECS = (
    ("provider_directory_bulk_acquisition_checkpoint", "dataset_id", "checkpoint_id"),
    ("provider_directory_dataset_affiliation_organization", "dataset_id",
     "participating_organization_resource_id, row.affiliation_resource_id"),
    ("provider_directory_dataset_insurance_plan", "dataset_id", "resource_id"),
    ("provider_directory_dataset_network_plan", "dataset_id",
     "network_resource_id, row.insurance_plan_resource_id"),
    ("provider_directory_dataset_rehydration_checkpoint", "dataset_id",
     "source_id, row.acquisition_root_run_id, row.resource_type"),
    ("provider_directory_dataset_resource", "dataset_id",
     "resource_type, row.resource_id"),
    ("provider_directory_pagination_checkpoint", "dataset_id",
     "canonical_api_base, row.resource_type, row.source_scope_hash"),
    ("provider_directory_uhc_flex_npi_cohort", "official_dataset_id", "cohort_id"),
    ("provider_directory_uhc_flex_practitioner_dataset", "dataset_id", "dataset_id"),
    ("provider_directory_uhc_flex_practitioner_dataset_resource", "dataset_id",
     "resource_id"),
)


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _qf(schema: str, relation: str) -> str:
    return f"{_q(schema)}.{_q(relation)}"


def _row_digest_sql(row_sql: str, order_sql: str) -> str:
    return f"""
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    COALESCE(
                        pg_catalog.string_agg(
                            pg_catalog.encode(
                                pg_catalog.sha256(
                                    pg_catalog.convert_to(
                                        pg_catalog.to_jsonb({row_sql})::text,
                                        'UTF8'
                                    )
                                ),
                                'hex'
                            ),
                            '' ORDER BY {order_sql},
                                pg_catalog.to_jsonb({row_sql})::text
                        ),
                        ''
                    ),
                    'UTF8'
                )
            ),
            'hex'
        )
    """


def _relation_query(
    schema: str,
    relation: str,
    alias: str,
    where_sql: str,
    order_sql: str,
    *,
    join_sql: str = "",
    row_sql: str | None = None,
) -> str:
    relation_ref = _qf(schema, relation)
    digest_sql = _row_digest_sql(row_sql or alias, order_sql)
    return f"""
        SELECT pg_catalog.jsonb_build_object(
                   'row_count', pg_catalog.count(*)::bigint,
                   'row_sha256', {digest_sql}
               )
          FROM {relation_ref} AS {alias}
          {join_sql}
         WHERE {where_sql}
    """


def _relation_specs(schema: str) -> dict[str, str]:
    bulk_ref = _qf(schema, "provider_directory_bulk_acquisition_checkpoint")
    relation_query_by_name = {
        relation: _relation_query(
            schema, relation, "row",
            f"row.{dataset_column} = candidate_dataset_id",
            ", ".join(f"row.{part.strip()}" if "." not in part else part.strip()
                      for part in order_columns.split(",")),
        )
        for relation, dataset_column, order_columns in _RELATION_SPECS
    }
    relation_query_by_name.update({
        "provider_directory_bulk_output_checkpoint": _relation_query(
            schema, "provider_directory_bulk_output_checkpoint", "row",
            "bulk.dataset_id = candidate_dataset_id",
            "row.checkpoint_id, row.output_id",
            join_sql=f"JOIN {bulk_ref} AS bulk USING (checkpoint_id)",
        ),
        "provider_directory_dataset_proof_shard": _relation_query(
            schema, "provider_directory_dataset_proof_shard", "row",
            "row.dataset_id = candidate_dataset_id", "row.shard_id",
            row_sql="(pg_catalog.to_jsonb(row) - 'payload_bytes') || "
                    "pg_catalog.jsonb_build_object('payload_bytes_sha256', "
                    "pg_catalog.encode(pg_catalog.sha256(row.payload_bytes), 'hex'))",
        ),
        "provider_directory_endpoint_dataset_previous_reference": _relation_query(
            schema, "provider_directory_endpoint_dataset", "row",
            "row.previous_dataset_id = candidate_dataset_id", "row.dataset_id",
        ),
    })
    return dict(sorted(relation_query_by_name.items()))


def relation_evidence_function_sql(schema: str) -> str:
    """Return SQL for the closed child-relation evidence dispatcher."""

    relation_cases = "\n".join(
        f"WHEN {_ql(name)} THEN RETURN ({query});"
        for name, query in _relation_specs(schema).items()
    )
    function_ref = _qf(schema, RELATION_EVIDENCE_FUNCTION)
    return f"""
    CREATE FUNCTION {function_ref}(
        candidate_dataset_id text,
        candidate_relation text
    ) RETURNS jsonb
    LANGUAGE plpgsql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog
    SET TimeZone = 'UTC'
    AS $function$
    BEGIN
        CASE candidate_relation
            {relation_cases}
            ELSE
                RAISE EXCEPTION
                    'provider_directory_terminal_root_retirement_relation_unknown'
                    USING ERRCODE = '55000';
        END CASE;
    END;
    $function$;
    """


def _child_relations_sql(schema: str) -> str:
    function_ref = _qf(schema, RELATION_EVIDENCE_FUNCTION)
    pairs = ",\n".join(
        f"{_ql(name)}, {function_ref}(parent.dataset_id, {_ql(name)})"
        for name in _relation_specs(schema)
    )
    return f"pg_catalog.jsonb_build_object({pairs})"


def _identity_digest_sql(value_sql: str) -> str:
    return f"""
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(({value_sql})::text, 'UTF8')
            ),
            'hex'
        )
    """


def _lineage_walk_sql(schema: str) -> str:
    import_run = _qf(schema, "import_run")
    return f"""
    , lineage_walk(import_row, path, depth) AS (
        SELECT run, ARRAY[run.run_id]::text[], 0
          FROM {import_run} AS run
        JOIN parent ON run.run_id = parent.acquisition_root_run_id
        UNION ALL
        SELECT child, ancestor.path || child.run_id, ancestor.depth + 1
          FROM {import_run} AS child
        JOIN lineage_walk AS ancestor
          ON child.retry_of_run_id = (ancestor.import_row).run_id
         AND NOT child.run_id = ANY(ancestor.path)
         AND ancestor.depth < 128
    ), lineage AS (
        SELECT (lineage_walk.import_row).* FROM lineage_walk
    ), lineage_edges AS (
        SELECT (ancestor.import_row).run_id,
               pg_catalog.count(child.run_id)::bigint AS child_count,
               COALESCE(pg_catalog.bool_or(
                   child.run_id IS NOT NULL
                   AND (child.run_id = ANY(ancestor.path)
                        OR ancestor.depth >= 128)
               ), FALSE) AS invalid_edge
          FROM lineage_walk AS ancestor
          LEFT JOIN {import_run} AS child
            ON child.retry_of_run_id = (ancestor.import_row).run_id
         GROUP BY (ancestor.import_row).run_id
    )
    """


def _lineage_shape_sql() -> str:
    return """
    , lineage_shape AS (
        SELECT (
            pg_catalog.count(*) > 0
            AND pg_catalog.count(*) =
                pg_catalog.count(DISTINCT lineage_walk.depth)
            AND pg_catalog.min(lineage_walk.depth) = 0
            AND pg_catalog.max(lineage_walk.depth) =
                pg_catalog.count(*) - 1
            AND pg_catalog.count(*) FILTER (
                WHERE lineage_walk.depth = 0
                  AND (lineage_walk.import_row).run_id =
                      parent.acquisition_root_run_id
                  AND (lineage_walk.import_row).retry_of_run_id IS NULL
            ) = 1
            AND pg_catalog.count(*) FILTER (
                WHERE (lineage_walk.import_row).importer
                      IS DISTINCT FROM 'provider-directory-fhir'
            ) = 0
            AND pg_catalog.count(*) FILTER (
                WHERE (lineage_walk.import_row).run_id = parent.import_run_id
            ) = 1
            AND pg_catalog.max(lineage_walk.depth) FILTER (
                WHERE (lineage_walk.import_row).run_id = parent.import_run_id
            ) = pg_catalog.max(lineage_walk.depth)
            AND NOT EXISTS (
                SELECT 1 FROM lineage_edges
                 WHERE lineage_edges.child_count > 1
                    OR lineage_edges.invalid_edge
            )
            AND COALESCE((
                SELECT lineage_edges.child_count FROM lineage_edges
                 WHERE lineage_edges.run_id = parent.import_run_id
            ), -1) = 0
        ) AS is_linear
          FROM lineage_walk CROSS JOIN parent
         GROUP BY parent.acquisition_root_run_id, parent.import_run_id
    )
    """


def lineage_ctes_sql(schema: str) -> str:
    """Return closed traversal SQL shared by evidence and eligibility."""

    return _lineage_walk_sql(schema) + _lineage_shape_sql()


def _evidence_ctes_sql(schema: str) -> str:
    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    resource = _qf(schema, "provider_directory_dataset_resource")
    proof = _qf(schema, "provider_directory_dataset_proof_shard")
    lineage_digest = _row_digest_sql("lineage", "lineage.created_at, lineage.run_id")
    return f"""
    WITH RECURSIVE parent AS (
        SELECT row.* FROM {dataset} AS row
         WHERE row.dataset_id = candidate_dataset_id
           AND row.status IN ('acquiring', {_ql(STATUS)})
    )
    {lineage_ctes_sql(schema)}
    , lineage_summary AS (
        SELECT pg_catalog.count(*)::bigint AS run_count,
               pg_catalog.to_char(
                   pg_catalog.max(finished_at) AT TIME ZONE 'UTC',
                   'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
               ) AS finished_at,
               {lineage_digest} AS row_sha256
          FROM lineage
    ), resource_summary AS (
        SELECT COALESCE(pg_catalog.sum(grouped.row_count), 0)::bigint
                   AS actual_count,
               COALESCE(
                   pg_catalog.jsonb_object_agg(
                       grouped.resource_type,
                       grouped.row_count ORDER BY grouped.resource_type
                   ),
                   '{{}}'::jsonb
               ) AS resource_counts
          FROM (
              SELECT row.resource_type, pg_catalog.count(*)::bigint AS row_count
                FROM {resource} AS row
               WHERE row.dataset_id = candidate_dataset_id
               GROUP BY row.resource_type
          ) AS grouped
    ), proof_summary AS (
        SELECT pg_catalog.count(*)::bigint AS shard_count,
               COALESCE(pg_catalog.sum(row.resource_count), 0)::bigint AS row_count
          FROM {proof} AS row
         WHERE row.dataset_id = candidate_dataset_id
    )
    """


def _evidence_select_sql(schema: str) -> str:
    """Return the terminal-root evidence projection and identity joins."""

    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    source_ref = _qf(schema, "provider_directory_source")
    endpoint = _qf(schema, "provider_directory_api_endpoint")
    child_relations = _child_relations_sql(schema)
    parent_identity = _identity_digest_sql(
        "(pg_catalog.to_jsonb(parent) - ARRAY['status', "
        "'publication_metadata_json']::text[]) || "
        "pg_catalog.jsonb_build_object('publication_metadata_json', "
        f"COALESCE(parent.publication_metadata_json::jsonb, '{{}}'::jsonb) - {_ql(MARKER)})"
    )
    predecessor_identity = _identity_digest_sql("pg_catalog.to_jsonb(predecessor)")
    source_identity = _identity_digest_sql("pg_catalog.to_jsonb(source_row)")
    target_identity = _identity_digest_sql("pg_catalog.to_jsonb(endpoint_row)")
    return f"""
    SELECT pg_catalog.jsonb_build_object(
               'actual_resource_count', resource_summary.actual_count,
               'child_relations', {child_relations},
               'lineage_finished_at', lineage_summary.finished_at,
               'lineage_sha256', lineage_summary.row_sha256,
               'parent_identity_sha256', {parent_identity},
               'parent_resource_count', parent.resource_count,
               'predecessor_identity_sha256',
                   {predecessor_identity},
               'prior_status', 'acquiring',
               'proof_shard_count', proof_summary.shard_count,
               'proof_row_count', proof_summary.row_count,
               'resource_counts', resource_summary.resource_counts,
               'source_identity_sha256',
                   {source_identity},
               'target_identity_sha256',
                   {target_identity},
               'terminal_run_count', lineage_summary.run_count
           )
      FROM parent
      JOIN {dataset} AS predecessor
        ON predecessor.dataset_id = parent.previous_dataset_id
      JOIN {endpoint} AS endpoint_row
        ON endpoint_row.endpoint_id = parent.endpoint_id
      JOIN {source_ref} AS source_row
        ON source_row.endpoint_id = parent.endpoint_id
       AND source_row.source_id = COALESCE(
            CASE
                WHEN pg_catalog.jsonb_typeof(
                         parent.publication_metadata_json::jsonb -> 'source_ids'
                     ) = 'array'
                 AND pg_catalog.jsonb_array_length(
                         parent.publication_metadata_json::jsonb -> 'source_ids'
                     ) = 1
                THEN parent.publication_metadata_json::jsonb
                         -> 'source_ids' ->> 0
            END,
            parent.publication_metadata_json::jsonb ->> 'source_id'
       )
      CROSS JOIN lineage_summary CROSS JOIN resource_summary
      CROSS JOIN proof_summary CROSS JOIN lineage_shape
     WHERE lineage_shape.is_linear;
    """


def evidence_function_sql(schema: str) -> str:
    """Return SQL for the complete terminal-root evidence function."""

    function_ref = _qf(schema, EVIDENCE_FUNCTION)
    return f"""
    CREATE FUNCTION {function_ref}(candidate_dataset_id text)
    RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog
    SET TimeZone = 'UTC'
    AS $function$
    {_evidence_ctes_sql(schema)}
    {_evidence_select_sql(schema)}
    $function$;
    """


def evidence_function_names() -> tuple[str, ...]:
    """Return installed evidence functions in dependency order."""

    return EVIDENCE_FUNCTION, RELATION_EVIDENCE_FUNCTION
