        INSERT INTO {{EDGE_REF}} (
            source_id,
            canonical_api_base,
            resource_type,
            resource_id,
            last_seen_run_id,
            observed_at,
            updated_at
        )
        SELECT DISTINCT r.source_id,
               {{BASE_SQL}} AS canonical_api_base,
               {{RESOURCE_TYPE_SQL}} AS resource_type,
               r.resource_id,
               r.last_seen_run_id,
               r.observed_at,
               COALESCE(r.updated_at, r.observed_at) AS updated_at
          FROM {{RESOURCE_REF}} AS r
          JOIN {{SOURCE_REF}} AS src
            ON src.source_id = r.source_id
         WHERE {{ROWS_WHERE_SQL}}
        ON CONFLICT (source_id, resource_type, resource_id) DO UPDATE
            SET canonical_api_base = EXCLUDED.canonical_api_base,
                last_seen_run_id = EXCLUDED.last_seen_run_id,
                observed_at = EXCLUDED.observed_at,
                updated_at = EXCLUDED.updated_at;
