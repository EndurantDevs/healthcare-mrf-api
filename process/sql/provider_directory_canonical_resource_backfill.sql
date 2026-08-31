        INSERT INTO {{CANONICAL_REF}} (
            canonical_api_base,
            resource_type,
            resource_id,
            resource_url,
            fhir_meta,
            fhir_self_url,
            fhir_fetch_url,
            fhir_fetch_mode,
            payload_hash,
            payload_json,
            first_seen_run_id,
            last_seen_run_id,
            observed_at,
            updated_at
        )
        SELECT canonical_api_base,
               resource_type,
               resource_id,
               resource_url,
               fhir_meta,
               fhir_self_url,
               fhir_fetch_url,
               fhir_fetch_mode,
               payload_hash,
               payload_json,
               first_seen_run_id,
               last_seen_run_id,
               observed_at,
               updated_at
          FROM (
            SELECT DISTINCT ON ({{BASE_SQL}}, r.resource_id)
                   {{BASE_SQL}} AS canonical_api_base,
                   {{RESOURCE_TYPE_SQL}} AS resource_type,
                   r.resource_id,
                   r.resource_url,
                   r.fhir_meta,
                   r.fhir_self_url,
                   r.fhir_fetch_url,
                   r.fhir_fetch_mode,
                   md5(({{PAYLOAD_SQL}})::text) AS payload_hash,
                   {{PAYLOAD_SQL}} AS payload_json,
                   r.last_seen_run_id AS first_seen_run_id,
                   r.last_seen_run_id,
                   r.observed_at,
                   COALESCE(r.updated_at, r.observed_at) AS updated_at
              FROM {{RESOURCE_REF}} AS r
              JOIN {{SOURCE_REF}} AS src
                ON src.source_id = r.source_id
             WHERE {{ROWS_WHERE_SQL}}
             ORDER BY {{BASE_SQL}},
                      r.resource_id,
                      r.updated_at DESC NULLS LAST,
                      r.observed_at DESC NULLS LAST,
                      r.source_id
          ) AS ranked
        ON CONFLICT (canonical_api_base, resource_type, resource_id) DO UPDATE
            SET resource_url = EXCLUDED.resource_url,
                fhir_meta = EXCLUDED.fhir_meta,
                fhir_self_url = EXCLUDED.fhir_self_url,
                fhir_fetch_url = EXCLUDED.fhir_fetch_url,
                fhir_fetch_mode = EXCLUDED.fhir_fetch_mode,
                payload_hash = EXCLUDED.payload_hash,
                payload_json = EXCLUDED.payload_json,
                first_seen_run_id = COALESCE(
                    {{CANONICAL_TARGET}}.first_seen_run_id,
                    EXCLUDED.first_seen_run_id
                ),
                last_seen_run_id = EXCLUDED.last_seen_run_id,
                observed_at = EXCLUDED.observed_at,
                updated_at = EXCLUDED.updated_at;
