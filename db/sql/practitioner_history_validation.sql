-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- Adoption only: the original admin already holds authenticated, write-blocking
-- family locks. Names below are rendered only from that captured family.
WITH
work AS NOT MATERIALIZED (__WORK_ROWS__),
resources AS NOT MATERIALIZED (__RESOURCE_ROWS__),
loading(acquisition_id) AS (SELECT pg_catalog.unnest($1::text[])),
initialized(acquisition_id) AS (SELECT pg_catalog.unnest($2::text[])),
headers AS NOT MATERIALIZED (
    SELECT * FROM "__SCHEMA__".provider_directory_uhc_flex_practitioner_acquisition
),
members AS NOT MATERIALIZED (
    SELECT cohort_id,npi FROM "__SCHEMA__".provider_directory_uhc_flex_npi_member
),
cohorts AS MATERIALIZED (
    SELECT * FROM "__SCHEMA__".provider_directory_uhc_flex_npi_cohort
),
source_rows AS MATERIALIZED (
    SELECT h.cohort_id,r.* FROM cohorts h JOIN "__SCHEMA__".provider_directory_dataset_resource r
        ON r.dataset_id=h.official_dataset_id
),
source_npis AS MATERIALIZED (
    SELECT r.*,CASE WHEN pg_catalog.jsonb_typeof(r.payload_json::jsonb->'npi')='number'
            AND r.payload_json::jsonb->>'npi' ~ '^[0-9]{10}$'
        THEN (r.payload_json::jsonb->>'npi')::bigint END AS normalized_npi
      FROM source_rows r WHERE r.resource_type='Practitioner'
),
source_counts AS (
    SELECT cohort_id,pg_catalog.count(*)::bigint AS total FROM source_rows GROUP BY cohort_id
),
npi_counts AS (
    SELECT cohort_id,pg_catalog.count(*)::bigint AS practitioners,
        pg_catalog.count(DISTINCT normalized_npi)::bigint AS npis FROM source_npis GROUP BY cohort_id
),
member_counts AS (
    SELECT cohort_id,pg_catalog.count(*)::bigint AS npis FROM members GROUP BY cohort_id
),
invalid_source AS (
    SELECT r.* FROM source_npis r WHERE
        (r.resource_id IS NOT NULL AND r.resource_id=r.payload_json::jsonb->>'resource_id'
         AND (__SOURCE_NPI_VALID__)
         AND pg_catalog.jsonb_typeof(r.payload_json::jsonb->'identifiers')='array'
         AND 1=(SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_array_elements(CASE
             WHEN pg_catalog.jsonb_typeof(r.payload_json::jsonb->'identifiers')='array'
             THEN r.payload_json::jsonb->'identifiers' ELSE '[]'::jsonb END) i
             WHERE i->>'system'=$9::text)
         AND 1=(SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_array_elements(CASE
             WHEN pg_catalog.jsonb_typeof(r.payload_json::jsonb->'identifiers')='array'
             THEN r.payload_json::jsonb->'identifiers' ELSE '[]'::jsonb END) i
             WHERE i->>'system'=$9::text AND pg_catalog.jsonb_typeof(i->'value')='string'
                 AND i->>'value'=r.payload_json::jsonb->>'npi')) IS NOT TRUE
),
invalid_cohorts AS (
    SELECT h.* FROM cohorts h LEFT JOIN "__SCHEMA__".provider_directory_endpoint_dataset d
        ON d.dataset_id=h.official_dataset_id
      LEFT JOIN source_counts s ON s.cohort_id=h.cohort_id
      LEFT JOIN npi_counts n ON n.cohort_id=h.cohort_id LEFT JOIN member_counts m ON m.cohort_id=h.cohort_id
    WHERE COALESCE(s.total,0) IS DISTINCT FROM d.resource_count
       OR COALESCE(n.practitioners,0) IS DISTINCT FROM h.practitioner_resource_count
       OR COALESCE(n.npis,0) IS DISTINCT FROM h.npi_count OR COALESCE(m.npis,0) IS DISTINCT FROM h.npi_count
       OR EXISTS(SELECT FROM members m WHERE m.cohort_id=h.cohort_id AND NOT EXISTS(
            SELECT FROM source_npis r WHERE (r.cohort_id,r.normalized_npi)=(m.cohort_id,m.npi)))
       OR EXISTS(SELECT FROM source_npis r WHERE r.cohort_id=h.cohort_id AND NOT EXISTS(
            SELECT FROM members m WHERE (m.cohort_id,m.npi)=(r.cohort_id,r.normalized_npi)))
),
resource_sets AS MATERIALIZED (
    SELECT r.acquisition_id,r.cohort_id,r.npi,r.attempt,
           pg_catalog.count(*)::bigint AS total,
           pg_catalog.count(DISTINCT r.resource_id)::bigint AS distinct_total,
           pg_catalog.min(r.created_at) AS first_created,
           pg_catalog.max(r.created_at) AS last_created,
           pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
               '{"contract_id":' || pg_catalog.to_json($3::text)::text
               || ',"outcome":"matched","requested_npi":' || r.npi::text
               || ',"resources":[' || pg_catalog.string_agg(
                   '{"resource_id":' || pg_catalog.to_json(r.resource_id::text)::text
                   || ',"sha256":' || pg_catalog.to_json(r.payload_sha256::text)::text
                   || '}', ',' ORDER BY r.resource_id COLLATE pg_catalog."C"
               ) || ']}', 'UTF8'::name)), 'hex'::text) AS result_sha256
      FROM resources r GROUP BY r.acquisition_id,r.cohort_id,r.npi,r.attempt
),
work_census AS MATERIALIZED (
    SELECT w.acquisition_id,pg_catalog.count(*)::bigint AS total,
           pg_catalog.count(DISTINCT w.npi)::bigint AS distinct_total,
           pg_catalog.count(*) FILTER (WHERE w.status='pending')::bigint AS pending,
           pg_catalog.count(*) FILTER (WHERE w.status='leased')::bigint AS leased,
           pg_catalog.count(*) FILTER (WHERE w.status='matched')::bigint AS matched,
           pg_catalog.count(*) FILTER (WHERE w.status='unmatched')::bigint AS unmatched,
           pg_catalog.count(*) FILTER (WHERE w.status='error')::bigint AS errors,
           pg_catalog.count(*) FILTER (WHERE w.status='error' AND
               (w.error_code IS DISTINCT FROM $6::text OR w.attempt_count<$7::integer))::bigint AS invalid_errors,
           COALESCE(pg_catalog.sum(s.total) FILTER (WHERE w.status='matched'),0)::bigint AS selected_resources
      FROM work w LEFT JOIN resource_sets s ON
           (s.acquisition_id,s.cohort_id,s.npi,s.attempt)=
           (w.acquisition_id,w.cohort_id,w.npi,w.attempt_count)
     GROUP BY w.acquisition_id
),
leaf AS (
    SELECT w.acquisition_id,w.npi/1000 AS bucket_id,pg_catalog.count(*)::bigint AS row_count,
           pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.string_agg(
               w.npi::text || pg_catalog.chr(31::integer) || w.terminal_record_sha256,
               pg_catalog.chr(30::integer) ORDER BY w.npi), 'UTF8'::name)), 'hex'::text) AS digest
      FROM work w WHERE w.status IN ('matched','unmatched','error')
     GROUP BY w.acquisition_id,w.npi/1000
),
middle AS (
    SELECT acquisition_id,bucket_id/1000 AS bucket_id,pg_catalog.sum(row_count)::bigint AS row_count,
           pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.string_agg(
               bucket_id::text || pg_catalog.chr(31::integer) || row_count::text
               || pg_catalog.chr(31::integer) || digest,
               pg_catalog.chr(30::integer) ORDER BY bucket_id), 'UTF8'::name)), 'hex'::text) AS digest
      FROM leaf GROUP BY acquisition_id,bucket_id/1000
),
terminal_sets AS (
    SELECT acquisition_id,pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
               $5::text || pg_catalog.chr(31::integer) || pg_catalog.string_agg(
                   bucket_id::text || pg_catalog.chr(31::integer) || row_count::text
                   || pg_catalog.chr(31::integer) || digest,
                   pg_catalog.chr(30::integer) ORDER BY bucket_id), 'UTF8'::name)), 'hex'::text) AS digest
      FROM middle GROUP BY acquisition_id
),
invalid_work AS (
    SELECT w.* FROM work w WHERE
        NOT EXISTS(SELECT FROM headers h WHERE (h.acquisition_id,h.cohort_id)=(w.acquisition_id,w.cohort_id))
        OR NOT EXISTS(SELECT FROM members m WHERE (m.cohort_id,m.npi)=(w.cohort_id,w.npi))
        OR (w.declared_acquisition IS NOT NULL AND w.acquisition_id IS DISTINCT FROM w.declared_acquisition)
        OR (w.npi BETWEEN 1000000000 AND 2999999999 AND w.attempt_count>=0
            AND w.created_at IS NOT NULL AND w.updated_at IS NOT NULL
            AND pg_catalog.isfinite(w.created_at) AND pg_catalog.isfinite(w.updated_at)
            AND (w.lease_token IS NULL OR w.lease_token ~ '^[0-9a-f]{64}$')
            AND (w.result_sha256 IS NULL OR w.result_sha256 ~ '^[0-9a-f]{64}$')
            AND (w.terminal_record_sha256 IS NULL OR w.terminal_record_sha256 ~ '^[0-9a-f]{64}$')
            AND (
                (w.status='pending' AND w.lease_token IS NULL AND w.lease_expires_at IS NULL
                 AND w.lease_heartbeat_at IS NULL AND w.result_sha256 IS NULL AND w.resource_count IS NULL
                 AND w.error_code IS NULL AND w.terminal_record_sha256 IS NULL AND w.terminal_at IS NULL)
                OR (w.status='leased' AND w.attempt_count>0 AND w.lease_token IS NOT NULL
                 AND w.lease_expires_at IS NOT NULL AND pg_catalog.isfinite(w.lease_expires_at)
                 AND w.lease_heartbeat_at=w.updated_at AND w.lease_expires_at>w.lease_heartbeat_at
                 AND w.result_sha256 IS NULL AND w.resource_count IS NULL AND w.error_code IS NULL
                 AND w.terminal_record_sha256 IS NULL AND w.terminal_at IS NULL)
                OR (w.status IN ('matched','unmatched') AND w.attempt_count>0
                 AND w.lease_token IS NULL AND w.lease_expires_at IS NULL AND w.lease_heartbeat_at IS NULL
                 AND w.result_sha256 IS NOT NULL AND w.resource_count>=0 AND w.error_code IS NULL
                 AND w.terminal_record_sha256 IS NOT NULL AND w.terminal_at=w.updated_at)
                OR (w.status='error' AND w.attempt_count>0 AND w.lease_token IS NULL
                 AND w.lease_expires_at IS NULL AND w.lease_heartbeat_at IS NULL AND w.result_sha256 IS NULL
                 AND w.resource_count=0 AND w.error_code ~ '^[a-z][a-z0-9_]{0,127}$'
                 AND w.terminal_record_sha256 IS NOT NULL AND w.terminal_at=w.updated_at)
            )) IS NOT TRUE
        OR ((w.declared_acquisition IS NOT NULL OR w.acquisition_id IN (SELECT acquisition_id FROM loading)) AND
            (w.status='pending' AND w.attempt_count=0 AND w.created_at=w.updated_at) IS NOT TRUE)
),
invalid_resources AS (
    SELECT r.* FROM resources r WHERE
        NOT EXISTS(SELECT FROM work w WHERE (w.acquisition_id,w.cohort_id,w.npi)=
            (r.acquisition_id,r.cohort_id,r.npi) AND r.attempt BETWEEN 1 AND w.attempt_count)
        OR (r.declared_acquisition IS NOT NULL AND r.acquisition_id IS DISTINCT FROM r.declared_acquisition)
        OR (r.npi BETWEEN 1000000000 AND 2999999999 AND r.attempt>0
            AND r.resource_id ~ '^[A-Za-z0-9.-]{1,64}$'
            AND r.payload_sha256 ~ '^[0-9a-f]{64}$'
            AND pg_catalog.octet_length(r.payload_json_text) BETWEEN 2 AND 1048576
            AND r.created_at IS NOT NULL AND pg_catalog.isfinite(r.created_at)
            AND r.payload_sha256=pg_catalog.encode(pg_catalog.sha256(
                pg_catalog.convert_to(r.payload_json_text::text,'UTF8'::name)),'hex'::text)
            AND pg_catalog.jsonb_typeof(r.payload_json_text::jsonb)='object'
            AND (r.payload_json_text::jsonb)->>'resourceType'='Practitioner'
            AND pg_catalog.jsonb_typeof((r.payload_json_text::jsonb)->'id')='string'
            AND (r.payload_json_text::jsonb)->>'id'=r.resource_id
            AND pg_catalog.jsonb_typeof((r.payload_json_text::jsonb)->'identifier')='array'
            AND EXISTS(SELECT FROM pg_catalog.jsonb_array_elements(CASE
                WHEN pg_catalog.jsonb_typeof((r.payload_json_text::jsonb)->'identifier')='array'
                THEN (r.payload_json_text::jsonb)->'identifier' ELSE '[]'::jsonb END) i
                WHERE i->>'system'=$9::text AND pg_catalog.jsonb_typeof(i->'value')='string'
                    AND i->>'value'=r.npi::text)
            AND NOT EXISTS(SELECT FROM pg_catalog.jsonb_array_elements(CASE
                WHEN pg_catalog.jsonb_typeof((r.payload_json_text::jsonb)->'identifier')='array'
                THEN (r.payload_json_text::jsonb)->'identifier' ELSE '[]'::jsonb END) i
                WHERE pg_catalog.jsonb_typeof(i) IS DISTINCT FROM 'object'
                   OR (i->>'system'=$9::text AND (pg_catalog.jsonb_typeof(i->'value') IS DISTINCT FROM 'string'
                    OR i->>'value' IS DISTINCT FROM r.npi::text)))) IS NOT TRUE
),
invalid_results AS (
    SELECT w.* FROM work w LEFT JOIN resource_sets s ON
        (s.acquisition_id,s.cohort_id,s.npi,s.attempt)=
        (w.acquisition_id,w.cohort_id,w.npi,w.attempt_count)
    WHERE (w.status='matched' AND (s.total BETWEEN 1 AND 16
        AND w.resource_count=s.total AND w.result_sha256=s.result_sha256
        AND w.terminal_at=s.first_created AND s.first_created=s.last_created) IS NOT TRUE)
       OR (w.status='unmatched' AND (COALESCE(s.total,0)<>0 OR w.resource_count IS DISTINCT FROM 0
           OR w.result_sha256 IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
               '{"contract_id":' || pg_catalog.to_json($3::text)::text
               || ',"outcome":"unmatched","requested_npi":' || w.npi::text
               || ',"resources":[]}', 'UTF8'::name)), 'hex'::text)))
       OR (w.status IN ('error','pending','leased') AND COALESCE(s.total,0)<>0)
       OR (w.status IN ('matched','unmatched','error') AND w.terminal_record_sha256 IS DISTINCT FROM
           pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
               $4::text || pg_catalog.chr(31::integer) || w.npi::text || pg_catalog.chr(31::integer)
               || w.status || pg_catalog.chr(31::integer) || COALESCE(w.result_sha256,'')
               || pg_catalog.chr(31::integer) || w.resource_count::text || pg_catalog.chr(31::integer)
               || COALESCE(w.error_code,''), 'UTF8'::name)), 'hex'::text))
),
invalid_headers AS (
    SELECT h.* FROM headers h LEFT JOIN work_census c ON c.acquisition_id=h.acquisition_id
      LEFT JOIN terminal_sets t ON t.acquisition_id=h.acquisition_id
    WHERE NOT EXISTS(SELECT FROM "__SCHEMA__".provider_directory_uhc_flex_npi_cohort p
              WHERE p.cohort_id=h.cohort_id AND p.npi_count=h.expected_npi_count)
       OR h.acquisition_id IS DISTINCT FROM 'pdufpa_' || pg_catalog.substr(pg_catalog.encode(
           pg_catalog.sha256(pg_catalog.convert_to(h.storage_contract_id || pg_catalog.chr(31::integer)
           || h.cohort_id || pg_catalog.chr(31::integer) || h.acquisition_role || pg_catalog.chr(31::integer)
           || h.source_id || pg_catalog.chr(31::integer) || h.connector_id || pg_catalog.chr(31::integer)
           || h.query_contract_id || pg_catalog.chr(31::integer) || h.run_id || pg_catalog.chr(31::integer)
           || h.dataset_intent_id || pg_catalog.chr(31::integer) || h.expected_npi_count::text
           || pg_catalog.chr(31::integer) || 'false' || pg_catalog.chr(31::integer) || 'false',
           'UTF8'::name)), 'hex'::text),1::integer,48::integer)
       OR COALESCE(c.total,0)>h.expected_npi_count OR c.total IS DISTINCT FROM c.distinct_total
       OR ((COALESCE(c.total,0)>0 OR h.status='sealed'
             OR h.acquisition_id IN (SELECT acquisition_id FROM initialized))
           AND h.acquisition_id NOT IN (SELECT acquisition_id FROM loading)
           AND (COALESCE(c.total,0)<>h.expected_npi_count OR EXISTS(
               SELECT FROM members m WHERE m.cohort_id=h.cohort_id AND NOT EXISTS(
                   SELECT FROM work w WHERE (w.acquisition_id,w.cohort_id,w.npi)=
                       (h.acquisition_id,h.cohort_id,m.npi)))))
       OR (h.status='sealed' AND (h.pending_count=c.pending AND h.leased_count=c.leased
           AND c.pending=0 AND c.leased=0 AND h.matched_count=c.matched AND h.unmatched_count=c.unmatched
           AND h.error_count=c.errors AND h.resource_count=c.selected_resources
           AND c.matched+c.unmatched+c.errors=h.expected_npi_count
           AND h.cohort_complete=(c.errors=0) AND c.invalid_errors=0
           AND c.errors::numeric*$8::numeric<h.expected_npi_count::numeric
           AND h.terminal_set_sha256=t.digest AND h.sealed_at=h.updated_at) IS NOT TRUE)
)
SELECT (SELECT pg_catalog.count(*) FROM headers)::bigint AS acquisitions,
       (SELECT pg_catalog.count(*) FROM cohorts)::bigint AS cohorts,
       (SELECT pg_catalog.count(*) FROM work)::bigint AS work_rows,
       (SELECT pg_catalog.count(*) FROM resources)::bigint AS resource_rows,
       (SELECT COALESCE(pg_catalog.sum(selected_resources),0)::bigint FROM work_census) AS selected_resource_rows,
       (SELECT pg_catalog.count(*) FROM invalid_work)::bigint AS invalid_work,
       (SELECT pg_catalog.count(*) FROM invalid_resources)::bigint AS invalid_resources,
       (SELECT pg_catalog.count(*) FROM invalid_results)::bigint AS invalid_results,
       (SELECT pg_catalog.count(*) FROM invalid_headers)::bigint AS invalid_headers,
       (SELECT pg_catalog.count(*) FROM invalid_cohorts)::bigint AS invalid_cohorts,
       (SELECT pg_catalog.count(*) FROM invalid_source)::bigint AS invalid_source,
       (SELECT pg_catalog.count(*) FROM members m WHERE (__MEMBER_NPI_VALID__) IS NOT TRUE
            OR NOT EXISTS(SELECT FROM cohorts h WHERE h.cohort_id=m.cohort_id))::bigint AS invalid_members,
       (SELECT pg_catalog.count(*) FROM resource_sets
           WHERE total NOT BETWEEN 1 AND 16 OR total<>distinct_total OR first_created IS DISTINCT FROM last_created
       )::bigint AS invalid_resource_sets;
