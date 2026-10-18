-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- Read-only adoption of complete authenticated physical sets, including history.
WITH RECURSIVE
expected_relations(name, oid, parent_oid, acquisition_id) AS (VALUES __RELATIONS__),
read_relations(name, oid) AS (VALUES __READ_RELATIONS__),
headers AS MATERIALIZED (SELECT * FROM __SCHEMA__.provider_directory_rooted_graph_acquisition),
work AS MATERIALIZED (__WORK__),
resources AS MATERIALIZED (__RESOURCES__),
edges AS MATERIALIZED (__EDGES__),
field_contract(source_type, field_name, repeated, target_type) AS (VALUES __FIELDS__),
work_identity AS MATERIALIZED (
    SELECT w.*, '{"kind":' || pg_catalog.to_json(w.kind::pg_catalog.text)::pg_catalog.text
        || ',"page_size":' || CASE WHEN kind='direct_read' THEN 'null' ELSE '100' END
        || ',"pagination":' || pg_catalog.to_json(CASE WHEN kind='direct_read' THEN 'forbidden'
            ELSE 'same-origin-source-issued-until-terminal' END::pg_catalog.text)::pg_catalog.text
        || ',"reference":' || CASE WHEN reference_type IS NULL THEN 'null' ELSE
            pg_catalog.to_json((reference_type || '/' || reference_id)::pg_catalog.text)::pg_catalog.text END
        || ',"resource_type":' || pg_catalog.to_json(resource_type::pg_catalog.text)::pg_catalog.text
        || ',"search_parameter":' || CASE WHEN search_parameter IS NULL THEN 'null' ELSE
            pg_catalog.to_json(search_parameter::pg_catalog.text)::pg_catalog.text END || '}' AS identity_text
    FROM work w
),
payloads AS MATERIALIZED (
    SELECT r.*, payload_json_text::pg_catalog.jsonb AS payload FROM resources r
),
json_nodes AS (
    SELECT acquisition_id,query_id,attempt,resource_type,resource_id,ARRAY[]::pg_catalog.text[] AS path,
           payload AS node FROM payloads
    UNION ALL
    SELECT parent.acquisition_id,parent.query_id,parent.attempt,parent.resource_type,parent.resource_id,
           parent.path || child.part,child.node
    FROM json_nodes parent CROSS JOIN LATERAL (
        SELECT item.key AS part,item.value AS node
        FROM pg_catalog.jsonb_each(CASE WHEN pg_catalog.jsonb_typeof(parent.node)='object'
            THEN parent.node ELSE '{}'::pg_catalog.jsonb END) item
        UNION ALL
        SELECT (item.ordinality-1)::pg_catalog.text,item.value
        FROM pg_catalog.jsonb_array_elements(CASE WHEN pg_catalog.jsonb_typeof(parent.node)='array'
            THEN parent.node ELSE '[]'::pg_catalog.jsonb END) WITH ORDINALITY item(value,ordinality)
    ) child
),
extensions AS (
    SELECT r.acquisition_id,r.query_id,r.attempt,r.resource_type,r.resource_id,
        ARRAY['extension',(item.ordinality-1)::pg_catalog.text] AS path,item.value AS node,1 AS depth
    FROM payloads r CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
        CASE WHEN pg_catalog.jsonb_typeof(r.payload->'extension')='array'
             THEN r.payload->'extension' ELSE '[]'::pg_catalog.jsonb END
    ) WITH ORDINALITY item(value,ordinality)
    UNION ALL
    SELECT parent.acquisition_id,parent.query_id,parent.attempt,parent.resource_type,parent.resource_id,
        parent.path || ARRAY['extension',(item.ordinality-1)::pg_catalog.text],item.value,parent.depth+1
    FROM extensions parent CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
        CASE WHEN pg_catalog.jsonb_typeof(parent.node->'extension')='array'
             THEN parent.node->'extension' ELSE '[]'::pg_catalog.jsonb END
    ) WITH ORDINALITY item(value,ordinality) WHERE parent.depth<=6
),
standard_fields AS MATERIALIZED (
    SELECT r.*,f.field_name,f.repeated,f.target_type
    FROM payloads r JOIN field_contract f ON f.source_type=r.resource_type
    WHERE r.payload ? f.field_name
),
standard_references AS (
    SELECT r.acquisition_id,r.query_id,r.attempt,r.resource_type AS source_resource_type,
        r.resource_id AS source_resource_id,
        r.field_name || CASE WHEN r.repeated THEN '['||(item.ordinality-1)::pg_catalog.text||']' ELSE '' END AS field_path,
        CASE WHEN r.repeated THEN ARRAY[r.field_name,(item.ordinality-1)::pg_catalog.text]
             ELSE ARRAY[r.field_name] END AS path,
        r.target_type,item.value->>'reference' AS reference,r.closure_scope
    FROM standard_fields r CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
        CASE WHEN r.repeated AND pg_catalog.jsonb_typeof(r.payload->r.field_name)='array'
            THEN r.payload->r.field_name
            WHEN NOT r.repeated THEN pg_catalog.jsonb_build_array(r.payload->r.field_name)
            ELSE '[]'::pg_catalog.jsonb END
    ) WITH ORDINALITY item(value,ordinality) WHERE item.value ? 'reference'
),
extension_references AS (
    SELECT e.acquisition_id,e.query_id,e.attempt,e.resource_type AS source_resource_type,
        e.resource_id AS source_resource_id,
        pg_catalog.regexp_replace(pg_catalog.array_to_string(e.path,'.'),'\.([0-9]+)','[\1]','g')
            || '.valueReference' AS field_path,
        e.path || ARRAY['valueReference'] AS path,'Organization'::pg_catalog.text AS target_type,
        e.node#>>'{valueReference,reference}' AS reference,r.closure_scope
    FROM extensions e JOIN payloads r USING(acquisition_id,query_id,attempt,resource_type,resource_id)
    WHERE e.resource_type='PractitionerRole'
        AND pg_catalog.split_part(e.node->>'url','|',1) IN (__NETWORK_URLS__)
),
expected_edges AS MATERIALIZED (
    SELECT *,pg_catalog.split_part(reference,'/',2) AS target_id,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            source_resource_type||pg_catalog.chr(31)||source_resource_id||pg_catalog.chr(31)||field_path
            ||pg_catalog.chr(31)||target_type||pg_catalog.chr(31)||pg_catalog.split_part(reference,'/',2),
            'UTF8')),'hex') AS digest
    FROM (SELECT * FROM standard_references UNION ALL SELECT * FROM extension_references) all_references
),
resource_sets AS MATERIALIZED (
    SELECT acquisition_id,query_id,attempt,pg_catalog.count(*) AS row_count,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            'healthporta.provider-directory.rooted-graph-resource-set.v1'||pg_catalog.chr(31)
            ||pg_catalog.string_agg(resource_type||pg_catalog.chr(31)||resource_id||pg_catalog.chr(31)
                ||payload_sha256||pg_catalog.chr(31)||closure_scope,pg_catalog.chr(30)
                ORDER BY resource_id COLLATE pg_catalog."C"),'UTF8')),'hex') AS digest
    FROM resources GROUP BY acquisition_id,query_id,attempt
),
edge_sets AS MATERIALIZED (
    SELECT acquisition_id,query_id,attempt,pg_catalog.count(*) AS row_count,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            'healthporta.provider-directory.rooted-graph-edge-set.v1'||pg_catalog.chr(31)
            ||pg_catalog.string_agg(edge_sha256||pg_catalog.chr(31)||closure_scope,pg_catalog.chr(30)
                ORDER BY edge_sha256 COLLATE pg_catalog."C"),'UTF8')),'hex') AS digest
    FROM edges GROUP BY acquisition_id,query_id,attempt
),
query_sets AS MATERIALIZED (
    SELECT w.*,COALESCE(r.row_count,0) AS actual_resource_count,COALESCE(e.row_count,0) AS actual_edge_count,
        COALESCE(r.digest,pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            'healthporta.provider-directory.rooted-graph-resource-set.v1'||pg_catalog.chr(31),'UTF8')),'hex')) AS resource_digest,
        COALESCE(e.digest,pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            'healthporta.provider-directory.rooted-graph-edge-set.v1'||pg_catalog.chr(31),'UTF8')),'hex')) AS edge_digest
    FROM work w LEFT JOIN resource_sets r ON r.acquisition_id=w.acquisition_id AND r.query_id=w.query_id
        AND r.attempt=w.attempt_count
    LEFT JOIN edge_sets e ON e.acquisition_id=w.acquisition_id AND e.query_id=w.query_id AND e.attempt=w.attempt_count
),
result_hashes AS MATERIALIZED (
    SELECT q.*,pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(resource_digest||pg_catalog.chr(31)
        ||edge_digest||CASE WHEN missing_http_status IS NULL THEN '' ELSE pg_catalog.chr(31)
            ||missing_response_sha256||pg_catalog.chr(31)||missing_response_bytes::pg_catalog.text END,'UTF8')),'hex')
        AS result_digest FROM query_sets q
),
terminal_hashes AS MATERIALIZED (
    SELECT q.*,pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
        'healthporta.provider-directory.rooted-graph-terminal-record.v2'||pg_catalog.chr(31)
        ||query_identity_sha256||pg_catalog.chr(31)
        ||CASE WHEN status='error' THEN 'error'||pg_catalog.chr(31)||''||pg_catalog.chr(31)||'0'
                ||pg_catalog.chr(31)||'0'||pg_catalog.chr(31)||''||pg_catalog.chr(31)||'0'
                ||pg_catalog.chr(31)||'false'||pg_catalog.chr(31)||error_code
            WHEN missing_http_status IS NOT NULL THEN 'missing'||pg_catalog.chr(31)||result_digest
                ||pg_catalog.chr(31)||'0'||pg_catalog.chr(31)||'0'||pg_catalog.chr(31)||''
                ||pg_catalog.chr(31)||'1'||pg_catalog.chr(31)||'true'||pg_catalog.chr(31)
                ||missing_http_status::pg_catalog.text||pg_catalog.chr(31)||missing_response_sha256
                ||pg_catalog.chr(31)||missing_response_bytes::pg_catalog.text
            ELSE 'completed'||pg_catalog.chr(31)||result_digest||pg_catalog.chr(31)
                ||actual_resource_count::pg_catalog.text||pg_catalog.chr(31)||actual_edge_count::pg_catalog.text
                ||pg_catalog.chr(31)||COALESCE(advertised_total::pg_catalog.text,'')||pg_catalog.chr(31)
                ||terminal_page_count::pg_catalog.text||pg_catalog.chr(31)||'true'||pg_catalog.chr(31)||'' END,
        'UTF8')),'hex') AS terminal_digest FROM result_hashes q
),
work_totals AS MATERIALIZED (
    SELECT acquisition_id,pg_catalog.count(*) AS rows,
        pg_catalog.count(*) FILTER(WHERE status='pending') AS pending,
        pg_catalog.count(*) FILTER(WHERE status='leased') AS leased,
        pg_catalog.count(*) FILTER(WHERE status='completed') AS completed,
        pg_catalog.count(*) FILTER(WHERE status='error') AS errors,
        pg_catalog.count(*) FILTER(WHERE status='error' AND error_code<>'transport_timeout') AS invalid_errors,
        pg_catalog.count(*) FILTER(WHERE kind='full_insurance_plan_census') AS censuses,
        pg_catalog.max(advertised_total) FILTER(WHERE kind='full_insurance_plan_census' AND status='completed') AS plan_total,
        pg_catalog.max(terminal_page_count) FILTER(WHERE kind='full_insurance_plan_census' AND status='completed') AS plan_pages,
        COALESCE(pg_catalog.sum(missing_response_bytes),0) AS missing_bytes,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            'healthporta.provider-directory.rooted-graph-terminal-set.v1'||pg_catalog.chr(31)
            ||COALESCE(pg_catalog.string_agg(query_identity_sha256||pg_catalog.chr(31)||terminal_record_sha256,
                pg_catalog.chr(30) ORDER BY query_identity_sha256 COLLATE pg_catalog."C")
                FILTER(WHERE status IN ('completed','error')),''),'UTF8')),'hex') AS terminal_digest
    FROM work GROUP BY acquisition_id
),
resource_totals AS MATERIALIZED (
    SELECT r.acquisition_id,pg_catalog.count(*) AS rows,pg_catalog.sum(pg_catalog.octet_length(payload_json_text)) AS bytes,
        pg_catalog.count(*) FILTER(WHERE w.status='completed' AND r.attempt=w.attempt_count) AS selected_rows,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            'healthporta.provider-directory.rooted-graph-resource-set.v1'||pg_catalog.chr(31)
            ||COALESCE(pg_catalog.string_agg(r.resource_type||pg_catalog.chr(31)||r.resource_id||pg_catalog.chr(31)
                ||r.payload_sha256||pg_catalog.chr(31)||r.closure_scope,pg_catalog.chr(30)
                ORDER BY w.query_identity_sha256 COLLATE pg_catalog."C",r.resource_type COLLATE pg_catalog."C",
                    r.resource_id COLLATE pg_catalog."C")
                FILTER(WHERE w.status='completed' AND r.attempt=w.attempt_count),''),'UTF8')),'hex') AS digest
    FROM resources r LEFT JOIN work w USING(acquisition_id,query_id) GROUP BY r.acquisition_id
),
edge_totals AS MATERIALIZED (
    SELECT e.acquisition_id,pg_catalog.count(*) AS rows,
        pg_catalog.count(*) FILTER(WHERE w.status='completed' AND e.attempt=w.attempt_count) AS selected_rows,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            'healthporta.provider-directory.rooted-graph-edge-set.v1'||pg_catalog.chr(31)
            ||COALESCE(pg_catalog.string_agg(e.edge_sha256||pg_catalog.chr(31)||e.closure_scope,pg_catalog.chr(30)
                ORDER BY w.query_identity_sha256 COLLATE pg_catalog."C",e.edge_sha256 COLLATE pg_catalog."C")
                FILTER(WHERE w.status='completed' AND e.attempt=w.attempt_count),''),'UTF8')),'hex') AS digest
    FROM edges e LEFT JOIN work w USING(acquisition_id,query_id) GROUP BY e.acquisition_id
),
root_headers AS MATERIALIZED (
    SELECT dataset_id,source_id,endpoint_id,publication_contract_id,dataset_hash,cohort_id,
        resource_count,terminal_set_sha256 AS content_proof,cohort_complete
    FROM __SCHEMA__.provider_directory_uhc_flex_practitioner_dataset
    UNION ALL
    SELECT dataset_id,source_id,endpoint_id,publication_contract_id,dataset_hash,root_cohort_id,
        practitioner_resource_count,root_content_proof_sha256,cohort_complete
    FROM __SCHEMA__.provider_directory_rooted_graph_dataset
),
lineage AS MATERIALIZED (
    SELECT a.acquisition_id,c.npi_count,h.cohort_complete,p.publication_metadata_json::pg_catalog.jsonb AS metadata
    FROM headers a JOIN __SCHEMA__.provider_directory_endpoint_dataset p
        ON p.dataset_id=a.root_dataset_id AND p.endpoint_id=a.root_endpoint_id
        AND p.dataset_hash=a.root_dataset_hash AND p.status IN ('published','superseded')
    JOIN root_headers h ON h.dataset_id=a.root_dataset_id AND h.source_id=a.root_source_id
        AND h.endpoint_id=a.root_endpoint_id AND h.publication_contract_id=a.root_publication_contract_id
        AND h.dataset_hash=a.root_dataset_hash AND h.cohort_id=a.root_cohort_id
        AND h.resource_count=a.root_resource_count AND h.content_proof=a.root_content_proof_sha256
    JOIN __SCHEMA__.provider_directory_uhc_flex_npi_cohort c ON c.cohort_id=a.root_cohort_id
        AND c.contract_id='healthporta.provider-directory.uhc-flex-official-practitioner-npi-cohort.v1'
        AND c.authority_id=a.source_authority_id AND c.official_source_id=__OFFICIAL_SOURCE__
        AND c.resource_type='Practitioner' AND c.cohort_complete IS TRUE
        AND c.endpoint_collection_complete IS FALSE AND c.endpoint_complete IS FALSE
),
header_totals AS MATERIALIZED (
    SELECT a.*,COALESCE(w.rows,0) AS actual_work,COALESCE(w.pending,0) AS actual_pending,
        COALESCE(w.leased,0) AS actual_leased,COALESCE(w.completed,0) AS actual_completed,
        COALESCE(w.errors,0) AS actual_errors,COALESCE(w.invalid_errors,0) AS invalid_errors,
        COALESCE(w.censuses,0) AS actual_censuses,w.plan_total,w.plan_pages,
        COALESCE(r.rows,0) AS all_resources,COALESCE(e.rows,0) AS all_edges,
        COALESCE(r.selected_rows,0) AS selected_resources,COALESCE(e.selected_rows,0) AS selected_edges,
        COALESCE(r.bytes,0)+COALESCE(w.missing_bytes,0) AS actual_bytes,
        w.terminal_digest,
        COALESCE(r.digest,pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            'healthporta.provider-directory.rooted-graph-resource-set.v1'||pg_catalog.chr(31),'UTF8')),'hex')) AS resource_digest,
        COALESCE(e.digest,pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            'healthporta.provider-directory.rooted-graph-edge-set.v1'||pg_catalog.chr(31),'UTF8')),'hex')) AS edge_digest,
        l.npi_count,l.cohort_complete,l.metadata,COALESCE((l.metadata->>'retry_exhausted_count')::pg_catalog.int8,0) AS inherited_errors
    FROM headers a LEFT JOIN work_totals w USING(acquisition_id)
    LEFT JOIN resource_totals r USING(acquisition_id) LEFT JOIN edge_totals e USING(acquisition_id)
    LEFT JOIN lineage l USING(acquisition_id)
),
coverage AS MATERIALIZED (
    SELECT h.*,CASE WHEN actual_errors+inherited_errors=0 THEN NULL::pg_catalog.jsonb ELSE
        pg_catalog.jsonb_build_object('policy_id',__FAILURE_POLICY__,
            'total_requests',npi_count+actual_work,'failed_requests',inherited_errors+actual_errors,
            'rooted_total_requests',actual_work,'rooted_failed_requests',actual_errors,'resource_coverage','unknown')
        END AS expected_coverage FROM header_totals h
),
violations(category) AS (
    SELECT 'native_identity' WHERE EXISTS (
        SELECT FROM expected_relations x LEFT JOIN pg_catalog.pg_class c ON c.oid=x.oid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_catalog.pg_roles o ON o.oid=c.relowner
        WHERE c.relname IS DISTINCT FROM x.name OR n.nspname IS DISTINCT FROM __SCHEMA_LITERAL__
            OR o.rolname IS DISTINCT FROM __OWNER_LITERAL__ OR c.relrowsecurity OR c.relforcerowsecurity
            OR c.relpersistence<>'p' OR c.relkind NOT IN ('r','p')
            OR (SELECT pg_catalog.array_agg(i.inhparent ORDER BY i.inhparent) FROM pg_catalog.pg_inherits i
                WHERE i.inhrelid=c.oid) IS DISTINCT FROM
                CASE WHEN x.parent_oid IS NULL THEN NULL::pg_catalog.oid[] ELSE ARRAY[x.parent_oid] END
            OR NOT EXISTS(SELECT FROM pg_catalog.pg_locks l WHERE l.pid=pg_catalog.pg_backend_pid()
                AND l.relation=c.oid AND l.granted AND l.mode IN
                    ('ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock'))
    ) OR EXISTS (
        SELECT FROM pg_catalog.pg_inherits i JOIN expected_relations p ON p.oid=i.inhparent
        WHERE NOT EXISTS(SELECT FROM expected_relations x WHERE x.oid=i.inhrelid)
    ) OR EXISTS (
        SELECT FROM read_relations x LEFT JOIN pg_catalog.pg_class c ON c.oid=x.oid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_catalog.pg_roles o ON o.oid=c.relowner
        WHERE c.relname IS DISTINCT FROM x.name OR n.nspname IS DISTINCT FROM __SCHEMA_LITERAL__
            OR o.rolname IS DISTINCT FROM __OWNER_LITERAL__ OR c.relrowsecurity OR c.relforcerowsecurity
            OR c.relpersistence<>'p' OR c.relkind NOT IN ('r','p')
            OR NOT EXISTS(SELECT FROM pg_catalog.pg_locks l WHERE l.pid=pg_catalog.pg_backend_pid()
                AND l.relation=c.oid AND l.granted AND l.mode IN ('AccessShareLock','RowShareLock',
                    'RowExclusiveLock','ShareUpdateExclusiveLock','ShareLock','ShareRowExclusiveLock',
                    'ExclusiveLock','AccessExclusiveLock'))
    )
    UNION ALL SELECT 'shape' WHERE EXISTS(SELECT FROM work WHERE (__WORK_CHECKS__) IS NOT TRUE)
        OR EXISTS(SELECT FROM resources WHERE (__RESOURCE_CHECKS__) IS NOT TRUE)
        OR EXISTS(SELECT FROM edges WHERE (__EDGE_CHECKS__) IS NOT TRUE)
        OR EXISTS(SELECT FROM headers WHERE (__HEADER_CHECKS__) IS NOT TRUE)
    UNION ALL SELECT 'duplicate_identity' WHERE EXISTS(
        SELECT FROM work GROUP BY acquisition_id,query_id HAVING pg_catalog.count(*)<>1)
        OR EXISTS(SELECT FROM work GROUP BY acquisition_id,scope_id,query_identity_sha256 HAVING pg_catalog.count(*)<>1)
        OR EXISTS(SELECT FROM resources GROUP BY acquisition_id,query_id,attempt,resource_type,resource_id HAVING pg_catalog.count(*)<>1)
        OR EXISTS(SELECT FROM edges GROUP BY acquisition_id,query_id,attempt,edge_sha256 HAVING pg_catalog.count(*)<>1)
    UNION ALL SELECT 'scope' WHERE EXISTS (
        SELECT FROM work w LEFT JOIN headers a USING(acquisition_id)
        LEFT JOIN expected_relations s ON s.name=w.storage_name
        WHERE a.acquisition_id IS NULL OR a.scope_id IS DISTINCT FROM w.scope_id
            OR (s.acquisition_id IS NOT NULL AND s.acquisition_id IS DISTINCT FROM w.acquisition_id)
            OR w.created_at<a.created_at OR w.updated_at<w.created_at
            OR (w.terminal_at IS NOT NULL AND w.updated_at IS DISTINCT FROM w.terminal_at)
            OR (w.status='leased' AND (w.lease_expires_at<=w.lease_heartbeat_at OR w.lease_heartbeat_at<w.created_at))
    ) OR EXISTS (
        SELECT FROM headers a WHERE a.scope_id IS DISTINCT FROM 'pdrgs_'||pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(__SCOPE_JSON__,'UTF8')),'hex'),1,48)
        OR a.acquisition_id IS DISTINCT FROM 'pdrga_'||pg_catalog.substr(pg_catalog.encode(pg_catalog.sha256(
            pg_catalog.convert_to(pg_catalog.concat_ws(pg_catalog.chr(31),a.storage_contract_id,a.scope_id,
                a.root_cohort_id,a.endpoint_signature_sha256,a.graph_contract_sha256,a.query_contract_sha256,
                a.acquisition_role,a.run_id,a.dataset_intent_id),'UTF8')),'hex'),1,48)
    )
    UNION ALL SELECT 'query_identity' WHERE EXISTS (
        SELECT FROM work_identity WHERE query_identity_json_text IS DISTINCT FROM identity_text
            OR query_identity_sha256 IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(
                pg_catalog.convert_to(identity_text,'UTF8')),'hex')
            OR query_id IS DISTINCT FROM 'pdrgq_'||pg_catalog.substr(pg_catalog.encode(pg_catalog.sha256(
                pg_catalog.convert_to('healthporta.provider-directory.rooted-graph-identity.v1'||pg_catalog.chr(31)
                    ||scope_id||pg_catalog.chr(31)||identity_text,'UTF8')),'hex'),1,48)
    )
    UNION ALL SELECT 'root_membership' WHERE EXISTS (
        SELECT FROM work w JOIN headers a USING(acquisition_id)
        WHERE w.kind='exact_reference_search' AND w.resource_type='PractitionerRole'
            AND NOT EXISTS(SELECT FROM __SCHEMA__.provider_directory_dataset_resource r
                WHERE r.dataset_id=a.root_dataset_id AND r.resource_type='Practitioner' AND r.resource_id=w.reference_id)
    )
    UNION ALL SELECT 'discovery' WHERE EXISTS (
        SELECT FROM work w WHERE w.kind='direct_read' AND NOT EXISTS (
            SELECT FROM edges e JOIN work p ON p.acquisition_id=e.acquisition_id AND p.query_id=e.query_id
                AND p.attempt_count=e.attempt AND p.status='completed'
            WHERE e.acquisition_id=w.acquisition_id AND e.query_id=w.discovered_by_query_id
                AND e.source_resource_type=w.discovered_source_type AND e.source_resource_id=w.discovered_source_id
                AND e.edge_sha256=w.discovered_edge_sha256 AND e.target_resource_type=w.reference_type
                AND e.target_resource_id=w.reference_id AND e.closure_scope=w.closure_scope
                AND p.terminal_at IS NOT DISTINCT FROM w.created_at)
    ) OR EXISTS (
        SELECT FROM work w WHERE w.resource_type='OrganizationAffiliation' AND NOT EXISTS (
            SELECT FROM resources r JOIN work p ON p.acquisition_id=r.acquisition_id AND p.query_id=r.query_id
                AND p.attempt_count=r.attempt AND p.status='completed'
            WHERE r.acquisition_id=w.acquisition_id AND r.query_id=w.discovered_by_query_id
                AND r.resource_type='Organization' AND r.resource_id=w.discovered_source_id
                AND r.resource_id=w.reference_id AND r.closure_scope=w.closure_scope
                AND p.terminal_at IS NOT DISTINCT FROM w.created_at)
    )
    UNION ALL SELECT 'resource' WHERE EXISTS (
        SELECT FROM payloads r LEFT JOIN work w USING(acquisition_id,query_id)
        LEFT JOIN expected_relations s ON s.name=r.storage_name
        WHERE w.query_id IS NULL OR w.scope_id IS DISTINCT FROM r.scope_id OR r.attempt>w.attempt_count
            OR (s.acquisition_id IS NOT NULL AND s.acquisition_id IS DISTINCT FROM r.acquisition_id)
            OR pg_catalog.jsonb_typeof(r.payload) IS DISTINCT FROM 'object'
            OR pg_catalog.jsonb_typeof(r.payload->'resourceType') IS DISTINCT FROM 'string'
            OR pg_catalog.jsonb_typeof(r.payload->'id') IS DISTINCT FROM 'string'
            OR r.payload->>'resourceType' IS DISTINCT FROM r.resource_type OR r.payload->>'id' IS DISTINCT FROM r.resource_id
            OR r.resource_type IS DISTINCT FROM w.resource_type OR r.created_at<w.created_at
            OR (w.status='pending' AND r.attempt=w.attempt_count)
            OR (w.terminal_at IS NOT NULL AND r.created_at>w.terminal_at)
            OR r.payload_sha256 IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(
                pg_catalog.convert_to(r.payload_json_text,'UTF8')),'hex')
            OR (w.kind='exact_reference_search' AND CASE w.resource_type
                WHEN 'PractitionerRole' THEN r.payload#>>'{practitioner,reference}'
                WHEN 'OrganizationAffiliation' THEN r.payload#>>'{participatingOrganization,reference}' END
                IS DISTINCT FROM w.reference_type||'/'||w.reference_id)
            OR (w.kind='direct_read' AND r.resource_id IS DISTINCT FROM w.reference_id)
            OR (w.kind='full_insurance_plan_census' AND r.closure_scope NOT IN ('plan','census'))
            OR (w.kind<>'full_insurance_plan_census' AND r.closure_scope IS DISTINCT FROM w.closure_scope)
            OR (r.payload ? 'extension' AND pg_catalog.jsonb_typeof(r.payload->'extension') IS DISTINCT FROM 'array')
    )
    UNION ALL SELECT 'reference_shape' WHERE EXISTS (
        SELECT FROM standard_fields WHERE pg_catalog.jsonb_typeof(payload->field_name)
            IS DISTINCT FROM CASE WHEN repeated THEN 'array' ELSE 'object' END
    ) OR EXISTS (
        SELECT FROM standard_fields r CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
            CASE WHEN r.repeated AND pg_catalog.jsonb_typeof(r.payload->r.field_name)='array'
                THEN r.payload->r.field_name ELSE '[]'::pg_catalog.jsonb END) item
        WHERE pg_catalog.jsonb_typeof(item) IS DISTINCT FROM 'object'
    ) OR EXISTS (
        SELECT FROM expected_edges WHERE reference IS NULL OR reference !~ '^[A-Za-z]+/[A-Za-z0-9.-]{1,64}$'
            OR pg_catalog.split_part(reference,'/',1) IS DISTINCT FROM target_type
    ) OR EXISTS (
        SELECT FROM json_nodes n WHERE pg_catalog.jsonb_typeof(node)='object' AND node ? 'reference'
            AND NOT EXISTS(SELECT FROM expected_edges e WHERE e.acquisition_id=n.acquisition_id
                AND e.query_id=n.query_id AND e.attempt=n.attempt AND e.source_resource_type=n.resource_type
                AND e.source_resource_id=n.resource_id AND e.path=n.path)
    ) OR EXISTS (
        SELECT FROM json_nodes GROUP BY acquisition_id,query_id,attempt,resource_type,resource_id
        HAVING pg_catalog.count(*)>1048576
    )
    UNION ALL SELECT 'extension' WHERE EXISTS (
        SELECT FROM extensions e WHERE pg_catalog.jsonb_typeof(node) IS DISTINCT FROM 'object'
            OR depth>6 OR (depth=6 AND node ? 'extension')
            OR pg_catalog.jsonb_typeof(node->'url') IS DISTINCT FROM 'string'
            OR pg_catalog.length(node->>'url') NOT BETWEEN 1 AND 1024
            OR node->>'url' IS DISTINCT FROM pg_catalog.btrim(node->>'url',__STRIP_CHARACTERS__)
            OR node->>'url' !~ '^[^|]*(\|[A-Za-z0-9.-]{1,64})?$'
            OR (node ? 'extension' AND pg_catalog.jsonb_typeof(node->'extension') IS DISTINCT FROM 'array')
            OR (e.resource_type='PractitionerRole'
                AND pg_catalog.split_part(node->>'url','|',1) IN (__NETWORK_URLS__) AND (
                EXISTS(SELECT FROM pg_catalog.jsonb_object_keys(node) k
                    WHERE k NOT IN ('id','url','valueReference'))
                OR pg_catalog.jsonb_typeof(node->'valueReference') IS DISTINCT FROM 'object'
                OR NOT (node->'valueReference' ? 'reference')))
            OR ((e.resource_type<>'PractitionerRole'
                OR pg_catalog.split_part(node->>'url','|',1) NOT IN (__NETWORK_URLS__)) AND (
                EXISTS(SELECT FROM pg_catalog.jsonb_object_keys(node) k WHERE k NOT IN ('id','url','extension') AND k NOT LIKE 'value%')
                OR (SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_object_keys(node) k WHERE k LIKE 'value%')>1
                OR node ? 'valueReference'
                OR (node ? 'extension' AND EXISTS(SELECT FROM pg_catalog.jsonb_object_keys(node) k WHERE k LIKE 'value%'))))
    ) OR EXISTS (
        SELECT FROM extensions GROUP BY acquisition_id,query_id,attempt,resource_type,resource_id HAVING pg_catalog.count(*)>4096
    )
    UNION ALL SELECT 'edge_set' WHERE EXISTS (
        SELECT acquisition_id,scope_id,query_id,attempt,source_resource_type,source_resource_id,field_path,
            target_resource_type,target_resource_id,edge_sha256,closure_scope FROM edges
        EXCEPT SELECT e.acquisition_id,r.scope_id,e.query_id,e.attempt,e.source_resource_type,e.source_resource_id,
            e.field_path,e.target_type,e.target_id,e.digest,e.closure_scope FROM expected_edges e
            JOIN resources r ON r.acquisition_id=e.acquisition_id AND r.query_id=e.query_id AND r.attempt=e.attempt
                AND r.resource_type=e.source_resource_type AND r.resource_id=e.source_resource_id
    ) OR EXISTS (
        SELECT e.acquisition_id,r.scope_id,e.query_id,e.attempt,e.source_resource_type,e.source_resource_id,
            e.field_path,e.target_type,e.target_id,e.digest,e.closure_scope FROM expected_edges e
            JOIN resources r ON r.acquisition_id=e.acquisition_id AND r.query_id=e.query_id AND r.attempt=e.attempt
                AND r.resource_type=e.source_resource_type AND r.resource_id=e.source_resource_id
        EXCEPT SELECT acquisition_id,scope_id,query_id,attempt,source_resource_type,source_resource_id,field_path,
            target_resource_type,target_resource_id,edge_sha256,closure_scope FROM edges
    ) OR EXISTS (
        SELECT FROM edges e LEFT JOIN resources r ON r.acquisition_id=e.acquisition_id AND r.query_id=e.query_id
            AND r.attempt=e.attempt AND r.resource_type=e.source_resource_type AND r.resource_id=e.source_resource_id
        LEFT JOIN expected_relations s ON s.name=e.storage_name
        WHERE r.resource_id IS NULL OR e.created_at IS DISTINCT FROM r.created_at
            OR (s.acquisition_id IS NOT NULL AND s.acquisition_id IS DISTINCT FROM e.acquisition_id)
    )
    UNION ALL SELECT 'terminal' WHERE EXISTS (
        SELECT FROM terminal_hashes WHERE status IN ('completed','error') AND (
            terminal_record_sha256 IS DISTINCT FROM terminal_digest
            OR (status='error' AND (actual_resource_count<>0 OR actual_edge_count<>0))
            OR (status='completed' AND (resource_count IS DISTINCT FROM actual_resource_count
                OR edge_count IS DISTINCT FROM actual_edge_count OR resource_set_sha256 IS DISTINCT FROM resource_digest
                OR edge_set_sha256 IS DISTINCT FROM edge_digest OR result_sha256 IS DISTINCT FROM result_digest)))
    )
    UNION ALL SELECT 'missing' WHERE EXISTS (
        SELECT FROM work WHERE missing_http_status IS NOT NULL AND (
            (missing_response_json_text IS JSON OBJECT WITH UNIQUE KEYS) IS NOT TRUE
            OR missing_response_bytes IS DISTINCT FROM pg_catalog.octet_length(missing_response_json_text)
            OR missing_response_sha256 IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(
                pg_catalog.convert_to(missing_response_json_text,'UTF8')),'hex')
            OR missing_response_json_text::pg_catalog.jsonb->>'resourceType' IS DISTINCT FROM 'OperationOutcome'
            OR pg_catalog.jsonb_typeof(missing_response_json_text::pg_catalog.jsonb->'issue') IS DISTINCT FROM 'array'
            OR NOT (__MISSING_SHAPES__))
    )
    UNION ALL SELECT 'lineage' WHERE EXISTS (
        SELECT FROM headers a WHERE (SELECT pg_catalog.count(*) FROM lineage l WHERE l.acquisition_id=a.acquisition_id)<>1
    ) OR EXISTS (
        SELECT FROM headers a WHERE NOT EXISTS (
            SELECT FROM __SCHEMA__.provider_directory_source rs JOIN __SCHEMA__.provider_directory_api_endpoint re
                ON re.endpoint_id=a.root_endpoint_id
            JOIN __SCHEMA__.provider_directory_source s ON s.source_id=a.acquisition_source_id
            JOIN __SCHEMA__.provider_directory_api_endpoint e ON e.endpoint_id=a.acquisition_endpoint_id
            WHERE rs.source_id=a.root_source_id AND e.endpoint_signature_hash=a.endpoint_signature_sha256
                AND a.acquisition_source_id=__ACQUISITION_SOURCE__
                AND a.acquisition_endpoint_id=__ACQUISITION_ENDPOINT__
                AND rs.canonical_api_base=__API_BASE__ AND re.canonical_api_base=__API_BASE__
                AND s.canonical_api_base=__API_BASE__ AND e.canonical_api_base=__API_BASE__
                AND rs.metadata_json::pg_catalog.jsonb->>'provider_directory_authority_id'=a.source_authority_id
                AND re.metadata_json::pg_catalog.jsonb->>'authority_id'=a.source_authority_id
                AND s.metadata_json::pg_catalog.jsonb->>'provider_directory_authority_id'=a.source_authority_id
                AND e.metadata_json::pg_catalog.jsonb->>'authority_id'=a.source_authority_id)
    )
    UNION ALL SELECT 'budget' WHERE EXISTS (
        SELECT FROM coverage WHERE used_work_items IS DISTINCT FROM actual_work OR used_resource_rows IS DISTINCT FROM all_resources
            OR used_edge_rows IS DISTINCT FROM all_edges OR used_payload_bytes IS DISTINCT FROM actual_bytes
            OR actual_work>max_work_items OR all_resources>max_resource_rows OR all_edges>max_edge_rows OR actual_bytes>max_payload_bytes
    )
    UNION ALL SELECT 'sealed_census' WHERE EXISTS (
        SELECT FROM coverage WHERE status='sealed' AND (
            actual_pending<>0 OR actual_leased<>0 OR actual_completed+actual_errors<>actual_work OR actual_censuses<>1
            OR invalid_errors<>0 OR pending_count IS DISTINCT FROM actual_pending OR leased_count IS DISTINCT FROM actual_leased
            OR completed_count IS DISTINCT FROM actual_completed OR error_count IS DISTINCT FROM actual_errors
            OR resource_count IS DISTINCT FROM selected_resources OR edge_count IS DISTINCT FROM selected_edges
            OR all_resources<>selected_resources OR all_edges<>selected_edges OR insurance_plan_count IS DISTINCT FROM plan_total
            OR insurance_plan_page_count IS DISTINCT FROM plan_pages OR terminal_set_sha256 IS DISTINCT FROM terminal_digest
            OR resource_set_sha256 IS DISTINCT FROM resource_digest OR edge_set_sha256 IS DISTINCT FROM edge_digest
            OR rooted_graph_sha256 IS DISTINCT FROM pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                'healthporta.provider-directory.rooted-graph-root.v1'||pg_catalog.chr(31)||scope_id||pg_catalog.chr(31)
                ||terminal_digest||pg_catalog.chr(31)||resource_digest||pg_catalog.chr(31)||edge_digest,'UTF8')),'hex'))
    )
    UNION ALL SELECT 'failure_coverage' WHERE EXISTS (
        SELECT FROM coverage WHERE status='sealed' AND (
            npi_count IS NULL OR npi_count<=0 OR metadata->'cohort_complete' IS DISTINCT FROM pg_catalog.to_jsonb(cohort_complete)
            OR (metadata ? 'retry_exhausted_count' AND (pg_catalog.jsonb_typeof(metadata->'retry_exhausted_count')<>'number'
                OR metadata->>'retry_exhausted_count' !~ '^(0|[1-9][0-9]{0,15})$'
                OR (metadata->>'retry_exhausted_count')::pg_catalog.numeric>9007199254740991))
            OR inherited_errors>npi_count OR cohort_complete IS DISTINCT FROM (inherited_errors=0)
            OR request_failure_coverage IS DISTINCT FROM expected_coverage
            OR (expected_coverage IS NOT NULL AND 50*(actual_errors+inherited_errors)>=actual_work+npi_count))
    )
    UNION ALL SELECT 'root_fixed_point' WHERE EXISTS (
        SELECT FROM headers a WHERE (a.status='sealed' OR EXISTS(SELECT FROM work w
            WHERE w.acquisition_id=a.acquisition_id AND w.kind='full_insurance_plan_census')) AND (
            EXISTS(SELECT r.resource_id FROM __SCHEMA__.provider_directory_dataset_resource r
                WHERE r.dataset_id=a.root_dataset_id AND r.resource_type='Practitioner'
                EXCEPT SELECT w.reference_id FROM work w WHERE w.acquisition_id=a.acquisition_id
                    AND w.kind='exact_reference_search' AND w.resource_type='PractitionerRole' AND w.closure_scope='root'
                    AND (w.status='completed' OR (w.status='error' AND w.error_code='transport_timeout')))
            OR EXISTS(SELECT FROM work w WHERE w.acquisition_id=a.acquisition_id AND w.closure_scope='root'
                AND NOT (w.status='completed' OR (w.status='error' AND w.error_code='transport_timeout')))
            OR a.root_resource_count IS DISTINCT FROM (SELECT pg_catalog.count(*)
                FROM __SCHEMA__.provider_directory_dataset_resource r
                WHERE r.dataset_id=a.root_dataset_id AND r.resource_type='Practitioner'))
    )
    UNION ALL SELECT 'closure' WHERE EXISTS (
        SELECT FROM edges e JOIN work p ON p.acquisition_id=e.acquisition_id AND p.query_id=e.query_id
            AND p.attempt_count=e.attempt AND p.status='completed'
        WHERE e.closure_scope IN ('root','plan') AND e.target_resource_type IN ('Organization','Location','HealthcareService','Endpoint')
            AND NOT EXISTS(SELECT FROM work w WHERE w.acquisition_id=e.acquisition_id AND w.kind='direct_read'
                AND w.reference_type=e.target_resource_type AND w.reference_id=e.target_resource_id
                AND (w.closure_scope=e.closure_scope OR (e.closure_scope='plan' AND w.closure_scope='root')))
    ) OR EXISTS (
        SELECT FROM resources r JOIN work p ON p.acquisition_id=r.acquisition_id AND p.query_id=r.query_id
            AND p.attempt_count=r.attempt AND p.status='completed'
        WHERE r.resource_type='Organization' AND r.closure_scope IN ('root','plan') AND NOT EXISTS(
            SELECT FROM work w WHERE w.acquisition_id=r.acquisition_id AND w.resource_type='OrganizationAffiliation'
                AND w.kind='exact_reference_search' AND w.reference_id=r.resource_id
                AND (w.closure_scope=r.closure_scope OR (r.closure_scope='plan' AND w.closure_scope='root')))
    )
    UNION ALL SELECT 'plan_intersection' WHERE EXISTS (
        SELECT FROM resources r JOIN work w USING(acquisition_id,query_id)
        WHERE w.kind='full_insurance_plan_census' AND ((r.closure_scope='plan') IS DISTINCT FROM EXISTS (
            SELECT FROM edges e WHERE e.acquisition_id=r.acquisition_id AND e.query_id=r.query_id AND e.attempt=r.attempt
                AND e.source_resource_type='InsurancePlan' AND e.source_resource_id=r.resource_id
                AND e.field_path LIKE 'network[%' AND e.target_resource_type='Organization' AND EXISTS (
                    SELECT FROM edges a JOIN work p ON p.acquisition_id=a.acquisition_id AND p.query_id=a.query_id
                        AND p.attempt_count=a.attempt AND p.status='completed'
                    WHERE a.acquisition_id=e.acquisition_id AND a.closure_scope='root' AND a.target_resource_type='Organization'
                        AND a.target_resource_id=e.target_resource_id AND ((a.source_resource_type IN ('PractitionerRole','OrganizationAffiliation')
                            AND a.field_path LIKE 'network[%') OR (a.source_resource_type='PractitionerRole'
                            AND a.field_path LIKE 'extension[%.valueReference')))))
    )
)
SELECT pg_catalog.jsonb_build_object(
    'invalid',COALESCE((SELECT pg_catalog.jsonb_agg(category ORDER BY category) FROM violations),'[]'::pg_catalog.jsonb),
    'acquisitions',(SELECT pg_catalog.count(*) FROM headers),
    'work',(SELECT pg_catalog.count(*) FROM work),
    'resources',(SELECT pg_catalog.count(*) FROM resources),
    'edges',(SELECT pg_catalog.count(*) FROM edges),
    'payload_bytes',(SELECT COALESCE(pg_catalog.sum(pg_catalog.octet_length(payload_json_text)),0) FROM resources),
    'missing_responses',(SELECT pg_catalog.count(*) FROM work WHERE missing_response_json_text IS NOT NULL)
);
