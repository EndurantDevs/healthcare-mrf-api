-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- Validate the complete staged edge batch using its live logical work claim.
WITH expected AS (SELECT work AS claim FROM __SCHEMA__.provider_directory_rooted_graph_work work WHERE acquisition_id=:acquisition_id AND query_id=:query_id), field_contract(source_type, field_name, repeated, target_type) AS (VALUES ('PractitionerRole', 'practitioner', false, 'Practitioner'),
('PractitionerRole', 'organization', false, 'Organization'),
('PractitionerRole', 'network', true, 'Organization'),
('PractitionerRole', 'location', true, 'Location'),
('PractitionerRole', 'healthcareService', true, 'HealthcareService'),
('PractitionerRole', 'endpoint', true, 'Endpoint'),
('OrganizationAffiliation', 'organization', false, 'Organization'),
('OrganizationAffiliation', 'participatingOrganization', false, 'Organization'),
('OrganizationAffiliation', 'network', true, 'Organization'),
('OrganizationAffiliation', 'location', true, 'Location'),
('OrganizationAffiliation', 'healthcareService', true, 'HealthcareService'),
('OrganizationAffiliation', 'endpoint', true, 'Endpoint'),
('Organization', 'partOf', false, 'Organization'),
('Organization', 'endpoint', true, 'Endpoint'),
('Location', 'managingOrganization', false, 'Organization'),
('Location', 'partOf', false, 'Location'),
('Location', 'endpoint', true, 'Endpoint'),
('HealthcareService', 'providedBy', false, 'Organization'),
('HealthcareService', 'location', true, 'Location'),
('HealthcareService', 'coverageArea', true, 'Location'),
('HealthcareService', 'endpoint', true, 'Endpoint'),
('InsurancePlan', 'ownedBy', false, 'Organization'),
('InsurancePlan', 'administeredBy', false, 'Organization'),
('InsurancePlan', 'coverageArea', true, 'Location'),
('InsurancePlan', 'network', true, 'Organization'),
('Endpoint', 'managingOrganization', false, 'Organization')),
        joined AS MATERIALIZED (
            SELECT edge.*, resource.payload_json_text::jsonb AS payload,
                   resource.scope_id AS parent_scope_id, resource.closure_scope AS parent_scope,
                   string_to_array(replace(replace(edge.field_path, '[', '.'), ']', ''), '.') AS path,
                   edge.field_path ~ '^extension\[' AS extension,
                   fields.target_type AS field_target, fields.repeated
              FROM pg_temp.pdrg_edge_stage AS edge LEFT JOIN pg_temp.pdrg_resource_stage AS resource
                ON resource.acquisition_id = edge.acquisition_id
               AND resource.query_id = edge.query_id AND resource.attempt = edge.attempt
               AND resource.resource_type = edge.source_resource_type
               AND resource.resource_id = edge.source_resource_id
              LEFT JOIN field_contract AS fields
                ON fields.source_type = edge.source_resource_type
               AND fields.field_name = regexp_replace(edge.field_path, '\[[0-9]+\]$', '')
        )
        SELECT count(*)::bigint, coalesce(bool_or(
            acquisition_id IS DISTINCT FROM (expected.claim).acquisition_id
            OR scope_id IS DISTINCT FROM (expected.claim).scope_id
            OR query_id IS DISTINCT FROM (expected.claim).query_id OR attempt IS DISTINCT FROM (expected.claim).attempt_count
            OR created_at IS DISTINCT FROM transaction_timestamp()
            OR payload IS NULL OR parent_scope IS DISTINCT FROM closure_scope
            OR parent_scope_id IS DISTINCT FROM scope_id
            OR (extension AND (
                source_resource_type <> 'PractitionerRole' OR target_resource_type <> 'Organization'
                OR field_path !~ '^extension\[[0-9]+\](\.extension\[[0-9]+\]){0,5}\.valueReference$'
                OR payload #>> (path[1:cardinality(path)-1] || ARRAY['url']) IS NULL
                OR split_part(payload #>> (path[1:cardinality(path)-1] || ARRAY['url']), '|', 1) NOT IN ('http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference', 'https://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference', 'http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-ParticipatingNetwork-extension')
                OR payload #>> (path[1:cardinality(path)-1] || ARRAY['url']) !~ '^[^|]+(\|[A-Za-z0-9.-]{1,64})?$'
            ))
            OR (NOT extension AND (field_target IS NULL OR target_resource_type IS DISTINCT FROM field_target
                OR repeated IS DISTINCT FROM (field_path ~ '\[[0-9]+\]$')))
            OR payload #>> (path || ARRAY['reference']) IS DISTINCT FROM target_resource_type || '/' || target_resource_id
            OR edge_sha256 IS DISTINCT FROM encode(sha256(convert_to(
                source_resource_type || chr(31) || source_resource_id || chr(31) || field_path
                || chr(31) || target_resource_type || chr(31) || target_resource_id, 'UTF8')), 'hex')
        ), false) FROM joined CROSS JOIN expected;
