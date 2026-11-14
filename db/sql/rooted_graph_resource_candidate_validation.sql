-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- Validate the complete staged resource batch using its live logical work claim.
WITH RECURSIVE expected AS (SELECT work AS claim FROM __SCHEMA__.provider_directory_rooted_graph_work work WHERE acquisition_id=:acquisition_id AND query_id=:query_id), payloads AS MATERIALIZED (
            SELECT incoming.*, payload_json_text::jsonb AS payload FROM pg_temp.pdrg_resource_stage AS incoming
        ), extensions AS (
            SELECT resource_type, resource_id, item.value AS node, 1 AS depth
              FROM payloads CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(payload -> 'extension') = 'array'
                     THEN payload -> 'extension' ELSE '[]'::jsonb END
              ) AS item(value)
            UNION ALL
            SELECT parent.resource_type, parent.resource_id, child.value, parent.depth + 1
              FROM extensions AS parent CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(parent.node -> 'extension') = 'array'
                     THEN parent.node -> 'extension' ELSE '[]'::jsonb END
              ) AS child(value) WHERE parent.depth <= 6
        ), extension_checks AS (
            SELECT resource_type, resource_id, count(*) AS nodes,
                   bool_or(
                     CASE WHEN jsonb_typeof(node) <> 'object' THEN true
                       WHEN node ->> 'url' IS NULL OR node ->> 'url' !~ '^[^|]+(\|[A-Za-z0-9.-]{1,64})?$' THEN true
                       WHEN split_part(node ->> 'url', '|', 1) IN ('http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference', 'https://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference', 'http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-ParticipatingNetwork-extension') THEN
                         resource_type <> 'PractitionerRole' OR node ? 'extension'
                         OR NOT node ? 'valueReference'
                         OR EXISTS (SELECT 1 FROM jsonb_object_keys(node) AS key(name)
                                     WHERE key.name NOT IN ('url', 'valueReference'))
                         OR jsonb_typeof(node -> 'valueReference') <> 'object'
                         OR EXISTS (SELECT 1 FROM jsonb_object_keys(node -> 'valueReference') AS key(name)
                                     WHERE key.name <> 'reference')
                         OR node #>> '{valueReference,reference}' !~ '^Organization/[A-Za-z0-9.-]{1,64}$'
                       ELSE (node ? 'extension' AND EXISTS (
                               SELECT 1 FROM jsonb_object_keys(node) AS key(name) WHERE key.name LIKE 'value%'))
                         OR EXISTS (SELECT 1 FROM jsonb_each(node) AS field(name, value)
                                     WHERE field.name = 'valueReference'
                                        OR (jsonb_typeof(field.value) = 'object' AND field.value ? 'reference'))
                     END OR depth > 6 OR (node ? 'extension' AND jsonb_typeof(node -> 'extension') <> 'array')
                   ) AS invalid
              FROM extensions GROUP BY resource_type, resource_id
        )
        SELECT count(*)::bigint, coalesce(sum(octet_length(payload_json_text)), 0)::bigint,
               coalesce(bool_or(
                   incoming.acquisition_id IS DISTINCT FROM (expected.claim).acquisition_id
                   OR incoming.scope_id IS DISTINCT FROM (expected.claim).scope_id
                   OR incoming.query_id IS DISTINCT FROM (expected.claim).query_id
                   OR incoming.attempt IS DISTINCT FROM (expected.claim).attempt_count
                   OR incoming.created_at IS DISTINCT FROM transaction_timestamp()
                   OR incoming.payload_sha256 IS DISTINCT FROM encode(sha256(convert_to(payload_json_text, 'UTF8')), 'hex')
                   OR jsonb_typeof(payload) IS DISTINCT FROM 'object'
                   OR payload ->> 'resourceType' IS DISTINCT FROM incoming.resource_type
                   OR payload ->> 'id' IS DISTINCT FROM incoming.resource_id
                   OR incoming.resource_type IS DISTINCT FROM (expected.claim).resource_type
                   OR (payload ? 'extension' AND jsonb_typeof(payload -> 'extension') <> 'array')
                   OR extension_checks.nodes > 4096 OR extension_checks.invalid IS TRUE
                   OR ((expected.claim).kind = 'exact_reference_search' AND CASE (expected.claim).resource_type
                         WHEN 'PractitionerRole' THEN payload #>> '{practitioner,reference}'
                         WHEN 'OrganizationAffiliation' THEN payload #>> '{participatingOrganization,reference}'
                         ELSE NULL END IS DISTINCT FROM (expected.claim).reference_type || '/' || (expected.claim).reference_id)
                   OR ((expected.claim).kind = 'direct_read' AND incoming.resource_id IS DISTINCT FROM (expected.claim).reference_id)
                   OR ((expected.claim).kind = 'full_insurance_plan_census' AND incoming.closure_scope NOT IN ('census', 'plan'))
                   OR ((expected.claim).kind <> 'full_insurance_plan_census' AND incoming.closure_scope IS DISTINCT FROM (expected.claim).closure_scope)
               ), false)
          FROM payloads AS incoming LEFT JOIN extension_checks USING (resource_type, resource_id) CROSS JOIN expected;
