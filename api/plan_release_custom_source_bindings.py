# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed custom-composition predicates for canonical Source binding reads."""

CUSTOM_SOURCE_BINDING_STORAGE = "source-custom-plan-release-bindings-v1"

CUSTOM_METADATA_SQL = """jsonb_build_object(
 'authority',binding.metadata_json->'authority',
 'custom_plan_component_id',binding.custom_plan_component_id,
 'benefit_domain',binding.metadata_json->'benefit_domain',
 'source_as_of',binding.metadata_json->'source_as_of')"""

CUSTOM_SEMANTIC_JOIN_SQL = """
 CROSS JOIN LATERAL (SELECT CAST(release.semantic_manifest_json AS jsonb) AS manifest) semantic
 JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(semantic.manifest->'components')='array'
   THEN semantic.manifest->'components' ELSE jsonb_build_array() END) component(payload)
 ON component.payload=jsonb_build_object(
   'custom_plan_component_id',binding.custom_plan_component_id,
   'source_file_import_id',binding.source_file_import_id,
   'source_key',binding.source_key,'snapshot_id',binding.snapshot_id,
   'plan_id',binding.plan_id,'market_type',binding.market_type,
   'role',binding.role,'ordinal',binding.ordinal,'required',binding.required,
   'benefit_domain',binding.metadata_json->'benefit_domain',
   'source_as_of',binding.metadata_json->'source_as_of')
"""

CUSTOM_BINDING_PREDICATE_SQL = """
  AND sealed.custom_plan_component_id=binding.custom_plan_component_id
  AND sealed.custom_plan_component_id<>''
  AND sealed.logical_content_id IS NOT DISTINCT FROM binding.logical_content_id
  AND sealed.market_type=binding.market_type
  AND to_jsonb(sealed.benefit_domain)=binding.metadata_json->'benefit_domain'
  AND to_jsonb(sealed.source_as_of)=binding.metadata_json->'source_as_of'
"""

CUSTOM_MANIFEST_PREDICATE_SQL = """
   AND normalized.manifest->>'contract'='hp-plan-serving-revision-v1'
   AND jsonb_typeof(normalized.manifest->'authority')='object'
   AND normalized.manifest->'authority_cohort'=normalized.manifest->'authority'
   AND normalized.manifest->'authority'->>'contract'='tenant_composition_v1'
   AND jsonb_typeof(normalized.manifest->'authority'->'definition_id')='string'
   AND normalized.manifest->'authority'->>'definition_id' ~ '^hpdef_[0-9A-HJKMNP-TV-Z]{26}$'
   AND jsonb_typeof(normalized.manifest->'authority'->'definition_digest')='string'
   AND normalized.manifest->'authority'->>'definition_digest' ~ '^[0-9a-f]{64}$'
   AND jsonb_typeof(normalized.manifest->'authority'->'definition_revision_no')='number'
   AND normalized.manifest->'authority'->>'definition_revision_no' ~ '^[1-9][0-9]*$'
   AND binding.metadata_json->'authority'=normalized.manifest->'authority'
   AND semantic.manifest->'authority'=normalized.manifest->'authority'
   AND semantic.manifest->>'contract'='hp-plan-release-v2'
   AND semantic.manifest->'plan_version_id'=to_jsonb(release.plan_version_id)
   AND semantic.manifest->'release_month'=to_jsonb(release.release_month)
   AND jsonb_array_length(CASE WHEN jsonb_typeof(semantic.manifest->'components')='array'
     THEN semantic.manifest->'components' ELSE jsonb_build_array() END)=header.expected_binding_count
   AND NOT EXISTS (
     SELECT entry->'role',entry->'ordinal'
     FROM jsonb_array_elements(CASE WHEN jsonb_typeof(semantic.manifest->'components')='array'
       THEN semantic.manifest->'components' ELSE jsonb_build_array() END) entry
     GROUP BY entry->'role',entry->'ordinal' HAVING count(*)<>1
   )
"""

CUSTOM_ENTRY_PREDICATE_SQL = """
        OR NOT (entry ? 'logical_content_id')
        OR jsonb_typeof(entry->'custom_plan_component_id') IS DISTINCT FROM 'string'
        OR jsonb_typeof(entry->'benefit_domain') IS DISTINCT FROM 'string'
        OR jsonb_typeof(entry->'source_as_of') IS DISTINCT FROM 'string'
"""
