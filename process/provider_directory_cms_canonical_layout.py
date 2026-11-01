# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed product-migration layout; no custody, fence, profile or issuance authority.

This starting class has four partitioned generic roots and legacy CMS witness/
graph heaps. It is not a completed Source snapshot conversion. The caller must
independently qualify descendants, role/DDL stability, guards and callable bodies.
Only PostgreSQL 18 and an explicit transaction with search_path=pg_catalog are
supported. Expected expressions are source-reviewed parser forms, never a hash
recaptured from the database being checked.
"""

from collections import Counter

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_relation

CONTRACT = "cms-canonical-legacy-layout-v1"

# Fixed semantics from the existing product migrations. Names of implicit
# NOT NULL constraints are omitted; their exact columns and native flags remain.
# Ownership, ACLs, OIDs, physical placement and descendants belong to custody.
# SQL expressions use a schema placeholder only in this trusted fixed contract.
# fmt: off
_LAYOUTS = {
    'provider_directory_cms_candidate_coverage': {
        "kind": 'r',
        "columns": (
            ('dataset_id', 'character varying(96)', True, '', None),
            ('endpoint_id', 'character varying(96)', True, '', None),
            ('release_id', 'character varying(64)', True, '', None),
            ('dataset_hash', 'character varying(64)', True, '', None),
            ('proof_version', 'smallint', True, '', None),
            ('admission_sha256', 'character varying(64)', True, '', None),
            ('metadata_sha256', 'character varying(64)', True, '', None),
            ('relationship_count', 'bigint', True, '', None),
            ('created_at', 'timestamp with time zone', True, '', None),
        ),
        "constraints": (
            ('provider_directory_cms_candidate_cover_relationship_count_check', 'c', 'CHECK (relationship_count >= 0)', ('relationship_count',)),
            ('provider_directory_cms_candidate_coverage_dataset_hash_check', 'c', "CHECK (dataset_hash::text ~ '^[0-9a-f]{64}$'::text)", ('dataset_hash',)),
            ('provider_directory_cms_candidate_coverage_proof_version_check', 'c', 'CHECK (proof_version = 2)', ('proof_version',)),
            ('provider_directory_cms_candidate_coverage_release_id_check', 'c', "CHECK (release_id::text ~ '^[0-9a-f]{64}$'::text)", ('release_id',)),
            ('provider_directory_cms_candidate_coverage_dataset_id_fkey', 'f', 'FOREIGN KEY (dataset_id) REFERENCES __schema__.provider_directory_endpoint_dataset(dataset_id)', ('dataset_id',)),
            ('provider_directory_cms_candidate_coverage_pkey', 'p', 'PRIMARY KEY (dataset_id, release_id, proof_version)', ('dataset_id', 'release_id', 'proof_version')),
        ),
        "indexes": (
            ('cms_npd_candidate_coverage_release_idx', 'CREATE INDEX cms_npd_candidate_coverage_release_idx ON __schema__.provider_directory_cms_candidate_coverage USING btree (release_id)', ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, False, False),
            ('provider_directory_cms_candidate_coverage_pkey', 'CREATE UNIQUE INDEX provider_directory_cms_candidate_coverage_pkey ON __schema__.provider_directory_cms_candidate_coverage USING btree (dataset_id, release_id, proof_version)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.int2_ops'), ('pg_catalog.default', 'pg_catalog.default', None), '0 0 0', 3, 3, True, True),
        ),
    },
    'provider_directory_cms_npd_relationship': {
        "kind": 'r',
        "columns": (
            ('dataset_id', 'character varying(96)', True, '', None),
            ('source_id', 'character varying(64)', True, '', "'cms-npd'::character varying"),
            ('release_id', 'character varying(256)', True, '', None),
            ('resource_type', 'character varying(64)', True, '', None),
            ('resource_id', 'character varying(256)', True, '', None),
            ('source_payload_hash', 'character varying(64)', True, '', None),
            ('raw_payload_sha256', 'character varying(64)', True, '', None),
            ('reference_field', 'character varying(64)', True, '', None),
            ('parent_ordinal', 'integer', True, '', None),
            ('reference_ordinal', 'integer', True, '', None),
            ('target_type', 'character varying(64)', True, '', None),
            ('target_reference', 'text', False, '', None),
            ('target_resource_id', 'character varying(256)', False, '', None),
            ('resolution_status', 'character varying(16)', True, '', None),
            ('period_start', 'text', False, '', None),
            ('period_end', 'text', False, '', None),
        ),
        "constraints": (
            ('cms_npd_relationship_hash_check', 'c', "CHECK (source_payload_hash::text ~ '^[0-9a-f]{64}$'::text AND raw_payload_sha256::text ~ '^[0-9a-f]{64}$'::text)", ('source_payload_hash', 'raw_payload_sha256')),
            ('cms_npd_relationship_ordinal_check', 'c', 'CHECK (parent_ordinal >= 0 AND reference_ordinal > 0)', ('parent_ordinal', 'reference_ordinal')),
            ('cms_npd_relationship_release_check', 'c', "CHECK (release_id::text ~ '^[0-9a-f]{64}$'::text)", ('release_id',)),
            ('cms_npd_relationship_resolved_target_check', 'c', "CHECK (resolution_status::text = 'unresolved'::text OR target_resource_id IS NOT NULL)", ('resolution_status', 'target_resource_id')),
            ('cms_npd_relationship_source_check', 'c', "CHECK (source_id::text = 'cms-npd'::text)", ('source_id',)),
            ('cms_npd_relationship_status_check', 'c', "CHECK (resolution_status::text = ANY (ARRAY['resolved'::character varying, 'unresolved'::character varying, 'ambiguous'::character varying]::text[]))", ('resolution_status',)),
            ('provider_directory_cms_npd_r_dataset_id_resource_type_res_fkey1', 'f', 'FOREIGN KEY (dataset_id, resource_type, resource_id) REFERENCES __schema__.provider_directory_dataset_resource(dataset_id, resource_type, resource_id) ON DELETE CASCADE', ('dataset_id', 'resource_type', 'resource_id')),
            ('provider_directory_cms_npd_r_dataset_id_resource_type_res_fkey2', 'f', 'FOREIGN KEY (dataset_id, resource_type, resource_id) REFERENCES __schema__.provider_directory_cms_npd_resource_witness(dataset_id, resource_type, resource_id) ON DELETE CASCADE', ('dataset_id', 'resource_type', 'resource_id')),
            ('provider_directory_cms_npd_relationship_pkey', 'p', 'PRIMARY KEY (dataset_id, resource_type, resource_id, reference_field, parent_ordinal, reference_ordinal)', ('dataset_id', 'resource_type', 'resource_id', 'reference_field', 'parent_ordinal', 'reference_ordinal')),
        ),
        "indexes": (
            ('cms_npd_relationship_target_idx', 'CREATE INDEX cms_npd_relationship_target_idx ON __schema__.provider_directory_cms_npd_relationship USING btree (dataset_id, target_type, target_resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, False, False),
            ('provider_directory_cms_npd_relationship_pkey', 'CREATE UNIQUE INDEX provider_directory_cms_npd_relationship_pkey ON __schema__.provider_directory_cms_npd_relationship USING btree (dataset_id, resource_type, resource_id, reference_field, parent_ordinal, reference_ordinal)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.int4_ops', 'pg_catalog.int4_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default', None, None), '0 0 0 0 0 0', 6, 6, True, True),
        ),
    },
    'provider_directory_cms_npd_relationship_receipt': {
        "kind": 'r',
        "columns": (
            ('dataset_id', 'character varying(96)', True, '', None),
            ('release_id', 'character varying(64)', True, '', None),
            ('projection_contract', 'character varying(64)', True, '', None),
            ('relationship_count', 'bigint', True, '', None),
        ),
        "constraints": (
            ('provider_directory_cms_npd_relationsh_projection_contract_check', 'c', "CHECK (projection_contract::text = 'cms-npd-reference-ledger-v1'::text)", ('projection_contract',)),
            ('provider_directory_cms_npd_relationshi_relationship_count_check', 'c', 'CHECK (relationship_count >= 0)', ('relationship_count',)),
            ('provider_directory_cms_npd_relationship_receip_release_id_check', 'c', "CHECK (release_id::text ~ '^[0-9a-f]{64}$'::text)", ('release_id',)),
            ('provider_directory_cms_npd_relationship_receipt_dataset_id_fkey', 'f', 'FOREIGN KEY (dataset_id) REFERENCES __schema__.provider_directory_endpoint_dataset(dataset_id)', ('dataset_id',)),
            ('provider_directory_cms_npd_relationship_receipt_pkey', 'p', 'PRIMARY KEY (dataset_id)', ('dataset_id',)),
        ),
        "indexes": (
            ('provider_directory_cms_npd_relationship_receipt_pkey', 'CREATE UNIQUE INDEX provider_directory_cms_npd_relationship_receipt_pkey ON __schema__.provider_directory_cms_npd_relationship_receipt USING btree (dataset_id)', ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, True, True),
        ),
    },
    'provider_directory_cms_npd_resource_witness': {
        "kind": 'r',
        "columns": (
            ('dataset_id', 'character varying(96)', True, '', None),
            ('source_id', 'character varying(64)', True, '', None),
            ('release_id', 'character varying(64)', True, '', None),
            ('resource_type', 'character varying(64)', True, '', None),
            ('resource_id', 'character varying(256)', True, '', None),
            ('raw_payload_sha256', 'character varying(64)', True, '', None),
            ('normalized_payload_hash', 'character varying(64)', True, '', None),
            ('raw_payload_json', 'jsonb', True, '', None),
        ),
        "constraints": (
            ('cms_npd_witness_normalized_hash_check', 'c', "CHECK (normalized_payload_hash::text ~ '^[0-9a-f]{64}$'::text)", ('normalized_payload_hash',)),
            ('cms_npd_witness_raw_hash_check', 'c', "CHECK (raw_payload_sha256::text ~ '^[0-9a-f]{64}$'::text)", ('raw_payload_sha256',)),
            ('cms_npd_witness_release_check', 'c', "CHECK (release_id::text ~ '^[0-9a-f]{64}$'::text)", ('release_id',)),
            ('cms_npd_witness_source_check', 'c', "CHECK (source_id::text = 'cms-npd'::text)", ('source_id',)),
            ('provider_directory_cms_npd_re_dataset_id_resource_type_res_fkey', 'f', 'FOREIGN KEY (dataset_id, resource_type, resource_id) REFERENCES __schema__.provider_directory_dataset_resource(dataset_id, resource_type, resource_id) ON DELETE CASCADE', ('dataset_id', 'resource_type', 'resource_id')),
            ('provider_directory_cms_npd_resource_witness_pkey', 'p', 'PRIMARY KEY (dataset_id, resource_type, resource_id)', ('dataset_id', 'resource_type', 'resource_id')),
        ),
        "indexes": (
            ('provider_directory_cms_npd_resource_witness_pkey', 'CREATE UNIQUE INDEX provider_directory_cms_npd_resource_witness_pkey ON __schema__.provider_directory_cms_npd_resource_witness USING btree (dataset_id, resource_type, resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, True, True),
        ),
    },
    'provider_directory_dataset_affiliation_organization': {
        "kind": 'p',
        "columns": (
            ('dataset_id', 'character varying(96)', True, '', None),
            ('participating_organization_resource_id', 'character varying(256)', True, '', None),
            ('affiliation_resource_id', 'character varying(256)', True, '', None),
        ),
        "constraints": (
            ('provider_directory_dataset_affiliation_organization_pkey', 'p', 'PRIMARY KEY (dataset_id, participating_organization_resource_id, affiliation_resource_id)', ('dataset_id', 'participating_organization_resource_id', 'affiliation_resource_id')),
        ),
        "indexes": (
            ('provider_directory_dataset_affiliation_organization_pkey', 'CREATE UNIQUE INDEX provider_directory_dataset_affiliation_organization_pkey ON ONLY __schema__.provider_directory_dataset_affiliation_organization USING btree (dataset_id, participating_organization_resource_id, affiliation_resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, True, True),
        ),
    },
    'provider_directory_dataset_insurance_plan': {
        "kind": 'p',
        "columns": (
            ('dataset_id', 'character varying(96)', True, '', None),
            ('resource_id', 'character varying(256)', True, '', None),
            ('payload_hash', 'character varying(64)', True, '', None),
            ('payload_json', 'jsonb', True, '', None),
            ('plan_active', 'boolean', False, 's', "COALESCE(NULLIF(lower(btrim(payload_json ->> 'status'::text)), ''::text), 'active'::text) = 'active'::text"),
            ('plan_identifier', 'text', False, 's', "NULLIF(btrim(payload_json ->> 'plan_identifier'::text), ''::text)"),
        ),
        "constraints": (
            ('provider_directory_dataset_insurance_plan_pkey', 'p', 'PRIMARY KEY (dataset_id, resource_id)', ('dataset_id', 'resource_id')),
        ),
        "indexes": (
            ('provider_directory_dataset_insurance_plan_active_lookup_idx', 'CREATE INDEX provider_directory_dataset_insurance_plan_active_lookup_idx ON ONLY __schema__.provider_directory_dataset_insurance_plan USING btree (dataset_id, resource_id) INCLUDE (plan_identifier) WHERE plan_active', ('pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default'), '0 0', 2, 3, False, False),
            ('provider_directory_dataset_insurance_plan_pkey', 'CREATE UNIQUE INDEX provider_directory_dataset_insurance_plan_pkey ON ONLY __schema__.provider_directory_dataset_insurance_plan USING btree (dataset_id, resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default'), '0 0', 2, 2, True, True),
        ),
    },
    'provider_directory_dataset_network_plan': {
        "kind": 'p',
        "columns": (
            ('dataset_id', 'character varying(96)', True, '', None),
            ('network_resource_id', 'character varying(256)', True, '', None),
            ('insurance_plan_resource_id', 'character varying(256)', True, '', None),
        ),
        "constraints": (
            ('provider_directory_dataset_network_plan_pkey', 'p', 'PRIMARY KEY (dataset_id, network_resource_id, insurance_plan_resource_id)', ('dataset_id', 'network_resource_id', 'insurance_plan_resource_id')),
        ),
        "indexes": (
            ('provider_directory_dataset_network_plan_pkey', 'CREATE UNIQUE INDEX provider_directory_dataset_network_plan_pkey ON ONLY __schema__.provider_directory_dataset_network_plan USING btree (dataset_id, network_resource_id, insurance_plan_resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, True, True),
            ('provider_directory_dataset_network_plan_reverse_lookup_idx', 'CREATE INDEX provider_directory_dataset_network_plan_reverse_lookup_idx ON ONLY __schema__.provider_directory_dataset_network_plan USING btree (dataset_id, insurance_plan_resource_id) INCLUDE (network_resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default'), '0 0', 2, 3, False, False),
        ),
    },
    'provider_directory_dataset_resource': {
        "kind": 'p',
        "columns": (
            ('dataset_id', 'character varying(96)', True, '', None),
            ('resource_type', 'character varying(64)', True, '', None),
            ('resource_id', 'character varying(256)', True, '', None),
            ('payload_hash', 'character varying(64)', True, '', None),
            ('payload_json', 'jsonb', True, '', None),
            ('acquired_resource_sha256', 'character varying(64)', False, '', None),
        ),
        "constraints": (
            ('provider_directory_dataset_resource_pkey', 'p', 'PRIMARY KEY (dataset_id, resource_type, resource_id)', ('dataset_id', 'resource_type', 'resource_id')),
        ),
        "indexes": (
            ('provider_directory_dataset_resource_content_cursor_c_idx', 'CREATE INDEX provider_directory_dataset_resource_content_cursor_c_idx ON ONLY __schema__.provider_directory_dataset_resource USING btree (dataset_id, resource_type COLLATE "C", resource_id COLLATE "C")', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.C', 'pg_catalog.C'), '0 0 0', 3, 3, False, False),
            ('provider_directory_dataset_resource_hash_idx', 'CREATE INDEX provider_directory_dataset_resource_hash_idx ON ONLY __schema__.provider_directory_dataset_resource USING btree (payload_hash)', ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, False, False),
            ('provider_directory_dataset_resource_observed_npi_idx', "CREATE INDEX provider_directory_dataset_resource_observed_npi_idx ON ONLY __schema__.provider_directory_dataset_resource USING btree (((payload_json ->> 'npi'::text)), dataset_id, resource_type) WHERE ((resource_type)::text = ANY (ARRAY[('Practitioner'::character varying)::text, ('PractitionerRole'::character varying)::text]))", ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, False, False),
            ('provider_directory_dataset_resource_pkey', 'CREATE UNIQUE INDEX provider_directory_dataset_resource_pkey ON ONLY __schema__.provider_directory_dataset_resource USING btree (dataset_id, resource_type, resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, True, True),
            ('provider_directory_dataset_resource_plan_lookup_idx', "CREATE INDEX provider_directory_dataset_resource_plan_lookup_idx ON ONLY __schema__.provider_directory_dataset_resource USING btree (dataset_id, resource_id) WHERE ((resource_type)::text = 'InsurancePlan'::text)", ('pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default'), '0 0', 2, 2, False, False),
            ('provider_directory_dataset_resource_type_id_idx', 'CREATE INDEX provider_directory_dataset_resource_type_id_idx ON ONLY __schema__.provider_directory_dataset_resource USING btree (resource_type, resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default'), '0 0', 2, 2, False, False),
        ),
    },
    'provider_directory_endpoint_dataset': {
        "kind": 'r',
        "columns": (
            ('dataset_id', 'character varying(96)', True, '', None),
            ('endpoint_id', 'character varying(64)', True, '', None),
            ('import_run_id', 'character varying(64)', False, '', None),
            ('previous_dataset_id', 'character varying(96)', False, '', None),
            ('dataset_hash', 'character varying(64)', False, '', None),
            ('status', 'character varying(32)', True, '', None),
            ('is_current', 'boolean', True, '', 'false'),
            ('resource_count', 'bigint', True, '', '0'),
            ('created_at', 'timestamp without time zone', False, '', None),
            ('validated_at', 'timestamp without time zone', False, '', None),
            ('published_at', 'timestamp without time zone', False, '', None),
            ('superseded_at', 'timestamp without time zone', False, '', None),
            ('publication_metadata_json', 'jsonb', False, '', None),
            ('acquisition_root_run_id', 'character varying(64)', False, '', None),
            ('completion_proof_required_version', 'integer', False, '', None),
            ('completion_proof_json', 'jsonb', False, '', None),
            ('completion_proof_sha256', 'character varying(64)', False, '', None),
            ('artifact_selection_receipt_json', 'jsonb', False, '', None),
            ('publication_metadata_summary_json', 'jsonb', False, '', None),
            ('publication_metadata_sha256', 'character varying(64)', False, '', None),
            ('content_proof_admission_version', 'smallint', False, '', None),
            ('content_proof_admission_kind', 'character varying(32)', False, '', None),
            ('content_proof_admission_sha256', 'character varying(64)', False, '', None),
            ('content_proof_resource_types', 'character varying(64)[]', False, '', None),
            ('candidate_storage_json', 'jsonb', False, '', None),
        ),
        "constraints": (
            ('pd_endpoint_dataset_completion_digest_check', 'c', 'CHECK (completion_proof_sha256 IS NULL OR __schema__.provider_directory_subset_completion_proof_pair_valid(completion_proof_json, completion_proof_sha256::text) IS TRUE)', ('completion_proof_sha256', 'completion_proof_json')),
            ('pd_endpoint_dataset_completion_marker_check', 'c', 'CHECK (completion_proof_required_version IS NOT NULL OR completion_proof_json IS NULL AND completion_proof_sha256 IS NULL)', ('completion_proof_required_version', 'completion_proof_json', 'completion_proof_sha256')),
            ('pd_endpoint_dataset_completion_pair_check', 'c', 'CHECK ((completion_proof_json IS NULL) = (completion_proof_sha256 IS NULL))', ('completion_proof_json', 'completion_proof_sha256')),
            ('pd_endpoint_dataset_completion_shape_check', 'c', 'CHECK (completion_proof_json IS NULL OR jsonb_typeof(completion_proof_json) = \'object\'::text AND completion_proof_json ?& ARRAY[\'proof_version\'::text, \'contract_version\'::text, \'semantics\'::text, \'strategy_version\'::text, \'traversal_version\'::text, \'canonicalization_version\'::text, \'completion_scopes\'::text, \'campaign_id\'::text, \'cutoff\'::text, \'page_count\'::text, \'resources\'::text, \'dataset\'::text] AND (completion_proof_json - ARRAY[\'proof_version\'::text, \'contract_version\'::text, \'semantics\'::text, \'strategy_version\'::text, \'traversal_version\'::text, \'canonicalization_version\'::text, \'completion_scopes\'::text, \'campaign_id\'::text, \'cutoff\'::text, \'page_count\'::text, \'resources\'::text, \'dataset\'::text]) = \'{}\'::jsonb AND (completion_proof_json ->> \'proof_version\'::text) = \'provider-directory-fhir-server-issued-subset-completion-v1\'::text AND (completion_proof_json -> \'contract_version\'::text) = \'3\'::jsonb AND (completion_proof_json ->> \'semantics\'::text) = \'server-issued-traversal-subset\'::text AND ((completion_proof_json ->> \'strategy_version\'::text) = \'provider-directory-fhir-server-issued-traversal-subset-v3\'::text AND (completion_proof_json -> \'completion_scopes\'::text) = \'["advertised-count-stability", "source-issued-continuation", "returned-resource-content"]\'::jsonb OR (completion_proof_json ->> \'strategy_version\'::text) = \'provider-directory-fhir-server-issued-traversal-subset-v4\'::text AND (completion_proof_json -> \'completion_scopes\'::text) = \'["advertised-count-monotone-decrease-at-most-one", "source-issued-continuation", "returned-resource-content"]\'::jsonb OR (completion_proof_json ->> \'strategy_version\'::text) = \'provider-directory-fhir-server-issued-traversal-subset-v5\'::text AND (completion_proof_json -> \'completion_scopes\'::text) = \'["advertised-count-monotone-decrease-bounded-by-one-percent-and-twenty-pages", "terminal-logical-window-covers-advertised-pre", "source-issued-continuation", "returned-resource-content"]\'::jsonb) AND (completion_proof_json ->> \'traversal_version\'::text) = \'provider-directory-fhir-smile-logical-offset-v3\'::text AND (completion_proof_json ->> \'canonicalization_version\'::text) = \'provider-directory-fhir-returned-resource-json-v2\'::text AND jsonb_typeof(completion_proof_json -> \'campaign_id\'::text) = \'string\'::text AND (completion_proof_json ->> \'campaign_id\'::text) <> \'\'::text AND jsonb_typeof(completion_proof_json -> \'cutoff\'::text) = \'string\'::text AND (completion_proof_json ->> \'cutoff\'::text) ~ \'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z$\'::text AND jsonb_typeof(completion_proof_json -> \'page_count\'::text) = \'number\'::text AND (completion_proof_json ->> \'page_count\'::text) ~ \'^([1-9][0-9]{0,2}|1000)$\'::text AND jsonb_typeof(completion_proof_json -> \'resources\'::text) = \'object\'::text AND (completion_proof_json -> \'resources\'::text) ?& ARRAY[\'HealthcareService\'::text, \'InsurancePlan\'::text, \'Location\'::text, \'Organization\'::text, \'OrganizationAffiliation\'::text, \'Practitioner\'::text, \'PractitionerRole\'::text] AND ((completion_proof_json -> \'resources\'::text) - ARRAY[\'HealthcareService\'::text, \'InsurancePlan\'::text, \'Location\'::text, \'Organization\'::text, \'OrganizationAffiliation\'::text, \'Practitioner\'::text, \'PractitionerRole\'::text]) = \'{}\'::jsonb AND jsonb_typeof(completion_proof_json -> \'dataset\'::text) = \'object\'::text AND (completion_proof_json -> \'dataset\'::text) ?& ARRAY[\'hash\'::text, \'count\'::text, \'resource_hashes\'::text, \'resource_counts\'::text, \'acquired_resource_hashes\'::text] AND ((completion_proof_json -> \'dataset\'::text) - ARRAY[\'hash\'::text, \'count\'::text, \'resource_hashes\'::text, \'resource_counts\'::text, \'acquired_resource_hashes\'::text]) = \'{}\'::jsonb AND dataset_hash IS NOT NULL AND ((completion_proof_json -> \'dataset\'::text) ->> \'hash\'::text) = dataset_hash::text AND ((completion_proof_json -> \'dataset\'::text) -> \'count\'::text) = to_jsonb(resource_count) AND __schema__.provider_directory_subset_completion_proof_shape_valid(completion_proof_json, dataset_hash::text, resource_count) IS TRUE)', ('completion_proof_json', 'dataset_hash', 'resource_count')),
            ('pd_endpoint_dataset_completion_version_check', 'c', 'CHECK (completion_proof_required_version IS NULL OR completion_proof_required_version = 3)', ('completion_proof_required_version',)),
            ('provider_directory_endpoint_dataset_endpoint_id_fkey', 'f', 'FOREIGN KEY (endpoint_id) REFERENCES __schema__.provider_directory_api_endpoint(endpoint_id)', ('endpoint_id',)),
            ('provider_directory_endpoint_dataset_pkey', 'p', 'PRIMARY KEY (dataset_id)', ('dataset_id',)),
            ('cms_serving_dataset_transition', 't', 'TRIGGER DEFERRABLE INITIALLY DEFERRED', ()),
            ('pd_exact_logical_current_provider_directory_endpoint_dataset_gu', 't', 'TRIGGER DEFERRABLE INITIALLY DEFERRED', ()),
            ('pd_rooted_graph_provider_directory_endpoint_dataset_dependency_', 't', 'TRIGGER DEFERRABLE INITIALLY DEFERRED', ()),
            ('pd_subset_abandonment_dataset_consistency_guard', 't', 'TRIGGER DEFERRABLE INITIALLY DEFERRED', ()),
            ('pd_subset_terminal_disposition_dataset_consistency_guard', 't', 'TRIGGER DEFERRABLE INITIALLY DEFERRED', ()),
            ('pd_uhc_flex_dataset_parent_guard', 't', 'TRIGGER DEFERRABLE INITIALLY DEFERRED', ()),
        ),
        "indexes": (
            ('pd_endpoint_dataset_admission_source_ids_idx', "CREATE INDEX pd_endpoint_dataset_admission_source_ids_idx ON __schema__.provider_directory_endpoint_dataset USING gin (((publication_metadata_summary_json -> 'source_ids'::text))) WHERE (((status)::text = 'validated'::text) AND (is_current = false) AND (superseded_at IS NULL))", ('pg_catalog.jsonb_ops',), (None,), '0', 1, 1, False, False),
            ('pd_endpoint_dataset_subset_baseline_generation_key', "CREATE UNIQUE INDEX pd_endpoint_dataset_subset_baseline_generation_key ON __schema__.provider_directory_endpoint_dataset USING btree (endpoint_id, ((publication_metadata_json ->> 'verification_campaign_id'::text)), ((publication_metadata_json ->> 'verification_source_scope_hash'::text))) WHERE ((completion_proof_required_version = 3) AND ((status)::text = 'verification_baseline'::text))", ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, True, False),
            ('provider_directory_dataset_affiliation_ready_v1_idx', "CREATE INDEX provider_directory_dataset_affiliation_ready_v1_idx ON __schema__.provider_directory_endpoint_dataset USING btree (dataset_id) WHERE ((is_current IS TRUE) AND ((status)::text = 'published'::text) AND (published_at IS NOT NULL) AND (superseded_at IS NULL) AND (COALESCE(acquisition_root_run_id, import_run_id) IS NOT NULL) AND (COALESCE(((publication_metadata_json -> 'dataset_affiliation_organization'::text) ->> 'complete'::text), 'false'::text) = 'true'::text) AND (COALESCE(((publication_metadata_json -> 'dataset_affiliation_organization'::text) ->> 'version'::text), ''::text) = '1'::text) AND (COALESCE(((publication_metadata_json -> 'dataset_affiliation_organization'::text) ->> 'dataset_id'::text), ''::text) = (dataset_id)::text))", ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, False, False),
            ('provider_directory_dataset_network_plan_ready_v1_idx', "CREATE INDEX provider_directory_dataset_network_plan_ready_v1_idx ON __schema__.provider_directory_endpoint_dataset USING btree (dataset_id) WHERE ((is_current IS TRUE) AND ((status)::text = 'published'::text) AND (published_at IS NOT NULL) AND (superseded_at IS NULL) AND (COALESCE(acquisition_root_run_id, import_run_id) IS NOT NULL) AND (COALESCE(((publication_metadata_json -> 'dataset_network_plan'::text) ->> 'complete'::text), 'false'::text) = 'true'::text) AND (COALESCE(((publication_metadata_json -> 'dataset_network_plan'::text) ->> 'version'::text), ''::text) = '1'::text) AND (COALESCE(((publication_metadata_json -> 'dataset_network_plan'::text) ->> 'dataset_id'::text), ''::text) = (dataset_id)::text))", ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, False, False),
            ('provider_directory_endpoint_dataset_acquisition_root_idx', 'CREATE UNIQUE INDEX provider_directory_endpoint_dataset_acquisition_root_idx ON __schema__.provider_directory_endpoint_dataset USING btree (endpoint_id, acquisition_root_run_id) WHERE (acquisition_root_run_id IS NOT NULL)', ('pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default'), '0 0', 2, 2, True, False),
            ('provider_directory_endpoint_dataset_current_idx', 'CREATE UNIQUE INDEX provider_directory_endpoint_dataset_current_idx ON __schema__.provider_directory_endpoint_dataset USING btree (endpoint_id) WHERE (is_current = true)', ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, True, False),
            ('provider_directory_endpoint_dataset_endpoint_idx', 'CREATE INDEX provider_directory_endpoint_dataset_endpoint_idx ON __schema__.provider_directory_endpoint_dataset USING btree (endpoint_id)', ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, False, False),
            ('provider_directory_endpoint_dataset_hash_idx', 'CREATE INDEX provider_directory_endpoint_dataset_hash_idx ON __schema__.provider_directory_endpoint_dataset USING btree (dataset_hash)', ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, False, False),
            ('provider_directory_endpoint_dataset_pkey', 'CREATE UNIQUE INDEX provider_directory_endpoint_dataset_pkey ON __schema__.provider_directory_endpoint_dataset USING btree (dataset_id)', ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, True, True),
            ('provider_directory_endpoint_dataset_status_idx', 'CREATE INDEX provider_directory_endpoint_dataset_status_idx ON __schema__.provider_directory_endpoint_dataset USING btree (status)', ('pg_catalog.text_ops',), ('pg_catalog.default',), '0', 1, 1, False, False),
        ),
    },
    'provider_directory_entity_release_evidence': {
        "kind": 'r',
        "columns": (
            ('source_id', 'character varying(64)', True, '', None),
            ('resource_type', 'character varying(16)', True, '', None),
            ('resource_id', 'character varying(256)', True, '', None),
            ('release_id', 'character varying(256)', True, '', None),
            ('payload_sha256', 'character varying(64)', True, '', None),
            ('payload_json', 'jsonb', True, '', None),
            ('observed_at', 'timestamp with time zone', True, '', None),
        ),
        "constraints": (
            ('provider_directory_entity_evidence_binding_fkey', 'f', 'FOREIGN KEY (source_id, resource_type, resource_id) REFERENCES __schema__.provider_directory_entity_source_binding(source_id, resource_type, resource_id) ON DELETE RESTRICT', ('source_id', 'resource_type', 'resource_id')),
            ('provider_directory_entity_release_evidence_pkey', 'p', 'PRIMARY KEY (source_id, resource_type, resource_id, release_id)', ('source_id', 'resource_type', 'resource_id', 'release_id')),
        ),
        "indexes": (
            ('provider_directory_entity_evidence_release_idx', 'CREATE INDEX provider_directory_entity_evidence_release_idx ON __schema__.provider_directory_entity_release_evidence USING btree (source_id, release_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default'), '0 0', 2, 2, False, False),
            ('provider_directory_entity_release_evidence_pkey', 'CREATE UNIQUE INDEX provider_directory_entity_release_evidence_pkey ON __schema__.provider_directory_entity_release_evidence USING btree (source_id, resource_type, resource_id, release_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0 0', 4, 4, True, True),
        ),
    },
    'provider_directory_entity_source_binding': {
        "kind": 'r',
        "columns": (
            ('source_id', 'character varying(64)', True, '', None),
            ('resource_type', 'character varying(16)', True, '', None),
            ('resource_id', 'character varying(256)', True, '', None),
            ('organization_id', 'uuid', False, '', None),
            ('site_id', 'uuid', False, '', None),
            ('created_at', 'timestamp with time zone', True, '', None),
        ),
        "constraints": (
            ('provider_directory_entity_binding_kind_check', 'c', "CHECK (resource_type::text = 'Organization'::text AND organization_id IS NOT NULL AND site_id IS NULL OR resource_type::text = 'Location'::text AND site_id IS NOT NULL AND organization_id IS NULL)", ('resource_type', 'organization_id', 'site_id')),
            ('provider_directory_entity_source_binding_organization_id_fkey', 'f', 'FOREIGN KEY (organization_id) REFERENCES __schema__.provider_directory_organization_identity(organization_id) ON DELETE RESTRICT', ('organization_id',)),
            ('provider_directory_entity_source_binding_site_id_fkey', 'f', 'FOREIGN KEY (site_id) REFERENCES __schema__.provider_directory_site_identity(site_id) ON DELETE RESTRICT', ('site_id',)),
            ('provider_directory_entity_source_binding_pkey', 'p', 'PRIMARY KEY (source_id, resource_type, resource_id)', ('source_id', 'resource_type', 'resource_id')),
        ),
        "indexes": (
            ('provider_directory_entity_binding_organization_idx', 'CREATE INDEX provider_directory_entity_binding_organization_idx ON __schema__.provider_directory_entity_source_binding USING btree (organization_id)', ('pg_catalog.uuid_ops',), (None,), '0', 1, 1, False, False),
            ('provider_directory_entity_binding_site_idx', 'CREATE INDEX provider_directory_entity_binding_site_idx ON __schema__.provider_directory_entity_source_binding USING btree (site_id)', ('pg_catalog.uuid_ops',), (None,), '0', 1, 1, False, False),
            ('provider_directory_entity_source_binding_pkey', 'CREATE UNIQUE INDEX provider_directory_entity_source_binding_pkey ON __schema__.provider_directory_entity_source_binding USING btree (source_id, resource_type, resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, True, True),
        ),
    },
    'provider_directory_insurance_network_identity': {
        "kind": 'r',
        "columns": (
            ('network_id', 'uuid', True, '', None),
            ('created_at', 'timestamp with time zone', True, '', None),
        ),
        "constraints": (
            ('provider_directory_insurance_network_identity_pkey', 'p', 'PRIMARY KEY (network_id)', ('network_id',)),
        ),
        "indexes": (
            ('provider_directory_insurance_network_identity_pkey', 'CREATE UNIQUE INDEX provider_directory_insurance_network_identity_pkey ON __schema__.provider_directory_insurance_network_identity USING btree (network_id)', ('pg_catalog.uuid_ops',), (None,), '0', 1, 1, True, True),
        ),
    },
    'provider_directory_insurance_network_plan_evidence': {
        "kind": 'r',
        "columns": (
            ('source_id', 'character varying(64)', True, '', None),
            ('release_id', 'character varying(256)', True, '', None),
            ('network_resource_type', 'character varying(16)', True, '', None),
            ('network_resource_id', 'character varying(256)', True, '', None),
            ('insurance_plan_resource_id', 'character varying(256)', True, '', None),
            ('network_refs', 'jsonb', True, '', None),
            ('owned_by_ref', 'text', False, '', None),
            ('administered_by_ref', 'text', False, '', None),
            ('plan_payload_sha256', 'character varying(64)', True, '', None),
            ('plan_payload_json', 'jsonb', True, '', None),
            ('observed_at', 'timestamp with time zone', True, '', None),
        ),
        "constraints": (
            ('pd_insurance_network_plan_org_type_check', 'c', "CHECK (network_resource_type::text = 'Organization'::text)", ('network_resource_type',)),
            ('pd_insurance_network_plan_binding_fkey', 'f', 'FOREIGN KEY (source_id, network_resource_id) REFERENCES __schema__.provider_directory_insurance_network_source_binding(source_id, resource_id) ON DELETE RESTRICT', ('source_id', 'network_resource_id')),
            ('pd_insurance_network_plan_release_fkey', 'f', 'FOREIGN KEY (source_id, network_resource_type, network_resource_id, release_id) REFERENCES __schema__.provider_directory_entity_release_evidence(source_id, resource_type, resource_id, release_id) ON DELETE RESTRICT', ('source_id', 'network_resource_type', 'network_resource_id', 'release_id')),
            ('provider_directory_insurance_network_plan_evidence_pkey', 'p', 'PRIMARY KEY (source_id, release_id, network_resource_id, insurance_plan_resource_id)', ('source_id', 'release_id', 'network_resource_id', 'insurance_plan_resource_id')),
        ),
        "indexes": (
            ('pd_insurance_network_plan_release_idx', 'CREATE INDEX pd_insurance_network_plan_release_idx ON __schema__.provider_directory_insurance_network_plan_evidence USING btree (source_id, release_id, insurance_plan_resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, False, False),
            ('provider_directory_insurance_network_plan_evidence_pkey', 'CREATE UNIQUE INDEX provider_directory_insurance_network_plan_evidence_pkey ON __schema__.provider_directory_insurance_network_plan_evidence USING btree (source_id, release_id, network_resource_id, insurance_plan_resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0 0', 4, 4, True, True),
        ),
    },
    'provider_directory_insurance_network_source_binding': {
        "kind": 'r',
        "columns": (
            ('source_id', 'character varying(64)', True, '', None),
            ('resource_type', 'character varying(16)', True, '', None),
            ('resource_id', 'character varying(256)', True, '', None),
            ('network_id', 'uuid', True, '', None),
            ('created_at', 'timestamp with time zone', True, '', None),
        ),
        "constraints": (
            ('pd_insurance_network_org_type_check', 'c', "CHECK (resource_type::text = 'Organization'::text)", ('resource_type',)),
            ('pd_insurance_network_entity_binding_fkey', 'f', 'FOREIGN KEY (source_id, resource_type, resource_id) REFERENCES __schema__.provider_directory_entity_source_binding(source_id, resource_type, resource_id) ON DELETE RESTRICT', ('source_id', 'resource_type', 'resource_id')),
            ('pd_insurance_network_identity_fkey', 'f', 'FOREIGN KEY (network_id) REFERENCES __schema__.provider_directory_insurance_network_identity(network_id) ON DELETE RESTRICT', ('network_id',)),
            ('provider_directory_insurance_network_source_binding_pkey', 'p', 'PRIMARY KEY (source_id, resource_id)', ('source_id', 'resource_id')),
        ),
        "indexes": (
            ('pd_insurance_network_binding_id_idx', 'CREATE INDEX pd_insurance_network_binding_id_idx ON __schema__.provider_directory_insurance_network_source_binding USING btree (network_id)', ('pg_catalog.uuid_ops',), (None,), '0', 1, 1, False, False),
            ('provider_directory_insurance_network_source_binding_pkey', 'CREATE UNIQUE INDEX provider_directory_insurance_network_source_binding_pkey ON __schema__.provider_directory_insurance_network_source_binding USING btree (source_id, resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default'), '0 0', 2, 2, True, True),
        ),
    },
    'provider_directory_organization_identity': {
        "kind": 'r',
        "columns": (
            ('organization_id', 'uuid', True, '', None),
            ('created_at', 'timestamp with time zone', True, '', None),
        ),
        "constraints": (
            ('provider_directory_organization_identity_pkey', 'p', 'PRIMARY KEY (organization_id)', ('organization_id',)),
        ),
        "indexes": (
            ('provider_directory_organization_identity_pkey', 'CREATE UNIQUE INDEX provider_directory_organization_identity_pkey ON __schema__.provider_directory_organization_identity USING btree (organization_id)', ('pg_catalog.uuid_ops',), (None,), '0', 1, 1, True, True),
        ),
    },
    'provider_directory_resource_identity': {
        "kind": 'r',
        "columns": (
            ('source_id', 'character varying(64)', True, '', None),
            ('resource_type', 'character varying(64)', True, '', None),
            ('resource_id', 'character varying(64)', True, '', None),
            ('entity_id', 'uuid', True, '', None),
            ('created_at', 'timestamp with time zone', True, '', None),
        ),
        "constraints": (
            ('pd_resource_identity_type_check', 'c', "CHECK (resource_type::text = ANY (ARRAY['InsurancePlan'::character varying, 'PractitionerRole'::character varying]::text[]))", ('resource_type',)),
            ('provider_directory_resource_identity_pkey', 'p', 'PRIMARY KEY (source_id, resource_type, resource_id)', ('source_id', 'resource_type', 'resource_id')),
        ),
        "indexes": (
            ('pd_resource_identity_seek_idx', 'CREATE UNIQUE INDEX pd_resource_identity_seek_idx ON __schema__.provider_directory_resource_identity USING btree (source_id, resource_type, entity_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.uuid_ops'), ('pg_catalog.default', 'pg_catalog.default', None), '0 0 0', 3, 3, True, False),
            ('provider_directory_resource_identity_pkey', 'CREATE UNIQUE INDEX provider_directory_resource_identity_pkey ON __schema__.provider_directory_resource_identity USING btree (source_id, resource_type, resource_id)', ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops'), ('pg_catalog.default', 'pg_catalog.default', 'pg_catalog.default'), '0 0 0', 3, 3, True, True),
        ),
    },
    'provider_directory_site_identity': {
        "kind": 'r',
        "columns": (
            ('site_id', 'uuid', True, '', None),
            ('created_at', 'timestamp with time zone', True, '', None),
        ),
        "constraints": (
            ('provider_directory_site_identity_pkey', 'p', 'PRIMARY KEY (site_id)', ('site_id',)),
        ),
        "indexes": (
            ('provider_directory_site_identity_pkey', 'CREATE UNIQUE INDEX provider_directory_site_identity_pkey ON __schema__.provider_directory_site_identity USING btree (site_id)', ('pg_catalog.uuid_ops',), (None,), '0', 1, 1, True, True),
        ),
    },
}
# fmt: on

_TYPES = {"varchar", "_varchar", "text", "uuid", "bool", "int2", "int4", "int8", "jsonb", "timestamp", "timestamptz"}
_OPCLASSES = {
    "text_ops": ("btree", "text", None, "text_ops"),
    "uuid_ops": ("btree", "uuid", None, "uuid_ops"),
    "int2_ops": ("btree", "int2", None, "integer_ops"),
    "int4_ops": ("btree", "int4", None, "integer_ops"),
    "jsonb_ops": ("gin", "jsonb", "text", "jsonb_ops"),
}
_ROOTS_SQL = """SELECT c.oid::bigint,c.relname,n.nspname,c.relkind::text,c.relpersistence::text,
 c.relispartition,c.relrowsecurity,c.relforcerowsecurity,c.relreplident::text,am.amname,
 EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid) AS inherited,
 EXISTS(SELECT FROM pg_catalog.pg_attribute WHERE attrelid=c.oid AND attnum>0 AND attisdropped) AS dropped,
 EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid) AS rules,
 EXISTS(SELECT FROM pg_catalog.pg_policy WHERE polrelid=c.oid) AS policies
 FROM pg_catalog.unnest($1::text[]) requested(name)
 JOIN pg_catalog.pg_class c ON c.oid=pg_catalog.to_regclass(requested.name)
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 LEFT JOIN pg_catalog.pg_am am ON am.oid=c.relam ORDER BY c.relname"""

_PARTITIONS_SQL = """SELECT c.oid::bigint AS root,pg_catalog.pg_get_partkeydef(c.oid) AS partition_key
 FROM pg_catalog.pg_class c WHERE c.oid=ANY($1::oid[])"""

_COLUMNS_SQL = """SELECT a.attrelid::bigint AS root,a.attnum,a.attname,
 CASE WHEN tn.nspname='pg_catalog' AND t.typtype='b' AND t.typname=ANY($2::text[])
 THEN pg_catalog.format_type(a.atttypid,a.atttypmod) END AS type,
 a.attnotnull,a.attgenerated::text,a.attidentity::text,a.attndims,a.atthasmissing,
 pg_catalog.pg_get_expr(d.adbin,d.adrelid,true) AS expression,
 tn.nspname AS type_schema,t.typname,t.typtype::text,
 cn.nspname AS collation_schema,co.collname,co.collprovider::text,co.collisdeterministic
 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
 JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace
 LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 LEFT JOIN pg_catalog.pg_collation co ON co.oid=a.attcollation
 LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid=co.collnamespace
 WHERE a.attrelid=ANY($1::oid[]) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attrelid,a.attnum"""

# Only these two source-declared FKs reference a partitioned root. PostgreSQL
# adds action-only clones as resource children attach. This checks native FK
# lineage, not child provenance/bounds: allocation/history custody stays separate.
_RESOURCE_FKS = (
    "provider_directory_cms_npd_re_dataset_id_resource_type_res_fkey",
    "provider_directory_cms_npd_r_dataset_id_resource_type_res_fkey1",
)
_CLONES_SQL = """SELECT k.oid::bigint,k.conrelid::bigint AS root,k.conparentid::bigint AS parent,
 k.contype='f' AND b.contype='f' AND b.conparentid=0 AND b.conname=ANY($2::text[])
 AND b.confrelid=pg_catalog.to_regclass($3) AND k.conrelid=b.conrelid
 AND k.conkey=b.conkey AND k.confkey=b.confkey AND k.conpfeqop=b.conpfeqop
 AND k.conppeqop=b.conppeqop AND k.conffeqop=b.conffeqop
 AND k.confupdtype=b.confupdtype AND k.confdeltype=b.confdeltype AND k.confmatchtype=b.confmatchtype
 AND NOT k.conperiod AND NOT b.conperiod AND k.confdelsetcols IS NULL AND b.confdelsetcols IS NULL
 AND k.conexclop IS NULL AND b.conexclop IS NULL AND k.conbin IS NULL AND b.conbin IS NULL
 AND k.convalidated AND k.conenforced AND NOT k.condeferrable AND NOT k.condeferred
 AND NOT k.conislocal AND k.coninhcount=1 AND NOT k.connoinherit
 AND child.relnamespace=parent.relnamespace AND child.relkind='r' AND child.relpersistence='p'
 AND child.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap' AND amtype='t')
 AND child.relispartition AND NOT child.relrowsecurity AND NOT child.relforcerowsecurity
 AND (SELECT count(*)=1 AND bool_and(inhparent=b.confrelid) FROM pg_catalog.pg_inherits WHERE inhrelid=k.confrelid)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhparent=k.confrelid)
 AND (SELECT count(*)=1 AND bool_and(inhparent=b.conindid) FROM pg_catalog.pg_inherits WHERE inhrelid=k.conindid)
 AND ix.indrelid=k.confrelid AND ix.indisunique AND ix.indisprimary
 AND child_index.relkind='i' AND child_index.relpersistence='p' AND child_index.relam=parent_index.relam
 AND ix.indisvalid AND ix.indisready AND ix.indislive AND ix.indimmediate AND NOT ix.indisexclusion
 AND ix.indkey=bx.indkey AND ix.indclass=bx.indclass AND ix.indcollation=bx.indcollation
 AND ix.indoption=bx.indoption AND ix.indnkeyatts=bx.indnkeyatts AND ix.indnatts=bx.indnatts
 AND ix.indnullsnotdistinct=bx.indnullsnotdistinct AND ix.indexprs IS NULL AND ix.indpred IS NULL
 AND NOT EXISTS(SELECT FROM pg_catalog.unnest(k.confkey) num
  LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=k.confrelid AND a.attnum=num
  LEFT JOIN pg_catalog.pg_attribute original ON original.attrelid=b.confrelid AND original.attnum=num
  WHERE a.attname IS DISTINCT FROM original.attname OR a.atttypid IS DISTINCT FROM original.atttypid
   OR a.atttypmod IS DISTINCT FROM original.atttypmod OR a.attcollation IS DISTINCT FROM original.attcollation
   OR a.attnotnull IS DISTINCT FROM original.attnotnull OR a.attisdropped OR a.attname IS NULL)
 AND (SELECT count(*)=2 AND count(DISTINCT t.tgtype)=2 AND bool_and(COALESCE((
   t.tgrelid=k.confrelid AND t.tgconstrrelid=k.conrelid AND t.tgconstrindid=k.conindid
   AND t.tgisinternal AND t.tgenabled='O' AND t.tgtype IN (9,17)
   AND t.tgqual IS NULL AND t.tgnargs=0 AND t.tgargs='\\x'::bytea AND t.tgattr::text=''
   AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL AND NOT t.tgdeferrable AND NOT t.tginitdeferred
   AND pt.tgconstraint=b.oid AND pt.tgrelid=b.confrelid AND pt.tgtype=t.tgtype
   AND t.tgfoid=pt.tgfoid AND pn.nspname='pg_catalog'
   AND p.proname=CASE t.tgtype WHEN 9 THEN 'RI_FKey_cascade_del' ELSE 'RI_FKey_noaction_upd' END),false))
  FROM pg_catalog.pg_trigger t LEFT JOIN pg_catalog.pg_trigger pt ON pt.oid=t.tgparentid
  JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_namespace pn ON pn.oid=p.pronamespace
  WHERE t.tgconstraint=k.oid) AS valid
 FROM pg_catalog.pg_constraint k LEFT JOIN pg_catalog.pg_constraint b ON b.oid=k.conparentid
 LEFT JOIN pg_catalog.pg_class child ON child.oid=k.confrelid
 LEFT JOIN pg_catalog.pg_class parent ON parent.oid=b.confrelid
 LEFT JOIN pg_catalog.pg_index ix ON ix.indexrelid=k.conindid
 LEFT JOIN pg_catalog.pg_index bx ON bx.indexrelid=b.conindid
 LEFT JOIN pg_catalog.pg_class child_index ON child_index.oid=k.conindid
 LEFT JOIN pg_catalog.pg_class parent_index ON parent_index.oid=b.conindid
 WHERE k.conrelid=ANY($1::oid[]) AND k.conparentid<>0 ORDER BY k.oid"""

_CLONE_COVERAGE_SQL = """WITH observed AS (
 SELECT conparentid,confrelid,count(*) AS copies FROM pg_catalog.pg_constraint
 WHERE conrelid=ANY($1::oid[]) AND conparentid<>0 GROUP BY conparentid,confrelid)
 SELECT NOT EXISTS(
 SELECT FROM pg_catalog.pg_constraint b JOIN pg_catalog.pg_inherits i ON i.inhparent=b.confrelid
 LEFT JOIN observed k ON k.conparentid=b.oid AND k.confrelid=i.inhrelid
 WHERE b.conrelid=ANY($1::oid[]) AND b.conparentid=0 AND b.conname=ANY($2::text[])
 AND COALESCE(k.copies,0)<>1)"""

_CONSTRAINTS_SQL = """SELECT k.conrelid::bigint AS root,k.oid::bigint,k.conname,k.contype::text,
 k.convalidated,k.conenforced,k.condeferrable,k.condeferred,k.connoinherit,k.conislocal,k.coninhcount,
 k.conparentid::bigint,pg_catalog.pg_get_constraintdef(k.oid,true) AS definition,
 ARRAY(SELECT a.attname::text FROM pg_catalog.unnest(k.conkey) WITH ORDINALITY x(num,pos)
 JOIN pg_catalog.pg_attribute a ON a.attrelid=k.conrelid AND a.attnum=x.num ORDER BY x.pos) AS columns,
 CASE WHEN k.contype<>'f' THEN true ELSE
  (SELECT count(*)=2 AND count(DISTINCT p.proname)=2 AND bool_and(t.tgenabled='O' AND t.tgisinternal
    AND pn.nspname='pg_catalog' AND p.proname IN ('RI_FKey_check_ins','RI_FKey_check_upd'))
   FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
   JOIN pg_catalog.pg_namespace pn ON pn.oid=p.pronamespace
   WHERE t.tgconstraint=k.oid AND t.tgrelid=k.conrelid)
  AND (SELECT count(*)=4 AND bool_and(t.tgenabled='O' AND t.tgisinternal AND pn.nspname='pg_catalog'
    AND p.proname=CASE WHEN t.tgrelid=k.conrelid THEN CASE t.tgtype WHEN 5 THEN 'RI_FKey_check_ins'
     WHEN 17 THEN 'RI_FKey_check_upd' ELSE '' END
     WHEN t.tgtype=17 THEN 'RI_FKey_noaction_upd'
     WHEN t.tgtype=9 THEN CASE k.confdeltype WHEN 'a' THEN 'RI_FKey_noaction_del'
      WHEN 'c' THEN 'RI_FKey_cascade_del' WHEN 'r' THEN 'RI_FKey_restrict_del' ELSE '' END ELSE '' END)
   FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
   JOIN pg_catalog.pg_namespace pn ON pn.oid=p.pronamespace WHERE t.tgconstraint=k.oid)
 END AS native_enforcement
 FROM pg_catalog.pg_constraint k WHERE k.conrelid=ANY($1::oid[]) ORDER BY k.conrelid,k.conname"""

_INDEXES_SQL = """SELECT i.indrelid::bigint AS root,ci.relname,am.amname,
 i.indisunique,i.indisprimary,i.indisexclusion,i.indisvalid,i.indisready,i.indislive,i.indimmediate,
 i.indnullsnotdistinct,i.indnkeyatts,i.indnatts,i.indoption::text,pg_catalog.pg_get_indexdef(i.indexrelid) AS definition,
 ARRAY(SELECT ns.nspname||'.'||op.opcname FROM pg_catalog.unnest(i.indclass) WITH ORDINALITY x(oid,pos)
 JOIN pg_catalog.pg_opclass op ON op.oid=x.oid JOIN pg_catalog.pg_namespace ns ON ns.oid=op.opcnamespace
 ORDER BY pos) AS opclasses,
 ARRAY(SELECT ns.nspname||'.'||co.collname FROM pg_catalog.unnest(i.indcollation) WITH ORDINALITY x(oid,pos)
 LEFT JOIN pg_catalog.pg_collation co ON co.oid=x.oid
 LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid=co.collnamespace ORDER BY pos) AS collations,
 ARRAY(SELECT pg_catalog.jsonb_build_array(ns.nspname,op.opcname,m.amname,it.typname,kt.typname,
   fn.nspname,f.opfname,op.opcdefault,itn.nspname,ktn.nspname)
 FROM pg_catalog.unnest(i.indclass) WITH ORDINALITY x(oid,pos)
 JOIN pg_catalog.pg_opclass op ON op.oid=x.oid JOIN pg_catalog.pg_namespace ns ON ns.oid=op.opcnamespace
 JOIN pg_catalog.pg_am m ON m.oid=op.opcmethod JOIN pg_catalog.pg_type it ON it.oid=op.opcintype
 JOIN pg_catalog.pg_namespace itn ON itn.oid=it.typnamespace
 LEFT JOIN pg_catalog.pg_type kt ON kt.oid=op.opckeytype
 LEFT JOIN pg_catalog.pg_namespace ktn ON ktn.oid=kt.typnamespace
 JOIN pg_catalog.pg_opfamily f ON f.oid=op.opcfamily JOIN pg_catalog.pg_namespace fn ON fn.oid=f.opfnamespace
 ORDER BY pos) AS native_opclasses
 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class ci ON ci.oid=i.indexrelid
 JOIN pg_catalog.pg_am am ON am.oid=ci.relam WHERE i.indrelid=ANY($1::oid[]) ORDER BY i.indrelid,ci.relname"""

# Native pinned builtins have no pg_depend edges. Every recorded layout edge
# must stay inside its declared root/FK, native key/trigger constraint or the two
# existing header CHECK callees. Their bodies/owners require independent custody.
_DEPENDENCIES_SQL = """WITH objects(root,classid,objid) AS (
 SELECT r,'pg_catalog.pg_class'::regclass,r FROM pg_catalog.unnest($1::oid[]) r UNION
 SELECT adrelid,'pg_catalog.pg_attrdef'::regclass,oid FROM pg_catalog.pg_attrdef WHERE adrelid=ANY($1::oid[]) UNION
 SELECT conrelid,'pg_catalog.pg_constraint'::regclass,oid FROM pg_catalog.pg_constraint WHERE conrelid=ANY($1::oid[]) UNION
 SELECT indrelid,'pg_catalog.pg_class'::regclass,indexrelid FROM pg_catalog.pg_index WHERE indrelid=ANY($1::oid[]))
 SELECT NOT EXISTS(SELECT FROM objects o JOIN pg_catalog.pg_depend d ON d.classid=o.classid AND d.objid=o.objid
 WHERE d.deptype NOT IN ('a','i','n') OR NOT (
  (d.refclassid='pg_catalog.pg_namespace'::regclass
   AND d.refobjid=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=$2))
  OR (d.refclassid='pg_catalog.pg_class'::regclass AND d.refobjid=o.root)
  OR (d.classid='pg_catalog.pg_constraint'::regclass AND d.refclassid='pg_catalog.pg_class'::regclass
   AND EXISTS(SELECT FROM pg_catalog.pg_constraint k WHERE k.oid=o.objid AND k.contype='f'
    AND d.refobjid IN(k.confrelid,k.conindid)))
  OR (d.classid='pg_catalog.pg_constraint'::regclass AND d.refclassid='pg_catalog.pg_constraint'::regclass
   AND d.objid=ANY($4::oid[]) AND d.deptype='i' AND d.objsubid=0 AND d.refobjsubid=0
   AND EXISTS(SELECT FROM pg_catalog.pg_constraint k WHERE k.oid=d.objid AND k.conparentid=d.refobjid))
  OR (d.classid='pg_catalog.pg_class'::regclass AND d.refclassid='pg_catalog.pg_constraint'::regclass
   AND EXISTS(SELECT FROM pg_catalog.pg_constraint k WHERE k.oid=d.refobjid AND k.conrelid=o.root AND k.conindid=o.objid))
  OR (d.classid='pg_catalog.pg_constraint'::regclass AND d.refclassid='pg_catalog.pg_trigger'::regclass
   AND EXISTS(SELECT FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_trigger t ON t.oid=d.refobjid
    WHERE k.oid=o.objid AND k.contype='t' AND k.conrelid=o.root AND t.tgconstraint=k.oid AND t.tgname=k.conname))
  OR (d.classid='pg_catalog.pg_constraint'::regclass AND d.refclassid='pg_catalog.pg_proc'::regclass
   AND EXISTS(SELECT FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_proc p ON p.oid=d.refobjid
    WHERE k.oid=o.objid AND k.conrelid=pg_catalog.to_regclass($3)
     AND ((k.conname='pd_endpoint_dataset_completion_digest_check'
      AND p.oid=pg_catalog.to_regprocedure(pg_catalog.format('%I.provider_directory_subset_completion_proof_pair_valid(jsonb,text)',$2)))
      OR (k.conname='pd_endpoint_dataset_completion_shape_check'
      AND p.oid=pg_catalog.to_regprocedure(pg_catalog.format('%I.provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)',$2))))))
 )) AS valid"""


def _require(condition, reason, root=""):
    if not condition:
        raise ValueError("cms_canonical_layout_" + reason + (":" + root if root else ""))


def _expected_expression(value, schema_sql):
    return _normalized_expression(None if value is None else value.replace("__schema__", schema_sql))


def _require_columns(column_rows, expected, root, schema_sql):
    actual_columns = []
    for column_row in column_rows:
        _require(
            column_row["type_schema"] == "pg_catalog"
            and column_row["typname"] in _TYPES
            and column_row["typtype"] == "b",
            "type",
            root,
        )
        is_collatable = column_row["typname"] in {"varchar", "_varchar", "text"}
        collation_values = tuple(
            column_row[k] for k in ("collation_schema", "collname", "collprovider", "collisdeterministic")
        )
        _require(
            collation_values == (("pg_catalog", "default", "d", True) if is_collatable else (None,) * 4),
            "collation",
            root,
        )
        _require(
            not column_row["attidentity"]
            and not column_row["atthasmissing"]
            and column_row["attndims"] == int(column_row["typname"] == "_varchar"),
            "column_native",
            root,
        )
        actual_columns.append(
            (
                column_row["attname"],
                column_row["type"],
                column_row["attnotnull"],
                column_row["attgenerated"],
                _normalized_expression(column_row["expression"]),
            )
        )
    expected_columns = [
        (name, kind, required, generated, _expected_expression(expression, schema_sql))
        for name, kind, required, generated, expression in expected
    ]
    _require(actual_columns == expected_columns, "columns", root)


def _require_constraints(constraint_rows, expected, columns, root, schema_sql, clones):
    actual, not_null = [], []
    for constraint_row in constraint_rows:
        if constraint_row["oid"] in clones:
            # Already authenticated from the fixed base FK and native index/
            # trigger lineage before any dependency or expression rendering.
            continue
        kind = constraint_row["contype"]
        _require(
            kind in {"c", "f", "n", "p", "t", "u"}
            and constraint_row["convalidated"]
            and constraint_row["conenforced"]
            and constraint_row["native_enforcement"]
            and constraint_row["conislocal"]
            and constraint_row["coninhcount"] == 0
            and constraint_row["conparentid"] == 0
            and constraint_row["condeferrable"] == (kind == "t")
            and constraint_row["condeferred"] == (kind == "t")
            and constraint_row["connoinherit"] == (kind in {"f", "p", "t", "u"}),
            "constraint_native",
            root,
        )
        if kind == "n":
            _require(
                len(constraint_row["columns"]) == 1
                and _normalized_expression(constraint_row["definition"])
                == _normalized_expression("NOT NULL " + constraint_row["columns"][0]),
                "not_null",
                root,
            )
            not_null.extend(constraint_row["columns"])
        else:
            actual.append(
                (
                    constraint_row["conname"],
                    kind,
                    _normalized_expression(constraint_row["definition"]),
                    tuple(constraint_row["columns"]),
                )
            )
    expected_constraints = [
        (name, kind, _expected_expression(definition, schema_sql), tuple(keys))
        for name, kind, definition, keys in expected
    ]
    _require(Counter(actual) == Counter(expected_constraints), "constraints", root)
    _require(
        Counter(not_null)
        == Counter(name for name, _kind, required, _generated, _expression_value in columns if required),
        "not_null",
        root,
    )


def _without_authenticated_history_bounds(constraints, expected):
    retained_constraints = []
    seen = Counter()
    by_oid = {entry["native"]["constraint"]["oid"]: entry["old_oid"] for entry in expected.values()}
    for constraint in constraints:
        if constraint["oid"] in by_oid:
            _require(
                constraint["root"] == by_oid[constraint["oid"]]
                and constraint["conname"] == "cms_history_bound"
                and constraint["contype"] == "c",
                "history_bound_changed",
            )
            seen[constraint["oid"]] += 1
        else:
            retained_constraints.append(constraint)
    _require(seen == Counter(dict.fromkeys(by_oid, 1)), "history_bound_changed")
    return retained_constraints


def _require_indexes(index_rows, expected, root, schema_sql):
    import json

    actual_indexes = []
    for index_row in index_rows:
        _require(
            all(index_row[k] for k in ("indisvalid", "indisready", "indislive", "indimmediate"))
            and not index_row["indisexclusion"]
            and not index_row["indnullsnotdistinct"],
            "index_native",
            root,
        )
        for encoded_opclass in index_row["native_opclasses"]:
            op = json.loads(encoded_opclass) if isinstance(encoded_opclass, str) else encoded_opclass
            (
                namespace,
                name,
                method,
                input_type,
                key_type,
                family_namespace,
                family,
                default,
                input_namespace,
                key_namespace,
            ) = op
            _require(
                namespace == family_namespace == input_namespace == "pg_catalog"
                and default
                and name in _OPCLASSES
                and (method, input_type, key_type, family) == _OPCLASSES[name]
                and key_namespace == ("pg_catalog" if key_type else None),
                "operator_class",
                root,
            )
        actual_indexes.append(
            (
                index_row["relname"],
                _normalized_expression(index_row["definition"]),
                tuple(index_row["opclasses"]),
                tuple(index_row["collations"]),
                index_row["indoption"],
                index_row["indnkeyatts"],
                index_row["indnatts"],
                index_row["indisunique"],
                index_row["indisprimary"],
            )
        )
    expected_indexes = [
        (name, _expected_expression(definition, schema_sql), *rest) for name, definition, *rest in expected
    ]
    _require(Counter(actual_indexes) == Counter(expected_indexes), "indexes", root)


def _require_root_layouts(
    roots, catalog_schema, settings, clone_oid_set, partitions_by_oid, columns, constraints, indexes
):
    for root_row in roots:
        root = root_row["relname"]
        expected = _LAYOUTS[root]
        is_partitioned = expected["kind"] == "p"
        _require(
            root_row["nspname"] == catalog_schema
            and root_row["relkind"] == expected["kind"]
            and root_row["relpersistence"] == "p"
            and not any(
                root_row[k]
                for k in (
                    "relispartition",
                    "relrowsecurity",
                    "relforcerowsecurity",
                    "inherited",
                    "dropped",
                    "rules",
                    "policies",
                )
            )
            and root_row["relreplident"] == "d"
            and root_row["amname"] == (None if is_partitioned else "heap")
            and partitions_by_oid[root_row["oid"]] == ("LIST (dataset_id)" if is_partitioned else None),
            "root_native",
            root,
        )
        oid = root_row["oid"]
        schema_sql = settings["schema_sql"]
        _require_columns([entry for entry in columns if entry["root"] == oid], expected["columns"], root, schema_sql)
        _require_constraints(
            [entry for entry in constraints if entry["root"] == oid],
            expected["constraints"],
            expected["columns"],
            root,
            schema_sql,
            clone_oid_set,
        )
        _require_indexes([entry for entry in indexes if entry["root"] == oid], expected["indexes"], root, schema_sql)


async def _require_layout_dependencies(connection, catalog_schema, oids):
    clones = await connection.fetch(
        _CLONES_SQL, oids, list(_RESOURCE_FKS), storage_relation(catalog_schema, "provider_directory_dataset_resource")
    )
    _require(all(row["valid"] is True for row in clones), "fk_clone")
    _require(await connection.fetchval(_CLONE_COVERAGE_SQL, oids, list(_RESOURCE_FKS)), "fk_clone_coverage")
    clone_oids = [row["oid"] for row in clones]
    # Reject nonnative dependencies before metadata rendering invokes native output functions.
    _require(
        await connection.fetchval(
            _DEPENDENCIES_SQL,
            oids,
            catalog_schema,
            storage_relation(catalog_schema, "provider_directory_endpoint_dataset"),
            clone_oids,
        ),
        "dependencies",
    )
    return frozenset(clone_oids)


async def require_cms_canonical_layout(connection, *, catalog_schema, storage_class, expected_history_bounds=None):
    """Read fixed legacy root semantics only; caller retains every authority/fence.

    No payload scan, explicit lock, setting change or observation hash is
    used. Caller-owned role/DDL stability is required before native deparsing.
    Both native-only search paths are accepted without changing the caller.
    Referenced child provenance/bounds remain separate authenticated custody.
    New root classes and snapshot conversion must
    be qualified explicitly rather than silently broadening this starting class.
    """
    _require(storage_class == CONTRACT, "storage_class")
    _require(type(catalog_schema) is str and 0 < len(catalog_schema.encode()) <= 63, "schema")
    if expected_history_bounds is not None:
        from process.provider_directory_cms_history_bounds import require_cms_history_expectations

        expected_history_bounds = require_cms_history_expectations(expected_history_bounds)
    _require(connection.is_in_transaction(), "transaction")
    settings = await connection.fetchrow(
        "SELECT pg_catalog.current_setting('server_version_num')::integer AS version,"
        "pg_catalog.current_setting('search_path') AS search_path,pg_catalog.quote_ident($1::text) AS schema_sql",
        catalog_schema,
    )
    _require(
        180000 <= settings["version"] < 190000 and settings["search_path"] in {"pg_catalog", "pg_catalog, pg_temp"},
        "environment",
    )
    roots = await connection.fetch(_ROOTS_SQL, [storage_relation(catalog_schema, name) for name in _LAYOUTS])
    _require(len(roots) == len(_LAYOUTS) and {entry["relname"] for entry in roots} == set(_LAYOUTS), "roots")
    if expected_history_bounds is not None:
        roots_by_name = {entry["relname"]: entry for entry in roots}
        _require(
            all(roots_by_name[root]["oid"] == entry["old_oid"] for root, entry in expected_history_bounds.items()),
            "history_bound_changed",
        )
    oids = [root_row["oid"] for root_row in roots]
    clone_oid_set = await _require_layout_dependencies(connection, catalog_schema, oids)
    if expected_history_bounds is not None:
        from process.provider_directory_cms_history_bounds import require_cms_source_history_bounds

        expected_history_bounds = await require_cms_source_history_bounds(
            connection, catalog_schema=catalog_schema, expected=expected_history_bounds
        )
    partitions_by_oid = {
        root_row["root"]: root_row["partition_key"] for root_row in await connection.fetch(_PARTITIONS_SQL, oids)
    }
    columns = await connection.fetch(_COLUMNS_SQL, oids, sorted(_TYPES))
    constraints = await connection.fetch(_CONSTRAINTS_SQL, oids)
    if expected_history_bounds is not None:
        constraints = _without_authenticated_history_bounds(constraints, expected_history_bounds)
    indexes = await connection.fetch(_INDEXES_SQL, oids)
    _require_root_layouts(
        roots, catalog_schema, settings, clone_oid_set, partitions_by_oid, columns, constraints, indexes
    )
    return {"contract": CONTRACT, "root_count": len(roots), "evidence": "fixed_layout_only"}
