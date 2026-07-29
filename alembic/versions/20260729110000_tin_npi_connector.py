"""Add the generation-swapped TIN-to-NPI connector foundation.

Revision ID: 20260729110000_tin_npi_connector
Revises: 20260729100000_ptg2_candidate_audit_hold
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260729110000_tin_npi_connector"
down_revision = "20260729100000_ptg2_candidate_audit_hold"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _qf(schema: str, function: str) -> str:
    return f"{_q(schema)}.{_q(function)}"


def upgrade() -> None:
    """Install an empty, token-only connector generation catalog."""

    schema = _schema()
    token_policy = _qt(schema, "tin_npi_connector_token_policy")
    identifier_policy = _qt(
        schema,
        "tin_npi_connector_identifier_policy",
    )
    generation = _qt(schema, "tin_npi_connector_generation")
    generation_policy = _qt(schema, "tin_npi_connector_generation_policy")
    lookup = _qt(schema, "tin_npi_connector_lookup")
    evidence = _qt(schema, "tin_npi_connector_evidence")
    current = _qt(schema, "tin_npi_connector_current")
    provider_directory_endpoint = _qt(
        schema,
        "provider_directory_api_endpoint",
    )
    provider_directory_source = _qt(schema, "provider_directory_source")
    provider_directory_dataset = _qt(
        schema,
        "provider_directory_endpoint_dataset",
    )
    provider_directory_resource = _qt(
        schema,
        "provider_directory_dataset_resource",
    )
    ptg_tax_identity_manifest = _qt(
        schema,
        "ptg2_provider_tax_identity_manifest",
    )

    valid_npi = _qf(schema, "tin_npi_connector_valid_npi")
    valid_npis = _qf(schema, "tin_npi_connector_valid_npis")
    policy_descriptor = _qf(
        schema,
        "tin_npi_connector_token_policy_descriptor_sha256",
    )
    valid_source_vector = _qf(
        schema,
        "tin_npi_connector_valid_source_vector",
    )
    valid_identifier_policy = _qf(
        schema,
        "tin_npi_connector_valid_identifier_policy",
    )
    identifier_rule_digest = _qf(
        schema,
        "tin_npi_connector_identifier_rule_sha256",
    )
    valid_source_ordinals = _qf(
        schema,
        "tin_npi_connector_valid_source_ordinal_map",
    )
    valid_source_evidence = _qf(
        schema,
        "tin_npi_connector_valid_source_evidence",
    )
    valid_scan_proof = _qf(
        schema,
        "tin_npi_connector_valid_scan_proof",
    )
    lookup_row_digest = _qf(
        schema,
        "tin_npi_connector_lookup_row_sha256",
    )
    lookup_set_digest = _qf(
        schema,
        "tin_npi_connector_lookup_set_sha256",
    )
    evidence_id_digest = _qf(
        schema,
        "tin_npi_connector_evidence_id_sha256",
    )
    evidence_set_digest = _qf(
        schema,
        "tin_npi_connector_evidence_set_sha256",
    )
    build_token_matches = _qf(
        schema,
        "tin_npi_connector_build_token_matches",
    )
    token_policy_guard = _qf(
        schema,
        "guard_tin_npi_connector_token_policy",
    )
    generation_guard = _qf(schema, "guard_tin_npi_connector_generation")
    child_insert_guard = _qf(
        schema,
        "guard_tin_npi_connector_child_insert",
    )
    child_mutation_guard = _qf(
        schema,
        "guard_tin_npi_connector_child_mutation",
    )
    current_guard = _qf(schema, "guard_tin_npi_connector_current")
    truncate_guard = _qf(schema, "guard_tin_npi_connector_truncate")
    dataset_resource_guard = _qf(
        schema,
        "guard_tin_npi_connector_dataset_resource",
    )
    endpoint_dataset_guard = _qf(
        schema,
        "guard_tin_npi_connector_endpoint_dataset",
    )
    source_fence = _qf(schema, "assert_tin_npi_connector_source_fence")
    token_policy_fence = _qf(
        schema,
        "assert_tin_npi_connector_token_policy_fence",
    )
    publish_generation = _qf(
        schema,
        "publish_tin_npi_connector_generation",
    )
    rollback_generation = _qf(
        schema,
        "rollback_tin_npi_connector_generation",
    )
    abandon_generation = _qf(
        schema,
        "abandon_tin_npi_connector_generation",
    )
    retire_generation = _qf(
        schema,
        "retire_tin_npi_connector_generation",
    )
    gc_generation = _qf(schema, "gc_tin_npi_connector_generation")

    op.execute(
        f"""
        CREATE FUNCTION {valid_npi}(candidate bigint)
        RETURNS boolean
        LANGUAGE sql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
            SELECT CASE
                WHEN candidate BETWEEN 1000000000 AND 2999999999 THEN
                    (
                        SELECT (
                            24
                            + SUM(
                                CASE
                                    WHEN digit_position = 10 THEN digit
                                    WHEN digit_position % 2 = 1 THEN
                                        CASE
                                            WHEN digit * 2 > 9
                                            THEN digit * 2 - 9
                                            ELSE digit * 2
                                        END
                                    ELSE digit
                                END
                            )
                        ) % 10 = 0
                          FROM (
                                SELECT digit_position,
                                       substring(
                                           candidate::text
                                           FROM digit_position
                                           FOR 1
                                       )::integer AS digit
                                  FROM generate_series(1, 10)
                                       AS digit_position
                               ) AS digits
                    )
                ELSE FALSE
            END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {valid_npis}(candidate bigint[])
        RETURNS boolean
        LANGUAGE sql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
            SELECT COALESCE(array_lower(candidate, 1) = 1, FALSE)
               AND cardinality(candidate) > 0
               AND NOT EXISTS (
                       SELECT 1
                         FROM generate_subscripts(candidate, 1)
                              AS digit_position
                        WHERE candidate[digit_position] IS NULL
                           OR NOT {valid_npi}(candidate[digit_position])
                           OR (
                               digit_position > 1
                               AND candidate[digit_position - 1]
                                   >= candidate[digit_position]
                           )
                   );
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {policy_descriptor}(candidate_policy_id text)
        RETURNS bytea
        LANGUAGE sql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
            SELECT sha256(
                convert_to('PTG2V4TINPOLICY', 'UTF8')
                || decode('01', 'hex')
                || int4send(
                    octet_length(convert_to(candidate_policy_id, 'UTF8'))
                )
                || convert_to(candidate_policy_id, 'UTF8')
                || int4send(
                    octet_length(
                        convert_to(
                            'ein_ascii_digits_or_2_7_hyphen_v1',
                            'UTF8'
                        )
                    )
                )
                || convert_to(
                    'ein_ascii_digits_or_2_7_hyphen_v1',
                    'UTF8'
                )
                || int4send(
                    octet_length(
                        convert_to(
                            'hmac_sha256_ptg_tin_v1',
                            'UTF8'
                        )
                    )
                )
                || convert_to('hmac_sha256_ptg_tin_v1', 'UTF8')
                || int4send(
                    octet_length(
                        convert_to(
                            'tin_id_128=first_16_bytes(tin_hmac_sha256)',
                            'UTF8'
                        )
                    )
                )
                || convert_to(
                    'tin_id_128=first_16_bytes(tin_hmac_sha256)',
                    'UTF8'
                )
                || int4send(
                    octet_length(
                        convert_to(
                            'tin_hmac_sha256_full_32_bytes_authoritative',
                            'UTF8'
                        )
                    )
                )
                || convert_to(
                    'tin_hmac_sha256_full_32_bytes_authoritative',
                    'UTF8'
                )
            );
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {valid_source_ordinals}(
            canonical_json text,
            expected_count integer
        )
        RETURNS boolean
        LANGUAGE plpgsql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
        DECLARE
            entries jsonb;
            source_entry jsonb;
            ordinal_position bigint;
            previous_source_id text;
            key_count bigint;
        BEGIN
            entries := canonical_json::jsonb;
            IF jsonb_typeof(entries) <> 'array'
               OR jsonb_array_length(entries) <> expected_count
               OR expected_count <= 0 THEN
                RETURN FALSE;
            END IF;
            FOR source_entry, ordinal_position IN
                SELECT value, ordinality
                  FROM jsonb_array_elements(entries)
                       WITH ORDINALITY AS source_entries(value, ordinality)
                 ORDER BY ordinality
            LOOP
                IF jsonb_typeof(source_entry) <> 'object' THEN
                    RETURN FALSE;
                END IF;
                SELECT COUNT(*)
                  INTO key_count
                  FROM jsonb_object_keys(source_entry);
                IF key_count <> 2
                   OR NOT (source_entry ?& ARRAY['ordinal', 'source_id'])
                   OR jsonb_typeof(source_entry -> 'ordinal') <> 'number'
                   OR source_entry ->> 'ordinal'
                        !~ '^(0|[1-9][0-9]*)$'
                   OR (source_entry ->> 'ordinal')::numeric
                        <> ordinal_position - 1
                   OR jsonb_typeof(source_entry -> 'source_id') <> 'string'
                   OR source_entry ->> 'source_id'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,63}}$'
                   OR (
                       previous_source_id IS NOT NULL
                       AND convert_to(
                               source_entry ->> 'source_id',
                               'UTF8'
                           )
                           <= convert_to(previous_source_id, 'UTF8')
                   ) THEN
                    RETURN FALSE;
                END IF;
                previous_source_id := source_entry ->> 'source_id';
            END LOOP;
            RETURN TRUE;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN FALSE;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {identifier_rule_digest}(candidate_rule jsonb)
        RETURNS bytea
        LANGUAGE plpgsql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
        DECLARE
            ein_systems_json text;
            ein_type_codings_json text;
            excluded_identifier_uses_json text;
            npi_systems_json text;
            npi_type_codings_json text;
            canonical_json text;
        BEGIN
            SELECT '[' || COALESCE(
                       string_agg(
                           to_jsonb(value #>> '{{}}')::text,
                           ',' ORDER BY ordinality
                       ),
                       ''
                   ) || ']'
              INTO ein_systems_json
              FROM jsonb_array_elements(candidate_rule -> 'ein_systems')
                   WITH ORDINALITY AS values(value, ordinality);
            SELECT '[' || COALESCE(
                       string_agg(
                           '['
                           || to_jsonb(value ->> 0)::text
                           || ','
                           || to_jsonb(value ->> 1)::text
                           || ']',
                           ',' ORDER BY ordinality
                       ),
                       ''
                   ) || ']'
              INTO ein_type_codings_json
              FROM jsonb_array_elements(
                       candidate_rule -> 'ein_type_codings'
                   ) WITH ORDINALITY AS values(value, ordinality);
            SELECT '[' || COALESCE(
                       string_agg(
                           to_jsonb(value #>> '{{}}')::text,
                           ',' ORDER BY ordinality
                       ),
                       ''
                   ) || ']'
              INTO excluded_identifier_uses_json
              FROM jsonb_array_elements(
                       candidate_rule -> 'excluded_identifier_uses'
                   ) WITH ORDINALITY AS values(value, ordinality);
            SELECT '[' || COALESCE(
                       string_agg(
                           to_jsonb(value #>> '{{}}')::text,
                           ',' ORDER BY ordinality
                       ),
                       ''
                   ) || ']'
              INTO npi_systems_json
              FROM jsonb_array_elements(candidate_rule -> 'npi_systems')
                   WITH ORDINALITY AS values(value, ordinality);
            SELECT '[' || COALESCE(
                       string_agg(
                           '['
                           || to_jsonb(value ->> 0)::text
                           || ','
                           || to_jsonb(value ->> 1)::text
                           || ']',
                           ',' ORDER BY ordinality
                       ),
                       ''
                   ) || ']'
              INTO npi_type_codings_json
              FROM jsonb_array_elements(
                       candidate_rule -> 'npi_type_codings'
                   ) WITH ORDINALITY AS values(value, ordinality);
            canonical_json :=
                '{{"ein_systems":' || ein_systems_json
                || ',"ein_type_codings":' || ein_type_codings_json
                || ',"endpoint_id":'
                || to_jsonb(candidate_rule ->> 'endpoint_id')::text
                || ',"excluded_identifier_uses":'
                || excluded_identifier_uses_json
                || ',"npi_systems":' || npi_systems_json
                || ',"npi_type_codings":' || npi_type_codings_json
                || ',"period_policy_id":'
                || to_jsonb(candidate_rule ->> 'period_policy_id')::text
                || ',"rule_id":'
                || to_jsonb(candidate_rule ->> 'rule_id')::text
                || ',"source_id":'
                || to_jsonb(candidate_rule ->> 'source_id')::text
                || '}}';
            RETURN sha256(
                convert_to(
                    'healthporta.tin-npi.fhir-identifier-rule.v1',
                    'UTF8'
                )
                || decode('00', 'hex')
                || convert_to(canonical_json, 'UTF8')
            );
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {valid_identifier_policy}(
            canonical_json text,
            expected_policy_id text
        )
        RETURNS boolean
        LANGUAGE plpgsql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
        DECLARE
            payload jsonb;
            rule jsonb;
            key_count bigint;
            previous_source_id text;
            previous_endpoint_id text;
            previous_rule_id text;
        BEGIN
            payload := canonical_json::jsonb;
            IF jsonb_typeof(payload) <> 'object' THEN
                RETURN FALSE;
            END IF;
            SELECT COUNT(*) INTO key_count FROM jsonb_object_keys(payload);
            IF key_count <> 2
               OR NOT (
                   payload ?& ARRAY[
                       'policy_id',
                       'rules'
                   ]
               )
               OR payload ->> 'policy_id' <> expected_policy_id
               OR jsonb_typeof(payload -> 'rules') <> 'array'
               OR jsonb_array_length(payload -> 'rules') <= 0 THEN
                RETURN FALSE;
            END IF;
            FOR rule IN
                SELECT value
                  FROM jsonb_array_elements(payload -> 'rules')
            LOOP
                IF jsonb_typeof(rule) <> 'object' THEN
                    RETURN FALSE;
                END IF;
                SELECT COUNT(*) INTO key_count FROM jsonb_object_keys(rule);
                IF key_count <> 10
                   OR NOT (
                       rule ?& ARRAY[
                           'endpoint_id',
                           'ein_systems',
                           'ein_type_codings',
                           'excluded_identifier_uses',
                           'identifier_rule_sha256',
                           'npi_systems',
                           'npi_type_codings',
                           'period_policy_id',
                           'rule_id',
                           'source_id'
                       ]
                   )
                   OR rule ->> 'source_id'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,63}}$'
                   OR rule ->> 'endpoint_id'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,63}}$'
                   OR rule ->> 'rule_id'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,127}}$'
                   OR rule ->> 'identifier_rule_sha256'
                        !~ '^[0-9a-f]{{64}}$'
                   OR rule ->> 'period_policy_id'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,63}}$'
                   OR jsonb_typeof(rule -> 'ein_systems') <> 'array'
                   OR jsonb_typeof(rule -> 'ein_type_codings') <> 'array'
                   OR jsonb_typeof(rule -> 'excluded_identifier_uses')
                        <> 'array'
                   OR jsonb_typeof(rule -> 'npi_systems') <> 'array'
                   OR jsonb_typeof(rule -> 'npi_type_codings') <> 'array'
                   OR (
                       jsonb_array_length(rule -> 'npi_systems') = 0
                       AND jsonb_array_length(rule -> 'npi_type_codings') = 0
                   )
                   OR (
                       jsonb_array_length(rule -> 'ein_systems') = 0
                       AND jsonb_array_length(rule -> 'ein_type_codings') = 0
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                                  (rule -> 'ein_systems')
                                  || (rule -> 'npi_systems')
                                  || (rule -> 'excluded_identifier_uses')
                              ) AS values(value)
                        WHERE jsonb_typeof(value) <> 'string'
                           OR value #>> '{{}}' = ''
                           OR octet_length(value #>> '{{}}')
                                <> length(value #>> '{{}}')
                           OR length(value #>> '{{}}') > 256
                           OR value #>> '{{}}' ~ '[[:space:]"\\\\]'
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                                  (rule -> 'ein_type_codings')
                                  || (rule -> 'npi_type_codings')
                              ) AS codings(coding)
                        WHERE jsonb_typeof(coding) <> 'array'
                           OR jsonb_array_length(coding) <> 2
                           OR EXISTS (
                               SELECT 1
                                 FROM jsonb_array_elements(coding)
                                      AS coding_values(value)
                                WHERE jsonb_typeof(value) <> 'string'
                                   OR value #>> '{{}}' = ''
                                   OR octet_length(value #>> '{{}}')
                                        <> length(value #>> '{{}}')
                                   OR length(value #>> '{{}}') > 256
                                   OR value #>> '{{}}'
                                        ~ '[[:space:]"\\\\]'
                           )
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements_text(
                                  rule -> 'excluded_identifier_uses'
                              ) AS excluded_use(value)
                        WHERE length(value) > 32
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM (
                               VALUES
                                   ('ein_systems'),
                                   ('excluded_identifier_uses'),
                                   ('npi_systems')
                              ) AS fields(field_name)
                        WHERE COALESCE(
                                  (
                                      SELECT jsonb_agg(
                                                 value
                                                 ORDER BY convert_to(
                                                     value #>> '{{}}',
                                                     'UTF8'
                                                 )
                                             )
                                        FROM jsonb_array_elements(
                                                 rule -> fields.field_name
                                             ) AS entries(value)
                                  ),
                                  '[]'::jsonb
                              ) IS DISTINCT FROM
                                  rule -> fields.field_name
                           OR (
                                  SELECT COUNT(*)
                                    FROM jsonb_array_elements(
                                             rule -> fields.field_name
                                         ) AS entries(value)
                              ) <> (
                                  SELECT COUNT(DISTINCT value)
                                    FROM jsonb_array_elements(
                                             rule -> fields.field_name
                                         ) AS entries(value)
                              )
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM (
                               VALUES
                                   ('ein_type_codings'),
                                   ('npi_type_codings')
                              ) AS fields(field_name)
                        WHERE COALESCE(
                                  (
                                      SELECT jsonb_agg(
                                                 value
                                                 ORDER BY
                                                     convert_to(
                                                         value ->> 0,
                                                         'UTF8'
                                                     ),
                                                     convert_to(
                                                         value ->> 1,
                                                         'UTF8'
                                                     )
                                             )
                                        FROM jsonb_array_elements(
                                                 rule -> fields.field_name
                                             ) AS entries(value)
                                  ),
                                  '[]'::jsonb
                              ) IS DISTINCT FROM
                                  rule -> fields.field_name
                           OR (
                                  SELECT COUNT(*)
                                    FROM jsonb_array_elements(
                                             rule -> fields.field_name
                                         ) AS entries(value)
                              ) <> (
                                  SELECT COUNT(DISTINCT value)
                                    FROM jsonb_array_elements(
                                             rule -> fields.field_name
                                         ) AS entries(value)
                              )
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements_text(
                                  rule -> 'npi_systems'
                              ) AS npi(value)
                         JOIN jsonb_array_elements_text(
                                  rule -> 'ein_systems'
                              ) AS ein(value)
                           ON ein.value = npi.value
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                                  rule -> 'npi_type_codings'
                              ) AS npi(value)
                         JOIN jsonb_array_elements(
                                  rule -> 'ein_type_codings'
                              ) AS ein(value)
                           ON ein.value = npi.value
                   )
                   OR decode(
                          rule ->> 'identifier_rule_sha256',
                          'hex'
                      ) <> {identifier_rule_digest}(
                          rule - 'identifier_rule_sha256'
                      )
                   OR (
                       previous_source_id IS NOT NULL
                       AND ROW(
                               convert_to(rule ->> 'source_id', 'UTF8'),
                               convert_to(rule ->> 'endpoint_id', 'UTF8'),
                               convert_to(rule ->> 'rule_id', 'UTF8')
                           )
                           <= ROW(
                               convert_to(previous_source_id, 'UTF8'),
                               convert_to(previous_endpoint_id, 'UTF8'),
                               convert_to(previous_rule_id, 'UTF8')
                           )
                   ) THEN
                    RETURN FALSE;
                END IF;
                previous_source_id := rule ->> 'source_id';
                previous_endpoint_id := rule ->> 'endpoint_id';
                previous_rule_id := rule ->> 'rule_id';
            END LOOP;
            RETURN TRUE;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN FALSE;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {valid_source_vector}(
            canonical_json text,
            expected_schema_version smallint,
            expected_lookup_schema_version smallint,
            expected_lookup_contract_id text,
            expected_projection_policy_id text,
            expected_site_contract_id text,
            expected_source_record_contract_id text,
            expected_identifier_policy_id text,
            expected_identifier_policy_sha256 bytea,
            expected_evidence_as_of text,
            expected_dataset_count integer,
            expected_relation_count integer,
            expected_token_policy_count integer
        )
        RETURNS boolean
        LANGUAGE plpgsql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
        DECLARE
            payload jsonb;
            item jsonb;
            key_count bigint;
            previous_policy_id text;
        BEGIN
            payload := canonical_json::jsonb;
            IF expected_evidence_as_of !~
                    '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T'
                    '[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}'
                    '\\.[0-9]{{6}}Z$'
               OR jsonb_typeof(payload) <> 'object' THEN
                RETURN FALSE;
            END IF;
            PERFORM expected_evidence_as_of::timestamptz;
            SELECT COUNT(*) INTO key_count FROM jsonb_object_keys(payload);
            IF key_count <> 15
               OR NOT (
                   payload ?& ARRAY[
                       'evidence_as_of',
                       'fhir_datasets',
                       'identifier_policy_id',
                       'identifier_policy_sha256',
                       'input_relations',
                       'lookup_contract_id',
                       'lookup_schema_version',
                       'projection_policy_id',
                       'schema_version',
                       'site_resolution_contract_id',
                       'source_scope_contract_id',
                       'source_record_identity_contract_id',
                       'token_policies',
                       'token_policy_scope_contract_id',
                       'token_policy_ids'
                   ]
               )
               OR payload ->> 'schema_version'
                    <> expected_schema_version::text
               OR payload ->> 'lookup_schema_version'
                    <> expected_lookup_schema_version::text
               OR payload ->> 'lookup_contract_id'
                    <> expected_lookup_contract_id
               OR payload ->> 'projection_policy_id'
                    <> expected_projection_policy_id
               OR payload ->> 'site_resolution_contract_id'
                    <> expected_site_contract_id
               OR payload ->> 'source_scope_contract_id'
                    <> 'healthporta.tin-npi.'
                       'all-current-published-organization-sources.v1'
               OR payload ->> 'source_record_identity_contract_id'
                    <> expected_source_record_contract_id
               OR payload ->> 'token_policy_scope_contract_id'
                    <> 'healthporta.tin-npi.'
                       'all-retained-ptg-tax-policy-descriptors.v1'
               OR payload ->> 'identifier_policy_id'
                    <> expected_identifier_policy_id
               OR payload ->> 'identifier_policy_sha256'
                    <> encode(expected_identifier_policy_sha256, 'hex')
               OR payload ->> 'evidence_as_of'
                    <> expected_evidence_as_of
               OR jsonb_typeof(payload -> 'fhir_datasets') <> 'array'
               OR jsonb_array_length(payload -> 'fhir_datasets')
                    <> expected_dataset_count
               OR jsonb_typeof(payload -> 'input_relations') <> 'array'
               OR jsonb_array_length(payload -> 'input_relations')
                    <> expected_relation_count
               OR expected_relation_count <> 1
               OR jsonb_typeof(payload -> 'token_policies') <> 'array'
               OR jsonb_array_length(payload -> 'token_policies')
                    <> expected_token_policy_count
               OR jsonb_typeof(payload -> 'token_policy_ids') <> 'array'
               OR jsonb_array_length(payload -> 'token_policy_ids')
                    <> expected_token_policy_count THEN
                RETURN FALSE;
            END IF;

            FOR item IN
                SELECT value
                  FROM jsonb_array_elements(payload -> 'fhir_datasets')
            LOOP
                IF jsonb_typeof(item) <> 'object' THEN
                    RETURN FALSE;
                END IF;
                SELECT COUNT(*) INTO key_count FROM jsonb_object_keys(item);
                IF key_count <> 20
                   OR NOT (
                       item ?& ARRAY[
                           'dataset_hash',
                           'dataset_id',
                           'endpoint_id',
                           'evidence_run_id',
                           'expected_incumbent_dataset_id',
                           'expected_resources',
                           'identifier_rule_id',
                           'identifier_rule_sha256',
                           'is_current',
                           'organization_resource_count',
                           'organization_resource_sha256',
                           'previous_dataset_id',
                           'promote_on_cutover',
                           'recorded_expected_resources',
                           'resource_count',
                           'selected_resources',
                           'source_id',
                           'source_summary_sha256',
                           'status',
                           'validated_at'
                       ]
                   )
                   OR item ->> 'dataset_hash' !~ '^[0-9a-f]{{64}}$'
                   OR COALESCE(item ->> 'dataset_id', '') = ''
                   OR COALESCE(item ->> 'endpoint_id', '') = ''
                   OR COALESCE(item ->> 'evidence_run_id', '') = ''
                   OR item ->> 'identifier_rule_id'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,127}}$'
                   OR item ->> 'identifier_rule_sha256'
                        !~ '^[0-9a-f]{{64}}$'
                   OR item ->> 'source_id'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,63}}$'
                   OR item ->> 'status' <> 'published'
                   OR jsonb_typeof(item -> 'is_current') <> 'boolean'
                   OR jsonb_typeof(item -> 'promote_on_cutover') <> 'boolean'
                   OR item -> 'is_current' <> 'true'::jsonb
                   OR item -> 'promote_on_cutover' <> 'false'::jsonb
                   OR jsonb_typeof(
                          item -> 'expected_incumbent_dataset_id'
                      ) <> 'null'
                   OR jsonb_typeof(item -> 'previous_dataset_id')
                        NOT IN ('string', 'null')
                   OR jsonb_typeof(item -> 'validated_at') <> 'string'
                   OR COALESCE(item ->> 'validated_at', '') = ''
                   OR jsonb_typeof(item -> 'resource_count') <> 'number'
                   OR item ->> 'resource_count' !~ '^(0|[1-9][0-9]*)$'
                   OR jsonb_typeof(
                          item -> 'organization_resource_count'
                      ) <> 'number'
                   OR item ->> 'organization_resource_count'
                        !~ '^(0|[1-9][0-9]*)$'
                   OR (item ->> 'organization_resource_count')::numeric
                        > (item ->> 'resource_count')::numeric
                   OR item ->> 'organization_resource_sha256'
                        !~ '^[0-9a-f]{{64}}$'
                   OR item ->> 'source_summary_sha256'
                        !~ '^[0-9a-f]{{64}}$'
                   OR jsonb_typeof(item -> 'selected_resources') <> 'array'
                   OR NOT (
                       (item -> 'selected_resources')
                           @> '["Organization"]'::jsonb
                   )
                   OR jsonb_typeof(item -> 'expected_resources') <> 'array'
                   OR jsonb_typeof(
                          item -> 'recorded_expected_resources'
                      ) <> 'array'
                   OR jsonb_array_length(
                          item -> 'recorded_expected_resources'
                   ) <> jsonb_array_length(item -> 'expected_resources')
                   OR NOT (
                       (item -> 'recorded_expected_resources')
                           @> (item -> 'expected_resources')
                       AND (item -> 'recorded_expected_resources')
                           <@ (item -> 'expected_resources')
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                                  item -> 'selected_resources'
                              ) AS resources(value)
                        WHERE jsonb_typeof(value) <> 'string'
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                                  item -> 'expected_resources'
                              ) AS resources(value)
                        WHERE jsonb_typeof(value) <> 'string'
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                                  item -> 'recorded_expected_resources'
                              ) AS resources(value)
                        WHERE jsonb_typeof(value) <> 'string'
                   ) THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
            IF (
                SELECT COUNT(*)
                  FROM jsonb_array_elements(payload -> 'fhir_datasets')
            ) <> (
                SELECT COUNT(*)
                  FROM (
                        SELECT DISTINCT
                               value ->> 'source_id',
                               value ->> 'endpoint_id',
                               value ->> 'dataset_id'
                          FROM jsonb_array_elements(
                                   payload -> 'fhir_datasets'
                               )
                       ) AS distinct_datasets
            ) THEN
                RETURN FALSE;
            END IF;
            IF (
                SELECT COUNT(*)
                  FROM jsonb_array_elements(payload -> 'fhir_datasets')
            ) <> (
                SELECT COUNT(
                           DISTINCT value ->> 'source_id'
                       )
                  FROM jsonb_array_elements(payload -> 'fhir_datasets')
            ) OR EXISTS (
                SELECT 1
                 FROM jsonb_array_elements(payload -> 'fhir_datasets')
                       AS datasets(dataset)
                 GROUP BY dataset ->> 'endpoint_id'
                HAVING COUNT(
                           DISTINCT dataset
                               - 'source_id'
                               - 'identifier_rule_id'
                               - 'identifier_rule_sha256'
                       ) <> 1
            ) THEN
                RETURN FALSE;
            END IF;

            FOR item IN
                SELECT value
                  FROM jsonb_array_elements(payload -> 'input_relations')
            LOOP
                IF jsonb_typeof(item) <> 'object' THEN
                    RETURN FALSE;
                END IF;
                SELECT COUNT(*) INTO key_count FROM jsonb_object_keys(item);
                IF key_count <> 5
                   OR NOT (
                       item ?& ARRAY[
                           'relation',
                           'relation_oid',
                           'relkind',
                           'relpersistence',
                           'schema'
                       ]
                   )
                   OR item ->> 'schema'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,62}}$'
                   OR item ->> 'schema' <> {_ql(schema)}
                   OR item ->> 'relation'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,62}}$'
                   OR item ->> 'relation_oid' !~ '^[1-9][0-9]*$'
                   OR item ->> 'relation' <>
                        'provider_directory_dataset_resource'
                   OR item ->> 'relkind' NOT IN ('r', 'p')
                   OR item ->> 'relpersistence' <> 'p' THEN
                    RETURN FALSE;
                END IF;
            END LOOP;

            FOR item IN
                SELECT value
                  FROM jsonb_array_elements(payload -> 'token_policies')
                 ORDER BY convert_to(value ->> 'token_policy_id', 'UTF8')
            LOOP
                IF jsonb_typeof(item) <> 'object' THEN
                    RETURN FALSE;
                END IF;
                SELECT COUNT(*) INTO key_count FROM jsonb_object_keys(item);
                IF key_count <> 2
                   OR NOT (
                       item ?& ARRAY[
                           'token_policy_descriptor_sha256',
                           'token_policy_id'
                       ]
                   )
                   OR item ->> 'token_policy_id'
                        !~ '^ptg-tin-hmac-sha256-v1:[a-z0-9]'
                            '[a-z0-9._-]{{0,31}}$'
                   OR octet_length(item ->> 'token_policy_id') > 55
                   OR item ->> 'token_policy_descriptor_sha256'
                        !~ '^[0-9a-f]{{64}}$'
                   OR (
                       previous_policy_id IS NOT NULL
                       AND convert_to(
                               item ->> 'token_policy_id',
                               'UTF8'
                           )
                           <= convert_to(previous_policy_id, 'UTF8')
                   ) THEN
                    RETURN FALSE;
                END IF;
                previous_policy_id := item ->> 'token_policy_id';
            END LOOP;
            IF EXISTS (
                (
                    SELECT value #>> '{{}}' AS token_policy_id
                      FROM jsonb_array_elements(
                               payload -> 'token_policy_ids'
                           )
                     WHERE jsonb_typeof(value) = 'string'
                    EXCEPT
                    SELECT value ->> 'token_policy_id'
                      FROM jsonb_array_elements(
                               payload -> 'token_policies'
                           )
                )
                UNION ALL
                (
                    SELECT value ->> 'token_policy_id'
                      FROM jsonb_array_elements(
                               payload -> 'token_policies'
                           )
                    EXCEPT
                    SELECT value #>> '{{}}'
                      FROM jsonb_array_elements(
                               payload -> 'token_policy_ids'
                           )
                     WHERE jsonb_typeof(value) = 'string'
                )
            ) OR EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(payload -> 'token_policy_ids')
                       WITH ORDINALITY AS policies(value, ordinal)
                 WHERE jsonb_typeof(value) <> 'string'
            ) THEN
                RETURN FALSE;
            END IF;
            RETURN TRUE;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN FALSE;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {valid_source_evidence}(
            candidate_source_bitmap bytea,
            candidate_npi_source_bitmap_matrix bytea,
            candidate_source_evidence_counts bigint[],
            candidate_evidence_count bigint,
            candidate_npis bigint[]
        )
        RETURNS boolean
        LANGUAGE plpgsql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
        DECLARE
            source_count integer;
            npi_count integer;
            bitmap_width integer;
            source_ordinal integer;
            npi_ordinal integer;
            source_evidence_count bigint;
            npi_support_count bigint;
            observed_evidence_count numeric := 0;
            observed_bit boolean;
            expected_bit boolean;
            npi_bit boolean;
            segment_has_source boolean;
        BEGIN
            source_count := cardinality(candidate_source_evidence_counts);
            npi_count := cardinality(candidate_npis);
            bitmap_width := (source_count + 7) / 8;
            IF source_count <= 0
               OR npi_count <= 0
               OR array_lower(candidate_source_evidence_counts, 1) <> 1
               OR array_lower(candidate_npis, 1) <> 1
               OR candidate_evidence_count <= 0
               OR octet_length(candidate_source_bitmap)
                    <> bitmap_width
               OR octet_length(candidate_npi_source_bitmap_matrix)
                    <> npi_count * bitmap_width THEN
                RETURN FALSE;
            END IF;
            FOR npi_ordinal IN 0..(npi_count - 1) LOOP
                segment_has_source := FALSE;
                FOR source_ordinal IN 1..source_count LOOP
                    npi_bit := (
                        get_byte(
                            candidate_npi_source_bitmap_matrix,
                            npi_ordinal * bitmap_width
                                + (source_ordinal - 1) / 8
                        )
                        & (1 << ((source_ordinal - 1) % 8))
                    ) <> 0;
                    segment_has_source := segment_has_source OR npi_bit;
                END LOOP;
                IF NOT segment_has_source
                   OR (
                       source_count % 8 <> 0
                       AND get_byte(
                               candidate_npi_source_bitmap_matrix,
                               npi_ordinal * bitmap_width
                                   + bitmap_width - 1
                           ) >= (1 << (source_count % 8))
                   ) THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
            FOR source_ordinal IN 1..source_count LOOP
                source_evidence_count :=
                    candidate_source_evidence_counts[source_ordinal];
                IF source_evidence_count IS NULL
                   OR source_evidence_count < 0 THEN
                    RETURN FALSE;
                END IF;
                expected_bit := source_evidence_count > 0;
                observed_bit := (
                    get_byte(
                        candidate_source_bitmap,
                        (source_ordinal - 1) / 8
                    )
                    & (1 << ((source_ordinal - 1) % 8))
                ) <> 0;
                npi_support_count := 0;
                FOR npi_ordinal IN 0..(npi_count - 1) LOOP
                    npi_bit := (
                        get_byte(
                            candidate_npi_source_bitmap_matrix,
                            npi_ordinal * bitmap_width
                                + (source_ordinal - 1) / 8
                        )
                        & (1 << ((source_ordinal - 1) % 8))
                    ) <> 0;
                    IF npi_bit THEN
                        npi_support_count := npi_support_count + 1;
                    END IF;
                END LOOP;
                IF observed_bit <> expected_bit
                   OR observed_bit <> (npi_support_count > 0)
                   OR source_evidence_count < npi_support_count THEN
                    RETURN FALSE;
                END IF;
                observed_evidence_count :=
                    observed_evidence_count + source_evidence_count;
            END LOOP;
            IF source_count % 8 <> 0
               AND get_byte(
                       candidate_source_bitmap,
                       octet_length(candidate_source_bitmap) - 1
                   ) >= (1 << (source_count % 8)) THEN
                RETURN FALSE;
            END IF;
            RETURN observed_evidence_count = candidate_evidence_count;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN FALSE;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {valid_scan_proof}(
            canonical_json text,
            source_vector_canonical_json text,
            expected_dataset_count integer,
            expected_token_policy_count integer,
            expected_organization_count bigint,
            expected_matched_organization_count bigint,
            expected_evidence_count bigint
        )
        RETURNS boolean
        LANGUAGE plpgsql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
        DECLARE
            payload jsonb;
            source_vector jsonb;
            dataset_proof jsonb;
            key_count bigint;
            state_count_sum numeric;
            matched_organization_count numeric;
            policy_count_min numeric;
            policy_count_max numeric;
            total_organization_count numeric := 0;
            total_matched_organization_count numeric := 0;
            total_evidence_count numeric := 0;
        BEGIN
            payload := canonical_json::jsonb;
            source_vector := source_vector_canonical_json::jsonb;
            IF jsonb_typeof(payload) <> 'object'
               OR jsonb_typeof(source_vector) <> 'object' THEN
                RETURN FALSE;
            END IF;
            SELECT COUNT(*) INTO key_count FROM jsonb_object_keys(payload);
            IF key_count <> 3
               OR NOT (
                   payload ?& ARRAY[
                       'contract_id',
                       'datasets',
                       'organization_identity_contract_id'
                   ]
               )
               OR payload ->> 'contract_id' <>
                    'healthporta.tin-npi.fhir-organization-scan.v2'
               OR payload ->> 'organization_identity_contract_id' <>
                    'provider_directory_dataset_resource_type_id_payload_hash_newline_v1'
               OR jsonb_typeof(payload -> 'datasets') <> 'array'
               OR jsonb_array_length(payload -> 'datasets')
                    <> expected_dataset_count THEN
                RETURN FALSE;
            END IF;

            FOR dataset_proof IN
                SELECT value
                  FROM jsonb_array_elements(payload -> 'datasets')
                 ORDER BY convert_to(value ->> 'source_id', 'UTF8'),
                          convert_to(value ->> 'endpoint_id', 'UTF8'),
                          convert_to(value ->> 'dataset_id', 'UTF8')
            LOOP
                SELECT COUNT(*)
                  INTO key_count
                  FROM jsonb_object_keys(dataset_proof);
                IF key_count <> 12
                   OR NOT (
                       dataset_proof ?& ARRAY[
                           'dataset_id',
                           'endpoint_id',
                           'identifier_rule_id',
                           'identifier_rule_sha256',
                           'matched_evidence_counts',
                           'matched_evidence_sha256',
                           'matched_organization_count',
                           'organization_resource_count',
                           'organization_resource_sha256',
                           'source_id',
                           'source_summary_sha256',
                           'state_counts'
                       ]
                   )
                   OR dataset_proof ->> 'source_id'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,63}}$'
                   OR COALESCE(dataset_proof ->> 'endpoint_id', '') = ''
                   OR COALESCE(dataset_proof ->> 'dataset_id', '') = ''
                   OR dataset_proof ->> 'identifier_rule_id'
                        !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,127}}$'
                   OR dataset_proof ->> 'identifier_rule_sha256'
                        !~ '^[0-9a-f]{{64}}$'
                   OR dataset_proof ->> 'matched_evidence_sha256'
                        !~ '^[0-9a-f]{{64}}$'
                   OR dataset_proof ->> 'organization_resource_sha256'
                        !~ '^[0-9a-f]{{64}}$'
                   OR dataset_proof ->> 'source_summary_sha256'
                        !~ '^[0-9a-f]{{64}}$'
                   OR dataset_proof ->> 'organization_resource_count'
                        !~ '^(0|[1-9][0-9]*)$'
                   OR dataset_proof ->> 'matched_organization_count'
                        !~ '^(0|[1-9][0-9]*)$'
                   OR jsonb_typeof(dataset_proof -> 'state_counts')
                        <> 'object'
                   OR jsonb_typeof(
                          dataset_proof -> 'matched_evidence_counts'
                      ) <> 'object' THEN
                    RETURN FALSE;
                END IF;
                SELECT COUNT(*)
                  INTO key_count
                  FROM jsonb_object_keys(
                           dataset_proof -> 'state_counts'
                       );
                IF key_count <> 11
                   OR NOT (
                       (dataset_proof -> 'state_counts') ?& ARRAY[
                           'ambiguous_ein',
                           'conflicting_identifier_class',
                           'inactive',
                           'malformed_ein',
                           'malformed_identifier_period',
                           'malformed_npi',
                           'matched',
                           'missing_ein',
                           'missing_identifiers',
                           'missing_npi',
                           'unresolved_identifier_period'
                       ]
                   )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_each_text(
                                  dataset_proof -> 'state_counts'
                              ) AS states(state_name, state_count)
                        WHERE state_count !~ '^(0|[1-9][0-9]*)$'
                   ) THEN
                    RETURN FALSE;
                END IF;
                SELECT COALESCE(SUM(state_count::numeric), 0),
                       MAX(state_count::numeric)
                           FILTER (WHERE state_name = 'matched')
                  INTO state_count_sum,
                       matched_organization_count
                  FROM jsonb_each_text(
                           dataset_proof -> 'state_counts'
                       ) AS states(state_name, state_count);
                IF state_count_sum <>
                        (dataset_proof
                            ->> 'organization_resource_count')::numeric
                   OR matched_organization_count <>
                        (dataset_proof
                            ->> 'matched_organization_count')::numeric THEN
                    RETURN FALSE;
                END IF;

                SELECT COUNT(*),
                       MIN(policy_count::numeric),
                       MAX(policy_count::numeric),
                       COALESCE(SUM(policy_count::numeric), 0)
                  INTO key_count,
                       policy_count_min,
                       policy_count_max,
                       state_count_sum
                  FROM jsonb_each_text(
                           dataset_proof -> 'matched_evidence_counts'
                       ) AS policy_counts(token_policy_id, policy_count)
                 WHERE policy_count ~ '^(0|[1-9][0-9]*)$';
                IF key_count <> expected_token_policy_count
                   OR policy_count_min IS DISTINCT FROM policy_count_max
                   OR policy_count_min <
                        (dataset_proof
                            ->> 'matched_organization_count')::numeric
                   OR (
                       (
                           dataset_proof
                               ->> 'matched_organization_count'
                       )::numeric = 0
                   ) <> (state_count_sum = 0)
                   OR EXISTS (
                       (
                           SELECT token_policy_id
                             FROM jsonb_each(
                                      dataset_proof
                                          -> 'matched_evidence_counts'
                                  ) AS proof_policies(
                                      token_policy_id,
                                      policy_count
                                  )
                           EXCEPT
                           SELECT value ->> 'token_policy_id'
                             FROM jsonb_array_elements(
                                      source_vector -> 'token_policies'
                                  )
                       )
                       UNION ALL
                       (
                           SELECT value ->> 'token_policy_id'
                             FROM jsonb_array_elements(
                                      source_vector -> 'token_policies'
                                  )
                           EXCEPT
                           SELECT token_policy_id
                             FROM jsonb_each(
                                      dataset_proof
                                          -> 'matched_evidence_counts'
                                  ) AS proof_policies(
                                      token_policy_id,
                                      policy_count
                                  )
                       )
                   ) THEN
                    RETURN FALSE;
                END IF;
                IF NOT EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements(
                               source_vector -> 'fhir_datasets'
                           ) AS source_datasets(source_dataset)
                     WHERE source_dataset ->> 'source_id'
                            = dataset_proof ->> 'source_id'
                       AND source_dataset ->> 'endpoint_id'
                            = dataset_proof ->> 'endpoint_id'
                       AND source_dataset ->> 'dataset_id'
                            = dataset_proof ->> 'dataset_id'
                       AND source_dataset ->> 'identifier_rule_id'
                            = dataset_proof ->> 'identifier_rule_id'
                       AND source_dataset ->> 'identifier_rule_sha256'
                            = dataset_proof
                               ->> 'identifier_rule_sha256'
                       AND source_dataset ->> 'source_summary_sha256'
                            = dataset_proof ->> 'source_summary_sha256'
                       AND source_dataset
                               ->> 'organization_resource_count'
                            = dataset_proof
                               ->> 'organization_resource_count'
                       AND source_dataset
                               ->> 'organization_resource_sha256'
                            = dataset_proof
                               ->> 'organization_resource_sha256'
                ) THEN
                    RETURN FALSE;
                END IF;
                total_organization_count := total_organization_count
                    + (
                        dataset_proof
                            ->> 'organization_resource_count'
                    )::numeric;
                total_matched_organization_count :=
                    total_matched_organization_count
                    + (
                        dataset_proof
                            ->> 'matched_organization_count'
                    )::numeric;
                total_evidence_count :=
                    total_evidence_count + state_count_sum;
            END LOOP;
            IF (
                SELECT COUNT(*)
                  FROM (
                        SELECT DISTINCT
                               value ->> 'source_id',
                               value ->> 'endpoint_id',
                               value ->> 'dataset_id'
                          FROM jsonb_array_elements(payload -> 'datasets')
                       ) AS distinct_datasets
            ) <> expected_dataset_count
               OR total_organization_count <> expected_organization_count
               OR total_matched_organization_count
                    <> expected_matched_organization_count
               OR total_evidence_count <> expected_evidence_count THEN
                RETURN FALSE;
            END IF;
            RETURN TRUE;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN FALSE;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {lookup_row_digest}(
            candidate_policy_id text,
            candidate_tin_hmac_sha256 bytea,
            candidate_npis bigint[],
            candidate_evidence_count bigint,
            candidate_source_bitmap bytea,
            candidate_npi_source_bitmap_matrix bytea,
            candidate_source_evidence_counts bigint[]
        )
        RETURNS bytea
        LANGUAGE sql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
            SELECT sha256(
                convert_to(
                    'healthporta.tin-npi.lookup-row.v3',
                    'UTF8'
                )
                || decode('00', 'hex')
                || int2send(
                    octet_length(
                        convert_to(candidate_policy_id, 'UTF8')
                    )::smallint
                )
                || convert_to(candidate_policy_id, 'UTF8')
                || candidate_tin_hmac_sha256
                || int4send(cardinality(candidate_npis))
                || COALESCE(
                       (
                           SELECT string_agg(
                                      int8send(npi),
                                      decode('', 'hex')
                                      ORDER BY ordinal_position
                                  )
                             FROM unnest(candidate_npis)
                                  WITH ORDINALITY
                                  AS npi_values(npi, ordinal_position)
                       ),
                       decode('', 'hex')
                   )
                || int8send(candidate_evidence_count)
                || int4send(octet_length(candidate_source_bitmap))
                || candidate_source_bitmap
                || int4send(
                       octet_length(candidate_npi_source_bitmap_matrix)
                   )
                || candidate_npi_source_bitmap_matrix
                || int4send(cardinality(candidate_source_evidence_counts))
                || COALESCE(
                       (
                           SELECT string_agg(
                                      int8send(source_evidence_count),
                                      decode('', 'hex')
                                      ORDER BY ordinal_position
                                  )
                             FROM unnest(candidate_source_evidence_counts)
                                  WITH ORDINALITY
                                  AS source_evidence_values(
                                      source_evidence_count,
                                      ordinal_position
                                  )
                       ),
                       decode('', 'hex')
                   )
            );
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {evidence_id_digest}(
            candidate_policy_id text,
            candidate_tin_hmac_sha256 bytea,
            candidate_npi bigint,
            candidate_relationship_class text,
            candidate_source_record_hmac_sha256 bytea,
            candidate_source_record_identity_sha256 bytea,
            candidate_source_record_payload_sha256 bytea,
            candidate_identifier_policy_sha256 bytea,
            candidate_identifier_rule_sha256 bytea
        )
        RETURNS bytea
        LANGUAGE sql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $function$
            SELECT sha256(
                convert_to(
                    'healthporta.tin-npi.fhir-evidence.v2',
                    'UTF8'
                )
                || decode('00', 'hex')
                || int2send(
                    octet_length(
                        convert_to(candidate_policy_id, 'UTF8')
                    )::smallint
                )
                || convert_to(candidate_policy_id, 'UTF8')
                || candidate_tin_hmac_sha256
                || int8send(candidate_npi)
                || int2send(
                    octet_length(
                        convert_to(
                            candidate_relationship_class,
                            'UTF8'
                        )
                    )::smallint
                )
                || convert_to(candidate_relationship_class, 'UTF8')
                || candidate_source_record_hmac_sha256
                || candidate_source_record_identity_sha256
                || candidate_source_record_payload_sha256
                || candidate_identifier_policy_sha256
                || candidate_identifier_rule_sha256
            );
        $function$;
        """
    )

    op.execute(
        f"""
        CREATE TABLE {token_policy} (
            token_policy_id varchar(55) NOT NULL,
            token_policy_descriptor_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL
                DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('tin_npi_connector_token_policy_pkey')}
                PRIMARY KEY (token_policy_id),
            CONSTRAINT {_q('tin_npi_connector_token_policy_id_check')}
                CHECK (
                    token_policy_id ~
                        '^ptg-tin-hmac-sha256-v1:[a-z0-9]'
                        '[a-z0-9._-]{{0,31}}$'
                    AND octet_length(token_policy_id) <= 55
                ),
            CONSTRAINT {_q('tin_npi_connector_token_policy_digest_check')}
                CHECK (
                    octet_length(token_policy_descriptor_sha256) = 32
                    AND token_policy_descriptor_sha256 =
                        {policy_descriptor}(token_policy_id)
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {identifier_policy} (
            identifier_policy_id varchar(128) NOT NULL,
            descriptor_canonical_json text NOT NULL,
            identifier_policy_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL
                DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('tin_npi_connector_identifier_policy_pkey')}
                PRIMARY KEY (identifier_policy_id),
            CONSTRAINT {_q('tin_npi_connector_identifier_policy_identity_key')}
                UNIQUE (
                    identifier_policy_id,
                    identifier_policy_sha256
                ),
            CONSTRAINT {_q('tin_npi_connector_identifier_policy_id_check')}
                CHECK (
                    identifier_policy_id ~
                        '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,127}}$'
                ),
            CONSTRAINT {_q('tin_npi_connector_identifier_policy_digest_check')}
                CHECK (
                    octet_length(identifier_policy_sha256) = 32
                    AND {valid_identifier_policy}(
                        descriptor_canonical_json,
                        identifier_policy_id
                    )
                    AND identifier_policy_sha256 = sha256(
                        convert_to(
                            'healthporta.tin-npi.fhir-identifier-policy.v2',
                            'UTF8'
                        )
                        || decode('00', 'hex')
                        || convert_to(
                            descriptor_canonical_json,
                            'UTF8'
                        )
                    )
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {generation} (
            generation_key bigint GENERATED ALWAYS AS IDENTITY,
            generation_id bytea NOT NULL,
            source_vector_id bytea NOT NULL,
            source_vector_canonical_json text NOT NULL,
            schema_version smallint NOT NULL,
            lookup_schema_version smallint NOT NULL,
            lookup_contract_id varchar(128) NOT NULL,
            generation_contract varchar(64) NOT NULL,
            raw_policy varchar(32) NOT NULL,
            projection_policy_id varchar(128) NOT NULL,
            relationship_class varchar(48) NOT NULL,
            site_resolution_contract_id varchar(128) NOT NULL,
            source_record_identity_contract_id varchar(128) NOT NULL,
            identifier_policy_id varchar(128) NOT NULL,
            identifier_policy_sha256 bytea NOT NULL,
            evidence_as_of varchar(64) NOT NULL,
            source_ordinal_contract varchar(64) NOT NULL,
            source_ordinal_map_canonical_json text NOT NULL,
            source_ordinal_map_digest bytea NOT NULL,
            scan_contract_id varchar(128) NOT NULL,
            scan_proof_canonical_json text NOT NULL,
            scan_proof_digest bytea NOT NULL,
            source_count integer NOT NULL,
            source_dataset_count integer NOT NULL,
            source_relation_count integer NOT NULL,
            token_policy_count integer NOT NULL,
            lookup_digest bytea NOT NULL,
            organization_count bigint NOT NULL,
            matched_organization_count bigint NOT NULL,
            evidence_count bigint NOT NULL,
            forward_row_count bigint NOT NULL,
            reverse_row_count bigint NOT NULL,
            npi_edge_count bigint NOT NULL,
            build_token_sha256 bytea NOT NULL,
            build_lease_expires_at timestamptz NOT NULL,
            state varchar(16) NOT NULL,
            created_at timestamptz NOT NULL
                DEFAULT transaction_timestamp(),
            completed_at timestamptz,
            failed_at timestamptz,
            retired_at timestamptz,
            gc_after timestamptz,
            CONSTRAINT {_q('tin_npi_connector_generation_pkey')}
                PRIMARY KEY (generation_key),
            CONSTRAINT {_q('tin_npi_connector_generation_id_key')}
                UNIQUE (generation_id),
            CONSTRAINT {_q('tin_npi_connector_generation_source_vector_key')}
                UNIQUE (source_vector_id),
            CONSTRAINT {_q('tin_npi_connector_generation_identifier_policy_fkey')}
                FOREIGN KEY (
                    identifier_policy_id,
                    identifier_policy_sha256
                )
                REFERENCES {identifier_policy} (
                    identifier_policy_id,
                    identifier_policy_sha256
                )
                ON DELETE RESTRICT,
            CONSTRAINT {_q('tin_npi_connector_generation_digest_check')}
                CHECK (
                    octet_length(generation_id) = 32
                    AND octet_length(source_vector_id) = 32
                    AND octet_length(identifier_policy_sha256) = 32
                    AND octet_length(source_ordinal_map_digest) = 32
                    AND octet_length(scan_proof_digest) = 32
                    AND octet_length(lookup_digest) = 32
                    AND octet_length(build_token_sha256) = 32
                    AND source_vector_id = sha256(
                        convert_to(
                            'healthporta.tin-npi.source-vector.v1',
                            'UTF8'
                        )
                        || decode('00', 'hex')
                        || convert_to(
                            source_vector_canonical_json,
                            'UTF8'
                        )
                    )
                    AND source_ordinal_map_digest = sha256(
                        convert_to(
                            'healthporta.tin-npi.source-ordinal-map.v1',
                            'UTF8'
                        )
                        || decode('00', 'hex')
                        || convert_to(
                            source_ordinal_map_canonical_json,
                            'UTF8'
                        )
                    )
                    AND scan_proof_digest = sha256(
                        convert_to(
                            'healthporta.tin-npi.fhir-organization-scan-proof.v2',
                            'UTF8'
                        )
                        || decode('00', 'hex')
                        || convert_to(
                            scan_proof_canonical_json,
                            'UTF8'
                        )
                    )
                    AND generation_id = sha256(
                        convert_to(
                            'healthporta.tin-npi.generation.v3',
                            'UTF8'
                        )
                        || decode('00', 'hex')
                        || source_vector_id
                        || scan_proof_digest
                        || lookup_digest
                    )
                ),
            CONSTRAINT {_q('tin_npi_connector_generation_contract_check')}
                CHECK (
                    schema_version = 3
                    AND lookup_schema_version = 2
                    AND lookup_contract_id =
                        'healthporta.tin-npi.compact-lookup.v2'
                    AND generation_contract =
                        'tin_npi_connector_generation_v3'
                    AND raw_policy = 'token_only_v1'
                    AND projection_policy_id =
                        'healthporta.tin-npi.compact-same-organization-lookup.v3'
                    AND relationship_class =
                        'same_organization_identifier'
                    AND site_resolution_contract_id =
                        'healthporta.tin-npi.site-by-current-entity-address-unified.v1'
                    AND source_record_identity_contract_id =
                        'healthporta.tin-npi.fhir-source-record-hmac.v1'
                    AND identifier_policy_id ~
                        '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,127}}$'
                    AND source_ordinal_contract =
                        'source_id_sorted_utf8_lsb0_bitmap_v1'
                    AND scan_contract_id =
                        'healthporta.tin-npi.fhir-organization-scan.v2'
                ),
            CONSTRAINT {_q('tin_npi_connector_generation_source_check')}
                CHECK (
                    source_count > 0
                    AND source_dataset_count >= source_count
                    AND source_relation_count = 1
                    AND token_policy_count > 0
                    AND {valid_source_ordinals}(
                        source_ordinal_map_canonical_json,
                        source_count
                    )
                    AND {valid_source_vector}(
                        source_vector_canonical_json,
                        schema_version,
                        lookup_schema_version,
                        lookup_contract_id,
                        projection_policy_id,
                        site_resolution_contract_id,
                        source_record_identity_contract_id,
                        identifier_policy_id,
                        identifier_policy_sha256,
                        evidence_as_of,
                        source_dataset_count,
                        source_relation_count,
                        token_policy_count
                    )
                    AND {valid_scan_proof}(
                        scan_proof_canonical_json,
                        source_vector_canonical_json,
                        source_dataset_count,
                        token_policy_count,
                        organization_count,
                        matched_organization_count,
                        evidence_count
                    )
                ),
            CONSTRAINT {_q('tin_npi_connector_generation_count_check')}
                CHECK (
                    organization_count >= 0
                    AND matched_organization_count >= 0
                    AND matched_organization_count <= organization_count
                    AND evidence_count >= 0
                    AND forward_row_count >= 0
                    AND reverse_row_count >= 0
                    AND npi_edge_count >= 0
                    AND evidence_count >= npi_edge_count
                    AND evidence_count::numeric >=
                        matched_organization_count::numeric
                            * token_policy_count
                    AND npi_edge_count >= forward_row_count
                    AND npi_edge_count >= reverse_row_count
                    AND (
                        (
                            forward_row_count = 0
                            AND reverse_row_count = 0
                            AND npi_edge_count = 0
                            AND evidence_count = 0
                        )
                        OR (
                            forward_row_count > 0
                            AND reverse_row_count > 0
                            AND npi_edge_count > 0
                            AND evidence_count > 0
                        )
                    )
                ),
            CONSTRAINT {_q('tin_npi_connector_generation_state_check')}
                CHECK (
                    state IN ('building', 'complete', 'failed', 'retired')
                    AND (
                        (
                            state = 'building'
                            AND completed_at IS NULL
                            AND failed_at IS NULL
                            AND retired_at IS NULL
                            AND gc_after IS NULL
                        )
                        OR (
                            state = 'complete'
                            AND completed_at IS NOT NULL
                            AND failed_at IS NULL
                            AND retired_at IS NULL
                            AND gc_after IS NULL
                        )
                        OR (
                            state = 'failed'
                            AND completed_at IS NULL
                            AND failed_at IS NOT NULL
                            AND retired_at IS NULL
                            AND gc_after IS NULL
                        )
                        OR (
                            state = 'retired'
                            AND completed_at IS NOT NULL
                            AND failed_at IS NULL
                            AND retired_at IS NOT NULL
                            AND gc_after IS NOT NULL
                            AND gc_after >= retired_at
                        )
                    )
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('tin_npi_connector_generation_gc_idx')}
        ON {generation} (gc_after, generation_key)
        WHERE state = 'retired';
        """
    )
    op.execute(
        f"""
        CREATE TABLE {generation_policy} (
            generation_key bigint NOT NULL,
            token_policy_id varchar(55) NOT NULL,
            CONSTRAINT {_q('tin_npi_connector_generation_policy_pkey')}
                PRIMARY KEY (generation_key, token_policy_id),
            CONSTRAINT {_q('tin_npi_connector_generation_policy_generation_fkey')}
                FOREIGN KEY (generation_key)
                REFERENCES {generation} (generation_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('tin_npi_connector_generation_policy_registry_fkey')}
                FOREIGN KEY (token_policy_id)
                REFERENCES {token_policy} (token_policy_id)
                ON DELETE RESTRICT,
            CONSTRAINT {_q('tin_npi_connector_generation_policy_id_check')}
                CHECK (
                    token_policy_id ~
                        '^ptg-tin-hmac-sha256-v1:[a-z0-9]'
                        '[a-z0-9._-]{{0,31}}$'
                    AND octet_length(token_policy_id) <= 55
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {lookup} (
            generation_key bigint NOT NULL,
            token_policy_id varchar(55) NOT NULL,
            tin_id_128 bytea NOT NULL,
            tin_hmac_sha256 bytea NOT NULL,
            npis bigint[] NOT NULL,
            evidence_count bigint NOT NULL,
            source_bitmap bytea NOT NULL,
            npi_source_bitmap_matrix bytea NOT NULL,
            source_evidence_counts bigint[] NOT NULL,
            CONSTRAINT {_q('tin_npi_connector_lookup_pkey')}
                PRIMARY KEY (
                    generation_key,
                    token_policy_id,
                    tin_id_128,
                    tin_hmac_sha256
                ),
            CONSTRAINT {_q('tin_npi_connector_lookup_policy_fkey')}
                FOREIGN KEY (generation_key, token_policy_id)
                REFERENCES {generation_policy} (
                    generation_key,
                    token_policy_id
                )
                ON DELETE CASCADE,
            CONSTRAINT {_q('tin_npi_connector_lookup_policy_check')}
                CHECK (
                    token_policy_id ~
                        '^ptg-tin-hmac-sha256-v1:[a-z0-9]'
                        '[a-z0-9._-]{{0,31}}$'
                    AND octet_length(token_policy_id) <= 55
                ),
            CONSTRAINT {_q('tin_npi_connector_lookup_token_check')}
                CHECK (
                    generation_key > 0
                    AND octet_length(tin_id_128) = 16
                    AND octet_length(tin_hmac_sha256) = 32
                    AND tin_id_128 =
                        substring(tin_hmac_sha256 FROM 1 FOR 16)
                ),
            CONSTRAINT {_q('tin_npi_connector_lookup_payload_check')}
                CHECK (
                    {valid_npis}(npis)
                    AND evidence_count >= cardinality(npis)
                    AND {valid_source_evidence}(
                        source_bitmap,
                        npi_source_bitmap_matrix,
                        source_evidence_counts,
                        evidence_count,
                        npis
                    )
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {evidence} (
            generation_key bigint NOT NULL,
            evidence_id bytea NOT NULL,
            token_policy_id varchar(55) NOT NULL,
            tin_id_128 bytea NOT NULL,
            tin_hmac_sha256 bytea NOT NULL,
            npi bigint NOT NULL,
            source_ordinal integer NOT NULL,
            relationship_class varchar(48) NOT NULL,
            source_record_hmac_sha256 bytea NOT NULL,
            source_record_identity_sha256 bytea NOT NULL,
            source_record_payload_sha256 bytea NOT NULL,
            identifier_policy_sha256 bytea NOT NULL,
            identifier_rule_id varchar(128) NOT NULL,
            identifier_rule_sha256 bytea NOT NULL,
            CONSTRAINT {_q('tin_npi_connector_evidence_pkey')}
                PRIMARY KEY (generation_key, evidence_id),
            CONSTRAINT {_q('tin_npi_connector_evidence_lookup_fkey')}
                FOREIGN KEY (
                    generation_key,
                    token_policy_id,
                    tin_id_128,
                    tin_hmac_sha256
                )
                REFERENCES {lookup} (
                    generation_key,
                    token_policy_id,
                    tin_id_128,
                    tin_hmac_sha256
                )
                ON DELETE RESTRICT,
            CONSTRAINT {_q('tin_npi_connector_evidence_identity_check')}
                CHECK (
                    generation_key > 0
                    AND octet_length(evidence_id) = 32
                    AND octet_length(tin_id_128) = 16
                    AND octet_length(tin_hmac_sha256) = 32
                    AND tin_id_128 =
                        substring(tin_hmac_sha256 FROM 1 FOR 16)
                    AND {valid_npi}(npi)
                    AND source_ordinal >= 0
                    AND relationship_class =
                        'same_organization_identifier'
                    AND octet_length(source_record_hmac_sha256) = 32
                    AND octet_length(source_record_identity_sha256) = 32
                    AND octet_length(source_record_payload_sha256) = 32
                    AND octet_length(identifier_policy_sha256) = 32
                    AND identifier_rule_id ~
                        '^[A-Za-z0-9][A-Za-z0-9._:/-]{{0,127}}$'
                    AND octet_length(identifier_rule_sha256) = 32
                    AND evidence_id = {evidence_id_digest}(
                        token_policy_id,
                        tin_hmac_sha256,
                        npi,
                        relationship_class,
                        source_record_hmac_sha256,
                        source_record_identity_sha256,
                        source_record_payload_sha256,
                        identifier_policy_sha256,
                        identifier_rule_sha256
                    )
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('tin_npi_connector_evidence_lookup_idx')}
        ON {evidence} (
            generation_key,
            token_policy_id,
            tin_hmac_sha256,
            npi,
            source_ordinal
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('tin_npi_connector_evidence_source_digest_idx')}
        ON {evidence} (
            generation_key,
            source_ordinal,
            evidence_id
        );
        """
    )
    op.execute(
        f"""
        CREATE TABLE {current} (
            pointer_key smallint NOT NULL,
            pointer_version bigint NOT NULL,
            generation_key bigint,
            published_at timestamptz,
            updated_at timestamptz NOT NULL
                DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('tin_npi_connector_current_pkey')}
                PRIMARY KEY (pointer_key),
            CONSTRAINT {_q('tin_npi_connector_current_generation_fkey')}
                FOREIGN KEY (generation_key)
                REFERENCES {generation} (generation_key)
                ON DELETE RESTRICT,
            CONSTRAINT {_q('tin_npi_connector_current_singleton_check')}
                CHECK (
                    pointer_key = 1
                    AND pointer_version >= 0
                    AND (
                        (
                            generation_key IS NULL
                            AND pointer_version = 0
                            AND published_at IS NULL
                        )
                        OR (
                            generation_key > 0
                            AND pointer_version > 0
                            AND published_at IS NOT NULL
                        )
                    )
                )
        );
        """
    )
    op.execute(
        f"""
        INSERT INTO {current} (
            pointer_key,
            pointer_version,
            generation_key,
            published_at
        ) VALUES (1, 0, NULL, NULL);
        """
    )

    op.execute(
        f"""
        CREATE FUNCTION {lookup_set_digest}(target_generation_key bigint)
        RETURNS bytea
        LANGUAGE sql
        STABLE
        PARALLEL RESTRICTED
        STRICT
        AS $function$
            WITH row_hashes AS (
                SELECT token_policy_id,
                       tin_hmac_sha256,
                       {lookup_row_digest}(
                           token_policy_id,
                           tin_hmac_sha256,
                           npis,
                           evidence_count,
                           source_bitmap,
                           npi_source_bitmap_matrix,
                           source_evidence_counts
                       ) AS row_sha256
                  FROM {lookup}
                 WHERE generation_key = target_generation_key
            ),
            bucketed_rows AS (
                SELECT get_byte(row_sha256, 0) AS bucket,
                       token_policy_id,
                       tin_hmac_sha256,
                       row_sha256
                  FROM row_hashes
            ),
            bucket_hashes AS (
                SELECT bucket_number,
                       sha256(
                           convert_to(
                               'healthporta.tin-npi.lookup-bucket.v1',
                               'UTF8'
                           )
                           || decode('00', 'hex')
                           || int2send(bucket_number::smallint)
                           || COALESCE(
                                  string_agg(
                                      bucketed_rows.row_sha256,
                                      decode('', 'hex')
                                      ORDER BY
                                          convert_to(
                                              bucketed_rows.token_policy_id,
                                              'UTF8'
                                          ),
                                          bucketed_rows.tin_hmac_sha256
                                  ) FILTER (
                                      WHERE bucketed_rows.row_sha256
                                          IS NOT NULL
                                  ),
                                  decode('', 'hex')
                              )
                       ) AS bucket_sha256
                  FROM generate_series(0, 255) AS buckets(bucket_number)
                  LEFT JOIN bucketed_rows
                    ON bucketed_rows.bucket = bucket_number
                 GROUP BY bucket_number
            )
            SELECT sha256(
                convert_to(
                    'healthporta.tin-npi.lookup-set.v4',
                    'UTF8'
                )
                || decode('00', 'hex')
                || string_agg(
                       bucket_sha256,
                       decode('', 'hex')
                       ORDER BY bucket_number
                   )
            )
              FROM bucket_hashes;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {evidence_set_digest}(
            target_generation_key bigint,
            target_source_ordinal integer
        )
        RETURNS bytea
        LANGUAGE sql
        STABLE
        PARALLEL RESTRICTED
        STRICT
        AS $function$
            SELECT sha256(
                convert_to(
                    'healthporta.tin-npi.fhir-evidence-set.v1',
                    'UTF8'
                )
                || decode('00', 'hex')
                || COALESCE(
                       string_agg(
                           evidence_id,
                           decode('', 'hex')
                           ORDER BY evidence_id
                       ),
                       decode('', 'hex')
                   )
            )
              FROM {evidence}
             WHERE generation_key = target_generation_key
               AND source_ordinal = target_source_ordinal;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {build_token_matches}(expected_sha256 bytea)
        RETURNS boolean
        LANGUAGE sql
        STABLE
        PARALLEL SAFE
        STRICT
        AS $function$
            SELECT COALESCE(
                sha256(
                    convert_to(
                        current_setting(
                            'healthporta.tin_npi_build_token',
                            TRUE
                        ),
                        'UTF8'
                    )
                ) = expected_sha256,
                FALSE
            );
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {token_policy_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION
                    'tin_npi_connector_policy_registry_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {generation_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            observed_policy_count bigint;
            observed_forward_row_count bigint;
            observed_evidence_count bigint;
            observed_audit_evidence_count bigint;
            observed_npi_edge_count bigint;
            observed_reverse_row_count bigint;
            invalid_source_bitmap_count bigint;
            invalid_policy_count bigint;
            unused_policy_count bigint;
            identifier_rule_difference_count bigint;
            source_membership_difference_count bigint;
            source_policy_evidence_difference_count bigint;
            invalid_evidence_count bigint;
            invalid_evidence_record_count bigint;
            evidence_projection_difference_count bigint;
            evidence_scan_digest_difference_count bigint;
            source_matched_record_difference_count bigint;
            observed_matched_record_count bigint;
            current_reference_count bigint;
            calculated_lookup_digest bytea;
            abandon_generation_key text;
            gc_generation_key text;
            retire_generation_key text;
            generation_owner name;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.state <> 'building'
                   OR NEW.build_lease_expires_at
                        <= clock_timestamp()
                   OR NEW.created_at IS DISTINCT FROM
                        transaction_timestamp()
                   OR NEW.evidence_as_of::timestamptz >
                        NEW.created_at THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_generation_must_start_building'
                        USING ERRCODE = '23514';
                END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE' THEN
                gc_generation_key := current_setting(
                    'healthporta.tin_npi_gc_generation_key',
                    TRUE
                );
                SELECT pg_catalog.pg_get_userbyid(relation.relowner)
                  INTO generation_owner
                  FROM pg_catalog.pg_class AS relation
                 WHERE relation.oid = TG_RELID;
                SELECT COUNT(*)
                  INTO current_reference_count
                  FROM {current}
                 WHERE generation_key = OLD.generation_key;
                IF OLD.state NOT IN ('failed', 'retired')
                   OR current_reference_count <> 0
                   OR gc_generation_key IS DISTINCT FROM
                        OLD.generation_key::text
                   OR current_user <> generation_owner
                   OR (
                       OLD.state = 'retired'
                       AND (
                           OLD.gc_after IS NULL
                           OR OLD.gc_after > clock_timestamp()
                       )
                   )
                   OR (
                       OLD.state = 'failed'
                       AND OLD.build_lease_expires_at > clock_timestamp()
                   ) THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_generation_delete_forbidden'
                        USING ERRCODE = '55000';
                END IF;
                RETURN OLD;
            END IF;
            IF (
                to_jsonb(NEW)
                    - ARRAY[
                        'state',
                        'completed_at',
                        'failed_at',
                        'retired_at',
                        'gc_after',
                        'build_lease_expires_at'
                    ]
                <>
                to_jsonb(OLD)
                    - ARRAY[
                        'state',
                        'completed_at',
                        'failed_at',
                        'retired_at',
                        'gc_after',
                        'build_lease_expires_at'
                    ]
            ) THEN
                RAISE EXCEPTION
                    'tin_npi_connector_generation_payload_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF OLD.state = 'building' AND NEW.state = 'building' THEN
                IF NOT {build_token_matches}(OLD.build_token_sha256)
                   OR NEW.build_lease_expires_at
                        <= GREATEST(
                            OLD.build_lease_expires_at,
                            clock_timestamp()
                        ) THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_build_lease_extension_forbidden'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.state = 'building' AND NEW.state = 'failed' THEN
                abandon_generation_key := current_setting(
                    'healthporta.tin_npi_abandon_generation_key',
                    TRUE
                );
                SELECT pg_catalog.pg_get_userbyid(relation.relowner)
                  INTO generation_owner
                  FROM pg_catalog.pg_class AS relation
                 WHERE relation.oid = TG_RELID;
                IF NOT {build_token_matches}(OLD.build_token_sha256)
                   AND NOT (
                       OLD.build_lease_expires_at <= clock_timestamp()
                       AND abandon_generation_key IS NOT DISTINCT FROM
                            OLD.generation_key::text
                       AND current_user = generation_owner
                   ) THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_build_token_invalid'
                        USING ERRCODE = '55000';
                END IF;
                NEW.failed_at := transaction_timestamp();
                RETURN NEW;
            END IF;
            IF OLD.state = 'complete' AND NEW.state = 'retired' THEN
                retire_generation_key := current_setting(
                    'healthporta.tin_npi_retire_generation_key',
                    TRUE
                );
                SELECT pg_catalog.pg_get_userbyid(relation.relowner)
                  INTO generation_owner
                  FROM pg_catalog.pg_class AS relation
                 WHERE relation.oid = TG_RELID;
                SELECT COUNT(*)
                  INTO current_reference_count
                  FROM {current}
                 WHERE generation_key = OLD.generation_key;
                IF current_reference_count <> 0
                   OR retire_generation_key IS DISTINCT FROM
                        OLD.generation_key::text
                   OR current_user <> generation_owner
                   OR NEW.gc_after IS NULL
                   OR NEW.gc_after <
                        clock_timestamp() + interval '24 hours' THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_generation_retire_forbidden'
                        USING ERRCODE = '55000';
                END IF;
                NEW.retired_at := transaction_timestamp();
                RETURN NEW;
            END IF;
            IF OLD.state <> 'building' OR NEW.state <> 'complete' THEN
                RAISE EXCEPTION
                    'tin_npi_connector_generation_transition_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            IF NOT {build_token_matches}(OLD.build_token_sha256)
               OR OLD.build_lease_expires_at <= clock_timestamp() THEN
                RAISE EXCEPTION
                    'tin_npi_connector_build_token_or_lease_invalid'
                    USING ERRCODE = '55000';
            END IF;

            SELECT COUNT(*)
              INTO observed_policy_count
              FROM {generation_policy}
             WHERE generation_key = NEW.generation_key;
            SELECT COUNT(*)
              INTO invalid_policy_count
              FROM {generation_policy} AS generation_membership
              JOIN {token_policy} AS registry
                USING (token_policy_id)
             WHERE generation_membership.generation_key = NEW.generation_key
               AND NOT EXISTS (
                   SELECT 1
                     FROM jsonb_array_elements(
                              NEW.source_vector_canonical_json::jsonb
                              -> 'token_policies'
                          ) AS policies(policy)
                    WHERE policy ->> 'token_policy_id'
                            = generation_membership.token_policy_id
                      AND policy
                            ->> 'token_policy_descriptor_sha256'
                            = encode(
                                registry.token_policy_descriptor_sha256,
                                'hex'
                      )
               );
            SELECT COUNT(*)
              INTO unused_policy_count
              FROM {generation_policy} AS generation_membership
             WHERE generation_membership.generation_key =
                       NEW.generation_key
               AND NOT EXISTS (
                   SELECT 1
                     FROM {lookup} AS policy_lookup
                    WHERE policy_lookup.generation_key =
                              generation_membership.generation_key
                      AND policy_lookup.token_policy_id =
                              generation_membership.token_policy_id
               );
            SELECT COUNT(*)
              INTO source_membership_difference_count
              FROM (
                    (
                        SELECT source_entry ->> 'source_id' AS source_id
                          FROM jsonb_array_elements(
                                   NEW.source_ordinal_map_canonical_json::jsonb
                               ) AS ordinals(source_entry)
                        EXCEPT
                        SELECT DISTINCT
                               dataset ->> 'source_id'
                          FROM jsonb_array_elements(
                                   NEW.source_vector_canonical_json::jsonb
                                   -> 'fhir_datasets'
                               ) AS datasets(dataset)
                    )
                    UNION ALL
                    (
                        SELECT DISTINCT
                               dataset ->> 'source_id'
                          FROM jsonb_array_elements(
                                   NEW.source_vector_canonical_json::jsonb
                                   -> 'fhir_datasets'
                               ) AS datasets(dataset)
                        EXCEPT
                        SELECT source_entry ->> 'source_id'
                          FROM jsonb_array_elements(
                                   NEW.source_ordinal_map_canonical_json::jsonb
                               ) AS ordinals(source_entry)
                    )
                   ) AS membership_difference;
            WITH registered_rules AS MATERIALIZED (
                SELECT rule ->> 'source_id' AS source_id,
                       rule ->> 'endpoint_id' AS endpoint_id,
                       rule ->> 'rule_id' AS identifier_rule_id,
                       rule ->> 'identifier_rule_sha256'
                           AS identifier_rule_sha256
                  FROM {identifier_policy} AS registry
                  CROSS JOIN LATERAL jsonb_array_elements(
                       registry.descriptor_canonical_json::jsonb -> 'rules'
                  ) AS rules(rule)
                 WHERE registry.identifier_policy_id =
                           NEW.identifier_policy_id
                   AND registry.identifier_policy_sha256 =
                           NEW.identifier_policy_sha256
            ),
            source_rules AS MATERIALIZED (
                SELECT dataset ->> 'source_id' AS source_id,
                       dataset ->> 'endpoint_id' AS endpoint_id,
                       dataset ->> 'identifier_rule_id'
                           AS identifier_rule_id,
                       dataset ->> 'identifier_rule_sha256'
                           AS identifier_rule_sha256
                  FROM jsonb_array_elements(
                           NEW.source_vector_canonical_json::jsonb
                               -> 'fhir_datasets'
                       ) AS datasets(dataset)
            )
            SELECT COUNT(*)
              INTO identifier_rule_difference_count
              FROM (
                    (
                        SELECT * FROM registered_rules
                        EXCEPT
                        SELECT * FROM source_rules
                    )
                    UNION ALL
                    (
                        SELECT * FROM source_rules
                        EXCEPT
                        SELECT * FROM registered_rules
                    )
                   ) AS rule_difference;
            SELECT COUNT(*)
              INTO invalid_evidence_count
              FROM {evidence} AS audit_evidence
              JOIN {lookup} AS policy_lookup
                ON policy_lookup.generation_key =
                       audit_evidence.generation_key
               AND policy_lookup.token_policy_id =
                       audit_evidence.token_policy_id
               AND policy_lookup.tin_id_128 =
                       audit_evidence.tin_id_128
               AND policy_lookup.tin_hmac_sha256 =
                       audit_evidence.tin_hmac_sha256
              LEFT JOIN LATERAL (
                   SELECT source_entry ->> 'source_id' AS source_id
                     FROM jsonb_array_elements(
                              NEW.source_ordinal_map_canonical_json::jsonb
                          ) AS source_entries(source_entry)
                    WHERE (
                              source_entry ->> 'ordinal'
                          )::integer = audit_evidence.source_ordinal
              ) AS source_scope ON TRUE
              LEFT JOIN LATERAL (
                   SELECT dataset ->> 'identifier_rule_id'
                              AS identifier_rule_id,
                          dataset ->> 'identifier_rule_sha256'
                              AS identifier_rule_sha256
                     FROM jsonb_array_elements(
                              NEW.source_vector_canonical_json::jsonb
                                  -> 'fhir_datasets'
                          ) AS datasets(dataset)
                    WHERE dataset ->> 'source_id' =
                              source_scope.source_id
              ) AS dataset_scope ON TRUE
             WHERE audit_evidence.generation_key = NEW.generation_key
               AND (
                   audit_evidence.identifier_policy_sha256 <>
                       NEW.identifier_policy_sha256
                   OR audit_evidence.relationship_class <>
                       NEW.relationship_class
                   OR audit_evidence.source_ordinal >= NEW.source_count
                   OR source_scope.source_id IS NULL
                   OR dataset_scope.identifier_rule_id IS NULL
                   OR audit_evidence.identifier_rule_id <>
                       dataset_scope.identifier_rule_id
                   OR encode(
                          audit_evidence.identifier_rule_sha256,
                          'hex'
                      ) <> dataset_scope.identifier_rule_sha256
                   OR NOT (
                       audit_evidence.npi = ANY(policy_lookup.npis)
                   )
               );
            SELECT COUNT(*)
              INTO evidence_scan_digest_difference_count
              FROM jsonb_array_elements(
                       NEW.scan_proof_canonical_json::jsonb -> 'datasets'
                   ) AS proofs(dataset_proof)
              JOIN LATERAL (
                   SELECT (
                              source_entry ->> 'ordinal'
                          )::integer AS source_ordinal
                     FROM jsonb_array_elements(
                              NEW.source_ordinal_map_canonical_json::jsonb
                          ) AS source_entries(source_entry)
                    WHERE source_entry ->> 'source_id' =
                              dataset_proof ->> 'source_id'
              ) AS source_scope ON TRUE
             WHERE decode(
                       dataset_proof ->> 'matched_evidence_sha256',
                       'hex'
                   ) <> {evidence_set_digest}(
                       NEW.generation_key,
                       source_scope.source_ordinal
                   );
            WITH record_policy AS MATERIALIZED (
                SELECT source_ordinal,
                       source_record_identity_sha256,
                       token_policy_id,
                       COUNT(DISTINCT tin_hmac_sha256)
                           AS token_count,
                       COUNT(DISTINCT source_record_hmac_sha256)
                           AS source_record_hmac_count,
                       COUNT(DISTINCT source_record_payload_sha256)
                           AS payload_count,
                       MIN(source_record_payload_sha256)
                           AS payload_sha256,
                       COUNT(DISTINCT identifier_policy_sha256)
                           AS identifier_policy_count,
                       COUNT(DISTINCT identifier_rule_id)
                           AS identifier_rule_id_count,
                       COUNT(DISTINCT identifier_rule_sha256)
                           AS identifier_rule_sha256_count,
                       ARRAY_AGG(DISTINCT npi ORDER BY npi)
                           AS npis
                  FROM {evidence}
                 WHERE generation_key = NEW.generation_key
                 GROUP BY source_ordinal,
                          source_record_identity_sha256,
                          token_policy_id
            ),
            record_parity AS MATERIALIZED (
                SELECT source_ordinal,
                       source_record_identity_sha256,
                       COUNT(*) AS policy_count,
                       COUNT(DISTINCT token_policy_id)
                           AS distinct_policy_count,
                       COUNT(DISTINCT npis) AS distinct_npi_set_count,
                       COUNT(DISTINCT payload_sha256)
                           AS distinct_payload_count,
                       COUNT(*) FILTER (
                           WHERE token_count <> 1
                              OR source_record_hmac_count <> 1
                              OR payload_count <> 1
                              OR identifier_policy_count <> 1
                              OR identifier_rule_id_count <> 1
                              OR identifier_rule_sha256_count <> 1
                       ) AS invalid_record_policy_count
                  FROM record_policy
                 GROUP BY source_ordinal,
                          source_record_identity_sha256
            )
            SELECT COUNT(*)
              INTO invalid_evidence_record_count
              FROM record_parity
             WHERE policy_count <> NEW.token_policy_count
                OR distinct_policy_count <> NEW.token_policy_count
                OR distinct_npi_set_count <> 1
                OR distinct_payload_count <> 1
                OR invalid_record_policy_count <> 0;
            SELECT COUNT(
                       DISTINCT (
                           source_ordinal,
                           source_record_identity_sha256
                       )
                   )
              INTO observed_matched_record_count
              FROM {evidence}
             WHERE generation_key = NEW.generation_key;
            SELECT COUNT(*)
              INTO source_matched_record_difference_count
              FROM jsonb_array_elements(
                       NEW.scan_proof_canonical_json::jsonb -> 'datasets'
                   ) AS proofs(dataset_proof)
              JOIN LATERAL (
                   SELECT (
                              source_entry ->> 'ordinal'
                          )::integer AS source_ordinal
                     FROM jsonb_array_elements(
                              NEW.source_ordinal_map_canonical_json::jsonb
                          ) AS source_entries(source_entry)
                    WHERE source_entry ->> 'source_id' =
                              dataset_proof ->> 'source_id'
              ) AS source_scope ON TRUE
             WHERE (
                       dataset_proof
                           ->> 'matched_organization_count'
                   )::bigint <> (
                       SELECT COUNT(
                                  DISTINCT
                                      audit_evidence
                                          .source_record_identity_sha256
                              )
                         FROM {evidence} AS audit_evidence
                        WHERE audit_evidence.generation_key =
                                  NEW.generation_key
                          AND audit_evidence.source_ordinal =
                                  source_scope.source_ordinal
                   );
            WITH evidence_key AS MATERIALIZED (
                SELECT token_policy_id,
                       tin_id_128,
                       tin_hmac_sha256,
                       COUNT(*) AS evidence_count,
                       ARRAY_AGG(DISTINCT npi ORDER BY npi) AS npis
                  FROM {evidence}
                 WHERE generation_key = NEW.generation_key
                 GROUP BY token_policy_id,
                          tin_id_128,
                          tin_hmac_sha256
            ),
            evidence_source AS MATERIALIZED (
                SELECT token_policy_id,
                       tin_id_128,
                       tin_hmac_sha256,
                       source_ordinal,
                       COUNT(*) AS evidence_count
                  FROM {evidence}
                 WHERE generation_key = NEW.generation_key
                 GROUP BY token_policy_id,
                          tin_id_128,
                          tin_hmac_sha256,
                          source_ordinal
            ),
            evidence_npi_source AS MATERIALIZED (
                SELECT DISTINCT
                       token_policy_id,
                       tin_id_128,
                       tin_hmac_sha256,
                       npi,
                       source_ordinal
                  FROM {evidence}
                 WHERE generation_key = NEW.generation_key
            ),
            invalid_key AS (
                SELECT policy_lookup.token_policy_id,
                       policy_lookup.tin_id_128,
                       policy_lookup.tin_hmac_sha256
                  FROM {lookup} AS policy_lookup
                  LEFT JOIN evidence_key AS aggregate_evidence
                    ON aggregate_evidence.token_policy_id =
                           policy_lookup.token_policy_id
                   AND aggregate_evidence.tin_id_128 =
                           policy_lookup.tin_id_128
                   AND aggregate_evidence.tin_hmac_sha256 =
                           policy_lookup.tin_hmac_sha256
                 WHERE policy_lookup.generation_key = NEW.generation_key
                   AND (
                       aggregate_evidence.token_policy_id IS NULL
                       OR aggregate_evidence.evidence_count <>
                           policy_lookup.evidence_count
                       OR aggregate_evidence.npis IS DISTINCT FROM
                           policy_lookup.npis
                   )
            ),
            invalid_source AS (
                SELECT policy_lookup.token_policy_id,
                       policy_lookup.tin_id_128,
                       policy_lookup.tin_hmac_sha256
                  FROM {lookup} AS policy_lookup
                 CROSS JOIN generate_series(
                       0,
                       NEW.source_count - 1
                 ) AS source_ordinals(source_ordinal)
                  LEFT JOIN evidence_source AS aggregate_source
                    ON aggregate_source.token_policy_id =
                           policy_lookup.token_policy_id
                   AND aggregate_source.tin_id_128 =
                           policy_lookup.tin_id_128
                   AND aggregate_source.tin_hmac_sha256 =
                           policy_lookup.tin_hmac_sha256
                   AND aggregate_source.source_ordinal =
                           source_ordinals.source_ordinal
                 WHERE policy_lookup.generation_key = NEW.generation_key
                   AND (
                       policy_lookup.source_evidence_counts[
                           source_ordinals.source_ordinal + 1
                       ] <> COALESCE(
                           aggregate_source.evidence_count,
                           0
                       )
                       OR (
                              get_byte(
                                  policy_lookup.source_bitmap,
                                  source_ordinals.source_ordinal / 8
                              )
                              & (
                                  1 << (
                                      source_ordinals.source_ordinal % 8
                                  )
                              )
                          ) <> 0
                          IS DISTINCT FROM (
                              aggregate_source.token_policy_id IS NOT NULL
                          )
                   )
            ),
            invalid_npi_source AS (
                SELECT policy_lookup.token_policy_id,
                       policy_lookup.tin_id_128,
                       policy_lookup.tin_hmac_sha256
                  FROM {lookup} AS policy_lookup
                 CROSS JOIN LATERAL unnest(policy_lookup.npis)
                      WITH ORDINALITY
                      AS npi_values(npi, npi_ordinal)
                 CROSS JOIN generate_series(
                       0,
                       NEW.source_count - 1
                 ) AS source_ordinals(source_ordinal)
                  LEFT JOIN evidence_npi_source AS aggregate_npi_source
                    ON aggregate_npi_source.token_policy_id =
                           policy_lookup.token_policy_id
                   AND aggregate_npi_source.tin_id_128 =
                           policy_lookup.tin_id_128
                   AND aggregate_npi_source.tin_hmac_sha256 =
                           policy_lookup.tin_hmac_sha256
                   AND aggregate_npi_source.npi = npi_values.npi
                   AND aggregate_npi_source.source_ordinal =
                           source_ordinals.source_ordinal
                 WHERE policy_lookup.generation_key = NEW.generation_key
                   AND (
                          get_byte(
                              policy_lookup.npi_source_bitmap_matrix,
                              (
                                  (npi_values.npi_ordinal - 1)
                                  * ((NEW.source_count + 7) / 8)
                              )::integer
                              + source_ordinals.source_ordinal / 8
                          )
                          & (
                              1 << (
                                  source_ordinals.source_ordinal % 8
                              )
                          )
                       ) <> 0
                       IS DISTINCT FROM (
                           aggregate_npi_source.token_policy_id IS NOT NULL
                       )
            )
            SELECT COUNT(*)
              INTO evidence_projection_difference_count
              FROM (
                    SELECT * FROM invalid_key
                    UNION ALL
                    SELECT * FROM invalid_source
                    UNION ALL
                    SELECT * FROM invalid_npi_source
                   ) AS projection_difference;
            WITH expected_source_policy_counts AS MATERIALIZED (
                SELECT dataset_proof ->> 'source_id' AS source_id,
                       policy_count.key AS token_policy_id,
                       policy_count.value #>> '{{}}' AS evidence_count
                  FROM jsonb_array_elements(
                           NEW.scan_proof_canonical_json::jsonb
                               -> 'datasets'
                       ) AS dataset_proofs(dataset_proof)
                  CROSS JOIN LATERAL jsonb_each(
                       dataset_proof -> 'matched_evidence_counts'
                  ) AS policy_count(key, value)
            ),
            source_policy_scope AS MATERIALIZED (
                SELECT source_entry ->> 'source_id' AS source_id,
                       (
                           source_entry ->> 'ordinal'
                       )::integer + 1 AS source_array_ordinal,
                       generation_membership.token_policy_id
                  FROM jsonb_array_elements(
                           NEW.source_ordinal_map_canonical_json::jsonb
                       ) AS ordinals(source_entry)
                  CROSS JOIN {generation_policy} AS generation_membership
                 WHERE generation_membership.generation_key =
                           NEW.generation_key
            ),
            observed_source_policy_counts AS MATERIALIZED (
                SELECT scope.source_id,
                       scope.token_policy_id,
                       COALESCE(
                           SUM(
                               policy_lookup.source_evidence_counts[
                                   scope.source_array_ordinal
                               ]
                           ),
                           0
                       )::text AS evidence_count
                  FROM source_policy_scope AS scope
                  LEFT JOIN {lookup} AS policy_lookup
                    ON policy_lookup.generation_key = NEW.generation_key
                   AND policy_lookup.token_policy_id =
                           scope.token_policy_id
                 GROUP BY scope.source_id,
                          scope.token_policy_id,
                          scope.source_array_ordinal
            )
            SELECT COUNT(*)
              INTO source_policy_evidence_difference_count
              FROM (
                    (
                        SELECT * FROM expected_source_policy_counts
                        EXCEPT
                        SELECT * FROM observed_source_policy_counts
                    )
                    UNION ALL
                    (
                        SELECT * FROM observed_source_policy_counts
                        EXCEPT
                        SELECT * FROM expected_source_policy_counts
                    )
                   ) AS source_policy_evidence_difference;
            SELECT COUNT(*),
                   COALESCE(SUM(evidence_count), 0),
                   COALESCE(SUM(cardinality(npis)), 0),
                   COUNT(*) FILTER (
                       WHERE octet_length(source_bitmap)
                             <> (NEW.source_count + 7) / 8
                          OR cardinality(source_evidence_counts)
                             <> NEW.source_count
                          OR octet_length(npi_source_bitmap_matrix)
                             <> cardinality(npis)
                                * ((NEW.source_count + 7) / 8)
                          OR source_bitmap = decode(
                              repeat(
                                  '00',
                                  (NEW.source_count + 7) / 8
                              ),
                              'hex'
                          )
                          OR CASE
                                 WHEN NEW.source_count % 8 <> 0
                                  AND octet_length(source_bitmap)
                                      = (NEW.source_count + 7) / 8
                                 THEN get_byte(
                                          source_bitmap,
                                          octet_length(source_bitmap) - 1
                                      ) >= (
                                          1 << (NEW.source_count % 8)
                                      )
                                 ELSE FALSE
                             END
                   )
              INTO observed_forward_row_count,
                   observed_evidence_count,
                   observed_npi_edge_count,
                   invalid_source_bitmap_count
              FROM {lookup}
             WHERE generation_key = NEW.generation_key;
            SELECT COUNT(DISTINCT expanded_npi)
              INTO observed_reverse_row_count
              FROM {lookup}
             CROSS JOIN LATERAL unnest(npis)
                   AS expanded(expanded_npi)
             WHERE generation_key = NEW.generation_key;
            SELECT COUNT(*)
              INTO observed_audit_evidence_count
              FROM {evidence}
             WHERE generation_key = NEW.generation_key;
            calculated_lookup_digest :=
                {lookup_set_digest}(NEW.generation_key);
            IF invalid_source_bitmap_count <> 0
               OR invalid_policy_count <> 0
               OR identifier_rule_difference_count <> 0
               OR invalid_evidence_count <> 0
               OR invalid_evidence_record_count <> 0
               OR evidence_projection_difference_count <> 0
               OR evidence_scan_digest_difference_count <> 0
               OR source_matched_record_difference_count <> 0
               OR observed_matched_record_count <>
                    NEW.matched_organization_count
               OR (
                   NEW.forward_row_count > 0
                   AND unused_policy_count <> 0
               )
               OR source_membership_difference_count <> 0
               OR source_policy_evidence_difference_count <> 0
               OR observed_policy_count <> NEW.token_policy_count
               OR observed_forward_row_count <> NEW.forward_row_count
               OR observed_evidence_count <> NEW.evidence_count
               OR observed_audit_evidence_count <> NEW.evidence_count
               OR observed_npi_edge_count <> NEW.npi_edge_count
               OR observed_reverse_row_count <> NEW.reverse_row_count
               OR calculated_lookup_digest <> NEW.lookup_digest THEN
                RAISE EXCEPTION
                    'tin_npi_connector_generation_seal_mismatch'
                    USING ERRCODE = '23514';
            END IF;
            NEW.completed_at := transaction_timestamp();
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {child_insert_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            inserted_generation_key bigint;
            generation_state varchar(16);
            generation_lease timestamptz;
            generation_build_token_sha256 bytea;
        BEGIN
            FOR inserted_generation_key IN
                SELECT DISTINCT generation_key
                  FROM inserted_rows
                 ORDER BY generation_key
            LOOP
                SELECT state,
                       build_lease_expires_at,
                       build_token_sha256
                  INTO generation_state,
                       generation_lease,
                       generation_build_token_sha256
                  FROM {generation}
                 WHERE generation_key = inserted_generation_key
                 FOR SHARE;
            IF generation_state IS DISTINCT FROM 'building'
               OR generation_lease <= clock_timestamp()
                   OR NOT {build_token_matches}(
                       generation_build_token_sha256
                   ) THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_generation_not_loadable'
                        USING ERRCODE = '55000';
                END IF;
            END LOOP;
            RETURN NULL;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {child_mutation_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            gc_generation_key text;
            generation_state varchar(16);
            generation_build_lease_expires_at timestamptz;
            generation_gc_after timestamptz;
            current_reference_count bigint;
            generation_owner name;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                gc_generation_key := current_setting(
                    'healthporta.tin_npi_gc_generation_key',
                    TRUE
                );
                SELECT state,
                       build_lease_expires_at,
                       gc_after,
                       pg_catalog.pg_get_userbyid(relation.relowner)
                  INTO generation_state,
                       generation_build_lease_expires_at,
                       generation_gc_after,
                       generation_owner
                  FROM {generation}
                  JOIN pg_catalog.pg_class AS relation
                    ON relation.oid = {_ql(generation)}::regclass
                 WHERE generation_key = OLD.generation_key;
                SELECT COUNT(*)
                  INTO current_reference_count
                  FROM {current}
                 WHERE generation_key = OLD.generation_key;
                IF generation_state IN ('failed', 'retired')
                   AND gc_generation_key = OLD.generation_key::text
                   AND current_user = generation_owner
                   AND current_reference_count = 0
                   AND (
                       (
                           generation_state = 'retired'
                           AND generation_gc_after IS NOT NULL
                           AND generation_gc_after <= clock_timestamp()
                       )
                       OR (
                           generation_state = 'failed'
                           AND generation_build_lease_expires_at
                               <= clock_timestamp()
                       )
                   ) THEN
                    RETURN OLD;
                END IF;
            END IF;
            RAISE EXCEPTION
                'tin_npi_connector_child_immutable'
                USING ERRCODE = '55000';
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {current_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            generation_state varchar(16);
            pointer_generation_key text;
            pointer_owner name;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION
                    'tin_npi_connector_current_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'INSERT' THEN
                RETURN NEW;
            END IF;
            pointer_generation_key := current_setting(
                'healthporta.tin_npi_pointer_generation_key',
                TRUE
            );
            SELECT pg_catalog.pg_get_userbyid(relation.relowner)
              INTO pointer_owner
              FROM pg_catalog.pg_class AS relation
             WHERE relation.oid = TG_RELID;
            IF pointer_generation_key IS DISTINCT FROM
                    NEW.generation_key::text
               OR current_user <> pointer_owner THEN
                RAISE EXCEPTION
                    'tin_npi_connector_pointer_action_invalid'
                    USING ERRCODE = '55000';
            END IF;
            IF NEW.pointer_key <> OLD.pointer_key
               OR NEW.pointer_version <> OLD.pointer_version + 1
               OR NEW.generation_key IS NULL
               OR NEW.published_at IS DISTINCT FROM
                    transaction_timestamp()
               OR NEW.updated_at IS DISTINCT FROM
                    transaction_timestamp() THEN
                RAISE EXCEPTION
                    'tin_npi_connector_current_cas_contract_invalid'
                    USING ERRCODE = '55000';
            END IF;
            SELECT state
              INTO generation_state
              FROM {generation}
             WHERE generation_key = NEW.generation_key
             FOR SHARE;
            IF generation_state IS DISTINCT FROM 'complete' THEN
                RAISE EXCEPTION
                    'tin_npi_connector_generation_not_complete'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {truncate_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            RAISE EXCEPTION
                'tin_npi_connector_truncate_forbidden'
                USING ERRCODE = '55000';
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {dataset_resource_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            immutable_parent_count bigint;
        BEGIN
            IF TG_OP = 'TRUNCATE' THEN
                RAISE EXCEPTION
                    'tin_npi_connector_dataset_resource_truncate_forbidden'
                    USING ERRCODE = '55000';
            ELSIF TG_OP = 'INSERT' THEN
                PERFORM dataset.dataset_id
                  FROM {provider_directory_dataset} AS dataset
                  JOIN (
                        SELECT DISTINCT dataset_id
                          FROM new_rows
                       ) AS affected
                    ON affected.dataset_id = dataset.dataset_id
                 ORDER BY dataset.dataset_id
                   FOR SHARE OF dataset;
                SELECT COUNT(*)
                  INTO immutable_parent_count
                  FROM (
                        SELECT DISTINCT dataset_id
                          FROM new_rows
                       ) AS affected
                  JOIN {provider_directory_dataset} AS dataset
                    ON dataset.dataset_id = affected.dataset_id
                 WHERE dataset.status IN (
                           'validated',
                           'published',
                           'superseded'
                       );
            ELSIF TG_OP = 'DELETE' THEN
                PERFORM dataset.dataset_id
                  FROM {provider_directory_dataset} AS dataset
                  JOIN (
                        SELECT DISTINCT dataset_id
                          FROM old_rows
                       ) AS affected
                    ON affected.dataset_id = dataset.dataset_id
                 ORDER BY dataset.dataset_id
                   FOR SHARE OF dataset;
                SELECT COUNT(*)
                  INTO immutable_parent_count
                  FROM (
                        SELECT DISTINCT dataset_id
                          FROM old_rows
                       ) AS affected
                  JOIN {provider_directory_dataset} AS dataset
                    ON dataset.dataset_id = affected.dataset_id
                 WHERE dataset.status IN (
                           'validated',
                           'published'
                       );
            ELSIF TG_OP = 'UPDATE' THEN
                PERFORM dataset.dataset_id
                  FROM {provider_directory_dataset} AS dataset
                  JOIN (
                        SELECT dataset_id FROM old_rows
                        UNION
                        SELECT dataset_id FROM new_rows
                       ) AS affected
                    ON affected.dataset_id = dataset.dataset_id
                 ORDER BY dataset.dataset_id
                   FOR SHARE OF dataset;
                SELECT COUNT(*)
                  INTO immutable_parent_count
                  FROM (
                        SELECT dataset_id FROM old_rows
                        UNION
                        SELECT dataset_id FROM new_rows
                       ) AS affected
                  JOIN {provider_directory_dataset} AS dataset
                    ON dataset.dataset_id = affected.dataset_id
                 WHERE dataset.status IN (
                           'validated',
                           'published',
                           'superseded'
                       );
            ELSE
                RAISE EXCEPTION
                    'tin_npi_connector_dataset_resource_action_invalid'
                    USING ERRCODE = '55000';
            END IF;
            IF immutable_parent_count <> 0 THEN
                RAISE EXCEPTION
                    'tin_npi_connector_dataset_resource_parent_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {endpoint_dataset_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.status IN (
                    'validated',
                    'published',
                    'superseded'
                ) THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_endpoint_dataset_insert_invalid'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.status NOT IN ('validated', 'published', 'superseded') THEN
                IF TG_OP = 'DELETE' THEN
                    RETURN OLD;
                END IF;
                IF NEW.status IN ('published', 'superseded') THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_endpoint_dataset_transition_invalid'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_delete_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            IF (
                to_jsonb(NEW)
                    - ARRAY[
                        'status',
                        'is_current',
                        'published_at',
                        'superseded_at',
                        'publication_metadata_json'
                    ]
                <>
                to_jsonb(OLD)
                    - ARRAY[
                        'status',
                        'is_current',
                        'published_at',
                        'superseded_at',
                        'publication_metadata_json'
                    ]
            ) OR (
                OLD.status = 'validated'
                AND NOT (
                    (
                        NEW.status = 'validated'
                        AND NEW.is_current IS NOT DISTINCT FROM
                            OLD.is_current
                        AND NEW.published_at IS NOT DISTINCT FROM
                            OLD.published_at
                        AND NEW.superseded_at IS NOT DISTINCT FROM
                            OLD.superseded_at
                    )
                    OR (
                        NEW.status = 'published'
                        AND OLD.is_current IS FALSE
                        AND OLD.validated_at IS NOT NULL
                        AND OLD.published_at IS NULL
                        AND OLD.superseded_at IS NULL
                        AND NEW.is_current IS TRUE
                        AND NEW.published_at IS NOT DISTINCT FROM
                            transaction_timestamp()
                        AND NEW.superseded_at IS NULL
                    )
                )
            ) OR (
                OLD.status = 'published'
                AND NOT (
                    (
                        NEW.status = 'published'
                        AND NEW.is_current IS NOT DISTINCT FROM
                            OLD.is_current
                        AND NEW.published_at IS NOT DISTINCT FROM
                            OLD.published_at
                        AND NEW.superseded_at IS NOT DISTINCT FROM
                            OLD.superseded_at
                    )
                    OR (
                        NEW.status = 'superseded'
                        AND OLD.is_current IS TRUE
                        AND OLD.published_at IS NOT NULL
                        AND OLD.superseded_at IS NULL
                        AND NEW.is_current IS FALSE
                        AND NEW.published_at IS NOT DISTINCT FROM
                            OLD.published_at
                        AND NEW.superseded_at IS NOT NULL
                        AND NEW.superseded_at >= NEW.published_at
                        AND NEW.superseded_at IS NOT DISTINCT FROM
                            transaction_timestamp()
                    )
                )
            ) OR (
                OLD.status = 'superseded'
                AND (
                    NEW.status <> 'superseded'
                    OR NEW.is_current IS NOT DISTINCT FROM TRUE
                    OR NEW.is_current IS DISTINCT FROM OLD.is_current
                    OR NEW.published_at IS DISTINCT FROM
                        OLD.published_at
                    OR NEW.superseded_at IS DISTINCT FROM
                        OLD.superseded_at
                )
            ) THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {token_policy_fence}(
            target_generation_key bigint,
            require_exact_scope boolean
        )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        SET lock_timeout = '500ms'
        AS $function$
        DECLARE
            target_state varchar(16);
            target_source_vector jsonb;
            policy_scope_difference_count bigint;
        BEGIN
            SELECT state, source_vector_canonical_json::jsonb
              INTO target_state, target_source_vector
              FROM {generation}
             WHERE generation_key = target_generation_key
             FOR SHARE;
            IF target_state IS DISTINCT FROM 'complete'
               OR target_source_vector IS NULL THEN
                RAISE EXCEPTION
                    'tin_npi_connector_token_policy_fence_target_invalid'
                    USING ERRCODE = '55000';
            END IF;
            LOCK TABLE {ptg_tax_identity_manifest} IN SHARE MODE;
            WITH expected_policy_scope AS MATERIALIZED (
                SELECT policy ->> 'token_policy_id' AS token_policy_id,
                       policy
                           ->> 'token_policy_descriptor_sha256'
                           AS token_policy_descriptor_sha256
                  FROM jsonb_array_elements(
                           target_source_vector -> 'token_policies'
                       ) AS policies(policy)
            ),
            observed_policy_scope AS MATERIALIZED (
                SELECT DISTINCT
                       token_policy_id,
                       encode(
                           token_policy_descriptor_sha256,
                           'hex'
                       ) AS token_policy_descriptor_sha256
                  FROM {ptg_tax_identity_manifest}
            )
            SELECT COUNT(*)
              INTO policy_scope_difference_count
              FROM (
                    (
                        SELECT * FROM observed_policy_scope
                        EXCEPT
                        SELECT * FROM expected_policy_scope
                    )
                    UNION ALL
                    (
                        SELECT * FROM expected_policy_scope
                        EXCEPT
                        SELECT * FROM observed_policy_scope
                    )
                   ) AS policy_scope_difference
             WHERE require_exact_scope
                OR policy_scope_difference.token_policy_id IN (
                    SELECT token_policy_id FROM observed_policy_scope
                );
            IF policy_scope_difference_count <> 0 THEN
                RAISE EXCEPTION
                    'tin_npi_connector_token_policy_scope_changed'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {source_fence}(
            target_generation_key bigint
        )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        SET lock_timeout = '500ms'
        AS $function$
        DECLARE
            target_state varchar(16);
            target_source_vector jsonb;
            dataset_entry jsonb;
            relation_entry jsonb;
            expected_endpoint_count bigint;
            locked_endpoint_count bigint;
            expected_source_count bigint;
            locked_source_count bigint;
            source_scope_difference_count bigint;
            token_policy_scope_difference_count bigint;
            matching_dataset_count bigint;
            current_dataset_count bigint;
            current_dataset_id text;
            expected_relation_oid oid;
            observed_relation_oid oid;
            observed_relkind "char";
            observed_relpersistence "char";
            immutable_guard_count bigint;
            endpoint_dataset_guard_count bigint;
        BEGIN
            SELECT state, source_vector_canonical_json::jsonb
              INTO target_state, target_source_vector
              FROM {generation}
             WHERE generation_key = target_generation_key
             FOR SHARE;
            IF target_state IS DISTINCT FROM 'complete'
               OR target_source_vector IS NULL THEN
                RAISE EXCEPTION
                    'tin_npi_connector_source_fence_target_invalid'
                    USING ERRCODE = '55000';
            END IF;

            LOCK TABLE {provider_directory_endpoint} IN EXCLUSIVE MODE;
            LOCK TABLE {provider_directory_source} IN EXCLUSIVE MODE;
            LOCK TABLE {provider_directory_dataset} IN EXCLUSIVE MODE;
            LOCK TABLE {ptg_tax_identity_manifest} IN EXCLUSIVE MODE;
            LOCK TABLE {provider_directory_resource} IN SHARE MODE;
            SELECT COUNT(*)
              INTO immutable_guard_count
              FROM (
                    VALUES
                        (
                            'tin_npi_connector_dataset_resource_insert_guard',
                            4::smallint,
                            NULL::name,
                            'new_rows'::name
                        ),
                        (
                            'tin_npi_connector_dataset_resource_update_guard',
                            16::smallint,
                            'old_rows'::name,
                            'new_rows'::name
                        ),
                        (
                            'tin_npi_connector_dataset_resource_delete_guard',
                            8::smallint,
                            'old_rows'::name,
                            NULL::name
                        ),
                        (
                            'tin_npi_connector_dataset_resource_truncate_guard',
                            34::smallint,
                            NULL::name,
                            NULL::name
                        )
                   ) AS expected(
                       trigger_name,
                       trigger_type,
                       old_table,
                       new_table
                   )
              JOIN pg_catalog.pg_trigger AS trigger_row
                ON trigger_row.tgrelid =
                       {_ql(provider_directory_resource)}::regclass
               AND trigger_row.tgname = expected.trigger_name
               AND trigger_row.tgenabled = 'A'
               AND trigger_row.tgisinternal IS FALSE
               AND trigger_row.tgfoid =
                       {_ql(dataset_resource_guard + '()')}::regprocedure
               AND trigger_row.tgtype = expected.trigger_type
               AND trigger_row.tgattr = ''::int2vector
               AND trigger_row.tgqual IS NULL
               AND trigger_row.tgnargs = 0
               AND octet_length(trigger_row.tgargs) = 0
               AND trigger_row.tgoldtable IS NOT DISTINCT FROM
                       expected.old_table
               AND trigger_row.tgnewtable IS NOT DISTINCT FROM
                       expected.new_table;
            IF immutable_guard_count <> 4 THEN
                RAISE EXCEPTION
                    'tin_npi_connector_dataset_resource_guard_changed'
                    USING ERRCODE = '55000';
            END IF;
            SELECT COUNT(*)
              INTO endpoint_dataset_guard_count
              FROM pg_catalog.pg_trigger AS trigger_row
             WHERE trigger_row.tgrelid =
                       {_ql(provider_directory_dataset)}::regclass
               AND trigger_row.tgname =
                       'tin_npi_connector_endpoint_dataset_guard'
               AND trigger_row.tgenabled = 'A'
               AND trigger_row.tgisinternal IS FALSE
               AND trigger_row.tgfoid =
                       {_ql(endpoint_dataset_guard + '()')}::regprocedure
               AND trigger_row.tgtype = 31
               AND trigger_row.tgattr = ''::int2vector
               AND trigger_row.tgqual IS NULL
               AND trigger_row.tgnargs = 0
               AND octet_length(trigger_row.tgargs) = 0
               AND trigger_row.tgoldtable IS NULL
               AND trigger_row.tgnewtable IS NULL;
            IF endpoint_dataset_guard_count <> 1 THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_guard_changed'
                    USING ERRCODE = '55000';
            END IF;
            WITH expected_policy_scope AS MATERIALIZED (
                SELECT policy ->> 'token_policy_id' AS token_policy_id,
                       policy
                           ->> 'token_policy_descriptor_sha256'
                           AS token_policy_descriptor_sha256
                  FROM jsonb_array_elements(
                           target_source_vector -> 'token_policies'
                       ) AS policies(policy)
            ),
            observed_policy_scope AS MATERIALIZED (
                SELECT DISTINCT
                       token_policy_id,
                       encode(
                           token_policy_descriptor_sha256,
                           'hex'
                       ) AS token_policy_descriptor_sha256
                  FROM {ptg_tax_identity_manifest}
            )
            SELECT COUNT(*)
              INTO token_policy_scope_difference_count
              FROM (
                    (
                        SELECT * FROM expected_policy_scope
                        EXCEPT
                        SELECT * FROM observed_policy_scope
                    )
                    UNION ALL
                    (
                        SELECT * FROM observed_policy_scope
                        EXCEPT
                        SELECT * FROM expected_policy_scope
                    )
                   ) AS policy_scope_difference;
            IF token_policy_scope_difference_count <> 0 THEN
                RAISE EXCEPTION
                    'tin_npi_connector_token_policy_scope_changed'
                    USING ERRCODE = '55000';
            END IF;

            WITH expected_scope AS MATERIALIZED (
                SELECT dataset ->> 'source_id' AS source_id,
                       dataset ->> 'endpoint_id' AS endpoint_id,
                       dataset ->> 'dataset_id' AS dataset_id
                  FROM jsonb_array_elements(
                           target_source_vector -> 'fhir_datasets'
                       ) AS datasets(dataset)
            ),
            observed_scope AS MATERIALIZED (
                SELECT source.source_id,
                       source.endpoint_id,
                       dataset.dataset_id
                  FROM {provider_directory_source} AS source
                  JOIN {provider_directory_dataset} AS dataset
                    ON dataset.endpoint_id = source.endpoint_id
                 WHERE dataset.status = 'published'
                   AND dataset.is_current IS TRUE
                   AND dataset.published_at IS NOT NULL
                   AND dataset.superseded_at IS NULL
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'source_ids'
                       ) = 'array'
                   AND (
                           dataset.publication_metadata_json::jsonb
                               -> 'source_ids'
                       ) @> jsonb_build_array(source.source_id)
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'selected_resources'
                       ) = 'array'
                   AND (
                           dataset.publication_metadata_json::jsonb
                               -> 'selected_resources'
                       ) @> '["Organization"]'::jsonb
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'expected_resources'
                       ) = 'array'
            )
            SELECT COUNT(*)
              INTO source_scope_difference_count
              FROM (
                    (
                        SELECT * FROM expected_scope
                        EXCEPT
                        SELECT * FROM observed_scope
                    )
                    UNION ALL
                    (
                        SELECT * FROM observed_scope
                        EXCEPT
                        SELECT * FROM expected_scope
                    )
                   ) AS scope_difference;
            IF source_scope_difference_count <> 0 THEN
                RAISE EXCEPTION
                    'tin_npi_connector_fhir_source_scope_changed'
                    USING ERRCODE = '55000';
            END IF;

            SELECT COUNT(DISTINCT dataset ->> 'endpoint_id')
              INTO expected_endpoint_count
              FROM jsonb_array_elements(
                       target_source_vector -> 'fhir_datasets'
                   ) AS datasets(dataset);
            PERFORM endpoint.endpoint_id
              FROM {provider_directory_endpoint} AS endpoint
             WHERE EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                                  target_source_vector -> 'fhir_datasets'
                              ) AS datasets(dataset)
                        WHERE dataset ->> 'endpoint_id' =
                              endpoint.endpoint_id
                   )
             ORDER BY convert_to(endpoint.endpoint_id, 'UTF8')
            ;
            GET DIAGNOSTICS locked_endpoint_count = ROW_COUNT;
            IF locked_endpoint_count <> expected_endpoint_count THEN
                RAISE EXCEPTION
                    'tin_npi_connector_fhir_endpoint_changed'
                    USING ERRCODE = '55000';
            END IF;

            expected_source_count := jsonb_array_length(
                target_source_vector -> 'fhir_datasets'
            );
            PERFORM source.source_id
              FROM {provider_directory_source} AS source
             WHERE EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                                  target_source_vector -> 'fhir_datasets'
                              ) AS datasets(dataset)
                        WHERE dataset ->> 'source_id' = source.source_id
                   )
             ORDER BY convert_to(source.source_id, 'UTF8')
            ;
            GET DIAGNOSTICS locked_source_count = ROW_COUNT;
            IF locked_source_count <> expected_source_count THEN
                RAISE EXCEPTION
                    'tin_npi_connector_fhir_source_changed'
                    USING ERRCODE = '55000';
            END IF;
            FOR dataset_entry IN
                SELECT value
                  FROM jsonb_array_elements(
                           target_source_vector -> 'fhir_datasets'
                       )
                 ORDER BY convert_to(value ->> 'endpoint_id', 'UTF8'),
                          convert_to(value ->> 'source_id', 'UTF8')
            LOOP
                SELECT COUNT(*)
                  INTO matching_dataset_count
                  FROM {provider_directory_source} AS source
                  JOIN {provider_directory_dataset} AS dataset
                    ON dataset.endpoint_id = source.endpoint_id
                 WHERE source.source_id =
                           dataset_entry ->> 'source_id'
                   AND source.endpoint_id =
                           dataset_entry ->> 'endpoint_id'
                   AND dataset.dataset_id =
                           dataset_entry ->> 'dataset_id'
                   AND COALESCE(
                           dataset.acquisition_root_run_id,
                           dataset.import_run_id
                       ) = dataset_entry ->> 'evidence_run_id'
                   AND dataset.previous_dataset_id IS NOT DISTINCT FROM
                           dataset_entry ->> 'previous_dataset_id'
                   AND dataset.dataset_hash =
                           dataset_entry ->> 'dataset_hash'
                   AND dataset.status = 'published'
                   AND dataset.is_current IS TRUE
                   AND dataset.resource_count =
                           (dataset_entry ->> 'resource_count')::bigint
                   AND dataset.validated_at::text IS NOT DISTINCT FROM
                           dataset_entry ->> 'validated_at'
                   AND dataset.published_at IS NOT NULL
                   AND dataset.superseded_at IS NULL
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'source_ids'
                       ) = 'array'
                   AND (
                           dataset.publication_metadata_json::jsonb
                               -> 'source_ids'
                       ) @> jsonb_build_array(
                           dataset_entry ->> 'source_id'
                       )
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'selected_resources'
                       ) = 'array'
                   AND jsonb_array_length(
                           dataset.publication_metadata_json::jsonb
                               -> 'selected_resources'
                       ) = jsonb_array_length(
                           dataset_entry -> 'selected_resources'
                       )
                   AND (
                           dataset.publication_metadata_json::jsonb
                               -> 'selected_resources'
                       ) @> (dataset_entry -> 'selected_resources')
                   AND (
                           dataset.publication_metadata_json::jsonb
                               -> 'selected_resources'
                       ) <@ (dataset_entry -> 'selected_resources')
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'expected_resources'
                       ) = 'array'
                   AND jsonb_array_length(
                           dataset.publication_metadata_json::jsonb
                               -> 'expected_resources'
                       ) = jsonb_array_length(
                           dataset_entry -> 'recorded_expected_resources'
                       )
                   AND (
                           dataset.publication_metadata_json::jsonb
                               -> 'expected_resources'
                       ) @> (
                           dataset_entry -> 'recorded_expected_resources'
                       )
                   AND (
                           dataset.publication_metadata_json::jsonb
                               -> 'expected_resources'
                       ) <@ (
                           dataset_entry -> 'recorded_expected_resources'
                       )
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'source_summary_v1'
                       ) = 'object'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           ->> 'contract_id'
                       = 'healthporta.provider-directory.source-summary.v1'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           ->> 'contract_version' = '1'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           -> 'complete' = 'true'::jsonb
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           ->> 'summary_sha256'
                       = dataset_entry ->> 'source_summary_sha256'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           ->> 'dataset_id'
                       = dataset_entry ->> 'dataset_id'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           ->> 'endpoint_id'
                       = dataset_entry ->> 'endpoint_id'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           ->> 'acquisition_root_run_id'
                       = dataset_entry ->> 'evidence_run_id'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           ->> 'dataset_hash'
                       = dataset_entry ->> 'dataset_hash'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           ->> 'total_resources'
                       = dataset_entry ->> 'resource_count'
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'source_summary_v1'
                               -> 'source_ids'
                       ) = 'array'
                   AND (
                           dataset.publication_metadata_json::jsonb
                               -> 'source_summary_v1'
                               -> 'source_ids'
                       ) @> jsonb_build_array(
                           dataset_entry ->> 'source_id'
                       )
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'source_summary_v1'
                               -> 'resource_counts'
                       ) = 'object'
                   AND jsonb_typeof(
                           dataset.publication_metadata_json::jsonb
                               -> 'source_summary_v1'
                               -> 'resource_hashes'
                       ) = 'object'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           -> 'resource_counts'
                           ->> 'Organization'
                       = dataset_entry ->> 'organization_resource_count'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           -> 'resource_hashes'
                           ->> 'Organization'
                       = dataset_entry ->> 'organization_resource_sha256'
                   AND dataset.publication_metadata_json::jsonb
                           -> 'source_summary_v1'
                           ->> 'organization_resources'
                       = dataset_entry ->> 'organization_resource_count';
                IF matching_dataset_count <> 1 THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_fhir_dataset_changed'
                        USING ERRCODE = '55000';
                END IF;
                SELECT COUNT(*), MIN(dataset_id)
                  INTO current_dataset_count, current_dataset_id
                  FROM {provider_directory_dataset}
                 WHERE endpoint_id =
                           dataset_entry ->> 'endpoint_id'
                   AND status = 'published'
                   AND is_current IS TRUE
                   AND published_at IS NOT NULL
                   AND superseded_at IS NULL;
                IF current_dataset_count <> 1
                   OR current_dataset_id IS DISTINCT FROM
                        dataset_entry ->> 'dataset_id' THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_fhir_current_dataset_changed'
                        USING ERRCODE = '55000';
                END IF;
            END LOOP;

            FOR relation_entry IN
                SELECT value
                  FROM jsonb_array_elements(
                           target_source_vector -> 'input_relations'
                       )
                 ORDER BY convert_to(value ->> 'schema', 'UTF8'),
                          convert_to(value ->> 'relation', 'UTF8')
            LOOP
                expected_relation_oid :=
                    (relation_entry ->> 'relation_oid')::oid;
                SELECT relation.oid,
                       relation.relkind,
                       relation.relpersistence
                  INTO observed_relation_oid,
                       observed_relkind,
                       observed_relpersistence
                  FROM pg_class AS relation
                  JOIN pg_namespace AS namespace
                    ON namespace.oid = relation.relnamespace
                 WHERE namespace.nspname = relation_entry ->> 'schema'
                   AND relation.relname = relation_entry ->> 'relation';
                IF observed_relation_oid IS DISTINCT FROM
                        expected_relation_oid
                   OR observed_relkind::text <>
                        relation_entry ->> 'relkind'
                   OR observed_relpersistence::text <>
                        relation_entry ->> 'relpersistence' THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_source_relation_changed'
                        USING ERRCODE = '55000';
                END IF;
                EXECUTE format(
                    'LOCK TABLE %I.%I IN ACCESS SHARE MODE',
                    relation_entry ->> 'schema',
                    relation_entry ->> 'relation'
                );
                SELECT to_regclass(
                           format(
                               '%I.%I',
                               relation_entry ->> 'schema',
                               relation_entry ->> 'relation'
                           )
                       )::oid
                  INTO observed_relation_oid;
                IF observed_relation_oid IS DISTINCT FROM
                        expected_relation_oid THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_source_relation_changed'
                        USING ERRCODE = '55000';
                END IF;
            END LOOP;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {publish_generation}(
            expected_pointer_version bigint,
            expected_generation_key bigint,
            target_generation_key bigint,
            expected_source_vector_id bytea
        )
        RETURNS bigint
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            observed_pointer_version bigint;
            observed_generation_key bigint;
            target_state varchar(16);
            target_source_vector_id bytea;
            target_evidence_as_of timestamptz;
            incumbent_evidence_as_of timestamptz;
            updated_pointer_version bigint;
        BEGIN
            SELECT pointer_version, generation_key
              INTO observed_pointer_version, observed_generation_key
              FROM {current}
             WHERE pointer_key = 1
             FOR UPDATE;
            IF observed_pointer_version IS DISTINCT FROM
                    expected_pointer_version
               OR observed_generation_key IS DISTINCT FROM
                    expected_generation_key THEN
                RAISE EXCEPTION
                    'tin_npi_connector_pointer_cas_conflict'
                    USING ERRCODE = '40001';
            END IF;
            SELECT state,
                   source_vector_id,
                   evidence_as_of::timestamptz
              INTO target_state,
                   target_source_vector_id,
                   target_evidence_as_of
              FROM {generation}
             WHERE generation_key = target_generation_key
             FOR SHARE;
            IF target_state IS DISTINCT FROM 'complete'
               OR target_source_vector_id IS DISTINCT FROM
                    expected_source_vector_id THEN
                RAISE EXCEPTION
                    'tin_npi_connector_publish_target_invalid'
                    USING ERRCODE = '55000';
            END IF;
            IF observed_generation_key IS NOT NULL THEN
                SELECT evidence_as_of::timestamptz
                  INTO incumbent_evidence_as_of
                  FROM {generation}
                 WHERE generation_key = observed_generation_key
                 FOR SHARE;
                IF target_evidence_as_of < incumbent_evidence_as_of THEN
                    RAISE EXCEPTION
                        'tin_npi_connector_publish_source_regression'
                    USING ERRCODE = '55000';
                END IF;
            END IF;
            PERFORM {source_fence}(target_generation_key);
            PERFORM set_config(
                'healthporta.tin_npi_pointer_generation_key',
                target_generation_key::text,
                TRUE
            );
            UPDATE {current}
               SET pointer_version = pointer_version + 1,
                   generation_key = target_generation_key,
                   published_at = transaction_timestamp(),
                   updated_at = transaction_timestamp()
             WHERE pointer_key = 1
               AND pointer_version = expected_pointer_version
               AND generation_key IS NOT DISTINCT FROM
                    expected_generation_key
             RETURNING pointer_version
                  INTO updated_pointer_version;
            PERFORM set_config(
                'healthporta.tin_npi_pointer_generation_key',
                '',
                TRUE
            );
            IF updated_pointer_version IS NULL THEN
                RAISE EXCEPTION
                    'tin_npi_connector_pointer_cas_conflict'
                    USING ERRCODE = '40001';
            END IF;
            RETURN updated_pointer_version;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {rollback_generation}(
            expected_pointer_version bigint,
            expected_generation_key bigint,
            target_generation_key bigint
        )
        RETURNS bigint
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            observed_pointer_version bigint;
            observed_generation_key bigint;
            target_state varchar(16);
            updated_pointer_version bigint;
        BEGIN
            SELECT pointer_version, generation_key
              INTO observed_pointer_version, observed_generation_key
              FROM {current}
             WHERE pointer_key = 1
             FOR UPDATE;
            IF observed_pointer_version IS DISTINCT FROM
                    expected_pointer_version
               OR observed_generation_key IS DISTINCT FROM
                    expected_generation_key
               OR target_generation_key IS NOT DISTINCT FROM
                    observed_generation_key THEN
                RAISE EXCEPTION
                    'tin_npi_connector_pointer_cas_conflict'
                    USING ERRCODE = '40001';
            END IF;
            SELECT state
              INTO target_state
              FROM {generation}
             WHERE generation_key = target_generation_key
             FOR SHARE;
            IF target_state IS DISTINCT FROM 'complete' THEN
                RAISE EXCEPTION
                    'tin_npi_connector_rollback_target_invalid'
                    USING ERRCODE = '55000';
            END IF;
            PERFORM {token_policy_fence}(
                target_generation_key,
                FALSE
            );
            PERFORM set_config(
                'healthporta.tin_npi_pointer_generation_key',
                target_generation_key::text,
                TRUE
            );
            UPDATE {current}
               SET pointer_version = pointer_version + 1,
                   generation_key = target_generation_key,
                   published_at = transaction_timestamp(),
                   updated_at = transaction_timestamp()
             WHERE pointer_key = 1
               AND pointer_version = expected_pointer_version
               AND generation_key IS NOT DISTINCT FROM
                    expected_generation_key
             RETURNING pointer_version
                  INTO updated_pointer_version;
            PERFORM set_config(
                'healthporta.tin_npi_pointer_generation_key',
                '',
                TRUE
            );
            IF updated_pointer_version IS NULL THEN
                RAISE EXCEPTION
                    'tin_npi_connector_pointer_cas_conflict'
                    USING ERRCODE = '40001';
            END IF;
            RETURN updated_pointer_version;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {abandon_generation}(
            target_generation_key bigint
        )
        RETURNS bigint
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            target_state varchar(16);
            target_build_lease_expires_at timestamptz;
            current_reference_count bigint;
        BEGIN
            SELECT state, build_lease_expires_at
              INTO target_state, target_build_lease_expires_at
              FROM {generation}
             WHERE generation_key = target_generation_key
             FOR UPDATE;
            SELECT COUNT(*)
              INTO current_reference_count
              FROM {current}
             WHERE generation_key = target_generation_key;
            IF target_state IS DISTINCT FROM 'building'
               OR target_build_lease_expires_at > clock_timestamp()
               OR current_reference_count <> 0 THEN
                RAISE EXCEPTION
                    'tin_npi_connector_generation_not_abandonable'
                    USING ERRCODE = '55000';
            END IF;
            PERFORM set_config(
                'healthporta.tin_npi_abandon_generation_key',
                target_generation_key::text,
                TRUE
            );
            UPDATE {generation}
               SET state = 'failed'
             WHERE generation_key = target_generation_key;
            PERFORM set_config(
                'healthporta.tin_npi_abandon_generation_key',
                '',
                TRUE
            );
            RETURN target_generation_key;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {retire_generation}(
            target_generation_key bigint,
            retain_until timestamptz
        )
        RETURNS bigint
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            target_state varchar(16);
            current_reference_count bigint;
        BEGIN
            SELECT state
              INTO target_state
              FROM {generation}
             WHERE generation_key = target_generation_key
             FOR UPDATE;
            SELECT COUNT(*)
              INTO current_reference_count
              FROM {current}
             WHERE generation_key = target_generation_key;
            IF target_state IS DISTINCT FROM 'complete'
               OR current_reference_count <> 0
               OR retain_until IS NULL
               OR retain_until <
                    clock_timestamp() + interval '24 hours' THEN
                RAISE EXCEPTION
                    'tin_npi_connector_generation_not_retirable'
                    USING ERRCODE = '55000';
            END IF;
            PERFORM set_config(
                'healthporta.tin_npi_retire_generation_key',
                target_generation_key::text,
                TRUE
            );
            UPDATE {generation}
               SET state = 'retired',
                   gc_after = retain_until
             WHERE generation_key = target_generation_key;
            PERFORM set_config(
                'healthporta.tin_npi_retire_generation_key',
                '',
                TRUE
            );
            RETURN target_generation_key;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {gc_generation}(
            target_generation_key bigint,
            batch_size integer DEFAULT 10000
        )
        RETURNS TABLE(
            deleted_evidence_rows bigint,
            deleted_lookup_rows bigint,
            generation_removed boolean
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            target_state varchar(16);
            target_gc_after timestamptz;
            target_build_lease_expires_at timestamptz;
            current_reference_count bigint;
            remaining_evidence_rows boolean;
            remaining_lookup_rows boolean;
        BEGIN
            IF batch_size IS NULL
               OR batch_size < 1
               OR batch_size > 100000 THEN
                RAISE EXCEPTION
                    'tin_npi_connector_gc_batch_invalid'
                    USING ERRCODE = '22023';
            END IF;
            SELECT state, gc_after, build_lease_expires_at
              INTO target_state,
                   target_gc_after,
                   target_build_lease_expires_at
              FROM {generation}
             WHERE generation_key = target_generation_key
             FOR UPDATE;
            SELECT COUNT(*)
              INTO current_reference_count
              FROM {current}
             WHERE generation_key = target_generation_key;
            IF current_reference_count <> 0
               OR target_state IS NULL
               OR target_state NOT IN ('failed', 'retired')
               OR (
                   target_state = 'retired'
                   AND (
                       target_gc_after IS NULL
                       OR target_gc_after > clock_timestamp()
                   )
               )
               OR (
                   target_state = 'failed'
                   AND (
                       target_build_lease_expires_at IS NULL
                       OR target_build_lease_expires_at >
                           clock_timestamp()
                   )
               ) THEN
                RAISE EXCEPTION
                    'tin_npi_connector_generation_not_collectable'
                    USING ERRCODE = '55000';
            END IF;
            PERFORM set_config(
                'healthporta.tin_npi_gc_generation_key',
                target_generation_key::text,
                TRUE
            );
            WITH deletion_candidates AS (
                SELECT ctid
                  FROM {evidence}
                 WHERE generation_key = target_generation_key
                 ORDER BY evidence_id
                 LIMIT batch_size
                 FOR UPDATE SKIP LOCKED
            ),
            deleted_rows AS (
                DELETE FROM {evidence}
                 WHERE ctid IN (
                     SELECT ctid FROM deletion_candidates
                 )
                RETURNING 1
            )
            SELECT COUNT(*)
              INTO deleted_evidence_rows
              FROM deleted_rows;
            SELECT EXISTS (
                SELECT 1
                  FROM {evidence}
                 WHERE generation_key = target_generation_key
            )
              INTO remaining_evidence_rows;
            IF remaining_evidence_rows THEN
                deleted_lookup_rows := 0;
                generation_removed := FALSE;
                PERFORM set_config(
                    'healthporta.tin_npi_gc_generation_key',
                    '',
                    TRUE
                );
                RETURN NEXT;
                RETURN;
            END IF;
            WITH deletion_candidates AS (
                SELECT ctid
                  FROM {lookup}
                 WHERE generation_key = target_generation_key
                 ORDER BY
                     token_policy_id,
                     tin_id_128,
                     tin_hmac_sha256
                 LIMIT batch_size
                 FOR UPDATE SKIP LOCKED
            ),
            deleted_rows AS (
                DELETE FROM {lookup}
                 WHERE ctid IN (
                     SELECT ctid FROM deletion_candidates
                 )
                RETURNING 1
            )
            SELECT COUNT(*)
              INTO deleted_lookup_rows
              FROM deleted_rows;
            SELECT EXISTS (
                SELECT 1
                  FROM {lookup}
                 WHERE generation_key = target_generation_key
            )
              INTO remaining_lookup_rows;
            generation_removed := FALSE;
            IF NOT remaining_lookup_rows THEN
                DELETE FROM {generation_policy}
                 WHERE generation_key = target_generation_key;
                DELETE FROM {generation}
                 WHERE generation_key = target_generation_key;
                generation_removed := FOUND;
            END IF;
            PERFORM set_config(
                'healthporta.tin_npi_gc_generation_key',
                '',
                TRUE
            );
            RETURN NEXT;
        END;
        $function$;
        """
    )

    for table_name in (
        "tin_npi_connector_token_policy",
        "tin_npi_connector_identifier_policy",
    ):
        op.execute(
            f"""
            CREATE TRIGGER {_q(table_name + '_guard')}
            BEFORE UPDATE OR DELETE ON {_qt(schema, table_name)}
            FOR EACH ROW
            EXECUTE FUNCTION {token_policy_guard}();
            """
        )
    op.execute(
        f"""
        CREATE TRIGGER {_q('tin_npi_connector_endpoint_dataset_guard')}
        BEFORE INSERT OR UPDATE OR DELETE ON {provider_directory_dataset}
        FOR EACH ROW
        EXECUTE FUNCTION {endpoint_dataset_guard}();
        """
    )
    op.execute(
        f"""
        ALTER TABLE {provider_directory_dataset}
        ENABLE ALWAYS TRIGGER
            {_q('tin_npi_connector_endpoint_dataset_guard')};
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('tin_npi_connector_dataset_resource_insert_guard')}
        AFTER INSERT ON {provider_directory_resource}
        REFERENCING NEW TABLE AS new_rows
        FOR EACH STATEMENT
        EXECUTE FUNCTION {dataset_resource_guard}();
        """
    )
    op.execute(
        f"""
        ALTER TABLE {provider_directory_resource}
        ENABLE ALWAYS TRIGGER
            {_q('tin_npi_connector_dataset_resource_insert_guard')};
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('tin_npi_connector_dataset_resource_update_guard')}
        AFTER UPDATE ON {provider_directory_resource}
        REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
        FOR EACH STATEMENT
        EXECUTE FUNCTION {dataset_resource_guard}();
        """
    )
    op.execute(
        f"""
        ALTER TABLE {provider_directory_resource}
        ENABLE ALWAYS TRIGGER
            {_q('tin_npi_connector_dataset_resource_update_guard')};
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('tin_npi_connector_dataset_resource_delete_guard')}
        AFTER DELETE ON {provider_directory_resource}
        REFERENCING OLD TABLE AS old_rows
        FOR EACH STATEMENT
        EXECUTE FUNCTION {dataset_resource_guard}();
        """
    )
    op.execute(
        f"""
        ALTER TABLE {provider_directory_resource}
        ENABLE ALWAYS TRIGGER
            {_q('tin_npi_connector_dataset_resource_delete_guard')};
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('tin_npi_connector_dataset_resource_truncate_guard')}
        BEFORE TRUNCATE ON {provider_directory_resource}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {dataset_resource_guard}();
        """
    )
    op.execute(
        f"""
        ALTER TABLE {provider_directory_resource}
        ENABLE ALWAYS TRIGGER
            {_q('tin_npi_connector_dataset_resource_truncate_guard')};
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('tin_npi_connector_generation_guard')}
        BEFORE INSERT OR UPDATE OR DELETE ON {generation}
        FOR EACH ROW
        EXECUTE FUNCTION {generation_guard}();
        """
    )
    for table_name in (
        "tin_npi_connector_generation_policy",
        "tin_npi_connector_lookup",
        "tin_npi_connector_evidence",
    ):
        op.execute(
            f"""
            CREATE TRIGGER {_q(table_name + '_insert_guard')}
            AFTER INSERT ON {_qt(schema, table_name)}
            REFERENCING NEW TABLE AS inserted_rows
            FOR EACH STATEMENT
            EXECUTE FUNCTION {child_insert_guard}();
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER {_q(table_name + '_mutation_guard')}
            BEFORE UPDATE OR DELETE ON {_qt(schema, table_name)}
            FOR EACH ROW
            EXECUTE FUNCTION {child_mutation_guard}();
            """
        )
    op.execute(
        f"""
        CREATE TRIGGER {_q('tin_npi_connector_current_guard')}
        BEFORE INSERT OR UPDATE OR DELETE ON {current}
        FOR EACH ROW
        EXECUTE FUNCTION {current_guard}();
        """
    )
    for table_name in (
        "tin_npi_connector_token_policy",
        "tin_npi_connector_identifier_policy",
        "tin_npi_connector_generation",
        "tin_npi_connector_generation_policy",
        "tin_npi_connector_lookup",
        "tin_npi_connector_evidence",
        "tin_npi_connector_current",
    ):
        op.execute(
            f"""
            CREATE TRIGGER {_q(table_name + '_truncate_guard')}
            BEFORE TRUNCATE ON {_qt(schema, table_name)}
            FOR EACH STATEMENT
            EXECUTE FUNCTION {truncate_guard}();
            """
        )

    op.execute(f"REVOKE ALL ON FUNCTION {source_fence} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {token_policy_fence} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {dataset_resource_guard} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {endpoint_dataset_guard} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {publish_generation} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {rollback_generation} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {abandon_generation} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {retire_generation} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {gc_generation} FROM PUBLIC;")
    op.execute(
        f"""
        COMMENT ON TABLE {evidence}
        IS 'Immutable token-only Organization evidence retained for audit';
        """
    )
    op.execute(
        f"""
        COMMENT ON TABLE {lookup}
        IS 'Token-only same-Organization NPI arrays keyed by immutable generation';
        """
    )
    op.execute(
        f"""
        COMMENT ON COLUMN {lookup}.{_q('tin_hmac_sha256')}
        IS 'Full policy-scoped HMAC; authoritative after prefix lookup';
        """
    )
    op.execute(
        f"""
        COMMENT ON COLUMN {lookup}.{_q('npis')}
        IS 'Sorted unique NPIs asserted on the same source Organization';
        """
    )
    op.execute(
        f"""
        COMMENT ON COLUMN {lookup}.{_q('npi_source_bitmap_matrix')}
        IS 'Fixed-width source bitmaps aligned one-to-one with sorted NPIs';
        """
    )
    op.execute(
        f"""
        COMMENT ON FUNCTION {source_fence}(bigint)
        IS 'Locks and verifies current-published FHIR rows and input relations';
        """
    )
    op.execute(
        f"""
        COMMENT ON FUNCTION {publish_generation}(
            bigint,
            bigint,
            bigint,
            bytea
        )
        IS 'Owner-granted CAS cutover; serving roles must not receive table UPDATE';
        """
    )
    op.execute(
        f"""
        COMMENT ON FUNCTION {abandon_generation}(bigint)
        IS 'Owner-granted recovery for an expired building generation';
        """
    )
    op.execute(
        f"""
        COMMENT ON FUNCTION {retire_generation}(bigint, timestamptz)
        IS 'Owner-granted non-current retirement with a 24-hour retention floor';
        """
    )
    op.execute(
        f"""
        COMMENT ON FUNCTION {rollback_generation}(
            bigint,
            bigint,
            bigint
        )
        IS 'CAS rollback to retained sealed rows; source datasets may be superseded';
        """
    )


def downgrade() -> None:
    """Remove the connector foundation only while it is empty and inactive."""

    schema = _schema()
    token_policy = _qt(schema, "tin_npi_connector_token_policy")
    identifier_policy = _qt(
        schema,
        "tin_npi_connector_identifier_policy",
    )
    generation = _qt(schema, "tin_npi_connector_generation")
    current = _qt(schema, "tin_npi_connector_current")
    provider_directory_resource = _qt(
        schema,
        "provider_directory_dataset_resource",
    )
    op.execute(
        f"""
        DO $block$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {current} WHERE generation_key IS NOT NULL
            ) OR EXISTS (
                SELECT 1 FROM {generation}
            ) OR EXISTS (
                SELECT 1 FROM {token_policy}
            ) OR EXISTS (
                SELECT 1 FROM {identifier_policy}
            ) THEN
                RAISE EXCEPTION
                    'tin_npi_connector_downgrade_requires_empty_inactive_foundation'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$;
        """
    )
    for trigger_name in (
        "tin_npi_connector_dataset_resource_insert_guard",
        "tin_npi_connector_dataset_resource_update_guard",
        "tin_npi_connector_dataset_resource_delete_guard",
        "tin_npi_connector_dataset_resource_truncate_guard",
    ):
        op.execute(
            f"DROP TRIGGER IF EXISTS {_q(trigger_name)} "
            f"ON {provider_directory_resource};"
        )
    op.execute(
        f"DROP TRIGGER IF EXISTS "
        f"{_q('tin_npi_connector_endpoint_dataset_guard')} "
        f"ON {_qt(schema, 'provider_directory_endpoint_dataset')};"
    )
    for table_name in (
        "tin_npi_connector_current",
        "tin_npi_connector_evidence",
        "tin_npi_connector_lookup",
        "tin_npi_connector_generation_policy",
        "tin_npi_connector_generation",
        "tin_npi_connector_identifier_policy",
        "tin_npi_connector_token_policy",
    ):
        op.execute(f"DROP TABLE IF EXISTS {_qt(schema, table_name)};")
    for function_name, argument_types in (
        (
            "assert_tin_npi_connector_source_fence",
            "bigint",
        ),
        (
            "assert_tin_npi_connector_token_policy_fence",
            "bigint,boolean",
        ),
        (
            "retire_tin_npi_connector_generation",
            "bigint,timestamp with time zone",
        ),
        (
            "abandon_tin_npi_connector_generation",
            "bigint",
        ),
        (
            "rollback_tin_npi_connector_generation",
            "bigint,bigint,bigint",
        ),
        (
            "publish_tin_npi_connector_generation",
            "bigint,bigint,bigint,bytea",
        ),
        ("gc_tin_npi_connector_generation", "bigint,integer"),
        ("guard_tin_npi_connector_current", ""),
        ("guard_tin_npi_connector_child_mutation", ""),
        ("guard_tin_npi_connector_child_insert", ""),
        ("guard_tin_npi_connector_generation", ""),
        ("guard_tin_npi_connector_dataset_resource", ""),
        ("guard_tin_npi_connector_endpoint_dataset", ""),
        ("guard_tin_npi_connector_token_policy", ""),
        ("guard_tin_npi_connector_truncate", ""),
        ("tin_npi_connector_build_token_matches", "bytea"),
        ("tin_npi_connector_lookup_set_sha256", "bigint"),
        (
            "tin_npi_connector_evidence_set_sha256",
            "bigint,integer",
        ),
        (
            "tin_npi_connector_evidence_id_sha256",
            "text,bytea,bigint,text,bytea,bytea,bytea,bytea,bytea",
        ),
        (
            "tin_npi_connector_lookup_row_sha256",
            "text,bytea,bigint[],bigint,bytea,bytea,bigint[]",
        ),
        (
            "tin_npi_connector_valid_scan_proof",
            "text,text,integer,integer,bigint,bigint,bigint",
        ),
        (
            "tin_npi_connector_valid_source_evidence",
            "bytea,bytea,bigint[],bigint,bigint[]",
        ),
        (
            "tin_npi_connector_valid_source_vector",
            "text,smallint,smallint,text,text,text,text,text,bytea,text,"
            "integer,integer,integer",
        ),
        (
            "tin_npi_connector_valid_identifier_policy",
            "text,text",
        ),
        (
            "tin_npi_connector_identifier_rule_sha256",
            "jsonb",
        ),
        (
            "tin_npi_connector_valid_source_ordinal_map",
            "text,integer",
        ),
        (
            "tin_npi_connector_token_policy_descriptor_sha256",
            "text",
        ),
        ("tin_npi_connector_valid_npis", "bigint[]"),
        ("tin_npi_connector_valid_npi", "bigint"),
    ):
        op.execute(
            f"DROP FUNCTION IF EXISTS "
            f"{_q(schema)}.{_q(function_name)}({argument_types});"
        )
