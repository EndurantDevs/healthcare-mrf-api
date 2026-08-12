# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit reviewed single roots for the specialized Provider Directory paths.

Revision ID: 20260812030000_provider_directory_specialized_single_root_admission
Revises: 20260812020000_provider_directory_endpoint_dataset_admission_seal
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = (
    "20260812030000_provider_directory_specialized_single_root_admission"
)
down_revision = (
    "20260812020000_provider_directory_endpoint_dataset_admission_seal"
)
branch_labels = None
depends_on = None


_FLEX_ADMISSION_FILE = (
    "20260810070000_provider_directory_uhc_flex_practitioner_twin_admission.py"
)
_FLEX_COHORT_FILE = (
    "20260810050000_provider_directory_uhc_flex_npi_cohort.py"
)
_FLEX_PUBLICATION_FILE = (
    "20260810080000_provider_directory_uhc_flex_practitioner_publication.py"
)
_ROOTED_FILE = "20260811020000_provider_directory_rooted_graph_acquisition.py"

_FLEX_SINGLE_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-"
    "reviewed-single-root-admission.v1"
)
_FLEX_INTENT_DOMAIN = (
    "healthporta.provider-directory.uhc-flex-practitioner-"
    "reviewed-single-root-dataset-intent.v1"
)
_FLEX_RUN_DOMAIN = (
    "healthporta.provider-directory.uhc-flex-practitioner-"
    "reviewed-single-root-acquisition-run.v1"
)
_ROOTED_SINGLE_CONTRACT = (
    "healthporta.provider-directory.rooted-graph-single-root-admission.v1"
)
_ROOTED_SINGLE_OPERATOR_SHA256 = (
    "a8a6d85a7eff0812216589c85f6adeac3582a28325be86881bb71097252c3253"
)
_POLICY_VERSION = "provider-directory-reviewed-root-policy-v1"
_POLICY_JSON = (
    '{"policy_version":"provider-directory-reviewed-root-policy-v1",'
    '"required_root_count":1}'
)
_POLICY_SQL_JSON = _POLICY_JSON.replace(":1", r"\:1")

_FLEX_SINGLE_GUARD = (
    "guard_pd_uhc_flex_practitioner_single_root_admission_insert"
)
_ROOTED_SINGLE_GUARD = (
    "guard_provider_directory_rooted_graph_single_root_admission"
)
_ROOTED_LEGACY_INSERT_TRIGGER = (
    "provider_directory_rooted_graph_twin_admission_legacy_insert_guard"
)
_ROOTED_SINGLE_INSERT_TRIGGER = (
    "provider_directory_rooted_graph_twin_admission_single_root_insert_guard"
)


def _load(filename: str, module_name: str) -> ModuleType:
    path = Path(__file__).with_name(filename)
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(f"Provider Directory predecessor unavailable: {filename}")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


@lru_cache(maxsize=1)
def _flex_admission() -> ModuleType:
    return _load(_FLEX_ADMISSION_FILE, "_pd_single_root_flex_admission")


@lru_cache(maxsize=1)
def _flex_cohort() -> ModuleType:
    return _load(_FLEX_COHORT_FILE, "_pd_single_root_flex_cohort")


@lru_cache(maxsize=1)
def _flex_publication() -> ModuleType:
    return _load(_FLEX_PUBLICATION_FILE, "_pd_single_root_flex_publication")


@lru_cache(maxsize=1)
def _rooted() -> ModuleType:
    return _load(_ROOTED_FILE, "_pd_single_root_rooted_graph")


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qf(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _replace_once(sql: str, old: str, new: str, label: str) -> str:
    if sql.count(old) != 1:
        raise RuntimeError(f"Provider Directory {label} predecessor changed")
    return sql.replace(old, new, 1)


def _replace_function(sql: str, label: str) -> str:
    if sql.count("CREATE OR REPLACE FUNCTION") == 1:
        return sql
    return _replace_once(sql, "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", label)


def _digest_identifier_sql(
    prefix: str,
    contract: str,
    fields: tuple[str, ...],
) -> str:
    values = ", ".join((_ql(contract), *fields))
    return (
        f"{_ql(prefix)} || pg_catalog.substr("
        "pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to("
        f"pg_catalog.concat_ws(pg_catalog.chr(31), {values}), 'UTF8')), 'hex'),"
        " 1, 48)"
    )


def _policy_predicate(column: str) -> str:
    return f"{column} = {_ql(_POLICY_SQL_JSON)}::jsonb"


def _flex_check_sql(schema: str, *, historical: bool) -> str:
    predecessor = _flex_admission()
    admission = _qf(schema, predecessor._ADMISSION)
    constraint = _q("pd_uhc_flex_practitioner_twin_admission_check")
    if historical:
        expression = (
            f"admission_contract_id = {_ql(predecessor._ADMISSION_CONTRACT)} AND "
            "admission_id ~ '^pdufpad_[0-9a-f]{48}$' AND "
            "operation_key ~ '^[0-9a-f]{64}$' AND "
            "semantic_projection_as_of BETWEEN DATE '0001-01-01' "
            "AND DATE '9999-12-31' AND "
            "baseline_acquisition_id <> candidate_acquisition_id AND "
            "baseline_run_id <> candidate_run_id AND "
            "expected_npi_count > 0 AND resource_count >= 0 AND "
            "terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "publication_authority IS TRUE"
        )
    else:
        contract_branch = (
            "((admission_contract_id = "
            f"{_ql(predecessor._ADMISSION_CONTRACT)} AND attempt_id IS NOT NULL "
            "AND baseline_acquisition_id IS NOT NULL AND "
            "baseline_run_id IS NOT NULL AND "
            "baseline_acquisition_id <> candidate_acquisition_id AND "
            "baseline_run_id <> candidate_run_id AND "
            "reviewed_root_policy_json IS NULL) OR "
            f"(admission_contract_id = {_ql(_FLEX_SINGLE_CONTRACT)} AND "
            "attempt_id IS NULL AND baseline_acquisition_id IS NULL AND "
            "baseline_run_id IS NULL AND "
            "reviewed_root_policy_json IS NOT NULL AND "
            f"{_policy_predicate('reviewed_root_policy_json')}))"
        )
        expression = (
            "admission_id ~ '^pdufpad_[0-9a-f]{48}$' AND "
            "operation_key ~ '^[0-9a-f]{64}$' AND "
            "semantic_projection_as_of BETWEEN DATE '0001-01-01' "
            "AND DATE '9999-12-31' AND candidate_acquisition_id IS NOT NULL AND "
            "candidate_run_id IS NOT NULL AND expected_npi_count > 0 AND "
            "resource_count >= 0 AND terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "publication_authority IS TRUE AND "
            f"{contract_branch}"
        )
    return (
        f"ALTER TABLE {admission} ADD CONSTRAINT {constraint} "
        f"CHECK ({expression}) NOT VALID;"
    )


def _rooted_check_sql(schema: str, *, historical: bool) -> str:
    predecessor = _rooted()
    admission = _qf(schema, predecessor._TWIN_ADMISSION)
    constraint = _q("pd_rooted_graph_twin_admission_check")
    common = (
        "admission_id ~ '^pdrgad_[0-9a-f]{48}$' AND "
        f"storage_contract_id = {_ql(predecessor._STORAGE_CONTRACT)} AND "
        "publication_authority IS TRUE AND "
    )
    if historical:
        branch = (
            f"admission_contract_id = {_ql(predecessor._TWIN_ADMISSION_CONTRACT)}"
        )
    else:
        branch = (
            "((admission_contract_id = "
            f"{_ql(predecessor._TWIN_ADMISSION_CONTRACT)} AND "
            "attempt_id IS NOT NULL AND comparison_acquisition_id IS NOT NULL "
            "AND reviewed_root_policy_json IS NULL AND "
            "acquisition_operation_key IS NULL) OR "
            f"(admission_contract_id = {_ql(_ROOTED_SINGLE_CONTRACT)} AND "
            "attempt_id IS NULL AND comparison_acquisition_id IS NULL AND "
            "reviewed_root_policy_json IS NOT NULL AND "
            f"{_policy_predicate('reviewed_root_policy_json')} AND "
            "acquisition_operation_key IS NOT NULL AND "
            "acquisition_operation_key ~ '^[0-9a-f]{64}$'))"
        )
    return (
        f"ALTER TABLE {admission} ADD CONSTRAINT {constraint} "
        f"CHECK ({common}{branch}) NOT VALID;"
    )


def _flex_single_guard_sql(schema: str) -> str:
    flex = _flex_admission()
    cohort_migration = _flex_cohort()
    acquisition = _qf(schema, flex._ACQUISITION)
    cohort = _qf(schema, cohort_migration._COHORT)
    source = _qf(schema, "provider_directory_source")
    dataset = _qf(schema, "provider_directory_endpoint_dataset")
    guard = _qf(schema, _FLEX_SINGLE_GUARD)
    expected_admission_id = _digest_identifier_sql(
        "pdufpad_",
        _FLEX_SINGLE_CONTRACT,
        (
            "NEW.semantic_projection_as_of::text",
            "NEW.operation_key",
            "candidate.acquisition_id",
            "candidate.cohort_id",
            "candidate.dataset_intent_id",
            "candidate.source_id",
            "candidate.connector_id",
            "candidate.query_contract_id",
            "candidate.storage_contract_id",
            "candidate.run_id",
            "candidate.expected_npi_count::text",
            "candidate.terminal_set_sha256",
            "candidate.resource_count::text",
            _ql(_POLICY_VERSION),
            "'1'",
            "'true'",
        ),
    )
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        candidate record;
        current_official record;
        expected_intent_id text;
        expected_run_id text;
        expected_acquisition_id text;
        expected_admission_id text;
    BEGIN
        PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(NEW.candidate_acquisition_id, 2701)
        );
        SELECT * INTO candidate FROM {acquisition}
         WHERE acquisition_id = NEW.candidate_acquisition_id FOR SHARE;
        SELECT official_cohort.cohort_id,
               official_cohort.official_source_id,
               official_cohort.official_endpoint_id,
               official_cohort.official_dataset_id,
               official_cohort.official_acquisition_root_run_id,
               official_cohort.official_dataset_hash,
               official_cohort.official_content_proof_sha256,
               official_cohort.practitioner_resource_count,
               official_cohort.npi_count,
               official_cohort.cohort_complete,
               official_cohort.endpoint_collection_complete,
               official_cohort.endpoint_complete,
               official_source.endpoint_id AS source_endpoint_id,
               official_dataset.endpoint_id AS dataset_endpoint_id,
               official_dataset.acquisition_root_run_id
                   AS dataset_acquisition_root_run_id,
               official_dataset.dataset_hash AS dataset_hash,
               official_dataset.resource_count AS dataset_resource_count,
               official_dataset.publication_metadata_json::jsonb
                   -> {_ql(cohort_migration._CONTENT_PROOF_KEY)} AS content_proof
          INTO current_official
          FROM {cohort} AS official_cohort
          JOIN {source} AS official_source
            ON official_source.source_id = official_cohort.official_source_id
          JOIN {dataset} AS official_dataset
            ON official_dataset.dataset_id = official_cohort.official_dataset_id
         WHERE official_cohort.cohort_id = candidate.cohort_id
           AND official_source.source_id = {_ql(cohort_migration._SOURCE_ID)}
           AND official_dataset.status = 'published'
           AND official_dataset.is_current IS TRUE
         FOR SHARE OF official_cohort, official_source, official_dataset;
        expected_intent_id := {_digest_identifier_sql(
            "pdufdi_",
            _FLEX_INTENT_DOMAIN,
            (
                "candidate.cohort_id",
                "NEW.semantic_projection_as_of::text",
                "NEW.operation_key",
            ),
        )};
        expected_run_id := {_digest_identifier_sql(
            "pdufpr_", _FLEX_RUN_DOMAIN, ("expected_intent_id", "'candidate'")
        )};
        expected_acquisition_id := 'pdufpa_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                pg_catalog.concat_ws(pg_catalog.chr(31),
                    candidate.storage_contract_id, candidate.cohort_id,
                    'candidate', candidate.source_id, candidate.connector_id,
                    candidate.query_contract_id, expected_run_id,
                    expected_intent_id, candidate.expected_npi_count::text,
                    'false', 'false'
                ), 'UTF8')), 'hex'), 1, 48
        );
        expected_admission_id := {expected_admission_id};
        IF candidate.acquisition_id IS NULL
           OR current_official.cohort_id IS NULL
           OR current_official.npi_count IS DISTINCT FROM
              candidate.expected_npi_count
           OR current_official.cohort_complete IS DISTINCT FROM TRUE
           OR current_official.endpoint_collection_complete IS DISTINCT FROM FALSE
           OR current_official.endpoint_complete IS DISTINCT FROM FALSE
           OR current_official.source_endpoint_id IS DISTINCT FROM
              current_official.official_endpoint_id
           OR current_official.dataset_endpoint_id IS DISTINCT FROM
              current_official.official_endpoint_id
           OR current_official.dataset_acquisition_root_run_id IS DISTINCT FROM
              current_official.official_acquisition_root_run_id
           OR current_official.dataset_hash IS DISTINCT FROM
              current_official.official_dataset_hash
           OR current_official.content_proof IS NULL
           OR current_official.content_proof ->> 'contract_id' IS DISTINCT FROM
              {_ql(cohort_migration._CONTENT_PROOF_CONTRACT)}
           OR current_official.content_proof -> 'complete'
              IS DISTINCT FROM 'true'::jsonb
           OR current_official.content_proof ->> 'source_id' IS DISTINCT FROM
              current_official.official_source_id
           OR current_official.content_proof ->> 'dataset_id' IS DISTINCT FROM
              current_official.official_dataset_id
           OR current_official.content_proof ->> 'endpoint_id' IS DISTINCT FROM
              current_official.official_endpoint_id
           OR current_official.content_proof ->> 'acquisition_root_run_id'
              IS DISTINCT FROM current_official.official_acquisition_root_run_id
           OR current_official.content_proof ->> 'dataset_hash' IS DISTINCT FROM
              current_official.official_dataset_hash
           OR current_official.content_proof ->> 'proof_sha256' IS DISTINCT FROM
              current_official.official_content_proof_sha256
           OR COALESCE(
                current_official.content_proof ->> 'resource_count'
                    ~ '^[0-9]+$', FALSE
              ) IS NOT TRUE
           OR COALESCE(
                current_official.content_proof -> 'resource_counts'
                    ->> 'Practitioner' ~ '^[0-9]+$', FALSE
              ) IS NOT TRUE
           OR (current_official.content_proof ->> 'resource_count')::bigint
              IS DISTINCT FROM current_official.dataset_resource_count
           OR (current_official.content_proof -> 'resource_counts'
               ->> 'Practitioner')::bigint IS DISTINCT FROM
              current_official.practitioner_resource_count
           OR candidate.acquisition_role IS DISTINCT FROM 'candidate'
           OR candidate.status IS DISTINCT FROM 'sealed'
           OR candidate.cohort_complete IS DISTINCT FROM TRUE
           OR candidate.pending_count IS DISTINCT FROM 0
           OR candidate.leased_count IS DISTINCT FROM 0
           OR candidate.error_count IS DISTINCT FROM 0
           OR candidate.endpoint_collection_complete IS DISTINCT FROM FALSE
           OR candidate.endpoint_complete IS DISTINCT FROM FALSE
           OR candidate.sealed_at IS NULL
           OR candidate.terminal_set_sha256 IS NULL
           OR candidate.resource_count IS NULL
           OR candidate.dataset_intent_id IS DISTINCT FROM expected_intent_id
           OR candidate.run_id IS DISTINCT FROM expected_run_id
           OR candidate.acquisition_id IS DISTINCT FROM expected_acquisition_id
           OR NEW.admission_id IS DISTINCT FROM expected_admission_id
           OR NEW.admission_contract_id IS DISTINCT FROM {_ql(_FLEX_SINGLE_CONTRACT)}
           OR NEW.attempt_id IS NOT NULL
           OR NEW.baseline_acquisition_id IS NOT NULL
           OR NEW.baseline_run_id IS NOT NULL
           OR NEW.candidate_acquisition_id IS DISTINCT FROM candidate.acquisition_id
           OR NEW.cohort_id IS DISTINCT FROM candidate.cohort_id
           OR NEW.dataset_intent_id IS DISTINCT FROM expected_intent_id
           OR NEW.source_id IS DISTINCT FROM candidate.source_id
           OR NEW.connector_id IS DISTINCT FROM candidate.connector_id
           OR NEW.query_contract_id IS DISTINCT FROM candidate.query_contract_id
           OR NEW.storage_contract_id IS DISTINCT FROM candidate.storage_contract_id
           OR NEW.candidate_run_id IS DISTINCT FROM expected_run_id
           OR NEW.expected_npi_count IS DISTINCT FROM candidate.expected_npi_count
           OR NEW.terminal_set_sha256 IS DISTINCT FROM candidate.terminal_set_sha256
           OR NEW.resource_count IS DISTINCT FROM candidate.resource_count
           OR NEW.publication_authority IS DISTINCT FROM TRUE
           OR {_policy_predicate('NEW.reviewed_root_policy_json')} IS NOT TRUE
           OR NEW.admitted_at IS DISTINCT FROM transaction_timestamp()
           OR NEW.admitted_at < candidate.sealed_at THEN
            RAISE EXCEPTION
                'provider_directory_flex_single_root_admission_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _flex_metadata_sql(header: str, admission: str) -> str:
    legacy = _flex_publication()._metadata_sql(header, admission)
    return f"""
        CASE
          WHEN {admission}.admission_contract_id =
               {_ql(_flex_admission()._ADMISSION_CONTRACT)}
          THEN ({legacy})
          WHEN {admission}.admission_contract_id = {_ql(_FLEX_SINGLE_CONTRACT)}
          THEN (({legacy}) - ARRAY[
                    'baseline_acquisition_id', 'baseline_run_id'
                ]::text[] || pg_catalog.jsonb_build_object(
                    'provider_directory_reviewed_root_policy_v1',
                    {admission}.reviewed_root_policy_json
                ))
          ELSE NULL::jsonb
        END
    """


def _flex_valid_function_sql(schema: str) -> str:
    predecessor = _flex_publication()
    sql = predecessor._valid_function_sql(schema)
    sql = _replace_once(
        sql,
        predecessor._metadata_sql("header", "admission"),
        _flex_metadata_sql("header", "admission"),
        "Flex publication metadata",
    )
    sql = _replace_once(
        sql,
        f"AND admission.admission_contract_id = {_ql(predecessor._ADMISSION_CONTRACT)}",
        "AND ((admission.admission_contract_id = "
        f"{_ql(predecessor._ADMISSION_CONTRACT)} AND "
        "admission.attempt_id IS NOT NULL AND "
        "admission.baseline_acquisition_id IS NOT NULL AND "
        "admission.baseline_run_id IS NOT NULL AND "
        "admission.reviewed_root_policy_json IS NULL) OR "
        f"(admission.admission_contract_id = {_ql(_FLEX_SINGLE_CONTRACT)} AND "
        "admission.attempt_id IS NULL AND "
        "admission.baseline_acquisition_id IS NULL AND "
        "admission.baseline_run_id IS NULL AND "
        f"{_policy_predicate('admission.reviewed_root_policy_json')}))",
        "Flex admission selector",
    )
    return _replace_function(sql, "Flex valid function")


def _canonical_json_function(schema: str) -> str:
    return _qf(schema, "ptg_wave_canonical_json_ascii_v1")


def _rooted_current_root_json_sql(row: str) -> str:
    return f"""
        pg_catalog.jsonb_build_object(
            'dataset_id', {row}.dataset_id,
            'endpoint_id', {row}.endpoint_id,
            'source_id', {row}.source_id,
            'root_source_id', {row}.root_source_id,
            'root_endpoint_id', {row}.root_endpoint_id,
            'acquisition_source_id', {row}.acquisition_source_id,
            'acquisition_endpoint_id', {row}.acquisition_endpoint_id,
            'practitioner_origin_source_id', {row}.practitioner_origin_source_id,
            'practitioner_origin_endpoint_id', {row}.practitioner_origin_endpoint_id,
            'source_authority_id', {row}.source_authority_id,
            'endpoint_signature_sha256', {row}.endpoint_signature_sha256,
            'dataset_hash', {row}.dataset_hash,
            'resource_count', {row}.resource_count,
            'practitioner_resource_count', {row}.practitioner_resource_count,
            'root_content_proof_sha256', {row}.root_content_proof_sha256,
            'root_cohort_id', {row}.root_cohort_id,
            'semantic_projection_as_of', {row}.semantic_projection_as_of::text,
            'operation_key', {row}.operation_key,
            'acquisition_root_run_id', {row}.acquisition_root_run_id,
            'variant', {row}.variant,
            'root_publication_contract_id', {row}.root_publication_contract_id
        )
    """


def _rooted_single_guard_sql(schema: str) -> str:
    rooted = _rooted()
    guard = _qf(schema, _ROOTED_SINGLE_GUARD)
    acquisition = _qf(schema, rooted._ACQUISITION)
    legacy = _qf(schema, rooted._LEGACY_DATASET)
    rooted_header = _qf(schema, rooted._ROOTED_DATASET)
    parent = _qf(schema, rooted._DATASET)
    endpoint = _qf(schema, rooted._ENDPOINT)
    canonical = _canonical_json_function(schema)
    expected_admission_id = _digest_identifier_sql(
        "pdrgad_",
        _ROOTED_SINGLE_CONTRACT,
        (
            "NEW.admission_contract_id",
            "NEW.storage_contract_id",
            "'None'",
            "NEW.publication_acquisition_id",
            "'None'",
            "NEW.publication_run_id",
            "NEW.dataset_intent_id",
            "NEW.scope_id",
            "NEW.root_source_id",
            "NEW.root_endpoint_id",
            "NEW.acquisition_source_id",
            "NEW.acquisition_endpoint_id",
            "NEW.source_authority_id",
            "NEW.endpoint_signature_sha256",
            "NEW.root_dataset_id",
            "NEW.root_dataset_variant",
            "NEW.root_publication_contract_id",
            "NEW.root_dataset_hash",
            "NEW.root_content_proof_sha256",
            "NEW.root_cohort_id",
            "NEW.root_resource_count::text",
            "NEW.connector_id",
            "NEW.graph_contract_sha256",
            "NEW.query_contract_sha256",
            "NEW.max_work_items::text",
            "NEW.max_resource_rows::text",
            "NEW.max_edge_rows::text",
            "NEW.max_payload_bytes::text",
            "NEW.completed_count::text",
            "NEW.resource_count::text",
            "NEW.edge_count::text",
            "NEW.insurance_plan_count::text",
            "NEW.insurance_plan_page_count::text",
            "NEW.used_work_items::text",
            "NEW.used_resource_rows::text",
            "NEW.used_edge_rows::text",
            "NEW.used_payload_bytes::text",
            "NEW.terminal_set_sha256",
            "NEW.resource_set_sha256",
            "NEW.edge_set_sha256",
            "NEW.rooted_graph_sha256",
            "'True'",
            _ql(_POLICY_SQL_JSON),
            "NEW.acquisition_operation_key",
        ),
    )
    scope_digest = rooted._digest_sql(rooted._scope_identity_sql("candidate"))
    acquisition_digest = rooted._digest_sql(
        "candidate.storage_contract_id || pg_catalog.chr(31) || "
        "expected_scope_id || pg_catalog.chr(31) || candidate.root_cohort_id || "
        "pg_catalog.chr(31) || candidate.endpoint_signature_sha256 || "
        "pg_catalog.chr(31) || candidate.graph_contract_sha256 || "
        "pg_catalog.chr(31) || candidate.query_contract_sha256 || "
        "pg_catalog.chr(31) || 'candidate' || pg_catalog.chr(31) || "
        "expected_run_id || pg_catalog.chr(31) || expected_intent_id"
    )
    current_root_json = _rooted_current_root_json_sql("current_root")
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        candidate record;
        current_root record;
        expected_scope_id text;
        expected_intent_id text;
        expected_run_id text;
        expected_acquisition_id text;
        expected_admission_id text;
    BEGIN
        PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(NEW.publication_acquisition_id, 2801)
        );
        SELECT * INTO candidate FROM {acquisition}
         WHERE acquisition_id = NEW.publication_acquisition_id FOR SHARE;
        IF candidate.root_dataset_variant = {_ql(rooted._LEGACY_VARIANT)} THEN
            SELECT header.dataset_id, header.endpoint_id, header.source_id,
                   header.source_id AS root_source_id,
                   header.endpoint_id AS root_endpoint_id,
                   {_ql(rooted._ROOTED_SOURCE_ID)}::text AS acquisition_source_id,
                   {_ql(rooted._ROOTED_ENDPOINT_ID)}::text AS acquisition_endpoint_id,
                   header.source_id AS practitioner_origin_source_id,
                   header.endpoint_id AS practitioner_origin_endpoint_id,
                   header.source_authority_id,
                   graph_endpoint.endpoint_signature_hash
                       AS endpoint_signature_sha256,
                   parent.dataset_hash, parent.resource_count,
                   header.resource_count AS practitioner_resource_count,
                   header.terminal_set_sha256 AS root_content_proof_sha256,
                   header.cohort_id AS root_cohort_id,
                   header.semantic_projection_as_of, header.operation_key,
                   header.acquisition_root_run_id,
                   {_ql(rooted._LEGACY_VARIANT)}::text AS variant,
                   header.publication_contract_id AS root_publication_contract_id
              INTO current_root
              FROM {legacy} AS header
              JOIN {parent} AS parent ON parent.dataset_id = header.dataset_id
              JOIN {endpoint} AS graph_endpoint
                ON graph_endpoint.endpoint_id = {_ql(rooted._ROOTED_ENDPOINT_ID)}
             WHERE header.dataset_id = candidate.root_dataset_id
               AND header.status = 'published' AND header.is_current IS TRUE
               AND parent.status = 'published' AND parent.is_current IS TRUE
             FOR SHARE OF header, parent, graph_endpoint;
        ELSIF candidate.root_dataset_variant = {_ql(rooted._ROOTED_VARIANT)} THEN
            SELECT header.dataset_id, header.endpoint_id, header.source_id,
                   header.source_id AS root_source_id,
                   header.endpoint_id AS root_endpoint_id,
                   header.acquisition_source_id, header.acquisition_endpoint_id,
                   header.practitioner_origin_source_id,
                   header.practitioner_origin_endpoint_id,
                   header.source_authority_id, header.endpoint_signature_sha256,
                   parent.dataset_hash, parent.resource_count,
                   header.practitioner_resource_count,
                   header.root_content_proof_sha256, header.root_cohort_id,
                   header.semantic_projection_as_of, header.operation_key,
                   header.acquisition_root_run_id,
                   {_ql(rooted._ROOTED_VARIANT)}::text AS variant,
                   header.publication_contract_id AS root_publication_contract_id
              INTO current_root
              FROM {rooted_header} AS header
              JOIN {parent} AS parent ON parent.dataset_id = header.dataset_id
             WHERE header.dataset_id = candidate.root_dataset_id
               AND header.status = 'published' AND header.is_current IS TRUE
               AND parent.status = 'published' AND parent.is_current IS TRUE
             FOR SHARE OF header, parent;
        END IF;
        expected_scope_id := 'pdrgs_' || pg_catalog.substr({scope_digest}, 1, 48);
        expected_intent_id := 'pdrgi_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                {canonical}(pg_catalog.jsonb_build_object(
                    'operation_key', NEW.acquisition_operation_key,
                    'operator_contract_sha256',
                        {_ql(_ROOTED_SINGLE_OPERATOR_SHA256)},
                    'reviewed_root_policy', {_ql(_POLICY_SQL_JSON)}::jsonb,
                    'root', {current_root_json},
                    'scope_id', expected_scope_id
                )), 'UTF8')), 'hex'), 1, 48
        );
        expected_run_id := 'pdrgr_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                {canonical}(pg_catalog.jsonb_build_object(
                    'acquisition_role', 'candidate',
                    'dataset_intent_id', expected_intent_id,
                    'operation_key', NEW.acquisition_operation_key,
                    'operator_contract_sha256',
                        {_ql(_ROOTED_SINGLE_OPERATOR_SHA256)}
                )), 'UTF8')), 'hex'), 1, 48
        );
        expected_acquisition_id := 'pdrga_' || pg_catalog.substr(
            {acquisition_digest}, 1, 48
        );
        expected_admission_id := {expected_admission_id};
        IF candidate.acquisition_id IS NULL OR current_root.dataset_id IS NULL
           OR candidate.acquisition_role IS DISTINCT FROM 'candidate'
           OR candidate.status IS DISTINCT FROM 'sealed'
           OR candidate.rooted_graph_complete IS DISTINCT FROM TRUE
           OR candidate.endpoint_collection_complete IS DISTINCT FROM FALSE
           OR candidate.endpoint_complete IS DISTINCT FROM FALSE
           OR candidate.pending_count IS DISTINCT FROM 0
           OR candidate.leased_count IS DISTINCT FROM 0
           OR candidate.error_count IS DISTINCT FROM 0
           OR candidate.sealed_at IS NULL
           OR current_root.dataset_id IS DISTINCT FROM candidate.root_dataset_id
           OR current_root.root_source_id IS DISTINCT FROM candidate.root_source_id
           OR current_root.root_endpoint_id IS DISTINCT FROM candidate.root_endpoint_id
           OR current_root.acquisition_source_id IS DISTINCT FROM
                candidate.acquisition_source_id
           OR current_root.acquisition_endpoint_id IS DISTINCT FROM
                candidate.acquisition_endpoint_id
           OR current_root.source_authority_id IS DISTINCT FROM
                candidate.source_authority_id
           OR current_root.endpoint_signature_sha256 IS DISTINCT FROM
                candidate.endpoint_signature_sha256
           OR current_root.dataset_hash IS DISTINCT FROM candidate.root_dataset_hash
           OR current_root.practitioner_resource_count IS DISTINCT FROM
                candidate.root_resource_count
           OR current_root.root_content_proof_sha256 IS DISTINCT FROM
                candidate.root_content_proof_sha256
           OR current_root.root_cohort_id IS DISTINCT FROM candidate.root_cohort_id
           OR current_root.variant IS DISTINCT FROM candidate.root_dataset_variant
           OR current_root.root_publication_contract_id IS DISTINCT FROM
                candidate.root_publication_contract_id
           OR candidate.scope_id IS DISTINCT FROM expected_scope_id
           OR candidate.dataset_intent_id IS DISTINCT FROM expected_intent_id
           OR candidate.run_id IS DISTINCT FROM expected_run_id
           OR candidate.acquisition_id IS DISTINCT FROM expected_acquisition_id
           OR NEW.admission_id IS DISTINCT FROM expected_admission_id
           OR NEW.admission_contract_id IS DISTINCT FROM
                {_ql(_ROOTED_SINGLE_CONTRACT)}
           OR NEW.attempt_id IS NOT NULL
           OR NEW.comparison_acquisition_id IS NOT NULL
           OR {_policy_predicate('NEW.reviewed_root_policy_json')} IS NOT TRUE
           OR NEW.acquisition_operation_key IS NULL
           OR NEW.acquisition_operation_key !~ '^[0-9a-f]{{64}}$'
           OR ROW(NEW.storage_contract_id, NEW.publication_acquisition_id,
                  NEW.publication_run_id, NEW.dataset_intent_id, NEW.scope_id,
                  NEW.root_source_id, NEW.root_endpoint_id,
                  NEW.acquisition_source_id, NEW.acquisition_endpoint_id,
                  NEW.source_authority_id, NEW.endpoint_signature_sha256,
                  NEW.root_dataset_id, NEW.root_dataset_variant,
                  NEW.root_publication_contract_id, NEW.root_dataset_hash,
                  NEW.root_content_proof_sha256, NEW.root_cohort_id,
                  NEW.root_resource_count, NEW.connector_id,
                  NEW.graph_contract_sha256, NEW.query_contract_sha256,
                  NEW.max_work_items, NEW.max_resource_rows,
                  NEW.max_edge_rows, NEW.max_payload_bytes,
                  NEW.completed_count, NEW.resource_count, NEW.edge_count,
                  NEW.insurance_plan_count, NEW.insurance_plan_page_count,
                  NEW.used_work_items, NEW.used_resource_rows,
                  NEW.used_edge_rows, NEW.used_payload_bytes,
                  NEW.terminal_set_sha256, NEW.resource_set_sha256,
                  NEW.edge_set_sha256, NEW.rooted_graph_sha256)
              IS DISTINCT FROM
              ROW(candidate.storage_contract_id, candidate.acquisition_id,
                  candidate.run_id, candidate.dataset_intent_id,
                  candidate.scope_id, candidate.root_source_id,
                  candidate.root_endpoint_id, candidate.acquisition_source_id,
                  candidate.acquisition_endpoint_id,
                  candidate.source_authority_id,
                  candidate.endpoint_signature_sha256,
                  candidate.root_dataset_id, candidate.root_dataset_variant,
                  candidate.root_publication_contract_id,
                  candidate.root_dataset_hash,
                  candidate.root_content_proof_sha256,
                  candidate.root_cohort_id, candidate.root_resource_count,
                  candidate.connector_id, candidate.graph_contract_sha256,
                  candidate.query_contract_sha256, candidate.max_work_items,
                  candidate.max_resource_rows, candidate.max_edge_rows,
                  candidate.max_payload_bytes, candidate.completed_count,
                  candidate.resource_count, candidate.edge_count,
                  candidate.insurance_plan_count,
                  candidate.insurance_plan_page_count,
                  candidate.used_work_items, candidate.used_resource_rows,
                  candidate.used_edge_rows, candidate.used_payload_bytes,
                  candidate.terminal_set_sha256, candidate.resource_set_sha256,
                  candidate.edge_set_sha256, candidate.rooted_graph_sha256)
           OR NEW.publication_authority IS DISTINCT FROM TRUE
           OR NEW.admitted_at IS DISTINCT FROM transaction_timestamp()
           OR NEW.admitted_at < candidate.sealed_at THEN
            RAISE EXCEPTION
                'provider_directory_rooted_graph_single_root_admission_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _rooted_metadata_sql(header: str, admission: str) -> str:
    legacy = _rooted()._rooted_expected_metadata_sql(header, admission)
    return f"""
        CASE
          WHEN {admission}.admission_contract_id =
               {_ql(_rooted()._TWIN_ADMISSION_CONTRACT)}
          THEN ({legacy})
          WHEN {admission}.admission_contract_id = {_ql(_ROOTED_SINGLE_CONTRACT)}
          THEN (({legacy}) || pg_catalog.jsonb_build_object(
                    'provider_directory_reviewed_root_policy_v1',
                    {admission}.reviewed_root_policy_json,
                    'acquisition_operation_key',
                    {admission}.acquisition_operation_key
                ))
          ELSE NULL::jsonb
        END
    """


def _rooted_intrinsic_valid_function_sql(schema: str) -> str:
    predecessor = _rooted()
    sql = predecessor._rooted_intrinsic_valid_function_sql(schema)
    sql = _replace_once(
        sql,
        predecessor._rooted_expected_metadata_sql("header", "admitted"),
        _rooted_metadata_sql("header", "admitted"),
        "rooted publication metadata",
    )
    sql = _replace_once(
        sql,
        f"JOIN {_qf(schema, predecessor._TWIN_ATTEMPT)} AS comparison\n"
        "            ON comparison.attempt_id = header.attempt_id",
        f"LEFT JOIN {_qf(schema, predecessor._TWIN_ATTEMPT)} AS comparison\n"
        "            ON comparison.attempt_id = header.attempt_id",
        "rooted comparison join",
    )
    sql = _replace_once(
        sql,
        "AND admitted.admission_contract_id = "
        f"{_ql(predecessor._TWIN_ADMISSION_CONTRACT)}",
        "AND admitted.admission_contract_id IN ("
        f"{_ql(predecessor._TWIN_ADMISSION_CONTRACT)}, "
        f"{_ql(_ROOTED_SINGLE_CONTRACT)})",
        "rooted admission selector",
    )
    sql = _replace_once(
        sql,
        "AND admitted.attempt_id = comparison.attempt_id",
        "AND admitted.attempt_id IS NOT DISTINCT FROM header.attempt_id",
        "rooted attempt lineage",
    )
    sql = _replace_once(
        sql,
        "AND admitted.comparison_acquisition_id =\n"
        "               header.comparison_acquisition_id",
        "AND admitted.comparison_acquisition_id IS NOT DISTINCT FROM\n"
        "               header.comparison_acquisition_id",
        "rooted comparison lineage",
    )
    sql = _replace_once(
        sql,
        "AND comparison.matched IS TRUE",
        "AND (((admitted.admission_contract_id = "
        f"{_ql(predecessor._TWIN_ADMISSION_CONTRACT)} AND "
        "header.attempt_id IS NOT NULL AND "
        "header.comparison_acquisition_id IS NOT NULL AND "
        "admitted.reviewed_root_policy_json IS NULL AND "
        "admitted.acquisition_operation_key IS NULL AND "
        "comparison.matched IS TRUE) OR "
        f"(admitted.admission_contract_id = {_ql(_ROOTED_SINGLE_CONTRACT)} AND "
        "header.attempt_id IS NULL AND "
        "header.comparison_acquisition_id IS NULL AND "
        f"{_policy_predicate('admitted.reviewed_root_policy_json')} AND "
        "admitted.acquisition_operation_key ~ '^[0-9a-f]{64}$')))",
        "rooted authority branch",
    )
    return _replace_function(sql, "rooted intrinsic valid function")


def _lock_sql(schema: str) -> str:
    flex_admission = _flex_admission()
    rooted = _rooted()
    relations = (
        flex_admission._ADMISSION,
        flex_admission._ACQUISITION,
        _flex_publication()._HEADER,
        rooted._TWIN_ADMISSION,
        rooted._ACQUISITION,
        rooted._ROOTED_DATASET,
        rooted._DATASET,
        rooted._ENDPOINT,
    )
    return (
        "LOCK TABLE "
        + ", ".join(_qf(schema, relation) for relation in relations)
        + " IN ACCESS EXCLUSIVE MODE;"
    )


def _upgrade_flex(schema: str) -> None:
    predecessor = _flex_admission()
    admission = _qf(schema, predecessor._ADMISSION)
    op.execute(
        f"ALTER TABLE {admission} ADD COLUMN reviewed_root_policy_json jsonb;"
    )
    for column in ("attempt_id", "baseline_acquisition_id", "baseline_run_id"):
        op.execute(f"ALTER TABLE {admission} ALTER COLUMN {_q(column)} DROP NOT NULL;")
    op.execute(
        f"ALTER TABLE {admission} DROP CONSTRAINT "
        f"{_q('pd_uhc_flex_practitioner_twin_admission_check')};"
    )
    op.execute(_flex_check_sql(schema, historical=False))
    op.execute(
        f"ALTER TABLE {admission} VALIDATE CONSTRAINT "
        f"{_q('pd_uhc_flex_practitioner_twin_admission_check')};"
    )
    op.execute(_flex_single_guard_sql(schema))
    op.execute(
        f"REVOKE ALL ON FUNCTION {_qf(schema, _FLEX_SINGLE_GUARD)}() FROM PUBLIC;"
    )
    op.execute(
        f"DROP TRIGGER {_q('pd_uhc_flex_practitioner_admission_insert')} "
        f"ON {admission};"
    )
    op.execute(
        f"CREATE TRIGGER {_q('pd_uhc_flex_practitioner_admission_insert')} "
        f"BEFORE INSERT ON {admission} FOR EACH ROW WHEN "
        f"(NEW.admission_contract_id = {_ql(predecessor._ADMISSION_CONTRACT)}) "
        f"EXECUTE FUNCTION {_qf(schema, predecessor._ADMISSION_INSERT_GUARD)}();"
    )
    op.execute(
        f"CREATE TRIGGER {_q('pd_uhc_flex_practitioner_single_root_admission_insert')} "
        f"BEFORE INSERT ON {admission} FOR EACH ROW WHEN "
        f"(NEW.admission_contract_id = {_ql(_FLEX_SINGLE_CONTRACT)}) "
        f"EXECUTE FUNCTION {_qf(schema, _FLEX_SINGLE_GUARD)}();"
    )
    for trigger in (
        "pd_uhc_flex_practitioner_admission_insert",
        "pd_uhc_flex_practitioner_single_root_admission_insert",
    ):
        op.execute(
            f"ALTER TABLE {admission} ENABLE ALWAYS TRIGGER {_q(trigger)};"
        )
    op.execute(_flex_valid_function_sql(schema))


def _upgrade_rooted(schema: str) -> None:
    predecessor = _rooted()
    admission = _qf(schema, predecessor._TWIN_ADMISSION)
    header = _qf(schema, predecessor._ROOTED_DATASET)
    op.execute(
        f"ALTER TABLE {admission} ADD COLUMN reviewed_root_policy_json jsonb;"
    )
    op.execute(
        f"ALTER TABLE {admission} ADD COLUMN acquisition_operation_key varchar(64);"
    )
    for column in ("attempt_id", "comparison_acquisition_id"):
        op.execute(f"ALTER TABLE {admission} ALTER COLUMN {_q(column)} DROP NOT NULL;")
        op.execute(f"ALTER TABLE {header} ALTER COLUMN {_q(column)} DROP NOT NULL;")
    op.execute(
        f"ALTER TABLE {admission} DROP CONSTRAINT "
        f"{_q('pd_rooted_graph_twin_admission_check')};"
    )
    op.execute(_rooted_check_sql(schema, historical=False))
    op.execute(
        f"ALTER TABLE {admission} VALIDATE CONSTRAINT "
        f"{_q('pd_rooted_graph_twin_admission_check')};"
    )
    op.execute(_rooted_single_guard_sql(schema))
    op.execute(
        f"REVOKE ALL ON FUNCTION {_qf(schema, _ROOTED_SINGLE_GUARD)}() FROM PUBLIC;"
    )
    row_trigger = _q(predecessor._TWIN_ADMISSION + "_row_guard")
    op.execute(f"DROP TRIGGER {row_trigger} ON {admission};")
    op.execute(
        f"CREATE TRIGGER {row_trigger} BEFORE UPDATE OR DELETE ON {admission} "
        f"FOR EACH ROW EXECUTE FUNCTION "
        f"{_qf(schema, predecessor._TWIN_ADMISSION_GUARD)}();"
    )
    op.execute(
        f"CREATE TRIGGER {_q(_ROOTED_LEGACY_INSERT_TRIGGER)} BEFORE INSERT ON "
        f"{admission} FOR EACH ROW WHEN (NEW.admission_contract_id = "
        f"{_ql(predecessor._TWIN_ADMISSION_CONTRACT)}) EXECUTE FUNCTION "
        f"{_qf(schema, predecessor._TWIN_ADMISSION_GUARD)}();"
    )
    op.execute(
        f"CREATE TRIGGER {_q(_ROOTED_SINGLE_INSERT_TRIGGER)} BEFORE INSERT ON "
        f"{admission} FOR EACH ROW WHEN (NEW.admission_contract_id = "
        f"{_ql(_ROOTED_SINGLE_CONTRACT)}) EXECUTE FUNCTION "
        f"{_qf(schema, _ROOTED_SINGLE_GUARD)}();"
    )
    for trigger in (
        predecessor._TWIN_ADMISSION + "_row_guard",
        _ROOTED_LEGACY_INSERT_TRIGGER,
        _ROOTED_SINGLE_INSERT_TRIGGER,
    ):
        op.execute(
            f"ALTER TABLE {admission} ENABLE ALWAYS TRIGGER {_q(trigger)};"
        )
    op.execute(_rooted_intrinsic_valid_function_sql(schema))


def upgrade() -> None:
    schema = _rooted()._schema()
    op.execute(_lock_sql(schema))
    _upgrade_flex(schema)
    _upgrade_rooted(schema)


def _downgrade_fence_sql(schema: str) -> str:
    flex = _flex_admission()
    rooted = _rooted()
    return f"""
    DO $downgrade$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {_qf(schema, flex._ADMISSION)}
             WHERE admission_contract_id = {_ql(_FLEX_SINGLE_CONTRACT)}
                OR reviewed_root_policy_json IS NOT NULL
        ) OR EXISTS (
            SELECT 1 FROM {_qf(schema, rooted._TWIN_ADMISSION)}
             WHERE admission_contract_id = {_ql(_ROOTED_SINGLE_CONTRACT)}
                OR reviewed_root_policy_json IS NOT NULL
                OR acquisition_operation_key IS NOT NULL
        ) OR EXISTS (
            SELECT 1 FROM {_qf(schema, rooted._ROOTED_DATASET)}
             WHERE attempt_id IS NULL OR comparison_acquisition_id IS NULL
        ) THEN
            RAISE EXCEPTION
                'provider_directory_specialized_single_root_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $downgrade$;
    """


def _downgrade_rooted(schema: str) -> None:
    predecessor = _rooted()
    admission = _qf(schema, predecessor._TWIN_ADMISSION)
    header = _qf(schema, predecessor._ROOTED_DATASET)
    for trigger in (
        _ROOTED_SINGLE_INSERT_TRIGGER,
        _ROOTED_LEGACY_INSERT_TRIGGER,
        predecessor._TWIN_ADMISSION + "_row_guard",
    ):
        op.execute(f"DROP TRIGGER {_q(trigger)} ON {admission};")
    op.execute(f"DROP FUNCTION {_qf(schema, _ROOTED_SINGLE_GUARD)}();")
    op.execute(
        f"CREATE TRIGGER {_q(predecessor._TWIN_ADMISSION + '_row_guard')} "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {admission} FOR EACH ROW "
        f"EXECUTE FUNCTION {_qf(schema, predecessor._TWIN_ADMISSION_GUARD)}();"
    )
    op.execute(
        f"ALTER TABLE {admission} ENABLE ALWAYS TRIGGER "
        f"{_q(predecessor._TWIN_ADMISSION + '_row_guard')};"
    )
    op.execute(
        _replace_function(
            predecessor._rooted_intrinsic_valid_function_sql(schema),
            "rooted downgrade intrinsic function",
        )
    )
    op.execute(
        f"ALTER TABLE {admission} DROP CONSTRAINT "
        f"{_q('pd_rooted_graph_twin_admission_check')};"
    )
    op.execute(_rooted_check_sql(schema, historical=True))
    op.execute(
        f"ALTER TABLE {admission} VALIDATE CONSTRAINT "
        f"{_q('pd_rooted_graph_twin_admission_check')};"
    )
    for column in ("attempt_id", "comparison_acquisition_id"):
        op.execute(f"ALTER TABLE {header} ALTER COLUMN {_q(column)} SET NOT NULL;")
        op.execute(f"ALTER TABLE {admission} ALTER COLUMN {_q(column)} SET NOT NULL;")
    op.execute(
        f"ALTER TABLE {admission} DROP COLUMN acquisition_operation_key;"
    )
    op.execute(
        f"ALTER TABLE {admission} DROP COLUMN reviewed_root_policy_json;"
    )


def _downgrade_flex(schema: str) -> None:
    predecessor = _flex_admission()
    admission = _qf(schema, predecessor._ADMISSION)
    for trigger in (
        "pd_uhc_flex_practitioner_single_root_admission_insert",
        "pd_uhc_flex_practitioner_admission_insert",
    ):
        op.execute(f"DROP TRIGGER {_q(trigger)} ON {admission};")
    op.execute(f"DROP FUNCTION {_qf(schema, _FLEX_SINGLE_GUARD)}();")
    op.execute(
        f"CREATE TRIGGER {_q('pd_uhc_flex_practitioner_admission_insert')} "
        f"BEFORE INSERT ON {admission} FOR EACH ROW EXECUTE FUNCTION "
        f"{_qf(schema, predecessor._ADMISSION_INSERT_GUARD)}();"
    )
    op.execute(
        f"ALTER TABLE {admission} ENABLE ALWAYS TRIGGER "
        f"{_q('pd_uhc_flex_practitioner_admission_insert')};"
    )
    op.execute(
        _replace_function(
            _flex_publication()._valid_function_sql(schema),
            "Flex downgrade valid function",
        )
    )
    op.execute(
        f"ALTER TABLE {admission} DROP CONSTRAINT "
        f"{_q('pd_uhc_flex_practitioner_twin_admission_check')};"
    )
    op.execute(_flex_check_sql(schema, historical=True))
    op.execute(
        f"ALTER TABLE {admission} VALIDATE CONSTRAINT "
        f"{_q('pd_uhc_flex_practitioner_twin_admission_check')};"
    )
    for column in ("attempt_id", "baseline_acquisition_id", "baseline_run_id"):
        op.execute(f"ALTER TABLE {admission} ALTER COLUMN {_q(column)} SET NOT NULL;")
    op.execute(
        f"ALTER TABLE {admission} DROP COLUMN reviewed_root_policy_json;"
    )


def downgrade() -> None:
    schema = _rooted()._schema()
    op.execute(_lock_sql(schema))
    op.execute(_downgrade_fence_sql(schema))
    _downgrade_rooted(schema)
    _downgrade_flex(schema)
