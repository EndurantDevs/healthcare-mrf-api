# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guard exact-cohort Flex Practitioner dataset publication.

Revision ID: 20260810080000_provider_directory_uhc_flex_practitioner_publication
Revises: 20260810070000_provider_directory_uhc_flex_practitioner_twin_admission

The shared endpoint dataset remains the Profile-facing resource container.  A
small companion ledger is required because the generic endpoint-completion
guards intentionally do not treat an exact NPI cohort as endpoint completion.
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import create_table_or_validate
from db.migration_index_adoption import create_index_if_missing


revision = "20260810080000_provider_directory_uhc_flex_practitioner_publication"
down_revision = (
    "20260810070000_provider_directory_uhc_flex_practitioner_twin_admission"
)
branch_labels = None
depends_on = None


_HEADER = "provider_directory_uhc_flex_practitioner_dataset"
_PROVENANCE = "provider_directory_uhc_flex_practitioner_dataset_resource"
_ENDPOINT_DATASET = "provider_directory_endpoint_dataset"
_DATASET_RESOURCE = "provider_directory_dataset_resource"
_ADMISSION = "provider_directory_uhc_flex_practitioner_twin_admission"
_ACQUISITION = "provider_directory_uhc_flex_practitioner_acquisition"
_WORK = "provider_directory_uhc_flex_practitioner_work"
_RAW_RESOURCE = "provider_directory_uhc_flex_practitioner_resource"
_SOURCE = "provider_directory_source"
_ENDPOINT = "provider_directory_api_endpoint"

_PUBLICATION_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1"
)
_ROOT_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-root.v1"
)
_ADMISSION_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-matched-admission.v1"
)
_SOURCE_ID = "pdfhir_1ceb7c0986c320b7eb924881"
_ENDPOINT_ID = (
    "ad53a7446514ed65b3a8ea7ab68ceb9a1ef85bf6c04fcb882219ecb50928bab5"
)
_CONNECTOR_ID = (
    "pdufpc_16ebdbf260dc9815ae38830a6991fea5d6533ab8db7389da"
)
_QUERY_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-exact-npi.v1"
)
_STORAGE_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-acquisition.v1"
)
_SOURCE_AUTHORITY = "unitedhealthcare"
_API_BASE = "https://flex.optum.com/fhirpublic/R4"
_RESOURCE_TYPE = "Practitioner"
_HASH_CONTRACT = "semantic_content_v3"

_VALID_FUNCTION = "provider_directory_uhc_flex_practitioner_dataset_valid"
_READY_FUNCTION = "provider_directory_uhc_flex_practitioner_dataset_ready"
_HEADER_GUARD = "guard_pd_uhc_flex_practitioner_dataset"
_PROVENANCE_GUARD = "guard_pd_uhc_flex_practitioner_dataset_resource"
_PARENT_GUARD = "guard_pd_uhc_flex_practitioner_dataset_parent"
_SOURCE_GUARD = "guard_pd_uhc_flex_practitioner_dataset_source"
_ENDPOINT_GUARD = "guard_pd_uhc_flex_practitioner_dataset_endpoint"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _qf(schema: str, relation: str) -> str:
    return f"{_q(schema)}.{_q(relation)}"


def _digest_identifier_sql(prefix: str, contract: str, fields: tuple[str, ...]) -> str:
    components = ", ".join((_ql(contract), *fields))
    return f"""
        {_ql(prefix)} || pg_catalog.substr(
            pg_catalog.encode(
                pg_catalog.sha256(
                    pg_catalog.convert_to(
                        pg_catalog.concat_ws(pg_catalog.chr(31), {components}),
                        'UTF8'
                    )
                ),
                'hex'
            ),
            1,
            48
        )
    """


def _expected_dataset_id_sql(header: str, admission: str) -> str:
    return _digest_identifier_sql(
        "pdufpd_",
        _PUBLICATION_CONTRACT,
        (
            f"{header}.admission_id",
            f"{header}.candidate_acquisition_id",
            f"{header}.cohort_id",
            f"{header}.dataset_intent_id",
            f"{header}.source_id",
            f"{header}.endpoint_id",
            f"{header}.semantic_projection_as_of::text",
            f"{header}.operation_key",
            f"{header}.terminal_set_sha256",
            f"{admission}.resource_count::text",
        ),
    )


def _expected_root_id_sql(header: str, admission: str) -> str:
    return _digest_identifier_sql(
        "pdufpar_",
        _ROOT_CONTRACT,
        (
            f"{header}.admission_id",
            f"{header}.candidate_acquisition_id",
            f"{header}.cohort_id",
            f"{header}.dataset_intent_id",
            f"{header}.semantic_projection_as_of::text",
            f"{header}.operation_key",
            f"{header}.terminal_set_sha256",
            f"{admission}.resource_count::text",
        ),
    )


def _metadata_sql(header: str, admission: str) -> str:
    return f"""
        pg_catalog.jsonb_build_object(
            'acquisition_root_run_id', {header}.acquisition_root_run_id,
            'admission_contract_id', {admission}.admission_contract_id,
            'admission_id', {header}.admission_id,
            'baseline_acquisition_id', {admission}.baseline_acquisition_id,
            'baseline_run_id', {admission}.baseline_run_id,
            'candidate_acquisition_id', {header}.candidate_acquisition_id,
            'candidate_run_id', {admission}.candidate_run_id,
            'cohort_complete', true,
            'cohort_id', {header}.cohort_id,
            'connector_id', {admission}.connector_id,
            'dataset_id', {header}.dataset_id,
            'dataset_intent_id', {header}.dataset_intent_id,
            'endpoint_collection_complete', false,
            'endpoint_complete', false,
            'endpoint_id', {header}.endpoint_id,
            'expected_npi_count', {admission}.expected_npi_count,
            'expected_resources', pg_catalog.jsonb_build_array({_ql(_RESOURCE_TYPE)}),
            'operation_key', {header}.operation_key,
            'publication_contract_id', {header}.publication_contract_id,
            'query_contract_id', {admission}.query_contract_id,
            'resource_counts', pg_catalog.jsonb_build_object(
                {_ql(_RESOURCE_TYPE)}, {header}.resource_count
            ),
            'resource_hash_contract', {header}.resource_hash_contract,
            'selected_resources', pg_catalog.jsonb_build_array({_ql(_RESOURCE_TYPE)}),
            'semantic_projection_as_of', {header}.semantic_projection_as_of::text,
            'source_authority_id', {header}.source_authority_id,
            'source_id', {header}.source_id,
            'source_ids', pg_catalog.jsonb_build_array({header}.source_id),
            'storage_contract_id', {admission}.storage_contract_id,
            'terminal_set_sha256', {header}.terminal_set_sha256
        )
    """


def _dataset_hash_sql(resource_ref: str, dataset_id_sql: str) -> str:
    identity = (
        "'[\"' || resource.resource_type || '\",\"' || "
        "resource.resource_id || '\",\"' || resource.payload_hash || '\"]'"
    )
    return f"""
        SELECT pg_catalog.encode(
                   pg_catalog.sha256(
                       pg_catalog.convert_to(
                           COALESCE(
                               pg_catalog.string_agg(
                                   {identity},
                                   E'\\n' ORDER BY
                                       resource.resource_type,
                                       resource.resource_id
                               ),
                               ''
                           ),
                           'UTF8'
                       )
                   ),
                   'hex'
               )
          FROM {resource_ref} AS resource
         WHERE resource.dataset_id = {dataset_id_sql}
    """


def _valid_function_sql(schema: str) -> str:
    header_ref = _qf(schema, _HEADER)
    provenance_ref = _qf(schema, _PROVENANCE)
    endpoint_dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    dataset_resource_ref = _qf(schema, _DATASET_RESOURCE)
    admission_ref = _qf(schema, _ADMISSION)
    acquisition_ref = _qf(schema, _ACQUISITION)
    work_ref = _qf(schema, _WORK)
    raw_resource_ref = _qf(schema, _RAW_RESOURCE)
    source_ref = _qf(schema, _SOURCE)
    endpoint_ref = _qf(schema, _ENDPOINT)
    valid_ref = _qf(schema, _VALID_FUNCTION)
    expected_dataset_id = _expected_dataset_id_sql("header", "admission")
    expected_root_id = _expected_root_id_sql("header", "admission")
    expected_metadata = _metadata_sql("header", "admission")
    dataset_hash = _dataset_hash_sql(
        dataset_resource_ref,
        "header.dataset_id",
    )
    return f"""
    CREATE OR REPLACE FUNCTION {valid_ref}(candidate_dataset_id text)
    RETURNS boolean
    LANGUAGE sql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
        SELECT pg_catalog.count(*) = 1
          FROM {header_ref} AS header
          JOIN {endpoint_dataset_ref} AS endpoint_dataset
            ON endpoint_dataset.dataset_id = header.dataset_id
          JOIN {admission_ref} AS admission
            ON admission.admission_id = header.admission_id
           AND admission.candidate_acquisition_id =
               header.candidate_acquisition_id
          JOIN {acquisition_ref} AS candidate
            ON candidate.acquisition_id = header.candidate_acquisition_id
          JOIN {source_ref} AS source
            ON source.source_id = header.source_id
          JOIN {endpoint_ref} AS endpoint
            ON endpoint.endpoint_id = header.endpoint_id
         WHERE header.dataset_id = candidate_dataset_id
           AND header.publication_contract_id = {_ql(_PUBLICATION_CONTRACT)}
           AND header.dataset_id = ({expected_dataset_id})
           AND header.acquisition_root_run_id = ({expected_root_id})
           AND header.source_id = {_ql(_SOURCE_ID)}
           AND header.endpoint_id = {_ql(_ENDPOINT_ID)}
           AND header.source_authority_id = {_ql(_SOURCE_AUTHORITY)}
           AND header.resource_hash_contract = {_ql(_HASH_CONTRACT)}
           AND header.selected_resource_type = {_ql(_RESOURCE_TYPE)}
           AND header.expected_resource_type = {_ql(_RESOURCE_TYPE)}
           AND header.cohort_complete IS TRUE
           AND header.endpoint_collection_complete IS FALSE
           AND header.endpoint_complete IS FALSE
           AND admission.admission_contract_id = {_ql(_ADMISSION_CONTRACT)}
           AND admission.publication_authority IS TRUE
           AND admission.source_id = header.source_id
           AND admission.connector_id = {_ql(_CONNECTOR_ID)}
           AND admission.query_contract_id = {_ql(_QUERY_CONTRACT)}
           AND admission.storage_contract_id = {_ql(_STORAGE_CONTRACT)}
           AND admission.cohort_id = header.cohort_id
           AND admission.dataset_intent_id = header.dataset_intent_id
           AND admission.semantic_projection_as_of =
               header.semantic_projection_as_of
           AND admission.operation_key = header.operation_key
           AND admission.terminal_set_sha256 = header.terminal_set_sha256
           AND admission.resource_count = header.resource_count
           AND candidate.status = 'sealed'
           AND candidate.acquisition_role = 'candidate'
           AND candidate.cohort_complete IS TRUE
           AND candidate.endpoint_collection_complete IS FALSE
           AND candidate.endpoint_complete IS FALSE
           AND candidate.pending_count = 0
           AND candidate.leased_count = 0
           AND candidate.error_count = 0
           AND candidate.matched_count + candidate.unmatched_count =
               candidate.expected_npi_count
           AND candidate.cohort_id = header.cohort_id
           AND candidate.dataset_intent_id = header.dataset_intent_id
           AND candidate.run_id = admission.candidate_run_id
           AND candidate.resource_count = header.resource_count
           AND candidate.terminal_set_sha256 = header.terminal_set_sha256
           AND endpoint_dataset.endpoint_id = header.endpoint_id
           AND endpoint_dataset.import_run_id = admission.candidate_run_id
           AND endpoint_dataset.acquisition_root_run_id =
               header.acquisition_root_run_id
           AND endpoint_dataset.previous_dataset_id IS NOT DISTINCT FROM
               header.previous_dataset_id
           AND endpoint_dataset.dataset_hash IS NOT DISTINCT FROM
               header.dataset_hash
           AND endpoint_dataset.status = header.status
           AND endpoint_dataset.is_current = header.is_current
           AND endpoint_dataset.resource_count = header.resource_count
           AND endpoint_dataset.created_at = header.created_at
           AND endpoint_dataset.validated_at IS NOT DISTINCT FROM
               header.validated_at
           AND endpoint_dataset.published_at IS NOT DISTINCT FROM
               header.published_at
           AND endpoint_dataset.superseded_at IS NOT DISTINCT FROM
               header.superseded_at
           AND endpoint_dataset.completion_proof_required_version IS NULL
           AND endpoint_dataset.completion_proof_json IS NULL
           AND endpoint_dataset.completion_proof_sha256 IS NULL
           AND endpoint_dataset.publication_metadata_json::jsonb =
               ({expected_metadata})
           AND source.endpoint_id = header.endpoint_id
           AND source.canonical_api_base = {_ql(_API_BASE)}
           AND source.requires_registration IS FALSE
           AND source.requires_api_key IS FALSE
           AND source.auth_type = 'none'
           AND source.metadata_json::jsonb
                   ->> 'provider_directory_authority_id' =
               header.source_authority_id
           AND source.metadata_json::jsonb
                   ->> 'provider_directory_connector_id' = {_ql(_CONNECTOR_ID)}
           AND source.metadata_json::jsonb
                   ->> 'provider_directory_query_contract_id' =
               {_ql(_QUERY_CONTRACT)}
           AND source.metadata_json::jsonb
                   -> 'provider_directory_resource_types' =
               pg_catalog.jsonb_build_array({_ql(_RESOURCE_TYPE)})
           AND source.metadata_json::jsonb
                   -> 'provider_directory_acquisition_enabled' = 'false'::jsonb
           AND source.metadata_json::jsonb
                   ->> 'provider_directory_acquisition_mode' = 'manual'
           AND source.metadata_json::jsonb
                   -> 'provider_directory_endpoint_collection_complete' =
               'false'::jsonb
           AND source.metadata_json::jsonb
                   -> 'provider_directory_endpoint_complete' = 'false'::jsonb
           AND endpoint.canonical_api_base = {_ql(_API_BASE)}
           AND endpoint.metadata_json::jsonb ->> 'authority_id' =
               header.source_authority_id
           AND endpoint.metadata_json::jsonb -> 'resource_types' =
               pg_catalog.jsonb_build_array({_ql(_RESOURCE_TYPE)})
           AND (
                header.status = 'building'
                OR (
                    header.status IN ('validated', 'published', 'superseded')
                    AND (
                        SELECT pg_catalog.count(*)
                          FROM {dataset_resource_ref} AS resource
                         WHERE resource.dataset_id = header.dataset_id
                    ) = header.resource_count
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {dataset_resource_ref} AS resource
                         WHERE resource.dataset_id = header.dataset_id
                           AND resource.resource_type <> {_ql(_RESOURCE_TYPE)}
                    )
                    AND ({dataset_hash}) = header.dataset_hash
                    AND (
                        SELECT pg_catalog.count(*)
                          FROM {provenance_ref} AS provenance
                         WHERE provenance.dataset_id = header.dataset_id
                    ) = header.resource_count
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {provenance_ref} AS provenance
                          LEFT JOIN {dataset_resource_ref} AS resource
                            ON resource.dataset_id = provenance.dataset_id
                           AND resource.resource_type = provenance.resource_type
                           AND resource.resource_id = provenance.resource_id
                          LEFT JOIN {raw_resource_ref} AS raw_resource
                            ON raw_resource.acquisition_id =
                               provenance.candidate_acquisition_id
                           AND raw_resource.npi = provenance.requested_npi
                           AND raw_resource.resource_id = provenance.resource_id
                           AND raw_resource.payload_sha256 =
                               provenance.acquired_resource_sha256
                          LEFT JOIN {work_ref} AS work
                            ON work.acquisition_id = raw_resource.acquisition_id
                           AND work.cohort_id = raw_resource.cohort_id
                           AND work.npi = raw_resource.npi
                           AND work.attempt_count = raw_resource.attempt
                           AND work.status = 'matched'
                         WHERE provenance.dataset_id = header.dataset_id
                           AND (
                                provenance.resource_type <>
                                    {_ql(_RESOURCE_TYPE)}
                                OR provenance.candidate_acquisition_id <>
                                    header.candidate_acquisition_id
                                OR resource.dataset_id IS NULL
                                OR resource.payload_hash <>
                                    provenance.payload_hash
                                OR resource.acquired_resource_sha256 IS NOT NULL
                                OR raw_resource.acquisition_id IS NULL
                                OR work.acquisition_id IS NULL
                           )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {raw_resource_ref} AS raw_resource
                          JOIN {work_ref} AS work
                            ON work.acquisition_id = raw_resource.acquisition_id
                           AND work.cohort_id = raw_resource.cohort_id
                           AND work.npi = raw_resource.npi
                           AND work.attempt_count = raw_resource.attempt
                           AND work.status = 'matched'
                          LEFT JOIN {provenance_ref} AS provenance
                            ON provenance.dataset_id = header.dataset_id
                           AND provenance.candidate_acquisition_id =
                               raw_resource.acquisition_id
                           AND provenance.requested_npi = raw_resource.npi
                           AND provenance.resource_id = raw_resource.resource_id
                           AND provenance.acquired_resource_sha256 =
                               raw_resource.payload_sha256
                         WHERE raw_resource.acquisition_id =
                               header.candidate_acquisition_id
                           AND provenance.dataset_id IS NULL
                    )
                )
           );
    $function$;
    """


def _ready_function_sql(schema: str) -> str:
    header_ref = _qf(schema, _HEADER)
    valid_ref = _qf(schema, _VALID_FUNCTION)
    ready_ref = _qf(schema, _READY_FUNCTION)
    return f"""
    CREATE OR REPLACE FUNCTION {ready_ref}(candidate_dataset_id text)
    RETURNS boolean
    LANGUAGE sql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
        SELECT EXISTS (
            SELECT 1
              FROM {header_ref} AS header
             WHERE header.dataset_id = candidate_dataset_id
               AND header.status = 'published'
               AND header.is_current IS TRUE
               AND {valid_ref}(header.dataset_id)
        );
    $function$;
    """


def _header_guard_sql(schema: str) -> str:
    valid_ref = _qf(schema, _VALID_FUNCTION)
    guard_ref = _qf(schema, _HEADER_GUARD)
    immutable = (
        "ROW(NEW.dataset_id, NEW.publication_contract_id, NEW.admission_id, "
        "NEW.candidate_acquisition_id, NEW.source_id, NEW.endpoint_id, "
        "NEW.cohort_id, NEW.dataset_intent_id, NEW.acquisition_root_run_id, "
        "NEW.semantic_projection_as_of, NEW.operation_key, "
        "NEW.source_authority_id, NEW.terminal_set_sha256, "
        "NEW.previous_dataset_id, NEW.resource_count, "
        "NEW.resource_hash_contract, NEW.selected_resource_type, "
        "NEW.expected_resource_type, NEW.cohort_complete, "
        "NEW.endpoint_collection_complete, NEW.endpoint_complete, "
        "NEW.created_at) IS DISTINCT FROM "
        "ROW(OLD.dataset_id, OLD.publication_contract_id, OLD.admission_id, "
        "OLD.candidate_acquisition_id, OLD.source_id, OLD.endpoint_id, "
        "OLD.cohort_id, OLD.dataset_intent_id, OLD.acquisition_root_run_id, "
        "OLD.semantic_projection_as_of, OLD.operation_key, "
        "OLD.source_authority_id, OLD.terminal_set_sha256, "
        "OLD.previous_dataset_id, OLD.resource_count, "
        "OLD.resource_hash_contract, OLD.selected_resource_type, "
        "OLD.expected_resource_type, OLD.cohort_complete, "
        "OLD.endpoint_collection_complete, OLD.endpoint_complete, "
        "OLD.created_at)"
    )
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION
                'provider_directory_uhc_flex_practitioner_dataset_truncate_forbidden'
                USING ERRCODE = '55000';
        ELSIF TG_WHEN = 'AFTER' THEN
            IF {valid_ref}(NEW.dataset_id) IS DISTINCT FROM TRUE THEN
                RAISE EXCEPTION
                    'provider_directory_uhc_flex_practitioner_dataset_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        ELSIF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION
                'provider_directory_uhc_flex_practitioner_dataset_delete_forbidden'
                USING ERRCODE = '55000';
        ELSIF TG_OP = 'INSERT' THEN
            IF NEW.status <> 'building' OR NEW.is_current IS NOT FALSE
               OR NEW.dataset_hash IS NOT NULL OR NEW.validated_at IS NOT NULL
               OR NEW.published_at IS NOT NULL OR NEW.superseded_at IS NOT NULL
            THEN
                RAISE EXCEPTION
                    'provider_directory_uhc_flex_practitioner_dataset_insert_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;

        IF ({immutable})
           OR (OLD.dataset_hash IS NOT NULL AND
               NEW.dataset_hash IS DISTINCT FROM OLD.dataset_hash)
           OR NOT (
                (NEW.status = OLD.status
                 AND NEW.is_current IS NOT DISTINCT FROM OLD.is_current
                 AND NEW.dataset_hash IS NOT DISTINCT FROM OLD.dataset_hash
                 AND NEW.validated_at IS NOT DISTINCT FROM OLD.validated_at
                 AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
                 AND NEW.superseded_at IS NOT DISTINCT FROM OLD.superseded_at)
                OR (OLD.status = 'building' AND NEW.status = 'validated'
                    AND OLD.dataset_hash IS NULL
                    AND NEW.dataset_hash ~ '^[0-9a-f]{{64}}$'
                    AND NEW.is_current IS FALSE
                    AND NEW.validated_at IS NOT DISTINCT FROM
                        transaction_timestamp()
                    AND NEW.published_at IS NULL
                    AND NEW.superseded_at IS NULL)
                OR (OLD.status = 'validated' AND NEW.status = 'published'
                    AND NEW.dataset_hash = OLD.dataset_hash
                    AND NEW.is_current IS TRUE
                    AND NEW.validated_at = OLD.validated_at
                    AND NEW.published_at IS NOT DISTINCT FROM
                        transaction_timestamp()
                    AND NEW.superseded_at IS NULL)
                OR (OLD.status = 'published' AND NEW.status = 'superseded'
                    AND NEW.dataset_hash = OLD.dataset_hash
                    AND NEW.is_current IS FALSE
                    AND NEW.validated_at = OLD.validated_at
                    AND NEW.published_at = OLD.published_at
                    AND NEW.superseded_at IS NOT DISTINCT FROM
                        transaction_timestamp())
           ) THEN
            RAISE EXCEPTION
                'provider_directory_uhc_flex_practitioner_dataset_transition_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _provenance_guard_sql(schema: str) -> str:
    header_ref = _qf(schema, _HEADER)
    guard_ref = _qf(schema, _PROVENANCE_GUARD)
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION
                'provider_directory_uhc_flex_practitioner_resource_truncate_forbidden'
                USING ERRCODE = '55000';
        ELSIF TG_OP <> 'INSERT' THEN
            RAISE EXCEPTION
                'provider_directory_uhc_flex_practitioner_resource_immutable'
                USING ERRCODE = '55000';
        ELSIF NOT EXISTS (
            SELECT 1
              FROM {header_ref} AS header
             WHERE header.dataset_id = NEW.dataset_id
               AND header.status = 'building'
               AND header.is_current IS FALSE
               AND header.candidate_acquisition_id =
                   NEW.candidate_acquisition_id
        ) THEN
            RAISE EXCEPTION
                'provider_directory_uhc_flex_practitioner_resource_parent_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _parent_guard_sql(schema: str) -> str:
    header_ref = _qf(schema, _HEADER)
    valid_ref = _qf(schema, _VALID_FUNCTION)
    guard_ref = _qf(schema, _PARENT_GUARD)
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        target_dataset_id text;
    BEGIN
        target_dataset_id := CASE WHEN TG_OP = 'DELETE'
                                  THEN OLD.dataset_id ELSE NEW.dataset_id END;
        IF EXISTS (
            SELECT 1 FROM {header_ref} AS header
             WHERE header.dataset_id = target_dataset_id
        ) AND {valid_ref}(target_dataset_id) IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION
                'provider_directory_uhc_flex_practitioner_parent_drift'
                USING ERRCODE = '55000';
        END IF;
        RETURN NULL;
    END;
    $function$;
    """


def _source_guard_sql(schema: str) -> str:
    header_ref = _qf(schema, _HEADER)
    valid_ref = _qf(schema, _VALID_FUNCTION)
    guard_ref = _qf(schema, _SOURCE_GUARD)
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        target_source_id text;
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (SELECT 1 FROM {header_ref}) THEN
                RAISE EXCEPTION
                    'provider_directory_uhc_flex_practitioner_source_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        target_source_id := CASE WHEN TG_OP = 'DELETE'
                                 THEN OLD.source_id ELSE NEW.source_id END;
        IF EXISTS (
            SELECT 1
              FROM {header_ref} AS header
             WHERE header.source_id = target_source_id
               AND header.status <> 'building'
               AND {valid_ref}(header.dataset_id) IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION
                'provider_directory_uhc_flex_practitioner_source_drift'
                USING ERRCODE = '55000';
        END IF;
        RETURN NULL;
    END;
    $function$;
    """


def _endpoint_guard_sql(schema: str) -> str:
    header_ref = _qf(schema, _HEADER)
    valid_ref = _qf(schema, _VALID_FUNCTION)
    guard_ref = _qf(schema, _ENDPOINT_GUARD)
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        target_endpoint_id text;
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (SELECT 1 FROM {header_ref}) THEN
                RAISE EXCEPTION
                    'provider_directory_uhc_flex_practitioner_endpoint_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        target_endpoint_id := CASE WHEN TG_OP = 'DELETE'
                                   THEN OLD.endpoint_id ELSE NEW.endpoint_id END;
        IF EXISTS (
            SELECT 1
              FROM {header_ref} AS header
             WHERE header.endpoint_id = target_endpoint_id
               AND header.status <> 'building'
               AND {valid_ref}(header.dataset_id) IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION
                'provider_directory_uhc_flex_practitioner_endpoint_drift'
                USING ERRCODE = '55000';
        END IF;
        RETURN NULL;
    END;
    $function$;
    """


def _create_tables(schema: str) -> None:
    create_table_or_validate(
        op,
        _HEADER,
        sa.Column("dataset_id", sa.String(55), nullable=False),
        sa.Column("publication_contract_id", sa.String(96), nullable=False),
        sa.Column("admission_id", sa.String(56), nullable=False),
        sa.Column("candidate_acquisition_id", sa.String(55), nullable=False),
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("endpoint_id", sa.String(64), nullable=False),
        sa.Column("cohort_id", sa.String(54), nullable=False),
        sa.Column("dataset_intent_id", sa.String(55), nullable=False),
        sa.Column("acquisition_root_run_id", sa.String(56), nullable=False),
        sa.Column("semantic_projection_as_of", sa.Date(), nullable=False),
        sa.Column("operation_key", sa.String(64), nullable=False),
        sa.Column("source_authority_id", sa.String(64), nullable=False),
        sa.Column("terminal_set_sha256", sa.String(64), nullable=False),
        sa.Column("previous_dataset_id", sa.String(55), nullable=True),
        sa.Column("dataset_hash", sa.String(64), nullable=True),
        sa.Column("resource_count", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("resource_hash_contract", sa.String(32), nullable=False),
        sa.Column("selected_resource_type", sa.String(64), nullable=False),
        sa.Column("expected_resource_type", sa.String(64), nullable=False),
        sa.Column("cohort_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_collection_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_complete", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("is_current", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("transaction_timestamp()"), nullable=False),
        sa.Column("validated_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("published_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("superseded_at", sa.TIMESTAMP(), nullable=True),
        sa.PrimaryKeyConstraint("dataset_id", name="pd_uhc_flex_practitioner_dataset_pkey"),
        sa.UniqueConstraint("admission_id", name="pd_uhc_flex_practitioner_dataset_admission_key"),
        sa.UniqueConstraint("candidate_acquisition_id", name="pd_uhc_flex_practitioner_dataset_candidate_key"),
        sa.UniqueConstraint("acquisition_root_run_id", name="pd_uhc_flex_practitioner_dataset_root_key"),
        sa.ForeignKeyConstraint(["dataset_id"], [f"{schema}.{_ENDPOINT_DATASET}.dataset_id"], name="pd_uhc_flex_practitioner_dataset_parent_fkey"),
        sa.ForeignKeyConstraint(["admission_id"], [f"{schema}.{_ADMISSION}.admission_id"], name="pd_uhc_flex_practitioner_dataset_admission_fkey"),
        sa.ForeignKeyConstraint(["candidate_acquisition_id"], [f"{schema}.{_ADMISSION}.candidate_acquisition_id"], name="pd_uhc_flex_practitioner_dataset_candidate_fkey"),
        sa.ForeignKeyConstraint(["source_id"], [f"{schema}.{_SOURCE}.source_id"], name="pd_uhc_flex_practitioner_dataset_source_fkey"),
        sa.ForeignKeyConstraint(["endpoint_id"], [f"{schema}.{_ENDPOINT}.endpoint_id"], name="pd_uhc_flex_practitioner_dataset_endpoint_fkey"),
        sa.ForeignKeyConstraint(["previous_dataset_id"], [f"{schema}.{_HEADER}.dataset_id"], name="pd_uhc_flex_practitioner_dataset_previous_fkey"),
        sa.CheckConstraint(
            f"publication_contract_id = {_ql(_PUBLICATION_CONTRACT)} AND dataset_id ~ '^pdufpd_[0-9a-f]{{48}}$' AND acquisition_root_run_id ~ '^pdufpar_[0-9a-f]{{48}}$' AND operation_key ~ '^[0-9a-f]{{64}}$' AND terminal_set_sha256 ~ '^[0-9a-f]{{64}}$' AND resource_hash_contract = {_ql(_HASH_CONTRACT)} AND selected_resource_type = {_ql(_RESOURCE_TYPE)} AND expected_resource_type = {_ql(_RESOURCE_TYPE)} AND cohort_complete IS TRUE AND endpoint_collection_complete IS FALSE AND endpoint_complete IS FALSE AND resource_count >= 0 AND semantic_projection_as_of BETWEEN DATE '0001-01-01' AND DATE '9999-12-31' AND ((status = 'building' AND is_current IS FALSE AND dataset_hash IS NULL AND validated_at IS NULL AND published_at IS NULL AND superseded_at IS NULL) OR (status = 'validated' AND is_current IS FALSE AND dataset_hash ~ '^[0-9a-f]{{64}}$' AND validated_at IS NOT NULL AND published_at IS NULL AND superseded_at IS NULL) OR (status = 'published' AND is_current IS TRUE AND dataset_hash ~ '^[0-9a-f]{{64}}$' AND validated_at IS NOT NULL AND published_at IS NOT NULL AND superseded_at IS NULL) OR (status = 'superseded' AND is_current IS FALSE AND dataset_hash ~ '^[0-9a-f]{{64}}$' AND validated_at IS NOT NULL AND published_at IS NOT NULL AND superseded_at IS NOT NULL AND superseded_at >= published_at))",
            name="pd_uhc_flex_practitioner_dataset_check",
        ),
        schema=schema,
    )
    create_table_or_validate(
        op,
        _PROVENANCE,
        sa.Column("dataset_id", sa.String(55), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("resource_id", sa.String(256), nullable=False),
        sa.Column("requested_npi", sa.BigInteger(), nullable=False),
        sa.Column("candidate_acquisition_id", sa.String(55), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("acquired_resource_sha256", sa.String(64), nullable=False),
        sa.PrimaryKeyConstraint("dataset_id", "resource_id", name="pd_uhc_flex_dataset_resource_pkey"),
        sa.ForeignKeyConstraint(["dataset_id"], [f"{schema}.{_HEADER}.dataset_id"], name="pd_uhc_flex_dataset_resource_dataset_fkey"),
        sa.ForeignKeyConstraint(["candidate_acquisition_id"], [f"{schema}.{_ADMISSION}.candidate_acquisition_id"], name="pd_uhc_flex_dataset_resource_candidate_fkey"),
        sa.ForeignKeyConstraint(
            ["dataset_id", "resource_type", "resource_id"],
            [f"{schema}.{_DATASET_RESOURCE}.dataset_id", f"{schema}.{_DATASET_RESOURCE}.resource_type", f"{schema}.{_DATASET_RESOURCE}.resource_id"],
            name="pd_uhc_flex_dataset_resource_parent_fkey",
        ),
        sa.CheckConstraint(
            f"resource_type = {_ql(_RESOURCE_TYPE)} AND resource_id ~ '^[A-Za-z0-9.-]{{1,64}}$' AND requested_npi BETWEEN 1000000000 AND 2999999999 AND payload_hash ~ '^[0-9a-f]{{64}}$' AND acquired_resource_sha256 ~ '^[0-9a-f]{{64}}$'",
            name="pd_uhc_flex_dataset_resource_check",
        ),
        schema=schema,
    )


def _create_indexes(schema: str) -> None:
    create_index_if_missing(
        op,
        "pd_uhc_flex_practitioner_dataset_current_idx",
        _HEADER,
        ["source_id"],
        unique=True,
        schema=schema,
        postgresql_where=sa.text("is_current = true"),
    )
    create_index_if_missing(
        op,
        "pd_uhc_flex_practitioner_dataset_hash_idx",
        _HEADER,
        ["dataset_hash"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_uhc_flex_dataset_resource_npi_idx",
        _PROVENANCE,
        ["dataset_id", "requested_npi"],
        schema=schema,
    )


def _create_triggers(schema: str) -> None:
    header_ref = _qf(schema, _HEADER)
    provenance_ref = _qf(schema, _PROVENANCE)
    parent_ref = _qf(schema, _ENDPOINT_DATASET)
    source_ref = _qf(schema, _SOURCE)
    endpoint_ref = _qf(schema, _ENDPOINT)
    header_guard = _qf(schema, _HEADER_GUARD)
    provenance_guard = _qf(schema, _PROVENANCE_GUARD)
    parent_guard = _qf(schema, _PARENT_GUARD)
    source_guard = _qf(schema, _SOURCE_GUARD)
    endpoint_guard = _qf(schema, _ENDPOINT_GUARD)
    statements = (
        f"CREATE TRIGGER pd_uhc_flex_dataset_row_guard BEFORE INSERT OR UPDATE OR DELETE ON {header_ref} FOR EACH ROW EXECUTE FUNCTION {header_guard}();",
        f"CREATE CONSTRAINT TRIGGER pd_uhc_flex_dataset_valid_guard AFTER INSERT OR UPDATE ON {header_ref} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {header_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_dataset_truncate_guard BEFORE TRUNCATE ON {header_ref} FOR EACH STATEMENT EXECUTE FUNCTION {header_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_dataset_resource_row_guard BEFORE INSERT OR UPDATE OR DELETE ON {provenance_ref} FOR EACH ROW EXECUTE FUNCTION {provenance_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_dataset_resource_truncate_guard BEFORE TRUNCATE ON {provenance_ref} FOR EACH STATEMENT EXECUTE FUNCTION {provenance_guard}();",
        f"CREATE CONSTRAINT TRIGGER pd_uhc_flex_dataset_parent_guard AFTER INSERT OR UPDATE OR DELETE ON {parent_ref} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {parent_guard}();",
        f"CREATE CONSTRAINT TRIGGER pd_uhc_flex_dataset_source_guard AFTER UPDATE OR DELETE ON {source_ref} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {source_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_dataset_source_truncate_guard BEFORE TRUNCATE ON {source_ref} FOR EACH STATEMENT EXECUTE FUNCTION {source_guard}();",
        f"CREATE CONSTRAINT TRIGGER pd_uhc_flex_dataset_endpoint_guard AFTER UPDATE OR DELETE ON {endpoint_ref} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {endpoint_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_dataset_endpoint_truncate_guard BEFORE TRUNCATE ON {endpoint_ref} FOR EACH STATEMENT EXECUTE FUNCTION {endpoint_guard}();",
    )
    for statement in statements:
        op.execute(statement)


def _revoke_guard_execute(schema: str) -> None:
    for function_name in (
        _HEADER_GUARD,
        _PROVENANCE_GUARD,
        _PARENT_GUARD,
        _SOURCE_GUARD,
        _ENDPOINT_GUARD,
    ):
        op.execute(
            f"REVOKE ALL ON FUNCTION {_qf(schema, function_name)}() FROM PUBLIC;"
        )


def upgrade() -> None:
    schema = _schema()
    guarded = (
        _ENDPOINT_DATASET,
        _DATASET_RESOURCE,
        _ADMISSION,
        _ACQUISITION,
        _WORK,
        _RAW_RESOURCE,
        _SOURCE,
        _ENDPOINT,
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(_qf(schema, relation) for relation in guarded)
        + " IN SHARE ROW EXCLUSIVE MODE;"
    )
    _create_tables(schema)
    _create_indexes(schema)
    op.execute(_valid_function_sql(schema))
    op.execute(_ready_function_sql(schema))
    op.execute(_header_guard_sql(schema))
    op.execute(_provenance_guard_sql(schema))
    op.execute(_parent_guard_sql(schema))
    op.execute(_source_guard_sql(schema))
    op.execute(_endpoint_guard_sql(schema))
    _create_triggers(schema)
    _revoke_guard_execute(schema)


def downgrade() -> None:
    schema = _schema()
    header_ref = _qf(schema, _HEADER)
    op.execute(
        f"""
        DO $migration$
        BEGIN
            IF EXISTS (SELECT 1 FROM {header_ref}) THEN
                RAISE EXCEPTION
                    'provider_directory_uhc_flex_practitioner_publication_downgrade_blocked'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $migration$;
        """
    )
    op.execute(
        f"DROP TRIGGER IF EXISTS pd_uhc_flex_dataset_endpoint_truncate_guard ON {_qf(schema, _ENDPOINT)};"
    )
    op.execute(
        f"DROP TRIGGER IF EXISTS pd_uhc_flex_dataset_endpoint_guard ON {_qf(schema, _ENDPOINT)};"
    )
    op.execute(
        f"DROP TRIGGER IF EXISTS pd_uhc_flex_dataset_source_truncate_guard ON {_qf(schema, _SOURCE)};"
    )
    op.execute(
        f"DROP TRIGGER IF EXISTS pd_uhc_flex_dataset_source_guard ON {_qf(schema, _SOURCE)};"
    )
    op.execute(
        f"DROP TRIGGER IF EXISTS pd_uhc_flex_dataset_parent_guard ON {_qf(schema, _ENDPOINT_DATASET)};"
    )
    for function_name in (
        _SOURCE_GUARD,
        _ENDPOINT_GUARD,
        _PARENT_GUARD,
        _PROVENANCE_GUARD,
        _HEADER_GUARD,
    ):
        op.execute(f"DROP FUNCTION {_qf(schema, function_name)}();")
    op.execute(f"DROP FUNCTION {_qf(schema, _READY_FUNCTION)}(text);")
    op.execute(f"DROP FUNCTION {_qf(schema, _VALID_FUNCTION)}(text);")
    op.drop_table(_PROVENANCE, schema=schema)
    op.drop_table(_HEADER, schema=schema)
