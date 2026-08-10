# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add dormant source-neutral rooted graph acquisition storage.

Revision ID: 20260811020000_provider_directory_rooted_graph_acquisition
Revises: 20260811010000_provider_directory_profile_capacity_preflight_receipt
"""

from __future__ import annotations

import os
import re

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import create_table_or_validate
from db.migration_index_adoption import create_index_if_missing


revision = "20260811020000_provider_directory_rooted_graph_acquisition"
down_revision = "20260811010000_provider_directory_profile_capacity_preflight_receipt"
branch_labels = None
depends_on = None


_ACQUISITION = "provider_directory_rooted_graph_acquisition"
_WORK = "provider_directory_rooted_graph_work"
_RESOURCE = "provider_directory_rooted_graph_resource"
_EDGE = "provider_directory_rooted_graph_edge"
_ENDPOINT = "provider_directory_api_endpoint"
_SOURCE = "provider_directory_source"
_DATASET = "provider_directory_endpoint_dataset"
_DATASET_RESOURCE = "provider_directory_dataset_resource"
_DATASET_INSURANCE_PLAN = "provider_directory_dataset_insurance_plan"
_DATASET_NETWORK_PLAN = "provider_directory_dataset_network_plan"
_DATASET_AFFILIATION_ORGANIZATION = (
    "provider_directory_dataset_affiliation_organization"
)
_LEGACY_DATASET = "provider_directory_uhc_flex_practitioner_dataset"
_LEGACY_PROVENANCE = "provider_directory_uhc_flex_practitioner_dataset_resource"
_LEGACY_COHORT = "provider_directory_uhc_flex_npi_cohort"
_LEGACY_VALID = "provider_directory_uhc_flex_practitioner_dataset_valid"
_LEGACY_READY = "provider_directory_uhc_flex_practitioner_dataset_ready"
_TWIN_ATTEMPT = "provider_directory_rooted_graph_twin_attempt"
_TWIN_ADMISSION = "provider_directory_rooted_graph_twin_admission"
_ROOTED_DATASET = "provider_directory_rooted_graph_dataset"
_ROOTED_PROVENANCE = "provider_directory_rooted_graph_dataset_resource"
_ROOTED_INTRINSIC_VALID = "provider_directory_rooted_graph_dataset_intrinsic_valid"
_ROOTED_OFFICIAL_LINEAGE_CURRENT = (
    "provider_directory_rooted_graph_official_lineage_current"
)
_ROOTED_VALID = "provider_directory_rooted_graph_dataset_valid"
_ROOTED_READY = "provider_directory_rooted_graph_dataset_ready"
_ACQUISITION_GUARD = "guard_provider_directory_rooted_graph_acquisition"
_WORK_GUARD = "guard_provider_directory_rooted_graph_work"
_RESOURCE_GUARD = "guard_provider_directory_rooted_graph_resource"
_EDGE_GUARD = "guard_provider_directory_rooted_graph_edge"
_WORK_BUDGET_GUARD = "account_provider_directory_rooted_graph_work_budget"
_RESOURCE_BUDGET_GUARD = "account_provider_directory_rooted_graph_resource_budget"
_EDGE_BUDGET_GUARD = "account_provider_directory_rooted_graph_edge_budget"
_TWIN_ATTEMPT_GUARD = "guard_provider_directory_rooted_graph_twin_attempt"
_TWIN_ADMISSION_GUARD = "guard_provider_directory_rooted_graph_twin_admission"
_ROOTED_DATASET_GUARD = "guard_provider_directory_rooted_graph_dataset"
_ROOTED_PROVENANCE_GUARD = "guard_provider_directory_rooted_graph_dataset_resource"
_LOGICAL_CURRENT_GUARD = "guard_provider_directory_exact_logical_current"
_ROOTED_DEPENDENCY_GUARD = "guard_provider_directory_rooted_graph_dependency"
_TERMINAL_SET = "provider_directory_rooted_graph_terminal_set_sha256"
_RESOURCE_SET = "provider_directory_rooted_graph_resource_set_sha256"
_EDGE_SET = "provider_directory_rooted_graph_edge_set_sha256"
_ROOT_HASH = "provider_directory_rooted_graph_sha256"

_STORAGE_CONTRACT = "healthporta.provider-directory.rooted-graph-acquisition.v1"
_GRAPH_CONTRACT = "healthporta.provider-directory.rooted-graph.v1"
_IDENTITY_CONTRACT = "healthporta.provider-directory.rooted-graph-identity.v1"
_TERMINAL_CONTRACT = "healthporta.provider-directory.rooted-graph-terminal-record.v2"
_TERMINAL_SET_CONTRACT = "healthporta.provider-directory.rooted-graph-terminal-set.v1"
_RESOURCE_SET_CONTRACT = "healthporta.provider-directory.rooted-graph-resource-set.v1"
_EDGE_SET_CONTRACT = "healthporta.provider-directory.rooted-graph-edge-set.v1"
_ROOT_CONTRACT = "healthporta.provider-directory.rooted-graph-root.v1"
_PAGINATION = "same-origin-source-issued-until-terminal"
_DIRECT_PAGINATION = "forbidden"
_PLAN_NET_NETWORK_EXTENSION_URLS = (
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference",
    "https://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference",
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-ParticipatingNetwork-extension",
)
_CONNECTOR_ID = "pdrgc_66b9a3c04ecb2368db3a6cbc33de3e8d9203b4e0002cc80a"
_GRAPH_CONTRACT_SHA256 = (
    "66b9a3c04ecb2368db3a6cbc33de3e8d9203b4e0002cc80a6147a09ba2f61351"
)
_QUERY_CONTRACT_SHA256 = (
    "4b93928781ea6a3d821a1ac21bd4d7f533ee5ada25184a540d31dfcbdfb2ea28"
)
_ACTION_SETTING = "healthporta.rooted_graph_action"
_ACQUISITION_SETTING = "healthporta.rooted_graph_acquisition"
_LEASE_SETTING = "healthporta.rooted_graph_lease"
_LEGACY_VARIANT = "uhc_flex_practitioner"
_ROOTED_VARIANT = "rooted_combined"
_PUBLICATION_CONTRACT = "healthporta.provider-directory.rooted-graph-publication.v1"
_LEGACY_PUBLICATION_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1"
)
_PUBLICATION_KIND = "rooted_combined"
_PUBLICATION_ROOT_CONTRACT = (
    "healthporta.provider-directory.rooted-graph-dataset-root.v1"
)
_TWIN_ATTEMPT_CONTRACT = "healthporta.provider-directory.rooted-graph-twin-attempt.v1"
_TWIN_ADMISSION_CONTRACT = (
    "healthporta.provider-directory.rooted-graph-matched-admission.v1"
)
_HASH_CONTRACT = "semantic_content_v3"
_ROOTED_SOURCE_ID = "pdfhir_2b088f28554b9e51505b455e"
_ROOTED_ENDPOINT_ID = "42d85e85d6214cf898aef33591756d0231d11f1ef250d8c404c804cda8f36161"
_ROOTED_ENDPOINT_SIGNATURE = (
    "ec925b980d5f937abd5ca144a2041dda0c2b224fbe3fa8b70ccbe088f2222140"
)
_LEGACY_SOURCE_ID = "pdfhir_1ceb7c0986c320b7eb924881"
_LEGACY_ENDPOINT_ID = "ad53a7446514ed65b3a8ea7ab68ceb9a1ef85bf6c04fcb882219ecb50928bab5"
_LEGACY_ENDPOINT_SIGNATURE = (
    "bdee3163e522418c674885160e14681ee5bab00819b022cc72428d9b49845458"
)
_SOURCE_AUTHORITY = "unitedhealthcare"
_OFFICIAL_SOURCE_ID = "pdfhir_2754e999dd691175821ec26e"
_OFFICIAL_COHORT_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-official-practitioner-npi-cohort.v1"
)
_API_BASE = "https://flex.optum.com/fhirpublic/R4"
_EXACT_PUBLICATION_LOCK_IDENTITY = (
    "provider-directory-uhc-flex-practitioner-publication:" + _LEGACY_SOURCE_ID
)
_DATASET_DEPENDENCY_TRIGGERS = {
    _DATASET_RESOURCE: ("pd_rg_dep_resource_row", "pd_rg_dep_resource_truncate"),
    _DATASET_INSURANCE_PLAN: (
        "pd_rg_dep_insurance_plan_row",
        "pd_rg_dep_insurance_plan_truncate",
    ),
    _DATASET_NETWORK_PLAN: (
        "pd_rg_dep_network_plan_row",
        "pd_rg_dep_network_plan_truncate",
    ),
    _DATASET_AFFILIATION_ORGANIZATION: (
        "pd_rg_dep_affiliation_org_row",
        "pd_rg_dep_affiliation_org_truncate",
    ),
}


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    schema = runtime_schema or legacy_schema or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise RuntimeError("Provider Directory database schema is invalid")
    return schema


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qf(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _digest_sql(value_sql: str) -> str:
    return (
        "pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to("
        f"{value_sql}, 'UTF8')), 'hex')"
    )


def _digest_identifier_sql(prefix: str, contract: str, fields: tuple[str, ...]) -> str:
    values = ", ".join((_ql(contract), *fields))
    return (
        f"{_ql(prefix)} || pg_catalog.substr("
        "pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to("
        f"pg_catalog.concat_ws(pg_catalog.chr(31), {values}), 'UTF8')), 'hex'),"
        "1, 48)"
    )


def _rooted_identity_tail(header: str) -> tuple[str, ...]:
    return (
        f"{header}.admission_id",
        f"{header}.publication_acquisition_id",
        f"{header}.source_id",
        f"{header}.endpoint_id",
        f"{header}.source_authority_id",
        f"{header}.root_dataset_variant",
        f"{header}.root_publication_contract_id",
        f"{header}.root_source_id",
        f"{header}.root_endpoint_id",
        f"{header}.practitioner_origin_source_id",
        f"{header}.practitioner_origin_endpoint_id",
        f"{header}.root_dataset_id",
        f"{header}.root_dataset_hash",
        f"{header}.root_content_proof_sha256",
        f"{header}.root_cohort_id",
        f"{header}.root_practitioner_resource_count::text",
        f"{header}.semantic_projection_as_of::text",
        f"{header}.operation_key",
        f"{header}.rooted_graph_sha256",
    )


def _rooted_expected_metadata_sql(header: str, admission: str) -> str:
    resource_counts = (
        "pg_catalog.jsonb_build_object("
        f"'InsurancePlan', {header}.insurance_plan_resource_count, "
        f"'PractitionerRole', {header}.practitioner_role_resource_count, "
        f"'Practitioner', {header}.practitioner_resource_count, "
        f"'Organization', {header}.organization_resource_count, "
        f"'Location', {header}.location_resource_count, "
        f"'HealthcareService', {header}.healthcare_service_resource_count, "
        f"'OrganizationAffiliation', {header}.organization_affiliation_resource_count, "
        f"'Endpoint', {header}.endpoint_resource_count)"
    )
    resources = (
        "pg_catalog.jsonb_build_array('InsurancePlan','PractitionerRole',"
        "'Practitioner','Organization','Location','HealthcareService',"
        "'OrganizationAffiliation','Endpoint')"
    )
    return f"""
        (pg_catalog.jsonb_build_object(
            'acquisition_root_run_id', {header}.acquisition_root_run_id,
            'admission_contract_id', {admission}.admission_contract_id,
            'admission_id', {header}.admission_id,
            'attempt_id', {header}.attempt_id,
            'comparison_acquisition_id', {header}.comparison_acquisition_id,
            'publication_acquisition_id', {header}.publication_acquisition_id,
            'publication_run_id', {header}.publication_run_id,
            'publication_contract_id', {header}.publication_contract_id,
            'publication_kind', {header}.publication_kind,
            'dataset_id', {header}.dataset_id,
            'previous_dataset_id', {header}.previous_dataset_id,
            'dataset_intent_id', {header}.dataset_intent_id,
            'scope_id', {header}.scope_id,
            'root_source_id', {header}.root_source_id,
            'root_endpoint_id', {header}.root_endpoint_id,
            'acquisition_source_id', {header}.source_id,
            'acquisition_endpoint_id', {header}.endpoint_id,
            'endpoint_id', {header}.endpoint_id,
            'endpoint_signature_sha256', {header}.endpoint_signature_sha256,
            'source_id', {header}.source_id,
            'source_ids', pg_catalog.jsonb_build_array({header}.source_id),
            'source_authority_id', {header}.source_authority_id,
            'semantic_projection_as_of', {header}.semantic_projection_as_of::text,
            'operation_key', {header}.operation_key,
            'root_variant', {header}.root_dataset_variant,
            'root_publication_contract_id',
                {header}.root_publication_contract_id,
            'root_dataset_id', {header}.root_dataset_id,
            'root_dataset_hash', {header}.root_dataset_hash,
            'root_content_proof_sha256', {header}.root_content_proof_sha256,
            'root_cohort_id', {header}.root_cohort_id,
            'root_practitioner_resource_count',
                {header}.root_practitioner_resource_count
        ) || pg_catalog.jsonb_build_object(
            'practitioner_origin_source_id',
                {header}.practitioner_origin_source_id,
            'practitioner_origin_endpoint_id',
                {header}.practitioner_origin_endpoint_id,
            'connector_id', {header}.connector_id,
            'storage_contract_id', {header}.storage_contract_id,
            'graph_contract_sha256', {header}.graph_contract_sha256,
            'query_contract_sha256', {header}.query_contract_sha256,
            'max_work_items', {header}.max_work_items,
            'max_resource_rows', {header}.max_resource_rows,
            'max_edge_rows', {header}.max_edge_rows,
            'max_payload_bytes', {header}.max_payload_bytes,
            'used_work_items', {header}.used_work_items,
            'used_resource_rows', {header}.used_resource_rows,
            'used_edge_rows', {header}.used_edge_rows,
            'used_payload_bytes', {header}.used_payload_bytes,
            'completed_count', {header}.completed_count,
            'resource_count', {header}.graph_resource_count,
            'edge_count', {header}.graph_edge_count,
            'insurance_plan_count', {header}.census_insurance_plan_count,
            'insurance_plan_page_count', {header}.insurance_plan_page_count,
            'terminal_set_sha256', {header}.terminal_set_sha256,
            'resource_set_sha256', {header}.resource_set_sha256,
            'edge_set_sha256', {header}.edge_set_sha256,
            'rooted_graph_sha256', {header}.rooted_graph_sha256,
            'resource_hash_contract', {header}.resource_hash_contract,
            'selected_resources', {resources},
            'expected_resources', {resources},
            'resource_counts', {resource_counts},
            'cohort_complete', true,
            'rooted_graph_complete', true,
            'endpoint_collection_complete', false,
            'endpoint_complete', false
        ))
    """


def _dataset_hash_sql(resource_ref: str, dataset_id_sql: str) -> str:
    identity = (
        "'[\"' || resource.resource_type || '\",\"' || "
        "resource.resource_id || '\",\"' || resource.payload_hash || '\"]'"
    )
    return f"""
        SELECT pg_catalog.encode(
                   pg_catalog.sha256(pg_catalog.convert_to(
                       COALESCE(pg_catalog.string_agg(
                           {identity}, E'\\n' ORDER BY resource.resource_type,
                           resource.resource_id), ''), 'UTF8')),
                   'hex')
          FROM {resource_ref} AS resource
         WHERE resource.dataset_id = {dataset_id_sql}
    """


def _scope_identity_sql(header: str) -> str:
    connector = f"pg_catalog.to_json({header}.connector_id)::text"
    return " || ".join(
        (
            "'{\"acquisition_endpoint_id\":'",
            f"pg_catalog.to_json({header}.acquisition_endpoint_id)::text",
            "',\"acquisition_source_id\":'",
            f"pg_catalog.to_json({header}.acquisition_source_id)::text",
            "',\"connector_id\":'",
            connector,
            "',\"contract_id\":'",
            f"pg_catalog.to_json({_ql(_GRAPH_CONTRACT)}::text)::text",
            "',\"identity_contract_id\":'",
            f"pg_catalog.to_json({_ql(_IDENTITY_CONTRACT)}::text)::text",
            "',\"max_edge_rows\":'",
            f"{header}.max_edge_rows::text",
            "',\"max_payload_bytes\":'",
            f"{header}.max_payload_bytes::text",
            "',\"max_resource_rows\":'",
            f"{header}.max_resource_rows::text",
            "',\"max_work_items\":'",
            f"{header}.max_work_items::text",
            "',\"root_content_proof_sha256\":'",
            f"pg_catalog.to_json({header}.root_content_proof_sha256)::text",
            "',\"root_dataset_hash\":'",
            f"pg_catalog.to_json({header}.root_dataset_hash)::text",
            "',\"root_dataset_id\":'",
            f"pg_catalog.to_json({header}.root_dataset_id)::text",
            "',\"root_dataset_variant\":'",
            f"pg_catalog.to_json({header}.root_dataset_variant)::text",
            "',\"root_endpoint_id\":'",
            f"pg_catalog.to_json({header}.root_endpoint_id)::text",
            "',\"root_publication_contract_id\":'",
            f"pg_catalog.to_json({header}.root_publication_contract_id)::text",
            "',\"root_resource_count\":'",
            f"{header}.root_resource_count::text",
            '\',"root_resource_type":"Practitioner","root_source_id":\'',
            f"pg_catalog.to_json({header}.root_source_id)::text",
            "',\"source_authority_id\":'",
            f"pg_catalog.to_json({header}.source_authority_id)::text",
            "'}'",
        )
    )


def _hash_functions_sql(schema: str) -> tuple[str, ...]:
    work = _qf(schema, _WORK)
    resource = _qf(schema, _RESOURCE)
    edge = _qf(schema, _EDGE)
    acquisition = _qf(schema, _ACQUISITION)
    terminal_function = _qf(schema, _TERMINAL_SET)
    resource_function = _qf(schema, _RESOURCE_SET)
    edge_function = _qf(schema, _EDGE_SET)
    root_function = _qf(schema, _ROOT_HASH)
    terminal_content = (
        f"{_ql(_TERMINAL_SET_CONTRACT)} || pg_catalog.chr(31) || "
        "COALESCE(pg_catalog.string_agg(work.query_identity_sha256 || "
        "pg_catalog.chr(31) || work.terminal_record_sha256, "
        "pg_catalog.chr(30) ORDER BY work.query_identity_sha256), '')"
    )
    resource_content = (
        f"{_ql(_RESOURCE_SET_CONTRACT)} || pg_catalog.chr(31) || "
        "COALESCE(pg_catalog.string_agg(resource.resource_type || "
        "pg_catalog.chr(31) || resource.resource_id || pg_catalog.chr(31) || "
        "resource.payload_sha256 || pg_catalog.chr(31) || "
        "resource.closure_scope, pg_catalog.chr(30) ORDER BY "
        "work.query_identity_sha256, resource.resource_type, "
        "resource.resource_id), '')"
    )
    edge_content = (
        f"{_ql(_EDGE_SET_CONTRACT)} || pg_catalog.chr(31) || "
        "COALESCE(pg_catalog.string_agg(edge.edge_sha256 || pg_catalog.chr(31) || "
        "edge.closure_scope, pg_catalog.chr(30) ORDER BY "
        "work.query_identity_sha256, edge.edge_sha256), '')"
    )
    root_content = (
        f"{_ql(_ROOT_CONTRACT)} || pg_catalog.chr(31) || header.scope_id || "
        f"pg_catalog.chr(31) || {terminal_function}(header.acquisition_id) || "
        f"pg_catalog.chr(31) || {resource_function}(header.acquisition_id) || "
        f"pg_catalog.chr(31) || {edge_function}(header.acquisition_id)"
    )
    return (
        f"""
        CREATE FUNCTION {terminal_function}(target_acquisition_id text)
        RETURNS text LANGUAGE sql STABLE STRICT PARALLEL SAFE
        SECURITY DEFINER SET search_path = pg_catalog AS $function$
            SELECT {_digest_sql(terminal_content)} FROM {work} AS work
             WHERE work.acquisition_id = target_acquisition_id
               AND work.status IN ('completed', 'error');
        $function$;
        """,
        f"""
        CREATE FUNCTION {resource_function}(target_acquisition_id text)
        RETURNS text LANGUAGE sql STABLE STRICT PARALLEL SAFE
        SECURITY DEFINER SET search_path = pg_catalog AS $function$
            SELECT {_digest_sql(resource_content)}
              FROM {resource} AS resource
              JOIN {work} AS work
                ON work.acquisition_id = resource.acquisition_id
               AND work.query_id = resource.query_id
               AND work.attempt_count = resource.attempt
             WHERE resource.acquisition_id = target_acquisition_id
               AND work.status = 'completed';
        $function$;
        """,
        f"""
        CREATE FUNCTION {edge_function}(target_acquisition_id text)
        RETURNS text LANGUAGE sql STABLE STRICT PARALLEL SAFE
        SECURITY DEFINER SET search_path = pg_catalog AS $function$
            SELECT {_digest_sql(edge_content)}
              FROM {edge} AS edge
              JOIN {work} AS work
                ON work.acquisition_id = edge.acquisition_id
               AND work.query_id = edge.query_id
               AND work.attempt_count = edge.attempt
             WHERE edge.acquisition_id = target_acquisition_id
               AND work.status = 'completed';
        $function$;
        """,
        f"""
        CREATE FUNCTION {root_function}(target_acquisition_id text)
        RETURNS text LANGUAGE sql STABLE STRICT PARALLEL SAFE
        SECURITY DEFINER SET search_path = pg_catalog AS $function$
            SELECT {_digest_sql(root_content)} FROM {acquisition} AS header
             WHERE header.acquisition_id = target_acquisition_id;
        $function$;
        """,
    )


def _acquisition_guard_sql(schema: str) -> str:
    guard = _qf(schema, _ACQUISITION_GUARD)
    acquisition = _qf(schema, _ACQUISITION)
    endpoint = _qf(schema, _ENDPOINT)
    source = _qf(schema, _SOURCE)
    dataset = _qf(schema, _DATASET)
    dataset_resource = _qf(schema, _DATASET_RESOURCE)
    legacy_dataset = _qf(schema, _LEGACY_DATASET)
    rooted_dataset = _qf(schema, _ROOTED_DATASET)
    legacy_ready = _qf(schema, _LEGACY_READY)
    rooted_ready = _qf(schema, _ROOTED_READY)
    work = _qf(schema, _WORK)
    resource = _qf(schema, _RESOURCE)
    edge = _qf(schema, _EDGE)
    terminal_function = _qf(schema, _TERMINAL_SET)
    resource_function = _qf(schema, _RESOURCE_SET)
    edge_function = _qf(schema, _EDGE_SET)
    root_function = _qf(schema, _ROOT_HASH)
    scope_digest = _digest_sql(_scope_identity_sql("NEW"))
    acquisition_identity = " || ".join(
        (
            "NEW.storage_contract_id",
            "pg_catalog.chr(31)",
            "NEW.scope_id",
            "pg_catalog.chr(31)",
            "NEW.root_cohort_id",
            "pg_catalog.chr(31)",
            "NEW.endpoint_signature_sha256",
            "pg_catalog.chr(31)",
            "NEW.graph_contract_sha256",
            "pg_catalog.chr(31)",
            "NEW.query_contract_sha256",
            "pg_catalog.chr(31)",
            "NEW.acquisition_role",
            "pg_catalog.chr(31)",
            "NEW.run_id",
            "pg_catalog.chr(31)",
            "NEW.dataset_intent_id",
        )
    )
    acquisition_digest = _digest_sql(acquisition_identity)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        parent_dataset record; root_header record;
        actual_root_count bigint; expected_scope_id text;
        expected_acquisition_id text; actual_work_count bigint;
        actual_pending bigint; actual_leased bigint; actual_completed bigint;
        actual_error bigint; actual_resource_count bigint; actual_edge_count bigint;
        actual_payload_bytes bigint;
        plan_count bigint; plan_total bigint; plan_pages integer;
    BEGIN
        IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_acquisition_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'INSERT' THEN
            SELECT parent.*, endpoint.endpoint_signature_hash
              INTO parent_dataset
              FROM {dataset} AS parent
              JOIN {endpoint} AS endpoint ON endpoint.endpoint_id = parent.endpoint_id
             WHERE parent.dataset_id = NEW.root_dataset_id FOR SHARE OF parent, endpoint;
            IF NEW.root_dataset_variant = {_ql(_LEGACY_VARIANT)} THEN
                SELECT header.dataset_id, header.publication_contract_id,
                       header.source_id, header.endpoint_id, header.status,
                       header.is_current, header.dataset_hash,
                       header.resource_count,
                       header.resource_count AS practitioner_resource_count,
                       header.cohort_id,
                       header.cohort_id AS root_cohort_id,
                       header.terminal_set_sha256,
                       header.terminal_set_sha256 AS root_content_proof_sha256
                  INTO root_header
                  FROM {legacy_dataset} AS header
                 WHERE header.dataset_id = NEW.root_dataset_id
                 FOR SHARE;
            ELSIF NEW.root_dataset_variant = {_ql(_ROOTED_VARIANT)} THEN
                SELECT header.dataset_id, header.publication_contract_id,
                       header.source_id, header.endpoint_id, header.status,
                       header.is_current, header.dataset_hash,
                       header.practitioner_resource_count AS resource_count,
                       header.practitioner_resource_count,
                       header.root_cohort_id AS cohort_id,
                       header.root_cohort_id,
                       header.root_content_proof_sha256 AS terminal_set_sha256,
                       header.root_content_proof_sha256
                  INTO root_header
                  FROM {rooted_dataset} AS header
                 WHERE header.dataset_id = NEW.root_dataset_id
                 FOR SHARE;
            END IF;
            SELECT count(*)::bigint INTO actual_root_count
              FROM {dataset_resource} AS member
             WHERE member.dataset_id = NEW.root_dataset_id
               AND member.resource_type = 'Practitioner';
            expected_scope_id := 'pdrgs_' || pg_catalog.substr({scope_digest}, 1, 48);
            expected_acquisition_id := 'pdrga_' || pg_catalog.substr(
                {acquisition_digest}, 1, 48
            );
            IF parent_dataset.dataset_id IS NULL
               OR parent_dataset.status IS DISTINCT FROM 'published'
               OR parent_dataset.is_current IS DISTINCT FROM TRUE
               OR parent_dataset.endpoint_id IS DISTINCT FROM NEW.root_endpoint_id
               OR parent_dataset.dataset_hash IS DISTINCT FROM NEW.root_dataset_hash
               OR actual_root_count IS DISTINCT FROM NEW.root_resource_count
               OR NEW.acquisition_source_id IS DISTINCT FROM {_ql(_ROOTED_SOURCE_ID)}
               OR NEW.acquisition_endpoint_id IS DISTINCT FROM {_ql(_ROOTED_ENDPOINT_ID)}
               OR NEW.endpoint_signature_sha256 IS DISTINCT FROM
                    {_ql(_ROOTED_ENDPOINT_SIGNATURE)}
               OR NEW.source_authority_id IS DISTINCT FROM {_ql(_SOURCE_AUTHORITY)}
               OR NOT EXISTS (
                    SELECT 1 FROM {source} AS root_source
                      JOIN {endpoint} AS root_endpoint
                        ON root_endpoint.endpoint_id = root_source.endpoint_id
                      JOIN {source} AS acquisition_source
                        ON acquisition_source.source_id = NEW.acquisition_source_id
                      JOIN {endpoint} AS acquisition_endpoint
                        ON acquisition_endpoint.endpoint_id =
                           NEW.acquisition_endpoint_id
                     WHERE root_source.source_id = NEW.root_source_id
                       AND root_source.endpoint_id = NEW.root_endpoint_id
                       AND acquisition_source.endpoint_id =
                           NEW.acquisition_endpoint_id
                       AND root_source.canonical_api_base = {_ql(_API_BASE)}
                       AND root_endpoint.canonical_api_base = {_ql(_API_BASE)}
                       AND acquisition_source.canonical_api_base = {_ql(_API_BASE)}
                       AND acquisition_endpoint.canonical_api_base = {_ql(_API_BASE)}
                       AND root_source.metadata_json::jsonb
                              ->> 'provider_directory_authority_id' =
                           NEW.source_authority_id
                       AND root_endpoint.metadata_json::jsonb ->> 'authority_id' =
                           NEW.source_authority_id
                       AND acquisition_source.metadata_json::jsonb
                              ->> 'provider_directory_authority_id' =
                           NEW.source_authority_id
                       AND acquisition_endpoint.metadata_json::jsonb
                              ->> 'authority_id' = NEW.source_authority_id
                       AND acquisition_endpoint.endpoint_signature_hash =
                           NEW.endpoint_signature_sha256
                       AND (
                            (NEW.root_dataset_variant = {_ql(_LEGACY_VARIANT)}
                             AND root_endpoint.endpoint_signature_hash =
                                 {_ql(_LEGACY_ENDPOINT_SIGNATURE)})
                            OR
                            (NEW.root_dataset_variant = {_ql(_ROOTED_VARIANT)}
                             AND root_endpoint.endpoint_signature_hash =
                                 {_ql(_ROOTED_ENDPOINT_SIGNATURE)})
                       )
               )
               OR NOT (
                    (
                        NEW.root_dataset_variant = {_ql(_LEGACY_VARIANT)}
                        AND NEW.root_publication_contract_id =
                            {_ql(_LEGACY_PUBLICATION_CONTRACT)}
                        AND NEW.root_source_id = {_ql(_LEGACY_SOURCE_ID)}
                        AND NEW.root_endpoint_id = {_ql(_LEGACY_ENDPOINT_ID)}
                        AND NEW.root_source_id <> NEW.acquisition_source_id
                        AND NEW.root_endpoint_id <> NEW.acquisition_endpoint_id
                        AND root_header.dataset_id = NEW.root_dataset_id
                        AND root_header.publication_contract_id =
                            NEW.root_publication_contract_id
                        AND root_header.source_id = NEW.root_source_id
                        AND root_header.endpoint_id = NEW.root_endpoint_id
                        AND root_header.status = 'published'
                        AND root_header.is_current IS TRUE
                        AND root_header.dataset_hash = NEW.root_dataset_hash
                        AND root_header.resource_count = NEW.root_resource_count
                        AND root_header.cohort_id = NEW.root_cohort_id
                        AND root_header.terminal_set_sha256 =
                            NEW.root_content_proof_sha256
                        AND {legacy_ready}(NEW.root_dataset_id)
                    ) OR (
                        NEW.root_dataset_variant = {_ql(_ROOTED_VARIANT)}
                        AND NEW.root_publication_contract_id =
                            {_ql(_PUBLICATION_CONTRACT)}
                        AND NEW.root_source_id = NEW.acquisition_source_id
                        AND NEW.root_endpoint_id = NEW.acquisition_endpoint_id
                        AND root_header.dataset_id = NEW.root_dataset_id
                        AND root_header.publication_contract_id =
                            NEW.root_publication_contract_id
                        AND root_header.source_id = NEW.root_source_id
                        AND root_header.endpoint_id = NEW.root_endpoint_id
                        AND root_header.status = 'published'
                        AND root_header.is_current IS TRUE
                        AND root_header.dataset_hash = NEW.root_dataset_hash
                        AND root_header.practitioner_resource_count =
                            NEW.root_resource_count
                        AND root_header.root_cohort_id = NEW.root_cohort_id
                        AND root_header.root_content_proof_sha256 =
                            NEW.root_content_proof_sha256
                        AND {rooted_ready}(NEW.root_dataset_id)
                    )
               )
               OR parent_dataset.publication_metadata_json::jsonb
                      ->> 'dataset_id' IS DISTINCT FROM NEW.root_dataset_id
               OR parent_dataset.publication_metadata_json::jsonb
                      ->> 'publication_contract_id' IS DISTINCT FROM
                    NEW.root_publication_contract_id
               OR parent_dataset.publication_metadata_json::jsonb
                      -> 'cohort_complete' IS DISTINCT FROM 'true'::jsonb
               OR parent_dataset.publication_metadata_json::jsonb
                      -> 'endpoint_collection_complete' IS DISTINCT FROM 'false'::jsonb
               OR parent_dataset.publication_metadata_json::jsonb
                      -> 'endpoint_complete' IS DISTINCT FROM 'false'::jsonb
               OR (parent_dataset.publication_metadata_json::jsonb
                      #>> ARRAY['resource_counts', 'Practitioner'])::bigint
                    IS DISTINCT FROM NEW.root_resource_count
               OR parent_dataset.publication_metadata_json::jsonb
                      ->> 'source_id' IS DISTINCT FROM NEW.root_source_id
               OR parent_dataset.publication_metadata_json::jsonb
                      ->> 'endpoint_id' IS DISTINCT FROM NEW.root_endpoint_id
               OR NEW.scope_id IS DISTINCT FROM expected_scope_id
               OR NEW.acquisition_id IS DISTINCT FROM expected_acquisition_id
               OR NEW.created_at IS DISTINCT FROM transaction_timestamp()
               OR NEW.updated_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_acquisition_invalid'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        IF OLD.status = 'building' AND NEW.status = 'building'
           AND pg_catalog.pg_trigger_depth() > 1
           AND ROW(OLD.acquisition_id, OLD.storage_contract_id, OLD.scope_id,
                   OLD.root_source_id, OLD.root_endpoint_id,
                   OLD.acquisition_source_id, OLD.acquisition_endpoint_id,
                   OLD.source_authority_id, OLD.root_dataset_variant,
                   OLD.root_publication_contract_id,
                   OLD.endpoint_signature_sha256, OLD.root_dataset_id,
                   OLD.root_dataset_hash, OLD.root_content_proof_sha256,
                   OLD.root_cohort_id, OLD.root_resource_type,
                   OLD.root_resource_count, OLD.connector_id,
                   OLD.graph_contract_sha256, OLD.query_contract_sha256,
                   OLD.acquisition_role, OLD.run_id, OLD.dataset_intent_id,
                   OLD.max_work_items, OLD.max_resource_rows,
                   OLD.max_edge_rows, OLD.max_payload_bytes,
                   OLD.rooted_graph_complete,
                   OLD.endpoint_collection_complete, OLD.endpoint_complete,
                   OLD.pending_count, OLD.leased_count, OLD.completed_count,
                   OLD.error_count, OLD.resource_count, OLD.edge_count,
                   OLD.insurance_plan_count, OLD.insurance_plan_page_count,
                   OLD.terminal_set_sha256, OLD.resource_set_sha256,
                   OLD.edge_set_sha256, OLD.rooted_graph_sha256,
                   OLD.created_at, OLD.sealed_at)
               IS NOT DISTINCT FROM
               ROW(NEW.acquisition_id, NEW.storage_contract_id, NEW.scope_id,
                   NEW.root_source_id, NEW.root_endpoint_id,
                   NEW.acquisition_source_id, NEW.acquisition_endpoint_id,
                   NEW.source_authority_id, NEW.root_dataset_variant,
                   NEW.root_publication_contract_id,
                   NEW.endpoint_signature_sha256, NEW.root_dataset_id,
                   NEW.root_dataset_hash, NEW.root_content_proof_sha256,
                   NEW.root_cohort_id, NEW.root_resource_type,
                   NEW.root_resource_count, NEW.connector_id,
                   NEW.graph_contract_sha256, NEW.query_contract_sha256,
                   NEW.acquisition_role, NEW.run_id, NEW.dataset_intent_id,
                   NEW.max_work_items, NEW.max_resource_rows,
                   NEW.max_edge_rows, NEW.max_payload_bytes,
                   NEW.rooted_graph_complete,
                   NEW.endpoint_collection_complete, NEW.endpoint_complete,
                   NEW.pending_count, NEW.leased_count, NEW.completed_count,
                   NEW.error_count, NEW.resource_count, NEW.edge_count,
                   NEW.insurance_plan_count, NEW.insurance_plan_page_count,
                   NEW.terminal_set_sha256, NEW.resource_set_sha256,
                   NEW.edge_set_sha256, NEW.rooted_graph_sha256,
                   NEW.created_at, NEW.sealed_at)
           AND NEW.used_work_items >= OLD.used_work_items
           AND NEW.used_resource_rows >= OLD.used_resource_rows
           AND NEW.used_edge_rows >= OLD.used_edge_rows
           AND NEW.used_payload_bytes >= OLD.used_payload_bytes
           AND NEW.used_work_items <= NEW.max_work_items
           AND NEW.used_resource_rows <= NEW.max_resource_rows
           AND NEW.used_edge_rows <= NEW.max_edge_rows
           AND NEW.used_payload_bytes <= NEW.max_payload_bytes
           AND NEW.updated_at IS NOT DISTINCT FROM transaction_timestamp() THEN
            RETURN NEW;
        END IF;
        IF ROW(OLD.acquisition_id, OLD.storage_contract_id, OLD.scope_id,
               OLD.root_source_id, OLD.root_endpoint_id,
               OLD.acquisition_source_id, OLD.acquisition_endpoint_id,
               OLD.source_authority_id, OLD.root_dataset_variant,
               OLD.root_publication_contract_id,
               OLD.endpoint_signature_sha256,
               OLD.root_dataset_id, OLD.root_dataset_hash,
               OLD.root_content_proof_sha256, OLD.root_cohort_id,
               OLD.root_resource_type, OLD.root_resource_count, OLD.connector_id,
               OLD.graph_contract_sha256, OLD.query_contract_sha256,
               OLD.acquisition_role, OLD.run_id, OLD.dataset_intent_id,
               OLD.max_work_items, OLD.max_resource_rows,
               OLD.max_edge_rows, OLD.max_payload_bytes,
               OLD.used_work_items, OLD.used_resource_rows,
               OLD.used_edge_rows, OLD.used_payload_bytes,
               OLD.endpoint_collection_complete, OLD.endpoint_complete, OLD.created_at)
           IS DISTINCT FROM
           ROW(NEW.acquisition_id, NEW.storage_contract_id, NEW.scope_id,
               NEW.root_source_id, NEW.root_endpoint_id,
               NEW.acquisition_source_id, NEW.acquisition_endpoint_id,
               NEW.source_authority_id, NEW.root_dataset_variant,
               NEW.root_publication_contract_id,
               NEW.endpoint_signature_sha256,
               NEW.root_dataset_id, NEW.root_dataset_hash,
               NEW.root_content_proof_sha256, NEW.root_cohort_id,
               NEW.root_resource_type, NEW.root_resource_count, NEW.connector_id,
               NEW.graph_contract_sha256, NEW.query_contract_sha256,
               NEW.acquisition_role, NEW.run_id, NEW.dataset_intent_id,
               NEW.max_work_items, NEW.max_resource_rows,
               NEW.max_edge_rows, NEW.max_payload_bytes,
               NEW.used_work_items, NEW.used_resource_rows,
               NEW.used_edge_rows, NEW.used_payload_bytes,
               NEW.endpoint_collection_complete, NEW.endpoint_complete, NEW.created_at)
           OR OLD.status <> 'building' OR NEW.status <> 'sealed'
           OR OLD.rooted_graph_complete IS DISTINCT FROM FALSE
           OR NEW.rooted_graph_complete IS DISTINCT FROM TRUE
           OR NEW.updated_at IS DISTINCT FROM transaction_timestamp()
           OR NEW.sealed_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_acquisition_immutable'
                USING ERRCODE = '55000';
        END IF;
        LOCK TABLE {work}, {resource}, {edge} IN SHARE MODE;
        SELECT count(*)::bigint,
               count(*) FILTER (WHERE status = 'pending')::bigint,
               count(*) FILTER (WHERE status = 'leased')::bigint,
               count(*) FILTER (WHERE status = 'completed')::bigint,
               count(*) FILTER (WHERE status = 'error')::bigint
          INTO actual_work_count, actual_pending, actual_leased,
               actual_completed, actual_error
          FROM {work} WHERE acquisition_id = NEW.acquisition_id;
        SELECT count(*)::bigint,
               COALESCE(sum(octet_length(witness.payload_json_text)), 0)::bigint
          INTO actual_resource_count, actual_payload_bytes
          FROM {resource} AS witness JOIN {work} AS query
            ON query.acquisition_id = witness.acquisition_id
           AND query.query_id = witness.query_id
           AND query.attempt_count = witness.attempt
         WHERE witness.acquisition_id = NEW.acquisition_id
           AND query.status = 'completed';
        actual_payload_bytes := actual_payload_bytes + COALESCE((
            SELECT sum(octet_length(query.missing_response_json_text))::bigint
              FROM {work} AS query
             WHERE query.acquisition_id = NEW.acquisition_id
               AND query.status = 'completed'
               AND query.missing_http_status IN (404, 410)
        ), 0);
        SELECT count(*)::bigint INTO actual_edge_count
          FROM {edge} AS witness JOIN {work} AS query
            ON query.acquisition_id = witness.acquisition_id
           AND query.query_id = witness.query_id
           AND query.attempt_count = witness.attempt
         WHERE witness.acquisition_id = NEW.acquisition_id
           AND query.status = 'completed';
        SELECT count(*)::bigint, max(advertised_total), max(terminal_page_count)
          INTO plan_count, plan_total, plan_pages FROM {work}
         WHERE acquisition_id = NEW.acquisition_id
           AND kind = 'full_insurance_plan_census' AND status = 'completed';
        IF actual_pending <> 0 OR actual_leased <> 0 OR actual_error <> 0
           OR actual_completed IS DISTINCT FROM actual_work_count
           OR plan_count <> 1 OR plan_total IS NULL OR plan_pages IS NULL
           OR NEW.pending_count IS DISTINCT FROM actual_pending
           OR NEW.leased_count IS DISTINCT FROM actual_leased
           OR NEW.completed_count IS DISTINCT FROM actual_completed
           OR NEW.error_count IS DISTINCT FROM actual_error
           OR NEW.resource_count IS DISTINCT FROM actual_resource_count
           OR NEW.edge_count IS DISTINCT FROM actual_edge_count
           OR NEW.used_work_items IS DISTINCT FROM actual_work_count
           OR NEW.used_resource_rows IS DISTINCT FROM actual_resource_count
           OR NEW.used_edge_rows IS DISTINCT FROM actual_edge_count
           OR NEW.used_payload_bytes IS DISTINCT FROM actual_payload_bytes
           OR actual_work_count > NEW.max_work_items
           OR actual_resource_count > NEW.max_resource_rows
           OR actual_edge_count > NEW.max_edge_rows
           OR actual_payload_bytes > NEW.max_payload_bytes
           OR NEW.insurance_plan_count IS DISTINCT FROM plan_total
           OR NEW.insurance_plan_page_count IS DISTINCT FROM plan_pages
           OR NEW.terminal_set_sha256 IS DISTINCT FROM
                {terminal_function}(NEW.acquisition_id)
           OR NEW.resource_set_sha256 IS DISTINCT FROM
                {resource_function}(NEW.acquisition_id)
           OR NEW.edge_set_sha256 IS DISTINCT FROM
                {edge_function}(NEW.acquisition_id)
           OR NEW.rooted_graph_sha256 IS DISTINCT FROM
                {root_function}(NEW.acquisition_id) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_incomplete'
                USING ERRCODE = '23514';
        END IF;
        IF EXISTS (
            SELECT member.resource_id FROM {dataset_resource} AS member
             WHERE member.dataset_id = NEW.root_dataset_id
               AND member.resource_type = 'Practitioner'
            EXCEPT
            SELECT query.reference_id FROM {work} AS query
             WHERE query.acquisition_id = NEW.acquisition_id
               AND query.kind = 'exact_reference_search'
               AND query.resource_type = 'PractitionerRole'
               AND query.status = 'completed'
        ) OR EXISTS (
            SELECT query.reference_id FROM {work} AS query
             WHERE query.acquisition_id = NEW.acquisition_id
               AND query.kind = 'exact_reference_search'
               AND query.resource_type = 'PractitionerRole'
            EXCEPT
            SELECT member.resource_id FROM {dataset_resource} AS member
             WHERE member.dataset_id = NEW.root_dataset_id
               AND member.resource_type = 'Practitioner'
        ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_root_census_incomplete'
                USING ERRCODE = '23514';
        END IF;
        IF EXISTS (
            SELECT 1 FROM {resource} AS plan
              JOIN {work} AS plan_query
                ON plan_query.acquisition_id = plan.acquisition_id
               AND plan_query.query_id = plan.query_id
               AND plan_query.attempt_count = plan.attempt
             WHERE plan.acquisition_id = NEW.acquisition_id
               AND plan.resource_type = 'InsurancePlan'
               AND plan_query.kind = 'full_insurance_plan_census'
               AND ((plan.closure_scope = 'plan') IS DISTINCT FROM EXISTS (
                    SELECT 1 FROM {edge} AS network_edge
                     WHERE network_edge.acquisition_id = plan.acquisition_id
                       AND network_edge.query_id = plan.query_id
                       AND network_edge.attempt = plan.attempt
                       AND network_edge.source_resource_type = 'InsurancePlan'
                       AND network_edge.source_resource_id = plan.resource_id
                       AND network_edge.field_path LIKE 'network[%'
                       AND network_edge.target_resource_type = 'Organization'
                       AND EXISTS (
                            SELECT 1 FROM {edge} AS root_network_edge
                              JOIN {work} AS root_network_query
                                ON root_network_query.acquisition_id =
                                   root_network_edge.acquisition_id
                               AND root_network_query.query_id =
                                   root_network_edge.query_id
                               AND root_network_query.attempt_count =
                                   root_network_edge.attempt
                             WHERE root_network_edge.acquisition_id =
                                   plan.acquisition_id
                               AND root_network_edge.closure_scope = 'root'
                               AND root_network_edge.target_resource_type =
                                   'Organization'
                               AND root_network_edge.target_resource_id =
                                   network_edge.target_resource_id
                               AND root_network_query.status = 'completed'
                               AND (
                                    (root_network_edge.source_resource_type IN (
                                        'PractitionerRole',
                                        'OrganizationAffiliation'
                                    ) AND root_network_edge.field_path LIKE
                                        'network[%')
                                    OR (
                                        root_network_edge.source_resource_type =
                                            'PractitionerRole'
                                        AND root_network_edge.field_path LIKE
                                            'extension[%.valueReference'
                                    )
                               )
                       )
               ))
        ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_plan_intersection_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF EXISTS (
            SELECT 1 FROM {edge} AS reference_edge
              JOIN {work} AS source_query
                ON source_query.acquisition_id = reference_edge.acquisition_id
               AND source_query.query_id = reference_edge.query_id
               AND source_query.attempt_count = reference_edge.attempt
             WHERE reference_edge.acquisition_id = NEW.acquisition_id
               AND source_query.status = 'completed'
               AND reference_edge.closure_scope IN ('root', 'plan')
               AND reference_edge.target_resource_type IN
                   ('Organization', 'Location', 'HealthcareService', 'Endpoint')
               AND NOT EXISTS (
                    SELECT 1 FROM {work} AS target_query
                     WHERE target_query.acquisition_id = NEW.acquisition_id
                       AND target_query.kind = 'direct_read'
                       AND target_query.reference_type =
                           reference_edge.target_resource_type
                       AND target_query.reference_id = reference_edge.target_resource_id
                       AND (target_query.closure_scope = reference_edge.closure_scope
                            OR (reference_edge.closure_scope = 'plan'
                                AND target_query.closure_scope = 'root'))
                       AND target_query.status = 'completed'
               )
        ) OR EXISTS (
            SELECT 1 FROM {resource} AS organization
              JOIN {work} AS organization_query
                ON organization_query.acquisition_id = organization.acquisition_id
               AND organization_query.query_id = organization.query_id
               AND organization_query.attempt_count = organization.attempt
             WHERE organization.acquisition_id = NEW.acquisition_id
               AND organization.resource_type = 'Organization'
               AND organization.closure_scope IN ('root', 'plan')
               AND organization_query.status = 'completed'
               AND NOT EXISTS (
                    SELECT 1 FROM {work} AS affiliation_query
                     WHERE affiliation_query.acquisition_id = NEW.acquisition_id
                       AND affiliation_query.kind = 'exact_reference_search'
                       AND affiliation_query.resource_type = 'OrganizationAffiliation'
                       AND affiliation_query.reference_id = organization.resource_id
                       AND (affiliation_query.closure_scope = organization.closure_scope
                            OR (organization.closure_scope = 'plan'
                                AND affiliation_query.closure_scope = 'root'))
                       AND affiliation_query.status = 'completed'
               )
        ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_fixed_point_incomplete'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _query_document_sql(row: str) -> str:
    reference = (
        f"CASE WHEN {row}.reference_type IS NULL THEN NULL ELSE "
        f"{row}.reference_type || '/' || {row}.reference_id END"
    )
    page_size = f"CASE WHEN {row}.kind = 'direct_read' THEN NULL::integer ELSE 100 END"
    pagination = (
        f"CASE WHEN {row}.kind = 'direct_read' THEN {_ql(_DIRECT_PAGINATION)} "
        f"ELSE {_ql(_PAGINATION)} END"
    )
    return f"pg_catalog.jsonb_build_object('kind', {row}.kind, 'page_size', {page_size}, 'pagination', {pagination}, 'reference', {reference}, 'resource_type', {row}.resource_type, 'search_parameter', {row}.search_parameter)"


def _work_guard_sql(schema: str) -> str:
    guard = _qf(schema, _WORK_GUARD)
    acquisition = _qf(schema, _ACQUISITION)
    dataset_resource = _qf(schema, _DATASET_RESOURCE)
    work = _qf(schema, _WORK)
    resource = _qf(schema, _RESOURCE)
    edge = _qf(schema, _EDGE)
    expected_document = _query_document_sql("NEW")
    query_id_digest = _digest_sql(
        f"{_ql(_IDENTITY_CONTRACT)} || pg_catalog.chr(31) || NEW.scope_id || "
        "pg_catalog.chr(31) || NEW.query_identity_json_text"
    )
    resource_set_content = (
        f"{_ql(_RESOURCE_SET_CONTRACT)} || pg_catalog.chr(31) || "
        "COALESCE(pg_catalog.string_agg(witness.resource_type || "
        "pg_catalog.chr(31) || witness.resource_id || pg_catalog.chr(31) || "
        "witness.payload_sha256 || pg_catalog.chr(31) || witness.closure_scope, "
        "pg_catalog.chr(30) ORDER BY witness.resource_id), '')"
    )
    edge_set_content = (
        f"{_ql(_EDGE_SET_CONTRACT)} || pg_catalog.chr(31) || "
        "COALESCE(pg_catalog.string_agg(witness.edge_sha256 || pg_catalog.chr(31) "
        "|| witness.closure_scope, pg_catalog.chr(30) "
        "ORDER BY witness.edge_sha256), '')"
    )
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        action text; action_acquisition text; action_lease text;
        actual_resource_count bigint; actual_edge_count bigint;
        expected_resource_set text; expected_edge_set text;
        expected_result text; expected_terminal text;
        payload_budget_updated integer;
    BEGIN
        IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_work_immutable'
                USING ERRCODE = '55000';
        END IF;
        action := pg_catalog.current_setting({_ql(_ACTION_SETTING)}, TRUE);
        action_acquisition := pg_catalog.current_setting(
            {_ql(_ACQUISITION_SETTING)}, TRUE
        );
        action_lease := pg_catalog.current_setting({_ql(_LEASE_SETTING)}, TRUE);
        IF TG_OP = 'INSERT' THEN
            IF action NOT IN ('initialize', 'derive', 'census')
               OR action_acquisition IS DISTINCT FROM NEW.acquisition_id
               OR NEW.created_at IS DISTINCT FROM transaction_timestamp()
               OR NEW.updated_at IS DISTINCT FROM transaction_timestamp()
               OR NEW.query_identity_json_text::jsonb IS DISTINCT FROM
                    {expected_document}
               OR NEW.query_identity_sha256 IS DISTINCT FROM
                    {_digest_sql('NEW.query_identity_json_text')}
               OR NEW.query_id IS DISTINCT FROM
                    'pdrgq_' || pg_catalog.substr({query_id_digest}, 1, 48)
               OR NOT EXISTS (
                    SELECT 1 FROM {acquisition} AS header
                     WHERE header.acquisition_id = NEW.acquisition_id
                       AND header.scope_id = NEW.scope_id
                       AND header.status = 'building'
               ) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_work_invalid'
                    USING ERRCODE = '23514';
            END IF;
            IF action = 'initialize' AND NOT (
                NEW.resource_type = 'PractitionerRole' AND EXISTS (
                    SELECT 1 FROM {acquisition} AS header
                      JOIN {dataset_resource} AS member
                        ON member.dataset_id = header.root_dataset_id
                     WHERE header.acquisition_id = NEW.acquisition_id
                       AND member.resource_type = 'Practitioner'
                       AND member.resource_id = NEW.reference_id
                )
            ) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_root_work_invalid'
                    USING ERRCODE = '23514';
            END IF;
            IF action = 'derive' AND (
                NEW.resource_type = 'PractitionerRole'
                OR NEW.kind = 'full_insurance_plan_census'
            ) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_root_work_invalid'
                    USING ERRCODE = '23514';
            END IF;
            IF action = 'derive' AND NEW.closure_scope = 'root' AND EXISTS (
                SELECT 1 FROM {work} AS census_query
                 WHERE census_query.acquisition_id = NEW.acquisition_id
                   AND census_query.kind = 'full_insurance_plan_census'
            ) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_root_closure_frozen'
                    USING ERRCODE = '55000';
            END IF;
            IF action = 'census' AND (
                NEW.kind <> 'full_insurance_plan_census'
                OR NOT EXISTS (
                    SELECT 1 FROM {work} AS root_query
                     WHERE root_query.acquisition_id = NEW.acquisition_id
                       AND root_query.closure_scope = 'root'
                )
                OR EXISTS (
                    SELECT 1 FROM {work} AS root_query
                     WHERE root_query.acquisition_id = NEW.acquisition_id
                       AND (root_query.closure_scope <> 'root'
                            OR root_query.status <> 'completed')
                )
                OR EXISTS (
                    SELECT member.resource_id
                      FROM {acquisition} AS header
                      JOIN {dataset_resource} AS member
                        ON member.dataset_id = header.root_dataset_id
                     WHERE header.acquisition_id = NEW.acquisition_id
                       AND member.resource_type = 'Practitioner'
                    EXCEPT
                    SELECT root_query.reference_id FROM {work} AS root_query
                     WHERE root_query.acquisition_id = NEW.acquisition_id
                       AND root_query.kind = 'exact_reference_search'
                       AND root_query.resource_type = 'PractitionerRole'
                       AND root_query.closure_scope = 'root'
                       AND root_query.status = 'completed'
                )
                OR EXISTS (
                    SELECT root_query.reference_id FROM {work} AS root_query
                     WHERE root_query.acquisition_id = NEW.acquisition_id
                       AND root_query.kind = 'exact_reference_search'
                       AND root_query.resource_type = 'PractitionerRole'
                       AND root_query.closure_scope = 'root'
                    EXCEPT
                    SELECT member.resource_id
                      FROM {acquisition} AS header
                      JOIN {dataset_resource} AS member
                        ON member.dataset_id = header.root_dataset_id
                     WHERE header.acquisition_id = NEW.acquisition_id
                       AND member.resource_type = 'Practitioner'
                )
                OR EXISTS (
                    SELECT 1 FROM {edge} AS reference_edge
                      JOIN {work} AS source_query
                        ON source_query.acquisition_id =
                           reference_edge.acquisition_id
                       AND source_query.query_id = reference_edge.query_id
                       AND source_query.attempt_count = reference_edge.attempt
                     WHERE reference_edge.acquisition_id = NEW.acquisition_id
                       AND reference_edge.closure_scope = 'root'
                       AND source_query.status = 'completed'
                       AND reference_edge.target_resource_type IN (
                           'Organization', 'Location', 'HealthcareService', 'Endpoint'
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM {work} AS target_query
                            WHERE target_query.acquisition_id = NEW.acquisition_id
                              AND target_query.kind = 'direct_read'
                              AND target_query.reference_type =
                                  reference_edge.target_resource_type
                              AND target_query.reference_id =
                                  reference_edge.target_resource_id
                              AND target_query.closure_scope = 'root'
                              AND target_query.status = 'completed'
                       )
                )
                OR EXISTS (
                    SELECT 1 FROM {resource} AS organization
                      JOIN {work} AS organization_query
                        ON organization_query.acquisition_id =
                           organization.acquisition_id
                       AND organization_query.query_id = organization.query_id
                       AND organization_query.attempt_count = organization.attempt
                     WHERE organization.acquisition_id = NEW.acquisition_id
                       AND organization.resource_type = 'Organization'
                       AND organization.closure_scope = 'root'
                       AND organization_query.status = 'completed'
                       AND NOT EXISTS (
                           SELECT 1 FROM {work} AS affiliation_query
                            WHERE affiliation_query.acquisition_id =
                                  NEW.acquisition_id
                              AND affiliation_query.kind = 'exact_reference_search'
                              AND affiliation_query.resource_type =
                                  'OrganizationAffiliation'
                              AND affiliation_query.reference_id =
                                  organization.resource_id
                              AND affiliation_query.closure_scope = 'root'
                              AND affiliation_query.status = 'completed'
                       )
                )
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_rooted_graph_root_fixed_point_incomplete'
                    USING ERRCODE = '23514';
            END IF;
            IF action = 'derive' AND NEW.kind = 'direct_read' AND NOT EXISTS (
                SELECT 1 FROM {edge} AS proof
                  JOIN {work} AS parent
                    ON parent.acquisition_id = proof.acquisition_id
                   AND parent.query_id = proof.query_id
                   AND parent.attempt_count = proof.attempt
                 WHERE proof.acquisition_id = NEW.acquisition_id
                   AND proof.query_id = NEW.discovered_by_query_id
                   AND proof.source_resource_type = NEW.discovered_source_type
                   AND proof.source_resource_id = NEW.discovered_source_id
                   AND proof.edge_sha256 = NEW.discovered_edge_sha256
                   AND proof.target_resource_type = NEW.reference_type
                   AND proof.target_resource_id = NEW.reference_id
                   AND proof.closure_scope = NEW.closure_scope
                   AND parent.status = 'completed'
                   AND parent.terminal_at IS NOT DISTINCT FROM
                       transaction_timestamp()
            ) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_discovery_invalid'
                    USING ERRCODE = '23514';
            END IF;
            IF action = 'derive'
               AND NEW.resource_type = 'OrganizationAffiliation' AND NOT EXISTS (
                SELECT 1 FROM {resource} AS proof
                  JOIN {work} AS parent
                    ON parent.acquisition_id = proof.acquisition_id
                   AND parent.query_id = proof.query_id
                   AND parent.attempt_count = proof.attempt
                 WHERE proof.acquisition_id = NEW.acquisition_id
                   AND proof.query_id = NEW.discovered_by_query_id
                   AND proof.resource_type = 'Organization'
                   AND proof.resource_id = NEW.reference_id
                   AND proof.closure_scope = NEW.closure_scope
                   AND parent.status = 'completed'
                   AND parent.terminal_at IS NOT DISTINCT FROM
                       transaction_timestamp()
            ) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_discovery_invalid'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        IF ROW(OLD.acquisition_id, OLD.scope_id, OLD.query_id,
               OLD.query_identity_sha256, OLD.query_identity_json_text,
               OLD.kind, OLD.resource_type, OLD.search_parameter,
               OLD.reference_type, OLD.reference_id, OLD.closure_scope,
               OLD.discovered_by_query_id, OLD.discovered_source_type,
               OLD.discovered_source_id, OLD.discovered_edge_sha256, OLD.created_at)
           IS DISTINCT FROM
           ROW(NEW.acquisition_id, NEW.scope_id, NEW.query_id,
               NEW.query_identity_sha256, NEW.query_identity_json_text,
               NEW.kind, NEW.resource_type, NEW.search_parameter,
               NEW.reference_type, NEW.reference_id, NEW.closure_scope,
               NEW.discovered_by_query_id, NEW.discovered_source_type,
               NEW.discovered_source_id, NEW.discovered_edge_sha256, NEW.created_at)
           OR action_acquisition IS DISTINCT FROM NEW.acquisition_id
           OR NEW.updated_at IS DISTINCT FROM transaction_timestamp()
           OR NOT EXISTS (
                SELECT 1 FROM {acquisition} AS header
                 WHERE header.acquisition_id = NEW.acquisition_id
                   AND header.status = 'building'
           ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_work_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF action IN ('claim', 'claim_census') THEN
            IF NOT (((action = 'claim' AND
                       OLD.kind <> 'full_insurance_plan_census') OR
                      (action = 'claim_census' AND
                       OLD.kind = 'full_insurance_plan_census'))
                    AND (OLD.status = 'pending' OR (
                        OLD.status = 'leased'
                        AND OLD.lease_expires_at <= clock_timestamp()
                    )) AND NEW.status = 'leased'
                    AND NEW.attempt_count = OLD.attempt_count + 1
                    AND NEW.lease_token = action_lease
                    AND NEW.lease_token ~ '^[0-9a-f]{{64}}$'
                    AND NEW.lease_token IS DISTINCT FROM OLD.lease_token
                    AND NEW.lease_expires_at > clock_timestamp()
                    AND NEW.lease_heartbeat_at IS NOT NULL) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_claim_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF action = 'heartbeat' THEN
            IF NOT (OLD.status = 'leased'
                    AND OLD.lease_expires_at > clock_timestamp()
                    AND NEW.status = 'leased'
                    AND NEW.attempt_count = OLD.attempt_count
                    AND NEW.lease_token = OLD.lease_token
                    AND action_lease = OLD.lease_token
                    AND NEW.lease_expires_at > OLD.lease_expires_at
                    AND NEW.lease_heartbeat_at >= OLD.lease_heartbeat_at) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_lease_lost'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF action = 'release' THEN
            IF NOT (OLD.status = 'leased'
                    AND OLD.lease_expires_at > clock_timestamp()
                    AND action_lease = OLD.lease_token
                    AND NEW.status = 'pending'
                    AND NEW.attempt_count = OLD.attempt_count
                    AND NEW.lease_token IS NULL
                    AND NEW.lease_expires_at IS NULL
                    AND NEW.lease_heartbeat_at IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM {resource} AS witness
                         WHERE witness.acquisition_id = OLD.acquisition_id
                           AND witness.query_id = OLD.query_id
                           AND witness.attempt = OLD.attempt_count
                    ) AND NOT EXISTS (
                        SELECT 1 FROM {edge} AS witness
                         WHERE witness.acquisition_id = OLD.acquisition_id
                           AND witness.query_id = OLD.query_id
                           AND witness.attempt = OLD.attempt_count
                    )) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_lease_lost'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF action <> 'terminal' OR OLD.status <> 'leased'
           OR OLD.lease_expires_at <= clock_timestamp()
           OR action_lease IS DISTINCT FROM OLD.lease_token
           OR NEW.status NOT IN ('completed', 'error')
           OR NEW.attempt_count <> OLD.attempt_count
           OR NEW.lease_token IS NOT NULL OR NEW.lease_expires_at IS NOT NULL
           OR NEW.lease_heartbeat_at IS NOT NULL
           OR NEW.terminal_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_lease_lost'
                USING ERRCODE = '55000';
        END IF;
        SELECT count(*)::bigint, {_digest_sql(resource_set_content)}
          INTO actual_resource_count, expected_resource_set
          FROM {resource} AS witness
         WHERE witness.acquisition_id = NEW.acquisition_id
           AND witness.query_id = NEW.query_id
           AND witness.attempt = NEW.attempt_count;
        SELECT count(*)::bigint, {_digest_sql(edge_set_content)}
          INTO actual_edge_count, expected_edge_set
          FROM {edge} AS witness
         WHERE witness.acquisition_id = NEW.acquisition_id
           AND witness.query_id = NEW.query_id
           AND witness.attempt = NEW.attempt_count;
        IF NEW.status = 'error' THEN
            expected_terminal := {_digest_sql(
                f"{_ql(_TERMINAL_CONTRACT)} || pg_catalog.chr(31) || NEW.query_identity_sha256 || pg_catalog.chr(31) || 'error' || pg_catalog.chr(31) || '' || pg_catalog.chr(31) || '0' || pg_catalog.chr(31) || '0' || pg_catalog.chr(31) || '' || pg_catalog.chr(31) || '0' || pg_catalog.chr(31) || 'false' || pg_catalog.chr(31) || NEW.error_code"
            )};
            IF actual_resource_count <> 0 OR actual_edge_count <> 0
               OR NEW.missing_http_status IS NOT NULL
               OR NEW.missing_response_sha256 IS NOT NULL
               OR NEW.missing_response_bytes IS NOT NULL
               OR NEW.missing_response_json_text IS NOT NULL
               OR NEW.terminal_record_sha256 IS DISTINCT FROM expected_terminal THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_result_invalid'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        IF NEW.missing_http_status IS NOT NULL THEN
            expected_result := {_digest_sql(
                "expected_resource_set || pg_catalog.chr(31) || expected_edge_set || pg_catalog.chr(31) || NEW.missing_response_sha256 || pg_catalog.chr(31) || NEW.missing_response_bytes::text"
            )};
            expected_terminal := {_digest_sql(
                f"{_ql(_TERMINAL_CONTRACT)} || pg_catalog.chr(31) || NEW.query_identity_sha256 || pg_catalog.chr(31) || 'missing' || pg_catalog.chr(31) || expected_result || pg_catalog.chr(31) || '0' || pg_catalog.chr(31) || '0' || pg_catalog.chr(31) || '' || pg_catalog.chr(31) || '1' || pg_catalog.chr(31) || 'true' || pg_catalog.chr(31) || NEW.missing_http_status::text || pg_catalog.chr(31) || NEW.missing_response_sha256 || pg_catalog.chr(31) || NEW.missing_response_bytes::text"
            )};
            IF NEW.status <> 'completed' OR NEW.kind <> 'direct_read'
               OR NEW.missing_http_status NOT IN (404, 410)
               OR NEW.missing_response_sha256 IS NULL
               OR NEW.missing_response_sha256 !~ '^[0-9a-f]{{64}}$'
               OR NEW.missing_response_bytes IS NULL
               OR NEW.missing_response_bytes NOT BETWEEN 1 AND 65536
               OR NEW.missing_response_json_text IS NULL
               OR octet_length(NEW.missing_response_json_text)
                    IS DISTINCT FROM NEW.missing_response_bytes
               OR pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                    NEW.missing_response_json_text, 'UTF8')), 'hex')
                    IS DISTINCT FROM NEW.missing_response_sha256
               OR NOT (NEW.missing_response_json_text IS JSON OBJECT WITH UNIQUE KEYS)
               OR NEW.missing_response_json_text::jsonb ->> 'resourceType'
                    IS DISTINCT FROM 'OperationOutcome'
               OR pg_catalog.jsonb_typeof(
                    NEW.missing_response_json_text::jsonb -> 'issue') <> 'array'
               OR NOT (
                    (
                      NEW.missing_http_status = 404
                      AND pg_catalog.jsonb_array_length(
                          NEW.missing_response_json_text::jsonb -> 'issue'
                      ) = 1
                      AND NEW.missing_response_json_text::jsonb
                              #>> '{{issue,0,severity}}' = 'error'
                      AND NEW.missing_response_json_text::jsonb
                              #>> '{{issue,0,code}}' = 'not-found'
                    ) OR (
                      NEW.missing_http_status = 410
                      AND pg_catalog.jsonb_array_length(
                          NEW.missing_response_json_text::jsonb -> 'issue'
                      ) = 1
                      AND NEW.missing_response_json_text::jsonb
                              #>> '{{issue,0,severity}}' = 'error'
                      AND NEW.missing_response_json_text::jsonb
                              #>> '{{issue,0,code}}' = 'deleted'
                    ) OR (
                      pg_catalog.jsonb_array_length(
                          NEW.missing_response_json_text::jsonb -> 'issue'
                      ) = 2
                      AND (SELECT count(*) FROM pg_catalog.jsonb_array_elements(
                              NEW.missing_response_json_text::jsonb -> 'issue'
                          ) AS issue(value)
                           WHERE issue.value ->> 'severity' = 'information'
                             AND issue.value ->> 'code' = 'informational') = 1
                      AND (SELECT count(*) FROM pg_catalog.jsonb_array_elements(
                              NEW.missing_response_json_text::jsonb -> 'issue'
                          ) AS issue(value)
                           WHERE issue.value ->> 'severity' = 'error'
                             AND issue.value ->> 'code' = 'processing') = 1
                    )
               )
               OR actual_resource_count <> 0 OR actual_edge_count <> 0
               OR NEW.resource_count <> 0 OR NEW.edge_count <> 0
               OR NEW.resource_set_sha256 IS DISTINCT FROM expected_resource_set
               OR NEW.edge_set_sha256 IS DISTINCT FROM expected_edge_set
               OR NEW.result_sha256 IS DISTINCT FROM expected_result
               OR NEW.terminal_record_sha256 IS DISTINCT FROM expected_terminal THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_result_invalid'
                    USING ERRCODE = '23514';
            END IF;
            UPDATE {acquisition}
               SET used_payload_bytes = used_payload_bytes
                                        + NEW.missing_response_bytes,
                   updated_at = transaction_timestamp()
             WHERE acquisition_id = NEW.acquisition_id
               AND status = 'building'
               AND used_payload_bytes + NEW.missing_response_bytes
                   <= max_payload_bytes;
            GET DIAGNOSTICS payload_budget_updated = ROW_COUNT;
            IF payload_budget_updated <> 1 THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_budget_exceeded'
                    USING ERRCODE = '54000';
            END IF;
            RETURN NEW;
        END IF;
        expected_result := {_digest_sql(
            "expected_resource_set || pg_catalog.chr(31) || expected_edge_set"
        )};
        expected_terminal := {_digest_sql(
            f"{_ql(_TERMINAL_CONTRACT)} || pg_catalog.chr(31) || NEW.query_identity_sha256 || pg_catalog.chr(31) || 'completed' || pg_catalog.chr(31) || expected_result || pg_catalog.chr(31) || actual_resource_count::text || pg_catalog.chr(31) || actual_edge_count::text || pg_catalog.chr(31) || COALESCE(NEW.advertised_total::text, '') || pg_catalog.chr(31) || NEW.terminal_page_count::text || pg_catalog.chr(31) || 'true' || pg_catalog.chr(31) || ''"
        )};
        IF NEW.resource_count IS DISTINCT FROM actual_resource_count
           OR NEW.edge_count IS DISTINCT FROM actual_edge_count
           OR NEW.resource_set_sha256 IS DISTINCT FROM expected_resource_set
           OR NEW.edge_set_sha256 IS DISTINCT FROM expected_edge_set
           OR NEW.result_sha256 IS DISTINCT FROM expected_result
           OR NEW.terminal_record_sha256 IS DISTINCT FROM expected_terminal
           OR NEW.missing_http_status IS NOT NULL
           OR NEW.missing_response_sha256 IS NOT NULL
           OR NEW.missing_response_bytes IS NOT NULL
           OR NEW.missing_response_json_text IS NOT NULL
           OR (NEW.kind = 'direct_read' AND NEW.advertised_total IS NOT NULL)
           OR (NEW.kind = 'exact_reference_search'
               AND NEW.advertised_total IS NOT NULL
               AND NEW.advertised_total IS DISTINCT FROM actual_resource_count)
           OR (NEW.kind = 'full_insurance_plan_census' AND (
                NEW.advertised_total IS DISTINCT FROM actual_resource_count
                OR EXISTS (
                    SELECT 1 FROM {resource} AS plan
                     WHERE plan.acquisition_id = NEW.acquisition_id
                       AND plan.query_id = NEW.query_id
                       AND plan.attempt = NEW.attempt_count
                       AND plan.resource_type <> 'InsurancePlan'
                )
           )) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_result_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _resource_guard_sql(schema: str) -> str:
    guard = _qf(schema, _RESOURCE_GUARD)
    work = _qf(schema, _WORK)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE payload jsonb; parent_scope text; parent_kind text;
        parent_resource_type text; parent_reference_type text;
        parent_reference_id text; extension_count bigint;
        extension_invalid boolean;
    BEGIN
        IF TG_OP <> 'INSERT' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_resource_immutable'
                USING ERRCODE = '55000';
        END IF;
        BEGIN payload := NEW.payload_json_text::jsonb;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_resource_invalid'
                USING ERRCODE = '23514';
        END;
        WITH RECURSIVE extension_nodes(node, depth) AS (
            SELECT item.value, 1
              FROM pg_catalog.jsonb_array_elements(
                CASE
                  WHEN pg_catalog.jsonb_typeof(payload -> 'extension') = 'array'
                    THEN payload -> 'extension'
                  ELSE '[]'::jsonb
                END
              ) AS item(value)
            UNION ALL
            SELECT child.value, parent.depth + 1
              FROM extension_nodes AS parent
              CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
                CASE
                  WHEN pg_catalog.jsonb_typeof(parent.node -> 'extension') = 'array'
                    THEN parent.node -> 'extension'
                  ELSE '[]'::jsonb
                END
              ) AS child(value)
             WHERE parent.depth <= 6
        )
        SELECT count(*)::bigint,
               COALESCE(bool_or(
                 CASE
                   WHEN pg_catalog.jsonb_typeof(node) <> 'object' THEN true
                   WHEN node ->> 'url' IS NULL
                     OR node ->> 'url' !~ '^[^|]+(\\|[A-Za-z0-9.-]{{1,64}})?$'
                     THEN true
                   WHEN pg_catalog.split_part(node ->> 'url', '|', 1) IN (
                        {_ql(_PLAN_NET_NETWORK_EXTENSION_URLS[0])},
                        {_ql(_PLAN_NET_NETWORK_EXTENSION_URLS[1])},
                        {_ql(_PLAN_NET_NETWORK_EXTENSION_URLS[2])}
                   ) THEN
                        NEW.resource_type <> 'PractitionerRole'
                        OR node ? 'extension'
                        OR NOT node ? 'valueReference'
                        OR EXISTS (
                            SELECT 1
                              FROM pg_catalog.jsonb_object_keys(node) AS key(name)
                             WHERE key.name NOT IN ('url', 'valueReference')
                        )
                        OR pg_catalog.jsonb_typeof(node -> 'valueReference')
                           <> 'object'
                        OR EXISTS (
                            SELECT 1 FROM pg_catalog.jsonb_object_keys(
                                node -> 'valueReference'
                            ) AS key(name)
                             WHERE key.name <> 'reference'
                        )
                        OR node #>> '{{valueReference,reference}}'
                           !~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
                   ELSE
                        (node ? 'extension' AND EXISTS (
                            SELECT 1
                              FROM pg_catalog.jsonb_object_keys(node) AS key(name)
                             WHERE key.name LIKE 'value%'
                        ))
                        OR EXISTS (
                            SELECT 1
                              FROM pg_catalog.jsonb_each(node) AS field(name, value)
                             WHERE field.name = 'valueReference'
                                OR (
                                    pg_catalog.jsonb_typeof(field.value) = 'object'
                                    AND field.value ? 'reference'
                                )
                        )
                 END
                 OR depth > 6
                 OR (node ? 'extension' AND
                     pg_catalog.jsonb_typeof(node -> 'extension') <> 'array')
               ), false)
          INTO extension_count, extension_invalid
          FROM extension_nodes;
        SELECT query.closure_scope, query.kind, query.resource_type,
               query.reference_type, query.reference_id
          INTO parent_scope, parent_kind, parent_resource_type,
               parent_reference_type, parent_reference_id
          FROM {work} AS query
         WHERE query.acquisition_id = NEW.acquisition_id
           AND query.scope_id = NEW.scope_id AND query.query_id = NEW.query_id
           AND query.attempt_count = NEW.attempt AND query.status = 'leased'
           AND query.lease_token = pg_catalog.current_setting(
                {_ql(_LEASE_SETTING)}, TRUE
           ) AND query.lease_expires_at > clock_timestamp();
        IF pg_catalog.current_setting({_ql(_ACTION_SETTING)}, TRUE) <> 'witness'
           OR pg_catalog.current_setting({_ql(_ACQUISITION_SETTING)}, TRUE)
                IS DISTINCT FROM NEW.acquisition_id
           OR parent_scope IS NULL
           OR NEW.created_at IS DISTINCT FROM transaction_timestamp()
           OR NEW.payload_sha256 IS DISTINCT FROM
                {_digest_sql('NEW.payload_json_text')}
           OR (payload ? 'extension' AND
               pg_catalog.jsonb_typeof(payload -> 'extension') <> 'array')
           OR extension_count > 4096 OR extension_invalid IS TRUE
           OR payload ->> 'resourceType' IS DISTINCT FROM NEW.resource_type
           OR payload ->> 'id' IS DISTINCT FROM NEW.resource_id
           OR parent_resource_type IS DISTINCT FROM NEW.resource_type
           OR (parent_kind = 'exact_reference_search' AND
               CASE parent_resource_type
                   WHEN 'PractitionerRole' THEN
                       payload -> 'practitioner' ->> 'reference'
                   WHEN 'OrganizationAffiliation' THEN
                       payload -> 'participatingOrganization' ->> 'reference'
                   ELSE NULL::text
               END IS DISTINCT FROM
                   parent_reference_type || '/' || parent_reference_id)
           OR (parent_kind = 'direct_read' AND
               parent_reference_id IS DISTINCT FROM NEW.resource_id)
           OR (parent_kind = 'full_insurance_plan_census' AND
               NEW.closure_scope NOT IN ('census', 'plan'))
           OR (parent_kind <> 'full_insurance_plan_census' AND
               NEW.closure_scope IS DISTINCT FROM parent_scope) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_resource_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _edge_guard_sql(schema: str) -> str:
    guard = _qf(schema, _EDGE_GUARD)
    work = _qf(schema, _WORK)
    resource = _qf(schema, _RESOURCE)
    edge_identity = (
        "NEW.source_resource_type || pg_catalog.chr(31) || "
        "NEW.source_resource_id || pg_catalog.chr(31) || NEW.field_path || "
        "pg_catalog.chr(31) || NEW.target_resource_type || pg_catalog.chr(31) || "
        "NEW.target_resource_id"
    )
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE payload jsonb; field_name text; array_index integer;
        actual_reference text; parent_scope text; parent_scope_id text;
        extension_cursor text; extension_match text[];
        extension_path text[]; extension_url text;
        expected_target_type text; expected_repeated boolean;
    BEGIN
        IF TG_OP <> 'INSERT' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_edge_immutable'
                USING ERRCODE = '55000';
        END IF;
        SELECT witness.payload_json_text::jsonb, witness.closure_scope,
               witness.scope_id
          INTO payload, parent_scope, parent_scope_id FROM {resource} AS witness
          JOIN {work} AS query
            ON query.acquisition_id = witness.acquisition_id
           AND query.query_id = witness.query_id
           AND query.attempt_count = witness.attempt
         WHERE witness.acquisition_id = NEW.acquisition_id
           AND witness.query_id = NEW.query_id AND witness.attempt = NEW.attempt
           AND witness.resource_type = NEW.source_resource_type
           AND witness.resource_id = NEW.source_resource_id
           AND query.status = 'leased'
           AND query.lease_token = pg_catalog.current_setting(
                {_ql(_LEASE_SETTING)}, TRUE
           ) AND query.lease_expires_at > clock_timestamp();
        IF NEW.field_path ~ '^extension\\[[0-9]+\\]' THEN
            extension_cursor := NEW.field_path;
            extension_path := ARRAY[]::text[];
            WHILE extension_cursor ~ '^extension\\[[0-9]+\\]' LOOP
                extension_match := pg_catalog.regexp_match(
                    extension_cursor, '^extension\\[([0-9]+)\\](\\.|$)'
                );
                IF extension_match IS NULL THEN
                    RAISE EXCEPTION 'provider_directory_rooted_graph_edge_invalid'
                        USING ERRCODE = '23514';
                END IF;
                extension_path := extension_path ||
                    ARRAY['extension', extension_match[1]];
                extension_cursor := pg_catalog.regexp_replace(
                    extension_cursor, '^extension\\[[0-9]+\\]\\.?', ''
                );
            END LOOP;
            extension_url := payload #>> (extension_path || ARRAY['url']);
            actual_reference := payload #>> (
                extension_path || ARRAY['valueReference', 'reference']
            );
            IF extension_cursor <> 'valueReference'
               OR NEW.source_resource_type <> 'PractitionerRole'
               OR extension_url IS NULL
               OR pg_catalog.split_part(extension_url, '|', 1) NOT IN (
                    {_ql(_PLAN_NET_NETWORK_EXTENSION_URLS[0])},
                    {_ql(_PLAN_NET_NETWORK_EXTENSION_URLS[1])},
                    {_ql(_PLAN_NET_NETWORK_EXTENSION_URLS[2])}
               )
               OR extension_url !~ '^[^|]+(\\|[A-Za-z0-9.-]{{1,64}})?$'
            THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_edge_invalid'
                    USING ERRCODE = '23514';
            END IF;
            expected_target_type := 'Organization';
        ELSE
            field_name := pg_catalog.regexp_replace(
                NEW.field_path, '\\[[0-9]+\\]$', ''
            );
            expected_target_type := CASE
                WHEN NEW.source_resource_type = 'PractitionerRole'
                     AND field_name = 'practitioner' THEN 'Practitioner'
                WHEN NEW.source_resource_type = 'PractitionerRole'
                     AND field_name IN ('organization', 'network')
                    THEN 'Organization'
                WHEN NEW.source_resource_type = 'PractitionerRole'
                     AND field_name = 'location' THEN 'Location'
                WHEN NEW.source_resource_type = 'PractitionerRole'
                     AND field_name = 'healthcareService' THEN 'HealthcareService'
                WHEN NEW.source_resource_type = 'PractitionerRole'
                     AND field_name = 'endpoint' THEN 'Endpoint'
                WHEN NEW.source_resource_type = 'OrganizationAffiliation'
                     AND field_name IN (
                        'organization', 'participatingOrganization', 'network'
                     ) THEN 'Organization'
                WHEN NEW.source_resource_type = 'OrganizationAffiliation'
                     AND field_name = 'location' THEN 'Location'
                WHEN NEW.source_resource_type = 'OrganizationAffiliation'
                     AND field_name = 'healthcareService' THEN 'HealthcareService'
                WHEN NEW.source_resource_type = 'OrganizationAffiliation'
                     AND field_name = 'endpoint' THEN 'Endpoint'
                WHEN NEW.source_resource_type = 'Organization'
                     AND field_name = 'partOf' THEN 'Organization'
                WHEN NEW.source_resource_type = 'Organization'
                     AND field_name = 'endpoint' THEN 'Endpoint'
                WHEN NEW.source_resource_type = 'Location'
                     AND field_name = 'managingOrganization' THEN 'Organization'
                WHEN NEW.source_resource_type = 'Location'
                     AND field_name = 'partOf' THEN 'Location'
                WHEN NEW.source_resource_type = 'Location'
                     AND field_name = 'endpoint' THEN 'Endpoint'
                WHEN NEW.source_resource_type = 'HealthcareService'
                     AND field_name = 'providedBy' THEN 'Organization'
                WHEN NEW.source_resource_type = 'HealthcareService'
                     AND field_name IN ('location', 'coverageArea') THEN 'Location'
                WHEN NEW.source_resource_type = 'HealthcareService'
                     AND field_name = 'endpoint' THEN 'Endpoint'
                WHEN NEW.source_resource_type = 'InsurancePlan'
                     AND field_name IN ('ownedBy', 'administeredBy', 'network')
                    THEN 'Organization'
                WHEN NEW.source_resource_type = 'InsurancePlan'
                     AND field_name = 'coverageArea' THEN 'Location'
                WHEN NEW.source_resource_type = 'Endpoint'
                     AND field_name = 'managingOrganization' THEN 'Organization'
                ELSE NULL
            END;
            expected_repeated := CASE
                WHEN NEW.source_resource_type = 'PractitionerRole'
                    THEN field_name IN (
                        'network', 'location', 'healthcareService', 'endpoint'
                    )
                WHEN NEW.source_resource_type = 'OrganizationAffiliation'
                    THEN field_name IN (
                        'network', 'location', 'healthcareService', 'endpoint'
                    )
                WHEN NEW.source_resource_type = 'Organization'
                    THEN field_name = 'endpoint'
                WHEN NEW.source_resource_type = 'Location'
                    THEN field_name = 'endpoint'
                WHEN NEW.source_resource_type = 'HealthcareService'
                    THEN field_name IN ('location', 'coverageArea', 'endpoint')
                WHEN NEW.source_resource_type = 'InsurancePlan'
                    THEN field_name IN ('coverageArea', 'network')
                WHEN NEW.source_resource_type = 'Endpoint' THEN false
                ELSE NULL
            END;
            IF NEW.field_path ~ '\\[[0-9]+\\]$' THEN
                array_index := pg_catalog.regexp_replace(
                    NEW.field_path, '^.*\\[([0-9]+)\\]$', '\\1'
                )::integer;
                actual_reference := payload -> field_name -> array_index
                    ->> 'reference';
            ELSE
                actual_reference := payload -> field_name ->> 'reference';
            END IF;
        END IF;
        IF pg_catalog.current_setting({_ql(_ACTION_SETTING)}, TRUE) <> 'witness'
           OR pg_catalog.current_setting({_ql(_ACQUISITION_SETTING)}, TRUE)
                IS DISTINCT FROM NEW.acquisition_id
           OR payload IS NULL OR parent_scope IS DISTINCT FROM NEW.closure_scope
           OR parent_scope_id IS DISTINCT FROM NEW.scope_id
           OR NEW.created_at IS DISTINCT FROM transaction_timestamp()
           OR expected_target_type IS NULL
           OR NEW.target_resource_type IS DISTINCT FROM expected_target_type
           OR (NEW.field_path !~ '^extension\\[' AND (
                expected_repeated IS NULL
                OR expected_repeated IS DISTINCT FROM
                   (NEW.field_path ~ '\\[[0-9]+\\]$')
           ))
           OR actual_reference IS DISTINCT FROM
                NEW.target_resource_type || '/' || NEW.target_resource_id
           OR NEW.edge_sha256 IS DISTINCT FROM {_digest_sql(edge_identity)} THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_edge_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _budget_guard_sql(schema: str) -> tuple[str, ...]:
    acquisition = _qf(schema, _ACQUISITION)
    work_guard = _qf(schema, _WORK_BUDGET_GUARD)
    resource_guard = _qf(schema, _RESOURCE_BUDGET_GUARD)
    edge_guard = _qf(schema, _EDGE_BUDGET_GUARD)
    return (
        f"""
        CREATE FUNCTION {work_guard}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path = pg_catalog AS $guard$
        DECLARE delta record;
        BEGIN
            FOR delta IN
                SELECT acquisition_id, count(*)::bigint AS row_count
                  FROM inserted_work GROUP BY acquisition_id
                  ORDER BY acquisition_id
            LOOP
                UPDATE {acquisition}
                   SET used_work_items = used_work_items + delta.row_count,
                       updated_at = transaction_timestamp()
                 WHERE acquisition_id = delta.acquisition_id
                   AND status = 'building'
                   AND used_work_items + delta.row_count <= max_work_items;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'provider_directory_rooted_graph_work_budget_exceeded'
                        USING ERRCODE = '54000';
                END IF;
            END LOOP;
            RETURN NULL;
        END;
        $guard$;
        """,
        f"""
        CREATE FUNCTION {resource_guard}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path = pg_catalog AS $guard$
        DECLARE delta record;
        BEGIN
            FOR delta IN
                SELECT acquisition_id, count(*)::bigint AS row_count,
                       COALESCE(sum(octet_length(payload_json_text)), 0)::bigint
                           AS payload_bytes
                  FROM inserted_resource GROUP BY acquisition_id
                  ORDER BY acquisition_id
            LOOP
                UPDATE {acquisition}
                   SET used_resource_rows = used_resource_rows + delta.row_count,
                       used_payload_bytes = used_payload_bytes + delta.payload_bytes,
                       updated_at = transaction_timestamp()
                 WHERE acquisition_id = delta.acquisition_id
                   AND status = 'building'
                   AND used_resource_rows + delta.row_count <= max_resource_rows
                   AND used_payload_bytes + delta.payload_bytes <= max_payload_bytes;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'provider_directory_rooted_graph_resource_budget_exceeded'
                        USING ERRCODE = '54000';
                END IF;
            END LOOP;
            RETURN NULL;
        END;
        $guard$;
        """,
        f"""
        CREATE FUNCTION {edge_guard}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path = pg_catalog AS $guard$
        DECLARE delta record;
        BEGIN
            FOR delta IN
                SELECT acquisition_id, count(*)::bigint AS row_count
                  FROM inserted_edge GROUP BY acquisition_id
                  ORDER BY acquisition_id
            LOOP
                UPDATE {acquisition}
                   SET used_edge_rows = used_edge_rows + delta.row_count,
                       updated_at = transaction_timestamp()
                 WHERE acquisition_id = delta.acquisition_id
                   AND status = 'building'
                   AND used_edge_rows + delta.row_count <= max_edge_rows;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'provider_directory_rooted_graph_edge_budget_exceeded'
                        USING ERRCODE = '54000';
                END IF;
            END LOOP;
            RETURN NULL;
        END;
        $guard$;
        """,
    )


def _twin_attempt_guard_sql(schema: str) -> str:
    guard = _qf(schema, _TWIN_ATTEMPT_GUARD)
    acquisition = _qf(schema, _ACQUISITION)
    proof_names = (
        "pending_count",
        "leased_count",
        "completed_count",
        "error_count",
        "resource_count",
        "edge_count",
        "insurance_plan_count",
        "insurance_plan_page_count",
        "used_work_items",
        "used_resource_rows",
        "used_edge_rows",
        "used_payload_bytes",
        "terminal_set_sha256",
        "resource_set_sha256",
        "edge_set_sha256",
        "rooted_graph_sha256",
    )
    identity_fields = (
        "NEW.attempt_contract_id",
        "NEW.storage_contract_id",
        "NEW.first_acquisition_id",
        "NEW.second_acquisition_id",
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
        *tuple(
            f"NEW.{side}_{name}::text"
            for name in proof_names[:12]
            for side in ("first", "second")
        ),
        *tuple(
            f"NEW.{side}_{name}"
            for name in proof_names[12:]
            for side in ("first", "second")
        ),
        "CASE WHEN NEW.matched THEN 'True' ELSE 'False' END",
    )
    expected_id = _digest_identifier_sql(
        "pdrgat_", _TWIN_ATTEMPT_CONTRACT, identity_fields
    )
    first_projection = ", ".join(f"first_root.{name}" for name in proof_names)
    second_projection = ", ".join(f"second_root.{name}" for name in proof_names)
    stored_first = ", ".join(f"NEW.first_{name}" for name in proof_names)
    stored_second = ", ".join(f"NEW.second_{name}" for name in proof_names)
    shared = (
        "storage_contract_id, scope_id, root_source_id, root_endpoint_id, "
        "acquisition_source_id, acquisition_endpoint_id, source_authority_id, "
        "endpoint_signature_sha256, root_dataset_id, root_dataset_variant, "
        "root_publication_contract_id, "
        "root_dataset_hash, root_content_proof_sha256, root_cohort_id, "
        "root_resource_count, connector_id, graph_contract_sha256, "
        "query_contract_sha256, dataset_intent_id, max_work_items, "
        "max_resource_rows, max_edge_rows, max_payload_bytes"
    )
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        first_root record;
        second_root record;
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_twin_attempt_immutable'
                USING ERRCODE = '55000';
        ELSIF TG_OP <> 'INSERT' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_twin_attempt_immutable'
                USING ERRCODE = '55000';
        END IF;
        SELECT * INTO first_root FROM {acquisition}
         WHERE acquisition_id = NEW.first_acquisition_id FOR SHARE;
        SELECT * INTO second_root FROM {acquisition}
         WHERE acquisition_id = NEW.second_acquisition_id FOR SHARE;
        IF first_root.acquisition_id IS NULL
           OR second_root.acquisition_id IS NULL
           OR NEW.first_acquisition_id >= NEW.second_acquisition_id
           OR first_root.status <> 'sealed'
           OR second_root.status <> 'sealed'
           OR first_root.rooted_graph_complete IS NOT TRUE
           OR second_root.rooted_graph_complete IS NOT TRUE
           OR NOT (
                (first_root.acquisition_role = 'baseline'
                 AND second_root.acquisition_role = 'candidate')
                OR
                (first_root.acquisition_role = 'candidate'
                 AND second_root.acquisition_role = 'baseline')
           )
           OR first_root.run_id = second_root.run_id
           OR ROW(first_root.{shared.replace(', ', ', first_root.')})
                IS DISTINCT FROM
              ROW(second_root.{shared.replace(', ', ', second_root.')})
           OR ROW(NEW.storage_contract_id, NEW.scope_id, NEW.root_source_id,
                  NEW.root_endpoint_id, NEW.acquisition_source_id,
                  NEW.acquisition_endpoint_id, NEW.source_authority_id,
                  NEW.endpoint_signature_sha256, NEW.root_dataset_id,
                  NEW.root_dataset_variant, NEW.root_publication_contract_id,
                  NEW.root_dataset_hash,
                  NEW.root_content_proof_sha256, NEW.root_cohort_id,
                  NEW.root_resource_count, NEW.connector_id,
                  NEW.graph_contract_sha256, NEW.query_contract_sha256,
                  NEW.dataset_intent_id, NEW.max_work_items,
                  NEW.max_resource_rows, NEW.max_edge_rows,
                  NEW.max_payload_bytes)
                IS DISTINCT FROM
              ROW(first_root.{shared.replace(', ', ', first_root.')})
           OR ROW({stored_first}) IS DISTINCT FROM ROW({first_projection})
           OR ROW({stored_second}) IS DISTINCT FROM ROW({second_projection})
           OR NEW.matched IS DISTINCT FROM
              (ROW({first_projection}) IS NOT DISTINCT FROM
               ROW({second_projection}))
           OR NEW.attempt_contract_id <> {_ql(_TWIN_ATTEMPT_CONTRACT)}
           OR NEW.attempt_id IS DISTINCT FROM ({expected_id})
           OR NEW.attempted_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_twin_attempt_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _twin_admission_guard_sql(schema: str) -> str:
    guard = _qf(schema, _TWIN_ADMISSION_GUARD)
    attempt = _qf(schema, _TWIN_ATTEMPT)
    acquisition = _qf(schema, _ACQUISITION)
    proof_names = (
        "completed_count",
        "resource_count",
        "edge_count",
        "insurance_plan_count",
        "insurance_plan_page_count",
        "used_work_items",
        "used_resource_rows",
        "used_edge_rows",
        "used_payload_bytes",
        "terminal_set_sha256",
        "resource_set_sha256",
        "edge_set_sha256",
        "rooted_graph_sha256",
    )
    identity_fields = (
        "NEW.admission_contract_id",
        "NEW.storage_contract_id",
        "NEW.attempt_id",
        "NEW.publication_acquisition_id",
        "NEW.comparison_acquisition_id",
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
        *tuple(
            (
                f"NEW.{name}::text"
                if name.endswith("count") or name.startswith("used_")
                else f"NEW.{name}"
            )
            for name in proof_names
        ),
        "CASE WHEN NEW.publication_authority THEN 'True' ELSE 'False' END",
    )
    expected_id = _digest_identifier_sql(
        "pdrgad_", _TWIN_ADMISSION_CONTRACT, identity_fields
    )
    common_names = (
        "storage_contract_id",
        "dataset_intent_id",
        "scope_id",
        "root_source_id",
        "root_endpoint_id",
        "acquisition_source_id",
        "acquisition_endpoint_id",
        "source_authority_id",
        "endpoint_signature_sha256",
        "root_dataset_id",
        "root_dataset_variant",
        "root_publication_contract_id",
        "root_dataset_hash",
        "root_content_proof_sha256",
        "root_cohort_id",
        "root_resource_count",
        "connector_id",
        "graph_contract_sha256",
        "query_contract_sha256",
        "max_work_items",
        "max_resource_rows",
        "max_edge_rows",
        "max_payload_bytes",
    )
    new_common = ", ".join(f"NEW.{name}" for name in common_names)
    attempt_common = ", ".join(f"comparison.{name}" for name in common_names)
    new_proof = ", ".join(f"NEW.{name}" for name in proof_names)
    candidate_proof = ", ".join(f"candidate.{name}" for name in proof_names)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        comparison record;
        candidate record;
        baseline record;
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_twin_admission_immutable'
                USING ERRCODE = '55000';
        ELSIF TG_OP <> 'INSERT' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_twin_admission_immutable'
                USING ERRCODE = '55000';
        END IF;
        SELECT * INTO comparison FROM {attempt}
         WHERE attempt_id = NEW.attempt_id FOR SHARE;
        SELECT * INTO candidate FROM {acquisition}
         WHERE acquisition_id = NEW.publication_acquisition_id FOR SHARE;
        SELECT * INTO baseline FROM {acquisition}
         WHERE acquisition_id = NEW.comparison_acquisition_id FOR SHARE;
        IF comparison.attempt_id IS NULL
           OR comparison.matched IS NOT TRUE
           OR candidate.acquisition_id IS NULL
           OR baseline.acquisition_id IS NULL
           OR candidate.status <> 'sealed' OR baseline.status <> 'sealed'
           OR candidate.acquisition_role <> 'candidate'
           OR baseline.acquisition_role <> 'baseline'
           OR NOT (
                (comparison.first_acquisition_id = candidate.acquisition_id
                 AND comparison.second_acquisition_id = baseline.acquisition_id)
                OR
                (comparison.second_acquisition_id = candidate.acquisition_id
                 AND comparison.first_acquisition_id = baseline.acquisition_id)
           )
           OR ROW({new_common}) IS DISTINCT FROM ROW({attempt_common})
           OR ROW({new_proof}) IS DISTINCT FROM ROW({candidate_proof})
           OR NEW.publication_run_id IS DISTINCT FROM candidate.run_id
           OR NEW.admission_contract_id <> {_ql(_TWIN_ADMISSION_CONTRACT)}
           OR NEW.publication_authority IS NOT TRUE
           OR NEW.admission_id IS DISTINCT FROM ({expected_id})
           OR NEW.admitted_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_twin_admission_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _rooted_relation_proof_sql(schema: str, header: str) -> str:
    resource = _qf(schema, _DATASET_RESOURCE)
    insurance_plan = _qf(schema, _DATASET_INSURANCE_PLAN)
    network_plan = _qf(schema, _DATASET_NETWORK_PLAN)
    affiliation = _qf(schema, _DATASET_AFFILIATION_ORGANIZATION)
    network_pattern = (
        r"(?i)(?:^|/)Organization/([A-Za-z0-9.-]{1,64})"
        r"(?:/_history/[A-Za-z0-9.-]{1,64})?/?(?:[?#].*)?$"
    )
    return f"""
           AND NOT EXISTS (
                SELECT 1 FROM {resource} AS plan
                  LEFT JOIN {insurance_plan} AS projection
                    ON projection.dataset_id = plan.dataset_id
                   AND projection.resource_id = plan.resource_id
                 WHERE plan.dataset_id = {header}.dataset_id
                   AND plan.resource_type = 'InsurancePlan'
                   AND (projection.resource_id IS NULL
                        OR projection.payload_hash <> plan.payload_hash
                        OR projection.payload_json::jsonb IS DISTINCT FROM
                           plan.payload_json::jsonb)
           )
           AND NOT EXISTS (
                SELECT 1 FROM {insurance_plan} AS projection
                  LEFT JOIN {resource} AS plan
                    ON plan.dataset_id = projection.dataset_id
                   AND plan.resource_type = 'InsurancePlan'
                   AND plan.resource_id = projection.resource_id
                 WHERE projection.dataset_id = {header}.dataset_id
                   AND plan.resource_id IS NULL
           )
           AND NOT EXISTS (
                SELECT 1 FROM {resource} AS plan
                 WHERE plan.dataset_id = {header}.dataset_id
                   AND plan.resource_type = 'InsurancePlan'
                   AND plan.payload_json::jsonb ? 'network_refs'
                   AND plan.payload_json::jsonb -> 'network_refs' <> 'null'::jsonb
                   AND pg_catalog.jsonb_typeof(
                           plan.payload_json::jsonb -> 'network_refs'
                       ) <> 'array'
           )
           AND NOT EXISTS (
                SELECT 1 FROM {resource} AS plan
                CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
                    CASE WHEN pg_catalog.jsonb_typeof(
                                  plan.payload_json::jsonb -> 'network_refs'
                              ) = 'array'
                         THEN plan.payload_json::jsonb -> 'network_refs'
                         ELSE '[]'::jsonb END
                ) AS reference(value)
                 WHERE plan.dataset_id = {header}.dataset_id
                   AND plan.resource_type = 'InsurancePlan'
                   AND (
                        pg_catalog.jsonb_typeof(reference.value) <> 'string'
                        OR NOT (
                            pg_catalog.btrim(reference.value #>> '{{}}')
                                ~ '^[A-Za-z0-9.-]{{1,64}}$'
                            OR pg_catalog.btrim(reference.value #>> '{{}}')
                                ~ {_ql(network_pattern)}
                        )
                   )
           )
           AND NOT EXISTS (
                WITH expected AS MATERIALIZED (
                    SELECT DISTINCT plan.resource_id AS plan_id,
                           CASE
                             WHEN pg_catalog.btrim(reference.value #>> '{{}}')
                                  ~ '^[A-Za-z0-9.-]{{1,64}}$'
                             THEN pg_catalog.btrim(reference.value #>> '{{}}')
                             ELSE pg_catalog.substring(
                                  pg_catalog.btrim(reference.value #>> '{{}}'),
                                  {_ql(network_pattern)})
                           END AS network_id
                      FROM {resource} AS plan
                     CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
                        CASE WHEN pg_catalog.jsonb_typeof(
                                      plan.payload_json::jsonb -> 'network_refs'
                                  ) = 'array'
                             THEN plan.payload_json::jsonb -> 'network_refs'
                             ELSE '[]'::jsonb END
                     ) AS reference(value)
                     WHERE plan.dataset_id = {header}.dataset_id
                       AND plan.resource_type = 'InsurancePlan'
                )
                SELECT 1 FROM expected
                  LEFT JOIN {resource} AS organization
                    ON organization.dataset_id = {header}.dataset_id
                   AND organization.resource_type = 'Organization'
                   AND organization.resource_id = expected.network_id
                 WHERE organization.resource_id IS NULL
           )
           AND NOT EXISTS (
                WITH expected AS MATERIALIZED (
                    SELECT DISTINCT
                           CASE
                             WHEN pg_catalog.btrim(reference.value #>> '{{}}')
                                  ~ '^[A-Za-z0-9.-]{{1,64}}$'
                             THEN pg_catalog.btrim(reference.value #>> '{{}}')
                             ELSE pg_catalog.substring(
                                  pg_catalog.btrim(reference.value #>> '{{}}'),
                                  {_ql(network_pattern)})
                           END AS network_id,
                           plan.resource_id AS plan_id
                      FROM {resource} AS plan
                     CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
                        CASE WHEN pg_catalog.jsonb_typeof(
                                      plan.payload_json::jsonb -> 'network_refs'
                                  ) = 'array'
                             THEN plan.payload_json::jsonb -> 'network_refs'
                             ELSE '[]'::jsonb END
                     ) AS reference(value)
                     WHERE plan.dataset_id = {header}.dataset_id
                       AND plan.resource_type = 'InsurancePlan'
                )
                (SELECT network_id, plan_id FROM expected
                 EXCEPT
                 SELECT edge.network_resource_id,
                        edge.insurance_plan_resource_id
                   FROM {network_plan} AS edge
                  WHERE edge.dataset_id = {header}.dataset_id)
                UNION ALL
                (SELECT edge.network_resource_id,
                        edge.insurance_plan_resource_id
                   FROM {network_plan} AS edge
                  WHERE edge.dataset_id = {header}.dataset_id
                 EXCEPT
                 SELECT network_id, plan_id FROM expected)
           )
           AND NOT EXISTS (
                SELECT 1 FROM {resource} AS item
                 WHERE item.dataset_id = {header}.dataset_id
                   AND item.resource_type = 'OrganizationAffiliation'
                   AND item.payload_json::jsonb
                           -> 'participating_organization_ref' IS NOT NULL
                   AND item.payload_json::jsonb
                           -> 'participating_organization_ref' <> 'null'::jsonb
                   AND (
                        pg_catalog.jsonb_typeof(
                            item.payload_json::jsonb
                                -> 'participating_organization_ref') <> 'string'
                        OR (
                            NULLIF(pg_catalog.btrim(
                                item.payload_json::jsonb
                                    ->> 'participating_organization_ref'), '')
                                IS NOT NULL
                            AND NOT (
                                pg_catalog.btrim(item.payload_json::jsonb
                                    ->> 'participating_organization_ref')
                                    ~ '^[A-Za-z0-9.-]{{1,64}}$'
                                OR pg_catalog.btrim(item.payload_json::jsonb
                                    ->> 'participating_organization_ref')
                                    ~ {_ql(network_pattern)}
                            )
                        )
                   )
           )
           AND NOT EXISTS (
                WITH expected AS MATERIALIZED (
                    SELECT DISTINCT item.resource_id AS affiliation_id,
                           CASE
                             WHEN pg_catalog.btrim(item.payload_json::jsonb
                                      ->> 'participating_organization_ref')
                                  ~ '^[A-Za-z0-9.-]{{1,64}}$'
                             THEN pg_catalog.btrim(item.payload_json::jsonb
                                      ->> 'participating_organization_ref')
                             ELSE pg_catalog.substring(
                                  pg_catalog.btrim(item.payload_json::jsonb
                                      ->> 'participating_organization_ref'),
                                  {_ql(network_pattern)})
                           END AS organization_id
                      FROM {resource} AS item
                     WHERE item.dataset_id = {header}.dataset_id
                       AND item.resource_type = 'OrganizationAffiliation'
                       AND NULLIF(pg_catalog.btrim(item.payload_json::jsonb
                               ->> 'participating_organization_ref'), '')
                           IS NOT NULL
                )
                SELECT 1 FROM expected
                  LEFT JOIN {resource} AS organization
                    ON organization.dataset_id = {header}.dataset_id
                   AND organization.resource_type = 'Organization'
                   AND organization.resource_id = expected.organization_id
                 WHERE organization.resource_id IS NULL
           )
           AND NOT EXISTS (
                WITH expected AS MATERIALIZED (
                    SELECT DISTINCT
                           CASE
                             WHEN pg_catalog.btrim(item.payload_json::jsonb
                                      ->> 'participating_organization_ref')
                                  ~ '^[A-Za-z0-9.-]{{1,64}}$'
                             THEN pg_catalog.btrim(item.payload_json::jsonb
                                      ->> 'participating_organization_ref')
                             ELSE pg_catalog.substring(
                                  pg_catalog.btrim(item.payload_json::jsonb
                                      ->> 'participating_organization_ref'),
                                  {_ql(network_pattern)})
                           END AS organization_id,
                           item.resource_id AS affiliation_id
                      FROM {resource} AS item
                     WHERE item.dataset_id = {header}.dataset_id
                       AND item.resource_type = 'OrganizationAffiliation'
                       AND NULLIF(pg_catalog.btrim(item.payload_json::jsonb
                               ->> 'participating_organization_ref'), '')
                           IS NOT NULL
                )
                (SELECT organization_id, affiliation_id FROM expected
                 EXCEPT
                 SELECT edge.participating_organization_resource_id,
                        edge.affiliation_resource_id
                   FROM {affiliation} AS edge
                  WHERE edge.dataset_id = {header}.dataset_id)
                UNION ALL
                (SELECT edge.participating_organization_resource_id,
                        edge.affiliation_resource_id
                   FROM {affiliation} AS edge
                  WHERE edge.dataset_id = {header}.dataset_id
                 EXCEPT
                 SELECT organization_id, affiliation_id FROM expected)
           )
    """


def _rooted_intrinsic_valid_function_sql(schema: str) -> str:
    valid = _qf(schema, _ROOTED_INTRINSIC_VALID)
    header_ref = _qf(schema, _ROOTED_DATASET)
    provenance = _qf(schema, _ROOTED_PROVENANCE)
    parent = _qf(schema, _DATASET)
    resource = _qf(schema, _DATASET_RESOURCE)
    admission = _qf(schema, _TWIN_ADMISSION)
    attempt = _qf(schema, _TWIN_ATTEMPT)
    acquisition = _qf(schema, _ACQUISITION)
    raw_resource = _qf(schema, _RESOURCE)
    work = _qf(schema, _WORK)
    source = _qf(schema, _SOURCE)
    endpoint = _qf(schema, _ENDPOINT)
    legacy_header = _qf(schema, _LEGACY_DATASET)
    legacy_valid = _qf(schema, _LEGACY_VALID)
    expected_dataset_id = _digest_identifier_sql(
        "pdrgpd_", _PUBLICATION_CONTRACT, _rooted_identity_tail("header")
    )
    expected_root_id = _digest_identifier_sql(
        "pdrgpr_", _PUBLICATION_ROOT_CONTRACT, _rooted_identity_tail("header")
    )
    expected_metadata = _rooted_expected_metadata_sql("header", "admitted")
    dataset_hash = _dataset_hash_sql(resource, "header.dataset_id")
    count_columns = {
        "Practitioner": "practitioner_resource_count",
        "PractitionerRole": "practitioner_role_resource_count",
        "OrganizationAffiliation": "organization_affiliation_resource_count",
        "Organization": "organization_resource_count",
        "Location": "location_resource_count",
        "HealthcareService": "healthcare_service_resource_count",
        "InsurancePlan": "insurance_plan_resource_count",
        "Endpoint": "endpoint_resource_count",
    }
    count_predicates = "\n".join(
        f"           AND (SELECT count(*) FROM {resource} AS counted "
        f"WHERE counted.dataset_id = header.dataset_id AND "
        f"counted.resource_type = {_ql(resource_type)}) = "
        f"header.{column_name}"
        for resource_type, column_name in count_columns.items()
    )
    relations = _rooted_relation_proof_sql(schema, "header")
    return f"""
    CREATE FUNCTION {valid}(candidate_dataset_id text) RETURNS boolean
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog
    AS $function$
        SELECT pg_catalog.count(*) = 1
          FROM {header_ref} AS header
          JOIN {parent} AS endpoint_dataset
            ON endpoint_dataset.dataset_id = header.dataset_id
          JOIN {admission} AS admitted
            ON admitted.admission_id = header.admission_id
          JOIN {attempt} AS comparison
            ON comparison.attempt_id = header.attempt_id
          JOIN {acquisition} AS candidate
            ON candidate.acquisition_id = header.publication_acquisition_id
          JOIN {source} AS graph_source
            ON graph_source.source_id = header.source_id
          JOIN {endpoint} AS graph_endpoint
            ON graph_endpoint.endpoint_id = header.endpoint_id
          JOIN {source} AS origin_source
            ON origin_source.source_id = header.practitioner_origin_source_id
          JOIN {endpoint} AS origin_endpoint
            ON origin_endpoint.endpoint_id = header.practitioner_origin_endpoint_id
          JOIN {parent} AS root_parent
            ON root_parent.dataset_id = header.root_dataset_id
         WHERE header.dataset_id = candidate_dataset_id
           AND header.dataset_id = ({expected_dataset_id})
           AND header.acquisition_root_run_id = ({expected_root_id})
           AND header.publication_contract_id = {_ql(_PUBLICATION_CONTRACT)}
           AND header.publication_kind = {_ql(_PUBLICATION_KIND)}
           AND header.source_id = {_ql(_ROOTED_SOURCE_ID)}
           AND header.endpoint_id = {_ql(_ROOTED_ENDPOINT_ID)}
           AND header.acquisition_source_id = header.source_id
           AND header.acquisition_endpoint_id = header.endpoint_id
           AND header.source_authority_id = {_ql(_SOURCE_AUTHORITY)}
           AND header.endpoint_signature_sha256 =
               {_ql(_ROOTED_ENDPOINT_SIGNATURE)}
           AND header.practitioner_origin_source_id = {_ql(_LEGACY_SOURCE_ID)}
           AND header.practitioner_origin_endpoint_id =
               {_ql(_LEGACY_ENDPOINT_ID)}
           AND header.previous_dataset_id = header.root_dataset_id
           AND header.resource_hash_contract = {_ql(_HASH_CONTRACT)}
           AND header.cohort_complete IS TRUE
           AND header.rooted_graph_complete IS TRUE
           AND header.endpoint_collection_complete IS FALSE
           AND header.endpoint_complete IS FALSE
           AND header.root_practitioner_resource_count =
               header.practitioner_resource_count
           AND admitted.admission_contract_id = {_ql(_TWIN_ADMISSION_CONTRACT)}
           AND admitted.publication_authority IS TRUE
           AND admitted.attempt_id = comparison.attempt_id
           AND admitted.publication_acquisition_id =
               header.publication_acquisition_id
           AND admitted.comparison_acquisition_id =
               header.comparison_acquisition_id
           AND admitted.publication_run_id = header.publication_run_id
           AND admitted.dataset_intent_id = header.dataset_intent_id
           AND admitted.scope_id = header.scope_id
           AND admitted.root_source_id = header.root_source_id
           AND admitted.root_endpoint_id = header.root_endpoint_id
           AND admitted.acquisition_source_id = header.acquisition_source_id
           AND admitted.acquisition_endpoint_id = header.acquisition_endpoint_id
           AND admitted.source_authority_id = header.source_authority_id
           AND admitted.endpoint_signature_sha256 =
               header.endpoint_signature_sha256
           AND admitted.root_dataset_id = header.root_dataset_id
           AND admitted.root_dataset_variant = header.root_dataset_variant
           AND admitted.root_publication_contract_id =
               header.root_publication_contract_id
           AND admitted.root_dataset_hash = header.root_dataset_hash
           AND admitted.root_content_proof_sha256 =
               header.root_content_proof_sha256
           AND admitted.root_cohort_id = header.root_cohort_id
           AND admitted.root_resource_count =
               header.root_practitioner_resource_count
           AND admitted.connector_id = header.connector_id
           AND admitted.storage_contract_id = header.storage_contract_id
           AND admitted.graph_contract_sha256 = header.graph_contract_sha256
           AND admitted.query_contract_sha256 = header.query_contract_sha256
           AND admitted.max_work_items = header.max_work_items
           AND admitted.max_resource_rows = header.max_resource_rows
           AND admitted.max_edge_rows = header.max_edge_rows
           AND admitted.max_payload_bytes = header.max_payload_bytes
           AND admitted.used_work_items = header.used_work_items
           AND admitted.used_resource_rows = header.used_resource_rows
           AND admitted.used_edge_rows = header.used_edge_rows
           AND admitted.used_payload_bytes = header.used_payload_bytes
           AND admitted.completed_count = header.completed_count
           AND admitted.resource_count = header.graph_resource_count
           AND admitted.edge_count = header.graph_edge_count
           AND admitted.insurance_plan_count =
               header.census_insurance_plan_count
           AND admitted.insurance_plan_page_count =
               header.insurance_plan_page_count
           AND admitted.terminal_set_sha256 = header.terminal_set_sha256
           AND admitted.resource_set_sha256 = header.resource_set_sha256
           AND admitted.edge_set_sha256 = header.edge_set_sha256
           AND admitted.rooted_graph_sha256 = header.rooted_graph_sha256
           AND comparison.matched IS TRUE
           AND candidate.status = 'sealed'
           AND candidate.rooted_graph_complete IS TRUE
           AND candidate.endpoint_collection_complete IS FALSE
           AND candidate.endpoint_complete IS FALSE
           AND candidate.acquisition_role = 'candidate'
           AND candidate.root_publication_contract_id =
               header.root_publication_contract_id
           AND candidate.run_id = header.publication_run_id
           AND candidate.rooted_graph_sha256 = header.rooted_graph_sha256
           AND candidate.used_work_items = header.used_work_items
           AND candidate.used_resource_rows = header.used_resource_rows
           AND candidate.used_edge_rows = header.used_edge_rows
           AND candidate.used_payload_bytes = header.used_payload_bytes
           AND graph_source.endpoint_id = header.endpoint_id
           AND graph_source.canonical_api_base = {_ql(_API_BASE)}
           AND graph_source.metadata_json::jsonb
                   ->> 'provider_directory_authority_id' =
               header.source_authority_id
           AND graph_endpoint.canonical_api_base = {_ql(_API_BASE)}
           AND graph_endpoint.endpoint_signature_hash =
               header.endpoint_signature_sha256
           AND graph_endpoint.metadata_json::jsonb ->> 'authority_id' =
               header.source_authority_id
           AND origin_source.endpoint_id = header.practitioner_origin_endpoint_id
           AND origin_source.canonical_api_base = {_ql(_API_BASE)}
           AND origin_source.metadata_json::jsonb
                   ->> 'provider_directory_authority_id' =
               header.source_authority_id
           AND origin_endpoint.canonical_api_base = {_ql(_API_BASE)}
           AND origin_endpoint.endpoint_signature_hash =
               {_ql(_LEGACY_ENDPOINT_SIGNATURE)}
           AND origin_endpoint.metadata_json::jsonb ->> 'authority_id' =
               header.source_authority_id
           AND root_parent.endpoint_id = header.root_endpoint_id
           AND root_parent.dataset_hash = header.root_dataset_hash
           AND root_parent.status IN ('published', 'superseded')
           AND (
                (header.root_dataset_variant = {_ql(_LEGACY_VARIANT)}
                 AND header.root_publication_contract_id =
                     {_ql(_LEGACY_PUBLICATION_CONTRACT)}
                 AND header.root_source_id = {_ql(_LEGACY_SOURCE_ID)}
                 AND header.root_endpoint_id = {_ql(_LEGACY_ENDPOINT_ID)}
                 AND EXISTS (
                    SELECT 1 FROM {legacy_header} AS root_header
                     WHERE root_header.dataset_id = header.root_dataset_id
                       AND root_header.source_id = header.root_source_id
                       AND root_header.endpoint_id = header.root_endpoint_id
                       AND root_header.dataset_hash = header.root_dataset_hash
                       AND root_header.resource_count =
                           header.root_practitioner_resource_count
                       AND root_header.cohort_id = header.root_cohort_id
                       AND root_header.terminal_set_sha256 =
                           header.root_content_proof_sha256
                       AND root_header.status IN ('published', 'superseded')
                       AND {legacy_valid}(root_header.dataset_id)
                 ))
                OR
                (header.root_dataset_variant = {_ql(_ROOTED_VARIANT)}
                 AND header.root_publication_contract_id =
                     {_ql(_PUBLICATION_CONTRACT)}
                 AND header.root_source_id = header.source_id
                 AND header.root_endpoint_id = header.endpoint_id
                 AND EXISTS (
                    SELECT 1 FROM {header_ref} AS root_header
                     WHERE root_header.dataset_id = header.root_dataset_id
                       AND root_header.source_id = header.root_source_id
                       AND root_header.endpoint_id = header.root_endpoint_id
                       AND root_header.dataset_hash = header.root_dataset_hash
                       AND root_header.practitioner_resource_count =
                           header.root_practitioner_resource_count
                       AND root_header.root_cohort_id = header.root_cohort_id
                       AND root_header.root_content_proof_sha256 =
                           header.root_content_proof_sha256
                       AND root_header.status IN ('published', 'superseded')
                 ))
           )
           AND endpoint_dataset.endpoint_id = header.endpoint_id
           AND endpoint_dataset.import_run_id = header.publication_run_id
           AND endpoint_dataset.acquisition_root_run_id =
               header.acquisition_root_run_id
           AND endpoint_dataset.previous_dataset_id = header.previous_dataset_id
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
           AND (SELECT count(*) FROM {resource} AS member
                 WHERE member.dataset_id = header.dataset_id) =
               header.resource_count
           AND NOT EXISTS (
                SELECT 1 FROM {resource} AS member
                 WHERE member.dataset_id = header.dataset_id
                   AND (member.resource_type NOT IN
                       ('InsurancePlan','PractitionerRole','Practitioner',
                        'Organization','Location','HealthcareService',
                        'OrganizationAffiliation','Endpoint')
                        OR member.acquired_resource_sha256 IS NOT NULL
                        OR pg_catalog.jsonb_typeof(member.payload_json::jsonb)
                           <> 'object')
           )
{count_predicates}
           AND (header.status = 'building' OR ({dataset_hash}) = header.dataset_hash)
           AND (SELECT count(*) FROM {provenance} AS evidence
                 WHERE evidence.dataset_id = header.dataset_id) =
               header.resource_count
           AND NOT EXISTS (
                SELECT 1 FROM {provenance} AS evidence
                  LEFT JOIN {resource} AS member
                    ON member.dataset_id = evidence.dataset_id
                   AND member.resource_type = evidence.resource_type
                   AND member.resource_id = evidence.resource_id
                 WHERE evidence.dataset_id = header.dataset_id
                   AND (member.dataset_id IS NULL
                        OR member.payload_hash <>
                           evidence.published_payload_hash
                        OR evidence.root_dataset_id <> header.root_dataset_id
                        OR evidence.publication_acquisition_id <>
                           header.publication_acquisition_id)
           )
           AND NOT EXISTS (
                SELECT 1 FROM {resource} AS member
                  LEFT JOIN {provenance} AS evidence
                    ON evidence.dataset_id = member.dataset_id
                   AND evidence.resource_type = member.resource_type
                   AND evidence.resource_id = member.resource_id
                 WHERE member.dataset_id = header.dataset_id
                   AND evidence.dataset_id IS NULL
           )
           AND NOT EXISTS (
                SELECT 1 FROM {resource} AS member
                  JOIN {provenance} AS evidence
                    ON evidence.dataset_id = member.dataset_id
                   AND evidence.resource_type = member.resource_type
                   AND evidence.resource_id = member.resource_id
                  LEFT JOIN {resource} AS root_member
                    ON root_member.dataset_id = header.root_dataset_id
                   AND root_member.resource_type = 'Practitioner'
                   AND root_member.resource_id = member.resource_id
                 WHERE member.dataset_id = header.dataset_id
                   AND member.resource_type = 'Practitioner'
                   AND (evidence.origin_kind <> 'root_practitioner'
                        OR root_member.dataset_id IS NULL
                        OR root_member.payload_hash <> member.payload_hash
                        OR root_member.payload_json::jsonb IS DISTINCT FROM
                           member.payload_json::jsonb)
           )
           AND NOT EXISTS (
                SELECT root_member.resource_id
                  FROM {resource} AS root_member
                 WHERE root_member.dataset_id = header.root_dataset_id
                   AND root_member.resource_type = 'Practitioner'
                EXCEPT
                SELECT member.resource_id FROM {resource} AS member
                 WHERE member.dataset_id = header.dataset_id
                   AND member.resource_type = 'Practitioner'
           )
           AND NOT EXISTS (
                SELECT 1 FROM {provenance} AS evidence
                  LEFT JOIN {raw_resource} AS raw
                    ON raw.acquisition_id = evidence.publication_acquisition_id
                   AND raw.query_id = evidence.query_id
                   AND raw.attempt = evidence.attempt
                   AND raw.resource_type = evidence.resource_type
                   AND raw.resource_id = evidence.resource_id
                   AND raw.payload_sha256 = evidence.source_payload_sha256
                  LEFT JOIN {work} AS query
                    ON query.acquisition_id = raw.acquisition_id
                   AND query.query_id = raw.query_id
                   AND query.attempt_count = raw.attempt
                   AND query.status = 'completed'
                 WHERE evidence.dataset_id = header.dataset_id
                   AND evidence.origin_kind = 'rooted_graph'
                   AND (raw.acquisition_id IS NULL
                        OR query.acquisition_id IS NULL
                        OR raw.closure_scope NOT IN ('root','plan')
                        OR raw.resource_type = 'Practitioner')
           )
           AND NOT EXISTS (
                SELECT raw.resource_type, raw.resource_id
                  FROM {raw_resource} AS raw
                  JOIN {work} AS query
                    ON query.acquisition_id = raw.acquisition_id
                   AND query.query_id = raw.query_id
                   AND query.attempt_count = raw.attempt
                   AND query.status = 'completed'
                 WHERE raw.acquisition_id = header.publication_acquisition_id
                   AND raw.closure_scope IN ('root','plan')
                   AND raw.resource_type <> 'Practitioner'
                EXCEPT
                SELECT evidence.resource_type, evidence.resource_id
                  FROM {provenance} AS evidence
                 WHERE evidence.dataset_id = header.dataset_id
                   AND evidence.origin_kind = 'rooted_graph'
           )
           AND NOT EXISTS (
                SELECT 1 FROM {raw_resource} AS raw
                  JOIN {work} AS query
                    ON query.acquisition_id = raw.acquisition_id
                   AND query.query_id = raw.query_id
                   AND query.attempt_count = raw.attempt
                   AND query.status = 'completed'
                 WHERE raw.acquisition_id = header.publication_acquisition_id
                   AND raw.closure_scope IN ('root','plan')
                   AND raw.resource_type <> 'Practitioner'
                 GROUP BY raw.resource_type, raw.resource_id
                HAVING count(DISTINCT raw.payload_sha256) <> 1
           )
{relations}
        ;
    $function$;
    """


def _rooted_official_lineage_current_function_sql(schema: str) -> str:
    lineage_current = _qf(schema, _ROOTED_OFFICIAL_LINEAGE_CURRENT)
    rooted_header = _qf(schema, _ROOTED_DATASET)
    legacy_header = _qf(schema, _LEGACY_DATASET)
    cohort = _qf(schema, _LEGACY_COHORT)
    official_dataset = _qf(schema, _DATASET)
    intrinsic_valid = _qf(schema, _ROOTED_INTRINSIC_VALID)
    legacy_valid = _qf(schema, _LEGACY_VALID)
    return f"""
    CREATE FUNCTION {lineage_current}(candidate_dataset_id text) RETURNS boolean
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog
    AS $function$
        WITH RECURSIVE lineage AS (
            SELECT header.dataset_id, header.root_dataset_id,
                   header.root_dataset_variant,
                   ARRAY[header.dataset_id]::text[] AS visited_dataset_ids,
                   FALSE AS cycle_detected, 1 AS depth
              FROM {rooted_header} AS header
             WHERE header.dataset_id = candidate_dataset_id
            UNION ALL
            SELECT parent.dataset_id, parent.root_dataset_id,
                   parent.root_dataset_variant,
                   lineage.visited_dataset_ids || parent.dataset_id,
                   parent.dataset_id = ANY(lineage.visited_dataset_ids),
                   lineage.depth + 1
              FROM lineage
              JOIN {rooted_header} AS parent
                ON lineage.root_dataset_variant = {_ql(_ROOTED_VARIANT)}
               AND parent.dataset_id = lineage.root_dataset_id
             WHERE lineage.cycle_detected IS FALSE
               AND lineage.depth < 1024
        ), terminal AS (
            SELECT legacy.dataset_id
              FROM lineage
              JOIN {rooted_header} AS child
                ON child.dataset_id = lineage.dataset_id
              JOIN {legacy_header} AS legacy
                ON lineage.root_dataset_variant = {_ql(_LEGACY_VARIANT)}
               AND legacy.dataset_id = lineage.root_dataset_id
               AND legacy.source_id = child.root_source_id
               AND legacy.endpoint_id = child.root_endpoint_id
               AND legacy.publication_contract_id =
                   child.root_publication_contract_id
               AND legacy.dataset_hash = child.root_dataset_hash
               AND legacy.resource_count = child.root_practitioner_resource_count
               AND legacy.cohort_id = child.root_cohort_id
               AND legacy.terminal_set_sha256 =
                   child.root_content_proof_sha256
              JOIN {cohort} AS official_cohort
                ON official_cohort.cohort_id = legacy.cohort_id
               AND official_cohort.contract_id =
                   {_ql(_OFFICIAL_COHORT_CONTRACT)}
               AND official_cohort.authority_id = {_ql(_SOURCE_AUTHORITY)}
               AND official_cohort.official_source_id =
                   {_ql(_OFFICIAL_SOURCE_ID)}
               AND official_cohort.resource_type = 'Practitioner'
               AND official_cohort.cohort_complete IS TRUE
               AND official_cohort.endpoint_collection_complete IS FALSE
               AND official_cohort.endpoint_complete IS FALSE
              JOIN {official_dataset} AS official
                ON official.dataset_id = official_cohort.official_dataset_id
               AND official.endpoint_id = official_cohort.official_endpoint_id
               AND official.acquisition_root_run_id =
                   official_cohort.official_acquisition_root_run_id
               AND official.dataset_hash = official_cohort.official_dataset_hash
               AND official.status = 'published'
               AND official.is_current IS TRUE
             WHERE lineage.cycle_detected IS FALSE
               AND {legacy_valid}(legacy.dataset_id)
        )
        SELECT (SELECT count(*) FROM lineage WHERE depth = 1) = 1
           AND NOT EXISTS (
                SELECT 1 FROM lineage
                 WHERE cycle_detected IS TRUE
                    OR {intrinsic_valid}(dataset_id) IS DISTINCT FROM TRUE
           )
           AND NOT EXISTS (
                SELECT 1 FROM lineage
                 WHERE root_dataset_variant = {_ql(_ROOTED_VARIANT)}
                   AND (
                        depth >= 1024
                        OR (SELECT count(*) FROM {rooted_header} AS parent
                             WHERE parent.dataset_id = lineage.root_dataset_id) <> 1
                   )
           )
           AND (SELECT count(*) FROM terminal) = 1;
    $function$;
    """


def _rooted_valid_function_sql(schema: str) -> str:
    intrinsic_valid = _qf(schema, _ROOTED_INTRINSIC_VALID)
    lineage_current = _qf(schema, _ROOTED_OFFICIAL_LINEAGE_CURRENT)
    valid = _qf(schema, _ROOTED_VALID)
    return f"""
    CREATE FUNCTION {valid}(candidate_dataset_id text) RETURNS boolean
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog
    AS $function$
        SELECT {intrinsic_valid}(candidate_dataset_id)
           AND {lineage_current}(candidate_dataset_id);
    $function$;
    """


def _rooted_ready_function_sql(schema: str) -> str:
    header = _qf(schema, _ROOTED_DATASET)
    parent = _qf(schema, _DATASET)
    valid = _qf(schema, _ROOTED_VALID)
    ready = _qf(schema, _ROOTED_READY)
    return f"""
    CREATE FUNCTION {ready}(candidate_dataset_id text) RETURNS boolean
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog
    AS $function$
        SELECT EXISTS (
            SELECT 1 FROM {header} AS header
              JOIN {parent} AS endpoint_dataset
                ON endpoint_dataset.dataset_id = header.dataset_id
             WHERE header.dataset_id = candidate_dataset_id
               AND header.status = 'published'
               AND header.is_current IS TRUE
               AND endpoint_dataset.status = 'published'
               AND endpoint_dataset.is_current IS TRUE
               AND {valid}(header.dataset_id)
        );
    $function$;
    """


def _rooted_header_guard_sql(schema: str) -> str:
    guard = _qf(schema, _ROOTED_DATASET_GUARD)
    intrinsic_valid = _qf(schema, _ROOTED_INTRINSIC_VALID)
    valid = _qf(schema, _ROOTED_VALID)
    ready = _qf(schema, _ROOTED_READY)
    immutable_names = (
        "dataset_id",
        "publication_contract_id",
        "publication_kind",
        "admission_id",
        "attempt_id",
        "publication_acquisition_id",
        "comparison_acquisition_id",
        "publication_run_id",
        "source_id",
        "endpoint_id",
        "acquisition_source_id",
        "acquisition_endpoint_id",
        "source_authority_id",
        "root_dataset_variant",
        "root_publication_contract_id",
        "root_source_id",
        "root_endpoint_id",
        "practitioner_origin_source_id",
        "practitioner_origin_endpoint_id",
        "endpoint_signature_sha256",
        "scope_id",
        "dataset_intent_id",
        "acquisition_root_run_id",
        "semantic_projection_as_of",
        "operation_key",
        "root_dataset_id",
        "root_dataset_hash",
        "root_content_proof_sha256",
        "root_cohort_id",
        "root_practitioner_resource_count",
        "connector_id",
        "storage_contract_id",
        "graph_contract_sha256",
        "query_contract_sha256",
        "max_work_items",
        "max_resource_rows",
        "max_edge_rows",
        "max_payload_bytes",
        "used_work_items",
        "used_resource_rows",
        "used_edge_rows",
        "used_payload_bytes",
        "completed_count",
        "graph_resource_count",
        "graph_edge_count",
        "census_insurance_plan_count",
        "insurance_plan_page_count",
        "terminal_set_sha256",
        "resource_set_sha256",
        "edge_set_sha256",
        "rooted_graph_sha256",
        "previous_dataset_id",
        "resource_count",
        "practitioner_resource_count",
        "practitioner_role_resource_count",
        "organization_affiliation_resource_count",
        "organization_resource_count",
        "location_resource_count",
        "healthcare_service_resource_count",
        "insurance_plan_resource_count",
        "endpoint_resource_count",
        "resource_hash_contract",
        "cohort_complete",
        "rooted_graph_complete",
        "endpoint_collection_complete",
        "endpoint_complete",
        "created_at",
    )
    new_values = ", ".join(f"NEW.{name}" for name in immutable_names)
    old_values = ", ".join(f"OLD.{name}" for name in immutable_names)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_dataset_immutable'
                USING ERRCODE = '55000';
        ELSIF TG_WHEN = 'AFTER' THEN
            IF {intrinsic_valid}(NEW.dataset_id) IS DISTINCT FROM TRUE
               OR (NEW.status IN ('validated', 'published')
                   AND {valid}(NEW.dataset_id) IS DISTINCT FROM TRUE)
               OR (NEW.status = 'published'
                   AND {ready}(NEW.dataset_id) IS DISTINCT FROM TRUE) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_dataset_invalid'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        ELSIF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_dataset_immutable'
                USING ERRCODE = '55000';
        ELSIF TG_OP = 'INSERT' THEN
            IF NEW.status <> 'building' OR NEW.is_current IS NOT FALSE
               OR NEW.dataset_hash IS NOT NULL
               OR NEW.validated_at IS NOT NULL
               OR NEW.published_at IS NOT NULL
               OR NEW.superseded_at IS NOT NULL
               OR NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_dataset_insert_invalid'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        IF ROW({new_values}) IS DISTINCT FROM ROW({old_values})
           OR (OLD.dataset_hash IS NOT NULL
               AND NEW.dataset_hash IS DISTINCT FROM OLD.dataset_hash)
           OR NOT (
                (NEW.status = OLD.status
                 AND NEW.is_current IS NOT DISTINCT FROM OLD.is_current
                 AND NEW.dataset_hash IS NOT DISTINCT FROM OLD.dataset_hash
                 AND NEW.validated_at IS NOT DISTINCT FROM OLD.validated_at
                 AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
                 AND NEW.superseded_at IS NOT DISTINCT FROM OLD.superseded_at)
                OR
                (OLD.status = 'building' AND NEW.status = 'validated'
                 AND OLD.dataset_hash IS NULL
                 AND NEW.dataset_hash ~ '^[0-9a-f]{{64}}$'
                 AND NEW.is_current IS FALSE
                 AND NEW.validated_at IS NOT DISTINCT FROM transaction_timestamp()
                 AND NEW.published_at IS NULL
                 AND NEW.superseded_at IS NULL)
                OR
                (OLD.status = 'validated' AND NEW.status = 'published'
                 AND NEW.dataset_hash = OLD.dataset_hash
                 AND NEW.is_current IS TRUE
                 AND NEW.validated_at = OLD.validated_at
                 AND NEW.published_at IS NOT DISTINCT FROM transaction_timestamp()
                 AND NEW.superseded_at IS NULL)
                OR
                (OLD.status = 'published' AND NEW.status = 'superseded'
                 AND NEW.dataset_hash = OLD.dataset_hash
                 AND NEW.is_current IS FALSE
                 AND NEW.validated_at = OLD.validated_at
                 AND NEW.published_at = OLD.published_at
                 AND NEW.superseded_at IS NOT DISTINCT FROM transaction_timestamp())
           ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_dataset_transition_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _rooted_provenance_guard_sql(schema: str) -> str:
    guard = _qf(schema, _ROOTED_PROVENANCE_GUARD)
    header = _qf(schema, _ROOTED_DATASET)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_provenance_immutable'
                USING ERRCODE = '55000';
        ELSIF TG_OP <> 'INSERT' THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_provenance_immutable'
                USING ERRCODE = '55000';
        ELSIF NOT EXISTS (
            SELECT 1 FROM {header} AS parent
             WHERE parent.dataset_id = NEW.dataset_id
               AND parent.status = 'building'
               AND parent.is_current IS FALSE
               AND parent.root_dataset_id = NEW.root_dataset_id
               AND parent.publication_acquisition_id =
                   NEW.publication_acquisition_id
        ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_provenance_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _rooted_dependency_guard_sql(schema: str) -> str:
    guard = _qf(schema, _ROOTED_DEPENDENCY_GUARD)
    header = _qf(schema, _ROOTED_DATASET)
    intrinsic_valid = _qf(schema, _ROOTED_INTRINSIC_VALID)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (SELECT 1 FROM {header}) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_dependency_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF TG_OP = 'UPDATE' AND (
              (TG_TABLE_NAME = {_ql(_SOURCE)} AND (
                   pg_catalog.to_jsonb(OLD) ->> 'source_id' =
                       {_ql(_ROOTED_SOURCE_ID)}
                   OR pg_catalog.to_jsonb(NEW) ->> 'source_id' =
                       {_ql(_ROOTED_SOURCE_ID)}
              )) OR
              (TG_TABLE_NAME = {_ql(_ENDPOINT)} AND (
                   pg_catalog.to_jsonb(OLD) ->> 'endpoint_id' =
                       {_ql(_ROOTED_ENDPOINT_ID)}
                   OR pg_catalog.to_jsonb(NEW) ->> 'endpoint_id' =
                       {_ql(_ROOTED_ENDPOINT_ID)}
              ))
           ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_registry_immutable'
                USING ERRCODE = '55000';
        ELSIF TG_OP = 'DELETE' AND (
              (TG_TABLE_NAME = {_ql(_SOURCE)}
                   AND pg_catalog.to_jsonb(OLD) ->> 'source_id' =
                       {_ql(_ROOTED_SOURCE_ID)}) OR
              (TG_TABLE_NAME = {_ql(_ENDPOINT)}
                   AND pg_catalog.to_jsonb(OLD) ->> 'endpoint_id' =
                       {_ql(_ROOTED_ENDPOINT_ID)})
           ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_registry_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF EXISTS (
            SELECT 1 FROM {header} AS dataset
             WHERE dataset.status <> 'building'
               AND {intrinsic_valid}(dataset.dataset_id) IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_dependency_drift'
                USING ERRCODE = '55000';
        END IF;
        RETURN NULL;
    END;
    $guard$;
    """


def _logical_current_guard_sql(schema: str) -> str:
    guard = _qf(schema, _LOGICAL_CURRENT_GUARD)
    parent = _qf(schema, _DATASET)
    legacy = _qf(schema, _LEGACY_DATASET)
    rooted = _qf(schema, _ROOTED_DATASET)
    legacy_valid = _qf(schema, _LEGACY_VALID)
    rooted_intrinsic_valid = _qf(schema, _ROOTED_INTRINSIC_VALID)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        generic_count bigint;
        exact_count bigint;
        generic_dataset_id text;
        exact_dataset_id text;
    BEGIN
        PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(
                {_ql(_EXACT_PUBLICATION_LOCK_IDENTITY)}, 0
            )
        );
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (
                SELECT 1 FROM {legacy} WHERE is_current IS TRUE
                UNION ALL
                SELECT 1 FROM {rooted} WHERE is_current IS TRUE
            ) THEN
                RAISE EXCEPTION 'provider_directory_exact_logical_current_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        SELECT count(*)::bigint, min(dataset_id)
          INTO generic_count, generic_dataset_id
          FROM {parent}
         WHERE endpoint_id IN ({_ql(_LEGACY_ENDPOINT_ID)},
                               {_ql(_ROOTED_ENDPOINT_ID)})
           AND is_current IS TRUE;
        SELECT count(*)::bigint, min(dataset_id)
          INTO exact_count, exact_dataset_id
          FROM (
              SELECT dataset_id FROM {legacy}
               WHERE source_id = {_ql(_LEGACY_SOURCE_ID)}
                 AND endpoint_id = {_ql(_LEGACY_ENDPOINT_ID)}
                 AND is_current IS TRUE
              UNION ALL
              SELECT dataset_id FROM {rooted}
               WHERE source_id = {_ql(_ROOTED_SOURCE_ID)}
                 AND endpoint_id = {_ql(_ROOTED_ENDPOINT_ID)}
                 AND is_current IS TRUE
          ) AS exact;
        IF generic_count > 1 OR exact_count > 1
           OR generic_count <> exact_count
           OR generic_dataset_id IS DISTINCT FROM exact_dataset_id
           OR EXISTS (
                SELECT 1 FROM {legacy} AS dataset
                 WHERE dataset.source_id = {_ql(_LEGACY_SOURCE_ID)}
                   AND dataset.endpoint_id = {_ql(_LEGACY_ENDPOINT_ID)}
                   AND dataset.is_current IS TRUE
                   AND {legacy_valid}(dataset.dataset_id) IS DISTINCT FROM TRUE
           )
           OR EXISTS (
                SELECT 1 FROM {rooted} AS dataset
                 WHERE dataset.source_id = {_ql(_ROOTED_SOURCE_ID)}
                   AND dataset.endpoint_id = {_ql(_ROOTED_ENDPOINT_ID)}
                   AND dataset.is_current IS TRUE
                   AND {rooted_intrinsic_valid}(dataset.dataset_id)
                       IS DISTINCT FROM TRUE
           ) THEN
            RAISE EXCEPTION 'provider_directory_exact_logical_current_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NULL;
    END;
    $guard$;
    """


def _create_acquisition(schema: str, *, table_creator=None) -> None:
    create_table = table_creator or create_table_or_validate
    create_table(
        op,
        _ACQUISITION,
        sa.Column("acquisition_id", sa.String(54), nullable=False),
        sa.Column("storage_contract_id", sa.String(96), nullable=False),
        sa.Column("scope_id", sa.String(54), nullable=False),
        sa.Column("root_source_id", sa.String(64), nullable=False),
        sa.Column("root_endpoint_id", sa.String(64), nullable=False),
        sa.Column("acquisition_source_id", sa.String(64), nullable=False),
        sa.Column("acquisition_endpoint_id", sa.String(64), nullable=False),
        sa.Column("source_authority_id", sa.String(64), nullable=False),
        sa.Column("root_dataset_variant", sa.String(32), nullable=False),
        sa.Column("root_publication_contract_id", sa.String(96), nullable=False),
        sa.Column("endpoint_signature_sha256", sa.String(64), nullable=False),
        sa.Column("root_dataset_id", sa.String(96), nullable=False),
        sa.Column("root_dataset_hash", sa.String(64), nullable=False),
        sa.Column("root_content_proof_sha256", sa.String(64), nullable=False),
        sa.Column("root_cohort_id", sa.String(128), nullable=False),
        sa.Column("root_resource_type", sa.String(64), nullable=False),
        sa.Column("root_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("connector_id", sa.String(54), nullable=False),
        sa.Column("graph_contract_sha256", sa.String(64), nullable=False),
        sa.Column("query_contract_sha256", sa.String(64), nullable=False),
        sa.Column("acquisition_role", sa.String(16), nullable=False),
        sa.Column("run_id", sa.String(54), nullable=False),
        sa.Column("dataset_intent_id", sa.String(54), nullable=False),
        sa.Column("max_work_items", sa.BigInteger(), nullable=False),
        sa.Column("max_resource_rows", sa.BigInteger(), nullable=False),
        sa.Column("max_edge_rows", sa.BigInteger(), nullable=False),
        sa.Column("max_payload_bytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "used_work_items",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "used_resource_rows",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "used_edge_rows",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "used_payload_bytes",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("rooted_graph_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_collection_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_complete", sa.Boolean(), nullable=False),
        sa.Column("pending_count", sa.BigInteger()),
        sa.Column("leased_count", sa.BigInteger()),
        sa.Column("completed_count", sa.BigInteger()),
        sa.Column("error_count", sa.BigInteger()),
        sa.Column("resource_count", sa.BigInteger()),
        sa.Column("edge_count", sa.BigInteger()),
        sa.Column("insurance_plan_count", sa.BigInteger()),
        sa.Column("insurance_plan_page_count", sa.Integer()),
        sa.Column("terminal_set_sha256", sa.String(64)),
        sa.Column("resource_set_sha256", sa.String(64)),
        sa.Column("edge_set_sha256", sa.String(64)),
        sa.Column("rooted_graph_sha256", sa.String(64)),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column("sealed_at", sa.TIMESTAMP(timezone=True)),
        sa.PrimaryKeyConstraint(
            "acquisition_id", name="provider_directory_rooted_graph_acquisition_pkey"
        ),
        sa.UniqueConstraint(
            "acquisition_id",
            "scope_id",
            name="provider_directory_rooted_graph_acquisition_scope_key",
        ),
        sa.UniqueConstraint(
            "scope_id",
            "dataset_intent_id",
            "acquisition_role",
            name="provider_directory_rooted_graph_intent_role_key",
        ),
        sa.UniqueConstraint("run_id", name="provider_directory_rooted_graph_run_key"),
        sa.ForeignKeyConstraint(
            ["root_source_id"],
            [f"{schema}.{_SOURCE}.source_id"],
            name="provider_directory_rooted_graph_root_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["root_endpoint_id"],
            [f"{schema}.{_ENDPOINT}.endpoint_id"],
            name="provider_directory_rooted_graph_root_endpoint_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["acquisition_source_id"],
            [f"{schema}.{_SOURCE}.source_id"],
            name="provider_directory_rooted_graph_acquisition_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["acquisition_endpoint_id"],
            [f"{schema}.{_ENDPOINT}.endpoint_id"],
            name="provider_directory_rooted_graph_endpoint_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["root_dataset_id"],
            [f"{schema}.{_DATASET}.dataset_id"],
            name="provider_directory_rooted_graph_dataset_fkey",
        ),
        sa.CheckConstraint(
            f"acquisition_id ~ '^pdrga_[0-9a-f]{{48}}$' AND scope_id ~ '^pdrgs_[0-9a-f]{{48}}$' AND storage_contract_id = {_ql(_STORAGE_CONTRACT)} AND connector_id = {_ql(_CONNECTOR_ID)} AND acquisition_endpoint_id ~ '^[0-9a-f]{{64}}$' AND root_endpoint_id ~ '^[0-9a-f]{{64}}$' AND endpoint_signature_sha256 = {_ql(_ROOTED_ENDPOINT_SIGNATURE)} AND source_authority_id = {_ql(_SOURCE_AUTHORITY)} AND root_dataset_hash ~ '^[0-9a-f]{{64}}$' AND root_content_proof_sha256 ~ '^[0-9a-f]{{64}}$' AND graph_contract_sha256 = {_ql(_GRAPH_CONTRACT_SHA256)} AND query_contract_sha256 = {_ql(_QUERY_CONTRACT_SHA256)} AND root_resource_type = 'Practitioner' AND root_resource_count > 0 AND ((root_dataset_variant = {_ql(_LEGACY_VARIANT)} AND root_publication_contract_id = {_ql(_LEGACY_PUBLICATION_CONTRACT)} AND root_source_id <> acquisition_source_id AND root_endpoint_id <> acquisition_endpoint_id) OR (root_dataset_variant = {_ql(_ROOTED_VARIANT)} AND root_publication_contract_id = {_ql(_PUBLICATION_CONTRACT)} AND root_source_id = acquisition_source_id AND root_endpoint_id = acquisition_endpoint_id)) AND max_work_items > root_resource_count AND max_work_items BETWEEN 1 AND 16500000 AND max_resource_rows BETWEEN 1 AND 25000000 AND max_edge_rows BETWEEN 1 AND 100000000 AND max_payload_bytes BETWEEN 1 AND 274877906944 AND used_work_items BETWEEN 0 AND max_work_items AND used_resource_rows BETWEEN 0 AND max_resource_rows AND used_edge_rows BETWEEN 0 AND max_edge_rows AND used_payload_bytes BETWEEN 0 AND max_payload_bytes AND acquisition_role IN ('baseline', 'candidate') AND run_id ~ '^pdrgr_[0-9a-f]{{48}}$' AND dataset_intent_id ~ '^pdrgi_[0-9a-f]{{48}}$' AND endpoint_collection_complete IS FALSE AND endpoint_complete IS FALSE",
            name="provider_directory_rooted_graph_acquisition_identity_check",
        ),
        sa.CheckConstraint(
            "(status = 'building' AND rooted_graph_complete IS FALSE AND pending_count IS NULL AND leased_count IS NULL AND completed_count IS NULL AND error_count IS NULL AND resource_count IS NULL AND edge_count IS NULL AND insurance_plan_count IS NULL AND insurance_plan_page_count IS NULL AND terminal_set_sha256 IS NULL AND resource_set_sha256 IS NULL AND edge_set_sha256 IS NULL AND rooted_graph_sha256 IS NULL AND sealed_at IS NULL) OR (status = 'sealed' AND rooted_graph_complete IS TRUE AND pending_count = 0 AND leased_count = 0 AND completed_count > 0 AND error_count = 0 AND resource_count >= 0 AND edge_count >= 0 AND insurance_plan_count >= 0 AND insurance_plan_page_count > 0 AND terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND resource_set_sha256 ~ '^[0-9a-f]{64}$' AND edge_set_sha256 ~ '^[0-9a-f]{64}$' AND rooted_graph_sha256 ~ '^[0-9a-f]{64}$' AND sealed_at IS NOT NULL)",
            name="provider_directory_rooted_graph_acquisition_state_check",
        ),
        schema=schema,
    )


def _create_work(schema: str, *, table_creator=None) -> None:
    create_table = table_creator or create_table_or_validate
    create_table(
        op,
        _WORK,
        sa.Column("acquisition_id", sa.String(54), nullable=False),
        sa.Column("scope_id", sa.String(54), nullable=False),
        sa.Column("query_id", sa.String(54), nullable=False),
        sa.Column("query_identity_sha256", sa.String(64), nullable=False),
        sa.Column("query_identity_json_text", sa.Text(), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("search_parameter", sa.String(64)),
        sa.Column("reference_type", sa.String(64)),
        sa.Column("reference_id", sa.String(64)),
        sa.Column("closure_scope", sa.String(16), nullable=False),
        sa.Column("discovered_by_query_id", sa.String(54)),
        sa.Column("discovered_source_type", sa.String(64)),
        sa.Column("discovered_source_id", sa.String(64)),
        sa.Column("discovered_edge_sha256", sa.String(64)),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("lease_token", sa.String(64)),
        sa.Column("lease_expires_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("lease_heartbeat_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("result_sha256", sa.String(64)),
        sa.Column("resource_count", sa.Integer()),
        sa.Column("edge_count", sa.Integer()),
        sa.Column("resource_set_sha256", sa.String(64)),
        sa.Column("edge_set_sha256", sa.String(64)),
        sa.Column("advertised_total", sa.BigInteger()),
        sa.Column("terminal_page_count", sa.Integer()),
        sa.Column("pagination_terminal", sa.Boolean(), nullable=False),
        sa.Column("missing_http_status", sa.SmallInteger()),
        sa.Column("missing_response_sha256", sa.String(64)),
        sa.Column("missing_response_bytes", sa.BigInteger()),
        sa.Column("missing_response_json_text", sa.Text()),
        sa.Column("error_code", sa.String(128)),
        sa.Column("terminal_record_sha256", sa.String(64)),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column("terminal_at", sa.TIMESTAMP(timezone=True)),
        sa.PrimaryKeyConstraint(
            "acquisition_id",
            "query_id",
            name="provider_directory_rooted_graph_work_pkey",
        ),
        sa.UniqueConstraint(
            "acquisition_id",
            "scope_id",
            "query_id",
            name="provider_directory_rooted_graph_work_scope_key",
        ),
        sa.ForeignKeyConstraint(
            ["acquisition_id", "scope_id"],
            [
                f"{schema}.{_ACQUISITION}.acquisition_id",
                f"{schema}.{_ACQUISITION}.scope_id",
            ],
            name="provider_directory_rooted_graph_work_acquisition_fkey",
        ),
        sa.CheckConstraint(
            "query_id ~ '^pdrgq_[0-9a-f]{48}$' AND query_identity_sha256 ~ '^[0-9a-f]{64}$' AND octet_length(query_identity_json_text) BETWEEN 2 AND 8192 AND kind IN ('exact_reference_search', 'direct_read', 'full_insurance_plan_census') AND closure_scope IN ('root', 'plan', 'census') AND attempt_count >= 0 AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$')",
            name="provider_directory_rooted_graph_work_value_check",
        ),
        sa.CheckConstraint(
            "(kind = 'exact_reference_search' AND ((resource_type = 'PractitionerRole' AND search_parameter = 'practitioner' AND reference_type = 'Practitioner' AND closure_scope = 'root' AND discovered_by_query_id IS NULL AND discovered_source_type IS NULL AND discovered_source_id IS NULL AND discovered_edge_sha256 IS NULL) OR (resource_type = 'OrganizationAffiliation' AND search_parameter = 'participating-organization' AND reference_type = 'Organization' AND closure_scope IN ('root', 'plan') AND discovered_by_query_id IS NOT NULL AND discovered_source_type = 'Organization' AND discovered_source_id = reference_id AND discovered_edge_sha256 IS NULL))) OR (kind = 'direct_read' AND search_parameter IS NULL AND resource_type IN ('Organization', 'Location', 'HealthcareService', 'Endpoint') AND reference_type = resource_type AND closure_scope IN ('root', 'plan') AND discovered_by_query_id IS NOT NULL AND discovered_source_type IS NOT NULL AND discovered_source_id IS NOT NULL AND discovered_edge_sha256 ~ '^[0-9a-f]{64}$') OR (kind = 'full_insurance_plan_census' AND resource_type = 'InsurancePlan' AND search_parameter IS NULL AND reference_type IS NULL AND reference_id IS NULL AND closure_scope = 'census' AND discovered_by_query_id IS NULL AND discovered_source_type IS NULL AND discovered_source_id IS NULL AND discovered_edge_sha256 IS NULL)",
            name="provider_directory_rooted_graph_work_shape_check",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND lease_token IS NULL AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND result_sha256 IS NULL AND resource_count IS NULL AND edge_count IS NULL AND resource_set_sha256 IS NULL AND edge_set_sha256 IS NULL AND advertised_total IS NULL AND terminal_page_count IS NULL AND pagination_terminal IS FALSE AND missing_http_status IS NULL AND missing_response_sha256 IS NULL AND missing_response_bytes IS NULL AND missing_response_json_text IS NULL AND error_code IS NULL AND terminal_record_sha256 IS NULL AND terminal_at IS NULL) OR (status = 'leased' AND attempt_count > 0 AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL AND lease_heartbeat_at IS NOT NULL AND result_sha256 IS NULL AND resource_count IS NULL AND edge_count IS NULL AND resource_set_sha256 IS NULL AND edge_set_sha256 IS NULL AND advertised_total IS NULL AND terminal_page_count IS NULL AND pagination_terminal IS FALSE AND missing_http_status IS NULL AND missing_response_sha256 IS NULL AND missing_response_bytes IS NULL AND missing_response_json_text IS NULL AND error_code IS NULL AND terminal_record_sha256 IS NULL AND terminal_at IS NULL) OR (status = 'completed' AND attempt_count > 0 AND lease_token IS NULL AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND result_sha256 ~ '^[0-9a-f]{64}$' AND resource_count >= 0 AND edge_count >= 0 AND resource_set_sha256 ~ '^[0-9a-f]{64}$' AND edge_set_sha256 ~ '^[0-9a-f]{64}$' AND terminal_page_count > 0 AND pagination_terminal IS TRUE AND error_code IS NULL AND terminal_record_sha256 ~ '^[0-9a-f]{64}$' AND terminal_at IS NOT NULL AND ((kind = 'full_insurance_plan_census' AND advertised_total = resource_count) OR (kind = 'exact_reference_search' AND (advertised_total IS NULL OR advertised_total = resource_count)) OR (kind = 'direct_read' AND advertised_total IS NULL)) AND ((missing_http_status IS NULL AND missing_response_sha256 IS NULL AND missing_response_bytes IS NULL AND missing_response_json_text IS NULL AND (kind <> 'direct_read' OR (terminal_page_count = 1 AND resource_count = 1))) OR (kind = 'direct_read' AND terminal_page_count = 1 AND missing_http_status IN (404, 410) AND missing_response_sha256 ~ '^[0-9a-f]{64}$' AND missing_response_bytes BETWEEN 1 AND 65536 AND octet_length(missing_response_json_text) = missing_response_bytes AND resource_count = 0 AND edge_count = 0))) OR (status = 'error' AND attempt_count > 0 AND lease_token IS NULL AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND result_sha256 IS NULL AND resource_count = 0 AND edge_count = 0 AND resource_set_sha256 IS NULL AND edge_set_sha256 IS NULL AND advertised_total IS NULL AND terminal_page_count = 0 AND pagination_terminal IS FALSE AND missing_http_status IS NULL AND missing_response_sha256 IS NULL AND missing_response_bytes IS NULL AND missing_response_json_text IS NULL AND error_code ~ '^[a-z][a-z0-9_]{0,127}$' AND terminal_record_sha256 ~ '^[0-9a-f]{64}$' AND terminal_at IS NOT NULL)",
            name="provider_directory_rooted_graph_work_state_check",
        ),
        schema=schema,
    )


def _create_resource(schema: str, *, table_creator=None) -> None:
    create_table = table_creator or create_table_or_validate
    create_table(
        op,
        _RESOURCE,
        sa.Column("acquisition_id", sa.String(54), nullable=False),
        sa.Column("scope_id", sa.String(54), nullable=False),
        sa.Column("query_id", sa.String(54), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("resource_id", sa.String(64), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("payload_json_text", sa.Text(), nullable=False),
        sa.Column("closure_scope", sa.String(16), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "acquisition_id",
            "query_id",
            "attempt",
            "resource_type",
            "resource_id",
            name="provider_directory_rooted_graph_resource_pkey",
        ),
        sa.ForeignKeyConstraint(
            ["acquisition_id", "scope_id", "query_id"],
            [
                f"{schema}.{_WORK}.acquisition_id",
                f"{schema}.{_WORK}.scope_id",
                f"{schema}.{_WORK}.query_id",
            ],
            name="provider_directory_rooted_graph_resource_work_fkey",
        ),
        sa.CheckConstraint(
            "attempt > 0 AND resource_type IN ('PractitionerRole', 'OrganizationAffiliation', 'Organization', 'Location', 'HealthcareService', 'InsurancePlan', 'Endpoint') AND resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND payload_sha256 ~ '^[0-9a-f]{64}$' AND closure_scope IN ('root', 'plan', 'census') AND octet_length(payload_json_text) BETWEEN 2 AND 1048576",
            name="provider_directory_rooted_graph_resource_value_check",
        ),
        schema=schema,
    )


def _create_edge(schema: str, *, table_creator=None) -> None:
    create_table = table_creator or create_table_or_validate
    create_table(
        op,
        _EDGE,
        sa.Column("acquisition_id", sa.String(54), nullable=False),
        sa.Column("scope_id", sa.String(54), nullable=False),
        sa.Column("query_id", sa.String(54), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("source_resource_type", sa.String(64), nullable=False),
        sa.Column("source_resource_id", sa.String(64), nullable=False),
        sa.Column("field_path", sa.String(128), nullable=False),
        sa.Column("target_resource_type", sa.String(64), nullable=False),
        sa.Column("target_resource_id", sa.String(64), nullable=False),
        sa.Column("edge_sha256", sa.String(64), nullable=False),
        sa.Column("closure_scope", sa.String(16), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "acquisition_id",
            "query_id",
            "attempt",
            "edge_sha256",
            name="provider_directory_rooted_graph_edge_pkey",
        ),
        sa.ForeignKeyConstraint(
            [
                "acquisition_id",
                "query_id",
                "attempt",
                "source_resource_type",
                "source_resource_id",
            ],
            [
                f"{schema}.{_RESOURCE}.acquisition_id",
                f"{schema}.{_RESOURCE}.query_id",
                f"{schema}.{_RESOURCE}.attempt",
                f"{schema}.{_RESOURCE}.resource_type",
                f"{schema}.{_RESOURCE}.resource_id",
            ],
            name="provider_directory_rooted_graph_edge_resource_fkey",
        ),
        sa.CheckConstraint(
            "attempt > 0 AND source_resource_type IN ('PractitionerRole', 'OrganizationAffiliation', 'Organization', 'Location', 'HealthcareService', 'InsurancePlan', 'Endpoint') AND source_resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND field_path ~ '^([A-Za-z][A-Za-z0-9]*(\\[[0-9]+\\])?|extension\\[[0-9]+\\](\\.extension\\[[0-9]+\\]){0,5}\\.valueReference)$' AND target_resource_type IN ('Practitioner', 'PractitionerRole', 'OrganizationAffiliation', 'Organization', 'Location', 'HealthcareService', 'InsurancePlan', 'Endpoint') AND target_resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND edge_sha256 ~ '^[0-9a-f]{64}$' AND closure_scope IN ('root', 'plan', 'census')",
            name="provider_directory_rooted_graph_edge_value_check",
        ),
        schema=schema,
    )


def _create_twin_tables(schema: str, *, table_creator=None) -> None:
    create_table = table_creator or create_table_or_validate
    attempt_columns: list[sa.SchemaItem] = [
        sa.Column("attempt_id", sa.String(55), nullable=False),
        sa.Column("attempt_contract_id", sa.String(96), nullable=False),
        sa.Column("storage_contract_id", sa.String(96), nullable=False),
        sa.Column("first_acquisition_id", sa.String(54), nullable=False),
        sa.Column("second_acquisition_id", sa.String(54), nullable=False),
        sa.Column("dataset_intent_id", sa.String(54), nullable=False),
        sa.Column("scope_id", sa.String(54), nullable=False),
        sa.Column("root_source_id", sa.String(64), nullable=False),
        sa.Column("root_endpoint_id", sa.String(64), nullable=False),
        sa.Column("acquisition_source_id", sa.String(64), nullable=False),
        sa.Column("acquisition_endpoint_id", sa.String(64), nullable=False),
        sa.Column("source_authority_id", sa.String(64), nullable=False),
        sa.Column("endpoint_signature_sha256", sa.String(64), nullable=False),
        sa.Column("root_dataset_id", sa.String(96), nullable=False),
        sa.Column("root_dataset_variant", sa.String(32), nullable=False),
        sa.Column("root_publication_contract_id", sa.String(96), nullable=False),
        sa.Column("root_dataset_hash", sa.String(64), nullable=False),
        sa.Column("root_content_proof_sha256", sa.String(64), nullable=False),
        sa.Column("root_cohort_id", sa.String(128), nullable=False),
        sa.Column("root_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("connector_id", sa.String(64), nullable=False),
        sa.Column("graph_contract_sha256", sa.String(64), nullable=False),
        sa.Column("query_contract_sha256", sa.String(64), nullable=False),
        sa.Column("max_work_items", sa.BigInteger(), nullable=False),
        sa.Column("max_resource_rows", sa.BigInteger(), nullable=False),
        sa.Column("max_edge_rows", sa.BigInteger(), nullable=False),
        sa.Column("max_payload_bytes", sa.BigInteger(), nullable=False),
    ]
    for side in ("first", "second"):
        for field_name in (
            "pending_count",
            "leased_count",
            "completed_count",
            "error_count",
            "resource_count",
            "edge_count",
            "insurance_plan_count",
            "insurance_plan_page_count",
            "used_work_items",
            "used_resource_rows",
            "used_edge_rows",
            "used_payload_bytes",
        ):
            attempt_columns.append(
                sa.Column(f"{side}_{field_name}", sa.BigInteger(), nullable=False)
            )
    for side in ("first", "second"):
        for field_name in (
            "terminal_set_sha256",
            "resource_set_sha256",
            "edge_set_sha256",
            "rooted_graph_sha256",
        ):
            attempt_columns.append(
                sa.Column(f"{side}_{field_name}", sa.String(64), nullable=False)
            )
    attempt_columns.extend(
        (
            sa.Column("matched", sa.Boolean(), nullable=False),
            sa.Column(
                "attempted_at",
                sa.TIMESTAMP(timezone=True),
                server_default=sa.text("transaction_timestamp()"),
                nullable=False,
            ),
            sa.PrimaryKeyConstraint(
                "attempt_id", name="pd_rooted_graph_twin_attempt_pkey"
            ),
            sa.UniqueConstraint(
                "first_acquisition_id",
                "second_acquisition_id",
                name="pd_rooted_graph_twin_attempt_pair_key",
            ),
            sa.ForeignKeyConstraint(
                ["first_acquisition_id"],
                [f"{schema}.{_ACQUISITION}.acquisition_id"],
                name="pd_rooted_graph_twin_attempt_first_fkey",
            ),
            sa.ForeignKeyConstraint(
                ["second_acquisition_id"],
                [f"{schema}.{_ACQUISITION}.acquisition_id"],
                name="pd_rooted_graph_twin_attempt_second_fkey",
            ),
            sa.CheckConstraint(
                f"attempt_id ~ '^pdrgat_[0-9a-f]{{48}}$' AND "
                f"attempt_contract_id = {_ql(_TWIN_ATTEMPT_CONTRACT)} AND "
                f"storage_contract_id = {_ql(_STORAGE_CONTRACT)} AND "
                "first_acquisition_id < second_acquisition_id",
                name="pd_rooted_graph_twin_attempt_check",
            ),
        )
    )
    create_table(op, _TWIN_ATTEMPT, *attempt_columns, schema=schema)

    admission_columns: list[sa.SchemaItem] = [
        sa.Column("admission_id", sa.String(55), nullable=False),
        sa.Column("admission_contract_id", sa.String(96), nullable=False),
        sa.Column("storage_contract_id", sa.String(96), nullable=False),
        sa.Column("attempt_id", sa.String(55), nullable=False),
        sa.Column("publication_acquisition_id", sa.String(54), nullable=False),
        sa.Column("comparison_acquisition_id", sa.String(54), nullable=False),
        sa.Column("publication_run_id", sa.String(54), nullable=False),
        sa.Column("dataset_intent_id", sa.String(54), nullable=False),
        sa.Column("scope_id", sa.String(54), nullable=False),
        sa.Column("root_source_id", sa.String(64), nullable=False),
        sa.Column("root_endpoint_id", sa.String(64), nullable=False),
        sa.Column("acquisition_source_id", sa.String(64), nullable=False),
        sa.Column("acquisition_endpoint_id", sa.String(64), nullable=False),
        sa.Column("source_authority_id", sa.String(64), nullable=False),
        sa.Column("endpoint_signature_sha256", sa.String(64), nullable=False),
        sa.Column("root_dataset_id", sa.String(96), nullable=False),
        sa.Column("root_dataset_variant", sa.String(32), nullable=False),
        sa.Column("root_publication_contract_id", sa.String(96), nullable=False),
        sa.Column("root_dataset_hash", sa.String(64), nullable=False),
        sa.Column("root_content_proof_sha256", sa.String(64), nullable=False),
        sa.Column("root_cohort_id", sa.String(128), nullable=False),
        sa.Column("root_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("connector_id", sa.String(64), nullable=False),
        sa.Column("graph_contract_sha256", sa.String(64), nullable=False),
        sa.Column("query_contract_sha256", sa.String(64), nullable=False),
        sa.Column("max_work_items", sa.BigInteger(), nullable=False),
        sa.Column("max_resource_rows", sa.BigInteger(), nullable=False),
        sa.Column("max_edge_rows", sa.BigInteger(), nullable=False),
        sa.Column("max_payload_bytes", sa.BigInteger(), nullable=False),
    ]
    for field_name in (
        "completed_count",
        "resource_count",
        "edge_count",
        "insurance_plan_count",
        "insurance_plan_page_count",
        "used_work_items",
        "used_resource_rows",
        "used_edge_rows",
        "used_payload_bytes",
    ):
        admission_columns.append(sa.Column(field_name, sa.BigInteger(), nullable=False))
    for field_name in (
        "terminal_set_sha256",
        "resource_set_sha256",
        "edge_set_sha256",
        "rooted_graph_sha256",
    ):
        admission_columns.append(sa.Column(field_name, sa.String(64), nullable=False))
    admission_columns.extend(
        (
            sa.Column("publication_authority", sa.Boolean(), nullable=False),
            sa.Column(
                "admitted_at",
                sa.TIMESTAMP(timezone=True),
                server_default=sa.text("transaction_timestamp()"),
                nullable=False,
            ),
            sa.PrimaryKeyConstraint(
                "admission_id", name="pd_rooted_graph_twin_admission_pkey"
            ),
            sa.UniqueConstraint(
                "publication_acquisition_id",
                name="pd_rooted_graph_twin_admission_publication_key",
            ),
            sa.ForeignKeyConstraint(
                ["attempt_id"],
                [f"{schema}.{_TWIN_ATTEMPT}.attempt_id"],
                name="pd_rooted_graph_twin_admission_attempt_fkey",
            ),
            sa.ForeignKeyConstraint(
                ["publication_acquisition_id"],
                [f"{schema}.{_ACQUISITION}.acquisition_id"],
                name="pd_rooted_graph_twin_admission_publication_fkey",
            ),
            sa.ForeignKeyConstraint(
                ["comparison_acquisition_id"],
                [f"{schema}.{_ACQUISITION}.acquisition_id"],
                name="pd_rooted_graph_twin_admission_comparison_fkey",
            ),
            sa.CheckConstraint(
                f"admission_id ~ '^pdrgad_[0-9a-f]{{48}}$' AND "
                f"admission_contract_id = {_ql(_TWIN_ADMISSION_CONTRACT)} AND "
                f"storage_contract_id = {_ql(_STORAGE_CONTRACT)} AND "
                "publication_authority IS TRUE",
                name="pd_rooted_graph_twin_admission_check",
            ),
        )
    )
    create_table(op, _TWIN_ADMISSION, *admission_columns, schema=schema)


def _create_publication_tables(schema: str, *, table_creator=None) -> None:
    create_table = table_creator or create_table_or_validate
    create_table(
        op,
        _ROOTED_DATASET,
        sa.Column("dataset_id", sa.String(55), nullable=False),
        sa.Column("publication_contract_id", sa.String(96), nullable=False),
        sa.Column("publication_kind", sa.String(32), nullable=False),
        sa.Column("admission_id", sa.String(55), nullable=False),
        sa.Column("attempt_id", sa.String(55), nullable=False),
        sa.Column("publication_acquisition_id", sa.String(54), nullable=False),
        sa.Column("comparison_acquisition_id", sa.String(54), nullable=False),
        sa.Column("publication_run_id", sa.String(54), nullable=False),
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("endpoint_id", sa.String(64), nullable=False),
        sa.Column("acquisition_source_id", sa.String(64), nullable=False),
        sa.Column("acquisition_endpoint_id", sa.String(64), nullable=False),
        sa.Column("source_authority_id", sa.String(64), nullable=False),
        sa.Column("root_dataset_variant", sa.String(32), nullable=False),
        sa.Column("root_publication_contract_id", sa.String(96), nullable=False),
        sa.Column("root_source_id", sa.String(64), nullable=False),
        sa.Column("root_endpoint_id", sa.String(64), nullable=False),
        sa.Column("practitioner_origin_source_id", sa.String(64), nullable=False),
        sa.Column("practitioner_origin_endpoint_id", sa.String(64), nullable=False),
        sa.Column("endpoint_signature_sha256", sa.String(64), nullable=False),
        sa.Column("scope_id", sa.String(54), nullable=False),
        sa.Column("dataset_intent_id", sa.String(54), nullable=False),
        sa.Column("acquisition_root_run_id", sa.String(55), nullable=False),
        sa.Column("semantic_projection_as_of", sa.Date(), nullable=False),
        sa.Column("operation_key", sa.String(64), nullable=False),
        sa.Column("root_dataset_id", sa.String(96), nullable=False),
        sa.Column("root_dataset_hash", sa.String(64), nullable=False),
        sa.Column("root_content_proof_sha256", sa.String(64), nullable=False),
        sa.Column("root_cohort_id", sa.String(128), nullable=False),
        sa.Column("root_practitioner_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("connector_id", sa.String(64), nullable=False),
        sa.Column("storage_contract_id", sa.String(96), nullable=False),
        sa.Column("graph_contract_sha256", sa.String(64), nullable=False),
        sa.Column("query_contract_sha256", sa.String(64), nullable=False),
        sa.Column("max_work_items", sa.BigInteger(), nullable=False),
        sa.Column("max_resource_rows", sa.BigInteger(), nullable=False),
        sa.Column("max_edge_rows", sa.BigInteger(), nullable=False),
        sa.Column("max_payload_bytes", sa.BigInteger(), nullable=False),
        sa.Column("used_work_items", sa.BigInteger(), nullable=False),
        sa.Column("used_resource_rows", sa.BigInteger(), nullable=False),
        sa.Column("used_edge_rows", sa.BigInteger(), nullable=False),
        sa.Column("used_payload_bytes", sa.BigInteger(), nullable=False),
        sa.Column("completed_count", sa.BigInteger(), nullable=False),
        sa.Column("graph_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("graph_edge_count", sa.BigInteger(), nullable=False),
        sa.Column("census_insurance_plan_count", sa.BigInteger(), nullable=False),
        sa.Column("insurance_plan_page_count", sa.Integer(), nullable=False),
        sa.Column("terminal_set_sha256", sa.String(64), nullable=False),
        sa.Column("resource_set_sha256", sa.String(64), nullable=False),
        sa.Column("edge_set_sha256", sa.String(64), nullable=False),
        sa.Column("rooted_graph_sha256", sa.String(64), nullable=False),
        sa.Column("previous_dataset_id", sa.String(96)),
        sa.Column("dataset_hash", sa.String(64)),
        sa.Column("resource_count", sa.BigInteger(), nullable=False),
        sa.Column("practitioner_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("practitioner_role_resource_count", sa.BigInteger(), nullable=False),
        sa.Column(
            "organization_affiliation_resource_count", sa.BigInteger(), nullable=False
        ),
        sa.Column("organization_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("location_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("healthcare_service_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("insurance_plan_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("endpoint_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("resource_hash_contract", sa.String(32), nullable=False),
        sa.Column("cohort_complete", sa.Boolean(), nullable=False),
        sa.Column("rooted_graph_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_collection_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_complete", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column(
            "is_current", sa.Boolean(), server_default=sa.false(), nullable=False
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("transaction_timestamp()"),
            nullable=False,
        ),
        sa.Column("validated_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("superseded_at", sa.TIMESTAMP(timezone=True)),
        sa.PrimaryKeyConstraint("dataset_id", name="pd_rooted_graph_dataset_pkey"),
        sa.UniqueConstraint(
            "admission_id", name="pd_rooted_graph_dataset_admission_key"
        ),
        sa.UniqueConstraint(
            "publication_acquisition_id",
            name="pd_rooted_graph_dataset_acquisition_key",
        ),
        sa.UniqueConstraint(
            "acquisition_root_run_id", name="pd_rooted_graph_dataset_root_run_key"
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            [f"{schema}.{_DATASET}.dataset_id"],
            name="pd_rooted_graph_dataset_parent_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["admission_id"],
            [f"{schema}.{_TWIN_ADMISSION}.admission_id"],
            name="pd_rooted_graph_dataset_admission_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            [f"{schema}.{_TWIN_ATTEMPT}.attempt_id"],
            name="pd_rooted_graph_dataset_attempt_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["publication_acquisition_id"],
            [f"{schema}.{_TWIN_ADMISSION}.publication_acquisition_id"],
            name="pd_rooted_graph_dataset_publication_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            [f"{schema}.{_SOURCE}.source_id"],
            name="pd_rooted_graph_dataset_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["endpoint_id"],
            [f"{schema}.{_ENDPOINT}.endpoint_id"],
            name="pd_rooted_graph_dataset_endpoint_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["acquisition_source_id"],
            [f"{schema}.{_SOURCE}.source_id"],
            name="pd_rooted_graph_dataset_acquisition_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["acquisition_endpoint_id"],
            [f"{schema}.{_ENDPOINT}.endpoint_id"],
            name="pd_rooted_graph_dataset_acquisition_endpoint_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["root_source_id"],
            [f"{schema}.{_SOURCE}.source_id"],
            name="pd_rooted_graph_dataset_root_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["root_endpoint_id"],
            [f"{schema}.{_ENDPOINT}.endpoint_id"],
            name="pd_rooted_graph_dataset_root_endpoint_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["practitioner_origin_source_id"],
            [f"{schema}.{_SOURCE}.source_id"],
            name="pd_rooted_graph_dataset_origin_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["practitioner_origin_endpoint_id"],
            [f"{schema}.{_ENDPOINT}.endpoint_id"],
            name="pd_rooted_graph_dataset_origin_endpoint_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["root_dataset_id"],
            [f"{schema}.{_DATASET}.dataset_id"],
            name="pd_rooted_graph_dataset_root_dataset_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["previous_dataset_id"],
            [f"{schema}.{_DATASET}.dataset_id"],
            name="pd_rooted_graph_dataset_previous_fkey",
        ),
        sa.CheckConstraint(
            f"publication_contract_id = {_ql(_PUBLICATION_CONTRACT)} AND "
            f"publication_kind = {_ql(_PUBLICATION_KIND)} AND "
            "dataset_id ~ '^pdrgpd_[0-9a-f]{48}$' AND "
            "acquisition_root_run_id ~ '^pdrgpr_[0-9a-f]{48}$' AND "
            f"source_id = {_ql(_ROOTED_SOURCE_ID)} AND "
            f"endpoint_id = {_ql(_ROOTED_ENDPOINT_ID)} AND "
            "acquisition_source_id = source_id AND "
            "acquisition_endpoint_id = endpoint_id AND "
            f"source_authority_id = {_ql(_SOURCE_AUTHORITY)} AND "
            f"endpoint_signature_sha256 = {_ql(_ROOTED_ENDPOINT_SIGNATURE)} AND "
            f"practitioner_origin_source_id = {_ql(_LEGACY_SOURCE_ID)} AND "
            f"practitioner_origin_endpoint_id = {_ql(_LEGACY_ENDPOINT_ID)} AND "
            "((root_dataset_variant = 'uhc_flex_practitioner' AND "
            f"root_publication_contract_id = {_ql(_LEGACY_PUBLICATION_CONTRACT)} AND "
            f"root_source_id = {_ql(_LEGACY_SOURCE_ID)} AND "
            f"root_endpoint_id = {_ql(_LEGACY_ENDPOINT_ID)}) OR "
            "(root_dataset_variant = 'rooted_combined' AND "
            f"root_publication_contract_id = {_ql(_PUBLICATION_CONTRACT)} AND "
            "root_source_id = source_id AND root_endpoint_id = endpoint_id)) AND "
            "root_dataset_hash ~ '^[0-9a-f]{64}$' AND "
            "root_content_proof_sha256 ~ '^[0-9a-f]{64}$' AND "
            "operation_key ~ '^[0-9a-f]{64}$' AND "
            "rooted_graph_sha256 ~ '^[0-9a-f]{64}$' AND "
            f"resource_hash_contract = {_ql(_HASH_CONTRACT)} AND "
            "cohort_complete IS TRUE AND rooted_graph_complete IS TRUE AND "
            "endpoint_collection_complete IS FALSE AND endpoint_complete IS FALSE "
            "AND max_work_items > root_practitioner_resource_count "
            "AND max_work_items BETWEEN 1 AND 16500000 "
            "AND max_resource_rows BETWEEN 1 AND 25000000 "
            "AND max_edge_rows BETWEEN 1 AND 100000000 "
            "AND max_payload_bytes BETWEEN 1 AND 274877906944 "
            "AND used_work_items BETWEEN 1 AND max_work_items "
            "AND used_resource_rows BETWEEN 0 AND max_resource_rows "
            "AND used_edge_rows BETWEEN 0 AND max_edge_rows "
            "AND used_payload_bytes BETWEEN 0 AND max_payload_bytes "
            "AND completed_count = used_work_items "
            "AND graph_resource_count = used_resource_rows "
            "AND graph_edge_count = used_edge_rows "
            "AND root_practitioner_resource_count > 0 "
            "AND practitioner_resource_count = root_practitioner_resource_count "
            "AND practitioner_role_resource_count >= 0 "
            "AND organization_affiliation_resource_count >= 0 "
            "AND organization_resource_count >= 0 AND location_resource_count >= 0 "
            "AND healthcare_service_resource_count >= 0 "
            "AND insurance_plan_resource_count >= 0 "
            "AND endpoint_resource_count >= 0 AND "
            "resource_count = practitioner_resource_count + "
            "practitioner_role_resource_count + organization_affiliation_resource_count + "
            "organization_resource_count + location_resource_count + "
            "healthcare_service_resource_count + insurance_plan_resource_count + "
            "endpoint_resource_count AND "
            "((status = 'building' AND is_current IS FALSE AND dataset_hash IS NULL "
            "AND validated_at IS NULL AND published_at IS NULL AND superseded_at IS NULL) "
            "OR (status = 'validated' AND is_current IS FALSE AND "
            "dataset_hash ~ '^[0-9a-f]{64}$' AND validated_at IS NOT NULL "
            "AND published_at IS NULL AND superseded_at IS NULL) OR "
            "(status = 'published' AND is_current IS TRUE AND "
            "dataset_hash ~ '^[0-9a-f]{64}$' AND validated_at IS NOT NULL "
            "AND published_at IS NOT NULL AND superseded_at IS NULL) OR "
            "(status = 'superseded' AND is_current IS FALSE AND "
            "dataset_hash ~ '^[0-9a-f]{64}$' AND validated_at IS NOT NULL "
            "AND published_at IS NOT NULL AND superseded_at IS NOT NULL))",
            name="pd_rooted_graph_dataset_check",
        ),
        schema=schema,
    )
    create_table(
        op,
        _ROOTED_PROVENANCE,
        sa.Column("dataset_id", sa.String(55), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("resource_id", sa.String(256), nullable=False),
        sa.Column("origin_kind", sa.String(32), nullable=False),
        sa.Column("root_dataset_id", sa.String(96), nullable=False),
        sa.Column("publication_acquisition_id", sa.String(54), nullable=False),
        sa.Column("query_id", sa.String(54)),
        sa.Column("attempt", sa.Integer()),
        sa.Column("closure_scope", sa.String(16)),
        sa.Column("source_payload_sha256", sa.String(64)),
        sa.Column("published_payload_hash", sa.String(64), nullable=False),
        sa.PrimaryKeyConstraint(
            "dataset_id",
            "resource_type",
            "resource_id",
            name="pd_rooted_graph_dataset_resource_pkey",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            [f"{schema}.{_ROOTED_DATASET}.dataset_id"],
            name="pd_rooted_graph_dataset_resource_dataset_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id", "resource_type", "resource_id"],
            [
                f"{schema}.{_DATASET_RESOURCE}.dataset_id",
                f"{schema}.{_DATASET_RESOURCE}.resource_type",
                f"{schema}.{_DATASET_RESOURCE}.resource_id",
            ],
            name="pd_rooted_graph_dataset_resource_parent_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["publication_acquisition_id"],
            [f"{schema}.{_ACQUISITION}.acquisition_id"],
            name="pd_rooted_graph_dataset_resource_acquisition_fkey",
        ),
        sa.CheckConstraint(
            "resource_type IN ('Practitioner','PractitionerRole',"
            "'OrganizationAffiliation','Organization','Location',"
            "'HealthcareService','InsurancePlan','Endpoint') AND "
            "resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND "
            "published_payload_hash ~ '^[0-9a-f]{64}$' AND "
            "((origin_kind = 'root_practitioner' AND resource_type = 'Practitioner' "
            "AND query_id IS NULL AND attempt IS NULL AND closure_scope IS NULL "
            "AND source_payload_sha256 IS NULL) OR "
            "(origin_kind = 'rooted_graph' AND resource_type <> 'Practitioner' "
            "AND query_id ~ '^pdrgq_[0-9a-f]{48}$' AND attempt > 0 "
            "AND closure_scope IN ('root','plan') AND "
            "source_payload_sha256 ~ '^[0-9a-f]{64}$'))",
            name="pd_rooted_graph_dataset_resource_check",
        ),
        schema=schema,
    )


def _owned_check_constraints(schema: str) -> tuple[tuple[str, str, str], ...]:
    schema_items_by_table: dict[str, tuple[sa.SchemaItem, ...]] = {}

    def capture_table(_operations, table_name, *schema_items, **_options):
        schema_items_by_table[table_name] = schema_items

    for create_tables in (
        _create_acquisition,
        _create_work,
        _create_resource,
        _create_edge,
        _create_twin_tables,
        _create_publication_tables,
    ):
        create_tables(schema, table_creator=capture_table)
    return tuple(
        (table_name, str(item.name), str(item.sqltext))
        for table_name, schema_items in schema_items_by_table.items()
        for item in schema_items
        if isinstance(item, sa.CheckConstraint) and item.name is not None
    )


def _adopt_check_constraint_sql(
    schema: str,
    table_name: str,
    constraint_name: str,
    expression: str,
    probe_number: int,
) -> tuple[str, ...]:
    relation_ref = _qf(schema, table_name)
    probe_name = f"pdrg_check_probe_{probe_number}"
    mismatch = (
        f"provider_directory_rooted_graph_check_mismatch:{table_name}.{constraint_name}"
    )
    add_probe = (
        f"ALTER TABLE {relation_ref} ADD CONSTRAINT {_q(probe_name)} "
        f"CHECK ({expression}) NOT VALID;"
    )
    compare_or_adopt = f"""
        DO $adopt_check$
        DECLARE
            actual_oid oid;
            actual_type "char";
            actual_local boolean;
            actual_inherited integer;
            actual_no_inherit boolean;
            probe_oid oid;
            probe_local boolean;
            probe_inherited integer;
            probe_no_inherit boolean;
        BEGIN
            SELECT constraint_row.oid,
                   constraint_row.contype,
                   constraint_row.conislocal,
                   constraint_row.coninhcount,
                   constraint_row.connoinherit
              INTO actual_oid, actual_type, actual_local, actual_inherited,
                   actual_no_inherit
              FROM pg_catalog.pg_constraint AS constraint_row
              JOIN pg_catalog.pg_class AS relation
                ON relation.oid = constraint_row.conrelid
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = {_ql(schema)}
               AND relation.relname = {_ql(table_name)}
               AND constraint_row.conname = {_ql(constraint_name)};
            SELECT constraint_row.oid,
                   constraint_row.conislocal,
                   constraint_row.coninhcount,
                   constraint_row.connoinherit
              INTO probe_oid, probe_local, probe_inherited, probe_no_inherit
              FROM pg_catalog.pg_constraint AS constraint_row
              JOIN pg_catalog.pg_class AS relation
                ON relation.oid = constraint_row.conrelid
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = {_ql(schema)}
               AND relation.relname = {_ql(table_name)}
               AND constraint_row.conname = {_ql(probe_name)};
            IF probe_oid IS NULL THEN
                RAISE EXCEPTION {_ql(mismatch)} USING ERRCODE = '55000';
            ELSIF actual_oid IS NULL THEN
                EXECUTE format(
                    'ALTER TABLE %I.%I RENAME CONSTRAINT %I TO %I',
                    {_ql(schema)}, {_ql(table_name)},
                    {_ql(probe_name)}, {_ql(constraint_name)}
                );
            ELSIF actual_type IS DISTINCT FROM 'c'::"char"
               OR actual_local IS DISTINCT FROM probe_local
               OR actual_inherited IS DISTINCT FROM probe_inherited
               OR actual_no_inherit IS DISTINCT FROM probe_no_inherit
               OR pg_catalog.pg_get_expr(
                      (SELECT conbin FROM pg_catalog.pg_constraint
                        WHERE oid = actual_oid),
                      (SELECT conrelid FROM pg_catalog.pg_constraint
                        WHERE oid = actual_oid),
                      false
                  )
                    IS DISTINCT FROM
                  pg_catalog.pg_get_expr(
                      (SELECT conbin FROM pg_catalog.pg_constraint
                        WHERE oid = probe_oid),
                      (SELECT conrelid FROM pg_catalog.pg_constraint
                        WHERE oid = probe_oid),
                      false
                  ) THEN
                RAISE EXCEPTION {_ql(mismatch)} USING ERRCODE = '55000';
            ELSE
                EXECUTE format(
                    'ALTER TABLE %I.%I DROP CONSTRAINT %I',
                    {_ql(schema)}, {_ql(table_name)}, {_ql(probe_name)}
                );
            END IF;
        END;
        $adopt_check$;
    """
    validate = f"ALTER TABLE {relation_ref} VALIDATE CONSTRAINT {_q(constraint_name)};"
    return add_probe, compare_or_adopt, validate


def _install_or_validate_owned_checks(schema: str) -> None:
    for probe_number, check_spec in enumerate(
        _owned_check_constraints(schema), start=1
    ):
        for statement in _adopt_check_constraint_sql(
            schema,
            *check_spec,
            probe_number,
        ):
            op.execute(statement)


def _fence_owned_adoption_tables(schema: str) -> None:
    owned_relations = (
        _ACQUISITION,
        _WORK,
        _RESOURCE,
        _EDGE,
        _TWIN_ATTEMPT,
        _TWIN_ADMISSION,
        _ROOTED_DATASET,
        _ROOTED_PROVENANCE,
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(_qf(schema, relation) for relation in owned_relations)
        + " IN ACCESS EXCLUSIVE MODE;"
    )
    nonempty = "\n               OR ".join(
        f"EXISTS (SELECT 1 FROM {_qf(schema, relation)} LIMIT 1)"
        for relation in owned_relations
    )
    op.execute(
        f"""
        DO $adoption_fence$ BEGIN
            IF {nonempty} THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_adoption_nonempty'
                    USING ERRCODE = '55000';
            END IF;
        END; $adoption_fence$;
        """
    )


def _create_indexes(schema: str) -> None:
    create_index_if_missing(
        op,
        "provider_directory_rooted_graph_work_claim_idx",
        _WORK,
        ["acquisition_id", "status", "lease_expires_at", "query_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_rooted_graph_dataset_current_idx",
        _ROOTED_DATASET,
        ["source_id"],
        unique=True,
        schema=schema,
        postgresql_where=sa.text("is_current = true"),
    )
    create_index_if_missing(
        op,
        "pd_rooted_graph_dataset_hash_idx",
        _ROOTED_DATASET,
        ["dataset_hash"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_rooted_graph_dataset_resource_origin_idx",
        _ROOTED_PROVENANCE,
        ["publication_acquisition_id", "resource_type", "resource_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_rooted_graph_plan_census_key",
        _WORK,
        ["acquisition_id", "kind"],
        unique=True,
        schema=schema,
        postgresql_where=sa.text("kind = 'full_insurance_plan_census'"),
    )
    create_index_if_missing(
        op,
        "provider_directory_rooted_graph_resource_closure_idx",
        _RESOURCE,
        ["acquisition_id", "closure_scope", "resource_type", "resource_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "provider_directory_rooted_graph_edge_target_idx",
        _EDGE,
        [
            "acquisition_id",
            "closure_scope",
            "target_resource_type",
            "target_resource_id",
        ],
        schema=schema,
    )


def _widen_legacy_previous_dataset_fk(schema: str) -> None:
    legacy = _qf(schema, _LEGACY_DATASET)
    op.execute(
        f"ALTER TABLE {legacy} DROP CONSTRAINT "
        f"{_q('pd_uhc_flex_practitioner_dataset_previous_fkey')};"
    )
    op.execute(
        f"ALTER TABLE {legacy} ADD CONSTRAINT "
        f"{_q('pd_uhc_flex_practitioner_dataset_previous_fkey')} "
        f"FOREIGN KEY (previous_dataset_id) REFERENCES {_qf(schema, _DATASET)}"
        "(dataset_id);"
    )


def _restore_legacy_previous_dataset_fk(schema: str) -> None:
    legacy = _qf(schema, _LEGACY_DATASET)
    op.execute(
        f"ALTER TABLE {legacy} DROP CONSTRAINT "
        f"{_q('pd_uhc_flex_practitioner_dataset_previous_fkey')};"
    )
    op.execute(
        f"ALTER TABLE {legacy} ADD CONSTRAINT "
        f"{_q('pd_uhc_flex_practitioner_dataset_previous_fkey')} "
        f"FOREIGN KEY (previous_dataset_id) REFERENCES {legacy}(dataset_id);"
    )


def _install_guards(schema: str) -> None:
    relations = (_ACQUISITION, _WORK, _RESOURCE, _EDGE)
    guards = {
        _ACQUISITION: _ACQUISITION_GUARD,
        _WORK: _WORK_GUARD,
        _RESOURCE: _RESOURCE_GUARD,
        _EDGE: _EDGE_GUARD,
    }
    for relation in relations:
        relation_ref = _qf(schema, relation)
        guard_ref = _qf(schema, guards[relation])
        op.execute(f"REVOKE ALL ON TABLE {relation_ref} FROM PUBLIC;")
        op.execute(f"REVOKE ALL ON FUNCTION {guard_ref}() FROM PUBLIC;")
        op.execute(
            f"CREATE TRIGGER {relation}_row_guard BEFORE INSERT OR UPDATE OR DELETE "
            f"ON {relation_ref} FOR EACH ROW EXECUTE FUNCTION {guard_ref}();"
        )
        op.execute(
            f"CREATE TRIGGER {relation}_truncate_guard BEFORE TRUNCATE ON "
            f"{relation_ref} FOR EACH STATEMENT EXECUTE FUNCTION {guard_ref}();"
        )
        op.execute(
            f"ALTER TABLE {relation_ref} ENABLE ALWAYS TRIGGER "
            f"{_q(relation + '_row_guard')};"
        )
        op.execute(
            f"ALTER TABLE {relation_ref} ENABLE ALWAYS TRIGGER "
            f"{_q(relation + '_truncate_guard')};"
        )
    for function_name in (_TERMINAL_SET, _RESOURCE_SET, _EDGE_SET, _ROOT_HASH):
        op.execute(
            f"REVOKE ALL ON FUNCTION {_qf(schema, function_name)}(text) FROM PUBLIC;"
        )
    budget_triggers = (
        (
            _WORK,
            "provider_directory_rooted_graph_work_budget_guard",
            _WORK_BUDGET_GUARD,
            "inserted_work",
        ),
        (
            _RESOURCE,
            "provider_directory_rooted_graph_resource_budget_guard",
            _RESOURCE_BUDGET_GUARD,
            "inserted_resource",
        ),
        (
            _EDGE,
            "provider_directory_rooted_graph_edge_budget_guard",
            _EDGE_BUDGET_GUARD,
            "inserted_edge",
        ),
    )
    for relation, trigger_name, function_name, transition_name in budget_triggers:
        op.execute(
            f"CREATE TRIGGER {_q(trigger_name)} AFTER INSERT ON "
            f"{_qf(schema, relation)} REFERENCING NEW TABLE AS "
            f"{_q(transition_name)} FOR EACH STATEMENT EXECUTE FUNCTION "
            f"{_qf(schema, function_name)}();"
        )
        op.execute(
            f"ALTER TABLE {_qf(schema, relation)} ENABLE ALWAYS TRIGGER "
            f"{_q(trigger_name)};"
        )
        op.execute(
            f"REVOKE ALL ON FUNCTION {_qf(schema, function_name)}() FROM PUBLIC;"
        )


def _install_publication_guards(schema: str) -> None:
    guarded_rows = (
        (_TWIN_ATTEMPT, _TWIN_ATTEMPT_GUARD),
        (_TWIN_ADMISSION, _TWIN_ADMISSION_GUARD),
        (_ROOTED_DATASET, _ROOTED_DATASET_GUARD),
        (_ROOTED_PROVENANCE, _ROOTED_PROVENANCE_GUARD),
    )
    for relation, function_name in guarded_rows:
        relation_ref = _qf(schema, relation)
        function_ref = _qf(schema, function_name)
        op.execute(f"REVOKE ALL ON TABLE {relation_ref} FROM PUBLIC;")
        op.execute(f"REVOKE ALL ON FUNCTION {function_ref}() FROM PUBLIC;")
        op.execute(
            f"CREATE TRIGGER {_q(relation + '_row_guard')} "
            f"BEFORE INSERT OR UPDATE OR DELETE ON {relation_ref} FOR EACH ROW "
            f"EXECUTE FUNCTION {function_ref}();"
        )
        op.execute(
            f"CREATE TRIGGER {_q(relation + '_truncate_guard')} "
            f"BEFORE TRUNCATE ON {relation_ref} FOR EACH STATEMENT "
            f"EXECUTE FUNCTION {function_ref}();"
        )
        for suffix in ("row_guard", "truncate_guard"):
            op.execute(
                f"ALTER TABLE {relation_ref} ENABLE ALWAYS TRIGGER "
                f"{_q(relation + '_' + suffix)};"
            )
    op.execute(
        f"CREATE CONSTRAINT TRIGGER {_q('pd_rooted_graph_dataset_valid_guard')} "
        f"AFTER INSERT OR UPDATE ON {_qf(schema, _ROOTED_DATASET)} "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
        f"{_qf(schema, _ROOTED_DATASET_GUARD)}();"
    )
    op.execute(
        f"ALTER TABLE {_qf(schema, _ROOTED_DATASET)} ENABLE ALWAYS TRIGGER "
        f"{_q('pd_rooted_graph_dataset_valid_guard')};"
    )
    dependency_ref = _qf(schema, _ROOTED_DEPENDENCY_GUARD)
    op.execute(f"REVOKE ALL ON FUNCTION {dependency_ref}() FROM PUBLIC;")
    for relation, (trigger, truncate) in _DATASET_DEPENDENCY_TRIGGERS.items():
        relation_ref = _qf(schema, relation)
        op.execute(
            f"CREATE TRIGGER {_q(trigger)} AFTER INSERT OR UPDATE OR DELETE ON "
            f"{relation_ref} FOR EACH STATEMENT EXECUTE FUNCTION {dependency_ref}();"
        )
        op.execute(
            f"CREATE TRIGGER {_q(truncate)} BEFORE TRUNCATE ON {relation_ref} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {dependency_ref}();"
        )
        for trigger_name in (trigger, truncate):
            op.execute(
                f"ALTER TABLE {relation_ref} ENABLE ALWAYS TRIGGER "
                f"{_q(trigger_name)};"
            )
    for relation in (_DATASET, _SOURCE, _ENDPOINT):
        relation_ref = _qf(schema, relation)
        trigger = f"pd_rooted_graph_{relation}_dependency_guard"
        op.execute(
            f"CREATE CONSTRAINT TRIGGER {_q(trigger)} AFTER INSERT OR UPDATE OR "
            f"DELETE ON {relation_ref} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
            f"EXECUTE FUNCTION {dependency_ref}();"
        )
        op.execute(f"ALTER TABLE {relation_ref} ENABLE ALWAYS TRIGGER {_q(trigger)};")
    current_ref = _qf(schema, _LOGICAL_CURRENT_GUARD)
    op.execute(f"REVOKE ALL ON FUNCTION {current_ref}() FROM PUBLIC;")
    for relation in (_DATASET, _LEGACY_DATASET, _ROOTED_DATASET):
        relation_ref = _qf(schema, relation)
        trigger = f"pd_exact_logical_current_{relation}_guard"
        op.execute(
            f"CREATE CONSTRAINT TRIGGER {_q(trigger)} AFTER INSERT OR UPDATE OR "
            f"DELETE ON {relation_ref} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
            f"EXECUTE FUNCTION {current_ref}();"
        )
        op.execute(f"ALTER TABLE {relation_ref} ENABLE ALWAYS TRIGGER {_q(trigger)};")
    for function_name in (
        _ROOTED_INTRINSIC_VALID,
        _ROOTED_OFFICIAL_LINEAGE_CURRENT,
        _ROOTED_VALID,
        _ROOTED_READY,
    ):
        op.execute(
            f"REVOKE ALL ON FUNCTION {_qf(schema, function_name)}(text) FROM PUBLIC;"
        )


def upgrade() -> None:
    schema = _schema()
    op.execute(
        "LOCK TABLE "
        + ", ".join(
            _qf(schema, relation)
            for relation in (
                _DATASET,
                _DATASET_RESOURCE,
                _DATASET_INSURANCE_PLAN,
                _DATASET_NETWORK_PLAN,
                _DATASET_AFFILIATION_ORGANIZATION,
                _LEGACY_DATASET,
                _SOURCE,
                _ENDPOINT,
            )
        )
        + " IN SHARE ROW EXCLUSIVE MODE;"
    )
    _widen_legacy_previous_dataset_fk(schema)
    _create_acquisition(schema)
    _create_work(schema)
    _create_resource(schema)
    _create_edge(schema)
    _create_twin_tables(schema)
    _create_publication_tables(schema)
    _fence_owned_adoption_tables(schema)
    _install_or_validate_owned_checks(schema)
    _create_indexes(schema)
    for statement in _hash_functions_sql(schema):
        op.execute(statement)
    op.execute(_rooted_intrinsic_valid_function_sql(schema))
    op.execute(_rooted_official_lineage_current_function_sql(schema))
    op.execute(_rooted_valid_function_sql(schema))
    op.execute(_rooted_ready_function_sql(schema))
    op.execute(_acquisition_guard_sql(schema))
    op.execute(_work_guard_sql(schema))
    op.execute(_resource_guard_sql(schema))
    op.execute(_edge_guard_sql(schema))
    for statement in _budget_guard_sql(schema):
        op.execute(statement)
    op.execute(_twin_attempt_guard_sql(schema))
    op.execute(_twin_admission_guard_sql(schema))
    op.execute(_rooted_header_guard_sql(schema))
    op.execute(_rooted_provenance_guard_sql(schema))
    op.execute(_rooted_dependency_guard_sql(schema))
    op.execute(_logical_current_guard_sql(schema))
    _install_guards(schema)
    _install_publication_guards(schema)


def downgrade() -> None:
    schema = _schema()
    owned_relations = (
        _ACQUISITION,
        _WORK,
        _RESOURCE,
        _EDGE,
        _TWIN_ATTEMPT,
        _TWIN_ADMISSION,
        _ROOTED_DATASET,
        _ROOTED_PROVENANCE,
    )
    external_relations = (
        _DATASET,
        _DATASET_RESOURCE,
        _DATASET_INSURANCE_PLAN,
        _DATASET_NETWORK_PLAN,
        _DATASET_AFFILIATION_ORGANIZATION,
        _LEGACY_DATASET,
        _SOURCE,
        _ENDPOINT,
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(
            _qf(schema, relation)
            for relation in (*owned_relations, *external_relations)
        )
        + " IN ACCESS EXCLUSIVE MODE;"
    )
    nonempty = "\n               OR ".join(
        f"EXISTS (SELECT 1 FROM {_qf(schema, relation)} LIMIT 1)"
        for relation in owned_relations
    )
    op.execute(
        f"""
        DO $downgrade$ BEGIN
            IF {nonempty} THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_downgrade_blocked'
                    USING ERRCODE = '55000';
            END IF;
        END; $downgrade$;
        """
    )

    for relation in (_DATASET, _LEGACY_DATASET, _ROOTED_DATASET):
        op.execute(
            f"DROP TRIGGER IF EXISTS "
            f"{_q('pd_exact_logical_current_' + relation + '_guard')} "
            f"ON {_qf(schema, relation)};"
        )
    for relation, trigger_names in _DATASET_DEPENDENCY_TRIGGERS.items():
        for trigger_name in trigger_names:
            op.execute(
                f"DROP TRIGGER IF EXISTS {_q(trigger_name)} "
                f"ON {_qf(schema, relation)};"
            )
    for relation in (_DATASET, _SOURCE, _ENDPOINT):
        op.execute(
            f"DROP TRIGGER IF EXISTS "
            f"{_q('pd_rooted_graph_' + relation + '_dependency_guard')} "
            f"ON {_qf(schema, relation)};"
        )
    op.execute(
        f"DROP TRIGGER IF EXISTS {_q('pd_rooted_graph_dataset_valid_guard')} "
        f"ON {_qf(schema, _ROOTED_DATASET)};"
    )
    for relation in (
        _TWIN_ATTEMPT,
        _TWIN_ADMISSION,
        _ROOTED_DATASET,
        _ROOTED_PROVENANCE,
        _ACQUISITION,
        _WORK,
        _RESOURCE,
        _EDGE,
    ):
        for suffix in ("row_guard", "truncate_guard"):
            op.execute(
                f"DROP TRIGGER IF EXISTS {_q(relation + '_' + suffix)} "
                f"ON {_qf(schema, relation)};"
            )
    for relation in (_WORK, _RESOURCE, _EDGE):
        op.execute(
            f"DROP TRIGGER IF EXISTS "
            f"{_q(relation + '_budget_guard')} ON {_qf(schema, relation)};"
        )

    for function_name in (
        _LOGICAL_CURRENT_GUARD,
        _ROOTED_DEPENDENCY_GUARD,
        _ROOTED_PROVENANCE_GUARD,
        _ROOTED_DATASET_GUARD,
        _TWIN_ADMISSION_GUARD,
        _TWIN_ATTEMPT_GUARD,
        _EDGE_BUDGET_GUARD,
        _RESOURCE_BUDGET_GUARD,
        _WORK_BUDGET_GUARD,
        _EDGE_GUARD,
        _RESOURCE_GUARD,
        _WORK_GUARD,
        _ACQUISITION_GUARD,
    ):
        op.execute(f"DROP FUNCTION {_qf(schema, function_name)}();")
    for function_name in (
        _ROOTED_READY,
        _ROOTED_VALID,
        _ROOTED_OFFICIAL_LINEAGE_CURRENT,
        _ROOTED_INTRINSIC_VALID,
        _ROOT_HASH,
        _EDGE_SET,
        _RESOURCE_SET,
        _TERMINAL_SET,
    ):
        op.execute(f"DROP FUNCTION {_qf(schema, function_name)}(text);")

    for index_name, table_name in (
        ("pd_rooted_graph_dataset_resource_origin_idx", _ROOTED_PROVENANCE),
        ("pd_rooted_graph_dataset_current_idx", _ROOTED_DATASET),
        ("pd_rooted_graph_dataset_hash_idx", _ROOTED_DATASET),
        ("provider_directory_rooted_graph_edge_target_idx", _EDGE),
        ("provider_directory_rooted_graph_resource_closure_idx", _RESOURCE),
        ("provider_directory_rooted_graph_plan_census_key", _WORK),
        ("provider_directory_rooted_graph_work_claim_idx", _WORK),
    ):
        op.drop_index(index_name, table_name=table_name, schema=schema)

    for relation in (
        _ROOTED_PROVENANCE,
        _ROOTED_DATASET,
        _TWIN_ADMISSION,
        _TWIN_ATTEMPT,
        _EDGE,
        _RESOURCE,
        _WORK,
        _ACQUISITION,
    ):
        op.drop_table(relation, schema=schema)
    _restore_legacy_previous_dataset_fk(schema)
